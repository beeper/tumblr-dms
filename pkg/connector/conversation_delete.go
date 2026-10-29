package connector

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/beeper/tumblr-dms/pkg/connector/tumblrdb"
	"github.com/beeper/tumblr-dms/pkg/tumblr"
)

const maxBlockCheckAttempts = 3

type conversationBlockCheckKey struct{}

type conversationBlockCheck struct {
	owner, peer string
	roomID      id.RoomID
	blocked     bool
	err         error
}

func (*conversationBlockCheck) Error() string {
	return "tumblr conversation deletion requires a block check"
}

func (tc *TumblrClient) resumeRemoteConversationDeleteWithSubmissionLock(
	ctx context.Context,
	conversationID string,
	expectedRevision int64,
) error {
	tc.syncLock.Lock()
	defer tc.syncLock.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	job, err := tc.connector.DB.Jobs.Get(ctx, tc.userLogin.ID, conversationID)
	if err != nil {
		return fmt.Errorf("failed to reload Tumblr conversation deletion: %w", err)
	}
	if expectedRevision > 0 && (job == nil || job.Revision != expectedRevision) {
		return errTumblrConversationJobSuperseded
	}
	if job == nil || job.DeleteRoomID == "" || job.NextAttemptAt.After(time.Now()) {
		if expectedRevision > 0 {
			return errTumblrConversationJobSuperseded
		}
		return nil
	}
	return tc.handleRemoteConversationDelete(ctx, conversationID, job.Revision)
}

func (tc *TumblrClient) handleRemoteConversationDelete(
	ctx context.Context,
	conversationID string,
	expectedRevision int64,
) error {
	if !validRemoteID(conversationID) {
		return fmt.Errorf("tumblr conversation ID is invalid")
	}
	if tc == nil || tc.connector == nil || tc.connector.Bridge == nil || tc.connector.DB == nil || tc.userLogin == nil {
		return fmt.Errorf("tumblr bridge is unavailable")
	}
	if err := tc.requireLoggedInForContext(ctx); err != nil {
		return err
	}
	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		return err
	}
	client, err := tc.tumblrClient()
	if err != nil {
		return err
	}
	response, probeErr := client.GetConversationBefore(ctx, meta.SelectedBlogName, conversationID, 1, "")
	if probeErr == nil {
		if err = validateConversationHistoryResponse(conversationID, response); err != nil {
			return fmt.Errorf("failed to validate Tumblr conversation before deletion: %w", err)
		}
		// A push or direct probe that proves the conversation exists must cancel
		// any stale delete continuation before normal sync runs again.
		if err = tc.connector.DB.Jobs.PutLiveConversation(ctx, tc.userLogin.ID, conversationID); err != nil {
			return fmt.Errorf("failed to cancel stale Tumblr conversation deletion: %w", err)
		}
		tc.wakeInboundSync()
		return nil
	}
	if !tumblr.IsNotFound(probeErr) {
		if tumblr.IsAuthError(probeErr) {
			return tc.handleRemoteError(probeErr)
		}
		return fmt.Errorf("failed to confirm deleted Tumblr conversation: %w", probeErr)
	}
	// Outbound recovery can discover the deletion before a sync job exists.
	// Persist the cleanup continuation before terminalizing sends so a crash or
	// later portal failure cannot strand a stale local room indefinitely.
	if err = tc.connector.DB.Jobs.Ensure(ctx, tc.userLogin.ID, conversationID); err != nil {
		return fmt.Errorf("failed to persist deleted Tumblr conversation cleanup: %w", err)
	}
	job, err := tc.connector.DB.Jobs.Get(ctx, tc.userLogin.ID, conversationID)
	if err != nil {
		return fmt.Errorf("failed to reload deleted Tumblr conversation cleanup: %w", err)
	}
	if expectedRevision <= 0 || job == nil || job.Revision != expectedRevision {
		return errTumblrConversationJobSuperseded
	}
	if job.DeleteRoomID == "" {
		listed, listErr := tc.conversationStillListed(ctx, client, meta.SelectedBlogUUID, job)
		if listErr != nil {
			return fmt.Errorf("failed to check Tumblr conversation list before deletion: %w", listErr)
		}
		if listed {
			if log := tc.log(); log != nil {
				log.Warn().Err(probeErr).
					Str("conversation_id_hash", logIdentifierHash(conversationID)).
					Msg("Tumblr still lists a conversation whose history returned not found, keeping its room")
			}
			return nil
		}
		portal, loadErr := tc.connector.Bridge.GetExistingPortalByKey(ctx, tc.portalKey(conversationID))
		if loadErr != nil {
			return loadErr
		}
		check, ok := ctx.Value(conversationBlockCheckKey{}).(*conversationBlockCheck)
		owner, peer, targetErr := tc.blockTarget(portal)
		if ok && (targetErr != nil || portal.MXID == "") {
			return fmt.Errorf("tumblr deletion target became ineligible during block check")
		}
		if targetErr == nil && portal.MXID != "" {
			if !ok {
				tc.wakeInboundSync()
				return &conversationBlockCheck{owner: owner, peer: peer, roomID: portal.MXID}
			}
			if check.owner != owner || check.peer != peer || check.roomID != portal.MXID {
				return fmt.Errorf("tumblr deletion target changed during block check")
			}
			if check.err != nil && (tumblr.IsAuthError(check.err) || job.AttemptCount < maxBlockCheckAttempts) {
				return fmt.Errorf("failed to check Tumblr blocks before conversation deletion: %w", check.err)
			}
			if check.blocked {
				tc.resyncDMBlock(portal)
				return fmt.Errorf("tumblr conversation is unavailable while its peer is blocked")
			}
		}
	}
	if err := tc.terminalizeOutboundConversation(ctx, conversationID); err != nil {
		return fmt.Errorf("failed to cancel outbound sends for deleted Tumblr conversation: %w", err)
	}

	portalKey := tc.portalKey(conversationID)
	var deleteRoomID id.RoomID
	err = tc.withPortalMutationLock(ctx, func() error {
		// Commit Tumblr-owned portal identity changes synchronously while the
		// coordinator is held. Matrix room cleanup is retryable and happens after
		// release. The deadline prevents a stalled dependency from stranding every
		// later portal mutation while still giving this atomic phase time to finish.
		mutationCtx, cancelMutation := context.WithTimeout(
			context.WithoutCancel(ctx),
			tumblrPortalMutationTimeout,
		)
		defer cancelMutation()
		persisted, loadErr := tc.connector.Bridge.DB.Portal.GetByKey(mutationCtx, portalKey)
		if loadErr != nil {
			return fmt.Errorf("failed to load persisted deleted Tumblr conversation: %w", loadErr)
		}
		portal, loadErr := tc.loadPortalWithPersistedParity(mutationCtx, portalKey, persisted)
		if loadErr != nil {
			return fmt.Errorf("failed to verify deleted Tumblr portal cache: %w", loadErr)
		}
		var deleteErr error
		deleteRoomID, deleteErr = tc.deletedConversationRoomID(
			mutationCtx,
			conversationID,
			expectedRevision,
			portal,
		)
		if deleteErr != nil {
			return deleteErr
		}
		if portal != nil && portal.Portal != nil {
			if portal.Receiver == "" || portal.Receiver != tc.userLogin.ID {
				return fmt.Errorf("deleted Tumblr portal is not owned by this login")
			}
			if deleteErr = portal.Delete(mutationCtx); deleteErr != nil {
				return fmt.Errorf("failed to remove deleted Tumblr portal: %w", deleteErr)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	cleanupCtx, cancelCleanup := context.WithTimeout(ctx, tumblrPortalMutationTimeout)
	defer cancelCleanup()
	if err = tc.finishTumblrConversationDelete(cleanupCtx, portalKey, deleteRoomID); err != nil {
		return err
	}

	// Close the small window between the first outbox update and Matrix room
	// deletion. After the remote delete event completes, the room can no longer
	// accept new sends, so this second pass is the final pending-send boundary.
	if err = tc.terminalizeOutboundConversation(cleanupCtx, conversationID); err != nil {
		return fmt.Errorf("failed to finish cancelling outbound sends for deleted Tumblr conversation: %w", err)
	}
	if err = tc.connector.DB.ConversationSync.Delete(cleanupCtx, tc.userLogin.ID, conversationID); err != nil {
		return fmt.Errorf("failed to clear deleted Tumblr conversation sync state: %w", err)
	}
	tc.clearDeletedConversationSeenState(conversationID)
	if log := tc.log(); log != nil {
		log.Info().Str("conversation_id_hash", logIdentifierHash(conversationID)).
			Msg("Removed Tumblr conversation after direct deletion confirmation")
	}
	return nil
}

func (tc *TumblrClient) conversationStillListed(
	ctx context.Context,
	client *tumblr.Client,
	selectedBlogUUID string,
	job *tumblrdb.ConversationJob,
) (bool, error) {
	conversationID := job.ConversationID
	if listed, ok := tc.recentListedConversation(job); ok {
		return listed, nil
	}
	walkStartedAt := time.Now()
	limit := tc.connector.Config.ConversationSyncBatchLimit()
	before := ""
	seenCursors := make(map[string]struct{})
	remoteConversationIDs := make(map[string]struct{})
	for page := 0; page < maxConversationListPages; page++ {
		resp, err := client.ListConversationsBefore(ctx, selectedBlogUUID, limit, before)
		if err != nil {
			return false, tc.handleRemoteError(err)
		}
		if resp == nil {
			return false, fmt.Errorf("tumblr conversation list response is missing")
		}
		for _, conversation := range resp.Conversations {
			if conversation.ID == conversationID {
				return true, nil
			}
			if validRemoteID(conversation.ID) {
				remoteConversationIDs[conversation.ID] = struct{}{}
			}
		}
		nextBefore, err := resp.NextBefore()
		if err != nil {
			return false, err
		}
		nextBefore = strings.TrimSpace(nextBefore)
		if nextBefore == "" {
			tc.rememberListedConversations(remoteConversationIDs, walkStartedAt)
			return false, nil
		}
		if _, duplicate := seenCursors[nextBefore]; duplicate {
			return false, fmt.Errorf("tumblr conversation list cursor repeated")
		}
		seenCursors[nextBefore] = struct{}{}
		before = nextBefore
	}
	return false, fmt.Errorf("tumblr conversation list exceeded %d pages", maxConversationListPages)
}

type queuedMissingConversation struct {
	revision  int64
	createdAt time.Time
}

func (tc *TumblrClient) rememberListedConversations(conversationIDs map[string]struct{}, walkStartedAt time.Time) {
	tc.listedConversationsLock.Lock()
	defer tc.listedConversationsLock.Unlock()
	tc.listedConversations = maps.Clone(conversationIDs)
	tc.listedConversationsAt = walkStartedAt
	tc.listedMissingQueued = nil
}

func (tc *TumblrClient) rememberQueuedMissingConversations(
	walkStartedAt time.Time,
	queued map[string]queuedMissingConversation,
) {
	tc.listedConversationsLock.Lock()
	defer tc.listedConversationsLock.Unlock()
	if tc.listedConversationsAt.Equal(walkStartedAt) {
		tc.listedMissingQueued = queued
	}
}

func (tc *TumblrClient) recentListedConversation(job *tumblrdb.ConversationJob) (listed, ok bool) {
	tc.listedConversationsLock.Lock()
	defer tc.listedConversationsLock.Unlock()
	if tc.listedConversations == nil || time.Since(tc.listedConversationsAt) > tumblrDeletionReconciliationInterval {
		return false, false
	}
	if _, listed = tc.listedConversations[job.ConversationID]; listed {
		return true, true
	}
	if tc.listedConversationsAt.After(job.UpdatedAt) {
		return false, true
	}
	queued, fromSnapshot := tc.listedMissingQueued[job.ConversationID]
	return false, fromSnapshot && queued.revision == job.Revision && queued.createdAt.Equal(job.CreatedAt)
}

func (tc *TumblrClient) deletedConversationRoomID(
	ctx context.Context,
	conversationID string,
	expectedRevision int64,
	portal *bridgev2.Portal,
) (id.RoomID, error) {
	if portal != nil && portal.Portal != nil && portal.MXID != "" {
		changed, err := tc.connector.DB.Jobs.SetDeleteRoom(
			ctx,
			tc.userLogin.ID,
			conversationID,
			expectedRevision,
			portal.MXID,
		)
		if err != nil {
			return "", fmt.Errorf("failed to persist deleted Tumblr room cleanup: %w", err)
		}
		if !changed {
			return "", errTumblrConversationJobSuperseded
		}
		return portal.MXID, nil
	}
	job, err := tc.connector.DB.Jobs.Get(ctx, tc.userLogin.ID, conversationID)
	if err != nil {
		return "", fmt.Errorf("failed to load deleted Tumblr room cleanup: %w", err)
	}
	if job == nil {
		return "", errTumblrConversationJobSuperseded
	}
	if job.Revision != expectedRevision {
		return "", errTumblrConversationJobSuperseded
	}
	return job.DeleteRoomID, nil
}

func (tc *TumblrClient) finishTumblrConversationDelete(
	ctx context.Context,
	portalKey networkid.PortalKey,
	roomID id.RoomID,
) error {
	persisted, err := tc.connector.Bridge.DB.Portal.GetByKey(ctx, portalKey)
	if err != nil {
		return fmt.Errorf("failed to load persisted deleted Tumblr portal: %w", err)
	}
	portal, err := tc.loadPortalWithPersistedParity(ctx, portalKey, persisted)
	if err != nil {
		return fmt.Errorf("failed to verify deleted Tumblr portal cache: %w", err)
	}
	if portal != nil && portal.Portal != nil {
		return fmt.Errorf("deleted Tumblr portal still exists after cleanup")
	}
	if roomID == "" {
		return nil
	}
	if err = tc.ensureTumblrMatrixRoomDeleted(ctx, roomID); err != nil {
		return err
	}
	return tc.ensureTumblrFilteringSpaceChildRemoved(ctx, roomID)
}

func (tc *TumblrClient) ensureTumblrFilteringSpaceChildRemoved(ctx context.Context, roomID id.RoomID) error {
	if roomID == "" {
		return nil
	}
	if tc == nil || tc.connector == nil || tc.connector.Bridge == nil || tc.connector.Bridge.Config == nil ||
		tc.connector.Bridge.Bot == nil || tc.userLogin == nil || tc.userLogin.UserLogin == nil {
		return fmt.Errorf("tumblr filtering-space cleanup is unavailable")
	}
	if !tc.connector.Bridge.Config.PersonalFilteringSpaces || tc.userLogin.SpaceRoom == "" {
		return nil
	}
	_, err := tc.connector.Bridge.Bot.SendState(
		ctx,
		tc.userLogin.SpaceRoom,
		event.StateSpaceChild,
		roomID.String(),
		&event.Content{Parsed: &event.SpaceChildEventContent{}},
		time.Now(),
	)
	if errors.Is(err, mautrix.MNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to remove deleted Tumblr room from its filtering space: %w", err)
	}
	return nil
}

func (tc *TumblrClient) ensureTumblrMatrixRoomDeleted(ctx context.Context, roomID id.RoomID) error {
	if tc == nil || tc.connector == nil || tc.connector.Bridge == nil || tc.connector.Bridge.Matrix == nil ||
		tc.connector.Bridge.Bot == nil {
		return fmt.Errorf("tumblr Matrix room cleanup is unavailable")
	}
	if roomID == "" {
		return nil
	}
	_, probeErr := tc.connector.Bridge.Matrix.GetMembers(ctx, roomID)
	if errors.Is(probeErr, mautrix.MNotFound) {
		return nil
	}
	deleteErr := tc.connector.Bridge.Bot.DeleteRoom(ctx, roomID, false)
	if errors.Is(deleteErr, mautrix.MNotFound) {
		// The room deletion endpoint is authoritative even if a crash left a
		// stale member snapshot in the local Matrix state store.
		return nil
	}
	if deleteErr != nil {
		if probeErr != nil {
			return errors.Join(
				fmt.Errorf("failed to confirm deleted Tumblr Matrix room: %w", probeErr),
				fmt.Errorf("failed to retry deleted Tumblr Matrix room cleanup: %w", deleteErr),
			)
		}
		return fmt.Errorf("failed to retry deleted Tumblr Matrix room cleanup: %w", deleteErr)
	}
	// A successful Beeper room-delete response is authoritative. The bot is no
	// longer joined afterward, so a /members probe normally returns M_FORBIDDEN
	// even though deletion succeeded. Crash recovery remains idempotent because
	// a later retry sees M_NOT_FOUND from the same delete endpoint.
	return nil
}

func (tc *TumblrClient) clearDeletedConversationSeenState(conversationID string) {
	tc.seenLock.Lock()
	defer tc.seenLock.Unlock()

	delete(tc.seenConversations, conversationID)
	delete(tc.seenConversationModifiedTS, conversationID)
	delete(tc.bridgedReadTimestamps, conversationID)
	prefix := conversationID + "\x00"
	kept := tc.seenMessageOrder[:0]
	for _, cacheKey := range tc.seenMessageOrder {
		if strings.HasPrefix(cacheKey, prefix) {
			delete(tc.seenMessages, cacheKey)
			continue
		}
		kept = append(kept, cacheKey)
	}
	tc.seenMessageOrder = kept
}
