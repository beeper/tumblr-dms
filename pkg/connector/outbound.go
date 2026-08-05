package connector

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"

	"github.com/ifixrobots/tumblr-dms/pkg/connector/tumblrdb"
	"github.com/ifixrobots/tumblr-dms/pkg/msgconv"
	"github.com/ifixrobots/tumblr-dms/pkg/tumblr"
	"github.com/ifixrobots/tumblr-dms/pkg/tumblrid"
)

const (
	outboundResolvedDelay       = time.Second
	outboundUnconfirmedTimeout  = 2 * time.Minute
	outboundStatusClaimTTL      = 2 * time.Minute
	outboundRecoveryRetry       = 5 * time.Minute
	outboundRecoveryRetryMax    = time.Hour
	outboundRecoveryMaxAge      = 24 * time.Hour
	outboundTerminalRetention   = 30 * 24 * time.Hour
	outboundWorkerPollInterval  = 2 * time.Second
	outboundMatchBefore         = 2 * time.Minute
	outboundMatchAfter          = 15 * time.Minute
	outboundInitialRecoveryMax  = 20
	outboundRecoveryPassMax     = 100
	outboundUnboundSearchPages  = 5
	outboundUnconfirmedMessage  = "Tumblr may have sent this message, but Beeper couldn't confirm it. It wasn't sent again to avoid a duplicate."
	outboundNotSubmittedMessage = "Tumblr did not receive this message. Try sending it again."
)

var errOutboundMappingConflict = errors.New("tumblr message is already mapped to another Matrix event")

type outboundPendingRegistration struct {
	message *bridgev2.MatrixMessage
}

type outboundCandidateResult struct {
	match         bool
	indeterminate bool
}

type outboundCandidateSet struct {
	send       *tumblrdb.OutboundSend
	messageIDs []string
}

type outboundReconciliationResult struct {
	transactions   map[string]networkid.TransactionID
	heldMessageIDs map[string]struct{}
}

// forcedOutboundCandidateMatches maximizes how many eligible remote echoes are
// explained by distinct durable sends, then returns only edges present in every
// maximum-cardinality matching. This resolves A->{A,B}, B->{B} without choosing
// an arbitrary edge in a genuinely ambiguous component.
func forcedOutboundCandidateMatches(sets []outboundCandidateSet) map[networkid.TransactionID]string {
	forced := make(map[networkid.TransactionID]string)
	baseline, baselineSize := maximumOutboundCandidateMatching(sets, -1, "")
	if baselineSize == 0 {
		return forced
	}
	for setIndex, messageID := range baseline {
		if messageID == "" || sets[setIndex].send == nil {
			continue
		}
		_, withoutEdgeSize := maximumOutboundCandidateMatching(sets, setIndex, messageID)
		if withoutEdgeSize < baselineSize {
			forced[sets[setIndex].send.TransactionID] = messageID
		}
	}
	return forced
}

func maximumOutboundCandidateMatching(
	sets []outboundCandidateSet,
	blockedSetIndex int,
	blockedMessageID string,
) ([]string, int) {
	matchedSetByMessage := make(map[string]int)
	var assign func(int, map[string]struct{}) bool
	assign = func(setIndex int, visited map[string]struct{}) bool {
		for _, messageID := range sets[setIndex].messageIDs {
			if messageID == "" || (setIndex == blockedSetIndex && messageID == blockedMessageID) {
				continue
			}
			if _, seen := visited[messageID]; seen {
				continue
			}
			visited[messageID] = struct{}{}
			matchedSet, occupied := matchedSetByMessage[messageID]
			if !occupied || assign(matchedSet, visited) {
				matchedSetByMessage[messageID] = setIndex
				return true
			}
		}
		return false
	}

	matchBySet := make([]string, len(sets))
	matchingSize := 0
	for setIndex := range sets {
		if assign(setIndex, make(map[string]struct{})) {
			matchingSize++
		}
	}
	for messageID, setIndex := range matchedSetByMessage {
		matchBySet[setIndex] = messageID
	}
	return matchBySet, matchingSize
}

func outboundTransactionID(loginID networkid.UserLoginID, eventID string) networkid.TransactionID {
	sum := sha256.Sum256([]byte(string(loginID) + "\x00" + eventID))
	return networkid.TransactionID("tumblr-outbound-" + hex.EncodeToString(sum[:]))
}

func newOutboundStatusClaimToken() (string, error) {
	var token [16]byte
	if _, err := cryptorand.Read(token[:]); err != nil {
		return "", fmt.Errorf("generate outbound status claim token: %w", err)
	}
	return hex.EncodeToString(token[:]), nil
}

func outboundStatusTransactionID(send *tumblrdb.OutboundSend, messageStatus *bridgev2.MessageStatus, channel string) string {
	hasher := sha256.New()
	for _, field := range []string{
		"tumblr-outbound-status-v1",
		channel,
		string(send.BridgeID),
		string(send.UserLoginID),
		string(send.TransactionID),
		string(send.MatrixRoomID),
		string(send.MatrixEventID),
		string(send.InputTransactionID),
		string(messageStatus.Status),
		string(messageStatus.ErrorReason),
		messageStatus.Message,
		fmt.Sprintf("%t", messageStatus.IsCertain),
		string(messageStatus.Step),
		fmt.Sprintf("%d", messageStatus.RetryNum),
	} {
		_, _ = fmt.Fprintf(hasher, "%d:", len(field))
		_, _ = hasher.Write([]byte(field))
	}
	return "tumblr-status-" + hex.EncodeToString(hasher.Sum(nil))
}

func outboundMessageType(messageType string) (tumblrdb.OutboundMessageType, error) {
	switch messageType {
	case tumblr.MessageTypeText:
		return tumblrdb.OutboundMessageText, nil
	case tumblr.MessageTypeImage:
		return tumblrdb.OutboundMessageImage, nil
	case tumblr.MessageTypePostRef:
		return tumblrdb.OutboundMessagePostRef, nil
	default:
		return "", fmt.Errorf("unsupported outbound Tumblr message type %q", messageType)
	}
}

func outboundContentHash(messageType, text string, image tumblr.ImageUpload, post *tumblr.PostShare) (tumblrdb.OutboundContentHash, error) {
	switch messageType {
	case tumblr.MessageTypeText:
		return tumblrdb.HashOutboundContent([]byte(text)), nil
	case tumblr.MessageTypeImage:
		if len(image.Data) == 0 {
			return tumblrdb.OutboundContentHash{}, fmt.Errorf("outbound image content is empty")
		}
		visualHash, err := tumblr.HashVisualImageContent(image.Data)
		if errors.Is(err, tumblr.ErrVisualImageHashUnavailable) {
			return tumblrdb.HashOutboundContent(image.Data), nil
		}
		if err != nil {
			return tumblrdb.OutboundContentHash{}, fmt.Errorf("%w: %v", bridgev2.ErrUnsupportedMediaType, err)
		}
		return tumblrdb.OutboundContentHash(visualHash), nil
	case tumblr.MessageTypePostRef:
		if post == nil || strings.TrimSpace(post.ID) == "" {
			return tumblrdb.OutboundContentHash{}, fmt.Errorf("outbound post ID is missing")
		}
		return tumblrdb.HashOutboundContent([]byte(strings.TrimSpace(post.ID))), nil
	default:
		return tumblrdb.OutboundContentHash{}, fmt.Errorf("unsupported outbound Tumblr message type %q", messageType)
	}
}

func outboundDatabaseMessage(send *tumblrdb.OutboundSend, remote *tumblr.Message) *database.Message {
	timestamp := send.SendStartedAt
	if remote != nil {
		if remoteTS, ok := saneTumblrTimestamp(remote.Timestamp, time.Now()); ok {
			timestamp = remoteTS
		}
	}
	return &database.Message{
		ID:         tumblrid.MakeMessageID(string(send.RemoteMessageID)),
		MXID:       send.MatrixEventID,
		Room:       send.PortalKey,
		SenderID:   send.SenderID,
		SenderMXID: send.MatrixSenderID,
		Timestamp:  timestamp,
		SendTxnID:  send.InputTransactionID,
		Metadata: &MessageMetadata{
			Type: msgconv.MessageMetadataType(string(send.MessageType)),
		},
	}
}

func (tc *TumblrClient) registerOutboundPending(msg *bridgev2.MatrixMessage, send *tumblrdb.OutboundSend) *database.Message {
	dbMessage := outboundDatabaseMessage(send, nil)
	dbMessage.ID = ""
	tc.outboundLock.Lock()
	if tc.outboundPending == nil {
		tc.outboundPending = make(map[networkid.TransactionID]*outboundPendingRegistration)
	}
	tc.outboundPending[send.TransactionID] = &outboundPendingRegistration{message: msg}
	tc.outboundLock.Unlock()
	msg.AddPendingToSave(dbMessage, send.TransactionID, tc.outboundEchoHandler(send.TransactionID))
	return dbMessage
}

func (tc *TumblrClient) outboundEchoHandler(txnID networkid.TransactionID) bridgev2.RemoteEchoHandler {
	return func(remote bridgev2.RemoteMessage, dbMessage *database.Message) (bool, error) {
		if !tc.beginOwnedOperation() {
			// The replacement client owns durable recovery, including the final
			// customer-visible status. Do not publish success from this stale client.
			return false, bridgev2.ErrNoStatus
		}
		defer tc.endOwnedOperation()
		tc.dropOutboundPending(txnID, false)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		send, err := tc.connector.DB.Outbound.Get(ctx, tc.userLogin.ID, txnID)
		if err != nil {
			if log := tc.log(); log != nil {
				log.Warn().Err(err).Msg("Could not load a Tumblr outbound echo claim; deferring status to recovery")
			}
			tc.wakeOutboundSync()
			return false, bridgev2.ErrNoStatus
		}
		if send == nil {
			if log := tc.log(); log != nil {
				log.Warn().Msg("Tumblr outbound echo arrived after its durable claim was cleaned up")
			}
			tc.wakeOutboundSync()
			return false, bridgev2.ErrNoStatus
		}
		remoteID := remote.GetID()
		// Reconciliation assigns the transaction only after claiming the remote
		// ID under outboundGraphLock. bridgev2 invokes this callback while holding
		// its own pending-message lock, so the callback must remain graph-read-only
		// to avoid lock inversion with AddPendingToSave/RemovePending.
		if send.State != tumblrdb.OutboundSendResolved || send.RemoteMessageID != remoteID {
			if log := tc.log(); log != nil {
				log.Warn().
					Str("outbound_state", string(send.State)).
					Str("expected_remote_id", string(send.RemoteMessageID)).
					Str("echo_remote_id", string(remoteID)).
					Msg("Tumblr outbound echo did not match its durable claim")
			}
			tc.wakeOutboundSync()
			return false, bridgev2.ErrNoStatus
		}
		dbMessage.ID = remoteID
		dbMessage.Room = send.PortalKey
		dbMessage.SenderID = send.SenderID
		dbMessage.SenderMXID = send.MatrixSenderID
		dbMessage.SendTxnID = send.InputTransactionID
		dbMessage.Metadata = &MessageMetadata{Type: msgconv.MessageMetadataType(string(send.MessageType))}
		if err = tc.persistOutboundMapping(ctx, send, dbMessage); err != nil {
			if log := tc.log(); log != nil {
				log.Warn().Err(err).Msg("Could not persist a resolved Tumblr outbound message mapping; deferring status to recovery")
			}
			tc.wakeOutboundSync()
			return false, bridgev2.ErrNoStatus
		}
		tc.markConversationMessageSeen(send.ConversationID, string(remoteID))
		tc.wakeOutboundSync()
		return false, bridgev2.ErrNoStatus
	}
}

func (tc *TumblrClient) dropOutboundPending(txnID networkid.TransactionID, removeFramework bool) {
	tc.outboundLock.Lock()
	pending := tc.outboundPending[txnID]
	delete(tc.outboundPending, txnID)
	tc.outboundLock.Unlock()
	if removeFramework && pending != nil && pending.message != nil {
		pending.message.RemovePending(txnID)
	}
}

// terminalizeOutboundConversation is the only connector-side entry point for
// cancelling pending sends after Tumblr confirms that a conversation was
// deleted. Callers must hold the login submission fence and must not already
// hold outboundGraphLock.
func (tc *TumblrClient) terminalizeOutboundConversation(ctx context.Context, conversationID string) error {
	if tc == nil || tc.connector == nil || tc.connector.DB == nil || tc.userLogin == nil {
		return fmt.Errorf("tumblr outbound database is unavailable")
	}
	tc.outboundGraphLock.Lock()
	defer tc.outboundGraphLock.Unlock()
	now := time.Now()
	txnIDs, err := tc.connector.DB.Outbound.TerminalizeConversation(
		ctx,
		tc.userLogin.ID,
		conversationID,
		now,
		now.Add(time.Millisecond),
	)
	if err != nil {
		return err
	}
	for _, txnID := range txnIDs {
		tc.dropOutboundPending(txnID, true)
	}
	tc.wakeOutboundSync()
	return nil
}

func (tc *TumblrClient) activeOutboundPending(send *tumblrdb.OutboundSend) bool {
	if send == nil {
		return false
	}
	tc.outboundLock.Lock()
	pending := tc.outboundPending[send.TransactionID]
	tc.outboundLock.Unlock()
	return pending != nil && pending.message != nil && pending.message.Portal != nil &&
		pending.message.Portal.Portal != nil && pending.message.Portal.MXID == send.MatrixRoomID
}

func (tc *TumblrClient) persistOutboundMapping(ctx context.Context, send *tumblrdb.OutboundSend, dbMessage *database.Message) error {
	if tc == nil || tc.connector == nil || tc.connector.Bridge == nil || tc.connector.Bridge.DB == nil ||
		tc.connector.DB == nil || tc.userLogin == nil || send == nil || dbMessage == nil {
		return fmt.Errorf("tumblr outbound mapping database is unavailable")
	}
	if send.State != tumblrdb.OutboundSendResolved || send.RemoteMessageID == "" || send.ConversationID == "" {
		return fmt.Errorf("tumblr outbound send is not resolved")
	}
	dbMessage.ID = send.RemoteMessageID
	dbMessage.MXID = send.MatrixEventID
	dbMessage.Room = send.PortalKey
	dbMessage.SenderID = send.SenderID
	dbMessage.SenderMXID = send.MatrixSenderID
	dbMessage.SendTxnID = send.InputTransactionID
	if dbMessage.Timestamp.IsZero() {
		dbMessage.Timestamp = send.SendStartedAt
	}
	if dbMessage.Metadata == nil {
		dbMessage.Metadata = &MessageMetadata{Type: msgconv.MessageMetadataType(string(send.MessageType))}
	}
	portal, err := tc.connector.Bridge.GetExistingPortalByKey(ctx, send.PortalKey)
	if err != nil {
		return fmt.Errorf("load Tumblr portal for outbound mapping: %w", err)
	}
	if portal == nil || portal.Portal == nil {
		return fmt.Errorf("%w: Tumblr portal for outbound mapping does not exist", errOutboundMappingConflict)
	}
	if portal.MXID == "" || portal.MXID != send.MatrixRoomID {
		return fmt.Errorf("%w: Tumblr portal does not own the outbound Matrix room", errOutboundMappingConflict)
	}
	if _, err = tc.connector.Bridge.GetGhostByID(ctx, send.SenderID); err != nil {
		return fmt.Errorf("load Tumblr sender for outbound mapping: %w", err)
	}
	existingByMXID, err := tc.connector.Bridge.DB.Message.GetPartByMXID(ctx, send.MatrixEventID)
	if err != nil {
		return err
	}
	if existingByMXID != nil {
		if existingByMXID.ID != send.RemoteMessageID || existingByMXID.Room != send.PortalKey {
			return fmt.Errorf("%w: matrix event is already mapped to another Tumblr message", errOutboundMappingConflict)
		}
		return nil
	}
	existingByRemote, err := tc.connector.Bridge.DB.Message.GetFirstPartByID(ctx, send.PortalKey.Receiver, send.RemoteMessageID)
	if err != nil {
		return err
	}
	if existingByRemote != nil {
		if existingByRemote.MXID != send.MatrixEventID || existingByRemote.Room != send.PortalKey {
			return errOutboundMappingConflict
		}
		return nil
	}
	if err = tc.connector.Bridge.DB.Message.Insert(ctx, dbMessage); err != nil {
		byMXID, mxidErr := tc.connector.Bridge.DB.Message.GetPartByMXID(ctx, send.MatrixEventID)
		if mxidErr == nil && byMXID != nil && byMXID.ID == send.RemoteMessageID && byMXID.Room == send.PortalKey {
			return nil
		}
		byRemote, remoteErr := tc.connector.Bridge.DB.Message.GetFirstPartByID(ctx, send.PortalKey.Receiver, send.RemoteMessageID)
		if remoteErr == nil && byRemote != nil && (byRemote.MXID != send.MatrixEventID || byRemote.Room != send.PortalKey) {
			return fmt.Errorf("%w: %v", errOutboundMappingConflict, err)
		}
		return fmt.Errorf("save Tumblr outbound message mapping: %w", err)
	}
	return nil
}

func (tc *TumblrClient) retainCompletedOutbound(ctx context.Context, send *tumblrdb.OutboundSend) error {
	completedAt := time.Now()
	if err := tc.connector.DB.Outbound.MarkCompleted(
		ctx,
		send.UserLoginID,
		send.TransactionID,
		send.RemoteMessageID,
		completedAt,
	); err != nil {
		return fmt.Errorf("retain completed Tumblr outbound send: %w", err)
	}
	send.State = tumblrdb.OutboundSendCompleted
	send.TerminalAt = completedAt
	send.NextAttemptAt = time.UnixMilli(tumblrdb.OutboundCompletedReceiptUnixMilli)
	return nil
}

func (tc *TumblrClient) startOutboundSync(generation *connectionGeneration) {
	if tc == nil || generation == nil || !tc.isCurrentGeneration(generation) || tc.connector == nil ||
		tc.connector.DB == nil || tc.userLogin == nil {
		return
	}
	tc.outboundLock.Lock()
	if tc.outboundGeneration == generation.id {
		tc.outboundLock.Unlock()
		return
	}
	tc.outboundGeneration = generation.id
	tc.outboundWake = make(chan struct{}, 1)
	wake := tc.outboundWake
	generation.wg.Add(1)
	tc.outboundLock.Unlock()
	go tc.outboundSyncLoop(generation, wake)
}

func (tc *TumblrClient) wakeOutboundSync() {
	tc.outboundLock.Lock()
	wake := tc.outboundWake
	tc.outboundLock.Unlock()
	if wake == nil {
		return
	}
	select {
	case wake <- struct{}{}:
	default:
	}
}

func (tc *TumblrClient) outboundSyncLoop(generation *connectionGeneration, wake <-chan struct{}) {
	defer generation.wg.Done()
	defer func() {
		tc.outboundLock.Lock()
		if tc.outboundGeneration == generation.id {
			tc.outboundGeneration = 0
			tc.outboundWake = nil
		}
		tc.outboundLock.Unlock()
	}()
	ticker := time.NewTicker(outboundWorkerPollInterval)
	defer ticker.Stop()
	for {
		if err := tc.processDueOutboundSends(generation.ctx, outboundRecoveryPassMax); err != nil && generation.ctx.Err() == nil {
			if log := tc.log(); log != nil {
				log.Warn().Err(err).Msg("Tumblr outbound recovery pass failed")
			}
		}
		select {
		case <-generation.ctx.Done():
			return
		case <-wake:
		case <-ticker.C:
		}
	}
}

func (tc *TumblrClient) processDueOutboundSends(ctx context.Context, limit int) error {
	if tc == nil || tc.connector == nil || tc.connector.DB == nil || tc.userLogin == nil {
		return fmt.Errorf("tumblr outbound database is unavailable")
	}
	if !tc.outboundProcessLock.TryLock() {
		return nil
	}
	defer tc.outboundProcessLock.Unlock()
	for processed := 0; ctx.Err() == nil && (limit <= 0 || processed < limit); processed++ {
		send, err := tc.processNextDueOutboundSend(ctx)
		if err != nil || send == nil {
			return err
		}
	}
	return ctx.Err()
}

func (tc *TumblrClient) processNextDueOutboundSend(ctx context.Context) (*tumblrdb.OutboundSend, error) {
	lockCtx, releaseSubmission, err := tc.acquireOutboundSubmissionLock(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseSubmission()

	send, needsSync, err := tc.prepareNextDueOutboundSend(lockCtx)
	if err != nil || send == nil || !needsSync {
		return send, err
	}
	syncErr := tc.syncConversationByIDWithSubmissionLock(lockCtx, send.ConversationID, 0)
	if syncErr != nil {
		if log := tc.log(); log != nil {
			log.Warn().Err(syncErr).Msg("Could not reconcile a pending Tumblr outbound message")
		}
	}
	return send, tc.finishDueOutboundSend(lockCtx, send.TransactionID, syncErr)
}

func (tc *TumblrClient) prepareNextDueOutboundSend(ctx context.Context) (*tumblrdb.OutboundSend, bool, error) {
	tc.outboundGraphLock.Lock()
	defer tc.outboundGraphLock.Unlock()

	send, err := tc.connector.DB.Outbound.GetNextDue(ctx, tc.userLogin.ID, time.Now())
	if err != nil || send == nil {
		return send, false, err
	}
	if send.State == tumblrdb.OutboundSendNotSubmitted {
		if !send.StatusNotifiedAt.IsZero() {
			_, err = tc.connector.DB.Outbound.DeleteNotSubmitted(ctx, send.UserLoginID, send.TransactionID)
			if err == nil {
				tc.dropOutboundPending(send.TransactionID, true)
			}
			return send, false, err
		}
		if err = tc.finishOutboundNotSubmitted(ctx, send); err != nil {
			return nil, false, err
		}
		return send, false, nil
	}
	if send.State == tumblrdb.OutboundSendUnconfirmed {
		if !send.StatusNotifiedAt.IsZero() {
			var deleted bool
			deleted, err = tc.connector.DB.Outbound.DeleteUnconfirmed(ctx, send.UserLoginID, send.TransactionID)
			if err == nil && deleted {
				tc.dropOutboundPending(send.TransactionID, true)
			}
			return send, false, err
		}
		if err = tc.finishOutboundUnconfirmed(ctx, send); err != nil {
			return nil, false, err
		}
		return send, false, nil
	}
	if send.IsTerminal() {
		return nil, false, fmt.Errorf("unexpected terminal Tumblr outbound state %q", send.State)
	}
	if send.State == tumblrdb.OutboundSendPrepared {
		terminalAt := time.Now()
		nextAttemptAt := terminalAt.Add(time.Millisecond)
		var claimed bool
		claimed, err = tc.connector.DB.Outbound.ClaimPreparedNotSubmitted(
			ctx,
			send.UserLoginID,
			send.TransactionID,
			terminalAt,
			nextAttemptAt,
		)
		if err != nil || !claimed {
			return send, false, err
		}
		send.State = tumblrdb.OutboundSendNotSubmitted
		send.TerminalAt = terminalAt
		send.NextAttemptAt = nextAttemptAt
		if err = tc.finishOutboundNotSubmitted(ctx, send); err != nil {
			return nil, false, err
		}
		return send, false, nil
	}
	if send.State == tumblrdb.OutboundSendResolved {
		if err = tc.finalizeOutboundWithoutEcho(ctx, send, nil); err != nil {
			if err = tc.deferOutboundRecovery(ctx, send, err); err != nil {
				return nil, false, err
			}
		}
		return send, false, nil
	}
	if send.ConversationID == "" {
		conversationID, bindErr := tc.recoverOutboundConversationBinding(ctx, send)
		if bindErr != nil {
			if err = tc.deferOutboundRecovery(ctx, send, bindErr); err != nil {
				return nil, false, err
			}
			return send, false, nil
		}
		if conversationID == "" {
			if err = tc.markOutboundUnconfirmed(ctx, send); err != nil {
				return nil, false, err
			}
			return send, false, nil
		}
		if send.IsTerminal() {
			return send, false, nil
		}
		send.ConversationID = conversationID
		send.PortalKey = tc.portalKey(conversationID)
	}
	return send, true, nil
}

func (tc *TumblrClient) finishOutboundNotSubmitted(ctx context.Context, send *tumblrdb.OutboundSend) error {
	if err := tc.notifyOutboundStatusAtLeastOnce(ctx, send, outboundNotSubmittedMessage, true); err != nil {
		return tc.deferOutboundRecovery(ctx, send, err)
	}
	tc.dropOutboundPending(send.TransactionID, true)
	return nil
}

func (tc *TumblrClient) finishOutboundUnconfirmed(ctx context.Context, send *tumblrdb.OutboundSend) error {
	if err := tc.notifyOutboundStatusAtLeastOnce(ctx, send, outboundUnconfirmedMessage, false); err != nil {
		return tc.deferOutboundRecovery(ctx, send, err)
	}
	tc.dropOutboundPending(send.TransactionID, true)
	return nil
}

func (tc *TumblrClient) finishDueOutboundSend(
	ctx context.Context,
	txnID networkid.TransactionID,
	syncErr error,
) error {
	tc.outboundGraphLock.Lock()
	defer tc.outboundGraphLock.Unlock()

	reloaded, reloadErr := tc.connector.DB.Outbound.Get(ctx, tc.userLogin.ID, txnID)
	if reloadErr != nil {
		return errors.Join(syncErr, reloadErr)
	}
	stopForSyncError := tumblr.IsAuthError(syncErr) || errors.Is(syncErr, context.Canceled) || errors.Is(syncErr, context.DeadlineExceeded)
	if reloaded == nil {
		if stopForSyncError {
			return syncErr
		}
		return nil
	}
	if reloaded.IsTerminal() {
		tc.dropOutboundPending(reloaded.TransactionID, true)
		if stopForSyncError {
			return syncErr
		}
		return nil
	}
	if reloaded.State == tumblrdb.OutboundSendResolved {
		if err := tc.finalizeOutboundWithoutEcho(ctx, reloaded, nil); err != nil {
			if err = tc.deferOutboundRecovery(ctx, reloaded, err); err != nil {
				return errors.Join(syncErr, err)
			}
		}
		if stopForSyncError {
			return syncErr
		}
		return nil
	}
	if syncErr != nil {
		return tc.deferOutboundRecovery(ctx, reloaded, syncErr)
	}
	if err := tc.markOutboundUnconfirmed(ctx, reloaded); err != nil {
		return err
	}
	return nil
}

func (tc *TumblrClient) markOutboundUnconfirmed(ctx context.Context, send *tumblrdb.OutboundSend) error {
	if send == nil {
		return fmt.Errorf("tumblr outbound send is required")
	}
	now := time.Now()
	if err := tc.parkOutboundUnconfirmed(ctx, send, now); err != nil {
		return err
	}
	return tc.finishOutboundUnconfirmed(ctx, send)
}

// notifyOutboundStatusAtLeastOnce claims one durable delivery attempt for a
// terminal outcome. Matrix transaction IDs make retries safe if delivery
// succeeds but the process stops before the claim is finished.
func (tc *TumblrClient) notifyOutboundStatusAtLeastOnce(ctx context.Context, send *tumblrdb.OutboundSend, message string, isCertain bool) error {
	if send == nil {
		return fmt.Errorf("tumblr outbound send is required")
	}
	if !send.StatusNotifiedAt.IsZero() {
		return nil
	}
	claimToken, err := newOutboundStatusClaimToken()
	if err != nil {
		return err
	}
	claimExpiresAt := time.Now().Add(outboundStatusClaimTTL)
	claimed, err := tc.connector.DB.Outbound.ClaimStatusDelivery(
		ctx,
		send.UserLoginID,
		send.TransactionID,
		claimToken,
		claimExpiresAt,
	)
	if err != nil {
		return err
	}
	if !claimed {
		return nil
	}
	send.StatusClaimToken = claimToken
	send.StatusClaimExpiresAt = claimExpiresAt
	if err := tc.sendOutboundStatusWithCertainty(ctx, send, event.MessageStatusRetriable, message, isCertain); err != nil {
		releaseErr := tc.connector.DB.Outbound.ReleaseStatusDelivery(ctx, send.UserLoginID, send.TransactionID, claimToken)
		send.StatusClaimToken = ""
		send.StatusClaimExpiresAt = time.Time{}
		return errors.Join(err, releaseErr)
	}
	notifiedAt := time.Now()
	expiresAt := notifiedAt.Add(outboundTerminalRetention)
	err = tc.connector.DB.Outbound.FinishStatusDelivery(
		ctx,
		send.UserLoginID,
		send.TransactionID,
		claimToken,
		notifiedAt,
		expiresAt,
	)
	if err != nil {
		return err
	}
	send.NextAttemptAt = expiresAt
	send.StatusNotifiedAt = notifiedAt
	send.StatusClaimToken = ""
	send.StatusClaimExpiresAt = time.Time{}
	return nil
}

func (tc *TumblrClient) parkOutboundUnconfirmed(ctx context.Context, send *tumblrdb.OutboundSend, now time.Time) error {
	if send == nil {
		return fmt.Errorf("tumblr outbound send is required")
	}
	if now.IsZero() {
		now = time.Now()
	}
	nextAttemptAt := now.Add(time.Millisecond)
	var err error
	if send.State == tumblrdb.OutboundSendResolved {
		err = tc.connector.DB.Outbound.MarkResolvedUnconfirmed(
			ctx,
			send.UserLoginID,
			send.TransactionID,
			send.RemoteMessageID,
			now,
			nextAttemptAt,
		)
	} else {
		err = tc.connector.DB.Outbound.MarkUnconfirmed(
			ctx,
			send.UserLoginID,
			send.TransactionID,
			now,
			nextAttemptAt,
		)
	}
	if err != nil {
		return err
	}
	send.State = tumblrdb.OutboundSendUnconfirmed
	send.RemoteMessageID = ""
	send.TerminalAt = now
	send.NextAttemptAt = nextAttemptAt
	tc.dropOutboundPending(send.TransactionID, true)
	return nil
}

func (tc *TumblrClient) markOutboundUncertain(ctx context.Context, send *tumblrdb.OutboundSend, now time.Time) error {
	nextAttempt := now.Add(outboundRecoveryBackoff(send.AttemptCount + 1))
	if err := tc.connector.DB.Outbound.MarkUncertain(ctx, send.UserLoginID, send.TransactionID, nextAttempt); err != nil {
		return err
	}
	send.AttemptCount++
	send.State = tumblrdb.OutboundSendUncertain
	send.NextAttemptAt = nextAttempt
	return nil
}

func (tc *TumblrClient) deferOutboundRecovery(ctx context.Context, send *tumblrdb.OutboundSend, cause error) error {
	if send == nil {
		return cause
	}
	var scheduleErr error
	switch {
	case send.State == tumblrdb.OutboundSendResolved || send.IsTerminal():
		nextAttempt := time.Now().Add(outboundRecoveryBackoff(send.AttemptCount + 1))
		scheduleErr = tc.connector.DB.Outbound.ScheduleRetry(ctx, send.UserLoginID, send.TransactionID, nextAttempt)
		if scheduleErr == nil {
			send.AttemptCount++
			send.NextAttemptAt = nextAttempt
		}
	default:
		now := time.Now()
		if !send.SendStartedAt.IsZero() && !now.Before(send.SendStartedAt.Add(outboundRecoveryMaxAge)) {
			scheduleErr = tc.markOutboundUnconfirmed(ctx, send)
		} else {
			scheduleErr = tc.markOutboundUncertain(ctx, send, now)
		}
	}
	if scheduleErr != nil {
		return errors.Join(cause, scheduleErr)
	}
	if log := tc.log(); log != nil {
		log.Warn().Err(cause).
			Str("outbound_state", string(send.State)).
			Int("attempt", send.AttemptCount).
			Dur("retry_after", time.Until(send.NextAttemptAt)).
			Msg("Tumblr outbound recovery will retry")
	}
	if tumblr.IsAuthError(cause) || errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		return cause
	}
	return nil
}

func outboundRecoveryBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	backoff := outboundRecoveryRetry
	for i := 1; i < attempt && backoff < outboundRecoveryRetryMax; i++ {
		backoff *= 2
		if backoff > outboundRecoveryRetryMax {
			backoff = outboundRecoveryRetryMax
		}
	}
	return jitterDuration(backoff, backoff/5)
}

func (tc *TumblrClient) finalizeOutboundWithoutEcho(ctx context.Context, send *tumblrdb.OutboundSend, remote *tumblr.Message) error {
	dbMessage := outboundDatabaseMessage(send, remote)
	if err := tc.persistOutboundMapping(ctx, send, dbMessage); err != nil {
		if errors.Is(err, errOutboundMappingConflict) {
			if parkErr := tc.parkOutboundUnconfirmed(ctx, send, time.Now()); parkErr != nil {
				return errors.Join(err, parkErr)
			}
			if notifyErr := tc.finishOutboundUnconfirmed(ctx, send); notifyErr != nil {
				return errors.Join(err, notifyErr)
			}
			tc.wakeInboundSync()
			return nil
		}
		return err
	}
	tc.dropOutboundPending(send.TransactionID, true)
	if err := tc.sendOutboundStatus(ctx, send, event.MessageStatusSuccess, ""); err != nil {
		return err
	}
	return tc.retainCompletedOutbound(ctx, send)
}

func (tc *TumblrClient) sendOutboundStatus(ctx context.Context, send *tumblrdb.OutboundSend, messageStatus event.MessageStatus, message string) error {
	return tc.sendOutboundStatusWithCertainty(ctx, send, messageStatus, message, false)
}

func (tc *TumblrClient) sendOutboundStatusWithCertainty(
	ctx context.Context,
	send *tumblrdb.OutboundSend,
	messageStatus event.MessageStatus,
	message string,
	isCertain bool,
) error {
	if tc == nil || tc.connector == nil || tc.connector.Bridge == nil || tc.connector.Bridge.Matrix == nil || send == nil {
		return fmt.Errorf("matrix status sender is unavailable")
	}
	if send.MatrixRoomID == "" {
		return fmt.Errorf("matrix room for Tumblr outbound status is unavailable")
	}
	msgType := event.MsgText
	if send.MessageType == tumblrdb.OutboundMessageImage {
		msgType = event.MsgImage
	}
	matrixConnector, ok := tc.connector.Bridge.Matrix.(*matrix.Connector)
	if !ok || matrixConnector == nil || matrixConnector.Bot == nil || matrixConnector.Config == nil {
		return fmt.Errorf("synchronous matrix status sender is unavailable")
	}
	if send.MatrixEventID == "" {
		return fmt.Errorf("matrix event for Tumblr outbound status is unavailable")
	}
	messageStatusInfo := &bridgev2.MessageStatus{Status: messageStatus, Message: message}
	if messageStatus != event.MessageStatusSuccess {
		messageStatusInfo.ErrorReason = event.MessageStatusGenericError
		messageStatusInfo.IsCertain = isCertain
		messageStatusInfo.SendNotice = true
	}
	eventInfo := &bridgev2.MessageStatusEventInfo{
		RoomID:        send.MatrixRoomID,
		TransactionID: string(send.InputTransactionID),
		SourceEventID: send.MatrixEventID,
		EventType:     event.EventMessage,
		MessageType:   msgType,
		Sender:        send.MatrixSenderID,
	}

	if err := matrixConnector.SendMessageCheckpoints(ctx, []*status.MessageCheckpoint{messageStatusInfo.ToCheckpoint(eventInfo)}); err != nil {
		if log := tc.log(); log != nil {
			log.Warn().Err(err).
				Stringer("room_id", eventInfo.RoomID).
				Stringer("event_id", eventInfo.SourceEventID).
				Msg("Failed to send Tumblr outbound message checkpoint")
		}
	}
	if !messageStatusInfo.DisableMSS && matrixConnector.Config.Matrix.MessageStatusEvents {
		_, err := matrixConnector.Bot.SendMessageEvent(
			ctx,
			eventInfo.RoomID,
			event.BeeperMessageStatus,
			messageStatusInfo.ToMSSEvent(eventInfo),
			mautrix.ReqSendEvent{TransactionID: outboundStatusTransactionID(send, messageStatusInfo, "mss")},
		)
		if err != nil {
			return fmt.Errorf("send Tumblr outbound Matrix status event: %w", err)
		}
	}
	if messageStatusInfo.SendNotice && matrixConnector.Config.Matrix.MessageErrorNotices && eventInfo.MessageType != event.MsgNotice &&
		(messageStatusInfo.Status == event.MessageStatusFail || messageStatusInfo.Status == event.MessageStatusRetriable || messageStatusInfo.Step == status.MsgStepDecrypted) {
		_, err := matrixConnector.Bot.SendMessageEvent(
			ctx,
			eventInfo.RoomID,
			event.EventMessage,
			messageStatusInfo.ToNoticeEvent(eventInfo),
			mautrix.ReqSendEvent{TransactionID: outboundStatusTransactionID(send, messageStatusInfo, "notice")},
		)
		if err != nil {
			return fmt.Errorf("send Tumblr outbound Matrix status notice: %w", err)
		}
	}
	if messageStatusInfo.Status == event.MessageStatusSuccess && matrixConnector.Config.Matrix.DeliveryReceipts {
		if err := matrixConnector.Bot.SendReceipt(ctx, eventInfo.RoomID, eventInfo.SourceEventID, event.ReceiptTypeRead, nil); err != nil {
			return fmt.Errorf("send Tumblr outbound Matrix delivery receipt: %w", err)
		}
	}
	return nil
}

func (tc *TumblrClient) recoverOutboundConversationBinding(ctx context.Context, send *tumblrdb.OutboundSend) (string, error) {
	if send.BindingConversationID != "" {
		return tc.bindOutboundSendToConversation(ctx, send, send.BindingConversationID)
	}
	client, err := tc.tumblrClient()
	if err != nil {
		return "", err
	}
	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		return "", err
	}
	portal, err := tc.connector.Bridge.GetExistingPortalByKey(ctx, send.PortalKey)
	if err != nil {
		return "", err
	}
	if portal != nil {
		if pendingMeta, ok := portal.Metadata.(*PortalMetadata); ok && pendingMeta != nil && pendingMeta.PendingParticipantName != "" {
			resp, fetchErr := client.GetConversationByParticipants(ctx, meta.SelectedBlogName, pendingMeta.PendingParticipantName, 1)
			if fetchErr != nil && !tumblr.IsNotFound(fetchErr) {
				return "", tc.handleRemoteError(fetchErr)
			}
			if resp != nil && resp.Conversation != nil && validRemoteID(resp.Conversation.ID) &&
				equalParticipantIDs(pendingMeta.PendingParticipantIDs, conversationParticipantIDs(*resp.Conversation)) {
				return tc.bindOutboundSendToConversation(ctx, send, resp.Conversation.ID)
			}
		}
	}
	before := ""
	for page := 0; page < outboundUnboundSearchPages; page++ {
		resp, fetchErr := client.ListConversationsBefore(ctx, meta.SelectedBlogUUID, tumblr.MaxRequestLimit, before)
		if fetchErr != nil {
			return "", tc.handleRemoteError(fetchErr)
		}
		for _, conversation := range resp.Conversations {
			if validRemoteID(conversation.ID) && tc.outboundPortalMatchesConversation(send.PortalKey, conversation) {
				return tc.bindOutboundSendToConversation(ctx, send, conversation.ID)
			}
		}
		next, cursorErr := resp.NextBefore()
		if cursorErr != nil {
			return "", cursorErr
		}
		before = strings.TrimSpace(next)
		if before == "" {
			break
		}
	}
	return "", nil
}

func (tc *TumblrClient) bindOutboundSendToConversation(ctx context.Context, send *tumblrdb.OutboundSend, conversationID string) (string, error) {
	if err := tc.connector.DB.Outbound.PrepareConversationBinding(
		ctx,
		send.UserLoginID,
		send.TransactionID,
		conversationID,
		time.Now().Add(outboundResolvedDelay),
	); err != nil {
		return "", err
	}
	send.BindingConversationID = conversationID
	targetKey := tc.portalKey(conversationID)
	portal, sourceRoomReplaced, err := tc.bindOutboundPortalOwnership(ctx, send, targetKey, outboundResolvedDelay)
	if err != nil {
		return "", err
	}
	if sourceRoomReplaced {
		if err = tc.parkOutboundUnconfirmed(ctx, send, time.Now()); err != nil {
			return "", err
		}
		if err = tc.finishOutboundUnconfirmed(ctx, send); err != nil {
			return "", err
		}
		tc.wakeInboundSync()
		return conversationID, nil
	}
	if portal == nil || portal.Portal == nil || portal.MXID != send.MatrixRoomID {
		return "", fmt.Errorf("tumblr target portal is unavailable after binding an outbound send")
	}
	return conversationID, nil
}

// bindOutboundPortalOwnership serializes the framework re-ID across bridge
// processes, proves Matrix-room ownership from the database before and after
// the re-ID, and keeps that coordination boundary through the outbox bind.
func (tc *TumblrClient) bindOutboundPortalOwnership(
	ctx context.Context,
	send *tumblrdb.OutboundSend,
	targetKey networkid.PortalKey,
	retryDelay time.Duration,
) (*bridgev2.Portal, bool, error) {
	if send == nil || send.MatrixRoomID == "" {
		return nil, false, fmt.Errorf("outbound Matrix room lineage is missing")
	}
	if targetKey.IsEmpty() || string(targetKey.ID) == "" {
		return nil, false, fmt.Errorf("outbound target portal is missing")
	}
	if retryDelay <= 0 {
		return nil, false, fmt.Errorf("outbound binding retry delay is invalid")
	}
	release, err := tc.acquirePortalMutationLock(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("coordinate Tumblr portal re-ID: %w", err)
	}
	defer release()

	current, err := tc.connector.DB.Outbound.Get(ctx, send.UserLoginID, send.TransactionID)
	if err != nil {
		return nil, false, fmt.Errorf("reload outbound send before portal re-ID: %w", err)
	}
	if current == nil || current.IsTerminal() {
		return nil, false, tumblrdb.ErrOutboundSendNotPending
	}
	if current.MatrixRoomID != send.MatrixRoomID {
		return nil, false, tumblrdb.ErrOutboundSendIdentityChanged
	}
	conversationID := string(targetKey.ID)
	switch {
	case current.ConversationID == conversationID && current.BindingConversationID == "" && current.PortalKey == targetKey:
	case current.ConversationID == "" && current.BindingConversationID == conversationID:
	default:
		return nil, false, tumblrdb.ErrOutboundSendBindingConflict
	}
	sourceKey := current.PortalKey
	persistedOwner, err := tc.connector.Bridge.DB.Portal.GetByMXID(ctx, current.MatrixRoomID)
	if err != nil {
		return nil, false, fmt.Errorf("load persisted outbound Matrix room owner: %w", err)
	}

	if persistedOwner == nil || (persistedOwner.PortalKey != sourceKey && persistedOwner.PortalKey != targetKey) {
		target, targetErr := tc.loadOutboundTargetPortal(ctx, targetKey)
		if targetErr != nil {
			return nil, false, targetErr
		}
		if target != nil && target.MXID == current.MatrixRoomID {
			persistedOwner, err = tc.connector.Bridge.DB.Portal.GetByMXID(ctx, current.MatrixRoomID)
			if err != nil {
				return nil, false, fmt.Errorf("recheck persisted outbound Matrix room owner: %w", err)
			}
			if persistedOwner != nil && persistedOwner.PortalKey == targetKey {
				return tc.finishOutboundPortalBinding(ctx, send, current, target, targetKey, retryDelay)
			}
		}
		if persistedOwner != nil {
			return target, true, nil
		}
		if target != nil && target.MXID != "" {
			return target, true, nil
		}
		return nil, false, fmt.Errorf("outbound Matrix room no longer has a persisted portal owner")
	}

	if persistedOwner.PortalKey == sourceKey && sourceKey != targetKey {
		persistedSource, sourceErr := tc.connector.Bridge.DB.Portal.GetByKey(ctx, sourceKey)
		if sourceErr != nil {
			return nil, false, fmt.Errorf("load persisted Tumblr source portal before re-ID: %w", sourceErr)
		}
		sourcePortal, sourceErr := tc.loadPortalWithPersistedParity(ctx, sourceKey, persistedSource)
		if sourceErr != nil {
			return nil, false, sourceErr
		}
		if sourcePortal == nil || sourcePortal.MXID != current.MatrixRoomID {
			return nil, false, fmt.Errorf("tumblr source portal cache does not match persisted room ownership")
		}
		persistedTarget, targetErr := tc.connector.Bridge.DB.Portal.GetByKey(ctx, targetKey)
		if targetErr != nil {
			return nil, false, fmt.Errorf("load persisted Tumblr target portal before re-ID: %w", targetErr)
		}
		targetPortal, targetErr := tc.loadPortalWithPersistedParity(ctx, targetKey, persistedTarget)
		if targetErr != nil {
			return nil, false, targetErr
		}
		if targetPortal != nil && targetPortal == sourcePortal {
			return nil, false, fmt.Errorf("tumblr source and target portal caches unexpectedly share one object")
		}
		result, _, reIDErr := tc.connector.Bridge.ReIDPortal(ctx, sourceKey, targetKey)
		if reIDErr != nil {
			return nil, false, fmt.Errorf("re-ID pending Tumblr portal: %w", reIDErr)
		}
		persistedOwner, err = tc.connector.Bridge.DB.Portal.GetByMXID(ctx, current.MatrixRoomID)
		if err != nil {
			return nil, false, fmt.Errorf("verify persisted owner after Tumblr portal re-ID: %w", err)
		}
		if persistedOwner == nil || persistedOwner.PortalKey != targetKey {
			target, targetErr := tc.loadOutboundTargetPortal(ctx, targetKey)
			if targetErr != nil {
				return nil, false, targetErr
			}
			if target != nil && target.MXID != "" && target.MXID != current.MatrixRoomID {
				return target, true, nil
			}
			if persistedOwner != nil && persistedOwner.PortalKey != sourceKey {
				return target, true, nil
			}
			return nil, false, fmt.Errorf("tumblr portal re-ID %s did not transfer persisted room ownership", result)
		}
	}
	if persistedOwner.PortalKey != targetKey || persistedOwner.MXID != current.MatrixRoomID {
		return nil, false, fmt.Errorf("tumblr target portal does not own the outbound Matrix room")
	}
	portal, err := tc.loadOutboundTargetPortal(ctx, targetKey)
	if err != nil {
		return nil, false, err
	}
	if portal == nil || portal.Portal == nil || portal.MXID != current.MatrixRoomID {
		return nil, false, fmt.Errorf("tumblr target portal cache does not match persisted room ownership")
	}
	return tc.finishOutboundPortalBinding(ctx, send, current, portal, targetKey, retryDelay)
}

// acquireOutboundSubmissionLock is taken before syncLock/outboundGraphLock and
// the portal-mutation coordinator. It fences one login's Tumblr POST,
// reconciliation, and terminalization across bridge processes. The database
// lock is session/file-descriptor scoped, so a crashed owner releases it.
func (tc *TumblrClient) acquireOutboundSubmissionLock(ctx context.Context) (context.Context, func(), error) {
	if tc == nil || tc.connector == nil || tc.connector.DB == nil || tc.userLogin == nil {
		return ctx, nil, fmt.Errorf("tumblr outbound submission coordinator is unavailable")
	}
	localLockValue, _ := tc.connector.outboundSubmissionLocks.LoadOrStore(tc.userLogin.ID, &sync.Mutex{})
	localLock := localLockValue.(*sync.Mutex)
	localLock.Lock()
	lockCtx, releaseDatabaseLock, err := tc.connector.DB.AcquireOutboundSubmissionLock(ctx, tc.userLogin.ID)
	if err != nil {
		localLock.Unlock()
		return ctx, nil, err
	}
	return lockCtx, func() {
		releaseDatabaseLock()
		localLock.Unlock()
	}, nil
}

func (tc *TumblrClient) acquirePortalMutationLock(ctx context.Context) (func(), error) {
	if tc == nil || tc.connector == nil || tc.connector.DB == nil {
		return nil, fmt.Errorf("tumblr portal mutation coordinator is unavailable")
	}
	tc.connector.portalMutationLock.Lock()
	releaseDatabaseLock, err := tc.connector.DB.AcquirePortalReIDLock(ctx)
	if err != nil {
		tc.connector.portalMutationLock.Unlock()
		return nil, err
	}
	return func() {
		releaseDatabaseLock()
		tc.connector.portalMutationLock.Unlock()
	}, nil
}

func (tc *TumblrClient) withPortalMutationLock(ctx context.Context, mutate func() error) error {
	if mutate == nil {
		return fmt.Errorf("tumblr portal mutation is missing")
	}
	release, err := tc.acquirePortalMutationLock(ctx)
	if err != nil {
		return err
	}
	defer release()
	return mutate()
}

func (tc *TumblrClient) loadOutboundTargetPortal(ctx context.Context, targetKey networkid.PortalKey) (*bridgev2.Portal, error) {
	persistedTarget, err := tc.connector.Bridge.DB.Portal.GetByKey(ctx, targetKey)
	if err != nil {
		return nil, fmt.Errorf("load persisted Tumblr target portal: %w", err)
	}
	return tc.loadPortalWithPersistedParity(ctx, targetKey, persistedTarget)
}

func (tc *TumblrClient) loadPortalWithPersistedParity(
	ctx context.Context,
	portalKey networkid.PortalKey,
	persisted *database.Portal,
) (*bridgev2.Portal, error) {
	portal, err := tc.connector.Bridge.GetExistingPortalByKey(ctx, portalKey)
	if err != nil {
		return nil, fmt.Errorf("load cached Tumblr portal: %w", err)
	}
	if persisted == nil {
		if portal != nil {
			// Another bridge process deleted or re-IDed this portal while this
			// process still had the old object cached. Delete is idempotent against
			// the already-missing row and evicts only this process's stale object.
			if err = portal.Delete(ctx); err != nil {
				return nil, fmt.Errorf("evict stale Tumblr portal cache: %w", err)
			}
		}
		return nil, nil
	}
	if persisted.PortalKey != portalKey {
		return nil, fmt.Errorf("persisted Tumblr portal key does not match the requested key")
	}
	if portal == nil || portal.Portal == nil || portal.BridgeID != persisted.BridgeID ||
		portal.PortalKey != persisted.PortalKey {
		return nil, fmt.Errorf("tumblr portal cache does not match its persisted row")
	}
	if portal.MXID != persisted.MXID {
		if persisted.MXID == "" {
			return nil, fmt.Errorf("tumblr portal cache has a room that is absent from its persisted row")
		}
		if err = portal.UpdateMatrixRoomID(ctx, persisted.MXID, bridgev2.UpdateMatrixRoomIDParams{
			SyncDBMetadata: func() {
				portal.Portal = persisted
			},
			FailIfMXIDSet: false,
		}); err != nil {
			return nil, fmt.Errorf("refresh stale Tumblr portal room cache: %w", err)
		}
	} else {
		// Portal.Save updates the full row, so refresh all persisted fields before
		// any connector mutation can otherwise overwrite another process's work.
		portal.Portal = persisted
	}
	return portal, nil
}

func (tc *TumblrClient) finishOutboundPortalBinding(
	ctx context.Context,
	send, current *tumblrdb.OutboundSend,
	portal *bridgev2.Portal,
	targetKey networkid.PortalKey,
	retryDelay time.Duration,
) (*bridgev2.Portal, bool, error) {
	conversationID := string(targetKey.ID)
	if err := tc.saveOutboundConversationPortal(ctx, portal, conversationID); err != nil {
		return nil, false, err
	}
	persistedOwner, err := tc.connector.Bridge.DB.Portal.GetByMXID(ctx, current.MatrixRoomID)
	if err != nil {
		return nil, false, fmt.Errorf("verify persisted owner before binding Tumblr conversation: %w", err)
	}
	if persistedOwner == nil || persistedOwner.PortalKey != targetKey || persistedOwner.MXID != current.MatrixRoomID {
		return nil, false, tumblrdb.ErrOutboundSendRoomMismatch
	}
	nextAttemptAt := time.Now().Add(retryDelay)
	if err = tc.connector.DB.Outbound.BindConversation(
		ctx,
		current.UserLoginID,
		current.TransactionID,
		conversationID,
		targetKey,
		nextAttemptAt,
	); err != nil {
		return nil, false, err
	}
	send.ConversationID = conversationID
	send.BindingConversationID = ""
	send.PortalKey = targetKey
	send.NextAttemptAt = nextAttemptAt
	return portal, false, nil
}

func (tc *TumblrClient) saveOutboundConversationPortal(ctx context.Context, portal *bridgev2.Portal, conversationID string) error {
	if portal == nil || portal.Portal == nil {
		return fmt.Errorf("tumblr target portal is missing")
	}
	if applyConversationPortalMetadata(portal, conversationID, "") {
		if err := portal.Save(ctx); err != nil {
			return fmt.Errorf("save re-IDed Tumblr DM portal: %w", err)
		}
	}
	return nil
}

func conversationParticipantIDs(conversation tumblr.Conversation) []string {
	ids := make([]string, 0, len(conversation.Participants))
	for _, participant := range conversation.Participants {
		if id := strings.TrimSpace(participant.UUID); validRemoteID(id) {
			ids = append(ids, id)
		}
	}
	return ids
}

func equalParticipantIDs(left, right []string) bool {
	left = append([]string(nil), left...)
	right = append([]string(nil), right...)
	sort.Strings(left)
	sort.Strings(right)
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func (tc *TumblrClient) outboundPortalMatchesConversation(portalKey networkid.PortalKey, conversation tumblr.Conversation) bool {
	participantIDs := conversationParticipantIDs(conversation)
	if len(participantIDs) == 0 {
		return false
	}
	if tc.pendingDMPortalKey(participantIDs) == portalKey {
		return true
	}
	sort.Strings(participantIDs)
	if tc.pendingDMPortalKey(participantIDs) == portalKey {
		return true
	}
	if len(participantIDs) == 2 {
		participantIDs[0], participantIDs[1] = participantIDs[1], participantIDs[0]
		return tc.pendingDMPortalKey(participantIDs) == portalKey
	}
	return false
}

func (tc *TumblrClient) reconcileOutboundConversation(ctx context.Context, conversation *tumblr.Conversation) (outboundReconciliationResult, error) {
	result := outboundReconciliationResult{
		transactions:   make(map[string]networkid.TransactionID),
		heldMessageIDs: make(map[string]struct{}),
	}
	if conversation == nil || tc == nil || tc.connector == nil || tc.connector.DB == nil || tc.userLogin == nil {
		return result, nil
	}
	// The caller holds the per-login submission fence. This graph lock then
	// covers the complete candidate graph and all claims, so a forced edge cannot
	// be invalidated by an unseen row.
	tc.outboundGraphLock.Lock()
	defer tc.outboundGraphLock.Unlock()
	unbound, err := tc.connector.DB.Outbound.ListUnbound(ctx, tc.userLogin.ID)
	if err != nil {
		return outboundReconciliationResult{}, err
	}
	for _, send := range unbound {
		if tc.outboundPortalMatchesConversation(send.PortalKey, *conversation) {
			if _, err = tc.bindOutboundSendToConversation(ctx, send, conversation.ID); err != nil {
				return outboundReconciliationResult{}, err
			}
		}
	}

	matchable, err := tc.connector.DB.Outbound.ListMatchable(ctx, tc.userLogin.ID, conversation.ID)
	if err != nil {
		return outboundReconciliationResult{}, err
	}
	quarantine := make(map[string]struct{})
	directlyFinalized := make(map[string]struct{})

	for i := range conversation.Messages.Data {
		message := &conversation.Messages.Data[i]
		if !validOutboundRemoteMessageID(message.ID) {
			continue
		}
		resolved, getErr := tc.connector.DB.Outbound.GetByRemoteID(
			ctx,
			tc.userLogin.ID,
			conversation.ID,
			tumblrid.MakeMessageID(message.ID),
		)
		if getErr != nil {
			return outboundReconciliationResult{}, getErr
		}
		if resolved == nil {
			continue
		}
		if tc.activeOutboundPending(resolved) {
			result.transactions[message.ID] = resolved.TransactionID
		} else {
			if err = tc.finalizeOutboundWithoutEcho(ctx, resolved, message); err != nil {
				return outboundReconciliationResult{}, err
			}
			directlyFinalized[message.ID] = struct{}{}
		}
	}

	sets := make([]outboundCandidateSet, 0, len(matchable))
	blockedSends := make(map[networkid.TransactionID]struct{})
	blockedMessages := make(map[string]struct{})
	for _, send := range matchable {
		set := outboundCandidateSet{send: send}
		structuralImages := make([]*tumblr.Message, 0)
		for i := range conversation.Messages.Data {
			message := &conversation.Messages.Data[i]
			if _, done := directlyFinalized[message.ID]; done || result.transactions[message.ID] != "" {
				continue
			}
			candidate, matchErr := tc.outboundMessageCandidate(ctx, send, message, false)
			if matchErr != nil {
				return outboundReconciliationResult{}, matchErr
			}
			if send.MessageType == tumblrdb.OutboundMessageImage && candidate.indeterminate {
				structuralImages = append(structuralImages, message)
				continue
			}
			if candidate.match {
				set.messageIDs = append(set.messageIDs, message.ID)
			}
		}
		if send.MessageType == tumblrdb.OutboundMessageImage {
			// Verify every structural relation so the graph below can resolve distinct
			// images while still blocking any component that contains an unknown edge.
			for _, message := range structuralImages {
				candidate, matchErr := tc.outboundMessageCandidate(ctx, send, message, true)
				if matchErr != nil || candidate.indeterminate {
					quarantine[message.ID] = struct{}{}
					blockedSends[send.TransactionID] = struct{}{}
					blockedMessages[message.ID] = struct{}{}
					continue
				}
				if candidate.match {
					set.messageIDs = append(set.messageIDs, message.ID)
				}
			}
		}
		for _, messageID := range set.messageIDs {
			quarantine[messageID] = struct{}{}
		}
		sets = append(sets, set)
	}

	// An unverified image relation blocks its whole connected candidate component:
	// omitting an unknown edge could otherwise make a different edge look forced.
	for changed := true; changed; {
		changed = false
		for _, set := range sets {
			_, sendBlocked := blockedSends[set.send.TransactionID]
			for _, messageID := range set.messageIDs {
				_, messageBlocked := blockedMessages[messageID]
				switch {
				case sendBlocked && !messageBlocked:
					blockedMessages[messageID] = struct{}{}
					changed = true
				case messageBlocked && !sendBlocked:
					blockedSends[set.send.TransactionID] = struct{}{}
					sendBlocked = true
					changed = true
				}
			}
		}
	}
	eligibleSets := make([]outboundCandidateSet, 0, len(sets))
	for _, set := range sets {
		if _, blocked := blockedSends[set.send.TransactionID]; !blocked {
			eligibleSets = append(eligibleSets, set)
		}
	}
	forcedMatches := forcedOutboundCandidateMatches(eligibleSets)
	for _, set := range sets {
		messageID := forcedMatches[set.send.TransactionID]
		if messageID == "" {
			continue
		}
		claimed, claimErr := tc.connector.DB.Outbound.ClaimRemoteMessage(
			ctx,
			tc.userLogin.ID,
			set.send.TransactionID,
			conversation.ID,
			tumblrid.MakeMessageID(messageID),
			time.Now().Add(outboundResolvedDelay),
		)
		if claimErr != nil {
			return outboundReconciliationResult{}, claimErr
		}
		if !claimed {
			reloaded, reloadErr := tc.connector.DB.Outbound.Get(ctx, tc.userLogin.ID, set.send.TransactionID)
			if reloadErr != nil {
				return outboundReconciliationResult{}, reloadErr
			}
			if reloaded == nil || reloaded.State != tumblrdb.OutboundSendResolved || string(reloaded.RemoteMessageID) != messageID {
				return outboundReconciliationResult{}, fmt.Errorf("tumblr outbound candidate graph changed while claiming a forced match")
			}
			set.send = reloaded
		} else {
			set.send.State = tumblrdb.OutboundSendResolved
			set.send.RemoteMessageID = tumblrid.MakeMessageID(messageID)
		}
		message := findConversationMessage(conversation.Messages.Data, messageID)
		if tc.activeOutboundPending(set.send) {
			result.transactions[messageID] = set.send.TransactionID
			delete(quarantine, messageID)
		} else if message != nil {
			if err = tc.finalizeOutboundWithoutEcho(ctx, set.send, message); err != nil {
				return outboundReconciliationResult{}, err
			}
			directlyFinalized[messageID] = struct{}{}
		}
	}

	filtered := make([]tumblr.Message, 0, len(conversation.Messages.Data))
	for _, message := range conversation.Messages.Data {
		if _, done := directlyFinalized[message.ID]; done {
			continue
		}
		if _, held := quarantine[message.ID]; held {
			result.heldMessageIDs[message.ID] = struct{}{}
			continue
		}
		filtered = append(filtered, message)
	}
	conversation.Messages.Data = filtered
	return result, nil
}

func (tc *TumblrClient) outboundMessageCandidate(ctx context.Context, send *tumblrdb.OutboundSend, message *tumblr.Message, verifyImage bool) (outboundCandidateResult, error) {
	if send == nil || message == nil || !validOutboundRemoteMessageID(message.ID) ||
		strings.EqualFold(message.ID, string(send.RemoteMessageID)) {
		return outboundCandidateResult{}, nil
	}
	for _, baselineID := range send.BaselineMessageIDs {
		if string(baselineID) == message.ID {
			return outboundCandidateResult{}, nil
		}
	}
	if message.Participant == nil || !tc.outboundSenderMatches(send.SenderID, *message.Participant) {
		return outboundCandidateResult{}, nil
	}
	messageTS, ok := saneTumblrTimestamp(message.Timestamp, time.Now())
	if !ok || messageTS.Before(send.SendStartedAt.Add(-outboundMatchBefore)) || messageTS.After(send.SendStartedAt.Add(outboundMatchAfter)) {
		return outboundCandidateResult{}, nil
	}
	if !strings.EqualFold(message.Type, string(send.MessageType)) {
		return outboundCandidateResult{}, nil
	}
	claimed, err := tc.connector.DB.Outbound.GetByRemoteID(ctx, send.UserLoginID, send.ConversationID, tumblrid.MakeMessageID(message.ID))
	if err != nil {
		return outboundCandidateResult{}, err
	}
	if claimed != nil && claimed.TransactionID != send.TransactionID {
		return outboundCandidateResult{}, nil
	}
	mapped, err := tc.connector.Bridge.DB.Message.GetFirstPartByID(
		ctx,
		send.PortalKey.Receiver,
		tumblrid.MakeMessageID(message.ID),
	)
	if err != nil {
		return outboundCandidateResult{}, err
	}
	if mapped != nil {
		return outboundCandidateResult{}, nil
	}
	switch send.MessageType {
	case tumblrdb.OutboundMessageText:
		if message.Content == nil {
			return outboundCandidateResult{}, nil
		}
		return outboundCandidateResult{match: tumblrdb.HashOutboundContent([]byte(message.Content.Text)) == send.ContentHash}, nil
	case tumblrdb.OutboundMessagePostRef:
		if message.Post == nil || strings.TrimSpace(message.Post.ID) == "" {
			return outboundCandidateResult{}, nil
		}
		return outboundCandidateResult{match: tumblrdb.HashOutboundContent([]byte(strings.TrimSpace(message.Post.ID))) == send.ContentHash}, nil
	case tumblrdb.OutboundMessageImage:
		if len(message.Images) != 1 {
			return outboundCandidateResult{}, nil
		}
		if !verifyImage {
			return outboundCandidateResult{indeterminate: true}, nil
		}
		client, clientErr := tc.tumblrClient()
		if clientErr != nil {
			return outboundCandidateResult{}, clientErr
		}
		assets := message.Images[0].Candidates()
		if len(assets) == 0 {
			return outboundCandidateResult{indeterminate: true}, nil
		}
		var lastErr error
		sourceIdentityIndeterminate := false
		// Any matching rendition proves the relation, but a mismatch is definitive
		// only after every rendition was evaluated successfully.
		for _, asset := range assets {
			downloaded, downloadErr := client.DownloadImage(ctx, asset.URL, tumblr.DefaultMaxDownloadBytes)
			if downloadErr != nil {
				lastErr = downloadErr
				continue
			}
			if send.SourceMediaDigest != "" {
				switch downloaded.SourceDigest {
				case send.SourceMediaDigest:
					return outboundCandidateResult{match: true}, nil
				case "":
					sourceIdentityIndeterminate = true
				default:
					// A different valid Tumblr source identity is stronger
					// disconfirming evidence than equal rendered pixels.
					continue
				}
			}
			visualHash, hashErr := tumblr.HashVisualImageContent(downloaded.Data)
			if errors.Is(hashErr, tumblr.ErrVisualImageHashUnavailable) {
				if tumblrdb.HashOutboundContent(downloaded.Data) == send.ContentHash {
					return outboundCandidateResult{match: true}, nil
				}
				continue
			}
			if hashErr != nil {
				lastErr = hashErr
				continue
			}
			if tumblrdb.OutboundContentHash(visualHash) == send.ContentHash ||
				tumblrdb.HashOutboundContent(downloaded.Data) == send.ContentHash {
				return outboundCandidateResult{match: true}, nil
			}
		}
		if lastErr != nil {
			return outboundCandidateResult{}, lastErr
		}
		if sourceIdentityIndeterminate {
			return outboundCandidateResult{indeterminate: true}, nil
		}
		return outboundCandidateResult{}, nil
	default:
		return outboundCandidateResult{}, nil
	}
}

func (tc *TumblrClient) outboundSenderMatches(senderID networkid.UserID, participant tumblr.Blog) bool {
	participantID := tumblrBlogUserID(participant)
	if participantID == "" || senderID == "" {
		return false
	}
	if participantID == senderID {
		return true
	}
	meta, err := tc.validatedLoginMetadata()
	return err == nil && tumblrUserIDMatchesLogin(participantID, meta) && tumblrUserIDMatchesLogin(senderID, meta)
}

func validOutboundRemoteMessageID(messageID string) bool {
	// Tumblr omits native IDs from conversation messages, so the stable IDs
	// derived from the remote payload are their canonical bridge identity.
	return validRemoteID(messageID) && !strings.HasPrefix(messageID, "matrix-")
}

func findConversationMessage(messages []tumblr.Message, messageID string) *tumblr.Message {
	for i := range messages {
		if messages[i].ID == messageID {
			return &messages[i]
		}
	}
	return nil
}

func (tc *TumblrClient) outboundBaseline(ctx context.Context, client *tumblr.Client, selectedBlogName, conversationID string) ([]networkid.MessageID, error) {
	if conversationID == "" {
		return []networkid.MessageID{}, nil
	}
	resp, err := client.GetConversation(ctx, selectedBlogName, conversationID, tumblr.MaxRequestLimit)
	if err != nil {
		return nil, tc.handleRemoteError(err)
	}
	if err = validateConversationHistoryResponse(conversationID, resp); err != nil {
		return nil, err
	}
	ids := make([]networkid.MessageID, 0, len(resp.Messages))
	seen := make(map[string]struct{}, len(resp.Messages))
	for _, message := range resp.Messages {
		if !validRemoteID(message.ID) {
			continue
		}
		if _, duplicate := seen[message.ID]; duplicate {
			continue
		}
		seen[message.ID] = struct{}{}
		ids = append(ids, tumblrid.MakeMessageID(message.ID))
	}
	return ids, nil
}
