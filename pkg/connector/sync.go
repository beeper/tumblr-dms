package connector

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"

	"github.com/ifixrobots/tumblr-dms/pkg/msgconv"
	"github.com/ifixrobots/tumblr-dms/pkg/tumblr"
	"github.com/ifixrobots/tumblr-dms/pkg/tumblrid"
)

const (
	maxSeenMessages                = 10000
	maxConversationListPages       = 1000
	maxConversationMessagePages    = 1000
	maxRemoteIDRunes               = 512
	maxConversationTitleRunes      = 200
	maxPostSummaryRunes            = 160
	maxUnsupportedMessageTypeRunes = 80
	maxTumblrTimestampFutureSkew   = 24 * time.Hour
	tumblrPortalMutationTimeout    = 2 * time.Minute
	tumblrRemoteDeliveryTimeout    = 5 * time.Minute
	tumblrRemoteDeliveryPoll       = 100 * time.Millisecond
	unknownTumblrUserID            = networkid.UserID("unknown-tumblr-user")
)

func validRemoteID(id string) bool {
	return strings.TrimSpace(id) != "" &&
		utf8.RuneCountInString(id) <= maxRemoteIDRunes &&
		!containsMetadataSpaceOrControl(id)
}

// syncConversationByIDWithSubmissionLock requires the caller to hold this
// login's submission fence before taking syncLock.
func (tc *TumblrClient) syncConversationByIDWithSubmissionLock(
	ctx context.Context,
	conversationID string,
	expectedRevision int64,
) error {
	if !validRemoteID(conversationID) {
		return fmt.Errorf("tumblr conversation ID is invalid")
	}
	tc.syncLock.Lock()
	defer tc.syncLock.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	job, err := tc.connector.DB.Jobs.Get(ctx, tc.userLogin.ID, conversationID)
	if err != nil {
		return fmt.Errorf("failed to reload Tumblr conversation sync job: %w", err)
	}
	if expectedRevision > 0 && (job == nil || job.Revision != expectedRevision || job.NextAttemptAt.After(time.Now())) {
		return errTumblrConversationJobSuperseded
	}
	if job != nil && job.DeleteRoomID != "" {
		if job.NextAttemptAt.After(time.Now()) {
			return errTumblrConversationJobSuperseded
		}
		return tc.handleRemoteConversationDelete(ctx, conversationID, job.Revision)
	}

	if err := tc.requireLoggedInForContext(ctx); err != nil {
		return err
	}
	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		return err
	}
	limit := defaultConversationSyncLimit
	if tc.connector != nil {
		limit = tc.connector.Config.ConversationSyncBatchLimit()
	}
	client, err := tc.tumblrClient()
	if err != nil {
		return err
	}
	conversation, fetchedHeadMessageID, err := tc.fetchConversationForSync(
		ctx,
		client,
		meta.SelectedBlogName,
		tumblr.Conversation{ID: conversationID},
		limit,
	)
	if err != nil {
		if tumblr.IsNotFound(err) {
			if job == nil {
				if expectedRevision > 0 {
					return errTumblrConversationJobSuperseded
				}
				if ensureErr := tc.connector.DB.Jobs.Ensure(ctx, tc.userLogin.ID, conversationID); ensureErr != nil {
					return fmt.Errorf("failed to persist missing Tumblr conversation cleanup: %w", ensureErr)
				}
				job, err = tc.connector.DB.Jobs.Get(ctx, tc.userLogin.ID, conversationID)
				if err != nil {
					return fmt.Errorf("failed to reload missing Tumblr conversation cleanup: %w", err)
				}
				if job == nil {
					return fmt.Errorf("missing Tumblr conversation cleanup was not persisted")
				}
			}
			return tc.handleRemoteConversationDelete(ctx, conversationID, job.Revision)
		}
		if tumblr.IsAuthError(err) {
			return tc.handleRemoteError(err)
		}
		return err
	}
	if log := tc.log(); log != nil {
		log.Info().
			Str("conversation_id_hash", logIdentifierHash(conversationID)).
			Int("message_count", len(conversation.Messages.Data)).
			Msg("Fetched pushed Tumblr conversation")
	}
	return tc.queueFetchedConversation(ctx, conversation, fetchedHeadMessageID)
}

func (tc *TumblrClient) fetchConversationForSync(
	ctx context.Context,
	client *tumblr.Client,
	selectedBlogName string,
	listConversation tumblr.Conversation,
	limit int,
) (tumblr.Conversation, string, error) {
	conversationID := listConversation.ID
	if !validRemoteID(conversationID) {
		return tumblr.Conversation{}, "", fmt.Errorf("tumblr conversation ID is invalid")
	}
	if tc == nil || tc.connector == nil || tc.connector.Bridge == nil || tc.connector.DB == nil || tc.userLogin == nil {
		return tumblr.Conversation{}, "", fmt.Errorf("tumblr bridge is unavailable")
	}
	portalKey := tc.portalKey(conversationID)
	portal, err := tc.connector.Bridge.GetExistingPortalByKey(ctx, portalKey)
	if err != nil {
		return tumblr.Conversation{}, "", fmt.Errorf("failed to load Tumblr portal before conversation sync: %w", err)
	}
	catchUpExistingPortal := portal != nil && portal.Portal != nil && portal.MXID != ""
	previousCompletedHeadMessageID := ""
	if catchUpExistingPortal {
		state, stateErr := tc.connector.DB.ConversationSync.Get(ctx, tc.userLogin.ID, conversationID)
		if stateErr != nil {
			return tumblr.Conversation{}, "", fmt.Errorf("failed to load Tumblr conversation sync boundary: %w", stateErr)
		}
		if state != nil && validRemoteID(state.CompletedHeadMessageID) {
			previousCompletedHeadMessageID = state.CompletedHeadMessageID
		}
		// A portal can be created before its completed boundary is saved if the
		// process stops mid-delivery. Scan to the end once to recover that work.
	}
	before := ""
	seenCursors := make(map[string]struct{})
	messagePages := make([][]tumblr.Message, 0, 1)
	var conversation tumblr.Conversation
	fetchedHeadMessageID := ""
	for page := 0; page < maxConversationMessagePages; page++ {
		resp, err := client.GetConversationBefore(ctx, selectedBlogName, conversationID, limit, before)
		if err != nil {
			if page > 0 && tumblr.IsNotFound(err) {
				// Page one already proved that the conversation exists. A later
				// cursor disappearing is retryable and must not delete a live room.
				return tumblr.Conversation{}, "", fmt.Errorf("tumblr conversation history page disappeared during sync")
			}
			return tumblr.Conversation{}, "", err
		}
		if err = validateConversationHistoryResponse(conversationID, resp); err != nil {
			return tumblr.Conversation{}, "", err
		}
		if page == 0 {
			conversation = mergeConversationHistoryForSync(listConversation, resp)
		}
		pageMessages := conversationMessagesFromResponse(resp)
		if page == 0 {
			fetchedHeadMessageID = conversationPageHeadMessageID(pageMessages)
		}
		messagePages = append(messagePages, pageMessages)
		if !catchUpExistingPortal {
			conversation.Messages.Data = aggregateConversationMessagePages(messagePages)
			return conversation, fetchedHeadMessageID, nil
		}
		if conversationPageContainsMessageID(pageMessages, previousCompletedHeadMessageID) {
			conversation.Messages.Data = aggregateConversationMessagePages(messagePages)
			return conversation, fetchedHeadMessageID, nil
		}
		nextBefore, cursorErr := resp.NextBefore()
		if cursorErr != nil {
			return tumblr.Conversation{}, "", cursorErr
		}
		nextBefore = strings.TrimSpace(nextBefore)
		if nextBefore == "" {
			conversation.Messages.Data = aggregateConversationMessagePages(messagePages)
			return conversation, fetchedHeadMessageID, nil
		}
		if _, duplicate := seenCursors[nextBefore]; duplicate {
			return tumblr.Conversation{}, "", fmt.Errorf("tumblr conversation history cursor repeated")
		}
		seenCursors[nextBefore] = struct{}{}
		before = nextBefore
	}
	return tumblr.Conversation{}, "", fmt.Errorf("tumblr conversation history exceeded %d pages", maxConversationMessagePages)
}

func conversationMessagesFromResponse(resp *tumblr.ConversationMessagesResponse) []tumblr.Message {
	if resp == nil {
		return nil
	}
	if len(resp.Messages) > 0 {
		return append([]tumblr.Message(nil), resp.Messages...)
	}
	if resp.Conversation != nil {
		return append([]tumblr.Message(nil), resp.Conversation.Messages.Data...)
	}
	return nil
}

func aggregateConversationMessagePages(pages [][]tumblr.Message) []tumblr.Message {
	total := 0
	for _, page := range pages {
		total += len(page)
	}
	messages := make([]tumblr.Message, 0, total)
	seenIDs := make(map[string]struct{}, total)
	for pageIndex := len(pages) - 1; pageIndex >= 0; pageIndex-- {
		for _, message := range pages[pageIndex] {
			if validRemoteID(message.ID) {
				if _, duplicate := seenIDs[message.ID]; duplicate {
					continue
				}
				seenIDs[message.ID] = struct{}{}
			}
			messages = append(messages, message)
		}
	}
	return messages
}

func conversationPageHeadMessageID(messages []tumblr.Message) string {
	sorted := sortedMessages(messages)
	for index := len(sorted) - 1; index >= 0; index-- {
		if validRemoteID(sorted[index].ID) {
			return sorted[index].ID
		}
	}
	return ""
}

func conversationPageContainsMessageID(messages []tumblr.Message, messageID string) bool {
	if messageID == "" {
		return false
	}
	for _, message := range messages {
		if message.ID == messageID {
			return true
		}
	}
	return false
}

type outboundReconciliationDeferredError struct {
	messageCount int
}

func (err *outboundReconciliationDeferredError) Error() string {
	return fmt.Sprintf("%d Tumblr message(s) held for safe outbound reconciliation", err.messageCount)
}

func (tc *TumblrClient) queueFetchedConversation(
	ctx context.Context,
	conversation tumblr.Conversation,
	fetchedHeadMessageID string,
) error {
	heldMessageIDs, err := tc.queueConversation(ctx, conversation, true)
	if err != nil {
		return err
	}
	if err = tc.confirmFetchedConversationMessagesStored(ctx, conversation, heldMessageIDs); err != nil {
		return err
	}
	if len(heldMessageIDs) > 0 {
		// Held messages were deliberately not handed to bridgev2. Keep the
		// durable head unchanged so this fetch is retried without losing them.
		return &outboundReconciliationDeferredError{messageCount: len(heldMessageIDs)}
	}
	if fetchedHeadMessageID == "" {
		return nil
	}
	if !validRemoteID(fetchedHeadMessageID) {
		return fmt.Errorf("tumblr conversation sync boundary is invalid")
	}
	if tc == nil || tc.connector == nil || tc.connector.DB == nil || tc.userLogin == nil {
		return fmt.Errorf("tumblr durable sync database is unavailable")
	}
	// Keep this as the final durable write: retries may repeat work, but they
	// can never skip over a fetched message that was not confirmed in Matrix.
	if err := tc.connector.DB.ConversationSync.SetCompletedHead(
		ctx,
		tc.userLogin.ID,
		conversation.ID,
		fetchedHeadMessageID,
		time.Now(),
	); err != nil {
		return fmt.Errorf("failed to save Tumblr conversation sync boundary: %w", err)
	}
	return nil
}

func (tc *TumblrClient) confirmFetchedConversationMessagesStored(
	ctx context.Context,
	conversation tumblr.Conversation,
	heldMessageIDs map[string]struct{},
) error {
	portalKey := tc.portalKey(conversation.ID)
	seenIDs := make(map[string]struct{}, len(conversation.Messages.Data))
	for _, message := range conversation.Messages.Data {
		if !validRemoteID(message.ID) {
			return fmt.Errorf("fetched Tumblr conversation contains an invalid message ID")
		}
		if _, duplicate := seenIDs[message.ID]; duplicate {
			continue
		}
		seenIDs[message.ID] = struct{}{}
		if _, held := heldMessageIDs[message.ID]; held {
			// queueFetchedConversation returns a typed retry error for these IDs
			// before it can advance the durable conversation head.
			continue
		}
		if err := tc.confirmTumblrMessageStored(ctx, portalKey, message.ID); err != nil {
			return fmt.Errorf("failed to confirm fetched Tumblr conversation was stored: %w", err)
		}
	}
	return nil
}

func mergeConversationHistoryForSync(listConversation tumblr.Conversation, history *tumblr.ConversationMessagesResponse) tumblr.Conversation {
	if history == nil || history.Conversation == nil {
		return listConversation
	}
	merged := *history.Conversation
	if merged.ID == "" {
		merged.ID = listConversation.ID
	}
	if len(merged.Participants) == 0 {
		merged.Participants = listConversation.Participants
	}
	if len(merged.Messages.Data) == 0 {
		merged.Messages = listConversation.Messages
	}
	if len(history.Messages) > 0 {
		merged.Messages.Data = history.Messages
	}
	if merged.UnreadMessagesCount == 0 {
		merged.UnreadMessagesCount = listConversation.UnreadMessagesCount
	}
	if merged.LastReadTimestamp <= 0 {
		merged.LastReadTimestamp = listConversation.LastReadTimestamp
	}
	if merged.LastModifiedTimestamp <= 0 {
		merged.LastModifiedTimestamp = listConversation.LastModifiedTimestamp
	}
	if merged.LastUpdated == nil {
		merged.LastUpdated = listConversation.LastUpdated
	}
	return merged
}

func (tc *TumblrClient) queueConversation(
	ctx context.Context,
	conversation tumblr.Conversation,
	forceChatResync bool,
) (map[string]struct{}, error) {
	if !validRemoteID(conversation.ID) {
		return nil, fmt.Errorf("tumblr conversation ID is invalid")
	}
	reconciliation, err := tc.reconcileOutboundConversation(ctx, &conversation)
	if err != nil {
		return nil, fmt.Errorf("failed to reconcile Tumblr outbound messages: %w", err)
	}
	firstSeen, newMessages := tc.markConversationSeen(conversation)
	portalKey := tc.portalKey(conversation.ID)
	fallbackTimestamp := time.Now()
	messages, err := tc.messagesForConversationSync(ctx, portalKey, conversation, firstSeen, forceChatResync, newMessages)
	if err != nil {
		return nil, err
	}
	readReceipt := tc.readReceiptEventFromConversation(conversation, portalKey, fallbackTimestamp)

	if err = tc.ensureConversationPortalForSync(
		ctx,
		portalKey,
		conversation,
		forceChatResync || firstSeen,
	); err != nil {
		return nil, err
	}
	if err = tc.queueConversationMessageChain(
		ctx,
		portalKey,
		messages,
		readReceipt,
		fallbackTimestamp,
		reconciliation.transactions,
	); err != nil {
		return nil, err
	}
	return reconciliation.heldMessageIDs, nil
}

func (tc *TumblrClient) ensureConversationPortalForSync(
	ctx context.Context,
	portalKey networkid.PortalKey,
	conversation tumblr.Conversation,
	forceResync bool,
) error {
	return tc.withPortalMutationLock(ctx, func() error {
		// Portal identity is committed synchronously while coordinated. Framework
		// queue callbacks are never awaited under a Tumblr-owned lock. Ignoring the
		// caller's cancellation keeps the identity update atomic, while the deadline
		// guarantees a stalled dependency cannot strand the coordinator forever.
		mutationCtx, cancelMutation := context.WithTimeout(
			context.WithoutCancel(ctx),
			tumblrPortalMutationTimeout,
		)
		defer cancelMutation()
		persisted, err := tc.connector.Bridge.DB.Portal.GetByKey(mutationCtx, portalKey)
		if err != nil {
			return fmt.Errorf("failed to load persisted Tumblr portal before chat sync: %w", err)
		}
		portal, err := tc.loadPortalWithPersistedParity(mutationCtx, portalKey, persisted)
		if err != nil {
			return fmt.Errorf("failed to verify Tumblr portal cache before chat sync: %w", err)
		}
		if portal == nil {
			portal, err = tc.connector.Bridge.GetPortalByKey(mutationCtx, portalKey)
			if err != nil {
				return fmt.Errorf("failed to create Tumblr portal before chat sync: %w", err)
			}
		}
		if portal == nil || portal.Portal == nil || portal.PortalKey != portalKey {
			return fmt.Errorf("tumblr portal is unavailable before chat sync")
		}
		// A direct UserPortal deletion cannot invalidate bridgev2's private
		// per-login cache. Recreate durable ownership explicitly for every live
		// Tumblr fetch so a conversation can always return in the same process.
		if _, err = tc.connector.Bridge.DB.UserPortal.GetOrCreate(
			mutationCtx,
			tc.userLogin.UserLogin,
			portalKey,
		); err != nil {
			return fmt.Errorf("failed to restore Tumblr login portal before chat sync: %w", err)
		}
		metadataChanged, err := tc.saveConversationMetadataIfChangedLocked(
			mutationCtx,
			portal,
			conversation,
			tc.conversationParticipantHash(conversation),
		)
		if err != nil {
			return err
		}
		if portal.MXID == "" {
			if err = portal.CreateMatrixRoom(mutationCtx, tc.userLogin, tc.chatInfoFromConversation(conversation)); err != nil {
				return fmt.Errorf("failed to create Tumblr conversation room: %w", err)
			}
			return tc.confirmTumblrPortalExists(mutationCtx, portalKey)
		}
		if !forceResync && !metadataChanged {
			return nil
		}
		portal.UpdateInfo(mutationCtx, tc.chatInfoFromConversation(conversation), tc.userLogin, nil, time.Time{})
		if err = mutationCtx.Err(); err != nil {
			return fmt.Errorf("timed out updating Tumblr conversation room: %w", err)
		}
		if err = portal.Save(mutationCtx); err != nil {
			return fmt.Errorf("failed to save updated Tumblr conversation room: %w", err)
		}
		return tc.confirmTumblrPortalExists(mutationCtx, portalKey)
	})
}

func (tc *TumblrClient) queueConversationMessageChain(
	ctx context.Context,
	portalKey networkid.PortalKey,
	messages []tumblr.Message,
	readReceipt *simplevent.Receipt,
	fallbackTimestamp time.Time,
	outboundTransactions map[string]networkid.TransactionID,
) error {
	deliveryCtx, cancelDelivery := context.WithTimeout(ctx, tumblrRemoteDeliveryTimeout)
	defer cancelDelivery()
	for _, message := range messages {
		if err := deliveryCtx.Err(); err != nil {
			return err
		}
		evt := tc.messageEventFromMessageWithTransaction(
			portalKey,
			message,
			false,
			fallbackTimestamp,
			outboundTransactions[message.ID],
		)
		if evt == nil {
			return fmt.Errorf("tumblr message has an invalid ID")
		}
		result := tc.queueRemoteEvent(evt)
		if err := remoteEventQueueError(result, "tumblr message"); err != nil && !result.Ignored {
			return err
		}
		// The durable message row is the completion signal. Polling it avoids
		// depending on PostHandle, which bridgev2 does not call after every panic.
		if err := tc.waitForTumblrMessageStored(deliveryCtx, portalKey, message.ID); err != nil {
			return err
		}
	}
	if readReceipt == nil {
		return nil
	}
	if err := deliveryCtx.Err(); err != nil {
		return err
	}
	result := tc.queueRemoteEvent(readReceipt)
	if err := remoteEventQueueError(result, "tumblr read receipt"); err != nil && !result.Ignored {
		return err
	}
	return nil
}

func (tc *TumblrClient) confirmTumblrPortalExists(ctx context.Context, portalKey networkid.PortalKey) error {
	if tc == nil || tc.connector == nil || tc.connector.Bridge == nil || tc.connector.Bridge.DB == nil {
		return fmt.Errorf("tumblr bridge is unavailable")
	}
	persisted, err := tc.connector.Bridge.DB.Portal.GetByKey(ctx, portalKey)
	if err != nil {
		return fmt.Errorf("failed to load persisted Tumblr portal: %w", err)
	}
	portal, err := tc.loadPortalWithPersistedParity(ctx, portalKey, persisted)
	if err != nil {
		return fmt.Errorf("failed to verify Tumblr portal cache: %w", err)
	}
	if portal == nil || portal.Portal == nil || portal.MXID == "" {
		return fmt.Errorf("tumblr portal does not exist")
	}
	return nil
}

func (tc *TumblrClient) confirmTumblrMessageStored(ctx context.Context, portalKey networkid.PortalKey, messageID string) error {
	stored, err := tc.tumblrMessageStored(ctx, portalKey, messageID)
	if err != nil {
		return err
	}
	if !stored {
		return fmt.Errorf("tumblr message was not stored after handling")
	}
	return nil
}

func (tc *TumblrClient) waitForTumblrMessageStored(
	ctx context.Context,
	portalKey networkid.PortalKey,
	messageID string,
) error {
	ticker := time.NewTicker(tumblrRemoteDeliveryPoll)
	defer ticker.Stop()
	for {
		stored, err := tc.tumblrMessageStored(ctx, portalKey, messageID)
		if err != nil {
			return err
		}
		if stored {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for Tumblr message delivery: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (tc *TumblrClient) tumblrMessageStored(
	ctx context.Context,
	portalKey networkid.PortalKey,
	messageID string,
) (bool, error) {
	if tc == nil || tc.connector == nil || tc.connector.Bridge == nil || tc.connector.Bridge.DB == nil {
		return false, fmt.Errorf("tumblr message database is unavailable")
	}
	existing, err := tc.connector.Bridge.DB.Message.GetFirstPartByID(
		ctx,
		portalKey.Receiver,
		tumblrid.MakeMessageID(messageID),
	)
	if err != nil {
		return false, fmt.Errorf("failed to confirm Tumblr message delivery: %w", err)
	}
	if existing == nil {
		return false, nil
	}
	if existing.Room != portalKey {
		return false, fmt.Errorf("tumblr message was stored in a different conversation")
	}
	return true, nil
}

func remoteEventQueueError(result bridgev2.EventHandlingResult, description string) error {
	if result.Success && !result.Ignored {
		return nil
	}
	if result.Error != nil {
		return fmt.Errorf("%s was not accepted: %w", description, result.Error)
	}
	return fmt.Errorf("%s was not accepted", description)
}

func (tc *TumblrClient) readReceiptEventFromConversation(conversation tumblr.Conversation, portalKey networkid.PortalKey, fallbackTimestamp time.Time) *simplevent.Receipt {
	if conversation.LastReadTimestamp <= 0 {
		return nil
	}
	readAt, ok := saneTumblrTimestamp(conversation.LastReadTimestamp, fallbackTimestamp)
	if !ok {
		return nil
	}
	return &simplevent.Receipt{
		EventMeta: simplevent.EventMeta{
			Type:        bridgev2.RemoteEventReadReceipt,
			PortalKey:   portalKey,
			Sender:      tc.loginEventSender(),
			Timestamp:   readAt,
			StreamOrder: readAt.UnixMilli(),
			LogContext: func(c zerolog.Context) zerolog.Context {
				return c.Str("conversation_id_hash", logIdentifierHash(conversation.ID))
			},
		},
		ReadUpTo:            readAt,
		ReadUpToStreamOrder: readAt.UnixMilli(),
	}
}

func (tc *TumblrClient) messagesForConversationSync(ctx context.Context, portalKey networkid.PortalKey, conversation tumblr.Conversation, firstSeen, forceChatResync bool, newMessages []tumblr.Message) ([]tumblr.Message, error) {
	if firstSeen || forceChatResync {
		return tc.missingConversationMessages(ctx, portalKey, conversation)
	}
	return newMessages, nil
}

func (tc *TumblrClient) missingConversationMessages(ctx context.Context, portalKey networkid.PortalKey, conversation tumblr.Conversation) ([]tumblr.Message, error) {
	if tc == nil || tc.connector == nil || tc.connector.Bridge == nil || tc.connector.Bridge.DB == nil {
		return nil, fmt.Errorf("tumblr message database is unavailable")
	}
	missing := make([]tumblr.Message, 0)
	for _, message := range sortedMessages(conversation.Messages.Data) {
		if !validRemoteID(message.ID) {
			continue
		}
		existing, err := tc.connector.Bridge.DB.Message.GetFirstPartByID(ctx, portalKey.Receiver, tumblrid.MakeMessageID(message.ID))
		if err != nil {
			return nil, fmt.Errorf("failed to check if Tumblr message already exists: %w", err)
		}
		if existing == nil {
			missing = append(missing, message)
		}
	}
	if len(missing) > 0 {
		if log := tc.log(); log != nil {
			log.Info().Int("message_count", len(missing)).Str("conversation_id_hash", logIdentifierHash(conversation.ID)).Msg("Queueing missing Tumblr messages from hydrated conversation")
		}
	}
	return missing, nil
}

func (tc *TumblrClient) markConversationSeen(conversation tumblr.Conversation) (firstSeen bool, newMessages []tumblr.Message) {
	if !validRemoteID(conversation.ID) {
		return false, nil
	}
	tc.seenLock.Lock()
	defer tc.seenLock.Unlock()

	if _, ok := tc.seenConversations[conversation.ID]; !ok {
		firstSeen = true
		tc.seenConversations[conversation.ID] = struct{}{}
	}
	if conversation.LastModifiedTimestamp > tc.seenConversationModifiedTS[conversation.ID] {
		tc.seenConversationModifiedTS[conversation.ID] = conversation.LastModifiedTimestamp
	}
	for _, message := range sortedMessages(conversation.Messages.Data) {
		if !validRemoteID(message.ID) {
			continue
		}
		cacheKey := seenMessageCacheKey(conversation.ID, message.ID)
		if tc.isMessageSeenLocked(cacheKey, message.ID) {
			continue
		}
		tc.storeSeenMessageLocked(cacheKey)
		newMessages = append(newMessages, message)
	}
	return
}

func (tc *TumblrClient) markConversationMessageSeen(conversationID, messageID string) {
	if !validRemoteID(messageID) || (conversationID != "" && !validRemoteID(conversationID)) {
		return
	}
	tc.seenLock.Lock()
	tc.storeSeenMessageLocked(seenMessageCacheKey(conversationID, messageID))
	tc.seenLock.Unlock()
}

func (tc *TumblrClient) markConversationMessagesSeen(conversationID string, messages []tumblr.Message) {
	if !validRemoteID(conversationID) {
		return
	}
	tc.seenLock.Lock()
	defer tc.seenLock.Unlock()
	for _, message := range messages {
		if !validRemoteID(message.ID) {
			continue
		}
		tc.storeSeenMessageLocked(seenMessageCacheKey(conversationID, message.ID))
	}
}

func (tc *TumblrClient) isMessageSeenLocked(cacheKey, messageID string) bool {
	if _, ok := tc.seenMessages[cacheKey]; ok {
		return true
	}
	if cacheKey == messageID {
		return false
	}
	_, ok := tc.seenMessages[messageID]
	return ok
}

func (tc *TumblrClient) storeSeenMessageLocked(cacheKey string) {
	if _, ok := tc.seenMessages[cacheKey]; ok {
		return
	}
	tc.seenMessages[cacheKey] = struct{}{}
	tc.seenMessageOrder = append(tc.seenMessageOrder, cacheKey)
	for len(tc.seenMessageOrder) > maxSeenMessages {
		delete(tc.seenMessages, tc.seenMessageOrder[0])
		tc.seenMessageOrder = tc.seenMessageOrder[1:]
	}
}

func seenMessageCacheKey(conversationID, messageID string) string {
	if conversationID == "" {
		return messageID
	}
	return conversationID + "\x00" + messageID
}

func sortedMessages(messages []tumblr.Message) []tumblr.Message {
	return sortedMessagesWithReference(messages, time.Now())
}

func sortedMessagesWithReference(messages []tumblr.Message, reference time.Time) []tumblr.Message {
	sorted := append([]tumblr.Message(nil), messages...)
	sort.SliceStable(sorted, func(i, j int) bool {
		left := messageSortTimestampWithReference(sorted[i], reference)
		right := messageSortTimestampWithReference(sorted[j], reference)
		return left.Before(right)
	})
	return sorted
}

func (tc *TumblrClient) messageEventFromMessageWithTransaction(
	portalKey networkid.PortalKey,
	message tumblr.Message,
	createPortal bool,
	fallbackTimestamp time.Time,
	transactionID networkid.TransactionID,
) *simplevent.Message[tumblr.Message] {
	if !validRemoteID(message.ID) {
		return nil
	}
	sender := tc.senderFromMessage(message)
	ts := messageTimestampWithFallback(message, fallbackTimestamp)
	return &simplevent.Message[tumblr.Message]{
		EventMeta: simplevent.EventMeta{
			Type:         bridgev2.RemoteEventMessage,
			PortalKey:    portalKey,
			CreatePortal: createPortal,
			Sender:       sender,
			Timestamp:    ts,
			StreamOrder:  ts.UnixMilli(),
			LogContext: func(c zerolog.Context) zerolog.Context {
				return c.
					Str("message_id_hash", logIdentifierHash(message.ID)).
					Str("message_type", logMessageType(message.Type))
			},
		},
		ID:            tumblrid.MakeMessageID(message.ID),
		TransactionID: transactionID,
		Data:          message,
		ConvertMessageFunc: func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data tumblr.Message) (*bridgev2.ConvertedMessage, error) {
			return tc.convertTumblrMessageWithMedia(ctx, portal, intent, data)
		},
	}
}

func (tc *TumblrClient) convertTumblrMessage(message tumblr.Message) *bridgev2.ConvertedMessage {
	return msgconv.ConvertTumblrMessage(message)
}

func (tc *TumblrClient) chatInfoFromConversation(conversation tumblr.Conversation) *bridgev2.ChatInfo {
	roomType := database.RoomTypeDM
	if tc.validParticipantCount(conversation) > 2 {
		roomType = database.RoomTypeGroupDM
	}
	name := tc.conversationTitle(conversation)
	info := &bridgev2.ChatInfo{
		Type:        &roomType,
		Members:     tc.chatMembersFromConversation(conversation),
		CanBackfill: true,
		ExtraUpdates: conversationPortalMetadataUpdater(
			conversation.ID,
			tc.conversationParticipantHash(conversation),
		),
	}
	if name != "" {
		info.Name = &name
	}
	if roomType == database.RoomTypeDM {
		if other := tc.otherParticipant(conversation); other != nil {
			info.Avatar = tc.blogAvatar(*other)
		}
	}
	return info
}

func conversationPortalMetadataUpdater(conversationID, participantHash string) bridgev2.ExtraUpdater[*bridgev2.Portal] {
	if !validRemoteID(conversationID) && participantHash == "" {
		return nil
	}
	return func(_ context.Context, portal *bridgev2.Portal) bool {
		return applyConversationPortalMetadata(portal, conversationID, participantHash)
	}
}

func applyConversationPortalMetadata(portal *bridgev2.Portal, conversationID, participantHash string) bool {
	if portal == nil || portal.Portal == nil {
		return false
	}
	meta, ok := portal.Metadata.(*PortalMetadata)
	if !ok || meta == nil {
		meta = &PortalMetadata{}
		portal.Metadata = meta
	}
	changed := false
	if validRemoteID(conversationID) && meta.ConversationID != conversationID {
		meta.ConversationID = conversationID
		meta.PendingParticipantIDs = nil
		meta.PendingParticipantName = ""
		changed = true
	}
	if participantHash != "" && meta.ParticipantHash != participantHash {
		meta.ParticipantHash = participantHash
		changed = true
	}
	return changed
}

func (tc *TumblrClient) saveConversationMetadataIfChangedLocked(
	ctx context.Context,
	portal *bridgev2.Portal,
	conversation tumblr.Conversation,
	participantHash string,
) (bool, error) {
	if participantHash == "" || portal == nil || portal.Portal == nil {
		return false, nil
	}
	originalMetadata := portal.Metadata
	existingMetadata, hadExistingMetadata := originalMetadata.(*PortalMetadata)
	var originalMetadataValue PortalMetadata
	if hadExistingMetadata && existingMetadata != nil {
		originalMetadataValue = *existingMetadata
		originalMetadataValue.PendingParticipantIDs = append([]string(nil), existingMetadata.PendingParticipantIDs...)
	}
	if !applyConversationPortalMetadata(portal, conversation.ID, participantHash) {
		return false, nil
	}
	if err := portal.Save(ctx); err != nil {
		if hadExistingMetadata && existingMetadata != nil {
			*existingMetadata = originalMetadataValue
			portal.Metadata = existingMetadata
		} else {
			portal.Metadata = originalMetadata
		}
		return false, fmt.Errorf("failed to save Tumblr portal participant metadata: %w", err)
	}
	return true, nil
}

func (tc *TumblrClient) conversationParticipantHash(conversation tumblr.Conversation) string {
	parts := make([]string, 0, len(conversation.Participants))
	for _, participant := range conversation.Participants {
		userID := strings.TrimSpace(string(tumblrBlogUserID(participant)))
		if userID == "" {
			continue
		}
		parts = append(parts, strings.Join([]string{
			userID,
			tumblrBlogNameID(participant.Name),
			strings.TrimSpace(participant.Title),
			bestAvatarURL(participant.Avatar),
		}, "\x00"))
	}
	if len(parts) == 0 {
		return ""
	}
	sort.Strings(parts)
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return fmt.Sprintf("%x", sum[:16])
}

func (tc *TumblrClient) chatMembersFromConversation(conversation tumblr.Conversation) *bridgev2.ChatMemberList {
	members := bridgev2.ChatMemberMap{
		"": {
			EventSender: bridgev2.EventSender{IsFromMe: true},
			Membership:  event.MembershipJoin,
		},
	}
	var otherUserID networkid.UserID
	for _, participant := range conversation.Participants {
		userID := tumblrBlogUserID(participant)
		if userID == "" {
			continue
		}
		isSelf := tc.isSelfBlog(participant)
		if !isSelf && otherUserID == "" {
			otherUserID = userID
		}
		member := bridgev2.ChatMember{
			EventSender: bridgev2.EventSender{
				IsFromMe: isSelf,
				Sender:   userID,
			},
			Membership: event.MembershipJoin,
			UserInfo:   tc.blogUserInfo(participant),
		}
		if isSelf {
			member.UserInfo = nil
			member.Sender = ""
		}
		members.Set(member)
	}
	if len(members) == 0 {
		return nil
	}
	list := &bridgev2.ChatMemberList{
		IsFull:           true,
		TotalMemberCount: len(members),
		MemberMap:        members,
	}
	if len(members) == 2 {
		list.OtherUserID = otherUserID
	}
	return list
}

func (tc *TumblrClient) otherParticipant(conversation tumblr.Conversation) *tumblr.Blog {
	for i := range conversation.Participants {
		if tumblrBlogUserID(conversation.Participants[i]) == "" {
			continue
		}
		if !tc.isSelfBlog(conversation.Participants[i]) {
			return &conversation.Participants[i]
		}
	}
	return nil
}

func (tc *TumblrClient) conversationTitle(conversation tumblr.Conversation) string {
	names := tc.participantDisplayNames(conversation, false)
	if len(names) == 0 {
		names = tc.participantDisplayNames(conversation, true)
	}
	return cleanConversationTitle(strings.Join(names, ", "))
}

func (tc *TumblrClient) participantDisplayNames(conversation tumblr.Conversation, includeSelf bool) []string {
	names := make([]string, 0, len(conversation.Participants))
	for _, participant := range conversation.Participants {
		if tumblrBlogUserID(participant) == "" {
			continue
		}
		if !includeSelf && tc.isSelfBlog(participant) {
			continue
		}
		if display := tc.blogDisplayName(participant); display != "" {
			names = append(names, display)
		}
	}
	return names
}

func (tc *TumblrClient) validParticipantCount(conversation tumblr.Conversation) int {
	count := 0
	for _, participant := range conversation.Participants {
		if tumblrBlogUserID(participant) != "" {
			count++
		}
	}
	return count
}

func (tc *TumblrClient) blogDisplayName(blog tumblr.Blog) string {
	if tc != nil && tc.connector != nil {
		return tc.connector.Config.FormatDisplayname(tumblrBlogNameID(blog.Name), blog.Title)
	}
	return fallbackDisplayname(tumblrBlogNameID(blog.Name), blog.Title)
}

func cleanConversationTitle(title string) string {
	fields := strings.FieldsFunc(title, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	})
	return truncateConversationTitle(strings.Join(fields, " "))
}

func truncateConversationTitle(title string) string {
	runes := []rune(title)
	if len(runes) <= maxConversationTitleRunes {
		return title
	}
	return string(runes[:maxConversationTitleRunes]) + displayNameTruncation
}

func messageTimestampWithFallback(message tumblr.Message, fallback time.Time) time.Time {
	fallback = nonZeroFallbackTime(fallback)
	if ts, ok := saneTumblrTimestamp(message.Timestamp, fallback); ok {
		return ts
	}
	return fallback
}

func nonZeroFallbackTime(fallback time.Time) time.Time {
	if fallback.IsZero() {
		return time.Now()
	}
	return fallback
}

func messageSortTimestampWithReference(message tumblr.Message, reference time.Time) time.Time {
	ts, ok := saneTumblrTimestamp(message.Timestamp, nonZeroFallbackTime(reference))
	if !ok {
		return time.Time{}
	}
	return ts
}

func saneTumblrTimestamp(timestamp int64, reference time.Time) (time.Time, bool) {
	if timestamp <= 0 {
		return time.Time{}, false
	}
	ts := tumblrTimestamp(timestamp)
	if ts.After(reference.Add(maxTumblrTimestampFutureSkew)) {
		return time.Time{}, false
	}
	return ts, true
}

func tumblrTimestamp(timestamp int64) time.Time {
	if timestamp > 1_000_000_000_000 {
		return time.UnixMilli(timestamp)
	}
	return time.Unix(timestamp, 0)
}

func (tc *TumblrClient) portalKey(conversationID string) networkid.PortalKey {
	var loginID networkid.UserLoginID
	if tc.userLogin != nil {
		loginID = tc.userLogin.ID
	}
	// Tumblr only exposes one-to-one conversations. Match the WhatsApp bridge's
	// DM invariant and always scope a portal to the connected login, independent
	// of the global split-portals setting. Deleting a conversation can therefore
	// never detach or preserve another Tumblr account's room by mistake.
	return tumblrid.MakePortalKey(conversationID, loginID)
}

func (tc *TumblrClient) loginEventSender() bridgev2.EventSender {
	var loginID networkid.UserLoginID
	if tc.userLogin != nil {
		loginID = tc.userLogin.ID
	}
	sender := bridgev2.EventSender{
		IsFromMe:    true,
		SenderLogin: loginID,
	}
	if meta, err := tc.validatedLoginMetadata(); err == nil {
		sender.Sender = tumblrid.MakeUserID(meta.SelectedBlogUUID)
	}
	return sender
}

func (tc *TumblrClient) senderFromMessage(message tumblr.Message) bridgev2.EventSender {
	if message.Participant == nil {
		return bridgev2.EventSender{Sender: unknownTumblrUserID}
	}
	senderID := tumblrBlogUserID(*message.Participant)
	if senderID == "" {
		return bridgev2.EventSender{Sender: unknownTumblrUserID}
	}
	meta, err := tc.validatedLoginMetadata()
	isFromMe := err == nil && tumblrUserIDMatchesLogin(senderID, meta)
	sender := bridgev2.EventSender{
		IsFromMe:    isFromMe,
		Sender:      senderID,
		ForceDMUser: !isFromMe,
	}
	return sender
}

func (tc *TumblrClient) isSelfBlog(blog tumblr.Blog) bool {
	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		return false
	}
	return tumblrUserIDMatchesLogin(tumblrBlogUserID(blog), meta)
}

func tumblrUserIDMatchesLogin(userID networkid.UserID, meta *UserLoginMetadata) bool {
	if userID == "" || meta == nil {
		return false
	}
	return userID == tumblrid.MakeUserID(meta.SelectedBlogUUID) || userID == tumblrid.MakeUserID(meta.SelectedBlogName)
}
