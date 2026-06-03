package connector

import (
	"context"
	"crypto/sha256"
	"fmt"
	"mime"
	"net/http"
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
	"maunium.net/go/mautrix/id"

	"github.com/ifixrobots/tumblr-dms/pkg/msgconv"
	"github.com/ifixrobots/tumblr-dms/pkg/tumblr"
	"github.com/ifixrobots/tumblr-dms/pkg/tumblrid"
)

const (
	maxSeenMessages                = 10000
	maxConversationListPages       = 1000
	maxRemoteIDRunes               = 512
	maxConversationTitleRunes      = 200
	maxPostSummaryRunes            = 160
	maxUnsupportedMessageTypeRunes = 80
	maxTumblrTimestampFutureSkew   = 24 * time.Hour
	unknownTumblrUserID            = networkid.UserID("unknown-tumblr-user")
)

func validRemoteID(id string) bool {
	return strings.TrimSpace(id) != "" &&
		utf8.RuneCountInString(id) <= maxRemoteIDRunes &&
		!containsMetadataSpaceOrControl(id)
}

func (tc *TumblrClient) syncConversations(ctx context.Context) error {
	return tc.fetchAndQueueConversations(ctx)
}

func (tc *TumblrClient) syncConversationByID(ctx context.Context, conversationID string) error {
	if !validRemoteID(conversationID) {
		return fmt.Errorf("tumblr conversation ID is invalid")
	}
	tc.syncLock.Lock()
	defer tc.syncLock.Unlock()

	if err := tc.requireLoggedIn(); err != nil {
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
	resp, err := client.GetConversation(ctx, meta.SelectedBlogName, conversationID, limit)
	if err != nil {
		if tumblr.IsAuthError(err) {
			return tc.handleRemoteError(err)
		}
		return err
	}
	if err = validateConversationHistoryResponse(conversationID, resp); err != nil {
		return err
	}
	conversation := mergeConversationHistoryForSync(tumblr.Conversation{ID: conversationID}, resp)
	if log := tc.log(); log != nil {
		log.Info().
			Str("conversation_id_hash", logIdentifierHash(conversationID)).
			Int("message_count", len(conversation.Messages.Data)).
			Msg("Fetched pushed Tumblr conversation")
	}
	tc.queueConversation(ctx, conversation, true)
	return nil
}

func (tc *TumblrClient) fetchAndQueueConversations(ctx context.Context) error {
	tc.syncLock.Lock()
	defer tc.syncLock.Unlock()

	if err := tc.requireLoggedIn(); err != nil {
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
	seenCursors := map[string]struct{}{}
	before := ""
	pagesFetched := 0
	conversationsQueued := 0
	defer func(startedAt time.Time) {
		if log := tc.log(); log != nil {
			log.Info().
				Dur("duration", time.Since(startedAt)).
				Int("pages_fetched", pagesFetched).
				Int("conversations_queued", conversationsQueued).
				Msg("Finished Tumblr conversation sync attempt")
		}
	}(time.Now())
	for page := 0; page < maxConversationListPages; page++ {
		resp, err := client.ListConversationsBefore(ctx, meta.SelectedBlogUUID, limit, before)
		if err != nil {
			return err
		}
		pagesFetched++
		for _, conversation := range resp.Conversations {
			conversation, err = tc.hydrateConversationForSync(ctx, client, meta.SelectedBlogName, conversation, limit)
			if err != nil {
				return err
			}
			tc.queueConversation(ctx, conversation, true)
			conversationsQueued++
		}
		nextBefore := strings.TrimSpace(resp.NextBefore())
		if nextBefore == "" {
			return nil
		}
		if _, ok := seenCursors[nextBefore]; ok {
			return nil
		}
		seenCursors[nextBefore] = struct{}{}
		before = nextBefore
	}
	return nil
}

func (tc *TumblrClient) hydrateConversationForSync(ctx context.Context, client *tumblr.Client, selectedBlogName string, conversation tumblr.Conversation, limit int) (tumblr.Conversation, error) {
	if !validRemoteID(conversation.ID) {
		return conversation, nil
	}
	resp, err := client.GetConversation(ctx, selectedBlogName, conversation.ID, limit)
	if err != nil {
		if tumblr.IsAuthError(err) {
			return conversation, tc.handleRemoteError(err)
		}
		if log := tc.log(); log != nil {
			log.Warn().Err(err).Str("conversation_id_hash", logIdentifierHash(conversation.ID)).Msg("Failed to fetch Tumblr conversation history during sync")
		}
		return conversation, nil
	}
	if err = validateConversationHistoryResponse(conversation.ID, resp); err != nil {
		if log := tc.log(); log != nil {
			log.Warn().Err(err).Str("conversation_id_hash", logIdentifierHash(conversation.ID)).Msg("Ignoring malformed Tumblr conversation history during sync")
		}
		return conversation, nil
	}
	return mergeConversationHistoryForSync(conversation, resp), nil
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

func (tc *TumblrClient) queueConversation(ctx context.Context, conversation tumblr.Conversation, forceChatResync bool) {
	if !validRemoteID(conversation.ID) {
		return
	}
	firstSeen, newMessages := tc.markConversationSeen(conversation)
	portalKey := tc.portalKey(conversation.ID)
	metadataChanged := tc.saveConversationMetadataIfChanged(ctx, portalKey, conversation)
	fallbackTimestamp := time.Now()
	latest := latestConversationTimeWithFallback(conversation, fallbackTimestamp)
	if forceChatResync || firstSeen || metadataChanged {
		tc.queueRemoteEvent(tc.chatResyncEventFromConversation(conversation, portalKey, latest))
	}
	if readReceipt := tc.readReceiptEventFromConversation(conversation, portalKey, fallbackTimestamp); readReceipt != nil &&
		tc.markConversationReadTimestampSeen(conversation.ID, conversation.LastReadTimestamp) {
		tc.queueRemoteEvent(readReceipt)
	}
	for _, message := range tc.messagesForConversationSync(ctx, portalKey, conversation, firstSeen, forceChatResync, newMessages) {
		tc.queueMessageWithFallback(portalKey, message, false, fallbackTimestamp)
	}
}

func (tc *TumblrClient) chatResyncEventFromConversation(conversation tumblr.Conversation, portalKey networkid.PortalKey, latest time.Time) *simplevent.ChatResync {
	return &simplevent.ChatResync{
		EventMeta: simplevent.EventMeta{
			Type:         bridgev2.RemoteEventChatResync,
			PortalKey:    portalKey,
			CreatePortal: true,
			Sender:       tc.loginEventSender(),
			Timestamp:    latest,
			StreamOrder:  latest.UnixMilli(),
			LogContext: func(c zerolog.Context) zerolog.Context {
				return c.Str("conversation_id_hash", logIdentifierHash(conversation.ID))
			},
		},
		ChatInfo:            tc.chatInfoFromConversation(conversation),
		LatestMessageTS:     latest,
		BundledBackfillData: bundledBackfillMessages(conversation.Messages.Data),
	}
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

func (tc *TumblrClient) messagesForConversationSync(ctx context.Context, portalKey networkid.PortalKey, conversation tumblr.Conversation, firstSeen, forceChatResync bool, newMessages []tumblr.Message) []tumblr.Message {
	if firstSeen || forceChatResync {
		return tc.missingConversationMessages(ctx, portalKey, conversation)
	}
	return newMessages
}

func (tc *TumblrClient) missingConversationMessages(ctx context.Context, portalKey networkid.PortalKey, conversation tumblr.Conversation) []tumblr.Message {
	if tc == nil || tc.connector == nil || tc.connector.Bridge == nil || tc.connector.Bridge.DB == nil {
		return nil
	}
	portal, err := tc.connector.Bridge.GetExistingPortalByKey(ctx, portalKey)
	if err != nil {
		if log := tc.log(); log != nil {
			log.Warn().Err(err).Str("conversation_id_hash", logIdentifierHash(conversation.ID)).Msg("Failed to load portal while checking for missing Tumblr messages")
		}
		return nil
	}
	if portal == nil || portal.Portal == nil || portal.MXID == "" {
		return nil
	}
	missing := make([]tumblr.Message, 0)
	for _, message := range sortedMessages(conversation.Messages.Data) {
		if !validRemoteID(message.ID) {
			continue
		}
		existing, err := tc.connector.Bridge.DB.Message.GetFirstPartByID(ctx, portalKey.Receiver, tumblrid.MakeMessageID(message.ID))
		if err != nil {
			if log := tc.log(); log != nil {
				log.Warn().Err(err).
					Str("conversation_id_hash", logIdentifierHash(conversation.ID)).
					Str("message_id_hash", logIdentifierHash(message.ID)).
					Msg("Failed to check if Tumblr message already exists")
			}
			continue
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
	return missing
}

func bundledBackfillMessages(messages []tumblr.Message) []tumblr.Message {
	if len(messages) == 0 {
		return nil
	}
	copied := make([]tumblr.Message, len(messages))
	for i, message := range messages {
		copied[i] = copyTumblrMessage(message)
	}
	return copied
}

func copyTumblrMessage(message tumblr.Message) tumblr.Message {
	copied := message
	if message.Participant != nil {
		participant := copyTumblrBlog(*message.Participant)
		copied.Participant = &participant
	}
	if message.Content != nil {
		content := *message.Content
		copied.Content = &content
	}
	if message.Post != nil {
		post := *message.Post
		copied.Post = &post
	}
	return copied
}

func copyTumblrBlog(blog tumblr.Blog) tumblr.Blog {
	copied := blog
	copied.Avatar = append([]tumblr.ImageAsset(nil), blog.Avatar...)
	if blog.Theme != nil {
		theme := *blog.Theme
		copied.Theme = &theme
	}
	return copied
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

func (tc *TumblrClient) markConversationReadTimestampSeen(conversationID string, timestamp int64) bool {
	if !validRemoteID(conversationID) || timestamp <= 0 {
		return false
	}
	tc.seenLock.Lock()
	defer tc.seenLock.Unlock()
	if previous := tc.seenReadTS[conversationID]; previous >= timestamp {
		return false
	}
	tc.seenReadTS[conversationID] = timestamp
	return true
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
		if left.Equal(right) {
			return sorted[i].ID < sorted[j].ID
		}
		return left.Before(right)
	})
	return sorted
}

func (tc *TumblrClient) queueMessageWithFallback(portalKey networkid.PortalKey, message tumblr.Message, createPortal bool, fallbackTimestamp time.Time) {
	evt := tc.messageEventFromMessage(portalKey, message, createPortal, fallbackTimestamp)
	if evt == nil {
		return
	}
	tc.queueRemoteEvent(evt)
}

func (tc *TumblrClient) messageEventFromMessage(portalKey networkid.PortalKey, message tumblr.Message, createPortal bool, fallbackTimestamp time.Time) *simplevent.Message[tumblr.Message] {
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
		TransactionID: networkid.TransactionID(message.ID),
		Data:          message,
		ConvertMessageFunc: func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, data tumblr.Message) (*bridgev2.ConvertedMessage, error) {
			return tc.convertTumblrMessageWithMedia(ctx, portal, intent, data)
		},
	}
}

func (tc *TumblrClient) convertTumblrMessageWithMedia(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, message tumblr.Message) (*bridgev2.ConvertedMessage, error) {
	if !msgconv.CanUseImageMedia(message) {
		return tc.convertTumblrMessage(message), nil
	}
	part, err := tc.convertTumblrImageMessage(ctx, portal, intent, message)
	if err != nil {
		if log := tc.log(); log != nil {
			log.Warn().Err(err).
				Str("message_id_hash", logIdentifierHash(message.ID)).
				Str("message_type", logMessageType(message.Type)).
				Msg("Falling back to Tumblr media notice after Matrix upload failed")
		}
		return tc.convertTumblrMessage(message), nil
	}
	if part == nil {
		return tc.convertTumblrMessage(message), nil
	}
	return &bridgev2.ConvertedMessage{
		Parts: []*bridgev2.ConvertedMessagePart{part},
	}, nil
}

func messageCanUseImageMedia(message tumblr.Message) bool {
	return msgconv.CanUseImageMedia(message)
}

func (tc *TumblrClient) convertTumblrImageMessage(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, message tumblr.Message) (*bridgev2.ConvertedMessagePart, error) {
	image := message.BestImage()
	if image == nil || strings.TrimSpace(image.URL) == "" {
		return nil, nil
	}
	if intent == nil {
		return nil, fmt.Errorf("matrix media uploader is not available")
	}
	client, err := tc.tumblrClient()
	if err != nil {
		return nil, err
	}
	data, err := client.Download(ctx, image.URL, tumblr.DefaultMaxDownloadBytes)
	if err != nil {
		return nil, err
	}
	mimeType := http.DetectContentType(data)
	fileName := tumblrImageFileName(mimeType)
	var roomID id.RoomID
	if portal != nil && portal.Portal != nil {
		roomID = portal.MXID
	}
	mxc, file, err := intent.UploadMedia(ctx, roomID, data, fileName, mimeType)
	if err != nil {
		return nil, fmt.Errorf("failed to upload Tumblr image to Matrix: %w", err)
	}
	info := &event.FileInfo{
		MimeType: mimeType,
		Size:     len(data),
		Width:    image.Width,
		Height:   image.Height,
	}
	if mimeType == "image/gif" {
		info.IsAnimated = true
		info.MauGIF = true
	}
	eventType := event.EventMessage
	msgType := event.MsgImage
	if message.Type == tumblr.MessageTypeSticker {
		eventType = event.EventSticker
		msgType = event.CapMsgSticker
	}
	return &bridgev2.ConvertedMessagePart{
		Type: eventType,
		Content: &event.MessageEventContent{
			MsgType:  msgType,
			Body:     fileName,
			FileName: fileName,
			URL:      mxc,
			File:     file,
			Info:     info,
		},
		DBMetadata: &MessageMetadata{Type: msgconv.MessageMetadataType(message.Type)},
	}, nil
}

func tumblrImageFileName(mimeType string) string {
	extensions, _ := mime.ExtensionsByType(mimeType)
	if len(extensions) > 0 {
		return "tumblr-image" + extensions[0]
	}
	return "tumblr-image"
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

func (tc *TumblrClient) saveConversationMetadataIfChanged(ctx context.Context, portalKey networkid.PortalKey, conversation tumblr.Conversation) bool {
	if tc == nil || tc.connector == nil || tc.connector.Bridge == nil || tc.connector.Bridge.DB == nil {
		return false
	}
	participantHash := tc.conversationParticipantHash(conversation)
	if participantHash == "" {
		return false
	}
	portal, err := tc.connector.Bridge.GetExistingPortalByKey(ctx, portalKey)
	if err != nil {
		if log := tc.log(); log != nil {
			log.Warn().Err(err).Str("conversation_id_hash", logIdentifierHash(conversation.ID)).Msg("Failed to load Tumblr portal metadata for participant sync")
		}
		return false
	}
	if portal == nil || portal.Portal == nil {
		return false
	}
	if !applyConversationPortalMetadata(portal, conversation.ID, participantHash) {
		return false
	}
	if err := portal.Save(ctx); err != nil {
		if log := tc.log(); log != nil {
			log.Warn().Err(err).Str("conversation_id_hash", logIdentifierHash(conversation.ID)).Msg("Failed to save Tumblr portal participant metadata")
		}
	}
	return true
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

func latestConversationTimeWithFallback(conversation tumblr.Conversation, fallback time.Time) time.Time {
	latest := time.Time{}
	for _, message := range conversation.Messages.Data {
		ts := messageSortTimestampWithReference(message, fallback)
		if ts.After(latest) {
			latest = ts
		}
	}
	if latest.IsZero() {
		latest = nonZeroFallbackTime(fallback)
	}
	return latest
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
	splitPortals := tc.connector != nil && tc.connector.Bridge != nil && tc.connector.Bridge.Config != nil && tc.connector.Bridge.Config.SplitPortals
	return tumblrid.MakePortalKey(conversationID, loginID, splitPortals)
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
