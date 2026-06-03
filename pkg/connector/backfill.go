package connector

import (
	"context"
	"fmt"
	"strings"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/ifixrobots/tumblr-dms/pkg/tumblr"
	"github.com/ifixrobots/tumblr-dms/pkg/tumblrid"
)

var _ bridgev2.BackfillingNetworkAPI = (*TumblrClient)(nil)
var _ bridgev2.BackfillingNetworkAPIWithLimits = (*TumblrClient)(nil)

func (tc *TumblrClient) GetBackfillMaxBatchCount(_ context.Context, portal *bridgev2.Portal, _ *database.BackfillTask) int {
	if tc == nil || tc.userLogin == nil || tc.userLogin.Bridge == nil {
		return 0
	}
	queueConfig := tc.userLogin.Bridge.Config.Backfill.Queue
	if portal == nil || portal.RoomType == "" {
		return queueConfig.MaxBatches
	}
	return queueConfig.GetOverride(string(portal.RoomType))
}

func (tc *TumblrClient) FetchMessages(ctx context.Context, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	if params.Portal == nil {
		return &bridgev2.FetchMessagesResponse{HasMore: false}, nil
	}
	conversationID, err := conversationIDFromPortal(params.Portal, "portal is required to fetch Tumblr messages")
	if err != nil {
		return nil, err
	}
	if params.ThreadRoot != "" || (params.Forward && params.Cursor != "") {
		return &bridgev2.FetchMessagesResponse{
			Forward: params.Forward,
			HasMore: false,
		}, nil
	}
	count := tc.backfillLimit(params.Count)

	if params.Forward {
		messages, ok := params.BundledData.([]tumblr.Message)
		if ok {
			return tc.backfillResponseFromMessagesWithAnchorAndLimit(ctx, params.Portal, messages, params.AnchorMessage, count), nil
		}
		return &bridgev2.FetchMessagesResponse{
			Forward: true,
			HasMore: false,
		}, nil
	}

	if err := tc.requireLoggedIn(); err != nil {
		return nil, err
	}
	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		return nil, err
	}
	cursor := string(params.Cursor)
	if cursor == "" && params.AnchorMessage != nil && !params.AnchorMessage.Timestamp.IsZero() {
		cursor = fmt.Sprintf("%d", params.AnchorMessage.Timestamp.UnixMilli())
	}
	client, err := tc.tumblrClient()
	if err != nil {
		return nil, err
	}
	resp, err := client.GetConversationBefore(ctx, meta.SelectedBlogName, conversationID, count, cursor)
	if err != nil {
		return nil, tc.handleRemoteError(err)
	}
	if err = validateConversationHistoryResponse(conversationID, resp); err != nil {
		return nil, err
	}
	tc.markConversationMessagesSeen(conversationID, resp.Messages)
	backfill := tc.backfillResponseFromMessages(ctx, params.Portal, resp.Messages, false)
	if nextBefore := strings.TrimSpace(resp.NextBefore()); nextBefore != "" {
		backfill.Cursor = networkid.PaginationCursor(nextBefore)
		backfill.HasMore = true
	}
	return backfill, nil
}

func validateConversationHistoryResponse(conversationID string, resp *tumblr.ConversationMessagesResponse) error {
	if resp == nil {
		return fmt.Errorf("conversation history response is missing")
	}
	if resp.Conversation == nil {
		return fmt.Errorf("conversation history response did not include conversation metadata")
	}
	if !validRemoteID(resp.Conversation.ID) {
		return fmt.Errorf("conversation history response did not include a valid conversation ID")
	}
	if resp.Conversation.ID != conversationID {
		return fmt.Errorf("conversation history response ID did not match requested conversation")
	}
	return nil
}

func (tc *TumblrClient) backfillLimit(requested int) int {
	limit := defaultConversationSyncLimit
	if tc != nil && tc.connector != nil {
		limit = tc.connector.Config.ConversationSyncBatchLimit()
	}
	if requested <= 0 || requested > limit {
		return limit
	}
	return requested
}

func (tc *TumblrClient) backfillResponseFromMessages(ctx context.Context, portal *bridgev2.Portal, messages []tumblr.Message, forward bool) *bridgev2.FetchMessagesResponse {
	return tc.backfillResponseFromMessagesWithFallbackAndLimit(ctx, portal, messages, forward, time.Now(), 0)
}

func (tc *TumblrClient) backfillResponseFromMessagesWithAnchorAndLimit(ctx context.Context, portal *bridgev2.Portal, messages []tumblr.Message, anchor *database.Message, limit int) *bridgev2.FetchMessagesResponse {
	fallbackTimestamp := time.Now()
	messages = messagesAfterAnchor(messages, anchor, fallbackTimestamp)
	return tc.backfillResponseFromMessagesWithFallbackAndLimit(ctx, portal, messages, true, fallbackTimestamp, limit)
}

func (tc *TumblrClient) backfillResponseFromMessagesWithFallbackAndLimit(ctx context.Context, portal *bridgev2.Portal, messages []tumblr.Message, forward bool, fallbackTimestamp time.Time, limit int) *bridgev2.FetchMessagesResponse {
	messages = sortedMessagesWithReference(messages, fallbackTimestamp)
	backfillMessages := make([]*bridgev2.BackfillMessage, 0, len(messages))
	seenIDs := make(map[string]struct{}, len(messages))
	for _, message := range messages {
		if !validRemoteID(message.ID) {
			continue
		}
		if _, ok := seenIDs[message.ID]; ok {
			continue
		}
		seenIDs[message.ID] = struct{}{}
		ts := messageTimestampWithFallback(message, fallbackTimestamp)
		backfillMessages = append(backfillMessages, &bridgev2.BackfillMessage{
			ConvertedMessage: tc.convertTumblrMessageForBackfill(ctx, portal, message),
			Sender:           tc.senderFromMessage(message),
			ID:               tumblrid.MakeMessageID(message.ID),
			TxnID:            networkid.TransactionID(message.ID),
			Timestamp:        ts,
			StreamOrder:      ts.UnixMilli(),
		})
	}
	if limit > 0 && len(backfillMessages) > limit {
		backfillMessages = backfillMessages[len(backfillMessages)-limit:]
	}
	return &bridgev2.FetchMessagesResponse{
		Messages:                backfillMessages,
		Forward:                 forward,
		HasMore:                 false,
		AggressiveDeduplication: true,
	}
}

func (tc *TumblrClient) convertTumblrMessageForBackfill(ctx context.Context, portal *bridgev2.Portal, message tumblr.Message) *bridgev2.ConvertedMessage {
	uploader := tc.backfillMediaUploader()
	if messageCanUseImageMedia(message) && uploader != nil {
		converted, err := tc.convertTumblrMessageWithMedia(ctx, portal, uploader, message)
		if err == nil && converted != nil {
			return converted
		}
	}
	return tc.convertTumblrMessage(message)
}

func (tc *TumblrClient) backfillMediaUploader() bridgev2.MatrixAPI {
	if tc == nil || tc.userLogin == nil || tc.userLogin.Bridge == nil {
		return nil
	}
	return tc.userLogin.Bridge.Bot
}

func messagesAfterAnchor(messages []tumblr.Message, anchor *database.Message, fallbackTimestamp time.Time) []tumblr.Message {
	if anchor == nil {
		return messages
	}
	anchorID := string(anchor.ID)
	filtered := make([]tumblr.Message, 0, len(messages))
	for _, message := range messages {
		if message.ID == anchorID {
			continue
		}
		if !anchor.Timestamp.IsZero() {
			if saneTS, ok := saneTumblrTimestamp(message.Timestamp, fallbackTimestamp); !ok || !saneTS.After(anchor.Timestamp) {
				continue
			}
		}
		filtered = append(filtered, message)
	}
	return filtered
}
