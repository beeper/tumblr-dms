package connector

import (
	"context"

	"maunium.net/go/mautrix/bridgev2"
)

var (
	_ bridgev2.ReadReceiptHandlingNetworkAPI = (*TumblrClient)(nil)
	_ bridgev2.ChatViewingNetworkAPI         = (*TumblrClient)(nil)
)

func (tc *TumblrClient) HandleMatrixReadReceipt(ctx context.Context, msg *bridgev2.MatrixReadReceipt) error {
	if msg == nil || msg.Portal == nil {
		return nil
	}
	conversationID, err := conversationIDFromPortal(msg.Portal, "portal is required to mark a Tumblr conversation read")
	if err != nil {
		return err
	}
	if !msg.ReadUpTo.After(msg.LastRead) {
		return nil
	}
	return tc.markConversationRead(ctx, conversationID)
}

func (tc *TumblrClient) HandleMatrixViewingChat(ctx context.Context, msg *bridgev2.MatrixViewingChat) error {
	if msg == nil || msg.Portal == nil {
		return nil
	}
	return tc.markPortalRead(ctx, msg.Portal)
}

func (tc *TumblrClient) markPortalRead(ctx context.Context, portal *bridgev2.Portal) error {
	conversationID, err := conversationIDFromPortal(portal, "portal is required to mark a Tumblr conversation read")
	if err != nil {
		return err
	}
	return tc.markConversationRead(ctx, conversationID)
}

func (tc *TumblrClient) markConversationRead(ctx context.Context, conversationID string) error {
	if err := tc.requireLoggedIn(); err != nil {
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
	return tc.handleRemoteError(client.MarkConversationAsRead(ctx, meta.SelectedBlogName, conversationID))
}
