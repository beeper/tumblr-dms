package connector

import (
	"context"
	"fmt"

	"maunium.net/go/mautrix/bridgev2"
)

var _ bridgev2.DeleteChatHandlingNetworkAPI = (*TumblrClient)(nil)

func (tc *TumblrClient) HandleMatrixDeleteChat(ctx context.Context, msg *bridgev2.MatrixDeleteChat) error {
	if msg == nil {
		return fmt.Errorf("delete-chat event is required")
	}
	if msg.Content == nil {
		return fmt.Errorf("delete-chat content is required")
	}
	if msg.Content.DeleteForEveryone {
		return fmt.Errorf("tumblr dms do not support deleting chats for everyone")
	}
	if msg.Event == nil {
		return fmt.Errorf("matrix event is required to delete a Tumblr conversation")
	}
	conversationID, err := conversationIDFromPortal(msg.Portal, "portal is required to delete a Tumblr conversation")
	if err != nil {
		return err
	}
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
	return tc.handleRemoteError(client.DeleteConversation(ctx, meta.SelectedBlogName, conversationID))
}
