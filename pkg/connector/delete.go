package connector

import (
	"context"
	"fmt"
	"time"

	"maunium.net/go/mautrix/bridgev2"

	"github.com/ifixrobots/tumblr-dms/pkg/tumblr"
)

const tumblrMatrixDeleteContinuationGrace = 2 * time.Minute

var _ bridgev2.DeleteChatHandlingNetworkAPI = (*TumblrClient)(nil)

func (tc *TumblrClient) HandleMatrixDeleteChat(ctx context.Context, msg *bridgev2.MatrixDeleteChat) error {
	if !tc.beginOwnedOperation() {
		return errTumblrClientRetired
	}
	defer tc.endOwnedOperation()
	if msg == nil {
		return fmt.Errorf("delete-chat event is required")
	}
	if msg.Content == nil {
		return fmt.Errorf("delete-chat content is required")
	}
	if msg.Content.DeleteForEveryone {
		return fmt.Errorf("tumblr dms do not support deleting chats for everyone")
	}
	conversationID, err := conversationIDFromPortal(msg.Portal, "portal is required to delete a Tumblr conversation")
	if err != nil {
		return err
	}
	if msg.Portal == nil || msg.Portal.MXID == "" {
		return fmt.Errorf("matrix room is required to delete a Tumblr conversation")
	}
	lockCtx, releaseSubmission, err := tc.acquireOutboundSubmissionLock(ctx)
	if err != nil {
		return err
	}
	defer releaseSubmission()
	ctx = lockCtx
	tc.syncLock.Lock()
	defer tc.syncLock.Unlock()
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = tc.requireLoggedInForContext(ctx); err != nil {
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
	// Persist cleanup before the remote request. If Tumblr accepts the delete
	// and this process exits before bridgev2 removes the room, the durable
	// inbound worker will resume from the saved room ID after restart. A failed
	// remote request is safe too: a direct probe of a still-live conversation
	// cancels this intent instead of deleting the room.
	if err = tc.connector.DB.Jobs.ScheduleDeleteRoom(
		ctx,
		tc.userLogin.ID,
		conversationID,
		msg.Portal.MXID,
		time.Now().Add(tumblrMatrixDeleteContinuationGrace),
	); err != nil {
		return fmt.Errorf("failed to persist Tumblr conversation deletion: %w", err)
	}
	deleteErr := client.DeleteConversation(ctx, meta.SelectedBlogName, conversationID)
	if deleteErr != nil && !tumblr.IsNotFound(deleteErr) {
		return tc.handleRemoteError(deleteErr)
	}
	if err = tc.terminalizeOutboundConversation(ctx, conversationID); err != nil {
		return fmt.Errorf("failed to cancel outbound sends for deleted Tumblr conversation: %w", err)
	}
	return nil
}
