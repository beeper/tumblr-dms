package connector

import (
	"context"
	"errors"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/simplevent"

	"github.com/beeper/tumblr-dms/pkg/tumblr"
)

var _ bridgev2.UserBlockingNetworkAPI = (*TumblrClient)(nil)

func (tc *TumblrClient) blockTarget(portal *bridgev2.Portal) (owner, peer string, err error) {
	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		return "", "", err
	}
	if !tumblr.IsBlogUUID(meta.SelectedBlogUUID) || string(tc.userLogin.ID) != meta.SelectedBlogUUID || portal == nil || portal.Portal == nil || portal.MXID == "" || portal.RoomType != database.RoomTypeDM || portal.PortalKey != tc.portalKey(string(portal.ID)) || isPendingDMPortalID(string(portal.ID)) || !tumblr.IsBlogUUID(string(portal.OtherUserID)) || tumblrUserIDMatchesLogin(portal.OtherUserID, meta) {
		return "", "", bridgev2.ErrNonDMBlockUser
	}
	return meta.SelectedBlogUUID, string(portal.OtherUserID), nil
}

func (tc *TumblrClient) HandleMatrixBlockUser(ctx context.Context, msg *bridgev2.MatrixBlockUser) error {
	if !tc.beginOwnedOperation() {
		return errTumblrClientRetired
	}
	defer tc.endOwnedOperation()
	if err := tc.requireLoggedIn(); err != nil {
		return err
	}
	if msg == nil || msg.Content == nil || msg.OrigSender != nil || msg.Content.ReportSpam {
		return errors.New("invalid Tumblr block request or unsupported spam report")
	}
	owner, peer, err := tc.blockTarget(msg.Portal)
	if err != nil {
		return err
	}
	client, err := tc.tumblrClient()
	if err != nil {
		return err
	}
	return tc.handleRemoteError(client.SetBlogBlocked(ctx, owner, peer, msg.Content.Block))
}

func (tc *TumblrClient) resyncDMBlock(portal *bridgev2.Portal) {
	owner, peer, err := tc.blockTarget(portal)
	if err != nil {
		return
	}
	if last, ok := tc.dmBlockResyncLast.Load(portal.PortalKey); ok && time.Since(last.(time.Time)) < 10*time.Minute {
		return
	}
	tc.dmBlockResyncLast.Store(portal.PortalKey, time.Now())
	roomID := portal.MXID
	go tc.userLogin.QueueRemoteEvent(&simplevent.ChatResync{
		EventMeta: simplevent.EventMeta{Type: bridgev2.RemoteEventChatResync, PortalKey: portal.PortalKey},
		GetChatInfoFunc: func(ctx context.Context, current *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
			if !tc.beginOwnedOperation() {
				return nil, errTumblrClientRetired
			}
			defer tc.endOwnedOperation()
			if err := tc.requireLoggedIn(); err != nil {
				return nil, err
			}
			currentOwner, currentPeer, err := tc.blockTarget(current)
			if err != nil {
				return nil, err
			}
			if currentOwner != owner || currentPeer != peer || current.MXID != roomID {
				return nil, errors.New("block target changed before refresh")
			}
			client, err := tc.tumblrClient()
			if err != nil {
				return nil, err
			}
			wasBlocked := current.UserBlocked
			blogs, err := client.ListBlockedBlogs(ctx, owner)
			if err != nil {
				return nil, tc.handleRemoteError(err)
			}
			blocked := false
			for _, blog := range blogs {
				blocked = blocked || blog.UUID == peer
			}
			if blocked == wasBlocked {
				return nil, nil
			}
			return &bridgev2.ChatInfo{UserBlocked: &blocked}, nil
		},
	})
}
