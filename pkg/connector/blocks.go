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
	if !tumblr.IsBlogUUID(meta.SelectedBlogUUID) ||
		string(tc.userLogin.ID) != meta.SelectedBlogUUID ||
		portal == nil || portal.Portal == nil ||
		portal.RoomType != database.RoomTypeDM ||
		portal.PortalKey != tc.portalKey(string(portal.ID)) ||
		isPendingDMPortalID(string(portal.ID)) ||
		!tumblr.IsBlogUUID(string(portal.OtherUserID)) ||
		tumblrUserIDMatchesLogin(portal.OtherUserID, meta) {
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
	if msg.Portal.MXID == "" {
		return bridgev2.ErrNonDMBlockUser
	}
	client, err := tc.tumblrClient()
	if err != nil {
		return err
	}
	lockCtx, release, err := tc.acquireOutboundSubmissionLock(ctx)
	if err != nil {
		return err
	}
	defer release()
	tc.dmBlockRevision.Add(1)
	defer tc.dmBlockRevision.Add(1)
	if err = tc.handleRemoteError(client.SetBlogBlocked(lockCtx, owner, peer, msg.Content.Block)); err != nil {
		return err
	}
	if !msg.Content.Block {
		if err = tc.connector.DB.Jobs.PutLiveConversation(lockCtx, tc.userLogin.ID, string(msg.Portal.ID)); err != nil {
			return err
		}
		tc.wakeInboundSync()
	}
	return nil
}

func (tc *TumblrClient) resyncDMBlock(portal *bridgev2.Portal) {
	if time.Now().UnixNano() < tc.dmBlockRetryAfter.Load() {
		return
	}
	owner, peer, err := tc.blockTarget(portal)
	if err != nil || portal.MXID == "" {
		return
	}
	if last, ok := tc.dmBlockResyncLast.Load(portal.PortalKey); ok && time.Since(last.(time.Time)) < 10*time.Minute {
		return
	}
	roomID := portal.MXID
	portalKey := portal.PortalKey
	revision := tc.dmBlockRevision.Load()
	go func() {
		refresh := func(ctx context.Context) (*bridgev2.ChatInfo, error) {
			if _, loaded := tc.dmBlockResyncInFlight.LoadOrStore(portalKey, true); loaded {
				return nil, nil
			}
			defer tc.dmBlockResyncInFlight.Delete(portalKey)
			if last, ok := tc.dmBlockResyncLast.Load(portalKey); ok && time.Since(last.(time.Time)) < 10*time.Minute {
				return nil, nil
			}
			if !tc.beginOwnedOperation() {
				return nil, errTumblrClientRetired
			}
			defer tc.endOwnedOperation()
			if err := tc.requireLoggedIn(); err != nil {
				return nil, err
			}
			client, err := tc.tumblrClient()
			if err != nil {
				return nil, err
			}
			if time.Now().UnixNano() < tc.dmBlockRetryAfter.Load() {
				return nil, nil
			}
			blockedBlogs, err := tc.blockedBlogUUIDs(ctx, client, owner)
			if err != nil {
				if ctx.Err() == nil {
					retryAfter := time.Now().Add(30 * time.Second).UnixNano()
					for previous := tc.dmBlockRetryAfter.Load(); previous < retryAfter; previous = tc.dmBlockRetryAfter.Load() {
						if tc.dmBlockRetryAfter.CompareAndSwap(previous, retryAfter) {
							break
						}
					}
				}
				return nil, tc.handleRemoteError(err)
			}
			blocked := blockedBlogs[peer]
			return &bridgev2.ChatInfo{UserBlocked: &blocked}, nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		info, err := refresh(ctx)
		if err != nil || info == nil {
			return
		}
		tc.userLogin.QueueRemoteEvent(&simplevent.ChatResync{
			EventMeta: simplevent.EventMeta{Type: bridgev2.RemoteEventChatResync, PortalKey: portalKey},
			GetChatInfoFunc: func(ctx context.Context, current *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
				if tc.dmBlockRevision.Load() != revision {
					return nil, nil
				}
				currentOwner, currentPeer, err := tc.blockTarget(current)
				if err != nil || currentOwner != owner || currentPeer != peer || current.MXID != roomID {
					return nil, errors.New("block target changed before refresh")
				}
				if err = tc.requireLoggedIn(); err != nil {
					return nil, err
				}
				tc.dmBlockResyncLast.Store(portalKey, time.Now())
				return info, nil
			},
		})
	}()
}

func (tc *TumblrClient) blockedBlogUUIDs(ctx context.Context, client *tumblr.Client, owner string) (map[string]bool, error) {
	tc.dmBlockListLock.Lock()
	defer tc.dmBlockListLock.Unlock()
	revision := tc.dmBlockRevision.Load()
	if tc.dmBlockList != nil && tc.dmBlockListRevision == revision && time.Since(tc.dmBlockListTime) < time.Hour {
		return tc.dmBlockList, nil
	}
	blogs, err := client.ListBlockedBlogs(ctx, owner)
	if err != nil {
		return nil, err
	}
	blocked := make(map[string]bool, len(blogs))
	for _, blog := range blogs {
		blocked[blog.UUID] = true
	}
	tc.dmBlockList, tc.dmBlockListTime, tc.dmBlockListRevision = blocked, time.Now(), revision
	return blocked, nil
}
