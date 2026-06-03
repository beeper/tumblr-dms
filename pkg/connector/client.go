package connector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"mime"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"

	"github.com/ifixrobots/tumblr-dms/pkg/msgconv"
	"github.com/ifixrobots/tumblr-dms/pkg/tumblr"
	"github.com/ifixrobots/tumblr-dms/pkg/tumblrid"
)

type TumblrClient struct {
	connector *TumblrConnector
	userLogin *bridgev2.UserLogin
	client    *tumblr.Client
	loggedIn  atomic.Bool

	pushLock   sync.Mutex
	pushCancel context.CancelFunc
	pushWG     sync.WaitGroup

	syncLock sync.Mutex
	seenLock sync.Mutex

	seenConversations          map[string]struct{}
	seenConversationModifiedTS map[string]int64
	seenMessages               map[string]struct{}
	seenMessageOrder           []string
	seenReadTS                 map[string]int64
}

var _ bridgev2.NetworkAPI = (*TumblrClient)(nil)

var (
	errTumblrEmptyText   = unsupportedMatrixMessageError(errors.New("message text is empty"))
	errTumblrTextTooLong = unsupportedMatrixMessageError(errors.New("message is too long for tumblr dms"))
)

func unsupportedMatrixMessageError(err error) error {
	return bridgev2.WrapErrorInStatus(err).
		WithErrorAsMessage().
		WithIsCertain(true).
		WithSendNotice(true).
		WithErrorReason(event.MessageStatusUnsupported)
}

func NewTumblrClient(login *bridgev2.UserLogin, connector *TumblrConnector, client *tumblr.Client) *TumblrClient {
	return &TumblrClient{
		connector:                  connector,
		userLogin:                  login,
		client:                     client,
		seenConversations:          make(map[string]struct{}),
		seenConversationModifiedTS: make(map[string]int64),
		seenMessages:               make(map[string]struct{}),
		seenReadTS:                 make(map[string]int64),
	}
}

func (tc *TumblrClient) tumblrClient() (*tumblr.Client, error) {
	if tc == nil || tc.client == nil {
		return nil, fmt.Errorf("tumblr client is not available")
	}
	return tc.client, nil
}

func (tc *TumblrClient) sendBridgeState(state status.BridgeState) {
	if tc == nil || tc.userLogin == nil || tc.userLogin.BridgeState == nil {
		return
	}
	tc.userLogin.BridgeState.Send(state)
}

func (tc *TumblrClient) failConnect(state status.BridgeState) {
	tc.loggedIn.Store(false)
	tc.sendBridgeState(state)
}

func (tc *TumblrClient) handleRemoteError(err error) error {
	if tumblr.IsAuthError(err) {
		tc.failConnect(tumblrBadCredentialsState(err))
	}
	return err
}

func tumblrBadCredentialsState(err error) status.BridgeState {
	message := ""
	if err != nil {
		message = err.Error()
	}
	return status.BridgeState{
		StateEvent: status.StateBadCredentials,
		Error:      "tumblr-bad-credentials",
		Message:    message,
	}
}

func (tc *TumblrClient) saveUserLogin(ctx context.Context) error {
	if tc == nil || tc.userLogin == nil || tc.userLogin.UserLogin == nil ||
		tc.userLogin.Bridge == nil || tc.userLogin.Bridge.DB == nil || tc.userLogin.Bridge.DB.UserLogin == nil {
		return nil
	}
	return tc.userLogin.Save(ctx)
}

func (tc *TumblrClient) queueRemoteEvent(evt bridgev2.RemoteEvent) {
	if tc == nil || tc.userLogin == nil || tc.userLogin.Bridge == nil {
		return
	}
	tc.userLogin.QueueRemoteEvent(evt)
}

func (tc *TumblrClient) log() *zerolog.Logger {
	if tc == nil || tc.userLogin == nil {
		return nil
	}
	return &tc.userLogin.Log
}

func (tc *TumblrConnector) LoadUserLogin(_ context.Context, login *bridgev2.UserLogin) error {
	if login == nil || login.UserLogin == nil {
		return fmt.Errorf("tumblr user login is missing")
	}
	meta, err := validateUserLoginMetadata(login.Metadata)
	if err != nil {
		return err
	}
	client := tumblr.NewClient(tumblr.Options{
		CookieHeader: meta.CookieHeader,
		APIToken:     meta.APIToken,
		CSRFToken:    meta.CSRFToken,
		APIVersion:   meta.APIVersion,
		UserAgent:    tc.Config.BrowserUserAgent(),
		HTTPClient:   tc.newHTTPClient(),
	})
	login.Client = NewTumblrClient(login, tc, client)
	return nil
}

func (tc *TumblrClient) Connect(ctx context.Context) {
	if tc == nil {
		return
	}
	tc.sendBridgeState(status.BridgeState{StateEvent: status.StateConnecting})
	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		tc.failConnect(status.BridgeState{
			StateEvent: status.StateBadCredentials,
			Error:      "tumblr-invalid-login-metadata",
			Message:    err.Error(),
		})
		return
	}
	client, err := tc.tumblrClient()
	if err != nil {
		tc.failConnect(status.BridgeState{
			StateEvent: status.StateUnknownError,
			Error:      "tumblr-client-unavailable",
			Message:    err.Error(),
		})
		return
	}
	if err := client.Bootstrap(ctx); err != nil {
		tc.failConnect(connectBootstrapFailureState(err))
		return
	}
	userInfo, err := client.CurrentUser(ctx)
	if err != nil {
		if tumblr.IsAuthError(err) {
			tc.failConnect(tumblrBadCredentialsState(err))
			return
		}
		tc.failConnect(status.BridgeState{
			StateEvent: status.StateUnknownError,
			Error:      "tumblr-current-user-failed",
			Message:    err.Error(),
		})
		return
	}
	blog, err := selectedBlogFromCurrentUser(userInfo, meta)
	if err != nil {
		tc.failConnect(status.BridgeState{
			StateEvent: status.StateBadCredentials,
			Error:      "tumblr-selected-blog-unavailable",
			Message:    err.Error(),
		})
		return
	}
	tc.loggedIn.Store(true)
	meta.APIToken = client.APIToken()
	meta.CSRFToken = client.CSRFToken()
	meta.APIVersion = client.APIVersion()
	meta.UserName = userName(userInfo, blog)
	meta.SelectedBlogName = blog.Name
	meta.SelectedBlogUUID = blog.UUID
	if tc.userLogin != nil && tc.userLogin.UserLogin != nil {
		tc.userLogin.RemoteName = blog.Name
		tc.userLogin.RemoteProfile = status.RemoteProfile{
			Username: blog.Name,
			Name:     displayName(tc.connector, blog),
		}
	}
	if err := tc.saveUserLogin(ctx); err != nil {
		if log := tc.log(); log != nil {
			log.Warn().Err(err).Msg("Failed to save refreshed Tumblr API tokens")
		}
	}
	if err := tc.ensureSelfHostedPushReceiver(ctx); err != nil {
		if log := tc.log(); log != nil {
			log.Warn().Err(err).Msg("Failed to start Tumblr web push receiver")
		}
	}
	if err := tc.syncConversations(ctx); err != nil {
		if tumblr.IsAuthError(err) {
			tc.failConnect(tumblrBadCredentialsState(err))
			return
		}
		if log := tc.log(); log != nil {
			log.Warn().Err(err).Msg("Failed to sync Tumblr conversations")
		}
	}
	tc.sendBridgeState(status.BridgeState{StateEvent: status.StateConnected})
}

func connectBootstrapFailureState(err error) status.BridgeState {
	if tumblr.IsAuthError(err) {
		return tumblrBadCredentialsState(err)
	}
	message := ""
	if err != nil {
		message = err.Error()
	}
	return status.BridgeState{
		StateEvent: status.StateUnknownError,
		Error:      "tumblr-bootstrap-failed",
		Message:    message,
	}
}

func (tc *TumblrClient) Disconnect() {
	if tc == nil {
		return
	}
	tc.stopPushReceiver()
	tc.loggedIn.Store(false)
}

func (tc *TumblrClient) IsLoggedIn() bool {
	if tc == nil {
		return false
	}
	return tc.loggedIn.Load()
}

func (tc *TumblrClient) requireLoggedIn() error {
	if !tc.IsLoggedIn() {
		return bridgev2.ErrNotLoggedIn
	}
	return nil
}

func (tc *TumblrClient) LogoutRemote(ctx context.Context) {
	if tc == nil {
		return
	}
	tc.stopPushReceiver()
	tc.unregisterTumblrWebPush(ctx)
	tc.loggedIn.Store(false)
}

func (tc *TumblrClient) IsThisUser(_ context.Context, userID networkid.UserID) bool {
	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		return false
	}
	parsed := tumblrid.ParseUserID(userID)
	return parsed == meta.SelectedBlogUUID || parsed == meta.SelectedBlogName || parsed == string(tc.userLogin.ID)
}

func (tc *TumblrClient) GetChatInfo(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	if portal == nil || portal.Portal == nil {
		return nil, fmt.Errorf("portal is required to get Tumblr chat info")
	}
	if conversationID, err := conversationIDFromPortal(portal, "portal is required to get Tumblr chat info"); err == nil && tc.IsLoggedIn() {
		meta, metaErr := tc.validatedLoginMetadata()
		if metaErr != nil {
			return nil, metaErr
		}
		client, clientErr := tc.tumblrClient()
		if clientErr != nil {
			return nil, clientErr
		}
		resp, remoteErr := client.GetConversation(ctx, meta.SelectedBlogName, conversationID, 1)
		if remoteErr != nil {
			return nil, tc.handleRemoteError(remoteErr)
		}
		if resp != nil && resp.Conversation != nil {
			return tc.chatInfoFromConversation(*resp.Conversation), nil
		}
	}
	roomType := database.RoomTypeDM
	info := &bridgev2.ChatInfo{
		Type: &roomType,
	}
	if name := cleanConversationTitle(portal.Name); name != "" {
		info.Name = &name
	}
	return info, nil
}

func (tc *TumblrClient) GetUserInfo(_ context.Context, ghost *bridgev2.Ghost) (*bridgev2.UserInfo, error) {
	if ghost == nil || ghost.Ghost == nil || strings.TrimSpace(string(ghost.ID)) == "" {
		return nil, fmt.Errorf("ghost is required to get Tumblr user info")
	}
	name := string(ghost.ID)
	if !validGhostID(name) {
		return nil, fmt.Errorf("tumblr ghost id contains invalid characters")
	}
	displayName := cleanDisplayName(name)
	return &bridgev2.UserInfo{
		Identifiers: []string{"tumblr:" + name},
		Name:        &displayName,
	}, nil
}

func validGhostID(userID string) bool {
	if !validRemoteID(userID) {
		return false
	}
	if strings.ContainsAny(userID, `/\?#.`) {
		return false
	}
	if strings.Contains(userID, ":") && !strings.HasPrefix(userID, "t:") {
		return false
	}
	return true
}

func (tc *TumblrClient) HandleMatrixMessage(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	if msg == nil || msg.Content == nil {
		return nil, fmt.Errorf("message content is required")
	}
	if msg.Event == nil {
		return nil, fmt.Errorf("matrix event is required to send a tumblr message")
	}
	if strings.TrimSpace(string(msg.Event.ID)) == "" {
		return nil, fmt.Errorf("matrix event id is required to send a tumblr message")
	}
	if msg.Content.RelatesTo.GetReplaceID() != "" || msg.Content.NewContent != nil {
		return nil, bridgev2.ErrEditsNotSupported
	}
	switch msg.Content.MsgType {
	case event.MsgText, event.MsgNotice:
		return tc.handleMatrixTextMessage(ctx, msg)
	case event.MsgImage, event.CapMsgSticker:
		return tc.handleMatrixImageMessage(ctx, msg)
	case event.MsgVideo:
		if canSendMatrixGIFAsTumblrImage(msg.Content) {
			return tc.handleMatrixImageMessage(ctx, msg)
		}
		return nil, unsupportedMatrixMessageError(fmt.Errorf("tumblr dms currently only support text, image, GIF, and sticker messages"))
	default:
		return nil, unsupportedMatrixMessageError(fmt.Errorf("tumblr dms currently only support text, image, GIF, and sticker messages"))
	}
}

func canSendMatrixGIFAsTumblrImage(content *event.MessageEventContent) bool {
	if content == nil || content.GetCapMsgType() != event.CapMsgGIF {
		return false
	}
	if content.Info == nil || strings.TrimSpace(content.Info.MimeType) == "" {
		return true
	}
	mediaType, _, err := mime.ParseMediaType(content.Info.MimeType)
	if err != nil {
		return false
	}
	return strings.EqualFold(mediaType, "image/gif")
}

func (tc *TumblrClient) handleMatrixTextMessage(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	content := *msg.Content
	if content.RelatesTo.GetReplyTo() != "" {
		content.RemoveReplyFallback()
		content.Body = event.TrimReplyFallbackText(content.Body)
	}
	text := content.Body
	if strings.TrimSpace(text) == "" {
		return nil, errTumblrEmptyText
	}
	if textLength := utf8.RuneCountInString(text); textLength > MaxTextLength {
		return nil, fmt.Errorf("%w: %d > %d", errTumblrTextTooLong, textLength, MaxTextLength)
	}
	if parsed, ok := parseTumblrPostShare(text); ok {
		if post, resolved, err := tc.resolveTumblrPostShare(ctx, parsed); err != nil {
			return nil, err
		} else if resolved {
			return tc.sendMatrixMessageToTumblr(ctx, msg, tumblr.MessageTypePostRef, "", tumblr.ImageUpload{}, &post)
		}
	}
	return tc.sendMatrixMessageToTumblr(ctx, msg, tumblr.MessageTypeText, text, tumblr.ImageUpload{}, nil)
}

func (tc *TumblrClient) handleMatrixImageMessage(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	if msg.Content.URL == "" && (msg.Content.File == nil || msg.Content.File.URL == "") {
		return nil, unsupportedMatrixMessageError(fmt.Errorf("image message is missing media"))
	}
	if msg.Portal == nil || msg.Portal.Bridge == nil || msg.Portal.Bridge.Bot == nil {
		return nil, fmt.Errorf("matrix media downloader is not available")
	}
	data, err := msg.Portal.Bridge.Bot.DownloadMedia(ctx, msg.Content.URL, msg.Content.File)
	if err != nil {
		return nil, fmt.Errorf("failed to download Matrix image: %w", err)
	}
	contentType := ""
	if msg.Content.Info != nil {
		contentType = msg.Content.Info.MimeType
	}
	return tc.sendMatrixMessageToTumblr(ctx, msg, tumblr.MessageTypeImage, "", tumblr.ImageUpload{
		Data:        data,
		FileName:    msg.Content.GetFileName(),
		ContentType: contentType,
	}, nil)
}

func (tc *TumblrClient) sendMatrixMessageToTumblr(ctx context.Context, msg *bridgev2.MatrixMessage, messageType, text string, image tumblr.ImageUpload, post *tumblr.PostShare) (*bridgev2.MatrixMessageResponse, error) {
	pendingMeta := pendingDMMetadataFromPortal(msg.Portal)
	conversationID := ""
	if pendingMeta == nil {
		var err error
		conversationID, err = conversationIDFromPortal(msg.Portal, "portal is required to send a Tumblr message")
		if err != nil {
			return nil, err
		}
	}
	if err := tc.requireLoggedIn(); err != nil {
		return nil, err
	}
	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		return nil, err
	}
	client, err := tc.tumblrClient()
	if err != nil {
		return nil, err
	}
	responsePortal := msg.Portal
	var resp *tumblr.SendMessageResponse
	if pendingMeta != nil {
		var usedExisting bool
		if pendingMeta.PendingParticipantName != "" {
			var existing *tumblr.ConversationMessagesResponse
			existing, err = client.GetConversationByParticipants(ctx, meta.SelectedBlogName, pendingMeta.PendingParticipantName, 1)
			if err != nil && !tumblr.IsNotFound(err) {
				return nil, tc.handleRemoteError(err)
			}
			if existing != nil && existing.Conversation != nil && validRemoteID(existing.Conversation.ID) {
				conversationID = existing.Conversation.ID
				usedExisting = true
			}
		}
		if usedExisting {
			switch messageType {
			case tumblr.MessageTypeImage:
				resp, err = client.SendImage(ctx, meta.SelectedBlogName, conversationID, image)
			case tumblr.MessageTypePostRef:
				if post == nil {
					return nil, fmt.Errorf("post share payload is required")
				}
				resp, err = client.SendPostRef(ctx, meta.SelectedBlogUUID, conversationID, *post)
			default:
				resp, err = client.SendText(ctx, meta.SelectedBlogName, conversationID, text)
			}
			if err != nil {
				return nil, tc.handleRemoteError(err)
			}
		} else {
			switch messageType {
			case tumblr.MessageTypeImage:
				resp, err = client.SendImageToParticipants(ctx, meta.SelectedBlogUUID, pendingMeta.PendingParticipantIDs, image)
			case tumblr.MessageTypePostRef:
				if post == nil {
					return nil, fmt.Errorf("post share payload is required")
				}
				resp, err = client.SendPostRefToParticipants(ctx, meta.SelectedBlogUUID, pendingMeta.PendingParticipantIDs, *post)
			default:
				resp, err = client.SendTextToParticipants(ctx, meta.SelectedBlogUUID, pendingMeta.PendingParticipantIDs, text)
			}
			if err != nil {
				return nil, tc.handleRemoteError(err)
			}
			conversationID = resp.Conversation.ID
		}
		responsePortal = tc.finalizePendingDMPortal(ctx, msg.Portal, conversationID)
	} else {
		switch messageType {
		case tumblr.MessageTypeImage:
			resp, err = client.SendImage(ctx, meta.SelectedBlogName, conversationID, image)
		case tumblr.MessageTypePostRef:
			if post == nil {
				return nil, fmt.Errorf("post share payload is required")
			}
			resp, err = client.SendPostRef(ctx, meta.SelectedBlogUUID, conversationID, *post)
		default:
			resp, err = client.SendText(ctx, meta.SelectedBlogName, conversationID, text)
		}
		if err != nil {
			return nil, tc.handleRemoteError(err)
		}
	}
	messageID := matrixEventFallbackMessageID(string(msg.Event.ID))
	responseMessageType := messageType
	if resp != nil && resp.Message != nil {
		if validRemoteID(resp.Message.ID) {
			messageID = resp.Message.ID
			tc.markConversationMessageSeen(conversationID, resp.Message.ID)
		}
		if resp.Message.Type != "" {
			responseMessageType = msgconv.MessageMetadataType(resp.Message.Type)
		}
	}
	ts := time.Now()
	if msg.Event.Timestamp > 0 {
		ts = time.UnixMilli(msg.Event.Timestamp)
	}
	responsePortalKey := msg.Portal.PortalKey
	if responsePortal != nil && responsePortal.Portal != nil {
		responsePortalKey = responsePortal.PortalKey
	}
	return &bridgev2.MatrixMessageResponse{
		DB: &database.Message{
			ID:         tumblrid.MakeMessageID(messageID),
			Room:       responsePortalKey,
			SenderID:   tumblrid.MakeUserID(meta.SelectedBlogUUID),
			SenderMXID: msg.Event.Sender,
			Timestamp:  ts,
			Metadata:   &MessageMetadata{Type: responseMessageType},
		},
	}, nil
}

type matrixPostShare struct {
	BlogName string
	PostID   string
}

func parseTumblrPostShare(text string) (matrixPostShare, bool) {
	fields := strings.Fields(strings.TrimSpace(text))
	if len(fields) != 1 {
		return matrixPostShare{}, false
	}
	parsed, err := url.Parse(fields[0])
	if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") {
		return matrixPostShare{}, false
	}
	host := strings.ToLower(strings.TrimSpace(parsed.Hostname()))
	segments := pathSegments(parsed.EscapedPath())
	switch {
	case host == "www.tumblr.com" || host == "tumblr.com":
		return parseTumblrDotComPostPath(segments)
	case strings.HasSuffix(host, ".tumblr.com"):
		blogName := strings.TrimSuffix(host, ".tumblr.com")
		if len(segments) >= 2 && segments[0] == "post" && isTumblrPostID(segments[1]) {
			return matrixPostShare{BlogName: blogName, PostID: segments[1]}, true
		}
	}
	return matrixPostShare{}, false
}

func parseTumblrDotComPostPath(segments []string) (matrixPostShare, bool) {
	if len(segments) >= 4 && segments[0] == "blog" && segments[1] == "view" && isTumblrPostID(segments[3]) {
		return matrixPostShare{BlogName: segments[2], PostID: segments[3]}, true
	}
	if len(segments) >= 2 && isTumblrPostID(segments[1]) {
		return matrixPostShare{BlogName: segments[0], PostID: segments[1]}, true
	}
	if len(segments) >= 3 && segments[1] == "post" && isTumblrPostID(segments[2]) {
		return matrixPostShare{BlogName: segments[0], PostID: segments[2]}, true
	}
	return matrixPostShare{}, false
}

func pathSegments(escapedPath string) []string {
	rawSegments := strings.Split(escapedPath, "/")
	segments := make([]string, 0, len(rawSegments))
	for _, raw := range rawSegments {
		if raw == "" {
			continue
		}
		segment, err := url.PathUnescape(raw)
		if err != nil {
			return nil
		}
		segment = strings.TrimSpace(segment)
		if segment == "" {
			continue
		}
		segments = append(segments, segment)
	}
	return segments
}

func isTumblrPostID(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func (tc *TumblrClient) resolveTumblrPostShare(ctx context.Context, parsed matrixPostShare) (tumblr.PostShare, bool, error) {
	blogName := tumblr.NormalizeBlogName(parsed.BlogName)
	if blogName == "" || parsed.PostID == "" {
		return tumblr.PostShare{}, false, nil
	}
	if err := tc.requireLoggedIn(); err != nil {
		return tumblr.PostShare{}, false, err
	}
	client, err := tc.tumblrClient()
	if err != nil {
		return tumblr.PostShare{}, false, err
	}
	resp, err := client.GetBlogInfo(ctx, blogName)
	if err != nil {
		if tumblr.IsNotFound(err) {
			return tumblr.PostShare{}, false, nil
		}
		return tumblr.PostShare{}, false, tc.handleRemoteError(err)
	}
	if resp == nil || resp.Blog == nil || !validRemoteID(resp.Blog.UUID) {
		return tumblr.PostShare{}, false, fmt.Errorf("tumblr did not return the blog UUID needed to share this post")
	}
	return tumblr.PostShare{
		ID:   parsed.PostID,
		Blog: resp.Blog.UUID,
	}, true, nil
}

func pendingDMMetadataFromPortal(portal *bridgev2.Portal) *PortalMetadata {
	if portal == nil || portal.Portal == nil || !isPendingDMPortalID(string(portal.ID)) {
		return nil
	}
	meta, ok := portal.Metadata.(*PortalMetadata)
	if !ok || meta == nil || len(meta.PendingParticipantIDs) == 0 {
		return nil
	}
	return meta
}

func (tc *TumblrClient) finalizePendingDMPortal(ctx context.Context, portal *bridgev2.Portal, conversationID string) *bridgev2.Portal {
	if portal == nil || portal.Portal == nil || !validRemoteID(conversationID) {
		return portal
	}
	newKey := tc.portalKey(conversationID)
	newMeta := &PortalMetadata{ConversationID: conversationID}
	if tc != nil && tc.connector != nil && tc.connector.Bridge != nil {
		_, updatedPortal, err := tc.connector.Bridge.ReIDPortal(ctx, portal.PortalKey, newKey)
		if err != nil {
			if log := tc.log(); log != nil {
				log.Warn().Err(err).Str("conversation_id_hash", logIdentifierHash(conversationID)).Msg("Failed to re-ID pending Tumblr DM portal")
			}
			return portal
		}
		if updatedPortal != nil {
			updatedPortal.Metadata = newMeta
			if err := updatedPortal.Save(ctx); err != nil {
				if log := tc.log(); log != nil {
					log.Warn().Err(err).Str("conversation_id_hash", logIdentifierHash(conversationID)).Msg("Failed to save re-IDed Tumblr DM portal metadata")
				}
			}
			return updatedPortal
		}
	}
	portal.PortalKey = newKey
	portal.Metadata = newMeta
	return portal
}

func matrixEventFallbackMessageID(eventID string) string {
	candidate := "matrix-" + eventID
	if strings.TrimSpace(eventID) != "" && validRemoteID(candidate) {
		return candidate
	}
	sum := sha256.Sum256([]byte(eventID))
	return "matrix-event-" + hex.EncodeToString(sum[:])
}
