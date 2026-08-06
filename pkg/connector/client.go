package connector

import (
	"context"
	"errors"
	"fmt"
	"maps"
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
	"maunium.net/go/mautrix/id"

	"github.com/ifixrobots/tumblr-dms/pkg/connector/tumblrdb"
	"github.com/ifixrobots/tumblr-dms/pkg/tumblr"
	"github.com/ifixrobots/tumblr-dms/pkg/tumblrid"
)

type TumblrClient struct {
	connector          *TumblrConnector
	userLogin          *bridgev2.UserLogin
	client             *tumblr.Client
	loggedIn           atomic.Bool
	stateLock          sync.Mutex
	ownershipLock      sync.Mutex
	ownershipWG        sync.WaitGroup
	retired            bool
	saveOrderLock      sync.Mutex
	saveTail           chan struct{}
	generationLock     sync.Mutex
	generationID       uint64
	generation         *connectionGeneration
	generationChanging bool
	generationChanged  chan struct{}

	// loginMetadataLock guards every field reachable from userLogin.Metadata.
	loginMetadataLock    sync.Mutex
	pushRegistrationDown atomic.Bool
	inboundLock          sync.Mutex
	inboundProcessLock   sync.Mutex
	inboundGeneration    uint64
	inboundWake          chan struct{}
	outboundLock         sync.Mutex
	outboundGraphLock    sync.Mutex
	outboundProcessLock  sync.Mutex
	outboundGeneration   uint64
	outboundWake         chan struct{}
	outboundPending      map[networkid.TransactionID]*outboundPendingRegistration

	syncLock sync.Mutex
	seenLock sync.Mutex

	seenConversations          map[string]struct{}
	seenConversationModifiedTS map[string]int64
	seenMessages               map[string]struct{}
	seenMessageOrder           []string
}

var _ bridgev2.NetworkAPI = (*TumblrClient)(nil)

var (
	errTumblrEmptyText     = unsupportedMatrixMessageError(errors.New("message text is empty"))
	errTumblrTextTooLong   = unsupportedMatrixMessageError(errors.New("message is too long for tumblr dms"))
	errTumblrClientRetired = errors.New("tumblr client was replaced")
)

const tumblrSessionFinalFlushTimeout = 10 * time.Second

func unsupportedMatrixMessageError(err error) error {
	return bridgev2.WrapErrorInStatus(err).
		WithErrorAsMessage().
		WithIsCertain(true).
		WithSendNotice(true).
		WithErrorReason(event.MessageStatusUnsupported)
}

func NewTumblrClient(login *bridgev2.UserLogin, connector *TumblrConnector, client *tumblr.Client) *TumblrClient {
	ready := make(chan struct{})
	close(ready)
	return &TumblrClient{
		connector:                  connector,
		userLogin:                  login,
		client:                     client,
		saveTail:                   ready,
		seenConversations:          make(map[string]struct{}),
		seenConversationModifiedTS: make(map[string]int64),
		seenMessages:               make(map[string]struct{}),
		outboundPending:            make(map[networkid.TransactionID]*outboundPendingRegistration),
	}
}

func (tc *TumblrClient) tumblrClient() (*tumblr.Client, error) {
	if tc == nil || tc.client == nil {
		return nil, fmt.Errorf("tumblr client is not available")
	}
	return tc.client, nil
}

func (tc *TumblrClient) sendBridgeState(state status.BridgeState) {
	if !tc.beginOwnedOperation() {
		return
	}
	defer tc.endOwnedOperation()
	if tc.userLogin == nil || tc.userLogin.BridgeState == nil {
		return
	}
	tc.userLogin.BridgeState.Send(state)
}

func (tc *TumblrClient) failConnect(state status.BridgeState) {
	if tc == nil || !tc.isActiveClient() {
		return
	}
	tc.generationLock.Lock()
	generation := tc.generation
	tc.generationLock.Unlock()
	if generation != nil {
		tc.failGeneration(generation, state)
		return
	}
	tc.setLoggedIn(false)
	tc.sendBridgeState(state)
}

func (tc *TumblrClient) setLoggedIn(loggedIn bool) {
	tc.stateLock.Lock()
	defer tc.stateLock.Unlock()
	tc.loggedIn.Store(loggedIn)
}

func (tc *TumblrClient) handleRemoteError(err error) error {
	if tumblr.IsAuthError(err) {
		if log := tc.log(); log != nil {
			log.Debug().Err(err).Msg("Tumblr session is no longer valid")
		}
		tc.failConnect(tumblrBadCredentialsState(err))
	}
	return err
}

func tumblrBadCredentialsState(err error) status.BridgeState {
	return tumblrBadCredentialsStateWithCode("tumblr-bad-credentials", err)
}

func tumblrBadCredentialsStateWithCode(errorCode status.BridgeStateErrorCode, _ error) status.BridgeState {
	message := "Tumblr couldn't verify the saved sign-in. Please sign in again to reconnect Tumblr DMs."
	if errorCode == "tumblr-invalid-login-metadata" {
		message = "Tumblr sign-in information is incomplete. Please sign in again to reconnect Tumblr DMs."
	}
	return status.BridgeState{
		StateEvent: status.StateBadCredentials,
		Error:      errorCode,
		Message:    message,
		UserAction: status.UserActionRelogin,
	}
}

func (tc *TumblrClient) saveUserLogin(ctx context.Context) error {
	if tc == nil || tc.userLogin == nil || tc.userLogin.UserLogin == nil ||
		tc.userLogin.Bridge == nil || tc.userLogin.Bridge.DB == nil || tc.userLogin.Bridge.DB.UserLogin == nil {
		return nil
	}
	if !tc.beginOwnedOperation() {
		return errTumblrClientRetired
	}
	defer tc.endOwnedOperation()
	return tc.saveOwnedUserLogin(ctx)
}

func (tc *TumblrClient) saveOwnedUserLogin(ctx context.Context) error {
	tc.saveOrderLock.Lock()
	previous := tc.saveTail
	done := make(chan struct{})
	tc.saveTail = done
	tc.saveOrderLock.Unlock()
	if previous != nil {
		select {
		case <-previous:
		case <-ctx.Done():
			// This save owns its place in the serialization chain even though
			// its caller stopped waiting. Keep later saves behind the predecessor
			// so an older database update can never finish after newer metadata.
			go func() {
				<-previous
				close(done)
			}()
			return ctx.Err()
		}
	}
	defer close(done)
	if err := ctx.Err(); err != nil {
		return err
	}

	tc.loginMetadataLock.Lock()
	snapshot := *tc.userLogin.UserLogin
	if meta, ok := tc.userLogin.Metadata.(*UserLoginMetadata); ok && meta != nil {
		snapshot.Metadata = meta.clone()
	}
	tc.loginMetadataLock.Unlock()
	return tc.userLogin.Bridge.DB.UserLogin.Update(ctx, &snapshot)
}

func (tc *TumblrClient) loginMetadataSnapshot() (*UserLoginMetadata, error) {
	if !tc.beginOwnedOperation() {
		return nil, errTumblrClientRetired
	}
	defer tc.endOwnedOperation()
	tc.loginMetadataLock.Lock()
	defer tc.loginMetadataLock.Unlock()
	if tc.userLogin == nil {
		return nil, fmt.Errorf("tumblr login metadata is missing")
	}
	meta, ok := tc.userLogin.Metadata.(*UserLoginMetadata)
	if !ok || meta == nil {
		return nil, fmt.Errorf("tumblr login metadata is missing")
	}
	return meta.clone(), nil
}

func (tc *TumblrClient) startSessionUpdateLoop(generation *connectionGeneration, client *tumblr.Client) {
	if tc == nil || generation == nil || client == nil || !tc.isCurrentGeneration(generation) {
		return
	}
	generation.wg.Add(1)
	go tc.sessionUpdateLoop(generation, client)
}

func (tc *TumblrClient) sessionUpdateLoop(generation *connectionGeneration, client *tumblr.Client) {
	defer generation.wg.Done()
	ctx := generation.ctx
	defer tc.flushFinalSessionSnapshot(ctx, client)
	pending := false
	for {
		if !pending {
			select {
			case <-ctx.Done():
				return
			case <-client.SessionUpdates():
				pending = true
			}
		}
		if err := tc.persistSessionSnapshot(ctx, client.SessionSnapshot()); err == nil {
			pending = false
			continue
		} else if log := tc.log(); log != nil {
			log.Warn().Err(err).Msg("Failed to save refreshed Tumblr session; retrying")
		}
		retryTimer := time.NewTimer(5 * time.Second)
		select {
		case <-ctx.Done():
			retryTimer.Stop()
			return
		case <-retryTimer.C:
		}
	}
}

func (tc *TumblrClient) flushFinalSessionSnapshot(generationCtx context.Context, client *tumblr.Client) {
	if tc == nil || client == nil {
		return
	}
	flushCtx, cancel := context.WithTimeout(
		context.WithoutCancel(generationCtx),
		tumblrSessionFinalFlushTimeout,
	)
	defer cancel()
	if err := tc.persistSessionSnapshot(flushCtx, client.SessionSnapshot()); err != nil &&
		!errors.Is(err, errTumblrClientRetired) {
		if log := tc.log(); log != nil {
			log.Warn().Err(err).Msg("Failed to save final Tumblr session snapshot")
		}
	}
}

func (tc *TumblrClient) persistSessionSnapshot(ctx context.Context, snapshot tumblr.SessionSnapshot) error {
	if tc == nil {
		return errTumblrClientRetired
	}
	if !tc.beginOwnedOperation() {
		return errTumblrClientRetired
	}
	defer tc.endOwnedOperation()
	return tc.persistOwnedSessionSnapshot(ctx, snapshot)
}

func (tc *TumblrClient) persistOwnedSessionSnapshot(ctx context.Context, snapshot tumblr.SessionSnapshot) error {
	if !tumblr.HasSessionCookies(snapshot.Cookies) {
		return fmt.Errorf("refreshed Tumblr session did not include session cookies")
	}
	tc.loginMetadataLock.Lock()
	meta, err := tc.validatedLoginMetadataLocked()
	if err != nil {
		tc.loginMetadataLock.Unlock()
		return err
	}
	cookies := tumblr.NormalizeSessionCookies(snapshot.Cookies)
	if sessionSnapshotMatchesMetadata(snapshot, cookies, meta) {
		tc.loginMetadataLock.Unlock()
		return nil
	}
	previousCookies := meta.SessionCookies
	previousAPIToken := meta.APIToken
	previousCSRFToken := meta.CSRFToken
	previousAPIVersion := meta.APIVersion
	meta.SessionCookies = cookies
	meta.APIToken = snapshot.APIToken
	meta.CSRFToken = snapshot.CSRFToken
	meta.APIVersion = snapshot.APIVersion
	tc.loginMetadataLock.Unlock()
	if err = tc.saveOwnedUserLogin(ctx); err != nil {
		tc.loginMetadataLock.Lock()
		if sessionSnapshotMatchesMetadata(snapshot, cookies, meta) {
			meta.SessionCookies = previousCookies
			meta.APIToken = previousAPIToken
			meta.CSRFToken = previousCSRFToken
			meta.APIVersion = previousAPIVersion
		}
		tc.loginMetadataLock.Unlock()
		return err
	}
	return nil
}

func sessionSnapshotMatchesMetadata(snapshot tumblr.SessionSnapshot, cookies map[string]string, meta *UserLoginMetadata) bool {
	if meta == nil || meta.APIToken != snapshot.APIToken ||
		meta.CSRFToken != snapshot.CSRFToken || meta.APIVersion != snapshot.APIVersion {
		return false
	}
	return maps.Equal(meta.SessionCookies, cookies)
}

func (tc *TumblrClient) queueRemoteEvent(evt bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
	if !tc.beginOwnedOperation() {
		return bridgev2.EventHandlingResultFailed.WithError(fmt.Errorf("tumblr remote event cannot be queued"))
	}
	defer tc.endOwnedOperation()
	if tc.userLogin == nil || tc.userLogin.Bridge == nil || evt == nil {
		return bridgev2.EventHandlingResultFailed.WithError(fmt.Errorf("tumblr remote event cannot be queued"))
	}
	return tc.userLogin.QueueRemoteEvent(evt)
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
	meta, err := normalizeUserLoginMetadata(login.Metadata)
	if err != nil {
		// Keep incomplete metadata loaded so Connect can publish a clear
		// BAD_CREDENTIALS/relogin state instead of making the account disappear.
		meta, _ = login.Metadata.(*UserLoginMetadata)
		if meta == nil {
			meta = &UserLoginMetadata{}
			login.Metadata = meta
		}
		bestEffortNormalizeUserLoginMetadata(meta)
		login.Log.Warn().Err(err).Msg("Loaded incomplete Tumblr login metadata for recoverable reauthentication")
	}
	client := tumblr.NewClient(tumblr.Options{
		SessionCookies: meta.SessionCookies,
		APIToken:       meta.APIToken,
		CSRFToken:      meta.CSRFToken,
		APIVersion:     meta.APIVersion,
		UserAgent:      tc.Config.BrowserUserAgent(),
		HTTPClient:     tc.newHTTPClient(),
	})
	login.Client = NewTumblrClient(login, tc, client)
	return nil
}

func bestEffortNormalizeUserLoginMetadata(meta *UserLoginMetadata) {
	if meta == nil {
		return
	}
	meta.SessionCookies = tumblr.NormalizeSessionCookies(meta.SessionCookies)
	meta.APIToken = normalizeBearerToken(meta.APIToken)
	meta.CSRFToken = normalizeOptionalHeaderCredential(meta.CSRFToken)
	meta.APIVersion = normalizeOptionalHeaderCredential(meta.APIVersion)
	meta.UserName = normalizeOptionalMetadataBlogName(meta.UserName)
	meta.SelectedBlogName = normalizeOptionalMetadataBlogName(meta.SelectedBlogName)
	meta.SelectedBlogUUID = strings.TrimSpace(meta.SelectedBlogUUID)
}

func (tc *TumblrClient) Connect(_ context.Context) {
	if !tc.beginOwnedOperation() {
		return
	}
	defer tc.endOwnedOperation()
	generation := tc.replaceConnectionGeneration()
	if !tc.isCurrentGeneration(generation) {
		generation.cancel()
		generation.wg.Done()
		return
	}
	tc.setLoggedIn(false)
	tc.sendBridgeState(status.BridgeState{StateEvent: status.StateConnecting})
	tc.pushRegistrationDown.Store(false)
	go tc.runConnectionGeneration(generation)
}

func (tc *TumblrClient) runConnectionGeneration(generation *connectionGeneration) {
	defer generation.wg.Done()
	ctx := generation.ctx
	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		tc.failGeneration(generation, tumblrBadCredentialsStateWithCode("tumblr-invalid-login-metadata", err))
		return
	}
	client, err := tc.tumblrClient()
	if err != nil {
		tc.failGeneration(generation, status.BridgeState{
			StateEvent: status.StateUnknownError,
			Error:      "tumblr-client-unavailable",
			Message:    err.Error(),
		})
		return
	}
	if err := client.Bootstrap(ctx); err != nil {
		if log := tc.log(); log != nil {
			log.Warn().Err(err).Msg("Failed to validate saved Tumblr session")
		}
		tc.failGeneration(generation, connectBootstrapFailureState(err))
		return
	}
	userInfo, err := client.CurrentUser(ctx)
	if err != nil {
		if tumblr.IsAuthError(err) {
			if log := tc.log(); log != nil {
				log.Debug().Err(err).Msg("Tumblr session validation failed")
			}
			tc.failGeneration(generation, tumblrBadCredentialsState(err))
			return
		}
		if log := tc.log(); log != nil {
			log.Warn().Err(err).Msg("Failed to load Tumblr account")
		}
		message := "Tumblr couldn't be reached to finish connecting. Please try again."
		errorCode := status.BridgeStateErrorCode("tumblr-current-user-failed")
		if tumblr.IsForbidden(err) {
			message = "Tumblr did not allow Beeper to load this account. Please try again later."
			errorCode = "tumblr-forbidden"
		}
		tc.failGeneration(generation, status.BridgeState{
			StateEvent: status.StateUnknownError,
			Error:      errorCode,
			Message:    message,
		})
		return
	}
	blog, err := selectedBlogFromCurrentUser(userInfo, meta)
	if err != nil {
		if log := tc.log(); log != nil {
			log.Debug().Err(err).Msg("Saved Tumblr blog is not available in the signed-in account")
		}
		blogName := strings.TrimPrefix(meta.SelectedBlogName, "@")
		tc.failGeneration(generation, status.BridgeState{
			StateEvent: status.StateBadCredentials,
			Error:      "tumblr-selected-blog-unavailable",
			Message:    fmt.Sprintf("Sign in to the Tumblr account that owns @%s to reconnect it.", blogName),
			UserAction: status.UserActionRelogin,
		})
		return
	}
	if !tc.beginOwnedOperation() {
		return
	}
	defer tc.endOwnedOperation()
	if !tc.isCurrentGeneration(generation) {
		return
	}
	tc.setLoggedIn(true)
	if err = tc.persistOwnedSessionSnapshot(ctx, client.SessionSnapshot()); err != nil {
		if log := tc.log(); log != nil {
			log.Warn().Err(err).Msg("Failed to save refreshed Tumblr session")
		}
	}
	tc.loginMetadataLock.Lock()
	liveMeta, metadataErr := tc.validatedLoginMetadataLocked()
	if metadataErr == nil {
		liveMeta.UserName = userName(userInfo, blog)
		liveMeta.SelectedBlogName = blog.Name
		liveMeta.SelectedBlogUUID = blog.UUID
	}
	if metadataErr == nil && tc.userLogin != nil && tc.userLogin.UserLogin != nil {
		tc.userLogin.RemoteName = blog.Name
		tc.userLogin.RemoteProfile = status.RemoteProfile{
			Username: blog.Name,
			Name:     displayName(tc.connector, blog),
		}
	}
	tc.loginMetadataLock.Unlock()
	if metadataErr != nil {
		tc.failGeneration(generation, tumblrBadCredentialsStateWithCode("tumblr-invalid-login-metadata", metadataErr))
		return
	}
	if err := tc.saveOwnedUserLogin(ctx); err != nil {
		if log := tc.log(); log != nil {
			log.Warn().Err(err).Msg("Failed to save refreshed Tumblr account")
		}
	}
	tc.ensureInboundSyncStarted(generation)
	tc.startSessionUpdateLoop(generation, client)
	tc.startPushSupervisor(generation)
	tc.startOutboundSync(generation)
}

func connectBootstrapFailureState(err error) status.BridgeState {
	if tumblr.IsAuthError(err) {
		return tumblrBadCredentialsState(err)
	}
	return status.BridgeState{
		StateEvent: status.StateUnknownError,
		Error:      "tumblr-bootstrap-failed",
		Message:    "Tumblr couldn't be reached to verify the saved sign-in. Please try again.",
	}
}

func (tc *TumblrClient) Disconnect() {
	if tc == nil {
		return
	}
	tc.stopConnectionGeneration()
	tc.setLoggedIn(false)
}

func (tc *TumblrClient) IsLoggedIn() bool {
	if tc == nil || !tc.isActiveClient() {
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

type tumblrBackgroundSyncContextKey struct{}

func (tc *TumblrClient) requireLoggedInForContext(ctx context.Context) error {
	if tc.IsLoggedIn() {
		return nil
	}
	if allowed, _ := ctx.Value(tumblrBackgroundSyncContextKey{}).(bool); allowed {
		return nil
	}
	return bridgev2.ErrNotLoggedIn
}

func (tc *TumblrClient) LogoutRemote(ctx context.Context) {
	if !tc.beginOwnedOperation() {
		return
	}
	defer tc.endOwnedOperation()
	tc.stopConnectionGeneration()
	tc.setLoggedIn(false)
	tc.unregisterTumblrWebPush(ctx)
}

func (tc *TumblrClient) IsThisUser(_ context.Context, userID networkid.UserID) bool {
	if !tc.beginOwnedOperation() {
		return false
	}
	defer tc.endOwnedOperation()
	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		return false
	}
	parsed := tumblrid.ParseUserID(userID)
	return parsed == meta.SelectedBlogUUID || parsed == meta.SelectedBlogName || parsed == string(tc.userLogin.ID)
}

func (tc *TumblrClient) GetChatInfo(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	if !tc.beginOwnedOperation() {
		return nil, bridgev2.ErrNotLoggedIn
	}
	defer tc.endOwnedOperation()
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
	if !tc.beginOwnedOperation() {
		return nil, bridgev2.ErrNotLoggedIn
	}
	defer tc.endOwnedOperation()
	if msg == nil || msg.Content == nil {
		return nil, fmt.Errorf("message content is required")
	}
	if msg.Event == nil {
		return nil, fmt.Errorf("matrix event is required to send a tumblr message")
	}
	if strings.TrimSpace(string(msg.Event.ID)) == "" {
		return nil, fmt.Errorf("matrix event id is required to send a tumblr message")
	}
	completed, err := tc.completedOutboundReplay(ctx, msg)
	if err != nil {
		return nil, err
	}
	if completed != nil {
		return completed, nil
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
	return content != nil && content.GetCapMsgType() == event.CapMsgGIF
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
	image, err := tc.downloadMatrixImage(ctx, msg)
	if err != nil {
		return nil, err
	}
	return tc.sendMatrixMessageToTumblr(ctx, msg, tumblr.MessageTypeImage, "", image, nil)
}

func (tc *TumblrClient) sendMatrixMessageToTumblr(ctx context.Context, msg *bridgev2.MatrixMessage, messageType, text string, image tumblr.ImageUpload, post *tumblr.PostShare) (*bridgev2.MatrixMessageResponse, error) {
	if msg.Portal == nil || msg.Portal.Portal == nil {
		return nil, fmt.Errorf("matrix room is required to send a Tumblr message")
	}
	lockCtx, releaseSubmission, err := tc.acquireOutboundSubmissionLock(ctx)
	if err != nil {
		return nil, err
	}
	defer releaseSubmission()
	ctx = lockCtx
	// Conversation sync owns mutable portal metadata under syncLock, while all
	// outbound binding mutations are under outboundGraphLock. Snapshot both the
	// room identity and pending participants while holding those locks in the
	// established order, then never read the mutable portal metadata again.
	tc.syncLock.Lock()
	tc.outboundGraphLock.Lock()
	if msg.Portal.MXID == "" {
		tc.outboundGraphLock.Unlock()
		tc.syncLock.Unlock()
		return nil, fmt.Errorf("matrix room is required to send a Tumblr message")
	}
	if msg.Event.RoomID == "" || msg.Event.RoomID != msg.Portal.MXID {
		tc.outboundGraphLock.Unlock()
		tc.syncLock.Unlock()
		return nil, fmt.Errorf("matrix event room does not match the Tumblr portal room")
	}
	matrixRoomID := msg.Event.RoomID
	portalKey := msg.Portal.PortalKey
	pendingMeta := pendingDMMetadataFromPortal(msg.Portal)
	conversationID := ""
	var portalSnapshotErr error
	if pendingMeta == nil {
		conversationID, portalSnapshotErr = conversationIDFromPortal(msg.Portal, "portal is required to send a Tumblr message")
	}
	tc.syncLock.Unlock()
	defer tc.outboundGraphLock.Unlock()
	if portalSnapshotErr != nil {
		return nil, portalSnapshotErr
	}
	completed, err := tc.completedOutboundMapping(ctx, msg.Event.ID, matrixRoomID)
	if err != nil {
		return nil, err
	}
	if completed != nil {
		return &bridgev2.MatrixMessageResponse{DB: completed}, nil
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
	if pendingMeta != nil {
		if pendingMeta.PendingParticipantName != "" {
			var existing *tumblr.ConversationMessagesResponse
			existing, err = client.GetConversationByParticipants(ctx, meta.SelectedBlogName, pendingMeta.PendingParticipantName, 1)
			if err != nil && !tumblr.IsNotFound(err) {
				return nil, tc.handleRemoteError(err)
			}
			if existing != nil && existing.Conversation != nil && validRemoteID(existing.Conversation.ID) &&
				equalParticipantIDs(pendingMeta.PendingParticipantIDs, conversationParticipantIDs(*existing.Conversation)) {
				conversationID = existing.Conversation.ID
			}
		}
	}
	dbMessageType, err := outboundMessageType(messageType)
	if err != nil {
		return nil, err
	}
	contentHash, err := outboundContentHash(messageType, text, image, post)
	if err != nil {
		return nil, err
	}
	sourceMediaDigest := ""
	if messageType == tumblr.MessageTypeImage {
		sourceMediaDigest = tumblr.ImageSourceDigest(image.Data)
	}
	storedConversationID := conversationID
	if pendingMeta != nil {
		storedConversationID = ""
	}
	sendStartedAt := time.Now()
	send := &tumblrdb.OutboundSend{
		UserLoginID:        tc.userLogin.ID,
		TransactionID:      outboundTransactionID(tc.userLogin.ID, string(msg.Event.ID)),
		PortalKey:          portalKey,
		ConversationID:     storedConversationID,
		MatrixRoomID:       matrixRoomID,
		MatrixEventID:      msg.Event.ID,
		MatrixSenderID:     msg.Event.Sender,
		SenderID:           tumblrid.MakeUserID(meta.SelectedBlogUUID),
		InputTransactionID: msg.InputTransactionID,
		MessageType:        dbMessageType,
		ContentHash:        contentHash,
		SourceMediaDigest:  sourceMediaDigest,
		NextAttemptAt:      sendStartedAt.Add(outboundUnconfirmedTimeout),
		SendStartedAt:      sendStartedAt,
	}
	persisted, created, err := tc.connector.DB.Outbound.LoadOrCreate(ctx, send)
	if errors.Is(err, tumblrdb.ErrOutboundSendAlreadyHandled) {
		return &bridgev2.MatrixMessageResponse{Pending: true}, nil
	}
	if errors.Is(err, tumblrdb.ErrOutboundSendAlreadyCompleted) {
		completed, completedErr := tc.completedOutboundMapping(ctx, msg.Event.ID, matrixRoomID)
		if completedErr != nil {
			return nil, completedErr
		}
		if completed == nil {
			return nil, fmt.Errorf("completed Tumblr outbound mapping is unavailable")
		}
		return &bridgev2.MatrixMessageResponse{DB: completed}, nil
	}
	if err != nil {
		return nil, err
	}
	if !created && persisted.State == tumblrdb.OutboundSendCompleted {
		completed, completedErr := tc.completedOutboundMapping(ctx, msg.Event.ID, matrixRoomID)
		if completedErr != nil {
			return nil, completedErr
		}
		if completed == nil || completed.ID != persisted.RemoteMessageID {
			return nil, fmt.Errorf("completed Tumblr outbound tombstone does not match its message mapping")
		}
		return &bridgev2.MatrixMessageResponse{DB: completed}, nil
	}
	if !created && persisted.IsTerminal() {
		tc.dropOutboundPending(persisted.TransactionID, true)
		tc.wakeOutboundSync()
		return &bridgev2.MatrixMessageResponse{Pending: true}, nil
	}
	if !created && persisted.State != tumblrdb.OutboundSendPrepared {
		tc.wakeOutboundSync()
		return &bridgev2.MatrixMessageResponse{DB: outboundDatabaseMessage(persisted, nil), Pending: true}, nil
	}
	if pendingMeta != nil || persisted.BindingConversationID != "" ||
		(persisted.ConversationID == "" && conversationID != "") {
		switch {
		case persisted.ConversationID != "":
			if conversationID != "" && conversationID != persisted.ConversationID {
				return tc.deletePreparedOutbound(ctx, persisted, tumblrdb.ErrOutboundSendBindingConflict)
			}
			conversationID = persisted.ConversationID
		case persisted.BindingConversationID != "":
			if conversationID != "" && conversationID != persisted.BindingConversationID {
				return tc.deletePreparedOutbound(ctx, persisted, tumblrdb.ErrOutboundSendBindingConflict)
			}
			conversationID = persisted.BindingConversationID
		}
		if conversationID != "" {
			var collision bool
			_, collision, err = tc.prepareAndBindOutboundPendingPortal(ctx, persisted, conversationID)
			if err != nil {
				return tc.deletePreparedOutbound(ctx, persisted, err)
			}
			if collision {
				//lint:ignore ST1005 This exact sentence is shown to the customer.
				collisionErr := bridgev2.WrapErrorInStatus(errors.New("This chat was merged into an existing Tumblr DM. Send the message again in the replacement chat.")).
					WithStatus(event.MessageStatusFail).
					WithIsCertain(true).
					WithSendNotice(true).
					WithErrorAsMessage()
				return tc.deletePreparedOutbound(ctx, persisted, collisionErr)
			}
		}
	}
	// Another bridge process may have advanced this transaction while this
	// process waited for the crash-released submission fence.
	persisted, err = tc.connector.DB.Outbound.Get(ctx, persisted.UserLoginID, persisted.TransactionID)
	if err != nil {
		return nil, err
	}
	if persisted == nil || persisted.State != tumblrdb.OutboundSendPrepared {
		return tc.pendingAfterPreparedOwnershipLoss(ctx, send)
	}

	baseline, err := tc.outboundBaseline(ctx, client, meta.SelectedBlogName, conversationID)
	if err != nil {
		return tc.deletePreparedOutbound(ctx, persisted, err)
	}
	submitStartedAt := time.Now()
	submitDeadline := submitStartedAt.Add(outboundUnconfirmedTimeout)
	claimed, err := tc.connector.DB.Outbound.MarkSubmitting(
		ctx,
		persisted.UserLoginID,
		persisted.TransactionID,
		baseline,
		submitStartedAt,
		submitDeadline,
	)
	if err != nil {
		return tc.deletePreparedOutbound(ctx, persisted, err)
	}
	if !claimed {
		return tc.pendingAfterPreparedOwnershipLoss(ctx, persisted)
	}
	persisted.State = tumblrdb.OutboundSendSubmitting
	persisted.BaselineMessageIDs = append([]networkid.MessageID(nil), baseline...)
	persisted.SendStartedAt = submitStartedAt
	persisted.NextAttemptAt = submitDeadline
	dbMessage := tc.registerOutboundPending(msg, persisted)

	var resp *tumblr.SendMessageResponse
	if conversationID == "" {
		switch messageType {
		case tumblr.MessageTypeImage:
			resp, err = client.SendImageToParticipants(ctx, meta.SelectedBlogUUID, pendingMeta.PendingParticipantIDs, image)
		case tumblr.MessageTypePostRef:
			resp, err = client.SendPostRefToParticipants(ctx, meta.SelectedBlogUUID, pendingMeta.PendingParticipantIDs, *post)
		default:
			resp, err = client.SendTextToParticipants(ctx, meta.SelectedBlogUUID, pendingMeta.PendingParticipantIDs, text)
		}
	} else {
		switch messageType {
		case tumblr.MessageTypeImage:
			resp, err = client.SendImage(ctx, meta.SelectedBlogName, conversationID, image)
		case tumblr.MessageTypePostRef:
			resp, err = client.SendPostRef(ctx, meta.SelectedBlogUUID, conversationID, *post)
		default:
			resp, err = client.SendText(ctx, meta.SelectedBlogName, conversationID, text)
		}
	}
	if err != nil {
		if tumblr.IsMessageSendOutcomeUnknown(err) {
			return tc.keepOutboundPendingAfterSubmit(ctx, persisted, dbMessage, err)
		}
		rejectedErr := tc.handleRemoteError(err)
		terminalAt := time.Now()
		nextAttemptAt := terminalAt.Add(time.Millisecond)
		claimed, markErr := tc.connector.DB.Outbound.MarkSubmittingNotSubmitted(
			ctx,
			persisted.UserLoginID,
			persisted.TransactionID,
			terminalAt,
			nextAttemptAt,
		)
		if markErr != nil {
			if log := tc.log(); log != nil {
				log.Warn().Err(errors.Join(rejectedErr, markErr)).Msg("Failed to save definite Tumblr message rejection")
			}
			return tc.pendingAfterPreparedOwnershipLoss(ctx, persisted)
		}
		if !claimed {
			return tc.pendingAfterPreparedOwnershipLoss(ctx, persisted)
		}
		persisted.State = tumblrdb.OutboundSendNotSubmitted
		persisted.TerminalAt = terminalAt
		persisted.NextAttemptAt = nextAttemptAt
		if finishErr := tc.finishOutboundNotSubmitted(ctx, persisted); finishErr != nil {
			if log := tc.log(); log != nil {
				log.Warn().Err(errors.Join(rejectedErr, finishErr)).Msg("Tumblr message rejection will finish asynchronously")
			}
		}
		tc.wakeOutboundSync()
		return &bridgev2.MatrixMessageResponse{Pending: true}, nil
	}

	if persisted.ConversationID == "" {
		if pendingMeta == nil || resp == nil || resp.Conversation == nil || !validRemoteID(resp.Conversation.ID) ||
			!equalParticipantIDs(pendingMeta.PendingParticipantIDs, conversationParticipantIDs(*resp.Conversation)) {
			return tc.keepOutboundPendingAfterSubmit(
				ctx,
				persisted,
				dbMessage,
				fmt.Errorf("tumblr send response did not identify the expected conversation participants"),
			)
		}
		conversationID = resp.Conversation.ID
		var collision bool
		_, collision, err = tc.prepareAndBindOutboundPendingPortal(ctx, persisted, conversationID)
		if err != nil {
			return tc.keepOutboundPendingAfterSubmit(ctx, persisted, dbMessage, err)
		}
		if collision {
			return tc.keepOutboundVisibleAfterRoomReplacement(ctx, persisted)
		}
		portalKey = persisted.PortalKey
		dbMessage.Room = portalKey
	}
	if err = tc.connector.DB.Outbound.MarkAwaitingEcho(
		ctx,
		persisted.UserLoginID,
		persisted.TransactionID,
		time.Now().Add(outboundUnconfirmedTimeout),
	); err != nil {
		return tc.keepOutboundPendingAfterSubmit(ctx, persisted, dbMessage, err)
	}
	tc.wakeOutboundSync()
	return &bridgev2.MatrixMessageResponse{
		DB:      dbMessage,
		Pending: true,
	}, nil
}

func (tc *TumblrClient) completedOutboundMapping(
	ctx context.Context,
	eventID id.EventID,
	roomID id.RoomID,
) (*database.Message, error) {
	if tc == nil || tc.connector == nil || tc.connector.Bridge == nil || tc.connector.Bridge.DB == nil {
		return nil, fmt.Errorf("tumblr bridge database is unavailable")
	}
	existing, err := tc.connector.Bridge.DB.Message.GetPartByMXID(ctx, eventID)
	if err != nil || existing == nil {
		return existing, err
	}
	owner, err := tc.connector.Bridge.DB.Portal.GetByMXID(ctx, roomID)
	if err != nil {
		return nil, fmt.Errorf("load persisted Tumblr room owner for completed message: %w", err)
	}
	if owner == nil || owner.PortalKey != existing.Room || existing.MXID != eventID || existing.ID == "" {
		return nil, fmt.Errorf("matrix event is already mapped to a different Tumblr room or message")
	}
	return existing, nil
}

func (tc *TumblrClient) completedOutboundReplay(
	ctx context.Context,
	msg *bridgev2.MatrixMessage,
) (*bridgev2.MatrixMessageResponse, error) {
	tc.syncLock.Lock()
	tc.outboundGraphLock.Lock()
	if msg.Portal == nil || msg.Portal.Portal == nil || msg.Portal.MXID == "" {
		tc.outboundGraphLock.Unlock()
		tc.syncLock.Unlock()
		return nil, fmt.Errorf("matrix room is required to send a Tumblr message")
	}
	if msg.Event.RoomID == "" || msg.Event.RoomID != msg.Portal.MXID {
		tc.outboundGraphLock.Unlock()
		tc.syncLock.Unlock()
		return nil, fmt.Errorf("matrix event room does not match the Tumblr portal room")
	}
	matrixRoomID := msg.Portal.MXID
	tc.syncLock.Unlock()
	completed, err := tc.completedOutboundMapping(ctx, msg.Event.ID, matrixRoomID)
	tc.outboundGraphLock.Unlock()
	if err != nil || completed == nil {
		return nil, err
	}
	return &bridgev2.MatrixMessageResponse{DB: completed}, nil
}

func (tc *TumblrClient) deletePreparedOutbound(
	ctx context.Context,
	send *tumblrdb.OutboundSend,
	cause error,
) (*bridgev2.MatrixMessageResponse, error) {
	if send == nil {
		return nil, cause
	}
	deleted, err := tc.connector.DB.Outbound.DeletePrepared(ctx, send.UserLoginID, send.TransactionID)
	if err != nil {
		return nil, errors.Join(cause, fmt.Errorf("delete unsent Tumblr message state: %w", err))
	}
	if !deleted {
		return tc.pendingAfterPreparedOwnershipLoss(ctx, send)
	}
	tc.dropOutboundPending(send.TransactionID, true)
	return nil, cause
}

func (tc *TumblrClient) pendingAfterPreparedOwnershipLoss(
	ctx context.Context,
	send *tumblrdb.OutboundSend,
) (*bridgev2.MatrixMessageResponse, error) {
	current, err := tc.connector.DB.Outbound.Get(ctx, send.UserLoginID, send.TransactionID)
	if err != nil {
		return nil, err
	}
	if current == nil || current.State == tumblrdb.OutboundSendCompleted {
		completed, completedErr := tc.completedOutboundMapping(ctx, send.MatrixEventID, send.MatrixRoomID)
		if completedErr != nil {
			return nil, completedErr
		}
		if completed != nil {
			if current != nil && completed.ID != current.RemoteMessageID {
				return nil, fmt.Errorf("completed Tumblr outbound tombstone does not match its message mapping")
			}
			return &bridgev2.MatrixMessageResponse{DB: completed}, nil
		}
	}
	if current == nil || current.IsTerminal() {
		tc.dropOutboundPending(send.TransactionID, true)
		tc.wakeOutboundSync()
		return &bridgev2.MatrixMessageResponse{Pending: true}, nil
	}
	tc.wakeOutboundSync()
	return &bridgev2.MatrixMessageResponse{DB: outboundDatabaseMessage(current, nil), Pending: true}, nil
}

func (tc *TumblrClient) prepareAndBindOutboundPendingPortal(
	ctx context.Context,
	send *tumblrdb.OutboundSend,
	conversationID string,
) (*bridgev2.Portal, bool, error) {
	if send == nil {
		return nil, false, fmt.Errorf("tumblr outbound send is required")
	}
	if !validRemoteID(conversationID) {
		return nil, false, fmt.Errorf("tumblr conversation ID is invalid")
	}
	if send.ConversationID != "" && send.ConversationID != conversationID {
		return nil, false, tumblrdb.ErrOutboundSendBindingConflict
	}
	if send.ConversationID != "" && send.PortalKey != tc.portalKey(conversationID) {
		return nil, false, tumblrdb.ErrOutboundSendBindingConflict
	}
	bindingDeadline := time.Now().Add(outboundUnconfirmedTimeout)
	if send.ConversationID == "" {
		if send.BindingConversationID != "" && send.BindingConversationID != conversationID {
			return nil, false, tumblrdb.ErrOutboundSendBindingConflict
		}
		if err := tc.connector.DB.Outbound.PrepareConversationBinding(
			ctx,
			send.UserLoginID,
			send.TransactionID,
			conversationID,
			bindingDeadline,
		); err != nil {
			return nil, false, err
		}
		send.BindingConversationID = conversationID
		send.NextAttemptAt = bindingDeadline
	}
	portal, sourceRoomReplaced, err := tc.bindOutboundPortalOwnership(
		ctx,
		send,
		tc.portalKey(conversationID),
		outboundUnconfirmedTimeout,
	)
	if err != nil {
		return nil, false, err
	}
	if sourceRoomReplaced {
		return portal, true, nil
	}
	if portal == nil || portal.MXID != send.MatrixRoomID {
		return nil, false, fmt.Errorf("tumblr target portal is unavailable after binding an outbound send")
	}
	return portal, false, nil
}

func (tc *TumblrClient) keepOutboundPendingAfterSubmit(
	ctx context.Context,
	send *tumblrdb.OutboundSend,
	dbMessage *database.Message,
	cause error,
) (*bridgev2.MatrixMessageResponse, error) {
	markErr := tc.connector.DB.Outbound.MarkUncertain(
		ctx,
		send.UserLoginID,
		send.TransactionID,
		time.Now().Add(outboundUnconfirmedTimeout),
	)
	if log := tc.log(); log != nil {
		log.Warn().Err(errors.Join(cause, markErr)).Msg("Tumblr message outcome needs durable local reconciliation")
	}
	tc.wakeOutboundSync()
	return &bridgev2.MatrixMessageResponse{DB: dbMessage, Pending: true}, nil
}

func (tc *TumblrClient) keepOutboundVisibleAfterRoomReplacement(
	ctx context.Context,
	send *tumblrdb.OutboundSend,
) (*bridgev2.MatrixMessageResponse, error) {
	tc.dropOutboundPending(send.TransactionID, true)
	if parkErr := tc.parkOutboundUnconfirmed(ctx, send, time.Now()); parkErr != nil {
		if log := tc.log(); log != nil {
			log.Warn().Err(parkErr).Msg("Failed to terminalize a Tumblr send after its source room was replaced")
		}
		tc.wakeInboundSync()
		tc.wakeOutboundSync()
		return &bridgev2.MatrixMessageResponse{Pending: true}, nil
	}
	if notifyErr := tc.finishOutboundUnconfirmed(ctx, send); notifyErr != nil {
		if log := tc.log(); log != nil {
			log.Warn().Err(notifyErr).Msg("Tumblr room-replacement status will finish asynchronously")
		}
	}
	tc.wakeInboundSync()
	tc.wakeOutboundSync()
	return &bridgev2.MatrixMessageResponse{Pending: true}, nil
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
	snapshot := *meta
	snapshot.PendingParticipantIDs = append([]string(nil), meta.PendingParticipantIDs...)
	return &snapshot
}
