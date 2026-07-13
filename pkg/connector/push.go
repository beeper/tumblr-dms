package connector

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"git.min.rip/min/webpush-client-go/rfc8291"
	pushreceiver "github.com/beeper/push-receiver"
	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/status"

	"github.com/ifixrobots/tumblr-dms/pkg/tumblr"
)

const tumblrWebPushVAPIDKey = "BBDh-66UWJ5mfoWvDjX5hRaKUYcwykutJHf4-f4oonC44K7wkRPtHi-BsLW7wPPMNLnju7fWMjpfwiOsZlU1LE0"

const (
	tumblrPushSubscriptionTTL        = 7 * 24 * time.Hour
	tumblrPushCheckinInterval        = 24 * time.Hour
	tumblrPushMaintenanceInterval    = time.Hour
	tumblrPushRequestTimeout         = 30 * time.Second
	maxStoredTumblrPushPersistentIDs = 32
)

var (
	_ bridgev2.PushableNetworkAPI          = (*TumblrClient)(nil)
	_ bridgev2.BackgroundSyncingNetworkAPI = (*TumblrClient)(nil)
)

var tumblrPushConfig = &bridgev2.PushConfig{
	Web: &bridgev2.WebPushConfig{VapidKey: tumblrWebPushVAPIDKey},
}

type webPushPayload struct {
	Bridge          string          `json:"bridge"`
	AccountID       string          `json:"account_id"`
	RegID           string          `json:"reg_id"`
	Data            json.RawMessage `json:"data"`
	CryptoKey       string          `json:"crypto-key"`
	Encryption      string          `json:"encryption"`
	ContentEncoding string          `json:"content-encoding"`
}

func (tc *TumblrClient) GetPushConfigs() *bridgev2.PushConfig {
	return tumblrPushConfig
}

func (tc *TumblrClient) RegisterPushNotifications(ctx context.Context, pushType bridgev2.PushType, token string) error {
	tc.pushRegistrationLock.Lock()
	defer tc.pushRegistrationLock.Unlock()
	return tc.registerPushNotificationsLocked(ctx, pushType, token)
}

func (tc *TumblrClient) registerPushNotificationsLocked(ctx context.Context, pushType bridgev2.PushType, token string) error {
	if pushType != bridgev2.PushTypeWeb {
		return fmt.Errorf("unsupported push type: %s", pushType)
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return fmt.Errorf("tumblr web push endpoint is empty")
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
	tc.pushMetadataLock.Lock()
	keysChanged, err := meta.ensurePushKeys()
	if err != nil {
		tc.pushMetadataLock.Unlock()
		return err
	}
	p256dh, auth, err := meta.encodedPushKeys()
	if err != nil {
		tc.pushMetadataLock.Unlock()
		return err
	}
	if keysChanged {
		if err = tc.saveUserLogin(ctx); err != nil {
			tc.pushMetadataLock.Unlock()
			return fmt.Errorf("failed to save Tumblr web push keys: %w", err)
		}
	}
	tc.pushMetadataLock.Unlock()
	registration := tumblr.NewWebPushDeviceRegistration(token, tumblr.WebPushKeys{
		P256DH: p256dh,
		Auth:   auth,
	})
	if err = client.RegisterWebPushDevice(ctx, registration); err != nil {
		return fmt.Errorf("failed to register Tumblr web push device: %w", err)
	}
	tc.pushMetadataLock.Lock()
	defer tc.pushMetadataLock.Unlock()
	if meta.PushKeys.Token != token {
		meta.PushKeys.Token = token
		if err = tc.saveUserLogin(ctx); err != nil {
			return fmt.Errorf("failed to save Tumblr web push keys: %w", err)
		}
	}
	if log := tc.log(); log != nil {
		log.Info().
			Str("endpoint_hash", logIdentifierHash(token)).
			Int("endpoint_len", len(token)).
			Bool("has_p256dh", p256dh != "").
			Bool("has_auth", auth != "").
			Msg("Registered Tumblr web push device")
	}
	return nil
}

func (tc *TumblrClient) ConnectBackground(ctx context.Context, params *bridgev2.ConnectBackgroundParams) error {
	if params == nil || len(params.RawData) == 0 {
		return fmt.Errorf("tumblr push payload is empty")
	}
	wasLoggedIn := tc.loggedIn.Load()
	if !wasLoggedIn {
		tc.setLoggedIn(true)
		defer tc.setLoggedIn(false)
	}
	return tc.handleWebPushPayload(ctx, params.RawData)
}

func (tc *TumblrClient) prepareSelfHostedPushReceiver(ctx context.Context) (*pushreceiver.GCMCredentials, error) {
	if err := tc.requireLoggedIn(); err != nil {
		return nil, err
	}
	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		return nil, err
	}
	tc.pushMetadataLock.Lock()
	keysChanged, err := meta.ensurePushKeys()
	if err != nil {
		tc.pushMetadataLock.Unlock()
		return nil, err
	}
	if keysChanged {
		if err = tc.saveUserLogin(ctx); err != nil {
			tc.pushMetadataLock.Unlock()
			return nil, fmt.Errorf("failed to save Tumblr web push keys: %w", err)
		}
	}
	tc.pushMetadataLock.Unlock()
	return tc.ensureSelfHostedPushRegistration(ctx, meta)
}

func (tc *TumblrClient) ensureSelfHostedPushRegistration(ctx context.Context, meta *UserLoginMetadata) (*pushreceiver.GCMCredentials, error) {
	tc.pushRegistrationLock.Lock()
	defer tc.pushRegistrationLock.Unlock()

	creds, err := tc.ensurePushReceiverCredentials(ctx, meta)
	if err != nil {
		return nil, err
	}
	endpoint, registeredCreds, renewed, err := tc.registerPushReceiverEndpoint(ctx, meta, creds)
	if registeredCreds != nil {
		creds = registeredCreds
	}
	if err != nil {
		return creds, err
	}
	if err = tc.registerPushNotificationsLocked(ctx, bridgev2.PushTypeWeb, endpoint); err != nil {
		return creds, err
	}
	if renewed {
		if err = tc.markPushReceiverRegistrationComplete(ctx, meta); err != nil {
			return creds, err
		}
	}
	return creds, nil
}

func (tc *TumblrClient) ensurePushReceiverCredentials(ctx context.Context, meta *UserLoginMetadata) (*pushreceiver.GCMCredentials, error) {
	tc.pushMetadataLock.Lock()
	creds, err := meta.pushReceiverCredentials()
	tc.pushMetadataLock.Unlock()
	if err != nil {
		return nil, err
	} else if creds != nil {
		return creds, nil
	}
	requestCtx, cancel := context.WithTimeout(ctx, tumblrPushRequestTimeout)
	defer cancel()
	creds, err = pushreceiver.CheckIn(requestCtx, &pushreceiver.GCMCredentials{})
	if err != nil {
		return nil, fmt.Errorf("failed to check in Tumblr web push receiver: %w", err)
	}
	tc.pushMetadataLock.Lock()
	defer tc.pushMetadataLock.Unlock()
	if existing, existingErr := meta.pushReceiverCredentials(); existingErr != nil {
		return nil, existingErr
	} else if existing != nil {
		return existing, nil
	}
	meta.setPushReceiverCredentials(creds)
	meta.PushKeys.LastCheckinTS = time.Now().UnixMilli()
	if err = tc.saveUserLogin(ctx); err != nil {
		return nil, fmt.Errorf("failed to save Tumblr web push receiver credentials: %w", err)
	}
	return creds, nil
}

func (tc *TumblrClient) registerPushReceiverEndpoint(ctx context.Context, meta *UserLoginMetadata, creds *pushreceiver.GCMCredentials) (string, *pushreceiver.GCMCredentials, bool, error) {
	tc.pushMetadataLock.Lock()
	if meta == nil || meta.PushKeys == nil {
		tc.pushMetadataLock.Unlock()
		return "", nil, false, fmt.Errorf("tumblr web push keys are missing")
	}
	if cachedEndpoint := strings.TrimSpace(meta.PushKeys.Token); cachedEndpoint != "" &&
		strings.TrimSpace(meta.PushKeys.FCMAppID) != "" &&
		creds != nil &&
		!tumblrPushReceiverRegistrationDue(meta.PushKeys) {
		if log := tc.log(); log != nil {
			log.Info().
				Str("endpoint_hash", logIdentifierHash(cachedEndpoint)).
				Int("endpoint_len", len(cachedEndpoint)).
				Str("fcm_app_id_hash", logIdentifierHash(meta.PushKeys.FCMAppID)).
				Int("fcm_app_id_len", len(meta.PushKeys.FCMAppID)).
				Msg("Using cached Tumblr push receiver endpoint")
		}
		tc.pushMetadataLock.Unlock()
		return cachedEndpoint, creds, false, nil
	}

	existingAppID := meta.PushKeys.FCMAppID
	opts := &pushreceiver.GCMRegistrationOpts{
		Expiry:     tumblrPushSubscriptionTTL,
		InstanceID: string(tc.userLogin.ID),
	}
	if existingAppID != "" {
		opts.AppID = existingAppID
	}
	tc.pushMetadataLock.Unlock()

	var fcmCreds *pushreceiver.FCMCredentials
	var err error
	requestCtx, cancel := context.WithTimeout(ctx, tumblrPushRequestTimeout)
	defer cancel()
	for attempt := 0; attempt < 5; attempt++ {
		fcmCreds, err = pushreceiver.RegisterGCM(requestCtx, tumblrWebPushVAPIDKey, *creds, opts)
		if err != nil {
			return "", nil, false, fmt.Errorf("failed to register Tumblr web push receiver with FCM: %w", err)
		} else if strings.TrimSpace(fcmCreds.Token) != "" {
			break
		}
		if log := tc.log(); log != nil {
			log.Warn().Msg("FCM returned an empty Tumblr web push token, retrying")
		}
		select {
		case <-requestCtx.Done():
			return "", nil, false, requestCtx.Err()
		case <-time.After(time.Second):
		}
	}
	if fcmCreds == nil || strings.TrimSpace(fcmCreds.Token) == "" {
		return "", nil, false, fmt.Errorf("FCM returned an empty Tumblr web push token")
	}

	endpoint := "https://fcm.googleapis.com/fcm/send/" + fcmCreds.Token
	if log := tc.log(); log != nil {
		log.Info().
			Str("endpoint_hash", logIdentifierHash(endpoint)).
			Int("endpoint_len", len(endpoint)).
			Str("fcm_app_id_hash", logIdentifierHash(fcmCreds.AppID)).
			Int("fcm_app_id_len", len(fcmCreds.AppID)).
			Bool("reused_fcm_app_id", existingAppID == fcmCreds.AppID).
			Msg("Registered Tumblr push receiver endpoint with FCM")
	}
	tc.pushMetadataLock.Lock()
	defer tc.pushMetadataLock.Unlock()
	if meta.PushKeys == nil {
		return "", nil, false, fmt.Errorf("tumblr web push keys are missing")
	}
	changed := false
	if meta.PushKeys.Token != endpoint {
		meta.PushKeys.Token = endpoint
		changed = true
	}
	if meta.PushKeys.FCMAppID != fcmCreds.AppID {
		meta.PushKeys.FCMAppID = fcmCreds.AppID
		changed = true
	}
	if meta.PushKeys.LastCheckinTS == 0 {
		meta.PushKeys.LastCheckinTS = time.Now().UnixMilli()
		changed = true
	}
	if changed {
		if err = tc.saveUserLogin(ctx); err != nil {
			return "", nil, false, fmt.Errorf("failed to save Tumblr web push receiver endpoint: %w", err)
		}
	}
	return endpoint, &fcmCreds.GCM, true, nil
}

func (tc *TumblrClient) markPushReceiverRegistrationComplete(ctx context.Context, meta *UserLoginMetadata) error {
	tc.pushMetadataLock.Lock()
	defer tc.pushMetadataLock.Unlock()

	if meta == nil || meta.PushKeys == nil {
		return fmt.Errorf("tumblr web push keys are missing")
	}
	meta.PushKeys.FCMRegisteredTS = time.Now().UnixMilli()
	if err := tc.saveUserLogin(ctx); err != nil {
		return fmt.Errorf("failed to save Tumblr web push receiver registration time: %w", err)
	}
	return nil
}

func tumblrPushReceiverRegistrationDue(keys *PushKeys) bool {
	if keys == nil || keys.FCMRegisteredTS <= 0 {
		return true
	}
	registeredAt := time.UnixMilli(keys.FCMRegisteredTS)
	return !time.Now().Before(registeredAt.Add(tumblrPushSubscriptionTTL / 2))
}

func (tc *TumblrClient) startPushReceiver(ctx context.Context, creds pushreceiver.GCMCredentials) error {
	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		return err
	}
	tc.pushMetadataLock.Lock()
	persistentIDs := []string(nil)
	if meta.PushKeys != nil {
		persistentIDs = append(persistentIDs, meta.PushKeys.PersistentIDs...)
	}
	tc.pushMetadataLock.Unlock()

	tc.pushLock.Lock()
	if tc.pushCancel != nil {
		tc.pushLock.Unlock()
		return nil
	}
	receiverCtx := context.Background()
	if tc.connector != nil && tc.connector.Bridge != nil && tc.connector.Bridge.BackgroundCtx != nil {
		receiverCtx = tc.connector.Bridge.BackgroundCtx
	}
	if log := tc.log(); log != nil {
		logger := log.With().Str("component", "tumblr_push_receiver").Logger().Level(zerolog.InfoLevel)
		receiverCtx = logger.WithContext(receiverCtx)
	}
	receiverCtx, cancel := context.WithCancel(receiverCtx)
	tc.pushCancel = cancel
	tc.pushWG.Add(2)
	tc.pushLock.Unlock()

	client := pushreceiver.New(
		pushreceiver.WithCreds(&creds),
		pushreceiver.WithHeartbeat(
			pushreceiver.WithServerInterval(time.Minute),
			pushreceiver.WithClientInterval(2*time.Minute),
			pushreceiver.WithAdaptive(true),
		),
		pushreceiver.WithReceivedPersistentID(persistentIDs),
		pushreceiver.WithMaxUnackedIDs(10),
	)

	go tc.pushListenLoop(receiverCtx, client)
	go tc.pushMaintenanceLoop(receiverCtx)
	return nil
}

func (tc *TumblrClient) stopPushReceiver() {
	tc.pushLock.Lock()
	defer tc.pushLock.Unlock()
	cancel := tc.pushCancel
	if cancel == nil {
		return
	}
	cancel()
	tc.pushWG.Wait()
	tc.pushCancel = nil
}

func (tc *TumblrClient) pushListenLoop(ctx context.Context, client *pushreceiver.MCSClient) {
	defer tc.pushWG.Done()
	if log := tc.log(); log != nil {
		log.Info().Msg("Started Tumblr web push listen loop")
		defer log.Info().Msg("Stopped Tumblr web push listen loop")
	}
	go client.Listen(ctx)
	for event := range client.Events {
		switch ev := event.(type) {
		case *pushreceiver.ConnectedEvent:
			if log := tc.log(); log != nil {
				log.Info().Msg("Tumblr web push receiver connected")
			}
			if err := tc.clearPushPersistentIDs(ctx); err != nil {
				if log := tc.log(); log != nil {
					log.Warn().Err(err).Msg("Failed to clear Tumblr web push persistent IDs")
				}
			}
		case *pushreceiver.StreamAck:
			if log := tc.log(); log != nil {
				log.Debug().Msg("Tumblr web push receiver stream ack")
			}
			if err := tc.clearPushPersistentIDs(ctx); err != nil {
				if log := tc.log(); log != nil {
					log.Warn().Err(err).Msg("Failed to clear Tumblr web push persistent IDs")
				}
			}
		case *pushreceiver.MessageEvent:
			if log := tc.log(); log != nil {
				log.Info().
					Str("persistent_id_hash", logIdentifierHash(ev.PersistentID)).
					Str("app_id_hash", logIdentifierHash(ev.AppID)).
					Int("app_id_len", len(ev.AppID)).
					Int("raw_data_len", len(ev.RawData)).
					Int("app_data_count", len(ev.AppData)).
					Msg("Received Tumblr web push message event")
			}
			if err := tc.handlePushReceiverMessage(ctx, *ev); err != nil {
				if log := tc.log(); log != nil {
					log.Warn().Err(err).Str("persistent_id_hash", logIdentifierHash(ev.PersistentID)).Msg("Failed to handle Tumblr web push message")
				}
			}
			if err := tc.savePushPersistentID(ctx, ev.PersistentID); err != nil {
				if log := tc.log(); log != nil {
					log.Warn().Err(err).Str("persistent_id_hash", logIdentifierHash(ev.PersistentID)).Msg("Failed to save Tumblr web push persistent ID")
				}
			}
		case *pushreceiver.UnauthorizedError:
			if log := tc.log(); log != nil {
				log.Warn().Err(ev.ErrorObj).Msg("Tumblr web push receiver unauthorized")
			}
		case *pushreceiver.HeartbeatError:
			if log := tc.log(); log != nil {
				log.Warn().Err(ev.ErrorObj).Msg("Tumblr web push receiver heartbeat failed")
			}
		case *pushreceiver.RetryEvent:
			if log := tc.log(); log != nil {
				log.Warn().Err(ev.ErrorObj).Dur("retry_after", ev.RetryAfter).Msg("Tumblr web push receiver retrying")
			}
		}
	}
}

func (tc *TumblrClient) pushMaintenanceLoop(ctx context.Context) {
	defer tc.pushWG.Done()
	ticker := time.NewTicker(tumblrPushMaintenanceInterval)
	defer ticker.Stop()
	registrationTimer := time.NewTimer(tc.nextPushReceiverRegistrationDelay())
	defer registrationTimer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := tc.refreshPushReceiverCheckin(ctx); err != nil {
				if log := tc.log(); log != nil {
					log.Warn().Err(err).Msg("Failed to refresh Tumblr web push receiver checkin")
				}
			}
		case <-registrationTimer.C:
			nextDelay := tumblrPushMaintenanceInterval
			if err := tc.refreshPushReceiverRegistration(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				if tc.pushReceiverRegistrationExpired() {
					tc.markPushRegistrationUnavailable(err)
				}
				if log := tc.log(); log != nil {
					log.Warn().Err(err).Msg("Failed to renew Tumblr web push receiver registration")
				}
			} else {
				nextDelay = tc.nextPushReceiverRegistrationDelay()
			}
			registrationTimer.Reset(nextDelay)
		}
	}
}

func (tc *TumblrClient) nextPushReceiverRegistrationDelay() time.Duration {
	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		return 0
	}
	tc.pushMetadataLock.Lock()
	defer tc.pushMetadataLock.Unlock()
	if meta.PushKeys == nil || meta.PushKeys.FCMRegisteredTS <= 0 {
		return 0
	}
	renewAt := time.UnixMilli(meta.PushKeys.FCMRegisteredTS).Add(tumblrPushSubscriptionTTL / 2)
	if delay := time.Until(renewAt); delay > 0 {
		return delay
	}
	return 0
}

func (tc *TumblrClient) refreshPushReceiverRegistration(ctx context.Context) error {
	if err := tc.requireLoggedIn(); err != nil {
		return err
	}
	tc.pushRegistrationLock.Lock()
	defer tc.pushRegistrationLock.Unlock()

	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		return err
	}
	tc.pushMetadataLock.Lock()
	registrationDue := tumblrPushReceiverRegistrationDue(meta.PushKeys)
	tc.pushMetadataLock.Unlock()
	if !registrationDue {
		tc.markPushRegistrationRecovered()
		return nil
	}
	creds, err := tc.ensurePushReceiverCredentials(ctx, meta)
	if err != nil {
		return err
	}
	endpoint, _, renewed, err := tc.registerPushReceiverEndpoint(ctx, meta, creds)
	if err != nil {
		return err
	} else if !renewed {
		return nil
	}
	if err = tc.registerPushNotificationsLocked(ctx, bridgev2.PushTypeWeb, endpoint); err != nil {
		return tc.handleRemoteError(err)
	}
	if err = tc.markPushReceiverRegistrationComplete(ctx, meta); err != nil {
		return err
	}
	if log := tc.log(); log != nil {
		log.Info().Msg("Renewed Tumblr web push receiver registration")
	}
	tc.markPushRegistrationRecovered()
	return nil
}

func (tc *TumblrClient) markPushRegistrationRecovered() {
	tc.stateLock.Lock()
	defer tc.stateLock.Unlock()
	if !tc.loggedIn.Load() {
		return
	}
	if tc.pushRegistrationDown.Swap(false) {
		tc.sendBridgeState(status.BridgeState{StateEvent: status.StateConnected})
	}
}

func (tc *TumblrClient) markPushRegistrationUnavailable(err error) {
	tc.stateLock.Lock()
	defer tc.stateLock.Unlock()
	if !tc.loggedIn.Load() {
		return
	}
	if tc.pushRegistrationDown.CompareAndSwap(false, true) {
		tc.sendBridgeState(tumblrPushUnavailableState(err))
	}
}

func (tc *TumblrClient) pushReceiverRegistrationExpired() bool {
	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		return true
	}
	tc.pushMetadataLock.Lock()
	defer tc.pushMetadataLock.Unlock()
	if meta.PushKeys == nil || meta.PushKeys.FCMRegisteredTS <= 0 {
		return true
	}
	expiresAt := time.UnixMilli(meta.PushKeys.FCMRegisteredTS).Add(tumblrPushSubscriptionTTL)
	return !time.Now().Before(expiresAt)
}

func (tc *TumblrClient) refreshPushReceiverCheckin(ctx context.Context) error {
	tc.pushMetadataLock.Lock()
	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		tc.pushMetadataLock.Unlock()
		return err
	} else if meta.PushKeys == nil || meta.PushKeys.LastCheckinTS == 0 {
		tc.pushMetadataLock.Unlock()
		return nil
	}
	lastCheckinTS := meta.PushKeys.LastCheckinTS
	lastCheckin := time.UnixMilli(lastCheckinTS)
	if time.Since(lastCheckin) <= tumblrPushCheckinInterval {
		tc.pushMetadataLock.Unlock()
		return nil
	}
	creds, err := meta.pushReceiverCredentials()
	tc.pushMetadataLock.Unlock()
	if err != nil {
		return err
	} else if creds == nil {
		return nil
	}
	requestCtx, cancel := context.WithTimeout(ctx, tumblrPushRequestTimeout)
	defer cancel()
	updated, err := pushreceiver.CheckIn(requestCtx, creds)
	if err != nil {
		return fmt.Errorf("failed to check in Tumblr web push receiver: %w", err)
	}
	tc.pushMetadataLock.Lock()
	defer tc.pushMetadataLock.Unlock()
	if meta.PushKeys == nil {
		return nil
	} else if meta.PushKeys.LastCheckinTS > lastCheckinTS {
		return nil
	}
	meta.setPushReceiverCredentials(updated)
	meta.PushKeys.LastCheckinTS = time.Now().UnixMilli()
	return tc.saveUserLogin(ctx)
}

func (tc *TumblrClient) handlePushReceiverMessage(ctx context.Context, event pushreceiver.MessageEvent) error {
	if strings.TrimSpace(event.AppID) == "" || len(event.RawData) == 0 {
		if log := tc.log(); log != nil {
			log.Warn().
				Bool("has_app_id", strings.TrimSpace(event.AppID) != "").
				Int("raw_data_len", len(event.RawData)).
				Msg("Ignoring Tumblr web push message with missing app ID or data")
		}
		return nil
	}
	data, err := json.Marshal(base64.StdEncoding.EncodeToString(event.RawData))
	if err != nil {
		return err
	}
	payload := webPushPayload{
		RegID:           event.AppID,
		Data:            data,
		CryptoKey:       pushAppDataValue(event, "crypto-key"),
		Encryption:      pushAppDataValue(event, "encryption"),
		ContentEncoding: pushAppDataValue(event, "content-encoding"),
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return tc.handleWebPushPayload(ctx, payloadBytes)
}

func pushAppDataValue(event pushreceiver.MessageEvent, key string) string {
	for _, item := range event.AppData {
		if strings.EqualFold(strings.TrimSpace(item.GetKey()), key) {
			return item.GetValue()
		}
	}
	return ""
}

func (tc *TumblrClient) savePushPersistentID(ctx context.Context, persistentID string) error {
	persistentID = strings.TrimSpace(persistentID)
	if persistentID == "" {
		return nil
	}
	tc.pushMetadataLock.Lock()
	defer tc.pushMetadataLock.Unlock()

	meta, err := tc.validatedLoginMetadata()
	if err != nil || meta.PushKeys == nil {
		return err
	}
	for _, existing := range meta.PushKeys.PersistentIDs {
		if existing == persistentID {
			return nil
		}
	}
	meta.PushKeys.PersistentIDs = append(meta.PushKeys.PersistentIDs, persistentID)
	if overflow := len(meta.PushKeys.PersistentIDs) - maxStoredTumblrPushPersistentIDs; overflow > 0 {
		meta.PushKeys.PersistentIDs = meta.PushKeys.PersistentIDs[overflow:]
	}
	return tc.saveUserLogin(ctx)
}

func (tc *TumblrClient) clearPushPersistentIDs(ctx context.Context) error {
	tc.pushMetadataLock.Lock()
	defer tc.pushMetadataLock.Unlock()

	meta, err := tc.validatedLoginMetadata()
	if err != nil || meta.PushKeys == nil || len(meta.PushKeys.PersistentIDs) == 0 {
		return err
	}
	meta.PushKeys.PersistentIDs = nil
	return tc.saveUserLogin(ctx)
}

func (tc *TumblrClient) unregisterTumblrWebPush(ctx context.Context) {
	tc.pushRegistrationLock.Lock()
	defer tc.pushRegistrationLock.Unlock()
	tc.pushMetadataLock.Lock()
	defer tc.pushMetadataLock.Unlock()

	meta, err := tc.validatedLoginMetadata()
	if err != nil || meta.PushKeys == nil || strings.TrimSpace(meta.PushKeys.Token) == "" {
		return
	}
	client, err := tc.tumblrClient()
	if err != nil {
		return
	}
	p256dh, auth, err := meta.encodedPushKeys()
	if err != nil {
		return
	}
	registration := tumblr.NewWebPushDeviceRegistration(meta.PushKeys.Token, tumblr.WebPushKeys{
		P256DH: p256dh,
		Auth:   auth,
	})
	if err = client.UnregisterWebPushDevice(ctx, registration); err != nil {
		if log := tc.log(); log != nil {
			log.Warn().Err(err).Msg("Failed to unregister Tumblr web push device")
		}
		return
	}
	meta.PushKeys.Token = ""
	meta.PushKeys.FCMAppID = ""
	if err = tc.saveUserLogin(ctx); err != nil {
		if log := tc.log(); log != nil {
			log.Warn().Err(err).Msg("Failed to save Tumblr metadata after unregistering web push")
		}
	}
}

func (tc *TumblrClient) handleWebPushPayload(ctx context.Context, data json.RawMessage) error {
	conversationID, err := tc.conversationIDFromPushPayload(data)
	if err != nil {
		return err
	}
	if log := tc.log(); log != nil {
		log.Info().Str("conversation_id_hash", logIdentifierHash(conversationID)).Msg("Handling Tumblr push for conversation")
	}
	return tc.syncConversationByID(ctx, conversationID)
}

func (tc *TumblrClient) conversationIDFromPushPayload(data json.RawMessage) (string, error) {
	payload, err := tc.decodePushPayload(data)
	if err != nil {
		return "", err
	}
	value, err := decodePushJSON(payload)
	if err != nil {
		return "", err
	}
	conversationID := findConversationID(value)
	if !validRemoteID(conversationID) {
		return "", fmt.Errorf("tumblr push payload did not include a valid conversation ID")
	}
	return conversationID, nil
}

func (tc *TumblrClient) decodePushPayload(data json.RawMessage) ([]byte, error) {
	data = normalizePushJSON(data)
	if len(data) == 0 {
		return nil, fmt.Errorf("tumblr push payload is empty")
	}
	var payload webPushPayload
	if err := json.Unmarshal(data, &payload); err == nil && len(payload.Data) > 0 {
		return tc.decodeWebPushEnvelope(payload)
	}
	return data, nil
}

func (tc *TumblrClient) decodeWebPushEnvelope(envelope webPushPayload) ([]byte, error) {
	payload, err := webPushDataBytes(envelope.Data)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(envelope.ContentEncoding) == "" && strings.TrimSpace(envelope.Encryption) == "" && strings.TrimSpace(envelope.CryptoKey) == "" {
		return normalizePushJSON(payload), nil
	}
	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		return nil, err
	}
	tc.pushMetadataLock.Lock()
	privateKey, err := meta.pushPrivateKey()
	if err != nil {
		tc.pushMetadataLock.Unlock()
		return nil, err
	}
	if meta.PushKeys == nil || len(meta.PushKeys.Auth) != 16 {
		tc.pushMetadataLock.Unlock()
		return nil, fmt.Errorf("tumblr web push auth secret is missing or invalid")
	}
	authSecret := append([]byte(nil), meta.PushKeys.Auth...)
	tc.pushMetadataLock.Unlock()
	encoding := strings.TrimSpace(envelope.ContentEncoding)
	if encoding == "" {
		encoding = string(rfc8291.EncodingAes128gcm)
	}
	decrypted, err := rfc8291.NewRFC8291(nil).Decrypt(
		payload,
		rfc8291.Encoding(encoding),
		envelope.Encryption,
		envelope.CryptoKey,
		authSecret,
		privateKey,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt Tumblr web push payload: %w", err)
	}
	return normalizePushJSON(decrypted), nil
}

func webPushDataBytes(data json.RawMessage) ([]byte, error) {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		return nil, fmt.Errorf("tumblr web push data is empty")
	}
	if data[0] == '"' {
		var encoded string
		if err := json.Unmarshal(data, &encoded); err != nil {
			return nil, fmt.Errorf("failed to parse Tumblr web push data string: %w", err)
		}
		if decoded, err := base64.StdEncoding.DecodeString(encoded); err == nil {
			return decoded, nil
		}
		if decoded, err := base64.RawStdEncoding.DecodeString(encoded); err == nil {
			return decoded, nil
		}
		if decoded, err := base64.RawURLEncoding.DecodeString(encoded); err == nil {
			return decoded, nil
		}
		return []byte(encoded), nil
	}
	return data, nil
}

func normalizePushJSON(data []byte) []byte {
	data = bytes.TrimSpace(data)
	for len(data) > 0 {
		last := data[len(data)-1]
		if last != 0 && last != 1 && last != 2 {
			break
		}
		data = bytes.TrimSpace(data[:len(data)-1])
	}
	return data
}

func decodePushJSON(data []byte) (any, error) {
	data = normalizePushJSON(data)
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("failed to parse Tumblr push JSON: %w", err)
	}
	return value, nil
}

func findConversationID(value any) string {
	switch typed := value.(type) {
	case map[string]any:
		for key, val := range typed {
			if isConversationIDKey(key) {
				if id := stringifyConversationID(val); id != "" {
					return id
				}
			}
		}
		for _, val := range typed {
			if id := findConversationID(val); id != "" {
				return id
			}
		}
	case []any:
		for _, val := range typed {
			if id := findConversationID(val); id != "" {
				return id
			}
		}
	}
	return ""
}

func isConversationIDKey(key string) bool {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "conversation_id", "conversationid", "conversation":
		return true
	default:
		return false
	}
}

func stringifyConversationID(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case json.Number:
		return strings.TrimSpace(typed.String())
	case float64:
		if typed == float64(int64(typed)) {
			return strconv.FormatInt(int64(typed), 10)
		}
	case int64:
		return strconv.FormatInt(typed, 10)
	case int:
		return strconv.Itoa(typed)
	}
	return ""
}
