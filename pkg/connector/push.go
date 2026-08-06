package connector

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"git.min.rip/min/webpush-client-go/rfc8291"
	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/status"

	pushreceiver "github.com/ifixrobots/tumblr-dms/internal/pushreceiver"
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
	if !tc.beginOwnedOperation() {
		return bridgev2.ErrNotLoggedIn
	}
	defer tc.endOwnedOperation()
	return tc.registerTumblrPushEndpoint(ctx, pushType, token)
}

func (tc *TumblrClient) registerTumblrPushEndpoint(ctx context.Context, pushType bridgev2.PushType, token string) error {
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
	client, err := tc.tumblrClient()
	if err != nil {
		return err
	}
	if err = tc.ensurePushKeyMaterial(ctx); err != nil {
		return err
	}
	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		return err
	}
	p256dh, auth, err := meta.encodedPushKeys()
	if err != nil {
		return err
	}
	registration := tumblr.NewWebPushDeviceRegistration(token, tumblr.WebPushKeys{
		P256DH: p256dh,
		Auth:   auth,
	})
	if err = client.RegisterWebPushDevice(ctx, registration); err != nil {
		return fmt.Errorf("failed to register Tumblr web push device: %w", err)
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

func (tc *TumblrClient) ConnectBackground(ctx context.Context, params *bridgev2.ConnectBackgroundParams) (err error) {
	if !tc.beginOwnedOperation() {
		return bridgev2.ErrNotLoggedIn
	}
	defer tc.endOwnedOperation()
	client, err := tc.tumblrClient()
	if err != nil {
		return err
	}
	defer func() {
		persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), tumblrPushRequestTimeout)
		defer cancel()
		if persistErr := tc.persistOwnedSessionSnapshot(persistCtx, client.SessionSnapshot()); persistErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to save refreshed Tumblr session after background sync: %w", persistErr))
		}
	}()
	if params == nil || len(params.RawData) == 0 {
		return fmt.Errorf("tumblr push payload is empty")
	}
	ctx = context.WithValue(ctx, tumblrBackgroundSyncContextKey{}, true)
	if err = tc.handleWebPushPayload(ctx, params.RawData); err != nil {
		return err
	}
	return tc.processDueConversationJobs(ctx, 0)
}

type preparedPushRegistration struct {
	registration *PushRegistration
	pending      bool
}

type pushReceiverOutcome int

const (
	pushReceiverStopped pushReceiverOutcome = iota
	pushReceiverRetry
	pushReceiverRotate
)

func (tc *TumblrClient) ensurePushKeyMaterial(ctx context.Context) error {
	tc.loginMetadataLock.Lock()
	meta, err := tc.validatedLoginMetadataLocked()
	if err != nil {
		tc.loginMetadataLock.Unlock()
		return err
	}
	previous := meta.PushKeys.clone()
	changed, err := meta.ensurePushKeys()
	tc.loginMetadataLock.Unlock()
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	if err = tc.saveUserLogin(ctx); err != nil {
		tc.loginMetadataLock.Lock()
		meta.PushKeys = previous
		tc.loginMetadataLock.Unlock()
		return fmt.Errorf("failed to save Tumblr web push keys: %w", err)
	}
	return nil
}

func (tc *TumblrClient) startPushSupervisor(generation *connectionGeneration) {
	if !tc.isCurrentGeneration(generation) {
		return
	}
	generation.wg.Add(1)
	go tc.pushSupervisor(generation)
}

func (tc *TumblrClient) pushSupervisor(generation *connectionGeneration) {
	defer generation.wg.Done()
	forceNew := false
	validateExisting := true
	retryDelay := 2 * time.Second
	for generation.ctx.Err() == nil && tc.isCurrentGeneration(generation) {
		prepared, prepErr := tc.preparePushRegistration(generation.ctx, forceNew, validateExisting)
		if prepared != nil {
			forceNew = false
			validateExisting = false
		}
		if prepErr != nil && tumblr.IsAuthError(prepErr) {
			tc.failGeneration(generation, tumblrBadCredentialsState(prepErr))
			return
		}
		if prepared == nil || prepared.registration == nil {
			tc.markPushTransient(generation, "tumblr-push-setup-failed", "Beeper is rebuilding the Tumblr push connection.", prepErr)
			if !waitForPushRetry(generation.ctx, retryDelay) {
				return
			}
			retryDelay = min(retryDelay*2, time.Minute)
			continue
		}
		if prepErr != nil {
			if log := tc.log(); log != nil {
				log.Warn().Err(prepErr).Msg("Tumblr push renewal failed; using the last known-good receiver")
			}
		}

		outcome, requestedDelay := tc.runPushReceiver(generation, prepared)
		if outcome == pushReceiverStopped {
			return
		}
		if outcome == pushReceiverRotate {
			forceNew = true
			validateExisting = false
			retryDelay = 2 * time.Second
			continue
		}
		if requestedDelay > retryDelay {
			retryDelay = requestedDelay
		}
		if !waitForPushRetry(generation.ctx, retryDelay) {
			return
		}
		retryDelay = min(retryDelay*2, time.Minute)
	}
}

func waitForPushRetry(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		delay = time.Second
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (tc *TumblrClient) preparePushRegistration(ctx context.Context, forceNew, validateExisting bool) (*preparedPushRegistration, error) {
	if err := tc.requireLoggedIn(); err != nil {
		return nil, err
	}
	if err := tc.ensurePushKeyMaterial(ctx); err != nil {
		return nil, err
	}
	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		return nil, err
	}
	if meta.PushKeys == nil {
		return nil, fmt.Errorf("tumblr web push keys are missing")
	}
	pending := meta.PushKeys.Pending.clone()
	active := meta.PushKeys.Active.clone()
	if !forceNew && pending != nil {
		if err = tc.finishPendingPushRegistration(ctx, pending); err == nil {
			if validateExisting {
				err = tc.refreshPushRegistrationCheckin(ctx, pending, true, true)
				if errors.Is(err, pushreceiver.ErrGcmAuthorization) {
					candidate, stageErr := tc.stagePushRegistration(ctx, active, true)
					if stageErr != nil {
						return nil, stageErr
					}
					return &preparedPushRegistration{registration: candidate, pending: true}, nil
				}
			}
			return &preparedPushRegistration{registration: pending, pending: true}, err
		}
		if active != nil && active.usable() {
			return &preparedPushRegistration{registration: active}, err
		}
		return nil, err
	}
	if !forceNew && active != nil && active.usable() && !tumblrPushReceiverRegistrationDue(active) {
		if validateExisting {
			err = tc.refreshPushRegistrationCheckin(ctx, active, false, true)
			if errors.Is(err, pushreceiver.ErrGcmAuthorization) {
				candidate, stageErr := tc.stagePushRegistration(ctx, active, true)
				if stageErr != nil {
					return nil, stageErr
				}
				return &preparedPushRegistration{registration: candidate, pending: true}, nil
			} else if err != nil {
				return &preparedPushRegistration{registration: active}, err
			}
		}
		return &preparedPushRegistration{registration: active}, nil
	}

	candidate, stageErr := tc.stagePushRegistration(ctx, active, forceNew)
	if stageErr != nil {
		if !forceNew && active != nil && active.usable() {
			return &preparedPushRegistration{registration: active}, stageErr
		}
		return nil, stageErr
	}
	return &preparedPushRegistration{registration: candidate, pending: true}, nil
}

func (r *PushRegistration) usable() bool {
	if r == nil || strings.TrimSpace(r.Token) == "" || strings.TrimSpace(r.FCMAppID) == "" || r.FCMRegisteredTS <= 0 {
		return false
	}
	credentials, err := r.credentials()
	return err == nil && credentials != nil
}

func tumblrPushReceiverRegistrationDue(registration *PushRegistration) bool {
	if registration == nil || registration.FCMRegisteredTS <= 0 {
		return true
	}
	registeredAt := time.UnixMilli(registration.FCMRegisteredTS)
	return !time.Now().Before(registeredAt.Add(tumblrPushSubscriptionTTL / 2))
}

func (tc *TumblrClient) stagePushRegistration(ctx context.Context, active *PushRegistration, forceNew bool) (*PushRegistration, error) {
	var credentials *pushreceiver.GCMCredentials
	var err error
	if !forceNew && active != nil {
		credentials, err = active.credentials()
		if err != nil {
			return nil, err
		}
	}
	requestCtx, cancel := context.WithTimeout(ctx, tumblrPushRequestTimeout)
	defer cancel()
	if credentials == nil {
		credentials, err = pushreceiver.CheckIn(requestCtx, &pushreceiver.GCMCredentials{})
	} else {
		credentials, err = pushreceiver.CheckIn(requestCtx, credentials)
		if errors.Is(err, pushreceiver.ErrGcmAuthorization) {
			forceNew = true
			credentials, err = pushreceiver.CheckIn(requestCtx, &pushreceiver.GCMCredentials{})
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create Beeper push receiver credentials: %w", err)
	}

	opts := &pushreceiver.GCMRegistrationOpts{
		Expiry:     tumblrPushSubscriptionTTL,
		InstanceID: string(tc.userLogin.ID),
	}
	if !forceNew && active != nil {
		opts.AppID = active.FCMAppID
	}
	fcmCredentials, err := pushreceiver.RegisterGCM(requestCtx, tumblrWebPushVAPIDKey, *credentials, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to register Beeper's Tumblr push receiver with FCM: %w", err)
	}
	if fcmCredentials == nil || strings.TrimSpace(fcmCredentials.Token) == "" || strings.TrimSpace(fcmCredentials.AppID) == "" {
		return nil, fmt.Errorf("FCM returned an incomplete Tumblr push receiver registration")
	}
	candidate := &PushRegistration{
		Token:         "https://fcm.googleapis.com/fcm/send/" + fcmCredentials.Token,
		FCMAppID:      fcmCredentials.AppID,
		LastCheckinTS: time.Now().UnixMilli(),
	}
	candidate.setCredentials(&fcmCredentials.GCM)
	if err = tc.persistPendingPushRegistration(ctx, candidate); err != nil {
		return nil, err
	}
	if err = tc.finishPendingPushRegistration(ctx, candidate); err != nil {
		return nil, err
	}
	return candidate, nil
}

func (tc *TumblrClient) persistPendingPushRegistration(ctx context.Context, candidate *PushRegistration) error {
	tc.loginMetadataLock.Lock()
	meta, err := tc.validatedLoginMetadataLocked()
	if err != nil {
		tc.loginMetadataLock.Unlock()
		return err
	}
	if meta.PushKeys == nil {
		tc.loginMetadataLock.Unlock()
		return fmt.Errorf("tumblr web push keys are missing")
	}
	previous := meta.PushKeys.Pending.clone()
	previousRetiring := clonePushRegistrations(meta.PushKeys.Retiring)
	if previous != nil && previous.FCMRegisteredTS > 0 && !samePushRegistration(previous, candidate) {
		meta.PushKeys.Retiring = appendUniquePushRegistration(meta.PushKeys.Retiring, previous)
	}
	meta.PushKeys.Pending = candidate.clone()
	tc.loginMetadataLock.Unlock()
	if err = tc.saveUserLogin(ctx); err != nil {
		tc.loginMetadataLock.Lock()
		if samePushRegistration(meta.PushKeys.Pending, candidate) {
			meta.PushKeys.Pending = previous
			meta.PushKeys.Retiring = previousRetiring
		}
		tc.loginMetadataLock.Unlock()
		return fmt.Errorf("failed to stage Tumblr push receiver metadata: %w", err)
	}
	return nil
}

func (tc *TumblrClient) finishPendingPushRegistration(ctx context.Context, pending *PushRegistration) error {
	if pending == nil || strings.TrimSpace(pending.Token) == "" {
		return fmt.Errorf("pending Tumblr push receiver registration is incomplete")
	}
	if pending.FCMRegisteredTS > 0 {
		return nil
	}
	if err := tc.registerTumblrPushEndpoint(ctx, bridgev2.PushTypeWeb, pending.Token); err != nil {
		return tc.handleRemoteError(err)
	}
	registeredAt := time.Now().UnixMilli()
	tc.loginMetadataLock.Lock()
	meta, err := tc.validatedLoginMetadataLocked()
	if err != nil {
		tc.loginMetadataLock.Unlock()
		return err
	}
	if meta.PushKeys == nil || !samePushRegistration(meta.PushKeys.Pending, pending) {
		tc.loginMetadataLock.Unlock()
		return fmt.Errorf("pending Tumblr push receiver changed while it was registering")
	}
	meta.PushKeys.Pending.FCMRegisteredTS = registeredAt
	pending.FCMRegisteredTS = registeredAt
	tc.loginMetadataLock.Unlock()
	if err = tc.saveUserLogin(ctx); err != nil {
		tc.loginMetadataLock.Lock()
		if meta.PushKeys != nil && samePushRegistration(meta.PushKeys.Pending, pending) {
			meta.PushKeys.Pending.FCMRegisteredTS = 0
			pending.FCMRegisteredTS = 0
		}
		tc.loginMetadataLock.Unlock()
		return fmt.Errorf("failed to save registered Tumblr push receiver metadata: %w", err)
	}
	return nil
}

func (tc *TumblrClient) runPushReceiver(generation *connectionGeneration, prepared *preparedPushRegistration) (pushReceiverOutcome, time.Duration) {
	registration := prepared.registration.clone()
	credentials, err := registration.credentials()
	if err != nil || credentials == nil {
		tc.markPushTransient(generation, "tumblr-push-receiver-invalid", "Beeper is rebuilding the Tumblr push connection.", err)
		return pushReceiverRotate, 0
	}

	receiverCtx, cancel := context.WithCancel(generation.ctx)
	defer cancel()
	if log := tc.log(); log != nil {
		logger := log.With().Str("component", "tumblr_push_receiver").Logger().Level(zerolog.InfoLevel)
		receiverCtx = logger.WithContext(receiverCtx)
	}
	client := pushreceiver.New(
		pushreceiver.WithCreds(credentials),
		pushreceiver.WithRetry(false),
		pushreceiver.WithHeartbeat(
			pushreceiver.WithServerInterval(time.Minute),
			pushreceiver.WithClientInterval(2*time.Minute),
			pushreceiver.WithAdaptive(true),
		),
		pushreceiver.WithReceivedPersistentID(append([]string(nil), registration.PersistentIDs...)),
		pushreceiver.WithMaxUnackedIDs(10),
	)
	listenDone := make(chan struct{})
	go func() {
		client.Listen(receiverCtx)
		close(listenDone)
	}()
	maintenance := time.NewTicker(tumblrPushMaintenanceInterval)
	defer maintenance.Stop()

	for {
		select {
		case <-generation.ctx.Done():
			cancel()
			<-listenDone
			return pushReceiverStopped, 0
		case <-maintenance.C:
			if tumblrPushReceiverRegistrationDue(registration) {
				tc.markPushTransient(generation, "tumblr-push-registration-renewing", "Beeper is renewing the Tumblr push connection.", nil)
				cancel()
				<-listenDone
				return pushReceiverRotate, 0
			}
			if err = tc.refreshPushRegistrationCheckin(receiverCtx, registration, prepared.pending, false); err != nil {
				if errors.Is(err, pushreceiver.ErrGcmAuthorization) {
					tc.markPushTransient(generation, "tumblr-push-receiver-unauthorized", "Beeper is renewing the Tumblr push connection.", err)
					cancel()
					<-listenDone
					return pushReceiverRotate, 0
				}
				if log := tc.log(); log != nil {
					log.Warn().Err(err).Msg("Failed to refresh Tumblr push receiver checkin")
				}
			}
		case event, ok := <-client.Events:
			if !ok {
				<-listenDone
				if generation.ctx.Err() != nil {
					return pushReceiverStopped, 0
				}
				tc.markPushTransient(generation, "tumblr-push-receiver-disconnected", "Beeper lost the Tumblr push connection and is reconnecting.", nil)
				return pushReceiverRetry, 0
			}
			switch ev := event.(type) {
			case *pushreceiver.ConnectedEvent:
				if prepared.pending {
					if err = tc.promotePendingPushRegistration(receiverCtx, registration); err != nil {
						tc.markPushTransient(generation, "tumblr-push-promotion-failed", "Beeper is finishing the Tumblr push connection.", err)
						cancel()
						<-listenDone
						return pushReceiverRetry, 0
					}
					prepared.pending = false
				}
				tc.markPushConnected(generation)
				if err = tc.clearPushPersistentIDs(receiverCtx, registration.Token); err != nil {
					if log := tc.log(); log != nil {
						log.Warn().Err(err).Msg("Failed to clear Tumblr web push persistent IDs")
					}
				}
				tc.startRetiringRegistrationCleanup(generation)
			case *pushreceiver.DisconnectedEvent:
				tc.markPushTransient(generation, "tumblr-push-receiver-disconnected", "Beeper lost the Tumblr push connection and is reconnecting.", ev.ErrorObj)
			case *pushreceiver.StreamAck:
				if err = tc.clearPushPersistentIDs(receiverCtx, registration.Token); err != nil && tc.log() != nil {
					tc.log().Warn().Err(err).Msg("Failed to clear Tumblr web push persistent IDs")
				}
			case *pushreceiver.MessageEvent:
				messageErr := tc.handlePushReceiverMessage(receiverCtx, *ev)
				if messageErr == nil {
					messageErr = tc.savePushPersistentID(receiverCtx, registration.Token, ev.PersistentID)
				}
				if completeErr := ev.Complete(receiverCtx, messageErr); completeErr != nil && receiverCtx.Err() == nil {
					if log := tc.log(); log != nil {
						log.Warn().Err(completeErr).Msg("Failed to finish handling a Tumblr web push message")
					}
				}
				if messageErr != nil {
					if log := tc.log(); log != nil {
						log.Warn().Err(messageErr).Str("persistent_id_hash", logIdentifierHash(ev.PersistentID)).Msg("Failed to durably handle Tumblr web push message")
					}
					continue
				}
			case *pushreceiver.UnauthorizedError:
				tc.markPushTransient(generation, "tumblr-push-receiver-unauthorized", "Beeper is renewing the Tumblr push connection.", ev.ErrorObj)
				cancel()
				<-listenDone
				return pushReceiverRotate, 0
			case *pushreceiver.HeartbeatError:
				tc.markPushTransient(generation, "tumblr-push-receiver-heartbeat", "Beeper lost the Tumblr push connection and is reconnecting.", ev.ErrorObj)
			case *pushreceiver.HeartbeatEvent:
				if log := tc.log(); log != nil {
					log.Debug().Bool("send", ev.Send).Bool("ack", ev.Ack).Msg("Tumblr push receiver heartbeat")
				}
			case *pushreceiver.RetryEvent:
				tc.markPushTransient(generation, "tumblr-push-receiver-retry", "Beeper lost the Tumblr push connection and is reconnecting.", ev.ErrorObj)
				cancel()
				<-listenDone
				return pushReceiverRetry, ev.RetryAfter
			}
		}
	}
}

func (tc *TumblrClient) markPushConnected(generation *connectionGeneration) {
	if !tc.isCurrentGeneration(generation) || !tc.loggedIn.Load() {
		return
	}
	tc.pushRegistrationDown.Store(false)
	tc.sendGenerationState(generation, status.BridgeState{StateEvent: status.StateConnected})
	tc.ensureInboundSyncStarted(generation)
}

func (tc *TumblrClient) markPushTransient(generation *connectionGeneration, code status.BridgeStateErrorCode, message string, err error) {
	if !tc.isCurrentGeneration(generation) || !tc.loggedIn.Load() {
		return
	}
	if !tc.pushRegistrationDown.CompareAndSwap(false, true) {
		return
	}
	if log := tc.log(); log != nil && err != nil {
		log.Warn().Err(err).Str("state_error", string(code)).Msg("Tumblr push receiver is temporarily unavailable")
	}
	tc.sendGenerationState(generation, status.BridgeState{
		StateEvent: status.StateTransientDisconnect,
		Error:      code,
		Message:    message,
	})
}

func (tc *TumblrClient) promotePendingPushRegistration(ctx context.Context, registration *PushRegistration) error {
	tc.loginMetadataLock.Lock()
	meta, err := tc.validatedLoginMetadataLocked()
	if err != nil {
		tc.loginMetadataLock.Unlock()
		return err
	}
	if meta.PushKeys == nil || !samePushRegistration(meta.PushKeys.Pending, registration) {
		tc.loginMetadataLock.Unlock()
		return fmt.Errorf("connected Tumblr push receiver no longer matches the pending registration")
	}
	previousActive := meta.PushKeys.Active.clone()
	previousRetiring := clonePushRegistrations(meta.PushKeys.Retiring)
	if previousActive != nil && !samePushRegistration(previousActive, registration) {
		meta.PushKeys.Retiring = appendUniquePushRegistration(meta.PushKeys.Retiring, previousActive)
	}
	meta.PushKeys.Active = meta.PushKeys.Pending.clone()
	meta.PushKeys.Pending = nil
	tc.loginMetadataLock.Unlock()
	if err = tc.saveUserLogin(ctx); err != nil {
		tc.loginMetadataLock.Lock()
		if meta.PushKeys != nil && samePushRegistration(meta.PushKeys.Active, registration) && meta.PushKeys.Pending == nil {
			meta.PushKeys.Active = previousActive
			meta.PushKeys.Pending = registration.clone()
			meta.PushKeys.Retiring = previousRetiring
		}
		tc.loginMetadataLock.Unlock()
		return fmt.Errorf("failed to promote Tumblr push receiver metadata: %w", err)
	}
	return nil
}

func (tc *TumblrClient) refreshPushRegistrationCheckin(ctx context.Context, registration *PushRegistration, pending, force bool) error {
	if registration == nil || (!force && (registration.LastCheckinTS <= 0 || time.Since(time.UnixMilli(registration.LastCheckinTS)) <= tumblrPushCheckinInterval)) {
		return nil
	}
	credentials, err := registration.credentials()
	if err != nil || credentials == nil {
		return err
	}
	requestCtx, cancel := context.WithTimeout(ctx, tumblrPushRequestTimeout)
	defer cancel()
	updated, err := pushreceiver.CheckIn(requestCtx, credentials)
	if err != nil {
		return err
	}
	lastCheckin := registration.LastCheckinTS
	tc.loginMetadataLock.Lock()
	meta, err := tc.validatedLoginMetadataLocked()
	if err != nil {
		tc.loginMetadataLock.Unlock()
		return err
	}
	if meta.PushKeys == nil {
		tc.loginMetadataLock.Unlock()
		return fmt.Errorf("tumblr web push keys are missing")
	}
	target := meta.PushKeys.Active
	if pending {
		target = meta.PushKeys.Pending
	}
	if !samePushRegistration(target, registration) || target.LastCheckinTS > lastCheckin {
		tc.loginMetadataLock.Unlock()
		return nil
	}
	target.setCredentials(updated)
	target.LastCheckinTS = time.Now().UnixMilli()
	registration.setCredentials(updated)
	registration.LastCheckinTS = target.LastCheckinTS
	tc.loginMetadataLock.Unlock()
	return tc.saveUserLogin(ctx)
}

func (tc *TumblrClient) startRetiringRegistrationCleanup(generation *connectionGeneration) {
	if !tc.isCurrentGeneration(generation) || !generation.retiringClean.CompareAndSwap(false, true) {
		return
	}
	generation.wg.Add(1)
	go func() {
		defer generation.wg.Done()
		defer generation.retiringClean.Store(false)
		if err := tc.cleanupRetiringPushRegistrations(generation.ctx); err != nil && generation.ctx.Err() == nil {
			if log := tc.log(); log != nil {
				log.Warn().Err(err).Msg("Failed to retire an old Tumblr push receiver registration")
			}
		}
	}()
}

func (tc *TumblrClient) cleanupRetiringPushRegistrations(ctx context.Context) error {
	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		return err
	}
	if meta.PushKeys == nil {
		return nil
	}
	retiring := clonePushRegistrations(meta.PushKeys.Retiring)
	active := meta.PushKeys.Active.clone()
	for _, registration := range retiring {
		if registration == nil || samePushRegistration(registration, active) {
			continue
		}
		if err = tc.unregisterTumblrPushEndpoint(ctx, registration.Token); err != nil {
			return err
		}
		tc.loginMetadataLock.Lock()
		liveMeta, metadataErr := tc.validatedLoginMetadataLocked()
		if metadataErr == nil && liveMeta.PushKeys != nil {
			liveMeta.PushKeys.Retiring = removePushRegistration(liveMeta.PushKeys.Retiring, registration)
		}
		tc.loginMetadataLock.Unlock()
		if metadataErr != nil {
			return metadataErr
		}
		if err = tc.saveUserLogin(ctx); err != nil {
			return err
		}
	}
	return nil
}

func samePushRegistration(left, right *PushRegistration) bool {
	return left != nil && right != nil && left.Token == right.Token && left.FCMAppID == right.FCMAppID &&
		left.AndroidID == right.AndroidID && left.SecurityToken == right.SecurityToken
}

func clonePushRegistrations(registrations []*PushRegistration) []*PushRegistration {
	cloned := make([]*PushRegistration, 0, len(registrations))
	for _, registration := range registrations {
		if registration != nil {
			cloned = append(cloned, registration.clone())
		}
	}
	return cloned
}

func appendUniquePushRegistration(registrations []*PushRegistration, candidate *PushRegistration) []*PushRegistration {
	if candidate == nil {
		return registrations
	}
	for _, registration := range registrations {
		if samePushRegistration(registration, candidate) {
			return registrations
		}
	}
	return append(registrations, candidate.clone())
}

func removePushRegistration(registrations []*PushRegistration, candidate *PushRegistration) []*PushRegistration {
	filtered := registrations[:0]
	for _, registration := range registrations {
		if !samePushRegistration(registration, candidate) {
			filtered = append(filtered, registration)
		}
	}
	return filtered
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

func (tc *TumblrClient) savePushPersistentID(ctx context.Context, registrationToken, persistentID string) error {
	persistentID = strings.TrimSpace(persistentID)
	if persistentID == "" {
		return nil
	}
	tc.loginMetadataLock.Lock()
	meta, err := tc.validatedLoginMetadataLocked()
	if err != nil || meta.PushKeys == nil {
		tc.loginMetadataLock.Unlock()
		return err
	}
	registration := pushRegistrationByToken(meta.PushKeys, registrationToken)
	if registration == nil {
		tc.loginMetadataLock.Unlock()
		return nil
	}
	for _, existing := range registration.PersistentIDs {
		if existing == persistentID {
			tc.loginMetadataLock.Unlock()
			return nil
		}
	}
	previous := append([]string(nil), registration.PersistentIDs...)
	registration.PersistentIDs = append(registration.PersistentIDs, persistentID)
	if overflow := len(registration.PersistentIDs) - maxStoredTumblrPushPersistentIDs; overflow > 0 {
		registration.PersistentIDs = registration.PersistentIDs[overflow:]
	}
	tc.loginMetadataLock.Unlock()
	if err = tc.saveUserLogin(ctx); err != nil {
		tc.loginMetadataLock.Lock()
		if current := pushRegistrationByToken(meta.PushKeys, registrationToken); current != nil {
			current.PersistentIDs = previous
		}
		tc.loginMetadataLock.Unlock()
	}
	return err
}

func (tc *TumblrClient) clearPushPersistentIDs(ctx context.Context, registrationToken string) error {
	tc.loginMetadataLock.Lock()
	meta, err := tc.validatedLoginMetadataLocked()
	if err != nil || meta.PushKeys == nil {
		tc.loginMetadataLock.Unlock()
		return err
	}
	registration := pushRegistrationByToken(meta.PushKeys, registrationToken)
	if registration == nil || len(registration.PersistentIDs) == 0 {
		tc.loginMetadataLock.Unlock()
		return nil
	}
	previous := append([]string(nil), registration.PersistentIDs...)
	registration.PersistentIDs = nil
	tc.loginMetadataLock.Unlock()
	if err = tc.saveUserLogin(ctx); err != nil {
		tc.loginMetadataLock.Lock()
		if current := pushRegistrationByToken(meta.PushKeys, registrationToken); current != nil && len(current.PersistentIDs) == 0 {
			current.PersistentIDs = previous
		}
		tc.loginMetadataLock.Unlock()
	}
	return err
}

func (tc *TumblrClient) unregisterTumblrWebPush(ctx context.Context) {
	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		return
	}
	registrations := []*PushRegistration{}
	if meta.PushKeys != nil {
		registrations = appendUniquePushRegistration(registrations, meta.PushKeys.Active)
		if meta.PushKeys.Pending != nil && meta.PushKeys.Pending.FCMRegisteredTS > 0 {
			registrations = appendUniquePushRegistration(registrations, meta.PushKeys.Pending)
		}
		for _, registration := range meta.PushKeys.Retiring {
			registrations = appendUniquePushRegistration(registrations, registration)
		}
	}
	for _, registration := range registrations {
		if err = tc.unregisterTumblrPushEndpoint(ctx, registration.Token); err != nil {
			if log := tc.log(); log != nil {
				log.Warn().Err(err).Msg("Failed to unregister Tumblr web push device")
			}
			continue
		}
		tc.loginMetadataLock.Lock()
		liveMeta, metadataErr := tc.validatedLoginMetadataLocked()
		if metadataErr == nil && liveMeta.PushKeys != nil {
			if samePushRegistration(liveMeta.PushKeys.Active, registration) {
				liveMeta.PushKeys.Active = nil
			}
			if samePushRegistration(liveMeta.PushKeys.Pending, registration) {
				liveMeta.PushKeys.Pending = nil
			}
			liveMeta.PushKeys.Retiring = removePushRegistration(liveMeta.PushKeys.Retiring, registration)
		}
		tc.loginMetadataLock.Unlock()
		if metadataErr != nil {
			return
		}
		if err = tc.saveUserLogin(ctx); err != nil && tc.log() != nil {
			tc.log().Warn().Err(err).Msg("Failed to save Tumblr metadata after unregistering web push")
		}
	}
}

func (tc *TumblrClient) unregisterTumblrPushEndpoint(ctx context.Context, token string) error {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil
	}
	meta, err := tc.validatedLoginMetadata()
	if err != nil {
		return err
	}
	client, err := tc.tumblrClient()
	if err != nil {
		return err
	}
	p256dh, auth, err := meta.encodedPushKeys()
	if err != nil {
		return err
	}
	registration := tumblr.NewWebPushDeviceRegistration(token, tumblr.WebPushKeys{
		P256DH: p256dh,
		Auth:   auth,
	})
	return client.UnregisterWebPushDevice(ctx, registration)
}

func pushRegistrationByToken(keys *PushKeys, token string) *PushRegistration {
	if keys == nil {
		return nil
	}
	if keys.Active != nil && keys.Active.Token == token {
		return keys.Active
	}
	if keys.Pending != nil && keys.Pending.Token == token {
		return keys.Pending
	}
	for _, registration := range keys.Retiring {
		if registration != nil && registration.Token == token {
			return registration
		}
	}
	return nil
}

func (tc *TumblrClient) handleWebPushPayload(ctx context.Context, data json.RawMessage) error {
	conversationID, err := tc.conversationIDFromPushPayload(data)
	if err != nil {
		return err
	}
	if log := tc.log(); log != nil {
		log.Info().Str("conversation_id_hash", logIdentifierHash(conversationID)).Msg("Handling Tumblr push for conversation")
	}
	return tc.enqueueConversationSync(ctx, conversationID)
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
	privateKey, err := meta.pushPrivateKey()
	if err != nil {
		return nil, err
	}
	if meta.PushKeys == nil || len(meta.PushKeys.Auth) != 16 {
		return nil, fmt.Errorf("tumblr web push auth secret is missing or invalid")
	}
	authSecret := append([]byte(nil), meta.PushKeys.Auth...)
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
