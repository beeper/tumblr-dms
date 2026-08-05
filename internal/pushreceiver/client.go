/*
 * Copyright (c) 2019 Zenichi Amano
 *
 * This file is part of go-push-receiver, which is MIT licensed.
 * See http://opensource.org/licenses/MIT
 */

// Receiver implementation copied from github.com/beeper/push-receiver at
// b7d6a21272e03107bda361a0eca1b7bf04e60611. Local changes reject error-bearing
// logins, gate remote acknowledgements on durable processing, and make event
// delivery and connection teardown cancellable.
package pushreceiver

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"time"

	"github.com/rs/zerolog"

	pb "github.com/beeper/push-receiver/pb/mcs"
)

type backoff struct {
	attempts int
	base     int64
	max      int64
}

func newBackoff(base, maximum time.Duration) *backoff {
	return &backoff{base: int64(base), max: int64(maximum)}
}

func (b *backoff) duration() time.Duration {
	if b == nil || b.base <= 0 || b.max <= 0 {
		return 0
	}
	// Saturate the exponent before either the shift count or duration can
	// overflow. Keeping the jitter bound positive also prevents Int63n panics.
	if b.attempts < 63 {
		b.attempts++
	}
	upper := b.base
	for attempt := 0; attempt < b.attempts && upper < b.max; attempt++ {
		if upper > b.max/2 {
			upper = b.max
			break
		}
		upper *= 2
	}
	if upper > b.max {
		upper = b.max
	}
	if upper <= 1 {
		return 0
	}
	return time.Duration(rand.Int63n(upper))
}

func (b *backoff) reset() {
	b.attempts = 0
}

type Heartbeat struct {
	clientInterval time.Duration
	serverInterval time.Duration
	deadmanTimeout time.Duration
	adaptive       bool
}

type HeartbeatOption func(*Heartbeat)

func WithClientInterval(interval time.Duration) HeartbeatOption {
	return func(heartbeat *Heartbeat) {
		heartbeat.clientInterval = interval
	}
}

func WithServerInterval(interval time.Duration) HeartbeatOption {
	return func(heartbeat *Heartbeat) {
		if interval > time.Minute {
			heartbeat.serverInterval = interval
		} else {
			heartbeat.serverInterval = time.Minute
		}
	}
}

func WithAdaptive(enabled bool) HeartbeatOption {
	return func(heartbeat *Heartbeat) {
		heartbeat.adaptive = enabled
	}
}

func newHeartbeat(options ...HeartbeatOption) *Heartbeat {
	heartbeat := &Heartbeat{}
	for _, option := range options {
		option(heartbeat)
	}
	return heartbeat
}

type MCSClient struct {
	tlsConfig     *tls.Config
	creds         *GCMCredentials
	dialer        *net.Dialer
	backoff       *backoff
	heartbeat     *Heartbeat
	maxUnackedIDs int

	receivedPersistentID []string
	retryDisabled        bool
	Events               chan Event
}

type ClientOption func(*MCSClient)

func WithCreds(credentials *GCMCredentials) ClientOption {
	return func(client *MCSClient) {
		client.creds = credentials
	}
}

func WithReceivedPersistentID(ids []string) ClientOption {
	return func(client *MCSClient) {
		client.receivedPersistentID = ids
	}
}

func WithHeartbeat(options ...HeartbeatOption) ClientOption {
	return func(client *MCSClient) {
		client.heartbeat = newHeartbeat(options...)
	}
}

func WithRetry(retry bool) ClientOption {
	return func(client *MCSClient) {
		client.retryDisabled = !retry
	}
}

func WithMaxUnackedIDs(maxIDs int) ClientOption {
	return func(client *MCSClient) {
		client.maxUnackedIDs = maxIDs
	}
}

func New(options ...ClientOption) *MCSClient {
	client := &MCSClient{Events: make(chan Event, 50)}
	for _, option := range options {
		option(client)
	}
	if client.backoff == nil {
		client.backoff = newBackoff(defaultBackoffBase*time.Second, defaultBackoffMax*time.Second)
	}
	if client.heartbeat == nil {
		client.heartbeat = newHeartbeat(WithClientInterval(defaultHeartbeatPeriod * time.Minute))
	}
	if client.tlsConfig == nil {
		client.tlsConfig = &tls.Config{MinVersion: tls.VersionTLS13}
	}
	if client.dialer == nil {
		client.dialer = &net.Dialer{
			Timeout:       defaultDialTimeout * time.Second,
			KeepAlive:     defaultKeepAlive * time.Minute,
			FallbackDelay: 30 * time.Millisecond,
		}
	}
	if client.maxUnackedIDs == 0 {
		client.maxUnackedIDs = 10
	}
	return client
}

func (c *MCSClient) Listen(ctx context.Context) {
	defer close(c.Events)
	for ctx.Err() == nil {
		err := c.tryToConnect(ctx)
		if err == nil {
			continue
		}
		if errors.Is(err, ErrGcmAuthorization) {
			if emitErr := c.emit(ctx, &UnauthorizedError{ErrorObj: err}); emitErr != nil {
				return
			}
			c.creds = nil
		}
		if c.retryDisabled {
			return
		}
		sleepDuration := c.backoff.duration()
		if emitErr := c.emit(ctx, &RetryEvent{ErrorObj: err, RetryAfter: sleepDuration}); emitErr != nil {
			return
		}
		timer := time.NewTimer(sleepDuration)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		}
	}
}

func (c *MCSClient) emit(ctx context.Context, event Event) error {
	select {
	case c.Events <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *MCSClient) tryToConnect(ctx context.Context) error {
	if c.creds == nil {
		return fmt.Errorf("%w: MCS credentials are missing", ErrGcmAuthorization)
	}
	conn, err := tls.DialWithDialer(c.dialer, "tcp", mtalkServer, c.tlsConfig)
	if err != nil {
		return fmt.Errorf("dial failed to FCM: %w", err)
	}
	defer conn.Close()

	mcsClient := newMCS(conn, *zerolog.Ctx(ctx), c.creds, c.heartbeat, c.Events)
	defer mcsClient.disconnect()
	stopCancellationWatch := make(chan struct{})
	cancellationWatchDone := make(chan struct{})
	go func() {
		defer close(cancellationWatchDone)
		select {
		case <-ctx.Done():
			mcsClient.disconnect()
		case <-stopCancellationWatch:
		}
	}()
	defer func() {
		close(stopCancellationWatch)
		<-cancellationWatchDone
	}()
	if err = mcsClient.SendLoginPacket(c.receivedPersistentID); err != nil {
		return fmt.Errorf("send login packet failed: %w", err)
	}
	go c.heartbeat.start(ctx, mcsClient)
	return c.readMessages(ctx, mcsClient)
}

func (c *MCSClient) readMessages(ctx context.Context, mcsClient *mcs) error {
	if err := mcsClient.ReceiveVersion(); err != nil {
		return fmt.Errorf("receive version failed: %w", err)
	}
	for ctx.Err() == nil {
		data, err := mcsClient.PerformReadTag()
		if err != nil {
			return fmt.Errorf("receive tag failed: %w", err)
		} else if data == nil {
			return errFcmNotEnoughData
		} else if ctx.Err() != nil {
			break
		}
		if err = c.onDataMessage(ctx, data); err != nil {
			return fmt.Errorf("process data message failed: %w", err)
		}
		if len(c.receivedPersistentID) >= c.maxUnackedIDs {
			if err = c.ackStreamPosition(ctx, mcsClient); err != nil {
				return fmt.Errorf("unable to acknowledge current stream position: %w", err)
			}
		}
	}
	return nil
}

func (c *MCSClient) ackStreamPosition(ctx context.Context, mcsClient *mcs) error {
	if err := mcsClient.SendStreamAck(); err != nil {
		return err
	}
	if err := c.emit(ctx, &StreamAck{}); err != nil {
		return err
	}
	c.receivedPersistentID = nil
	return nil
}

func (c *MCSClient) onDataMessage(ctx context.Context, tagData any) error {
	switch data := tagData.(type) {
	case *pb.LoginResponse:
		if loginErr := data.GetError(); loginErr != nil {
			return fmt.Errorf("%w: MCS login rejected with code %d", ErrGcmAuthorization, loginErr.GetCode())
		}
		c.backoff.reset()
		c.receivedPersistentID = nil
		return c.emit(ctx, &ConnectedEvent{ServerTimestamp: data.GetServerTimestamp()})
	case *pb.DataMessageStanza:
		c.receivedPersistentID = append(c.receivedPersistentID, data.GetPersistentId())
		event := &MessageEvent{
			PersistentID: data.GetPersistentId(),
			From:         data.GetFrom(),
			To:           data.GetTo(),
			TTL:          data.GetTtl(),
			Sent:         data.GetSent(),
			AppData:      data.GetAppData(),
			Token:        data.GetToken(),
			RegID:        data.GetRegId(),
			RawData:      data.GetRawData(),
			AppID:        data.GetAppID(),
			completed:    make(chan error),
		}
		if err := c.emit(ctx, event); err != nil {
			return err
		}
		select {
		case err := <-event.completed:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (h *Heartbeat) start(ctx context.Context, mcsClient *mcs) {
	deadmanTimeout := h.deadmanTimeout
	if deadmanTimeout <= 0 {
		if h.clientInterval < h.serverInterval {
			deadmanTimeout = h.serverInterval * 4
		} else {
			deadmanTimeout = h.clientInterval * 4
		}
	}

	var pingDeadman *time.Timer
	var pingDeadmanC <-chan time.Time
	if deadmanTimeout > 0 {
		pingDeadman = time.NewTimer(deadmanTimeout)
		pingDeadmanC = pingDeadman.C
		defer pingDeadman.Stop()
	}
	var pingTicker *time.Ticker
	var pingTickerC <-chan time.Time
	if h.clientInterval > 0 {
		pingTicker = time.NewTicker(h.clientInterval)
		pingTickerC = pingTicker.C
		defer pingTicker.Stop()
	}

	for {
		select {
		case <-ctx.Done():
			mcsClient.disconnect()
			return
		case <-mcsClient.done:
			return
		case <-mcsClient.heartbeatAck:
			if pingDeadman != nil {
				pingDeadman.Reset(deadmanTimeout)
			}
		case <-pingDeadmanC:
			mcsClient.disconnect()
			return
		case <-pingTickerC:
			if err := mcsClient.SendHeartbeatPingPacket(); err != nil {
				mcsClient.disconnect()
				return
			}
		}
	}
}
