/*
 * Copyright (c) 2019 Zenichi Amano
 *
 * This file is part of go-push-receiver, which is MIT licensed.
 * See http://opensource.org/licenses/MIT
 */

// Package pushreceiver contains the MCS receive path from
// github.com/beeper/push-receiver at b7d6a21272e03107bda361a0eca1b7bf04e60611.
//
// Registration and check-in remain delegated to that upstream module. The
// local receive path rejects failed logins, keeps remote acknowledgements
// behind durable processing, corrects and synchronizes the inbound stream
// counter, and makes event delivery and connection teardown cancellable.
package pushreceiver

import (
	"context"
	"errors"

	upstream "github.com/beeper/push-receiver"
	pb "github.com/beeper/push-receiver/pb/mcs"
)

type (
	Event               = upstream.Event
	ConnectedEvent      = upstream.ConnectedEvent
	RetryEvent          = upstream.RetryEvent
	DisconnectedEvent   = upstream.DisconnectedEvent
	HeartbeatEvent      = upstream.HeartbeatEvent
	StreamAck           = upstream.StreamAck
	HeartbeatError      = upstream.HeartbeatError
	UnauthorizedError   = upstream.UnauthorizedError
	GCMCredentials      = upstream.GCMCredentials
	FCMCredentials      = upstream.FCMCredentials
	GCMRegistrationOpts = upstream.GCMRegistrationOpts
)

// MessageEvent is held by the receive loop until Complete confirms that the
// connector durably accepted the wakeup. This keeps the remote stream ACK
// behind the durable conversation job.
type MessageEvent struct {
	PersistentID string        `json:"persistentId"`
	From         string        `json:"from"`
	To           string        `json:"to"`
	TTL          int32         `json:"ttl"`
	Sent         int64         `json:"sent"`
	AppData      []*pb.AppData `json:"app_data"`
	Token        string        `json:"token"`
	RegID        string        `json:"reg_id"`
	RawData      []byte        `json:"raw_data"`
	AppID        string        `json:"app_id"`

	completed chan error
}

func (e *MessageEvent) Complete(ctx context.Context, result error) error {
	if e == nil || e.completed == nil {
		return errors.New("push message completion is unavailable")
	}
	select {
	case e.completed <- result:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

var ErrGcmAuthorization = upstream.ErrGcmAuthorization

func CheckIn(ctx context.Context, credentials *GCMCredentials) (*GCMCredentials, error) {
	return upstream.CheckIn(ctx, credentials)
}

func RegisterGCM(ctx context.Context, authorizationEntity string, credentials GCMCredentials, opts *GCMRegistrationOpts) (*FCMCredentials, error) {
	return upstream.RegisterGCM(ctx, authorizationEntity, credentials, opts)
}
