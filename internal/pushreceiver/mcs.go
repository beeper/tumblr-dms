/*
 * Copyright (c) 2019 Zenichi Amano
 *
 * This file is part of go-push-receiver, which is MIT licensed.
 * See http://opensource.org/licenses/MIT
 */

// MCS wire implementation copied from github.com/beeper/push-receiver at
// b7d6a21272e03107bda361a0eca1b7bf04e60611. Local changes use an atomic,
// receiver-owned inbound stream counter and nonblocking connection teardown.
package pushreceiver

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	pb "github.com/beeper/push-receiver/pb/mcs"
)

type tagType byte

const (
	mtalkServer   = "mtalk.google.com:5228"
	mcsDomain     = "mcs.android.com"
	chromeVersion = "144.0.7559.132"
	fcmVersion    = 41

	versionPacketLen = 1
	tagPacketLen     = 1
	sizePacketLenMax = 5
	maxFrameSize     = 16 << 20

	defaultDialTimeout     = 30
	defaultKeepAlive       = 1
	defaultBackoffBase     = 5
	defaultBackoffMax      = 15 * 60
	defaultHeartbeatPeriod = 10
)

const (
	tagHeartbeatPing     tagType = 0
	tagHeartbeatAck      tagType = 1
	tagLoginRequest      tagType = 2
	tagLoginResponse     tagType = 3
	tagClose             tagType = 4
	tagIqStanza          tagType = 7
	tagDataMessageStanza tagType = 8
	tagStreamErrorStanza tagType = 10
)

var errFcmNotEnoughData = errors.New("FCM response did not include data")

type mcs struct {
	conn             *tls.Conn
	log              zerolog.Logger
	creds            *GCMCredentials
	incomingStreamID atomic.Int32
	heartbeatAck     chan bool
	heartbeat        *Heartbeat
	disconnectOnce   sync.Once
	done             chan struct{}
	events           chan Event
}

func newMCS(conn *tls.Conn, log zerolog.Logger, credentials *GCMCredentials, heartbeat *Heartbeat, events chan Event) *mcs {
	return &mcs{
		conn:         conn,
		log:          log,
		creds:        credentials,
		heartbeatAck: make(chan bool),
		heartbeat:    heartbeat,
		done:         make(chan struct{}),
		events:       events,
	}
}

func (mcsClient *mcs) disconnect() {
	mcsClient.disconnectOnce.Do(func() {
		close(mcsClient.done)
		_ = mcsClient.conn.Close()
		select {
		case mcsClient.events <- &DisconnectedEvent{}:
		default:
		}
	})
}

func (mcsClient *mcs) SendLoginPacket(receivedPersistentIDs []string) error {
	androidID := proto.String(strconv.FormatUint(mcsClient.creds.AndroidID, 10))
	settings := []*pb.Setting{{Name: proto.String("new_vc"), Value: proto.String("1")}}
	if mcsClient.heartbeat.serverInterval > 0 {
		settings = append(settings, &pb.Setting{
			Name:  proto.String("hbping"),
			Value: proto.String(strconv.FormatInt(mcsClient.heartbeat.serverInterval.Milliseconds(), 10)),
		})
	}
	request := &pb.LoginRequest{
		AccountId:            proto.Int64(1000000),
		AuthService:          pb.LoginRequest_ANDROID_ID.Enum(),
		AuthToken:            proto.String(strconv.FormatUint(mcsClient.creds.SecurityToken, 10)),
		Id:                   proto.String(fmt.Sprintf("chrome-%s", chromeVersion)),
		Domain:               proto.String(mcsDomain),
		DeviceId:             proto.String(fmt.Sprintf("android-%s", strconv.FormatUint(mcsClient.creds.AndroidID, 16))),
		NetworkType:          proto.Int32(1),
		Resource:             androidID,
		User:                 androidID,
		UseRmq2:              proto.Bool(true),
		LastRmqId:            proto.Int64(1),
		Setting:              settings,
		AdaptiveHeartbeat:    proto.Bool(mcsClient.heartbeat.adaptive),
		ReceivedPersistentId: receivedPersistentIDs,
	}
	return mcsClient.sendRequest(tagLoginRequest, request, true)
}

func (mcsClient *mcs) SendHeartbeatPingPacket() error {
	request := &pb.HeartbeatPing{LastStreamIdReceived: proto.Int32(mcsClient.incomingStreamID.Load())}
	return mcsClient.sendRequest(tagHeartbeatPing, request, false)
}

func (mcsClient *mcs) SendHeartbeatAckPacket() error {
	request := &pb.HeartbeatAck{LastStreamIdReceived: proto.Int32(mcsClient.incomingStreamID.Load())}
	return mcsClient.sendRequest(tagHeartbeatAck, request, false)
}

func (mcsClient *mcs) SendStreamAck() error {
	stanzaType := pb.IqStanza_SET
	request := &pb.IqStanza{
		Id:                   proto.String(""),
		Type:                 &stanzaType,
		LastStreamIdReceived: proto.Int32(mcsClient.incomingStreamID.Load()),
		Extension:            &pb.Extension{Id: proto.Int32(13), Data: []byte{}},
	}
	return mcsClient.sendRequest(tagIqStanza, request, false)
}

func (mcsClient *mcs) sendRequest(tag tagType, request proto.Message, containVersion bool) error {
	header := make([]byte, 0, 100)
	if containVersion {
		header = append(header, fcmVersion, byte(tag))
	} else {
		header = append(header, byte(tag))
	}
	header = protowire.AppendVarint(header, uint64(proto.Size(request)))
	data, err := proto.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode protocol buffer data: %w", err)
	}
	_, err = mcsClient.conn.Write(append(header, data...))
	return err
}

func (mcsClient *mcs) ReceiveVersion() error {
	buffer := make([]byte, versionPacketLen)
	length, err := io.ReadFull(mcsClient.conn, buffer)
	if err != nil {
		return fmt.Errorf("receive version packet: %w", err)
	}
	if length != versionPacketLen || buffer[0] != fcmVersion {
		return fmt.Errorf("FCM version mismatch: received %d, expected %d", buffer[0], fcmVersion)
	}
	return nil
}

func (mcsClient *mcs) PerformReadTag() (any, error) {
	tag, err := mcsClient.receiveTag()
	if err != nil {
		return nil, fmt.Errorf("receive tag packet: %w", err)
	}
	size, err := mcsClient.receiveSize()
	if err != nil {
		return nil, fmt.Errorf("receive size packet: %w", err)
	}
	buffer := make([]byte, size)
	if _, err = io.ReadFull(mcsClient.conn, buffer); err != nil {
		return nil, fmt.Errorf("receive data packet: %w", err)
	}
	return mcsClient.unmarshalTagData(tag, buffer)
}

func (mcsClient *mcs) unmarshalTagData(tag tagType, buffer []byte) (any, error) {
	generator, exists := tagMapping[tag]
	if !exists {
		return nil, fmt.Errorf("unknown FCM tag: %x", tag)
	}
	received := generator()
	if err := proto.Unmarshal(buffer, received.(proto.Message)); err != nil {
		return received, fmt.Errorf("unmarshal FCM tag %x: %w", tag, err)
	}
	// Each side owns its receive counter. The peer's last_stream_id_received
	// acknowledges our outbound stream and must not replace this value.
	mcsClient.incomingStreamID.Add(1)
	if err := mcsClient.handleTag(received); err != nil {
		return received, fmt.Errorf("handle FCM tag %x: %w", tag, err)
	}
	return received, nil
}

func (mcsClient *mcs) handleTag(received any) error {
	switch received.(type) {
	case *pb.HeartbeatPing:
		mcsClient.sendHeartbeatAck()
		return mcsClient.SendHeartbeatAckPacket()
	case *pb.HeartbeatAck:
		mcsClient.sendHeartbeatAck()
	}
	return nil
}

func (mcsClient *mcs) sendHeartbeatAck() {
	select {
	case mcsClient.heartbeatAck <- true:
	case <-mcsClient.done:
	}
}

func (mcsClient *mcs) receiveTag() (tagType, error) {
	buffer := make([]byte, tagPacketLen)
	if _, err := io.ReadFull(mcsClient.conn, buffer); err != nil {
		return 0, err
	}
	return tagType(buffer[0]), nil
}

func (mcsClient *mcs) receiveSize() (int, error) {
	buffer := make([]byte, sizePacketLenMax)
	for offset := 1; offset <= sizePacketLenMax; offset++ {
		if _, err := io.ReadFull(mcsClient.conn, buffer[offset-1:offset]); err != nil {
			return 0, err
		}
		size, consumed := protowire.ConsumeVarint(buffer[:offset])
		if consumed > 0 {
			if size > maxFrameSize {
				return 0, fmt.Errorf("FCM frame size %d exceeds limit %d", size, maxFrameSize)
			}
			return int(size), nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

type tagMessageGenerator func() any

var tagMapping = map[tagType]tagMessageGenerator{
	tagHeartbeatPing:     func() any { return &pb.HeartbeatPing{} },
	tagHeartbeatAck:      func() any { return &pb.HeartbeatAck{} },
	tagLoginRequest:      func() any { return &pb.LoginRequest{} },
	tagLoginResponse:     func() any { return &pb.LoginResponse{} },
	tagClose:             func() any { return &pb.Close{} },
	tagIqStanza:          func() any { return &pb.IqStanza{} },
	tagDataMessageStanza: func() any { return &pb.DataMessageStanza{} },
	tagStreamErrorStanza: func() any { return &pb.StreamErrorStanza{} },
}
