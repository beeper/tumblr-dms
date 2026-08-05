package connector

import (
	"context"
	"maps"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"github.com/ifixrobots/tumblr-dms/pkg/tumblr"
)

const MaxTextLength = tumblr.MaxMessageTextRunes

var generalCaps = bridgev2.NetworkGeneralCapabilities{
	Provisioning: bridgev2.ProvisioningCapabilities{
		ResolveIdentifier: bridgev2.ResolveIdentifierCapabilities{
			LookupUsername: true,
			CreateDM:       false,
			Search:         true,
		},
	},
}

var roomCaps = event.RoomFeatures{
	ID: "com.ifixrobots.tumblr_dms.capabilities.2026_08_04_media_hardening",
	File: event.FileFeatureMap{
		event.MsgImage: {
			MimeTypes: map[string]event.CapabilitySupportLevel{
				"image/jpeg": event.CapLevelFullySupported,
				"image/png":  event.CapLevelFullySupported,
				"image/webp": event.CapLevelFullySupported,
				"image/gif":  event.CapLevelPartialSupport,
			},
			Caption: event.CapLevelDropped,
			MaxSize: tumblr.DefaultMaxUploadBytes,
		},
		event.CapMsgSticker: {
			MimeTypes: map[string]event.CapabilitySupportLevel{
				"image/jpeg": event.CapLevelPartialSupport,
				"image/png":  event.CapLevelPartialSupport,
				"image/webp": event.CapLevelPartialSupport,
				"image/gif":  event.CapLevelPartialSupport,
			},
			Caption: event.CapLevelDropped,
			MaxSize: tumblr.DefaultMaxUploadBytes,
		},
		event.CapMsgGIF: {
			MimeTypes: map[string]event.CapabilitySupportLevel{
				"image/gif": event.CapLevelPartialSupport,
			},
			Caption: event.CapLevelDropped,
			MaxSize: tumblr.DefaultMaxUploadBytes,
		},
	},
	MaxTextLength:       MaxTextLength,
	LocationMessage:     event.CapLevelRejected,
	Poll:                event.CapLevelRejected,
	Thread:              event.CapLevelDropped,
	Reply:               event.CapLevelDropped,
	Edit:                event.CapLevelRejected,
	Delete:              event.CapLevelRejected,
	Reaction:            event.CapLevelRejected,
	ReadReceipts:        true,
	TypingNotifications: false,
	DeleteChat:          true,
}

func (tc *TumblrConnector) GetCapabilities() *bridgev2.NetworkGeneralCapabilities {
	caps := generalCaps
	caps.Provisioning.GroupCreation = maps.Clone(caps.Provisioning.GroupCreation)
	return &caps
}

func (tc *TumblrConnector) GetBridgeInfoVersion() (info, capabilities int) {
	return 1, 7
}

func (tc *TumblrClient) GetCapabilities(context.Context, *bridgev2.Portal) *event.RoomFeatures {
	return roomCaps.Clone()
}
