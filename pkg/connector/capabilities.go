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
			CreateDM:       true,
			Search:         true,
		},
	},
}

var roomCaps = event.RoomFeatures{
	ID: "com.ifixrobots.tumblr_dms.capabilities.2026_05_29_media_gif",
	File: event.FileFeatureMap{
		event.MsgImage: {
			MimeTypes: map[string]event.CapabilitySupportLevel{
				"image/*": event.CapLevelFullySupported,
			},
			Caption: event.CapLevelDropped,
			MaxSize: tumblr.DefaultMaxUploadBytes,
		},
		event.CapMsgSticker: {
			MimeTypes: map[string]event.CapabilitySupportLevel{
				"image/*": event.CapLevelFullySupported,
			},
			Caption: event.CapLevelDropped,
			MaxSize: tumblr.DefaultMaxUploadBytes,
		},
		event.CapMsgGIF: {
			MimeTypes: map[string]event.CapabilitySupportLevel{
				"image/gif": event.CapLevelFullySupported,
			},
			Caption: event.CapLevelDropped,
			MaxSize: tumblr.DefaultMaxUploadBytes,
		},
	},
	MaxTextLength: MaxTextLength,
	ReadReceipts:  true,
	DeleteChat:    true,
}

func (tc *TumblrConnector) GetCapabilities() *bridgev2.NetworkGeneralCapabilities {
	caps := generalCaps
	caps.Provisioning.GroupCreation = maps.Clone(caps.Provisioning.GroupCreation)
	return &caps
}

func (tc *TumblrConnector) GetBridgeInfoVersion() (info, capabilities int) {
	return 1, 5
}

func (tc *TumblrClient) GetCapabilities(context.Context, *bridgev2.Portal) *event.RoomFeatures {
	return roomCaps.Clone()
}
