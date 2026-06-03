package connector

import (
	"context"
	"net/http"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/id"
)

type TumblrConnector struct {
	Bridge *bridgev2.Bridge
	Config Config
}

var _ bridgev2.NetworkConnector = (*TumblrConnector)(nil)

func (tc *TumblrConnector) Init(bridge *bridgev2.Bridge) {
	tc.Bridge = bridge
}

func (tc *TumblrConnector) Start(context.Context) error { return nil }

func (tc *TumblrConnector) GetName() bridgev2.BridgeName {
	return bridgev2.BridgeName{
		DisplayName:          "Tumblr DMs",
		NetworkURL:           "https://www.tumblr.com",
		NetworkIcon:          id.ContentURIString(""),
		NetworkID:            "tumblrdms",
		BeeperBridgeType:     "tumblrdms",
		DefaultPort:          29341,
		DefaultCommandPrefix: "!tumblr",
	}
}

func (tc *TumblrConnector) newHTTPClient() *http.Client {
	timeout := (*Config)(nil).RequestTimeout()
	if tc != nil {
		timeout = tc.Config.RequestTimeout()
	}
	return &http.Client{Timeout: timeout}
}
