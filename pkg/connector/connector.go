package connector

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/id"

	"github.com/ifixrobots/tumblr-dms/pkg/connector/tumblrdb"
)

type TumblrConnector struct {
	Bridge                  *bridgev2.Bridge
	Config                  Config
	DB                      *tumblrdb.Database
	portalMutationLock      sync.Mutex
	outboundSubmissionLocks sync.Map
	maxFileSize             atomic.Int64
}

var _ bridgev2.NetworkConnector = (*TumblrConnector)(nil)
var _ bridgev2.MaxFileSizeingNetwork = (*TumblrConnector)(nil)

func (tc *TumblrConnector) SetMaxFileSize(maxSize int64) {
	tc.maxFileSize.Store(maxSize)
}

func (tc *TumblrConnector) Init(bridge *bridgev2.Bridge) {
	tc.Bridge = bridge
	tc.DB = tumblrdb.New(
		bridge.ID,
		bridge.DB.Database,
	)
}

func (tc *TumblrConnector) Start(ctx context.Context) error {
	if err := tc.DB.Initialize(ctx); err != nil {
		return err
	}
	return nil
}

func (tc *TumblrConnector) GetName() bridgev2.BridgeName {
	return bridgev2.BridgeName{
		DisplayName:          "Tumblr",
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
