package connector

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/id"

	"github.com/beeper/tumblr-dms/pkg/connector/tumblrdb"
	"github.com/beeper/tumblr-dms/pkg/tumblr"
)

type TumblrConnector struct {
	Bridge                  *bridgev2.Bridge
	Config                  Config
	DB                      *tumblrdb.Database
	portalMutationLock      sync.Mutex
	outboundSubmissionLocks sync.Map
	maxFileSize             atomic.Int64
	directMedia             bool
	directMediaClient       *tumblr.Client
}

var _ bridgev2.NetworkConnector = (*TumblrConnector)(nil)
var _ bridgev2.MaxFileSizeingNetwork = (*TumblrConnector)(nil)

func (tc *TumblrConnector) SetMaxFileSize(maxSize int64) {
	tc.maxFileSize.Store(maxSize)
}

func (tc *TumblrConnector) Init(bridge *bridgev2.Bridge) {
	tc.Bridge = bridge
	tc.directMediaClient = tumblr.NewClient(tumblr.Options{HTTPClient: tc.newHTTPClient(), UserAgent: tc.Config.BrowserUserAgent()})
	tc.DB = tumblrdb.New(
		bridge.ID,
		bridge.DB.Database,
		bridge.Log.With().Str("db_section", "tumblr").Logger(),
	)
}

func (tc *TumblrConnector) Start(ctx context.Context) error {
	if err := tc.DB.Upgrade(ctx); err != nil {
		return bridgev2.DBUpgradeError{Err: err, Section: "tumblr"}
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
	if proxy := strings.TrimSpace(getAndroidProp("debug.beeper.tumblr_proxy")); proxy != "" {
		if u, err := url.Parse(proxy); err == nil {
			return &http.Client{Timeout: timeout, Transport: &http.Transport{Proxy: http.ProxyURL(u), ForceAttemptHTTP2: true}}
		}
	}
	return &http.Client{Timeout: timeout}
}
