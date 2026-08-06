package main

import (
	"maunium.net/go/mautrix/bridgev2/matrix/mxmain"

	"github.com/ifixrobots/tumblr-dms/pkg/connector"
)

// These are filled at build time with -X linker flags.
var (
	Tag       = "unknown"
	Commit    = "unknown"
	BuildTime = "unknown"
)

var bridgeMain = mxmain.BridgeMain{
	Name:        "tumblr-dms",
	URL:         "https://github.com/beeper/tumblr-dms",
	Description: "A Matrix-Tumblr DMs puppeting bridge.",
	Version:     "0.1.1",
	Connector:   &connector.TumblrConnector{},
}

func main() {
	setSecureUmask()
	bridgeMain.InitVersion(Tag, Commit, BuildTime)
	bridgeMain.Run()
}
