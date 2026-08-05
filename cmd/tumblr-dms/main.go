package main

import (
	"fmt"
	"os"

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
	if handled, exitCode := runSQLiteOwnershipRepairCLI(os.Args[1:]); handled {
		if exitCode != 0 {
			os.Exit(exitCode)
		}
		return
	}
	bridgeMain.InitVersion(Tag, Commit, BuildTime)
	bridgeMain.PreInit()
	if err := secureRuntimeFiles(&bridgeMain); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Refusing to start: could not secure Tumblr DMs runtime files: %v\n", err)
		os.Exit(15)
	}
	bridgeMain.Init()
	bridgeMain.Start()
	exitCode := bridgeMain.WaitForInterrupt()
	bridgeMain.Stop()
	os.Exit(exitCode)
}
