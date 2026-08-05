//go:build !android && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package tumblrdb

import (
	"context"
	"fmt"
)

func acquirePortalReIDFileLock(context.Context, string) (func(), error) {
	return nil, fmt.Errorf("SQLite Tumblr portal ReID locking is unsupported on this operating system")
}
