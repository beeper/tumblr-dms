//go:build android || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package tumblrdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

func acquirePortalReIDFileLock(ctx context.Context, lockPath string) (func(), error) {
	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open SQLite Tumblr portal ReID lock file: %w", err)
	}

	for {
		if err = ctx.Err(); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("wait for Tumblr portal ReID lock: %w", err)
		}
		err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			var releaseOnce sync.Once
			return func() {
				releaseOnce.Do(func() {
					_ = unix.Flock(int(file.Fd()), unix.LOCK_UN)
					_ = file.Close()
				})
			}, nil
		}
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			_ = file.Close()
			return nil, fmt.Errorf("acquire SQLite Tumblr portal ReID lock: %w", err)
		}
		if err = waitForPortalReIDLock(ctx); err != nil {
			_ = file.Close()
			return nil, err
		}
	}
}
