//go:build windows

package tumblrdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"golang.org/x/sys/windows"
)

func acquirePortalReIDFileLock(ctx context.Context, lockPath string) (func(), error) {
	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open SQLite Tumblr portal ReID lock file: %w", err)
	}

	handle := windows.Handle(file.Fd())
	flags := uint32(windows.LOCKFILE_EXCLUSIVE_LOCK | windows.LOCKFILE_FAIL_IMMEDIATELY)
	for {
		if err = ctx.Err(); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("wait for Tumblr portal ReID lock: %w", err)
		}
		err = windows.LockFileEx(handle, flags, 0, 1, 0, &windows.Overlapped{})
		if err == nil {
			var releaseOnce sync.Once
			return func() {
				releaseOnce.Do(func() {
					_ = windows.UnlockFileEx(handle, 0, 1, 0, &windows.Overlapped{})
					_ = file.Close()
				})
			}, nil
		}
		if !errors.Is(err, windows.ERROR_LOCK_VIOLATION) && !errors.Is(err, windows.ERROR_IO_PENDING) {
			_ = file.Close()
			return nil, fmt.Errorf("acquire SQLite Tumblr portal ReID lock: %w", err)
		}
		if err = waitForPortalReIDLock(ctx); err != nil {
			_ = file.Close()
			return nil, err
		}
	}
}
