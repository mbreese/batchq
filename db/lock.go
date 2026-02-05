package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mbreese/batchq/support"
	"github.com/mbreese/socketlock"
)

var socketLockPath string
var socketLockPathMu sync.RWMutex

// SetSocketLockPath sets the socket path used for sqlite locking.
func SetSocketLockPath(path string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return errors.New("socketlock path is empty")
	}
	expanded, err := support.ExpandPathAbs(path)
	if err != nil {
		return err
	}
	socketLockPathMu.Lock()
	socketLockPath = expanded
	socketLockPathMu.Unlock()
	return nil
}

func socketLockPathOrDefault() string {
	socketLockPathMu.RLock()
	if socketLockPath != "" {
		path := socketLockPath
		socketLockPathMu.RUnlock()
		return path
	}
	socketLockPathMu.RUnlock()

	expanded, err := support.ExpandPathAbs("~/.batchq/lock.sock")
	if err != nil {
		return "~/.batchq/lock.sock"
	}
	return expanded
}

func ensureSocketDir(path string) error {
	dir := filepath.Dir(path)
	if dir == "" || dir == "." {
		return fmt.Errorf("socketlock path %q does not include a directory", path)
	}
	return os.MkdirAll(dir, 0755)
}

func acquireSocketLock(ctx context.Context, path string, write bool) (*socketlock.Client, *socketlock.Lock, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(path) == "" {
		path = socketLockPathOrDefault()
	}
	if err := ensureSocketDir(path); err != nil {
		return nil, nil, err
	}

	client, err := socketlock.Connect(ctx, path, socketlock.LockConfig{
		Policy: socketlock.WriterPreferred,
	})
	if err != nil {
		return nil, nil, err
	}

	var lock *socketlock.Lock
	if write {
		lock, err = client.AcquireWrite(ctx)
	} else {
		lock, err = client.AcquireRead(ctx)
	}
	if err != nil {
		_ = client.Close()
		return nil, nil, err
	}
	return client, lock, nil
}

func releaseSocketLock(client *socketlock.Client, lock *socketlock.Lock) {
	if lock != nil {
		_ = lock.Release()
	}
	if client != nil {
		_ = client.Close()
	}
}

func lockTimeoutContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
}
