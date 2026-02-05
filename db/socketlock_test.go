package db

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mbreese/socketlock"
)

func newSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "socketlock-")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})
	return filepath.Join(dir, "lock.sock")
}

func TestSocketLockMultipleReaders(t *testing.T) {
	socketPath := newSocketPath(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	client1, err := socketlock.Connect(ctx, socketPath, socketlock.LockConfig{Policy: socketlock.ReaderPreferred})
	if err != nil {
		t.Fatalf("connect client1: %v", err)
	}
	defer client1.Close()

	client2, err := socketlock.Connect(ctx, socketPath, socketlock.LockConfig{Policy: socketlock.ReaderPreferred})
	if err != nil {
		t.Fatalf("connect client2: %v", err)
	}
	defer client2.Close()

	ready := make(chan *socketlock.Lock, 2)
	errs := make(chan error, 2)

	go func() {
		lock, err := client1.AcquireRead(ctx)
		if err != nil {
			errs <- err
			return
		}
		ready <- lock
	}()
	go func() {
		lock, err := client2.AcquireRead(ctx)
		if err != nil {
			errs <- err
			return
		}
		ready <- lock
	}()

	var locks []*socketlock.Lock
	for len(locks) < 2 {
		select {
		case err := <-errs:
			t.Fatalf("acquire read: %v", err)
		case lock := <-ready:
			locks = append(locks, lock)
		case <-ctx.Done():
			t.Fatalf("timeout waiting for read locks: %v", ctx.Err())
		}
	}

	for _, lock := range locks {
		if err := lock.Release(); err != nil {
			t.Fatalf("release read lock: %v", err)
		}
	}
	_ = os.Remove(socketPath)
}

func TestSocketLockWriterBlocksReaders(t *testing.T) {
	socketPath := newSocketPath(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	readerClient, err := socketlock.Connect(ctx, socketPath, socketlock.LockConfig{Policy: socketlock.WriterPreferred})
	if err != nil {
		t.Fatalf("connect reader client: %v", err)
	}
	defer readerClient.Close()

	writerClient, err := socketlock.Connect(ctx, socketPath, socketlock.LockConfig{Policy: socketlock.WriterPreferred})
	if err != nil {
		t.Fatalf("connect writer client: %v", err)
	}
	defer writerClient.Close()

	readLock, err := readerClient.AcquireRead(ctx)
	if err != nil {
		t.Fatalf("acquire read: %v", err)
	}

	acquired := make(chan *socketlock.Lock, 1)
	go func() {
		lock, err := writerClient.AcquireWrite(ctx)
		if err != nil {
			t.Errorf("acquire write: %v", err)
			return
		}
		acquired <- lock
	}()

	select {
	case <-acquired:
		t.Fatalf("writer acquired while reader lock held")
	case <-time.After(200 * time.Millisecond):
	}

	if err := readLock.Release(); err != nil {
		t.Fatalf("release read: %v", err)
	}

	select {
	case lock := <-acquired:
		if err := lock.Release(); err != nil {
			t.Fatalf("release write: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("timeout waiting for writer lock: %v", ctx.Err())
	}
	_ = os.Remove(socketPath)
}
