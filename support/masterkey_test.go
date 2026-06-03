package support

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// First call creates the file with mode 0600; second call returns the
// same bytes (idempotent).
func TestLoadOrCreateMasterKey_CreatesAndReuses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "master.key")

	k1, err := LoadOrCreateMasterKey(path)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	if len(k1) != MasterKeySize {
		t.Fatalf("key length: got %d, want %d", len(k1), MasterKeySize)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("perm: got %#o, want 0600", perm)
	}

	k2, err := LoadOrCreateMasterKey(path)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if string(k1) != string(k2) {
		t.Fatalf("key changed across calls")
	}
}

// An existing key file with looser perms is rejected — operator
// intervention is the right answer when perms drift.
func TestLoadOrCreateMasterKey_RejectsLoosePerms(t *testing.T) {
	path := filepath.Join(t.TempDir(), "master.key")
	if err := os.WriteFile(path, make([]byte, MasterKeySize), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := LoadOrCreateMasterKey(path)
	if !errors.Is(err, ErrMasterKeyInsecurePerms) {
		t.Fatalf("got %v, want ErrMasterKeyInsecurePerms", err)
	}
}
