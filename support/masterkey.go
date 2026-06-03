package support

// masterkey.go owns the lifecycle of $BATCHQ_HOME/master.key — the
// HMAC secret used to sign and verify bearer tokens. The key is 32
// random bytes (crypto/rand), generated on first server start, and
// must be readable only by the user the server runs as.
//
// Operationally the key is high-value (anyone with it can forge any
// token), but it doesn't need rotation or escrow in v1: regenerating
// the key invalidates every existing token, which the operator can
// re-mint with the new key.

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// MasterKeyName is the file name (under $BATCHQ_HOME) that holds the
// HMAC secret.
const MasterKeyName = "master.key"

// MasterKeySize is the secret length (256 bits). HMAC-SHA256 doesn't
// gain meaningful security past 32 bytes; the constant is fixed so a
// caller swapping it later will need a migration story.
const MasterKeySize = 32

// ErrMasterKeyInsecurePerms is returned by LoadOrCreateMasterKey when
// an existing key file has permissions wider than 0600. The server
// refuses to use the key in this state — a leaked key on a shared
// host would let anyone with read access forge tokens.
var ErrMasterKeyInsecurePerms = errors.New("master key has insecure permissions; must be 0600")

// LoadOrCreateMasterKey returns the master key, creating it if no
// file exists at path. The created file is mode 0600. An existing
// file with looser permissions is rejected (ErrMasterKeyInsecurePerms)
// rather than silently used or tightened — operator intervention is
// the right answer when the perms drift.
//
// The returned slice has length MasterKeySize. Pass to NewTokenSigner
// (in tokens.go) to start signing/verifying.
func LoadOrCreateMasterKey(path string) ([]byte, error) {
	info, err := os.Stat(path)
	if err == nil {
		if info.Mode().Perm()&0o077 != 0 {
			return nil, fmt.Errorf("%w: %s mode is %#o", ErrMasterKeyInsecurePerms, path, info.Mode().Perm())
		}
		key, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("master key: read %s: %w", path, err)
		}
		if len(key) != MasterKeySize {
			return nil, fmt.Errorf("master key: %s wrong length (%d, want %d) — delete to regenerate", path, len(key), MasterKeySize)
		}
		return key, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("master key: stat %s: %w", path, err)
	}

	// Create.
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("master key: mkdir %s: %w", dir, err)
		}
	}
	key := make([]byte, MasterKeySize)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, fmt.Errorf("master key: generate: %w", err)
	}
	// Write with O_EXCL to avoid clobbering a key created in a race.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		// If another process won the race, just read theirs.
		if errors.Is(err, os.ErrExist) {
			return LoadOrCreateMasterKey(path)
		}
		return nil, fmt.Errorf("master key: create %s: %w", path, err)
	}
	if _, err := f.Write(key); err != nil {
		f.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("master key: write %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("master key: close %s: %w", path, err)
	}
	return key, nil
}
