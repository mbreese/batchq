package support

// tokens.go owns the bearer-token wire format and HMAC validation.
//
// Format:
//
//	batchq_pat_<base64url-no-pad of 32 random bytes>
//
// The "batchq_pat_" prefix makes leaked tokens obvious in logs / git
// history and lets future code distinguish token types by prefix
// (service-account tokens, refresh tokens, etc. — none of which
// exist yet). The 32-byte random body is what gives the token its
// 256 bits of unguessability.
//
// The server never stores the token bytes themselves. It stores
// HMAC-SHA256(token, master.key); on every request it recomputes
// the HMAC of the incoming token and looks the result up in
// tokens.hmac. Loss of the master.key invalidates every token at
// once — there is no decryption step, just a key-by-different-key
// comparison.

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
)

// TokenPrefix is the wire-format prefix every batchq token starts
// with. Used for routing on the server side and for visually
// distinguishing tokens from other secrets in logs.
const TokenPrefix = "batchq_pat_"

// TokenBodySize is the number of random bytes in a token (256 bits).
const TokenBodySize = 32

// ErrTokenMalformed is returned by ParseToken when the on-wire token
// does not match the expected shape (prefix + base64url body).
var ErrTokenMalformed = errors.New("token: malformed")

// TokenSigner computes HMACs of tokens for storage and lookup. It
// holds the master key by reference; callers should not mutate the
// slice after constructing the signer.
type TokenSigner struct {
	key []byte
}

// NewTokenSigner returns a signer that uses key (typically the bytes
// returned by LoadOrCreateMasterKey).
func NewTokenSigner(key []byte) *TokenSigner {
	return &TokenSigner{key: key}
}

// GenerateToken returns a freshly-generated token in wire format
// (prefix + base64-encoded random body) plus its HMAC under this
// signer's key. The caller stores the HMAC server-side and hands the
// plaintext token to the operator/user; the plaintext is the only
// place the secret exists.
func (s *TokenSigner) GenerateToken() (plaintext string, hmacBytes []byte, err error) {
	body := make([]byte, TokenBodySize)
	if _, err := io.ReadFull(rand.Reader, body); err != nil {
		return "", nil, fmt.Errorf("token: generate body: %w", err)
	}
	plaintext = TokenPrefix + base64.RawURLEncoding.EncodeToString(body)
	hmacBytes = s.HMAC(plaintext)
	return plaintext, hmacBytes, nil
}

// HMAC returns HMAC-SHA256(token, key). Used both at mint time (to
// store) and at validate time (to look up).
func (s *TokenSigner) HMAC(token string) []byte {
	mac := hmac.New(sha256.New, s.key)
	mac.Write([]byte(token))
	return mac.Sum(nil)
}

// ParseToken validates that token has the expected wire shape but
// does NOT verify it against any signer. Useful for early-rejecting
// obviously-bad inputs in the auth middleware before the DB lookup.
// Returns ErrTokenMalformed if the prefix is missing or the body
// isn't a valid base64url of TokenBodySize bytes.
func ParseToken(token string) error {
	if !strings.HasPrefix(token, TokenPrefix) {
		return fmt.Errorf("%w: missing prefix", ErrTokenMalformed)
	}
	body := token[len(TokenPrefix):]
	decoded, err := base64.RawURLEncoding.DecodeString(body)
	if err != nil {
		return fmt.Errorf("%w: body not base64url: %v", ErrTokenMalformed, err)
	}
	if len(decoded) != TokenBodySize {
		return fmt.Errorf("%w: body length %d, want %d", ErrTokenMalformed, len(decoded), TokenBodySize)
	}
	return nil
}

// BearerFromHeader extracts the token from an Authorization header
// value, returning ErrTokenMalformed if the header isn't "Bearer
// <something>". Empty header is a distinct empty return (no error)
// so callers can decide their own "no auth provided" policy.
func BearerFromHeader(header string) (string, error) {
	if header == "" {
		return "", nil
	}
	const scheme = "Bearer "
	if !strings.HasPrefix(header, scheme) {
		return "", fmt.Errorf("%w: Authorization header must start with %q", ErrTokenMalformed, scheme)
	}
	return strings.TrimSpace(header[len(scheme):]), nil
}
