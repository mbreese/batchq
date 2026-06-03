package support

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"strings"
	"testing"
)

func randKey(t *testing.T) []byte {
	t.Helper()
	k := make([]byte, MasterKeySize)
	if _, err := io.ReadFull(rand.Reader, k); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return k
}

func TestGenerateAndHMAC_RoundTrip(t *testing.T) {
	s := NewTokenSigner(randKey(t))

	tok, hmac1, err := s.GenerateToken()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if !strings.HasPrefix(tok, TokenPrefix) {
		t.Fatalf("token missing prefix: %s", tok)
	}
	hmac2 := s.HMAC(tok)
	if !bytes.Equal(hmac1, hmac2) {
		t.Fatalf("HMAC mismatch on the same token")
	}
}

// Two different signers (different master keys) MUST produce
// different HMACs for the same token. This is what makes leaking the
// master key invalidate every existing token.
func TestHMACChangesWithKey(t *testing.T) {
	s1 := NewTokenSigner(randKey(t))
	s2 := NewTokenSigner(randKey(t))

	tok, _, err := s1.GenerateToken()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if bytes.Equal(s1.HMAC(tok), s2.HMAC(tok)) {
		t.Fatalf("HMAC unchanged across keys")
	}
}

func TestParseToken_AcceptsGeneratedTokens(t *testing.T) {
	s := NewTokenSigner(randKey(t))
	tok, _, err := s.GenerateToken()
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if err := ParseToken(tok); err != nil {
		t.Fatalf("ParseToken on freshly-generated: %v", err)
	}
}

func TestParseToken_RejectsMalformed(t *testing.T) {
	cases := []string{
		"",
		"not-a-token",
		"batchq_pat_",                                  // empty body
		"batchq_pat_$$$bad$$$",                         // bad base64
		"batchq_pat_AAAA",                              // wrong length
		"BATCHQ_PAT_" + strings.Repeat("A", 43),        // wrong prefix case
		"bearer batchq_pat_" + strings.Repeat("A", 43), // has scheme prefix
	}
	for _, c := range cases {
		if err := ParseToken(c); !errors.Is(err, ErrTokenMalformed) {
			t.Errorf("ParseToken(%q): got %v, want ErrTokenMalformed", c, err)
		}
	}
}

func TestBearerFromHeader(t *testing.T) {
	cases := []struct {
		header  string
		want    string
		wantErr bool
	}{
		{"", "", false},
		{"Bearer batchq_pat_abc", "batchq_pat_abc", false},
		{"Bearer    spaced", "spaced", false},
		{"bearer lowercase", "", true},  // scheme is case-sensitive in this impl
		{"Basic dXNlcjpwYXNz", "", true},
		{"batchq_pat_abc", "", true}, // missing scheme
	}
	for _, c := range cases {
		got, err := BearerFromHeader(c.header)
		if c.wantErr {
			if err == nil {
				t.Errorf("BearerFromHeader(%q): want error, got %q", c.header, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("BearerFromHeader(%q): unexpected error %v", c.header, err)
			continue
		}
		if got != c.want {
			t.Errorf("BearerFromHeader(%q): got %q, want %q", c.header, got, c.want)
		}
	}
}
