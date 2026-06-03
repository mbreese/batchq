package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

// CreateToken + GetTokenByHMAC round trip: insert, fetch by HMAC,
// confirm the returned tenant matches.
func TestTokenRoundTrip(t *testing.T) {
	s := freshStore(t)
	ctx := context.Background()

	alice, _ := s.CreateTenant(ctx, "alice", TenantKindRemote)
	hmac := []byte("fake-hmac-bytes-for-test")

	created, err := s.CreateToken(ctx, alice.ID, hmac, "alice-laptop", time.Time{})
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	if created.TenantID != alice.ID {
		t.Fatalf("tenant id: got %s, want %s", created.TenantID, alice.ID)
	}

	got, tenant, err := s.GetTokenByHMAC(ctx, hmac)
	if err != nil {
		t.Fatalf("GetTokenByHMAC: %v", err)
	}
	if got.ID != created.ID {
		t.Fatalf("token id mismatch: %s vs %s", got.ID, created.ID)
	}
	if tenant.ID != alice.ID {
		t.Fatalf("returned tenant mismatch: %s vs %s", tenant.ID, alice.ID)
	}
}

// Revoked tokens MUST surface as ErrTokenNotFound — the auth path
// treats revoked and unknown identically.
func TestRevokedTokenIsNotFound(t *testing.T) {
	s := freshStore(t)
	ctx := context.Background()

	alice, _ := s.CreateTenant(ctx, "alice", TenantKindRemote)
	hmac := []byte("hmac-to-revoke")
	tok, err := s.CreateToken(ctx, alice.ID, hmac, "", time.Time{})
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}

	if err := s.RevokeToken(ctx, tok.ID); err != nil {
		t.Fatalf("RevokeToken: %v", err)
	}

	if _, _, err := s.GetTokenByHMAC(ctx, hmac); !errors.Is(err, ErrTokenNotFound) {
		t.Fatalf("GetTokenByHMAC after revoke: got %v, want ErrTokenNotFound", err)
	}

	// Re-revoking is a no-op-as-error: matches the design that
	// callers shouldn't have to distinguish "already gone" from
	// "wasn't there to begin with".
	if err := s.RevokeToken(ctx, tok.ID); !errors.Is(err, ErrTokenNotFound) {
		t.Fatalf("double revoke: got %v, want ErrTokenNotFound", err)
	}
}

// Expired tokens also surface as ErrTokenNotFound — same reason as
// revoked.
func TestExpiredTokenIsNotFound(t *testing.T) {
	s := freshStore(t)
	ctx := context.Background()

	alice, _ := s.CreateTenant(ctx, "alice", TenantKindRemote)
	hmac := []byte("hmac-expired")
	expiresInPast := time.Now().UTC().Add(-1 * time.Hour)
	if _, err := s.CreateToken(ctx, alice.ID, hmac, "", expiresInPast); err != nil {
		t.Fatalf("CreateToken: %v", err)
	}

	if _, _, err := s.GetTokenByHMAC(ctx, hmac); !errors.Is(err, ErrTokenNotFound) {
		t.Fatalf("GetTokenByHMAC on expired: got %v, want ErrTokenNotFound", err)
	}
}

// ListTokensForTenant returns active and inactive rows, ordered by
// creation. Used for `batchq token list` so operators can see
// everything (including revoked) for an audit.
func TestListTokensForTenant_IncludesInactive(t *testing.T) {
	s := freshStore(t)
	ctx := context.Background()

	alice, _ := s.CreateTenant(ctx, "alice", TenantKindRemote)
	if _, err := s.CreateToken(ctx, alice.ID, []byte("h-1"), "active", time.Time{}); err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	tok2, _ := s.CreateToken(ctx, alice.ID, []byte("h-2"), "revoked", time.Time{})
	if err := s.RevokeToken(ctx, tok2.ID); err != nil {
		t.Fatalf("RevokeToken: %v", err)
	}

	tokens, err := s.ListTokensForTenant(ctx, alice.ID)
	if err != nil {
		t.Fatalf("ListTokensForTenant: %v", err)
	}
	if len(tokens) != 2 {
		t.Fatalf("got %d tokens, want 2", len(tokens))
	}
}

// DeleteTenant refuses to drop a tenant that still has tokens (or
// jobs). Operator must clean up first.
func TestDeleteTenantWithTokensIsRejected(t *testing.T) {
	s := freshStore(t)
	ctx := context.Background()

	alice, _ := s.CreateTenant(ctx, "alice", TenantKindRemote)
	if _, err := s.CreateToken(ctx, alice.ID, []byte("h-1"), "", time.Time{}); err != nil {
		t.Fatalf("CreateToken: %v", err)
	}

	if err := s.DeleteTenant(ctx, alice.ID); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("DeleteTenant: got %v, want ErrInvalidState", err)
	}
}
