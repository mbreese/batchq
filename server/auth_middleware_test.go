package server

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/mbreese/batchq/service"
	"github.com/mbreese/batchq/storage"
	"github.com/mbreese/batchq/support"
)

// newResolver opens a fresh storage and returns a configured
// authResolver plus the storage for direct introspection.
func newResolver(t *testing.T) (*authResolver, storage.Storage) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "auth.db")
	st, err := storage.Open(context.Background(), dbPath, storage.Options{})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	svc := service.New(st)
	r, err := newAuthResolver(context.Background(), svc)
	if err != nil {
		t.Fatalf("newAuthResolver: %v", err)
	}
	return r, st
}

// Two different uids over peer creds get two different implicit
// local tenants — the whole point of the per-uid mapping.
func TestAuthResolver_PerUidTenants(t *testing.T) {
	r, st := newResolver(t)
	ctx := context.Background()

	ctxAlice := support.WithPeerCreds(ctx, support.PeerCreds{Uid: 1234, Gid: 100})
	ctxBob := support.WithPeerCreds(ctx, support.PeerCreds{Uid: 5678, Gid: 200})

	aliceTenant, err := r.resolve(ctxAlice)
	if err != nil {
		t.Fatalf("resolve alice: %v", err)
	}
	bobTenant, err := r.resolve(ctxBob)
	if err != nil {
		t.Fatalf("resolve bob: %v", err)
	}
	if aliceTenant == bobTenant {
		t.Fatalf("same tenant for different uids: %s", aliceTenant)
	}

	// Both tenants exist in storage as kind=local and with the
	// expected name shape.
	for _, name := range []string{"local-uid-1234", "local-uid-5678"} {
		got, err := st.GetTenantByName(ctx, name)
		if err != nil {
			t.Fatalf("GetTenantByName %s: %v", name, err)
		}
		if got.Kind != storage.TenantKindLocal {
			t.Fatalf("%s kind: got %q, want %q", name, got.Kind, storage.TenantKindLocal)
		}
	}
}

// Repeated resolves for the same uid return the same tenant ID and
// don't create duplicate tenants. The lazy-cache must be idempotent.
func TestAuthResolver_IsCached(t *testing.T) {
	r, st := newResolver(t)
	ctx := support.WithPeerCreds(context.Background(), support.PeerCreds{Uid: 4242, Gid: 4242})

	first, err := r.resolve(ctx)
	if err != nil {
		t.Fatalf("first resolve: %v", err)
	}
	second, err := r.resolve(ctx)
	if err != nil {
		t.Fatalf("second resolve: %v", err)
	}
	if first != second {
		t.Fatalf("tenant id changed across resolves: %s -> %s", first, second)
	}

	tenants, err := st.ListTenants(context.Background())
	if err != nil {
		t.Fatalf("ListTenants: %v", err)
	}
	// Expect exactly two: "_local" (created at newAuthResolver) and
	// "local-uid-4242". Anything more means we created duplicates.
	if len(tenants) != 2 {
		names := make([]string, len(tenants))
		for i, x := range tenants {
			names[i] = x.Name
		}
		t.Fatalf("tenant count: got %d (%v), want 2", len(tenants), names)
	}
}

// Requests without peer creds fall back to the "_local" tenant. This
// is the path remote clients hit today (until Phase 3 adds bearer-
// token lookup); preserving it keeps in-process HTTP test
// transports working.
func TestAuthResolver_FallbackToLocal(t *testing.T) {
	r, _ := newResolver(t)

	got, err := r.resolve(context.Background())
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != r.localTenant {
		t.Fatalf("fallback: got %s, want %s", got, r.localTenant)
	}
}
