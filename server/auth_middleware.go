package server

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/mbreese/batchq/service"
	"github.com/mbreese/batchq/support"
)

// authResolver turns an inbound request into a tenant ID. The Phase 2
// shape: peer credentials on a unix socket get a per-uid implicit
// local tenant (lazily created and cached); requests without peer
// credentials fall through to a shared "_local" tenant. Phase 3 adds
// Authorization: Bearer validation in front of the fallback so remote
// clients get their real per-tenant identity instead of bucketing
// into _local.
//
// The cache is keyed by tenant name (not uid) so the same code path
// serves the bearer-token case once it lands — the resolver just
// changes its mind about *which* name to look up.
type authResolver struct {
	svc          *service.Service
	localTenant  string // ID of the "_local" fallback tenant, populated at New()
	cache        sync.Map
}

// newAuthResolver provisions the "_local" fallback up front so the
// fallback path doesn't hit the database on every request, and the
// first peer-cred request for any uid doesn't pay a database round
// trip for the fallback case.
func newAuthResolver(ctx context.Context, svc *service.Service) (*authResolver, error) {
	tenant, err := svc.Store().EnsureLocalTenant(ctx, "_local")
	if err != nil {
		return nil, fmt.Errorf("server: ensure _local tenant: %w", err)
	}
	a := &authResolver{svc: svc, localTenant: tenant.ID}
	a.cache.Store("_local", tenant.ID)
	return a, nil
}

// resolve returns the tenant ID for ctx. A missing peer cred is not
// an error — it just means "fall back to _local" until Phase 3 makes
// bearer tokens authoritative for the remote path.
func (a *authResolver) resolve(ctx context.Context) (string, error) {
	if peer, ok := support.PeerCredsFromContext(ctx); ok {
		return a.ensureLocalForUid(ctx, peer.Uid)
	}
	return a.localTenant, nil
}

func (a *authResolver) ensureLocalForUid(ctx context.Context, uid uint32) (string, error) {
	name := fmt.Sprintf("local-uid-%d", uid)
	if v, ok := a.cache.Load(name); ok {
		return v.(string), nil
	}
	tenant, err := a.svc.Store().EnsureLocalTenant(ctx, name)
	if err != nil {
		return "", fmt.Errorf("server: ensure tenant %s: %w", name, err)
	}
	// Race: another goroutine may have already cached this name.
	// LoadOrStore lets the loser drop its result; both winners have
	// the same tenant ID anyway thanks to EnsureLocalTenant being
	// idempotent.
	actual, _ := a.cache.LoadOrStore(name, tenant.ID)
	return actual.(string), nil
}

// withAuth is the middleware that runs the resolver on every request
// and stashes the resulting tenant ID in the context. A resolver
// error returns 500 — the only failure mode in Phase 2 is the DB
// being unreachable, which is genuinely server-side. Once bearer
// tokens land, invalid-credential failures will surface as 401 here.
func (s *Server) withAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Health checks are unauthenticated by design — reverse
		// proxies and load balancers must be able to probe the
		// server without provisioning credentials. They don't need
		// a tenant either; the handler doesn't read one.
		if r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}
		tenantID, err := s.auth.resolve(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, fmt.Errorf("%w: resolve tenant: %v", service.ErrBadRequest, err))
			return
		}
		ctx := support.WithTenant(r.Context(), support.TenantID(tenantID))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
