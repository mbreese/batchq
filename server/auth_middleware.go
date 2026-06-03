package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/mbreese/batchq/api"
	"github.com/mbreese/batchq/service"
	"github.com/mbreese/batchq/storage"
	"github.com/mbreese/batchq/support"
)

// ErrUnauthorized is what the auth middleware surfaces when a bearer
// token is present but invalid (malformed, unknown, revoked, or
// expired). All four cases collapse to one error so an attacker
// probing tokens can't learn which one applies.
var ErrUnauthorized = errors.New("server: unauthorized")

// authResolver turns an inbound request into a tenant ID. Priority:
//
//  1. Authorization: Bearer <tok> → HMAC-verify against master.key,
//     look up tokens.hmac, return tokens.tenant_id. Invalid bearer →
//     401 (no fallback). Phase 3.
//  2. Peer credentials on a unix socket → per-uid implicit local
//     tenant (lazily created and cached). Phase 2.
//  3. Neither → fall through to a shared "_local" tenant for
//     in-process HTTP test transports that don't go through a real
//     socket. Production HTTPS clients always go through step 1.
//
// The cache is keyed by tenant name (not uid) so the same code path
// serves both peer-cred and bearer-token lookups.
type authResolver struct {
	svc         *service.Service
	signer      *support.TokenSigner
	localTenant string // ID of the "_local" fallback tenant
	cache       sync.Map
}

// newAuthResolver provisions the "_local" fallback up front and
// constructs a TokenSigner from the master key.
func newAuthResolver(ctx context.Context, svc *service.Service, masterKey []byte) (*authResolver, error) {
	tenant, err := svc.Store().EnsureLocalTenant(ctx, "_local")
	if err != nil {
		return nil, fmt.Errorf("server: ensure _local tenant: %w", err)
	}
	a := &authResolver{
		svc:         svc,
		signer:      support.NewTokenSigner(masterKey),
		localTenant: tenant.ID,
	}
	a.cache.Store("_local", tenant.ID)
	return a, nil
}

// resolve returns the tenant ID for the request. A bearer token, if
// present, takes priority and is authoritative — there's no fallback
// to peer creds or _local if the token is bad.
func (a *authResolver) resolve(ctx context.Context, header string) (string, error) {
	tokenStr, err := support.BearerFromHeader(header)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrUnauthorized, err)
	}
	if tokenStr != "" {
		return a.resolveBearer(ctx, tokenStr)
	}
	if peer, ok := support.PeerCredsFromContext(ctx); ok {
		return a.ensureLocalForUid(ctx, peer.Uid)
	}
	return a.localTenant, nil
}

func (a *authResolver) resolveBearer(ctx context.Context, token string) (string, error) {
	if err := support.ParseToken(token); err != nil {
		return "", fmt.Errorf("%w: %v", ErrUnauthorized, err)
	}
	hmacBytes := a.signer.HMAC(token)
	_, tenant, err := a.svc.Store().GetTokenByHMAC(ctx, hmacBytes)
	if err != nil {
		if errors.Is(err, storage.ErrTokenNotFound) {
			return "", ErrUnauthorized
		}
		return "", err
	}
	return tenant.ID, nil
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

// withAuth runs the resolver on every request and stashes the
// resulting tenant ID in the context. Failures map to 401 (bad
// credential) or 500 (DB unreachable).
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
		tenantID, err := s.auth.resolve(r.Context(), r.Header.Get(api.HeaderAuthorization))
		if err != nil {
			if errors.Is(err, ErrUnauthorized) {
				writeError(w, http.StatusUnauthorized, err)
				return
			}
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		ctx := support.WithTenant(r.Context(), support.TenantID(tenantID))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
