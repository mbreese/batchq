package support

import "context"

// TenantID identifies a logical queue owner. It corresponds to
// storage.Tenant.ID and is the value the service layer uses to scope
// every storage call.
type TenantID string

// String returns the underlying ID string.
func (t TenantID) String() string { return string(t) }

type tenantKey struct{}

// WithTenant returns a child context carrying the given tenant ID.
// The auth middleware sets this on every request after resolving the
// caller's identity (via peer creds on a unix socket or bearer token
// on HTTPS). The service layer reads it via TenantFromContext.
func WithTenant(ctx context.Context, id TenantID) context.Context {
	return context.WithValue(ctx, tenantKey{}, id)
}

// TenantFromContext returns the tenant previously stashed in ctx,
// plus a flag indicating presence. ok=false means the request was
// not authenticated (or the call is from an internal code path that
// hasn't been threaded yet) — the service layer treats that as a
// programmer error and returns service.ErrUnauthenticated.
func TenantFromContext(ctx context.Context) (TenantID, bool) {
	v, ok := ctx.Value(tenantKey{}).(TenantID)
	return v, ok
}
