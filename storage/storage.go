// Package storage defines the persistence interface for batchq and a SQLite
// implementation. The interface is the server-internal contract over which a
// future Postgres backend can be swapped in.
package storage

import (
	"context"
	"errors"
	"time"

	"github.com/mbreese/batchq/jobs"
)

// ErrJobNotFound is returned when a job ID does not exist OR exists
// but belongs to a different tenant. The two cases are deliberately
// indistinguishable so cross-tenant access doesn't leak job existence.
var ErrJobNotFound = errors.New("job not found")

// ErrInvalidState is returned when an operation is not valid for the job's
// current state (e.g. ending a job that isn't running).
var ErrInvalidState = errors.New("invalid state for operation")

// ErrTenantNotFound is returned when a tenant ID or name does not exist.
var ErrTenantNotFound = errors.New("tenant not found")

// ErrTenantExists is returned by CreateTenant when a tenant with the
// requested name already exists.
var ErrTenantExists = errors.New("tenant already exists")

// TenantKind identifies how a tenant authenticates. Local tenants are
// implicit, created at first request from a unix-socket peer-cred uid
// and never carry bearer tokens. Remote tenants are operator-created
// and reach the server via bearer tokens.
type TenantKind string

const (
	TenantKindLocal  TenantKind = "local"
	TenantKindRemote TenantKind = "remote"
)

// Tenant is a logical queue owner.
type Tenant struct {
	ID        string
	Name      string
	Kind      TenantKind
	CreatedAt time.Time
}

// Limits constrains which queued jobs a runner is willing to claim. A value
// of -1 means "no limit on this dimension".
type Limits struct {
	MaxProcs       int
	MaxMemoryMB    int
	MaxWalltimeSec int
}

// ClaimResult is the outcome of a ClaimNextJob call.
type ClaimResult struct {
	// Job is the claimed job (already transitioned to RUNNING) or nil if
	// nothing was claimed.
	Job *jobs.JobDef
	// MoreEligible is true if there are QUEUED jobs in the database that did
	// not fit the supplied limits. The runner can use this to decide whether
	// to keep polling soon vs sleep longer.
	MoreEligible bool
}

// ResolveResult summarizes a ResolveDependencies pass.
type ResolveResult struct {
	Promoted int // jobs moved WAITING -> QUEUED
	Canceled int // jobs canceled due to failed/canceled parents
}

// Storage is the persistence contract used by the batchq server. All
// job-scoped methods take an explicit tenantID so the storage layer
// enforces tenant isolation: a jobID that exists but belongs to a
// different tenant returns ErrJobNotFound (cross-tenant access does
// not leak existence). All methods return errors instead of panicking;
// the server must not crash on bad input.
type Storage interface {
	// Close releases any resources. Safe to call multiple times.
	Close() error

	// --- Tenant management ------------------------------------------

	// CreateTenant inserts a new tenant with the given name and kind.
	// Returns ErrTenantExists if the name is already taken.
	CreateTenant(ctx context.Context, name string, kind TenantKind) (*Tenant, error)

	// GetTenantByName returns the tenant with the given name.
	// Returns ErrTenantNotFound if no such tenant exists.
	GetTenantByName(ctx context.Context, name string) (*Tenant, error)

	// GetTenantByID returns the tenant with the given id.
	// Returns ErrTenantNotFound if no such tenant exists.
	GetTenantByID(ctx context.Context, id string) (*Tenant, error)

	// ListTenants returns every tenant in the system, ordered by name.
	ListTenants(ctx context.Context) ([]*Tenant, error)

	// EnsureLocalTenant returns the local tenant with the given name,
	// creating it (kind=local) if it does not exist. Used by the auth
	// middleware to lazily provision a tenant for every peer-cred uid
	// it sees for the first time.
	EnsureLocalTenant(ctx context.Context, name string) (*Tenant, error)

	// --- Job operations ---------------------------------------------

	// InsertJob persists a new job under tenantID. The caller is
	// responsible for setting job.JobId (UUID); InsertJob will compute
	// the initial status from the dependency list (QUEUED if no deps,
	// WAITING otherwise, USERHOLD if the caller set it explicitly).
	// Dependencies must belong to the same tenant or the call errors.
	InsertJob(ctx context.Context, tenantID string, job *jobs.JobDef) error

	// GetJob loads one job (with details, running details, and deps) by
	// ID, scoped to tenantID. Returns ErrJobNotFound if the job does
	// not exist or belongs to a different tenant.
	GetJob(ctx context.Context, tenantID, jobID string) (*jobs.JobDef, error)

	// ListJobs returns every job in tenantID, optionally restricted to
	// active statuses only. sortByStatus controls the ORDER BY clause.
	ListJobs(ctx context.Context, tenantID string, showAll, sortByStatus bool) ([]*jobs.JobDef, error)

	// ListJobsByStatus returns jobs in tenantID whose status is in the
	// given set.
	ListJobsByStatus(ctx context.Context, tenantID string, statuses []jobs.StatusCode, sortByStatus bool) ([]*jobs.JobDef, error)

	// SearchJobs returns jobs in tenantID whose ID, name, or script
	// content matches the query (substring). If statuses is non-empty,
	// results are further restricted to those statuses.
	SearchJobs(ctx context.Context, tenantID, query string, statuses []jobs.StatusCode) ([]*jobs.JobDef, error)

	// GetJobDependents returns the IDs of jobs in tenantID that depend
	// on the given job.
	GetJobDependents(ctx context.Context, tenantID, jobID string) ([]string, error)

	// GetJobStatusCounts returns a count of jobs per status in
	// tenantID. If !showAll, only active statuses (<= RUNNING) are
	// counted.
	GetJobStatusCounts(ctx context.Context, tenantID string, showAll bool) (map[jobs.StatusCode]int, error)

	// GetQueueJobs returns a minimal-detail listing suitable for the
	// queue view in tenantID. Includes deps and a small subset of
	// job_details / running_details.
	GetQueueJobs(ctx context.Context, tenantID string, showAll, sortByStatus bool) ([]*jobs.JobDef, error)

	// GetProxyJobs returns all jobs in tenantID currently in
	// PROXYQUEUED state. Used by the slurm runner to reconcile SLURM
	// state.
	GetProxyJobs(ctx context.Context, tenantID string) ([]*jobs.JobDef, error)

	// ResolveDependencies promotes WAITING jobs in tenantID whose deps
	// have all succeeded (or proxied) to QUEUED, and cancels WAITING
	// jobs whose parents failed/canceled. Should be called periodically
	// by the service layer.
	ResolveDependencies(ctx context.Context, tenantID string) (ResolveResult, error)

	// ClaimNextJob atomically transitions a single QUEUED job in
	// tenantID that fits the supplied limits to RUNNING, recording
	// runnerID and kind as the owner. Returns the claimed job along
	// with MoreEligible indicating whether further QUEUED jobs (that
	// did not fit the limits) remain.
	ClaimNextJob(ctx context.Context, tenantID, runnerID, kind string, limits Limits) (ClaimResult, error)

	// MarkJobProxied transitions a RUNNING job (claimed by runnerID)
	// to PROXYQUEUED and merges runningDetails. Used by the slurm
	// runner after a successful sbatch.
	MarkJobProxied(ctx context.Context, tenantID, jobID, runnerID string, runningDetails map[string]string) error

	// UpdateRunningDetails upserts key/value pairs into a job's running
	// details. Used for heartbeats and slurm state.
	UpdateRunningDetails(ctx context.Context, tenantID, jobID string, details map[string]string) error

	// EndJob transitions a RUNNING job to SUCCESS or FAILED based on
	// returnCode. On failure, dependents are cascade-canceled. A
	// non-empty notes is persisted into the jobs.notes column (visible
	// on the job detail page); pass "" to leave any existing notes
	// untouched.
	EndJob(ctx context.Context, tenantID, jobID, runnerID string, returnCode int, notes string) error

	// EndProxiedJob transitions a PROXYQUEUED job to a terminal status
	// with times recorded from SLURM. On non-success, dependents are
	// cascade-canceled. A non-empty notes is persisted into jobs.notes;
	// pass "" to leave any existing notes untouched.
	EndProxiedJob(ctx context.Context, tenantID, jobID string, status jobs.StatusCode, startTime, endTime time.Time, returnCode int, notes string) error

	// CancelJob marks a job CANCELED (with reason) and cascades to
	// dependents. No-op for already-terminal jobs.
	CancelJob(ctx context.Context, tenantID, jobID, reason string) error

	// HoldJob moves a QUEUED/WAITING/USERHOLD job to USERHOLD.
	HoldJob(ctx context.Context, tenantID, jobID string) error

	// ReleaseJob moves a USERHOLD job back to WAITING (so dependency
	// resolution can decide whether to promote it to QUEUED).
	ReleaseJob(ctx context.Context, tenantID, jobID string) error

	// AdjustJobPriority changes a job's priority by delta (positive or
	// negative). Only valid for QUEUED/WAITING/USERHOLD jobs.
	AdjustJobPriority(ctx context.Context, tenantID, jobID string, delta int) error

	// CleanupJob removes a job and all associated rows (details,
	// running details, deps, running claim). Caller must verify the
	// job is in a terminal state.
	CleanupJob(ctx context.Context, tenantID, jobID string) error

	// FindJobsByDetail returns the job IDs in tenantID that carry a
	// particular (key, value) row in job_details. Used for the run_id
	// filter on GET /jobs.
	FindJobsByDetail(ctx context.Context, tenantID, key, value string) ([]string, error)

	// FindJobsByInputPath / FindJobsByOutputPath return job IDs in
	// tenantID that list path as an input / output file, respectively.
	FindJobsByInputPath(ctx context.Context, tenantID, path string) ([]string, error)
	FindJobsByOutputPath(ctx context.Context, tenantID, path string) ([]string, error)
}
