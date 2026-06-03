package storage

import (
	"context"
	"time"

	"github.com/mbreese/batchq/jobs"
)

// scopedStorage is a test-only wrapper that binds Storage to a single
// tenant so test bodies don't have to thread tenantID through every
// call. Tests that exercise cross-tenant behavior should use the raw
// Storage interface directly.
//
// Every method on Storage that takes a tenantID is shadowed here so
// it can be called without the parameter. Methods that don't take
// tenantID (Close, the tenant-management methods themselves) are
// reached through the embedded Storage.
type scopedStorage struct {
	Storage
	tenant string
}

// --- Submission and reads ---------------------------------------------

func (s *scopedStorage) InsertJob(ctx context.Context, job *jobs.JobDef) error {
	return s.Storage.InsertJob(ctx, s.tenant, job)
}

func (s *scopedStorage) GetJob(ctx context.Context, jobID string) (*jobs.JobDef, error) {
	return s.Storage.GetJob(ctx, s.tenant, jobID)
}

func (s *scopedStorage) ListJobs(ctx context.Context, showAll, sortByStatus bool) ([]*jobs.JobDef, error) {
	return s.Storage.ListJobs(ctx, s.tenant, showAll, sortByStatus)
}

func (s *scopedStorage) ListJobsByStatus(ctx context.Context, statuses []jobs.StatusCode, sortByStatus bool) ([]*jobs.JobDef, error) {
	return s.Storage.ListJobsByStatus(ctx, s.tenant, statuses, sortByStatus)
}

func (s *scopedStorage) SearchJobs(ctx context.Context, query string, statuses []jobs.StatusCode) ([]*jobs.JobDef, error) {
	return s.Storage.SearchJobs(ctx, s.tenant, query, statuses)
}

func (s *scopedStorage) GetJobDependents(ctx context.Context, jobID string) ([]string, error) {
	return s.Storage.GetJobDependents(ctx, s.tenant, jobID)
}

func (s *scopedStorage) GetJobStatusCounts(ctx context.Context, showAll bool) (map[jobs.StatusCode]int, error) {
	return s.Storage.GetJobStatusCounts(ctx, s.tenant, showAll)
}

func (s *scopedStorage) GetQueueJobs(ctx context.Context, showAll, sortByStatus bool) ([]*jobs.JobDef, error) {
	return s.Storage.GetQueueJobs(ctx, s.tenant, showAll, sortByStatus)
}

func (s *scopedStorage) GetProxyJobs(ctx context.Context) ([]*jobs.JobDef, error) {
	return s.Storage.GetProxyJobs(ctx, s.tenant)
}

// --- Dependency resolution / atomic claim ------------------------------

func (s *scopedStorage) ResolveDependencies(ctx context.Context) (ResolveResult, error) {
	return s.Storage.ResolveDependencies(ctx, s.tenant)
}

func (s *scopedStorage) ClaimNextJob(ctx context.Context, runnerID, kind string, limits Limits) (ClaimResult, error) {
	return s.Storage.ClaimNextJob(ctx, s.tenant, runnerID, kind, limits)
}

// --- State transitions ------------------------------------------------

func (s *scopedStorage) MarkJobProxied(ctx context.Context, jobID, runnerID string, runningDetails map[string]string) error {
	return s.Storage.MarkJobProxied(ctx, s.tenant, jobID, runnerID, runningDetails)
}

func (s *scopedStorage) UpdateRunningDetails(ctx context.Context, jobID string, details map[string]string) error {
	return s.Storage.UpdateRunningDetails(ctx, s.tenant, jobID, details)
}

func (s *scopedStorage) EndJob(ctx context.Context, jobID, runnerID string, returnCode int, notes string) error {
	return s.Storage.EndJob(ctx, s.tenant, jobID, runnerID, returnCode, notes)
}

func (s *scopedStorage) EndProxiedJob(ctx context.Context, jobID string, status jobs.StatusCode, startTime, endTime time.Time, returnCode int, notes string) error {
	return s.Storage.EndProxiedJob(ctx, s.tenant, jobID, status, startTime, endTime, returnCode, notes)
}

// --- User actions ------------------------------------------------------

func (s *scopedStorage) CancelJob(ctx context.Context, jobID, reason string) error {
	return s.Storage.CancelJob(ctx, s.tenant, jobID, reason)
}

func (s *scopedStorage) HoldJob(ctx context.Context, jobID string) error {
	return s.Storage.HoldJob(ctx, s.tenant, jobID)
}

func (s *scopedStorage) ReleaseJob(ctx context.Context, jobID string) error {
	return s.Storage.ReleaseJob(ctx, s.tenant, jobID)
}

func (s *scopedStorage) AdjustJobPriority(ctx context.Context, jobID string, delta int) error {
	return s.Storage.AdjustJobPriority(ctx, s.tenant, jobID, delta)
}

func (s *scopedStorage) CleanupJob(ctx context.Context, jobID string) error {
	return s.Storage.CleanupJob(ctx, s.tenant, jobID)
}

// --- Reverse lookups ---------------------------------------------------

func (s *scopedStorage) FindJobsByDetail(ctx context.Context, key, value string) ([]string, error) {
	return s.Storage.FindJobsByDetail(ctx, s.tenant, key, value)
}

func (s *scopedStorage) FindJobsByInputPath(ctx context.Context, path string) ([]string, error) {
	return s.Storage.FindJobsByInputPath(ctx, s.tenant, path)
}

func (s *scopedStorage) FindJobsByOutputPath(ctx context.Context, path string) ([]string, error) {
	return s.Storage.FindJobsByOutputPath(ctx, s.tenant, path)
}
