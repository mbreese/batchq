package storage

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/mbreese/batchq/jobs"
)

// freshStore opens a raw Storage (no auto-tenant wrapper) so the test
// can drive tenant scoping explicitly.
func freshStore(t *testing.T) Storage {
	t.Helper()
	path := filepath.Join(t.TempDir(), "batchq.db")
	s, err := Open(context.Background(), path, Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// EnsureLocalTenant is idempotent: calling it twice returns the same
// tenant rather than creating a duplicate. The narrow race between
// concurrent callers is resolved by the UNIQUE constraint on name.
func TestEnsureLocalTenantIsIdempotent(t *testing.T) {
	s := freshStore(t)
	ctx := context.Background()

	t1, err := s.EnsureLocalTenant(ctx, "alice")
	if err != nil {
		t.Fatalf("first EnsureLocalTenant: %v", err)
	}
	t2, err := s.EnsureLocalTenant(ctx, "alice")
	if err != nil {
		t.Fatalf("second EnsureLocalTenant: %v", err)
	}
	if t1.ID != t2.ID {
		t.Fatalf("tenant id changed across calls: %s vs %s", t1.ID, t2.ID)
	}
	if t1.Kind != TenantKindLocal {
		t.Fatalf("kind: got %q, want %q", t1.Kind, TenantKindLocal)
	}
}

// CreateTenant returns ErrTenantExists when the name is already taken.
func TestCreateTenantDuplicate(t *testing.T) {
	s := freshStore(t)
	ctx := context.Background()

	if _, err := s.CreateTenant(ctx, "bob", TenantKindRemote); err != nil {
		t.Fatalf("first CreateTenant: %v", err)
	}
	if _, err := s.CreateTenant(ctx, "bob", TenantKindRemote); !errors.Is(err, ErrTenantExists) {
		t.Fatalf("second CreateTenant: got %v, want ErrTenantExists", err)
	}
}

// Cross-tenant isolation: a job submitted to alice is invisible to
// bob via GetJob, ListJobs, queue counts, and reverse lookups. This
// is the core Phase 1 invariant — leak this and the rest of the auth
// story is moot.
func TestCrossTenantIsolation(t *testing.T) {
	s := freshStore(t)
	ctx := context.Background()

	alice, _ := s.CreateTenant(ctx, "alice", TenantKindRemote)
	bob, _ := s.CreateTenant(ctx, "bob", TenantKindRemote)

	aliceJob := &jobs.JobDef{
		JobId: "alice-job-1",
		Name:  "alice-job",
		Details: []jobs.JobDefDetail{
			{Key: "script", Value: "echo alice"},
			{Key: "run_id", Value: "alice-run"},
		},
		InputFiles:  []string{"/shared/path"},
		OutputFiles: []string{"/shared/out"},
	}
	if err := s.InsertJob(ctx, alice.ID, aliceJob); err != nil {
		t.Fatalf("InsertJob: %v", err)
	}

	// bob cannot fetch alice's job by ID.
	if _, err := s.GetJob(ctx, bob.ID, aliceJob.JobId); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("bob.GetJob: got %v, want ErrJobNotFound", err)
	}

	// bob's listings are empty.
	if got, err := s.ListJobs(ctx, bob.ID, true, false); err != nil || len(got) != 0 {
		t.Fatalf("bob.ListJobs: got (%v, %v), want ([], nil)", got, err)
	}
	if counts, err := s.GetJobStatusCounts(ctx, bob.ID, true); err != nil {
		t.Fatalf("GetJobStatusCounts: %v", err)
	} else {
		total := 0
		for _, n := range counts {
			total += n
		}
		if total != 0 {
			t.Fatalf("bob sees %d jobs in counts", total)
		}
	}

	// Reverse lookups by detail / input / output are also scoped.
	if got, err := s.FindJobsByDetail(ctx, bob.ID, "run_id", "alice-run"); err != nil || len(got) != 0 {
		t.Fatalf("bob.FindJobsByDetail: got (%v, %v), want ([], nil)", got, err)
	}
	if got, err := s.FindJobsByInputPath(ctx, bob.ID, "/shared/path"); err != nil || len(got) != 0 {
		t.Fatalf("bob.FindJobsByInputPath: got (%v, %v), want ([], nil)", got, err)
	}
	if got, err := s.FindJobsByOutputPath(ctx, bob.ID, "/shared/out"); err != nil || len(got) != 0 {
		t.Fatalf("bob.FindJobsByOutputPath: got (%v, %v), want ([], nil)", got, err)
	}

	// Alice still sees her own job.
	if got, err := s.GetJob(ctx, alice.ID, aliceJob.JobId); err != nil || got.JobId != aliceJob.JobId {
		t.Fatalf("alice.GetJob: got (%v, %v)", got, err)
	}
}

// A runner in tenant bob cannot claim a queued job belonging to alice.
func TestClaimNextJobIsTenantScoped(t *testing.T) {
	s := freshStore(t)
	ctx := context.Background()

	alice, _ := s.CreateTenant(ctx, "alice", TenantKindRemote)
	bob, _ := s.CreateTenant(ctx, "bob", TenantKindRemote)

	if err := s.InsertJob(ctx, alice.ID, &jobs.JobDef{
		JobId:   "alice-q-1",
		Details: []jobs.JobDefDetail{{Key: "script", Value: "echo"}},
	}); err != nil {
		t.Fatalf("InsertJob: %v", err)
	}

	bobResult, err := s.ClaimNextJob(ctx, bob.ID, "bob-runner", "simple", Limits{})
	if err != nil {
		t.Fatalf("bob ClaimNextJob: %v", err)
	}
	if bobResult.Job != nil {
		t.Fatalf("bob claimed alice's job: %+v", bobResult.Job)
	}

	aliceResult, err := s.ClaimNextJob(ctx, alice.ID, "alice-runner", "simple", Limits{})
	if err != nil {
		t.Fatalf("alice ClaimNextJob: %v", err)
	}
	if aliceResult.Job == nil || aliceResult.Job.JobId != "alice-q-1" {
		t.Fatalf("alice should have claimed her own job, got %+v", aliceResult.Job)
	}
}

// Cancel, hold, release, cleanup must also refuse cross-tenant
// operations (returning ErrJobNotFound / ErrInvalidState rather than
// silently doing nothing on the other tenant's job).
func TestUserActionsAreTenantScoped(t *testing.T) {
	s := freshStore(t)
	ctx := context.Background()

	alice, _ := s.CreateTenant(ctx, "alice", TenantKindRemote)
	bob, _ := s.CreateTenant(ctx, "bob", TenantKindRemote)

	if err := s.InsertJob(ctx, alice.ID, &jobs.JobDef{
		JobId:   "alice-2",
		Details: []jobs.JobDefDetail{{Key: "script", Value: "echo"}},
	}); err != nil {
		t.Fatalf("InsertJob: %v", err)
	}

	if err := s.HoldJob(ctx, bob.ID, "alice-2"); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("bob.HoldJob alice's job: got %v, want ErrInvalidState", err)
	}
	if err := s.CancelJob(ctx, bob.ID, "alice-2", "intruder"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("bob.CancelJob alice's job: got %v, want ErrJobNotFound", err)
	}
}
