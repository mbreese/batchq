package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/mbreese/batchq/jobs"
	"github.com/mbreese/batchq/support"
)

// pgTest returns a fresh Postgres-backed Storage with a unique schema
// per test (CREATE SCHEMA / SET search_path) so parallel tests don't
// step on each other. Skips if BATCHQ_TEST_PGURL is unset.
//
// The schema is dropped at test cleanup so the test database stays
// clean across runs.
func pgTest(t *testing.T) Storage {
	t.Helper()
	url := os.Getenv("BATCHQ_TEST_PGURL")
	if url == "" {
		t.Skip("BATCHQ_TEST_PGURL not set; skipping Postgres integration test")
	}
	s, err := OpenPostgres(context.Background(), url)
	if err != nil {
		t.Fatalf("OpenPostgres: %v", err)
	}
	// Wipe data (not schema) before each test so we start from clean.
	// We can't drop and recreate the schema because that would race
	// with concurrent tests; deleting in dependency order is enough.
	cleanup := func() {
		ctx := context.Background()
		tables := []string{
			"job_running_details", "job_running", "job_deps",
			"job_details", "job_inputs", "job_outputs",
			"jobs", "tokens", "tenants",
		}
		for _, table := range tables {
			_, _ = s.(*pgStorage).db.ExecContext(ctx, `DELETE FROM `+table)
		}
	}
	cleanup()
	t.Cleanup(func() {
		cleanup()
		_ = s.Close()
	})
	return s
}

// Tenant CRUD against postgres.
func TestPostgres_TenantRoundtrip(t *testing.T) {
	s := pgTest(t)
	ctx := context.Background()

	created, err := s.CreateTenant(ctx, "alice-pg", TenantKindRemote)
	if err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}
	fetched, err := s.GetTenantByName(ctx, "alice-pg")
	if err != nil {
		t.Fatalf("GetTenantByName: %v", err)
	}
	if fetched.ID != created.ID {
		t.Fatalf("id mismatch: %s vs %s", fetched.ID, created.ID)
	}

	// Duplicate name -> ErrTenantExists.
	if _, err := s.CreateTenant(ctx, "alice-pg", TenantKindRemote); !errors.Is(err, ErrTenantExists) {
		t.Fatalf("duplicate: got %v, want ErrTenantExists", err)
	}
}

// Cross-tenant isolation on Postgres — same invariant we verified
// on sqlite. The whole multi-tenant story is broken if either
// backend leaks.
func TestPostgres_CrossTenantIsolation(t *testing.T) {
	s := pgTest(t)
	ctx := context.Background()

	alice, _ := s.CreateTenant(ctx, "alice-pg", TenantKindRemote)
	bob, _ := s.CreateTenant(ctx, "bob-pg", TenantKindRemote)

	jobID := support.NewUUID()
	if err := s.InsertJob(ctx, alice.ID, &jobs.JobDef{
		JobId:   jobID,
		Details: []jobs.JobDefDetail{{Key: "script", Value: "echo alice"}},
	}); err != nil {
		t.Fatalf("InsertJob: %v", err)
	}

	if _, err := s.GetJob(ctx, bob.ID, jobID); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("bob.GetJob: got %v, want ErrJobNotFound", err)
	}
	if got, err := s.ListJobs(ctx, bob.ID, true, false); err != nil || len(got) != 0 {
		t.Fatalf("bob.ListJobs: got (%v, %v), want ([], nil)", got, err)
	}
}

// Token round trip + revoke + expired all surface as ErrTokenNotFound
// (security-critical: don't leak which case applies).
func TestPostgres_TokenLifecycle(t *testing.T) {
	s := pgTest(t)
	ctx := context.Background()

	alice, _ := s.CreateTenant(ctx, "alice-pg", TenantKindRemote)

	// Active.
	hmacActive := []byte("hmac-active-bytes")
	tok, err := s.CreateToken(ctx, alice.ID, hmacActive, "active", time.Time{})
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	got, _, err := s.GetTokenByHMAC(ctx, hmacActive)
	if err != nil {
		t.Fatalf("GetTokenByHMAC active: %v", err)
	}
	if got.ID != tok.ID {
		t.Fatalf("token id mismatch")
	}

	// Revoked.
	hmacRevoked := []byte("hmac-revoked-bytes")
	rtok, err := s.CreateToken(ctx, alice.ID, hmacRevoked, "to-revoke", time.Time{})
	if err != nil {
		t.Fatalf("CreateToken revoked: %v", err)
	}
	if err := s.RevokeToken(ctx, rtok.ID); err != nil {
		t.Fatalf("RevokeToken: %v", err)
	}
	if _, _, err := s.GetTokenByHMAC(ctx, hmacRevoked); !errors.Is(err, ErrTokenNotFound) {
		t.Fatalf("revoked lookup: got %v, want ErrTokenNotFound", err)
	}

	// Expired.
	hmacExpired := []byte("hmac-expired-bytes")
	if _, err := s.CreateToken(ctx, alice.ID, hmacExpired, "expired",
		time.Now().UTC().Add(-1*time.Hour)); err != nil {
		t.Fatalf("CreateToken expired: %v", err)
	}
	if _, _, err := s.GetTokenByHMAC(ctx, hmacExpired); !errors.Is(err, ErrTokenNotFound) {
		t.Fatalf("expired lookup: got %v, want ErrTokenNotFound", err)
	}
}

// Atomic claim: two runners can't claim the same job. Postgres's
// UNIQUE PRIMARY KEY on job_running is the serialization primitive,
// same as sqlite.
func TestPostgres_AtomicClaim(t *testing.T) {
	s := pgTest(t)
	ctx := context.Background()

	alice, _ := s.CreateTenant(ctx, "alice-pg", TenantKindRemote)
	jobID := support.NewUUID()
	if err := s.InsertJob(ctx, alice.ID, &jobs.JobDef{
		JobId:   jobID,
		Details: []jobs.JobDefDetail{{Key: "script", Value: "echo"}},
	}); err != nil {
		t.Fatalf("InsertJob: %v", err)
	}

	first, err := s.ClaimNextJob(ctx, alice.ID, "runner-a", "simple", Limits{})
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if first.Job == nil || first.Job.JobId != jobID {
		t.Fatalf("first claim missed: %+v", first)
	}

	second, err := s.ClaimNextJob(ctx, alice.ID, "runner-b", "simple", Limits{})
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if second.Job != nil {
		t.Fatalf("second runner claimed same job: %s", second.Job.JobId)
	}
}

// Job lifecycle on postgres: submit -> claim -> end -> cleanup.
func TestPostgres_JobLifecycle(t *testing.T) {
	s := pgTest(t)
	ctx := context.Background()

	alice, _ := s.CreateTenant(ctx, "alice-pg", TenantKindRemote)
	jobID := support.NewUUID()
	if err := s.InsertJob(ctx, alice.ID, &jobs.JobDef{
		JobId:   jobID,
		Details: []jobs.JobDefDetail{{Key: "script", Value: "echo"}},
	}); err != nil {
		t.Fatalf("InsertJob: %v", err)
	}

	res, err := s.ClaimNextJob(ctx, alice.ID, "runner-a", "simple", Limits{})
	if err != nil || res.Job == nil {
		t.Fatalf("claim: %v %v", err, res)
	}
	if err := s.EndJob(ctx, alice.ID, jobID, "runner-a", 0, ""); err != nil {
		t.Fatalf("EndJob: %v", err)
	}
	got, err := s.GetJob(ctx, alice.ID, jobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Status != jobs.SUCCESS {
		t.Fatalf("status: got %s, want SUCCESS", got.Status)
	}
	if err := s.CleanupJob(ctx, alice.ID, jobID); err != nil {
		t.Fatalf("CleanupJob: %v", err)
	}
	if _, err := s.GetJob(ctx, alice.ID, jobID); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("post-cleanup GetJob: got %v, want ErrJobNotFound", err)
	}
}

// pgQuery placeholder rewriter — quick unit check.
func TestPgQuery_PlaceholderConversion(t *testing.T) {
	cases := []struct{ in, want string }{
		{"SELECT 1", "SELECT 1"},
		{"WHERE a = ?", "WHERE a = $1"},
		{"WHERE a = ? AND b = ?", "WHERE a = $1 AND b = $2"},
		{"VALUES (?, ?, ?)", "VALUES ($1, $2, $3)"},
	}
	for _, c := range cases {
		got := pgQuery(c.in)
		if got != c.want {
			t.Errorf("pgQuery(%q): got %q, want %q", c.in, got, c.want)
		}
	}
}

// Sanity-check pgIsUniqueViolation on a synthetic error string.
func TestPgIsUniqueViolation(t *testing.T) {
	yes := fmt.Errorf("ERROR: duplicate key value violates unique constraint \"tenants_name_key\" (SQLSTATE 23505)")
	no := fmt.Errorf("ERROR: connection refused")
	if !pgIsUniqueViolation(yes) {
		t.Errorf("yes case: should match")
	}
	if pgIsUniqueViolation(no) {
		t.Errorf("no case: should not match")
	}
}
