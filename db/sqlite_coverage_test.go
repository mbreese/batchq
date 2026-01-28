package db

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mbreese/batchq/jobs"
)

func TestGetQueueJobsRunningDetails(t *testing.T) {
	dbConn := newTestDB(t)
	ctx := context.Background()

	job := jobs.NewJobDef("runner", "echo hi")
	submitted := dbConn.SubmitJob(ctx, job)
	if submitted == nil {
		t.Fatal("expected job to submit")
	}

	if !dbConn.StartJob(ctx, submitted.JobId, "runner-1", map[string]string{"pid": "123"}) {
		t.Fatal("expected StartJob to succeed")
	}
	if !dbConn.UpdateJobRunningDetails(ctx, submitted.JobId, map[string]string{"slurm_status": "RUNNING"}) {
		t.Fatal("expected UpdateJobRunningDetails to succeed")
	}

	list := dbConn.GetQueueJobs(ctx, true, true)
	if len(list) != 1 {
		t.Fatalf("expected 1 job, got %d", len(list))
	}
	got := list[0]
	if len(got.RunningDetails) != 1 {
		t.Fatalf("expected 1 running detail entry, got %d", len(got.RunningDetails))
	}
	if got.RunningDetails[0].Key != "pid" {
		t.Fatalf("expected running detail key pid, got %q", got.RunningDetails[0].Key)
	}
	if !strings.Contains(got.RunningDetails[0].Value, "123") || !strings.Contains(got.RunningDetails[0].Value, "slurm_status=RUNNING") {
		t.Fatalf("expected running details to include pid and slurm_status, got %q", got.RunningDetails[0].Value)
	}
}

func TestParseRunningDetailsSkipsInvalid(t *testing.T) {
	raw := "pid=42\n\ninvalid\nslurm_job_id=999"
	details := parseRunningDetails(raw)
	if len(details) != 2 {
		t.Fatalf("expected 2 running details, got %d", len(details))
	}
}

func TestInitDBCreatesDir(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "nested", "dir", "batchq.db")
	if err := InitDB("sqlite3://"+dbPath, true); err != nil {
		t.Fatalf("InitDB: %v", err)
	}
	if _, err := os.Stat(filepath.Dir(dbPath)); err != nil {
		t.Fatalf("expected db dir to exist, got %v", err)
	}
}

func TestInitDBForceExisting(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "batchq.db")
	if err := os.WriteFile(dbPath, []byte("existing"), 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if err := InitDB("sqlite3://"+dbPath, false); err == nil {
		t.Fatal("expected InitDB to fail without force on existing file")
	}
	if err := InitDB("sqlite3://"+dbPath, true); err != nil {
		t.Fatalf("InitDB with force: %v", err)
	}
}

func TestOpenDBRejectsUnsupportedPrefix(t *testing.T) {
	if _, err := OpenDB("mysql:///tmp/batchq.db"); err == nil {
		t.Fatal("expected OpenDB to reject non-sqlite3 path")
	}
}

func TestGetQueueJobsFallbackWhenNoActive(t *testing.T) {
	dbConn := newTestDB(t)
	ctx := context.Background()

	job := jobs.NewJobDef("done", "echo done")
	submitted := dbConn.SubmitJob(ctx, job)
	if submitted == nil {
		t.Fatal("expected job to submit")
	}
	if !dbConn.StartJob(ctx, submitted.JobId, "runner-1", map[string]string{"pid": "1"}) {
		t.Fatal("expected StartJob to succeed")
	}
	if !dbConn.EndJob(ctx, submitted.JobId, "runner-1", 0) {
		t.Fatal("expected EndJob to succeed")
	}

	active := dbConn.GetQueueJobs(ctx, false, true)
	if len(active) != 0 {
		t.Fatalf("expected no active jobs, got %v", active)
	}

	all := dbConn.GetQueueJobs(ctx, true, true)
	if len(all) != 1 || all[0].JobId != submitted.JobId {
		t.Fatalf("expected job to be returned when showAll=true, got %v", all)
	}
}
