package db

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/mbreese/batchq/jobs"
)

func newTestDB(t *testing.T) BatchDB {
	t.Helper()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "batchq-test.db")
	if err := InitDB("sqlite3://"+dbPath, true); err != nil {
		t.Fatalf("InitDB: %v", err)
	}

	dbConn, err := OpenDB("sqlite3://" + dbPath)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}

	t.Cleanup(func() {
		dbConn.Close()
	})

	return dbConn
}

func jobDetail(job *jobs.JobDef, key string) (string, bool) {
	for _, detail := range job.Details {
		if detail.Key == key {
			return detail.Value, true
		}
	}
	return "", false
}

func TestSubmitJobAssignsIdAndDetails(t *testing.T) {
	dbConn := newTestDB(t)
	ctx := context.Background()

	job := jobs.NewJobDef("job-%JOBID", "echo hello")
	job.AddDetail("stdout", "/tmp/out-%JOBID.log")
	job.AddDetail("stderr", "/tmp/err-%JOBID.log")

	submitted := dbConn.SubmitJob(ctx, job)
	if submitted == nil {
		t.Fatal("expected job submission to succeed")
	}
	if submitted.JobId == "" {
		t.Fatal("expected job id to be assigned")
	}
	if submitted.Status != jobs.QUEUED {
		t.Fatalf("expected status QUEUED, got %v", submitted.Status)
	}
	if submitted.Name == "job-%JOBID" || submitted.Name == "" {
		t.Fatalf("expected name to be expanded with job id, got %q", submitted.Name)
	}

	stored := dbConn.GetJob(ctx, submitted.JobId)
	if stored == nil {
		t.Fatal("expected stored job to be found")
	}
	if stored.Name != submitted.Name {
		t.Fatalf("expected stored name %q, got %q", submitted.Name, stored.Name)
	}

	stdoutPath, ok := jobDetail(stored, "stdout")
	if !ok || stdoutPath == "/tmp/out-%JOBID.log" {
		t.Fatalf("expected stdout path to be expanded, got %q", stdoutPath)
	}

	stderrPath, ok := jobDetail(stored, "stderr")
	if !ok || stderrPath == "/tmp/err-%JOBID.log" {
		t.Fatalf("expected stderr path to be expanded, got %q", stderrPath)
	}
}

func TestSubmitJobDependencies(t *testing.T) {
	dbConn := newTestDB(t)
	ctx := context.Background()

	parent := jobs.NewJobDef("parent", "echo parent")
	parentSub := dbConn.SubmitJob(ctx, parent)
	if parentSub == nil {
		t.Fatal("expected parent job to submit")
	}

	child := jobs.NewJobDef("child", "echo child")
	child.AddAfterOk(parentSub.JobId)
	childSub := dbConn.SubmitJob(ctx, child)
	if childSub == nil {
		t.Fatal("expected child job to submit")
	}
	if childSub.Status != jobs.WAITING {
		t.Fatalf("expected child job status WAITING, got %v", childSub.Status)
	}

	deps := dbConn.GetJobDependents(ctx, parentSub.JobId)
	if len(deps) != 1 || deps[0] != childSub.JobId {
		t.Fatalf("expected dependent list to include %q, got %v", childSub.JobId, deps)
	}

	missing := jobs.NewJobDef("missing", "echo missing")
	missing.AddAfterOk("does-not-exist")
	if dbConn.SubmitJob(ctx, missing) != nil {
		t.Fatal("expected job submission to fail with missing dependency")
	}

	if !dbConn.CancelJob(ctx, parentSub.JobId, "cancel parent") {
		t.Fatal("expected parent cancel to succeed")
	}
	child2 := jobs.NewJobDef("child2", "echo child2")
	child2.AddAfterOk(parentSub.JobId)
	if dbConn.SubmitJob(ctx, child2) != nil {
		t.Fatal("expected job submission to fail with canceled dependency")
	}
}

func TestGetJobStatusCounts(t *testing.T) {
	dbConn := newTestDB(t)
	ctx := context.Background()

	canceled := jobs.NewJobDef("canceled", "echo canceled")
	canceledSub := dbConn.SubmitJob(ctx, canceled)
	if canceledSub == nil {
		t.Fatal("expected canceled job to submit")
	}
	if !dbConn.CancelJob(ctx, canceledSub.JobId, "stop") {
		t.Fatal("expected cancel to succeed")
	}

	proxied := jobs.NewJobDef("proxied", "echo proxied")
	proxiedSub := dbConn.SubmitJob(ctx, proxied)
	if proxiedSub == nil {
		t.Fatal("expected proxied job to submit")
	}
	if !dbConn.ProxyQueueJob(ctx, proxiedSub.JobId, "testrunner", map[string]string{"slurm_job_id": "123"}) {
		t.Fatal("expected proxy queue to succeed")
	}
	if !dbConn.ProxyEndJob(ctx, proxiedSub.JobId, jobs.SUCCESS, "2023-01-02 03:04:05 UTC", "2023-01-02 03:05:05 UTC", 0) {
		t.Fatal("expected proxy end to succeed")
	}

	counts := dbConn.GetJobStatusCounts(ctx, true)
	if counts[jobs.CANCELED] != 1 {
		t.Fatalf("expected 1 canceled job, got %d", counts[jobs.CANCELED])
	}
	if counts[jobs.SUCCESS] != 1 {
		t.Fatalf("expected 1 success job, got %d", counts[jobs.SUCCESS])
	}
}

func TestGetJobsFilters(t *testing.T) {
	dbConn := newTestDB(t)
	ctx := context.Background()

	queued := jobs.NewJobDef("queued", "echo queued")
	queuedSub := dbConn.SubmitJob(ctx, queued)
	if queuedSub == nil {
		t.Fatal("expected queued job to submit")
	}

	canceled := jobs.NewJobDef("canceled", "echo canceled")
	canceledSub := dbConn.SubmitJob(ctx, canceled)
	if canceledSub == nil {
		t.Fatal("expected canceled job to submit")
	}
	if !dbConn.CancelJob(ctx, canceledSub.JobId, "stop") {
		t.Fatal("expected cancel to succeed")
	}

	visible := dbConn.GetJobs(ctx, false, false)
	if len(visible) != 1 || visible[0].JobId != queuedSub.JobId {
		t.Fatalf("expected only queued job, got %v", visible)
	}

	all := dbConn.GetJobs(ctx, true, false)
	if len(all) != 2 {
		t.Fatalf("expected 2 jobs with showAll=true, got %d", len(all))
	}
}

func TestSearchJobs(t *testing.T) {
	dbConn := newTestDB(t)
	ctx := context.Background()

	alpha := jobs.NewJobDef("alpha-job", "echo hello world")
	alphaSub := dbConn.SubmitJob(ctx, alpha)
	if alphaSub == nil {
		t.Fatal("expected alpha job to submit")
	}

	beta := jobs.NewJobDef("beta-job", "echo beta")
	betaSub := dbConn.SubmitJob(ctx, beta)
	if betaSub == nil {
		t.Fatal("expected beta job to submit")
	}
	if !dbConn.CancelJob(ctx, betaSub.JobId, "stop") {
		t.Fatal("expected beta cancel to succeed")
	}

	byName := dbConn.SearchJobs(ctx, "alpha", nil)
	if len(byName) != 1 || byName[0].JobId != alphaSub.JobId {
		t.Fatalf("expected alpha search to return alpha job, got %v", byName)
	}

	byScript := dbConn.SearchJobs(ctx, "hello", []jobs.StatusCode{jobs.QUEUED})
	if len(byScript) != 1 || byScript[0].JobId != alphaSub.JobId {
		t.Fatalf("expected script search to return alpha job, got %v", byScript)
	}

	canceledOnly := dbConn.SearchJobs(ctx, "beta", []jobs.StatusCode{jobs.CANCELED})
	if len(canceledOnly) != 1 || canceledOnly[0].JobId != betaSub.JobId {
		t.Fatalf("expected beta search to return canceled beta job, got %v", canceledOnly)
	}

	noMatch := dbConn.SearchJobs(ctx, "beta", []jobs.StatusCode{jobs.QUEUED})
	if len(noMatch) != 0 {
		t.Fatalf("expected beta search in queued to return none, got %v", noMatch)
	}
}
