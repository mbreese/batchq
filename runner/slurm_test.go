package runner

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mbreese/batchq/db"
	"github.com/mbreese/batchq/jobs"
)

func writeFakeBin(t *testing.T, dir, name, content string) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0755); err != nil {
		t.Fatalf("WriteFile %s: %v", name, err)
	}
}

type fakeBatchDB struct {
	fetchJobs         []*jobs.JobDef
	fetchIndex        int
	proxyQueued       []string
	updates           []map[string]string
	proxyEnded        []jobs.StatusCode
	proxyJobs         []*jobs.JobDef
	jobsByID          map[string]*jobs.JobDef
	cancelledJobIDs   []string
	proxyQueueDetails []map[string]string
}

func (f *fakeBatchDB) SubmitJob(ctx context.Context, job *jobs.JobDef) *jobs.JobDef { return nil }
func (f *fakeBatchDB) FetchNext(ctx context.Context, limits []db.JobLimit) (*jobs.JobDef, bool) {
	if f.fetchIndex < len(f.fetchJobs) {
		job := f.fetchJobs[f.fetchIndex]
		f.fetchIndex++
		return job, f.fetchIndex < len(f.fetchJobs)
	}
	return nil, false
}
func (f *fakeBatchDB) GetJob(ctx context.Context, jobId string) *jobs.JobDef {
	if f.jobsByID == nil {
		return nil
	}
	return f.jobsByID[jobId]
}
func (f *fakeBatchDB) GetJobs(ctx context.Context, showAll bool, sortByStatus bool) []*jobs.JobDef {
	return nil
}
func (f *fakeBatchDB) GetJobsByStatus(ctx context.Context, statuses []jobs.StatusCode, sortByStatus bool) []*jobs.JobDef {
	return nil
}
func (f *fakeBatchDB) GetJobDependents(ctx context.Context, jobId string) []string { return nil }
func (f *fakeBatchDB) GetJobStatusCounts(ctx context.Context, showAll bool) map[jobs.StatusCode]int {
	return nil
}
func (f *fakeBatchDB) GetQueueJobs(ctx context.Context, showAll bool, sortByStatus bool) []*jobs.JobDef {
	return nil
}
func (f *fakeBatchDB) SearchJobs(ctx context.Context, query string, statuses []jobs.StatusCode) []*jobs.JobDef {
	return nil
}
func (f *fakeBatchDB) CancelJob(ctx context.Context, jobId string, reason string) bool {
	f.cancelledJobIDs = append(f.cancelledJobIDs, jobId)
	return true
}
func (f *fakeBatchDB) StartJob(ctx context.Context, jobId string, jobRunner string, details map[string]string) bool {
	return true
}
func (f *fakeBatchDB) ProxyQueueJob(ctx context.Context, jobId string, jobRunner string, details map[string]string) bool {
	f.proxyQueued = append(f.proxyQueued, jobId)
	f.proxyQueueDetails = append(f.proxyQueueDetails, details)
	return true
}
func (f *fakeBatchDB) GetProxyJobs(ctx context.Context) []*jobs.JobDef { return f.proxyJobs }
func (f *fakeBatchDB) ProxyEndJob(ctx context.Context, jobId string, status jobs.StatusCode, startTime string, endTime string, returnCode int) bool {
	f.proxyEnded = append(f.proxyEnded, status)
	return true
}
func (f *fakeBatchDB) UpdateJobRunningDetails(ctx context.Context, jobId string, details map[string]string) bool {
	f.updates = append(f.updates, details)
	return true
}
func (f *fakeBatchDB) EndJob(ctx context.Context, jobId string, jobRunner string, returnCode int) bool {
	return true
}
func (f *fakeBatchDB) CleanupJob(ctx context.Context, jobId string) bool { return true }
func (f *fakeBatchDB) TopJob(ctx context.Context, jobId string) bool     { return true }
func (f *fakeBatchDB) NiceJob(ctx context.Context, jobId string) bool    { return true }
func (f *fakeBatchDB) HoldJob(ctx context.Context, jobId string) bool    { return true }
func (f *fakeBatchDB) ReleaseJob(ctx context.Context, jobId string) bool { return true }
func (f *fakeBatchDB) Close()                                           {}

func TestSlurmHelpersWithFakeCommands(t *testing.T) {
	binDir := t.TempDir()
	writeFakeBin(t, binDir, "squeue", "#!/bin/sh\necho 'JOBID PARTITION NAME USER ST TIME NODES NODELIST'\necho '1 debug test user R 0:01 1 node'\necho '2 debug test user R 0:01 1 node'\n")
	writeFakeBin(t, binDir, "sacct", "#!/bin/sh\necho '123|COMPLETED|2023-01-02T03:04:05|2023-01-02T03:05:05|0:0|node'\n")
	writeFakeBin(t, binDir, "sbatch", "#!/bin/sh\necho '456'\n")

	oldPath := os.Getenv("PATH")
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+oldPath)

	count, err := SlurmGetUserJobCount("tester")
	if err != nil {
		t.Fatalf("SlurmGetUserJobCount: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected count 2, got %d", count)
	}

	state, err := SlurmGetJobState(123)
	if err != nil {
		t.Fatalf("SlurmGetJobState: %v", err)
	}
	if state == nil || state.State != "COMPLETED" {
		t.Fatalf("unexpected slurm state: %+v", state)
	}
	if state.ExitCodeInt() != 0 {
		t.Fatalf("expected exit code 0, got %d", state.ExitCodeInt())
	}
	if state.StartAsTimeString() == "" || state.EndAsTimeString() == "" {
		t.Fatal("expected start/end time strings to be populated")
	}

	jobID, err := SlurmSbatch("#!/bin/sh\n", nil)
	if err != nil {
		t.Fatalf("SlurmSbatch: %v", err)
	}
	if jobID != "456" {
		t.Fatalf("expected job id 456, got %q", jobID)
	}
}

func TestSlurmGetUserJobCountEmptyUser(t *testing.T) {
	binDir := t.TempDir()
	writeFakeBin(t, binDir, "squeue", "#!/bin/sh\necho 'JOBID PARTITION NAME USER ST TIME NODES NODELIST'\necho '1 debug test user R 0:01 1 node'\n")
	oldPath := os.Getenv("PATH")
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+oldPath)

	count, err := SlurmGetUserJobCount("")
	if err != nil {
		t.Fatalf("SlurmGetUserJobCount: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected count 1, got %d", count)
	}
}

func TestSlurmSecsToWalltime(t *testing.T) {
	if got := SlurmSecsToWalltime("3661"); got != "1:1:1" {
		t.Fatalf("expected 1:1:1, got %q", got)
	}
	if got := SlurmSecsToWalltime("bad"); got != "00:00:00" {
		t.Fatalf("expected 00:00:00, got %q", got)
	}
}

func TestSlurmJobStateParsing(t *testing.T) {
	state := SlurmJobState{Start: "UNKNOWN", End: "UNKNOWN", ExitCode: "bad"}
	if state.StartAsTimeString() != "" {
		t.Fatal("expected unknown start to return empty")
	}
	if state.EndAsTimeString() != "" {
		t.Fatal("expected unknown end to return empty")
	}
	if state.ExitCodeInt() != -1 {
		t.Fatalf("expected invalid exit code to return -1, got %d", state.ExitCodeInt())
	}
}

func TestSlurmGetJobStateInvalidID(t *testing.T) {
	if _, err := SlurmGetJobState(0); err == nil {
		t.Fatal("expected error on invalid job id")
	}
}

func TestBuildSBatchScript(t *testing.T) {
	runner := NewSlurmRunner(nil).
		SetSlurmAccount("acct").
		SetSlurmPartition("partition")

	job := jobs.NewJobDef("job", "#!/bin/sh\necho hi")
	job.AddDetail("procs", "2")
	job.AddDetail("mem", "1024")
	job.AddDetail("walltime", "3600")
	job.AddDetail("wd", "/tmp")
	job.AddDetail("stdout", "/tmp/out")
	job.AddDetail("stderr", "/tmp/err")
	job.AddDetail("env", "A=1")
	job.JobId = "abc"

	src, err := runner.buildSBatchScript(nil, job)
	if err != nil {
		t.Fatalf("buildSBatchScript: %v", err)
	}
	if !strings.Contains(src, "#SBATCH -A acct") {
		t.Fatalf("expected account in script, got %q", src)
	}
	if !strings.Contains(src, "#SBATCH -p partition") {
		t.Fatalf("expected partition in script")
	}
	if !strings.Contains(src, "#SBATCH -c 2") {
		t.Fatalf("expected procs in script")
	}
	if !strings.Contains(src, "#SBATCH --mem=1024") {
		t.Fatalf("expected mem in script")
	}
	if !strings.Contains(src, "#SBATCH -t ") {
		t.Fatalf("expected walltime in script")
	}
	if !strings.Contains(src, "JOB_ID=$SLURM_JOB_ID") {
		t.Fatalf("expected job id env in script")
	}
}

func TestBuildSBatchScriptDependencyError(t *testing.T) {
	dep := jobs.NewJobDef("dep", "#!/bin/sh\necho dep")
	dep.JobId = "dep-1"
	dep.Status = jobs.RUNNING

	fake := &fakeBatchDB{
		jobsByID: map[string]*jobs.JobDef{
			dep.JobId: dep,
		},
	}

	runner := NewSlurmRunner(fake)
	job := jobs.NewJobDef("job", "#!/bin/sh\necho hi")
	job.JobId = "job-1"
	job.AfterOk = []string{dep.JobId}

	if _, err := runner.buildSBatchScript(context.Background(), job); err == nil {
		t.Fatal("expected dependency error for missing slurm_job_id")
	}
}

func TestSlurmUpdateJobStatus(t *testing.T) {
	binDir := t.TempDir()
	writeFakeBin(t, binDir, "sacct", "#!/bin/sh\njobid=\"\"\nfor arg in \"$@\"; do jobid=\"$arg\"; done\ncase \"$jobid\" in\n  11) echo '11|COMPLETED|2023-01-02T03:04:05|2023-01-02T03:05:05|0:0|node' ;;\n  12) echo '12|FAILED|2023-01-02T03:04:05|2023-01-02T03:05:05|1:0|node' ;;\n  13) echo '13|CANCELED|2023-01-02T03:04:05|2023-01-02T03:05:05|1:0|node' ;;\n  14) echo '14|RUNNING|2023-01-02T03:04:05|2023-01-02T03:05:05|0:0|node' ;;\nesac\n")
	oldPath := os.Getenv("PATH")
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+oldPath)

	proxyJobs := []*jobs.JobDef{
		{JobId: "job-11", RunningDetails: []jobs.JobRunningDetail{{Key: "slurm_job_id", Value: "11"}}},
		{JobId: "job-12", RunningDetails: []jobs.JobRunningDetail{{Key: "slurm_job_id", Value: "12"}}},
		{JobId: "job-13", RunningDetails: []jobs.JobRunningDetail{{Key: "slurm_job_id", Value: "13"}}},
		{JobId: "job-14", RunningDetails: []jobs.JobRunningDetail{{Key: "slurm_job_id", Value: "14"}}},
	}

	fake := &fakeBatchDB{proxyJobs: proxyJobs}
	runner := NewSlurmRunner(fake)
	runner.UpdateSlurmJobStatus(context.Background())

	if len(fake.proxyEnded) != 3 {
		t.Fatalf("expected 3 proxy end calls, got %d", len(fake.proxyEnded))
	}
	if len(fake.updates) == 0 {
		t.Fatal("expected running detail updates")
	}
}

func TestSlurmStartSubmitsJob(t *testing.T) {
	binDir := t.TempDir()
	writeFakeBin(t, binDir, "sbatch", "#!/bin/sh\necho '777'\n")
	oldPath := os.Getenv("PATH")
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+oldPath)

	job := jobs.NewJobDef("job", "#!/bin/sh\necho hi")
	job.JobId = "job-1"

	fake := &fakeBatchDB{
		fetchJobs: []*jobs.JobDef{job},
	}

	runner := NewSlurmRunner(fake).
		SetSlurmUsername("tester").
		SetMaxJobCount(1)

	ok := runner.Start()
	if !ok {
		t.Fatal("expected Start to submit a job")
	}
	if len(fake.proxyQueued) != 1 || fake.proxyQueued[0] != job.JobId {
		t.Fatalf("expected proxy queue for %s, got %v", job.JobId, fake.proxyQueued)
	}
	if len(fake.proxyQueueDetails) != 1 || fake.proxyQueueDetails[0]["slurm_job_id"] != "777" {
		t.Fatalf("expected slurm job id 777, got %v", fake.proxyQueueDetails)
	}
}

func TestSlurmSbatchEnv(t *testing.T) {
	binDir := t.TempDir()
	writeFakeBin(t, binDir, "sbatch", "#!/bin/sh\nif [ \"$FOO\" = \"bar\" ]; then echo '999'; else echo '0'; fi\n")
	oldPath := os.Getenv("PATH")
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+oldPath)

	jobID, err := SlurmSbatch("#!/bin/sh\n", []string{"FOO=bar"})
	if err != nil {
		t.Fatalf("SlurmSbatch: %v", err)
	}
	if jobID != "999" {
		t.Fatalf("expected job id 999, got %q", jobID)
	}
}

func TestSlurmSetters(t *testing.T) {
	runner := NewSlurmRunner(nil).
		SetSlurmMaxUserJobs(3).
		SetMaxJobCount(2).
		SetSlurmAccount("acct").
		SetSlurmUsername("user").
		SetSlurmPartition("part")

	if runner.maxUserJobs != 3 || runner.maxJobs != 2 {
		t.Fatal("expected max job limits to be set")
	}
	if runner.account != "acct" || runner.username != "user" || runner.partition != "part" {
		t.Fatal("expected slurm account settings to be set")
	}
}
