package cmd

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mbreese/batchq/db"
	"github.com/mbreese/batchq/iniconfig"
	"github.com/mbreese/batchq/jobs"
	"github.com/spf13/cobra"
)

func captureOutput(t *testing.T, fn func()) string {
	t.Helper()

	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("Pipe: %v", err)
	}
	os.Stdout = w

	fn()

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	r.Close()
	return buf.String()
}

func setupCmdDB(t *testing.T) (string, db.BatchDB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "batchq.db")
	if err := db.InitDB("sqlite3://"+path, true); err != nil {
		t.Fatalf("InitDB: %v", err)
	}
	jobq, err := db.OpenDB("sqlite3://" + path)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	t.Cleanup(func() { jobq.Close() })
	return "sqlite3://" + path, jobq
}

func resetSubmitGlobals() {
	jobName = ""
	jobDeps = ""
	jobProcs = -1
	jobMemStr = ""
	jobTimeStr = ""
	jobWd = ""
	jobStdout = ""
	jobStderr = ""
	jobEnv = false
	jobHold = false
	verbose = false
	slurmMode = false
}

func TestParseNumericRange(t *testing.T) {
	start, end, ok := parseNumericRange("3-5")
	if !ok || start != 3 || end != 5 {
		t.Fatalf("expected range 3-5, got %d-%d ok=%t", start, end, ok)
	}
	if _, _, ok := parseNumericRange("bad"); ok {
		t.Fatal("expected bad range to be rejected")
	}
}

func TestQueueStatusSummaryCommands(t *testing.T) {
	path, jobq := setupCmdDB(t)
	dbpath = path
	Config = iniconfig.LoadConfig(filepath.Join(t.TempDir(), "missing"), "batchq")
	jobShowAll = false

	ctx := context.Background()
	job := jobs.NewJobDef("alpha", "#!/bin/sh\necho alpha")
	jobSub := jobq.SubmitJob(ctx, job)
	if jobSub == nil {
		t.Fatal("expected job to submit")
	}

	output := captureOutput(t, func() {
		queueCmd.Run(queueCmd, []string{})
	})
	if !strings.Contains(output, jobSub.JobId) {
		t.Fatalf("expected queue output to contain job id %q", jobSub.JobId)
	}

	statusOut := captureOutput(t, func() {
		statusCmd.Run(statusCmd, []string{})
	})
	if !strings.Contains(statusOut, jobSub.JobId) {
		t.Fatalf("expected status output to contain job id %q", jobSub.JobId)
	}

	summaryOut := captureOutput(t, func() {
		summaryCmd.Run(summaryCmd, []string{})
	})
	if !strings.Contains(summaryOut, "QUEUED") {
		t.Fatalf("expected summary output to include QUEUED")
	}
}

func TestHoldReleaseAndPriorityCommands(t *testing.T) {
	path, jobq := setupCmdDB(t)
	dbpath = path
	Config = iniconfig.LoadConfig(filepath.Join(t.TempDir(), "missing"), "batchq")

	ctx := context.Background()
	job := jobs.NewJobDef("beta", "#!/bin/sh\necho beta")
	jobSub := jobq.SubmitJob(ctx, job)
	if jobSub == nil {
		t.Fatal("expected job to submit")
	}

	captureOutput(t, func() { holdCmd.Run(holdCmd, []string{jobSub.JobId}) })
	if got := jobq.GetJob(ctx, jobSub.JobId); got.Status != jobs.USERHOLD {
		t.Fatalf("expected USERHOLD, got %v", got.Status)
	}

	captureOutput(t, func() { releaseCmd.Run(releaseCmd, []string{jobSub.JobId}) })
	if got := jobq.GetJob(ctx, jobSub.JobId); got.Status != jobs.WAITING {
		t.Fatalf("expected WAITING after release, got %v", got.Status)
	}

	captureOutput(t, func() { topCmd.Run(topCmd, []string{jobSub.JobId}) })
	captureOutput(t, func() { niceCmd.Run(niceCmd, []string{jobSub.JobId}) })
}

func TestCancelCommand(t *testing.T) {
	path, jobq := setupCmdDB(t)
	dbpath = path
	Config = iniconfig.LoadConfig(filepath.Join(t.TempDir(), "missing"), "batchq")

	ctx := context.Background()
	job := jobs.NewJobDef("gamma", "#!/bin/sh\necho gamma")
	jobSub := jobq.SubmitJob(ctx, job)
	if jobSub == nil {
		t.Fatal("expected job to submit")
	}

	cancelReason = "test cancel"
	captureOutput(t, func() { cancelCmd.Run(cancelCmd, []string{jobSub.JobId}) })
	if got := jobq.GetJob(ctx, jobSub.JobId); got.Status != jobs.CANCELED {
		t.Fatalf("expected CANCELED, got %v", got.Status)
	}
}

func TestDetailsCommand(t *testing.T) {
	path, jobq := setupCmdDB(t)
	dbpath = path
	Config = iniconfig.LoadConfig(filepath.Join(t.TempDir(), "missing"), "batchq")

	ctx := context.Background()
	job := jobs.NewJobDef("delta", "#!/bin/sh\necho delta")
	jobSub := jobq.SubmitJob(ctx, job)
	if jobSub == nil {
		t.Fatal("expected job to submit")
	}

	cmd := &cobra.Command{}
	output := captureOutput(t, func() {
		detailsCmd.Run(cmd, []string{jobSub.JobId})
	})
	if !strings.Contains(output, jobSub.JobId) {
		t.Fatalf("expected details output to contain job id %q", jobSub.JobId)
	}
}

func TestSubmitCommandBatchqScript(t *testing.T) {
	path, jobq := setupCmdDB(t)
	dbpath = path
	Config = iniconfig.LoadConfig(filepath.Join(t.TempDir(), "missing"), "batchq")
	resetSubmitGlobals()

	scriptPath := filepath.Join(t.TempDir(), "job.sh")
	script := strings.Join([]string{
		"#!/bin/sh",
		"#BATCHQ -name demo-job",
		"#BATCHQ -procs 3",
		"#BATCHQ -mem 2GB",
		"#BATCHQ -walltime 0:10:00",
		"#BATCHQ -wd /tmp",
		"#BATCHQ -stdout /tmp/out-%JOBID",
		"#BATCHQ -stderr /tmp/err-%JOBID",
		"echo hi",
	}, "\n")
	if err := os.WriteFile(scriptPath, []byte(script), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	output := captureOutput(t, func() {
		submitCmd.Run(submitCmd, []string{scriptPath})
	})
	jobID := strings.TrimSpace(output)
	if jobID == "" {
		t.Fatal("expected job id output")
	}

	ctx := context.Background()
	job := jobq.GetJob(ctx, jobID)
	if job == nil {
		t.Fatal("expected submitted job in db")
	}
	if job.Name != "demo-job" {
		t.Fatalf("expected job name demo-job, got %q", job.Name)
	}
	if job.GetDetail("procs", "") != "3" {
		t.Fatalf("expected procs=3, got %q", job.GetDetail("procs", ""))
	}
	if job.GetDetail("mem", "") != "2000" {
		t.Fatalf("expected mem=2000, got %q", job.GetDetail("mem", ""))
	}
	if job.GetDetail("walltime", "") != "600" {
		t.Fatalf("expected walltime=600, got %q", job.GetDetail("walltime", ""))
	}
	if job.GetDetail("wd", "") != "/tmp" {
		t.Fatalf("expected wd=/tmp, got %q", job.GetDetail("wd", ""))
	}
}

func TestSubmitCommandSlurmScript(t *testing.T) {
	path, jobq := setupCmdDB(t)
	dbpath = path
	Config = iniconfig.LoadConfig(filepath.Join(t.TempDir(), "missing"), "batchq")
	resetSubmitGlobals()
	slurmMode = true

	scriptPath := filepath.Join(t.TempDir(), "job.slurm")
	script := strings.Join([]string{
		"#!/bin/sh",
		"#SBATCH --job-name=slurm-job",
		"#SBATCH --cpus-per-task=4",
		"#SBATCH --mem=1G",
		"#SBATCH --time=0:05:00",
		"#SBATCH --chdir=/tmp",
		"#SBATCH --output=/tmp/out-%j",
		"#SBATCH --error=/tmp/err-%j",
		"#SBATCH --export=ALL",
		"echo hi",
	}, "\n")
	if err := os.WriteFile(scriptPath, []byte(script), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	output := captureOutput(t, func() {
		submitCmd.Run(submitCmd, []string{scriptPath})
	})
	jobID := strings.TrimSpace(output)
	if jobID == "" {
		t.Fatal("expected job id output")
	}

	ctx := context.Background()
	job := jobq.GetJob(ctx, jobID)
	if job == nil {
		t.Fatal("expected submitted job in db")
	}
	if job.Name != "slurm-job" {
		t.Fatalf("expected job name slurm-job, got %q", job.Name)
	}
	if job.GetDetail("procs", "") != "4" {
		t.Fatalf("expected procs=4, got %q", job.GetDetail("procs", ""))
	}
	if job.GetDetail("mem", "") != "1000" {
		t.Fatalf("expected mem=1000, got %q", job.GetDetail("mem", ""))
	}
	if job.GetDetail("walltime", "") != "300" {
		t.Fatalf("expected walltime=300, got %q", job.GetDetail("walltime", ""))
	}
	if !strings.Contains(job.GetDetail("stdout", ""), jobID) {
		t.Fatalf("expected stdout path to include job id, got %q", job.GetDetail("stdout", ""))
	}
	if !strings.Contains(job.GetDetail("stderr", ""), jobID) {
		t.Fatalf("expected stderr path to include job id, got %q", job.GetDetail("stderr", ""))
	}
	if job.GetDetail("env", "") == "" {
		t.Fatal("expected env detail to be populated")
	}
}

func TestIsDirectory(t *testing.T) {
	dir := t.TempDir()
	isDir, err := isDirectory(dir)
	if err != nil || !isDir {
		t.Fatalf("expected %q to be directory, got dir=%t err=%v", dir, isDir, err)
	}

	filePath := filepath.Join(t.TempDir(), "file.txt")
	if err := os.WriteFile(filePath, []byte("x"), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	isDir, err = isDirectory(filePath)
	if err != nil || isDir {
		t.Fatalf("expected %q to be file, got dir=%t err=%v", filePath, isDir, err)
	}

	isDir, err = isDirectory(filepath.Join(t.TempDir(), "missing") + string(os.PathSeparator))
	if err != nil || !isDir {
		t.Fatalf("expected missing path with trailing slash to be dir, got dir=%t err=%v", isDir, err)
	}
}
