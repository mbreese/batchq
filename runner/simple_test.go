package runner

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mbreese/batchq/db"
	"github.com/mbreese/batchq/jobs"
)

func newRunnerTestDB(t *testing.T) db.BatchDB {
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
	return jobq
}

func TestInterruptibleSleep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := interruptibleSleep(ctx, 10*time.Millisecond); err == nil {
		t.Fatal("expected sleep to be interrupted")
	}

	ctx2 := context.Background()
	if err := interruptibleSleep(ctx2, 5*time.Millisecond); err != nil {
		t.Fatalf("expected sleep to complete, got %v", err)
	}
}

func TestSimpleRunnerDrainFlag(t *testing.T) {
	jobq := newRunnerTestDB(t)
	runner := NewSimpleRunner(jobq)

	base := t.TempDir()
	oldHome := os.Getenv("BATCHQ_HOME")
	t.Setenv("BATCHQ_HOME", base)
	defer os.Setenv("BATCHQ_HOME", oldHome)

	drainPath := filepath.Join(base, "drain")
	if runner.IsDrain() {
		t.Fatal("expected drain to be false when file missing")
	}

	if err := os.WriteFile(drainPath, []byte("1"), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if !runner.IsDrain() {
		t.Fatal("expected drain to be true when file exists")
	}
}

func TestSimpleRunnerSetters(t *testing.T) {
	jobq := newRunnerTestDB(t)
	runner := NewSimpleRunner(jobq)

	runner.SetMaxProcs(4).
		SetMaxMemMB(1024).
		SetMaxWalltimeSec(3600).
		SetMaxJobCount(2).
		SetForever(true).
		SetShell("/bin/sh").
		SetCgroupV2(false).
		SetCgroupV1(false)

	if runner.maxProcs != 4 || runner.maxMemoryMb != 1024 || runner.maxWalltimeSec != 3600 {
		t.Fatal("expected runner limits to be updated")
	}
	if runner.maxJobs != 2 || !runner.foreverMode || runner.shellBin != "/bin/sh" {
		t.Fatal("expected runner settings to be updated")
	}
}

func TestSimpleRunnerExecutesJob(t *testing.T) {
	base := t.TempDir()
	t.Setenv("BATCHQ_HOME", base)

	jobq := newRunnerTestDB(t)
	runner := NewSimpleRunner(jobq).
		SetMaxJobCount(1).
		SetForever(false).
		SetCgroupV2(false).
		SetCgroupV1(false)

	script := "#!/bin/bash\necho \"hello\"\n"
	job := jobs.NewJobDef("hello-job", script)
	job.AddDetail("wd", base)
	job.AddDetail("stdout", filepath.Join(base, "out-%JOBID.txt"))
	job.AddDetail("stderr", filepath.Join(base, "err-%JOBID.txt"))

	ctx := context.Background()
	submitted := jobq.SubmitJob(ctx, job)
	if submitted == nil {
		t.Fatal("expected job submission to succeed")
	}

	if ok := runner.Start(); !ok {
		t.Fatal("expected runner to execute at least one job")
	}

	finished := jobq.GetJob(ctx, submitted.JobId)
	if finished == nil || finished.Status != jobs.SUCCESS {
		t.Fatalf("expected job success, got %v", finished)
	}

	stdoutPath := finished.GetDetail("stdout", "")
	if stdoutPath == "" {
		t.Fatal("expected stdout path")
	}
	data, err := os.ReadFile(stdoutPath)
	if err != nil {
		t.Fatalf("ReadFile stdout: %v", err)
	}
	if strings.TrimSpace(string(data)) != "hello" {
		t.Fatalf("expected stdout to contain hello, got %q", string(data))
	}
}
