package jobs

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

func captureStdout(t *testing.T, fn func()) string {
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

func TestJobPrintIncludesDetails(t *testing.T) {
	job := NewJobDef("demo", "#!/bin/sh\necho demo")
	job.AddDetail("walltime", "60")
	job.AddDetail("mem", "2048")
	job.AddDetail("env", "A=1\n-|-\nB=2")
	job.JobId = "job-1"
	job.Status = RUNNING
	job.RunningDetails = []JobRunningDetail{{Key: "slurm_script", Value: "#!/bin/sh\necho slurm"}}

	out := captureStdout(t, func() {
		job.Print()
	})

	if !strings.Contains(out, "jobid") || !strings.Contains(out, "job-1") {
		t.Fatalf("expected output to include job id, got %q", out)
	}
	if !strings.Contains(out, "walltime") || !strings.Contains(out, "1m") {
		t.Fatalf("expected output to include walltime, got %q", out)
	}
	if !strings.Contains(out, "mem") || !strings.Contains(out, "2048") {
		t.Fatalf("expected output to include mem, got %q", out)
	}
	if !strings.Contains(out, "slurm script") || !strings.Contains(out, "echo slurm") {
		t.Fatalf("expected output to include slurm script, got %q", out)
	}
}

func TestGetRunningDetailDefault(t *testing.T) {
	job := NewJobDef("demo", "#!/bin/sh\necho demo")
	if got := job.GetRunningDetail("missing", "default"); got != "default" {
		t.Fatalf("expected default, got %q", got)
	}
}
