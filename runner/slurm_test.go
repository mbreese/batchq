package runner

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mbreese/batchq/jobs"
)

func writeFakeBin(t *testing.T, dir, name, content string) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0755); err != nil {
		t.Fatalf("WriteFile %s: %v", name, err)
	}
}

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
