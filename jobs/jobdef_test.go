package jobs

import "testing"

func TestJobDefDetailMutationRules(t *testing.T) {
	job := NewJobDef("name", "echo test")
	baseLen := len(job.Details)

	job.AddDetail("foo", "bar")
	if len(job.Details) != baseLen+1 {
		t.Fatalf("expected detail added before submit, got %d", len(job.Details))
	}

	job.JobId = "job-123"
	job.AddDetail("bar", "baz")
	if len(job.Details) != baseLen+1 {
		t.Fatalf("expected no details added after submit, got %d", len(job.Details))
	}
}

func TestJobDefAfterOkUniqueAndLocked(t *testing.T) {
	job := NewJobDef("name", "echo test")
	job.AddAfterOk("dep-1")
	job.AddAfterOk("dep-1")
	job.AddAfterOk("dep-2")

	if len(job.AfterOk) != 2 {
		t.Fatalf("expected 2 unique deps, got %d", len(job.AfterOk))
	}

	job.JobId = "job-123"
	job.AddAfterOk("dep-3")
	if len(job.AfterOk) != 2 {
		t.Fatalf("expected no deps added after submit, got %d", len(job.AfterOk))
	}
}

func TestJobDefGetDetailDefault(t *testing.T) {
	job := NewJobDef("name", "echo test")
	if got := job.GetDetail("missing", "default"); got != "default" {
		t.Fatalf("expected default value, got %q", got)
	}
}
