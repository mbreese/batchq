package db

import (
	"context"
	"strings"
	"testing"

	"github.com/mbreese/batchq/jobs"
)

func TestStartAndEndJob(t *testing.T) {
	dbConn := newTestDB(t)
	ctx := context.Background()

	job := jobs.NewJobDef("runner", "echo hi")
	job.AddDetail("stdout", "/tmp/stdout-%JOBID")
	job.AddDetail("stderr", "/tmp/stderr-%JOBID")
	submitted := dbConn.SubmitJob(ctx, job)
	if submitted == nil {
		t.Fatal("expected job to submit")
	}

	if !dbConn.StartJob(ctx, submitted.JobId, "runner-1", map[string]string{"pid": "123"}) {
		t.Fatal("expected StartJob to succeed")
	}

	running := dbConn.GetJob(ctx, submitted.JobId)
	if running == nil || running.Status != jobs.RUNNING {
		t.Fatalf("expected job to be running, got %v", running)
	}
	if running.GetRunningDetail("pid", "") != "123" {
		t.Fatalf("expected running detail pid=123, got %q", running.GetRunningDetail("pid", ""))
	}
	if running.StartTime.IsZero() {
		t.Fatal("expected start time to be set")
	}

	if !dbConn.EndJob(ctx, submitted.JobId, "runner-1", 0) {
		t.Fatal("expected EndJob to succeed")
	}

	finished := dbConn.GetJob(ctx, submitted.JobId)
	if finished == nil || finished.Status != jobs.SUCCESS {
		t.Fatalf("expected job to be success, got %v", finished)
	}
	if finished.EndTime.IsZero() {
		t.Fatal("expected end time to be set")
	}
}

func TestUpdateJobRunningDetails(t *testing.T) {
	dbConn := newTestDB(t)
	ctx := context.Background()

	job := jobs.NewJobDef("runner", "echo hi")
	submitted := dbConn.SubmitJob(ctx, job)
	if submitted == nil {
		t.Fatal("expected job to submit")
	}

	if !dbConn.StartJob(ctx, submitted.JobId, "runner-1", map[string]string{"pid": "999"}) {
		t.Fatal("expected StartJob to succeed")
	}

	if !dbConn.UpdateJobRunningDetails(ctx, submitted.JobId, map[string]string{"slurm_status": "RUNNING"}) {
		t.Fatal("expected UpdateJobRunningDetails to succeed")
	}

	running := dbConn.GetJob(ctx, submitted.JobId)
	if running.GetRunningDetail("slurm_status", "") != "RUNNING" {
		t.Fatalf("expected slurm_status=RUNNING, got %q", running.GetRunningDetail("slurm_status", ""))
	}
}

func TestFetchNextHonorsLimits(t *testing.T) {
	dbConn := newTestDB(t)
	ctx := context.Background()

	big := jobs.NewJobDef("big", "echo big")
	big.AddDetail("procs", "8")
	big.AddDetail("mem", "4000")
	big.AddDetail("walltime", "3600")
	bigSub := dbConn.SubmitJob(ctx, big)
	if bigSub == nil {
		t.Fatal("expected big job to submit")
	}

	small := jobs.NewJobDef("small", "echo small")
	small.AddDetail("procs", "1")
	small.AddDetail("mem", "100")
	small.AddDetail("walltime", "60")
	smallSub := dbConn.SubmitJob(ctx, small)
	if smallSub == nil {
		t.Fatal("expected small job to submit")
	}

	job, hasMore := dbConn.FetchNext(ctx, []JobLimit{
		JobLimitProc(2),
		JobLimitMemoryMB(500),
		JobLimitTimeSec(120),
	})
	if job == nil || job.JobId != smallSub.JobId {
		t.Fatalf("expected small job to be selected, got %v", job)
	}
	if !hasMore {
		t.Fatal("expected hasMore to be true")
	}
}

func TestQueueAndCleanupControls(t *testing.T) {
	dbConn := newTestDB(t)
	ctx := context.Background()

	job := jobs.NewJobDef("queue", "echo queue")
	submitted := dbConn.SubmitJob(ctx, job)
	if submitted == nil {
		t.Fatal("expected job to submit")
	}

	if !dbConn.HoldJob(ctx, submitted.JobId) {
		t.Fatal("expected HoldJob to succeed")
	}
	held := dbConn.GetJob(ctx, submitted.JobId)
	if held.Status != jobs.USERHOLD {
		t.Fatalf("expected USERHOLD status, got %v", held.Status)
	}

	if !dbConn.ReleaseJob(ctx, submitted.JobId) {
		t.Fatal("expected ReleaseJob to succeed")
	}

	if !dbConn.TopJob(ctx, submitted.JobId) {
		t.Fatal("expected TopJob to succeed")
	}
	if !dbConn.NiceJob(ctx, submitted.JobId) {
		t.Fatal("expected NiceJob to succeed")
	}

	if !dbConn.CleanupJob(ctx, submitted.JobId) {
		t.Fatal("expected CleanupJob to succeed")
	}
	if dbConn.GetJob(ctx, submitted.JobId) != nil {
		t.Fatal("expected job to be removed after cleanup")
	}
}

func TestGetQueueJobsDetails(t *testing.T) {
	dbConn := newTestDB(t)
	ctx := context.Background()

	parent := jobs.NewJobDef("parent", "echo parent")
	parentSub := dbConn.SubmitJob(ctx, parent)
	if parentSub == nil {
		t.Fatal("expected parent to submit")
	}

	child := jobs.NewJobDef("child", "echo child")
	child.AddAfterOk(parentSub.JobId)
	child.AddDetail("procs", "2")
	child.AddDetail("mem", "256")
	childSub := dbConn.SubmitJob(ctx, child)
	if childSub == nil {
		t.Fatal("expected child to submit")
	}

	jobsList := dbConn.GetQueueJobs(ctx, true, false)
	if len(jobsList) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(jobsList))
	}

	foundChild := false
	for _, job := range jobsList {
		if job.JobId == childSub.JobId {
			foundChild = true
			if len(job.AfterOk) != 1 || job.AfterOk[0] != parentSub.JobId {
				t.Fatalf("expected child to have parent dep %q, got %v", parentSub.JobId, job.AfterOk)
			}
			procs := job.GetDetail("procs", "")
			mem := job.GetDetail("mem", "")
			if mem == "" || !strings.Contains(mem, "256") {
				t.Fatalf("expected mem detail to include 256, got %q", mem)
			}
			if procs == "" && !strings.Contains(mem, "procs=2") {
				t.Fatalf("expected procs detail to include 2, got %v", job.Details)
			}
		}
	}
	if !foundChild {
		t.Fatal("expected to find child job in queue list")
	}
}

func TestGetProxyJobs(t *testing.T) {
	dbConn := newTestDB(t)
	ctx := context.Background()

	job := jobs.NewJobDef("proxy", "echo proxy")
	submitted := dbConn.SubmitJob(ctx, job)
	if submitted == nil {
		t.Fatal("expected job to submit")
	}
	if !dbConn.ProxyQueueJob(ctx, submitted.JobId, "runner", map[string]string{"slurm_job_id": "321"}) {
		t.Fatal("expected proxy queue to succeed")
	}

	proxyJobs := dbConn.GetProxyJobs(ctx)
	if len(proxyJobs) != 1 || proxyJobs[0].JobId != submitted.JobId {
		t.Fatalf("expected proxy job %q, got %v", submitted.JobId, proxyJobs)
	}
}

func TestUpdateQueueTransitions(t *testing.T) {
	dbConn := newTestDB(t)
	ctx := context.Background()

	parent := jobs.NewJobDef("parent", "echo parent")
	parentSub := dbConn.SubmitJob(ctx, parent)
	if parentSub == nil {
		t.Fatal("expected parent to submit")
	}

	child := jobs.NewJobDef("child", "echo child")
	child.AddAfterOk(parentSub.JobId)
	childSub := dbConn.SubmitJob(ctx, child)
	if childSub == nil {
		t.Fatal("expected child to submit")
	}
	if childSub.Status != jobs.WAITING {
		t.Fatalf("expected child to start WAITING, got %v", childSub.Status)
	}

	if !dbConn.ProxyQueueJob(ctx, parentSub.JobId, "runner", map[string]string{"slurm_job_id": "999"}) {
		t.Fatal("expected proxy queue to succeed")
	}

	if next, _ := dbConn.FetchNext(ctx, nil); next == nil || next.JobId != childSub.JobId {
		t.Fatalf("expected child to be queued after parent proxy, got %v", next)
	}
}

func TestGetJobsByStatusAndCounts(t *testing.T) {
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

	queuedJobs := dbConn.GetJobsByStatus(ctx, []jobs.StatusCode{jobs.QUEUED}, false)
	if len(queuedJobs) != 1 || queuedJobs[0].JobId != queuedSub.JobId {
		t.Fatalf("expected queued list to include %q, got %v", queuedSub.JobId, queuedJobs)
	}

	counts := dbConn.GetJobStatusCounts(ctx, false)
	if counts[jobs.CANCELED] != 0 {
		t.Fatalf("expected canceled count to be 0 when showAll=false, got %d", counts[jobs.CANCELED])
	}
	if counts[jobs.QUEUED] != 1 {
		t.Fatalf("expected queued count to be 1, got %d", counts[jobs.QUEUED])
	}
}

func TestCancelCascade(t *testing.T) {
	dbConn := newTestDB(t)
	ctx := context.Background()

	parent := jobs.NewJobDef("parent", "echo parent")
	parentSub := dbConn.SubmitJob(ctx, parent)
	if parentSub == nil {
		t.Fatal("expected parent to submit")
	}

	child := jobs.NewJobDef("child", "echo child")
	child.AddAfterOk(parentSub.JobId)
	childSub := dbConn.SubmitJob(ctx, child)
	if childSub == nil {
		t.Fatal("expected child to submit")
	}

	grandchild := jobs.NewJobDef("grand", "echo grand")
	grandchild.AddAfterOk(childSub.JobId)
	grandSub := dbConn.SubmitJob(ctx, grandchild)
	if grandSub == nil {
		t.Fatal("expected grandchild to submit")
	}

	if !dbConn.CancelJob(ctx, parentSub.JobId, "cancel parent") {
		t.Fatal("expected parent cancel to succeed")
	}

	parentJob := dbConn.GetJob(ctx, parentSub.JobId)
	childJob := dbConn.GetJob(ctx, childSub.JobId)
	grandJob := dbConn.GetJob(ctx, grandSub.JobId)
	if parentJob.Status != jobs.CANCELED || childJob.Status != jobs.CANCELED || grandJob.Status != jobs.CANCELED {
		t.Fatalf("expected cascade cancels, got parent=%v child=%v grand=%v", parentJob.Status, childJob.Status, grandJob.Status)
	}
}

func TestSuccessCascadeQueuesChild(t *testing.T) {
	dbConn := newTestDB(t)
	ctx := context.Background()

	parent := jobs.NewJobDef("parent", "echo parent")
	parentSub := dbConn.SubmitJob(ctx, parent)
	if parentSub == nil {
		t.Fatal("expected parent to submit")
	}

	child := jobs.NewJobDef("child", "echo child")
	child.AddAfterOk(parentSub.JobId)
	childSub := dbConn.SubmitJob(ctx, child)
	if childSub == nil {
		t.Fatal("expected child to submit")
	}

	if !dbConn.StartJob(ctx, parentSub.JobId, "runner-1", map[string]string{"pid": "1"}) {
		t.Fatal("expected StartJob to succeed")
	}
	if !dbConn.EndJob(ctx, parentSub.JobId, "runner-1", 0) {
		t.Fatal("expected EndJob to succeed")
	}

	next, _ := dbConn.FetchNext(ctx, nil)
	if next == nil || next.JobId != childSub.JobId {
		t.Fatalf("expected child to be queued after parent success, got %v", next)
	}
}

func TestProxyFailureCascade(t *testing.T) {
	dbConn := newTestDB(t)
	ctx := context.Background()

	parent := jobs.NewJobDef("parent", "echo parent")
	parentSub := dbConn.SubmitJob(ctx, parent)
	if parentSub == nil {
		t.Fatal("expected parent to submit")
	}

	child := jobs.NewJobDef("child", "echo child")
	child.AddAfterOk(parentSub.JobId)
	childSub := dbConn.SubmitJob(ctx, child)
	if childSub == nil {
		t.Fatal("expected child to submit")
	}

	if !dbConn.ProxyQueueJob(ctx, parentSub.JobId, "runner", map[string]string{"slurm_job_id": "111"}) {
		t.Fatal("expected proxy queue to succeed")
	}
	if !dbConn.ProxyEndJob(ctx, parentSub.JobId, jobs.FAILED, "2023-01-01 00:00:00 UTC", "2023-01-01 00:00:01 UTC", 1) {
		t.Fatal("expected proxy end to succeed")
	}

	childJob := dbConn.GetJob(ctx, childSub.JobId)
	if childJob.Status != jobs.CANCELED {
		t.Fatalf("expected child to be canceled on proxy failure, got %v", childJob.Status)
	}
	if childJob.Notes == "" {
		t.Fatal("expected child cancel notes to be populated")
	}
}

func TestProxySuccessQueuesChild(t *testing.T) {
	dbConn := newTestDB(t)
	ctx := context.Background()

	parent := jobs.NewJobDef("parent", "echo parent")
	parentSub := dbConn.SubmitJob(ctx, parent)
	if parentSub == nil {
		t.Fatal("expected parent to submit")
	}

	child := jobs.NewJobDef("child", "echo child")
	child.AddAfterOk(parentSub.JobId)
	childSub := dbConn.SubmitJob(ctx, child)
	if childSub == nil {
		t.Fatal("expected child to submit")
	}

	if !dbConn.ProxyQueueJob(ctx, parentSub.JobId, "runner", map[string]string{"slurm_job_id": "222"}) {
		t.Fatal("expected proxy queue to succeed")
	}
	if !dbConn.ProxyEndJob(ctx, parentSub.JobId, jobs.SUCCESS, "2023-01-01 00:00:00 UTC", "2023-01-01 00:00:01 UTC", 0) {
		t.Fatal("expected proxy end to succeed")
	}

	next, _ := dbConn.FetchNext(ctx, nil)
	if next == nil || next.JobId != childSub.JobId {
		t.Fatalf("expected child to be queued after proxy success, got %v", next)
	}
}

func TestUpdateQueueMixedDeps(t *testing.T) {
	dbConn := newTestDB(t)
	ctx := context.Background()

	parentA := jobs.NewJobDef("parentA", "echo a")
	parentASub := dbConn.SubmitJob(ctx, parentA)
	if parentASub == nil {
		t.Fatal("expected parentA to submit")
	}
	if !dbConn.StartJob(ctx, parentASub.JobId, "runner-1", map[string]string{"pid": "1"}) {
		t.Fatal("expected StartJob to succeed")
	}
	if !dbConn.EndJob(ctx, parentASub.JobId, "runner-1", 0) {
		t.Fatal("expected EndJob to succeed")
	}

	parentB := jobs.NewJobDef("parentB", "echo b")
	parentBSub := dbConn.SubmitJob(ctx, parentB)
	if parentBSub == nil {
		t.Fatal("expected parentB to submit")
	}
	if !dbConn.ProxyQueueJob(ctx, parentBSub.JobId, "runner", map[string]string{"slurm_job_id": "333"}) {
		t.Fatal("expected proxy queue to succeed")
	}

	child := jobs.NewJobDef("child", "echo child")
	child.AddAfterOk(parentASub.JobId)
	child.AddAfterOk(parentBSub.JobId)
	childSub := dbConn.SubmitJob(ctx, child)
	if childSub == nil {
		t.Fatal("expected child to submit")
	}

	next, _ := dbConn.FetchNext(ctx, nil)
	if next == nil || next.JobId != childSub.JobId {
		t.Fatalf("expected child to be queued with mixed deps, got %v", next)
	}
}

func TestUpdateQueueCancelReason(t *testing.T) {
	dbConn := newTestDB(t)
	ctx := context.Background()

	parent := jobs.NewJobDef("parent", "echo parent")
	parentSub := dbConn.SubmitJob(ctx, parent)
	if parentSub == nil {
		t.Fatal("expected parent to submit")
	}

	child := jobs.NewJobDef("child", "echo child")
	child.AddAfterOk(parentSub.JobId)
	childSub := dbConn.SubmitJob(ctx, child)
	if childSub == nil {
		t.Fatal("expected child to submit")
	}

	if !dbConn.StartJob(ctx, parentSub.JobId, "runner-1", map[string]string{"pid": "1"}) {
		t.Fatal("expected StartJob to succeed")
	}
	if !dbConn.EndJob(ctx, parentSub.JobId, "runner-1", 2) {
		t.Fatal("expected EndJob to succeed")
	}

	dbConn.FetchNext(ctx, nil)
	childJob := dbConn.GetJob(ctx, childSub.JobId)
	if childJob.Status != jobs.CANCELED {
		t.Fatalf("expected child canceled, got %v", childJob.Status)
	}
	if childJob.Notes == "" {
		t.Fatal("expected cancel notes to be populated")
	}
}

func TestFetchNextNoJobs(t *testing.T) {
	dbConn := newTestDB(t)
	ctx := context.Background()

	job, hasMore := dbConn.FetchNext(ctx, nil)
	if job != nil || hasMore {
		t.Fatalf("expected no jobs, got %v hasMore=%t", job, hasMore)
	}
}

func TestGetQueueJobsActiveFilter(t *testing.T) {
	dbConn := newTestDB(t)
	ctx := context.Background()

	active := jobs.NewJobDef("active", "echo active")
	activeSub := dbConn.SubmitJob(ctx, active)
	if activeSub == nil {
		t.Fatal("expected active to submit")
	}

	done := jobs.NewJobDef("done", "echo done")
	doneSub := dbConn.SubmitJob(ctx, done)
	if doneSub == nil {
		t.Fatal("expected done to submit")
	}
	if !dbConn.CancelJob(ctx, doneSub.JobId, "stop") {
		t.Fatal("expected cancel to succeed")
	}

	list := dbConn.GetQueueJobs(ctx, false, false)
	if len(list) != 1 || list[0].JobId != activeSub.JobId {
		t.Fatalf("expected only active job, got %v", list)
	}
}
