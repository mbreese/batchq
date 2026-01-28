package web

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mbreese/batchq/db"
	"github.com/mbreese/batchq/iniconfig"
	"github.com/mbreese/batchq/jobs"
)

func newWebTestDB(t *testing.T) db.BatchDB {
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

func TestResolveSocketPathAndListener(t *testing.T) {
	path, err := normalizeSocketPath("-")
	if err != nil {
		t.Fatalf("normalizeSocketPath: %v", err)
	}
	if path != "" {
		t.Fatalf("expected empty path, got %q", path)
	}

	tmp := filepath.Join(t.TempDir(), "sock")
	norm, err := normalizeSocketPath(tmp)
	if err != nil {
		t.Fatalf("normalizeSocketPath: %v", err)
	}
	if norm != tmp {
		t.Fatalf("expected %q, got %q", tmp, norm)
	}

	listener, kind, address, cleanup, err := createListener("", "127.0.0.1:0", false)
	if err != nil {
		t.Fatalf("createListener: %v", err)
	}
	cleanup()
	listener.Close()
	if kind != "tcp" {
		t.Fatalf("expected tcp listener, got %q", kind)
	}
	if !strings.HasPrefix(address, "127.0.0.1") {
		t.Fatalf("unexpected address %q", address)
	}
}

func TestResolvePathsFromConfig(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "config")
	content := "[batchq]\nweb_socket = /tmp/batchq.sock\nweb_listen = 127.0.0.1:8080\n"
	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cfg := iniconfig.LoadConfig(cfgPath, "batchq")
	opts := Options{Config: cfg}

	socket, err := resolveSocketPath(opts)
	if err != nil {
		t.Fatalf("resolveSocketPath: %v", err)
	}
	if socket != "/tmp/batchq.sock" {
		t.Fatalf("expected socket path from config, got %q", socket)
	}

	addr := resolveListenAddress(opts)
	if addr != "127.0.0.1:8080" {
		t.Fatalf("expected listen address from config, got %q", addr)
	}
}

func TestRemoveSocketIfExists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sock")
	if err := os.WriteFile(path, []byte("x"), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if err := removeSocketIfExists(path, false); err == nil {
		t.Fatal("expected error when socket exists without force")
	}

	if err := removeSocketIfExists(path, true); err != nil {
		t.Fatalf("expected force remove to succeed: %v", err)
	}
}

func TestLoadTemplatesAndDetailHelpers(t *testing.T) {
	templates, err := loadWebTemplates()
	if err != nil {
		t.Fatalf("loadWebTemplates: %v", err)
	}
	if templates.Lookup("base") == nil {
		t.Fatal("expected base template to be present")
	}

	details := []jobs.JobDefDetail{
		{Key: "script", Value: "#!/bin/sh\necho hi"},
		{Key: "mem", Value: "2048"},
		{Key: "walltime", Value: "60"},
		{Key: "env", Value: "A=1\n-|-\nB=2"},
	}
	rows, script := buildDetailRows(details, "script")
	if script == "" {
		t.Fatal("expected script to be returned")
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 detail rows, got %d", len(rows))
	}
	if countEnvEntries(details[3].Value) != 2 {
		t.Fatalf("expected 2 env entries")
	}

	runningDetails := []jobs.JobRunningDetail{
		{Key: "slurm_script", Value: "script"},
		{Key: "pid", Value: "42"},
	}
	rRows, rScript := buildDetailRowsRunning(runningDetails, "slurm_script")
	if rScript == "" {
		t.Fatal("expected slurm script to be returned")
	}
	if len(rRows) != 1 || rRows[0].Key != "pid" {
		t.Fatalf("unexpected running rows: %v", rRows)
	}
}

func TestStatusParsingHelpers(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/jobs?status=QUEUED&status=running", nil)
	statuses, selected := parseStatusFilter(req)
	if len(statuses) != 2 {
		t.Fatalf("expected 2 statuses, got %d", len(statuses))
	}
	if !selected["QUEUED"] || !selected["RUNNING"] {
		t.Fatalf("expected selected map to include queued/running, got %v", selected)
	}
	if len(statusLookup()) == 0 || len(statusOptions()) == 0 {
		t.Fatal("expected status lookup/options to be populated")
	}
	if !isActiveStatusSet(activeStatuses()) {
		t.Fatal("expected active statuses to be recognized")
	}
}

func TestHandlersRender(t *testing.T) {
	jobq := newWebTestDB(t)
	ctx := context.Background()

	parent := jobs.NewJobDef("parent", "#!/bin/sh\necho parent")
	parentSub := jobq.SubmitJob(ctx, parent)
	if parentSub == nil {
		t.Fatal("expected parent to submit")
	}

	child := jobs.NewJobDef("child", "#!/bin/sh\necho child")
	child.AddAfterOk(parentSub.JobId)
	childSub := jobq.SubmitJob(ctx, child)
	if childSub == nil {
		t.Fatal("expected child to submit")
	}

	templates, err := loadWebTemplates()
	if err != nil {
		t.Fatalf("loadWebTemplates: %v", err)
	}
	server := &webServer{db: jobq, templates: templates, verbose: false}

	queueReq := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	queueResp := httptest.NewRecorder()
	server.handleQueue(queueResp, queueReq)
	if queueResp.Code != http.StatusOK {
		t.Fatalf("expected queue status 200, got %d", queueResp.Code)
	}
	if !strings.Contains(queueResp.Body.String(), childSub.JobId) {
		t.Fatalf("expected queue output to contain job id %q", childSub.JobId)
	}

	jobReq := httptest.NewRequest(http.MethodGet, "/jobs/"+childSub.JobId, nil)
	jobResp := httptest.NewRecorder()
	server.handleJob(jobResp, jobReq)
	if jobResp.Code != http.StatusOK {
		t.Fatalf("expected job status 200, got %d", jobResp.Code)
	}
	if !strings.Contains(jobResp.Body.String(), "child") {
		t.Fatalf("expected job output to contain job name")
	}

	parentReq := httptest.NewRequest(http.MethodGet, "/jobs/"+parentSub.JobId, nil)
	parentResp := httptest.NewRecorder()
	server.handleJob(parentResp, parentReq)
	if parentResp.Code != http.StatusOK {
		t.Fatalf("expected parent job status 200, got %d", parentResp.Code)
	}
	if !strings.Contains(parentResp.Body.String(), "parent") {
		t.Fatalf("expected parent job output to contain job name")
	}

	searchReq := httptest.NewRequest(http.MethodGet, "/search?q=child", nil)
	searchResp := httptest.NewRecorder()
	server.handleSearch(searchResp, searchReq)
	if searchResp.Code != http.StatusOK {
		t.Fatalf("expected search status 200, got %d", searchResp.Code)
	}
	if !strings.Contains(searchResp.Body.String(), childSub.JobId) {
		t.Fatalf("expected search output to contain job id")
	}
}

func TestBuildTreesAndFilters(t *testing.T) {
	jobq := newWebTestDB(t)
	ctx := context.Background()

	parent := jobs.NewJobDef("parent", "#!/bin/sh\necho parent")
	parentSub := jobq.SubmitJob(ctx, parent)
	if parentSub == nil {
		t.Fatal("expected parent to submit")
	}

	child := jobs.NewJobDef("child", "#!/bin/sh\necho child")
	child.AddAfterOk(parentSub.JobId)
	childSub := jobq.SubmitJob(ctx, child)
	if childSub == nil {
		t.Fatal("expected child to submit")
	}

	grand := jobs.NewJobDef("grand", "#!/bin/sh\necho grand")
	grand.AddAfterOk(childSub.JobId)
	grandSub := jobq.SubmitJob(ctx, grand)
	if grandSub == nil {
		t.Fatal("expected grand to submit")
	}

	parentTree := buildParentTree(ctx, jobq, grandSub.JobId, map[string]bool{})
	if parentTree == nil || len(parentTree.Children) != 1 {
		t.Fatalf("expected parent tree with one child, got %+v", parentTree)
	}
	if parentTree.Children[0].Job.JobId != childSub.JobId {
		t.Fatalf("expected child in parent tree, got %+v", parentTree.Children[0].Job)
	}

	childTree := buildChildTree(ctx, jobq, parentSub.JobId, map[string]bool{})
	if childTree == nil || len(childTree.Children) != 1 {
		t.Fatalf("expected child tree with one child, got %+v", childTree)
	}

	server := &webServer{db: jobq, templates: nil, verbose: false}
	active := server.jobsForFilter(ctx, activeStatuses())
	if len(active) == 0 {
		t.Fatal("expected active jobs to be returned")
	}

	canceled := jobs.NewJobDef("canceled", "#!/bin/sh\necho canceled")
	canceledSub := jobq.SubmitJob(ctx, canceled)
	if canceledSub == nil {
		t.Fatal("expected canceled to submit")
	}
	if !jobq.CancelJob(ctx, canceledSub.JobId, "stop") {
		t.Fatal("expected cancel to succeed")
	}

	onlyCanceled := server.jobsForFilter(ctx, []jobs.StatusCode{jobs.CANCELED})
	if len(onlyCanceled) != 1 || onlyCanceled[0].JobId != canceledSub.JobId {
		t.Fatalf("expected only canceled job, got %v", onlyCanceled)
	}

	all := server.jobsForFilter(ctx, nil)
	if len(all) < 4 {
		t.Fatalf("expected showAll to include all jobs, got %d", len(all))
	}
}

func TestParseStatusListInvalids(t *testing.T) {
	selected := map[string]bool{}
	statuses := parseStatusList([]string{"queued", "invalid", "  ", "FAILED"}, selected)
	if len(statuses) != 2 {
		t.Fatalf("expected 2 statuses, got %d", len(statuses))
	}
	if !selected["QUEUED"] || !selected["FAILED"] {
		t.Fatalf("expected selected map to include queued/failed, got %v", selected)
	}
}
