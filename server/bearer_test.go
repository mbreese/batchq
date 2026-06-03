package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/mbreese/batchq/api"
	"github.com/mbreese/batchq/service"
	"github.com/mbreese/batchq/storage"
	"github.com/mbreese/batchq/support"
)

// newServerWithMasterKey wires a server with a controlled master key
// path so the test can mint tokens under the same key the middleware
// will use to verify them.
func newServerWithMasterKey(t *testing.T) (*httptest.Server, storage.Storage, []byte) {
	t.Helper()
	dir := t.TempDir()
	st, err := storage.Open(context.Background(), filepath.Join(dir, "bq.db"), storage.Options{})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	svc := service.New(st)
	s, err := New(svc, Options{
		Listen:        "unix:///dev/null", // unused; httptest fronts the routes
		MasterKeyPath: filepath.Join(dir, "master.key"),
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	ts := httptest.NewServer(s.routes())
	t.Cleanup(ts.Close)
	key, err := support.LoadOrCreateMasterKey(filepath.Join(dir, "master.key"))
	if err != nil {
		t.Fatalf("load key: %v", err)
	}
	return ts, st, key
}

// mintToken creates a token for the named tenant and returns its
// plaintext form (what the client would send).
func mintToken(t *testing.T, st storage.Storage, key []byte, tenantName string) string {
	t.Helper()
	tenant, err := st.GetTenantByName(context.Background(), tenantName)
	if err != nil {
		t.Fatalf("GetTenantByName: %v", err)
	}
	signer := support.NewTokenSigner(key)
	plaintext, hmac, err := signer.GenerateToken()
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	if _, err := st.CreateToken(context.Background(), tenant.ID, hmac, "", time.Time{}); err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	return plaintext
}

// authedReq builds a request with the given bearer token attached.
func authedReq(t *testing.T, method, url, token string, body any) *http.Request {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatalf("new req: %v", err)
	}
	if token != "" {
		req.Header.Set(api.HeaderAuthorization, "Bearer "+token)
	}
	return req
}

// A submit with alice's bearer token writes a job under alice; bob's
// token can't see it.
func TestBearerToken_CrossTenantIsolation(t *testing.T) {
	ts, st, key := newServerWithMasterKey(t)
	ctx := context.Background()

	if _, err := st.CreateTenant(ctx, "alice", storage.TenantKindRemote); err != nil {
		t.Fatalf("CreateTenant alice: %v", err)
	}
	if _, err := st.CreateTenant(ctx, "bob", storage.TenantKindRemote); err != nil {
		t.Fatalf("CreateTenant bob: %v", err)
	}

	aliceTok := mintToken(t, st, key, "alice")
	bobTok := mintToken(t, st, key, "bob")

	// alice submits.
	body := api.SubmitJobRequest{Details: map[string]string{"script": "echo hi"}}
	req := authedReq(t, "POST", ts.URL+api.Prefix+api.RouteJobs, aliceTok, body)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("alice submit: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		got, _ := io.ReadAll(resp.Body)
		t.Fatalf("alice submit status: %d (%s)", resp.StatusCode, got)
	}
	var sr api.SubmitJobResponse
	if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	jobID := sr.Job.JobID

	// bob lists his queue — should be empty.
	listReq := authedReq(t, "GET", ts.URL+api.Prefix+api.RouteJobs, bobTok, nil)
	listResp, err := ts.Client().Do(listReq)
	if err != nil {
		t.Fatalf("bob list: %v", err)
	}
	defer listResp.Body.Close()
	var lr api.ListJobsResponse
	if err := json.NewDecoder(listResp.Body).Decode(&lr); err != nil {
		t.Fatalf("decode bob list: %v", err)
	}
	if len(lr.Jobs) != 0 {
		t.Fatalf("bob saw %d jobs in his own queue, want 0", len(lr.Jobs))
	}

	// bob GETs alice's job by id — 404 (not 403; don't leak existence).
	getReq := authedReq(t, "GET", ts.URL+api.Prefix+"/jobs/"+jobID, bobTok, nil)
	getResp, err := ts.Client().Do(getReq)
	if err != nil {
		t.Fatalf("bob get: %v", err)
	}
	getResp.Body.Close()
	if getResp.StatusCode != http.StatusNotFound {
		t.Fatalf("bob get of alice's job: %d, want 404", getResp.StatusCode)
	}
}

// An invalid bearer token (unknown to the server) returns 401.
func TestBearerToken_InvalidReturns401(t *testing.T) {
	ts, st, key := newServerWithMasterKey(t)
	if _, err := st.CreateTenant(context.Background(), "alice", storage.TenantKindRemote); err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}
	_ = key

	// Mint a token but use a wrong one in the request.
	bogus := "batchq_pat_" + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	req := authedReq(t, "GET", ts.URL+api.Prefix+api.RouteJobs, bogus, nil)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status: %d, want 401", resp.StatusCode)
	}
}

// A revoked token is treated identically to an unknown one (401).
func TestBearerToken_RevokedReturns401(t *testing.T) {
	ts, st, key := newServerWithMasterKey(t)
	ctx := context.Background()
	if _, err := st.CreateTenant(ctx, "alice", storage.TenantKindRemote); err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}

	alice, _ := st.GetTenantByName(ctx, "alice")
	signer := support.NewTokenSigner(key)
	plaintext, hmac, _ := signer.GenerateToken()
	tok, err := st.CreateToken(ctx, alice.ID, hmac, "", time.Time{})
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	if err := st.RevokeToken(ctx, tok.ID); err != nil {
		t.Fatalf("RevokeToken: %v", err)
	}

	req := authedReq(t, "GET", ts.URL+api.Prefix+api.RouteJobs, plaintext, nil)
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status: %d, want 401", resp.StatusCode)
	}
}
