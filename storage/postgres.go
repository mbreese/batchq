package storage

// postgres.go is the PostgreSQL implementation of the Storage
// interface. It mirrors sqlite.go's structure one-for-one — same SQL
// shape, same business logic, same error mapping — with two
// dialect-specific differences handled at the boundary:
//
//   - Placeholders: sqlite uses '?', postgres uses '$N'. The pgQuery
//     helper rewrites '?' -> '$1', '$2', ... at call time so the
//     embedded SQL strings can stay readable and source-compatible
//     with the sqlite impl.
//   - Concat: sqlite's group_concat(x, char(10)) is rewritten to
//     string_agg(x, chr(10)) inline (only GetQueueJobs uses this).
//   - Unique-violation detection: postgres returns SQLSTATE 23505;
//     pgIsUniqueViolation matches it.
//
// We deliberately keep timestamps as TEXT in both backends (RFC3339
// UTC) so parseTime/formatTime work uniformly. No native TIMESTAMPTZ
// path here.

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mbreese/batchq/jobs"
	"github.com/mbreese/batchq/support"

	_ "github.com/jackc/pgx/v5/stdlib"
)

//go:embed schema_postgres.sql
var pgSchemaSQL string

// OpenPostgres returns a Storage backed by the postgres URL.
// The schema is applied (idempotently) on every open.
func OpenPostgres(ctx context.Context, url string) (Storage, error) {
	if url == "" {
		return nil, errors.New("storage: empty postgres url")
	}
	conn, err := sql.Open("pgx", url)
	if err != nil {
		return nil, fmt.Errorf("storage: open postgres: %w", err)
	}
	if err := conn.PingContext(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("storage: ping postgres: %w", err)
	}
	if _, err := conn.ExecContext(ctx, pgSchemaSQL); err != nil {
		conn.Close()
		return nil, fmt.Errorf("storage: apply postgres schema: %w", err)
	}
	return &pgStorage{db: conn, url: url}, nil
}

type pgStorage struct {
	db  *sql.DB
	url string

	mu     sync.Mutex
	closed bool
}

func (s *pgStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.db.Close()
}

// pgQuery rewrites '?' placeholders to '$1', '$2', ... so SQL strings
// can be kept in sqlite-style and reused here. Bytes outside string
// literals are not analyzed — we don't have any '?' inside strings in
// this codebase, and the trade-off is keeping the function trivial.
func pgQuery(q string) string {
	var b strings.Builder
	b.Grow(len(q) + 8)
	n := 0
	for i := 0; i < len(q); i++ {
		c := q[i]
		if c == '?' {
			n++
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(n))
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

// pgIsUniqueViolation matches the SQLSTATE 23505 ("unique_violation")
// returned by postgres when a UNIQUE / PRIMARY KEY constraint fires.
// The pgx driver surfaces it via the error's Error() string in the
// form "ERROR: duplicate key value violates unique constraint ...
// (SQLSTATE 23505)".
func pgIsUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "SQLSTATE 23505")
}

// --- Tenants -----------------------------------------------------------

func (s *pgStorage) CreateTenant(ctx context.Context, name string, kind TenantKind) (*Tenant, error) {
	if name == "" {
		return nil, errors.New("storage: empty tenant name")
	}
	if kind != TenantKindLocal && kind != TenantKindRemote {
		return nil, fmt.Errorf("storage: invalid tenant kind %q", kind)
	}
	id := support.NewUUID()
	created := nowString()
	_, err := s.db.ExecContext(ctx,
		pgQuery(`INSERT INTO tenants (id, name, kind, created_at) VALUES (?, ?, ?, ?)`),
		id, name, string(kind), created)
	if err != nil {
		if pgIsUniqueViolation(err) {
			return nil, ErrTenantExists
		}
		return nil, fmt.Errorf("storage: create tenant: %w", err)
	}
	createdAt, _ := parseTime(created)
	return &Tenant{ID: id, Name: name, Kind: kind, CreatedAt: createdAt}, nil
}

func (s *pgStorage) GetTenantByName(ctx context.Context, name string) (*Tenant, error) {
	row := s.db.QueryRowContext(ctx,
		pgQuery(`SELECT id, name, kind, created_at FROM tenants WHERE name = ?`), name)
	t, err := scanTenant(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrTenantNotFound
		}
		return nil, err
	}
	return t, nil
}

func (s *pgStorage) GetTenantByID(ctx context.Context, id string) (*Tenant, error) {
	row := s.db.QueryRowContext(ctx,
		pgQuery(`SELECT id, name, kind, created_at FROM tenants WHERE id = ?`), id)
	t, err := scanTenant(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrTenantNotFound
		}
		return nil, err
	}
	return t, nil
}

func (s *pgStorage) ListTenants(ctx context.Context) ([]*Tenant, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, name, kind, created_at FROM tenants ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Tenant
	for rows.Next() {
		t, err := scanTenant(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *pgStorage) EnsureLocalTenant(ctx context.Context, name string) (*Tenant, error) {
	if name == "" {
		return nil, errors.New("storage: empty tenant name")
	}
	if t, err := s.GetTenantByName(ctx, name); err == nil {
		return t, nil
	} else if !errors.Is(err, ErrTenantNotFound) {
		return nil, err
	}
	t, err := s.CreateTenant(ctx, name, TenantKindLocal)
	if err == nil {
		return t, nil
	}
	if !errors.Is(err, ErrTenantExists) {
		return nil, err
	}
	return s.GetTenantByName(ctx, name)
}

func (s *pgStorage) DeleteTenant(ctx context.Context, id string) error {
	var jobCount, tokenCount int
	if err := s.db.QueryRowContext(ctx,
		pgQuery(`SELECT COUNT(*) FROM jobs WHERE tenant_id = ?`), id).Scan(&jobCount); err != nil {
		return err
	}
	if err := s.db.QueryRowContext(ctx,
		pgQuery(`SELECT COUNT(*) FROM tokens WHERE tenant_id = ?`), id).Scan(&tokenCount); err != nil {
		return err
	}
	if jobCount > 0 || tokenCount > 0 {
		return fmt.Errorf("%w: tenant has %d jobs and %d tokens", ErrInvalidState, jobCount, tokenCount)
	}
	res, err := s.db.ExecContext(ctx, pgQuery(`DELETE FROM tenants WHERE id = ?`), id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrTenantNotFound
	}
	return nil
}

// --- Tokens ------------------------------------------------------------

func (s *pgStorage) CreateToken(ctx context.Context, tenantID string, hmacBytes []byte, label string, expiresAt time.Time) (*Token, error) {
	if tenantID == "" {
		return nil, errors.New("storage: empty tenantID")
	}
	if len(hmacBytes) == 0 {
		return nil, errors.New("storage: empty hmac")
	}
	id := support.NewUUID()
	created := nowString()
	var expiresStr any
	if !expiresAt.IsZero() {
		expiresStr = formatTime(expiresAt)
	}
	_, err := s.db.ExecContext(ctx,
		pgQuery(`INSERT INTO tokens (id, tenant_id, hmac, label, created_at, expires_at)
		         VALUES (?, ?, ?, ?, ?, ?)`),
		id, tenantID, hmacBytes, label, created, expiresStr)
	if err != nil {
		return nil, fmt.Errorf("storage: insert token: %w", err)
	}
	createdAt, _ := parseTime(created)
	return &Token{
		ID:        id,
		TenantID:  tenantID,
		HMAC:      hmacBytes,
		Label:     label,
		CreatedAt: createdAt,
		ExpiresAt: expiresAt,
	}, nil
}

func (s *pgStorage) GetTokenByHMAC(ctx context.Context, hmacBytes []byte) (*Token, *Tenant, error) {
	if len(hmacBytes) == 0 {
		return nil, nil, ErrTokenNotFound
	}
	row := s.db.QueryRowContext(ctx,
		pgQuery(`SELECT id, tenant_id, hmac, label, created_at, expires_at, revoked_at
		         FROM tokens WHERE hmac = ?`), hmacBytes)
	tok, err := scanToken(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, ErrTokenNotFound
		}
		return nil, nil, err
	}
	if !tok.Active(time.Now().UTC()) {
		return nil, nil, ErrTokenNotFound
	}
	tenant, err := s.GetTenantByID(ctx, tok.TenantID)
	if err != nil {
		return nil, nil, err
	}
	return tok, tenant, nil
}

func (s *pgStorage) ListTokensForTenant(ctx context.Context, tenantID string) ([]*Token, error) {
	rows, err := s.db.QueryContext(ctx,
		pgQuery(`SELECT id, tenant_id, hmac, label, created_at, expires_at, revoked_at
		         FROM tokens WHERE tenant_id = ? ORDER BY created_at`), tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Token
	for rows.Next() {
		t, err := scanToken(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *pgStorage) RevokeToken(ctx context.Context, tokenID string) error {
	res, err := s.db.ExecContext(ctx,
		pgQuery(`UPDATE tokens SET revoked_at = ?
		         WHERE id = ? AND revoked_at IS NULL`),
		nowString(), tokenID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrTokenNotFound
	}
	return nil
}

// --- Submission ---------------------------------------------------------

func (s *pgStorage) InsertJob(ctx context.Context, tenantID string, job *jobs.JobDef) error {
	if tenantID == "" {
		return errors.New("storage: empty tenantID")
	}
	if job == nil {
		return errors.New("storage: nil job")
	}
	if job.JobId == "" {
		return errors.New("storage: job missing id")
	}

	for _, depID := range job.AfterOk {
		dep, err := s.GetJob(ctx, tenantID, depID)
		if err != nil {
			return fmt.Errorf("storage: dep %s: %w", depID, err)
		}
		if dep.Status == jobs.CANCELED || dep.Status == jobs.FAILED {
			return fmt.Errorf("storage: dep %s is %s", depID, dep.Status)
		}
	}

	status := job.Status
	if status != jobs.USERHOLD {
		if len(job.AfterOk) == 0 {
			status = jobs.QUEUED
		} else {
			status = jobs.WAITING
		}
	}

	submitTime := nowString()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	name := job.Name
	if name == "" {
		name = "batchq-%JOBID"
	}
	if strings.Contains(name, "%JOBID") {
		name = strings.ReplaceAll(name, "%JOBID", job.JobId)
	}

	if _, err := tx.ExecContext(ctx,
		pgQuery(`INSERT INTO jobs (id, tenant_id, status, priority, name, notes, submit_time)
		         VALUES (?, ?, ?, ?, ?, ?, ?)`),
		job.JobId, tenantID, status, job.Priority, name, job.Notes, submitTime,
	); err != nil {
		return fmt.Errorf("storage: insert job: %w", err)
	}

	for _, depID := range job.AfterOk {
		if _, err := tx.ExecContext(ctx,
			pgQuery(`INSERT INTO job_deps (job_id, afterok_id) VALUES (?, ?)`),
			job.JobId, depID,
		); err != nil {
			return fmt.Errorf("storage: insert dep: %w", err)
		}
	}

	for _, d := range job.Details {
		value := d.Value
		if d.Key == "stdout" || d.Key == "stderr" {
			value = strings.ReplaceAll(value, "%JOBID", job.JobId)
		}
		if _, err := tx.ExecContext(ctx,
			pgQuery(`INSERT INTO job_details (job_id, key, value) VALUES (?, ?, ?)`),
			job.JobId, d.Key, value,
		); err != nil {
			return fmt.Errorf("storage: insert detail %s: %w", d.Key, err)
		}
	}

	for _, p := range dedupNonEmpty(job.InputFiles) {
		if _, err := tx.ExecContext(ctx,
			pgQuery(`INSERT INTO job_inputs (job_id, path) VALUES (?, ?)`),
			job.JobId, p,
		); err != nil {
			return fmt.Errorf("storage: insert input %s: %w", p, err)
		}
	}
	for _, p := range dedupNonEmpty(job.OutputFiles) {
		if _, err := tx.ExecContext(ctx,
			pgQuery(`INSERT INTO job_outputs (job_id, path) VALUES (?, ?)`),
			job.JobId, p,
		); err != nil {
			return fmt.Errorf("storage: insert output %s: %w", p, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	job.Status = status
	job.Name = name
	job.SubmitTime, _ = parseTime(submitTime)
	return nil
}

// --- Read access --------------------------------------------------------

func (s *pgStorage) GetJob(ctx context.Context, tenantID, jobID string) (*jobs.JobDef, error) {
	job, err := s.pgFetchJob(ctx, tenantID, jobID)
	if err != nil {
		return nil, err
	}
	if err := s.pgLoadJobRelations(ctx, job); err != nil {
		return nil, err
	}
	return job, nil
}

func (s *pgStorage) pgFetchJob(ctx context.Context, tenantID, jobID string) (*jobs.JobDef, error) {
	row := s.db.QueryRowContext(ctx,
		pgQuery(`SELECT id, status, priority, name, notes, submit_time, start_time, end_time, return_code
		         FROM jobs WHERE id = ? AND tenant_id = ?`), jobID, tenantID)
	job, err := scanJob(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrJobNotFound
		}
		return nil, err
	}
	return job, nil
}

func (s *pgStorage) pgLoadJobRelations(ctx context.Context, job *jobs.JobDef) error {
	deps, err := s.pgFetchDeps(ctx, job.JobId)
	if err != nil {
		return err
	}
	job.AfterOk = deps

	details, err := s.pgFetchDetails(ctx, job.JobId)
	if err != nil {
		return err
	}
	job.Details = details

	rd, err := s.pgFetchRunningDetails(ctx, job.JobId)
	if err != nil {
		return err
	}
	job.RunningDetails = rd

	in, err := s.pgFetchPaths(ctx, "job_inputs", job.JobId)
	if err != nil {
		return err
	}
	job.InputFiles = in

	out, err := s.pgFetchPaths(ctx, "job_outputs", job.JobId)
	if err != nil {
		return err
	}
	job.OutputFiles = out
	return nil
}

func (s *pgStorage) pgFetchPaths(ctx context.Context, table, jobID string) ([]string, error) {
	if table != "job_inputs" && table != "job_outputs" {
		return nil, fmt.Errorf("storage: pgFetchPaths invalid table %q", table)
	}
	rows, err := s.db.QueryContext(ctx,
		pgQuery("SELECT path FROM "+table+" WHERE job_id = ? ORDER BY path"), jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var paths []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		paths = append(paths, p)
	}
	return paths, rows.Err()
}

func (s *pgStorage) pgFetchDeps(ctx context.Context, jobID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		pgQuery(`SELECT afterok_id FROM job_deps WHERE job_id = ? ORDER BY afterok_id`), jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var deps []string
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		deps = append(deps, d)
	}
	return deps, rows.Err()
}

func (s *pgStorage) pgFetchDetails(ctx context.Context, jobID string) ([]jobs.JobDefDetail, error) {
	rows, err := s.db.QueryContext(ctx,
		pgQuery(`SELECT key, value FROM job_details WHERE job_id = ?`), jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var details []jobs.JobDefDetail
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		details = append(details, jobs.JobDefDetail{Key: k, Value: v})
	}
	return details, rows.Err()
}

func (s *pgStorage) pgFetchRunningDetails(ctx context.Context, jobID string) ([]jobs.JobRunningDetail, error) {
	rows, err := s.db.QueryContext(ctx,
		pgQuery(`SELECT key, value FROM job_running_details WHERE job_id = ?`), jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var details []jobs.JobRunningDetail
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		details = append(details, jobs.JobRunningDetail{Key: k, Value: v})
	}
	return details, rows.Err()
}

func (s *pgStorage) ListJobs(ctx context.Context, tenantID string, showAll, sortByStatus bool) ([]*jobs.JobDef, error) {
	var query string
	args := []any{tenantID}
	switch {
	case showAll && sortByStatus:
		query = `SELECT id, status, priority, name, notes, submit_time, start_time, end_time, return_code
		         FROM jobs WHERE tenant_id = ?
		         ORDER BY status DESC, priority DESC, end_time, start_time, id`
	case showAll && !sortByStatus:
		query = `SELECT id, status, priority, name, notes, submit_time, start_time, end_time, return_code
		         FROM jobs WHERE tenant_id = ? ORDER BY id`
	case !showAll && sortByStatus:
		query = `SELECT id, status, priority, name, notes, submit_time, start_time, end_time, return_code
		         FROM jobs WHERE tenant_id = ? AND status <= ?
		         ORDER BY status DESC, priority DESC, end_time, start_time, id`
		args = append(args, jobs.RUNNING)
	default:
		query = `SELECT id, status, priority, name, notes, submit_time, start_time, end_time, return_code
		         FROM jobs WHERE tenant_id = ? AND status <= ? ORDER BY id`
		args = append(args, jobs.RUNNING)
	}
	return s.pgQueryJobs(ctx, query, args, true)
}

func (s *pgStorage) ListJobsByStatus(ctx context.Context, tenantID string, statuses []jobs.StatusCode, sortByStatus bool) ([]*jobs.JobDef, error) {
	if len(statuses) == 0 {
		return nil, nil
	}
	placeholders := make([]string, len(statuses))
	args := make([]any, 0, len(statuses)+1)
	args = append(args, tenantID)
	for i, st := range statuses {
		placeholders[i] = "?"
		args = append(args, st)
	}
	query := `SELECT id, status, priority, name, notes, submit_time, start_time, end_time, return_code
	          FROM jobs WHERE tenant_id = ? AND status IN (` + strings.Join(placeholders, ",") + `)`
	if sortByStatus {
		query += ` ORDER BY status DESC, priority DESC, end_time, start_time, id`
	} else {
		query += ` ORDER BY id`
	}
	return s.pgQueryJobs(ctx, query, args, true)
}

func (s *pgStorage) SearchJobs(ctx context.Context, tenantID, query string, statuses []jobs.StatusCode) ([]*jobs.JobDef, error) {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return nil, nil
	}
	like := "%" + trimmed + "%"
	sqlQuery := `
		SELECT j.id, j.status, j.priority, j.name, j.notes, j.submit_time, j.start_time, j.end_time, j.return_code
		FROM jobs j
		WHERE j.tenant_id = ? AND (
			j.id LIKE ?
			OR j.name LIKE ?
			OR EXISTS (
				SELECT 1 FROM job_details d
				WHERE d.job_id = j.id AND d.key = 'script' AND d.value LIKE ?
			)
			OR EXISTS (
				SELECT 1 FROM job_details d
				WHERE d.job_id = j.id AND d.key = 'run_id' AND d.value LIKE ?
			)
			OR EXISTS (
				SELECT 1 FROM job_inputs i
				WHERE i.job_id = j.id AND i.path LIKE ?
			)
			OR EXISTS (
				SELECT 1 FROM job_outputs o
				WHERE o.job_id = j.id AND o.path LIKE ?
			)
		)`
	args := []any{tenantID, like, like, like, like, like, like}
	if len(statuses) > 0 {
		placeholders := make([]string, len(statuses))
		for i, st := range statuses {
			placeholders[i] = "?"
			args = append(args, st)
		}
		sqlQuery += ` AND j.status IN (` + strings.Join(placeholders, ",") + `)`
	}
	sqlQuery += ` ORDER BY j.id`
	return s.pgQueryJobs(ctx, sqlQuery, args, true)
}

func (s *pgStorage) pgQueryJobs(ctx context.Context, query string, args []any, loadRelations bool) ([]*jobs.JobDef, error) {
	rows, err := s.db.QueryContext(ctx, pgQuery(query), args...)
	if err != nil {
		return nil, err
	}
	var out []*jobs.JobDef
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, job)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if loadRelations {
		for _, j := range out {
			if err := s.pgLoadJobRelations(ctx, j); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

func (s *pgStorage) GetJobDependents(ctx context.Context, tenantID, jobID string) ([]string, error) {
	if _, err := s.pgFetchJob(ctx, tenantID, jobID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx,
		pgQuery(`SELECT d.job_id FROM job_deps d
		         JOIN jobs j ON j.id = d.job_id
		         WHERE d.afterok_id = ? AND j.tenant_id = ?
		         ORDER BY d.job_id`), jobID, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *pgStorage) GetJobStatusCounts(ctx context.Context, tenantID string, showAll bool) (map[jobs.StatusCode]int, error) {
	counts := map[jobs.StatusCode]int{
		jobs.USERHOLD:    0,
		jobs.WAITING:     0,
		jobs.QUEUED:      0,
		jobs.PROXYQUEUED: 0,
		jobs.RUNNING:     0,
		jobs.SUCCESS:     0,
		jobs.FAILED:      0,
		jobs.CANCELED:    0,
	}
	query := `SELECT status, COUNT(*) FROM jobs WHERE tenant_id = ?`
	args := []any{tenantID}
	if !showAll {
		query += ` AND status <= ?`
		args = append(args, jobs.RUNNING)
	}
	query += ` GROUP BY status`

	rows, err := s.db.QueryContext(ctx, pgQuery(query), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var st jobs.StatusCode
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		counts[st] = n
	}
	return counts, rows.Err()
}

func (s *pgStorage) GetQueueJobs(ctx context.Context, tenantID string, showAll, sortByStatus bool) ([]*jobs.JobDef, error) {
	// Postgres flavor of the queue-with-aggregates query. Uses
	// string_agg + chr(10) where sqlite uses group_concat + char(10).
	// The parser on the receiving side (parseConcatKV) is unchanged.
	query := `
		SELECT j.id, j.status, j.priority, j.name, j.notes, j.submit_time, j.start_time, j.end_time, j.return_code,
			deps.deps, details.details, running.running
		FROM jobs j
		LEFT JOIN (
			SELECT job_id, string_agg(afterok_id, chr(10)) AS deps
			FROM job_deps
			GROUP BY job_id
		) deps ON deps.job_id = j.id
		LEFT JOIN (
			SELECT job_id, string_agg(key || '=' || value, chr(10)) AS details
			FROM job_details
			WHERE key IN ('procs', 'mem', 'walltime', 'user', 'run_id')
			GROUP BY job_id
		) details ON details.job_id = j.id
		LEFT JOIN (
			SELECT job_id, string_agg(key || '=' || value, chr(10)) AS running
			FROM job_running_details
			WHERE key IN ('pid', 'slurm_status', 'slurm_job_id')
			GROUP BY job_id
		) running ON running.job_id = j.id
		WHERE j.tenant_id = ?
	`
	args := []any{tenantID}
	if !showAll {
		query += ` AND j.status IN (?, ?, ?, ?)`
		args = append(args, jobs.WAITING, jobs.QUEUED, jobs.PROXYQUEUED, jobs.RUNNING)
	}
	if sortByStatus {
		query += ` ORDER BY j.status DESC, j.priority DESC, j.end_time, j.start_time, j.id`
	} else {
		query += ` ORDER BY j.id`
	}

	rows, err := s.db.QueryContext(ctx, pgQuery(query), args...)
	if err != nil {
		return nil, err
	}
	var out []*jobs.JobDef
	for rows.Next() {
		var job jobs.JobDef
		var submitTime, startTime, endTime string
		var depRaw, detailRaw, runningRaw sql.NullString
		if err := rows.Scan(&job.JobId, &job.Status, &job.Priority, &job.Name, &job.Notes,
			&submitTime, &startTime, &endTime, &job.ReturnCode,
			&depRaw, &detailRaw, &runningRaw); err != nil {
			rows.Close()
			return nil, err
		}
		if job.SubmitTime, err = parseTime(submitTime); err != nil {
			rows.Close()
			return nil, err
		}
		if job.StartTime, err = parseTime(startTime); err != nil {
			rows.Close()
			return nil, err
		}
		if job.EndTime, err = parseTime(endTime); err != nil {
			rows.Close()
			return nil, err
		}
		if depRaw.Valid && depRaw.String != "" {
			job.AfterOk = strings.Split(depRaw.String, "\n")
		}
		if detailRaw.Valid && detailRaw.String != "" {
			job.Details = parseConcatKV(detailRaw.String, func(k, v string) jobs.JobDefDetail {
				return jobs.JobDefDetail{Key: k, Value: v}
			})
		}
		if runningRaw.Valid && runningRaw.String != "" {
			job.RunningDetails = parseConcatKV(runningRaw.String, func(k, v string) jobs.JobRunningDetail {
				return jobs.JobRunningDetail{Key: k, Value: v}
			})
		}
		out = append(out, &job)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *pgStorage) GetProxyJobs(ctx context.Context, tenantID string) ([]*jobs.JobDef, error) {
	return s.pgQueryJobs(ctx,
		`SELECT id, status, priority, name, notes, submit_time, start_time, end_time, return_code
		 FROM jobs WHERE tenant_id = ? AND status = ? ORDER BY id`,
		[]any{tenantID, jobs.PROXYQUEUED}, true)
}

// --- Dependency resolution ---------------------------------------------

func (s *pgStorage) ResolveDependencies(ctx context.Context, tenantID string) (ResolveResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ResolveResult{}, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx,
		pgQuery(`SELECT id FROM jobs WHERE tenant_id = ? AND (status = ? OR status = ?)`),
		tenantID, jobs.WAITING, jobs.UNKNOWN)
	if err != nil {
		return ResolveResult{}, err
	}
	var candidates []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return ResolveResult{}, err
		}
		candidates = append(candidates, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return ResolveResult{}, err
	}

	res := ResolveResult{}
	for _, jobID := range candidates {
		depRows, err := tx.QueryContext(ctx,
			pgQuery(`SELECT j.id, j.status FROM job_deps d
			         JOIN jobs j ON j.id = d.afterok_id
			         WHERE d.job_id = ? AND j.tenant_id = ?`), jobID, tenantID)
		if err != nil {
			return ResolveResult{}, err
		}
		canQueue := true
		shouldCancel := false
		var cancelReason string
		for depRows.Next() {
			var depID string
			var depStatus jobs.StatusCode
			if err := depRows.Scan(&depID, &depStatus); err != nil {
				depRows.Close()
				return ResolveResult{}, err
			}
			if depStatus != jobs.SUCCESS && depStatus != jobs.PROXYQUEUED {
				canQueue = false
			}
			if depStatus == jobs.CANCELED || depStatus == jobs.FAILED {
				shouldCancel = true
				if cancelReason == "" {
					cancelReason = fmt.Sprintf("Depends on %s", depID)
				} else {
					cancelReason += ", " + depID
				}
			}
		}
		depRows.Close()
		if err := depRows.Err(); err != nil {
			return ResolveResult{}, err
		}

		switch {
		case shouldCancel:
			if _, err := tx.ExecContext(ctx,
				pgQuery(`UPDATE jobs SET status = ?, notes = ?, end_time = ?
				         WHERE id = ? AND tenant_id = ? AND status IN (?, ?)`),
				jobs.CANCELED, cancelReason+" failed/canceled", nowString(),
				jobID, tenantID, jobs.WAITING, jobs.UNKNOWN); err != nil {
				return ResolveResult{}, err
			}
			res.Canceled++
		case canQueue:
			if _, err := tx.ExecContext(ctx,
				pgQuery(`UPDATE jobs SET status = ? WHERE id = ? AND tenant_id = ? AND status IN (?, ?)`),
				jobs.QUEUED, jobID, tenantID, jobs.WAITING, jobs.UNKNOWN); err != nil {
				return ResolveResult{}, err
			}
			res.Promoted++
		}
	}

	if err := tx.Commit(); err != nil {
		return ResolveResult{}, err
	}
	return res, nil
}

// --- Atomic claim ------------------------------------------------------

func (s *pgStorage) ClaimNextJob(ctx context.Context, tenantID, runnerID, kind string, limits Limits) (ClaimResult, error) {
	if tenantID == "" {
		return ClaimResult{}, errors.New("storage: empty tenantID")
	}
	if runnerID == "" {
		return ClaimResult{}, errors.New("storage: empty runnerID")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ClaimResult{}, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx,
		pgQuery(`SELECT id FROM jobs WHERE tenant_id = ? AND status = ? ORDER BY priority DESC, submit_time, id`),
		tenantID, jobs.QUEUED)
	if err != nil {
		return ClaimResult{}, err
	}
	var queued []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return ClaimResult{}, err
		}
		queued = append(queued, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return ClaimResult{}, err
	}

	more := false
	for _, jobID := range queued {
		fits, err := pgJobFitsLimits(ctx, tx, jobID, limits)
		if err != nil {
			return ClaimResult{}, err
		}
		if !fits {
			more = true
			continue
		}
		if _, err := tx.ExecContext(ctx,
			pgQuery(`INSERT INTO job_running (job_id, job_runner, kind) VALUES (?, ?, ?)`),
			jobID, runnerID, kind); err != nil {
			if pgIsUniqueViolation(err) {
				continue
			}
			return ClaimResult{}, err
		}
		if _, err := tx.ExecContext(ctx,
			pgQuery(`UPDATE jobs SET status = ?, start_time = ?
			         WHERE id = ? AND tenant_id = ? AND status = ?`),
			jobs.RUNNING, nowString(), jobID, tenantID, jobs.QUEUED); err != nil {
			return ClaimResult{}, err
		}
		job, err := s.pgTxGetJob(ctx, tx, tenantID, jobID)
		if err != nil {
			return ClaimResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return ClaimResult{}, err
		}
		return ClaimResult{Job: job, MoreEligible: more}, nil
	}

	if err := tx.Commit(); err != nil {
		return ClaimResult{}, err
	}
	return ClaimResult{MoreEligible: more}, nil
}

func pgJobFitsLimits(ctx context.Context, tx *sql.Tx, jobID string, limits Limits) (bool, error) {
	vals := map[string]int{}
	rows, err := tx.QueryContext(ctx,
		pgQuery(`SELECT key, value FROM job_details
		         WHERE job_id = ? AND key IN ('procs', 'mem', 'walltime')`), jobID)
	if err != nil {
		return false, err
	}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			rows.Close()
			return false, err
		}
		if n, err := strconv.Atoi(v); err == nil {
			vals[k] = n
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return false, err
	}
	if limits.MaxProcs > 0 && vals["procs"] > limits.MaxProcs {
		return false, nil
	}
	if limits.MaxMemoryMB > 0 && vals["mem"] > limits.MaxMemoryMB {
		return false, nil
	}
	if limits.MaxWalltimeSec > 0 && vals["walltime"] > limits.MaxWalltimeSec {
		return false, nil
	}
	return true, nil
}

func (s *pgStorage) pgTxGetJob(ctx context.Context, tx *sql.Tx, tenantID, jobID string) (*jobs.JobDef, error) {
	row := tx.QueryRowContext(ctx,
		pgQuery(`SELECT id, status, priority, name, notes, submit_time, start_time, end_time, return_code
		         FROM jobs WHERE id = ? AND tenant_id = ?`), jobID, tenantID)
	job, err := scanJob(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrJobNotFound
		}
		return nil, err
	}
	depRows, err := tx.QueryContext(ctx,
		pgQuery(`SELECT afterok_id FROM job_deps WHERE job_id = ? ORDER BY afterok_id`), jobID)
	if err != nil {
		return nil, err
	}
	for depRows.Next() {
		var d string
		if err := depRows.Scan(&d); err != nil {
			depRows.Close()
			return nil, err
		}
		job.AfterOk = append(job.AfterOk, d)
	}
	depRows.Close()

	dRows, err := tx.QueryContext(ctx,
		pgQuery(`SELECT key, value FROM job_details WHERE job_id = ?`), jobID)
	if err != nil {
		return nil, err
	}
	for dRows.Next() {
		var k, v string
		if err := dRows.Scan(&k, &v); err != nil {
			dRows.Close()
			return nil, err
		}
		job.Details = append(job.Details, jobs.JobDefDetail{Key: k, Value: v})
	}
	dRows.Close()

	rdRows, err := tx.QueryContext(ctx,
		pgQuery(`SELECT key, value FROM job_running_details WHERE job_id = ?`), jobID)
	if err != nil {
		return nil, err
	}
	for rdRows.Next() {
		var k, v string
		if err := rdRows.Scan(&k, &v); err != nil {
			rdRows.Close()
			return nil, err
		}
		job.RunningDetails = append(job.RunningDetails, jobs.JobRunningDetail{Key: k, Value: v})
	}
	rdRows.Close()

	return job, nil
}

// --- State transitions -------------------------------------------------

func (s *pgStorage) MarkJobProxied(ctx context.Context, tenantID, jobID, runnerID string, runningDetails map[string]string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := pgAssertJobInTenant(ctx, tx, tenantID, jobID); err != nil {
		return err
	}
	if err := pgAssertRunnerOwnsJob(ctx, tx, jobID, runnerID); err != nil {
		return err
	}

	res, err := tx.ExecContext(ctx,
		pgQuery(`UPDATE jobs SET status = ? WHERE id = ? AND tenant_id = ? AND status = ?`),
		jobs.PROXYQUEUED, jobID, tenantID, jobs.RUNNING)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrInvalidState
	}

	for k, v := range runningDetails {
		// Postgres flavor of "INSERT OR REPLACE": upsert via
		// ON CONFLICT DO UPDATE.
		if _, err := tx.ExecContext(ctx,
			pgQuery(`INSERT INTO job_running_details (job_id, key, value)
			         VALUES (?, ?, ?)
			         ON CONFLICT (job_id, key) DO UPDATE SET value = EXCLUDED.value`),
			jobID, k, v); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *pgStorage) UpdateRunningDetails(ctx context.Context, tenantID, jobID string, details map[string]string) error {
	if len(details) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := pgAssertJobInTenant(ctx, tx, tenantID, jobID); err != nil {
		return err
	}
	for k, v := range details {
		if _, err := tx.ExecContext(ctx,
			pgQuery(`INSERT INTO job_running_details (job_id, key, value)
			         VALUES (?, ?, ?)
			         ON CONFLICT (job_id, key) DO UPDATE SET value = EXCLUDED.value`),
			jobID, k, v); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *pgStorage) EndJob(ctx context.Context, tenantID, jobID, runnerID string, returnCode int, notes string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := pgAssertJobInTenant(ctx, tx, tenantID, jobID); err != nil {
		return err
	}
	if err := pgAssertRunnerOwnsJob(ctx, tx, jobID, runnerID); err != nil {
		return err
	}

	newStatus := jobs.SUCCESS
	if returnCode != 0 {
		newStatus = jobs.FAILED
	}
	res, err := tx.ExecContext(ctx,
		pgQuery(`UPDATE jobs SET status = ?, end_time = ?, return_code = ?,
		                         notes = COALESCE(NULLIF(?, ''), notes)
		         WHERE id = ? AND tenant_id = ? AND status = ?`),
		newStatus, nowString(), returnCode, notes, jobID, tenantID, jobs.RUNNING)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrInvalidState
	}

	if newStatus != jobs.SUCCESS {
		if err := pgCascadeCancel(ctx, tx, tenantID, jobID,
			fmt.Sprintf("Parent job %s failed", jobID)); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *pgStorage) EndProxiedJob(ctx context.Context, tenantID, jobID string, status jobs.StatusCode, startTime, endTime time.Time, returnCode int, notes string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := pgAssertJobInTenant(ctx, tx, tenantID, jobID); err != nil {
		return err
	}

	res, err := tx.ExecContext(ctx,
		pgQuery(`UPDATE jobs SET status = ?, start_time = ?, end_time = ?, return_code = ?,
		                         notes = COALESCE(NULLIF(?, ''), notes)
		         WHERE id = ? AND tenant_id = ? AND status = ?`),
		status, formatTime(startTime), formatTime(endTime), returnCode, notes,
		jobID, tenantID, jobs.PROXYQUEUED)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrInvalidState
	}

	if status != jobs.SUCCESS {
		if err := pgCascadeCancel(ctx, tx, tenantID, jobID,
			fmt.Sprintf("Parent job %s failed/canceled", jobID)); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *pgStorage) CancelJob(ctx context.Context, tenantID, jobID, reason string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := pgCancelOne(ctx, tx, tenantID, jobID, reason); err != nil {
		return err
	}
	if err := pgCascadeCancel(ctx, tx, tenantID, jobID, reason); err != nil {
		return err
	}
	return tx.Commit()
}

func pgCancelOne(ctx context.Context, tx *sql.Tx, tenantID, jobID, reason string) error {
	row := tx.QueryRowContext(ctx,
		pgQuery(`SELECT status FROM jobs WHERE id = ? AND tenant_id = ?`), jobID, tenantID)
	var status jobs.StatusCode
	if err := row.Scan(&status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrJobNotFound
		}
		return err
	}
	if status >= jobs.CANCELED {
		return nil
	}
	_, err := tx.ExecContext(ctx,
		pgQuery(`UPDATE jobs SET status = ?, end_time = ?, notes = ?
		         WHERE id = ? AND tenant_id = ? AND status < ?`),
		jobs.CANCELED, nowString(), reason, jobID, tenantID, jobs.CANCELED)
	return err
}

func pgCascadeCancel(ctx context.Context, tx *sql.Tx, tenantID, parentID, reason string) error {
	queue := []string{parentID}
	seen := map[string]bool{parentID: true}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		rows, err := tx.QueryContext(ctx,
			pgQuery(`SELECT d.job_id FROM job_deps d
			         JOIN jobs j ON j.id = d.job_id
			         WHERE d.afterok_id = ? AND j.tenant_id = ? AND j.status < ?`),
			cur, tenantID, jobs.CANCELED)
		if err != nil {
			return err
		}
		var children []string
		for rows.Next() {
			var c string
			if err := rows.Scan(&c); err != nil {
				rows.Close()
				return err
			}
			if !seen[c] {
				seen[c] = true
				children = append(children, c)
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, c := range children {
			if err := pgCancelOne(ctx, tx, tenantID, c, reason); err != nil {
				return err
			}
			queue = append(queue, c)
		}
	}
	return nil
}

func pgAssertJobInTenant(ctx context.Context, tx *sql.Tx, tenantID, jobID string) error {
	var exists int
	row := tx.QueryRowContext(ctx,
		pgQuery(`SELECT 1 FROM jobs WHERE id = ? AND tenant_id = ?`), jobID, tenantID)
	if err := row.Scan(&exists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrJobNotFound
		}
		return err
	}
	return nil
}

func pgAssertRunnerOwnsJob(ctx context.Context, tx *sql.Tx, jobID, runnerID string) error {
	row := tx.QueryRowContext(ctx,
		pgQuery(`SELECT job_runner FROM job_running WHERE job_id = ?`), jobID)
	var owner string
	if err := row.Scan(&owner); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrInvalidState
		}
		return err
	}
	if owner != runnerID {
		return ErrInvalidState
	}
	return nil
}

// --- User actions ------------------------------------------------------

func (s *pgStorage) HoldJob(ctx context.Context, tenantID, jobID string) error {
	res, err := s.db.ExecContext(ctx,
		pgQuery(`UPDATE jobs SET status = ? WHERE id = ? AND tenant_id = ? AND status IN (?, ?, ?)`),
		jobs.USERHOLD, jobID, tenantID, jobs.QUEUED, jobs.WAITING, jobs.USERHOLD)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrInvalidState
	}
	return nil
}

func (s *pgStorage) ReleaseJob(ctx context.Context, tenantID, jobID string) error {
	res, err := s.db.ExecContext(ctx,
		pgQuery(`UPDATE jobs SET status = ? WHERE id = ? AND tenant_id = ? AND status = ?`),
		jobs.WAITING, jobID, tenantID, jobs.USERHOLD)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrInvalidState
	}
	return nil
}

func (s *pgStorage) AdjustJobPriority(ctx context.Context, tenantID, jobID string, delta int) error {
	if delta == 0 {
		return nil
	}
	res, err := s.db.ExecContext(ctx,
		pgQuery(`UPDATE jobs SET priority = priority + ?
		         WHERE id = ? AND tenant_id = ? AND status IN (?, ?, ?)`),
		delta, jobID, tenantID, jobs.QUEUED, jobs.WAITING, jobs.USERHOLD)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrInvalidState
	}
	return nil
}

func (s *pgStorage) CleanupJob(ctx context.Context, tenantID, jobID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := pgAssertJobInTenant(ctx, tx, tenantID, jobID); err != nil {
		return err
	}
	stmts := []struct {
		sql      string
		withTenant bool
	}{
		{`DELETE FROM job_running_details WHERE job_id = ?`, false},
		{`DELETE FROM job_running         WHERE job_id = ?`, false},
		{`DELETE FROM job_deps            WHERE job_id = ?`, false},
		{`DELETE FROM job_details         WHERE job_id = ?`, false},
		{`DELETE FROM job_inputs          WHERE job_id = ?`, false},
		{`DELETE FROM job_outputs         WHERE job_id = ?`, false},
		{`DELETE FROM jobs                WHERE id     = ? AND tenant_id = ?`, true},
	}
	for _, s := range stmts {
		var execErr error
		if s.withTenant {
			_, execErr = tx.ExecContext(ctx, pgQuery(s.sql), jobID, tenantID)
		} else {
			_, execErr = tx.ExecContext(ctx, pgQuery(s.sql), jobID)
		}
		if execErr != nil {
			return execErr
		}
	}
	return tx.Commit()
}

// --- Reverse lookups for run_id / inputs / outputs --------------------

func (s *pgStorage) FindJobsByDetail(ctx context.Context, tenantID, key, value string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		pgQuery(`SELECT d.job_id FROM job_details d
		         JOIN jobs j ON j.id = d.job_id
		         WHERE j.tenant_id = ? AND d.key = ? AND d.value = ?`),
		tenantID, key, value)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectIDs(rows)
}

func (s *pgStorage) FindJobsByInputPath(ctx context.Context, tenantID, path string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		pgQuery(`SELECT i.job_id FROM job_inputs i
		         JOIN jobs j ON j.id = i.job_id
		         WHERE j.tenant_id = ? AND i.path = ?`), tenantID, path)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectIDs(rows)
}

func (s *pgStorage) FindJobsByOutputPath(ctx context.Context, tenantID, path string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		pgQuery(`SELECT o.job_id FROM job_outputs o
		         JOIN jobs j ON j.id = o.job_id
		         WHERE j.tenant_id = ? AND o.path = ?`), tenantID, path)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectIDs(rows)
}
