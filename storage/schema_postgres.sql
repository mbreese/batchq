-- Postgres flavor of the batchq schema. Mirrors storage/schema.sql
-- one-for-one except for:
--   * BYTEA where sqlite uses BLOB (tokens.hmac)
-- Timestamps stay as TEXT (RFC3339 UTC) so the same parseTime
-- helper serves both backends without dialect-specific scanning.
-- CREATE TABLE IF NOT EXISTS lets the schema be applied
-- idempotently on every Open, same as sqlite.

CREATE TABLE IF NOT EXISTS tenants (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    kind        TEXT NOT NULL DEFAULT 'remote',
    created_at  TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS tokens (
    id          TEXT PRIMARY KEY,
    tenant_id   TEXT NOT NULL REFERENCES tenants(id),
    hmac        BYTEA NOT NULL UNIQUE,
    label       TEXT NOT NULL DEFAULT '',
    created_at  TEXT NOT NULL DEFAULT '',
    expires_at  TEXT,
    revoked_at  TEXT
);

CREATE INDEX IF NOT EXISTS tokens_tenant_id
    ON tokens(tenant_id);

CREATE TABLE IF NOT EXISTS jobs (
    id          TEXT PRIMARY KEY,
    tenant_id   TEXT NOT NULL REFERENCES tenants(id),
    status      INTEGER NOT NULL DEFAULT 0,
    priority    INTEGER NOT NULL DEFAULT 0,
    name        TEXT NOT NULL DEFAULT '',
    notes       TEXT NOT NULL DEFAULT '',
    submit_time TEXT NOT NULL DEFAULT '',
    start_time  TEXT NOT NULL DEFAULT '',
    end_time    TEXT NOT NULL DEFAULT '',
    return_code INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS jobs_tenant_id
    ON jobs(tenant_id);

CREATE TABLE IF NOT EXISTS job_details (
    job_id TEXT NOT NULL REFERENCES jobs(id),
    key    TEXT NOT NULL,
    value  TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (job_id, key)
);

CREATE TABLE IF NOT EXISTS job_deps (
    job_id     TEXT NOT NULL REFERENCES jobs(id),
    afterok_id TEXT NOT NULL REFERENCES jobs(id),
    PRIMARY KEY (job_id, afterok_id)
);

CREATE TABLE IF NOT EXISTS job_running (
    job_id     TEXT NOT NULL PRIMARY KEY REFERENCES jobs(id),
    job_runner TEXT NOT NULL DEFAULT '',
    kind       TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS job_running_details (
    job_id TEXT NOT NULL REFERENCES jobs(id),
    key    TEXT NOT NULL,
    value  TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (job_id, key)
);

CREATE TABLE IF NOT EXISTS job_inputs (
    job_id TEXT NOT NULL REFERENCES jobs(id),
    path   TEXT NOT NULL,
    PRIMARY KEY (job_id, path)
);

CREATE TABLE IF NOT EXISTS job_outputs (
    job_id TEXT NOT NULL REFERENCES jobs(id),
    path   TEXT NOT NULL,
    PRIMARY KEY (job_id, path)
);

CREATE INDEX IF NOT EXISTS jobs_tenant_status_priority_submit
    ON jobs(tenant_id, status, priority DESC, submit_time, id);

CREATE INDEX IF NOT EXISTS job_deps_afterok
    ON job_deps(afterok_id, job_id);

CREATE INDEX IF NOT EXISTS job_details_key
    ON job_details(key, job_id);

CREATE INDEX IF NOT EXISTS job_details_kv
    ON job_details(key, value, job_id);

CREATE INDEX IF NOT EXISTS job_inputs_path
    ON job_inputs(path);

CREATE INDEX IF NOT EXISTS job_outputs_path
    ON job_outputs(path);
