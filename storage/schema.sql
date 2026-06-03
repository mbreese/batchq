-- batchq v2 schema. Identical entity shape to v1 except:
--   * timestamps are stored as RFC3339 strings (UTC).
--   * supporting indexes added for queue scans and dependent lookups.
--   * multi-tenant: every job belongs to a tenant; the tenants and
--     tokens tables hold the auth identities.
-- Pragmas (foreign_keys, journal_mode, busy_timeout, synchronous) are set
-- from the driver-side DSN, not here, so that the same SQL can be re-applied
-- safely as a migration without overriding the runtime configuration.

-- A tenant is a logical queue owner. Local autospawn deployments have
-- a single implicit tenant (kind='local') created at first start;
-- remote multi-tenant deployments add operator-minted tenants
-- (kind='remote') that carry bearer tokens.
CREATE TABLE IF NOT EXISTS tenants (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    kind        TEXT NOT NULL DEFAULT 'remote',
    created_at  TEXT NOT NULL DEFAULT ''
);

-- A token is a bearer-credential issued for a tenant. The token bytes
-- themselves are never stored; only the HMAC-SHA256 of the token
-- under the server's master.key. Nullable expires_at means the
-- operator chose no expiry at mint time.
CREATE TABLE IF NOT EXISTS tokens (
    id          TEXT PRIMARY KEY,
    tenant_id   TEXT NOT NULL REFERENCES tenants(id),
    hmac        BLOB NOT NULL UNIQUE,
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

-- job_running stores the runner ownership claim. Its UNIQUE PRIMARY KEY on
-- job_id is the atomic-claim primitive: only one runner can INSERT for a
-- given job_id within a single transaction.
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

-- Per-job input and output files. Optional submit-time metadata used for
-- pipeline introspection ("which job produced X?" / "which jobs need X?").
-- Multi-valued, so they get dedicated tables rather than CSV-encoded
-- job_details rows.
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

-- Composite index for per-tenant queue scans. Every queue/list query
-- starts with tenant_id, so leading the index with it lets the same
-- index serve every sort variant.
CREATE INDEX IF NOT EXISTS jobs_tenant_status_priority_submit
    ON jobs(tenant_id, status, priority DESC, submit_time, id);

CREATE INDEX IF NOT EXISTS job_deps_afterok
    ON job_deps(afterok_id, job_id);

CREATE INDEX IF NOT EXISTS job_details_key
    ON job_details(key, job_id);

-- Value-covering index so `WHERE key='run_id' AND value=?` (the workflow
-- run filter) is an index lookup instead of a scan.
CREATE INDEX IF NOT EXISTS job_details_kv
    ON job_details(key, value, job_id);

CREATE INDEX IF NOT EXISTS job_inputs_path
    ON job_inputs(path);

CREATE INDEX IF NOT EXISTS job_outputs_path
    ON job_outputs(path);
