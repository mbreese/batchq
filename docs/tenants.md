# Tenants and tokens

batchq is multi-tenant under the hood: every job belongs to a tenant,
and the server scopes every request to that tenant. Two users with
different identities see two separate queues, even when they hit the
same server.

For a single user on a single machine (`batchq submit ./script.sh`
against the local autospawn), all of this is invisible. You never
create a tenant or mint a token — the server provisions an implicit
local tenant for your unix uid on first contact, peer-credential
authentication identifies you on subsequent requests, and the queue
behaves exactly like single-user batchq always did.

This page is for the other case: a remote server with multiple users,
each reaching it via HTTPS and a bearer token. Everything below is
operator-only and runs on the server host.

## What is a tenant?

A tenant is a logical queue owner. Conceptually it's just a name
(`alice`, `bioinformatics-group`, `mcp-llm-bot`) and an identity that
authenticated requests resolve to. Two kinds exist:

- **Local tenants** are created automatically when the auth
  middleware first sees a unix-socket peer's uid (`local-uid-1234`
  shape). They aren't manageable through the CLI; they appear and
  disappear with their underlying Unix user. The implicit `_local`
  tenant the server provisions at startup is the fallback bucket for
  in-process test transports and the catch-all when no peer creds
  and no bearer token are present.
- **Remote tenants** are created explicitly by an operator with
  `batchq tenant create`, identified at request time by an
  HMAC-validated bearer token, and managed through the rest of the
  CLI surface below.

## Master key

The server signs and verifies bearer tokens with an HMAC secret
stored at `$BATCHQ_HOME/master.key`. The file is 32 random bytes,
mode `0600`, owned by the user the server runs as. It's created on
first server start if absent; an existing file with looser perms is
rejected (the server refuses to start) on the theory that a leaked
key on a shared host would let anyone with read access forge tokens.

Rotation is not implemented in v1: regenerating the key invalidates
every existing token, and the operator re-mints. This is acceptable
because the operator is the only one with access to it and re-mints
are cheap.

**Do not back up or copy `master.key` off the server.** Anyone with
the file can forge tokens for any tenant.

## CLI surface

All commands run as the user the server runs as (or any user with
read+write access to `$BATCHQ_HOME`). They open the storage and the
master key directly; they do not go through the REST API.

```sh
# Tenants
batchq tenant create <name>
batchq tenant list
batchq tenant delete <name-or-id>

# Tokens
batchq token mint   --tenant <name-or-id> [--label <text>] [--expires-in <duration>]
batchq token list   --tenant <name-or-id>
batchq token revoke <token-id>
```

### `batchq tenant create <name>`

Creates a `remote` (bearer-token) tenant. The name must be unique
across the server; convention is lowercase with hyphens, matching
the user's identity in your wider environment (matches their email
local-part, their LDAP uid, etc.).

```sh
batchq tenant create alice
# Tenant created.
#   id:   3593252d-baae-44f6-b9a6-27139cf5ca97
#   name: alice
#   kind: remote
```

### `batchq tenant list`

Prints every tenant, including the implicit local ones.

### `batchq tenant delete <name-or-id>`

Refuses to drop a tenant that still has jobs or tokens. Operator
must clean those up first (revoke tokens, cleanup completed jobs);
this is intentionally a hard refusal rather than a silent cascade
so accidents are loud.

### `batchq token mint --tenant <X> [--label <Y>] [--expires-in <duration>]`

Generates a new bearer token for the named tenant and prints it
**once** on stdout. The server only stores the HMAC, not the token
itself, so there's no way to retrieve the plaintext later — if the
user loses it, mint a new one.

```sh
batchq token mint --tenant alice --label "alice's laptop" --expires-in 720h
# Token minted for tenant alice (3593252d-…).
#   id:    0fb3afbe-…
#   label: alice's laptop
#   expires: 2026-07-03 23:42:26 UTC
#
# The token is printed below ONCE; the server only stores its HMAC.
# Distribute it out-of-band (password manager, encrypted DM) — never commit.
#
# batchq_pat_Rye1WNluLxze6kuMvl9u3jK3lS0d8uMIwLoEosvM45s
```

Stdout (the token) is separated from stderr (the metadata) so it can
be safely piped into a password manager:

```sh
batchq token mint --tenant alice --label ci 2>/dev/null | op item create ...
```

**`--expires-in` is optional.** Default is no expiry, which is the
right call for long-running runners that would otherwise silently
break when their token rolled. Set a duration for short-lived /
high-risk use cases (CI tokens, LLM/MCP integrations, ad-hoc
demos). Format is anything Go's `time.ParseDuration` accepts:
`720h` (30 days), `8760h` (1 year), `30m`, etc.

**`--label`** is free text shown in `token list` so the operator can
remember which token went where. Recommend something like the
device, machine, or integration name.

### `batchq token list --tenant <X>`

Prints every token issued for the tenant, including revoked and
expired ones (for audit). Plaintext tokens are not shown — only
IDs, labels, and lifecycle state.

```
ID                                    LABEL         CREATED     EXPIRES     STATE
0fb3afbe-98e2-4b41-aafd-12dbdadc3f74  alice laptop  2026-06-03  2026-07-03  active
1afd4023-…                            ci-runner     2026-04-01  never       active
b22e9c14-…                            old-laptop    2025-12-15  never       revoked
```

### `batchq token revoke <token-id>`

Marks the token as revoked. The next request that presents it
returns 401. Repeated revocations are no-ops-as-errors so scripted
cleanup doesn't have to track state.

## Tenant + token lifecycle from the user's side

Once the operator has minted a token and sent it to the user, the
user configures their client one of three ways:

```sh
# Per-invocation (visible in `ps`):
batchq submit --remote https://batchq.example.com --token batchq_pat_… ./job.sh

# Environment (recommended for runners and scripts):
export BATCHQ_REMOTE=https://batchq.example.com
export BATCHQ_TOKEN=batchq_pat_…
batchq submit ./job.sh

# Config file (~/.batchq/config):
[batchq]
remote = "https://batchq.example.com"
token  = "batchq_pat_…"
```

The env-var form is the recommended slot for the secret — it stays
out of `ps` listings and out of any checked-in config.

A user with a token gets their own tenant's queue and nothing else.
They cannot see, hold, or cancel jobs in other tenants. They cannot
list tenants other than their own (in fact, the list endpoint
doesn't exist over the REST API — only the operator CLI).

## Cluster-runner setup

The SLURM runner is just another bearer-token client. On the cluster
login node, the user (alice) configures their environment with their
token, then runs:

```sh
export BATCHQ_REMOTE=https://batchq.example.com
export BATCHQ_TOKEN=batchq_pat_…
batchq run --slurm --max-jobs 50 --slurm-max-jobs 475
```

The runner pulls jobs from alice's queue on the remote server,
hands them to local SLURM via `sbatch`, and reports terminal status
back to the remote server. SLURM dispatches the jobs as alice on
the cluster — the SLURM identity is whoever ran `sbatch`, which is
whoever ran the runner. The remote batchq server doesn't need to
know or care about alice's cluster uid.

For the cron + lockfile pattern (recommended on shared clusters),
see [SLURM § running from cron](slurm.md#from-cron-recommended-on-shared-clusters).

## Operational notes

- **`master.key` co-location.** The default path is sibling to the
  server's listen socket. On a typical deployment, that's
  `$BATCHQ_HOME/master.key`. Set `Options.MasterKeyPath` (or use the
  default) to keep all server-side secrets in one place.
- **Concurrent operator commands and a running server are safe** on
  local-disk SQLite — sqlite's file locks coordinate. On a networked
  filesystem they are not safe (same caveat as everywhere else in
  batchq); stop the server before running operator commands.
- **Tokens are opaque random strings.** They carry no identity in
  themselves; the server looks them up by HMAC and resolves to a
  tenant. You can't derive a tenant name from a token by inspection.
- **Token rotation** isn't built in. The shape is "mint a new one,
  switch the client over, revoke the old one." A future branch can
  add `batchq token rotate` if it turns out to matter.
- **Web UI for token self-service** is not in this release. Today
  every tenant + token is created by an operator on the server host.
  A web UI with user auth/login (where end users mint and revoke
  their own tokens) is on the roadmap.

## Threat model

Bearer tokens give you per-tenant isolation under these assumptions:

- The HTTPS endpoint is real HTTPS (TLS terminated at the reverse
  proxy; clients reject self-signed certs unless explicitly
  configured).
- `master.key` on the server is readable only by the server user
  (the server enforces this on startup).
- Tokens are never committed to repos, never logged, never sent
  over insecure channels.

batchq does NOT defend against:

- A compromised server host (anyone with `master.key` can forge
  tokens for any tenant).
- A leaked token (anyone with it is that tenant, until the operator
  revokes it).
- Side channels in the reverse proxy (a logging proxy that captures
  Authorization headers will capture every token).

These are the same trust assumptions every personal-API-key system
makes; nothing batchq-specific.
