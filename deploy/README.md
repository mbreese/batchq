# batchq deployment recipes

Container images and orchestration configs for running batchq with
the Postgres backend. The Postgres backend is the right choice for
multi-tenant remote deployments where many users hit the same
server; for single-user / single-host setups, the SQLite backend is
simpler and you don't need any of this.

## Docker Compose

Turnkey local stack: Postgres + batchq server. Two layers; bring up
the base layer for a quick try, add the HTTPS overlay when you want
to terminate TLS at a reverse proxy.

### Base (Postgres + batchq, no HTTPS)

```sh
cd deploy
docker compose up --build
```

This builds the batchq image from the repository root (the Dockerfile
is at `../Dockerfile`), starts a Postgres container with a persistent
volume, and starts a batchq server pointed at it.

Operator commands run inside the batchq container so they share its
`$BATCHQ_HOME` (master.key, config, socket):

```sh
docker compose exec batchq batchq tenant create alice
docker compose exec batchq batchq token mint --tenant alice --label "test"
```

The mint command prints the plaintext token on stdout; copy it
somewhere safe. The CLI also works from the host if you ship a copy
of the binary and point `--remote` at whatever proxy you set up
below.

### Adding HTTPS (Caddy reverse-proxy overlay)

```sh
docker compose -f docker-compose.yml -f docker-compose.https.yml up
```

This adds a Caddy container that mounts batchq's `$BATCHQ_HOME`
volume read-only, reads the unix socket directly, and serves HTTPS
on `https://localhost`. Replace `localhost` in `Caddyfile` with your
real domain and Caddy will fetch a Let's Encrypt cert automatically
(needs ports 80 and 443 reachable from the public internet).

### Files

| File                          | Purpose                                              |
|-------------------------------|------------------------------------------------------|
| `../Dockerfile`               | Multi-stage build for a static batchq binary         |
| `docker-compose.yml`          | Postgres + batchq server                             |
| `docker-compose.https.yml`    | Caddy reverse-proxy overlay (HTTPS in front)         |
| `batchq.toml`                 | batchq config consumed by the compose stack          |
| `Caddyfile`                   | Caddy config the overlay mounts                      |

## Kubernetes

A single self-contained manifest:

```sh
kubectl apply -f deploy/k8s/batchq.yaml
```

This creates a `batchq` namespace, a Postgres StatefulSet (with a
PVC), and a batchq Deployment (replicas: 1, strategy: Recreate — the
single-instance invariant matters). The batchq Pod has a Caddy
sidecar that fronts the unix socket with HTTPS, and a LoadBalancer
Service exposes it.

**Before you apply:**

- Replace the placeholder Postgres password in the
  `batchq-postgres` Secret.
- Replace `localhost` in the `batchq-caddy` ConfigMap with your real
  domain so Caddy can fetch a real cert.
- Pick an image tag in the batchq Deployment (push a build from
  this repo's Dockerfile to a registry first; the manifest assumes
  `ghcr.io/mbreese/batchq:latest` by default).

**Production swaps** worth considering:

- Drop the Postgres StatefulSet in favor of a managed database
  (RDS, Cloud SQL, Neon, etc.); point `BATCHQ_DB` at the managed
  endpoint via a Secret.
- Swap LoadBalancer for ClusterIP + an Ingress (cleaner cert/route
  management).
- Use sealed-secrets or external-secrets for the Postgres + master
  key material instead of a plaintext Secret.

## A note on the single-instance invariant

batchq's single-instance election is a process-level guarantee
(socket-based; see `server/server.go:listenUnix`). When the database
is sqlite-on-NFS this matters operationally because two processes
touching the file is unsafe.

For Postgres-backed deployments, the database is safe under
concurrent access — that's Postgres's job. So in principle multiple
batchq replicas pointed at the same Postgres could coexist. Today
the rest of the server (the idle monitor, the ownership monitor)
still assumes a single instance, so the `replicas: 1` /
`strategy: Recreate` posture is the safe default. Multi-replica
deployments are not a v1 feature.
