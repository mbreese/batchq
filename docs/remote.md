# Remote access

batchq's server never binds a TCP port. It only ever listens on a unix
domain socket. That is a deliberate choice — exposing the API to the
network is a reverse proxy's job, and decoupling the two lets the
proxy handle TLS, network ACLs, rate limiting, and access logging
without batchq having to reinvent any of it.

This page covers how to put a reverse proxy in front of batchq and how
to configure remote clients to use it.

## When you need this

- You want users on their own laptops to submit jobs into a batchq
  instance running on a cluster login node.
- You want a runner on a compute node to talk to a batchq server on a
  different host (one batchq server, many compute nodes).
- You want the web UI accessible from a browser somewhere other than
  the host the server runs on. (Though for that, an SSH port-forward
  is often easier — see [web](web.md).)

If everything that talks to batchq is on the same host, you do not
need this page. The unix socket already works.

## The pieces

```
┌─────────────────────────┐   HTTPS    ┌────────────────────────────┐
│  remote client          │ ─────────► │  reverse proxy (nginx,     │
│  --remote=https://…     │            │  Caddy, Traefik, …)        │
│  $BATCHQ_TOKEN          │            │  terminates TLS, forwards  │
└─────────────────────────┘            │  to unix socket            │
                                       └──────────────┬─────────────┘
                                                      │
                                                      │ unix socket
                                                      ▼
                                       ┌──────────────────────────┐
                                       │  batchq server           │
                                       │  $BATCHQ_HOME/batchq.sock│
                                       └──────────────────────────┘
```

The proxy speaks HTTPS to the world and HTTP-over-unix to batchq. The
batchq server's authentication model is unchanged: anyone who can write
to the socket is trusted, and filesystem permissions (mode `0600`,
owned by the user running the server) are the only ACL on the socket
itself. The proxy is responsible for *deciding* who gets to send
requests through to the socket.

## URL scheme

Remote clients talk to batchq with this URL form:

```
https://host[:port]/[subpath]
```

- **`https://` only.** Plain HTTP is not supported. There is no way to
  configure a remote client to use it — `support.ParseRemote` rejects
  any non-HTTPS scheme. Terminate TLS at your proxy.
- **Default port is 443.** Specify a port if you are running the proxy
  elsewhere.
- **The path is a mount-point prefix.** If your proxy serves batchq at
  `https://cluster.example.com/batchq`, set `remote = "https://cluster.example.com/batchq"`.
  The client appends `/api/v1/…` to every request, so the URL you
  configure is the prefix the API is mounted under, not the API URL
  itself.

## Configuring the client

Either in `$BATCHQ_HOME/config`:

```toml
[batchq]
remote = "https://cluster.example.com/batchq"
# token = ""              # prefer BATCHQ_TOKEN env (when auth lands)
```

Or per-invocation:

```sh
batchq submit --remote https://cluster.example.com/batchq ./script.sh
```

When `remote` is set, the client:

- Dials the HTTPS URL directly — no unix socket lookup, no autospawn.
- Refuses to start a local `batchq server` (the server itself exits
  immediately if `[batchq] remote` is configured).

## Running the server behind a proxy

On the server host, run batchq normally. The server will create
`$BATCHQ_HOME/batchq.sock` with mode `0600`, owned by the user running
the server. The reverse proxy needs read+write access to that socket.

Two common ways to grant the proxy access:

1. **Run the proxy as the same user.** Simplest. Works on dedicated
   hosts. If you are running batchq as a system user `batchq`, run the
   proxy as `batchq` too.
2. **Use a group.** Run the server with `umask` set so the socket gets
   group write (`0660`), `chgrp` the socket to the proxy's group, and
   add the proxy to that group.

batchq does not currently offer a knob to set the socket mode or group
— it ships `0600`. If you need a wider mode, the cleanest approach is
to put the socket on a directory the proxy user has access to and run
the proxy as the same user.

## nginx example

```nginx
upstream batchq_socket {
    server unix:/var/lib/batchq/batchq.sock;
}

server {
    listen 443 ssl http2;
    server_name cluster.example.com;

    ssl_certificate     /etc/letsencrypt/live/cluster.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/cluster.example.com/privkey.pem;

    # Mount batchq at /batchq — clients use --remote https://cluster.example.com/batchq
    location /batchq/ {
        proxy_pass http://batchq_socket/;
        proxy_set_header Host $host;
        proxy_http_version 1.1;
        proxy_buffering off;
    }
}
```

Notes:

- The trailing slashes on both `location /batchq/` and the
  `proxy_pass` URL make nginx strip the `/batchq` prefix before
  forwarding. batchq does not see (or care about) the mount-point
  prefix; the client adds `/api/v1/...` directly.
- `proxy_buffering off` is a good idea for any streaming endpoints
  (log tails, future server-sent events).

To mount at the root instead, use `location /` and `proxy_pass
http://batchq_socket;` (no trailing slash on `proxy_pass`).

## Caddy example

```caddy
cluster.example.com {
    handle_path /batchq/* {
        reverse_proxy unix//var/lib/batchq/batchq.sock
    }
}
```

`handle_path` strips the matched prefix before forwarding, same as the
nginx setup above.

## Authentication

batchq has full bearer-token authentication for remote clients. The
client side is unchanged: `--token`, `BATCHQ_TOKEN`, or `[batchq]
token` in config. The client sends `Authorization: Bearer <token>`
on every HTTPS request.

The server side validates each incoming token by recomputing its
HMAC-SHA256 against `$BATCHQ_HOME/master.key` and looking the result
up in the tokens table. The master key file is created with mode
`0600` on first start and the server refuses to use it if the
permissions are wider. Lost master key means every existing token
becomes invalid — the operator re-mints. There is no decryption
step; just an HMAC comparison.

batchq is multi-tenant: every token belongs to a tenant, and the
server scopes every request to that tenant. Two users with different
tokens see two completely separate queues, even if they hit the
same server. See [tenants](tenants.md) for the operator workflow
(`batchq tenant create`, `batchq token mint`, etc.).

### Tenant onboarding flow

1. **Operator creates a tenant on the server host:**
   ```sh
   batchq tenant create alice
   ```
2. **Operator mints a token for that tenant:**
   ```sh
   batchq token mint --tenant alice --label "alice's laptop" --expires-in 720h
   ```
   The CLI prints the token to stdout exactly once — the server only
   stores its HMAC, so it cannot reprint. Operator distributes
   out-of-band (password manager, encrypted DM).
3. **User configures the client:**
   ```sh
   export BATCHQ_TOKEN=batchq_pat_…
   batchq submit --remote https://batchq.example.com ./job.sh
   ```
4. **Revocation** is `batchq token revoke <token-id>`; the next
   request using that token returns 401.

The CLI commands run on the server host because they touch the
storage and master key directly. They are not exposed over the REST
API in v1.

### What if the proxy still needs to gate access?

Reverse-proxy-level gating is still a good idea — defense in depth.
The bearer-token check happens server-side after the proxy already
saw the request; an IP allow-list, a corporate IdP, or VPN-only
access in front of the proxy reduces the surface for credential
guessing and rate-limiting headaches. Use the same proxy
configuration you would have used before bearer tokens landed.

## Operational notes

- **The server still autospawns.** If you run a reverse proxy and you
  are not supervising `batchq server`, the first request through the
  proxy will fail with a 502 because there is no server listening on
  the socket. For a remote-access deployment you almost always want a
  long-lived server: either start one with `batchq server` under a
  supervisor, or have something on the host trigger an autospawn
  periodically.
- **`--no-autospawn` for clients.** Remote clients never autospawn —
  the autospawn path only fires when the configured backend is the
  local unix socket. You do not need to pass `--no-autospawn` for
  remote clients; it is implicit.
- **The runner can be remote too.** `batchq run --slurm --remote
  https://…` is a perfectly fine setup. The runner connects to the
  remote batchq server for queue management and to the *local* SLURM
  via `sbatch`. You end up with a topology where the batchq server,
  the SLURM runner, and the SLURM head node can all be on different
  hosts.

## Where to go next

- [Architecture](architecture.md) — the why behind unix-socket-only
  and the autospawn model.
- [SLURM](slurm.md) — the SLURM runner often pairs with a remote
  batchq server on the cluster login node.
- [Web UI](web.md) — for browser access, SSH port-forwarding is often
  easier than a reverse proxy.
