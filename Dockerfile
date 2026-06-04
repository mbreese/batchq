# batchq server image. Pure Go, static binary, no CGO. Postgres or
# SQLite backend selected via [server] db / BATCHQ_HOME config.
#
# Build:    docker build -t batchq:dev .
# Compose:  see deploy/docker-compose.yml for a turnkey
#           postgres + batchq stack.

FROM golang:1.25-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=docker
RUN CGO_ENABLED=0 GOOS=linux \
    go build -trimpath \
    -ldflags "-X github.com/mbreese/batchq/cmd.Version=${VERSION}" \
    -o /out/batchq main.go

FROM alpine:3.20

# Minimal runtime: ca-certs (for HTTPS-fronted remote clients) and
# tzdata (so RFC3339 UTC parsing isn't surprised by an empty zoneinfo).
RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -S batchq && adduser -S -G batchq -h /var/lib/batchq batchq

COPY --from=build /out/batchq /usr/local/bin/batchq

USER batchq
ENV BATCHQ_HOME=/var/lib/batchq
WORKDIR /var/lib/batchq
VOLUME ["/var/lib/batchq"]

EXPOSE 8080

# The default command runs the server. The actual listener is a unix
# socket (per batchq's invariant); a sidecar proxy or `docker run -p`
# with a TCP proxy fronts it for HTTPS exposure. See
# deploy/docker-compose.yml for a working example with Caddy.
ENTRYPOINT ["batchq"]
CMD ["server"]
