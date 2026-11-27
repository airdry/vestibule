# Sample applications for sentry-ratelimiter

Three intentionally small applications used to exercise `sentry-ratelimiter`
with different Sentry SDKs, languages, and HTTP frameworks, plus the rate
limiter itself. Docker Compose starts four containers on one internal network:

| Service              | Purpose                                                         |
| -------------------- | --------------------------------------------------------------- |
| `sentry-ratelimiter` | Production image built from the repository-root `Dockerfile`    |
| `fastapi`            | Python 3.13 + FastAPI + `sentry-sdk[fastapi]` 2.23.1            |
| `fastify`            | Node.js 22 + Fastify 5 + `@sentry/node` 9.6.0                    |
| `fiber`              | Go 1.24 + Fiber 2 + `sentry-go` v0.31.0                          |

The SDK versions are pinned to the releases that were current when
Sentry self-hosted **25.3.0** shipped (2025-03-17). The framework versions are
pinned to the same point in time as well.

Every sample application:

- exposes `GET /events` (error events) and `GET /transactions` (transactions);
- accepts `count` (1..1000) and `environment` query parameters;
- emits one JSON object per log line at `trace`/`debug` level, including
  Sentry SDK debug output and the event/transaction payload in the
  `before_send` hooks;
- flushes the SDK before answering, so the HTTP response tells you whether the
  delivery attempt completed.

## Run everything with Docker Compose

No Python, Node.js, Go, Rust, or Docker build dependencies are required on the
host. Compose builds the rate limiter from the root `Dockerfile` and each sample
application from its own `Dockerfile`.

```bash
cd sample-apps
docker compose up -d --build
docker compose ps          # four containers
```

From the repository root the same flow is available through `mise`:

```bash
mise run sample-apps        # docker compose up -d --build, four containers
mise run sample-apps-logs   # follow all container logs
mise run sample-apps-down   # stop the stack
```

Published ports by default:

- `sentry-ratelimiter` Data Plane: <http://localhost:8080>
- `sentry-ratelimiter` Admin Plane: <http://localhost:8081>
- FastAPI: <http://localhost:8001>
- Fastify: <http://localhost:8002>
- Fiber:   <http://localhost:8003>

Generate traffic:

```bash
curl 'http://localhost:8001/events?count=5&environment=production'
curl 'http://localhost:8002/transactions?count=5&environment=production'
curl 'http://localhost:8003/events?count=5'
curl 'http://localhost:8003/transactions?count=5'
```

Scale the number of generated items with `count`. The endpoints return the
captured event IDs / trace IDs and the result of the final SDK flush.

Inspect the rate limiter:

```bash
curl http://localhost:8081/healthz
curl http://localhost:8081/readyz
curl http://localhost:8081/status
curl http://localhost:8081/metrics
```

## Addresses inside the Compose network

The sample apps use the rate limiter's Compose service name, not a host-side
address:

```text
http://public@sentry-ratelimiter:8080/1
```

The project number (`1` in the DSN path) becomes the `project_id` used by the
rate limiter's rules. Override the DSN only if you want to send the SDK traffic
somewhere else:

```bash
SENTRY_DSN='http://public@another-host:8080/1' docker compose up
```

You can also override:

- `SENTRY_ENVIRONMENT` - default Sentry environment (default `sample-apps`);
- `SENTRY_RELEASE` - default release name;
- `LOG_LEVEL` - `trace`, `debug`, `info`, `warn`, or `error` (default `debug`);
- `FASTAPI_PORT`, `FASTIFY_PORT`, `FIBER_PORT` - host-side app ports;
- `RATELIMITER_DATA_PORT`, `RATELIMITER_ADMIN_PORT` - host-side rate limiter
  ports.

Per-request `environment` query parameters override the SDK environment in the
payload, which lets you exercise environment-scoped rate limit rules without
restarting a container.

## sentry-ratelimiter configuration

The Compose stack mounts [sentry-ratelimiter.yaml](./sentry-ratelimiter.yaml)
at `/etc/sentry-ratelimiter/config.yaml`. The file demonstrates every supported
section:

- `server` - Data Plane/Admin Plane listeners and shutdown timeout;
- `upstream` - Sentry/Relay target plus active health-check settings;
- `ingest` - accepted status code and body size limits;
- `queue` - segmented disk WAL settings;
- `batch` - asynchronous delivery batching and throttling;
- `rate_limits` - global and project/environment rules;
- `observability` - log level and metric label allowlist.

The Ingestion Queue is backed by the named volume
`sentry-ratelimiter-queue` mounted at `/var/lib/sentry-ratelimiter/queue`, so
buffered telemetry survives container recreation.

The rate limiter container has no Compose-level `healthcheck`: the distroless
image intentionally contains no shell or curl/wget. Use the Admin Plane
endpoints from the host, or the Kubernetes probes from ticket 09, to check it.

The sample stack does **not** run Sentry itself. The default upstream is:

```text
http://upstream:9000
```

Point it at your self-hosted Sentry/Relay on the same Compose network, or run a
capture server with the network alias `upstream`. While the upstream is
unavailable, `sentry-ratelimiter` still accepts SDK traffic and stores it in the
disk queue; `GET /status` shows the pending items and
`GET /metrics` exposes the queue and upstream-health gauges.

## Reading the logs

All containers write JSON logs to stdout. Follow one service at a time:

```bash
docker compose logs -f sentry-ratelimiter
docker compose logs -f fastapi
docker compose logs -f fastify
docker compose logs -f fiber
```

Sample-app messages:

- `http.request.started` / `http.request.finished` - request lifecycle;
- `sample.events.requested` / `sample.transactions.requested` - endpoint input;
- `sentry.capture.event` / `sentry.capture.transaction` - item handed to the SDK;
- `sentry.payload` - event/transaction handed to `before_send` (large internal
  fields such as the installed-packages map are omitted);
- `sentry.sdk` - Sentry SDK debug output (transport, rate-limit headers,
  retries, failures);
- `sentry.flush` - result of the final SDK flush before the response.

Set `LOG_LEVEL=trace` for the extra per-item detail emitted at trace level.

## Layout

```text
sample-apps/
├── compose.yaml
├── sentry-ratelimiter.yaml  # mounted into the rate limiter container
├── fastapi/                 # Python 3.13 + FastAPI + sentry-sdk 2.23.1
├── fastify/                 # Node.js 22 + Fastify 5 + @sentry/node 9.6.0
└── fiber/                   # Go 1.24 + Fiber 2 + sentry-go v0.31.0
```

The rate limiter image is built from the repository-root `Dockerfile` and
`gcr.io/distroless/static-debian13:nonroot`. Each sample app has its own
multi-stage `Dockerfile`, runs as a non-root user, and includes a `/health`
endpoint used by its container health check.

The FastAPI app is served with `uvicorn` directly instead of the `fastapi` CLI:
the CLI does not expose uvicorn's `log_config`, which is needed to keep uvicorn
and Sentry SDK output on the same JSON formatter.

## Container images

Every sample app uses a distroless runtime image, so the shipped image contains
only the language runtime, glibc, CA certificates, and the application itself:
no shell, package manager, `curl`, or `wget`.

| Service   | Runtime image                                  | Build stage                    |
| --------- | ---------------------------------------------- | ------------------------------ |
| `fastapi` | `gcr.io/distroless/python3-debian13:nonroot`   | `python:3.13-slim-trixie`       |
| `fastify` | `gcr.io/distroless/nodejs22-debian13:nonroot`  | `node:22.23.5-bookworm-slim`    |
| `fiber`   | `gcr.io/distroless/static-debian13:nonroot`    | `golang:1.24.1-bookworm`        |

Because there is no shell, the `HEALTHCHECK` instructions invoke the runtime
that is already in the image instead of `curl`/`wget`:

- `fastapi` runs `/usr/bin/python3 -c ...` against `http://127.0.0.1:8000/health`;
- `fastify` runs `/nodejs/bin/node -e ...` against `http://127.0.0.1:3000/health`;
- `fiber` runs its own binary with `-healthcheck`, which probes
  `http://127.0.0.1:8080/health`.

All images run as the distroless `nonroot` user (UID/GID `65532`) and work with
the Compose hardening (`read_only`, `cap_drop: ALL`, `no-new-privileges`).

Supported platforms follow the upstream distroless images:

- `static-debian13` and `base-debian13`: `amd64`, `arm64`, `arm`, `s390x`,
  `ppc64le`, `riscv64`;
- `nodejs22-debian13`: `amd64`, `arm64`, `arm`, `s390x`, `ppc64le`;
- `python3-debian13`: `amd64`, `arm64`, `riscv64`.

### Debugging a distroless container

There is no shell to `docker compose exec` into. Rebuild the affected service
with the `debug` variant of the same image (it adds BusyBox) when you need to
inspect the filesystem:

```bash
docker build --build-arg RUNTIME_IMAGE=gcr.io/distroless/python3-debian13:debug-nonroot \
  -t fastapi-debug ./fastapi
```

### Vulnerability scanning

The distroless base images carry no fixable OS packages, so Trivy reports only
the pinned application dependencies (the SDK and framework versions are
intentionally frozen to the Sentry 25.3.0 era):

```bash
cd sample-apps
docker compose build
trivy image --scanners vuln --ignore-unfixed \
  sentry-ratelimiter-sample-apps-fastapi:latest
trivy image --scanners vuln --ignore-unfixed \
  sentry-ratelimiter-sample-apps-fastify:latest
trivy image --scanners vuln --ignore-unfixed \
  sentry-ratelimiter-sample-apps-fiber:latest
```
