# Sample applications for sentry-ratelimiter

Three intentionally small applications used to exercise `sentry-ratelimiter`
with different Sentry SDKs, languages, and HTTP frameworks:

| App       | Framework | Language | Sentry SDK                |
| --------- | --------- | -------- | ------------------------- |
| `fastapi` | FastAPI   | Python   | `sentry-sdk[fastapi]` 2.23.1 |
| `fastify` | Fastify   | Node.js  | `@sentry/node` 9.6.0      |
| `fiber`   | Fiber     | Go       | `sentry-go` v0.31.0       |

The SDK versions are pinned to the releases that were current when
Sentry self-hosted **25.3.0** shipped (2025-03-17). The framework versions are
pinned to the same point in time as well.

Every application:

- exposes `GET /events` (error events) and `GET /transactions` (transactions);
- accepts `count` (1..1000) and `environment` query parameters;
- emits one JSON object per log line at `trace`/`debug` level, including
  Sentry SDK debug output and the full event/transaction payload in the
  `before_send` hooks;
- flushes the SDK before answering, so the HTTP response tells you whether the
  delivery attempt completed.

## Run everything with Docker Compose

No Python, Node.js, Go, or other runtime is required on the host. Everything is
built inside Docker.

```bash
cd sample-apps
docker compose build
docker compose up
```

From the repository root the same flow is available through `mise`:

```bash
mise run sample-apps        # docker compose up -d --build
mise run sample-apps-logs   # follow all three app logs
mise run sample-apps-down   # stop the stack
```

Published ports by default:

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

## Pointing the apps at sentry-ratelimiter

The default DSN is:

```text
http://public@host.docker.internal:8080/1
```

That assumes `sentry-ratelimiter` is reachable on the host at port `8080`
(for example, started from the repository root Compose file with a published
port). On Linux, the Compose file adds `host.docker.internal:host-gateway` so
the same default works there too.

Override the DSN without editing files:

```bash
SENTRY_DSN='http://public@sentry-ratelimiter:8080/1' docker compose up
```

Use that service-name form when the sample apps and the rate limiter are on the
same Docker network. The project number (`1` in the DSN path) becomes the
`project_id` used by the rate limiter's rules.

You can also override:

- `SENTRY_ENVIRONMENT` - default Sentry environment (default `sample-apps`);
- `SENTRY_RELEASE` - default release name;
- `LOG_LEVEL` - `trace`, `debug`, `info`, `warn`, or `error` (default `debug`);
- `FASTAPI_PORT`, `FASTIFY_PORT`, `FIBER_PORT` - host-side published ports.

Per-request `environment` query parameters override the SDK environment in the
payload, which lets you exercise environment-scoped rate limit rules without
restarting a container.

## Reading the logs

All three apps write JSON lines to stdout. Follow one app at a time:

```bash
docker compose logs -f fastapi
docker compose logs -f fastify
docker compose logs -f fiber
```

Useful messages:

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
├── fastapi/        # Python 3.12 + FastAPI + sentry-sdk 2.23.1
├── fastify/        # Node.js 22 + Fastify 5 + @sentry/node 9.6.0
└── fiber/          # Go 1.24 + Fiber 2 + sentry-go v0.31.0
```

Each app has its own multi-stage `Dockerfile`, runs as a non-root user, and
includes a `/health` endpoint used by the container health check.

The FastAPI app is served with `uvicorn` directly instead of the `fastapi` CLI:
the CLI does not expose uvicorn's `log_config`, which is needed to keep uvicorn
and Sentry SDK output on the same JSON formatter.
