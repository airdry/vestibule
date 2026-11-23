# Keep `/readyz` healthy when Upstream is unavailable

The Kubernetes readiness probe (`/readyz`) continues returning `200 OK` even when `Upstream Health` is `Unhealthy`.

## Context

In Kubernetes, a failing `/readyz` probe removes the pod from Service endpoints, causing incoming traffic to fail at the Service/Ingress layer.

## Decision

`/readyz` reflects whether `sentry-ratelimiter` itself can accept and buffer SDK traffic (config loaded, listener bound, `Ingestion Queue` writable, and not shutting down). It returns `503` only during startup or graceful shutdown (`SIGTERM`), never due to `Upstream Health`. Upstream availability is surfaced separately via Prometheus (`upstream_healthy`) and `GET /status` on the admin port.

## Consequences

- During a Sentry outage, Kubernetes keeps routing SDK traffic to `sentry-ratelimiter` so telemetry is safely rate-limited and buffered to disk until Upstream recovers.
- Operators monitoring Sentry availability must alert on `upstream_healthy` rather than relying on pod readiness status.
