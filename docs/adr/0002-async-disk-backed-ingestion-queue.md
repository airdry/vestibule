# Always-async store-and-forward via disk-backed Ingestion Queue

All Ingestion Route requests are accepted locally into a durable, disk-backed segmented WAL (`Ingestion Queue`) and forwarded asynchronously in batches to Upstream, rather than synchronously reverse-proxied on the hot path.

## Context

`vestibule` must shield Sentry SDKs from latency spikes and outages in Upstream Sentry while supporting batched Envelope delivery and surviving pod restarts without losing buffered telemetry.

## Decision

Pingora synchronously reverse-proxies `Pass-Through Traffic` directly to Upstream, but intercepts all `Ingestion Route` traffic: after decompression, per-Item rate limiting, and envelope filtering, accepted payloads are appended to a disk-backed segmented WAL and acknowledged immediately (`201 Created` by default, configurable to `200`). Background workers asynchronously merge compatible Envelopes by `(project_id, auth)` and deliver them to Upstream with bounded concurrency and drain rate limits, pausing when `Upstream Health` is `Unhealthy`.

Delivery consumption is split into two cursors: a durable **Checkpoint** that advances only through idempotent, monotonic commits of contiguous delivery prefixes, and a volatile claim cursor that hands records to the delivery pool. Claims are replayed after a crash (at-least-once), so single-record failures never strand the queue.

## Considered Options

- **Synchronous proxy when healthy + failover queue only on outage**: rejected because it introduces two divergent code paths, prevents Envelope batching under normal load, and exposes SDKs to Upstream latency spikes before health checks trip.
- **In-memory-only queue**: rejected because buffered telemetry would be lost on pod OOMKill or restart during an Upstream outage.

## Consequences

- Single unified data path for both normal operation and Upstream outages, with consistently low SDK latency.
- Kubernetes deployments should use a `StatefulSet` with a PVC (or `emptyDir` when restart durability is not required).
- Synchronous Upstream `401`/`403`/`400` responses on Ingestion Routes are not returned to SDKs; rejected batches are dropped and tracked via metrics and logs instead.
- Merge Groups key on `(route, credentials)`; Envelopes with different project routes or different `sentry_key`/auth credentials are never combined into one outbound request.
- Records merge into a single gzip-compressed Envelope on egress (`Content-Encoding: gzip`); single-record batches and non-envelope records are forwarded verbatim.
- Backlog replay is bounded by both worker concurrency and a drain rate limit (`max_requests_per_sec`), so replays cannot overwhelm Upstream.
