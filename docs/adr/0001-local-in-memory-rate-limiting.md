# Local in-memory rate limiting per instance

We keep all rate-limiting counters in local process memory rather than sharing state via Redis or another external store.

## Context

`vestibule` sits on the hot path of all Sentry SDK traffic and must add minimal latency while remaining deployable anywhere from `docker-compose` to Kubernetes without external infrastructure dependencies.

## Decision

Each instance enforces its configured Token Bucket limits independently in local memory, using a bounded-cardinality bucket table (LRU/TTL eviction) to protect against unbounded `environment` values.

## Consequences

- Zero external network hops or failure modes on the ingestion hot path.
- In multi-replica Kubernetes deployments (`StatefulSet`), effective cluster-wide throughput scales linearly with replica count (`configured_limit * replicas`); operators size per-instance limits accordingly.
