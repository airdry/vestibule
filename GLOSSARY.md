# vestibule

An HTTP rate-limiting and buffering proxy that sits in front of a self-hosted Sentry installation to protect Sentry's processing pipeline from SDK traffic spikes.

The name is the metaphor: a vestibule is the small chamber between the street and the building's interior, where visitors wait until they are let in. `vestibule` is that chamber for Sentry SDK traffic — it absorbs the surge, holds telemetry in the Ingestion Queue, and admits it to Upstream at a controlled rate.

## Language

**Upstream**:
The target Sentry reverse proxy (typically self-hosted Sentry's nginx in front of Relay) that receives forwarded traffic.
_Avoid_: nginx, backend, downstream, target

**Ingestion Route**:
An HTTP endpoint carrying Sentry SDK telemetry that is inspected and subject to rate limiting (matching `/api/{project_id}/...` or `/api/store/`).
_Avoid_: SDK route, API endpoint, relay route

**Pass-Through Traffic**:
Non-ingestion HTTP requests (such as Sentry UI, static assets, `/js-sdk/`, and `/api/0/relays/`) forwarded transparently to Upstream without rate-limit inspection.
_Avoid_: static traffic, non-SDK traffic, bypassed requests

**Envelope**:
A newline-delimited Sentry ingestion payload containing an envelope header and one or more Items sent in a single HTTP request.
_Avoid_: batch, request body, payload

**Item**:
An individual telemetry unit within an Envelope — such as an error `event`, a `transaction`, or a `session` — that carries its own type header and body.
_Avoid_: event (when referring to items generically), record, entry

**Item Category**:
The rate-limiting classification of an Item: `event` (error event), `transaction`, or `other` (all remaining Sentry item types such as `session`, `replay_event`, `profile`, `attachment`, `client_report`).
_Avoid_: event type, kind, metric type

**Rate Limit Rule**:
A configured throughput constraint (`rate` and `burst`) scoped by `project_id`, `environment`, and Item Category, matched from most specific to global defaults.
_Avoid_: quota, policy, throttle

**Ingestion Queue**:
The durable, disk-backed store-and-forward queue that holds accepted telemetry between HTTP ingestion and asynchronous batched delivery to Upstream.
_Avoid_: failover queue, buffer, spool

**Upstream Health**:
The tracked availability state of Upstream (`Healthy` or `Unhealthy`), driven by active health probes and passive delivery failures, controlling whether queued telemetry is forwarded or held.
_Avoid_: circuit state, nginx status, backend health

**Checkpoint**:

The durable consumer position of the Ingestion Queue: every record before it is durably handled. Advanced only through monotonic, idempotent commits of contiguous delivery prefixes.
_Avoid_: cursor, offset, read position

**Claim**:
An in-memory lease of one queued record by the delivery dispatcher; claims advance a volatile scan cursor and are replayed (at-least-once) after a restart until a Checkpoint commits them.
_Avoid_: lock, lease, read

**Merge Group**:
A set of queued `/envelope/` records sharing one `(route, credentials)` key, eligible for merging into a single gzip-compressed Envelope on egress.
_Avoid_: batch key, group key

**Runtime Settings**:
The hot-reloadable slice of the YAML configuration (rate limits, healthcheck, batching, queue TTL, success status code, log level), published as one atomic snapshot; startup-only fields (listen addresses, `queue.dir`) stay out.
_Avoid_: dynamic config, live config, runtime config

**Reload**:
One validated application of the YAML configuration file to the Runtime Settings: load, validate, atomically swap in — or reject, keeping the previous Runtime Settings active. Triggered identically by the file watcher, `SIGHUP`, and Admin Plane `POST /reload`.
_Avoid_: refresh, reconfigure, config push

**Graceful Shutdown**:
The `SIGTERM` sequence: `/readyz` flips to `503`, new connections stop being accepted, in-flight ingestion writes finish within `shutdown_timeout`, the Ingestion Queue and Checkpoint are flushed and `fsync`ed, and the process exits with code `0`.
_Avoid_: drain mode, soft stop, termination handler
