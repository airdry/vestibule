# vestibule

An HTTP rate-limiting and buffering proxy that sits in front of a self-hosted Sentry installation and protects its ingestion pipeline from SDK traffic spikes.

Point your Sentry DSNs at `vestibule` instead of Sentry — no SDK changes required.

## Why

Self-hosted Sentry degrades when many applications emit a sudden burst of telemetry: Relay and the processing workers saturate, and once processing starts failing, Sentry's own rate limits can no longer take effect. The only remaining lever is reconfiguring the SDKs of every application, which is slow and operationally infeasible — so a single noisy service can blind the whole organization.

`vestibule` puts a controllable gate in front of Sentry:

- **Rate limits live in one YAML file**, not in hundreds of SDK deployments, and reload without a restart.
- **SDK latency is decoupled from Sentry**: accepted telemetry is written to a durable disk queue and acknowledged immediately, so a slow or broken Sentry never slows your applications down.
- **Nothing is lost during an outage**: telemetry accumulates on disk and is replayed automatically once Sentry recovers.

## How it works

One process, two HTTP listeners:

| Plane | Example port | Purpose |
| --- | --- | --- |
| Data Plane | `8080` | Reverse proxy for all Sentry traffic. Ingestion routes (`/api/{project_id}/...`, `/api/store/`) are inspected, rate-limited and queued; everything else (UI, static assets, `/js-sdk/`, `/api/0/relays/`) is passed through untouched. |
| Admin Plane | `8081` | `/healthz`, `/readyz`, `/metrics` (Prometheus), `/status`, `POST /reload`. |

For every ingestion request `vestibule`:

1. decompresses the body (`gzip`, `deflate`, `br`) and parses the Sentry `Envelope`;
2. classifies each `Item` as `event`, `transaction` or `other` and reads its `environment`;
3. applies a token-bucket `Rate Limit Rule`, resolved from the most specific scope (`project_id` + `environment` + category) down to global defaults;
4. strips only the throttled items, rebuilds a valid Envelope, appends it to the disk queue and acknowledges the SDK (`201 Created`, or `429 Too Many Requests` with `X-Sentry-Rate-Limits` when everything was throttled).

Background workers merge compatible queued Envelopes into gzip batches and drain them to Sentry with bounded concurrency, pausing while Sentry is unhealthy and resuming automatically on recovery. `/readyz` stays `200 OK` during a Sentry outage, so Kubernetes keeps routing SDK traffic into the buffer.

## Quick start

```bash
docker build -t vestibule .
docker run --rm -p 8080:8080 -p 8081:8081 \
  -v "$PWD/config.yaml:/etc/vestibule/config.yaml:ro" \
  -v vestibule-queue:/var/lib/vestibule \
  vestibule
```

Or from source:

```bash
cargo run --release -- --config config.yaml
```

The configuration path can also be supplied through `VESTIBULE_CONFIG`.

### Minimal configuration

```yaml
server:
  data_plane_addr: "0.0.0.0:8080"
  admin_addr: "0.0.0.0:8081"

upstream:
  url: "http://sentry-nginx:80"

queue:
  dir: "/var/lib/vestibule/queue"

rate_limits:
  defaults:
    event:
      rate: 5      # items per second
      burst: 10    # bucket capacity
    transaction:
      rate: 2
      burst: 5
```

Every other setting has a default. [`sample-apps/vestibule.yaml`](sample-apps/vestibule.yaml) is a complete example with per-project and per-environment overrides, and [`sample-apps/`](sample-apps/README.md) runs a full local stack (FastAPI, Fastify and Fiber apps reporting through `vestibule`).

## Documentation

Detailed documentation — every configuration key, rate-limit resolution rules, queue tuning, deployment recipes and troubleshooting — is planned. Until then:

- [`GLOSSARY.md`](GLOSSARY.md) — the domain language used across the codebase and docs;
- [`docs/adr/`](docs/adr/) — architectural decisions (in-memory rate limiting, disk-backed queue, readiness independent of upstream health);
- [`docs/release.md`](docs/release.md) — the GitHub Actions quality gates, image build and the approval-gated `ghcr.io` release process;
- [`sample-apps/vestibule.yaml`](sample-apps/vestibule.yaml) — the reference configuration.

## License

Apache-2.0
