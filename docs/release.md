# CI and release process

`vestibule` is validated and published entirely by GitHub Actions. The workflows
live in [`.github/workflows/`](../.github/workflows/) and are deliberately thin:
they call the same [`mise`](https://mise.jdx.dev) tasks developers run locally, so
the pinned tool versions in `mise.toml` are the single source of truth.

## Workflows

| Workflow | File | Trigger | What it does |
| --- | --- | --- | --- |
| Quality gates | `quality-gates.yml` | Called by the two workflows below | Rust 1.100.0, `cargo-deny` 0.22.0 and `cargo-machete` 0.10.1 from `mise.toml`; runs `fmt-check`, `machete`, `deny`, `lint`, `test` |
| CI | `ci.yml` | `pull_request` to `main`, `push` to `main`, manual | Runs the gates, then builds the production `Dockerfile` **without publishing** and uploads the image as a workflow artifact |
| Release | `release.yml` | `v*` tags, manual | Verifies the tag against `Cargo.toml`, runs the gates, waits for approval of the `release` environment, pushes the image to GHCR and creates a GitHub Release |

All runners are GitHub-hosted `ubuntu-26.04` (`x64`). Currency of every
third-party action is a major-version pin, for example `actions/checkout@v7`.

## The gates

Identical to the quality gates in `AGENTS.md`, executed through `mise`:

| Gate | CI step | Local equivalent |
| --- | --- | --- |
| `rustfmt` | `mise run fmt-check` | `mise run fmt-check` |
| unused dependencies | `mise run machete` | `mise run machete` |
| advisories, licenses, bans, sources | `mise run deny` | `mise run deny` |
| `clippy` with warnings denied | `mise run lint` | `mise run lint` |
| test suite | `mise run test` | `mise run test` |

`mise run gates` runs all five in dependency order. Every pull request against
`main` must pass the `mise quality gates` check, and the pull request itself never
publishes anything: it only proves that the image still builds.

## Pull requests

1. Push a branch and open a pull request against `main`.
2. `Quality gates` and `Build image (no publish)` run on every update. The gates
   are the required status check; the image build starts only after they pass.
3. The built image is available for seven days as the `vestibule-image-sha-<sha>`
   workflow artifact and can be inspected locally:
   ```bash
   gh run download --name vestibule-image-sha-<sha>
   docker load --input vestibule-image.tar
   ```

Nothing reaches `ghcr.io` from a pull request.

## Releasing

The published artifact is the container image `ghcr.io/airdry/vestibule`. No
separate binary archive is shipped.

1. Bump `version` in `Cargo.toml` (and refresh `Cargo.lock` with
   `cargo build --locked`) and merge the bump into `main`.
2. Tag the merged commit and push the tag:
   ```bash
   git tag v1.0.0
   git push origin v1.0.0
   ```
3. `Release` starts. `Verify tag matches Cargo.toml` fails the run when the tag
   and the crate version disagree, so image tags cannot drift from the manifest.
4. `Publish image to GHCR` waits for the `release` environment approval. Merging
   is not enough: a reviewer must approve the deployment before anything is
   pushed.
5. After approval the workflow pushes the image, attaches the same image as a
   `docker load`-able tarball to a generated GitHub Release, and prints the
   result in the run summary.

The release can also be re-run for an existing tag with `workflow_dispatch`
(input: the tag name, for example `v1.0.0`); it goes through the same gates,
verification and approval.

### Image tags

For `version = "1.0.0"` the published tags are:

| Tag | Meaning |
| --- | --- |
| `1.0.0` | immutable release |
| `1.0` | latest patch of the minor line |
| `1` | latest minor of the major line |
| `latest` | most recent published release |
| `sha-<short>` | the exact commit |

The image carries the standard OCI labels (`source`, `revision`, `version`,
`created`, `licenses`), so `org.opencontainers.image.revision` identifies the
commit no matter which tag was pulled.

### Pulling the image

```bash
docker pull ghcr.io/airdry/vestibule:1.0.0
```

The package is public: pulling requires no registry login. Only the initial
publish of a brand-new package may show it as private until its visibility is
switched to public once in the repository's *Packages* settings.

## One-time repository setup

These settings live in the GitHub UI, not in the repository, and are required
for the pipeline above:

- **Environment `release`** (*Settings → Environments*) with required reviewers —
  this is the approval gate in front of every publish.
- **Package visibility** (*Packages → vestibule → settings*) set to public, so the
  image can be pulled anonymously.
- **Workflow permissions** (*Settings → Actions → General*) allowing GitHub
  Actions to create and approve pull requests is **not** required; the workflows
  rely only on the built-in `GITHUB_TOKEN` with `contents: read` by default and
  escalate to `contents: write` / `packages: write` solely in the publishing job.
- **Branch protection for `main`** requiring the `mise quality gates` check, so a
  pull request cannot merge while the gates are red.

## Scope

The initial release targets `linux/amd64`, matching the `Dockerfile`, which
cross-compiles `x86_64-unknown-linux-musl`. Multi-architecture images
(`linux/arm64`) are a follow-up.
