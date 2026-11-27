# CI and release process

`vestibule` is validated and published entirely by GitHub Actions. The workflows
live in [`.github/workflows/`](../.github/workflows/) and are deliberately thin:
they call the same [`mise`](https://mise.jdx.dev) tasks developers run locally, so
the pinned tool versions in `mise.toml` are the single source of truth.

## Workflows

| Workflow | File | Trigger | What it does |
| --- | --- | --- | --- |
| Quality gates | `quality-gates.yml` | Called by the two workflows below | Rust 1.100.0, `cargo-deny` 0.22.0 and `cargo-machete` 0.10.1 from `mise.toml`; runs `fmt-check`, `machete`, `deny`, `lint-test` |
| CI | `ci.yml` | `pull_request` to `main`, `push` to `main`, manual | Runs the gates, then builds the production `Dockerfile` **without publishing** and uploads the image as a workflow artifact |
| Release | `release.yml` | `push` to `main`, `v*` tags, manual | Reads the version from `Cargo.toml`, runs the gates, waits for approval of the `release` environment and pushes the image to GHCR (skipping versions that are already published) and creates a GitHub Release |

All runners are GitHub-hosted `ubuntu-26.04` (`x64`). Currency of every
third-party action is a major-version pin, for example `actions/checkout@v7`.

## The gates

Identical to the quality gates in `AGENTS.md`, executed through `mise`:

| Gate | CI step | Local equivalent |
| --- | --- | --- |
| `rustfmt` | `mise run fmt-check` | `mise run fmt-check` |
| unused dependencies | `mise run machete` | `mise run machete` |
| advisories, licenses, bans, sources | `mise run deny` | `mise run deny` |
| `clippy` with warnings denied and the test suite | `mise run lint-test` | `mise run lint-test` |

`mise run gates` runs all of them in order. `lint-test` covers the clippy and
the test gate in a single compilation pass: `cargo clippy` only checks (rmeta),
so a following `cargo test` would rebuild every dependency for codegen, while
using `clippy-driver` as the workspace rustc wrapper lints the crate as it is
built for the tests. `mise run lint` and `mise run test` still run either gate on
its own. Every pull request against `main` must pass the `mise quality gates`
check, and the pull request itself never publishes anything: it only proves that
the image still builds.

## Pull requests

1. Push a branch and open a pull request against `main`.
2. `Quality gates` and `Build image (no publish)` run on every update. The gates
   are the required status check; the image build starts only after they pass.
3. The built image is available for seven days as the `vestibule-image-sha-<sha>`
   workflow artifact and can be inspected locally:
   ```bash
   gh run download --name vestibule-image-sha-<sha>
   docker load --input vestibule-image.tar
   docker run --rm vestibule:<version> --help
   ```
   The tarball carries two tags: `vestibule:<version>` from `Cargo.toml` and
   `vestibule:sha-<short sha>` for the commit it was built from.

Nothing reaches `ghcr.io` from a pull request.

## Releasing

The published artifact is the container image `ghcr.io/airdry/vestibule`, tagged
with the version from `Cargo.toml`. No separate binary archive is shipped and no
git tag is required to publish.

1. Bump `version` in `Cargo.toml` (refresh `Cargo.lock` with `cargo build
   --locked`) and merge the change into `main`.
2. The merge triggers `Release`. `Quality gates` must pass and `Read the crate
   version` reads the version from the merged `Cargo.toml`; when the run was
   started by a `v*` tag, the tag itself must match (`v1.0.0` ← `version =
   "1.0.0"`), so image tags cannot drift from the manifest.
3. `Publish image to GHCR` waits for the `release` environment approval. Merging
   is not enough: a reviewer must approve the deployment before anything is
   pushed.
4. After approval the image is pushed to `ghcr.io/airdry/vestibule` with the
   version tags described below, the same image is attached as a `docker
   load`-able tarball to a generated GitHub Release, and the run summary lists
   the published tags.

**A version is published once.** Every merge to `main` runs the workflow, so the
`X.Y.Z` tag is checked against the registry first: when it already exists the
push is skipped with a notice, because an immutable release tag must not move to
a later commit. To republish deliberately, run `workflow_dispatch` with
`force: true` and the ref to publish.

The workflow can also be started for a specific tag or branch with
`workflow_dispatch` (input: `tag`, for example `v1.0.0`); it goes through the
same gates, version check and approval.

### Image tags

There is no `latest` tag. Every release is addressable explicitly: by its
version, by a moving minor or major tag, or by its commit. A consumer therefore
pins the line it depends on instead of silently following whatever was published
last.

For `version = "1.0.0"` the published tags are:

| Tag | Meaning |
| --- | --- |
| `1.0.0` | immutable release |
| `1.0` | tip of the minor line |
| `1` | tip of the major line |
| `sha-<short>` | the exact commit |

A pre-1.0 release is deliberately narrower. For `version = "0.9.0"` only these
are published:

| Tag | Meaning |
| --- | --- |
| `0.9.0` | immutable release |
| `0.9` | tip of the minor line |
| `sha-<short>` | the exact commit |

A 0.x line makes no stability promise, so it gets no major tag either: `0` would
suggest a moving major line that does not exist yet. Major tags start at `1`.

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
  this is the approval gate in front of every publish. GitHub creates the
  environment on first use, but without reviewers the publish job runs
  immediately, so the reviewers are what actually gate the push.
- **Package visibility** (*Packages → vestibule → settings*) set to public, so the
  image can be pulled anonymously. The push itself does not depend on it: the
  image is in the registry either way, only anonymous pulls are affected.
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
