# sentry-ratelimiter

## Agent skills

### Issue tracker

Issues and specs live as markdown files under `.scratch/<feature>/` in this repo. See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical triage roles keep their default label strings. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `GLOSSARY.md` and one `docs/adr/` at the repo root. See `docs/agents/domain.md`.

## Engineering & TDD Workflow

### Language

- All code, tests, comments, documentation, and commit messages must be in **English**.
- Use the canonical terminology defined in `GLOSSARY.md` and respect all ADRs in `docs/adr/`.

### Strict Test-Driven Development (Red → Green → Refactor)

- Always load and follow the `tdd`, `rust-testing`, `rust-best-practices`, and `rust-async-patterns` skills when implementing tickets.
- **Test first, always**: write a failing test for the next acceptance criterion before writing production code. Run `cargo test` and confirm it fails for the expected reason (**Red**).
- Write the minimal production code required to make the test pass (**Green**).
- Refactor only while tests are green, keeping changes clean and minimal (**Refactor**).
- Test **external observable behavior** through the agreed test seams (localhost HTTP boundary + mock `Upstream` + real `tempdir`, injectable `Clock`, plus focused unit tests on the Sentry Wire Codec and segmented disk WAL). Avoid testing private internals or brittle mocks.

### Rust Quality Gates

Follow the curated Rust quality gates from `https://github.com/mokevnin/dev-quality-gates#rust`. Before considering any ticket complete, verify:

- `cargo fmt --check` (`rustfmt`) passes
- `cargo clippy --all-targets -- -D warnings` (`clippy`) passes
- `cargo machete` (`cargo-machete`) finds no unused dependencies in `Cargo.toml`
- `cargo deny check` (`cargo-deny` with a repo-root `deny.toml` covering RustSec advisories, licenses, bans/duplicates, and allowed registries) passes
- `cargo test` passes with zero ignored/failing tests
- No `.unwrap()` or `.expect()` in production (non-test) code paths; propagate typed errors cleanly (`thiserror` for library/domain errors, `anyhow` only at top-level binary boundaries).

### Ticket Execution Rhythm

- Work one ticket at a time from `.scratch/sentry-ratelimiter/issues/NN-*.md` in dependency order (`Blocked by`).
- Never commit `.scratch/` (it is gitignored and kept local only).
- After completing a ticket, check off its acceptance criteria and set `Status: resolved` in its local ticket file, then run `code-review` against the ticket and `AGENTS.md` before moving on.

<!-- Add project-specific implementation rules below before starting Ticket 01 -->

