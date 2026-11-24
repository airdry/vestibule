# sentry-ratelimiter

## Agent skills

### Issue tracker

Issues and specs live as markdown files under `.scratch/<feature>/` in this repo. See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical triage roles keep their default label strings. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `GLOSSARY.md` and one `docs/adr/` at the repo root. See `docs/agents/domain.md`.

### Code search & text processing

<!-- Search tool priority rule:
     Always prefer `rg` (ripgrep) or `ast-grep` over plain `grep`. Fallback to
     `grep` ONLY if ripgrep or ast-grep is unavailable or misbehaves in the
     current environment. Rationale: rg respects .gitignore, skips binaries,
     and is faster; ast-grep understands code structure (AST) instead of raw
     text, so pattern searches don't produce false positives.

     Substitution tool priority rule:
     Always prefer `sd` over `sed`. Rationale: sd matches literal strings by
     default (no accidental regex metacharacters), needs no delimiter
     escaping, and avoids the GNU vs BSD `sed -i` incompatibility that breaks
     scripts across macOS/Linux. Fallback to `sed` ONLY when `sd` is not
     available or misbehaves.

     File search tool priority rule:
     Always prefer `fd` over plain `find`. Rationale: fd respects .gitignore
     (skips target/, .git/, node_modules and other ignored paths) by default,
     has saner regex syntax, a friendlier output format, and is significantly
     faster. Fallback to `find` ONLY when `fd` is not available or when its
     .gitignore-aware filtering is itself the problem (e.g. you deliberately
     need to enumerate ignored files). -->

- Use the `ast-grep` skill for code search and structural queries instead of plain `grep`/`rg` when looking for code patterns, specific language constructs, or performing complex code queries. Plain text search is acceptable only for non-code files or trivial literal lookups.
- **Priority rule**: every time you are about to run `grep`, use `rg` (ripgrep) or `ast-grep` instead. Use plain `grep` only as a fallback when ripgrep or ast-grep do not work correctly in the given context (unavailable, broken, or producing wrong results).
- **Priority rule**: every time you are about to run `sed`, use `sd` instead for substitutions and in-place edits. Use plain `sed` only as a fallback when `sd` is not available or works incorrectly in the given context.
- **Priority rule**: every time you are about to run `find`, use `fd` instead for file and directory searches. Use plain `find` only as a fallback when `fd` is not available or works incorrectly in the given context.

## Engineering & TDD Workflow

### Language

- All code, tests, comments, documentation, and commit messages must be in **English**.
- Use the canonical terminology defined in `GLOSSARY.md` and respect all ADRs in `docs/adr/`.

### Strict Test-Driven Development (Red → Green → Refactor)

- Always load and follow the `tdd`, `rust-testing`, `rust-best-practices`, and `rust-async-patterns` skills when implementing tickets.
- Also load and follow the `clean-architecture` and `clean-code` skills for all production-code work: keep the Dependency Rule across module boundaries, and keep functions small, well-named, and free of smells.
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
- After finishing a ticket (and after heavy `cargo` usage), clean the build artifacts: run `cargo clean` so the `target/` directory does not accumulate thousands of small build files that do not belong in the project.

<!-- Add project-specific implementation rules below before starting Ticket 01 -->

