# `pkg/sessionctx/variable` parity audit ExecPlan

This is a living ExecPlan governed by `PLANS.md` at the repository root.

## Objective

Inventory the complete Go-master root `pkg/sessionctx/variable` package and
make the dependency-closed Rust session-variable behavior match it. The
observable outcome is that Rust recognizes the current Go-master embedding and
recent system-variable names, validates OpenAI-compatible embedding endpoints,
stores process-wide embedding settings, masks API keys, and advances the
configuration generation only when an effective value changes.

## Progress

- [x] (2026-09-01) Fetch and pin Go `origin/master` at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- [x] (2026-09-01) Read all 24 root artifacts (15,763 lines), including every
  production/test/support file and BUILD/OWNERS metadata. Confirm no root
  fixture, generated, platform, fuzz, or generator-input artifact exists.
- [x] (2026-09-01) Compare the full function/test inventory with Rust owners;
  keep nested `variable/tests` and `variable/tests/slowlog` as separate
  package claims.
- [x] (2026-09-01) Add the Rust embedding process-state owner, catalog the
  seven embedding variables and six current recent additions, and wire
  startup/SET/RESET/cluster publication and masked reads.
- [x] (2026-09-01) Add source-derived regressions for endpoint normalization,
  URL user-info stripping, key masking/versioning, and registry metadata.
- [x] (2026-09-01) Run the exact Go-master failpoint suite, Rust format check,
  `make lint`, and diff hygiene checks. Record the native OpenSSL blocker for
  the focused Rust cargo test.
- [ ] Fetch immediately before staging, create one meaningful batch commit,
  push it to `origin/hparser-integration`, and verify local/tracking/advertised
  SHAs and zero divergence.
- [ ] Continue the rolling audit with the next unrecorded nested package.

## Scope and implementation notes

The Go source is read directly from the fetched `origin/master` tree. The
package has no `doc.go`; `BUILD.bazel` declares the root library and its
50-shard flaky test target. The Rust implementation belongs in
`rust/crates/tidb-session` and must use its existing `GlobalSysvars` table and
system-variable registry. Scratch cluster registries must not publish
process-wide values until `replace_from` makes their committed image live.

The six non-embedding additions expose Go session fields, process atomics,
planner/transaction consumers, or duration parsing that do not yet have a
dependency-closed Rust owner. They are registered with exact metadata and
covered by a registry regression; their runtime hooks remain an explicit
boundary rather than a speculative second authority.

## Validation gate

Run from the repository root unless a command names the detached Go-master
worktree explicitly:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
    GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
    ./tools/check/failpoint-go-test.sh ./pkg/sessionctx/variable -count=1

    cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check

    cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml \
      -p tidb-session --lib tests_session_embedding_source -- --test-threads=1

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
    GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint

    git diff --check

The Go suite, format check, lint, and diff check must pass. The focused Rust
test is expected to remain blocked until `pkg-config` and native OpenSSL are
available on this macOS runner; retry after that environment changes. No Go,
test-import, Bazel, or module file changed, so `make bazel_prepare` is not
required for this batch.

## Surprises & Discoveries

- Go `net/url` stores credentials in `URL.User`, while `URL.Host` excludes
  user-info. A Rust endpoint rebuild that retained the authority verbatim would
  leak credentials and diverge from Go; the focused regression now pins the
  stripped result.
- The Rust catalog snapshot was 952 entries while current Go master exposes
  965 root entries. The six non-embedding entries cannot be made runtime
  complete without planner, session, transaction-file, process-atomic, and
  duration owners outside this package.
- The focused cargo test is blocked before crate compilation by
  `openssl-sys 0.9.117` requiring unavailable `pkg-config`/OpenSSL for
  `aarch64-apple-darwin`.

## Decision Log

- Decision: Keep endpoint and API-key state in one process-wide Rust module
  and publish it only from live `GlobalSysvars` mutations.
  Rationale: This is the closest native equivalent of Go's `vardef` atomics;
  scratch cluster tables must not change process behavior before commit.
  Date/Author: 2026-09-01, Codex.
- Decision: Register current Go-master variables whose runtime hooks lack a
  dependency-closed owner, but do not fabricate planner/session/transaction
  consumers.
  Rationale: Catalog recognition is useful and testable; a parallel authority
  would create behavior that is not Go-compatible.
  Date/Author: 2026-09-01, Codex.
- Decision: Treat the root package as an inventory/ownership boundary, not a
  package-complete Rust transcreation claim, until the cross-crate SessionVars,
  slow-log, status, and test integration owners are available.
  Rationale: Root `AGENTS.md` requires a complete dependency-closed package
  claim and forbids partial ports being reported as complete.
  Date/Author: 2026-09-01, Codex.

## Outcomes & Retrospective

The current batch adds executable embedding behavior and closes the Rust
registry gap for all 13 current Go-master additions. The complete inventory,
hashes, function/test counts, ownership boundary, and validation evidence are
recorded in `rust/testport/receipts/sessionctx_variable.md`. The package still
has cross-crate runtime gaps, and the rolling repository audit therefore
continues with nested packages after this batch is pushed.
