# `pkg/ttl/cache` audit ExecPlan

This living plan follows `PLANS.md` and records one complete Go-package batch.

## Purpose / Big Picture

`pkg/ttl/cache` owns TTL table metadata, split-range construction, task and
table-status row decoders, and the SQL statements used by TTL workers. The Rust
owner is `rust/crates/tidb-ttl/src/cache`. This batch closes the dependency-
available task codec and JSON state gaps while retaining the real-
`infoschema.InfoSchema` traversal as an explicit boundary.

## Progress

- [x] (2026-09-02) Pinned Go master at
  `c6054025ed4c32ab3672a2a24ea46892714d21ec` and read all thirteen Go
  artifacts (3,566 lines), including BUILD metadata and every test. Confirmed
  no doc, fixture, generated/platform, benchmark, fuzz, or ownership artifact.
- [x] (2026-09-02) Read the five Rust cache modules, aggregate owner tests,
  crate manifest/lockfile, and crate-level headers; confirmed `tidb-codec` is
  already a normal dependency and `serde_json` can be added without a new
  architectural edge.
- [x] (2026-09-02) Added the source-shaped timezone-aware datum encoding,
  shared range decoder, typed JSON state decoder, and focused range/state
  regressions. The pre-fix focused test failed to compile against the old raw
  byte/text API.
- [x] (2026-09-02) Ran the Rust owner focused/full tests, Go package tests with
  the required `intest` tag, Rust formatting/check, repository lint, and diff
  hygiene; the Ready profile is green.
- [x] (2026-09-02) Published commit `cca2f7711b4ac393d8ef0d979dda8accd9c3d243`
  to `origin/hparser-integration`, fetched and fast-forward pulled the latest
  tip, and verified local, tracking, and `git ls-remote` SHAs match.
- [x] (2026-09-02) Restored Go master's TTL task test harness: both task-row
  tests now stop and await the background TTL job manager before inserting
  fixtures, preventing nondeterministic task GC.
- [x] (2026-09-02) Ran focused task regressions, repository lint, and diff
  checks; attempted `make bazel_prepare` (blocked by missing `bazel`).
- [x] (2026-09-02) Committed this Go test-harness batch with the updated
  receipt, pushed to `hparser-integration`, verified the remote SHA, and
  fast-forward pulled.

## Scope and decisions

The atomic unit is the complete Go package: all nine production/test source
files plus BUILD metadata (thirteen artifacts total). `InsertIntoTTLTask`
encodes the caller's datum bounds using `SessionTimeZone`, preserving the
source location-sensitive key format. `RowToTTLTask` decodes ranges and parses
state with `serde_json`; `null` state maps to Go's allocated zero-value struct,
and malformed input returns the package error.

The `InfoSchemaCache.Update` and `TableStatusCache.Update` traversals remain
trait boundaries because the Go `pkg/infoschema` dependency is not
transcreated. No speculative mock or duplicate cache traversal is introduced.

## Validation gate

Run from the repository root:

    cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
    OPENSSL_DIR=<pinned OpenSSL dir> DYLD_LIBRARY_PATH=<pinned OpenSSL lib> \
      cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-ttl --all-targets
    OPENSSL_DIR=<pinned OpenSSL dir> DYLD_LIBRARY_PATH=<pinned OpenSSL lib> \
      cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-ttl --tests -- --test-threads=1
    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> \
      go test -tags=intest ./pkg/ttl/cache -count=1
    PATH=<pinned Go>/bin:$PATH GOPATH=<pinned GOPATH> make lint
    git diff --check

The Go test source changed in this follow-up, so `make bazel_prepare` is
required; the local executable is unavailable.

## Decision log

- 2026-09-02: Treat range encoding and JSON decoding as one cache/task batch;
  both were explicitly marked blocked only by dependencies that are now
  present, and both belong to the same `task.go` production contract.
- 2026-09-02: Keep info-schema cache updates as a named boundary instead of
  inventing a Rust `InfoSchema` implementation.

## Outcomes and retrospective

The complete task SQL/row behavior is source-shaped in published commit
`cca2f7711b4ac393d8ef0d979dda8accd9c3d243`; the package remains partial only
for the two real info-schema traversal boundaries and for live server/region
integration tests outside the dependency-closed owner.

## Follow-up: discardable cache returns (2026-09-06)

- [x] Re-read all thirteen Go artifacts at current origin/master f2c346fe4f368ff855e17c1f62e28a89ba7f9723: 3,572 lines, including BUILD metadata, five production files, and seven tests, with no fixtures, generated/platform variants, benchmark, fuzz, example, or README artifact.
- [x] Re-read the complete Rust cache modules, owner tests, manifest/workspace registration, and direct callers before editing.
- [x] Classify 31 explicit annotations: remove 26 direct Go-shaped constructor/accessor/helper annotations; retain five Rust-only or error-contract annotations (update_time, table_info_ptr_eq, from_i64, MockExpireTimeKey.get, and insert_into_ttl_task).
- [x] Add the focused deny-on-discard regression. It failed before the source edit with exactly 26 diagnostics and passes afterward.
- [x] Run the full tidb-ttl owner suite (39 tests), all-target compile, formatting, Ready lint, and diff hygiene.
- [x] Commit once for pkg/ttl/cache, rebase/push to origin/hparser-integration, and verify the remote SHA in the task handoff.
- [ ] Continue the rolling audit with the next complete package boundary.

Only Rust must-use metadata in the five cache modules, one source regression, and parity documentation change. Go's ordinary functions and methods permit discarding their return values, so the 26 direct counterparts must not add a Rust-only lint failure. The retained insert_into_ttl_task annotation is an error contract already enforced by its Result return type; the other four retained annotations are Rust-only adapters/accessors. No SQL text, argument encoding, cache refresh, key splitting/decoding, table identity, expiry, or error behavior changes.

Ready validation for this Rust-only follow-up used the focused cache regression, the full tidb-ttl owner nextest suite (39/39), tidb-ttl all-target check, workspace formatting, make lint, and git diff --check. No Go source or Bazel metadata changed, so make bazel_prepare was not required; Go execution and live TiDB/etcd integration remain outside this contract batch.

The cache owner already carried the source runtime behavior, including the documented info-schema/session boundaries. The remaining mismatch was solely Rust's stricter discard enforcement on 26 source-shaped values. Publication is tracked by the package-scoped commit and remote SHA in the task handoff while the rolling audit continues.
