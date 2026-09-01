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

No Go/Bazel artifact changed, so `make bazel_prepare` is not required for this
Rust-only batch.

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
