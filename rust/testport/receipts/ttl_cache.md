# `pkg/ttl/cache` parity receipt

Status: Ready for this scoped batch. This receipt covers the complete Go
package inventory; it is not a repository-wide parity claim.

Published commit: `cca2f7711b4ac393d8ef0d979dda8accd9c3d243` on
`origin/hparser-integration`; local, tracking, and `git ls-remote` SHAs were
verified equal after the push/pull.

Comparison source: Go `origin/master` at `c6054025ed4c32ab3672a2a24ea46892714d21ec`.
Rust owner: `rust/crates/tidb-ttl/src/cache` on the hparser-integration worktree
before this batch (`b8c2cb741fa361825db335fa42ac38320899282`).

## Complete Go inventory

All thirteen tracked artifacts in `pkg/ttl/cache` were read in full before
editing: 3,566 lines total, including production code, tests, and Bazel
metadata. There is no package `doc.go`, fixture or `testdata` directory,
generated source or input, platform/build-tag variant, benchmark, fuzz target,
README, or ownership artifact.

| artifact | lines | role |
| --- | ---: | --- |
| `BUILD.bazel` | 83 | Go library and test targets |
| `base.go` | 45 | cache interval and refresh state |
| `base_test.go` | 33 | base-cache tests |
| `infoschema.go` | 114 | TTL table info-schema cache |
| `infoschema_test.go` | 73 | info-schema cache tests |
| `main_test.go` | 34 | package test setup |
| `split_test.go` | 1,467 | key-range split fixtures and edge cases |
| `table.go` | 717 | physical TTL table and expiry/range logic |
| `table_test.go` | 292 | table and expiry tests |
| `task.go` | 199 | TTL task SQL, range codec, and row decoder |
| `task_test.go` | 135 | task SQL/row integration tests |
| `ttlstatus.go` | 193 | table status cache and row decoder |
| `ttlstatus_test.go` | 181 | table status tests |

The production Go files and BUILD metadata are byte-identical to current Go
master. The branch's `task_test.go` has an existing 18-line test-harness
stabilization relative to Go master; it was preserved and not part of this
batch. The Rust owner inventory was also read in full: the five cache modules,
the aggregate `cache_test.rs`, package module header, crate manifest, lockfile,
and crate root.

## Parity findings and implementation

`insert_into_ttl_task` previously accepted already-encoded byte vectors and
discarded Go's location argument. It now accepts the source datum slices,
encodes both bounds through `tidb_codec::encode_key_in_timezone`, and returns
the package `Result` so codec errors reach callers just as Go returns its
`EncodeKey` error.

`row_to_ttl_task` previously exposed encoded scan-range bytes and raw JSON
state text. It now decodes non-empty ranges with `tidb_codec::decode` and
deserializes `TTLTaskState` with `serde_json`, including Go's zero-value
defaults for omitted fields and `null`, while propagating malformed JSON as an
error. `TTLTask` consequently carries `Option<Vec<Datum>>` and
`Option<TTLTaskState>`, matching the Go shape instead of preserving Rust-only
raw representations.

The only remaining cache production boundary is the `Update` traversal in
`InfoSchemaCache` and `TableStatusCache`, which requires the real Go
`infoschema.InfoSchema` package and is explicitly retained as a trait boundary.
No Rust-only behavior was added or left in the task codec/JSON paths.

## Focused regression coverage

The cache owner test now asserts decoded integer ranges and a populated
`TTLTaskState`, exercises the source-shaped insert builder and encoded SQL
arguments, and adds `test_row_to_ttl_task_rejects_invalid_state_json` for the
Go error path. Before the production change, the focused test failed to
compile because the owner still exposed `Option<Vec<u8>>`/`Option<String>` and
the old encoded-byte insert signature. After the change these tests pass.

## Validation

Profile: **Ready**.

- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-ttl --all-targets`
- the same locked toolchain with `cargo ... test --offline --locked -p tidb-ttl --test cache_test -- --test-threads=1` — 20 passed.
- the same locked toolchain with `cargo ... test --offline --locked -p tidb-ttl --tests -- --test-threads=1` — cache (20), session (11), and SQL (5) tests passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test -tags=intest ./pkg/ttl/cache -count=1` — passed (19.402s).
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — required Ready gate.
- `git diff --check` — passed.

No Go source, import section, test target, or Bazel file changed, so the
`make bazel_prepare` gate was not applicable to this Rust-only implementation.

## Risks and unverified scope

- Correctness risk is limited to persisted TTL task range encoding/decoding and
  JSON state conversion; both now use shared codec/serde implementations and
  have focused malformed-input coverage.
- Compatibility risk: Rust callers of `insert_into_ttl_task` must now supply a
  `SessionTimeZone` and datum slices, and callers observe typed decoded task
  state rather than raw text. This is the intended source API correction; no
  in-tree Rust caller outside the owner tests existed.
- Performance risk is bounded by one encode/decode allocation per persisted
  range and one JSON parse per non-NULL state, matching the Go operations.
- Not verified locally: live Rust/Go cross-runtime task exchange, the two
  info-schema `Update` paths behind the explicit boundary, non-host platforms,
  and repository-wide integration suites.

The rolling repository audit continues with the remaining package checklist.
