# `pkg/util/kvcache` — complete package transcreation

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete inventory

The package has exactly four artifacts, all read in full: `simple_lru.go`,
`simple_lru_test.go`, `main_test.go`, and `BUILD.bazel`. There is no package
doc, README, fixture, benchmark, generated or platform variant, or ownership
file. The local Go package is byte-identical to the pin.

Production behavior includes byte-hash key identity, MRU promotion and
ordering, capacity and process-memory eviction, eviction callbacks only for
automatic eviction, deletion, full clearing, capacity changes, oldest-entry
removal, the package-global memory tracker, and the heap-profile method name.
The source contains exactly eight tests; the test main installs TiDB's common
test environment and leak checker.

## Rust ownership and audit result

The complete package maps to two native crates:

- `rust/crates/tidb-kvcache/src/lib.rs` owns the generic LRU, key interface,
  callback, capacity error, profile name, and injectable process-memory probe.
- `rust/crates/tidb-util/src/kvcache.rs` re-exports that surface at the
  existing utility boundary and owns the global memory tracker because the
  tracker implementation itself lives in `tidb-util`.

Stable indexed nodes replace Go's `container/list`; byte-hash identity and
ordering are unchanged. The caller-supplied process-memory probe is the native
runtime boundary for Go's `memory.InstanceMemUsed`, while quota zero skips it
exactly as Go does. Repository callers use the quota-free constructor or the
full injected-memory constructor according to their owner behavior.

The audit confirmed the earlier claimed `Peek` gap was erroneous: the pinned
Go package has no `Peek`. It removed the one remaining Rust-only test for the
global tracker and replaced the older batch receipt with this atomic package
inventory. The test suite now contains exactly the eight Go-owned cases.

## Validation

Profile: WIP; this is one completed package in the continuing package-by-
package audit, not a repository-wide readiness claim.

- `git diff --exit-code e2788410d8d696605e8cb002585877a063ccc909 -- pkg/util/kvcache` — passed.
- `go test ./pkg/util/kvcache -run '^(TestPut|TestZeroQuota|TestOOMGuard|TestGet|TestDelete|TestDeleteAll|TestValues|TestPutProfileName)$' -count=1` — blocked before package execution by the existing `google.golang.org/grpc/internal/transport` reference to missing `http2.TrailerPrefix`.
- `cargo test -p tidb-kvcache --locked` — passed (8 integration tests and doc tests).
- `cargo check -p tidb-stmtsummary -p tidb-datatype -p tidb-session -p tidb-executor --lib --locked` — passed.
- `cargo test -p tidb-util kvcache --lib --locked` — passed with no supplemental kvcache tests remaining.
- `cargo fmt --all --check` and `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required.

## Risk

- Correctness: production code is unchanged; all eight source-owned tests and
  every repository consumer compile.
- Compatibility: only a Rust-only test and its obsolete receipt were removed.
- Performance: unchanged.
