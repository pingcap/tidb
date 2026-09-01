# `pkg/util/kvcache` — complete Go-master package transcreation

Go source: `origin/master` at
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package has exactly four artifacts, all read in full: `simple_lru.go`,
`simple_lru_test.go`, `main_test.go`, and `BUILD.bazel`. There is no package
doc, README, benchmark, generated or platform variant, or ownership file.
The rolling checkout was missing Go master's `SimpleLRUCache.Peek`; this
batch restores the O(1) non-promoting method and folds its ordering assertion
into `TestGet` exactly as pinned above.

Production behavior includes byte-hash key identity, MRU promotion and
ordering, non-promoting `Peek`, capacity and process-memory eviction, eviction
callbacks only for automatic eviction, deletion, full clearing, capacity
changes, oldest-entry removal, the package-global memory tracker, and the
heap-profile method name. The source contains exactly eight tests after the
current-master assertion remains inside `TestGet`; the test main installs
TiDB's common test environment and leak checker.

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

Go master adds `Peek` as a read-only lookup that must not move the entry to the
front. Rust now exposes `SimpleLruCache::peek(&self, ...)`, preserving the
source's byte identity and MRU order; the source-shaped `test_get` regression
reads key 2 and verifies key 4 remains the newest. The eight Go-owned cases
remain the complete executable suite.

## Validation

Profile: Ready for this package batch; the repository-wide parity loop remains
open and this is not a final-status claim.

- `git ls-tree -r --name-only origin/master -- pkg/util/kvcache` and full-file
  reads — passed; confirmed the four-artifact inventory and current-master
  `Peek` delta.
- Before the fix, the focused `TestGet` command failed to compile with
  `lru.Peek undefined`; after restoring the method, the same test passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test
  ./pkg/util/kvcache -run '^TestGet$' -count=1` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test
  ./pkg/util/kvcache -count=1` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test -race
  ./pkg/util/kvcache -count=1` — passed (macOS linker warning only).
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-kvcache` — passed (8 integration tests and doc tests), including `Peek` ordering.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler
  DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib
  cargo +nightly-2026-08-22 check --offline --locked -p tidb-kvcache
  -p tidb-util -p tidb-stmtsummary -p tidb-session -p tidb-executor` — passed;
  existing warnings only.
- `cargo +nightly-2026-08-22 fmt --all -- --check` and `git diff --check` —
  passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.

The existing Go source and test files were updated without adding imports,
top-level tests, or Bazel targets, so `make bazel_prepare` is not required.

## Risk

- Correctness: the new read-only lookup preserves MRU state; all eight
  source-owned tests and every repository consumer compile.
- Compatibility: adds the current-master `Peek` API without changing `Get`,
  eviction, or callback behavior.
- Performance: `peek` is O(1) and does not relink the list.
