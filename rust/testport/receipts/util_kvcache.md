# `pkg/util/kvcache` — complete Go-master package transcreation

Go source: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

The package has exactly four artifacts, all read in full: `simple_lru.go`,
`simple_lru_test.go`, `main_test.go`, and `BUILD.bazel`. There is no package
doc, README, benchmark, generated or platform variant, or ownership file.
The production file is unchanged from the prior pin; Go master adds
`SimpleLRUCache.Peek` and folds its no-promotion ordering assertion into
`TestGet`.

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
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test
  ./pkg/util/kvcache -run '^(TestPut|TestZeroQuota|TestOOMGuard|TestGet|TestDelete|TestDeleteAll|TestValues|TestPutProfileName)$' -count=1` — passed on the checkout's pre-`Peek` Go source.
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

No Go or Bazel file changed, so `make bazel_prepare` is not required.
The current checkout's Go test predates the master-only `Peek` assertion; the
origin/master test was read in full but not executed from an alternate
worktree.

## Risk

- Correctness: the new read-only lookup preserves MRU state; all eight
  source-owned tests and every repository consumer compile.
- Compatibility: adds the current-master `peek` API without changing `get`,
  eviction, or callback behavior.
- Performance: `peek` is O(1) and does not relink the list.
