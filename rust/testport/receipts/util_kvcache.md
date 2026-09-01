# `pkg/util/kvcache` — complete Go-master package transcreation

Go source: `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02). The package is
unchanged from the prior extraction and byte-identical to the exact detached
latest-master worktree.

## Complete inventory

The package has exactly four artifacts (600 textual lines), all read in full:
`simple_lru.go` (238 lines), `simple_lru_test.go` (300 lines), `main_test.go`
(33 lines), and `BUILD.bazel` (29 lines). There is no package doc, README,
benchmark, generated or platform variant, fixture, `testdata`, fuzz target,
or ownership file. Go master's `SimpleLRUCache.Peek` is already present and
the package is byte-identical to the current authority.

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
front. Rust exposes `SimpleLruCache::peek(&self, ...)`, preserving the source's
byte identity and MRU order; the source-shaped `test_get` regression reads key
2 and verifies key 4 remains the newest. The eight Go-owned cases remain the
complete executable suite.

This authority refresh removed five Rust-only `#[must_use]` diagnostics from
`SimpleLruCache::new`, `peek`, `size`, `values`, and `keys`, plus the diagnostic
on the `tidb-util::kvcache::global_lru_memory_tracker` accessor. Go permits
callers to discard the corresponding function results (the global tracker is
initialized by package `init`). The focused deny-lint regressions failed with
five and one compiler errors before the edits and pass afterward.

## Validation

Profile: **Ready** for this focused parity fix within the continuing
package-by-package audit, not a repository-wide readiness claim.

- `git -c maintenance.auto=false -c gc.auto=0 fetch origin master --prune` —
  passed; `origin/master` is `c6054025ed4c32ab3672a2a24ea46892714d21ec`.
- `git diff --exit-code 5e8a1a229a7591ddac49a0cd3b795587c2595ab9..c6054025ed4c32ab3672a2a24ea46892714d21ec -- pkg/util/kvcache` — passed; no Go source drift.
- `git ls-tree -r -l c6054025ed4c32ab3672a2a24ea46892714d21ec pkg/util/kvcache` and full-file reads — passed; confirmed exactly the four listed artifacts.
- `cmp -s /tmp/tidb-go-latest-c605/pkg/util/kvcache/<file> pkg/util/kvcache/<file>` for all four artifacts — passed.
- Pre-fix focused Rust regressions failed with the expected five and one
  `unused_must_use` errors.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test
  ./pkg/util/kvcache -run '^TestGet$' -count=1` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test
  ./pkg/util/kvcache -count=1` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test -race
  ./pkg/util/kvcache -count=1` — passed (macOS linker warning only).
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib CARGO_INCREMENTAL=0 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-kvcache --test simple_lru_test -- --test-threads=1` — passed; nine tests including the return-contract regression.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib CARGO_INCREMENTAL=0 cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-util --lib kvcache::tests -- --test-threads=1` — passed; the global-tracker return-contract regression.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler
  DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib
  cargo +nightly-2026-08-22 check --offline --locked -p tidb-kvcache
  -p tidb-util -p tidb-stmtsummary -p tidb-session -p tidb-executor` — passed;
  existing warnings only.
- `cargo +nightly-2026-08-22 fmt --all -- --check` and `git diff --check` —
  passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.
- `git diff --check` — passed.

No Go, Bazel, module, or Cargo manifest file changed, so `make bazel_prepare`
was not required.

## Risk

- Correctness: the read-only lookup preserves MRU state; all eight source-owned
  cases, the return-contract regression, and every repository consumer compile.
- Compatibility: removes diagnostics only; `Get`, eviction, callback behavior,
  and the current-master `Peek` API are unchanged.
- Performance: `peek` remains O(1) and does not relink the list.
