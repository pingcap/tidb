# `pkg/util/tikvutil` — complete Go-master parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01). The package is
unchanged from the earlier pinned audit; this receipt refreshes the rolling
master authority and records the complete artifact hashes.

## Complete inventory

The package contains two tracked artifacts and 31 lines. Both artifacts were
read in full before this update. There is no package doc, test, test harness,
benchmark, fixture, generated input/output, platform variant, README, or
ownership file.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 9 | `e8391be50a080baf413a59348f98238cc170f1ad` | `33c974d1426e69ad9951cf3bfcb3b09eff759362ff1c03e07306e4d683c731f8` | public Go utility library target |
| `tikvutil.go` | 22 | `b19d642214d47385dd1da6dd6b9a088cd1aa0960` | `d7ed4273a9648aa17c5f018237bb9ae794894184bd2a62321a9e7f4a289babce` | process-wide atomic committer-concurrency setting |

`CommitterConcurrency` is one public sequentially consistent signed 32-bit
atomic initialized to 128. The three source consumers load it into TiKV client
configuration and use it as the GLOBAL `tidb_committer_concurrency` sysvar's
set/get authority; validation clamps values to the Go range 1–10,000.

## Rust ownership and parity

`rust/crates/tidb-tikvutil/src/lib.rs` owns the source-shaped public
`COMMITTER_CONCURRENCY: AtomicI32`, initialized to 128. The config and session
sysvar consumers load/store that single atomic with sequential consistency,
including cluster-table publication and reset. Earlier Rust-only wrapper
getters/setters and a private duplicate atomic were removed; no Rust-only
behavior remains in the current owner, and the Go-visible value width,
initialization, ordering, and publication semantics match.

## Validation and risk

Profile: **Ready** for this documentation-only authority refresh. No Go source,
imports, Bazel metadata, or module files changed, so `make bazel_prepare` is
not required. No source behavior changed and no regression test is added;
the package has no source test and this batch only refreshes authority data.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/tikvutil -count=1
# passed: package compiled; no test files

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml -p tidb-tikvutil -p tidb-config -p tidb-session --offline --locked
# passed for the owner and all three consumers (workspace warnings only)

cd rust && cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
```

Not verified here: full workspace tests, Bazel execution, or real TiKV commit
throughput. Existing unrelated session/planner worktree changes remain outside
this receipt.
