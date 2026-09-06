# `pkg/ddl/serverstate` → `tidb-ddl-serverstate`

Historical pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.
Current Go source rechecked at `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`.

## Atomic inventory

| Artifact | Lines | Git blob | Rust owner |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 41 | `4614fe956675acf3aaf52a322cb489760aeae8f2` | workspace member and crate manifest |
| `mem_syncer.go` | 81 | `6489e99e057e11de34b51ca43cc0cb8ad2d60b00` | `MemSyncer` and process-global state |
| `syncer.go` | 210 | `f622c7567acda68c512c8b1ac15c0521d23bd299` | `StateInfo`, `EtcdSyncer`, retry/watch paths |
| `syncer_test.go` | 122 | `459a801fc4df763482e09f9824f5dec379fbaa35` | ignored live-etcd source test and helper |

The package has 454 Go lines and no additional generated, platform-specific,
fixture, benchmark, fuzz, example, or build-input artifacts. The Go tree is
byte-identical to the historical pin. The Rust owner is the complete
`rust/crates/tidb-ddl-serverstate` crate (`Cargo.toml`, 26 lines;
`src/lib.rs`, 1,021 lines after this regression), with the shared aggregate
test build script registering its inline tests. All Go declarations and Rust
public/private functions, callers, and the b110 `TestStateSyncerSimple`
mapping were read before editing.

## Behavior mapping

- `StateInfo` preserves Go JSON encoding, field-folding, duplicate-field
  updates, null handling, and malformed-input mutation rules.
- `EtcdSyncer` owns the etcd lease session, bounded get/put retries and delays,
  watch channel conversion, global-state cache, cancellation/deadline handling,
  metrics, and asynchronous rewatch behavior.
- `MemSyncer` preserves the process-global state, one-slot update notification,
  initialization contract, and failpoint-controlled no-notification path.
- `WatchChannel`, `WatchResponse`, and `WatchEvent` expose the Go watch surface;
  `etcd_syncer_watches_and_then_reloads_the_global_state` is the live-etcd
  carrier for Go `TestStateSyncerSimple` (`TIDB_ETCD_PROBE_PD`).

## Follow-up closure — discardable constructor returns (2026-09-06)

The three direct Go constructors (`NewStateInfo`, `NewEtcdSyncer`, and
`NewMemSyncer`) return values that callers may discard. Rust had added
`#[must_use]` to their `StateInfo::new`, `EtcdSyncer::new`, and `MemSyncer::new`
counterparts, creating three Rust-only compile errors under a deny-on-discard
caller. The annotations were removed without changing state initialization,
watch setup, etcd retries, JSON behavior, metrics, or synchronization.

The focused regression `tests::go_constructor_return_values_can_be_ignored`
invokes all three constructors under `#[deny(unused_must_use)]`. Before the
implementation edit it failed with exactly three diagnostics; after the edit
it passes.

## Ready validation

Rust-only validation was requested; no Go execution was performed. No Go,
Bazel, Cargo dependency, or module file changed, so `make bazel_prepare` was
not required.

```text
OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-ddl-serverstate --offline --locked go_constructor_return_values_can_be_ignored -- --nocapture
PASS; 1 passed, 0 failed.

OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-ddl-serverstate --offline --locked -- --test-threads=1
PASS; full owner test suite.

OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml -p tidb-ddl-serverstate --all-targets --offline --locked
PASS.

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PASS.

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint
PASS.

git diff --check
PASS.
```

The live-PD integration remains an explicit ignored gate requiring
`TIDB_ETCD_PROBE_PD`; it was not run in this Rust-only compile-contract
follow-up.
