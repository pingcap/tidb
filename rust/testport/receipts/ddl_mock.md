# `pkg/ddl/mock` → `tidb-ddl-mock`

Historical pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.
Current Go source rechecked at `f2c346fe4f368ff855e17c1f62e28a89ba7f9723`.

## Atomic inventory

| Artifact | Lines | Git blob | Rust owner |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 16 | `bde7b92a40f6d9b31e45661748bb1925aef3d394` | workspace test-double target |
| `schema_loader_mock.go` | 58 | `faa595e7e337fcd397bdb5babdb6d99500fc61bd` | `MockSchemaLoader` and recorder |
| `systable_manager_mock.go` | 122 | `5ccaedcdc6a32ede16abf6621648d201381402c0` | `MockManager` and recorder |

The package has 196 Go lines. Both Go sources are MockGen-generated artifacts;
the package has no handwritten production file, Go test, fixture, benchmark,
fuzz target, example, platform variant, generated input, or additional build
artifact. The Go tree is byte-identical to the historical pin. The complete
Rust owner is `rust/crates/tidb-ddl-mock` (`Cargo.toml`, 15 lines;
`src/lib.rs`, 326 lines), registered in the workspace and lockfile. Its
schema-loader and system-table-manager callers, trait implementations, and
all three inline tests were read before editing.

## Behavior mapping

- `MockSchemaLoader::new`, `expect`, `is_mock`, `reload`, and `verify` mirror
  the generated `NewMockSchemaLoader`, `EXPECT`, `ISGOMOCK`, method dispatch,
  and controller-finish behavior with deterministic queued callbacks.
- `MockManager::new`, `expect`, `is_mock`, `get_job_by_id`,
  `get_job_bytes_by_id_with_session`, `get_mdl_version`, `get_min_job_id`,
  `has_flashback_cluster_job`, and `verify` mirror every generated manager
  method and recorder entry, including missing/unexpected-call checks.
- The existing scheduler and unsynced-job tests exercise the native mock
  contracts and their direct `tidb-exec` consumers.

## Follow-up closure — discardable generated-mock returns (2026-09-06)

Go permits callers to discard the results of generated `NewMockSchemaLoader`,
`NewMockManager`, and `EXPECT`. Rust had marked the four direct counterparts
(`MockSchemaLoader::new`, `MockSchemaLoader::expect`, `MockManager::new`, and
`MockManager::expect`) `#[must_use]`, creating four Rust-only compile errors
under a deny-on-discard caller. The annotations were removed without changing
queued callback behavior, drop verification, trait dispatch, or scheduler
semantics.

The focused regression `tests::go_mock_constructor_and_expect_returns_can_be_ignored`
invokes all four APIs under `#[deny(unused_must_use)]`. Before the source edit
it failed with exactly four diagnostics; after the edit it passes.

## Ready validation

Rust-only validation was requested; no Go execution was performed. No Go,
Bazel, Cargo dependency, or module file changed, so `make bazel_prepare` was
not required.

```text
OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-ddl-mock --offline --locked go_mock_constructor_and_expect_returns_can_be_ignored -- --nocapture
PASS; 1 passed, 0 failed.

OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-ddl-mock --offline --locked -- --test-threads=1
PASS; 3 unit tests passed, 0 failed; doc tests had 0 tests.

OPENSSL_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys-e4f1dd7465974733/out/openssl-build/install OPENSSL_STATIC=1 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb/rust/target cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml -p tidb-ddl-mock --all-targets --offline --locked
PASS.

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PASS.

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint
PASS.

git diff --check
PASS.
```

The package contains no live integration gate; its native scheduler and
unsynced-job tests remain the executable runtime coverage.
