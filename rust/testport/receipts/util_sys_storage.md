# Complete `pkg/util/sys/storage` package receipt

Status: Ready on the host target. Go remains the behavioral authority; Windows
and unsupported-target runtime execution remain unrun.

## Pinned inventory

Comparison source: Go commit
`e2788410d8d696605e8cb002585877a063ccc909`.

The complete package tree at that pin is:

    pkg/util/sys/storage/BUILD.bazel
    pkg/util/sys/storage/main_test.go
    pkg/util/sys/storage/sys_other.go
    pkg/util/sys/storage/sys_posix.go
    pkg/util/sys/storage/sys_test.go
    pkg/util/sys/storage/sys_windows.go

There is no `doc.go`, generated source, fixture, benchmark, or `testdata`
directory. `main_test.go` is a test setup/goleak harness and has no functional
behavior to reproduce.

## Go-to-Rust mapping

| Go artifact / contract | Rust evidence | Decision |
| --- | --- | --- |
| `sys_posix.go::GetTargetDirectoryCapacity` (`linux || darwin`) | `tidb_util::sys::storage::get_target_directory_capacity_impl`, `rustix::fs::statfs`, `f_bavail * f_bsize` | Direct behavior |
| `sys_windows.go::GetTargetDirectoryCapacity` (`windows`) | Same function's Windows target implementation, `fs4::available_space` (`GetDiskFreeSpaceExW` contract) | Direct platform behavior |
| `sys_other.go::GetTargetDirectoryCapacity` (other targets) | Same function's unsupported-target implementation returning `i64::MAX as u64` | Direct fallback behavior |
| `sys_test.go::TestGetTargetDirectoryCapacity` | `rust/crates/tidb-util/tests/sys_storage_source.rs::current_directory_has_positive_capacity` | Source test |
| `main_test.go::TestMain` | No Rust equivalent; Cargo owns test process setup and has no Go goroutine-leak harness | Harness-only omission |
| `BUILD.bazel` library/test targets and Windows dependency select | `tidb-util` module, `src/sys/mod.rs`, `Cargo.toml` target dependency, and Cargo integration test | Build mapping |
| Supported POSIX `Statfs` error propagation and exact block arithmetic | `missing_directory_returns_the_operating_system_error`, `uses_statfs_available_bytes` | Focused regressions |

The previous Rust module contained two additional tests for a missing path and
the unsupported-platform constant in the production module. They were Rust-only
test policy, not Go package artifacts; the source-shaped carrier now owns the
single Go functional test plus focused arithmetic and error-boundary
regressions.

## Integration boundary

The Go startup consumer is `cmd/tidb-server/main.go::checkTempStorageQuota`.
The Rust equivalent is the startup path
`tidb-server::open_spill_storage -> tidb_util::disk::SpillStorage::open`,
which calls the shared helper for a non-negative spill quota and retains the
existing `SpillStorageOpenError` wrapper around operating-system failures.
The spill-storage owner is the only Rust production implementation/call site;
no duplicate capacity implementation remains.

## Validation

Commands run from the repository root:

    git ls-tree -r --name-only e2788410d8d696605e8cb002585877a063ccc909 pkg/util/sys/storage
    cargo fmt --manifest-path rust/Cargo.toml --all -- --check
    cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
    cargo test --manifest-path rust/Cargo.toml --locked -p tidb-util --test sys_storage_source
    OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --locked -p tidb-util --test sys_storage_source
    OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --locked -p tidb-util --lib sys::storage
    cargo metadata --manifest-path rust/Cargo.toml --locked --offline --no-deps
    git diff --check
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint

Supplementary source-module check (outside the workspace, using only cached
dependencies):

    cargo +nightly-2026-08-22 test --offline
    # temporary crate; exact repository storage.rs included by absolute path

Observed results on 2026-09-01:

* `git ls-tree ... pkg/util/sys/storage` listed exactly the six pinned Go
  artifacts above.
* Both the default formatter and the pinned nightly formatter passed.
* Offline locked Cargo metadata and `git diff --check` passed.
* The pinned nightly package test passed all three focused tests with the
  command-local bundled OpenSSL tree.
* The pinned nightly library filter passed with zero remaining in-module
  `sys::storage` tests (501 unrelated tests filtered out).
* `make lint` passed end-to-end using the command-local Go 1.25.10 toolchain
  and GOPATH.
* The isolated offline harness also compiled the corrected `storage.rs` source
  module with cached `rustix` 1.1.4 and `tempfile` 3.27.0; all three focused
  tests passed.

The default `cargo test` invocation remains unsuitable on this host because
the active rustc is 1.95.0, below the workspace minimum 1.97; the repository's
pinned nightly command above is the Ready validation command.

The Bazel preparation gate was not required: this batch adds no Go files,
changes no Go imports or tests, and does not touch Bazel metadata or module
dependencies.

The supplementary harness is source-level corroboration. Windows and
unsupported-target execution remain unverified on this host.

## Risks and unverified targets

The POSIX implementation is directly exercised on the host. Windows and an
unsupported Go target are compile-mapped but are not executable on this host;
their target-selected code paths are not claimed as runtime-tested. The
filesystem API is inherently dependent on the host's available capacity, so
the source assertion intentionally checks only the Go contract's positive
lower bound.
