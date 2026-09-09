# Rust `tidb-util` master-key nonce boundary receipt

Status: bounded Rust-only alignment batch; this receipt does not claim the
entire `br/pkg/encryption/master_key` transcreation is complete.

Comparison source: Go `origin/master` at
`a85e0fd5dfa914e73eed97f17af584061252bc3c` (2026-09-02). The relevant
standard-library contract is `br/pkg/encryption/master_key/mem_backend.go`:
`cipher.NewGCM(block)` is followed by `AEAD.Seal`/`AEAD.Open`, whose nonce
argument must be exactly `NonceSize()` bytes or the Go implementation panics.

## Complete package inventory

Before editing, every tracked artifact in the direct Go package was enumerated
and read from the fetched tree: 11 artifacts and 943 lines:

- production: `common.go`, `file_backend.go`, `kms_backend.go`,
  `master_key.go`, `mem_backend.go`, `multi_master_key_backend.go`;
- tests: `file_backend_test.go`, `kms_backend_test.go`, `mem_backend_test.go`,
  `multi_master_key_backend_test.go` (11 upstream test functions);
- build metadata: `BUILD.bazel` (including the 11-shard test target).

The direct Go package has no generated source, platform-specific variant, or
fixture directory. Nested dependencies `br/pkg/kms`, `br/pkg/utils`, and
`encryptionpb` remain their own package/build boundaries.

The Rust owner is the `master_key` module under `rust/crates/tidb-util`: all 9
tracked owner files were read before editing (2,117 lines: `common.rs`,
`file_backend.rs`, `gcm.rs`, `kms.rs`, `kms_backend.rs`, `mem_backend.rs`,
`mod.rs`, `multi_master_key_backend.rs`, and `pb.rs`) and its 33 in-module
tests were inventoried. The containing `tidb-util` crate has 195 tracked
artifacts and 74,039 lines: 166 `src` files, 12 standalone tests, 15 benches,
`Cargo.toml`, and `build.rs`. Its source-level fixtures are outside this
module (`dbterror` text fixtures and standalone semantic TOMLs); its only
platform-named implementation is `src/sys/linux.rs`, with Windows behavior
selected through Cargo target dependencies. `build.rs` emits compiler identity
environment values and no generated master-key source. No master-key file is
generated or platform-specific.

## Alignment

The Rust GCM mode previously returned `GcmError::InvalidNonceLength` from its
`seal` and `open` methods. That was Rust-only behavior: Go's AEAD methods panic
when given a nonce of any length other than 12 bytes. Rust now performs the
same explicit panic with Go's diagnostic text before constructing `J0` and
removes the error variant that no longer exists. Authentication failures
remain a `Result` error, matching Go's `cipher: message authentication failed`
path. The in-memory backend consequently panics when handed the 16-byte CTR IV
that `NewIVFromSlice` accepts, just as Go does when `EncryptContent` reaches
`AEAD.Seal`.

Focused regressions now catch panics from both low-level `Aes256Gcm::seal` /
`open` and `MemAesGcmBackend::encrypt_content`; the old assertions expecting
`InvalidNonceLength` or a returned error would fail against the corrected
source.

The low-level regression was applied to a detached pre-fix `2990ecfcc7` tree
and failed as expected (`Seal must panic on a non-standard nonce`, exit 101),
then passed on the corrected source.

## Validation

Profile: Ready for this bounded Rust package batch.

- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-util --lib master_key::gcm::tests::test_nonce_length_panics_like_go -- --nocapture` — low-level Seal/Open panic regression passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-util --lib master_key::mem_backend::tests::test_ctr_iv_panics_like_go -- --nocapture` — backend boundary regression passed.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --offline --locked -p tidb-util --lib -- --test-threads=1` — 534 passed, 0 failed, and 2 ignored out of 536 crate-library tests.
- `OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 check --offline --locked -p tidb-util --all-targets` — passed.
- `cargo +nightly-2026-08-22 fmt --all -- --check`, `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint`, and `git diff --check` are the remaining Ready gates for the committed tree.

## Risks and boundaries

- Correctness: malformed nonce lengths now fail loudly rather than being
  converted into a recoverable error, preserving the Go crypto contract.
- Compatibility: `GcmError::InvalidNonceLength` is removed; no current Rust
  call site outside this owner referenced it. Valid encryption/decryption and
  authentication errors retain their existing result shapes.
- Security: this change does not alter AES/GCM arithmetic or tag comparison;
  it only restores the standard-library panic boundary.
- The cloud SDK implementations, protobuf codec, retry framework, and the
  other 186 artifacts in `tidb-util` remain explicit package boundaries.
