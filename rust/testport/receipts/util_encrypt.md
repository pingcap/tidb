# `pkg/util/encrypt` — complete Go-master package parity receipt

Comparison source: Go `origin/master` at commit
`f2c346fe4f368ff855e17c1f62e28a89ba7f9723`. The package is byte-identical to
the current checkout at that authority.

## Complete inventory

The package has exactly eight tracked artifacts, all read in full:

- `aes.go` — ECB, CBC, OFB, CTR, and CFB encryption/decryption, PKCS#7
  padding, and MySQL key derivation;
- `aes_layer.go` — random AES-CTR block geometry, buffered writer, and
  positional reader;
- `crypt.go` — the legacy MySQL `ENCODE`/`DECODE` codec;
- `aes_test.go` — 14 source tests covering padding, every AES mode, and key
  derivation;
- `aes_layer_test.go` — `TestReadAt` and its three benchmark helpers/cases;
- `crypt_test.go` — `TestSQLDecode` and `TestSQLEncode` with ten vectors;
- `main_test.go` — common setup and goleak test harness;
- `BUILD.bazel` — one library and one flaky short test target.

There is no `doc.go`, README, fixture/testdata directory, generated input or
output, platform/build-tag variant, fuzz target, or additional ownership
artifact. The package's source test surface is 17 Go test functions plus the
`BenchmarkReadAt` benchmark.

## Rust owner and alignment

`rust/crates/tidb-util/src/encrypt/{mod.rs,aes.rs,aes_layer.rs,crypt.rs}` owns
all three production files. `rust/crates/tidb-util/benches/encrypt.rs` carries
the source random-access benchmark, and the inline Rust suites retain the 17
source test identities. The Go package's common test harness has no Rust
runtime-worker analogue.

The audit removed ten explicit Rust-only `#[must_use]` diagnostics from the
Go-shaped API: `pkcs7_pad`, `derive_key_mysql`, `sql_decode`, `sql_encode`,
`Writer::{new,available_size,buffered,get_cache,get_cache_data_offset}`, and
`Reader::new`. Go permits every one of these return values to be discarded;
the regression `return_values_may_be_ignored_like_go` proves that contract
under `#[deny(unused_must_use)]`. No Rust-native helper or behavior was added.

## Validation

Profile: **Ready** for this package batch. No Go or Bazel source changed, so
`make bazel_prepare` is not required.

- Detached pre-fix owner with the focused regression failed with exactly ten
  `unused_must_use` diagnostics.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10
  TMPDIR=/tmp/tidb-codex go test ./pkg/util/encrypt -count=1` — passed.
- `OPENSSL_DIR="/Users/chenhuansheng/Documents/GitHub/tidb/rust/target/debug/build/openssl-sys/332dd69a952932bb/out/openssl-build/install" OPENSSL_STATIC=1 cargo +nightly-2026-08-22 test --offline --locked --manifest-path rust/Cargo.toml -p tidb-util --lib encrypt::aes_layer::tests::return_values_may_be_ignored_like_go -- --exact --nocapture` — passed.
- The same locked toolchain with `-p tidb-util --lib 'encrypt::' --
  --test-threads=1` — 18 owner tests passed (17 source tests plus the
  regression).
- The same locked toolchain with `cargo check ... -p tidb-util --all-targets`
  — passed, including the benchmark target.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml -p tidb-util
  -- --check` and `git diff --check` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH
  GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10
  TMPDIR=/tmp/tidb-codex make lint` — required Ready repository lint; recorded
  after the package change is complete.

## Risk

- Correctness: return-value discard behavior now matches Go while all source
  encryption, padding, codec, and random-access tests pass.
- Compatibility: only Rust lint policy changed; no public function or cipher
  behavior changed.
- Performance: no runtime path changed; the regression is test-only.
- Not verified locally: non-host platform builds and live encrypted spill
  integration beyond the package owner/all-target compilation.
