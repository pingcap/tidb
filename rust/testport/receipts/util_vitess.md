# `pkg/util/vitess` — complete Go-master parity receipt

Comparison source: Go `origin/master` at
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01). The package is
unchanged from the earlier pinned implementation; this receipt records the
rolling authority and complete artifact hashes.

## Complete inventory

All four Go-master artifacts were read in full: `BUILD.bazel`, `main_test.go`,
`vitess_hash.go`, and `vitess_hash_test.go` (154 lines total). The package has
one production function, one source test with five vectors, one common
TestMain/goleak harness, and one Bazel library/test pair. It has no package
`doc.go`, README, fixture, benchmark, generated file, platform variant, or
ownership file.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 25 | `7874526ca1ada01488dc59de0a3a9067f64315b3` | `78eb27d202ef69e730934c032b89d14dcbed54589840045eb8776a4124701bef` | library/test targets |
| `main_test.go` | 33 | `1aaae6eba9bdce43f4aa7dc12e34398fbc329364` | `71af6d9e18621d88c257f653345ca0dc58f4c57fffa5516da1e45e328c668bb1` | TestMain/goleak setup |
| `vitess_hash.go` | 43 | `c2ccb1440811060e0ecd8d668e1d3eba694d7a1c` | `0486d95f1004def99ad658e1418710620899673ece932c015d5ba9ed217eb80c` | DES hash helper |
| `vitess_hash_test.go` | 53 | `7e6587b2007a911cd0cf376b51d1d8a2abd83d47` | `7fb1974bf1fc8799015da54db2603d4b84a9bac28fbe46f4e42b16a5f12292b3` | five-vector source test |

## Rust ownership and audit result

`rust/crates/tidb-util/src/vitess.rs` is the sole owner. `hash_uint64` performs
one DES block encryption over the big-endian input with an all-zero key and
decodes the ciphertext as big-endian, matching Go. Rust returns the value
directly because fixed-width DES block encryption cannot fail; Go's returned
error is always nil after its package initializer successfully creates the
fixed-width cipher.

The audit removed the Rust-only expanded package narrative, `must_use` API
policy, named null-key constant, supplemental boundary-vector test, and its
four non-source cases. Only the minimal module-export documentation required
by the Rust crate lint remains. The remaining test is the exact five-row
`TestVitessHash` translation.

## Validation and risk

Profile: **Ready** for this documentation-only authority refresh. No Go
source, imports, Bazel metadata, or module files changed, so `make
bazel_prepare` is not required. No source behavior changed and no new
regression test is added; the existing five-vector source test remains the
focused regression.

```text
git diff --exit-code 0bc44483e3e41a8ea917d4382dc202369468d200..origin/master \
  -- pkg/util/vitess
# passed: current package is unchanged from the previous authority pin

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/util/vitess -count=1
# passed (current worktree and exact detached Go-master worktree)

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml -p tidb-util --lib vitess::tests::test_vitess_hash --offline --locked -- --exact --test-threads=1
# passed: one source-derived five-vector test

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml -p tidb-expr --lib --offline --locked
# passed: production consumer (workspace warnings only)

cd rust && cargo +nightly-2026-08-22 fmt --all -- --check
# passed
git diff --check
# passed
```

No Go or Bazel file changed, so `make bazel_prepare` is not required. Full
workspace tests and Bazel execution remain outside this leaf receipt.

## Risk

- Correctness: the source's five known ciphertexts cover the exact algorithm,
  byte order, key, and maximum input.
- Compatibility: removes only Rust-only test/documentation policy; the public
  function and all production consumers are unchanged.
- Performance: production encryption and one-time cipher initialization are
  unchanged.
