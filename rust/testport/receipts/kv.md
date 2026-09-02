# `pkg/kv` — Go-master parity receipt

Comparison source: Go `origin/master` at commit
`c6054025ed4c32ab3672a2a24ea46892714d21ec` (2026-09-02).

## Complete Go inventory

The package contains exactly 30 tracked artifacts and 5,145 lines. Every
production file, test, and Bazel target was read before editing. There is no
package `doc.go`, benchmark, fixture, generated Go source, or
platform-specific Go variant.

| Artifact | Lines | Git blob | SHA-256 |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 116 | `cb8d9b3cbb9501ca989b55c453773297918a58dd` | `43473fed5636d0fa957df553084e1f3f22fc51d4100e91fa5084899e7e652807` |
| `assertion.go` | 48 | `734df841bc62ca8acab6b4fb7fac35ae276052df` | `1b8f5b85ee599712bbb4672e9b893f8589bf972ad74bf4a1a505cebada423e00` |
| `cachedb.go` | 99 | `ac13955e4f39998917ec170f65309d9830f46fe3` | `76d7eba5ee6044c9ccc1fcfadfb1a8317629cb7adc84bbf5658829fb418d9c17` |
| `checker.go` | 70 | `cd8436dd356a84e653bb5f3ad31c9ca4130c8a3b` | `09c8744c19f81d6b66ccc162d666247a271ca76af5b46f68dbab9578de4c3323` |
| `checker_test.go` | 35 | `8e2dc9de2f3aacb7c475bd0cb2cd951858ae6614` | `d5368154d23f386aabdccc123a5f01d1fc27f85355ab6da06b94b2386b76bba0` |
| `error.go` | 101 | `b09326477bee3085236c84460db1e9fd780dc6f0` | `ade076091766dcf81339dbf6e577682942af38ddabdb4502e7a8a9cf6ba84936` |
| `error_test.go` | 43 | `08c2bde9e9c62b1284f9fb0ed7f85c097a0de8f4` | `897f0d7ac33ac935502bbe4b1fd1bbd513f63f20628281690c20160cfb9197ca` |
| `fault_injection.go` | 138 | `7716a9c3a874f1ea3752f30d4f9a2e516c7d100b` | `6ed3c1543a894ce924f42124c66dcf5040934208b522617c2db432dd2807de3c` |
| `fault_injection_test.go` | 95 | `e3a92e8c60bd29d8855f33a9b46656bb3d22eb2f` | `e1b317802242296a20583f87f91d17890bb4a01e83ef70354316a703b3eec879` |
| `interface_mock_test.go` | 325 | `e0f0d38a465e20f98ca19e7c4ee2eef092472828` | `6bdbcdc064017217af530751276dc81f533f9bf748c65e8adade505d8c7175f2` |
| `iter.go` | 28 | `13bf851b06117e9f08c7371a65793a2fbbd9b412` | `0c589106f9d4a900e7353498f4c6186f7ad97b05490d90861a636022201159e1` |
| `key.go` | 764 | `490e1e7aea65e97c8f67032c8adb2d37d5c3da0e` | `2f8dad6f5b54df5e90d230dddc8d336bfff78b35641bcd20b01395ad5d8d15b1` |
| `key_test.go` | 479 | `23a42443391c21484eb31db2ee3ee70f1971bdcb` | `5e26bc3afa5beb4883546ab34eec76d481c5512d9caf7cbbe6d142ebe9e449c9` |
| `keyflags.go` | 105 | `3ac8b666d369b4d742c05fec563f5f845e87e669` | `9b16a0046d47e739dfe28aef7efd1e439d46ae5bd8d5b745923a3541df27915b` |
| `kv.go` | 932 | `4333f345f6280e8c4da6ec15bc3313f8856612cc` | `5b9becb897b2bf97bc85c30732ee1d6cde6eaf99a5d683c695928b79ecd5c919` |
| `kv_test.go` | 86 | `f944c0f0e271956aee40de459374dbd538dd014d` | `c993f81f56b03940c7af585b7cce0460fefe27efed4fd1a030d76865e9d3c7dc` |
| `main_test.go` | 36 | `d62d0f9f7bb83a0313edac95e4f1f3eb4e0abf9f` | `fa97a86ce3a5d763c4c27d62dc45d2a77313e61c480bf84890f1d0cdb5549737` |
| `mock_test.go` | 88 | `10db70da8ebbce5dc4ecdb327b36a06327de65eb` | `55c967418dd78f68404ae7eb590b86e65ea1971969ce7c9db2ce2eb64768821f` |
| `mpp.go` | 265 | `f56738aab639d33507e2cbe5a5d22444c841d78f` | `ad57eaaabf5bddd07132caece41414db02908d509df7b9faf8caabfdc79d1c47` |
| `option.go` | 296 | `e3401865720474d73bd867a6eaaa243cf87832aa` | `c1879ec5e3e733c44e36e622f35c97c2f4fcceffa7cf817fd16b613530303558` |
| `option_test.go` | 111 | `922a1bc00f348c642a3479117c80561bc61ac9c6` | `c4276ab38599a43873ad1b7fa4e953a400419680ce21a5b40ebfc6b513d7177f` |
| `txn.go` | 246 | `848e7cd40d3f3009fb63050028c60bb2b02f4b15` | `706b23ece00d35e068cb35fb12c7ec63dff67fb470fa7a53e27a6960690ff222` |
| `txn_scope_var.go` | 76 | `34d0a1a2a9291b2ec8d599ba424278a2fcf8eb5d` | `b165536242490032fbfdeea8ec9fd7cb307571b9972154b4782744be4ecf9c95` |
| `txn_test.go` | 107 | `9da697f3efa155dee831a3b966178e6d889a3c84` | `27c2e49606f2d2cd4c2f20b07903123fdd57300b6556ca92dd589476d00d80e1` |
| `unistore.go` | 21 | `206051ddbc3d66967fe1f51a9ef6c10e1e1b3f43` | `ce9c267c70c1812bee9ddbb59d3760f6429715edfabd145442f7dc279ebff664` |
| `utils.go` | 98 | `20b2be14516f80e13ad5c61e31374c82ba27ba10` | `1384ac4d13a368b70f109e5a551a99d84d1c9fb18b8d571d68c770d8df13f5aa` |
| `utils_test.go` | 160 | `7680a5aa8f4ab54a6309a599f216e6dbda3156d0` | `1959694492ce7f1c9173003c43274211a41eca9d936a8c2246e9443f6c7ba8d2` |
| `variables.go` | 27 | `f0c5b4726820bf84b0c14eb40840f39efcec7ac4` | `d6dcae728e13d3fccc71eeb21257f6551a79985d599fe8aff4d6f1ebcdb5931e` |
| `version.go` | 52 | `137b47d32995a435cd81f09efdc828e7086cf7b1` | `8cf23d6218d0049ea167d889771fb79b8109acbeb1dd9af2e43e8b8638ab4474` |
| `version_test.go` | 98 | `019179424522b8dfe28ac68d4adc620f59deea69` | `6a4cdade17651ee24fac715942afa843964b0eb0f7eb420350474a4776ef1361` |

The production surface includes the root KV interfaces and request constants,
key/range/handle types, key flags and assertions, transaction options and
scope/source helpers, MPP contracts, cache and fault-injection wrappers,
retry and inner-transaction helpers, version and UniStore variables, and
numeric/meta utilities. Tests cover request support, SQL error identities,
fault injection, interface mocks, key/range/handle behavior, transaction
retry and inner-TXN timestamps, source bitfields, numeric counters, keyspace
classification, versions, and MPP compression settings. The Bazel target
contains one public library and a 27-shard test target.

## Rust ownership and decision

The Rust dependency-closed owner is split across `rust/crates/tidb-txnkv`:

- `key.rs`, `key_flags.rs`, `assertion.rs`, `error.rs`, `checker.rs`,
  `version.rs`, `option.rs`, `txn_scope.rs`, `mpp.rs`, `unistore.rs`, and
  `variables.rs` carry the leaf contracts.
- `kv_api.rs` and `kv_contract.rs` carry the root interfaces/data contracts;
  `cache_db.rs`, `fault_injection.rs`, `iteration.rs`, `inner_txn.rs`, and
  `new_txn.rs` carry their helpers.
- `driver`, `region`, `lock`, `transaction`, and `rpc` carry the live TiKV
  transaction and transport dependencies that the Go interfaces delegate to.
- `tests/kv_package_source.rs` is the source-derived package suite; the
  remaining `tidb-txnkv` source tests cover the dependency-closed RPC,
  region, lock, and transaction surfaces.

This is an explicit **SEED/boundary** receipt, not a claim that the complete
Go `pkg/kv` package has been transcreated. The Rust owner intentionally keeps
the SQL/session seam, TLS, several client-go transaction options, and some
MPP/storage implementations outside this package boundary. No Rust-only
production behavior was integrated for those missing contracts.

This batch did close two concrete owner defects:

1. `BatchCommandTag::ALL` is now generated in field-number order, so the
   source-derived opaque-wire round-trip test compiles and exercises every
   pinned command tag.
2. `BatchCoprocessorPending` resolves its publication receipt only for a
   successful response. A terminal transport/protocol error that occurs
   before admission no longer becomes a misleading “missing publication”
   error. The focused regression covers both nonblocking and blocking pulls.

The scheduler source test now pins its concrete payload/completion types,
which removes inference-only test wiring from the package gate.

The hparser branch was still missing three Go-master KV contracts. The Go
checker now accepts `tipb.ExprType_MaxCount` and `tipb.ExprType_MinCount` for
SELECT/INDEX pushdown, and `kv.Request` carries the
`AllowBatchTaskDataMerge` and `ExecuteBatchTasksSerially` controls already
consumed by the Rust distsql owner. Go regressions cover the two expression
types and both request flags; the pre-fix run failed to compile because the
request fields were absent.

## Validation

Profile: **Ready** for this package batch; the repository-wide package loop
remains in progress.

```text
OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline \
  -p tidb-txnkv --test all batch -- --test-threads=1
# 76 passed, 0 failed

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline \
  -p tidb-txnkv --test all batch_coprocessor_dispatch_source -- --test-threads=1
# 11 passed, 0 failed (includes the new blocking pre-admission regression)

RUST_MIN_STACK=67108864 OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline \
  -p tidb-txnkv --test all -- --test-threads=1
# 407 passed, 11 ignored, 0 failed

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
# passed

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test ./pkg/kv -run 'Test(IsRequestTypeSupported|RequestBatchTaskFlags)$' -count=1
# passed after the fix; pre-fix compile failed on the missing Request fields
tools/check/failpoint-go-test.sh ./pkg/kv -count=1
# passed with failpoint cleanup

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler \
DYLD_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib \
cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --workspace --offline --locked
# passed; existing warnings only

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
make lint
# passed
```

Go production and test sources changed, so `make bazel_prepare` was required
and attempted; it is blocked locally because the `bazel` executable is not
installed. No Bazel metadata could be regenerated. The full Rust owner test binary needs the
larger `RUST_MIN_STACK` setting for one existing mixed-mutation test; with it,
the suite passes.

## Risk and remaining boundaries

- Correctness: the fixed error path now preserves the original typed terminal
  transport/protocol error and never invents a route identity.
- Compatibility: `BatchCommandTag::ALL` is an additive testable enumeration;
  the package remains a boundary until the omitted SQL/session and transport
  options are owned together.
- Performance: no hot-path allocation or scheduling policy changed; only the
  already-completed-result branch avoids a receipt lookup.
- Not verified locally: external etcd/live TiKV integration, Bazel analysis,
  the Go real-cluster paths, and the omitted full `pkg/kv` client integrations.
