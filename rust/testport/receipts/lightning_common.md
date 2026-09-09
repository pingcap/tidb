# `pkg/lightning/common` — complete package parity receipt

Pinned Go source: `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`
(`origin/master`).

## Complete inventory

The package has exactly 24 tracked artifacts and 3,875 lines. Every BUILD,
production, platform, test, benchmark, and support line was read in full from
the pinned Go source. The hparser branch is byte-identical to Go master for
this package.

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 156 | `fdd9ced397527f2c2aab1b80beceba448c75047b` | public library, platform select, and 30-shard flaky test target; no Rust build input |
| `common.go` | 145 | `b045fba8872d8f0eba81660ed6e1780af07ea9ca` | auto-ID allocator discovery/rebase and Lightning defaults; no Rust owner |
| `common_test.go` | 228 | `b0fcbf5f1235773f582bf79022f172b8f1cc68d5` | allocator construction/rebase tests with mock TiKV metadata |
| `conn.go` | 119 | `e0413e9b4bc7340b9a6751db2c8331fd2935b062` | lazy, round-robin gRPC connection pools; no Rust owner |
| `dupdetect.go` | 151 | `a5b654fcfa1294ae319655725d10147c6625f4ce` | duplicate-key detector, Pebble batch flushing, and key/value iterator contract; no Rust owner |
| `errors.go` | 233 | `4ee394e7302cc68bb7240933e04dfc117df6cbc6` | Lightning/BR error normalization and stack preservation; no Rust owner |
| `errors_test.go` | 117 | `e1ff5df584a97819349cac1b0c6e3eed9de5c351` | three error identity, wrapping, stack, and redaction tests |
| `key_adapter.go` | 118 | `0cb6c86f7438fdf55df77ebd516f62e54b1a8709` | no-op and duplicate-detection mem-comparable key adapters; no Rust owner |
| `key_adapter_test.go` | 212 | `a3d75c0944e0ee8ad3c3acbf8c79ac354b08a0e1` | eight encode/decode, ordering, buffer reuse, and minimum-row-ID tests |
| `main_test.go` | 36 | `c91ae4fa8a4eda728d1b9bfe9be4dcdf69e13dc8` | TestMain common setup and goleak allowlist |
| `once_error.go` | 47 | `50e110842724b82751625374c51405dd1aa152d0` | mutex-protected first-error holder; no Rust owner |
| `once_error_test.go` | 51 | `30b7f0e293f11554ef30177603ea6d668b077016` | first-error and concurrent nil-assignment test |
| `pause.go` | 157 | `4cbb3a39cae9583cfcc4480849bf0f58a88557fa` | atomic pause/resume gate with cancellable waiters; no Rust owner |
| `pause_test.go` | 173 | `ba1d5d96e7d99a011e7ae8984b798252b9809f3f` | pause/resume/cancellation test plus three wait benchmarks |
| `retry.go` | 223 | `c9e8c10445070a837f5ec2a02a3abc5912507e70` | transient-error classification and gRPC/HTTP/SQL retry policy; no Rust owner |
| `retry_test.go` | 180 | `abd1796187891dfaa85f1a7c18ecaee8c6931e29` | retry matrix covering network, driver, TiKV, gRPC, SQL, and rate errors |
| `security.go` | 164 | `99abd68c6c80eb967e43d352a57bd368ccf868cf` | TLS construction, HTTP JSON, gRPC/PD/TiKV security conversion; no Rust owner |
| `security_test.go` | 146 | `e317b4385cd99ecd35a57cb8ba3c5f40ea688742` | four insecure/secure HTTP, host replacement, and invalid-TLS tests |
| `storage.go` | 24 | `ae690184317197a5c09f7edbc312e6bfa0fd212d` | shared `StorageSize` value; no Rust owner |
| `storage_test.go` | 31 | `b49ce7e47dad9aec8fe9e1a37ab6e290baea5a84` | storage-capacity smoke test |
| `storage_unix.go` | 79 | `a19dfb7c0ee983c0a29a7b45f48be869989f5a13` | non-Windows `statfs` capacity and device comparison, with `GetStorageSize` failpoint |
| `storage_windows.go` | 56 | `89b9483592f94130689fc4c6b55fc0d90e69b1af` | Windows `GetDiskFreeSpaceExW` variant and unsupported `SameDisk` stub, with failpoint |
| `util.go` | 722 | `1ecdbccc1b6b3bab62a405e6320c50d0ab4ec662` | MySQL connections/retries, SQL identifier/string builders, auto-ID/index helpers, and capability probes; no Rust owner |
| `util_test.go` | 307 | `480299022ed8c616472719fd1ee16f0a269a29f5` | nine directory, connection, retry, SQL-builder, schema, index, and row-count tests |

The 13 production files contain 94 function/method declarations (the Unix
and Windows files each provide the platform pair). The ten test/support files
contain 31 `TestXxx` functions (30 behavior tests plus `TestMain`) and three
benchmarks. The two logical production failpoints are `GetStorageSize` (one
implementation in each platform variant) and `MustMySQLPassword`; the test
suite enables the latter and the wrapper enables all generated failpoint
bindings. There are no fuzz corpora, fixtures, testdata directories, package
docs, generated source inputs, or artifacts beyond the BUILD/platform files.

Behavior spans auto-ID allocator compatibility (including separated
auto-increment and auto-random modes), lazy gRPC pooling, duplicate KV
capture, error-code normalization, memory-reusing key encoding, atomic pause
gates, exhaustive transient-error policy, TLS/HTTP/PD/TiKV adapters, Unix and
Windows disk accounting, MySQL retry transactions, identifier/string quoting,
index DDL generation, and table row-count safety decisions.

## Rust ownership and parity result

No Rust crate owns the dependency-closed `pkg/lightning/common` package.
Searches found no Rust owner or call site for its allocator bridge, gRPC pool,
Pebble duplicate detector, Lightning error-code catalog, key adapters, pause
gate, retry matrix, TLS wrapper, platform storage probes, SQL-with-retry
executor, or table/index helpers. Existing Rust Lightning crates implement
other packages (logging, metrics, duplicate resolution, verification, worker,
and import definition) and cannot substitute for these shared Go contracts.

No Rust-only behavior was found to remove, and no speculative Rust common
facade or ignored source carrier was added. A correct port must move the
concrete TiDB/TiKV/PD clients, Pebble storage, auto-ID service, SQL executor,
table metadata, platform disk APIs, and all downstream Lightning consumers as
one dependency-closed unit.

## Validation

Profile: Ready for this documentation-only boundary update; no Go or Rust
source changed. The full current-branch package suite was run with the
repository failpoint wrapper after a transient first invocation raced with
failpoint binding generation:

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh ./pkg/lightning/common -count=1
PASS
ok   github.com/pingcap/tidb/pkg/lightning/common 9.352s
```

The same exact Go-master failpoint-enabled suite passed in a detached worktree
(`9.494s`). Both successful wrapper runs returned failpoint refcount 0 after
cleanup. The first attempt failed before tests ran because another failpoint
toggle removed generated bindings; a clean rerun restored them and passed.
Rust formatting, repository lint, and `git diff --check` are run for the
receipt batch. No Rust regression test is applicable while the
dependency-closed common owner is absent. `make bazel_prepare` is not required
because this receipt batch changes no Go/Bazel/module source.

## Risk and next boundary

- Correctness: all 24 artifacts, 94 production declarations, 31 test
  functions, three benchmarks, both platform variants, and both logical
  failpoints are mapped; the successful rerun exercised the complete suite.
- Compatibility: allocator version gates, error IDs, retry classifications,
  TLS fallback, SQL quoting, platform disk semantics, and row-count policy
  remain explicit Rust integration boundaries.
- Performance: no runtime code changed; no alternate pool, retry, storage, or
  encoding path was introduced.

The next executable port must close the concrete client/storage/metadata
dependencies and all Lightning consumers together rather than adding an
isolated utility shim.
