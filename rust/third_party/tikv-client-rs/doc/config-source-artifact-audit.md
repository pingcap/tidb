# `config` source-artifact audit

This is the atomic completion receipt for the top-level client-go package `config`, pinned at commit `52c1e76cec993571493c81de442bcbef90cdc106`. The child package `config/retry` has its own receipt and is not folded into this inventory. The Rust implementation is in the reusable `tikv-client` crate and is validated with `nightly-2026-08-22`.

## Complete source inventory

`git ls-tree -r --name-only 52c1e76cec993571493c81de442bcbef90cdc106 config` contains one child package and the following complete top-level package inventory: ten files and 1,141 lines.

| Source artifact | Lines | SHA-256 | Rust owner |
| --- | ---: | --- | --- |
| `client.go` | 300 | `f376928961422a3cfc16edd66bf2c24000b44f6c930002e76334ed36663d8800` | `src/config.rs`, `src/config/client_go.rs`, and field consumers |
| `config.go` | 229 | `5c2e95914310f9760a55c630842e444091e2e338078f5fe3ff727cf5e88556eb` | `src/config.rs`, `src/config/client_go.rs`, transaction-client construction, and global consumers |
| `config_test.go` | 172 | `71651fe444eb05af3cd6018e027f76733be1549211bee436398197f6a851d787` | source-named tests in `src/config/client_go.rs` |
| `main_test.go` | 27 | `dbba0a6b61a885ce774947de30bc8a7cfc7c5c299c34fcd5a3ed132906ca4654` | failpoint-enabled Rust tests and awaited library lifecycle gates |
| `nextgen_off.go` | 20 | `37882c6db6443f831d7b6cc812e5c3a192757b5aae9b33684f8dc7c1f890a965` | Cargo build without feature `nextgen` |
| `nextgen_on.go` | 20 | `3fb781d3787d50fd735c116721ece4c16d171156367f5de1ae9f6b695e8d5d6b` | Cargo build with feature `nextgen` |
| `ruv2.go` | 66 | `d3e72e1e484567f1562077cd5816567fde45bd00764de24acf8fd19db0315822` | `src/config/client_go.rs`, `src/util/ru.rs`, and unary/batch/stream response integration |
| `ruv2_test.go` | 81 | `5f25a959e9bac6cb3a5be4f015f76d6ef16b67e463dcc35825d308e145720d2b` | two exact source-named RU tests |
| `security.go` | 106 | `fa5aed97334b8be350ad73dceaab0b351b20f5763b44b89dbe3ebd977a70eb65` | `Security` in `src/config/client_go.rs` and reload-on-connect `src/common/security.rs` |
| `security_test.go` | 120 | `fa3c227f9ce4b70011527acc042ada3a23cb7ba049579bfe0e72e4c763ec9fd1` | ephemeral-certificate source TLS test |

There is no package `doc.go`, benchmark, example, external fixture, generated source or generator input, package build file, or other platform variant. The PEM certificate and private key in the Go test are inline test data; Rust generates equivalent ephemeral material at runtime instead of checking in a private key. `config/retry`'s four files are excluded here and remain assigned to its independent complete row.

## Production mapping

| client-go surface | Rust behavior and integration decision |
| --- | --- |
| `Config`, `TiKVClient`, `PDClient`, `TxnLocalLatches`, `PessimisticTxn`, `AsyncCommit`, `CoprocessorCache`, `RUV2TiKVConfig`, constants, and defaults | Public native structs retain every source field, value, signedness boundary that affects validation, batch-policy string, and exact default. `Default*` constructors map to `Default`. Rust's existing per-client timeout, keyspace, raw-API, and maximum-decoding options remain explicit native additions. One serde representation replaces Go's independent TOML/JSON tag systems; hidden coprocessor-admission fields remain non-serialized, and `TxnLocalLatches` remains skipped. |
| `TiKVClient.Valid`, `GetGrpcKeepAliveTimeout`, and txn-file validation | Validation preserves source order and error text for connection count, compression, 50-ms timeout, chunk size, 4-GiB parallel budget, machine `MaxInt`, and writer concurrency. Empty writer address disables txn-file validation. The duration conversion now reproduces Go's valid positive-infinity saturation and NaN rejection without panicking; invalid negative durations have no signed `std::time::Duration` representation but are rejected before transport use. |
| `GetGlobalConfig`, `StoreGlobalConfig`, `UpdateGlobal` | An `RwLock<Arc<Config>>` is the native atomic-pointer owner. Reads publish one immutable snapshot, update copies before mutation, and restore reinstates the exact previous `Arc`. The restore closure is reusable like Go's `func()`, fixing the prior `FnOnce` API restriction. |
| `GetTxnScopeFromConfig` | Global config, `injectTxnScope`, empty fallback, explicit `global`, and `GLOBAL_TXN_SCOPE` semantics match the source. Failpoint tests are serialized around the process-wide owner. |
| `ParsePath` | Scheme matching, hierarchical and opaque URLs, userinfo removal, comma-separated authority, path/fragment boundaries, query `+`/percent decoding, first-value behavior, case-insensitive `true`/`false` booleans, keyspace extraction, and exact invalid-boolean error are retained. Like `net/url.URL.Query`, malformed percent or raw-semicolon query pairs are discarded without invalidating valid siblings. Non-query malformed escapes, control bytes, invalid ports, IPv6 zone escapes, and a fragment containing `?` now follow the pinned Go 1.25.12 differential output. Go strings can contain invalid UTF-8 bytes while Rust `String` replaces those bytes lossily; this is the only native type-boundary distinction. |
| `UpdateTiKVRUV2FromExecDetailsV2` | Optional context/details/RU guards, signed-to-unsigned RPC count wrapping, raw RU-v2 merge/drain, all seven executor-input counters, every fitted coefficient, write-count charge, zero suppression, and scale application match the source. Unary, BatchCommands, and streaming consumers invoke the shared owner with read/write counts only when resource control does not bypass accounting. |
| `Security`, `NewSecurity`, `ToTLSConfig` | Empty CA disables TLS; CA and optional identity are parsed eagerly; incomplete cert/key pairs produce CA-only TLS; errors retain source context. `SecurityManager` reads identity material when a connection is created, preserving certificate rotation at the native Tonic boundary. `ClusterVerifyCN` remains retained but unused, exactly as in the pinned source implementation. |
| `NextGen` | `NEXT_GEN` is false in default builds and true under Cargo feature `nextgen`; both configurations are compile- and test-gated. |
| `EnableAsyncBatchGet`, `EnableAsyncCommit`, `Enable1PC` integration | Async BatchGet already propagates from each `Config` into snapshots. This audit found the two commit defaults were exposed but dormant: transaction clients now retain both flags and OR them into every newly created transaction's options, while explicit per-transaction enables remain enabled. This matches `NewTiKVTxn` reading the source config defaults. |

Fields that are intentionally dormant in the pinned Go package or consumed by TiDB rather than client-go remain public configuration data rather than receiving invented Rust side effects. Transport, locate, transaction, snapshot, and root-store consumers keep their own package receipts; this package receipt assigns the configuration contract and the integration fixes discovered while tracing it.

## Complete unit-test and support mapping

The package declares seven ordinary tests plus `TestMain`. Every declaration and every named table row is executable or explicitly mapped:

| Source declaration | Rust evidence |
| --- | --- |
| `TestParsePath` | `source_test_parse_path` ports all three source calls and assertions. `parse_path_matches_net_url_query_error_suppression` adds differential cases for malformed siblings, semicolons, opaque URLs, userinfo, fragment ordering, IPv6 zones, ports, host escapes, and controls. |
| `TestTxnScopeValue` | `source_test_txn_scope_value` executes `bj`, empty, and `global` failpoint returns. `source_update_global_restore_is_reusable_and_preserves_identity` additionally proves copy-on-update, pointer identity, and repeated restore calls. |
| `TestValidateGRPCKeepAliveTimeout` | `source_test_validate_grpc_keep_alive_timeout` ports default, 0.05, and 0.04 seconds with exact durations/error text. `source_special_grpc_keep_alive_timeout_conversions_match_go` guards the Go 1.25.12 NaN and infinity boundaries. |
| `TestValidateTxnFileConfig` | `source_test_validate_txn_file_config` ports the disabled precondition and all eight named rows: default, zero chunk, maximum chunk, over-budget chunk, over-`MaxInt` chunk, zero concurrency, maximum concurrency, and over-`MaxInt` concurrency. It also executes the two source success boundaries with txn-file mode actually enabled; the source table leaves those rows disabled. |
| `TestUpdateTiKVRUV2FromExecDetailsV2` | `source_test_update_tikv_ru_v2_from_exec_details_v2` retains every counter, both executor-input assertions, the exact `57.96722001` delta, raw drain, and second-drain absence. |
| `TestUpdateTiKVRUV2FromExecDetailsV2PatchesReadRPCCountInRawRUV2` | `source_test_update_tikv_ru_v2_patches_read_rpc_count_in_raw_ru_v2` ports the mutation and drained raw-counter assertions. |
| `TestTLSConfig` | `source_test_tls_config` generates an ephemeral CA/client identity, writes it to files, validates source-shaped TLS construction, and additionally covers disabled TLS, missing CA, CA-only TLS, native builder integration, and a malformed key. |
| `TestMain` | Cargo's `fail` test dependency enables the required failpoints. All async owners used by the package are scoped/awaited; focused and complete library suites are the native leak/lifecycle gate. |

The exact pinned Go package tests pass under the module-required Go 1.25.12 toolchain. The URL/duration regression vectors were also executed through a temporary Go differential probe before being asserted in Rust.

## Consumer audit

Mechanical import inventory finds exactly 68 direct Go importers: 20 under `internal`, 22 under `txnkv`, seven under `tikv`, two under `rawkv`, two under `tikvrpc`, 14 integration-test files, and one example. They divide into the following assigned boundaries:

- `internal/client`, `internal/locate`, and `internal/resourcecontrol` consume transport, routing, retry, global, and RU settings; their independent complete receipts own those algorithms.
- `rawkv`, `tikv`, `tikvrpc`, and `txnkv` consume security, keyspace/store construction, request accounting, transaction defaults, safe-point paths, and public re-exports; each retains its own complete package receipt.
- `txnkv/transaction`, `txnkv/txnlock`, `txnkv/txnsnapshot`, and `txnkv/rangetask` consume commit, file, lock, retry, and snapshot settings; config propagation is covered here while their operation semantics remain assigned to their package receipts.
- Integration tests and the raw example are repository-level consumers. They introduce no hidden config artifact or package-local fixture.

## Validation boundary

Final validation passed on `nightly-2026-08-22-aarch64-apple-darwin` (`rustc 1.100.0-nightly (c656540d6 2026-08-21)`):

- `/private/tmp/go1.25.12/bin/go test ./config -count=1`: passed.
- `/private/tmp/go1.25.12/bin/go test -tags nextgen ./config -count=1`: passed.
- `cargo +nightly-2026-08-22 test -p tikv-client config::client_go::tests:: --lib -- --nocapture`: 12 passed.
- `cargo +nightly-2026-08-22 test -p tikv-client source_config_commit_defaults_apply_without_disabling_explicit_options --lib -- --nocapture`: one passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib source_ --quiet`: 525 passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --quiet`: 854 passed and one unrelated test remained ignored.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet`: 851 passed and one unrelated test remained ignored; the config tests execute with `NEXT_GEN = true`.
- `cargo +nightly-2026-08-22 check --all-targets --all-features`: passed.
- `cargo +nightly-2026-08-22 clippy -p tikv-client --lib --all-features --message-format short`: passed cleanly.
- `cargo +nightly-2026-08-22 doc -p tikv-client --no-deps --all-features`: passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --doc --all-features --quiet`: 51 passed.
- `cargo +nightly-2026-08-22 fmt --all -- --check` and `git diff --check`: passed.
- The Rust baseline before this batch is `ff286a132948d850eb5b603572b0709b6ef510c9`; the source checkout is exactly `52c1e76cec993571493c81de442bcbef90cdc106`. Recomputed SHA-256 values match all ten rows above.

The package has no live TiKV dependency. TLS parsing uses ephemeral local files; URL and special-duration outputs were differentially checked against the exact Go toolchain. No package behavior remains dependent on an unavailable external service.
