# `kv` source-artifact audit

This is the atomic completion receipt for client-go package `kv`, pinned at commit `52c1e76cec993571493c81de442bcbef90cdc106`. The Rust implementation is the public `tikv_client::kv` module and its root re-exports, validated with `nightly-2026-08-22`.

## Complete source inventory

The package is exactly eight files and 995 lines. There is no `doc.go`, benchmark, example, fixture, generated source or generator input, build/platform variant, package metadata, or package-specific build file.

| Source artifact | Lines | SHA-256 | Rust owner |
| --- | ---: | --- | --- |
| `key.go` | 88 | `5ed4903820ccc6513f12accb2fdb26b6adc86d89042df544b4dfd252801dccb3` | `src/kv/key.rs` |
| `key_test.go` | 54 | `b1f6626f49a3b0ce2ccbdfeed5351c1e35614f6aae7bd14eaf4a5ef5eac1ed96` | source-named tests in `src/kv/key.rs` |
| `keyflags.go` | 279 | `b30373f6017997d69aaedbc88db6a5a751792e93ffc7bc033e8a01dc6db7a7c3` | `src/kv/key_flags.rs` and authoritative transaction `MemDb` consumers |
| `kv.go` | 264 | `7658ade337979e30b09c91ca5069a779a9266cc8bf9be25e5b9f0355c2a8a68e` | `src/kv/types.rs` and transaction/snapshot consumers |
| `kv_test.go` | 91 | `f6c1334f069dd9b2aa205e13ae06dbbe4b4f6ab9d25542832a0c616e1b55790c` | three source-named tests in `src/kv/types.rs` |
| `main_test.go` | 25 | `f2138770b663ca0f52ef29f83760d85af59a91b9245e10bb55c14e5ed5ee3a15` | complete awaited Rust library lifecycle gate |
| `store_vars.go` | 102 | `b274aeb1c3b92dd9b4773e28fe395db41f7782147493159c5d3dd1fc3c475e54` | `src/kv/store_vars.rs`, routing, request dispatch, and transaction batching |
| `variables.go` | 92 | `2082805f7a18b01700621e9651ac45a8b8754a9f52bca17b3efd19b1ee1439f7` | `src/kv/variables.rs`, retry, snapshot, and transaction owners |

## Production mapping

| client-go surface | Rust behavior and integration decision |
| --- | --- |
| `NextKey`, `PrefixNextKey`, `CmpKey`, `KeyRange` | Byte ordering, zero suffix, carry/wrap, empty and all-`0xFF` prefix boundaries, three-way comparison, and half-open range fields match exactly. Rust `Key` additionally owns bytes and exposes the same operations idiomatically. |
| `KeyFlags`, `FlagBytes`, all 14 bits, all 22 `FlagsOp` values, `ApplyFlagsOps` | `u16` flag storage, fixed power-of-two `u32` operation identities, operation order, assertion exclusivity/unknown state, presume/not-exist implications, lock mode, existence results, constraint clearing, previous-presume behavior, and the four-bit persistent mask match the source. Rust's enum excludes unknown operations that source consumers never construct; `KeyFlags::from_bits` retains arbitrary stored flag bits. |
| `ReturnedValue`, lock wait sentinels, `LockCtx` | Every returned-value field and lock-context input/output/callback is retained. Default wait lazily becomes `i64::MAX`; explicit wait and no-wait values remain signed. One mutex owns the byte-keyed map, and methods expose insertion, filtered lookup, and filtered iteration without Go's separately mutable map/lock race surface. Go zero `time.Time` maps to `None`; constructors use `Some(SystemTime)`. `Arc<AtomicU32>` supplies safe shared kill/expiry ownership. |
| `ValueEntry` | Value bytes, unknown/known commit timestamp, constructor, empty-value predicate, and `size_of::<ValueEntry>() + value.len()` accounting match. Go nil and empty byte slices intentionally collapse to empty `Vec<u8>` because both have identical package behavior. |
| get/batch-get options and getter interfaces | The only pinned option, return-commit-TS, is a shared enum variant accepted by point and batch options. Defaults, ordered/repeated application, batch-to-point conversion, and consumer request/cache behavior match. Async traits replace Go context-bearing synchronous method signatures; dropping/cancelling the future is the native request-cancellation boundary. |
| `StoreLimit`, `TxnCommitBatchSize`, replica/access types | Process-wide signed/unsigned atomics start at zero and 16 KiB respectively and are consumed by request admission and every transaction batch owner. All five replica values, follower classification, exact display names, unknown byte round trips, and all three access-location values match. Native selector configuration is an additive consumer-owned extension. |
| `Variables`, `KillSignalHandler`, `DefaultVars` | Backoff defaults 10/2, shared kill signal, transaction-file controls, zero minimum, optional higher-priority handler, and default process value match. Rust uses `Arc<Variables>` so callers clone the source default safely rather than sharing a mutable raw pointer; consumer retry checkpoints preserve handler precedence. |

All source-shaped types and helpers are externally nameable through public `tikv_client::kv`; the most common types remain re-exported at crate root. Rust uses byte-vector map keys instead of Go's byte-preserving string conversion and typed errors/futures instead of unchecked interface/context plumbing. These are native representation decisions, not omitted package behavior.

## Complete unit-test and support mapping

The package declares four ordinary tests plus `TestMain`. Every source row now has an independent executable Rust case:

| Source declaration | Rust evidence |
| --- | --- |
| `TestPrefixNextKey` | `source_test_prefix_next_key` executes the source's one-, two-, and four-byte all-`0xFF` rows. `next_key_prefix_carry_and_compare_match_client_go` covers the remaining production branches. |
| `TestGetOptions` | `source_test_get_options` executes all four rows: default, direct return-commit-TS, empty batch conversion, and converted return-commit-TS. |
| `TestBatchGetOptions` | `source_test_batch_get_options` executes both default and return-commit-TS rows. |
| `TestValueEntry` | `source_test_value_entry` executes default, empty slice, nil-slice native equivalent, non-empty/known timestamp, and non-empty/unknown timestamp rows; it also validates native size accounting. |
| `TestMain` | The package starts no task or thread. Focused tests and both complete library suites terminate with every async owner awaited, which is the native goleak disposition. |

The prior combined Rust option/value test did not independently execute direct point options, empty conversion, default batch options, or the nil-slice row. This re-audit replaces it with the source-named ports. Source-uncovered tests also execute every one of the 22 operation values/branches, all flag queries and persistent bits, key carry/comparison, lock-context initialization/filtering, replica/access unknown values, atomics, selector build variants, and variable defaults.

## Consumer audit

Mechanical import inventory finds exactly 69 direct Go importers: three in `config/retry`; 31 under `internal` (ten `locate`, one `resourcecontrol`, 20 `unionstore`); 16 under `txnkv` (two `rangetask`, ten `transaction`, four `txnsnapshot`); five root `tikv`; one each in `rawkv`, `tikvrpc`, and examples; and 11 integration-test files.

- Retry and request dispatch consume variables, kill handling, store admission, replica/access values, and resource accounting; their algorithms retain independent complete receipts.
- Union store and transaction owners consume every key flag, lock result/context, commit batch atomic, and transaction-file variable. Their staged-buffer, 2PC, lock, and pipelined semantics remain owned by their package receipts.
- Snapshots and root `tikv` consume getters, value entries/commit timestamps, key ranges, replica modes, and defaults. Their routing/cache/GC behavior remains independently assigned.
- RawKV, RangeTask, integration tests, and the example use package types but introduce no hidden `kv` artifact or package-local fixture.

## Validation boundary

Completion requires exact pinned identity/hashes/line counts; declaration- and row-level reconciliation of all four source tests plus `TestMain`; the 12-test owned Rust module matrix; all source-derived and complete library configurations; all-target/all-feature checking, Clippy, rustdoc/doctests, rustfmt/diff checks; and the exact Go package test under Go 1.25.12. No live TiKV/PD service applies to this deterministic type/state package.

Final validation passed on `nightly-2026-08-22-aarch64-apple-darwin` (`rustc 1.100.0-nightly (c656540d6 2026-08-21)`):

- `/private/tmp/go1.25.12/bin/go test ./kv -count=1`: passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib kv::key::parity_tests --quiet`: two passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib kv::key_flags::tests --quiet`: two passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib kv::types::tests --quiet`: four passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib kv::store_vars::tests --quiet`: three passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib kv::variables::tests --quiet`: one passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib source_ --quiet`: 528 passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --quiet`: 857 passed and one unrelated test remained ignored.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet`: 854 passed and one unrelated test remained ignored.
- `cargo +nightly-2026-08-22 check --all-targets --all-features`: passed.
- `cargo +nightly-2026-08-22 clippy -p tikv-client --lib --all-features --message-format short`: passed cleanly.
- `cargo +nightly-2026-08-22 doc -p tikv-client --no-deps --all-features`: passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --doc --all-features --quiet`: 51 passed.
- `cargo +nightly-2026-08-22 fmt --all -- --check` and `git diff --check`: passed.
- The Rust baseline before this batch is `743a1f7724cf58cc8a0c7da7f5a33ff9f8be8442`; the source checkout is exactly `52c1e76cec993571493c81de442bcbef90cdc106`, and recomputed line counts/SHA-256 values match all eight inventory rows.

The exact Go tests and every Rust gate are local and deterministic. No package behavior remains dependent on an unavailable service.
