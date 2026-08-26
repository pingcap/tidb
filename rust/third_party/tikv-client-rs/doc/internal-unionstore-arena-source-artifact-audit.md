# `internal/unionstore/arena` source-artifact audit

This is the atomic completion receipt for client-go package `internal/unionstore/arena`, pinned at commit `52c1e76cec993571493c81de442bcbef90cdc106`. Its Rust owner is the public native module `tikv_client::transaction::arena`, validated with `nightly-2026-08-22`.

## Complete source inventory

The package is exactly two files and 508 lines:

| Source artifact | Lines | SHA-256 | Rust owner |
| --- | ---: | --- | --- |
| `arena.go` | 429 | `767d066be0a17e7e238777b5e6d275b0758f7df599d9cb7c622881854d91a758` | `src/transaction/arena.rs` and the safe ART/RBT representation decision |
| `arena_test.go` | 79 | `9d5916cb04113153cd43bfb760a8572b142a0017d55e4d132745c8d1e9110687` | two source-named tests plus two complete branch tests in `src/transaction/arena.rs` |

There is no `doc.go`, `TestMain`, benchmark, example, fixture, generated source or generator input, build/platform variant, package metadata, or package-specific build file.

## Production mapping

| client-go surface | Rust behavior and integration decision |
| --- | --- |
| constants, `MemdbArenaAddr`, `MemKeyHandle` | The 128-MiB limit, 4-KiB initial size, null/bad/u64 sentinels, empty tombstone, little-endian `(block, offset)` layout, u64 round trip, null classification when either half is `u32::MAX`, and truncating 16-bit key handle match. Rust exposes constructors/accessors instead of mutable exported sentinel variables and private-field struct literals. |
| block arena allocation | Stable block indexes/offsets replace raw pointers. Unaligned and eight-byte-aligned allocation, fit/fail behavior, first allocation, strict-greater power-of-two growth capped at 128 MiB, total allocated capacity, block count, and the source panic text for an over-limit total allocation match. Checked arithmetic makes an impossible native `usize` overflow fail as no-fit instead of wrapping. |
| data, memory hook, reset | Data begins at the addressed offset and extends to the backing block end. A safe `Arc<dyn Fn()>` replaces Go's atomically stored function pointer; arena mutation remains single-owner while callback invocation occurs at the exact source boundaries: value append that adds a block and reset, but not enlarge or truncate. Reset clears all blocks/size/capacity before invoking the hook. |
| `MemDBCheckpoint`, truncate/order | Checkpoints retain current growth size, block count, and last used offset. Position equality and strict ordering match. Rust references make the source's nil-check panic unrepresentable. Truncate drops later blocks, restores the last used length and growth size, and deliberately recomputes capacity from used lengths rather than retained allocation sizes. |
| value-log header and append/read | The exact 20-byte little-endian header follows each value; returned addresses point to record ends. Node/old-value addresses and `u32` value length match. Empty values read as the tombstone; non-empty reads are zero-copy borrowed slices. A new block invokes the memory hook only after the record is internally coherent. |
| history, snapshots, rollback, inspection | Old-value traversal, first predicate match, no-match null, checkpoint visibility, `CanModify` boundaries including an empty checkpoint, newest-to-oldest rollback callbacks, cursor crossing between blocks, current-version-only inspection, key flags, and superseded-version suppression match. Rust generic traits return owned node views and borrowed callback slices without unsafe pointer lifetimes. |

The 16 direct Go importers are all inside `internal/unionstore`: eight ART files/tests, four RBT files, and four parent MemDB/pipelined files. Rust's completed ART and RBT packages deliberately use safe `BTreeMap`-based value/undo logs instead of this manual allocator; their receipts prove the observable staging, history, iterator, snapshot, flags, memory-accounting, and rollback contracts. The arena remains a complete reusable public native module, but is not falsely described as backing those safe-map owners.

## Complete unit-test mapping

The package declares exactly two tests and no support harness:

| Source declaration | Rust evidence |
| --- | --- |
| `TestBigValue` | `source_test_big_value` executes the exact 80-MiB first record, 127-MiB second record, checkpoint rollback traversal, 128-MiB block-size/count assertions, and over-limit panic text. |
| `TestValueLargeThanBlock` | `source_test_value_larger_than_block` executes the exact one-byte, 4,096-byte, and 3,000-byte sequence, two-block assertions, and 3,000-byte value readback. |

`addresses_checkpoints_history_inspection_and_hooks` additionally covers sentinels, address/handle/header round trips, tombstones, snapshot selection, no-history, modification boundaries, strict checkpoint order, current-version inspection, rollback callbacks, truncation, hook counts, and reset. `allocator_alignment_capacity_and_truncation_match_source` covers aligned offsets, data access, allocated versus used capacity, truncate restoration, and immediate reuse.

## Validation boundary

Completion requires exact source identity, hashes, and line counts; both source declarations and every assertion; all four owned Rust tests; exact Go execution under Go 1.25.12; source/default/all-feature Rust suites; all-target checking, Clippy, rustdoc/doctests, rustfmt/diff checks; and explicit disposition of all 16 importers. No live TiKV/PD service applies to this deterministic allocator/value-log package.

Final validation passed on `nightly-2026-08-22-aarch64-apple-darwin` (`rustc 1.100.0-nightly (c656540d6 2026-08-21)`):

- `/private/tmp/go1.25.12/bin/go test ./internal/unionstore/arena -count=1`: passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib transaction::arena::tests --quiet`: four passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib source_ --quiet`: 530 passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --quiet`: 857 passed and one unrelated test remained ignored.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet`: 854 passed and one unrelated test remained ignored.
- `cargo +nightly-2026-08-22 check --all-targets --all-features`: passed.
- `cargo +nightly-2026-08-22 clippy -p tikv-client --lib --all-features --message-format short`: passed cleanly.
- `cargo +nightly-2026-08-22 doc -p tikv-client --no-deps --all-features`: passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --doc --all-features --quiet`: 51 passed.
- `cargo +nightly-2026-08-22 fmt --all -- --check` and `git diff --check`: passed.
- The Rust baseline before this batch is `8481c867d3ead0916f2a6a89505209483955f378`; the source checkout is exactly `52c1e76cec993571493c81de442bcbef90cdc106`, and recomputed line counts/SHA-256 values match both inventory rows.

The exact Go tests and every Rust gate are local and deterministic. No package behavior remains dependent on an unavailable service.
