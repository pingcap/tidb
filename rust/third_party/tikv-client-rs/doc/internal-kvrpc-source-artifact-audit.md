# `internal/kvrpc` source-artifact audit

This is the atomic completion receipt for client-go package `internal/kvrpc`, pinned at commit `52c1e76cec993571493c81de442bcbef90cdc106`. Rust ownership is split across the generic batching primitive in `src/request/shard.rs`, typed RawKV request shards in `src/raw/requests.rs`, and the root split-region consumer in `src/tikv.rs`. Validation uses `nightly-2026-08-22`.

## Complete source inventory

The package is exactly one 82-line production artifact:

| Source artifact | Lines | SHA-256 | Rust owner |
| --- | ---: | --- | --- |
| `internal/kvrpc/batch.go` | 82 | `a33469122ab16ca0d57189ce0be260e98a9684d8b6ad71a580ce952795179b0a` | `src/request/shard.rs`, typed request/plan shards, and direct consumers |

There is no package-local test, `TestMain`, benchmark, example, fixture, generated source or input, build/platform variant, package metadata, package-specific build file, build tag, or generation directive.

Mechanical import inventory finds exactly two direct Go consumers: `rawkv/rawkv.go` uses size-limited batches for BatchPut and count-limited batches for BatchGet/BatchDelete, while `tikv/split_region.go` uses count-limited batches for split keys. Their request, retry, concurrency, merge, and error algorithms retain their own completed package receipts; this receipt owns the shared batch representation and threshold semantics at those integration boundaries.

## Production mapping and differential findings

| client-go surface | Rust behavior and integration decision |
| --- | --- |
| `Batch` | A typed request shard paired with its `RegionWithLeader` replaces a loose struct. Raw BatchPut shards carry `(KvPair, TTL)` tuples, making key/value/TTL alignment structural; key-only shards preserve key order. The route carries the complete region identity used by the plan. |
| `BatchResult` | Native `Result<Response>` plan streams and merge stages replace the nullable response/error pair. Partial response and first-error policy remain owned and tested by RawKV and root split-region consumers. |
| `AppendBatches` | `Batchable::batches` checks accumulated size before appending the next item, so a batch may exceed the nominal limit by one complete item. Empty input, zero limit's initial empty batch, oversized items, exact-limit rollover, key/value size accounting, and positive consumer limits match. Rust uses `u64` for byte limits; client-go's unused negative `int` edge is not representable. |
| `AppendKeyBatches` | `key_batches` preserves the source's `count > limit` test: limit 512 produces 513 keys in the first full batch, limit zero produces one key per batch, empty input with a negative limit is empty, and non-empty negative input panics. |
| RawKV integration | BatchPut preserves region association, key/value/TTL alignment, source duplicate-key final value/TTL selection, 16-KiB accumulated-size rollover, and legacy first-TTL publication. BatchGet and BatchDelete both use the source 512 count limit and therefore the 513/1 boundary. |
| split-region integration | A regression introduced after the earlier receipt used ordinary `chunks(2_048)`, yielding `[2_048, 2]` for 2,050 same-region keys. The source routes this consumer through `AppendKeyBatches`, yielding `[2_049, 1]`. The new regression failed with the former shape and passes after the root consumer was wired to `key_batches`. |

Appending onto a caller-owned prefix vector is represented by each Rust consumer extending its typed shard stream/vector. Both source consumers initialize an empty batch list, so no observable prefix behavior is lost.

## Test and validation boundary

The source package declares no Go tests. Rust source-uncovered tests cover both split functions' boundaries, oversized items, zero/negative behavior, raw payload/TTL alignment across regions, the 16-KiB BatchPut boundary, the 513/1 RawBatchGet boundary, and the corrected 2,049/1 split-region consumer boundary. The split-region regression was run before the fix and failed with `left: [2048, 2]`, `right: [2049, 1]`.

Final validation passed on `nightly-2026-08-22-aarch64-apple-darwin` (`rustc 1.100.0-nightly (c656540d6 2026-08-21)`):

- Exact Go package compilation with task-local clean build/module caches: passed and reported `[no test files]`.
- Three exact primitive boundary tests and three exact RawKV region/payload/count consumer tests passed individually.
- Focused split-region boundary regression: 1 passed after the captured pre-fix failure.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib source_ --quiet`: 569 passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features source_ --quiet`: 566 passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --quiet`: 886 passed and one unrelated test remained ignored.
- `cargo +nightly-2026-08-22 test -p tikv-client --lib --all-features --quiet`: 883 passed and one unrelated test remained ignored.
- `cargo +nightly-2026-08-22 check --all-targets --all-features`: passed.
- `cargo +nightly-2026-08-22 clippy -p tikv-client --lib --all-features --message-format short -- -D warnings`: passed cleanly.
- `cargo +nightly-2026-08-22 doc -p tikv-client --no-deps --all-features --document-private-items`: passed.
- `cargo +nightly-2026-08-22 test -p tikv-client --doc --all-features --quiet`: 51 passed.
- `cargo +nightly-2026-08-22 fmt --all -- --check` and `git diff --check`: passed.

The Rust baseline before this batch is `d9a9fa1722bd5115ec9cc1c0d1e1fcf86f7ecc6b`; source identity, line count, the no-test/support boundary, and both direct imports were recomputed from the pinned checkout. No live cluster is required for this deterministic batching package; typed mock dispatch captures the exact physical request boundaries and region/TTL payloads.
