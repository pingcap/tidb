# `pkg/lightning/backend/kv` parity audit ExecPlan

## Objective

Inventory the complete Go-master Lightning KV backend package, compare every
production/test/build artifact with Rust ownership, and avoid a partial encoder
that would diverge from Go's table, datum, allocator, checksum, and buffer
contracts.

## Completed

- Read all 13 Go-master artifacts in full (3,150 lines): BUILD metadata, six
  production files, and six test files containing 22 tests and one benchmark.
- Counted and mapped 110 production function/method declarations, all source
  test identities, the 27-shard flaky target, and every dependency.
- Confirmed there are no fixtures, testdata, generated/platform variants,
  fuzz corpora, package docs, or additional build inputs.
- Recorded the five current-master deltas (new-collation context propagation
  and its test assertions) against the hparser branch.
- Searched the Rust workspace and found only adjacent generic tablecodec,
  transaction-buffer, and Lightning utility owners; no dependency-closed
  encoder/decoder or session owner exists.
- Current-branch and detached exact-Go-master package suites both pass.

## Validation gate

- [x] Complete pinned inventory and source-delta comparison.
- [x] Current-branch and exact-master focused Go tests pass.
- [x] Rust formatting, repository lint, and diff checks pass for the receipt
      batch.
- [ ] Push the receipt/ExecPlan batch to `origin/hparser-integration`, verify
      equal local/remote/advertised SHAs, and pull the explicit branch ref.

## Remaining boundary

The concrete Rust implementation must land atomically with table metadata and
datum conversion, expression/generated-column evaluation, tablecodec, auto-ID
allocation, duplicate detection, verification checksums, and backend writers.
Until those owners are available, keep this package as an explicit boundary;
do not add ignored tests, a cache-only path, or a narrowed interface.
