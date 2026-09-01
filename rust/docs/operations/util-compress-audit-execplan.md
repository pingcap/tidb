# `pkg/util/compress` parity audit ExecPlan

## Objective

Keep the complete Go-master pooled-gzip package aligned with its dependency-
closed Rust owner and preserve the integrated statistics block contract.

## Completed

- Read both current Go-master artifacts (`BUILD.bazel` and `gzip.go`, 45 lines)
  in full; confirmed no package tests, fixtures, generated/platform variants,
  benchmarks, fuzz targets, or nested packages exist.
- Revalidated current and exact detached Go-master package probes; both compile
  with no test files and the source is byte-identical at
  `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`.
- Confirmed `tidb-util::compress` is the sole generic owner, with pooled
  reader/writer reset and close behavior, discard-bound writer construction,
  invalid-header rejection, and unfinished-stream reset regressions.
- Re-ran the four focused pool regressions and the statistics JSON block
  round-trip consumer regression. The ingest-control consumer remains an
  explicit absent Rust integration boundary.

## Validation gate

- [x] Complete production/build inventory recorded in
      `rust/testport/receipts/util_compress_audit.md`.
- [x] Current and exact Go-master package compile probes pass.
- [x] Four Rust pool regressions and one integrated statistics regression pass.
- [x] Ready formatting, clean-tree repository lint, and diff checks pass.
- [ ] Push this receipt/ExecPlan batch, verify remote SHAs, and pull
      `origin/hparser-integration`.

## Next boundary

Any future compression change must preserve both process-wide pools and move
the missing ingest-control consumer as a complete integration unit. Do not
reintroduce direct consumer-specific compressors or arbitrary stream helpers.
