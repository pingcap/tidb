# `pkg/lightning/backend/tidb` parity audit ExecPlan

## Objective

Inventory the complete Go-master TiDB SQL backend package, including SQL-mock
fixtures and failpoint branches, and decide whether Rust can own the behavior
without a second disconnected database/Lightning writer path.

## Completed

- Read all three Go-master artifacts in full: BUILD metadata, 1,063-line
  production backend, and 1,096-line test suite.
- Mapped 42 production declarations, 17 functional tests, all helpers,
  conflict/retry/prepared-statement branches, both failpoints, and the 17-shard
  flaky BUILD target.
- Confirmed there are no package docs, testdata, binary fixtures, generated or
  platform variants, fuzz corpora, README files, or additional build inputs.
- Verified this path is unchanged between the hparser branch and Go master.
- Searched Rust SQL, tablecodec, transaction, error-manager, and Lightning
  modules; no dependency-closed TiDB SQL backend owner or call site exists.
- Ran the complete failpoint-enabled Go-master suite successfully.

## Validation gate

- [x] Complete pinned inventory and source/test fixture mapping.
- [x] Exact Go-master failpoint suite passes and failpoint state returns to zero.
- [x] Rust formatting, repository lint, and diff checks pass for the receipt
      batch.
- [ ] Push the receipt/ExecPlan batch to `origin/hparser-integration`, verify
      equal local/remote/advertised SHAs, and pull the explicit branch ref.

## Remaining boundary

An executable Rust port must move the SQL driver, metadata query compatibility,
literal encoder, table/row encoder, retry and conflict error manager, prepared
statement cache, and Lightning engine-writer lifecycle as one dependency-closed
unit. Keep this package as an explicit boundary until those owners exist; do
not add a serializer-only facade or ignored parity tests.
