# `pkg/lightning/backend` parity audit ExecPlan

## Objective

Inventory the complete Go-master Lightning backend abstraction and its
failpoint-driven lifecycle tests, then determine whether Rust has a
dependency-closed owner for engine management and writer behavior.

## Completed

- Read all three Go-master artifacts in full (846 lines): BUILD metadata,
  440-line production lifecycle/interfaces, and 362-line gomock test suite.
- Mapped 18 production declarations, all 12 public types/interfaces, 14
  functional tests, metric/retry branches, the `FailIfEngineCountExceeds`
  failpoint, and the 14-shard flaky target.
- Confirmed there are no fixtures, testdata, generated/platform variants,
  benchmarks, fuzz corpora, package docs, or extra build inputs.
- Verified the hparser branch and Go master are identical for this package.
- Searched Rust storage, transaction, tablecodec, import-protocol, and metric
  owners; no engine-manager/writer abstraction or call site exists.
- Ran current-branch and detached exact-Go-master failpoint suites successfully.

## Validation gate

- [x] Complete pinned inventory and source/test mapping.
- [x] Current and exact-master failpoint suites pass; failpoint state returns
      to zero.
- [x] Rust formatting, repository lint, and diff checks pass for the receipt
      batch.
- [ ] Push the receipt/ExecPlan batch to `origin/hparser-integration`, verify
      equal local/remote/advertised SHAs, and pull the explicit branch ref.

## Remaining boundary

An executable Rust port must move concrete local/external engines, engine
storage, metric context, UUID/tag logging, retry and duplicate-import policy,
and writer lifecycle together. Keep this abstraction as an explicit boundary
until those consumers are dependency-closed; do not add an unconnected wrapper
or ignored parity tests.
