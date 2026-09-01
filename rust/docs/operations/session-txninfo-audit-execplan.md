# `pkg/session/txninfo` parity audit ExecPlan

## Objective

Maintain a complete Go-master inventory for `pkg/session/txninfo` and record
a safe package-atomic Rust ownership boundary. Read every Go production and
build artifact before editing; do not port only the state enum while the
summary, metrics, infoschema, and Datum consumers remain split.

## Completed this batch

1. Inventoried all three artifacts (473 lines): the FNV/LRU transaction
   history recorder, running-state/metric definitions, `TxnInfo` conversion
   map, and the Bazel target. No tests, fixtures, generated outputs,
   benchmarks, fuzz inputs, or platform variants were omitted.
2. Compiled the exact Go-master package; it reported `[no test files]`.
3. Compared the complete package with Rust. Rust owns state labels and a
   partial live transaction registry, but lacks the dependency-closed Go
   summary recorder, Prometheus observer matrix, and `TIDB_TRX` Datum
   conversion contract.
4. Found no safe missing behavior to implement and no Rust-only behavior to
   remove. Recorded the inventory, hashes, validation evidence, and explicit
   SEED boundary in `rust/testport/receipts/session_txninfo.md`.

## Validation gate

- [x] Complete Go source/Bazel inventory and Rust owner comparison.
- [x] Exact Go-master package compilation passed (`[no test files]`).
- [x] No package-local fixture/generated/platform artifact omitted.
- [ ] Fetch remote, create one meaningful docs batch commit, push to
  `origin/hparser-integration`, and verify `rev-list` is `0 0`.

## Remaining boundaries

The live transaction registry, `TIDB_TRX`/`TRX_SUMMARY` infoschema rows,
Prometheus metrics, and Go Datum/type-conversion semantics remain explicit
cross-crate boundaries. The repository package loop continues after this
receipt; this plan does not claim whole-repository completion.
