# `pkg/sessionctx/slowlogrule` parity audit ExecPlan

## Objective

Inventory the complete slow-log rule data package, compare its public structs
and constructor with Rust owners, and record the boundary between metadata
ownership and the larger session-variable evaluator.

## Completed this batch

1. Read both Go-master artifacts (73 lines): the BUILD target and all data
   types/fields/constructor in `rules.go`. No tests, fixtures, generated,
   benchmark, fuzz, platform, or generator artifacts exist.
2. Ran the exact Go-master package compile/test command; it passed with no test
   files.
3. Compared the package with Rust's `tidb-exec::slow_log_rules` metadata and
   `slow_log_parse` parser/encoder/hash owners. Rust preserves the data-model
   contracts; session-variable evaluator wiring remains a larger package
   boundary.
4. Found no Rust-only behavior to remove and no safe package-local behavior to
   implement. Recorded exact hashes, the empty Go-master delta, and the
   explicit boundary in `rust/testport/receipts/sessionctx_slowlogrule.md`.

## Validation gate

- [x] Complete Go-master production/build inventory and Rust comparison.
- [x] Exact Go-master package compile/test passed.
- [x] No package-local fixture/generated/platform artifact omitted.
- [ ] Fetch latest refs, create one meaningful docs batch commit, push to
  `origin/hparser-integration`, and verify remote `rev-list` is `0 0`.

## Remaining boundaries

Faithful end-to-end parity requires session-variable registration, parsing
and evaluation against statement fields, effective-field invalidation, and
slow-log publication. Those owners belong to `pkg/sessionctx/variable` and
the session/executor runtime; this plan does not claim whole-repository
parity.
