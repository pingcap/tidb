# `pkg/extworkload` parity audit ExecPlan

This living plan follows `PLANS.md` at the repository root.

## Objective

Inventory the complete external-workload manager and client package, restore
Go-master lifecycle and duration semantics, add focused regressions, and push a
single verified batch.

## Progress

- [x] (2026-09-02) Pulled the latest branch tip and read all 9 package
  artifacts and 1,400 lines before editing, including nested client files and
  build metadata.
- [x] (2026-09-02) Restored duration-based GCV2 APIs, TTL table-info naming,
  store manager binding, and upgrade abort handling.
- [x] (2026-09-02) Ran focused manager and client tests with failpoint cleanup.
- [x] (2026-09-02) Ran the remaining Ready gates, staged only this package and
  its TTL test interface update plus receipts, committed, pushed, verified the
  remote SHA, and fast-forward pulled.
- [ ] Continue the rolling audit with the next unrecorded Go package.

## Constraints

The interface change is cross-package API-sensitive; all Go implementations and
test doubles must use duration semantics. Bazel preparation is mandatory for
the BUILD update but unavailable in this environment.

## Outcome

Evidence is recorded in `rust/testport/receipts/extworkload.md`; no Rust
package-completion claim is made.
