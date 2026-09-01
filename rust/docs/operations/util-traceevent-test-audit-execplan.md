# `pkg/util/traceevent/test` parity audit ExecPlan

## Objective

Inventory the nested trace-event integration-test package independently from the
root production package and record the live-session boundary.

## Completed

- Read both Go-master artifacts (461 lines), eight declarations, four test
  entries, and the flaky Bazel target.
- Confirmed the root `tidb-util::traceevent` owner covers unit/adapter behavior
  but not the complete next-gen SQL-session integration harness.
- Recorded the integration package as an explicit boundary without adding a
  fabricated Rust harness.

## Validation gate

- [x] Complete nested inventory recorded.
- [x] Root traceevent Go and Rust owner tests pass.
- [x] Ready formatting, repository lint, and diff checks pass.
- [ ] Nested integration test link/run; interrupted during linker build in the
      local environment.
- [ ] Push this receipt/ExecPlan refresh, verify remote SHAs, and pull
      `origin/hparser-integration`.

## Next boundary

Run the nested suite on a next-gen TiDB integration host when the linker/runtime
budget permits; keep its bootstrap, logger, recorder, and client-go hooks as a
single integration validation unit.

