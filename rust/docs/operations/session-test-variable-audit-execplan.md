# `pkg/session/test/variable` parity audit ExecPlan

## Objective

Maintain a complete Go-master inventory for the session variable test package
and map its variable domains, coprocessor controls, memory actions, replica
routing, query accounting, and logging semantics to existing Rust owners
without duplicating session or observability authorities.

## Completed this batch

1. Inventoried all three artifacts (593 lines): the TestMain/goleak harness,
   twelve tests, three `mockZapCore` methods, and twelve-shard flaky BUILD
   target. No production, fixture, generated, benchmark, fuzz, or platform
   artifact was omitted.
2. Ran the exact Go-master failpoint-managed suite; all twelve tests passed in
   10.868s and failpoints were disabled during teardown.
3. Compared every test with Rust. Session/vardef owners cover selected scope,
   isolation-read, replica-read, hint, and max-execution-time contracts, but
   coprocessor OOM/rate-limit, query RU accounting, general-log interception,
   and complete snapshot/staleness lifecycle seams are not dependency-closed.
4. Found no safe missing behavior to implement and no Rust-only behavior to
   remove. Recorded inventory, hashes, validation evidence, and the explicit
   SEED boundary in `rust/testport/receipts/session_test_variable.md`.

## Validation gate

- [x] Complete Go test/Bazel inventory and Rust owner comparison.
- [x] Exact Go-master failpoint-managed package suite passed.
- [x] No package-local fixture/generated/platform artifact omitted.
- [ ] Fetch remote, create one meaningful docs batch commit, push to
  `origin/hparser-integration`, and verify `rev-list` is `0 0`.

## Remaining boundaries

Faithful parity requires coordinated session/vardef validation, coprocessor
rate-limit and OOM action hooks, memory tracking, query RU accounting,
replica routing, snapshot/staleness state, and general-log observability
owners. The package loop continues with the next unrecorded session test
package; this plan does not claim whole-repo completion.
