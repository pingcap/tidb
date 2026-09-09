# `pkg/session/test/vars` parity audit ExecPlan

## Objective

Maintain a complete Go-master inventory for the session variable persistence
and compatibility test package and map its variable, domain, hint, timezone,
TTL, and timestamp behavior to existing Rust owners without duplicating
registries or storage callbacks.

## Completed this batch

1. Inventoried all three Go-master artifacts (638 lines): the TestMain/goleak
   harness, twelve tests, helper methods, and twelve-shard flaky BUILD target.
   No production, fixture, generated, benchmark, fuzz, or platform artifact
   was omitted. The working branch's separate unstaged package edits were
   preserved and not included in this audit commit.
2. Ran the exact Go-master failpoint-managed suite; all twelve tests passed in
   10.868s and failpoints were disabled during teardown.
3. Compared every test with Rust. Session/vardef owners cover selected
   variable state, scope, hints, timezone, and timestamp contracts, while
   mock TiKV transport, persistent upgrade values, TTL callbacks,
   deployment-mode policy, and checkpoint integration are not
   dependency-closed together.
4. Found no safe missing behavior to implement and no Rust-only behavior to
   remove. Recorded inventory, hashes, validation evidence, and the explicit
   SEED boundary in `rust/testport/receipts/session_test_vars.md`.

## Validation gate

- [x] Complete Go-master test/Bazel inventory and Rust owner comparison.
- [x] Exact Go-master failpoint-managed package suite passed.
- [x] No package-local fixture/generated/platform artifact omitted.
- [ ] Fetch remote, create one meaningful docs batch commit, push to
  `origin/hparser-integration`, and verify `rev-list` is `0 0`.

## Remaining boundaries

Faithful parity requires coordinated variable registry and persistence,
mock-TiKV variable propagation, TTL external-workload callback,
deployment-mode policy, prepared-hint execution, and checkpoint/timestamp
owners. The package loop continues with the next unrecorded session test
package; this plan does not claim whole-repo completion.
