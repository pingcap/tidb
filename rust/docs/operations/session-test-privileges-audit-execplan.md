# `pkg/session/test/privileges` parity audit ExecPlan

## Objective

Maintain a complete Go-master inventory for the session privilege/auth test
package and map it to the existing Rust account and server owners without
inventing a duplicate authentication harness.

## Completed this batch

1. Inventoried all three artifacts (138 lines): the common TestMain/goleak
   harness, `SkipWithGrant` role/auth test, unknown-user auth test, and
   two-shard flaky BUILD target. No fixture, generated, benchmark, fuzz, or
   platform artifact was omitted.
2. Ran the exact Go-master failpoint-managed package suite; both tests passed
   in 4.220s and failpoints were disabled during teardown.
3. Compared the tests with Rust. Existing configured-user-store and session
   privilege owners cover authentication and bypass behavior, with executable
   server/session tests; the exact Go TestKit/global-variable harness remains
   an explicit source-carrier boundary.
4. Found no safe missing behavior to implement and no Rust-only behavior to
   remove. Recorded the inventory, hashes, validation evidence, and boundary
   in `rust/testport/receipts/session_test_privileges.md`.

## Validation gate

- [x] Complete Go test/Bazel inventory and Rust owner comparison.
- [x] Exact Go-master failpoint-managed package suite passed.
- [x] No package-local fixture/generated/platform artifact omitted.
- [ ] Fetch remote, create one meaningful docs batch commit, push to
  `origin/hparser-integration`, and verify `rev-list` is `0 0`.

## Remaining boundaries

Exact Go TestKit/Auth integration, external authentication plugins, host
matching, and process-global `SkipWithGrant` lifecycle remain owned by the
server/session integration. The repository package loop continues with the
next unrecorded session test package; this plan does not claim whole-repo
completion.
