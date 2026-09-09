# `pkg/sessionctx/sessionstates` parity audit ExecPlan

## Objective

Inventory the complete session-state and session-migration Go package,
including JSON state transfer, prepared/binding protocol tests, certificate
rotation/signature tests, failpoint lifecycle, and the eighteen-shard flaky
BUILD target; compare every owner with Rust and record a dependency-closed
boundary.

## Completed this batch

1. Read all five Go-master artifacts (2,578 lines): two production files,
   eighteen behavior tests, wire-format helpers, temporary certificate test
   support, failpoint hooks, and BUILD dependencies. No checked-in fixture,
   generated, benchmark, fuzz, platform, or generator artifact exists.
2. Ran the complete exact Go-master package suite through the failpoint
   wrapper; all eighteen tests passed in 29.543s and failpoints were disabled
   during teardown.
3. Compared every production declaration/test with Rust's session/executor
   state carriers and `session_token_timing` owner. Rust preserves timing
   constants but lacks dependency-closed state serialization/restoration,
   migration protocol, TLS certificate/signature, and authentication owners.
4. Found no Rust-only behavior to remove and no safe package-local behavior to
   implement. Recorded exact hashes, the empty Go-master delta, validation
   evidence, and the explicit boundary in
   `rust/testport/receipts/sessionctx_sessionstates.md`.

## Validation gate

- [x] Complete Go-master production/test/build inventory and Rust comparison.
- [x] Complete exact Go-master failpoint-managed suite passed.
- [x] No package-local fixture/generated/platform artifact omitted.
- [ ] Fetch latest refs, create one meaningful docs batch commit, push to
  `origin/hparser-integration`, and verify remote `rev-list` is `0 0`.

## Remaining boundaries

Faithful parity requires coordinated session-state JSON schema, variable and
binding restoration, prepared protocol/cursor handling, migration guards,
certificate rotation and multi-algorithm signing, token validation, and server
authentication. The loop continues with the next unrecorded package; this
plan does not claim whole-repository parity.
