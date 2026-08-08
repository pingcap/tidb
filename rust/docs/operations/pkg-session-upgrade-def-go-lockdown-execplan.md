# `pkg/session/upgrade_def.go` file-lockdown seed

This ExecPlan follows repository `PLANS.md` and Task #325's checked-in lockdown contract. The accepted source commit is `c8dbb60fb5756fe782cae5442cb84fa33007b192`. This unit owns only `rust/crates/tidb-exec` and the adjacent checker and receipt for `pkg/session/upgrade_def.go`. It is a file-lockdown seed, not completion of the Go `pkg/session` package.

## Progress

- [x] Verified both accepted remote refs at the coordinator-provided commit and created an isolated worktree and exclusive Cargo target.
- [x] Proved no checked-in `upgrade_def.go` lockdown or live `tidb-exec` owner exists; the existing `upgrade_versions.rs` is an unlocked registry seed.
- [x] Identified the direct source tests: `bootstrap_test.go`, `upgrade_test.go`, `upgrade_backfill_test.go`, the two exact `session_test.go` current-version owners, and the three external bootstrap-upgrade files.
- [ ] Check in the exact artifact manifest, AST inventory, executable drift/symbol gate, and content-addressed receipt.
- [ ] Falsify the pre-lockdown tests, then kill every planned boundary mutation and record restored-source evidence.
- [ ] Run Ready validation, direct ratchet grep, and the clean detached locked workspace replay.
- [ ] Preserve the local branch ref and reclaim only this unit's worktrees and Cargo targets.

## Decisions

- Go's mutable `upgradeToVerFunctions` contains executable session callbacks. Rust has no bootstrap-upgrade SQL/session engine, so the registry variable and every migration body are `DECLINED`; an ordered list of version numbers is not a port of those callbacks.
- The 176 top-level `version<N>` declarations are natively representable and are `PORTED` by an exact declared-version table. The exact 173 registered versions, their deliberate declared-but-unregistered set, strict ordering, current terminal version, and function-name convention are pinned by boundary tests.
- Direct tests that require mock stores, SQL mutation, DDL, bindings, metadata, retries, or cluster bootstrap are `DECLINED` with their AST quote and the measured missing runtime boundary. `TestUpgradeToVerFunctionsCheck` remains partly declined because Rust carries no callback pointers; only its individually representable ordering, naming, terminal, and loop obligations are ported.

## Validation contract

The checker must reproduce every selected Go AST obligation in isolation, compare every checked-in verdict byte-for-byte, verify source hashes/sizes/lines, compile every named Rust symbol through the source-backed integration test, validate mutation source paths and hashes, and verify the content-addressed receipt. Mutation probes must demonstrate that the prior seed did not notice Go-source drift, then kill declared-list, registered-list, current-terminal, gap, name, duplicate, empty, and missing-entry variants. Completion requires formatting, scoped `tidb-exec` tests and strict clippy, failpoint-safe scoped Go tests where practical, `make -j12 lint`, direct ratchets `0/100/1/78`, and `cargo test --offline --locked -j12 --workspace` in a clean detached worktree.

## Outcome

To be filled after validation. Zero oracle movement is an acceptable successful outcome: this unit's deliverable is a complete, drift-gated ownership receipt, not a ratchet increment.
