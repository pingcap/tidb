# `pkg/session/upgrade_def.go` file-lockdown seed

This ExecPlan follows repository `PLANS.md` and Task #325's checked-in lockdown contract. The accepted source commit is `c8dbb60fb5756fe782cae5442cb84fa33007b192`. This unit owns only `rust/crates/tidb-exec` and the adjacent checker and receipt for `pkg/session/upgrade_def.go`. It is a file-lockdown seed, not completion of the Go `pkg/session` package.

## Progress

- [x] Verified both accepted remote refs at the coordinator-provided commit and created an isolated worktree and exclusive Cargo target.
- [x] Proved no checked-in `upgrade_def.go` lockdown or live `tidb-exec` owner exists; the existing `upgrade_versions.rs` is an unlocked registry seed.
- [x] Identified the direct source tests: `bootstrap_test.go`, `upgrade_test.go`, `upgrade_backfill_test.go`, the two exact `session_test.go` current-version owners, and the three external bootstrap-upgrade files.
- [x] Checked in the exact eight-artifact manifest, 2,501-row AST inventory, executable drift/symbol gate, and content-addressed receipt.
- [x] Falsified the pre-lockdown test with a Go current-version mutation, then killed and restored all 12 planned boundary mutations across eight suites.
- [x] Ran scoped Ready validation and directly inspected ratchets `0/100/1/78`. Per coordinator efficiency override, the coordinator owns the single clean detached full-workspace replay after integration.
- [x] Preserve the local branch ref and reclaim only this unit's worktrees and Cargo targets after the final local handoff commit.

## Decisions

- Go's mutable `upgradeToVerFunctions` contains executable session callbacks. Rust has no bootstrap-upgrade SQL/session engine, so the registry variable and every migration body are `DECLINED`; an ordered list of version numbers is not a port of those callbacks.
- The 176 top-level `version<N>` declarations are natively representable and are `PORTED` by an exact declared-version table. The exact 173 registered versions, their deliberate declared-but-unregistered set, strict ordering, current terminal version, and function-name convention are pinned by boundary tests.
- Direct tests that require mock stores, SQL mutation, DDL, bindings, metadata, retries, or cluster bootstrap are `DECLINED` with their AST quote and the measured missing runtime boundary. `TestUpgradeToVerFunctionsCheck` remains partly declined because Rust carries no callback pointers; only its individually representable ordering, naming, terminal, and loop obligations are ported.

## Validation contract

The checker must reproduce every selected Go AST obligation in isolation, compare every checked-in verdict byte-for-byte, verify source hashes/sizes/lines, compile every named Rust symbol through the source-backed integration test, validate mutation source paths and hashes, and verify the content-addressed receipt. Mutation probes must demonstrate that the prior seed did not notice Go-source drift, then kill declared-list, registered-list, current-terminal, gap, name, duplicate, empty, and missing-entry variants. Completion requires formatting, scoped `tidb-exec` tests and strict clippy, failpoint-safe scoped Go tests where practical, `make -j12 lint`, direct ratchets `0/100/1/78`, and `cargo test --offline --locked -j12 --workspace` in a clean detached worktree.

## Outcome

COMPLETE and FALSIFIED. The inventory contains 550 production and 1,951 direct test/support obligations: 181 are `PORTED` and compile-anchored, while 2,320 are individually `DECLINED` at the absent session/SQL/DDL/storage/callback boundary. There are no blank verdicts, TODO classifications, or false whole-migration ports. The old seed was falsified: changing Go's `currentBootstrapVersion` left its Rust source test green, while the new checker rejected the source manifest drift.

All 12 boundary mutations were killed and restored. Focused failpoint-safe Go oracles passed for the registry, the historical upgrade chain, versions 259/261/262/263, versioned schemas, and seven external bootstrap-version cases; both wrappers restored the failpoint refcount to zero. `tidb-exec --all-targets` passed 98 library tests, 551 aggregate tests with one ignored, and 24 standalone schema tests. Strict all-target clippy, formatting, the checker, `git diff --check`, and `make -j12 lint` exited zero; lint printed the repository's pre-existing Darwin `find -name` and Go internal-package diagnostics. Direct grep confirmed query/catalog/table/integration ratchets remain exactly `0/100/1/78`.

No oracle or ratchet moved. That is a successful result: this unit's deliverable is complete, drift-gated ownership of one Go source file, not ratchet movement and not completion of the whole `pkg/session` package. The coordinator will run the required clean detached full-workspace replay and perform the only allowed remote update.
