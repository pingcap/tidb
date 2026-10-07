# Share foreign-key access and retained cascade identity


This living ExecPlan follows PLANS.md.

## Purpose and scope


Replace FK whole-table value matching with the access selected by Go's physical FK owner. Repair child existence checks, parent restrictions, collated-key changes, ALTER validation and cascade row identity together. Baseline is 730f8b2f052d29742ac288e314df541411131be2; refreshed Go master is 7a3dacb52efe58d28db360ae8639d8838c376544. E02/E03/K03 remain broader findings; complete packages and distributed FK locking are not accepted by this maintenance batch.

## Progress


- [x] Refresh both remotes and confirm clean Cloud checkouts and unchanged Go master.
- [x] Trace Go executor/foreign_key.go and planner/core/operator/physicalop/foreign_key.go against all Rust FK consumers.
- [x] Capture related baseline failures together: all five fail; live server reproduces the supported DML failures.
- [x] Retain selected lookup metadata, use canonical key access, preserve errors/context and cascade handles; remove obsolete scans after all consumers migrate.
- [x] Run grouped regressions, affected all-target checks, lint and locked server build; update both registers and receipt.
- [ ] Commit through the actual hook; fresh locked build immediately before normal push, verify remote HEAD and update reusable Cloud checkpoint.

## Context and ownership


`tidb-planner::physical::FkTriggerNode` retains resolved offsets but only renders the selected index. `tidb-executor::driver::fk_trigger_plan` selects that index. `foreign_key.rs` instead scans parent/child tables, uses raw Datum equality, discards scan errors and rescans cascaded rows to recover handles. `kv_table` already owns canonical index/record keys, snapshot-plus-buffer reads and contextual row decoding. Move FK access into that owner and carry handles to shared mutation methods.

## Plan of work and interfaces


Retain the chosen index or clustered handle on the FK node. Use existing index cursors and common-handle encoding for exact/prefix existence and cascade selection. Keep MATCH SIMPLE and missing-parent planning policy. Share DDL validation with the same key owner. Route all decoded rows through RowDecodeContext from the submitting StmtContext. Preserve Go collated-equal update suppression. Cascades mutate retained handles/preimages, eliminating full-table rediscovery. Avoid adding an alternate transaction or SQL interpreter.

## Validation and concrete commands


From `/workspace/tidb/rust`, source `/workspace/.cloud-setup/env.sh`, set CARGO_BUILD_JOBS=1, and run `cargo test --locked -p tidb-session --lib -- fk_access_batch --test-threads=1` before implementation. Afterward group the FK, statement rollback, DML and relevant table-reader suites in one Cargo invocation. Run `cargo check --locked --all-targets -p tidb-planner -p tidb-executor -p tidb-session -p tidb-server`, root `make lint`, and `cargo build --locked -p tidb-server`. Record commands/results in the durable receipt; failed probes stay visible. Required precommit and immediate prepush locked builds cannot be bypassed.

## Acceptance and limits


Collated references and common-primary prefixes follow Go key semantics, errors propagate, and matching cascades retain exact handles and submitting decode policy. Existing FK rollback and depth limits remain. Full Go packages, live multi-node shared locks, performance and crash recovery require separate evidence.

## Recovery


All source work stays in Cloud. Preserve concurrent changes and exact upstream destinations. No force push or cache purge. Test sessions are isolated. Revert only this batch's owned edits if an approach proves unsound; never remove a safety regression to obtain a pass.

## Decisions, discoveries and outcomes


The first grouped FK/rollback/multi-table run passes all 148 cases. Six live baseline probes reproduce five supported DML failures and the separate unchanged cluster ALTER-FK refusal. Add a typed storage-error regression before final validation. Inspection identified four competing scans, a swallowed error path, raw equality and missing execution metadata. Batch these through one access owner rather than adding separate fixes to each caller.


## Validation outcome


All 203 final grouped Rust cases pass, including the typed backend-error/IGNORE regression. Affected all-target checks, make lint and locked server build pass. Five supported real MySQL/unistore scenarios pass; the unchanged cluster ALTER-FK adapter refuses before this path, while session ALTER validation passes. Five obsolete scan/rediscovery helpers are removed. No broad finding or complete package is closed. Exact logs, hashes, commands and limits are in [the receipt](parity/current-audit/fk-access-batch-validation.json). Actual hook, fresh prepush build, remote SHA and saved environment checkpoint follow in the external final handoff.
