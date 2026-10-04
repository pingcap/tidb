# Complete shared mutation policy and conversion handoffs


This living ExecPlan follows root PLANS.md. Work in /workspace/tidb on hparser-integration, starting at 529c38ddd4f046a408bfe8d2082a62dcf672f505. Go master is freshly fetched at 93a01d31f6da205ae4bf376825293903a6899fdb; client-rust remains 19a56ccda1e128218cd33c69709038219aced9bc. Do not push. Preserve unmerged remote integration 219a0de48ec86ab988c536e587e150543c524948.

## Purpose / Big Picture


Maintain the connected T01/K03/E03 table mutation boundary: choose duplicate checks from the real transaction and statement policy, address moved rows by their physical partition keys, and preserve the existing datatype owner's typed conversion diagnostics. A lazy check defers uniqueness to locking or prewrite; it never waives uniqueness. A physical key includes partition ID as well as row handle. Generated and ordinary values must share conversion without inventing errors from a lossy event.

## Progress


- [x] Inspect instructions, refresh Go references and verify both managed Git remotes remain readable after OAuth reconnect.
- [x] Recheck the connected live sources and identify stale batch-map counts.
- [x] Add regressions together: five Rust failures and three real-server failures captured before implementation.
- [x] Implement transaction policy, physical partition/hidden-handle identity and shared conversion changes across callers.
- [x] Run grouped checks: 115 Rust cases pass; one retained zero-date warning mismatch reproduces on the unchanged server. Five final real-server assertions, affected all-target checks, lint, scoped formatting and locked build pass.
- [x] Normal local commit passed the actual locked-build hook; final commit identity and the evidence-only amendment outcome are recorded in the external final-handoff.json.
- [x] Update both finding registers and batch map: 86 tracked, 30 repaired, 56 unresolved (32 open, 24 partial); E03/T01/K03 remain partial.
- [ ] Verify the replacement recovery bundle and save/read back reusable Cloud configuration after the local commit; final-handoff.json records these post-commit operations.

## Context and Orientation


Go pkg/executor/insert.go and update.go distinguish normal INSERT, pessimistic UPDATE and IGNORE. pkg/table/tables/index.go and tables.go retain independent absence-check and assertion flags. Rust kv_table.rs and kv_table/index_entries.rs currently carry INSERT policy but force eager UPDATE checks. The shared cluster_storage.rs buffer already retains native per-key flags and statement rollback. Go table/column.go consumes typed Datum.ConvertTo errors. Rust tidb-datatype already provides convert_to_in_context; driver/write_cast.rs still reconstructs some errors from a single ScalarConversionEvent, losing precedence and multiple warnings.

## Plan of Work


Use existing library tests to reproduce transaction-view duplicate behavior and raw conversion identities together. Migrate destination checks and index rewrite callers to one policy, preserving eager IGNORE and optimistic UPDATE. Resolve source and destination physical keys from row data rather than searching all partitions for a repeated hidden handle. Connect supported contextual datatype conversions and preserve source warning order and caller-specific INSERT/UPDATE naming. Retain temporal and other explicitly unported diagnostic paths until their owners support them; do not claim full package acceptance.

## Concrete Steps


Activate source /workspace/.cloud-setup/env.sh in every shell. Cargo runs from rust/ with CARGO_BUILD_JOBS=1. Capture baseline and final output under /workspace/.cloud-setup/table-mutation-batch. Select all new tests with table_mutation_batch in one cargo test --locked -p tidb-executor --lib invocation. Group related table, storage, cast and session regressions at the final boundary. Run affected cargo check --locked --all-targets, root make lint, scoped rustfmt and git diff --check. Commit normally with core.hooksPath=hooks and TERM=xterm; cd rust && cargo build --locked -p tidb-server is mandatory in the actual hook.

## Validation and Acceptance


Prove failed-before/passed-after cases, preserved eager checks, local tombstones and deferred flags, partition-specific destination checks, and original typed conversion values/errors/warnings. Retain unrelated known failures and explicit incomplete package obligations. No full Go package, multi-node TiKV or performance claim follows from a focused pass.

## Idempotence and Recovery


Preserve source changes, native checkout and concurrent remote commits. Do not reset, force-push or bypass hooks. Restore only edits made in this batch if a candidate is rejected. Keep baseline logs and verify the replacement recovery bundle before replacing the old one. Only positively identified obsolete generated artifacts may be pruned for disk space.

## Interfaces and Dependencies


Use the existing StmtContext transaction mode/IGNORE policy, TableStorage and MutationBuffer. Use tidb-datatype ConversionContext and its ordered warning sink; do not introduce a second conversion engine or a new dependency.

## Surprises & Discoveries


Go partitionedTableUpdateRecord allocates a new hidden row ID when a heap row crosses partitions. Keeping the old ID and searching all partitions could silently replace an unrelated row; the before/after wire checks reproduce this. Go deliberately keeps optimistic UPDATE eager even when constraint_check_in_place is disabled. The current Markdown batch table still includes repaired O01; counts must derive from the JSON finding authority. Git read access succeeds after the user's reconnect; writes remain untested under no-push.

## Decision Log


Decision: maintain connected existing table owners in a single batch. Rationale: native parent work requires a published dependency revision, which no-push currently prevents; table mutation prerequisites can progress without publishing or competing implementations. Date: 2026-10-04.

## Outcomes & Retrospective


The connected mutation batch fixes duplicate-check policy, row preservation during partition moves, and diagnostic ownership for ordinary/generated writes. Five initial Rust regressions fail before the repair; final grouped runs pass 115 distinct Rust cases, with one retained pre-existing zero-date warning-text failure. The unchanged and final real-server binaries produce the same warning in that control. Five final real-server checks pass, including preserving both rows when hidden handles repeat across partitions. Two stale generated-column expectations were corrected from Go, preserving behavioral assertions. No parent finding or package is closed.

Exact commands, complete before/after wire results, log hashes, source contracts and remaining limits are in rust/docs/parity/current-audit/table-mutation-batch-validation.json. Local commit-hook, bundle and draft persistence results are recorded after commit in /workspace/.cloud-setup/table-mutation-batch/final-handoff.json; no push is authorized.

Revision 2026-10-04: completed the connected implementation and grouped validation; retained the independent zero-date failure and explicit parent/package gaps. Post-commit operational outcomes belong to the external handoff receipt.
