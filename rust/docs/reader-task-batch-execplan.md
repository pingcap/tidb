# Share table request policy across reader tasks

This living ExecPlan follows root PLANS.md.

## Purpose / Big Picture


Ordinary double reads, index-join probes and index-merge table tasks must carry the same statement policy and Go response estimates. Table tasks must restore requested handle order even when region results complete out of order. A handle identifies a stored record; a double read first reads index handles and then fetches table rows.

## Progress


- [x] Confirm clean integration 1eaa91e142, refresh unchanged Go master 7a3dacb52e and read instructions.
- [x] Trace ordinary, probe and merge producers and the shared table request/response boundary.
- [x] Four regressions fail before production changes; 43 selected and 64 point/fallback controls pass.
- [x] Migrate connected request policy and response ordering; retain unsupported-shape fallbacks. Final grouped suite: 214 passed, zero failed/ignored.
- [x] Run grouped tests, all-target checking, lint and locked server build.
- [ ] Update registers and receipt; commit through actual hook, fresh build before push, verify remote and save checkpoint.

## Context and Orientation


Go pkg/executor/builder.go::buildNoRangeIndexLookUpReader uses GetAvgTableRowSize for both index and table estimates. Rust driver/physical_builder.rs instead uses total index bytes for ordinary reads. Go index_merge_reader.go::buildFinalTableReader creates a table request and scales its width by task handle count; Rust IndexMergeReaderExec constructs HandleSourceExec, which only performs BatchGet. Shared KvTable table_scan.rs finish_rows_by_handles/finish_lookup_chunks_by_handles assumes sorted input handles imply ordered remote output, although requests allow unordered completion.

## Plan of Work


Use one physical-builder helper for ordinary/probe reader statement estimates. Give merge final table tasks their retained table width and scan identity, and opt their existing handle source into the shared table request path while point gets retain their native policy. Preserve local/dirty/common/partition fallback behavior. Carry statement row decoding through the shared table request builder, including worker clones. Remove obsolete staging aliases only after callers migrate. Restore handle order from actual returned handles for both row and chunk results. Extend nearest existing suites, including integer and hidden handles and chunk order permutations.

## Milestones


First reproduce request and ordering failures with one selected executor suite. Then complete producer, source and response changes before one final grouped run. Boundary validation must prove existing point/merge/probe behaviors remain correct and required build gates pass. Complete Go packages remain unaccepted.

## Concrete Steps


From /workspace/tidb/rust, source /workspace/.cloud-setup/env.sh and export CARGO_BUILD_JOBS=1, then run:

    cargo test --locked -p tidb-executor --lib -- reader_task_ remote_scan::tests:: access_path::tests:: kv_table::table_scan::remote_cursor_tests:: index_merge point_get --test-threads=1
    cargo check --locked --all-targets -p tidb-executor -p tidb-session -p tidb-server
    cargo build --locked -p tidb-server

From root run make lint in the same environment. Actual hooks/pre-commit must build the locked server. Repeat the locked build immediately before every normal push. Never force-push.

## Validation and Acceptance


Before edits, ordinary index estimates differ from table width, merge table requests are absent, and sorted requested handles return in remote completion order. After edits, request captures show correct estimates and statement flags, row/chunk results restore handle order, and existing related suites pass. Fake coprocessor tests do not establish live TiKV placement or performance. No whole-package acceptance or count decrease is implied by this existing-owner batch.

## Idempotence and Recovery


Preserve concurrent changes and cached build artifacts. No worktrees or cargo clean. Keep logs under /workspace/.cloud-setup/reader-task-batch. Only retire task-local helpers after recording hashes; never replay historical cleanup paths.

## Surprises & Discoveries


The previous probe fix exposed ordinary estimate drift. Merge final tasks bypass the existing table request owner. Sorted request handles do not prove ordered responses.

## Decision Log


Repair the shared request-to-task lifecycle across N03/O13. Reuse existing owners and explicit fallback gates; don't claim missing partition/common-handle remote support repaired by a narrower route.

## Outcomes & Retrospective


Four regressions failed before repair; 214 distinct Rust cases and 8 real MySQL controls pass after repair. Required checking, lint and locked build pass. Register counts remain 30 repaired and 56 unresolved. Publication completion belongs to external final-handoff.json after the actual hook, fresh build and remote check.

## Artifacts and Notes


Source authority is separately exported Go7a3 under /workspace/.cloud-setup/go-master. External logs and final publication evidence will be stored with the batch.

## Interfaces and Dependencies


Reuse PushdownStatementContext, RowDecodeContext, KvTable::build_table_reader_from_handles and existing HandleSourceExec/IndexMergeReaderExec. No native-client or dependency changes are planned.

Review follow-up: removing the small-batch shortcut also orphaned get_rows_by_handles_prepared_with_context. Its projection/duplicate/missing-row regression now runs through the live projected fallback; shared prepared point decoders remain. Three duplicate handle-source constructors, the unused staging alias and DirectRows handoff state are removed. The pruned remote output walks stored-column positions directly. This extends the initial request migration without changing point-get or unsupported-shape contracts. N03 is the consumer/configuration root advanced alongside O13; E03 write-streaming acceptance is not claimed. The complete final suite was rerun after cleanup.
