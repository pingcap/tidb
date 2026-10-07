# Share common-handle identity across remote readers

This living ExecPlan follows root PLANS.md.

## Purpose / Big Picture


A common handle is the encoded clustered primary-key tuple. Clean table lookups and ordered partition readers should retain this identity independently of projected output, as Go does. Ordinary, probe and merge readers must share row/chunk association, prefix/collation encoding and statement policy instead of refusing common handles into native BatchGet.

## Progress


- [x] Confirm clean66084, freshly fetched Go7a3 and active locked-build hook.
- [x] Trace request ranges, row/chunk handoff and ordered partition merge against Go executor and distsql owners.
- [x] Four baseline regressions failed; 212 neighboring cases and 11 wire controls passed.
- [x] Migrate shared identity and connected consumers; retire integer-only duplication.
- [x] Final grouped suite: 217 passed, zero failed/ignored. All-target checking, lint and locked build passed; both registers updated.
- [ ] Actual hook, fresh prepush build, remote verification and saved cloud checkpoint (external final-handoff.json).

## Context and Orientation


The shared builder in rust/crates/tidb-executor/src/kv_table/table_scan.rs refuses common handles. Its row and chunk finishers separately extract integers. RemoteCommonHandle already owns tuple prefix/collation/timezone encoding for staged merges. PartitionMergedPushdownStream also extracts integers independently and its producer rejects common handles. Go pkg/distsql/request_builder.go TableHandlesToKVRanges accepts tuples; executor/builder.go buildTableReaderFromHandles sorts them; model's CommonHandleCols supplies identity for table workers. Go table_reader.go merges sorted partition results. No complete Go package is accepted by this maintenance batch.

## Plan of Work


Reuse the existing tuple encoder for row and typed chunk values. Retain transport identity outside requested output and preserve virtual projection. Extend table handle range construction to common handles using the existing tidb-distsql builder. Connect ordered partition merges to that same identity. Keep dirty and partition-routed lookup fallbacks until their distinct route ownership is implemented. Extend existing remote_scan and table_scan tests rather than add a harness.

## Milestones


First run all new regressions and related controls in one executor target. Complete request, identity, projection and merge changes together before the final run. Then validate downstream compilation and publication gates once for the batch. Shared configuration consumers N03 and remote request policy O13 remain partial.

## Concrete Steps


From /workspace/tidb/rust, source /workspace/.cloud-setup/env.sh and export CARGO_BUILD_JOBS=1:

    cargo test --locked -p tidb-executor --lib -- common_reader reader_task_ remote_scan::tests:: access_path::tests:: kv_table::table_scan::remote_cursor_tests:: index_merge point_get --test-threads=1
    cargo check --locked --all-targets -p tidb-executor -p tidb-session -p tidb-server
    cargo build --locked -p tidb-server

Run make lint from repository root with the same environment. Commit through hooks/pre-commit, rerun the locked build immediately before an authorized normal push, and verify remote SHA. Never force-push.

## Validation and Acceptance


Baseline must fail for common-handle request admission, chunk association and ordered partition scans. Final results must match requested order and projection, retaining integer/hidden-handle controls. Fake coprocessor evidence does not establish live multi-node TiKV or performance. Add real MySQL controls where the existing unistore supports the selected shapes.

## Idempotence and Recovery


Preserve concurrent changes and build caches; use no worktree or cargo clean. Keep external logs under /workspace/.cloud-setup/common-reader-batch. Historical helpers are not startup instructions.

## Surprises & Discoveries


The existing tuple encoder and generic range owner already exist; refusals and duplicated consumers prevent their composition. Initial final compilation caught the broader RecordHandle partition variant; the shared extractor now returns TableHandle, accurately representing this nonpartition identity boundary. Real MySQL then caught pruned wire output dropping a key part for partition merging; response offsets and identity now share one remapping. The expanded partition regression and all required gates rerun on the final source.

## Decision Log


Use shared source owners and preserve unsupported partition-routed lookup fallback. Do not count this batch as parent closure or complete package transcreation.

## Outcomes & Retrospective


Four baseline failures are repaired. Final 217 Rust cases and 11 real MySQL controls pass; required checks, lint and locked production build pass. No parent finding or whole package closes. External final-handoff.json records subsequent publication gates.

## Artifacts and Notes


Go authority: separately fetched origin/master7a3dacb52efe58d28db360ae8639d8838c376544, exported under /workspace/.cloud-setup/go-master. Baseline TiDB66084c94f8ccee412fa6e16cc6c086bd2701887a; native8b752f9638ad157931725b66ffdc57e0465432a9 unchanged.

## Interfaces and Dependencies


Reuse RemoteCommonHandle, RemoteRowCursor, StagedHandlesLookup, FinishedLookupChunk, PartitionMergedPushdownStream and tidb_distsql::table_handles_to_kv_ranges. No dependency or native-client change.
