# Run a partition split through external sort and real TiKV ingest


This ExecPlan follows root `PLANS.md`. It implements an isolated executable experiment for the final design in `docs/design/2026-11-02-partition-global-sort.md`, not a SQL feature flag.

## Purpose / Big Picture


Run real TiKV MVCC records through the production simplesst external sorter and ImportSST protocol, then repair concurrent changes and build local/global indexes. Verify exact keys and values, old snapshot layout binding, write conflicts, and quarantine after a lost import receipt. Keep ordinary REORGANIZE behavior untouched.

## Progress


- [x] Read repository policy, design, DDL map, ingestor interfaces and existing ingest RPC implementation.
- [x] Implement isolated Go harness and local cluster runner.
- [x] Compile and pass focused unit checks; generate and diff-check package BUILD with the same Gazelle source. Full make bazel_prepare remains blocked at repository loading (recorded below).
- [x] Run end-to-end on owned local TiKV clusters, preserve result JSON and verify owned processes/endpoints stopped.
- [x] Self-review protocol/runner and record Ready verification scope and remaining integration/build gaps.

- [x] Follow-up: capture online index mutations with production temp encoding and persist group state transitions.
- [x] Follow-up: validate unique-key reuse, NULL, cross-partition movement, rollback and merge/foreground conflicts; fence Inactive, Running and Merging transactions.
- [x] Follow-up: complete larger online run and final evidence/limitations review.

- [x] 2026-11-12: export consistent pre-merge snapshots from real TiKV prototype, verified by a frozen standalone adapter replay.
- [x] 2026-11-12: replay snapshots through actual production DXF merge step and compare exact physical-row-derived indexes.
- [x] 2026-11-12: verify conflict retry, commit failure atomicity, duplicate rejection and replay on owned TiKV.
- [x] 2026-11-12: record exact tests, pass/fail evidence and remaining integration boundaries.

- [x] Scale follow-up: bounded gzip fixtures, full batched index lookup, configurable mutation count and timeout.
- [x] Scale follow-up: multi-Region real TiKV runs up to 200,000 seed rows; no sampling of correctness checks.
- [x] Scale follow-up: both large production replays, negative/rollback checks, implementation review and persisted evidence.

## Surprises & Discoveries


The baseline already exposes `simplesst.Writer` / `NewMergeKVIter` and the low-level Write/MultiIngest protocol. The normal ingest backend retries region jobs internally; the strict prototype instead sends each mutating RPC once and quarantines any unconfirmed attempt. This does not establish that the production backend currently satisfies the final strict-import contract.

The index merge worker in `pkg/ddl/index_merge_tmp.go` is unexported and tied to DDL/table context. The follow-up uses the existing `tablecodec.TempIndexValueElem`, decode/filter/version protocol with a standalone client-go merge adapter. It does not claim to execute the production merge worker. Per-group MVCC state writes fence old mutation transactions across phase changes; this coarse serialization is an experimental substitute for schema/MDL barriers.

## Decision Log


- Decision: Add a standalone tool under `tools/partition-reorg-prototype`, using client-go transactions and production TiDB row/index encoders. Do not modify SQL/planner/table mutation code for this experiment.
  Rationale: An executable storage experiment can test real ingest and transaction conflicts before spreading incomplete state changes across online SQL. Existing Python models remain useful but cannot establish this chain.
  Date/Author: 2026-11-10 / Codex.
- Decision: Restrict the experiment to one replica, small fixed integer RANGE refinements and implicit integer handles. The initial version quiesced foreground writes during the index phases; the online follow-up below removes this restriction using a prototype state barrier.
  Rationale: The first experiment exercises duplicate handles across old parents, nullable index encoding and exact row repair. The independent online temp-event adapter is now exercised; SQL ADD INDEX/MDL/DXF integration must not be claimed by this harness.
  Date/Author: 2026-11-10 / Codex.

- Decision: Continue with online temp-event capture and merge inside the independent harness. Reuse production temp codecs/filtering; keep SQL/DXF/MDL integration pending. Maintain completed local indexes while global indexes are built.
  Rationale: This removes the foreground-quiescence restriction during indexing and directly tests the final-layout index stream adapter without modifying production SQL semantics.
  Date/Author: 2026-11-10 / Codex.

## Outcomes & Retrospective


The storage experiment passes on actual Classic PD/TiKV v8.5.8. Current-code final reports include 32 rows/1 writer with empty child partitions and 8,192 rows/8 writers with 288 sorted files. Both pass ten checks including independent decoded index handle/PID row lookup. Earlier 2,000-row runs also pass. The observed duration is a small local experiment, not an online REORGANIZE performance result.

The prototype adds four Go files, generated BUILD.bazel, README and an owned-process Python runner under `tools/partition-reorg-prototype`. It uses existing dependencies and changes no SQL execution path or module file. SQL/DXF/MDL, production merge-worker integration, pessimistic allocator, GC, failover and general admission remain future implementation work. Independent online index capture/merge is added in the follow-up.

## Context and Orientation


`pkg/ingestor/simplesst/writer.go` creates sorted runs on external storage; `iter.go` merges them. `pkg/ingestor/ingestctrl/region_job.go` documents the existing Write and MultiIngest wire format. `pkg/tablecodec/tablecodec.go` encodes TiDB records and local/global indexes. The harness uses its own MVCC layout record, markers, repair gate and RPC journal in TiKV; these are not TiDB's SQL schema metadata or MDL implementation.

## Plan of Work


First implement fixed pure-split planning and row/index encoding with namespace checks. Then connect source MVCC scans, parallel sorted-run writers, globally merged output and one-shot SST import. Persist phase/intent/receipt state; replay deterministic foreground updates/deletes/moves and repair through current-source reads in transactions. Build indexes only after exact row equality; the online follow-up keeps records mirrored and captures index changes through phase transitions. Publish a layout record and verify old/current reads choose matching physical layouts. Inject a successful ingest with a deliberately lost receipt, quarantine it and rebuild fresh targets. Finally add an owned-process runner and documentation.

## Concrete Steps


From repository root, run `make bazel_prepare` after adding Go files. Compile/test the new tool only before broad checks. The runner will choose unused loopback ports, start installed PD/TiKV binaries in a new directory, wait for readiness, invoke the compiled tool, save JSON/logs and stop only those owned processes. Validation commands actually run:

    go test -tags=intest,deadlock ./tools/partition-reorg-prototype -run '^TestPrototype' -count=1
    go vet ./tools/partition-reorg-prototype
    /Users/joechenrh/go/bin/goimports -w tools/partition-reorg-prototype/*.go
    tools/bin/revive -formatter friendly -config tools/check/revive.toml ./tools/partition-reorg-prototype/...
    python3 tools/partition-reorg-prototype/run.py --rows-per-parent 1000 --workers 4
    python3 tools/partition-reorg-prototype/run.py --rows-per-parent 16 --workers 1
    python3 tools/partition-reorg-prototype/run.py --binary .codex-tmp/partition-reorg-prototype-upssxp04/prototype --rows-per-parent 4096 --workers 8
    python3 -m py_compile tools/partition-reorg-prototype/run.py
    make lint
    git diff --check

The new package has no failpoint/testfailpoint references or dependency, and its tests use no failpoint-based expectation; no instrumentation was enabled. Verification profile is Ready for the isolated prototype. Unit tests, vet, scoped revive, formatting, Python syntax and real-cluster reports pass. Full `make lint` exits 0 but emits pre-existing mixed-package errors under local-notes and macOS `find: illegal option -- n`; scoped revive compensates for the new package, without claiming repository-wide lint coverage.

Bazel preparation was attempted before compiling the new package:

    make bazel_prepare
    make bazel_prepare BAZEL_CMD_CONFIG=--fetch=false

The first remained at 0 loaded packages for 478 seconds and was interrupted. The offline retry identifies the unavailable `bazel_gazelle_go_repository_config` repository. This is not a passing full preparation gate. The same Gazelle v0.51.3 source already extracted by Bazel was used to generate only the added package, without editing generated output manually:

    go build -o /Users/joechenrh/code/pingcap/tidb-fix/.codex-tmp/partition-prototype-gazelle ./cmd/gazelle

That build ran in `/private/var/tmp/_bazel_joechenrh/4d81ea9057d4a72e00fd8a37edf2bfcb/external/bazel_gazelle`; from repository root:

    .codex-tmp/partition-prototype-gazelle -r=false tools/partition-reorg-prototype
    .codex-tmp/partition-prototype-gazelle -mode=diff -r=false tools/partition-reorg-prototype

The final command produces no diff. Full Bazel preparation/build remains unverified; no PR-readiness claim is made. Existing user changes elsewhere were preserved.

## Validation and Acceptance


Require byte-for-byte equality of projected source and new rows, complete local/global index maps with NULL and duplicate handles, real optimistic write conflicts for repair/DML and a closed repair gate, rollback leaving no source/marker changes, historical layout reads after cutover, and refusal to publish an uncertain generation. Source and target must never share IDs; lost receipt rebuild must allocate fresh IDs. Success is a passing real TiKV report, not compilation alone. No performance extrapolation from a small local run.

## Idempotence and Recovery


Every runner invocation owns a fresh cluster directory. An unknown RPC causes quarantine without retry; a pending persisted journal cannot be treated as unsent. The initial tool reports and aborts on unexpected storage/topology errors rather than silently restarting. Phase start is reserved once in MVCC; phase sealing rejects new RPC registration, and a published phase cannot be replayed. Publication checks a row-verification certificate, closed gate, all three seals and every stored RPC receipt. Certification currently occurs under controlled foreground quiescence in this harness; it does not replace the final design's online completion proof. The harness keeps cluster data isolated and records subprocess cleanup. It cannot resume an interrupted full SQL job.

## Artifacts and Notes


Final reports:

    .codex-tmp/partition-reorg-prototype-upssxp04/result.json
    .codex-tmp/partition-reorg-prototype-f9iefw9d/result.json

The first has 32 rows, 1 writer, 7 sorted files; the second 8,192 rows, 8 writers, 288 sorted files. Both record 14 mutating RPCs (separate Write/ingest intents), 3 default-CF and 7 write-CF SSTs, ten successful checks, owned_processes_stopped=true and pd_endpoint_stopped=true. Binary/component versions and commands accompany each report. Main experiment binaries were built from this checkout; PD/TiKV are installed v8.5.8 binaries. No server compiled from the previously used dirty TiDB binary is involved.

Use `.codex-tmp/partition-reorg-prototype-*` for local build/run artifacts. The existing worktree has unrelated user changes; do not reset, stage or commit them. Preserve generated metadata only for this new package and identify any generator drift.

## Interfaces and Dependencies


Use existing Go module dependencies only: client-go txnkv, kvproto ImportSST gRPC, TiDB tablecodec/types/model, simplesst, and local object storage. No new module dependency. The harness adds no background RPC retry loop and will explicitly disable gRPC retries. It supports one-replica API-v1 Classic TiKV only, with an exact expected cluster ID passed by the owned runner.

## Online index follow-up milestones


Milestone A introduces per-group Running/Merging/Direct state and atomic source/record/index event maintenance. Unique ownership is validated against a canonical fixture constraint key, not incomplete shadow indexes. Milestone B merges production-format temp history in fresh transactions, retries conflicts, preserves full global PID/handle identity, and prevents stale workers committing after group closure. Milestone C runs deterministic key-reuse/NULL/move/rollback schedules and bounded concurrent writers during sort/ingest/merge, then compares exact rows and indexes. Record actual outcomes before declaring completion.

## Online follow-up evidence and discoveries


The follow-up adds `online_index.go` and `online_scenarios.go`, adapts the mutation entrypoint, and adds focused temp-history and logical-row-oracle unit tests. It uses the production temp element codec, decoder, versions and FilterOverwritten. The production merge worker is private and bound to DDL context, so this tool implements an explicitly separate transaction adapter rather than changing production visibility or claiming full worker reuse.

Each generation initializes local/global groups to Inactive. Every mutation writes both group-state keys, including Inactive state, before commit. This closes the uncaptured-writer window at activation: old Inactive DML conflicts with Running activation; Running-only DML conflicts with Merging; stale merge workers conflict with Direct. Foreground operations maintain completed local indexes during global backfill. These coarse state writes serialize prototype writers and are not a production performance design or MDL implementation.

The oracle review found a real workload bug in the original prototype: after moving a source row to another parent, the subsequent supposed INSERT selected the now-occupied handle and was interpreted as an upsert. Both source and shadow lost logical identity 3 and still compared equal. Regression evidence `.codex-tmp/partition-reorg-prototype-7odvjlbz/result.json` fails with `logical row set mismatch: missing identity 3`, exit 1, and owned processes/endpoints stopped. Explicit INSERT now uses oldPID=0 to allocate around occupied source/shadow slots. The independent logical identity oracle and TestPrototypeLogicalRowOracle cover loss and duplicate logical rows; the corrected small run has 38 final rows from 32 seed rows plus six online fixture rows.

Online rollback checks the exact before/after KV footprint of a prepared then rolled-back mutation, including keys absent before the mutation. The fixture's canonical unique guard validates business uniqueness before shadow maintenance. The runtime workload covers multiple handles reusing one unique key, NULL/non-NULL transitions, cross-parent movement, and a separate goroutine issuing up to 64 ordinary updates per index group. A successful small online report is `.codex-tmp/partition-reorg-prototype-boo71von/result.json`: 15 checks, 114 background commits, 2 index-merge conflicts, 6 phase conflicts, owned processes/endpoints stopped. Background counts vary with scheduling.

The renewed `make bazel_prepare` attempt was capped at 45 seconds after remaining at zero packages; its log is `.codex-tmp/partition-prototype-bazel-online.log`. Offline preparation still fails on missing bazel_gazelle_go_repository_config. Same-version scoped Gazelle generation remains the metadata fallback; full Bazel validation remains incomplete.

## Online follow-up final validation


The larger current-code run is `.codex-tmp/partition-reorg-prototype-vky_52eq/result.json`: 8,192 seed rows, 8,198 final logical rows, 8 sort writers, 288 sorted files, 82 successful background DML commits, 2 deterministic merge conflicts and 6 phase conflicts. All 15 checks pass. The small current-code run `.codex-tmp/partition-reorg-prototype-boo71von/result.json` also passes with 32 seed / 38 final rows and empty child ranges. Each report verifies owned_processes_stopped=true and pd_endpoint_stopped=true. The failed-before-fix logical-oracle report is retained separately and is not counted as a passing run.

Commands for this follow-up (repository root):

    go test -tags=intest,deadlock ./tools/partition-reorg-prototype -run '^TestPrototype' -count=1
    go vet ./tools/partition-reorg-prototype
    tools/bin/revive -formatter friendly -config tools/check/revive.toml ./tools/partition-reorg-prototype/...
    /Users/joechenrh/go/bin/goimports -w tools/partition-reorg-prototype/*.go
    python3 tools/partition-reorg-prototype/run.py --rows-per-parent 16 --workers 1
    python3 tools/partition-reorg-prototype/run.py --binary .codex-tmp/partition-reorg-prototype-boo71von/prototype --rows-per-parent 4096 --workers 8
    .codex-tmp/partition-prototype-gazelle -r=false tools/partition-reorg-prototype
    .codex-tmp/partition-prototype-gazelle -mode=diff -r=false tools/partition-reorg-prototype
    make bazel_prepare
    make bazel_prepare BAZEL_CMD_CONFIG=--fetch=false
    make lint
    git diff --check

Ready profile is used for the independent prototype. Unit tests, vet, scoped revive, generated-package diff and real TiKV reports pass. No failpoints are referenced or enabled in this package. The full Bazel preparation and repository-wide lint coverage limitations recorded earlier remain: a renewed 45-second loading stall, missing offline repository config, and make lint exiting 0 with existing local-notes/find errors. No product Go code, module dependencies or unrelated user changes were modified.

Remaining implementation work is SQL mutation/transaction/planner integration, actual production DDL merge worker reuse, MDL replacing coarse per-group serialization, DXF task scheduling, source GC protection/retired-history GC, pessimistic allocator and general recovery/cleanup. Successful codec/adapter experiments do not certify these integrations. Large-data speedup remains unmeasured.

Revision: 2026-11-10 online temp-index adapter, phase barriers, concurrent writer, exact rollback footprint and logical-row regression oracle implemented and verified; previous quiescent experiments remain historical evidence only.

## Production merge-step integration milestone (2026-11-12)


The user requires sufficient behavioral validation after every increment. The next increment executes the existing `mergeTempIndexExecutor.RunSubtask` and its operator/worker pipeline, without exposing a new product API or enabling SQL REORGANIZE. The standalone tool exports a single-TS snapshot of target records, normal index KVs and production-format temp history before its own merge. A same-package DDL integration test reconstructs the table projection and executes actual production subtasks on those bytes. Expected normal indexes derive from final snapshot rows, not the prototype merge algorithm.

The test matrix must include small batch boundaries, local/global unique/nonunique/NULL, repeated handles across partition IDs, missing-partition admission, real transaction conflict retry, injected nonretryable commit failure with unchanged data, and successful replay after failure. Default tests use fresh mock stores; an owned real TiKV replay mode verifies the same production step on actual storage. Source/row equality and expected index sets are mandatory; compilation is only a prerequisite.

No production visibility/refactor is planned. New test files and generated BUILD entries follow Bazel/failpoint/Ready rules. Current full Bazel and repository lint limitations are still tracked and must not be represented as passes.

## Go concurrency constraint (2026-11-12)


The user requires Go concurrency 2. All subsequent Go build/test commands use `-p 2` and `GOMAXPROCS=2`; directly executed Go test binaries also use GOMAXPROCS=2. The production test binary compilation already in progress used `-p 2` (its earlier GOMAXPROCS setting was 4); do not represent that earlier setting as 2.

## Production merge-step evidence in progress


The first production test executable passed 99 unistore cases / 268 actual subtasks, including injected commit failure after staged mutations, conflict retry, idempotent replay and missing-partition rejection. The initial real-store run `.codex-tmp/partition-reorg-prototype-opo3j4jp/result.json` failed before mutation at the ownership guard: the driver UUID is `tikv-<clusterID>/<keyspace>`, with a trailing slash for this empty-keyspace Classic run (`pkg/store/driver/tikv_driver.go`). The test had assumed the bare client UUID. The guard is corrected to the exact driver format; failed execution is retained and not counted as a passing replay.

The source was subsequently expanded with delete-only and m-delete overwrite cases and explicit three-way snapshot verification. A final test binary is being compiled from a frozen hash of both added DDL test files, stored in `.codex-tmp/partition-production-final-source-hashes.json`. Earlier binaries do not establish validation of these later assertions. No more Go source edits are planned until that binary and actual case counts are checked.

The six existing SQL regressions in the instrumented `indexmerge.test` executable passed: TestAddIndexMergeProcess, TestAddIndexMergeVersionIndexValue, TestAddIndexMergeDeleteNullUnique, TestAddIndexMergeReplaceDelete, TestAddIndexMergeDeleteDifferentHandle, and TestAddUniqueIndexFalsePositiveDuplicate. Command output is `.codex-tmp/partition-prototype-indexmerge-regression.log`.

Full Bazel preparation was retried: Gazelle and repository updating completed, but the later tazel dependency build did not finish. The interrupted DEPS output removed existing nextgen/intest build tags; since DEPS was clean at turn entry and no dependency was added, only this intermediate generator output was restored to its original bytes. Added test metadata is regenerated with the same-version scoped Gazelle. Complete Bazel preparation remains unverified.

## Production merge-step final validation (2026-11-12)


This increment adds two test-only files under pkg/ddl, generated test BUILD metadata, a pre-merge snapshot exporter under the standalone tool, physical NULL row encoding, and optional fresh-cluster replay in the Python runner. Production DDL execution files and SQL REORGANIZE routing are unchanged. The external test factory avoids importing the production driver back into the internal ddl test package; it introduces no public product API.

The frozen-snapshot comparison is three-way: `applyTempHistory` replays the standalone adapter on exactly the captured bytes; exported expected index KVs derive from that same snapshot's rows; the consumer independently decodes physical nullable columns and executes `mergeTempIndexExecutor.RunSubtask` through its real scan/operator/worker/sink pipeline. All three normal-index sets must agree. Root tests additionally check unchanged row bytes and exact failure rollback of normal/temp KVs. Replay is offline with respect to the exported foreground stream; only the explicit conflict hook introduces a live competing transaction inside the production step.

Final evidence:

    .codex-tmp/partition-prototype-production-final-unit.json
    .codex-tmp/partition-prototype-production-final-unit.log
    .codex-tmp/partition-reorg-prototype-sv9rgrfo/result.json
    .codex-tmp/partition-reorg-prototype-sv9rgrfo/output/merge-fixtures.json
    .codex-tmp/partition-reorg-prototype-vo7w4e4j/result.json
    .codex-tmp/partition-prototype-indexmerge-regression.log

The final unistore matrix passes 147 cases / 412 actual production subtask executions. It covers batch 1/2/16 and CPU 1/2, local/global nonunique/unique/NULL, repeated handles in different PIDs, unique reuse, b/m/d filtering, delete-only obsolete events, m-delete overriding b-put, duplicate rejection, idempotent replay, missing-partition rejection, genuine write-conflict retry, and a nonretryable commit error after staged index/temp modifications followed by successful replay.

The real TiKV consumer passes 15 cases / 124 actual production subtask executions on the two large exported snapshots. The producer uses 8,192 seed rows / 8,198 final rows, 128 background commits, and 312 sorted files. Its local/global snapshot row counts are 8,195 and 8,198. Both producer and consumer verify owned_processes_stopped=true and pd_endpoint_stopped=true. The initial wrong-format ownership-guard failure is retained separately and never counted as a pass.

Six existing SQL regressions pass in the separate instrumented indexmerge.test executable: merge process, versioned temp values, NULL-unique deletion, replace/delete, deleting a different handle, and false-positive unique detection. Exact binary source hashes were frozen before the final DDL compilation; those hashes still match both added test files. Earlier test binaries that predate the final assertions are not used as final-source evidence.

Commands actually run from repository root (Go compilation is package-parallelism 2; commands after the user's request also use GOMAXPROCS=2):

    GOMAXPROCS=2 go test -p 2 -tags=intest,deadlock ./tools/partition-reorg-prototype -run '^TestPrototype' -count=1
    GOMAXPROCS=2 go build -p 2 -o .codex-tmp/partition-prototype-fixture-export ./tools/partition-reorg-prototype
    GOMAXPROCS=2 python3 tools/partition-reorg-prototype/run.py --binary .codex-tmp/partition-prototype-fixture-export --rows-per-parent 4096 --workers 8
    GOGC=50 GOMAXPROCS=2 ./tools/check/failpoint-go-test.sh pkg/ddl -c -o /Users/joechenrh/code/pingcap/tidb-fix/.codex-tmp/partition-production-tests/ -p 2 . ./tests/indexmerge
    GOGC=50 GOMAXPROCS=2 ./tools/check/failpoint-go-test.sh pkg/ddl -c -o /Users/joechenrh/code/pingcap/tidb-fix/.codex-tmp/partition-production-tests/ddl-final.test -p 2
    GOMAXPROCS=2 PARTITION_PROTO_REPORT=/Users/joechenrh/code/pingcap/tidb-fix/.codex-tmp/partition-prototype-production-final-unit.json .codex-tmp/partition-production-tests/ddl-final.test -test.run '^TestPartitionPrototypeProductionMerge$' -test.count=1 -test.parallel=2 -test.timeout=5m -test.v
    GOMAXPROCS=2 python3 tools/partition-reorg-prototype/run.py --production-test-binary .codex-tmp/partition-production-tests/ddl-final.test --replay-fixtures .codex-tmp/partition-reorg-prototype-sv9rgrfo/output/merge-fixtures.json
    GOMAXPROCS=2 .codex-tmp/partition-production-tests/indexmerge.test -test.run '^(TestAddIndexMergeProcess|TestAddIndexMergeVersionIndexValue|TestAddIndexMergeDeleteNullUnique|TestAddIndexMergeReplaceDelete|TestAddIndexMergeDeleteDifferentHandle|TestAddUniqueIndexFalsePositiveDuplicate)$' -test.count=1 -test.parallel=2 -test.timeout=5m -test.v
    GOMAXPROCS=2 go vet -p 2 ./tools/partition-reorg-prototype
    GOMAXPROCS=2 tools/bin/revive -formatter friendly -config tools/check/revive.toml pkg/ddl/partition_merge_proto_test.go
    GOMAXPROCS=2 tools/bin/revive -formatter friendly -config tools/check/revive.toml pkg/ddl/partition_merge_store_external_test.go
    GOMAXPROCS=2 tools/bin/revive -formatter friendly -config tools/check/revive.toml ./tools/partition-reorg-prototype/...
    GOMAXPROCS=2 .codex-tmp/partition-prototype-gazelle -r=false pkg/ddl tools/partition-reorg-prototype
    GOMAXPROCS=2 .codex-tmp/partition-prototype-gazelle -mode=diff -r=false pkg/ddl tools/partition-reorg-prototype
    GOMAXPROCS=2 GOFLAGS='-p=2' make lint
    git diff --check

The canonical failpoint wrapper compiled the test executables with instrumentation and automatically restored all source afterward; directly executing those instrumented binaries preserves the runtime failpoint hooks. Final failpoint-state refcount is zero. Instrumentation walks the existing worktrees directory's approximately 100,000 Go files, so instrumentation/build wall time is not a merge performance measurement. Verification profile is Ready for this increment. Full Bazel preparation and repository-wide lint coverage remain limited as recorded above; scoped generated metadata and lint checks cover the new files.

Not verified: SQL REORGANIZE admission, task-store loading of private target projections, MDL replacing the prototype barrier, full DXF task scheduling/owner failover, continuous SQL foreground load through this production step, generation/range fencing around production merge, real GC, multi-region/multi-replica faults, and large-data performance. These remain explicit implementation/release gates. No PR-readiness or product feature-completion claim is made.

Revision: 2026-11-12, actual production merge-step snapshot integration tested on unistore and owned real TiKV, source/metadata/failpoint cleanup audited, user Go concurrency preference persisted.

## Scale validation milestone


The user requests larger-scale testing and clear, concise implementation, with correctness first. Preserve every full row/index/logical-identity check. Replace only the concrete scale bottlenecks: per-entry remote index lookup becomes bounded BatchGet; snapshot artifacts use standard gzip JSON with a decompressed size bound; test sizes and mutation counts are bounded explicit CLI settings. Keep Go -p 2/GOMAXPROCS=2 and one owned cluster at a time.

The controlled test topology has fixture handles near 201,000 and identities near 1,001,000. Cap seed rows at 100,000 per old parent (200,000 total), keeping those fixture namespaces disjoint. Move a seed handle near the occupied range end so a synthetic linear allocator scan does not dominate the test; logical identity is preserved. Multi-Region lost-receipt validation must accept a verified nonempty applied subset while permanently forbidding publication, then validate a full fresh-generation rebuild. Do not mistake partial apply for all rows imported.

Run small/mid/large tiers, vary sort workers and increase foreground writes, then replay large frozen snapshots through the production merge step. No sampled checking or performance extrapolation to hundreds of GB. Functional success does not establish production SQL/DXF/MDL/GC readiness or absence of all bugs.

## Scale implementation and first regression evidence


The scale path keeps the original protocol, but uses full BatchGet index-to-row validation in batches of 128 and gzip JSON snapshots. The production reader retains legacy plain JSON support and bounds decompressed input at 1 GiB, rejecting corruption, trailing JSON and oversized trailing whitespace. Snapshot writes use a temporary file and rename after gzip/file close. No correctness check samples the data.

Background updates now operate round-robin over eight fixed seed handles, move each between children across rounds, and complete the requested iteration count on successful runs. The independent terminal-value check verifies each handle's last intended write. Error paths cancel and join the writer. This keeps the workload from turning into an accidental quadratic single-hot-key history benchmark while preserving b/m history, key movement and merge conflicts.

The old after-apply fault oracle was reproduced failing on a genuinely multi-Region cluster: `.codex-tmp/partition-reorg-prototype-75z1jyfk/result.json` reports 38 expected versus 9 applied rows, exit 1, and clean shutdown. The bug was an assumption in the test, not publication of incorrect data. A lost first-region receipt need not mean later regions were sent. The revised oracle accepts only a nonempty, byte-exact subset, rejects unknown/corrupt/repeated keys, forbids publication, and requires full rebuilding of fresh targets. The small corrected run `.codex-tmp/partition-reorg-prototype-6ghr1uto/result.json` passes all 16 checks with 15 namespace splits, 10 ingest regions, 9 quarantined applied rows, 256 intended background commits and 38 correct final rows.

Full make bazel_prepare completed successfully during this scale increment (logs `.codex-tmp/partition-prototype-scale-bazel.log` and `.codex-tmp/partition-prototype-scale-bazel-final.log`); earlier blocked preparation attempts remain historical evidence. Final generated metadata must still be diff-checked after all changes. Go -p 2 / GOMAXPROCS=2 remain the normal build/test settings.

## Scale contention regression


The 40,000-row / 1,024-updates-per-group / multi-Region run reproduced starvation in the prototype's old immediate-256-retry loop: `.codex-tmp/partition-reorg-prototype-15vnc8ua/result.json` fails with foreground retry exhausted on the global state key. It does not count as a passing run; owned processes and PD endpoint were stopped.

The three duplicated retry loops are replaced by one context-bounded helper with a 1–20 ms exponential delay. Only client-go's confirmed ErrWriteConflict is retried, with a fresh transaction each attempt. Unknown commit/network errors return immediately; SST RPC retry policy is unchanged. Unit tests cover successful conflict retry, one-shot unknown failure, cancellation during conflict backoff and no operation after cancellation. This is an experimental fairness fix for coarse state-key serialization, not a proposed production MDL substitute.

The workload moves a seed handle near the occupied range end to avoid an O(N) synthetic allocator probe in the scale test. Full independent logical-ID validation is retained. Writers are round-robin over eight existing handles, with per-handle terminal-value checks and exact requested completion counts on success. The scale cap keeps reserved scenario namespaces disjoint from seeds. No new data identity mapping was introduced.

## Scale producer results


After the contention fix, all three complete producer profiles pass all 16 checks. Machine-readable commands/results are `.codex-tmp/partition-scale-runs.json`:

    mid: .codex-tmp/partition-reorg-prototype-nmruolq2/result.json
    large-16: .codex-tmp/partition-reorg-prototype-goy716qr/result.json
    large-1: .codex-tmp/partition-reorg-prototype-znf8hv84/result.json

Mid uses 40,000 seed rows, 8 sort workers and 2,048 completed foreground updates; both large profiles use 200,000 seed rows, 8,192 completed updates and respectively 16/1 sort workers. Final logical row counts are 40,006 / 200,006 / 200,006. Each run touches 10 distinct ingest Regions after 15 namespace splits. The quarantined generation contains an exact applied subset of 9,994 / 49,994 / 49,994 rows and is never published. Source/target and row-derived/actual local/global indexes agree byte for byte with no sampling. The two large final row digests are identical: a5f5a3a7e6da65b87fa87fb6be3f3c06b19e4afd18512af444a6b65f17a4a438.

All requested foreground commits complete; independent per-handle terminal values are checked. Successful producer reports verify owned processes/endpoints stopped. The data has a 512-byte payload per row and remains synthetic; timings include validation/export and are not a product speedup benchmark. Frozen local/global gzip snapshots retain both complete physical row data and full index/temp inputs for production replay.

## Scale production replay evidence in progress


The first large frozen replay, produced with 16 sort workers, passes on real TiKV: `.codex-tmp/partition-reorg-prototype-0024q64k/result.json` records 15 cases / 124 actual production merge-step executions with unchanged rows, exact normal-index equality, idempotent replay, duplicate/missing-partition rejection and commit-failure/conflict tests. Its owned processes and PD endpoint stopped. The second large snapshot replay is still running and is not counted as passed yet.

The instrumented scale test binary passes the original 147-case / 412-subtask unistore matrix plus TestPartitionPrototypeFixtureCodec (plain/gzip, checksum corruption, trailing JSON and decoded-size rejection). The six selected existing SQL index-merge regressions were rerun successfully; log `.codex-tmp/partition-scale-sql-regressions.log`. All Go compilation uses -p 2 / GOMAXPROCS=2; prebuilt binaries run with GOMAXPROCS=2.

The 16-worker / 1-worker large gzip files are 16,353,137 / 16,352,261 bytes; decoded JSON lengths are 515,216,103 / 515,220,109 bytes, below the 1 GiB decoded bound. They contain full rows and index/temp history, not samples. The differing frozen histories are replayed independently even though the final row digests agree.

## Scale validation complete


Both large frozen replays pass: `.codex-tmp/partition-reorg-prototype-0024q64k/result.json` and `.codex-tmp/partition-reorg-prototype-cv6vk50d/result.json`, each 15 cases / 124 production subtask executions and owned processes/endpoints stopped. Full commands are in `.codex-tmp/partition-scale-production-replays.json`. The compressed-reader production test binary matches `.codex-tmp/partition-scale-ddl-source-hashes.json`; no test source changed after compilation.

A concise Chinese report with profile table, failures/fixes, exact commands and limitations is [scale validation results](2026-11-12-partition-reorg-scale-results.md). Final commands include the canonical failpoint wrapper with -c/-p 2, the 147-case production matrix plus fixture codec checks, the prototype unit suite including subset/retry regressions, two 200,000-row real TiKV replay runs, six existing SQL corner cases, full make bazel_prepare, tool vet, scoped revive and make lint. All complete successfully within their reported scopes; the pre-existing root lint coverage warnings remain. Failpoint state is restored and no owned server remains.

Revision: larger validation completed with 200,000 rows, 8,192 foreground commits per large run, multi-Region partial-apply faults, cross-worker equality and two production replays. No sampling, no production-path enablement and no new row-identity mapping.

## Retired-worker admission counterexample (2026-11-13)


Next integration gate: production merge transaction fencing. The owned-cluster negative fixture `tools/partition-reorg-prototype/testdata/retired-merge-negative.json` deliberately supplies Public/Inapplicable index metadata, no live rows or normal indexes, and a stale b-version nonunique put. Its adapter_verified flag bypasses the replay harness provenance check solely for fault injection; this is NOT a successfully exported/adapter-verified snapshot. Expected empty outputs express the required retired-stage no-write contract.

Actual command:

    GOMAXPROCS=2 python3 tools/partition-reorg-prototype/run.py --production-test-binary .codex-tmp/partition-production-tests/ddl-scale.test --replay-fixtures .codex-tmp/partition-retired-negative.json --timeout-seconds 300

Result `.codex-tmp/partition-reorg-prototype-ln761qgn/result.json` is intentionally FAIL, exit 1. All six batch 1/2/16 × CPU 1/2 cases execute the actual production step and then detect one orphan normal-index KV instead of an empty set. Worker logs show added=1/scanned=1. Three unrelated built-in control cases pass. Both owned process/endpoint cleanup fields are true. An earlier draft failed on empty-versus-nil slice representation before executing the worker; that draft is not evidence of this counterexample.

This is a deliberately out-of-lifecycle subtask invocation. It proves a missing integration prerequisite for the proposed REORGANIZE path, not reachability of a corruption bug in current ADD INDEX scheduling. No product behavior was changed. WIP validation applies: this is failing-contract evidence, not a completed fence implementation.

Required implementation: persist job/generation/group/range identity; the merge transaction reads the matching Open fence and writes a nonce to that same key alongside normal/temp mutations. Owner closure writes Closed to the same key; late transactions conflict, retries read Closed and stop, and missing fences never reopen. Range completion and closure must be distinct durable facts. Initialization-only metadata checks cannot close the read/commit race. Preserve existing ADD INDEX behavior unless the new protocol explicitly supplies a fence.

Acceptance before the next integration milestone: reject fresh attempts after closure, conflict with already-open transactions, re-read fence after retry, reject stale generation/range identity, distinguish abort closure from successful completion, and rerun the production replay/SQL regression matrix. The fence implementation and these tests remain outstanding; SQL/DXF task admission must remain disabled for the new path meanwhile.

## Existing ADD INDEX lifecycle audit (2026-11-13)


The user requested further investigation of actual ADD INDEX reachability. [The audit](2026-11-13-add-index-late-merge-audit.md) records the normal success barriers and a concrete reassignment / retained b-temp / post-delete fresh-transaction candidate. This is source-supported risk analysis, not a completed scheduler reproduction or a confirmed existing corruption bug. Dynamic lifecycle testing remains outstanding.

## Skipped unique-temp cleanup fix follow-up


User Go concurrency is now 1 (superseding prior settings). Both production merge workers consume skipped validated temp keys transactionally. Before/after regressions and a real SQL ADD UNIQUE INDEX test confirm the cleanup omission and fix. Issue https://github.com/pingcap/tidb/issues/73115 records this narrow finding without asserting unproven migration-induced corruption. See the [lifecycle audit](2026-11-13-add-index-late-merge-audit.md) for commands, validation and outstanding reachability work.

## Explicit partition input integration


The user directs REORGANIZE implementation to continue without blocking on issue #73115. BackfillTaskMeta gains optional PartitionIDs; empty preserves existing ADD INDEX behavior. Both scheduler and executor use getBackfillTaskTable. Nonempty selection is restricted to REORGANIZE jobs and constructs a cloned RANGE read projection containing only requested AddingDefinitions or unchanged Definitions, and selected index IDs. Dropping/unknown/duplicate PIDs, missing/duplicate indexes and old mapped global indexes are rejected. No REORGANIZE task-submission or SQL publication path is enabled yet.

Tests must verify private targets and untouched global inputs, exclusion of old PIDs, input rejection, clone/column-bound non-aliasing, old task metadata compatibility and actual merge executor operation with the projection. Go concurrency 1. Issue investigation proceeds independently, not as an implementation blocker.

## Explicit partition input validation outcome


Implemented optional BackfillTaskMeta.PartitionIDs and a shared scheduler/executor loader. The projection clones metadata, selects AddingDefinitions/unchanged Definitions, filters EleIDs, clears transition routing metadata in the private view, and requires active RANGE REORGANIZE with unchanged logical table ID. Old dropping partitions, duplicate/unknown PIDs, absent/duplicate index IDs and old/unmapped global IDs are rejected. Public metadata and bound slices remain unchanged. An absent field in legacy JSON retains the existing path.

Tests passed: TestPartitionBackfillReadProjection (including actual production merge execution on target PID 9201, local subset, final-layout inputs, metadata immutability and replacement-global acceptance/rejection), plus TestPartitionPrototypeProductionMerge, TestPartitionPrototypeFixtureCodec and TestAddIndexSkippedTempCleanup. Logs are .codex-tmp/partition-view-tests.log and .codex-tmp/partition-view-boundary-tests.log. Canonical failpoint wrapper uses GOMAXPROCS=1, -p 1, -count=1, -timeout=10m. Source cleanup returns refcount 0.

Commands:

    GOMAXPROCS=1 ./tools/check/failpoint-go-test.sh pkg/ddl -p 1 -run '^(TestPartitionBackfillReadProjection|TestPartitionPrototypeProductionMerge|TestPartitionPrototypeFixtureCodec|TestAddIndexSkippedTempCleanup)$' -count=1 -timeout=10m
    GOMAXPROCS=1 ./tools/check/failpoint-go-test.sh pkg/ddl -p 1 -run '^TestPartitionBackfillReadProjection$' -count=1 -timeout=10m
    GOMAXPROCS=1 GOFLAGS='-p=1' make bazel_prepare
    GOMAXPROCS=1 GOFLAGS='-p=1' make lint

Ready scope is this internal input-selection increment. Full Bazel preparation and scoped revive/generated metadata checks pass; root make lint still has the existing local-notes mixed-package/macOS find coverage limitation. No REORGANIZE task submission or SQL-path enablement is implemented by this increment. Generation/phase/capability-version admission, canonical PID ordering/pure-split validation at task creation, GC/read leases and full coordinator recovery remain required before enabling it. Issue #73115 is not an implementation blocker and is tracked separately.
