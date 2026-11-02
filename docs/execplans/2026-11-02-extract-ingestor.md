# Extract standalone SST ingestion from the local backend

This ExecPlan is a living document maintained according to repository-root PLANS.md.

## Purpose / Big Picture

Global sort stores intermediate sorted key/value data in external storage. Its SST ingest controller should not create a local sorting directory or initialize a local engine manager. Extract an Ingestor that owns ingest clients, throttling, workers, and their memory pool. Preserve local sorting, duplicate detection, dynamic concurrency, and caller ownership of the PD client.

## Progress

- [x] Read ingestor package contract, local backend, engine manager, worker creation, production callers, and repository validation instructions.
- [x] Check requested ownership split; identify unconditional sort-directory preparation in IMPORT INTO and DDL cloud callers.
- [x] Extend focused coverage to exercise external ingestion independently of local engines and real classic writes/NextGen worker creation.
- [x] Extract Ingestor and compose it into Backend; migrate existing tests according to their responsibilities.
- [x] Switch global-sort production callers to standalone Ingestor; preserve FROM SELECT local engines even with an inherited cloud URI.
- [x] Run scoped classic/NextGen tests, required lint, inspect generated metadata, and review ownership/caller routing.

## Surprises & Discoveries

The engine manager's memory pool serves local writers, classic writes, and NextGen workers. Ingestor now owns one pool and the manager borrows it. Manager close leaves the pool alive; Ingestor close destroys it. Backend memory reporting adds the manager's engine bytes and the shared pool exactly once.

Extracting the core alone is insufficient: IMPORT INTO prepares its directory before NewBackend, and DDL cloud import constructs a BackendCtx that also prepares the directory. Both callers need migration.

## Decision Log

- Decision: Embed *Ingestor in Backend and retain BackendConfig for this refactor rather than introduce a second overlapping config API.
  Rationale: Preserve existing config literals and promoted controls; isolate resource ownership without unrelated configuration churn.
  Date/Author: 2026-11-02, Codex.
- Decision: Keep lazy TiKV KVStore construction and duplicate-controller assembly on Backend.
  Rationale: These serve local duplicate detection rather than SST ingest control.
  Date/Author: 2026-11-02, Codex.
- Decision: Ingestor owns one pool borrowed by the engine manager.
  Rationale: Independent review identified doubled cache retention and incomplete LastAlloc tracking with separate pools. Borrowing preserves cache capacity and allocator tracking while supporting CloseEngineMgr independently.
  Date/Author: 2026-11-02, Codex.

## Outcomes & Retrospective

Implementation and Ready validation are complete. Core classic and NextGen tests passed. DDL and DXF classic caller checks passed; the final classic importer/core supplemental run passed after correcting a codec-identity test assertion. All four caller packages passed with NextGen tags. make lint and final diff checks passed. No SQL semantics, SST retry behavior, durable DDL job state, or region-splitting policy is intended to change.

## Context and Orientation

pkg/ingestor/ingestctrl/local.go currently combines BackendConfig, Backend construction, local engine adapters, and engine-independent SST ingestion. engine_mgr.go owns Pebble engines and duplicate data. region_job.go and localhelper.go implement writes, ingest RPCs, and split/scatter using Backend fields. job_worker.go creates classic and NextGen worker pools. pkg/executor/importer/table_import.go and pkg/dxf/importinto/task_executor.go serve IMPORT INTO; pkg/ddl/backfilling_import_cloud.go serves distributed add-index cloud ingestion.

PD is the placement driver. A caller-owned codec PD client encodes keys for the appropriate keyspace; neither Backend nor Ingestor may close that client. Ingestor owns its HTTP PD client, import-client factory, optional NextGen HTTP client, and worker buffer pool. A local engine is a sorted Pebble database; an external engine reads sorted files from object storage.

## Plan of Work

First adapt TestExternalEngine to use a standalone Ingestor without an engine manager. Add focused construction/close coverage for no filesystem effects and caller-owned PD lifetime where existing mocks permit. Run this focused test before implementation and retain any environmental blocker.

Extract ingest fields and engine-independent methods into Ingestor within local.go; retain existing declaration positions to keep this refactor reviewable. Keep Backend as an embedded Ingestor plus local engine manager and lazy duplicate-check KVStore. NewBackend first creates an Ingestor, then a manager, and closes the Ingestor if manager creation fails. Backend.Close closes both ownership domains. Move classic writes and NextGen worker allocation to Ingestor.bufferPool. Inject the pool into engineManager without transferring ownership; preserve manual allocator tracking.

Adapt test literals: ingestion-only tests construct Ingestor; tests exercising local engine operations construct Backend with nested Ingestor. Preserve coverage and avoid broad test rewrites.

Change IMPORT INTO constructors to create only an Ingestor for global sort, avoiding directory and disk-quota setup. Keep local Backend access for actual local engine operations and add an Ingestor accessor for shared controls. Change DDL cloud executor to own an Ingestor directly, preserving configuration and metrics without BackendCtx registration. Inspect the global read stage for unnecessary local initialization.

Finally run generated-metadata preparation, scoped tests in classic and NextGen builds, lint, and an independent code review. Update this document with exact results and unresolved limits.

## Concrete Steps

All commands run from repository root /Users/jujiajia/.codex/worktrees/52dc/tidb.

    make bazel_prepare
    ./tools/check/failpoint-go-test.sh pkg/ingestor/ingestctrl -run 'Test(ExternalEngine|BackendCloseWithoutTiKVClient|EngineManager|LocalIngestLoop|MultiIngest|DoImport|CloudRegionJobWorker|WorkerPoolWithErrors)' -count=1
    ./tools/check/failpoint-go-test.sh pkg/ingestor/ingestctrl -tags=intest,deadlock,nextgen -run 'Test(ExternalEngine|CloudRegionJobWorker|WorkerPoolWithErrors)' -count=1
    make lint
    git diff --check

Additional focused caller-package test commands will be recorded after locating their existing tests. Failpoint runs are serialized and the wrapper disables failpoints on exit. No make bazel_lint_changed is authorized.

## Validation and Acceptance

A standalone Ingestor can create classic/NextGen workers, perform classic writes through migrated tests, read an external engine, and generate ingestion jobs without an engine manager or sorting directory. NextGen workers obtain a usable owned buffer pool. Closing Ingestor releases owned clients and pool without closing the caller's PD client. Local writer/import/cleanup and duplicate-control tests retain their behavior. Global-sort IMPORT INTO and DDL cloud-import construction avoids local sorting setup. Local-sort callers retain disk quotas and local engine APIs.

## Idempotence and Recovery

Constructors return errors while releasing initialized owned resources; caller-owned PD is retained. Test commands are repeatable and use temporary directories. Generated Bazel files must be produced by make bazel_prepare; never manually edit them. Keep unrelated generated churn out of the final diff after inspecting it. No production remediation or deployment is part of this task. In the subsequent PR follow-up, the user authorized committing and pushing the changes and opening a PR referencing #72437.

## Interfaces and Dependencies

Expose NewIngestor(ctx context.Context, tls *common.TLS, config BackendConfig, callerOwnedCodecPDCli *tikvclient.CodecPDClient) (*Ingestor, error), Ingestor.Close, and Ingestor.IngestExternalEngine. Retain NewBackend's signature and backend.Backend conformance. NewIngestorForTest mirrors existing NewBackendForTest setup without a filesystem dependency. Backend retains ImportEngine, local engine adapters, and GetDupeController. Worker helpers accept *Ingestor.

## Artifacts and Notes

Initial RED attempt for importer was blocked by the DDL lane's new references to missing Ingestor symbols; no behavioral RED was obtained in that run. Initial core compile found mechanical import/declaration migration errors, corrected before runtime validation. make bazel_prepare completed successfully; final scoped validation passed.

Independent review found that IMPORT INTO FROM SELECT still uses local engines even when the plan inherits a global cloud-storage URI. Constructor routing must preserve this existing local path; the caller lane is adding a regression before correction.

Core classic focused command:

    ./tools/check/failpoint-go-test.sh pkg/ingestor/ingestctrl -run 'Test(IngestorWithoutLocalStorage|BackendAndIngestorBufferLifetimes|ExternalEngine|BackendCloseWithoutTiKVClient|EngineManager|LocalIngestLoop|MultiIngest|DoImport|CloudRegionJobWorker|WorkerPoolWithErrors|CheckPeersBusy|NotLeaderErrorNeedUpdatePeers|PartialWriteIngest|GetDupeController|RefAllJobs|GenerateAndSendJob)' -count=1

Plan and implementation review were read-only subagent lanes. The shared-pool refinement supersedes the original separate-pool approach.


## Validation record

The Ready profile applies because Go behavior and resource ownership changed. make bazel_prepare succeeded and produced only three relevant BUILD.bazel changes. Failpoints are required in each affected package; all test runs use the wrapper with cleanup.

Passed core classic coverage (11.721s):

    ./tools/check/failpoint-go-test.sh pkg/ingestor/ingestctrl -run 'Test(IngestorWithoutLocalStorage|BackendAndIngestorBufferLifetimes|ExternalEngine|BackendCloseWithoutTiKVClient|EngineManager|LocalIngestLoop|MultiIngest|DoImport|CloudRegionJobWorker|WorkerPoolWithErrors|CheckPeersBusy|NotLeaderErrorNeedUpdatePeers|PartialWriteIngest|GetDupeController|RefAllJobs|GenerateAndSendJob)' -count=1

Passed core NextGen coverage (1.209s):

    ./tools/check/failpoint-go-test.sh pkg/ingestor/ingestctrl -tags=intest,deadlock,nextgen -run 'Test(IngestorWithoutLocalStorage|BackendAndIngestorBufferLifetimes|ExternalEngine|CloudRegionJobWorker|WorkerPoolWithErrors)' -count=1

Classic caller run passed DDL (1.710s), DDL ingest (9.296s), and DXF import (5.549s). The importer run exposed an invalid identity-based comparison of separately constructed mock codecs; the corrected assertion checks API version and keyspace:

    ./tools/check/failpoint-go-test.sh pkg/executor/importer -run 'Test(StartDiskQuotaCheck|ImportFromSelectCleanup|ProcessChunkWith|PopulateChunks|GlobalSortReadIndexInitWithoutLocalBackend|CloudImportResourceModifiedUsesIngestor|CreateIngestorTLSFailureDoesNotCreateSortDirectory|MockBackendLocalStoreDir|EncodeAndSortOperator|ImportTaskExecutor)' -count=1 . ../../ddl ../../ddl/ingest ../../dxf/importinto

The corrected importer and additional core classic checks passed (20.103s and 11.179s respectively):

    ./tools/check/failpoint-go-test.sh pkg/executor/importer -run 'Test(StartDiskQuotaCheck|ImportFromSelectCleanup|ProcessChunkWith|PopulateChunks|LocalBackendSortedWriterWithTestKitTable|EngineLocalWriter|CleanupAllLocalEngines|SplitRangeAgain|RegionJobResetRetryCounter|CtxCancelIsIgnored|WorkerFailedWhenGeneratingJobs|UpdateAndGetLimiterConcurrencySafety|SplitAndScatterRegionInBatchesTwoLevel)' -count=1 . ../../ingestor/ingestctrl

The inherited-cloud-URI SELECT regression failed before the routing fix at the assertion that the local Backend must exist. The same test passed in the final classic importer run:

    ./tools/check/failpoint-go-test.sh pkg/executor/importer -run '^TestImportFromSelectCleanup$' -count=1

NextGen caller verification passed: importer 2.782s, DDL 5.016s, DDL ingest 8.338s, and DXF import 3.686s:

    ./tools/check/failpoint-go-test.sh pkg/executor/importer -tags=intest,deadlock,nextgen -run 'Test(StartDiskQuotaCheck|ImportFromSelectCleanup|GlobalSortReadIndexInitWithoutLocalBackend|CloudImportResourceModifiedUsesIngestor|CreateIngestorTLSFailureDoesNotCreateSortDirectory|MockBackendLocalStoreDir|EncodeAndSortOperator|ImportTaskExecutor)' -count=1 . ../../ddl ../../ddl/ingest ../../dxf/importinto

make lint passed, including revive and dashboard validation. git diff --check passed after the final test runs; failpoint cleanup returned its reference count to zero and left no transformed source artifacts. A source comparison confirmed all 14 moved pre-existing methods have unchanged bodies after substituting the receiver type and pool access; constructors and ownership methods were reviewed separately. The independent review identified the shared-cache and SELECT-routing issues; both were corrected and covered by targeted tests.

## Changed files and residual risks

Core: pkg/ingestor/ingestctrl/local.go, engine_mgr.go, job_worker.go, region_job.go, localhelper.go, their relevant tests (ingestor_test.go, local_test.go, engine_mgr_test.go, region_job_test.go, localhelper_test.go), and BUILD.bazel.

IMPORT INTO: pkg/executor/importer/table_import.go, table_import_testkit_test.go, importer_testkit_test.go; pkg/dxf/importinto/task_executor.go and encode_and_sort_operator.go.

DDL: pkg/ddl/backfilling_import_cloud.go, backfilling_import_cloud_test.go, backfilling_read_index.go, BUILD.bazel; pkg/ddl/ingest/backend_mgr.go, mock.go, env_test.go, BUILD.bazel. This ExecPlan is also new.

Correctness risk centers on resource ownership and mode routing; targeted tests cover pool lifetime, no local storage, caller-owned PD, SELECT compatibility, and both kernels. The shared pool preserves cache capacity and avoids double counting or changing manual allocator tracking. Configuration fields remain BackendConfig for compatibility; in-repository Backend literals are migrated to the embedded Ingestor. Global duplicate detection retains its existing local resources. No live TiKV cluster, full SQL integration suite, race run, or performance benchmark was run; no ingest algorithm or protocol changed. DDL documentation was checked and contains no conflicting claim about global-sort local storage.

Revision note: updated the original separate-pool proposal to a single Ingestor-owned borrowed pool, preserved SELECT routing after review, and recorded targeted verification evidence.


Revision note (user follow-up): keep the Ingestor implementation in local.go and defer the physical file split. Restored existing method declaration order and removed the new ingestor.go file. This changes file organization only; constructor and ownership behavior remain unchanged. Bazel preparation, make lint, focused classic/NextGen core validation, and git diff --check were rerun successfully. local.go now has 134 added and 60 removed lines, with no new production source file.


Follow-up Ready verification (file organization only):

    make bazel_prepare
    make lint
    ./tools/check/failpoint-go-test.sh pkg/ingestor/ingestctrl -run 'Test(IngestorWithoutLocalStorage|BackendAndIngestorBufferLifetimes|ExternalEngine|DoImport)' -count=1
    ./tools/check/failpoint-go-test.sh pkg/ingestor/ingestctrl -tags=intest,deadlock,nextgen -run 'Test(IngestorWithoutLocalStorage|BackendAndIngestorBufferLifetimes|ExternalEngine|CloudRegionJobWorker|WorkerPoolWithErrors)' -count=1
    git diff --check

Both focused test runs passed (classic 2.256s; NextGen 1.462s), and failpoints were disabled afterward. Existing caller-test evidence remains applicable because declarations were merged without changing their implementation. The file-removal change adds no new correctness, compatibility, or performance risk beyond the ownership refactor already covered above.
