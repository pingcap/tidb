# Structural parity audit: current evidence

The [DML metadata batch](dml-owner-batch-repair.md) advances **A01/E02/E03 together**: metadata-only joined sources, shared write-target authorization and per-table FK plans. Retained failures also repair synchronous FK-ID rollback and strict single-statement parsing. **86 tracked, 29 repaired, 57 unresolved (39 open, 18 partial)** remain; executable FK objects, row identities and matrix execution are still open. See the validation receipt for actual grouped checks. No complete package acceptance or push.


The [transport/expression cleanup](transport-expression-empty-test-removal.md) removes **80 empty ignored tests and five pure placeholder modules**, with **102 candidate rows relocated** to the [obligation ledger](transport-expression-empty-test-obligations.json). All retained executable test code is byte-for-byte unchanged. Counts remain **86 tracked, 29 repaired, 57 unresolved (39 open, 18 partial)**. This cleanup closes no finding and accepts no whole Go package. Three original expression failures remain explicit; the local HTTP fixture passes outside the socket sandbox. No push.


The [admission-policy batch](admission-policy-batch-repair.md) maintains **A02/A03 together**: retained passwords and CIPHER/ISSUER/SUBJECT/SAN use shared account storage and verified socket evidence, with CREATE/ALTER/SHOW and durable writeback. Seven runtime baseline failures, **126 distinct passing Rust cases** and 52 Go URI plus three Go JSON oracle cases validate the connected maintenance. **86 tracked, 29 repaired, 57 unresolved (39 open, 18 partial)** remain; A02/A03 stay partial for the named wider owners. Other 55 IDs retain earlier evidence rather than fresh behavioral reproduction. No complete package acceptance or push.

Earlier checkpoints below are historical.

The [account TLS batch](account-tls-policy-batch-repair.md) advances **A02/A03/N03 together**: durable global_priv, verified TLS/X509 admission and shared CA/protocol startup. Five runtime baseline failures, **114 distinct passing Rust cases** and 12 Go JSON controls validate the connected maintenance. **86 tracked, 29 repaired, 57 unresolved (39 open, 18 partial)** remain. A03 moves from open to partial; the other 54 IDs retain previous evidence. No complete package acceptance or push.

Earlier checkpoints below are historical.

The [shared MPP lifecycle batch](shared-mpp-lifecycle-repair.md) advances **M04/N03/T02 together**: one process fleet, generation retirement/joined close, and actual PD/store security bootstrap. Five runtime baseline failures and 58 distinct passing Rust cases validate this maintenance. **86 tracked, 29 repaired, 57 unresolved (40 open, 17 partial)** remain; other 54 IDs retain previous evidence. No whole-package acceptance or push.

The [MPP transport batch](mpp-transport-batch-repair.md) advances **M04/N03 together** through cluster TLS, shared store limits and cancellation/deadline setup. Five independent baseline failures and 32 distinct passing Rust cases validate the connected repairs. Both findings remain partial for their named wider owners. **Counts remain 86 tracked, 29 repaired, 57 unresolved (40 open, 17 partial)**; other 55 IDs retain prior evidence. No push.

Earlier checkpoints below are historical.

The [structural batch map](remaining-batches.md) assigns all **57 unresolved findings to ten shared-owner batches**, with explicit dependencies and completion boundaries. Whole Go packages retain atomic acceptance. Regression filters/compatible targets are grouped; final required gates run at the completed batch boundary.

The [table-policy batch](table-policy-batch-repair.md) repairs **six Go contracts across T01/K03**, with **57 distinct passing Rust cases**, lint, all-target checks and locked server build. Both findings stay partial. **Counts remain 86 tracked, 29 repaired, 57 unresolved (40 open, 17 partial).** Other 55 IDs retain prior evidence. No push.

Earlier checkpoints below are historical.

The [statement attribution batch](statement-attribution-batch-repair.md) repairs three connected producer gaps under **O11/O18**, which remain partial: failed physical compilation is not an execution, summaries consume existing deduplicated table visits, and real parse/physical compile measurements share the statement lifetime. **51 distinct Rust tests and 12 MySQL/unistore checks pass**. Broader profiling, complete visits/phases and transport remain open. **Counts remain 29 repaired, 57 unresolved (40 open, 17 partial), 86 tracked.** Other IDs retain prior evidence. No push.

The exhaustive placeholder cleanup below is historical.

The [exhaustive planner cleanup](planner-empty-module-removal.md) removes **612 ignored empty tests across 64 pure-placeholder modules**. All 72 retained test files are unchanged; no pure-placeholder module remains. All contracts/current Go identities and 674 relocated candidate rows remain in the [unverified ledger](planner-empty-module-obligations.json). The retained aggregate passes 277 tests with 362 ignored; 355 distinct Rust cases pass overall. **Counts remain 29 repaired, 57 unresolved (40 open, 17 partial), 86 tracked.** No production behavior or finding status changed.

The preceding 55-entry cleanup below is historical.

The [placeholder cleanup](placeholder-test-removal.md) removes **55 ignored empty tests in five planner modules** and two assertion-free LRU diagnostic loops. All 55 Go declarations and their historical contracts remain in an [unverified ledger](placeholder-test-obligations.json); 81 dangling candidate-index rows moved there. Every other aggregate test entry is preserved. **Counts remain 29 repaired, 57 unresolved (40 open, 17 partial), 86 tracked.** Production owners and safety fallbacks are unchanged.

The cache/Apply checkpoint below is historical.

The [cache/Apply batch](cache-apply-batch-repair.md) repairs **C02** and advances **E04 and N03 together**. **57 unresolved (40 open, seventeen partial), 29 repaired, 86 tracked.** E04 retains named CTE/shuffle/full-matrix gaps. The other unresolved IDs retain prior evidence, not fresh whole-register behavioral reproduction. No whole-package acceptance or push is claimed.

The statement-observation checkpoint below is historical.

The [statement observation batch](statement-observation-batch-repair.md) advances **O18, O11 and N03 together**: shared SQL completion/counters, routed durable publication, full current/history schema, persistent startup/fallback/readers and joined shutdown. **58 unresolved (42 open, sixteen partial), 28 repaired, 86 tracked.** Detailed telemetry and complete profiling/transport remain unresolved; no whole-package acceptance is claimed. Other IDs retain their prior evidence.

The DML checkpoint below is historical.

The [DML policy batch](dml-policy-batch-repair.md) advances **E03 and T01 together** to partial: physical row consumption/early quota/cleanup, shared statement retirement/recovery and explicit caller-owned absence/assertion metadata. The connected SET/config checks remove duplicate GC-trigger/packet validation under N03, which remains partial, and correct stale classic-kernel/query-info tests. **58 unresolved (44 open, fourteen partial), 28 repaired, 86 tracked.** Complete planner/matrix/system-index/pessimistic owners remain open; no whole-package acceptance is claimed. Other IDs retain their prior evidence.

The MPP checkpoint below is historical.

The [MPP read batch](mpp-read-batch-repair.md) repairs **M02 and M03 together** and advances M04 to partial through canonical process PD/cache ownership and stale-region invalidation. **58 unresolved (46 open, twelve partial), 28 repaired, 86 tracked.** Three fail-before regressions and 37 targeted Rust cases validate ranges, streaming and cleanup. Thirty ignored empty shells are removed with all [upstream/golden obligations retained](mpp-unverified-test-obligations.json). No full MPP package, transport or live multi-node acceptance is claimed; the other findings retain their prior evidence.

The TiFlash checkpoint below is historical.

The [TiFlash replica batch](tiflash-replica-batch-repair.md) repairs F03's classic cache/backoff/discovery contract and advances F01/F02 together, including six failing metadata/cadence regressions and N03's secure cluster HTTP consumer. **60 unresolved (49 open, eleven partial), 26 repaired, 86 tracked.** Eight ignored empty test shells are removed; every unverified upstream obligation is [retained explicitly](tiflash-unverified-test-obligations.json). No full DDL/infosync acceptance or fresh reproduction of every other finding is claimed.

The cache checkpoint below retains its historical counts.

The [shared cache batch](shared-cache-batch-repair.md) repairs B01, B02, C03 and
C04 together: one pinned Ristretto implementation, live incremental binding
maintenance, effective coprocessor configuration and deferred LFU admission.
**61 unresolved (52 open, nine partial), 25 repaired, 86 tracked.**
See [validation](shared-cache-batch-validation.json), the
[91-artifact dependency decisions](ristretto-native-inventory.json) and
[five-artifact LFU receipt](lfu-lifecycle-package.json). Other findings retain
their recorded scope; source continuity is not fresh runtime reproduction.

The entries below retain their historical checkpoint counts.

The [account history and locking-image repair](account-history-locking-repair.md)
closes A04 and preserves locking limits/counts/epochs and raw attributes through
account writes. **65 unresolved (55 open, ten partial), 21 repaired, 86 tracked.**
A02 remains partial for TLS/global_priv and durable wire-login state. Fresh master
is unchanged at 93a01d31f6; concurrent schema-acknowledgement commits were preserved.

The entries below retain their historical checkpoint counts.

The [expiry durability repair](account-expiry-durability-repair.md) preserves
typed lifetime/timestamp values through loading, publication and writeback. A02
remains partial; the 66-unresolved count is unchanged.

Latest cloud [source/test review](cloud-account-test-review.md): 66 unresolved
(56 open, ten partial), 20 repaired. Empty-account preservation advances A02 to
partial; complete durable policy and password reuse remain open.

Full-register review baseline: TiDB Go master `93a01d31f6da205ae4bf376825293903a6899fdb`, client-go
`v2.0.8-0.20260928031501-8edb23f6c7ee`, client-rust
`6163ecfc587b248dcbf0e30c1c9d905b4bc5a665` (published PD errors prerequisite).
The current review starts at integration `cfc6a174bb3e46312dae48a7b85a53053b2f5ea0`.

This is the list of **currently confirmed findings and explicit review gaps**.
It is not a claim that every semantic mismatch has been discovered or removed.
The coverage inventories enumerate every tracked artifact in 856 TiDB, 41 client-go,
41 kvproto, 24 PD-client and 7 etcd-API package directories, with 83 Rust crates
awaiting current-master acceptance. Original tests, generated/build/platform inputs, fixtures and
unassigned root artifacts are retained. The 2,043 candidate lines are search
evidence, not a defect count. Some are errors Go intentionally returns.

Reproduce the inventory from the repository root with
`python3 rust/scripts/inventory-go-rust-parity.py --go-ref origin/master`.
Receipts and reviewed findings live separately so regeneration cannot certify
unreviewed packages. Other external dependencies still require complete inventories
before acceptance.

The [explicit PD shutdown repair](../../pd-shutdown-ownership-execplan.md)
composes the existing native close chain through PD/TSO, preserves concurrent
and interrupted joins, and cancels cache-owned RPC waits. P06 remains partial;
this does not accept the complete root/discovery/TSO packages or close P03.

The expanded [remaining structural finding register](structural-findings.md)
consolidates 86 tracked ownership/contract findings (58 unresolved, 28 repaired), review candidates and the
limits of the review. The [historical protocol comparison](protocol-projections.json)
lists 400 omissions, one PD oneof contract mismatch and 71 deliberate opaque
representations separately. It includes the keyspace-zero wire reproduction.
The [replacement-owner comparison](protocol-contracts-after.json) now reports
zero omissions/contract differences and retains the 71 opaque representations.
See [the removal receipt](complete-protocol-owner-repair.md) for caller and
validation coverage. Neither document claims that every repository semantic
mismatch is known.

The [configuration/statistics maintenance batch](config-statistics-maintenance-repair.md)
closes O07 and advances N03 to partial from integration `43e827e2c4` at the same
Go master. Startup shares one effective configuration; both stores run the
existing statistics GC, health and cache maintenance owners. Auto-analyze reads
the live process switch. 139 distinct Rust tests and a stock-MySQL unistore process
check pass. Native deployment defaults and missing configuration consumers,
including A03 TLS policy, remain unresolved; this grants no package acceptance.

The [shared server session batch](shared-server-session-repair.md) closes S01 and
S02 together at Go master `93a01d31f6`, from integration `44be2a9d756`; the native
dependency remains `19a56cc`. Both stores now use one ordinary session/catalog
lifecycle. Ninety-seven distinct targeted Rust tests and a stock-client unistore
process check pass. Live TiKV topology and benchmark measurements remain unverified.

The [2026-10-02 review after shared worker repairs](worker-followup-structural-review.md)
rechecked every then-unresolved ID. **All 75 were valid parity gaps: 69 open
and six partial.** Of these, 73 concern live behavior or missing production
integration and two (D09/D10) concern disabled seeds. Fourteen IDs have fresh
symptom/refusal observations; 59 others have source/caller evidence. This is not
75 reproduced bugs. The ten recorded repairs remain repaired, including D04/D06;
old D05 error-conversion and E02 orphan-acceptance allegations are corrected.
The [per-ID review](worker-followup-recheck/source-continuity.json) retains exact
source hashes, all intervening crate changes and the remaining contract for
every ID. No production code or package acceptance changes in this review.

The [atomic health-publication repair](../../health-feedback-publication-execplan.md)
subsequently closes T04. Native `c97dafb89883312deb526dc8d8f36cc7f7001f47`
publishes feedback metadata independently of the writer mutex and owns the
client-score/callback/decay sequence. Three regressions fail before repair and
pass afterward; TiDB synchronizes that owner without a second implementation.
At that checkpoint there were **74 unresolved (68 open, six partial), eleven
repaired, 85 tracked**. T02 and complete native locate ownership remain open.

The [PD request-ownership follow-up](../../pd-request-ownership-execplan.md)
closes P07 with native `952013279bc64e590f17c18b9c9222fdaf5a3604`, synchronized
through the maintained dependency workflow. Metadata and TSO requests retain
connections without holding the cluster lock across I/O; discovery and retired
stream joins also run outside that lock. The current count is **73 unresolved
(67 open, six partial), twelve repaired, 85 tracked**. P03/P06 and complete
PD parent-package acceptance remain open.

The subsequent [DDL error identity repair](../../ddl-error-identity-execplan.md)
removes CHECK's premature wire-error conversion and retains typed errors from the
existing persisted action producers through the shared worker and history.
Cancellation/rollback distinguish equal numbers with different source identities;
plain decode failures persist as ddl:-1 while SQL still returns 1105. Old numeric
Rust history remains readable. D05 stayed partial and all 75 structural IDs remained
unresolved at that checkpoint; other producers, transaction reset, retry configuration/metrics and
complete action ownership still require work. No new action is enabled.

The [CHECK error generation follow-up](../../ddl-error-generation-execplan.md)
removes handwritten CHECK diagnostics in favor of the existing shared source
catalog formatter. It preserves original missing-constraint names, lowercased
validation names and Go's distinct ADD/DROP/ALTER state-error decisions. A
successful CHECK step re-encodes its decoded arguments even without a schema
mutation; failed steps retain their original raw arguments. D05 remains partial;
other action producers and full package/runtime ownership are not accepted.

The [2026-10-01 full register reconciliation](remaining-structure-review.md) adds
11 previously unregistered source-confirmed boundaries, records the statement-summary
SQL probe, and corrects stale T03 status. [Machine-readable findings](structural-findings.json)
and [source continuity](structural-source-continuity.json) retain every ID and current source pin.
No production code was repaired by that review. The subsequent
[review of every unresolved finding](structural-review-followup.md) retains
all 77 statuses, corrects stale PD lifetime claims, and records six fresh SQL/wire
diagnostics. At that historical baseline, accepted repartition made existing rows
invisible in both tested shapes; this strengthened D01 without counting its
missing DDL owner twice.
[That review's per-ID continuity](structural-recheck/source-continuity.json) preserves
the previous source evidence separately from those new observations.

The earlier [review after removals](post-removal-structural-review.md) reconciles
all 77 unresolved findings again at 68d6de685a, with six fresh diagnostics and
[per-ID source continuity](post-removal-recheck/source-continuity.json).
Password history, partial multi-action ALTER, generated-column conversion and
Latin-1 byte corruption still reproduce. Repartition and IMPORT now refuse
without mutation; fabricated cluster config/topology have been removed. Their
complete Go owners remain absent, so counts do not fall. The review gives each
finding's replacement/migration prerequisite and keeps source-only risks distinct
from reproduced failures. No production code is changed by the review.

The [partition shortcut removal](../../partition-owner-removal-execplan.md)
subsequently withdraws unsafe local repartition and cluster direct planning,
and removes their thread-local metadata dependency. Refused operations preserve
existing rows/schema; ordinary ADD/DROP/TRUNCATE work on fresh threads again.
Go's supported online repartition still requires the complete durable owner.
D01 remains open, and all 77 unresolved structural IDs retain their status.

The [IMPORT shortcut removal](../../import-shortcut-removal-execplan.md) also
withdraws the private CSV parser, nested SQL precheck/row INSERT loop and SELECT
rewrite. Both source forms now refuse before file access or data mutation; syntax
and source-derived helpers remain. Go's import controller, durable file-import
jobs/tasks and SELECT importer still require complete runtime integration. E05
remains open, with the prior ignored-option observations retained as history.

The [cluster fixture removal](../../cluster-fixture-removal-execplan.md)
withdraws captured configuration and the fake store1 row, and propagates
server-discovery errors. CLUSTER_CONFIG explicitly refuses until live retrieval
exists. I01/I02 remain open for full runtime ownership and missing discovery.

The [PD preface ownership experiment](pd-grpcutil-contract/h2-preface-review.md)
subsequently resolves the isolated candidate's two HTTP/2 readiness failures.
Opt-in validation inside the protocol decoder passes 14 tests and the extended
Go race/goleak oracle; the rejected baseline remains reproducible. This is not a
production grpcutil integration: TLS, backoff, GOAWAY, options, interception and
connection-cache gates remain. Native client-rust and the 77 statuses are unchanged.

The [full-picture repair sequence](repair-sequence.md) starts from the published
integration audit `4285385fad20855487ec1d8ff113290d48f949a5` and maps all 77 open
IDs exactly once into 12 workstreams. It identifies complete source owners,
caller migrations, removal gates and acceptance behavior. The
[living ExecPlan](../../full-structural-parity-execplan.md) sets the next native
PD package closure, coupled schema/DDL/GC sequence, independent correctness
work, benchmark method and publication gates. Workstreams are not partial
package completion claims; no production code changes in this planning update.

The [PD deadline owner repair](pd-deadline-owner-repair.md) implements the complete
pinned deadline package and its native TSO batch/retirement integration, including
a shared cancellation lost-wakeup correction. Native master is `5928b6e480b441496f9a3cd9bed6a7e8d56215a1`.
P06 is partial; public PD close and the whole parent packages remain open. The
77-unresolved count is unchanged. See the receipt for synchronized-source and
publication checks; earlier source snapshots above retain their original pins.

The [PD connection-context repair](pd-connectionctx-owner-repair.md) implements
the complete pinned connectionctx package and replaces unconditional healthy
TSO stream replacement with URL-keyed ownership. Native master is
`4e3169ed93e433eab38638a1dd92a24f25f7caaf`. Source tests, rejected ownership,
replacement and retained-stream lifecycle are covered; public PD close,
discovery and metadata concurrency remain open. P06 and the 77-ID count do not
advance to complete from this prerequisite.

The [PD batch-controller repair](pd-batch-owner-repair.md) implements the complete
pinned batch package and migrates native TSO to source default collection,
token ownership and buffer reuse. Native master is
`bcf74b7282b01372f93fb814ba601eb4c22b5d12`. The 64-request collector and
65,536-outstanding-batch policy are removed; full dispatcher/options/router
and public close/discovery/concurrency obligations remain open. This is another
complete prerequisite, not closure of P06 or all 77 unresolved findings.

The [PD retry-owner repair](pd-retry-owner-repair.md) implements the complete
pinned retry package and migrates native default initialization through it,
with bounded membership probes. Native master is `2fd0ecebadf0e8274a2b10ebfed729b03284a52b`.
Both transport regressions fail before repair; original Go race/goleak, 95 PD
cases and 1,470 native tests pass (two existing ignored). Full discovery/root/
TSO lifecycle acceptance remains open; P06 and the 77-ID count are unchanged.

The [PD options owner repair](pd-opt-owner-repair.md) covers the complete pinned
opt package and removes the scan-only options declaration. Original source
cases and every constructor are mapped; native cache/codec/cluster and TiDB
bridge callers use the shared region policy. Native master is `df0d4ccc5b595959f496b3cf6e6b87f22b337bf4`;
original Go race/goleak, native library/static and TiDB adapter/lint checks pass.
The receipt records locked publication gates. Full discovery/root/TSO acceptance
and the 77-ID count remain open.

The [statistics LFU follow-up](lfu-lifecycle-repair.md) removes synthetic trigger
tables and repairs joined shutdown after reviewing all five Go package artifacts.
At that checkpoint C04 remained open: the concurrent-pressure reproduction failed in Stretto,
and its admission/metrics contract is not accepted as Ristretto-equivalent.
The [review follow-up](lfu-review-followup.md) closes two missed shutdown paths,
isolates the admission mismatch with a paused worker, and inventories all 91
artifacts of the pinned external module. Both probes were red there; the shared-cache batch above now enables and passes them.

The [shared cache ownership review](shared-cache-owner-review.md) traces all
four Ristretto consumers on current Go master, including inference, which is
absent from the integration Go checkout. It joins the B01/B02, C03 and C04
dependency work while preserving separate consumer lifetimes and acceptance
gates. Replacing storage alone leaves binding reload and coprocessor
configuration mismatches. This is a design checkpoint, not a runtime repair.

The [restore-utils protocol repair](restore-utils-protocol-repair.md) removes
P04's two handwritten protocol owners after reviewing the complete eight-artifact
Go package at master `93a01d31f6`. Complete generated files retain shared identity
through merging; lookups borrow generated rules. All original Go tests pass with
the race detector, and Rust tests plus all five source merge workloads pass.
Live BRIE execution remains open as E07.

The [range-tree protocol repair](rtree-protocol-repair.md) follows across the
complete seven-artifact `br/pkg/rtree` package. P05 removes generic file payloads,
the narrowed test fixture and local types at generated RPC boundaries. Original
Go tests, Rust identity/error-path cases and fixed-size update/merge workloads
pass. This does not accept metautil, progress-handle aliasing or live BRIE.

The [global-config synchronization repair](global-config-sync-repair.md) implements
O12's complete three-artifact Go package and its production session/factory/PD
integration. Explicit validated writes notify one domain keeper; reloads remain
quiet and failed stores are not retried. Original Go tests and the new Rust
regressions pass. Three broader session failures reproduce unchanged and remain
open. Parent-package and benchmark parity are not claimed.

The [subsystem ownership review](subsystem-structure-review.md) compares integration
`dae65456f9` with the same master and adds 18 findings across optimization,
expression execution, table/write policy, historical schemas, session migration,
worker lifetimes, control-plane services and remaining BR protocol consumers.
The retained probe reproduces strict-mode generated-column overflow and missing
session-state/historical-read/BR job handlers. The [complete scope matrix](structural-coverage.md)
accounts for every Rust crate and inventoried Go package directory, including
explicitly unreviewed queues. Scope accounting does not establish semantic
coverage or package acceptance. No production code is changed in this audit.

The [expanded production-owner review](expanded-ownership-review.md) compares
integration `13689e0b13` with the same current master and adds 13 findings in
privilege/account policy, binding ownership, import, wire/config/admission and
domain workers. Retained SQL and loopback-wire probes reproduce six finding
groups, including unauthorized joined UPDATE, ignored password history/import
options and changed Latin-1 bytes. Passing controls exclude stale keyword
matches. No production behavior or package acceptance is changed.

The [session/executor follow-up](session-ownership-review.md) adds 12 findings
against integration `960fa95b48` and the same Go master. Its retained SQL probe
reproduces lost self-join updates, bypassed multi-update foreign keys,
non-atomic in-process ALTER, missing shared cache eviction/flush, and dynamic
virtual tables served from fixtures or constants. Seven further owner gaps
are source-confirmed with explicit runtime limits. No production fix or new
package acceptance is included in this follow-up.

The [shared session cache repair](shared-session-plan-cache-repair.md) removes
C01's separate physical owners and metadata LRU, including close and flush
callers. C02's instance physical cache and full package acceptance remain open.

| Finding | Evidence and Go ownership | Status |
| --- | --- | --- |
| Partial TiPB declarations and stale dependency selection | Four local projections omitted 157 declarations from master's pin; the old checker validated only locally present declarations against this branch's older pin. `Executor` decode discarded a valid ExplainForConnection body. | Fixed by complete upstream input ownership; individual gaps listed below. Source gate, original Go tests, Rust wire tests and consumers validated. |
| Duplicate coprocessor and MPP contracts | Local projections lost 27 declarations and had two field-contract differences (MPP keyspace oneof/API version enum). Native generation copied four Go SharedBytes fields. | Fixed through complete native package re-exports and source regeneration; native repair published as b2b3783. Individual gaps listed below. |
| MPP dispatch bypassed the native API-context codec | The local literal api_version=1 is V1TTL; classic transactional Go uses V1=0 and the null keyspace from its codec. | Fixed by using native Keyspace and Request setters; the regression failed with V1TTL and now passes with V1. |
| MPP query/task identity has no statement owner | Go executor/mpp_gather.go gets one atomic query ID and UnixNano timestamp from StmtCtx.MPPQueryInfo; builder.go uses domain ServerID. Rust used local_query_id=1, gather_id=1, process ID and Unix seconds. | Fixed for the existing scan path: statement-owned query/task/gather atomics, shared across reader contexts and cluster retry decisions, retired at completed statement/record-set close. Server identity comes from the existing server-info getter; the absent lease allocator is tracked separately below. |
| Domain numeric server-ID lease owner is absent | Go domain.go owns acquireServerID/refreshServerIDTTL and exposes ServerID to executors. Rust node_server_info leaves its numeric identity at zero; sql_node also uses a standalone connection-ID constant. | Open. Sharing the existing getter removes process-ID synthesis from MPP but does not implement cluster-wide server-ID allocation, renewal or loss handling. |
| TiFlash poller had duplicate startup, detached lifetime and a private DDL publisher | Go ddl.Start owns one joined PollTiFlashRoutine, gates work on its owner manager, and publishes through executor.UpdateTableReplicaInfo. | Repaired in the existing classic path: one retained/joined worker, shared owner gate and DDL executor, no poller transaction opener. Required publication gates are recorded in the ExecPlan. This is not full DDL/infosync package acceptance. |
| TiFlash HTTP and status protocols diverged | Raw TCP did not decode chunked responses or bound reads; helper.ComputeTiFlashStatus expects a count plus a space-separated region line. Go uses unique per-store regions, the configured replica count, and errors from live stores. | Repaired with the existing HTTP library and Go-compatible parsing/progress; available tables no longer republish. Six behavior regressions failed before correction. |
| A table update replaced sibling TiFlash placement rules | The invented new_tiflash_bundle put one table into the shared tiflash group; PD partial bundle updates replace named groups. Go infosync.SetPlacementRule uses the individual-rule API with index 120 and group priority. | Removed the bundle constructor and its caller. Individual-rule delivery preserves sibling rules; region-count queries use escaped memcomparable keys. Two-table HTTP regression passes. |
| Classic TiFlash placement lifecycle still belongs to the poller | Go onSetTableFlashReplica configures rules before metadata; GC owns dropped-table cleanup. Rust still repairs/deletes rules and accelerates scheduling every poll. Go runs refreshTiFlashPlacementRules only for NextGen. | Open. Migrate all set/create-like/partition/drop/reset consumers to DDL/GC before deleting reconciliation. Partition IDs and other keyspaces must not be classified as stale by a logical-table-only snapshot. |
| TiFlash DDL status/reset and partition semantics are incomplete | Go preserves existing availability unless ResetAvailable is requested and publishes physical partition availability. Rust SetTiFlashReplica always reconstructs unavailable metadata; UpdateTiFlashReplicaStatus/polling only use logical table IDs. | Open, requiring the complete DDL consumer lifecycle above. |
| TiFlash polling lacks shared progress/backoff and PD HTTP discovery owners | Go refreshTiFlashTicker owns periodic store refresh, unavailable backoff and available-table progress caching; infosync gets PD HTTP store states including Down/Disconnected. Rust calls gRPC discovery every tick and filters legacy Up stores; no shared progress cache or cluster-security HTTP manager. | Open. Errors from selected Up stores are fixed, but that does not certify discovery, offline-store progress, cache, TLS/failover or scheduling parity. |
| Generic mutation constructor chooses table assertion policy | `tidb-txnkv/src/transaction/mutation.rs::BufferMutation::insert` combines presume-not-exists with AssertNotExist. `tidb-exec/src/real_tikv_dml.rs::plan_insert` intentionally does no snapshot check. Go `pkg/table/tables` chooses Unknown for a lazy optimistic miss and NotExist for an eager/pessimistic insert. | Open. Reconcile all callers; blanket Unknown would break eager/pessimistic ownership. Configured WritePlanningSnapshot lacks transaction-mode input; system-row index writes omit assertions. These need the table owner, not another global default. |
| Region/cache/RPC algorithms have competing owners | `tidb-txnkv/src/driver/client_bridge.rs::ClientPd` delegates to TiDB routing/recovery/transport while client-rust also implements these algorithms. DistSQL still needs TiDB capabilities. | Open architecture migration. Duplication alone is not proof of a runtime failure; native RetryBackoffer already owns RegionBackoffBudget. |
| Background lifetime inferred from request type/client mode | The earlier bridge used a TxnHeartBeatRequest exception and native transaction_tasks flag. | Repaired as T03 by native 488bb73 and the synchronized TiDB dependency. Explicit operation lifetimes now cross the bridge; foreground ResolveLock remains cancellable. The prior approval block is historical and resolved. Broader native TSO shutdown remains separately open as P06. |
| Alternate storage session dispatch remains incomplete | The lightweight path's supported SET assignments now use the normal parser, but its transaction mode/autocommit lifecycle is not fully unified with the ordinary session owner. | Review required at session package scope; do not delete a dispatcher before migrating all its callers. |
| Other locally projected protocol packages | Five local PD/TiKV/BR/etcd projections caused 400 omissions and a PD presence mismatch. | Removed. Complete native ownership, descriptor-derived TiKV transport and full pinned etcd inputs eliminate the compared gaps; all 71 opaque representations retain presence. P01/P02 contract ownership repaired; P03 discovery and external package/runtime acceptance remain open. |

The earlier five diff comments (explicit lock retry limits, secondary retry
budget, locked snapshot commit timestamps, mock wake-up semantics and detached
resolver lifetime) have regression receipts in
`../../remove-extra-storage-policies-execplan.md`. They are not reopened solely
because the broader routing/lifetime migration remains unfinished.

## Reproduced baseline failures still requiring root-cause review

The previous embedded suite reproduced these ten failures both before and
after the transaction activation repair. These are failed validations, not yet
ten proven structural root causes. They remain open. The expanded audit reran
`system_table_ddl_does_not_publish_statistics_events_like_go`: it fails at
`p1 exists` after ADD PARTITION. Current DDL already filters system-schema
notifications, so this failure does not establish a missing event filter:

- `add_partition_statistics_follow_global_prune_mode_like_go`
- `cluster_info_reports_this_node`
- `drop_partitions_statistics_match_go`
- `exchange_partition_validates_and_swaps_real_rows_atomically`
- `global_stats_drive_partition_plans_like_go`
- `partition_scoped_analyze_refreshes_global_count_and_modify_count`
- `stats_notifier_uses_a_real_internal_transaction_like_go`
- `system_table_ddl_does_not_publish_statistics_events_like_go`
- `truncate_hash_partition_statistics_match_go`
- `truncate_partitions_refreshes_global_stats_meta_like_go`

## Complete TiPB declaration gap list

All 157 entries below were absent from the four old projections and are now
owned by the complete upstream inputs: 76 messages, 51 fields, 29 enums and
one enum value. The machine-readable before-image retains tags and source
revisions in `tipb-mismatches-before.json`. This fixes declaration loss; it
does not invent SQL executor implementations for previously unsupported types.

| Declaration | Former gap | Status |
| --- | --- | --- |
| `.tipb.TableInfo` | missing message | Fixed by complete upstream schema |
| `.tipb.IndexInfo` | missing message | Fixed by complete upstream schema |
| `.tipb.KeyRange` | missing message | Fixed by complete upstream schema |
| `.tipb.ChecksumRewriteRule` | missing message | Fixed by complete upstream schema |
| `.tipb.ChecksumRequest` | missing message | Fixed by complete upstream schema |
| `.tipb.ChecksumResponse` | missing message | Fixed by complete upstream schema |
| `.tipb.Expr.rpn_args_len` | missing field | Fixed by complete upstream schema |
| `.tipb.Expr.order_by` | missing field | Fixed by complete upstream schema |
| `.tipb.RpnExpr` | missing message | Fixed by complete upstream schema |
| `.tipb.ByItem.rpn_expr` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.exchange_receiver` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.join` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.kill` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.Projection` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.partition_table_scan` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.sort` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.window` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.fine_grained_shuffle_stream_count` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.fine_grained_shuffle_batch_size` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.expand` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.expand2` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.broadcast_query` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.cte_sink` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.cte_source` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.index_lookup` | missing field | Fixed by complete upstream schema |
| `.tipb.Executor.explain_for_connection` | missing field | Fixed by complete upstream schema |
| `.tipb.ExchangeSender.partition_keys` | missing field | Fixed by complete upstream schema |
| `.tipb.ExchangeSender.types` | missing field | Fixed by complete upstream schema |
| `.tipb.ExchangeSender.compression` | missing field | Fixed by complete upstream schema |
| `.tipb.ExchangeSender.upstream_cte_task_meta` | missing field | Fixed by complete upstream schema |
| `.tipb.ExchangeSender.same_zone_flag` | missing field | Fixed by complete upstream schema |
| `.tipb.CTESink` | missing message | Fixed by complete upstream schema |
| `.tipb.CTESource` | missing message | Fixed by complete upstream schema |
| `.tipb.IndexLookUp` | missing message | Fixed by complete upstream schema |
| `.tipb.EncodedBytesSlice` | missing message | Fixed by complete upstream schema |
| `.tipb.ExchangeReceiver` | missing message | Fixed by complete upstream schema |
| `.tipb.ANNQueryInfo` | missing message | Fixed by complete upstream schema |
| `.tipb.InvertedQueryInfo` | missing message | Fixed by complete upstream schema |
| `.tipb.FTSBooleanTerm` | missing message | Fixed by complete upstream schema |
| `.tipb.FTSBooleanQuery` | missing message | Fixed by complete upstream schema |
| `.tipb.FTSBooleanNode` | missing message | Fixed by complete upstream schema |
| `.tipb.FTSQueryInfo` | missing message | Fixed by complete upstream schema |
| `.tipb.TiCIVectorQueryInfo` | missing message | Fixed by complete upstream schema |
| `.tipb.ColumnarIndexInfo` | missing message | Fixed by complete upstream schema |
| `.tipb.TableScan.ranges` | missing field | Fixed by complete upstream schema |
| `.tipb.TableScan.pushed_down_filter_conditions` | missing field | Fixed by complete upstream schema |
| `.tipb.TableScan.runtime_filter_list` | missing field | Fixed by complete upstream schema |
| `.tipb.TableScan.deprecated_ann_query` | missing field | Fixed by complete upstream schema |
| `.tipb.TableScan.used_columnar_indexes` | missing field | Fixed by complete upstream schema |
| `.tipb.PartitionTableScan` | missing message | Fixed by complete upstream schema |
| `.tipb.Join` | missing message | Fixed by complete upstream schema |
| `.tipb.RuntimeFilter` | missing message | Fixed by complete upstream schema |
| `.tipb.IndexScan.fts_query_info` | missing field | Fixed by complete upstream schema |
| `.tipb.IndexScan.tici_vector_query_info` | missing field | Fixed by complete upstream schema |
| `.tipb.Selection.rpn_conditions` | missing field | Fixed by complete upstream schema |
| `.tipb.Selection.child` | missing field | Fixed by complete upstream schema |
| `.tipb.Projection` | missing message | Fixed by complete upstream schema |
| `.tipb.Aggregation.rpn_group_by` | missing field | Fixed by complete upstream schema |
| `.tipb.Aggregation.rpn_agg_func` | missing field | Fixed by complete upstream schema |
| `.tipb.Aggregation.child` | missing field | Fixed by complete upstream schema |
| `.tipb.Aggregation.pre_agg_mode` | missing field | Fixed by complete upstream schema |
| `.tipb.TopN.child` | missing field | Fixed by complete upstream schema |
| `.tipb.TopN.partition_by` | missing field | Fixed by complete upstream schema |
| `.tipb.Limit.child` | missing field | Fixed by complete upstream schema |
| `.tipb.Limit.partition_by` | missing field | Fixed by complete upstream schema |
| `.tipb.Kill` | missing message | Fixed by complete upstream schema |
| `.tipb.ExplainForConnection` | missing message | Fixed by complete upstream schema |
| `.tipb.ExecutorExecutionSummary.tiflash_hash_table_stats` | missing field | Fixed by complete upstream schema |
| `.tipb.TiFlashExecutionInfo` | missing message | Fixed by complete upstream schema |
| `.tipb.Sort` | missing message | Fixed by complete upstream schema |
| `.tipb.WindowFrameBound` | missing message | Fixed by complete upstream schema |
| `.tipb.WindowFrame` | missing message | Fixed by complete upstream schema |
| `.tipb.Window` | missing message | Fixed by complete upstream schema |
| `.tipb.GroupingExpr` | missing message | Fixed by complete upstream schema |
| `.tipb.GroupingSet` | missing message | Fixed by complete upstream schema |
| `.tipb.Expand` | missing message | Fixed by complete upstream schema |
| `.tipb.ExprSlice` | missing message | Fixed by complete upstream schema |
| `.tipb.Expand2` | missing message | Fixed by complete upstream schema |
| `.tipb.BroadcastQuery` | missing message | Fixed by complete upstream schema |
| `.tipb.TiFlashHashTableStats` | missing message | Fixed by complete upstream schema |
| `.tipb.InUnionMetadata` | missing message | Fixed by complete upstream schema |
| `.tipb.CompareInMetadata` | missing message | Fixed by complete upstream schema |
| `.tipb.GroupingMark` | missing message | Fixed by complete upstream schema |
| `.tipb.GroupingFunctionMetadata` | missing message | Fixed by complete upstream schema |
| `.tipb.IntermediateOutputChannel` | missing message | Fixed by complete upstream schema |
| `.tipb.DAGRequest.start_ts_fallback` | missing field | Fixed by complete upstream schema |
| `.tipb.DAGRequest.collect_range_counts` | missing field | Fixed by complete upstream schema |
| `.tipb.DAGRequest.max_warning_count` | missing field | Fixed by complete upstream schema |
| `.tipb.DAGRequest.sql_mode` | missing field | Fixed by complete upstream schema |
| `.tipb.DAGRequest.max_allowed_packet` | missing field | Fixed by complete upstream schema |
| `.tipb.DAGRequest.is_rpn_expr` | missing field | Fixed by complete upstream schema |
| `.tipb.DAGRequest.user` | missing field | Fixed by complete upstream schema |
| `.tipb.DAGRequest.force_encode_type` | missing field | Fixed by complete upstream schema |
| `.tipb.DAGRequest.intermediate_output_channels` | missing field | Fixed by complete upstream schema |
| `.tipb.UserIdentity` | missing message | Fixed by complete upstream schema |
| `.tipb.SPFreshTeardownRequest` | missing message | Fixed by complete upstream schema |
| `.tipb.SPFreshSearchRequest` | missing message | Fixed by complete upstream schema |
| `.tipb.SPFreshEvalContext` | missing message | Fixed by complete upstream schema |
| `.tipb.SPFreshFilterExprColumn` | missing message | Fixed by complete upstream schema |
| `.tipb.SPFreshSearchResponse` | missing message | Fixed by complete upstream schema |
| `.tipb.SPFreshSearchResult` | missing message | Fixed by complete upstream schema |
| `.tipb.SPFreshSearchRow` | missing message | Fixed by complete upstream schema |
| `.tipb.SPFreshSearchStats` | missing message | Fixed by complete upstream schema |
| `.tipb.SPFreshANNStats` | missing message | Fixed by complete upstream schema |
| `.tipb.tici.CreateIndexRequest` | missing message | Fixed by complete upstream schema |
| `.tipb.tici.CreateIndexResponse` | missing message | Fixed by complete upstream schema |
| `.tipb.tici.DropIndexRequest` | missing message | Fixed by complete upstream schema |
| `.tipb.tici.DropIndexResponse` | missing message | Fixed by complete upstream schema |
| `.tipb.tici.TiCITableInfo` | missing message | Fixed by complete upstream schema |
| `.tipb.tici.TiCIColumnInfo` | missing message | Fixed by complete upstream schema |
| `.tipb.tici.TiCIIndexInfo` | missing message | Fixed by complete upstream schema |
| `.tipb.tici.TiCIIndexInfo.OtherParamsEntry` | missing message | Fixed by complete upstream schema |
| `.tipb.tici.ParserInfo` | missing message | Fixed by complete upstream schema |
| `.tipb.tici.ParserInfo.ParserParamsEntry` | missing message | Fixed by complete upstream schema |
| `.tipb.tici.GetIndexProgressRequest` | missing message | Fixed by complete upstream schema |
| `.tipb.tici.GetIndexProgressResponse` | missing message | Fixed by complete upstream schema |
| `.tipb.TopSQLRecord` | missing message | Fixed by complete upstream schema |
| `.tipb.TopRURecord` | missing message | Fixed by complete upstream schema |
| `.tipb.TopRURecordItem` | missing message | Fixed by complete upstream schema |
| `.tipb.TopSQLRecordItem` | missing message | Fixed by complete upstream schema |
| `.tipb.TopSQLRecordItem.StmtKvExecCountEntry` | missing message | Fixed by complete upstream schema |
| `.tipb.SQLMeta` | missing message | Fixed by complete upstream schema |
| `.tipb.PlanMeta` | missing message | Fixed by complete upstream schema |
| `.tipb.EmptyResponse` | missing message | Fixed by complete upstream schema |
| `.tipb.TopRUConfig` | missing message | Fixed by complete upstream schema |
| `.tipb.TopSQLSubRequest` | missing message | Fixed by complete upstream schema |
| `.tipb.TopSQLSubResponse` | missing message | Fixed by complete upstream schema |
| `.tipb.ChecksumScanOn` | missing enum | Fixed by complete upstream schema |
| `.tipb.ChecksumAlgorithm` | missing enum | Fixed by complete upstream schema |
| `.tipb.ExecType.TypeExplainForConnection` | missing or mismatched enum value | Fixed by complete upstream schema |
| `.tipb.CompressionMode` | missing enum | Fixed by complete upstream schema |
| `.tipb.VectorIndexKind` | missing enum | Fixed by complete upstream schema |
| `.tipb.VectorDistanceMetric` | missing enum | Fixed by complete upstream schema |
| `.tipb.ANNQueryType` | missing enum | Fixed by complete upstream schema |
| `.tipb.FTSQueryType` | missing enum | Fixed by complete upstream schema |
| `.tipb.FTSBooleanOccur` | missing enum | Fixed by complete upstream schema |
| `.tipb.FTSBooleanModifier` | missing enum | Fixed by complete upstream schema |
| `.tipb.FTSBooleanTermType` | missing enum | Fixed by complete upstream schema |
| `.tipb.ColumnarIndexType` | missing enum | Fixed by complete upstream schema |
| `.tipb.JoinType` | missing enum | Fixed by complete upstream schema |
| `.tipb.JoinExecType` | missing enum | Fixed by complete upstream schema |
| `.tipb.RuntimeFilterType` | missing enum | Fixed by complete upstream schema |
| `.tipb.RuntimeFilterMode` | missing enum | Fixed by complete upstream schema |
| `.tipb.TiFlashPreAggMode` | missing enum | Fixed by complete upstream schema |
| `.tipb.WindowBoundType` | missing enum | Fixed by complete upstream schema |
| `.tipb.RangeCmpDataType` | missing enum | Fixed by complete upstream schema |
| `.tipb.WindowFrameType` | missing enum | Fixed by complete upstream schema |
| `.tipb.TiFlashHashTableSizeKind` | missing enum | Fixed by complete upstream schema |
| `.tipb.GroupingMode` | missing enum | Fixed by complete upstream schema |
| `.tipb.SPFreshTruncateMode` | missing enum | Fixed by complete upstream schema |
| `.tipb.SPFreshFilterExprColumnSource` | missing enum | Fixed by complete upstream schema |
| `.tipb.SPFreshErrorCode` | missing enum | Fixed by complete upstream schema |
| `.tipb.tici.IndexType` | missing enum | Fixed by complete upstream schema |
| `.tipb.tici.ParserType` | missing enum | Fixed by complete upstream schema |
| `.tipb.CollectorType` | missing enum | Fixed by complete upstream schema |
| `.tipb.ItemInterval` | missing enum | Fixed by complete upstream schema |
| `.tipb.Event` | missing enum | Fixed by complete upstream schema |

## Complete coprocessor and MPP gap list

These 29 gaps are removed by native package ownership. In addition to 10
messages and 17 fields, the old MPP TaskMeta declared a plain keyspace_id
instead of a oneof arm and int32 instead of the APIVersion enum. The same
native response generator now respects all four upstream SharedBytes fields.

| Declaration | Former gap | Status |
| --- | --- | --- |
| `.coprocessor.Request.tasks` | missing field | Fixed through native package ownership |
| `.coprocessor.Request.table_shard_infos` | missing field | Fixed through native package ownership |
| `.coprocessor.Request.versioned_ranges` | missing field | Fixed through native package ownership |
| `.coprocessor.ShardInfo` | missing message | Fixed through native package ownership |
| `.coprocessor.TableShardInfos` | missing message | Fixed through native package ownership |
| `.coprocessor.TiCIEstimateCountRequest` | missing message | Fixed through native package ownership |
| `.coprocessor.TiCIEstimateCountResponse` | missing message | Fixed through native package ownership |
| `.coprocessor.TableRegions` | missing message | Fixed through native package ownership |
| `.coprocessor.BatchRequest.context` | missing field | Fixed through native package ownership |
| `.coprocessor.BatchRequest.tp` | missing field | Fixed through native package ownership |
| `.coprocessor.BatchRequest.data` | missing field | Fixed through native package ownership |
| `.coprocessor.BatchRequest.start_ts` | missing field | Fixed through native package ownership |
| `.coprocessor.BatchRequest.schema_ver` | missing field | Fixed through native package ownership |
| `.coprocessor.BatchRequest.table_regions` | missing field | Fixed through native package ownership |
| `.coprocessor.BatchRequest.log_id` | missing field | Fixed through native package ownership |
| `.coprocessor.BatchRequest.connection_id` | missing field | Fixed through native package ownership |
| `.coprocessor.BatchRequest.connection_alias` | missing field | Fixed through native package ownership |
| `.coprocessor.BatchRequest.table_shard_infos` | missing field | Fixed through native package ownership |
| `.coprocessor.BatchResponse` | missing message | Fixed through native package ownership |
| `.coprocessor.StoreBatchTaskResponse.data_merged_into_response` | missing field | Fixed through native package ownership |
| `.coprocessor.DelegateRequest` | missing message | Fixed through native package ownership |
| `.coprocessor.DelegateResponse` | missing message | Fixed through native package ownership |
| `.mpp.TaskMeta.keyspace_identity` | missing field | Fixed through native package ownership |
| `.mpp.IsAliveRequest` | missing message | Fixed through native package ownership |
| `.mpp.IsAliveResponse` | missing message | Fixed through native package ownership |
| `.mpp.DispatchTaskRequest.table_regions` | missing field | Fixed through native package ownership |
| `.mpp.DispatchTaskRequest.table_shard_infos` | missing field | Fixed through native package ownership |
| `.mpp.TaskMeta.keyspace_id` | field contract differs | Fixed through native package ownership |
| `.mpp.TaskMeta.api_version` | field contract differs | Fixed through native package ownership |


## Persisted DDL lifecycle follow-up (2026-09-30)

Compared worker/scheduler sources at Go master e953a09d9d; the intervening
master change affects numeric COALESCE planning, not these sources or module
pins. Existing persisted CHECK/create schema/create table/create tables/rename
tables/drop schema/drop table paths now share one lifecycle. Removed seven
worker loops, action-owned history writes, premature SYNCED assignments, and
repeated full queue scans for one job. DONE remains active; durable MDL recovery
precedes any next phase or the separate history transaction. Full submitted
table-ID scopes, system-schema owner-column omission, notification failure,
history failure, owner-conditioned MDL cleanup, and pre-commit ownership checks
use shared handling. Batch completion uses Go SetTableInfos rather than a
single-table finish, preserving the full history list and completing empty batches.

Still open: non-CHECK SQL admission usually bypasses persisted jobs; DROP lacks
Go finishDDLJob's delete-range registration/GC owner and typed DropTableArgs;
materialized-view seed action/reorg gaps remain and those actions are not
dispatched (shared completion cleanup is recorded below); general
pause/cancel/reorg scheduling and MDL-disabled operation
are not complete. TiFlash placement creation/deletion, partition handling and
progress/backoff remain open as described above. These are source-backed
maintenance changes, not a complete pkg/ddl or pkg/meta/model acceptance claim.


## Materialized-view seed completion ownership (2026-09-30)

Compared pkg/ddl/mview_worker.go and job_worker.go at master e953a09d9d. Five
remaining private history writers in tidb-exec/src/cluster_ddl.rs are removed.
Both explicit seed entrypoints reuse the shared point lookup, MDL recovery,
active-state lifecycle and history finalizer. DONE/ROLLBACK_DONE remain active
until synchronization; cancellation uses the same finalizer immediately and
persists its error rather than constructing and discarding mutations. Initial
build errors persist in Job.Error/ErrorCount through rollback and history,
replacing an invented success warning. The shared helper also owns CHECK's
error-field update. Seed actions remain excluded from the live dispatcher.

Three red reproductions now pass, along with 93 source planner, 7 commit
classification and 4 embedded DDL tests, affected all-target compilation and
root lint. Exact commands, source decisions, files and limitations are in
[the materialized-view receipt](../../full-structural-parity-execplan.md#materialized-view-seed-completion-cleanup-receipt-2026-09-30).

This does not certify complete materialized-view or pkg/ddl parity. Remaining
seed differences include build/reorg transaction ownership, rollback data GC
and base/log back-references, required-system-table failure transitions, and
worker validation/error identity details. General DDL admission/scheduling,
delete-range GC, MDL-disabled operation and TiFlash placement gaps above remain
open. No full package validation, multi-node interoperability run or benchmark
performance claim accompanies this maintenance repair.


## Persisted action queue ownership (2026-09-30)

Go master e953a09d9d job_worker.go owns updateDDLJob and immediate cancelled-job
finalization after discarding action mutations. Removed 17 action-local queue
writes from existing Rust CHECK/catalog/MV seed planners. The shared worker
now receives borrowed job state, metadata writes, updateRawArgs and any handled
rollback error. It persists one envelope update or finalizes cancellation.
Historical Job.Error no longer cancels successful create/schema/batch/rename
actions. Fresh cancellation errors increment ErrorCount once, including after
an owner-loss retry; cancellation neither announces nor waits for a new schema.
The separate CHECK validation-error state transaction remains explicit.

Red/green regressions and all 95 planner + 7 commit-classification + 4 embedded
DDL tests pass, as do affected compilation and root lint. Files, exact commands,
source decisions and publication gates are in
[the action-state receipt](../../full-structural-parity-execplan.md#persisted-action-state-ownership-receipt-2026-09-30).

Still open: ordinary non-cancelling errors return without Go's persisted
countForError/global error-limit/CANCELLING lifecycle; CHECK missing-object and
constraint validation does not yet consistently select Go's cancelled state.
Other worker validation/error identities and the previously listed admission,
reorg, rollback dependency/GC, scheduler, MDL-disabled and TiFlash ownership
gaps remain. The supported action list is unchanged; no unaccepted seed action
was dispatched and no package-complete parity or performance claim is made.

The [shared UPDATE owner repair](shared-update-owner-repair.md) removes the
UPDATE/ODKU write bypasses and executor undo logs, with fail-before/pass-after
regressions. E01 is repaired; E02 still requires FK plan integration.

The cache-owner continuation additionally reproduced two executor failures on
unchanged integration `5fcc321d48`: `prepared_in_predicate_uses_filtered_stats_for_cache_admission`
and `cached_index_join_compare_filter_rebinds_its_parameter`. Their original
assertions remain enabled. See the [cache repair receipt](shared-session-plan-cache-repair.md)
for the commands and independent baseline evidence. The broader MySQL pipeline
test also reproduces its existing account-name quoting assertion on unchanged
source; this is not counted as a passing protocol suite.

The [progress-ownership follow-up](rtree-progress-ownership-repair.md) rechecks
the same complete range-tree package and removes deep progress/result-tree
clones and detached test snapshots. Inserted, returned and completed progress
handles now share the original record. This closes the recorded P05 ownership
limitation without changing the unresolved-finding count.


The complete PD `metrics` and `resource_group/controller/metrics` prerequisites
are published in native `61e9a86b9261aff9588597a5899941df57f15b38`; formatting
follow-up `e3e8de80f2791ed725b6f06c25dfe4ed339ce564` is synchronized.
The [metrics owner receipt](pd-metrics-owner-repair.md) records both complete
inventories, independent Go runtime oracles, consumer rebinding and removal of
six native plus six TiDB collector definitions. Integration validation and the
mandatory publication gates are tracked there. Circuit breaker/grpcutil and the
broader W01 root/TSO/discovery owners remain open; finding counts are unchanged.


The complete PD `pkg/circuitbreaker` prerequisite is published in native
`44afb53ffadfb5a711fa9c459d95fd197a3adb1a` and synchronized; see the
[shared circuit-breaker receipt](pd-circuitbreaker-owner-repair.md). It removes
the private region-cache state machine and restores the shared execution,
settings, context and metric owner. Source, regression, complete-library and
TiDB integration evidence and publication gates are recorded there. Full grpcutil per-RPC placement, root/TSO/discovery and
parent-package acceptance remain open; finding counts are unchanged.


The [retry value-ownership re-review](pd-retry-value-ownership-repair.md) corrects
the earlier unrestricted retry acceptance. Go copies backoffers for each RPC;
Rust needed shared callback identity with independent scalar state, signed timing
arithmetic and reusable constructor options. The entire retry package and source
support are revalidated before implementing grpcutil. Parent ownership and
finding counts remain open and unchanged.


The [complete PD error owner](pd-errors-owner-repair.md) supplies all source
definitions, codes, cause chains and classification/logging helpers before the
grpcutil migration. It removes the uncoded breaker enum and duplicate native TSO
EOF/count diagnostics. Parent transport/lifecycle and all 77 findings remain open.

The subsequent [DDL pause lifecycle repair](../../ddl-pause-lifecycle-execplan.md)
repairs pause checkpoints and scheduler release in the existing shared worker.
D04 is partial: cancellation conversion and D05 error budgets remain open.
There are still 77 unresolved findings (71 open, six partial); no additional
package is accepted. Earlier continuity files are historical snapshots.

The [shared cancellation and error-checkpoint repair](../../ddl-cancellation-lifecycle-execplan.md)
subsequently closes D04/D06's recorded live control/object-validation gaps and
advances D05 to partial. It removes the detached CHECK rollback transaction,
preserves concurrent administrative state through the original transaction and
connects error checkpoints to the current global retry limit. At that checkpoint
the register had 75 unresolved findings (69 open, six partial) and ten repaired findings.
These are maintained live-path contracts, not complete pkg/ddl acceptance; external
error/panic/retry policy, durable SQL submission and scheduler ownership remain open.
Earlier reviews above retain their historical counts and pins.

The [action panic recovery repair](../../ddl-panic-owner-execplan.md) continues
that same transaction owner for planner and staged-validator unwinds. It preserves
Go's distinct panic counts, cancellation states, old errors and raw arguments,
discards unfinished writes and shares the existing process panic counter across
DDL and sessions. Owner loss and a conflicting pause cannot publish a stale
checkpoint. D05 remains partial and all counts remain unchanged: full returned-error
classification, transaction/retry policy and whole-action external-effect integration
are still open. The previous external-delivery allegation is qualified: none of the
nine live persisted handlers emits placement/label requests today.

The [shared DDL error conversion repair](../../ddl-error-conversion-execplan.md)
removes the server-owned DdlPlanError code map and the history waiter's forced
HY000. All 21 existing plan-error variants now share code selection through direct
execution, durable checkpoint and history readback; plain action errors persist
DDL/CodeUnknown as Go does. Legacy coded envelopes remain readable. This does not
restore RFC/class identity already erased by admission/storage producers, so D05
and the 75-unresolved count remain unchanged. Mixed-node error interoperability,
retry policy and whole-package acceptance are still open.

The [shared worker continuation repair](../../ddl-worker-continuation-execplan.md)
removes CHECK's private validation-failure memory and the escape of committed
action errors to scheduler polling. All live persisted actions continue through
the shared worker; retry waiting follows a successful checkpoint, uses the
committed count/current limit, and is interrupted by owner retirement. Failed SQL
results remain in durable history, including after owner replacement. D05 remains
partial and counts stay unchanged: source identity, complete error taxonomy,
configurable wait/metrics, transaction-reset policy and whole-action effects are open.
