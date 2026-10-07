# Audit and remove Go/Rust structural mismatches

This living ExecPlan follows root PLANS.md. Use the [current audit index](parity/current-audit/README.md), [finding register](parity/current-audit/structural-findings.json) and [structural batch map](parity/current-audit/remaining-batches.md) for current state. Historical sections below retain dated evidence; their counts are not current. Latest implementation: [shared table-request lifecycle](reader-task-batch-execplan.md). Latest cleanup: [unused DDL leaf retirement](parity/current-audit/unused-carrier-cleanup-validation.json).

Use the [current audit index](parity/current-audit/README.md) for publication policy and access status. Preserve concurrent changes in the existing Cloud checkouts and run the actual locked-build commit hook.

## Purpose and acceptance


Make Rust TiDB and native client-rust follow the production owners, state
transitions and observable behavior of current TiDB Go master and its pinned
client-go. SQL results, warnings/errors, authorization, isolation, failure
recovery, configuration and lifecycle behavior must match. Improve
sysbench/TPC-C/TPC-H/YCSB through source-compatible implementations; neither
extra features nor reduced correctness are acceptable ways to improve results.
Rust representation, ownership and crate boundaries should remain native.

The current plan starts at integration
`4285385fad20855487ec1d8ff113290d48f949a5`, freshly fetched Go master
`93a01d31f6da205ae4bf376825293903a6899fdb`, and native client-rust
`6f663b396552eec6d1bfad76b65f813e317884a4`. Normative external pins come from
that master's go.mod, not this integration branch's older Go checkout.
The [repair sequence](parity/current-audit/repair-sequence.md) records those
pins and assigns all 77 known open findings to 12 related workstreams. Eight
other findings retain their repair receipts. Finding counts are not proof that
every semantic mismatch has been discovered.

One complete Go package or pinned external-module package is the minimum
implementation and acceptance unit, including every production source,
build/platform/generated variant and input, original test/support artifact,
fixture and required validation gate. A package may map to multiple Rust crates,
but retains one atomic inventory, integration decision and receipt. Findings
and workstreams are planning aids, not partial port dispatch units. A correct
helper bypassed by a live entrypoint does not qualify. A Go package spanning
multiple workstreams remains open until all its obligations are met.

The active design and validation sections below supersede old priorities in
chronological receipts. Earlier revisions, counts, pending-work descriptions
and validation results in those receipts apply only to their recorded point
in time. This revision is a plan; it closes no production finding.

## Progress

- [x] (Table-request batch) Share ordinary/probe estimates and merge/lookup request policy; restore actual row/chunk handle order and retire the private small-batch bypass. Four baseline failures, 214 Rust passes and 8 wire controls; see `parity/current-audit/reader-task-batch-validation.json`. N03/O13 remain partial.

- [x] (Index-probe batch) Repair direct GROUP BY admission, self-join lookup alias identity and retained reader estimates together. See `parity/current-audit/index-probe-batch-validation.json`; three baseline failures, 21 Rust passes and six passing wire controls. E03/O13 remain partial; no package closure.

- [x] (2026-10-07, B04 constraint admission) Share FK/CHECK original-schema admission, implicit-index backfill and persisted CHECK metadata. Remove the metadata-only FK index path and replace stale grouped-CHECK success assertions. See `parity/current-audit/constraint-admission-batch-validation.json` for grouped results and limits; D01/D02/D11 remain unresolved.

- [x] (2026-10-06, PD bootstrap policy) P03/P06/N03 now share nonblocking configured connections, accepted member publication and forced-PD timestamp provider selection. Eight regressions failed before repair; 364 selected tests, affected all-target checks and lint pass. The separate TSO primary may be unreachable while an explicitly enabled healthy proxy serves requests. Automatic network-failure forwarding and full-package obligations remain open. See `parity/current-audit/pd-bootstrap-policy-batch-validation.json`.
- [x] (2026-10-06, TSO proxy batch) Maintain P03/P06/N03 discovery, shared wire streams, single-dispatcher ownership and SQL option publication together. Three regressions failed before; 343 selected tests and affected checks pass. See `parity/current-audit/tso-proxy-batch-validation.json`; all parent/package boundaries remain explicit.


- [x] (2026-10-06, PD forwarding batch) Connect P03/T02/N03 unary selection, canonical startup options, immutable metadata and all supported native/adapter callers; remove three duplicated failover loops. Four baseline socket failures precede 336 selected passes and affected checks. See `parity/current-audit/pd-forwarding-batch-validation.json`; TSO proxy/router and parent packages remain open.


- [x] (2026-10-05, JSON numeric batch) Maintain K03/X01 through shared JSON source conversion, table warning/error completion, integer/real/decimal expression callers and typed cast batches. Four baseline Rust and six MySQL failures precede 129 Rust and seven MySQL passes (eleven existing ignored tests). See `parity/current-audit/json-numeric-batch-validation.json`; both parents and complete package acceptance remain partial.

- [x] (2026-10-05, ordinary forwarding batch) Connect T02/N03 forwarding configuration, canonical proxy selection/publication, all command metadata and exact-generation failure feedback. Retain source mode and caller cancellation/deadlines; correct unary command metrics. Three baseline failures precede 31 native and 74 TiDB passes, affected checks and lint. See `parity/current-audit/ordinary-forwarding-batch-validation.json`; complete cache/health/flow/package obligations remain open.

- [x] (2026-10-05, connected replica routing) Share native ReplicaRouting between PdRpcClient and ClientPd; repair point/batch/scan follower/learner routing, stale leader-probe retries and busy/all-busy fallback. Canonical store load/health feed the selector; remove the private adapter address cache. Three grouped regressions fail before/pass after; 52 native and 31 TiDB tests pass. See `parity/current-audit/replica-routing-batch-validation.json`. T02/O13/N03 remain partial for broader ownership and live-cluster/package obligations.


- [x] (2026-10-05, O13/N03 batch) Connect retained settings, physical response estimates and lookup sizes to adaptive coprocessor routing, timeouts and counters. Three baseline regressions fail; 93 distinct Rust cases pass (one existing scaling test ignored). See `parity/current-audit/cop-read-policy-batch-validation.json`; parent findings remain partial.

- [x] (2026-10-05, session migration batch) Compose portable state, shared text/binary prepared IDs, bindings, variable/RNG restoration, historical-read admission and grant SQL-mode/output consumers. Remove stale unsupported/response-shape assertions. S03 advances to partial; N03/S04 remain partial. Complete source-package acceptance remains open. See `parity/current-audit/session-migration-batch-validation.json`.

- [x] (2026-10-05, PD provider/bootstrap batch) Repair requested-keyspace initialization before TSO discovery, shared minimum-provider/compatibility selection and optional metadata-header handling across native adapters. Four grouped regressions fail before; 167 PD and 40 keyspace cases pass after. See `parity/current-audit/pd-provider-bootstrap-batch-validation.json`; P03/P06 remain partial and broader package acceptance remains open.

- [x] (2026-10-05 UTC, B01 connected repair) Share service-mode/group discovery and wire routing across native/TiDB; bound refresh, preserve metadata independence, retire streams and join discovery on close. Native master bb8206e is published and synchronized. Four baseline failures precede 163 native PD and 79 TiDB PD passes (one existing live-PD test ignored); see `parity/current-audit/pd-service-discovery-batch-validation.json`. P03/P06 remain partial; GetMinTS provider selection, option consumers and whole-package obligations remain explicit. No-op sync now preserves all 322 source-file contents and mtimes, avoiding regeneration-only rebuilds.

- [x] (2026-10-03, TiFlash batch) Repair count/reset/physical status and retired-ID readiness, retained cache/backoff/HTTP discovery and secure configuration consumers together. Six red regressions, 29 distinct targeted Rust cases, isolated Go policy trace, affected all-target checks and lint pass. Full package and multi-node acceptance remains separate; publication gates are tracked in the batch receipt.


- [x] (2026-10-01, concurrent integration) Preserve incoming 32666fbaf0 while removing its Go-absent TopN heap dump. A new virtual-row regression fails on its assumed column zero before removal; all 54 executor TopN cases, all targets, affected-file formatting and merged-tree lint pass. No whole sortexec acceptance is claimed. The merge still uses the actual hook and fresh locked build; see `parity/current-audit/pd-opt-owner-repair.md`.
- [x] (2026-10-01, W01 prerequisite) Complete pinned PD `metrics` and `resource_group/controller/metrics`, with independent inventories for both packages. Native master publishes 61e9a86 and formatting follow-up e3e8de8; all 32 collectors and 81 prebound observers match Go runtime oracles, 110 PD/1,486 native tests pass (two ignored), strict Clippy/all targets/formatting pass. Remove six native and six TiDB duplicate collectors. Two runtime regressions prove the old batch schema and missing failure-total observation. TiDB sync, 28 PD/23 driver tests (one existing ignored), all-target compilation and lint pass. Actual-hook/fresh-locked-build gates control publication; incoming configured TopN scope retains one stable-tie failure and now passes the previously failing hidden-key projection case. See `parity/current-audit/pd-metrics-owner-repair.md`. Circuit breaker, grpcutil and parent discovery/root/TSO remain open.
- [x] (2026-10-01, W01 prerequisite) Complete pinned PD `pkg/circuitbreaker` in native `44afb53` and synchronize its published master revision. Remove the private region-cache state machine and migrate all seven existing call sites. Inventory all 14 production functions and ten original cases; 18 focused cases, original Go race/goleak plus boundary oracle, 1,504 native tests (two ignored), strict Clippy/all targets/formatting pass. TiDB passes 28 PD/23 driver tests (one existing ignored), all targets and lint. See `parity/current-audit/pd-circuitbreaker-owner-repair.md` for compatibility and mandatory actual-hook/fresh-locked-build publication gates. Full grpcutil/per-RPC interception and root/TSO/discovery remain open.
- [x] (2026-10-01, W01 transport contract review only) Inventory all eleven production functions, three constants, original tests/support and ten PD consumers of the complete pinned `pkg/utils/grpcutil` package. Original cases plus six added Go contracts pass with race/goleak under PD-owned and TiDB-selected error/zap versions. Paired socket probes show tonic 0.12.3 lazy dialing does not start without an RPC and eager dialing returns before server HTTP/2 SETTINGS, unlike the tested Go contracts. See `parity/current-audit/pd-grpcutil-contract/README.md` and generated observations. No native production integration, dependency advancement or package acceptance; grpcutil and the 85/77/8 structural register remain open/unchanged. The plan records all remaining adapter acceptance gates before migration/removal.
- [x] (2026-10-01, W01 adapter experiment only) Correct the proposed reconnect design from pinned Go: an established default-policy connection returns to Idle and waits for demand. A narrow h2 readiness accessor and isolated owner pass eight lifecycle cases but fail two Go first-frame tests (PING/unknown before SETTINGS; initial SETTINGS ACK). Preserve failures and reject production promotion; see `parity/current-audit/pd-grpcutil-contract/h2-candidate-review.md`. No native dependency change, complete-package acceptance, or structural-register closure.
- [x] (2026-10-01, W01 error prerequisite) Complete pinned PD `errs` in native 6163ecf: all 36 definitions, six constants, four helpers and both resource-group wrapper methods, plus 32 source consumer files. Remove the private breaker enum and duplicate TSO EOF/count diagnostics. Three runtime regressions fail before and pass after; independent Go race oracles match under PD-own and TiDB-master dependency pins. Native passes 1,523 library/147 PD cases (two ignored), strict Clippy/all targets/formatting; exact TiDB sync, 28 PD/23 driver tests (one ignored), all targets and lint pass. Actual-hook/fresh-locked-build gates control TiDB publication. See `parity/current-audit/pd-errors-owner-repair.md`; grpcutil and parent lifecycle remain open.
- [x] (2026-10-01, W01 dependency re-review) Revalidate complete PD `pkg/retry` after restoring Go value copies, signed timing/count arithmetic and ordered reusable options. The transport review invalidated earlier unrestricted coverage: unsigned saturation and uncloneable callbacks were gaps. All source functions/support are remapped; original Go race/goleak and source oracles pass, along with 136 focused PD/1,512 native cases (two ignored), strict Clippy, all targets and formatting. Native master publishes 69877b9; exact TiDB sync, 28 PD/23 driver tests (one ignored), all targets and lint pass. Actual-hook/fresh-locked-build gates control TiDB publication. See `parity/current-audit/pd-retry-value-ownership-repair.md`. Full grpcutil remains open.
- [x] (2026-10-01, W01 prerequisite) Complete pinned PD `opt`, remove the scan-only options struct and migrate existing native/TiDB callers. All 38 functions, source artifacts, original tests, shared support and 22 caller files are inventoried. Original Go race/goleak, 103 focused PD cases, 1,478 native library tests (two existing ignored), strict Clippy/all targets/formatting pass. Native master publishes df0d4cc; exact TiDB sync, 23 driver/26 PD tests (one existing ignored), all-target compilation, root lint and inventory checks pass. Actual hook and fresh locked server build gate publication. See `parity/current-audit/pd-opt-owner-repair.md`. Parent discovery/root/TSO acceptance remains open.
- [x] (2026-10-01, W01 prerequisite) Implement complete pinned PD `pkg/retry`, default initialization and bounded membership probes. Both transport regressions fail before repair; original Go race/goleak, 95 focused PD cases, 1,470 library tests (two existing ignored), strict Clippy/all targets/formatting and artifact checks pass. Native master publishes 2fd0ece; exact TiDB sync, 10 bridge/26 PD tests (one existing ignored), affected all-target compilation, root lint and inventory checks pass. Actual hook and fresh locked server builds gate publication. See `parity/current-audit/pd-retry-owner-repair.md`.
- [x] (2026-10-01, integration merge) Preserve incoming 107e8e2a5d while fixing its TopN candidate loss against current Go master; remove the older competing serial-spill lifecycle and reconnect the existing workers. Red regressions precede each correction; all 53 executor TopN cases pass. The eight-case session suite retains only two EXPLAIN estimate failures reproduced with unchanged HEAD. No sortexec package acceptance or register closure is claimed. Merge hook and fresh locked build still gate publication.
- [x] (2026-10-01, W01 prerequisite) Implement the complete pinned PD `pkg/batch` owner and replace native TSO collection/default admission/completion. Two source wire regressions and callback ordering fail before repair; all 78 focused PD cases and 1,453 native library tests pass (two existing ignored), with original Go race/goleak, strict Clippy/all-targets/formatting. Published native master as bcf74b7; exact TiDB synchronization, 10 bridge tests, 26 PD tests (one existing ignored), affected all-target compilation and root lint pass. The actual hook and fresh pre-push locked server builds gate TiDB publication; the final response records their results. See `parity/current-audit/pd-batch-owner-repair.md`.
- [x] (2026-10-01, W01 prerequisite) Implement the complete pinned PD `pkg/connectionctx` owner and native single-leader integration. Reproduce same-URL healthy-stream replacement before edits; preserve URL-keyed ownership, rejected candidates, cancellation and retained handles. Original Go race/goleak tests and 1,435 native tests pass (two pre-existing ignored). Native strict Clippy/all-targets/formatting, master publication as 4e3169e, exact TiDB synchronization, 10 bridge tests, 26 PD tests (one existing ignored), affected all-target compilation and root lint pass. The actual hook and fresh pre-push locked builds gate TiDB publication; see `parity/current-audit/pd-connectionctx-owner-repair.md` and the final publication response.
- [x] (2026-10-01, W01 prerequisite) Inventory and implement the complete pinned PD `pkg/deadline` package plus its native TSO caller integration. Reproduce missing response/body deadlines, completed-result loss on retirement, and a lost wakeup in the existing cancellation adapter. Complete-package evidence is the deadline leaf only; parent PD/TSO/discovery acceptance and P03/P06/P07 remain open.
- [x] Finish deadline-leaf validation: 1,422 native library tests (two existing ignored), original Go race tests, strict Clippy/all-targets/formatting, native master publication as 5928b6e, exact TiDB synchronization, 10 bridge tests, 26 PD tests (one existing ignored), all-target compilation and root lint. TiDB publication is gated by the actual hook and fresh pre-push locked builds; see `parity/current-audit/pd-deadline-owner-repair.md` and the publication response.
- [x] Preserve the three incoming partition-DDL commits through b0eccee03f in a conflict-free merge. Merged-tree validation passes 11 partition metadata tests, four cluster exchange-partition tests and root lint. The combined merge commit still requires the actual hook and a fresh pre-push locked server build; incoming DDL package acceptance remains open.
- [x] (2026-10-01, full-picture plan) Pull both implementation branches, fetch Go master, reconcile the current 85-record register and assign all 77 open findings exactly once. Replace stale active TiPB and background-lifetime work with the current dependency and removal gates. No production code or finding status changes.
- [ ] Establish the next complete PD root/TSO/discovery package closure, original-case mapping and fail-before lifecycle/transport probes (W01). Preserve all other root APIs, variants and dependencies in its acceptance scope.
- [ ] Migrate complete native routing owners and every TiDB storage consumer before retiring competing TiDB algorithms (W02); retain the MPP transport retirement dependency.
- [ ] Complete the coupled shared session/table, versioned schema/identity and durable DDL migrations (W03–W05). Prove min-start-TS reporting before GC activation.
- [ ] Complete ready independent account/charset/configuration and cache packages (W06/W07), then their full parent-package integration obligations.
- [ ] Complete shared optimization/typed execution, MPP, Domain job services, runtime information and inference (W08–W12), gathering overlapping Go-package obligations into one claim.
- [ ] Establish comparable workload baselines before relevant runtime changes; validate all four benchmark families without suppressing errors or unsupported cases.
- [ ] Complete the remaining source/test/variant coverage queue beyond known findings. Audit closure requires current evidence for every package, not only an empty finding list.

The entries below preserve earlier completed work and still-open obligations.

- [x] (2026-10-01, A01 runtime follow-up) Refresh master/integration. Reproduce the ordinary and prepared joined UPDATE privilege bypass before edits; verify Go master rejects it and re-resolves targets after DDL. Disprove the column-only SELECT allegation with a Go oracle.
- [x] Complete A01 runtime regression validation and correct the audit: 20 table-privilege tests and the Go oracle pass; five fail-before regressions are repaired. Broad grant/prepared/EXPLAIN suites have no new failures versus HEAD (31/1/12 remain). All-target compilation and root lint pass. Publication uses both locked build gates; full planner/session package acceptance remains open.
- [x] (2026-10-01, system-wide design review) Recheck selectable SQL entrypoints, compiler/transaction boundaries, DML handoff, DDL dispatch, boot composition and client routing. Put the shared cache work inside the wider dependency/risk order below. This source review changes the implementation plan only; no runtime finding is repaired or package accepted.
- [x] (2026-10-01, shared cache design review) Refresh integration/master; trace all four production Ristretto importers, their configuration and owning lifetimes. Recheck 98 consumer/subpackage artifacts against the existing inventory. Include inference, which is absent from this branch's Go checkout. Record the shared dependency and consumer retirement sequence in `parity/current-audit/shared-cache-owner-review.md`; no runtime/package acceptance is claimed.
- [x] (2026-10-03, shared cache batch) Implement and validate the complete pinned Ristretto root, migrate all three existing cache owners, remove Stretto and both FIFO stores, and repair B01/B02/C03/C04. Root and LFU have atomic package receipts; broader bindinfo/copr/Domain and inference acceptance remains separate.
- [x] (2026-10-01, LFU review follow-up) Recheck all five LFU artifacts and inventory all 91 artifacts of pinned Ristretto v0.1.1. Reproduce unguarded public eviction lifetime and fix TriggerEvict/SetCapacity; both are exercised by the passing regression. Add a deterministic primary-admission failure and restore the pressure test's Go Get observation path (still fails). Native owner/parent suites pass 33 tests; two dependency probes remain ignored and unaccepted. The original 10 LFU and 73 Ristretto tests pass with race detection. All-target compilation, lint, formatting and inventory checks pass. Both locked publication builds remain required.
- [x] (2026-10-01, LFU owner review/native repair) Review all five artifacts at current master; reproduce and repair premature Close, fake-table/negative-key classification, and signed-shard drift; remove the exclusive per-access primary mutex/clones and synthetic trigger tables. Map all ten original tests; 19 LFU and 13 parent tests, original Go race suite, all-target compilation and lint pass. Publication still requires both locked server builds.
- [x] (2026-10-03, C04 external boundary) Replace Stretto with the complete pinned root owner. Both retained admission/pressure regressions execute and pass; original policy metrics assertions are restored. See `parity/current-audit/shared-cache-batch-repair.md` and the atomic LFU receipt.

- [x] (2026-10-01, retained progress ownership) Recheck the complete seven-artifact `br/pkg/rtree` package against master; reproduce stale retained progress records; replace value copies with shared handles through insertion, lookup, collection and deletion; restore original retained-pointer tests. Forty Rust tests, all original Go race tests, all-target compilation and lint pass. Both locked server-build gates remain required for publication.

- [x] (2026-10-01, rtree follow-up) Review all seven artifacts of `br/pkg/rtree`; remove RangeFile/TestFile, generic file adapters and RPC-range projection; migrate restore callers. Both RPC type regressions, 38 Rust tests, all original Go race tests, update/merge workloads, all-target check and lint pass. Publication requires the hook and fresh pre-push locked build.

- [x] (2026-10-01, P04 repair) Remove both local restore-protocol types and the PITR test projection across the complete eight-artifact `br/pkg/restore/utils` owner; preserve generated payloads/shared references and borrow selected rules. All original Go race tests, 36 Rust tests, five merge workloads, all-target compilation and lint pass. Publication requires both locked-build gates below.

- [x] (2026-10-01, O12 repair) Transcreate the complete `pkg/domain/globalconfigsync` package (all three artifacts), its session metadata/publication and domain keeper integration. Preserve bounded blocking notification, no reload notification, one PD attempt and joined shutdown. Validate original Go cases, SQL regressions and real mock-RPC transport before publication.

- [x] (2026-09-30, subsystem review) Refresh integration/master/native references; trace additional optimizer, table, schema, session, executor and domain owners. Add 18 findings, bringing the register to 72 records / 68 unresolved. Reproduce generated-column strict-mode failure and unsupported session-state/historical/BR job entrypoints.
- [x] Account for all 83 Rust crates and 856 inventoried TiDB package directories in a checked scope matrix; preserve unreviewed variants/tests/dependencies and exclude disproved candidates. No whole-package acceptance or production repair is claimed.
- [x] Validate subsystem audit evidence: diagnostic, scope generation, 72 unique IDs, Python syntax, receipt/source links, rustfmt and root lint pass. Publication must use the actual hook and fresh pre-push locked build; the publication response records those results.

- [x] (2026-09-30, expanded review) Refresh integration/master and verify native master; add 13 source-confirmed findings with six groups of executable observations and passing controls. The consolidated register has 54 tracked findings, 50 unresolved. No production repair or complete-package acceptance is claimed.
- [x] Validate expanded audit evidence: both retained probes complete, 54 unique IDs and receipt links checked, diagnostic source formatted, root lint passes. Publish through the mandatory hook build and fresh pre-push locked build; the publication response records their results.

- [x] (2026-09-30) Remove C01's per-statement physical vectors and DML-only metadata LRU; integrate the shared session owner, close policy, flush and server lifetime/response paths.
- [x] Reproduce four session ownership failures and LRU replacement pressure on baseline; 128 session prepared tests, 9 LRU tests, 31 server prepared tests and both new server regressions pass. Three broader-suite failures reproduce unchanged and remain open.
- [x] Complete the C01 all-target check and root lint. Publication uses the mandatory hook build and a fresh pre-push build; commands/results/limits are in `parity/current-audit/shared-session-plan-cache-repair.md` and the publication response.

- [x] (2026-09-30) Remove all five remaining handwritten PD/BR/TiKV/etcd schema projections and the handwritten MPP fixture stub owner; migrate all callers to complete native/descriptor/upstream contracts. P01/P02 contract gaps are repaired; external helper/runtime package acceptance remains open.
- [x] Confirm three fail-before wire/identity regressions; 55 protocol tests, 45 PD and 423 transaction tests (10 already ignored), original Go package tests, every direct consumer target and root lint pass. Details: `parity/current-audit/complete-protocol-owner-repair.md`.

- [x] Migrate ordinary/joined UPDATE and ODKU record writes to a shared owner; remove executor undo logs and route remaining session DML callers through statement staging.
- [x] Reproduce alias/FK/ODKU/EXPLAIN rollback failures before fixes; preserve distinct per-target update tracking and base-row merging.
- [x] Validate shared UPDATE changes and record remaining plan-driven FK/materialized-source gaps; publication uses the mandatory hook and fresh pre-push builds.

- [x] Pull integration and refresh master; clean initial tree.
- [x] Enumerate upstream scope: 856 Go package directories, 4,420 Go source/test files; 83 Rust crate manifests.
- [x] Inventory all tracked TiDB and pinned client-go artifacts and Rust gap candidates; classify confirmed mismatches separately from unreviewed evidence.
- [x] Replace partial TiPB schema ownership with the complete pinned external package inputs, generation and drift gate; validate original Go tests and Rust consumers.
- [x] Repair MPP statement/query/gather/task identity and carry the existing server-info identity; 98 targeted tests, lint, hook and fresh pre-push locked builds passed; published as 7d8d69b6a0.
- [x] Remove duplicate TiFlash poller startup, detached lifetime and private DDL publisher; validate the shared owner and HTTP consumers.
- [x] Publish shared persisted DDL worker synchronization and completion (d54903d0b2).
- [x] Remove five materialized-view seed history writers; preserve durable errors and reuse the shared barrier/finalizer without enabling seed dispatch. Published as 6ed271503c after targeted tests, lint and both locked build gates.
- [x] Remove all 17 action-stage queue writers; share fresh cancellation/error handling and preserve historical retry diagnostics. Published as 9f0a41b5db after targeted tests, compilation, lint and both locked server build gates.
- [x] (2026-09-30) Expand the current-source register to 29 known structural findings; compare all five remaining local protobuf projections; reproduce PD keyspace-zero presence loss; inventory PD-client and etcd-API packages. This is an audit checkpoint, not exhaustive semantic or package acceptance.
- [x] (2026-09-30, follow-up) Trace session/planner/executor and runtime-provider owners beyond the initial register: 12 additional findings, five groups reproduced through SQL, 41 known findings total. Retain passing controls and source-only limits; no partial package accepted.
- [ ] Reconcile generic insertion policy with the ordinary table owner.
- [ ] Reconcile remaining native routing/RPC and operation-lifetime owners.
- [ ] Resolve each confirmed baseline SQL/DDL/statistics failure at its owning package.
- [ ] Audit all remaining package source, variants, original tests, fixtures and integration paths; retain unreviewed status until complete.
- [ ] Run scope-specific validation, root lint, commit hook locked server build, fresh pre-push locked build; commit and push each reviewed package repair.

### Review after removals at 68d6de685a


The [latest complete known-register review](parity/current-audit/post-removal-structural-review.md)
retains 77 unresolved IDs. Six fresh diagnostics confirm removed repartition,
IMPORT and cluster fixtures no longer produce their old misleading behavior;
partial multi-action ALTER, ignored password history, generated-value truncation,
Latin-1 corruption and absent live summary data still reproduce. Complete owners
remain missing after withdrawal. Every unresolved ID has one primary workstream
and a current evidence disposition; no package acceptance or benchmark result
is claimed by this review. The audit ExecPlan records validation/publication.

## System ownership and repair order


The execution chain is server protocol to Session, then preprocessing/resolved
planning, then executor construction and Open/Next/Close. Table mutation and
distributed reads call the KV driver and native client. Domain owns shared
schema, statistics, privileges, bindings and service lifetimes. SQL DDL submits
persisted jobs and waits; workers own metadata transactions, reorganization
and schema synchronization. An owner here means the code responsible for state,
its transitions and its lifetime, not necessarily one struct or crate.

Go `pkg/session/session.go::executeStmtImpl` establishes transaction/statement
context before `pkg/executor/compiler.go::Compile` preprocesses with the
transaction context provider and optimizes against its InfoSchema.
`pkg/sessiontxn/interface.go` keeps separate statement timestamp, initialization,
retry, commit and rollback hooks. Preserve these responsibilities across text,
prepared, internal-session and specialized fast paths. Replace table-count or
storage-mode SQL interpreters with storage adapters to this common execution
chain. Preserve Go's shared candidate lifecycle in merge planning; whole
planner acceptance includes its other candidate rules and original tests.

The full finding/owner/removal/acceptance mapping is in
[repair-sequence.md](parity/current-audit/repair-sequence.md). Use its W01–W12
labels only to discuss dependencies. Build the actual package dependency closure
from pinned source before implementation; the map is not an invented import
DAG. Runtime activation can be cyclic even when imports are acyclic. In
particular, schema, SQL transaction context, DDL jobs and GC need coordinated
integration rather than another fallback owner.

Keep Go's distinct SQL TxnManager, native KV transaction, table/index assertion
policy, distributed task planning and per-consumer cache ownership. Remove only
the competing implementation whose responsibility and all callers have moved.
The following current paths illustrate the coupling: server startup still
selects one/two-table sessions; `real_tikv_node/mod.rs` retains the two-table
static catalog; cluster boot supplies an inert ConfiguredTable to process
construction; `physical_builder.rs::execute_dml_source` fully drains children;
`RealClusterDdl::execute` routes non-CHECK statements to direct metadata work;
and `client_bridge.rs::ClientPd` routes native lookups back through TiDB.
Changing one helper beneath these entrypoints leaves the structure incomplete.

Independent complete correctness repairs can proceed once their prerequisites
are available. Account-policy loss, charset corruption and generated-column
conversion must not wait for unrelated cache optimization. Broad package
completion still gathers every obligation: for example, executor acceptance
includes account statements, DML, parallel workers, IMPORT/BRIE and runtime
producers; Domain acceptance includes all enabled service lifecycles.

## Milestones and design


### 0. Freeze evidence and form complete package units


Refresh integration, Go master and native master. Record exact revisions and
source module pins, then use the existing inventory and coverage generators.
For each selected package, read doc.go if present and account for every artifact,
original case, production caller and dependency. Classify existing code as
reusable, needing replacement, or explicit unaccepted seed. Record changed-input
receipt invalidation; never relabel old partial receipts as current acceptance.
Keep all 969 inventoried package directories and 83 Rust crates in the review
queue. Inventory additional external dependencies when reached.

Use the 77-finding assignment as the known defect queue. Add newly proved gaps
with source evidence; reject stale keyword matches. Recheck broad baseline test
failures by exact test/source owner. Include master's range-count variable,
skyline policy and bootstrap-upgrade delta within complete affected packages.
The repaired TiPB/native protocol generation remains the source of declarations:
select pins from master, regenerate from full inputs and run drift checks;
do not introduce handwritten projections or edit generated outputs.

### 1. Complete native PD and storage ownership


Start with W01: the pinned PD root, `clients/tso`, `servicediscovery` and their
required complete dependency packages. Current inventory starts with 13, seven
and nine artifacts respectively, including original tests; these counts do not
bound the dependency closure. Review all root APIs as well as the three findings.
In native `src/pd/{client,retry,timestamp}.rs`, make request connections live
outside short metadata synchronization, retain the TSO worker completion owner,
apply source deadlines and propagate close through pending work and streams.
Implement source service-mode discovery and fallback through that same owner.
Validate stalled stream, concurrent metadata, leader change, close/reconnect and
mode-transition behavior before accepting it. Do not patch P07's lock in isolation
and report a complete PD port.

Then W02 supplies the complete client-go routing/RPC/transaction owners to
TiDB. Inventory `internal/locate`, `internal/client`, `tikvrpc`, `tikv` and
transaction/snapshot dependencies, preserve explicit retry limits and operation
lifetimes, and wire TiDB's latency/health/tick events to native state. Migrate
all TiDB consumers of competing routing/transport methods. Distributed task
planning remains in TiDB. Final removal of a transport still used by MPP waits
for W09. Publish accepted native packages to client-rust master, synchronize
TiDB using the existing script, and validate the integrated callers.

### 2. Unify SQL/table, schema and durable DDL lifecycles


W03 removes alternate interpreters and narrowed execution handoffs by moving
all entrypoints to the ordinary session/compiler/executor and table owners.
Resolved privileges, FK plans, handle positions, mutation context and chunk
accounting survive planning through execution. Table/index callers choose
assertions and auto-ID mode. The buffer transports those decisions. Session
migration and historical queries use source state/timestamp/schema providers.

W04 supplies leased server identity, versioned bootstrap/upgrade and
InfoSchema, cached-table leases and min-active-start-TS reporting. First prove
that active transactions, cursors and required internal sessions protect their
timestamps, including with a Go peer already running GC. Only then activate the
Rust server's GC worker. Complete durable delete-range registration in W05 and
its consumption before enabling corresponding cleanup. Numeric server identity
also gates global KILL and distributed task identity.

W05 moves every SQL DDL caller to durable submission and completion. Complete
scheduler dependencies, pause/cancel/error/rollback transitions and schema
barriers before online reorganization or currently disabled MV seeds can become
live. Preserve both MDL and lease modes, partition identity and multi-action
atomicity. Move classic TiFlash rule creation to DDL and retired-table cleanup
to GC, keeping NextGen refresh where Go defines it. Compose progress/backoff,
PD HTTP security/discovery and affinity under their existing source owners.
DXF-backed reorganization requires W10. Delete the displaced SQL publisher,
private reorganization shortcuts and poller policies only with caller migration.

### 3. Complete independent policy and shared dependencies


W06 repairs complete account/security, charset and configuration owners with
their server/executor callers. Preserve durable epochs/policy fields, certificate
verification/reload, password history and input bytes. Connect command admission,
remote KILL and admin APIs to validated source configuration; accepting ignored
settings is not equivalent. Ready independent packages may precede milestone 2;
remote KILL and runtime administration wait for their actual shared providers.

W07 follows the [cache design](parity/current-audit/shared-cache-owner-review.md).
Implement the full pinned Ristretto root and dependency decisions, then complete
LFU/parent, binding and coprocessor consumer packages. Admission, publication,
queues, TTL, callbacks, metrics and close belong together. Both red C04 probes
must pass without compensating wrapper policy. The proposed
`rust/crates/tidb-ristretto` now implements the pinned root; the 2026-10-03 shared-cache receipt records package acceptance and native decisions.
Keep four independent cache instances/budgets/lifetimes including W12 inference.
Migrate binding refresh/GC/usage persistence and effective coprocessor config
along with storage. Remove Stretto and private FIFO stores when callers migrate.
C02 instead follows Go's instance plan cache/cloning; preserve the session LRU.

### 4. Complete shared planning, execution and runtime composition


W08 completes optimizer selection, candidate lifecycles, typed SQL/PB expression
construction/vector execution, parallel Apply and projection close/join. W09
uses those contracts and W02's storage owner for MPP fragments/tasks, exact range
splitting, incremental tracked responses and remote cancellation. Compose
TiFlash Compute topology and supported dispatch modes. Remove the private
scan-only planner, range envelope, full-stream materialization and plaintext
MPP transport only with their complete replacements.

W10 composes TTL, resource/runaway control, RU history, statistics GC, DXF,
cross-keyspace runtimes, IMPORT and BRIE as complete source packages become
ready. Preserve role/configuration gates, recovery and joined shutdown. W11
connects live virtual tables/discovery/fanout, summaries, statistics metrics,
plan replay, TopSQL, learning, telemetry and AZ policy to production events and
readers. An injectable test provider or registered metric does not prove this
integration. Go's current telemetry logs reports; no new uploader is needed.
W12 integrates the complete inference/provider/batcher closure, shared cache
and Domain lifetime through typed expressions. Do not invent provider modes.

These are integration gates, not instructions to defer every service until the
end. Ready leaf owners land with required callers; overlapping parent packages
retain one open acceptance claim until their complete source obligations pass.

### 5. Close coverage and validate workload performance


Recheck every inventoried package, original test and generated/build/platform
variant, including packages without known findings. A zero count in the known
register is insufficient. Trace each live entrypoint through owner construction,
config gates, success/failure, retry/cancel, persistence and close. Compare
mixed Go/Rust and Rust-only clusters for authorization, schema/DDL, timestamp
protection, owner failover, routing and configured services. Do not let a Go peer
mask a missing Rust background responsibility.

Measure performance before and after each relevant accepted owner change, not
only after the entire migration. Use matched Go/previous-Rust/new-Rust builds,
release profiles, hardware, storage topology, dataset, statistics, cache state,
security, isolation, quotas and concurrency. Preserve tool versions, seeds and
workload mixes. Capture throughput, latency distributions, CPU/RSS, errors,
retries and relevant network/first-row/memory behavior. Use repeated baseline
runs to quantify noise; investigate regressions instead of choosing favorable
samples or changing defaults. Historical benchmark reports are not current
baselines. No speedup is promised before measurement.

## Validation and commands


### Per-package development and acceptance


From `/Users/qiliu/projects/tidb`, refresh the existing source accounting:

    python3 rust/scripts/inventory-go-rust-parity.py --go-ref origin/master
    python3 rust/scripts/build-structural-coverage.py
    git diff --check

Review generator diffs; generation cannot certify semantics. Use a separate
checkout at the exact Go master pin for original Go tests, rather than the
integration branch's older Go files. External original tests use the selected
module version in a writable test checkout, never mutate the Go module cache.
Read `docs/agents/testing-flow.md`; apply the Bazel prerequisite gate only when
its source/import/module/build/test-target triggers apply. For a selected package
and test, substitute the recorded names in these command templates:

    ./tools/check/failpoint-go-test.sh pkg/<package> -run <TestName> -count=1
    go test -run <TestName> -tags=intest,deadlock ./pkg/<package>
    cargo test --locked --manifest-path rust/Cargo.toml -p <crate> --lib <filter>
    cargo test --locked --manifest-path rust/Cargo.toml -p <crate> --test all <filter>
    cargo check --locked --manifest-path rust/Cargo.toml -p <crate> --all-targets

Use the first Go command where failpoints require it, otherwise the second;
choose actual Rust test targets from that crate. Run targeted cases during
development and the complete original-package case/variant acceptance set
before claiming the package. A regression must fail before the fix and pass
afterward. Include failures after partial work, ambiguous RPC completion,
explicit exhausted retries, cancellation, close and restart where applicable.
Keep test results with exact commands/source pins. Ignored cases, untested
platforms and baseline failures remain explicit open obligations.

For native changes in `/Users/qiliu/projects/client-rust`, include appropriate
original client-go/PD cases, transport regressions and:

    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings
    cargo fmt --all --check

Unit success alone does not replace the complete package test/integration map.
Real TiKV and SQL integration follow the repository's playground and recording
workflows, with cleanup. At code completion in TiDB run `make lint`, then review
the diff for unrelated edits, duplicated policy and generated-source violations.

### Workload commands and interpretation


The existing sysbench harness assumes separately prepared comparable servers
and data. It does not create or restore the dataset. Use supported workload
names or Lua scripts and record all substituted values:

    python3 rust/scripts/compare-sysbench.py --server go:<port> --server before:<port> --server after:<port> --database <db> --workload <workload> --threads <n> --seconds 30 --rounds 3 --table-size <rows> --tables <n> --ps-mode auto --output <path>

TPC-C uses a pinned go-tpc tool with the repository's input-seed/measurement
patches. Verify the tool's actual supported options. Use identical restored
initial data for mutating samples; a fixed seed does not reset the database.
The current harness does not restore data between warmup/rounds. Before treating
mutating rounds as independent samples, add or reuse explicit dataset-restore
orchestration around each sample and verify it. The existing command below is
the execution interface, not evidence that those reset gates are implemented:

    python3 rust/scripts/compare-tpcc.py --tool <pinned-go-tpc> --server before:<port> --server after:<port> --server go:<port> --database <db> --warehouses <n> --threads 2 8 --count 10000 --warmup-count 1000 --rounds 4 --seed 1 --output <directory>

For TPC-H, re-establish a baseline for all 22 queries and compare result rows
before timing. The old SF50 report has stale pins and a q15 view-lifecycle race;
reuse its query artifacts only after isolating setup/cleanup per sample.
For YCSB, first pin an available tool and its SQL binding, schema, operation mix,
key distribution and seeds; no checked-in runnable harness was found in this
review. Record the resulting exact invocation before measuring. Raw KV numbers
do not establish TiDB SQL performance. Missing tools or unsupported queries
remain visible gaps; do not invent an invocation or silently omit failed runs.

### Publication and recovery


Native package changes publish first with normal `git push origin HEAD:master`
from `/Users/qiliu/projects/client-rust`. Then, from TiDB root, synchronize using:

    bash rust/scripts/sync-tikv-client-rs.sh

Inspect the recorded source SHA, applied compatibility patches and regenerated
outputs. If master advanced beyond the reviewed commit, review the delta before
acceptance. Do not edit the vendored/generated copy to bypass native ownership.
Re-run affected consumer gates, update receipts, and stage only reviewed files.
For every commit touching `rust/`, including plan-only changes, use the actual
pre-commit hook and confirm its locked server build succeeded:

    TERM=xterm git -c core.hooksPath=hooks commit -m '<package and behavior>'

After the final commit or amend, immediately before pushing, run a fresh locked
server build and only push on success:

    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

Do not bypass failed hooks, force-push, or present an incomplete package as
transcreated. If a package is unfinished, keep its implementation as explicit
local seed evidence and its acceptance open. Preserve user edits. Inventory
checks are rerunnable; synchronization must be inspected for source advancement.
Use disposable benchmark/test datasets and repository-prescribed cleanup;
never reclaim disk by deleting uncommitted work or the sole source of evidence.
If validation fails, retain the failure/log and repair its source owner before
publication. Recover a published bad change with a reviewed revert, not history
rewriting; test durable schema/state compatibility before reverting runtime work.

## Surprises & Discoveries


Incoming 32666fbaf0 adds a TopN column-zero debug read despite a removal title.
The diff, not the title, reveals the behavior. Three virtual rows without payload
columns reproduce a bounds panic; removing the Go-absent probe restores the
existing generic spill path. Preserve history and retain the new regression.

PD opt's production follower-handle setter sends a notification despite the
original test's contrary comment. Typed atomics must also retain Go's one-CAS
interval/concurrency behavior, not silently retry until a store wins. The whole
options package is now covered before activating unfinished discovery modes.

Retry integration's first push encountered incoming configured-TopN commit
0b2cf64069. It removes an implicit final ordinal tie-break absent from Go; two
exact source-order expectations fail only with that incoming file, while all
six pass on its pre-merge version with the same native retry dependency. Preserve
the incoming history and tests, and record both failures in the retry receipt.
Configured heap ordinals and stable-sort behavior still require W03's complete
shared-owner migration; this dependency publication does not certify them.

The next PD prerequisite reveals first-error initialization and an ignored
membership timeout. A retry policy alone cannot recover a stalled probe.
Source exponential and fixed helpers also intentionally return different errors
on cancellation and account time differently; their full shared owner replaces
ad hoc initializer behavior while retaining explicit parent obligations.
Self-review also fixes eager exponential timer construction and Tokio's short-
interval catch-up tolerance; red regressions pin both source timing contracts.

During batch publication, the integration remote advanced to 107e8e2a5d. Its
TopN chunk truncation loses candidates: ascending LIMIT 1 on [3, 0, 1] returns
3. Fresh Go master keeps complete chunks and trims the heap. Broader testing
also finds the pre-existing disconnected spill-worker path; 49d7ed0fca removed
the production call while leaving the workers and their failing test in place.
The merge repair preserves both histories, removes the limit-derived child
request/truncation and repeated serial-spill loop, and uses the existing worker
lifecycle for one or multiple workers. All 53 focused TopN cases pass. This is
maintenance of the existing sort port, not whole-sortexec acceptance.

Native TSO retained a private 64-request collector and 65,536-outstanding-batch
policy despite Go default 20,000-entry collection and a single RPC token. Both
wire regressions fail before the full batch dependency migration. A separate
self-review regression catches token return after discarded-request callbacks;
Rust field drop order now preserves the source order. Buffer reuse retains
allocation without retaining completed senders. No SQL throughput result follows
from the deterministic 20,001-request batching case.

The next complete PD prerequisite exposes unconditional native TSO replacement
on metadata reconnect. Go retains a registered healthy leader stream. A loopback
regression fails before the connectionctx migration, and six transport cases
now cover reuse, replacement, failed refresh, actual dialed URL and retained
pending-stream cleanup. This does not prove real PD timestamp duplication or
close the broad root/discovery/dispatcher findings.

During deadline publication, the remote integration branch advanced through three
partition-DDL commits to b0eccee03f. They are retained without history rewriting;
merge validation and publication builds are repeated. The incoming live
repartition helper explicitly lacks row backfill and skips index covering checks,
so W05/D01 must include that caller. This deadline prerequisite does not accept
its DDL behavior or certify complete DDL-package parity.

The current 77 open findings span shared broad packages, so assigning each ID
to an independent patch would fragment the very owners being restored. PD
shutdown/discovery/concurrency belong to one lifecycle; DDL, schema protection
and GC require coordinated activation; MPP retains a dependency on native
transport retirement. The 969-package inventory is broader than the finding
register and remains an independent completion obligation.

The full-picture follow-up confirms that dependency correctness alone cannot
repair live callers which bypass their intended owners. The default cluster
session and optional configured-table sessions are distinct paths; only the
latter has S01/S02's table-count dispatch. The main process authority also
requires an inert table at cluster startup. Do not generalize that finding
into a claim that the default session bypasses ordinary planning.

The wider cache scan found four production Ristretto consumers on Go master,
not three: `pkg/inference/sqlembed.go` is absent from this integration branch's
Go checkout. Rust has an explicit inference boundary, not an implemented fourth
cache. All 98 artifacts in the four consumer directories match the existing
inventory. A source-membership recheck is not semantic acceptance. Binding
full-image reload would also reset a new LFU engine's history on every refresh;
coprocessor storage replacement alone would retain its hardcoded configuration.

The LFU shutdown review found two public paths outside the new lifetime gate:
TriggerEvict and SetCapacity's final trigger. State's Weak upgrade can keep the
primary alive after Close has dropped its own Arc. A test-only pause after
that upgrade makes the early Close return deterministic. Callback-triggered
upgrades must remain free of the lifetime gate because Close drains callbacks
while holding its exclusive side; public calls must hold the shared side.

The rtree package still permits arbitrary narrowed file payloads through
RangeFile, including a four-field TestFile projection in its original-case
tests. Its missing-range API returns the local algebra KeyRange, while Go
returns generated kvrpcpb.KeyRange and keeps a distinct local KeyRange for
containment/intersection/logging. The latter is not a duplicate to delete.

O12's source keeper performs no retry: it logs one PD store failure and moves
on. Go also notifies before global-variable persistence and ignores this RPC's
response-body Error. The Rust repair preserves those less-obvious contracts.
The new SQL regression initially produced no notifications; it now passes.
Three broader sysvar tests fail identically on unchanged 021de80 (60 pass / 3
fail) and this repair (62 pass / 3 fail), including serial execution.

The subsystem probe stores generated TINYINT 127 from input 1000 under strict
mode while ordinary TINYINT correctly rejects the same input; the immediate
warning list is empty. The generated-column owner uses default conversion
flags. Complete generated protocols also do not eliminate narrowed consumers:
BR range helpers still own an incomplete File type. Conversely, Go itself drops
the new singleton fields from baseCollector, so the analogous Rust collector
projection is not counted as a demonstrated behavioral mismatch.


The expanded owner review reproduces an unqualified joined UPDATE succeeding
for a SELECT-only user while the qualified control is denied. PASSWORD HISTORY
is accepted without enforcement; IMPORT INTO ignores skip_rows; raw Latin-1
E9 becomes C3A9 on the wire. The durable account image omits connection and
password policy, independently of the TLS transport gap. Keyword-only review
would have incorrectly reported absent column GRANT, privilege reload,
auto-analyze workers and resource-manager startup: these do have live callers.

The session follow-up reproduces lost writes across aliases of one row and
multi-update FK bypass, although ordinary single-table FK and multi-DELETE
checks work. A failed in-process multi-action ALTER leaks its first column
addition. Cache size 1 leaves two prepared plans available because ownership
is per statement. Dynamic information-schema readers still use captured
oracle rows/errors; a working sequence is invisible in SEQUENCES. These are
current SQL observations, not conclusions from old gap comments. The USING
join control passes and is not promoted to an identity-corruption claim.

The expanded audit found a PD oneof projected as a plain scalar: explicit
keyspace zero encodes as empty locally versus 08 00 upstream. Matching fields
by wire tag avoids falsely counting the 70 renamed opaque command fields as
missing. The system-table event regression currently fails on an absent p1
partition; source already filters system schemas. Its name alone would have
led to the wrong fix.

The action-state follow-up found that four successful action paths used a
historical Job.Error as a current cancellation decision. The multi-action
fixture reproduces CANCELLED history for an otherwise successful create after
a prior error. Go counts fresh run errors and lets action state control finalization.

The old testport manifest contains only 45 package mappings and does not describe the current 856-directory Go tree. An initial Rust source search found 2,041 lines matching go-parity-gap, not implemented, not supported yet, or unimplemented!; this is a candidate count, not a mismatch count. Go supports some of those errors itself. The last embedded run has ten failures independently reproduced on unchanged integration HEAD; their names and logs remain in remove-extra-storage-policies-execplan.md.

## Decision Log


Decision (2026-10-01): centralize complete PD opt policy, replacing the scan-only
struct with a type alias to GetRegionOp and migrating every existing caller.
Represent shared values and notification channels safely in Rust; retain source
field/default/CAS/notification behavior. Do not claim service behavior merely
because its setting now exists. Full root/discovery/TSO owners remain open.

Decision (2026-10-01): preserve incoming 0b2cf64069 and retain its two diagnosed
SQL tie-order failures as an explicit inherited limitation. Go does not add a
source ordinal to final TopN comparison, and restoring one solely for the tests
would contradict the requested design. Keep all assertions and assign complete
source-oracle reconciliation to W03's configured/shared SQL owner replacement.
The independent PD retry package retains its complete passing acceptance gates.

Decision (2026-10-01): complete the pinned PD retry dependency and integrate its
source default initialization policy with one absolute member-probe deadline.
Reuse existing TraceContext, native errors and monotonic timers. Do not fold
client-go's distinct KV retry budget into PD or claim the legacy per-RPC/root
reconnect loop accepted before the entire discovery/root migration.

Decision (2026-10-01): correct incoming TopN candidate loss before publishing
the batch integration merge, and reconnect Go's post-spill worker transition.
Source `loadChunksUntilTotalLimit` explicitly avoids limit-derived RequiredRows;
`executeTopNWhenSpillTriggered` starts workers after spilling the initial heap.
The alternative of preserving the serial fallback retains two lifecycles and
fails the existing worker test. Preserve original tests and incoming history;
record the focused repair separately from complete Go package acceptance.

Decision (2026-10-01): migrate the complete PD batch dependency plus required
default TSO queue/token/completion call sites. A size-constant edit would retain
the missing shared collection/finisher/timer lifecycle and allocation churn.
Use a native permit and cancellation-safe fetch guard; return the token before
callbacks and recycle the controller after completion. Keep full dynamic
concurrency, options, router and parent TSO acceptance open.

Decision (2026-10-01): migrate the complete pinned `pkg/connectionctx` dependency
and its native single-leader callers. A same-URL conditional alone would leave
ownership rejection, cancellation and retained-context behavior without their
shared source owner. Preserve Go callback/lock semantics with Rust Arc/RwLock,
and keep PD-root/discovery/dispatcher acceptance open until their full scopes
qualify. No proxy/service mode or new public close policy is introduced.

- Decision: Implement the complete PD deadline dependency first, with the TSO
  batch start/completion and worker-retirement call sites required to use it.
  Rationale: This is the whole two-artifact source package, not a partial TSO
  dispatcher claim. Native timer/cancellation adapters preserve its lifecycle;
  the proven shared cancellation race must be repaired for shutdown to work.
  Public PD close, discovery, root APIs and retry/concurrency remain in W01.
  Date/Author: 2026-10-01 / Codex, deadline prerequisite implementation.

- Decision: Start implementation with the complete pinned PD root/TSO/discovery
  closure, then native routing and TiDB consumers; allow ready independent
  correctness packages without waiting for unrelated performance work.
  Rationale: Discovery, deadlines, synchronization and shutdown share state;
  migrating consumers before deleting competing owners prevents new bypasses.
  Date/Author: 2026-10-01 / Codex, full-picture execution plan.

- Decision: Preserve atomic Go-package acceptance across the 12 workstreams,
  and gate GC on min-start-TS protection and durable delete-range integration.
  Rationale: Finding-level closure cannot certify a broad source package, and
  safe deletion depends on active readers as well as worker implementation.
  Date/Author: 2026-10-01 / Codex, full-picture execution plan.

- Decision: Replace stale active milestones for protocol generation and the
  cleanup-lifetime approval block with their current repaired status; retain
  historical receipts as historical evidence.
  Rationale: Current planning must not redispatch repaired work or request
  authorization that has already been resolved.
  Date/Author: 2026-10-01 / Codex, full-picture execution plan.

- Decision: Order work by Go's system ownership, correctness risk and complete
  package prerequisites; retain the cache design as one bounded milestone.
  Rationale: Alternate SQL/DDL paths, narrowed plan handoffs and split routing
  can bypass correctly implemented helpers. Go itself separates session
  transaction policy, table mutation policy and native KV execution. Preserve
  those layers while retiring competing implementations after caller migration.
  Date/Author: 2026-10-01 / Codex, system-wide follow-Go review.

- Decision: Follow the complete shared Ristretto dependency and all four Go
  consumers, with separate cache instances and consumer-specific lifetimes.
  Rationale: Wrapper patches cannot repair eager publication, write-queue,
  callback and metrics differences; engine substitution alone cannot repair
  binding reload or coprocessor configuration. Keep necessary side indexes and
  unrelated Go LRU owners. Native Rust representation must preserve ownership
  and observable behavior without copying Go runtime internals mechanically.
  Date/Author: 2026-10-01 / Codex, full-picture follow-Go review.

For the LFU review, retain the existing shared owner and cover both missing
public accesses with its guard. Do not add another close flag, counter or
wait loop, and never take that guard in a callback. For C04, isolate admission
by pausing the processor in a rejection callback and submitting another key:
Go's primary must miss before admission and hit after Wait. A failed probe is
dependency evidence, not justification for another TiDB-wrapper cache.

(2026-10-01) Implement O12 as the complete three-artifact globalconfigsync leaf
plus required production callers. Use generated PD messages and the existing
PD worker; retain the notification handle on Session, not the scratch/cache
registry. Remove the two obsolete ignored test placeholders and map the Go
cases to executable owner/keeper tests. Do not broaden this into TopSQL O11
or silently change Go's no-retry/pre-persistence contract.

(2026-09-30, subsystem audit) Keep the latest request as a structural review.
Use one stable register and an exhaustive scope queue, not a claim of exhaustive
semantic coverage. Compare handwritten protocol consumers with the actual Go
consumer before demanding deletion. Record source-only consequences separately
from executable observations, and retain all package acceptance obligations.


Keep this continuation an audit. Record all established ownership mismatches
in one stable-ID register and retain executable diagnostic outputs without
asserting that wrong behavior is correct. Review production callers and Go
master before promoting a candidate. Preserve repaired statuses and separate
unreviewed package coverage from known defects. Do not apply isolated fixes to
privilege qualifiers, import options or HEX; each needs its complete Go owner.

The C01 continuation repairs the shared physical-entry owner: remove
PreparedSelectPlan/PreparedDmlPlan's private vectors and move every prepared
and non-prepared SELECT/DML lookup/insertion to one Session-owned cache.
Parameter signatures, schemas/statistics, database, bindings and environment
remain in the identity; statement definitions do not own cached physical plans.
Consolidate non-prepared definition metadata into Go's one separate statement
LRU. Use the existing plan-cache container and O(1) LRU primitive, maintaining
per-SQL parameter buckets and one global recency/capacity budget. Cover capacity,
parameter variants, mixed callers, captured capacity, invalidation and flush with failing
regressions before edits. C02's instance physical-plan cloning is separate;
this repair must not claim complete planner/session package acceptance.


The protocol-owner continuation removes all five remaining local projection
files, rather than copying missing fields into them. PD and BR re-export the
complete native generated packages. TiKV's RPC/envelope view derives from the
complete native input descriptor; only batch body representation changes to
Bytes, preserving tags/presence and buffer sharing. Complete etcd API inputs
are pinned and checked by the existing synchronization workflow. Generated
default server stubs replace mock-maintained unsupported-method lists, as Go
embeds Unimplemented servers. The three new wire/identity regressions failed
on integration 1203ee4487 before implementation. Source membership, descriptor
coverage, real client request construction, existing wire tests, affected RPC
fixtures, all-target checks, lint and both publication builds are required.
Native generated files remain unchanged; full Go helper/runtime package
acceptance and non-protocol structural findings remain separate.


The next production milestone repairs the existing executor UPDATE owner at
Go master e953a09d9d5e29e60c62f42d3aacebb819af49a5. Ordinary UPDATE, joined
UPDATE, and ON DUPLICATE KEY UPDATE must share row validation, unchanged-row
locking, IGNORE decisions and foreign-key completion. Keep update-once state
per target position and a separate row merge per physical table/handle, as
UpdateExec does. Remove the multi-update raw write bypass and executor undo
logs after confirming session/cluster statement staging owns rollback.
Ordinary FK checks run after the statement writes; IGNORE performs its checks
before each row, and cascades follow statement checks. Tests in the existing
session DML suite must fail on alias lost updates/FK bypass before migration,
then pass alongside statement rollback, FK and DML tests. This maintains
already integrated paths; it does not accept the complete upstream executor
package or close unrelated audit findings.


Keep this follow-up an audit with executable diagnostic evidence. Preserve
the complete probe source/output under current-audit and run it as a temporary
example using the existing locked session crate, then remove that temporary
example. Do not assert current incorrect outputs as passing regression tests
or claim whole-package acceptance. Production root fixes must migrate the
shared DML/cache/runtime-provider owners and add fail-before/pass-after tests.

For the expanded audit, preserve production behavior and consolidate current
source evidence, prior open findings and reproduced failures in a stable-ID
register. Enumerate omitted protocol declarations separately from intentional
opaque transport representations. Record all package artifacts without
automatically accepting them; an audit of selected owners cannot certify all
856 upstream package directories.

For the action-state follow-up, move queue writes to the shared worker instead
of routing each existing writer through another thin wrapper. Borrowing the
worker-owned job lets cancellation discard action mutations, while an explicit
updateRawArgs flag preserves each action's decoding contract. The public plan
and live dispatch set stay unchanged.

For the 2026-09-30 materialized-view cleanup, use explicit seed entrypoints
around the shared planner instead of adding those actions to live dispatch.
This removes duplicate completion ownership without integrating unaccepted
build/reorg implementations. Cancellation shares the finalizer immediately,
matching Go's transaction reset plus handleJobDone; it does not gain an extra
schema publication or require another action tick.

Inventory coverage explicitly and implement package-sized owner corrections. Do not promise a complete semantic audit from partial receipts, suppress failing tests, or replace Go policies with broad defaults. Complete generated schemas are the owner of protocol declarations; Rust execution support remains a separately audited consumer.

## Outcomes & Retrospective


Native 2fd0ece supplied the initial retry prerequisite and fixed initialization
and membership timeout regressions. The later value-ownership re-review qualifies
its unrestricted acceptance and replaces unsigned timing/callback restrictions
(see parity/current-audit/pd-retry-value-ownership-repair.md). Original and native validation pass. TiDB synchronization, adapter tests,
affected all-target compilation and root lint pass; publication follows the
actual hook and separate fresh locked server build gates. The receipt covers
every artifact and language adapter. W01 parent ownership and all 77 known open
findings remain explicit; no benchmark result is inferred from retry tests.

The complete batch leaf replaces native TSO's private drain loop and default
outstanding-RPC policy. Source and native package tests, fail-before behavior
and buffer lifetime are recorded in the batch receipt. P06 remains partial;
public PD close, discovery, metadata serialization and complete parent policy
remain open, with the same 77 known unresolved findings.

The integration merge also repairs TopN candidate loss and disconnected spill
workers, with 53 passing focused cases. Session comparisons reproduce two
existing planner-estimate expectation failures and six passing cases on both
baseline and repaired code. The batch receipt records exact commands and red
evidence; this maintenance does not certify full sortexec transcreation.

The complete connectionctx leaf removes unconditional healthy-stream replacement
and provides shared cancellation/retention ownership in native client-rust.
The receipt records original tests, source inputs, fail-before evidence and
publication gates. P06 remains partial and the 77 unresolved finding count is
unchanged; public close, discovery, metadata concurrency and parent TSO policy
remain W01 work.

The W01 prerequisite now implements and integrates the complete pinned PD
`pkg/deadline` package. Native master `5928b6e` supplies the shared owner to TiDB;
the [repair receipt](parity/current-audit/pd-deadline-owner-repair.md) records all
source/support artifacts, fail-before regressions and passing validation. It
also repairs completed-result loss during stream retirement and the shared
cancellation adapter's registration race. Public PD close and full root/TSO/
discovery acceptance remain open, so P06 is partial and 77 known findings remain.
About 5.3 GiB of obsolete incremental compiler caches were reclaimed. No workload
performance result is claimed. Publication uses both required locked build gates.

The full-picture planning revision maps every known open finding, identifies
coupled activation/removal gates, selects the next complete native owner and
specifies original-case, distributed, performance and publication evidence.
It changes three documentation files only: this ExecPlan, the current-audit
README and the new repair sequence. No production fix, dependency update,
package acceptance or benchmark result is claimed. Planning validation passed:
`python3 /private/tmp/check-full-parity-plan.py` verified all 77 unique
assignments, the eight excluded repaired IDs, source revisions, six module pins,
five inventory sizes, initial PD artifact counts and 31 documentation links.
`python3 rust/scripts/compare-sysbench.py --help`,
`python3 rust/scripts/compare-tpcc.py --help` and `git diff --check` passed.
Publication still requires the actual hook build and a fresh locked pre-push
build; final tool results determine whether those gates pass. Runtime suites,
`make lint` and benchmarks are not rerun for this documentation-only revision.
The chronological outcomes below retain their own scope.

The system-wide follow-up changes repair prioritization and the evidence needed
for removal. No production code, dependency pin or finding status changes in
this review. Existing reproductions remain evidence at their receipt revisions;
this source inspection does not claim new runtime validation.

The shared-cache review establishes a complete direct-importer map and a root
repair sequence, including the previously omitted inference consumer. It changes
no production code and closes no open finding. The dependency core, consumer
migrations, original-case validation and SQL benchmark measurements remain work.

The LFU review repaired the missed public trigger/capacity lifetime without
adding another shutdown owner. A paused-worker comparison now proves the
dependency's eager nonresident primary publication separately from the
pressure workload. The latter still fails after using Go's Get path rather
than fallback-only Values. C04 remains unresolved. The full 91-artifact
external module inventory preserves six root production files, six test files,
73 original tests, five benchmarks and all separate support/build variants.
No partial replacement cache is integrated. See
`parity/current-audit/lfu-review-followup.md` for exact checks, limits and the
reproducible Go admission probe. This advances the review and repairs the
native lifetime gap; it does not complete the broad parity goal.


The shared UPDATE follow-up removes the raw joined write and statement-specific
undo owners. The eight new in-process regressions and embedded cluster buffer
checks pass; ordinary FK, generated-column and rollback tests retain their
behavior. The four DML/default and 31 grant-suite failures reproduce on unchanged HEAD and
remain explicit. See parity/current-audit/shared-update-owner-repair.md for
exact commands and limitations. E01 is repaired; E02 runtime behavior is
repaired while joined FK plan metadata still needs integration. Full package
acceptance and the remaining structural findings are not complete.


The session/executor follow-up extends the register from 29 to 41 findings.
Five added ownership groups have in-process SQL reproductions; seven are
source-confirmed design/integration gaps. No production behavior changed.
Current-master package acceptance and workload parity remain open.

The expanded review records 29 known structural findings across DDL,
transactions, domain services, TiFlash/MPP, protocols and PD discovery. The
remaining-projection audit records 400 omissions, one PD presence mismatch
and 71 opaque representations, with a reproducible compiler-produced wire
example. No production fix is claimed in this checkpoint. The full semantic
review and benchmark goals remain unfinished.

The action-state maintenance follow-up removes 17 action-owned queue writes
and the stale-error cancellation predicates. All targeted planner and embedded
lifecycle checks pass. Ordinary retry/error-limit handling and remaining action
validation still require separate source-backed work; passing these tests does
not establish package acceptance.

The materialized-view maintenance follow-up removes five remaining seed
history writers and preserves cancellation/build errors. The shared live
worker tests still pass. This is existing seed maintenance, not new package
acceptance; the remaining materialized-view action/reorg gaps stay explicit.

The full review remains in progress. The first completed repair removes the partial TiPB schema owner. It does not certify every generated-runtime behavior or SQL consumer as package-complete, and does not establish repository-wide parity.


## Complete TiPB ownership repair receipt (2026-09-30)


The source comparison at integration 5503f8860883c6cd80bdd0d487d34c53787daf24
found 157 declaration gaps against master's TiPB pin: 76 messages, 51 fields,
29 enums and one enum value. Every gap is listed in
parity/current-audit/README.md and tipb-mismatches-before.json. The new wire
regressions failed before the production change: Executor decoding retained
type 21 but discarded its field-26 ExplainForConnection body, and ExecType
rejected 21. The expression regression covers rpn_args_len field 6 as well.
The failing log is /private/tmp/tidb-full-proto-red.log.

The complete external package is github.com/pingcap/tipb/go-tipb at
v0.0.0-20260908093239-fed7bc47c39d. Its 13 generated production files,
spfresh_test.go (all nine original tests), proto sources, compiler includes,
module metadata, license, CI and build/generation artifacts are retained as
66 path/hash records in crates/tidb-proto/tipb-source.json. No platform-specific
Go implementation is present in this generated package. All 13 upstream proto
entrypoints and their imported options are compiled by prost/tonic from 28
exact checked-in source/license inputs. The sharedbytes Go representation maps
to the existing Rust Bytes field for Chunk.rows_data. Recursive executor
children use Rust Box pointers. Original Go generator/runtime support is
replaced by the existing Rust generators, without hand-editing generated code.
The tici protobuf package remains a nested Rust module. Unused protocol types
become available; their SQL execution remains separately owned and unreviewed.

Removed: four handwritten TiPB projections and two partial synchronization
scripts. Replaced them with sync-tipb.py, which requires an explicit master
revision for updates, verifies the recorded revision when available, compares
all upstream bytes/file membership and module artifacts, and rejects a changed
pin in a fetched master. Builds remain offline from checked-in inputs. The
root lint gate also runs four checker regressions. Consumer changes in
 tidb-exec, tidb-expr and tidb-unistore only supply defaults for newly represented
fields and Box pointers for recursive types; their existing encoded fields and
wire-vector tests are retained. No client-rust source or pin changed; the
current vendor already matches native master 884589f.

Exact validation commands, from rust/ unless a different directory is stated:

    cargo test --locked -p tidb-proto --test tipb_selection_expression_source complete_contract
    cargo test --locked -p tidb-proto
    cargo check --locked -p tidb-server -p tidb-proto --all-targets
    cargo check --locked -p tidb-exec -p tidb-expr -p tidb-distsql -p tidb-executor -p tidb-unistore -p tidb-planner -p tidb-txnkv -p tidb-util -p tidb-protocol -p tidb-pd-client --all-targets
    cargo test --locked -p tidb-exec --test all dag
    cargo test --locked -p tidb-exec --test all cop_scan
    cargo test --locked -p tidb-exec --test all wide_scan_selection_source
    cargo test --locked -p tidb-expr --test all pb_
    cargo test --locked -p tidb-unistore --lib cophandler::
    cargo test --locked -p tidb-distsql --test all select_result
    cargo test --locked -p tidb-server --lib pushed_conditional_signatures_evaluate_in_the_coprocessor_like_go
    cargo test --locked -p tidb-server --lib a_derived_aggregate_over_the_coprocessor_answers_its_output

The first command is the red reproduction. The complete Rust protocol suite
passes 45 tests, including both regressions and all nine translated original
SPFresh tests. The consumer/SQL commands pass 14, 11, 16, 10, 86, 20, 1 and 1
tests respectively (159 total), with two pre-existing DistSQL ignored tests.
All direct consumer targets compile. The original nine Go tests pass via
 go test ./go-tipb from the pinned module-cache directory. From repository
root, python3 rust/scripts/sync-tipb.py, python3 -m unittest discover -s
rust/scripts/tests -p test_tipb_sync.py, make lint and git diff --check pass.
Logs use /private/tmp/tidb-full-proto-*.log.

Scope-specific tests were chosen to preserve request lowering, zero-copy
response ownership, recursive executor representation and end-to-end SQL
pushdown while checking the whole generated package's original tests. No
Bazel preparation or Go failpoint toggling applies: no Go source/module/Bazel
input changed. RealTiKV, TiFlash cluster tests, full-workspace tests and
sysbench/TPC-C/TPC-H/YCSB were not run; no performance gain is claimed. The ten
previously reproduced embedded-suite baseline failures remain listed for
separate root-cause repair. Publication requires the locked server build in
the pre-commit hook and a separate fresh locked server build before push.

Inventory regeneration now includes all 41 package directories in master's
pinned client-go module, in addition to the 856 TiDB package directories. The
2,051 marker hits remain candidates only. The inventory generator records
unreviewed status by design; acceptance receipts are separate. Other external
modules still need complete inventories before any acceptance claim.

## Coprocessor and MPP protocol owner follow-up


TiPB repair 25870bf5d0 was committed and pushed after the hook and fresh locked
server build passed. The subsequent table-policy review confirms that a safe
repair must carry both transaction mode and actual existence-read evidence to
the table/index owner. Configured DML currently omits transaction mode from its
planning interface, and system-row index mutations omit assertions. Do not
replace the generic constructor with a blanket Unknown rule.

Two complete generated packages can instead immediately share their existing
native owner: kvproto/pkg/coprocessor and kvproto/pkg/mpp at master's
v0.0.0-20260820070758-623e58e60fa9. Native schemas match that pin byte-for-byte.
TiDB's local copies omit batched/versioned request fields, merged task response
markers and MPP partition/shard metadata. Add failing wire regressions, then
remove both local schemas and re-export the complete native types, using
extern_path for imported references in the local TiKV service.

First make the native generator preserve all four upstream SharedBytes custom
fields (three coprocessor response bodies and TiFlashSystemTableResponse.data)
with prost Bytes, reproducing the copy before changing generation. This keeps
the existing TiDB response decoder's shared-buffer contract when its duplicate
schema is removed. Publish native changes before synchronizing the vendor,
regenerating artifacts rather than editing generated code. Validate original
Go package artifacts/tests, native protocol/client tests and Clippy, all TiDB
consumer targets, source wire vectors, coprocessor/RPC/SQL tests, lint, hook
build and fresh pre-push build. The transaction-lifetime patches remain
unapplied. No SQL feature is added by exposing complete protocol fields.

The complete descriptor comparison also found two changed field contracts:
MPP TaskMeta.keyspace_id belongs to a oneof, and api_version is the APIVersion
enum. The before-image contains all 29 gaps (10 messages, 17 fields, two changed
contracts). Both package aliases now share the native type identities, tested
along with explicit zero keyspace presence and last-oneof-arm decoding.

Using the generated API enum exposed a caller defect: api_version=1 meant
V1TTL, not transactional V1. The extracted dispatch-context regression failed
with V1TTL before the fix. Dispatch now obtains the version and null keyspace
from native Keyspace::Disable and applies them through the native Request
setters, like client-go internal/apicodec.setAPICtx. The duplicate numeric
metadata values were removed. The receiver/sender task metadata remains the
unencoded task identity; dispatch encodes its clone as Go does. The larger MPP
statement-identity gap (fixed IDs and Unix seconds instead of StmtCtx atomic
query/task ownership and UnixNano) is recorded separately and remains open.

Native client-rust b2b3783cee3982ad39c0a70df2654176aafc784d is published to
master. Full native library tests pass 1,401 with two ignored; native protocol
and generator suites pass three each, all workspace targets/features compile,
and strict library Clippy passes. The maintained vendor script regenerated the
TiDB-compatible output from that commit. Only the context of transport patch
020 changed; its version/API adaptation is unchanged. No transaction-lifetime
patch was applied.

The shared-contract source gate now verifies all upstream kvproto schemas and
includes, exact file membership (excluding the separately owned gRPC Channelz
input), six shared package outputs and build inputs, and all 139 Go module
artifacts. Its receipt explicitly pins Go master. The overall review inventory
also includes every artifact in kvproto's 41 package directories. Source checks
run in make lint. No hand edits to generated files were made.


Coprocessor/MPP validation from rust/:

    cargo test --locked -p tidb-proto --test coprocessor_mpp_wire_source
    cargo test --locked -p tidb-proto
    cargo test --locked -p tikv-client-kvproto
    cargo test --locked -p tidb-exec --lib classic_mpp_dispatch_uses_the_transactional_v1_codec
    cargo test --locked -p tidb-txnkv --test resource_group_tag_source
    cargo test --locked -p tidb-txnkv --test all tikv_client_coprocessor_transport_source
    cargo test --locked -p tidb-unistore --lib cophandler::
    cargo test --locked -p tidb-distsql --test all select_result
    cargo test --locked -p tidb-server --lib pushed_conditional_signatures_evaluate_in_the_coprocessor_like_go
    cargo check --locked -p tidb-server -p tidb-proto -p tidb-exec -p tidb-expr -p tidb-distsql -p tidb-executor -p tidb-unistore -p tidb-planner -p tidb-txnkv -p tidb-util -p tidb-protocol -p tidb-pd-client --all-targets

The first command reproduced three discarded-payload failures before removing
the local schemas. The complete protocol suite now passes 49 tests. The native
protocol crate under TiDB's transport versions passes three; the MPP API
regression passes one after failing with V1TTL; resource tagging, native
transport, mock coprocessor, DistSQL and embedded SQL pass 3, 2, 86, 20 and 1
respectively. Two existing DistSQL cases remain ignored. All direct consumer
targets compile. From repository root, make lint, make rust_proto_check and
git diff --check pass. Original Go checks pass from the pinned module using
 go test ./pkg/coprocessor ./pkg/mpp ./pkg/sharedbytes (the two generated packages
have no original tests; sharedbytes has one). Logs are
/private/tmp/tidb-copro-mpp-*.log and /private/tmp/tidb-mpp-api-{red,green}.log.

Changed files: tidb-proto's build/module exports and source tests; the two
removed schemas; tiflash_mpp_scan.rs's dispatch context; two txnkv test
fixtures; native source/generated/test updates from b2b3783; source-check and
inventory scripts; Makefile's protocol gate; the transport patch context;
and audit/receipt documentation. No Go/Bazel/module input changed, so no
Bazel preparation or failpoint toggling applies. This validates generated
contract ownership and the codec caller, not complete MPP execution parity.
RealTiKV/TiFlash clusters, full-workspace runtime tests, sysbench/TPC-C/TPC-H/
YCSB were not run. Bytes changes the public Rust response-body type; all local
consumers compile. No benchmark speedup is claimed.

Disk cleanup confirmed no cargo/rustc/rustdoc process was active and removed
418 incremental-cache directories whose files were untouched for three days,
37.08 GiB of regenerable file contents. Free filesystem space afterward was
44 GiB; no source, worktree, fixtures or generated checked-in inputs were
removed. Publication still runs the required hook and fresh locked server
build even though caches were reclaimed.


## MPP identity owner repair receipt (2026-09-30)

Refreshed hparser-integration and origin/master; both unchanged. The supported
single-fragment MPP path synthesizes fixed IDs, process ID and Unix seconds.
Master executor/mpp_gather.go and stmtctx.MPPQueryInfo require shared statement
query ID/nanoseconds, with monotonically allocated task/gather IDs; adapter.go
clears state only at statement completion, not ResetForRetry. Add that owner to
the existing statement context and preserve it across separately built contexts,
clones and scan requests. Allocate one gather per supported scan coordinator.
Use the existing server-info getter, not process ID. The separate domain server-ID
lease allocator is absent from Rust and remains an explicit audit gap; this
repair does not claim to implement it or complete all MPP packages.

First extract the current metadata construction without changing its behavior
and demonstrate failing metadata regressions. Then implement the owner, session
lifecycle and wire consumers, translate the previously ignored upstream task-ID
test at the owning Rust context boundary, and test concurrent allocation and
statement completion. Validate affected crates, lint and required publication
build gates. No Go/Bazel changes or failpoints are required.


MPP implementation and validation update: the production metadata helper was
first extracted unchanged. Three runtime assertions then failed: two queries
both used ID 1, two gathers both used ID 1, and query timestamps were seconds
instead of nanoseconds (/private/tmp/tidb-mpp-identity-red.log). They now pass.
MppQueryInfo is the statement-owned atomic state, shared through StmtContext and
PushdownStatementContext. Its process query-ID allocator and per-statement
counters match master. The existing single-fragment coordinator allocates one
gather and task for each open; its receiver stays task -1. Both dispatch and
connection metadata derive from this state. The original TestAllocMPPID now
executes at the Rust statement owner instead of remaining an ignored planner
placeholder, avoiding a planner/executor dependency cycle.

Tracing actual retries found that the cluster runner closes attempts before it
decides whether to retry. Its outer scope now defers retirement through that
decision. A returned open stream retains its state until record-set Close;
completed results and final failures release it at the outer boundary. Ordinary
session completion resets uniquely owned storage in place; retained readers are
detached from the next statement by replacing the Arc. Query IDs and clocks are
allocated lazily on the MPP path. The session captures the existing server-info
getter once when bound, avoiding a cloned ServerInfo/lock on every statement.
No cluster server-ID allocation is invented: that absent domain owner and the
two detached TiFlash replica pollers discovered during tracing are explicit open
audit entries.

Focused tests currently pass: five MPP metadata/codec tests, two statement-owner
tests (including 32 concurrent gathers), and all 18 session lifecycle tests.
The latter cover rebuilt/retried contexts, successful/failed completion, streaming
finish versus close, and a live server identity getter. All targets in the five
affected consumer crates compile. Remaining publication checks and exact command
receipt will be recorded below. This is maintenance of the existing MPP path,
not acceptance of entire Go executor/stmtctx/planner/session packages; their
complete source/test/platform/build/fixture inventories remain unreviewed in
package-coverage.json. No Go or Bazel files changed.


Final MPP validation commands, from rust/ unless otherwise noted:

    cargo test --locked -p tidb-exec --lib dispatch_context_tests
    cargo test --locked -p tidb-executor --lib mpp_query
    cargo test --locked -p tidb-session --lib tests_core::lifecycle
    cargo test --locked -p tidb-executor --lib stmt_context::
    cargo test --locked -p tidb-executor --lib remote_scan::
    cargo test --locked -p tidb-server --lib cluster_session_node::tests::autocommit_transactions
    cargo check --locked -p tidb-server -p tidb-session -p tidb-executor -p tidb-exec -p tidb-planner --all-targets
    make lint  # repository root
    git diff --check  # repository root

All passed: 5 + 2 + 18 + 19 + 31 + 23 = 98 targeted tests. The server fixture
initially failed in the sandbox because macOS sysctl hw.memsize was denied;
the same command passed with host access. Lint first lacked network access for
its pinned revive dependency and passed on the permitted rerun. Existing
compiler warnings remain. Logs: /private/tmp/tidb-mpp-{identity-green,
owner-green,lifecycle,stmt-context,remote-scan,server-retry,check,lint}.log.

Changed implementation files: tidb-executor/src/{mpp_query.rs,lib.rs,
stmt_context.rs,remote_scan.rs}, tidb-exec/src/tiflash_mpp_scan.rs,
tidb-session/src/{lib.rs,stmt_ctx.rs}, and
tidb-server/src/cluster_session_node/mod.rs. Test changes are in the metadata
helper's unit tests, mpp_query.rs, tidb-session/src/tests_core/lifecycle.rs and
tidb-planner/tests/casetest_physicalplantest_hint_plans_source.rs (moving the
ignored upstream task-ID case to its executable owner). This plan and the
current-audit README retain the receipt and unresolved findings.

Compatibility: fixed protocol identity fields now match Go's units and lifetime;
statement/task IDs are concurrent atomics and retry state is retained until the
outer completion decision. No SQL syntax, transaction algorithm or dependency
revision changed. The domain numeric ID lease allocator remains absent; the
existing server-info getter's zero/default is no substitute for that owner.
Real TiFlash/cluster interoperability, complete upstream Go package test suites,
full-workspace runtime tests and sysbench/TPC-C/TPC-H/YCSB benchmarks were not
run. No speedup or complete MPP/package/repository parity is claimed. The 10
previous baseline embedded failures were not rerun or reclassified here.


## TiFlash poller ownership removal receipt (2026-09-30)

User asks to keep removing duplication. Pull/fetch left integration and master
unchanged. Go ddl.Start launches one PollTiFlashRoutine, cancelled by ddl.ctx and
joined by its wait group. Only ownerManager.IsOwner invokes refreshTiFlashTicker;
status changes go through executor.UpdateTableReplicaInfo. The current Rust boot
starts two workers and forgets both handles. Each worker owns another transaction
opener, writes metadata directly without the DDL executor's notifier/reload, and
runs on non-owner nodes. Its raw TCP HTTP implementation has no timeout and
cannot decode chunked responses; its parser treats the count header as a region.

Remove the generic transaction opener from TiFlashReplicaManager. Bind the
existing RealClusterDdl through a narrow ownership/status-update capability and
retain one poller handle in the node shutdown scope before its DDL/PD owners.
Use the existing reqwest dependency for correctly framed, bounded HTTP; stop
wakes the interval and joins the current pass before releasing capabilities.
Add deterministic owner-handoff, status-response and lifetime tests, reproducing
owner/parser failures before their fixes. Keep publication on the shared DDL
path. Go package inventories remain unreviewed; this is maintenance of the
existing classic-cluster poller, not a complete DDL/infosync/helper port.

The classic placement-rule workflow needs a separate owner migration: Go configures
individual rules in the DDL job, while Rust currently repairs them in this poller.
Do not silently delete this path before migrating its DDL consumers. Its bundle
replacement and repeated status-publication behavior must be tested here because
the current implementation can replace sibling rules and continually publish
already-available tables. No new Go-absent polling policy is authorized.


Implementation update: removed the generic transaction opener/private commit,
duplicate boot call and forgotten handles, raw TCP HTTP code, and the invented
TiFlash bundle constructor/export. The node retains a single worker guard and
joins it before releasing its DDL/PD capabilities; an idle worker wakes on stop,
while an active pass finishes with Go's per-request deadlines (PD HTTP 30 seconds,
TiFlash InternalHTTPClient five minutes). DDL
ownership is checked anew each pass. The callback uses RealClusterDdl.execute,
including its existing notifier and immediate catalog reload.

The additional protocol review reproduced six failures on the pre-fix logic:
followers performed discovery; the parser counted the header; multiple table
updates used group replacement; availability publication was incorrect; duplicate
region reports/progress ignored replica counts; live-store HTTP errors were
ignored. Logs: /private/tmp/tidb-tiflash-poller-{red,protocol-red-host}.log.
Loopback tests needed host access because the sandbox rejects socket binding.
Ten focused tests now pass, including original Go helper count/region cases,
malformed/truncated reports, count mismatch tolerance, duplicate-region handling,
owner handoff, group configuration errors, chunked responses and joined shutdown.
The source parser/progress contract comes from helper.go and infosync; individual
rule/group and binary-range HTTP contracts were checked against master's pinned
PD client v0.0.0-20260805103528-afa43111d149.

Remaining TiFlash scope is explicitly split into audit findings: classic rule
creation/cleanup belongs in DDL/GC rather than the poller; physical partitions,
ResetAvailable semantics, available-table progress cache/backoff, PD HTTP store
state discovery, shared security and endpoint failover are not implemented by
this removal. SetTiFlashReplica still reconstructs unavailable metadata. Normal
non-CHECK DDL still bypasses persisted jobs in the existing shared executor;
reusing that executor does not certify the entire DDL scheduler. No complete Go
package acceptance or repository-wide parity is claimed.


Source recheck during self-review confirmed that these specific owner/parser
functions match origin/master despite unrelated differences in the branch's Go
files. It also caught an incorrect preliminary deadline assumption: helper
requests use util.InternalHTTPClient's five minutes, whereas the pinned PD HTTP
client uses 30 seconds. Requests now carry that owner-specific deadline explicitly;
shutdown joins an ongoing pass and does not promise immediate cancellation.


The embedded DDL regression replayed the former private commit path and failed:
metadata committed but the shared catalog stayed at schema version 1 rather than
2 (/private/tmp/tidb-tiflash-poller-ddl-red.log). Restoring the shared executor
passed, including immediate availability visibility, idempotent repeat update
and missing-table errors. A final source-edge regression rejected an added HTTP
status policy: Go parses a TiFlash response body even with non-200 status. The
policy was removed and that regression now passes too.

Validation commands from the repository root:

    cargo test --manifest-path rust/Cargo.toml --locked -p tidb-exec --lib tiflash_replica_manager::tests
    cargo test --manifest-path rust/Cargo.toml --locked -p tidb-server --lib cluster_session_node::ddl::schema_sync_tests
    cargo test --manifest-path rust/Cargo.toml --locked -p tidb-placement --lib bundle
    cargo check --manifest-path rust/Cargo.toml --locked -p tidb-server -p tidb-exec -p tidb-placement --all-targets
    make lint
    git diff --check

The 11 poller, 3 DDL and 12 existing placement tests passed. Lint passed with
network access for its pinned revive tool; loopback/embedded tests used host
access for sockets and macOS sysctl. Cargo.lock adds only the existing serde
dependency edge to tidb-exec for the typed PD rule-group response. No Go/Bazel
files or generated artifacts changed; no failpoint/Bazel preparation was needed.
Final publication must run the locked server build inside the pre-commit hook
and again immediately before push; results are reported with the published
commit. Logs are /private/tmp/tidb-tiflash-{poller-*,placement-tests}.log.

Changed files: tidb-exec/src/tiflash_replica_manager.rs and its manifest/lockfile;
tidb-placement/src/{bundle.rs,lib.rs}; tidb-server/src/cluster_session_node/
{boot.rs,ddl.rs,mod.rs}; this ExecPlan and the current-audit README.

Not verified locally: real PD/TiKV/TiFlash cluster interoperability, TLS/failover,
complete upstream package test suites, full workspace runtime tests, and
sysbench/TPC-C/TPC-H/YCSB. Existing compiler/linker/jemalloc configuration warnings
remain. Shutdown can wait for the current poll pass; deadlines bound individual
requests, not the entire pass. The still-open placement/partition/state-owner
gaps above remain material correctness and compatibility risks, not accepted
parity. Performance improvement from removing duplicate polling is not measured.


## Shared persisted DDL lifecycle repair (2026-09-30)

The next placement-owner migration is blocked on a real DDL/GC lifecycle: Rust
has no delete-range GC worker. Source comparison against freshly fetched Go
master e953a09d9d exposed seven copied live worker loops, six without schema
synchronization, and action planners deleting active jobs before the wait.
Repair the existing persisted CHECK/create schema/create table/create tables/
rename tables/drop schema/drop table execution paths as one maintenance unit.
Actions retain DONE/ROLLBACK_DONE/CANCELLED in the queue; a shared worker first
recovers pending MDL synchronization, then finishes history in a separate
transaction. Register MDL from the submitted table_ids, propagate history errors,
and remove the copied loops and server dispatch branches. Add failing queue/
history regressions first, then recovery and cancellation coverage. Validate
the source-backed planner tests, embedded worker tests, affected all-target
builds, make lint, the commit-hook locked server build, and a fresh pre-push
locked server build.

This is maintenance of existing live paths, not acceptance of pkg/ddl. Direct
non-CHECK SQL DDL, undispatched materialized-view seed planners, delete-range
GC, complete package inventory/gates, and TiFlash rule ownership remain open.

Implementation and self-review: removed the seven execution loops and per-action
history writers for these existing live paths. The shared planner reloads only
the requested queue row, retains completed jobs until schema acknowledgement,
and writes history/deletes the active row in a later transaction. MDL table IDs
are copied verbatim from the queue. Recovery waits without re-announcing old
versions; newly committed versions propagate notifier failure. Metadata-lock
cleanup follows Go's owner predicate (omitted for system schemas), and cleanup
errors remain best effort after acknowledgement. Only the same worker run may
reuse a successful wait; a replacement recovers the durable row. Ownership is
checked before action/history commits and validation-error state commits. DROP
steps preserve raw args they did not decode or change. Batch completion now uses
HistoryInfo.SetTableInfos for all published tables, including an empty batch,
instead of a single-table finish that could leave an empty batch RUNNING.

Three behavioral failures were reproduced before their fixes: premature CHECK
history publication, cleanup deleting another owner's MDL row, and batch
history losing its table list. Evidence is in
/private/tmp/tidb-ddl-lifecycle-{red,owner-red,batch-red}.log. Existing CHECK
fixtures now explicitly exercise the separate history transaction. The six
other integrated action paths are covered through all their phases, including
retained MDL barriers after restart and exact submitted table-ID scopes. An
embedded storage regression verifies pre-commit owner loss, notification failure,
failed/retried synchronization, preserved args and no duplicate schema version.

Files changed: tidb-exec/src/{cluster_ddl,ddl_job_table,real_tikv_ddl}.rs,
tidb-exec/tests/cluster_ddl_source.rs, tidb-server/src/cluster_session_node/ddl.rs,
this ExecPlan and parity/current-audit/README.md. No Go/Bazel inputs, generated
artifacts, dependencies or native client-rust files changed. bazel_prepare and
Go failpoint enablement are not applicable.

Validation (run from rust/ unless indicated):

    cargo test --locked -p tidb-exec --test all cluster_ddl_source
    cargo test --locked -p tidb-exec --lib real_tikv_ddl::tests
    cargo test --locked -p tidb-server --lib cluster_session_node::ddl::schema_sync_tests
    cargo check --locked -p tidb-exec -p tidb-server --all-targets

The 92 planner, 7 commit-classification and 4 embedded DDL tests pass. Embedded
tests need host access for their existing system-memory/storage setup. Affected
all-target checks pass with existing warnings. From the repository root:

    make lint
    git diff --check

Both pass. The commit uses the repository hook via core.hooksPath=hooks, which
must build the locked Rust server. A fresh `cd rust && cargo build --locked -p tidb-server` is also required
after commit and immediately before push.

Correctness/compatibility impact: persisted DDL completion is now delayed until
the required acknowledgement; errors keep the queue recoverable. No SQL feature
was added. A point lookup replaces repeated full active-queue decoding, but no
benchmark speedup is claimed. Real multi-node TiDB/TiKV/TiFlash interoperability,
full upstream package validation, and sysbench/TPC-C/TPC-H/YCSB were not run. The
previous ten embedded baseline failures remain unverified. Direct SQL DDL
admission, delete-range GC/DropTableArgs, undispatched materialized-view seed
lifecycles, pause/cancel/reorg scheduling, MDL-disabled operation and TiFlash
placement ownership remain open in the audit. No complete package or repository
parity is claimed.


## Materialized-view seed completion cleanup receipt (2026-09-30)

Integration was pulled at d54903d0b2 and Go master refreshed at e953a09d9d.
Inspection of pkg/ddl/mview_worker.go and job_worker.go shows that action
handlers finish metadata in DONE/ROLLBACK_DONE while the shared worker owns
schema acknowledgement and history. Five private Rust seed history writers
bypass that lifecycle; cancellation constructs mutations and then discards
them by returning an error. Build rollback records a warning instead of the
durable Job.Error and ErrorCount.

The explicit seed entrypoints now reuse plan_persisted_ddl_job_with, while the
live supported-action allowlist remains unchanged. One finish_persisted_ddl_job
owns SQL/KV history and active-row removal. Actions retain DONE/ROLLBACK_DONE
in the queue until the MDL (metadata-lock) publication barrier is acknowledged.
Only finalization stamps finished_ts/sequence_number and converts DONE to
SYNCED. Cancellation calls this same finalizer immediately with no action
mutations; its error/count and submitted raw args reach history. Build failures
persist Job.Error/ErrorCount, replacing the invented statement warning. CHECK
uses the same error-field owner. All three entrypoints and CHECK error recovery
now point-read one job instead of scanning and decoding the entire queue.

The before-fix planner regressions failed on premature log history and missing
build error; the separate cancellation regression failed because no committable
write set was returned. Logs: /private/tmp/tidb-mview-lifecycle-red.log and
/private/tmp/tidb-mview-cancel-red.log. The final suite covers each seed schema
phase's retained active row, full queued table-ID scope, replacement-owner MDL
recovery, separate history transaction, raw args and completion timestamps.
Six cancellation cases cover nil/missing metadata and decoding failures;
wrong-action dispatch is refused and SQL-history failures keep cancellation or
completed rows retryable. Successful/rolled-back seed actions remain excluded
from live dispatch. These changes maintain existing seed evidence; they do
not accept or dispatch a partial upstream package.

Files changed: rust/crates/tidb-exec/src/cluster_ddl.rs,
rust/crates/tidb-exec/tests/cluster_ddl_source.rs, this ExecPlan and
rust/docs/parity/current-audit/README.md. No Go/Bazel, generated code or native
client/dependency inputs changed. bazel_prepare and Go failpoints do not apply.

Exact validation commands from rust/:

    cargo test --locked -p tidb-exec --test all cluster_ddl_source::persisted_materialized_view -- --nocapture
    cargo test --locked -p tidb-exec --test all cluster_ddl_source::materialized_view_cancellation_persists_history_and_error
    cargo test --locked -p tidb-exec --test all cluster_ddl_source
    cargo test --locked -p tidb-exec --lib real_tikv_ddl::tests
    cargo test --locked -p tidb-server --lib cluster_session_node::ddl::schema_sync_tests
    cargo check --locked -p tidb-exec --lib
    cargo check --locked -p tidb-exec -p tidb-server --all-targets

The first two commands are the red reproductions. The final planner, commit
classification and embedded server runs pass 93 + 7 + 4 tests. The embedded
server tests use host access for the existing memory/storage setup. Both
compilation checks pass with existing warnings. Green/check logs are
/private/tmp/tidb-mview-{lifecycle-green,worker-tests,server-tests,all-targets}.log.
From the repository root, make lint and git diff --check pass. The hook must
run its locked Rust server build at commit, followed by a separate
`cd rust && cargo build --locked -p tidb-server` immediately before push.

Correctness impact: successful and rolled-back jobs remain recoverable until
schema acknowledgement; cancellation/build errors persist across owners.
Compatibility: the two seed APIs now return the shared Step/SchemaSync plan
and accept the worker-local previously_synced_version. Their only callers are
source tests; live SQL dispatch does not expand. Point lookups avoid redundant
queue decoding, but no benchmark speedup is claimed.

Not verified locally: full upstream pkg/ddl/package variants and original tests,
real multi-node TiDB/TiKV/TiFlash interoperability, the ten previously recorded
embedded baseline failures, or sysbench/TPC-C/TPC-H/YCSB. Open seed mismatches
include the build/reorg transaction boundary, rollback data GC and base/log
back-references, system-table failure transitions, and worker validation/error
identity details. Direct DDL admission, delete-range GC, general scheduling,
MDL-disabled operation and TiFlash placement ownership remain open. No complete
package or repository parity is claimed.


## Persisted action state ownership receipt (2026-09-30)

Pulled integration 6ed271503c and refreshed master e953a09d9d. Go
pkg/ddl/job_worker.go transitOneJobStep owns updateDDLJob after actions return;
countForError records fresh errors, and cancellation resets the action
transaction and immediately calls handleJobDone. Rust still has action-local
queue writes, cancellation persistence, and checks of the historical Job.Error
to decide whether a successful action should cancel.

Removed all 17 action-stage append_update call sites across the existing
integrated and explicit seed planners. Actions now borrow the worker-owned
active job and return PersistedDdlActionStep: metadata writes, updateRawArgs,
and an optional handled rollback error. They no longer receive a queue-table
handle. The shared lifecycle appends one durable envelope update after a
successful/handled step. On cancellation it discards action writes, records
only the fresh error, and calls the existing history finalizer immediately.
DROP keeps updateRawArgs=false, preserving undecoded arguments. The separate
CHECK validation-error transaction retains its explicit state write; it is not
an action planner or a second ordinary worker pipeline.

Removed the checks that inferred cancellation from any historical Job.Error.
Successful retries preserve their earlier error/count and finish DONE/SYNCED
as Go does. Decoding failures for create/schema/batch/rename/CHECK cancel via
the worker. Fresh create/drop/rename refusal codes travel with the refusal;
CREATE SCHEMA no longer uses the table-exists code, and missing-database
errors no longer become table-not-found solely because of a shared literal.
The view build's handled rollback error also moves from action-local recording
to this shared owner. No supported-action allowlist or public planner API
changes, SQL feature additions, native dependency changes or generated edits.

Before the production fix, the new source regression failed because cancelled
CREATE SCHEMA needed a second transaction; the existing multi-action lifecycle
fixture, seeded with an earlier retry error, finished with CANCELLED instead
of SYNCED. Red evidence: /private/tmp/tidb-ddl-action-owner-red.log. The final
source suite covers success after historical errors through all six catalog
actions (including empty batch), atomic refusal/no partial batch publication,
retryable history failures, argument-decode cancellation for seven actions,
and preserved raw arguments. The embedded worker test adds ownership loss
before cancellation commit, then successful retry with exactly one additional
error count and no schema notification/acknowledgement.

Files: rust/crates/tidb-exec/src/cluster_ddl.rs,
rust/crates/tidb-exec/tests/cluster_ddl_source.rs,
rust/crates/tidb-server/src/cluster_session_node/ddl.rs, this ExecPlan and
rust/docs/parity/current-audit/README.md. Rust-only scope does not trigger
bazel_prepare or Go failpoint setup.

Exact validation commands, from rust/:

    cargo test --locked -p tidb-exec --test all cluster_ddl_source::persisted_
    cargo check --locked -p tidb-exec --lib
    cargo test --locked -p tidb-exec --test all cluster_ddl_source
    cargo test --locked -p tidb-exec --lib real_tikv_ddl::tests
    cargo test --locked -p tidb-server --lib cluster_session_node::ddl::schema_sync_tests
    cargo check --locked -p tidb-exec -p tidb-server --all-targets

The first command is the red reproduction. Final results: 95 source planner,
7 commit-classification and 4 embedded DDL tests pass; affected compilation
passes with existing warnings. Embedded tests require host memory/storage
setup access. Green logs: /private/tmp/tidb-ddl-action-owner-{green,worker,server,all-targets}.log.
From the repository root, make lint and git diff --check pass. Commit must use
TERM=xterm git -c core.hooksPath=hooks commit and pass the hook's locked server
build; then a separate `cd rust && cargo build --locked -p tidb-server` must
succeed immediately before normal push to hparser-integration.

Correctness/compatibility impact: cancellation completes atomically without
schema publication, successful retries retain their diagnostics without being
misclassified, and queue writes have one action-independent owner. Tests cover
phase ordering and raw-argument preservation. No measured performance change
is claimed. This is maintenance of existing paths, not new package acceptance.
Normal non-cancelling errors still lack Go's complete persisted error-count,
global retry-limit and CANCELLING transition owner. CHECK missing-object and
constraint checks, other worker validation/error identities, MV build/reorg
and rollback dependencies, direct SQL admission, delete-range GC, scheduling,
MDL-disabled operation and TiFlash placement gaps remain open. Full upstream
package variants/original tests, real multi-node interoperability, prior ten
embedded baseline failures and sysbench/TPC-C/TPC-H/YCSB were not verified.
No complete package or repository parity is claimed.


## Expanded structural audit receipt (2026-09-30)


Refreshed integration 9f0a41b5db and Go master e953a09d9d; pull was already
current and dependency pins unchanged. The current-audit/structural-findings.md
register consolidates all currently recorded open structural findings with
source owners, affected live/seed scope and dependencies before removal.
Newly traced responsibilities include versioned bootstrap, TTL and GC workers,
resource-control installation and RU history, MPP graph/range/stream/security
ownership and PD service-mode discovery. These are source findings; no live
cluster or workload behavior has been measured in this checkpoint.

The new audit-protocol-projections.py compiles five local projections plus
complete pinned upstream inputs with protoc, compares descriptor declarations
and field tags, and writes protocol-projections.json. It records 400 omitted
items, one KeyspaceScope oneof mismatch and 71 explicit message-to-bytes
representations. Explicit keyspace 0 encodes to empty bytes locally and 0800
upstream. mvccpb has no differences in the compared descriptor contracts;
generator options/reserved ranges/runtime parity remain outside this check.
The inventory tool now includes all 24 PD-client and 7 etcd-API package
directories and their original/support/build/root artifacts, with unreviewed
acceptance status. It still enumerates all 856 TiDB, 41 client-go and 41 kvproto
directories, 83 Rust manifests and 2,050 keyword candidates.

Exact evidence commands from the repository root:

    git pull --ff-only origin hparser-integration
    git fetch origin master
    python3 rust/scripts/inventory-go-rust-parity.py --go-ref origin/master
    python3 rust/scripts/audit-protocol-projections.py --go-ref origin/master

Existing regression rerun from rust/:

    cargo test --locked -p tidb-server --lib system_table_ddl_does_not_publish_statistics_events_like_go

It fails at unistore_cop.rs:4049, `p1 exists`, after ADD PARTITION; no new
production change was made. The current source already excludes system-schema
notifier events. The other nine previously recorded embedded failures were
not rerun. This audit does not assign all ten failures to distinct root causes.
Local test log: /private/tmp/tidb-all-audit-system-ddl.log. Protocol output:
/private/tmp/tidb-all-audit-protocol.log.

The inventory and protocol generators reproduce byte-identical output when
rerun at the same HEAD. Script syntax, all inventory acceptance statuses and
the protocol omission/presence counts were checked. The opaque classifier
rejects changed tag/cardinality/real-oneof membership while recognizing the
optional-bytes equivalent of singular-message presence. Root make lint passed
after retrying its tool bootstrap with network access; the first sandboxed
invocation could not resolve proxy.golang.org. Publication commands are:

    make lint
    git diff --check
    TERM=xterm git -c core.hooksPath=hooks commit -m "audit: inventory remaining Go Rust structural mismatches"
    cd rust && cargo build --locked -p tidb-server
    git push origin HEAD:hparser-integration

The commit hook must itself run the locked server build, and the separate
locked build must succeed immediately before push. Logs use
/private/tmp/tidb-all-audit-{lint-final,commit,prepush-build}.log. The final
response records whether these publication gates succeeded.

No real TiKV/TiFlash, concurrent
DDL/owner-loss, upgrade/GC/TTL deployment, cluster TLS or sysbench/TPCC/TPCH/YCSB
benchmark was run. No package or repository-wide completion is claimed.


## Session/executor/runtime-provider audit receipt (2026-09-30)

Pulled integration 960fa95b48 and refreshed Go master e953a09d9d; both were
already current. Reviewed the shared physical builder, DML source/write
handoff, per-statement SELECT/DML cache stores and session cache admission,
Apply construction, optional configured-server startup and metadata refresh,
and live information-schema row providers. The register adds D11, C01–C02,
E01–E04, S01–S02 and I01–I03: 12 additions, 41 known open findings in total.
No keyword hit was promoted merely because a gap comment existed.

The retained session-ownership-probe.rs runs 46 SQL commands through Session.
It reproduces five ownership groups: lost alias updates, multi-update FK
bypass, partial in-process ALTER publication after error, per-statement cache
capacity/flush gaps, and fixture/constant dynamic virtual-table results.
JOIN USING and referred-FK multi-DELETE controls behave correctly. Go expected
contracts come from pinned master source; no new Go-server execution occurred.
The diagnostic intentionally prints SQL errors instead of returning a failed
process status, so its successful exit must not be called a passing parity
suite. Full SQL stdout is committed in session-ownership-probe.txt.

Files changed: this ExecPlan, current-audit/README.md, structural-findings.md,
session-ownership-review.md, session-ownership-probe.rs and its .txt output.
The example was temporarily copied into the existing session crate for the
locked run and removed after verifying byte identity. No production crate,
Go/Bazel input, dependency pin, generated source or native client file changed.
No bazel_prepare or Go failpoint setup is triggered. No complete package was
implemented, integrated or accepted by this audit.

Exact validation commands from the repository root:

    git pull --ff-only origin hparser-integration
    git fetch origin master
    mkdir -p rust/crates/tidb-session/examples
    cp rust/docs/parity/current-audit/session-ownership-probe.rs rust/crates/tidb-session/examples/audit_session_ownership.rs
    (cd rust && cargo run --locked -p tidb-session --example audit_session_ownership)
    rustfmt --check --edition 2024 rust/docs/parity/current-audit/session-ownership-probe.rs
    make lint
    git diff --check

The cargo command alone is scoped to rust/; the others are root commands.
Probe, formatting, root lint and whitespace checks completed successfully;
the probe's observed SQL failures remain unresolved by design. An inline
Python check verified 41 unique IDs, 12 additions, all 46 SQL/result pairs,
local Markdown links and removal of the temporary example. Source files were
read from origin/master using git show/git grep. Existing source/descriptor
inventory receipts were not regenerated because their inputs did not change.
Logs: /private/tmp/tidb-session-ownership-{probe,lint}.log.

Publication commands, with the final result reported in the response:

    TERM=xterm git -c core.hooksPath=hooks commit -m "audit: trace session executor and runtime provider mismatches"
    (cd rust && cargo build --locked -p tidb-server)
    git push origin HEAD:hparser-integration

The pre-commit hook must itself pass the locked server build, and the separate
locked build must pass immediately before push. Publication logs use
/private/tmp/tidb-session-ownership-{commit,prepush-build}.log.

Correctness risks discovered include lost writes, orphan FK values and local
schema changes surviving statement errors. Compatibility/performance findings
include incomplete cache eviction/sharing, whole-read materialization,
serial-only Apply, stale optional-server descriptors and missing real cluster
providers. This audit introduces no runtime behavior changes. Full upstream
package variants/original tests, distributed failure injection, the ten prior
embedded baseline failures, multi-node/TLS interoperability and
sysbench/TPC-C/TPC-H/YCSB remain unverified. The exhaustive audit is unfinished;
41 records are all currently established findings, not a proof of no others.

## Complete protocol-owner removal outcome

The continuation removes all five remaining local projections and their stale
field lists, plus the handwritten MPP UNIMPLEMENTED helper and repeated fixture
forwarders. Actual generated descriptor comparison now reports zero omissions
or contract mismatches; all 71 intentional opaque TiKV fields remain. Keyspace
zero presence, new global GC fields, store stats and watch progress are retained.
Complete inventories and source-membership gates prevent selected-schema drift.

See `parity/current-audit/complete-protocol-owner-repair.md` for the complete
change boundary, red/green evidence, commands and risks. Generated contracts
do not supply PD service discovery, BR helper constants, etcd logging/gateway
behavior or whole-package acceptance. The other structural findings stay open.

## Shared session plan-cache owner outcome

Prepared/non-prepared SELECT/DML now use one physical-entry owner. Capacity,
recency, compatible variants, invalidation and cache close belong to that
session owner; prepared definitions retain syntax and key metadata. SQL and
binary statement close share Go's retention switch. Instance flush uses a
factory-lifetime invalidation handle across independent session catalog images,
and the server sends the correct OK response for flush.

The repair receipt is `parity/current-audit/shared-session-plan-cache-repair.md`.
Three broader checks still fail on unchanged integration: filtered-IN access
path selection, an index-join parameter rebuild and MySQL grants quoting.
No assertion was disabled. C02, exact native retained-heap accounting, workload
benchmarks and complete package acceptance remain outside this repair.

## Expanded production-owner review outcome

Integration 13689e0b13 and master e953a09d9d5e29e60c62f42d3aacebb819af49a5
were current. Native master remains b2b3783. The refreshed inventory covers
856 TiDB and 113 recorded external-module package directories and 83 Rust
manifests; it does not accept these packages. Thirteen additional findings
bring the consolidated register to 54 records, with 50 unresolved. The
new receipt is `parity/current-audit/expanded-ownership-review.md`.

Both diagnostic programs completed with six finding groups reproduced through
SQL, public configuration APIs and an ephemeral loopback MySQL connection.
The register excludes disproved candidates and retains source-only limits.
No runtime code or dependency was changed. Root `make lint` succeeded after
the initial sandbox attempt could not resolve the Go module proxy. Formatting,
Python syntax, receipt links, ID uniqueness and temporary-source cleanup were
checked. Publication must use the actual locked-build hook followed by a fresh
locked server build; never bypass these gates for documentation under rust/.
Full package/source/test/variant acceptance, the other unresolved findings,
distributed failure tests and all four workload benchmarks remain unfinished.

Revision note: expanded the audit beyond session cache ownership into
privilege policy, import, binding maintenance, wire/configuration and domain
worker composition; retained executable observations and their controls.

## Subsystem ownership review outcome

Integration dae65456f9, freshly fetched master e953a09d9d and native master
b2b3783 were current. The follow-up records 18 additional findings and a scope
matrix accounting for all 83 Rust crates and 856 inventoried TiDB package
directories. The stable register now has 72 records, 68 unresolved and four
repaired. Details and exact commands are in
`parity/current-audit/subsystem-structure-review.md`.

The retained diagnostic completed after checking warnings immediately after
the generated-column INSERT. Ordinary strict conversion rejects 1000; generated
conversion stores 127 without warnings. Session migration, BR job lookup and
ordinary staleness reads are explicitly refused. Configuration/cache metadata
observations are kept separate from the source-only owner findings.

Production code and client-rust dependencies are unchanged. Scope accounting
is not package acceptance, and the other original tests, variants, external
dependencies, distributed scenarios and all four benchmarks remain outstanding.
Root `make lint` passed after retrying outside the network-restricted sandbox;
the initial failure was DNS resolution of the Go lint dependency. The probe,
coverage generator, Python syntax, 72 IDs, receipt/source links, rustfmt and
`git diff --check` passed. The actual commit hook must run the locked server
build, followed by a separate fresh locked server build before push. The
publication response records those final gate outcomes.

Revision note: broadened the owner audit and added complete scope accounting,
retained diagnostics and negative controls for stale comments/partial models.

## Global-config synchronization implementation plan


Go master e953a09d9d5e29e60c62f42d3aacebb819af49a5 owns the complete leaf
package `pkg/domain/globalconfigsync`: globalconfig.go, globalconfig_test.go
and BUILD.bazel. There is no doc.go, generated/platform variant or fixture in
that package. Rust will implement it in tidb-domain::globalconfigsync. The
existing tidb-session registry carries GlobalConfigName for the two source
variables; explicit validated writes notify the syncer, including DEFAULT,
while startup/cache rebuilds remain quiet. The existing PD worker performs
StoreGlobalConfig with an empty config path, source item fields and its usual
timeout. Go ignores response-body Error for this call and performs no retry.
The node keeper starts once with the existing PD handle, logs failures and
joins before PD shutdown. Rust must release blocked senders when the receiver
is stopped; no statement context is inherited by this background worker.

Milestones: implement the leaf queue/store contract and metadata; retain a
failing SQL-to-notification regression before connecting explicit SET; compose
the transport/keeper/factory lifetime; run original Go cases and corresponding
Rust unit/SQL/RPC/shutdown cases. Record the complete artifact mapping and
validation receipt, update O12 only after the owner and callers pass, then run
root lint, actual hook build and fresh pre-push locked build. Other Domain,
session and PD packages remain outside this leaf's acceptance claim.

## Global-config synchronization outcome


The complete leaf and production integration are implemented. O12 is repaired;
the stable register now has 72 records, 67 unresolved and five repaired.
`parity/current-audit/global-config-sync-package.json` pins every source/test/
build artifact and mapping; `global-config-sync-repair.md` records commands,
semantics and limitations. Ten scoped tests (nine new-contract tests and one
existing config test), six cluster-global-variable tests, 26 PD library tests
and 46 PD RPC/lifecycle tests pass. One pre-existing live-PD test is ignored.
All five affected crate targets check. Both original Go package tests pass
using master’s Go 1.25.14 toolchain and dependencies; installed Go 1.27 does not
match its map-ABI variants. Root lint passes. Three unchanged session-suite
failures are reproduced on isolated baseline and retained without changes.

The Go compiler cache was cleared after the original Go tests completed,
recovering about 46 GiB (free space increased from 3.2 GiB to 49 GiB). Both
managed reference checkouts were archived after their processes finished.
Bazel preparation initially lacked the binary; a checksum-verified pinned
7.7.1 binary was fetched into a temporary directory and preparation was
retried. `PATH="/private/tmp/tidb-globalconfig-tools:$PATH" make bazel_prepare`
passed in the unchanged integration checkout. In the master checkout, Gazelle
built but repository preparation failed because proxy.golang.org reset the
downloads of github.com/ajstarks/deck, modernc.org/tcl and modernc.org/ccorpus.
The original Go package tests independently passed. No Go/Bazel source delta
belongs to this Rust repair; reference preparation artifacts were not copied.
The final lint rerun and the PD projection/path regression pass. Mandatory
hook and fresh pre-push build results are recorded in the publication response.

After the interruption, integration advanced by two non-overlapping commits
to 1e570fd1e7 and was fast-forwarded before publication; the focused tests and
all-target check were rerun. The latter initially exposed four incoming test
constructor calls missing the new collation argument; a complete call-site
sweep found twelve across executor and planner tests. Passing `None` retains
their prior scenarios. The repair receipt lists the existing order/TopN/planner
tests and the expanded six-crate all-target command. Master advanced to
93a01d31f6. All three leaf
artifacts, Domain keeper, session publication path and module inputs remain
byte-identical to the original Go test revision e953a09d9d; the two notification
metadata entries/constants are also unchanged. The receipt distinguishes that
source equivalence from an original-Go test rerun, which was not performed.

Revision note: implemented the first complete new leaf owner from the expanded
review, including tests, generated-protocol transport, caller metadata and
background lifetime; did not claim broader package or benchmark parity.

## Restore-utils protocol ownership plan


Starting from integration aa7b8d864d and Go master 93a01d31f6, inventory all
eight artifacts of br/pkg/restore/utils (four production files, three original
test files and BUILD.bazel). There is no doc.go, platform variant or fixture.
The current native owner declares a seven-field File and a duplicate RewriteRule
in restore_utils/proto.rs. Go takes generated backup.File pointers, retains all
metadata through range merging, and clones generated rules only at its explicit
Clone boundary.

Remove proto.rs and share the existing complete native protocol package via
tidb-proto. Carry Arc<File> through grouping/range merging, so copying range
containers preserves payload identity and does not copy checksums, encryption
metadata or per-table metadata. Match/rewrite lookup returns borrowed generated
rules; explicit go_clone remains a deep copy. Keep source equality's explicit
field policy and timestamp-reset behavior. Extend existing package tests with
a failing full-generated-file API/identity regression before replacement and
cover rule lookup/clone/filter behavior. Re-run every original Rust case, the
original Go package (including its race case), and the five source benchmark
sizes as executable workloads, without claiming workload performance parity.

Record all artifacts, original test/benchmark mappings, dependencies and seed
integration status. This removes P04's narrowed payload contract; BRIE execution
and acceptance of metautil/rtree/spans remain separate. Update the old receipts
that called this narrowing harmless. Root lint, the actual locked server build
hook and a fresh locked build before push remain mandatory.

Decision: reuse generated types instead of appending currently missing fields
to the local structs. That removes the duplicate schema owner and ensures
future generated fields survive grouping/return without a handwritten mapping.
Native shared file handles preserve Go's pointer flow while avoiding repeated
large-message clones. No Go/Bazel production files are changed.

Outcome: deleted the local File/RewriteRule owner and test DataFileInfo; all
original cases now consume complete generated protocols. The new API regression
failed before replacement and passes afterward. The source gate includes
import_sstpb and all 139 kvproto artifacts. The Go-master reference passed its
required Bazel preparation and original race suite, then its disposable outputs
were cleaned and the worktree archived. All five source merge workloads run
instead of an empty placeholder. Package inventory, case/support mapping,
commands and remaining limits are in
`parity/current-audit/restore-utils-protocol-repair.md` and its JSON inventory.
P04 is repaired, leaving 66 unresolved registered findings. Historical parity
claims are marked superseded; no live BRIE or benchmark parity is claimed.
The commit hook and separate pre-push build results belong to the publication
response, and must both pass before this repair is pushed.

Revision note: completed the full restore-utils protocol-consumer review and
repair rather than adding missing fields to the old projection. Other BR
packages and live integration remain explicit separate owners.

## Range-tree protocol follow-up plan


Starting from integration 566163c58c and Go master 93a01d31f6, review the
complete seven-artifact br/pkg/rtree package: both production files, original
tests, TestMain/goleak harness, fuzz input and BUILD.bazel. Keep its own
KeyRange for algebra/logging, but return generated kvrpcpb.KeyRange from both
missing-range APIs. Remove RangeFile and generic file parameters throughout
Range, RangeStats, range/progress trees, checksum collection and MetaSink.
Use Arc<backup.File> directly, matching Go's generated pointer payloads.
Migrate the sole sibling restore-utils caller and all original tests; remove
the handwritten TestFile fixture. This prevents a future caller from
reintroducing narrowed or deeply cloned protobuf payloads at this boundary.

First reproduce the public RPC type mismatch with a test of both gap APIs.
Then validate generated-file identity through tree clone, merged ranges and
the metadata sink, including source error/checksum sequencing. Run all Rust
crate tests, source benchmark workloads and original Go tests with -race in
a disposable master checkout. Keep fuzz seeds and record fuzz-runtime limits.
Run all-target checks, make lint, scoped formatting, the actual commit-hook
locked server build and a fresh locked build immediately before pushing.
Clean reference build outputs and archive the temporary checkout.

Decision: remove the generic adapter, rather than adding conversion shims to
callers. The Go package has one concrete payload owner. Borrowed progress-tree
access, the metautil sink boundary and local API-V2 decoding remain explicit
existing integration boundaries; this does not enable live backup/restore or
accept those dependency packages. Preserve their limits in the final receipt.

Outcome: both missing-range APIs failed the generated-type regression before
replacement. All 38 active tests pass with concrete generated files, including
identity through range/tree clones and metadata failure/retry. The source
checksum/callback ordering remains intact. Both workload tests execute (no
empty rtree benchmark placeholder remains), and the original Go race suite,
leak harness and fixed-size update workload pass. The source inventory and
exact commands are in `parity/current-audit/rtree-protocol-repair.md` and its
JSON inventory. P05 records this repaired boundary; 66 registered findings
remain unresolved. Existing progress aliasing, metautil/diagnostic/keyspace
boundaries and live BRIE are explicitly not accepted as complete.

Revision note: extended concrete generated ownership through the full range
tree package, eliminating the permissive payload layer beneath the P04 repair.
Publication still requires the actual hook and fresh pre-push locked builds;
their results are recorded in the final response.


## Retained progress ownership follow-up (2026-10-01)

The preceding protocol repair recorded a remaining ownership difference in
`br/pkg/rtree`. Go inserts and returns the same `*ProgressRange`; Rust stores
a value and returns an exclusive borrow, and its translated callback tests
clone the entire record to survive deletion. A retained clone cannot observe
subsequent backup responses. The acceptance scenario retains two handles,
updates through one, observes the same coverage through the other and tree,
and keeps the original object alive after completion without reinserting it.

The complete package inventory remains `rtree-protocol-package.json`: both
production files, all four original test/support/fuzz/benchmark files and
BUILD.bazel at master 93a01d31f6da205ae4bf376825293903a6899fdb. Recheck all
blobs before acceptance. No new package is dispatched as a partial port.

Milestone one adds a regression to the existing Rust rtree tests and runs
`cd rust && cargo test --locked -p tidb-br --lib retained_progress`. Expect
the retained copy to report stale coverage before the fix. Milestone two
changes `ProgressRangeTree` to store and return shared, mutex-protected
progress handles; removes deep Clone from progress and its result tree;
migrates all callers and both original callback tests. Keep mutable guards
out of external callbacks and metadata delivery. Preserve Go's deferred
deletion and checksum behavior on metadata errors. This is a Rust ownership
adaptation, not permission to add background synchronization or workers.

Milestone three runs the whole tidb-br unit suite, original Go package tests
with race detection in the restored master reference checkout after required
Bazel preparation, all-target compilation, formatting, `make lint`, and
`git diff --check`. The existing update/merge workloads are unchanged; rerun
only if changes reach those algorithms. Publication uses the actual commit
hook locked server build, then a fresh locked server build and normal push
to hparser-integration. Clean the reference build outputs and archive its
managed checkout afterward. All commands are repeatable; retain the failing
test and log on failure instead of weakening its contract.

Decision: share the actual record rather than adding snapshot refresh or
copy-back helpers. Range and RangeStats remain value containers where Go
explicitly copies them. Object-storage integration, BRIE dispatch and
external dependency/lifecycle acceptance remain open. Connection-ID review
also found that internal tracking depends on the absent Domain SysProcesses
owner; do not replace its counters alone and claim that lifecycle repaired.

Outcomes: the pre-fix regression reported all `[a,d)` missing through a retained
copy while the tree reported only `[c,d)`. Forty Rust tests now pass, including
identity before insertion/after lookup, retained updates, callback/sink lock
release, deferred PhysicalID reads, same-key replacement and final deallocation.
All original Go tests pass under the race detector with actual local-storage
MetaWriter and goleak. Removing temporary callback/writer extraction also
preserves their ownership on early return. The prior send-failure test still
verifies Go's repeated callback on retry and deferred checksum publication.
All-target compilation, lint, formatting, diff and inventory checks pass;
publication gates are recorded in the linked receipt and publication response. No new complete BR application claim is made.


## Statistics LFU lifecycle follow-up (2026-10-01)

The complete Go package has three production files (`key_set.go`,
`key_set_shard.go`, `lfu_cache.go`), one test file with ten tests, and
BUILD.bazel. Current master 93a01d31f6da205ae4bf376825293903a6899fdb still
matches the five blobs in the older LFU audit receipt. There is no doc.go,
fixture, generated input, platform variant or benchmark. The Rust owner is
`tidb-stats-handle-cache-internal-lfu`, used by the parent statistics cache.
Its Ristretto/Stretto dependency equivalence remains explicitly unaccepted.

Rust substitutes an allocated empty Table for Go's nil eviction trigger and
classifies all negative keys as triggers. Go's shard operation uses signed
remainder: -1 is invalid but -256 reaches shard zero and remains a real table.
Rust instead uses Euclidean remainder, contradicting its own retained
should-panic regression. Rust Close also marks itself closed before acquiring
the primary-cache mutex; a concurrent Close returns immediately instead of
waiting for Go's sync.Once completion. Every Get and Put clones its primary
handle through this exclusive mutex, adding avoidable contention.

Milestone one runs the unchanged LFU suite, then adds regressions for closure
waiting and negative real-table eviction. Preserve fail-before evidence.
The baseline sandbox denies the host-memory sysctl used before the zero-quota
test override; rerun that test with host access instead of changing Go's error
ordering. Milestone two represents nil as Option<Arc<Table>>, removes the fake
empty Table and sign-based callback filter, restores signed shard selection,
and uses a shared-read/exclusive-close cache lifetime. All aliases must wait
for completed shutdown, with callbacks closed before the cache is drained.
Never hold a caller lifetime lock from a cache callback. Keep primary-first
reads, publication-before-admission, eviction/drop/cost order and shared Copy.

Milestone three maps all ten original test functions and supporting table
fixtures, including concurrent replacement, low-capacity eviction and capacity
reduction. Run the original Go package with race detection in the prepared
master reference; run the Rust owner and parent cache tests, all-target check,
formatting, root make lint and diff hygiene. Add no new cache policy. Tests
must not assert a deterministic sampled victim. Before publishing, run the
actual commit hook's locked server build and a separate fresh locked server
build before pushing hparser-integration. Clean/ archive the reference after
validation. Existing source receipts remain historical, with a link to the
new receipt rather than an unsupported complete external-module claim.

Decision: encode nil directly and share the cache's lifetime, rather than
adding special-case negative-key exceptions or removing synchronization from
a concurrently closed cache. This preserves Go's native lifetime boundary
while respecting Rust's worker/drop ownership. The complete package review
does not accept its external cache implementation.

Outcomes: three regressions failed before repair and pass afterward. The new
Clear path reuses the shared owner, drops fallback map storage, preserves Go's
post-close accounting, and still clears metadata published after Close.
Callbacks recover independently with diagnostics and run onExit after a
recovered eviction/rejection. Fake trigger allocation, sequential key counter,
sign-based payload classification and primary Arc clones are removed.

The original Go race suite passes; native owner/parent suites have 32 passing
tests and one retained failing C04 reproduction. The full pressure workload
exposed a Stretto nonresident admission/replacement mismatch. An initial
Cost==0 assertion was disproved against Go (three runs retain positive cost
but all 50 table payloads are evicted); no such assertion remains. Rust still
retains a full 136-byte payload, so its payload regression stays unaccepted.
Stretto's cached archive and extracted sources match the locked checksum.
There is no safe package-complete dependency replacement in this batch; do
not add a second eviction policy to the wrapper. Current receipt and all five
artifact hashes: `parity/current-audit/lfu-lifecycle-repair.md` and its inventory.
The register is 74 findings, seven repaired, 67 unresolved. No benchmark or
complete package acceptance is claimed. All-target compilation, root lint,
formatting and inventory/diff checks pass; publication gates remain mandatory.

Revision note (2026-10-01): added system ownership and risk/dependency ordering
after the repeated full-picture request. This prevents the Ristretto milestone
from replacing the wider session/planner/executor/DDL/client parity objective.
For this plan-only change, validate with `git diff --check` and the required
`cargo build --locked -p tidb-server` in both the commit hook and immediately
before push. Runtime regression suites, Go package tests, root lint and SQL
benchmarks are not rerun for a documentation-only change; earlier results above
remain historical. No Go/Bazel metadata changed, so bazel_prepare is not needed.


## Resolved UPDATE privilege repair (2026-10-01)

Go master 93a01d31f6da205ae4bf376825293903a6899fdb resolves each
assignment with expression.FindFieldName over the logical output names before
recording its base table's UpdatePriv. Rust's session AST collector skips an
unqualified joined target. Ordinary execution and execution after REVOKE both
write successfully in the pre-fix regression. EXPLAIN dispatch also bypasses
the table-privilege boundary, including EXPLAIN ANALYZE writes.

Use the existing logical FROM-plan builder and shared FindFieldName port in
`tidb-executor/src/driver/planner_bridge.rs`; restrict writable identities to
the outer base-table sources, as Go's updatableTableListResolver does. Remove
the session's UPDATE qualifier guesser. Ordinary execution, SQL PREPARE,
binary PREPARE, and EXPLAIN must share the resulting requests and live grant
evaluator. Joined DML is not admitted to the physical plan cache and rebuilds
on each execute; re-resolve its requests too, because DDL can transfer an
unqualified column to a different table after PREPARE. Keep the fast single
table path and avoid catalog/planner work when collecting read privileges.

Discovery: the earlier A01 column-only SELECT claim is false. Go buildDataSource
records table SELECT with an empty column, and the master session oracle rejects
SELECT x under only SELECT(x), code 1142. Correct the audit, not Rust semantics.

Decision: repair the demonstrated authorization holes using the existing logical
name owner. Do not claim the complete planner/core or session packages accepted:
read/DDL/DELETE collectors and the narrower DML executor handoff remain open.
This replaces the fail-open UPDATE branch, but does not finish the unified Go
visit-info production lifecycle for all statement kinds.

Validation: table_scope regressions cover aliases, empty sources, multiple
targets, derived sources, name errors, revocation, schema changes, binary and
SQL preparation, and EXPLAIN. Run original Go session privilege tests with the
additional oracle cases. Compare broad Rust suite failures against unchanged
HEAD before classifying them as pre-existing. Run formatting, root lint, and
both mandatory locked server builds before commit/push.

Revision note: advance the full-picture plan with an authorization repair and
correct a disproved audit allegation; retain package-level acceptance boundaries.


Outcome: the demonstrated joined UPDATE and EXPLAIN authorization bypasses
are repaired; five regressions fail on unchanged HEAD and pass after the fix.
The full table-privilege suite passes 20 tests. The parameterized-derived and
temporary-overlay controls preserve existing accepted execution. Broader suites
retain only their reproduced HEAD failures. Exact commands, Go oracle patch,
remaining risks and comparison names are in
`parity/current-audit/update-privilege-repair.md`. A01 stays partially repaired;
the register remains 74 findings, seven repaired and 67 unresolved.

## Native operation-lifetime removal (2026-10-01)

Refreshed both repositories and reviewed T03 against master 93a01d31f6da205ae4bf376825293903a6899fdb's client-go 8edb23f6c7ee. The renewed user instruction authorizes this continuation; the old pending patch was rederived from source, not applied wholesale. Native commit 488bb73 is published on ngaut/client-rust master. It gives transaction completion and retry/RPC dispatch the owning Go lifetime, preserves initialization rollback's caller, and validates queued/in-flight transport cancellation. The native library passes 1,405 tests (two ignored), with strict Clippy passing.

TiDB removes both the heartbeat request-type exception and the transaction_tasks client-mode flag. Timestamp cancellation now uses the same explicit operation scope before and after the synchronous timestamp provider call. Regression tests reproduce the old foreground bypass and confirm background work survives statement cancellation but stops when its own owner closes. The provider's synchronous in-flight timestamp call cannot itself be interrupted by this adapter; cancellation is checked on return. That broader provider API limitation and T02's competing routing/RPC owners remain explicit follow-ups.

See parity/current-audit/operation-lifetime-repair.md for source decisions, final validation and publication evidence. This is maintenance of existing transaction/transport ports, not whole-package acceptance or full TiDB/client-go parity. Native source-only synchronization regenerates protocol artifacts through the existing script; no protocol schema or generated file is edited by hand. Both mandatory locked server publication gates remain required.

The next continuation removes foreground RPC error stringification and native tikv.rs's duplicate retry-terminal converter. Native b23c6d37 is published and synchronized; the common converter now uses Go's configured PD timeout while retaining diagnostics in retry history. All 33 resolver tests pass after preserving native identity and correcting the stale cancellation-after-status expectation to Go's best-effort read-cleanup contract. Native full library validation passes 1,405 tests with two ignored; TiDB's affected suites pass 685 with 11 ignored. Lint and formatting pass. The receipt is parity/current-audit/resolver-error-identity-repair.md. T02 and the broader foreground/shutdown audit remain open, and both locked server publication gates remain mandatory.


## Shared replica candidate policy (2026-10-01)

The candidate-selection continuation publishes native 568a68d9 and public-facade follow-up 5777c01c, then synchronizes TiDB. Native idle selection no longer rejects every previously attempted peer before Go's DataIsNotReady exception can apply. TiDB removes its independent candidate attempt/scoring/tie loop, and DistSQL removes the per-query selection seed. Remaining metadata/health/request state is explicit T02 work; no complete internal/locate acceptance is claimed.

Native validation passes 1,405 library tests with two ignored and strict Clippy. TiDB validation passes 884 tests with 14 ignored across transaction, region and DistSQL suites; root lint passes. The living implementation plan is replica-selection-owner-execplan.md and the receipt is parity/current-audit/replica-selection-owner-repair.md. The actual hook and separate post-commit locked server builds passed. The final receipt amendment repeats the hook and requires another locked build immediately before push.


## Shared store health (2026-10-01)

Native 6f663b3 is published and synchronized. TiDB removes its separate slow-score and feedback/decay implementations and uses the native StoreLoadStats with monotonic Instants. Its topology copies retain an Arc to native StoreHealthStatus. Native score reads are atomic and feedback/decay writes skip contention. Both the lost shared-health identity regression and held-lock feedback regression fail before repair and pass after it.

The native library passes 1,406 tests/two ignored with strict Clippy. TiDB transaction/region/DistSQL validation passes 886 tests/14 ignored; root lint passes. The actual pre-commit locked server build passed; the final publication command reruns the locked build after the receipt amendment and pushes only on success. See store-health-owner-execplan.md and parity/current-audit/store-health-owner-repair.md. Production TiDB latency/feedback/tick wiring, broader T02 ownership and complete-package acceptance remain open.


## Revision note — 2026-10-01 full-picture plan


Replaced active stale priorities with a complete finding-to-owner work map,
source-pinned package acceptance units, dependency/activation/removal gates,
concrete validation/publication steps and ongoing workload measurement. Preserved
historical receipts and repaired findings. The next implementation unit is the
native PD root/TSO/discovery closure; this revision performs planning only.


## Revision note — 2026-10-01 PD deadline prerequisite


Implemented the complete W01 deadline leaf with native batch/retirement callers
and the required cancellation adapter correction. Recorded native publication,
TiDB synchronization and validation in the linked receipt. Preserved the open
parent-package obligations and current 77-finding assignment.


## Revision note — 2026-10-01 PD retry prerequisite


Executed the next complete native dependency from the approved sequence, with
source-pinned inventory, initialization/timeout red regressions, original tests
and native publication. Parent discovery/TSO/root acceptance remains open.


## Revision note — 2026-10-01 PD metrics dependency closure


Completed both entire metrics packages before adding the circuit-breaker owner.
Native 61e9a86 and generator-formatting follow-up e3e8de8 are published; TiDB shares its collectors and removes its independent
registration and label materialization. Runtime Go oracles cover source metadata,
all observer bindings and initialization ordering. The linked metrics receipt
records exact validation, incoming configured TopN evidence and publication gates.
No parent lifecycle finding is closed by these prerequisite packages.


## Revision note — 2026-10-01 shared PD circuit breaker


Completed the next whole PD dependency and removed the private cache state
machine. The receipt distinguishes shared leaf acceptance from the remaining
logical-call versus per-RPC interceptor boundary. Signed durations preserve Go
inputs that unsigned Rust Duration could not represent. No parent lifecycle
finding closes until the complete service owner and its callers are accepted.


## Revision note — 2026-10-01 retry value ownership re-review


The complete grpcutil review found that its per-RPC value copy could not use the
existing callback owner, and the prior retry port excluded signed Go duration
behavior. Revalidate the whole retry package before transport integration. Remove
unsigned saturation and the Rust-only public log builder, preserve all original
cases and validate signed domains, constructor options and independent copies.
The linked receipt explicitly corrects the earlier unrestricted coverage claim.


## Revision note — 2026-10-01 shared PD error prerequisite


The whole grpcutil review requires a shared error owner as well as retry and
circuit-breaker dependencies. The complete errs package now retains source codes,
prototype identity, wrapped causes and classifier/logging behavior; the previous
uncoded breaker adapter is removed. Exact scope, language adapters and independent
source/runtime evidence are recorded in the linked receipt. The full transport
package and its callers remain the next unit; no top-level finding closes here.


## Revision note — 2026-10-01 withdraw unsafe repartition execution


The full-register review at b01f97d6b5 reproduced accepted repartition making
existing rows invisible. The [removal ExecPlan](partition-owner-removal-execplan.md)
withdraws that complete unaccepted local/cluster shortcut and its thread-local
metadata handoff. Three regressions fail before removal; five scoped tests pass
afterward, including ordinary ADD/DROP/TRUNCATE on fresh threads. Parser and AST
support remain, but execution refuses repartition before mutating rows or schema.
This is a correctness containment step while the full Go DDL owner is incomplete;
it does not close D01, accept a partial package or replace Go's reorganization
with another private algorithm. All 77 unresolved structural findings remain.


## Revision note — 2026-10-01 PD protocol-preface feasibility


The [preface-owner experiment](parity/current-audit/pd-grpcutil-contract/h2-preface-review.md)
resolves the prior candidate's two readiness failures in the shared HTTP/2 decoder.
All 14 isolated tests and the extended pinned Go race/goleak comparison pass,
including fragmented/malformed SETTINGS and preserving h2's default policy.
The old eight-pass/two-fail accessor remains reproducible. This completes the
preface feasibility milestone only; the whole grpcutil package still requires
its remaining transport and integration gates before replacing native owners.
No partial production package, dependency update or structural closure follows.


## Revision note — 2026-10-01 remove private import execution


The [import removal ExecPlan](import-shortcut-removal-execplan.md) withdraws the
entire private IMPORT INTO runtime: local CSV parsing, nested COUNT/INSERT
execution and the SELECT-to-INSERT rewrite. The incomplete adapter ignored
options and assignments and did not own Go's import lifecycle. File/SELECT
containment regressions fail before removal and preserve target/source data,
schema and surrounding transaction rollback afterward. Go supports these forms;
refusal does not close E05 or accept part of the executor/importer/DXF packages.
Original source fixtures and the historical audit evidence remain intact.


## Revision note — 2026-10-01 shared DDL pause lifecycle


The [pause lifecycle repair](ddl-pause-lifecycle-execplan.md) removes implicit
forward execution of PAUSING/PAUSED jobs in the existing shared worker.
Pause now checkpoints only the active envelope, preserves previous errors and
raw arguments, and releases the worker without reporting SQL completion.
Both planner and embedded-worker regressions fail before and pass after;
all nine integrated actions, owner loss, MDL recovery and resume are covered.
D04 remains partial because cancellation conversion is still absent; D05's
global error-budget owner also remains open. No partial Go package is accepted,
no disabled action is activated and the unresolved count stays 77.


### Shared cancellation and error checkpoints, 2026-10-01


Starting at 7a7c756a390008771fb327942e95ff8a826a25c2 with unchanged Go master
93a01d31f6da205ae4bf376825293903a6899fdb and native 6163ecfc, the live worker
now dispatches cancellation before forward action, persists action error counts
and reads the current global limit. CHECK errors use the original transaction;
the detached rollback transaction/retry implementation is removed. A concurrent
pause remains authoritative through write-conflict retry. Object lookup failures
cancel with source errors, and CHECK argument/metadata pointer identity survives
publication. See ddl-cancellation-lifecycle-execplan.md for red/green evidence,
source owners, 115 passing scoped cases and publication gates.

The current register closes the recorded D04/D06 gaps and advances D05 to partial:
75 unresolved (69 open, six partial), ten repaired, 85 total. This supersedes the
counts in older review snapshots without rewriting them. D05 still needs external
error delivery, panic and retry policy. D01/D03 still own the larger durable SQL
and scheduler migration; disabled actions remain disabled. No upstream Go package
is accepted by this maintenance repair, and no benchmark improvement is claimed.


### Shared DDL panic recovery, 2026-10-02


Starting at 7ab955b0d44f41716f39b4841ec9d4f798790170, with unchanged Go/native
pins, existing planner and staged-validator unwinds now use the same original
transaction failure checkpoint. The old error-only checkpoint API is replaced;
no detached recovery transaction or retry owner is introduced. Go's countForPanic
rules preserve prior errors/raw arguments, select cancellation state and read the
current limit. Incomplete metadata and row buffers are discarded. The existing
panic counter moves to shared utility ownership and remains re-exported to server
consumers; every recovery counts even if owner loss or a pause prevents commit.
See ddl-panic-owner-execplan.md for red/green cases and publication evidence.

D05 stays partial and the register stays at 75 unresolved, ten repaired, 85 total.
Full returned-error taxonomy, rollback-transaction classification and retry timing
remain open. Review of all nine live persisted handlers qualifies the former
external-delivery allegation: they currently emit no placement/label requests;
the missing effects and their failure lifecycle belong to whole-action integration.
No upstream package is accepted or disabled action activated by this maintenance.


### Shared DDL error conversion, 2026-10-02


Starting at 5d0ee83ced20a1cee3d8f0c9bb99dda274c785e9 with unchanged Go/native
pins, move the complete existing DdlPlanError code map out of the server and into
the error owner. Checkpoints no longer flatten non-admission coded failures to
1105; history reads derive SQLSTATE from the common MySQL catalog. Plain action,
rollback-budget and panic diagnostics share Go's DDL/CodeUnknown fallback.
All 21 enum variants are covered through queue/history persistence, and existing
direct SQL tests remain valid. See ddl-error-conversion-execplan.md for the three
red reproductions, final checks and publication gates.

This maintains existing contracts without accepting an upstream package. D05
remains partial: admission/storage producers still erase source RFC/class
identities, and numeric compatibility does not prove mixed-node interoperability.
Rollback-transaction classification, retry timing and complete action owners remain
open. Counts remain 85 tracked, 75 unresolved and ten repaired.


### Shared DDL worker continuation, 2026-10-02


Starting at 77683d6dfb4b77ae11e14fba70e165a648200387 with unchanged Go/native
pins, remove the worker's private CHECK validation result and its escape of other
committed action errors to scheduler polling. The existing shared worker now
continues all live actions after their checkpoint, applies source retry code/message
classification and current-limit rules, and waits through the owner's stop channel.
Worker completion means history was committed; SQL success/failure comes from that
history consistently with and without owner replacement. See
ddl-worker-continuation-execplan.md for red/green and publication evidence.

D05 remains partial. Source error identity, full taxonomy, rollback-transaction
classification, configurable retry timing/metrics and missing action effects remain
open. This is live-path maintenance, not pkg/ddl acceptance; the register remains
85 tracked, 75 unresolved and ten repaired.


### Shared DDL error identity, 2026-10-02


Starting at f8367f2db5 with unchanged Go/native pins, retain typed source errors
through 30 conversion sites in the existing persisted action handlers and CHECK
validation. Remove the CHECK-specific wire-error outcome and reconstructing its
identity from an errno. Source equality governs cancellation and CHECK rollback;
legacy job history keeps its compatible envelope, and plain decode failures use
DDL/CodeUnknown before SQL conversion. The targeted receipt and publication gates
are in ddl-error-identity-execplan.md.

This repairs an existing owner without accepting an upstream package. D05 remains
partial for remaining numeric/string producers, complete taxonomy, transaction
reset, configurable retry timing/metrics and whole-action effects. Counts remain
85 tracked, 75 unresolved and ten repaired. No disabled action is activated.


### CHECK error generation and argument checkpoints, 2026-10-02


Following published 4eb761a825, remove nine handwritten CHECK error-generation
sites using the existing shared Go catalog formatter and native stack capture.
Source state selection distinguishes ADD's invalid state, DROP's invalid job
and ALTER's no-error switch fallthrough. Missing names preserve original case;
validation names use Go lowercase. Remove CHECK's schema-dependent argument
encoding flag: all successful steps re-encode decoded args, while the shared
worker preserves original raw args after failure. Red/green tests, a Go message
oracle and publication evidence are in ddl-error-generation-execplan.md.

All 187 scoped tests, the affected all-target check, lint and audit consistency
pass. D05 stays partial and counts remain 85 tracked, 75 unresolved, ten repaired.
Other producers, action effects, reset/retry/metric policy and whole-package
acceptance remain open; no benchmark or distributed interoperability is claimed.

### Atomic store-health publication, 2026-10-02

The [health-publication continuation](health-feedback-publication-execplan.md)
repairs recorded T04 in the existing native owner and synchronizes published
client-rust c97dafb89883312deb526dc8d8f36cc7f7001f47. Presence/time no longer live
behind the feedback mutex; only writers contend. The native owner performs
client-score update before the asynchronous callback and rechecks freshness
before decay. Three regressions fail with the prior implementation and pass
after repair; original Go health cases and a contention oracle pass with -race.

All 1,524 native library cases pass (two ignored). Native strict library Clippy
passes; all-target Clippy exposes two existing unrelated test warnings. TiDB
health/topology/recovery/distsql and locked all-target checks pass. The focused
receipt records lint and required locked publication builds. T04 is repaired;
T02 and whole internal/locate acceptance remain open. The current register is
85 tracked, 74 unresolved (68 open, six partial), eleven repaired. Earlier
review snapshots preserve their historical source hashes and counts.

### Native PD request connection ownership, 2026-10-02

The [PD request-ownership continuation](pd-request-ownership-execplan.md)
repairs P07 in the existing native owner and synchronizes published client-rust
952013279bc64e590f17c18b9c9222fdaf5a3604. Every existing request constructs an
owned future under a short read lock, then awaits without retaining the guard.
The write-locking retry_mut helper is removed. Reconnect preparation and TSO joins
also run outside the publication lock, while a separate guard serializes
refreshes and preserves leader publication and healthy same-URL stream reuse.

Four native transport tests fail before repair and pass afterward, including
metadata/TSO overlap and requests during discovery/replacement. All 151 native
PD, 89 cache and 1,528 library cases pass (two library cases ignored), as do
strict library Clippy and formatting. Original Go client cases and the retained
metadata oracle pass under race/leak checks. The focused plan records exact
commands, downstream validation and required publication gates.

The current register is 85 tracked, 73 unresolved (67 open, six partial), twelve
repaired. This does not accept the complete PD parent packages; service-mode,
public shutdown and other P03/P06 obligations remain. Removed serialization is
proven through controlled transport, not a measured workload speedup.

## Native PD explicit shutdown continuation (2026-10-02)


The [shutdown receipt](pd-shutdown-ownership-execplan.md) repairs another P06
existing-owner boundary. Public close joins cache work before TiKV and PD/TSO;
retained shared completion handles concurrent and interrupted async callers.
Cache-owned RPC waits now cancel, and current/retired TSO joins remain owned
across an interrupted reconnect. Four failure-before-fix observations and six
focused tests establish the boundary without claiming complete PD packages.
P06 remains partial and the register remains 85 tracked / 73 unresolved /
12 repaired. Native publication, maintained sync and the two mandatory locked
server-build gates are recorded in the focused receipt.

## Parallel projection completion ownership (2026-10-02)


The [projection close receipt](projection-close-ownership-execplan.md) repairs
E06 in the existing executor. One task owner removes queued work and joins
running evaluations before child close, reopen or Rust drop. Result-channel
errors and recovered panics cannot detach the completion barrier. Other CPU
pool users retain their scheduling and queued work; no new threads are added.

Five initial lifecycle regressions fail before production edits. All 27 final
focused/integration cases pass, including six lifecycle and three queue-owner
cases. The complete executor library still has 36 failures, exactly matching
an unchanged-commit control; 1,468 cases pass. All-target compilation and root
lint pass. The focused receipt records the required hook and fresh locked
server-build publication gates. No full pkg/executor acceptance or workload
speedup is claimed. Native client-rust remains current at 19a56cc. The current
register is 85 tracked, 72 unresolved (66 open/six partial), 13 repaired.

## Server command admission ownership (2026-10-02)


The [command admission receipt](command-admission-ownership-execplan.md) repairs
N02 in the existing server owner. All connections share the effective configured
limit through streaming and every command exit. The metrics-only substitute is
removed; acquisition wait is recorded at acquisition, as Go does. Config-file
normalization and explicit CLI precedence reach the same owner. An unwind-safe
watcher guard also removes the retained panic socket found by the regression.

The review adds N06: outer connection recovery has already unwound the framed
writer before it could attempt Go's panic ERR. Fixing command retirement does
not close that error-delivery owner. N03 retains its broader configuration gaps;
its token-limit refusal is now historical. With N02 repaired and N06 added, the
register is 86 tracked, 72 unresolved (66 open/six partial), 14 repaired.

Focused regressions and the isolated unchanged Go limiter race oracle pass.
The focused receipt records broader-suite results, baseline controls, lint and
mandatory publication gates. No complete pkg/server/pkg/util acceptance or
workload speedup is claimed. Native client-rust and its dependency are current
and unchanged at 19a56cc.

## Connection recovery ownership (2026-10-02)


The [connection recovery receipt](connection-recovery-ownership-execplan.md)
repairs N06 in the existing server. The session, prepared registry and framed
writer now survive command unwind until Go's ERR attempt; transport retirement
precedes session drop. Shared reader/writer sequences include LOCAL INFILE,
TLS and negotiated compression. The outer-only recovery substitute is removed
from command handling; setup/cleanup retain worker containment.

Two authenticated command/partial-result regressions fail before repair.
Five final tests cover 25 cases, and 37 affected admission/protocol/shutdown
cases pass. All-target compilation, formatting and root lint pass. The focused
receipt records the mandatory commit-hook and fresh pre-push locked builds.
The register is 86 tracked, 71 unresolved (65 open/six partial), 15 repaired.
Client-rust and its dependency remain current at 19a56cc. No complete Go
pkg/server acceptance, cluster correctness sweep or benchmark gain is claimed.


## Generated mutation ownership (2026-10-02)


The [generated-write receipt](generated-write-ownership-execplan.md) partially
repairs K03 in the existing executor. Generated values use ordinary-column
statement conversion, including INSERT/UPDATE/ODKU's different warning and NULL
ordering. Every existing mutation producer reaches that owner, including FK
cascades before nested changes. Table insert/update's redundant expression pass
and row copies are removed; partition maintenance preserves completed rows.

Nine new session regressions fail under unchanged production and pass after the
repair. Full-suite controls retain the same 19 session and 36 executor failures.
The receipt records 43 focused session/cluster-session/decoder passes, all-target
checking, lint and required publication gates. Concurrent remote DDL commits through
e8c7c9211d are integrated before publication, with affected checks repeated.

K03 stays partial because read/DDL/ANALYZE raw conversion has not acquired the
complete Go CastColumnValue contract; shared casting also has documented datatype
and error-identity gaps. The register remains 86 tracked/71 unresolved/15 repaired,
now 64 open and seven partial. Native client-rust is current and unchanged at
19a56cc. This is existing-owner maintenance, with no complete Go package
acceptance or measured sysbench/TPC-C/TPC-H/YCSB gain.


## Generated read and backfill conversion ownership (2026-10-02)


The [generated-read receipt](generated-read-ownership-execplan.md) removes the
remaining generated-column fixed-flags/raw conversion wrappers. Row decoding,
virtual table/point reads and ANALYZE use the shared column cast and complete
caller type flags. Go's virtual-fill clipping/NULL policy is separate from
rowDecoder/reorg. Cast and evaluation diagnostics survive decoder, DDL and
executor boundaries; union scan retains typed cast errors too.

Eight new regressions and two strengthened existing checks fail on unchanged
production and pass with the repair. Full controls retain the same 36 executor
and 19 session failures. The receipt records focused tests, lint, all-target
checking and publication gates, including the concurrent loader integration.
K03 remains partial for lower datatype error/value identities, legacy ENUM/SET
context and ANALYZE diagnostic transport. Counts remain 86 tracked, 71 unresolved
(64 open/seven partial), 15 repaired. No complete package acceptance or measured
workload speedup is claimed. Native client-rust and its dependency are current
and unchanged at 19a56cc.


## Generated execution error ownership through ANALYZE (2026-10-02)


The [ANALYZE error receipt](analyze-error-ownership-execplan.md) removes the string
adapters from generated sample evaluation, local ANALYZE scans, cluster results
and remote virtual-row projection. Existing DriverError/ExecError retain code,
SQLSTATE and diagnostic until delivery. Failed jobs retain the rendered error;
failed samples do not publish histograms, and the session can analyze again.

K03 remains partial for lower conversion identities, generated-expression
context and legacy ENUM/SET policy. Other ANALYZE storage/build diagnostics are
still generic. The receipt records regressions, affected checks, baseline
controls and publication gates. Counts remain 86 tracked, 71 unresolved (64 open,
seven partial), 15 repaired. Native client-rust remains current and unchanged at
19a56cc. No complete Go package or workload-performance acceptance is claimed.

### Cloud test hygiene and empty-account prerequisite, 2026-10-02

The [cloud review](parity/current-audit/cloud-account-test-review.md) compares all
66 carried findings against prior source hashes (44 unchanged evidence, 22 changed
owner references). No entire unresolved finding closes. A02 advances to partial
for empty-account preservation across loading/matching/writeback; policy state
and A04 history remain open. A stale discard test is removed with its incorrect
production behavior, three parser API-stale calls are corrected, and a valid Go
EOF diagnostic mismatch is repaired without weakening original assertions.
Current counts are 66 unresolved (56 open, ten partial), 20 repaired, 86 tracked.
This maintains existing owners; no complete Go package or benchmark is accepted.

### Account expiry durability milestone


The [cloud expiry repair](parity/current-audit/account-expiry-durability-repair.md)
maintains the existing account loader, shared privilege record and mutation owner.
Stored nullable lifetime and original timestamp survive reload and writeback;
only password changes refresh the epoch. A02 remains partial for TLS/locking/
attributes and A04 history remains open. Counts stay 66 unresolved (56 open,
ten partial), 20 repaired. This is existing owner maintenance, not package
acceptance or performance evidence.

### Shared account history and locking image, 2026-10-03

Fresh Go master remains 93a01d31f6; native remains 19a56cc. Concurrent integration
schema-acknowledgement commits through ee637c3a39 were merged with local account
prerequisites. The existing account transaction/registry/storage owners now carry
nullable reuse limits, timestamped history and raw attributes. Reuse fails before
credential publication, including multi-account rollback; CREATE/ALTER/SET/DROP/
RENAME compose that owner. Locking counts and original epochs survive unrelated
writes, while unlock/removal follows existing Go policy.

A04's recorded no-op finding is repaired. A02 remains partial for global_priv/TLS,
durable wire-login counters, secondary-authentication consumers and broader cache
invalidation. Counts are 65 unresolved (55 open, ten partial), 21 repaired, 86
tracked. Go-based test maintenance preserves valid regressions and removes stale
expectations; real ALL role-expansion and synthetic-schema visibility defects
are repaired through shared owners. See current-audit/account-history-locking-
repair.md and account-history-locking-validation.json for gates and limits. This
maintenance batch accepts no complete executor/privilege package, platform or
mixed-node security contract and claims no workload performance improvement.


### 2026-10-03: shared cache batch closes four existing findings

Starting at b38a25eb0e and preserving concurrent MDL-default commit 7b991676da,
with unchanged freshly fetched Go master 93a01d31f6 and native 19a56cc, the
[batch receipt](parity/current-audit/shared-cache-batch-repair.md) repairs B01,
B02, C03 and C04. Ristretto owns the common admission/publication/lifetime;
bindings retain one live owner with incremental reload, owner GC and usage
writes; coprocessor construction uses effective optional configuration; LFU
preserves fallback identity/accounting and both original dependency regressions
pass. Stretto, both FIFO stores and write-triggered binding GC are removed.
The stale negative-ID panic test is retired and FIFO/whole-image test assumptions
are corrected to Go's source contract. Counts: 61 unresolved (52 open/nine
partial), 25 repaired, 86 tracked. C02, X02 and broader parent-package obligations
remain distinct. Detailed checks and publication state are in the validation JSON.


## DML policy maintenance checkpoint, 2026-10-03

The [DML batch ExecPlan](dml-policy-batch-execplan.md) maintains E03 and T01 together against freshly fetched master 93a01d31f6. Both are partial; 58 unresolved (44 open, fourteen partial), 28 repaired remain. The intermediate physical datum matrix and generic insertion policy are removed after caller migration. Complete planner/handle metadata, final joined buffering, matrix interpretation, system-index uniqueness and pessimistic constraint checks remain. This is existing-owner maintenance, not complete package acceptance. User no-push remains binding. The [receipt](parity/current-audit/dml-policy-batch-repair.md) records focused and baseline validation without certifying the twelve pre-existing driver failures.

Final connected DML/SET checkpoint: E03/T01/N03 remain partial after eleven behavioral fail-before/pass-after cases, shared statement retirement/recovery and canonical GC-trigger/packet validation. Harmful separate-helper, classic-kernel trace-event and outdated query-info expectations are corrected against current Go. 698 distinct Rust cases pass; seventeen controlled baseline failures remain visible. Affected all-target checks, lint, actual source-commit locked-build hooks and real MySQL/unistore cancellation/recovery plus normal-quota controls pass. The recovery bundle and cloud draft retain the unpublished local handoff; no whole-package or multi-node/performance acceptance is granted.


## Statement observation checkpoint, 2026-10-03

The [observation ExecPlan](statement-observation-batch-execplan.md) advances O18/O11/N03 together through the existing SummaryStmt gate, real SQL summary/counter publication, routed durable completion and persistent startup/fallback/readers/flush. The current register has 58 unresolved (42 open, sixteen partial), 28 repaired and 86 tracked. Plan/phase/RPC/network/RU/CPU attribution and complete TopSQL registration/profiling/transport remain unresolved; empty measurements are not behavioral acceptance. Three harmful historical schema/order assertions are corrected and the injected cumulative-summary fixture is replaced by real SQL. The [durable receipt](parity/current-audit/statement-observation-batch-repair.md) records the actual validation and local commit gate. No push or whole-package acceptance is authorized by this checkpoint.


## Shared MPP/process lifecycle checkpoint, 2026-10-04

The [shared MPP lifecycle batch](parity/current-audit/shared-mpp-lifecycle-repair.md) advances **M04/N03/T02 together**: one process fleet, generation retirement/joined close, and actual PD/store security bootstrap. Five runtime baseline failures and 58 distinct passing Rust cases validate this maintenance. **86 tracked, 29 repaired, 57 unresolved (40 open, 17 partial)** remain; other 54 IDs retain previous evidence. No whole-package acceptance or push.



## Authentication durability checkpoint, 2026-10-04


The [authentication durability plan](auth-durability-batch-execplan.md) advances A02/A03/N03 through shared login policy, pooled pessimistic SQL persistence and post-commit lock publication, plus canonical generated TLS material and renewal. Seven corrected real-server checks fail before repair;164 grouped Rust cases and18 MySQL/unistore assertions pass after repair. The duplicated configured-account source harness and in-memory-only TLS resolution paths are removed after caller migration. Counts remain 86 tracked,29 repaired,57 unresolved(36 open,21 partial); parent package and mixed-node/platform obligations remain. [Exact validation](parity/current-audit/auth-durability-batch-validation.json) preserves failures and boundaries. No push.

## Observed-plan maintenance checkpoint, 2026-10-04

The [observed-plan batch](observation-plan-batch-execplan.md) advances existing O11/O18/N03 owners together: shared textual/binary plan samples, prepared process-list separation, live GLOBAL binary-plan policy, runtime TopSQL/fast-plan admission and Go SET labels. Full package, normalized digest/hint/runtime/scalar-subquery capture, TopRU and profiling transport acceptance remain open. Counts remain 86 tracked, 29 repaired, 57 unresolved (35 open, 22 partial); other 54 unresolved roots are carried evidence. Preserve remote schema-ack changes at 39cf68700fac483512b22ce68e5d900ab6d866ea without overwriting local work. No push.

Snapshot selection continuation: [plan](snapshot-validation-batch-execplan.md) and [receipt](parity/current-audit/snapshot-validation-batch-validation.json) track the shared SET/GC/schema/typed-policy repair. Named parents remain partial; the register owns current dispositions.

Comment-only harness cleanup continues the existing [test-build cleanup plan](test-build-cleanup-execplan.md). The [receipt](parity/current-audit/comment-test-cleanup-validation.json) retains exact retired identities and recovery coordinates; no behavioral root or complete package is accepted by this removal.

The [aggregate-model cleanup](parity/current-audit/aggregate-leaf-cleanup-validation.json) removes unused JSON/percentile models after tracing their only test callers and moving distinctive vectors to the live HashAgg owners. Package and root acceptance remain unchanged.

The shared snapshot continuation maintains S04, I04 and O09 together through
statement-only storage/catalog overrides, cursor timestamp retention and bounded
publication caching. See [the current plan](schema-snapshot-batch-execplan.md)
and [validation](parity/current-audit/schema-snapshot-batch-validation.json).
These are existing-owner repairs; complete package and lazy V2/GC obligations remain open.

## Deferred pessimistic uniqueness batch

T01/N03 share setting selection, table/native flags, per-key locking, successful-stage preservation, savepoint refusal and terminal DML abort. The [batch plan](deferred-uniqueness-batch-execplan.md) and [validation](parity/current-audit/deferred-uniqueness-batch-validation.json) record 63 distinct Rust cases and 23 TCP assertions passing after three baseline Rust and thirteen TCP failures. Both parents remain partial; counts remain 86 tracked, 30 repaired and 56 unresolved. Native pins are unchanged. Full packages, multi-node TiKV and performance remain unverified.


## PD endpoint-channel ownership batch


P03/P06 now share native and adapter metadata, keyspace, discovery and TSO channels through one endpoint owner. The adapter removes its private discovery runtime; the process runtime drives idle discovery and joins the task on close. The [batch plan](pd-channel-batch-execplan.md) and [validation](parity/current-audit/pd-channel-batch-validation.json) retain four baseline socket-count failures and final gates. Complete package, follower/forwarding, health, live-cluster and performance obligations remain unaccepted. Counts stay 86 tracked, 30 repaired and 56 unresolved. Concurrent integration commit f29d961bad is preserved.


## Snapshot read-policy batch


The [snapshot plan](snapshot-read-policy-batch-execplan.md) and [receipt](parity/current-audit/snapshot-read-policy-batch-validation.json) connect O13/N03 SQL policy, retained point estimates, deferred/MaxTS/explicit/prepared lifetimes and native timeout/resource groups. Native read overrides no longer replace prewrite/commit groups. T02 now has a concrete leader-only adapter fallback to repair at the shared routing owner. All three parents remain partial; counts stay 86 tracked, 30 repaired and 56 unresolved. No full-package, live multi-node or performance acceptance is claimed.

The 2026-10-05 [internal-process batch](internal-process-batch-execplan.md) connects I01/N04/O09 through shared session entries, scoped system-task publication and cancellation, and leased internal timestamp diagnostics. See [receipt](parity/current-audit/internal-process-batch-validation.json). All three parents remain partial; no package acceptance or GC activation. Counts remain 86 tracked /30 repaired /56 unresolved; other 53 unresolved roots are not freshly re-audited.

The 2026-10-05 [typed user-variable batch](user-variable-batch-execplan.md) connects S03/X01 through one session value/type owner, plan-time inline type publication, retained SET plans and independent migration maps. The duplicate map, value-derived read/type reconstruction and duplicate integration harness are removed. See [receipt](parity/current-audit/user-variable-batch-validation.json). Both parents remain partial; counts remain 86 tracked /30 repaired /56 unresolved. Other54 unresolved roots were not freshly reproduced.

## JSON result and expression-index checkpoint — 2026-10-06

Existing-owner X01/K03 maintenance joins JSON value/type signatures, byte-sized
ESCAPE errors, JSON column charset identity and CREATE TABLE hidden-column
construction through shared owners. See [the batch plan](json-result-batch-execplan.md)
and [validation](parity/current-audit/json-result-batch-validation.json).
211 Rust cases and fourteen TCP checks pass; five historical DDL diagnostics
remain unchanged. Counts stay 86 tracked /30 repaired /56 unresolved; no whole
Go package or performance acceptance is claimed.


## Statistics lifecycle checkpoint — 2026-10-06


The [statistics plan](statistics-ndv-batch-execplan.md) connects four current-Go gaps across collection, schema admission, three partition merge consumers, JSON, canonical tables and durable sketch writes. [Validation](parity/current-audit/statistics-ndv-batch-validation.json) records five baseline Rust failures and three baseline TCP failures, then 220 Rust and 15 TCP passes. Duplicate in-process global sketch merging is removed. Full package acceptance, live multi-node TiKV and performance remain unverified; the 56 broader unresolved roots keep their existing dispositions.


## PD region lifecycle checkpoint — 2026-10-06

The [PD region plan](pd-region-batch-execplan.md) and [receipt](parity/current-audit/pd-region-batch-validation.json) connect native selection/metadata, both transports, cache flags/stale-response fallback, TiDB bridge and live SQL startup/mutation/reload policy. Native fixes are published then synchronized through maintained patches and protobuf regeneration. Complete discovery health/forwarding, TSO proxying, shared cache ownership and remaining settings stay open; no whole Go package is accepted. The other 53 unresolved roots are carried evidence.


## PD service availability checkpoint — 2026-10-06

The [availability plan](pd-availability-batch-execplan.md) and
[receipt](parity/current-audit/pd-availability-batch-validation.json) compose
network health, ten-second region cooldown, topology-safe feedback and joined
maintenance in native and TiDB. Native fixes are published then synchronized
with the maintained codec patch and protobuf regeneration. P03/P06/T02 remain
partial for their retained broader obligations; no complete package acceptance.

The [duplicate-test cleanup](duplicate-test-cleanup-execplan.md) removes repeated behavioral cases across seven crates. Its [mapping and validation receipt](parity/current-audit/duplicate-test-cleanup-validation.json) identifies each retained owner; no root or complete package is newly accepted.


## TSO failure/recovery continuation — 2026-10-07

The connected B01/B05 P03/P06/N03 maintenance batch uses one native feedback and routing owner across both clients. Native 8b752f9638ad157931725b66ffdc57e0465432a9 is synchronized through all maintained patches and protobuf regeneration. Three original failures precede 370 selected passes; an intermediate response-phase classification error was caught and corrected. Both registers remain 30 repaired/56 unresolved; see parity/current-audit/tso-failure-batch-validation.json and tso-failure-batch-execplan.md for exact gates, source boundary and remaining timing/readiness obligations. Actual hook, fresh locked prepush builds and remote verification remain publication gates.


## DML writable-row checkpoint — 2026-10-07


The [batch plan](dml-identity-batch-execplan.md) and [receipt](parity/current-audit/dml-identity-batch-validation.json) connect B02 E02/E03/K03 through complete retained writable rows and explicit candidate handles. Joined hidden-column UPDATE/DELETE, generation, REPLACE/IGNORE/ODKU and FK callbacks share the repaired layout. Three original session regressions, a further index-join regression and an independent original-server REPLACE failure precede 286 Rust passes and 20 TCP passes. Partial-row fallback rereads are removed; speculative partition expectations are excluded because Go uses the same bare-handle maps. No complete package acceptance; counts remain 30 repaired/56 unresolved.

## Shared DML contract follow-up


The [connected plan](shared-dml-contract-batch-execplan.md) and [receipt](parity/current-audit/shared-dml-contract-batch-validation.json) maintain E02/E03/K03 together. Ten original regressions and 209 retained passes cover SQL-visible heap handles versus writable hidden tails, Go’s partitioned-heap UPDATE exception, and shared IGNORE FK checks before bad-NULL substitution. The old claim that enabled ordinary heap-handle assignment must move the row is corrected by current Go’s writable ranges. Earlier UPDATE partition dismissal is superseded; DELETE remains separate. Broader streaming, indexed FK/physical cascades, conversions and complete packages remain unaccepted; counts are unchanged. Final publication and Cloud checkpoint evidence is external to the committed receipt.


## Index lifecycle continuation — 2026-10-07


Latest connected repair: [index evaluation and removal](parity/current-audit/index-lifecycle-batch-validation.json). Seven baseline assertions failed (five wire, two Rust); 107 selected Rust tests and 6 real MySQL assertions pass after repair. Index creation preserves SQL mode/timezone and typed errors; removal uses physical ranges, including implicit indexes removed with columns. D02 stays open; K03/D11 stay partial for broader durable/package obligations. Counts remain86 tracked/30 repaired/56 unresolved (27open,29partial). The [plan](index-lifecycle-batch-execplan.md) records connected production migration and rollback coverage. Shared statement/table owners replace default contexts, string error flattening and row-based index deletion.
