# Audit and remove Go/Rust structural mismatches

This is a living ExecPlan under root PLANS.md. Maintain Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective. The existing storage plan retains earlier repair receipts.

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
- [ ] Implement and validate the complete pinned Ristretto root package before consumer migration. Remove Stretto and both private FIFO stores only as their whole Go owners and production callers acquire equivalent validated behavior. Keep B01/B02, C03, C04 and inference acceptance separate.
- [x] (2026-10-01, LFU review follow-up) Recheck all five LFU artifacts and inventory all 91 artifacts of pinned Ristretto v0.1.1. Reproduce unguarded public eviction lifetime and fix TriggerEvict/SetCapacity; both are exercised by the passing regression. Add a deterministic primary-admission failure and restore the pressure test's Go Get observation path (still fails). Native owner/parent suites pass 33 tests; two dependency probes remain ignored and unaccepted. The original 10 LFU and 73 Ristretto tests pass with race detection. All-target compilation, lint, formatting and inventory checks pass. Both locked publication builds remain required.
- [x] (2026-10-01, LFU owner review/native repair) Review all five artifacts at current master; reproduce and repair premature Close, fake-table/negative-key classification, and signed-shard drift; remove the exclusive per-access primary mutex/clones and synthetic trigger tables. Map all ten original tests; 19 LFU and 13 parent tests, original Go race suite, all-target compilation and lint pass. Publication still requires both locked server builds.
- [ ] (2026-10-01, C04 external boundary) Replace or accept the complete pinned Ristretto dependency owner. The original low-capacity concurrent workload retains full payloads in Stretto after Wait; its failing native reproduction is explicitly ignored, not accepted. Policy-metric assertions also remain unavailable in its synchronous public API. Do not claim complete LFU package parity.

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
`rust/crates/tidb-ristretto` is a design target, not an existing accepted crate.
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
