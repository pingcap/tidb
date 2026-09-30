# Audit and remove Go/Rust structural mismatches

This is a living ExecPlan under root PLANS.md. Maintain Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective. The existing storage plan retains earlier repair receipts.

## Purpose and acceptance


The user requests every mismatch to be listed and removed, following TiDB Go master and its pinned client-go. Exhaustive coverage means every production source, platform/build variant, generated input, original test, fixture and support/build artifact in each owning Go package. A search hit or passing subset is not package acceptance. The rolling source starts at master 6b2781326b722f217a61852ab403350858549bd0 and integration 5503f8860883c6cd80bdd0d487d34c53787daf24. Native client-rust master is 884589f0365053c0f5bd300209751187a4811782. No new SQL features absent from Go are authorized.

## Progress


- [x] Pull integration and refresh master; clean initial tree.
- [x] Enumerate upstream scope: 856 Go package directories, 4,420 Go source/test files; 83 Rust crate manifests.
- [x] Inventory all tracked TiDB and pinned client-go artifacts and Rust gap candidates; classify confirmed mismatches separately from unreviewed evidence.
- [x] Replace partial TiPB schema ownership with the complete pinned external package inputs, generation and drift gate; validate original Go tests and Rust consumers.
- [x] Repair MPP statement/query/gather/task identity and carry the existing server-info identity; 98 targeted tests, lint, hook and fresh pre-push locked builds passed; published as 7d8d69b6a0.
- [x] Remove duplicate TiFlash poller startup, detached lifetime and private DDL publisher; validate the shared owner and HTTP consumers.
- [ ] Reconcile generic insertion policy with the ordinary table owner.
- [ ] Reconcile remaining native routing/RPC and operation-lifetime owners.
- [ ] Resolve each confirmed baseline SQL/DDL/statistics failure at its owning package.
- [ ] Audit all remaining package source, variants, original tests, fixtures and integration paths; retain unreviewed status until complete.
- [ ] Run scope-specific validation, root lint, commit hook locked server build, fresh pre-push locked build; commit and push each reviewed package repair.

## Milestones and design


First produce a machine-readable package coverage inventory and a searchable candidate list. Candidate strings such as unsupported or go-parity-gap include valid Go errors and historical comments; they are evidence to review, never an automatic defect count. Keep confirmed findings with concrete Go/Rust source and validation evidence. Historical receipts cannot certify current master without rechecking changed package inputs.

The first complete dependency boundary is github.com/pingcap/tipb/go-tipb. Current Rust duplicates selected messages in four local files and compares them to this branch's older June go.mod. Master pins September fed7bc47c39d; missing messages and fields escape the one-sided comparison. Replace those inputs with the complete upstream proto/include files and generate from them. A single synchronization/check command selects the dependency from an explicit Go master revision, records the complete package and generation inputs, and verifies all source bytes/file membership. Generation remains offline from checked-in source. Do not fix only ExecType or keep a second hand-maintained enum list. Preserve native Bytes ownership and protobuf presence semantics at consumers. Translate the upstream package's original wire tests and retain existing Rust wire vectors. Any consumer changes must be mechanical adaptations to complete generated contracts, with no new executor support invented.

Next reconcile transaction insertion, statement options and operation lifetime with the actual Go owners; remove only policies whose responsibility has moved to the authoritative owner. Review all routing/cache/transport consumers before changing ownership. The previously rejected /private/tmp/client-rust-background-lifetimes.patch stays unapplied without the specifically requested authorization. Other independently authorized repairs continue.

## Validation and commands


Run regressions before production fixes and afterward. Protocol validation uses complete source checks, Rust wire tests, original go-tipb tests, affected consumer tests, and all-target compilation. Whole-repository completion requires all package coverage rows to have current, complete evidence; no keyword search can establish it. Record exact commands and results as work progresses. Publication uses TERM=xterm git -c core.hooksPath=hooks commit, then a separate cd rust && cargo build --locked -p tidb-server before normal push to hparser-integration. Native changes publish to client-rust master before synchronization. No forced pushes or hook bypasses.

## Surprises & Discoveries


The old testport manifest contains only 45 package mappings and does not describe the current 856-directory Go tree. An initial Rust source search found 2,041 lines matching go-parity-gap, not implemented, not supported yet, or unimplemented!; this is a candidate count, not a mismatch count. Go supports some of those errors itself. The last embedded run has ten failures independently reproduced on unchanged integration HEAD; their names and logs remain in remove-extra-storage-policies-execplan.md.

## Decision Log


Inventory coverage explicitly and implement package-sized owner corrections. Do not promise a complete semantic audit from partial receipts, suppress failing tests, or replace Go policies with broad defaults. Complete generated schemas are the owner of protocol declarations; Rust execution support remains a separately audited consumer.

## Outcomes & Retrospective


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
