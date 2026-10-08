# Complete Go/Rust structural parity through shared owners


This living ExecPlan follows root [PLANS.md](../../PLANS.md). Maintain Progress,
Surprises & Discoveries, Decision Log and Outcomes & Retrospective as work lands.
The [exact prior plan](https://github.com/pingcap/tidb/blob/9a319a5d6d78593e623a9db8e8aed1f750380507/rust/docs/full-structural-parity-execplan.md)
preserves all dated decisions, commands, failures and receipts. Its old counts,
laptop paths and residual descriptions are historical, not startup instructions.

## Purpose and acceptance


Make Rust TiDB and native client-rust follow current TiDB Go master and the
client-go/PD versions selected by that master's go.mod. SQL results, warnings,
errors, authorization, transaction isolation, failure recovery, configuration
and resource lifetimes must match. Improve workloads only with semantic parity
and measured evidence. Preserve native Rust representation and memory safety.

An owner is the code responsible for state, transitions and lifetime. A batch
moves a connected owner and its producers/consumers together. One complete Go
package remains the minimum transcreation and acceptance unit: include every
production source, original test/support artifact, fixture, generated input,
build/platform variant and required gate. Multiple Rust crates may implement
one package, but it retains one inventory, integration decision and receipt.
Partial maintenance never accepts the enclosing package.

The [finding register](parity/current-audit/structural-findings.json) records
86 findings: 30 repaired and 56 unresolved (27 open, 29 partial). The
[batch map](parity/current-audit/remaining-batches.md) assigns every unresolved
ID once across B01–B10. Counts describe the known register, not exhaustive
semantic coverage. [README.md](parity/current-audit/README.md) indexes current
and historical evidence; consult each receipt's actual limits.

## Progress

- [x] Consolidate P03/P06 retained TSO streams and completion/order ownership in both consumers; [batch plan](tso-completion-execplan.md) and [receipt](parity/current-audit/tso-completion-validation.json) track validation and publication.

- [x] Maintain P03/P06 discovery observations, primaryless revision floors and stale-callee cache lookup retirement together; see [plan](pd-observation-execplan.md) and [receipt](parity/current-audit/pd-observation-validation.json). Complete transport/discovery packages remain partial; 56 broader findings remain unresolved.

- [x] Maintain N03/D11/E02 together through shared FK policy, metadata versions and persistent admission; see [current plan](fk-global-policy-execplan.md) and [receipt](parity/current-audit/fk-global-policy-validation.json). Broad counts remain56 unresolved; complete package/durable lifecycle obligations remain open.

- [x] Restore clean Cloud checkouts at TiDB `9a319a5d6d78593e623a9db8e8aed1f750380507` and native `aa2c60f37481c2fe7f03997535d4f238ed485956`; refresh Go master at `7a3dacb52efe58d28db360ae8639d8838c376544`.
- [x] Retain the latest [rename admission/publication repair](parity/current-audit/rename-owner-batch-validation.json) and [executor scaffolding cleanup](parity/current-audit/executor-scaffold-cleanup-validation.json), without expanding their acceptance claims.
- [x] Replace duplicated planning histories and stale startup directions with this plan, the current batch allocation and immutable archives. Verification is recorded in [planning cleanup](parity/current-audit/planning-history-cleanup-validation.json).
- [ ] Complete B01 native PD/transport and all TiDB consumer ownership.
- [ ] Complete coupled B02 SQL/table, B03 schema/identity/timestamp and B04 durable DDL lifecycles.
- [ ] Complete B05 security/configuration, B06 typed execution, B07 MPP, B08 service runtimes, B09 observability and B10 inference with their actual prerequisites.
- [ ] Accept every inventoried package and variant, including packages without known findings; verify distributed interoperability and measured workloads.

## Context and orientation


All source edits, builds and validation run in Cloud. Use `/workspace/tidb` on
`hparser-integration` and `/workspace/client-rust` on `master`. Preserve concurrent
changes and local commits. Do not create competing worktrees or implementation
continuations. Read root/deeper AGENTS.md and the selected package's doc.go first.
Inspect status, branches, HEADs and ancestry before refreshing or integrating refs.
Never reset another contributor's work or force-push.

Go comparison sources are kept separately at `/workspace/.cloud-setup/go-master`;
verify their recorded pin before using them. Fetch `origin/master` independently
of the editable integration branch. Read external versions from that master's
go.mod, not integration's older Go files or a stale oracle cache. At this
checkpoint client-go is `v2.0.8-0.20260928031501-8edb23f6c7ee` and PD is
`afa43111d149`; derive replacements when master advances.

The execution chain is protocol → Session → preprocessing/resolved planning →
executor Open/Next/Close → table/KV/native storage. Domain owns shared schema,
statistics, privileges and service lifetimes. SQL DDL submits persisted jobs
and waits; workers own metadata transactions, reorganization and schema barriers.
Go `pkg/session/session.go::executeStmtImpl`, `pkg/executor/compiler.go::Compile`
and `pkg/sessiontxn/interface.go` define the shared context and retry hooks.
Keep text, prepared, migrated and internal sessions on those same contracts.

Rust counterparts are under `rust/crates/tidb-server`, `tidb-session`,
`tidb-planner`, `tidb-executor`, `tidb-exec`, `tidb-domain`, `tidb-txnkv` and
`tidb-pd-client`. Native ownership lives in client-rust `src/pd`, `src/region.rs`
and `src/transaction`. Confirm exact current callers from source and the register;
do not revive alternate interpreters, private caches or disabled seeds because
an archived plan once named them.

## Milestones and plan of work


### 1. Establish the selected ownership boundary


Choose multiple related findings from the current batch map and recheck their
live Go/Rust producers, consumers, configuration and failure paths. Gather the
complete package/dependency inventory before editing. Classify retained code as
reusable, requiring replacement, or explicitly unaccepted seed. Record changed
source pins and invalidate stale receipts; keyword matches alone are not evidence.
Read original tests and fixtures before designing regressions. Package inventories
and coverage are maintained by `rust/scripts/inventory-go-rust-parity.py` and
`rust/scripts/build-structural-coverage.py`; run them when their inputs change,
not as an automatic test for every cleanup. Generation does not certify semantics.

Acceptance of this milestone is an explicit owner/caller/removal map and a
combined regression scenario that exposes the selected mismatch. A helper
bypassed by a live entrypoint cannot satisfy it. Keep original generated and
platform/build obligations visible even when not runnable in this environment.

### 2. Migrate shared lifecycles and retire competing implementations


Start with B01 pinned PD root, TSO, discovery, grpcutil and client-go routing/RPC
owners. Cover service-mode changes, deadlines, connection/stream ownership,
retry exhaustion, health feedback, cancellation and joined shutdown. Migrate
ordinary, coprocessor and MPP consumers before removing competing transport or
caches. Native fixes publish first and enter TiDB through maintained sync.

B02 SQL/table execution carries resolved privileges, FK plans, physical handles,
mutation context and chunk accounting through all callers. Keep Go's distinct
SQL transaction manager, native KV transaction, table assertion policy and
per-consumer caches. Preserve shared planner candidate lifecycles and original
candidate rules. B03 schema/identity/timestamp ownership and B04 durable DDL
must integrate together: protect active transactions, cursors and internal
sessions before MVCC GC, including when a Go peer already runs GC. Durable
delete-range registration and consumption gate corresponding cleanup.

SQL DDL must share submission, scheduling, pause/cancel/error/rollback,
reorganization, schema barriers and completion. Preserve MDL and lease modes,
partition identity and multi-action atomicity. Keep unsafe REORGANIZE, private
IMPORT shortcuts and incomplete materialized-view actions disabled until their
complete owners exist. Placement follows source DDL/GC ownership, with NextGen
refresh only where Go defines it. Identity gates remote KILL and distributed jobs.

Independent B05 account, charset and configuration owners need not wait for
unrelated optimization. Preserve durable policy, certificate verification/reload,
password history and input bytes. B06 typed SQL/PB construction and optimizer
execution supply B07 MPP and B10 inference consumers. B08 resource/job runtimes
precede bulk/history consumers. B09 observability must consume real production
events and state. Retain role/configuration gates, recovery and joined close;
a registered metric or injectable fake does not prove runtime integration.
Shared caches retain their consumer-specific budgets and lifetimes.

At the milestone boundary every displaced implementation has no remaining live
caller, and grouped before/after regressions prove the source contract. Do not
weaken useful tests to hide failures. Remove only redundant or obsolete tests,
harnesses and documents with recorded caller/coverage or archival evidence.

### 3. Validate complete packages and workload behavior


Run the selected owner regressions together, then required original-package,
generated/platform/build and integration gates before accepting a package.
Exercise failures after partial work, ambiguous RPC completion, retries,
cancellation, close and restart where applicable. Distinguish passed, failed,
ignored and unrun outcomes. Update both registers and the batch receipt; partial
maintenance leaves parent findings/packages open. Do not rerun broad optional
sweeps unless a change, failure or unresolved concern justifies them.

Distributed acceptance covers Rust-only and mixed Go/Rust clusters: authorization,
schema/DDL recovery, timestamp protection, owner failover, routing and configured
services. A Go peer must not hide a missing Rust background responsibility.
The final coverage review includes packages without registered findings.

For performance, compare matched Go/previous-Rust/new-Rust release builds,
hardware, topology, dataset, statistics, caches, security, isolation, quotas and
concurrency. Record versions, seeds, throughput, latency, CPU/RSS, errors/retries
and first-row/network/memory behavior. Repeat baselines to quantify noise.
`compare-sysbench.py` assumes prepared comparable servers/data. `compare-tpcc.py`
does not restore mutating datasets between rounds; verified reset orchestration
is required. TPC-H needs all 22 result checks and isolated setup/cleanup (the old
SF50 report includes a q15 view-lifecycle race). Pin a real SQL YCSB tool/binding
before naming an invocation. Historical reports and raw KV numbers are not
current TiDB SQL performance evidence. Do not silently omit unsupported queries.

## Concrete steps and validation commands


Activate the existing tools in every shell:

    source /workspace/.cloud-setup/env.sh
    export CARGO_BUILD_JOBS=1
    cd /workspace/tidb/rust

This supplies nightly-2026-08-22, Go 1.25.14, protoc 35.1 and the shared target.
Run TiDB Cargo from `rust/` so `.cargo` configuration applies; invoking only
`--manifest-path` from root is not equivalent. Substitute actual selected crate,
target and filters in these templates; batch related filters in one invocation:

    cargo test --locked -p <crate> --lib -- <filter_a> <filter_b> --test-threads=1
    cargo test --locked -p <crate> --test all -- <module_a> <module_b> --test-threads=1
    cargo check --locked -p <crate> --all-targets

Use existing aggregate targets documented in [scripts/README.md](../scripts/README.md).
The manual `cluster-session-smoke` target requires `--features diagnostics`;
ordinary server builds must not recreate it. Native Cargo runs in
`/workspace/client-rust` with `--config /workspace/.cloud-setup/native-cargo.toml`,
`--locked` and the same shared target. Select its affected tests/checks from the
maintained native workflow; preserve its resolved lockfile.

Original Go tests run against the exact comparison pin and selected external
versions, not the integration Go tree or a modified module cache. Follow
[testing-flow.md](../../docs/agents/testing-flow.md) for failpoints, real TiKV
playground cleanup and integration recording. Apply `make bazel_prepare` only
when root AGENTS.md's Go/Bazel/module/source/test-target conditions require it.
At a code-complete batch boundary, run `make lint` from `/workspace/tidb` and
`git diff --check`. Documentation-only cleanup needs link/archive/consistency
checks and publication gates; do not invent a behavioral test or broad sweep.

## Publication, idempotence and recovery


The user authorizes normal pushes to `pingcap/tidb:hparser-integration` and
`ngaut/client-rust:master`. Use the managed HTTPS proxy; preserve destinations
and never extract credentials. Read access or account metadata alone does not
prove write permission. Report an actual blocked push accurately and continue
independent work. Never bypass hooks or force-push.

Validated native repairs publish before TiDB synchronization. Immediately before
every push, including native publication, run the locked TiDB server build from
`/workspace/tidb/rust`. After native publication, run from TiDB root:

    bash rust/scripts/sync-tikv-client-rs.sh

Inspect the recorded native SHA, compatibility patches and regenerated protobufs;
a failed patch requires reconciliation. Never hand-copy vendored/generated code.
Run affected integrated gates after sync. Stage only reviewed files. The actual
executable `hooks/pre-commit`, selected by `core.hooksPath=hooks`, must run
`cd rust && cargo build --locked -p tidb-server` for every Rust commit, including
documentation-only changes. After the final commit, immediately before pushing:

    cd /workspace/tidb
    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:refs/heads/hparser-integration
    git ls-remote origin refs/heads/hparser-integration
    git status --short

Require remote SHA equality and a clean or accurately explained tree. A rejected
concurrent update requires inspection and safe integration, then affected gates
again; never overwrite it. Recover published mistakes with a reviewed revert,
considering durable-state compatibility. Preserve failures and exact logs.

Disk is constrained. Reuse caches and remove only identified inactive regeneratable
artifacts with recorded outcomes; never run `cargo clean`, purge dependencies,
freeze embedded Git version metadata or discard work/evidence to shorten gates.
Refresh the recovery bundle outside checkout roots and the reusable Cloud draft
with complete checkout discovery. Saving a draft, user Publish and fresh-task
restoration are distinct; claim each only when verified. Runtime services must
restart using saved instructions, not presumed surviving processes.

## Surprises & Discoveries


The previous 2,725-line plan mixed active workflow with old 77-finding counts,
retired entrypoints and laptop commands. The current register has 56 unresolved
findings, and Cloud Cargo requires the `rust/` working directory. Repeating that
history at startup creates conflicting instructions without adding validation.
Exact prior bytes remain in Git archives, with checksums in the cleanup receipt.

## Decision Log


Decision (2026-10-07, America/Los_Angeles): retain one current allocation and
workflow, with dated implementation detail in existing receipts and immutable
archives. Preserve all complete-package, ownership, regression and publication
requirements. Historical descriptions of already-repaired findings are not a
new defect queue, and absence of a same-named Go test alone does not make a Rust
correctness regression useless.

## Outcomes & Retrospective


The latest TSO batch consolidates retained stream and timestamp completion owners
across native and TiDB callers. Native76 and adapter76 selected Rust tests pass,
as do affected all-target checks and repository lint. Native publication and
maintained synchronization are verified; the actual TiDB hook and postcommit
publication outcomes are recorded in the external final handoff named by the
[current receipt](parity/current-audit/tso-completion-validation.json). P03/P06
remain partial and other54 roots retain prior evidence. No complete package,
live distributed behavior or performance result is newly accepted.

Revision note: replace copied history with current Cloud instructions and the
B01–B10 queue; retain exact earlier plans for evidence and recovery.
