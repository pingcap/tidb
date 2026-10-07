# Share temporary DDL target ownership


This living ExecPlan follows repository `PLANS.md`. Maintain Progress, discoveries,
decisions and outcomes while implementing or validating it.

## Purpose / Big Picture


A session-local temporary table can hide a permanent table with the same name.
DDL must resolve that overlay before deciding which metadata owner can execute it.
The batch repairs DROP splitting, grant admission, transaction boundaries and
observation together. Unsupported local ALTER/index/rename operations must return
Go's 8200 error without changing the hidden permanent table. Local-only DROP must
preserve an open transaction; mixed DROP commits before its persistent operation
and removes local targets only after that operation succeeds.

## Progress


- [x] Refresh integration and Go master; inspect current owners and prior receipts.
- [x] Reproduce the connected failures with one real MySQL/unistore baseline run.
- [x] Integrate shared target splitting and every ordinary/routed consumer.
- [x] Run grouped tests, live SQL, affected all-target checks and root lint.
- [x] Update both finding registers and prepare the required publication gates.
  The actual hook and fresh pre-push build results belong to external final-handoff.json.

## Context and Orientation


Editable checkout is `/workspace/tidb`, branch `hparser-integration`, initially
45e4abb96e9c5f027d7e1c80df2c4aadc4094bfd. Fresh Go master is
7a3dacb52efe58d28db360ae8639d8838c376544; comparison sources live at
`/workspace/.cloud-setup/go-master`. Native client master remains
8b752f9638ad157931725b66ffdc57e0465432a9 and needs no edit for this batch.

`tidb-session` owns local table identity and transaction state; `tidb-executor`
owns catalog DROP execution. `tidb-server/src/cluster_session_node/mod.rs` chooses
between local execution and the existing cluster metadata transaction. The latter
is not a durable DDL job owner. Existing A01/D01/O18 findings remain partial/open
for their full planner, job and observation boundaries. No complete Go package is
accepted by maintenance of these existing owners.

## Plan of Work


Expose the existing DROP executor over its parsed AST and share one backward
local-target split between Session routing and catalog execution. Keep original
visits for privileges and observation, pass only persistent names to cluster DDL,
and retire local targets without starting another observed statement. Share Go's
local CREATE/DROP implicit-commit exceptions between both session wrappers.
Resolve local ALTER, CREATE/DROP INDEX and RENAME sources through the ordinary
local refusal owner. Apply GLOBAL DROP's kind check before privileges or commit.

## Milestones


First capture the baseline with an isolated unistore server and two authenticated
connections. Then integrate all producers and consumers before compiling. Finally
validate transaction rollback, hidden-table preservation, partial errors/notes,
prepared targets and summary attribution together, followed by required gates.

## Concrete Steps / Validation and Acceptance


Activate `source /workspace/.cloud-setup/env.sh` in every build shell and use
`CARGO_BUILD_JOBS=1`. Run Cargo from `/workspace/tidb/rust`:

    cargo test --locked -p tidb-session --lib -- tests_temporary_tables tests_grants tests_observation_batch --test-threads=1
    cargo check --locked --all-targets -p tidb-session -p tidb-server -p tidb-executor -p tidb-exec
    cargo build --locked -p tidb-server

From `/workspace/tidb` run `make lint` and:

    PYTHONPATH=/workspace/.cloud-setup/python python3 /workspace/.cloud-setup/temporary-ddl-batch/wire.py

The live regression starts its own isolated server, exercises actual SQL and joins
shutdown. Baseline has 17 failed assertions among31 checks, with clean exit0.
Final checks must all pass; errors and skipped work remain visible in the receipt.
Actual precommit must run the locked server build. Repeat it immediately before
any normal authorized push and verify the remote SHA; never bypass hooks.

## Surprises & Discoveries


Baseline local ALTER, CREATE INDEX, DROP INDEX and RENAME all succeeded against the
hidden permanent object. Local-only DROP committed unrelated persistent writes;
mixed DROP sent missing local names to the cluster and failed to retire locals.
Explicit local mismatches produced multiple notes instead of one joined note.
GLOBAL DROP missed local-kind preprocessing and committed before its wrong error.

## Decision Log


- Decision: share DROP resolution and reuse existing local refusal execution.
  Rationale: modifying only grants would authorize access to the wrong persistent
  object; routing, grants and transaction policy must migrate together.
  Date/Author: 2026-10-07 / Codex.
- Decision: retain the complete Go package acceptance boundary and existing parent
  finding statuses. Remote cluster administration needs an absent RPC owner and
  is not silently replaced by a private HTTP protocol in this batch.
  Date/Author: 2026-10-07 / Codex.

## Idempotence and Recovery


Preserve concurrent changes and never force-push. Tests use isolated catalogs and
owned servers. Reuse dependency caches; full server builds link two executables
requiring approximately1GiB headroom. Retire only proven inactive owned executables
with hash/process receipts, not library caches. Mutation helper scripts outside the
repository are one-use tools and must not be replayed.

## Outcomes & Retrospective


Seventeen baseline live assertions are repaired. 196 grouped Rust tests and 51
live MySQL/unistore checks pass, along with affected all-target checks, root lint
and the locked server build. Actual hook and fresh pre-push gates are recorded in
the external final handoff after commit. Full Go suites, multi-node TiKV, complete
package acceptance and performance remain unverified.

## Interfaces and Artifacts


`split_drop_table_targets` accepts a DROP AST and resolver and returns persistent
AST plus reversed local names. `run_drop_table_stmt_in` is its catalog execution
consumer. Session exposes matching resolution, commit policy and local completion.
The cluster wrapper retains the original AST for admission/summary and holds the
persistent command plus local names until successful completion. Evidence is kept
under `/workspace/.cloud-setup/temporary-ddl-batch` and the committed validation
receipt in `rust/docs/parity/current-audit`.


## Identity prerequisite discovered during live validation


The first integrated run passed43 of44 assertions and exposed a real collision:
limited_local returned66 from an unrelated persistent table instead of its55.
Go temptable.newTemporaryTableFromTableInfo explicitly requires real global IDs;
the process-local Catalog counter cannot substitute for metadata allocation.
CREATE and LIKE now allocate after admission through the existing global-ID
planner in an independent optimistic transaction. Both cluster catalog creation
and reload install that owner. TRUNCATE reserves another ID and replaces local
storage and memory auto-ID allocators while preserving metadata. Embedded catalogs
retain their shared in-process allocator. Retry only9007 write conflicts with a
fresh snapshot and the existing bounded jitter policy; never retry an uncertain
commit. The intermediate failed wire receipt is retained separately. Validation
adds concurrent persistent/local allocation, LIKE rollback and TRUNCATE identity.


Grouped validation checkpoint:196 selected Rust tests passed, zero failed/ignored;
affected all-target checks and repository lint passed. The first added identity
test incorrectly used a temporary LIKE source, which Go rejects; its fixture was
corrected to a persistent source and rerun. That test-fixture correction is not
counted as a product regression. Final live validation subsequently passed51 checks, with clean server exit0.
Publication proceeds through the actual hook and a fresh locked build; its final
commit, remote SHA and draft readback are recorded in external final-handoff.json.
