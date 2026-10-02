# Remove unaccepted repartition execution until the durable owner is ready

This living ExecPlan follows `PLANS.md`. Maintain Progress, Surprises &
Discoveries, Decision Log, and Outcomes & Retrospective as work proceeds.

## Purpose / Big Picture


Prevent successful SQL from making existing rows invisible. The current
`ALTER TABLE ... PARTITION BY` shortcut changes physical routing without moving
rows or running Go's durable online reorganization. Remove that complete unsafe
shortcut and its thread-local metadata handoff. Repartition remains explicitly
unsupported on these Rust execution paths until the complete Go DDL owner can
be integrated. This is withdrawal of an unaccepted partial implementation,
not a completed Go package port or a closure of structural finding D01.

Go master is `93a01d31f6da205ae4bf376825293903a6899fdb`; starting integration is
`b01f97d6b5dcecb6aa517384efe7fde217862233`. Both refs were fetched before work.
Native client-rust remains `6163ecfc587b248dcbf0e30c1c9d905b4bc5a665` and needs
no change for this removal. User authorization includes removal of displaced
or incorrect implementations and committing/pushing completed work. Root
`AGENTS.md` forbids activating partial ports as accepted Go packages.

## Progress


- [x] Read Go DDL package documentation, the DDL execution map, complete
  jobsubmit production sources and the partition reorganization states; verify
  current callers and previous data-loss diagnostics.
- [x] Confirm no Cargo/rustc process is active and remove the repository's
  rebuildable incremental cache to recover validation disk headroom.
- [x] Add regressions and observe failures before changing production code.
- [x] Remove local/cluster repartition dispatch, its private builder, carrier
  and thread-local metadata; preserve existing ADD/DROP/TRUNCATE behavior.
- [x] Run the same regressions and relevant partition checks after removal.
- [x] Update D01 evidence without closing the durable DDL finding; run lint,
  formatting and register/symbol checks.
- [x] Pass the actual locked-build commit hook and fresh post-commit build.
  Repeat both gates for the final receipt amendment before publication; record
  the final commit and remote verification in the task result.

## Context and source contract


Read `docs/agents/ddl/README.md` and Go `pkg/ddl/doc.go` before DDL edits. Go
`pkg/ddl/partition.go::onReorganizePartition` owns persisted None, DeleteOnly,
WriteOnly, WriteReorganization, DeleteReorganization and Public transitions.
Writers see the required old/new partition and index state. Reorganization
moves records and indexes before read routing changes. Rollback, schema-version
barriers and eventual delete-range cleanup are part of that owner. Go
`pkg/ddl/jobsubmit` allocates and inserts the typed job, including all new
physical IDs, transactionally. The Rust jobsubmit helpers and CHECK worker do
not establish the complete parent DDL ownership.

The complete `pkg/ddl` and dependency artifact scopes are retained in
`rust/docs/parity/current-audit/package-coverage.json` and the workstreams in
`repair-sequence.md`. No source artifact, original test, generated/platform
variant or dependency is accepted by this removal. Future implementation must
use the whole Go package inventory and acceptance receipt; do not activate an
isolated repartition row-copy loop as its replacement.

Current local path: `tidb-executor/src/ddl/alter_table.rs` parses actions, then
`repartition_partition_action` builds a shell CREATE with empty index/handle
metadata and calls `set_partition` with new IDs. Current cluster path:
`tidb-exec/src/cluster_ddl.rs` lowers a `RepartitionPartitions` carrier and routes
it through ADD/DROP planning, with incorrect metadata/ID assumptions. Both use
or feed the newly introduced `LAST_BUILT_METADATA` thread-local in
`tidb-executor/src/ddl/table_partition.rs`. Ordinary ADD/DROP/TRUNCATE now read
that side channel despite not necessarily building metadata on the same thread.

## Plan of Work


First extend `tidb-session/tests/add_truncate_partition_source.rs` with a safety
regression for nonpartitioned and RANGE tables containing existing rows. Check
rows and SHOW CREATE before/after the refused operation; include an action list
so unsupported repartition cannot mutate an earlier action before refusing.
These are Rust containment tests, not claims that Go refuses the same SQL.

Extend `tidb-exec/tests/cluster_ddl_source.rs` to verify cluster lowering cannot
produce a direct repartition plan. Run existing ADD/DROP/TRUNCATE planning on a
fresh thread against metadata prepared on another thread; it must use only the
explicit snapshot. Confirm failures before edits, then remove the carrier,
builders and side channel. Keep CREATE's explicit `(metadata, routing spec)`
return value and ordinary partition-change planning's routing-spec result.
Reject local repartition before executing any action in a combined statement.

Retain unrelated Go-derived changes to bound expression restoration, charset
formatting, KEY coalescing and TopN. Do not modify generated parser/protocol
artifacts or weaken ignored source parity cases into passing refusal tests.
Record the removed behavior and still-required durable owner in D01.

## Validation


From `rust/`, use the existing aggregate test target with scoped filters:

    cargo test --locked -p tidb-session --test all repartition_refusal_preserves_rows_and_schema
    cargo test --locked -p tidb-exec --test all cluster_repartition_has_no_direct_metadata_plan
    cargo test --locked -p tidb-exec --test all cluster_partition_changes_do_not_require_prior_thread_metadata
    cargo test --locked -p tidb-session --test all add_truncate_partition_source
    cargo test --locked -p tidb-exec --test all workload_repository_partition_changes_use_cluster_ddl

Adjust only if the existing aggregate build is blocked by an unrelated failure;
record the failure and retain the same source tests in a scoped harness. Run
`git diff --check` and `make lint` from repository root. The real commit command
must use `TERM=xterm git -c core.hooksPath=hooks commit` and pass
`cd rust && cargo build --locked -p tidb-server`. After the final commit, rerun
that exact locked build before `git push origin HEAD:hparser-integration`.
No Go/Bazel/module files change, so Bazel preparation and Go failpoint enablement
do not apply. Live multi-node crash recovery and workload benchmarks cannot be
claimed by these containment tests.

## Surprises & Discoveries


- The previous review observed RANGE-to-HASH and nonpartitioned-to-HASH return
  success followed by zero visible rows. Its outputs remain historical evidence
  under `parity/current-audit/structural-recheck/`.
- The metadata side channel also affects unrelated partition changes on fresh
  threads. Restoring explicit values removes that dependency rather than adding
  a fallback value that could belong to another table.
- The dependency/fingerprint caches were already absent when this continuation
  began. Incremental artifacts occupied roughly 57 GiB, leaving under 2 GiB free.
  Their removal restored about 49 GiB of available space; validation must rebuild
  missing dependencies. No source, user data or shared Go/module cache was removed.
- The initial combined-action example used a comma before PARTITION BY, which
  Go's grammar rejects. The final test uses the source's terminal space-separated
  form and explicitly requires parsing to succeed before checking admission.
  The standalone row-loss baseline is unchanged; the strengthened combined case
  passes after removal and reaches the executor's pre-mutation refusal.

## Decision Log


- Decision: remove the entire unsafe live shortcut before implementing its
  replacement. A local row-copy patch would omit persisted states, dual writes,
  indexes, rollback and recovery. Retaining successful data-loss behavior while
  working on prerequisites violates correctness and the repository acceptance
  rule. Date: 2026-10-01.
- Decision: keep all structural findings open. Refusal contains data loss but
  does not provide Go's supported repartition behavior. Date: 2026-10-01.

## Idempotence and Recovery


Use ordinary source edits and dedicated regression filters. Do not revert whole
historical commits because they also contain independent partition improvements.
Build caches are disposable; diagnostic inputs, outputs and original parity
fixtures remain. Do not publish unless mandatory validation succeeds.

## Outcomes & Retrospective


Removed both private repartition builders, local execution dispatch, the
cluster DdlStatement carrier and lowering/planning arm, plus the exported
thread-local metadata stash/getter. CREATE retains its explicit metadata/spec
result. Ordinary ADD/DROP/TRUNCATE use only their supplied snapshot again.
Local admission refuses repartition before any action in the statement mutates
the catalog. The executor's only Repartition match now marks that refusal;
parser and AST support remain for future complete-owner integration.

All three new regressions failed before removal for the expected reason:
session rows became empty, cluster lowering returned a live direct plan, and
fresh-thread ADD planning panicked at the metadata-stash expect. After removal,
five scoped tests pass, covering those three regressions plus the preexisting
local ADD/TRUNCATE lifecycle and cluster workload-repository ADD/DROP behavior.
The fresh-thread test independently covers ADD, DROP and TRUNCATE. The session
test covers both input table shapes and combined-action refusal. No test is
ignored or changed to accept data loss.

Baseline logs are /private/tmp/tidb-partition-removal-session-before.log and
/private/tmp/tidb-partition-removal-cluster_*-before.log. The four successful
after commands use corresponding /private/tmp/tidb-partition-removal-*-after.log
files. The strengthened final session regression additionally passes in
/private/tmp/tidb-partition-removal-session-final.log. Formatting of the session
test and extracted changed function/test blocks, plus git diff --check, pass.
Root make lint and the register/symbol checks pass. Lint output is retained in
/private/tmp/tidb-partition-removal-lint.log. The register still agrees exactly
between Markdown and JSON, and searches confirm that both private builders,
the cluster carrier and metadata stash/getter no longer exist in Rust crates.
The actual commit hook passed the locked server build in 17.80 seconds; its log
is /private/tmp/tidb-partition-removal-commit.log. The separate post-commit
`cd rust && cargo build --locked -p tidb-server` passed in 13.08 seconds, recorded
in /private/tmp/tidb-partition-removal-prepush.log. The receipt-only amendment
reruns the hook, and publication requires another locked build after that final
commit. Final gate logs use the corresponding -commit-final.log and
-prepush-final.log names; the task result records their outcome and the pushed
commit, avoiding a self-referential commit hash in this receipt.

Compatibility limitation: SQL repartition is refused again instead of returning
unsafe success. Go supports it; this is containment and removal of an unaccepted
implementation, not completed parity. D01 and the related durable DDL, schema,
table-write and delete-range owners remain open, with all 77 unresolved IDs
retained. Native client-rust and its dependency pin are unchanged. No live
TiKV/mixed-node recovery, original Go package suite or workload benchmark ran.
