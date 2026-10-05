# Retire the disconnected aggregate and sort chain

This living ExecPlan follows root PLANS.md. Earlier receipts stay indexed in
parity/current-audit/README.md; Git preserves removed source and test obligations.

## Purpose / Big Picture


Remove three unused server result-set wrappers and their isolated aggregate,
DISTINCT and ordering implementation as one batch. Compilation and test discovery
will no longer include 41 obsolete files. Actual SQL aggregation remains owned
by HashAgg, StreamAgg and WindowExec in tidb-executor. No speedup is measured.

## Context and Orientation


Base fe8a0cbe328f475297cde9c2f646cea22affa72e on hparser-integration in
/workspace/tidb; refreshed Go master b36c940a4332c866d8b0e2afde88f5e7c2fd7fed.
Native master cfafb1eb01594cecd5926fa63e57e2a6e20c7ee1 is unchanged.
The private AggregateResultSetSource, DistinctResultSetSource and
SortingResultSetSource have no callers beyond their own tests/root exports.
They are the only outside consumers of tidb-exec's aggregate/runtime, distinct
checker and order module. GroupConcat's compatibility module has only private
tests. Public-type tracing finds only exports and comments outside this chain;
Pair in a session test is an independent local alias. Production tidb-executor
does not depend on tidb-exec and owns its actual aggregate state/merge/spill.

## Progress


- [x] Trace wrappers, all public state types, module imports and Go owners.
- [x] Remove 23 source files and 18 harnesses; preserve the mixed metadata test.
- [x] Remove stale exports, unused into_source and obsolete ownership/runner notes.
- [x] Verify retained bytes, fresh registration and grouped live owner tests/checks.
- [ ] Complete hook, fresh pre-push locked build, remote verification and cloud save.

## Milestones and Plan of Work


Remove rust/crates/tidb-server/src/{aggregate,distinct,sorting}_result_set.rs;
remove tidb-exec/src/aggregate.rs, aggregate_distinct.rs, aggregate/runtime/,
bit_agg.rs, first_row.rs, group_concat.rs, minmax_deque.rs and order.rs with their
private harnesses. Remove lib.rs registrations/exports and the unused
QueryResult::into_source adapter. All production callers are already migrated.

Move variance_result_metadata_is_always_double_23_with_unspecified_scale verbatim
into existing result_field_resolver_source, adding only its AST imports. Preserve
all other retained test bodies and production SQL operators. The 106 registered model tests and six unregistered orphan tests assert the retired models. Original Go aggfuncs/aggregation/sort
obligations remain; deleting model tests does not prove complete live coverage.

Replace stale runtime ownership and runner instructions. Keep the historical
Go package inventory and explicit incomplete obligations. Record removal paths,
hashes and validation in parity/current-audit/aggregate-chain-cleanup-validation.json;
external inventory includes original test names and Go anchors.

## Concrete Steps / Validation and Acceptance


Source /workspace/.cloud-setup/env.sh. Run from rust/ with CARGO_BUILD_JOBS=1:

    cargo check --locked -p tidb-exec -p tidb-server --all-targets
    cargo test --locked -p tidb-exec --test all -- result_field_resolver_source --test-threads=1
    cargo test --locked -p tidb-executor --lib -- hash_agg::tests::min_max_skip_nulls hash_agg::tests::max_min_count hash_agg::parallel::tests::typed_count_distinct hash_agg::tests::grouped_stream_agg hash_agg::tests::group_concat_stringifies --test-threads=1

Verify fresh aggregate registration excludes all 18 deleted harnesses and keeps
result_field_resolver_source. Compare all retained operator/test hashes to the
baseline, including the moved test body. Run make lint and git diff --check at
repository root. Actual precommit must run cd rust && cargo build --locked -p
tidb-server; rerun the same locked build immediately before the authorized push.
Verify the remote SHA and preserve concurrent commits without force push.
No new harness or broad runtime/Go sweep is warranted by disconnected deletion.

## Surprises & Discoveries


Module comments described these wrappers and models as live, but no production
caller constructs the wrappers. The attempted stream_agg::tests filter selected nothing because stream_agg.rs
was unregistered. Active StreamAggExec/GroupedStreamAggExec live in hash_agg.rs;
the orphan file and its six never-registered tests are removed too. Registered
stream/group-concat tests replace that filter in the final grouped command.
One mixed variance harness tests the live result
metadata owner, so deleting it wholesale would lose a meaningful contract.

## Decision Log


Retire the entire unreachable chain, preserve the metadata assertion in its
existing owner suite, and retain all original Go obligations. No dependencies,
SQL semantics or active scripts change. Date: 2026-10-05 UTC.

## Idempotence and Recovery / Interfaces and Dependencies


Recover before-images with git show fe8a0cbe32:<path> into temporary files for
review. External logs/inventory: /workspace/.cloud-setup/aggregate-chain-cleanup.
No Cargo manifest, dependency, lockfile, hook or native client change. Retired
Rust model APIs are intentionally removed; supported SQL interfaces are unchanged.
Rerun checks safely, preserving caches and concurrent work.

## Outcomes & Retrospective


Removed 41 files (8272 deleted file lines; 8277 net Rust lines), 106 registered
model tests and six unregistered orphan tests. Preserved the useful metadata
assertion verbatim. Seven metadata and eight registered live-operator tests,
all-target checking, root lint and diff checks pass. Fresh registration excludes
all 18 retired harnesses. No live aggregate algorithm changes. Register dispositions remain
86 tracked /30 repaired /56 unresolved. Full Go packages, live multi-node TiKV
and performance remain unverified. Post-commit gates and setup-save results are
recorded in external final-handoff.json.
