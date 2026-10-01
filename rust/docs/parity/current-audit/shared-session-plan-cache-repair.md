# Shared session plan-cache owner repair, 2026-09-30

This maintains the existing Rust planner/executor/session/server integration
against TiDB master `e953a09d9d5e29e60c62f42d3aacebb819af49a5`, starting from
integration `5fcc321d4801c33a6670af7653544fbabe7eb5f2`. The integration pull and
master fetch were current. No native client or dependency pin changed.
This repairs C01's ownership/lifetime defect; it does not accept the complete
upstream planner, executor, session or server packages. C02's domain physical
cache and execution cloning remain open, along with the other tracked findings.

## Removed ownership and replacement

`PreparedSelectPlan::cached_plans`, `PreparedDmlPlan::cached_plans`, both private
entry structs and `NonPreparedDmlCache` are removed. Prepared definitions retain
syntax and cache-key metadata. One `SessionPlanCache` owns the physical entries
for prepared/non-prepared SELECT/INSERT/UPDATE/DELETE. Non-prepared statements
have one separate metadata LRU, matching `SessionVars.nonPreparedPlanCacheStmts`.
Metadata eviction no longer bypasses or dictates physical-entry eviction.

The existing `LruPlanCache` now uses per-key parameter buckets and the existing
`tidb-kvcache` linked LRU for global recency. It no longer scans/removes entries
from a global `VecDeque`. Its native typed key retains SQL/database identity,
schema/statistics versions, environment/bindings and parameterized LIMIT values;
only parameter compatibility is searched within a bucket. Every physical
variant counts toward the shared capacity. Compatible replacement promotes the
entry and updates accounting without invoking memory control, matching Go's
replacement arm. New insertion applies capacity and process-memory guards.

The physical owner maintains plan-count and memory-estimate metrics on insertion,
eviction, clear and session destruction. Memory accounting uses the existing
physical-tree estimator plus key/type/value storage; it is an estimate, not a
measurement of all retained Rust AST/allocator bytes. The guard uses the native
process/allocator sampler and configured server quota capped to system memory.
Open execution leases retain their Arc after eviction; that does not make the
entry discoverable as a subsequent cache hit. Rebuild and admission continue
through their existing owners.

Go captures capacity at the first `GetSessionPlanCache` construction. Rust does
likewise, including construction through flush or statement close, and does not
resize an existing cache merely because the variable changed. SQL DEALLOCATE
and protocol COM_STMT_CLOSE delete the current environment's parameter bucket
unless `tidb_ignore_prepared_cache_close_stmt` requests retention. The server
registry passes close to the same session owner as SQL DEALLOCATE.

ADMIN FLUSH SESSION clears physical entries without dropping definitions.
ADMIN FLUSH INSTANCE advances a shared invalidation epoch, observed before a
session's next lookup. Embedded peers share the catalog's default invalidation
owner; cluster connections receive a factory-owned handle independent of their
separate/rebuilt catalog images. Catalog checkpoints preserve that handle.
Separate factories are separate instances. The global-scope refusal and the
prepared-cache-disabled warning follow Go, including retaining non-prepared
entries when the prepared switch is off. The server classifies flush as an
OK-packet statement so its dispatch actually executes the command.

Source contracts checked at the master above:

- `pkg/session/session.go::GetSessionPlanCache` and prepared-statement cleanup.
- `pkg/sessionctx/variable/session.go::GetNonPreparedPlanCacheStmt` and insertion.
- `pkg/planner/core/plan_cache_lru.go` bucket, recency, replacement, pressure,
  delete, close and metrics lifecycle.
- `pkg/planner/core/plan_cache.go::{planCachePreprocess,lookupPlanCache,generateNewPlan}`
  and `plan_cache_utils.go` identity/type compatibility.
- `pkg/executor/simple.go::executeAdminFlushPlanCache`,
  `pkg/executor/prepared.go::DeallocateExec.Next` and server statement close.
- `cmd/tidb-server/main.go` quota initialization.

## Regression evidence

The four initial `session_plan_cache_` regressions all fail with unchanged
production at the integration revision above: separate SELECT/DML owners exceed
capacity one, incompatible signed/unsigned parameter variants exceed capacity,
prepared/non-prepared callers have different budgets, and session flush is
unsupported. They pass with the shared owner. Baseline source came from
`git show HEAD:<path>`; a Python `finally` restored every working source byte.

A ninth LRU test also fails against the standalone unchanged module: replacing
an existing entry under memory pressure incorrectly removes it. The repaired
replacement returns before memory control; a subsequent new insertion still
evicts under pressure. The baseline module plus the same added test was built
with `rustc --edition 2021 --test` and run with the exact regression filter.

Additional tests cover shared SELECT/DML recency, all four caller classes,
first-construction capacity, per-session/instance isolation, disabled/global
flush policy, same-SQL sharing, SQL/binary close retention, deletion scoped to
the current environment, memory reset and execution leases surviving eviction.
The cluster factory regression uses separate session catalogs and a separate
factory control. The MySQL regression sends real COM_STMT_CLOSE packets over
loopback and verifies both cache hits and fresh bound results.

## Validation commands

Commands below run from `rust/` unless marked as repository-root commands.
Filters exercise the changed owners, original prepared statement/admission
coverage and the affected protocol callers; they are not full-package receipts.

    cargo test --locked -p tidb-session --lib session_plan_cache_
    cargo test --locked -p tidb-session --lib plan_cache
    cargo test --locked -p tidb-session --lib prepared
    cargo test --locked -p tidb-planner --lib plan_cache_lru
    cargo test --locked -p tidb-executor --lib prepared
    cargo test --locked -p tidb-executor --lib prepared_in_predicate_uses_filtered_stats_for_cache_admission
    cargo test --locked -p tidb-executor --test all shared_physical_select_builder_source
    cargo test --locked -p tidb-server session_plan_cache_
    cargo test --locked -p tidb-server --lib prepared
    cargo test --locked -p tidb-server --test all pipeline_mysql_client_source
    cargo test --locked -p tidb-server --test all pipeline_mysql_client_source::mysql_client_runs_the_pipeline_end_to_end
    cargo check --locked -p tidb-planner -p tidb-executor -p tidb-session -p tidb-server --all-targets

The session prepared filter passes 128 tests (2 already ignored); the LRU
filter passes 9 tests. The executor prepared filter passes 41 tests and fails
`prepared_in_predicate_uses_filtered_stats_for_cache_admission`; the shared
builder filter passes 4 and fails
`cached_index_join_compare_filter_rebinds_its_parameter`. Both failures were
independently reproduced with unchanged production AND test sources from
integration HEAD. The former expects an IndexLookUpReader that the baseline
planner does not select; the latter cannot bind its index-join comparison
parameter. Their assertions remain unchanged. These are outstanding validation
failures, not attributed to this cache-owner repair or counted as passing.

Both new server regressions pass (one cluster factory, one wire test), as do
31 server prepared-statement tests. The full pipeline MySQL suite passes 9 and
fails its existing `mysql_client_runs_the_pipeline_end_to_end` grants assertion:
actual backtick-quoted account names versus single-quoted expected names. This
failure also reproduces on unchanged source; no assertion was suppressed.
All targets of the four affected crates compile successfully. Root lint and
whitespace checks pass. Publication is gated by the hook build and the separate
fresh locked server build below; their command results are reported with the
published commit. Logs use
`/private/tmp/tidb-cache-*.log` and `/private/tmp/tidb-session-cache-*.log`.

Repository-root gates and publication:

    make lint
    git diff --check
    TERM=xterm git -c core.hooksPath=hooks commit -m "planner: share session physical plan cache ownership"
    (cd rust && cargo build --locked -p tidb-server)
    git push origin HEAD:hparser-integration

The commit hook itself must pass the locked server build; the separate locked
build must then pass immediately before push. Rust-only changes do not trigger
Bazel preparation or Go failpoint setup. Changed Rust ranges were formatted
with edition 2021 and child/module/import traversal disabled to avoid unrelated
formatting changes. Fifty stale incremental cache directories, 85 finished older incremental caches
for the four affected crates, and 21 old, unopened generated binaries were
removed. Active compilers were checked before the second cleanup. Current
compiled outputs and all source remain; free space returned to about 18 GiB.

## Limits and remaining work

Full upstream package variants/original tests, real multi-node TiKV failure
injection and sysbench/TPC-C/TPC-H/YCSB throughput were not run. No measured
workload speedup is claimed. The global LRU removes a whole-cache linear scan
and shared capacity bounds retained variants; actual hit rates and performance
need workload measurement. The memory estimate does not establish exact native
heap accounting. C02 and the broader planner/session/domain integration gaps
remain explicit; this receipt does not close the exhaustive parity goal.
