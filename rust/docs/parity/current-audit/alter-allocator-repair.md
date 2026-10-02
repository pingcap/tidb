# Defer shared allocator effects until ALTER preparation succeeds

Reviewed 2026-10-02 from integration
`f0a26898c504e7727aac19806c2d994f212be9ee`, against Go master
`93a01d31f6da205ae4bf376825293903a6899fdb`. Native client-rust and the dependency
remain current at `19a56ccda1e128218cd33c69709038219aced9bc`. Both repositories
were pulled before work. This is maintenance of the existing local ALTER owner,
not acceptance of the complete Go DDL or autoid packages.

## Source contract and repair

Go `pkg/ddl/executor.go::alterTable` collects subjobs before running any of them.
`RebaseAutoID` prepares typed arguments and does not mutate the allocator.
`table.go::onRebaseAutoID` first marks a multi-schema subjob non-revertible and
returns without applying its rebase. `multi_schema_change.go::onMultiSchemaChange`
drives the remaining revertible subjobs before running those rebases.
`column.go::checkNewAutoRandomBits` advances the source counter before its
capacity check; `applyNewAutoRandomBits` migrates the counter for conversion.
Those operations belong to execution, after statement preparation.

The prior local ALTER repair staged catalog/schema/row/index publication, but
its shared allocator stores and cached reservations remained outside that image.
Early ordinary/forced rebases and AUTO_RANDOM conversion therefore escaped a
later preparation failure. A duplicate column after AUTO_INCREMENT=1000000
changed the next generated ID even though the ALTER failed.

The common dispatcher now retains typed operations using the existing live
allocator handles. Once all catalog actions succeed, it executes layout checks,
then rebases, then publishes the catalog. Both session dispatch and cluster
partition-metadata derivation retain the same dispatcher. The latter constructs
an isolated in-memory catalog; this change does not replace external transaction
savepoints. AUTO_ID_CACHE still creates a fresh local range over the shared
store without writing the counter during preparation.

Removed: eager allocator mutation from ALTER option traversal and column
conversion, and the old immediate layout-change helper. The underlying allocator
remains the sole counter owner. No saved-counter rollback, cloned counter store,
second transaction engine, or closure capturing complete table objects is added.
Forced rebase semantics and existing store error identities remain intact.

## Validation

Seven executor regressions and one session regression fail against unchanged
production code and pass after repair. They cover ordinary/forced increment and
random rebases, random layout changes, increment-to-random conversion, unique
backfill failure, invalid FORCE arguments, and layout-before-FORCE ordering.
The ordering regression previously succeeded after FORCE hid an overflowing
layout; it now rejects before that rebase. Successful controls verify ID generation
and both cache-option orders. Allocator tests also interleave a live writer with
prepared operations and inject store errors.

Commands below run from `rust/`:

```sh
cargo test --locked -p tidb-executor --lib tests_ddl_multi_schema_change_sql -- --nocapture
cargo test --locked -p tidb-executor --lib kv_table::auto_id::tests -- --nocapture
cargo test --locked -p tidb-executor --test all serial_auto_random_source:: -- --nocapture
cargo test --locked -p tidb-session --lib tests_alter_column -- --nocapture
cargo check --locked -p tidb-executor -p tidb-exec -p tidb-session -p tidb-server --all-targets
```

Results: 28 multi-schema tests pass; 12 allocator tests pass and two fail;
eight AUTO_RANDOM tests pass and three remain ignored; 15 session tests pass.
All-target checking passes. The two allocator failures, `test_in_memory_alloc`
and `test_issue40584`, also fail identically with all five production files
temporarily restored from the starting HEAD. They expect 2 and 20001 from the
existing `next()` accessor, which returns the reserved global boundary 30001.
The continuation does not change that accessor or those tests. They are retained
as baseline failures, not counted as passing tests or newly fixed contracts.

The retained [session diagnostic](alter-residual-probe.rs) was rerun using
`run-expanded-probes.py::run` with temporary example name
`alter_allocator_repaired`. Its [new output](alter-allocator-repaired.txt)
returns IDs 1 and 2; the [prior output](alter-review-recheck/alter-residual.txt)
returned 1 and 1000000. The temporary source was removed after execution.
[Validation data](alter-allocator-validation.json) records command results and
compact failure evidence.

Root `GOTOOLCHAIN=go1.25.14 make lint` and `git diff --check` pass. No Go, Bazel,
generated or dependency inputs change; `make bazel_prepare` is not required.
The actual commit hook passed the locked server build in 17.96 seconds. The
receipt amendment repeats that hook. Publication then requires a fresh build
from the final commit, immediately before push, followed by remote-SHA and clean
checkout verification:

```sh
TERM=xterm git -c core.hooksPath=hooks commit -m "ddl: defer shared allocator effects until ALTER preparation succeeds"
(cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration
```

## Remaining boundary

D11 remains partial. Local actions still derive subsequent admission from their
staged image rather than the complete Go original-schema subjob lifecycle;
drop-column plus rename-of-its-index remains a known difference. Durable worker
transactions, retries, rollback and recovery remain separate unresolved owners.
If an execution-time store operation fails after earlier effects, catalog staging
alone cannot recover those effects. Counter rollback would also discard concurrent
allocations and is deliberately not introduced. AUTO_RANDOM's execution-time
capacity check still advances the source counter, as the existing source-derived
operation does; this repair guarantees no such effect during failed preparation.

The prior full review's counts stay **71 unresolved (63 open, eight partial),
15 repaired, 86 tracked**. Other IDs retain that review's evidence. This
continuation does not rerun a live Go SQL oracle, distributed DDL failure recovery,
the complete upstream package tests, or sysbench/TPC-C/TPC-H/YCSB. No measured
performance improvement or complete parity is claimed.
