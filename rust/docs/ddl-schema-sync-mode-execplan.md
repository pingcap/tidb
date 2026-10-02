# Recheck structural findings and restore DDL schema-sync mode ownership

## Purpose / Big Picture


Persisted DDL must obey Go's metadata-lock (MDL) mode. In classic mode with MDL
disabled, a new or replacement worker must wait for loaded schema versions
without requiring mysql.tidb_mdl_info. With MDL enabled, durable per-job barriers
and owner-qualified cleanup must remain intact. This repairs existing worker
ownership, not a partial or complete transcreation claim for pkg/ddl.

The user requests a current review of every unresolved finding, fixes following
Go master, and commit/push to hparser-integration. Recheck the existing register
before changing its count; distinguish removed symptoms from remaining contracts.

## Progress


- [x] Pull integration/native client, fetch Go master, read repository/DDL/test guidance.
- [x] Compare all recorded source references and intervening production changes.
- [x] Trace Go mode selection through registration, scheduler recovery, wait and cleanup.
- [x] Add failing regressions for disabled-mode registration and restart recovery.
- [x] Migrate existing planner/worker callers to the source mode and shared synchronizer.
- [x] Validate both modes, failures, cancellation, affected callers and lint.
- [x] Update every finding's evidence and count; record remaining boundaries.
- [x] Commit using the actual locked-build hook (12.94 seconds); the receipt amendment repeats the hook.
- [x] Rebuilt from the amended commit immediately before push (12.31 seconds); remote hparser-integration matched 2e66b7c28f60ab2083559222735496c0596f1b4a and the checkout was clean.

## Context and Orientation


Starting integration d0f1c371150530fb7b5c37730456c5d56ad11e64, Go master
93a01d31f6da205ae4bf376825293903a6899fdb, and native client/dependency
19a56ccda1e128218cd33c69709038219aced9bc are current. The previous full review
was based on b52dbfef7f9a0076500132bc63b276617440aa89. The 71 unresolved IDs
remain before this repair (63 open, eight partial); 15 are repaired. D11's
allocator, index and column symptoms have since been repaired, but other action
admission and durable execution remain missing.

rust/crates/tidb-exec/src/cluster_ddl.rs plans persisted actions and a recovered
barrier before allowing the next action/history. real_tikv_ddl.rs commits the
plan, notifies schema versions, waits and cleans acknowledgement state.
tidb-server/src/cluster_session_node/ddl.rs adapts the shared tidb-schemaver
syncer; schema_sync.rs reports loaded versions and MDL acknowledgements.
The lower syncer already selects non-MDL self-version keys correctly. The
planner unconditionally requires/registers MDL rows, so changing only the wait
cannot repair recovery.

Go source is read with git show origin/master:<path>, not the older Go working
tree. job_worker.go::registerMDLInfo skips disabled mode.
job_scheduler.go::transitOneJobStepAndWaitSync uses durable MDL rows when enabled;
otherwise a started job with LastSchemaVersion recovers the latest schema with
a nonempty diff. schema_version.go::waitVersionSyncedWithoutMDL and
job_worker.go::updateGlobalVersionAndWaitSynced publish/wait using the shared
syncer. Source comments mention lease waits, but the current scheduler passes
its own context: do not invent a fixed-delay fallback.

## Plan of Work and Milestones


First retain per-ID source continuity under current-audit/mdl-review-recheck.
Unchanged Go/native revisions and inspection of all intervening Rust owner
changes permit carrying prior source evidence; old executable observations stay
explicitly historical. Refresh D11 and qualify D07's wording.

Next extend existing persisted-DDL tests. Demonstrate disabled-mode jobs do not
read/write MDL metadata, and replacement workers must synchronize a committed
schema before history even without an MDL row. Cover normal enabled mode,
nonempty-diff recovery, failure and owner retirement. Regressions must fail
against unchanged production before changing its APIs.

Then carry mode through the common planner and worker, retaining one shared
synchronization/cleanup lifecycle. Keep seed action dispatch disabled. Reuse
existing metadata reads and tidb-schemaver; no new DDL engine, scheduler or
speculative lease timer. Migrate every caller and explicitly set test policy
where tests previously assumed MDL registration independent of process state.

Finally run targeted executor/server/schema-sync suites and all-target checks,
compare unexpected failures with baseline, run root lint, update the review and
register, self-review the diff, and publish with both locked build gates.

## Concrete Steps and Validation


From rust/, select focused tests from the existing cluster_ddl_source integration
module and cluster_session_node::ddl/schema_sync unit modules. Run:

    cargo test --locked -p tidb-exec --test all cluster_ddl_source:: -- --nocapture
    cargo test --locked -p tidb-server --lib cluster_session_node::ddl::schema_sync_tests -- --nocapture
    cargo test --locked -p tidb-server --lib cluster_session_node::schema_sync::tests -- --nocapture
    cargo test --locked -p tidb-schemaver --lib -- --nocapture
    cargo check --locked -p tidb-exec -p tidb-server --all-targets

From the repository root:

    GOTOOLCHAIN=go1.25.14 make lint
    git diff --check
    TERM=xterm git -c core.hooksPath=hooks commit -m "ddl: honor schema synchronization mode in persisted workers"
    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

No Go/Bazel/generated/dependency inputs are planned, so bazel_prepare is not
required. Rust tests need no Go failpoint enablement. A passing in-process
worker test is not evidence of mixed-node or live TiKV crash recovery.

## Idempotence and Recovery


Use fresh in-process fixtures and isolate process-global mode tests. Baseline
controls restore only task-owned files and restore edited bytes in finally.
Never discard unrelated work or force-push. If publication races with another
writer, inspect changes and repeat required checks after safe integration.

## Surprises & Discoveries


The prior description overstated D07: tidb-schemaver already supports both modes.
The concrete gap is the persisted worker's compulsory MDL registration/recovery
and mode-insensitive cleanup. Recent identity changes do not close D01/D02,
other DDL gaps, cache wrappers, affinity or lower datatype contracts.

## Decision Log


- Decision: Fix source mode selection and recovery at the existing worker owner.
  Rationale: A second synchronizer or artificial delay would duplicate already
  implemented policy and leave durable recovery incorrect.
  Date: 2026-10-02.

## Outcomes & Retrospective


D07's recorded mode boundary is repaired, leaving 70 unresolved IDs (62 open,
eight partial), 16 repaired and 86 tracked. Four regressions failed before the
repairs. Final scoped tests report 140 pass and five baseline failures reproduced
with unchanged production/test APIs. All-target checking and root lint pass.
Four retained diagnostics supply fresh observations for 13 remaining IDs; other
evidence explicitly uses source continuity. The actual commit hook passed; the
receipt amendment repeats it before the final build/push gate.

The follower review also found stale per-job versions overwriting loaded
self-version reports with MDL disabled. Its loop now follows Go MDLCheckLoop's
mode guard. Normal committed phases and owner recovery carry separate state;
non-MDL recovery republishes the latest nonempty diff. Notification failure
controls and three DROP-phase waits preserve the source policy. No fixed-delay
fallback, second synchronizer or new live seed action was introduced.
