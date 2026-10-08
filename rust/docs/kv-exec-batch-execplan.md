# Connect statement KV execution counting to live requests


This living ExecPlan follows root PLANS.md. TiDB starts at 3438819684e44084b964572df42d1eb5cac65007, native client-rust at de4c53c34f9fcf53e2e07cb928b6b4af890b02c0. Refreshed Go master remains ab37692e9ebef44a736cd7243deef6020067b743; client-go is 8edb23f6c7ee. All work runs in the existing Cloud checkouts.

## Purpose and context


TopSQL's existing StatementStats and KvExecCounter owners must count one execution per physical KV target per statement. Currently the session publishes SQL execution counts but never creates or passes the KV counter; DistSQL exposes an unused boolean identity function instead of binding the interceptor. This batch connects O11 with the ordinary/coprocessor request ownership seams tracked by T02/O13. It does not accept a complete upstream package or the full profiling/reporting pipeline.

Go creates the counter at executor admission only when TopSQL is enabled. Each intercepted RPC checks the current enable switch, then marks its target once; failed calls count too. A later statement must get a fresh counter even inside an existing transaction. Worker/context copies retain the same statement counter. Region retries and duplicate reads on the same target do not inflate it. MPP dispatch and establishment inherit the statement context; health probes and background cancellation do not.

## Progress


- [x] Refresh refs, read current residuals and existing counter/native-interceptor/statement/request owners.
- [x] Add a late-bound statement counter reference and regression fixtures; capture grouped baseline failures before binding production consumers.
- [x] Bind admission and every maintained ordinary/coprocessor/MPP consumer; remove the boolean placeholder and its tautological assertions.
- [x] Run grouped regressions, lint, locked build, appropriate live validation and self-review; update both registers and the Cloud checkpoint.
- [ ] Execute actual precommit and fresh pre-push builds, normal publication and remote verification.

## Plan of work


Reuse tidb-util/src/topsql_stmtstats/kv_exec_count.rs. An execution reference allows executor construction to retain the counter before Go's admission point initializes it; it owns no worker and no new counting map. Carry it through the existing StmtContext, PushdownStatementContext, SnapshotReadOptions and DistSQL transport binding. Bind ordinary reads through the existing adapter UnaryCallContext and physical ClientKv dispatch. Native transaction interceptors also cover commit/write/background scopes; a snapshot-only binding must not leak into those calls. Each read installs the current statement reference, while commit/write scopes retain their own context. Bind coprocessor attempts at their actual dispatch boundary, including async batch attempts. MPP should mark only dispatch and stream establishment, before invoking the real RPC.

Extend existing owning tests rather than creating another harness. Check admission toggles, statement reuse, target deduplication, failures and real socket MPP requests. Preserve original Go counter assertions in the retained test, migrating them from the unused generic wrapper to the live handle. Record the exact baseline failures and final commands in a committed receipt under rust/docs/parity/current-audit.

## Validation and recovery


Source /workspace/.cloud-setup/env.sh and export CARGO_BUILD_JOBS=1. Run Cargo from /workspace/tidb/rust; preserve caches and sufficient linker space. Group related filters per crate, then make lint from the repository root and cargo build --locked -p tidb-server from rust/. Actual hooks/pre-commit must run that build, repeated immediately before every push. Never bypass hooks, force-push or reset concurrent work. Native APIs already exist; native source should not need changes. Log evidence under /workspace/.cloud-setup/kv-exec-batch. Live unistore checks establish startup/SQL readiness, not real multi-node KV attribution.

## Surprises & Discoveries


The native client already supports named RPC interceptors, but they span writes and commit as well as reads. Scope the snapshot binding in the existing adapter context instead. The missing behavior is caller composition, not a missing counter implementation. Native transaction interceptor scope would also charge commit/write/background work, so the adapter read-call scope is deliberate. Completed binaries and six obsolete pre-baseline library versions were retired with hash/process/alias receipts to preserve linker space; current libraries and compiler caches remain. The DistSQL boolean helper has no production consumer and its assertions exercise only identity.

## Decision Log


Preserve the existing SQL digest/plan attribution boundary; do not invent a plan digest or claim CPU/RU profiling. Use Go's admission and per-RPC enable windows, including disabled-at-admission executions remaining without a KV counter even if enabled later. Keep background MPP cancellation separate from foreground statement RPCs.

## Outcomes & Retrospective


Implementation and batch validation passed: four behavioral failures reproduced before the binding; 191 distinct Rust cases pass with zero ignored, including asynchronous workers and real socket MPP retries. make lint, the locked server build, diff/source review and 23 live startup/MySQL assertions pass. The historical Ready skill is absent, so applicable root AGENTS.md gates were followed. Actual precommit and fresh postcommit/pre-push builds remain mandatory; publication is pending at this receipt boundary and its final evidence will be /workspace/.cloud-setup/kv-exec-batch/final-handoff.json. Broader T02/O13 cache/routing and O11 profiling/reporting obligations remain explicit.
