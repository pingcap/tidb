# Share statement attribution across counters and summaries


This living ExecPlan follows root PLANS.md. Work starts from 77759b60eea08dd3b287768c188fcf119fadf3fc on hparser-integration in /workspace/tidb; /workspace/client-rust master is unchanged. Freshly fetched Go master is 93a01d31f6da205ae4bf376825293903a6899fdb. No push is authorized.

## Purpose / Big Picture


Repair connected O11/O18 producer gaps: failed physical planning must not increment execution counts; real text/prepared/streaming statements must retain measured parse/compile times and the existing privilege visits' database/table attribution. Go executor/adapter.go observes execution after construction and summary uses StatementContext.Tables produced from planner visits. This fixes existing runtime integration, not acceptance of complete Go session/executor/profiling packages.

## Progress


- [x] Read instructions, inspect live owners and fetch comparison refs.
- [x] Add real-session regressions and capture failures before repair.
- [x] Share physical first-run observation, phase measurements and existing privilege visits; preserve prepared/routed/close lifetimes.
- [x] Run relevant tests, lint, all-target checking and locked server build; update both registers and receipts.
- [ ] Commit through actual locked-build hooks, verify recovery and Cloud draft without pushing.

## Context and Orientation


rust/crates/tidb-session/src/observation.rs currently increments TopSQL on AST recognition, before planning, and supplies empty tables and zero timings. dispatch.rs has a compile wrapper that also executes some statements; its entire duration cannot be called pure compilation. tidb-executor/src/stmt_context.rs::notify_before_executor_first_run is already shared by physical query/DML/explain execution, after construction and before Open. A statement-owned observer attached to plan publication and first run must be once-only even through nested contexts and retries. identity.rs already checks privilege visits, including prepared retained requests; use these rather than another AST scanner. Parser duration is measured only when parsing actually happens and transferred across frontend parse/execute doors.

## Milestones and Plan of Work


First extend tests_observation_batch.rs with actual failed planning, ordinary/prepared/streaming query and table metadata regressions. Capture nonzero failures from cargo test before production changes. Then add a shared observation state and optional first-run callback on StmtContext, installed by both Session context constructors. Preserve total cost (Go includes parsing and compilation); use physical plan-ready time before executor construction to delimit measured compilation rather than subtracting execution from total incorrectly. Attribute tables from existing privilege requests in stable first-occurrence order, excluding dynamic empty pairs. Prepared retargeting retains the outer parse timing and replaces visit metadata.

Finally exercise old observation/lifecycle/privilege cases as well as new failures, build/check/lint, self-review and update audit receipts, both registers and this plan. O11/O18 remain partial for the explicitly unimplemented plan/profiling/transport/phase details; never turn this producer repair into whole-package acceptance.

## Concrete Steps and Acceptance


Source /workspace/.cloud-setup/env.sh in each shell. From /workspace/tidb/rust run cargo test --locked -p tidb-session --lib observation_batch -- --test-threads=1. New cases must fail before and pass after, and old cases must remain passing. Run relevant privilege and streaming cases, cargo check --locked -p tidb-session --all-targets and cargo build --locked -p tidb-server. From /workspace/tidb run make lint and git diff --check. Normal commits must execute hooks/pre-commit with core.hooksPath=hooks and pass cd rust && cargo build --locked -p tidb-server; never bypass it. Nothing is pushed.

## Surprises & Discoveries


Go TopSQL ExecDuration is total cost including parse/compile; retain this rather than inventing an execution-only counter. Three new behavioral cases fail before repair; three follow-on baseline failures were lock poisoning, not independent production findings. New assertions release shared locks before checking values. Existing compile wrapper contains DML execution, so measure the first-run boundary where available and leave unsupported phase attribution explicit.

## Decision Log


Decision: repair O11/O18 together through existing statement/privilege/executor owners. Rationale: one statement identity and lifetime governs both producers; independent duplicated metadata collectors would drift. Date: 2026-10-04.

## Idempotence and Recovery


Read status before editing, preserve concurrent edits and local commits. Logs belong in /workspace/.cloud-setup/statement-attribution. Never reset a dirty checkout. Verify a replacement bundle before replacing the previous recovery artifact. Save only exact inspected repository refs/startup instructions; preserve installer/network/secrets/runtime settings. Fresh-task restoration remains unverified.

## Interfaces and Dependencies


The optional StmtContext phase callback is an Arc-backed Send+Sync Fn(StatementPhase) function; PlanReady stops physical compilation before construction, ExecutorReady admits counts before Open. Its captured statement state survives clones, is once-only and contains no reference to Session. TableEntry is reused from tidb-stmtsummary. Existing TopSQL StatementStats owns counts. No new dependencies/native client/Go source changes are planned.

## Outcomes & Retrospective


Three behavioral gaps repaired under O11/O18, which remain partial. Fifty-one distinct Rust cases and twelve real MySQL/unistore checks pass; lint, all-target checking and locked server build pass after documented disk recovery. Final normal hook commits/recovery/draft delivery are pending. Full Go package fixtures, generated/platform/profiling transport, live multi-node TiKV and performance remain unverified.
