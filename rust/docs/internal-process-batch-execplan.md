# Share internal-session process ownership

This is a living ExecPlan governed by root PLANS.md.

## Purpose / Big Picture


Make running auto-analyze tasks visible to the server's process manager and interruptible by KILL and the analysis time-window worker. Retain pooled internal-session timestamps and report their long-running diagnostics without leaking idle internal sessions into SHOW PROCESSLIST or TIDB_TRX. These connected maintenance repairs advance N04, I01 and O09; they do not accept entire Go packages.

## Progress


- [x] Refreshed integration and Go master; both remain at the previous checkpoint. Read source owners and confirmed isolated registries, unit-valued tracking payload and no-op callbacks.
- [x] Capture baseline regression and implement shared lifecycle.
- [x] Run grouped regressions, all-target check, lint and review; update evidence.
- [ ] Run actual hook locked build and fresh pre-push locked build at publication.
- [ ] Commit through actual hook, rebuild immediately before authorized push, verify remote, refresh Cloud draft.

## Context and Orientation


Work in /workspace/tidb on hparser-integration, initially 42d7c7960f7ede39f4d91a74d8bb4685811b71d0. Go master is b36c940a4332c866d8b0e2afde88f5e7c2fd7fed, exported in /workspace/.cloud-setup/go-master. Native client cfafb1eb01594cecd5926fa63e57e2a6e20c7ee1 is unchanged. Go pkg/domain/domain.go SysProcesses distinguishes tracked tasks from pkg/server/server.go internalSessions and clients. GetInternalSessionStartTSList excludes active auto-analyze IDs; infosync reports internal diagnostics separately. A timestamp (TSO) represents the physical milliseconds plus eighteen logical bits.

Rust tidb-session/src/process.rs owns live process entries and guards. Internal guards currently use private directories. tidb-server/src/cluster_session_node/mod.rs creates pooled sessions and passes a unit value through the restricted-execution tracker; its auto-analyze callbacks do nothing. boot.rs merges physical guards and client timestamps but does not report internal-session diagnostics. Preserve physical timestamp holds conservatively; full DDL exclusion and lazy schema V2 remain separate prerequisites.

## Plan of Work


Keep client, pooled internal, and tracked system-task membership distinct while sharing each session's process entry. Add an owned live system-process handle containing the entry and cancellation capability; pass it through the existing opaque TrackProcess contract. The server's tracker registers that actual handle, rejects occupied IDs, and removes tasks on completion/error. Enumeration merges only tracked tasks with clients; TIDB_TRX remains client-only. Internal session guards register and unregister in a shared directory and supply active timestamps separately, excluding tracked auto-analyze tasks as Go does. Kill fallback sends query interruption even for KILL CONNECTION on a system task. Cancellation must never lock the executing session mutex.

## Concrete Steps


Source /workspace/.cloud-setup/env.sh before commands. In /workspace/tidb/rust use CARGO_BUILD_JOBS=1 and cargo test --locked -p tidb-session -p tidb-server --lib --no-fail-fast with grouped process and auto_analyze filters. Capture original failure before production edits. Then cargo check --locked -p tidb-session -p tidb-server --all-targets. At repository root run make lint and git diff --check. Evidence lives in /workspace/.cloud-setup/internal-process-batch and a committed validation JSON beside the finding registers.

## Validation and Acceptance


Prove that restricted ANALYZE passes a live session, registered system tasks expose actual statement information, cancellation reaches the same SQL killer, both KILL modes interrupt without closing pooled sessions, untracking retires visibility, idle internal sessions remain hidden, timestamp reporting includes internal transactions and releases them when sessions drop, and client behavior remains unchanged. Preserve useful Go-contract tests. Run the actual precommit locked build and a new locked build immediately before push; verify origin/hparser-integration SHA. Full Go suites, multi-node RPC and performance are outside this maintenance receipt.

## Idempotence and Recovery


Do not overwrite concurrent changes or force-push. On push rejection retain the validated commit and report the exact sanitized diagnostic. Regenerable inactive build executables may be pruned only when disk pressure requires; never discard source or dependency caches. Cloud draft save is distinct from publishing or a fresh restore.

## Interfaces and Dependencies


Reuse ProcessRegistry, ProcessGuard, StatementCancellation and the restricted-execution TrackSysProc/UntrackSysProc callbacks. No new dependency, wire protocol or native-client change is required. Keep no lock over SQL execution or a cancellation callback.

## Surprises & Discoveries


Schema-cache timestamp ranges need actual schema-diff commit timestamps and cannot safely use snapshot read times. The selected connected repair is instead the live internal-session owner found while tracing timestamp reporting.

## Decision Log


- Decision: share internal entry ownership and scoped task publication, retaining conservative physical timestamp holds. Rationale: Go separates these directories; removing protection before complete ownership would be unsafe. Date: 2026-10-05.

## Outcomes & Retrospective


The shared lifecycle is implemented. The original restricted-execution regression fails before repair; 56 distinct Rust tests pass after repair, including real unistore ANALYZE and separate SQL KILL sessions. All-target checking, lint and changed-region formatting pass. An intermediate regression exposed missing kill reset across pooled-session reuse and drove the Go tracking-boundary reset. Review also serialized interruption/reset and made test evidence survive the source panic-recovery wrapper. N04/I01/O09 remain partial; counts remain 86 tracked /30 repaired /56 unresolved (27 open, 29 partial). Other53 roots were not freshly re-audited. Actual commit-hook and pre-push build results follow in /workspace/.cloud-setup/internal-process-batch/final-handoff.json and saved Cloud startup instructions; full Go suites, multi-node/GC, performance and fresh restore remain unverified.
