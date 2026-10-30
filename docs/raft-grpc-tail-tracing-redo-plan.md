# Re-run SQL tail diagnosis with runtime recording controls

This ExecPlan is maintained according to repository-root `PLANS.md`. Keep Progress, Surprises & Discoveries, Decision Log, and Outcomes & Retrospective current.

## Purpose / Big Picture

The user requests a complete rerun of recorder-enabled IDC comparisons and an updated Chinese report. The previous diagnostic mode changed shared Raft commit-log P999 from approximately 39–44 ms to 1–2 ms, while the recorder-disabled run reproduced the original behavior. Recorded numbers exist in raw Prometheus buckets and raw client WriteDetail, but the cause of this change is unresolved. We must establish recording comparability before attributing original SQL tails from the new traces.

Use the existing IDC cluster, original data and load. Runtime controls must permit stopping and starting additional diagnostic events without restarting TiDB/TiKV or recreating their gRPC connections. Preserve existing tracing behavior instead of replacing the old sink or forcing TiKV trace-control flags to zero. The outcome is new validated comparison data, figures, tables and conclusions at https://pingcap.feishu.cn/wiki/MGSjwKITJi90dEkb3XVc2lI8neg.

## Progress

- [x] (2026-10-15 16:52Z) Confirm existing namespace and pods remain available; all business TiDB/TiKV and benchmark containers have zero restarts since the previous run.
- [x] Preserve previous source revisions and proposed controls in `/Users/hjt110/experiments/raft-grpc-tail-trace-20261016/redo-state.json`.
- [x] Add failing regression: original flags 0xe become 0 with old diagnostic mode; fixed regression and the full traceevent package pass with the race detector.
- [x] Implement runtime controls and independent TiDB diagnostic event tap; preserve legacy sinks, modes, categories and dump triggers.
- [x] Preserve client stream correlation across capture toggles; client-go race tests pass; grpc-rs runtime/native tests are being built.
- [ ] Complete required formatting, dependency/build preparation, targeted tests and CSE lint.
- [ ] Update existing build-only PR branches and submit PR-mode EE ChatOps builds once.
- [ ] Verify immutable image source SHA/digest/NextGen and deploy to existing IDC test cluster.
- [ ] Run fixed-process, independent TiDB/TiKV recording ablations and select a representative recording level.
- [ ] Rerun three configurations, two read concurrencies and three repetitions with adjacent recorder-off controls; repeat kernel diagnosis.
- [ ] Audit raw data, replace report figures/tables/analysis with actual new results, and retain historical data.

## Surprises & Discoveries

Validation note: `make lint` passes. `make bazel_prepare` was attempted but this Mac has no `bazel` executable; Go builds and targeted/package tests remain available. No generated Bazel metadata has been hand-edited.

The old diagnostic mode does more than emit extra events: `initDiagnosticRecorderFromEnv` installs a new global sink and disables old recorder/logging flags; `handleTraceControlExtractor` returns zero; dump triggers are bypassed. These differences are not acceptable confounds for a pure recording comparison. However, current live `@@global.tidb_trace_event` is empty in both business TiDB processes, and historical effective settings were not saved, so legacy logging is not an established explanation for the commit-log change.

Native CQ readiness is recorded by a callback in C-core before a completion becomes visible. A completion queue (CQ) is consumed by a gRPC poll thread. Recording after CQ pop alone misses receive/IO progress that has not yet produced a completion. Request IDs are only unique within a stream; stream nonces must survive off/on transitions without reconnecting streams.

## Decision Log

- Decision: Reuse the existing independent diagnostic branches and build-only PRs CSE #6581 and TiDB #72001; do not modify original isolation PRs CSE #6449 or grpc-rs #679.
  Rationale: The user requests runnable testing images and IDC results, not new delivery PRs.
  Date/Author: 2026-10-16, Codex.
- Decision: Control event recording with startup-configured files `GRPCIO_TRACE_CONTROL` and `TIDB_CQ_TRACE_CONTROL`, accepting `off`/`on`; initialize configured capture off and acknowledge the active state in trace statistics.
  Rationale: A file can be changed using existing IDC exec access without adding a server API. Missing or invalid control state fails closed. No restarts are required for capture toggles.
  Date/Author: 2026-10-16, Codex.
- Decision: Maintain stream nonce metadata and stream_bind lifecycle correlation independently of event recording; gate hot event emission at runtime.
  Rationale: Recreating connections to acquire correlation would invalidate the fixed-binding comparison. Off mode may emit low-volume headers, bindings and control statistics, which must be disclosed.
  Date/Author: 2026-10-16, Codex.
- Decision: First compare both sides off, TiDB-only capture, both off, TiKV-only capture, full capture and both off on shared c16. If full recording materially changes the distributions, reduce capture volume before the formal attribution matrix.
  Rationale: Separate causes instead of accepting a convenient result or assuming instrumentation is harmless.
  Date/Author: 2026-10-16, Codex.

## Context and Orientation

TiDB worktree: `/Users/hjt110/pingcap/worktrees/raft-grpc-trace/tidb`, starting at bbb4093623546bbf244ae83cce413407318e3d43. Its client-go dependency worktree is sibling `client-go`, starting at 59f7293d7dd3f3c3f6470f3e7333109dcc158870. TiKV worktree is `/Users/hjt110/.codex/worktrees/raft-grpc-latency-trace/cloud-storage-engine`, starting at 5eac7a289a0fee21e913288f47181400188811cd. Its grpc-rs dependency is `/Users/hjt110/pingcap/worktrees/raft-grpc-trace/grpc-rs`, starting at f5882d0ebb45f9184b458a88af58fc68443141b8. C-core remains pinned at e15464cffa052bb6965fc9b96ad541ac9ff9f1c0 unless a native ABI change proves necessary.

IDC namespace is `raft-grpc-tail-trace-idc-20261015-tps-8237805-1-668`, kubeconfig `/Users/hjt110/experiments/raft-grpc-tail-trace-20261015/idc-deploy/kubeconfig.yml`, current expiry 2026-10-16 17:43:56 UTC+8. Use explicit namespace/config for all operations. Deployment metadata, pinned images and workload scripts are under the previous experiment directory. New artifacts go only under `/Users/hjt110/experiments/raft-grpc-tail-trace-20261016`.

## Plan of Work

In `pkg/util/traceevent/diagnostic.go`, retain an independent diagnostic sink rather than replacing `CurrentSink` or old recording flags. Poll an optional control path on the writer goroutine. `diagnosticEnabled` reflects runtime event capture. Split category checks in `traceevent.go` so additional capture categories do not cause those events to enter the legacy flight recorder. Remove the diagnostic overrides in `adapter.go` and `flightrecorder.go`. Keep existing legacy event handling intact when legacy categories are actually enabled.

In grpc-rs `src/diagnostics.rs`, separate configured recorder availability from active event recording. Gate record submission with an atomic runtime flag, keep stream_bind lifecycle records, poll control files on the writer thread, and register/unregister the existing native callback when recording changes. A nullable Rust function pointer can use the existing C setter ABI. Include active state in statistics. Preserve the old startup-only behavior when no control-file environment is supplied so existing test probes remain usable.

In client-go `internal/client/diagnostics.go`, generate a configured stream nonce independently of current capture-category activation. Per-request events remain gated by TiDB category activation. This makes existing streams identifiable when capture starts later.

Pin the new dependency revisions in the existing TiDB/CSE diagnostic branches. Preserve source parity between Mac worktrees, Linux build-validation copies and CI. Git commits use signoff. No multiple Cargo commands run concurrently.

## Concrete Steps

Run targeted regression tests first on the unfixed TiDB source to record a genuine failure. Follow `docs/agents/testing-flow.md` and the WIP/Ready verification profiles. Module or import changes trigger `make bazel_prepare`. Test runtime off/on/off, unchanged legacy trace flags/sinks, nonce continuity, queue drops, and disabled capture. Use Linux dev only for compilation and unit/smoke validation; all performance loads run in IDC.

After code validation, push only the independent test branches and reuse existing PR-mode EE ChatOps builds. Read the lark-im skill before sending the already-authorized ChatOps commands. Save build IDs immediately, never blindly duplicate submissions. Verify image digest, source revision, linux/amd64, entrypoint and NextGen before patching IDC workloads.

The original load is one writer in keyspace_a, target100 transactions/s, BEGIN→UPDATE one fixed row→COMMIT, 1PC/async-commit off; keyspace_b continuously drains full 64 MiB query results at c8 or c16. Each measured trial has 30-second baseline, 30-second mixed load and 30-second recovery. Use the unchanged Go read generator and writer, fixed Region bounds 1892/1900 and three voters; business leaders go to store1393. Disable PD merge and pause balancing only during trials. Preserve 8 CPU TiKV limit and actual CQ settings 5/0, 4/1, 5/1. Record effective old tracing settings in every trial, PID/start time and process/stream continuity across recording toggles.

The formal design has three balanced configuration blocks per mode and c8/c16 cells. In each cell, run off/on/off with no process restart within the triplet. Pool raw samples and histogram bucket increments only within identical conditions; never average quantiles or subtract unrelated P99 values. If recording changes the failure mechanism, do not call its stage breakdown representative of the unrecorded workload. Reduce scope/rate of capture and validate again instead.

## Validation and Acceptance

Behavioral acceptance is that recording can be disabled, enabled and disabled in the same processes with the same live streams; old tracing flags and output policy remain unchanged; full trace joins have no drops or invalid clocks; and any measured capture effect is explicitly quantified. There is no requirement that a particular Raft or SQL latency improve. New raw data must explain which conclusions are measured directly and which remain hypotheses. All performance figures and tables in the report must use the new data or clearly identify retained history.

## Idempotence and Recovery

Never overwrite previous trial directories or source changes not made in this task. Save control transitions and build/deployment state. A failed capture must remain recorded as a failed capture; do not quietly drop unfavorable samples. Runtime controls fail closed. Restore off state and balancing schedulers after a failed trial. Existing cluster images and lease receipts remain in the previous deployment directory for recovery. Do not merge draft PRs, create duplicate builds or modify unrelated clusters.

## Artifacts and Notes

The independent audit is `/Users/hjt110/experiments/raft-grpc-tail-trace-20261015/comparison/anomaly-audit-20261016/raw-audit.json`. It checks raw counters using a separate parser, full90-second windows, actual shared5/raft0 threads, and raw client details. It establishes that the low numbers exist in captured data, not why diagnostic mode differs. Current report revision44 adds that limitation.

## Interfaces and Dependencies

Runtime files contain exactly `off` or `on`, are written atomically by the IDC controller, and are polled within a bounded interval. Active state must be observable in trace_stats so the harness waits for acknowledgement before warming and measuring. `enabled()` in grpc-rs continues to mean configured availability for lifecycle binding; add an explicit active-recording accessor internally. Keep source IDs, stream nonces and protobuf semantics unchanged across toggles. Native readiness callback is disabled in runtime off mode.

## Outcomes & Retrospective

Implementation and rerun are in progress. No new benchmark has run yet. The previous results remain preserved and are not considered replaced until new trials and report readback are verified.
