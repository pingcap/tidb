# Nextgen Cross-KS Limited DDL From SYSTEM Keyspace (v1)

## Summary

Enable SYSTEM-keyspace runtime to execute a tightly allowlisted DDL on a user keyspace table (`ALTER TABLE MODE` only in v1), without enabling full DDL owner/worker in SYSTEM for that target keyspace.

This is for the `IMPORT INTO` path first, with hybrid routing:
1. If current session keyspace already equals target keyspace, keep local DDL path.
2. If keyspaces differ, use a SYSTEM-side cross-keyspace DDL submit proxy.

The proxy is submit-only (no local owner/worker), initialized lazily per target keyspace, and kept alive. Wait behavior follows current `DoDDLJobWrapper` semantics (polling without fixed cap).

## Alternatives Considered

1. Full per-keyspace DDL bootstrap in SYSTEM (owner/worker/scheduler).
- Pros: minimal adaptation at call sites, standard DDL path.
- Cons: high blast radius, heavy runtime, and conflicts with `ddl.NewDDL` global side effects (global task registration and `variable.EnableDDL/DisableDDL/SwitchMDL` reassignment). Rejected for v1.

2. Submit-only cross-KS DDL proxy (recommended).
- Pros: smallest safe surface, matches allowlist intent, no worker startup in SYSTEM, can reuse existing submission semantics.
- Cons: requires a focused refactor in `pkg/ddl` to expose side-effect-free submit/wait building blocks.

3. Direct metadata mutation (skip DDL job framework).
- Pros: simplest code.
- Cons: breaks DDL contracts (job tracking, schema propagation, dependency handling). Rejected.

## Key Implementation Changes

### 1) Add a side-effect-free DDL submit/wait path for allowlisted jobs

Create a lightweight submit runtime in `pkg/ddl` that reuses existing job submission and waiting behavior but does not call `NewDDL` and does not register global hooks/factories.

- Introduce an internal/exported helper focused on:
  - submit `JobWrapper` into target keyspace DDL job table with existing checks.
  - notify owner via existing notify mechanism.
  - wait for completion via history polling semantics aligned with `DoDDLJobWrapper`.
- Keep this runtime scoped to allowlisted internal DDL usage; do not expose generic SQL DDL execution.
- Ensure it supports caller-provided schema/table names for `InvolvingSchemaInfo` and job metadata.

### 2) Extend cross-keyspace manager with lazy DDL proxy runtime

In `pkg/domain/crossks`, add per-target-keyspace DDL proxy state alongside existing `SessionManager` resources.

- Lazy init on first call; keep alive with existing manager lifecycle.
- Reuse target keyspace resources already created by cross-KS manager:
  - target store
  - target etcd client
  - cross-KS internal session pool
- Do not start owner/worker/job scheduler in SYSTEM for target keyspace.
- Wire clean shutdown with `SessionManager.close()`.

### 3) Add a restricted domain-level API for cross-KS allowlisted DDL

Add a domain API (used by internal components) that executes allowlisted cross-KS DDL, starting with `AlterTableMode` only.

- Request payload includes:
  - target keyspace
  - schema/table IDs
  - schema/table names (caller-provided in v1)
  - target table mode
- Enforce allowlist centrally (internal only; no SQL-layer relaxation).
- Keep planner guard unchanged: cross-KS SQL sessions still reject DDL.

### 4) Integrate v1 caller: `IMPORT INTO`

Update `pkg/dxf/importinto` to use hybrid routing for table mode switches.

- `job.go`:
  - On entering import mode, use local DDL when same keyspace; use cross-KS proxy when different keyspace.
- `scheduler.go` and `clean_up.go`:
  - For reset-to-normal mode, use same hybrid routing.
  - Preserve current cleanup behavior for table-not-exists handling.
- Remove/replace current `kerneltype.IsClassic()` gating for table mode transitions in this path.

## Test Plan

1. DDL submit proxy unit tests (`pkg/ddl` / `pkg/domain/crossks`):
- Submit allowlisted `AlterTableMode` job through proxy runtime and verify:
  - job enqueued in target keyspace tables
  - completion observed correctly
  - no full DDL bootstrap side effects are required.
- Reject non-allowlisted DDL type through proxy API.

2. Cross-KS manager tests:
- Lazy init once per keyspace.
- Repeated calls reuse runtime.
- Close path releases proxy resources with manager close.

3. `IMPORT INTO` integration tests:
- Nextgen + keyspace mismatch scenario:
  - entering import mode succeeds via SYSTEM-side cross-KS path.
  - done/cancel/cleanup transitions restore normal mode.
- Same-keyspace scenario keeps local path and still works.
- Preserve existing expected behavior on table-not-exists during cleanup.

4. Regression coverage:
- Existing cross-KS SQL DDL rejection remains unchanged.
- Existing system-table-only cross-KS visibility remains unchanged.

## Assumptions and Defaults

- v1 allowlist: `AlterTableMode` only.
- v1 caller scope: `IMPORT INTO` only.
- Routing policy: hybrid (local when same keyspace, proxy when different).
- Job naming metadata source: caller-provided schema/table names.
- Wait policy: no fixed timeout cap (consistent with current `DoDDLJobWrapper` behavior).
- No proactive owner-availability precheck; submit and wait behavior is authoritative.
