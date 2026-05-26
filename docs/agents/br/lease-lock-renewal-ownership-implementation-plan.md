# Lease Lock Renewal Ownership Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 收敛 lease lock renewal ownership，让业务代码不再直接调用 `RemoteLock.StartRenewal`，而是在 acquire 成功后由 lifecycle lock API 立即启动 renewal。

**Architecture:** `TryLockRemoteRead`、`TryLockRemoteWrite`、`TryLockRemoteTruncate` 保持 single-attempt primitive，只负责尝试创建 lock instance，不启动 renewal。`LockWithRetry` 和新的 truncate lifecycle wrapper 是业务可用 acquire API，要求非 nil `onLeaseLost` callback，成功拿到 lock 后内部启动 renewal。BR 业务调用方负责决定 lease lost 时取消哪一段工作，并把对应 callback 传给 lock API。

**Tech Stack:** Go, `pkg/objstore`, `br/pkg/stream`, `br/pkg/restore/log_client`, `br/pkg/task`, failpoint-enabled tests, TiDB `make bazel_prepare` / `make lint` validation.

---

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan must be maintained according to it.

## Purpose / Big Picture

Lease lock 的安全性不只取决于 acquire 是否互斥，也取决于 holder 在 critical section 期间是否持续刷新 `ExpireAt`。当前分支里业务代码直接调用 `lock.StartRenewal(ctx, cancelFn)`，这把 lock 层内部续租 goroutine 暴露给了业务方，也让 acquire 和 renewal ownership 分离。`GetLockedMigrations` 当前先 acquire read lock，再 load migrations，最后由 `restoreStream` 调用 `StartRenewal`；这使 migration load 和 `BuildMigrations` 处在 renewal 启动之前。

完成本计划后，生产业务代码里不再直接出现 `.StartRenewal(`。业务可用 acquire API 成功拿到 lease lock 后会立即启动 renewal；如果 renewal 发现 lease lost，它只调用业务传入的 `onLeaseLost` callback。业务层仍然决定取消谁，例如传入当前 critical section 的 child context cancel function。`StartRenewal` 的调度策略保持不变：不做 acquire 后的 immediate renew，仍然等待 `renewInterval` 后第一次刷新，因为 acquire 已经写入 fresh `ExpireAt`。

## Progress

- [x] (2026-05-26) 通过 design grilling 明确 API 边界：`Try*` 是 primitive，`Lock*` 是 lifecycle API，生产业务代码不直接调用 `StartRenewal`。
- [x] (2026-05-26) 明确 `onLeaseLost` 语义：public lifecycle API 在 acquire 前拒绝 nil callback；测试可显式传 no-op callback。
- [x] (2026-05-26) 明确本轮不把 `TryLockRemoteRead` / `TryLockRemoteWrite` 私有化，只补充 primitive 注释和生产调用约束。
- [x] (2026-05-26) 更新 objstore renewal API 和 tests。
- [x] (2026-05-26) 更新 BR migration/restore/truncate 调用点和 tests。
- [x] (2026-05-26) 更新 `docs/agents/br/lease-lock-business-review-notes.md`，记录 renewal ownership 已收敛和 immediate renew 已评估不改。
- [x] (2026-05-26) 运行 WIP 和 Ready validation，并把结果记录到本计划的 `Outcomes & Retrospective`。

## Surprises & Discoveries

- Observation: 生产代码没有直接调用 `TryLockRemoteRead` 或 `TryLockRemoteWrite` 来 acquire lock；它们只作为 `LockWithRetry` 的 `Locker` 参数出现。
  Evidence: `rg -n "TryLockRemote(Read|Write)\\(" br pkg -g '*.go' -g '!**/*_test.go'` 只命中 `pkg/objstore/locking.go` 中的函数定义。
- Observation: `TryLockRemoteTruncate` 是生产业务直接调用的 single-attempt primitive。
  Evidence: `br/pkg/task/stream.go` 的 `RunStreamTruncate` 当前先调用 `CleanUpStaleTruncateLock`，再调用 `TryLockRemoteTruncate`，之后显式调用 `StartRenewal`。
- Observation: `br/pkg/stream`, `br/pkg/restore/log_client`, `br/pkg/task`, and `pkg/objstore` all contain failpoint imports or failpoint-instrumented code. Targeted package tests should use `./tools/check/failpoint-go-test.sh`.
  Evidence: `rg -n "failpoint" br/pkg/stream br/pkg/restore/log_client br/pkg/task pkg/objstore -g '*.go'` reports imports and injected failpoints in these packages.

## Decision Log

- Decision: `RemoteLock.StartRenewal` should no longer be exposed to production business packages.
  Rationale: Renewal is an internal lease-lock lifecycle mechanism. Business code should pass an `onLeaseLost` callback to acquire APIs, not manually start the renewal goroutine.
  Date/Author: 2026-05-26 / design session.
- Decision: `TryLockRemoteRead`, `TryLockRemoteWrite`, and `TryLockRemoteTruncate` remain single-attempt primitives and do not start renewal.
  Rationale: `Try*` functions are low-level building blocks for tests and lifecycle wrappers. Keeping them primitive avoids mixing retry/acquire/renewal ownership.
  Date/Author: 2026-05-26 / design session.
- Decision: `LockWithRetry` becomes a lifecycle acquire API by adding a required `onLeaseLost func()` parameter.
  Rationale: All production read/write migration locks are acquired through `LockWithRetry`; starting renewal there prevents call sites from forgetting to renew after acquire.
  Date/Author: 2026-05-26 / design session.
- Decision: Add a truncate lifecycle wrapper instead of changing `TryLockRemoteTruncate` semantics.
  Rationale: Truncate does not use `LockWithRetry`, but production code should still use a lifecycle API that starts renewal. Keeping `TryLockRemoteTruncate` primitive makes `Try*` behavior consistent.
  Date/Author: 2026-05-26 / design session.
- Decision: Public lifecycle acquire APIs reject nil `onLeaseLost` before touching remote storage.
  Rationale: A nil callback would allow lease loss without canceling business work, which risks false success. Failing before acquire avoids creating a lock that then needs cleanup.
  Date/Author: 2026-05-26 / design session.
- Decision: Do not change `StartRenewal` to immediate renew/verify.
  Rationale: Acquire already writes fresh `ExpireAt`; waiting `renewInterval` before the first refresh is the normal renewal schedule. The correctness issue is late ownership start, not the first renewal cadence.
  Date/Author: 2026-05-26 / design session.
- Decision: `LockedMigrations` should hide its internal `RemoteLock` and expose `Unlock(ctx)` instead of `ReadLock`.
  Rationale: `GetLockedMigrations` returns a locked resource. Callers should release that resource without depending on the internal lock field or renewal mechanism.
  Date/Author: 2026-05-26 / design session.

## Outcomes & Retrospective

- 2026-05-26 / Milestone 1: Implemented objstore lifecycle acquire ownership. `RemoteLock.StartRenewal` is now unexported as `startRenewal`; `LockWithRetry` requires `onLeaseLost` and starts renewal after acquire; `LockRemoteTruncate` wraps the truncate primitive without retry or cleanup. Added nil-callback rejection tests and a renewal-start test. Verified with:

      ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLockWithRetry|TestLockRemoteTruncate|TestStartRenewal|TestTryRenew|TestUnlock' -count=1

- 2026-05-26 / Milestone 2: Migrated BR production call sites. `RunStreamTruncate` uses `LockRemoteTruncate`; `GetLockedMigrations` accepts the lease-loss callback and starts renewal before loading migrations; `restoreStream` no longer starts renewal directly; `MergeAndMigrateTo` and `AppendMigration` pass child-context cancel functions into lock acquisition. Added tests for renewal during blocked migration load and renewal of both append locks. Verified with:

      ./tools/check/failpoint-go-test.sh br/pkg/restore/log_client -run 'TestGetLockedMigrationsReleasesReadLockOnLoadError|TestGetLockedMigrations.*Renew' -count=1

      ./tools/check/failpoint-go-test.sh br/pkg/stream -run 'TestMergeAndMigrateToRenewsWriteLock|Test.*AppendMigration' -count=1

      ./tools/check/failpoint-go-test.sh br/pkg/stream -run 'TestLockForAppendRenewsBothLocks' -count=1

      ./tools/check/failpoint-go-test.sh br/pkg/task -run TestNonExistent -count=1

- 2026-05-26 / Execution note: interrupted or repeated failpoint test runs can leave generated failpoint rewrites or unrelated deleted files in the working tree. During this implementation, `make failpoint-disable` and targeted `git restore` were needed to remove those test-workflow artifacts before continuing.

- 2026-05-26 / Milestone 3 and Ready validation: Updated the business review notes and ran static ownership checks. Production code has no direct `.StartRenewal(` calls; production `LockWithRetry` calls all pass callbacks; production direct `TryLockRemote*` calls are limited to primitive definitions and the `LockRemoteTruncate` wrapper. The `docs/agents/agents-review-guide.md` checks were completed by verifying no wrapped normative keywords in the changed agent docs, checking referenced agent-doc paths exist, and confirming `git diff --check` is clean. `make bazel_prepare` was required because this change added top-level tests in existing Go test files; it completed without producing Bazel metadata changes. `make lint` passed.

- 2026-05-26 / Review follow-up: Independent review found that `LockRemoteTruncate` had nil-callback coverage but no positive renewal coverage. Added `TestLockRemoteTruncateStartsRenewal` and reran:

      ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLockWithRetry|TestLockRemoteTruncate|TestStartRenewal|TestTryRenew|TestUnlock' -count=1

  The same review also found that the Ready `br/pkg/stream` regex did not include `TestLockForAppendRenewsBothLocks`; the Ready command below now includes that test explicitly. After adding the new top-level objstore test, reran `make bazel_prepare` and `make lint`; both passed, and `make bazel_prepare` still produced no Bazel metadata changes.

## Context and Orientation

The main lock implementation lives in `pkg/objstore/locking.go` and `pkg/objstore/locking_helper.go`.

Important terms:

- `RemoteLock`: the handle returned after a lock instance is acquired. It stores the physical object path, the storage, the transaction ID, and renewal goroutine state.
- `tryRenew`: an unexported method on `RemoteLock` that reads the lock file, verifies the `TxnID`, checks `ExpireAt`, and writes a later `ExpireAt`.
- `StartRenewal`: currently an exported method that starts the background renewal loop. This plan renames it to unexported `startRenewal`.
- `onLeaseLost`: a callback supplied by business code. The lock layer calls it when renewal determines the lease is permanently lost or renewal retries are exhausted. The lock layer does not decide which context to cancel.
- `onLeaseLost` callback constraint: callbacks must be fast and non-blocking. In production BR paths they should normally be `context.CancelFunc` values or similarly small notification closures. A callback must not call `Unlock` on the same `RemoteLock`, because the renewal goroutine invokes the callback synchronously and `Unlock` waits for that goroutine to exit.
- `Try* primitive`: a single-attempt acquire function. It writes one lock instance through `conditionalPut` and returns success or conflict without retry or renewal.
- `Lifecycle acquire API`: a public function intended for production business code. It acquires a lock and starts renewal before returning the handle.

Original production call paths before this plan:

- `br/pkg/task/stream.go::RunStreamTruncate` calls `CleanUpStaleTruncateLock`, then `TryLockRemoteTruncate`, then `lock.StartRenewal(ctx, cancelFn)`.
- `br/pkg/task/stream.go::restoreStream` calls `client.GetLockedMigrations(ctx)`, then `client.BuildMigrations`, then `migs.ReadLock.StartRenewal(ctx, cancelFn)`.
- `br/pkg/stream/stream_metas.go::MergeAndMigrateTo` calls `LockWithRetry(... TryLockRemoteWrite ...)`, creates a child context, then calls `lock.StartRenewal(workCtx, cancel)`.
- `br/pkg/stream/stream_metas.go::AppendMigration` calls `lockForAppend`, which acquires a migration read lock and append write lock through `LockWithRetry`; it currently does not start renewal.

After this plan, all four production paths should acquire lifecycle locks that already renew:

- `RunStreamTruncate` uses a new truncate lifecycle wrapper.
- `restoreStream` calls `GetLockedMigrations(ctx, cancelFn)`.
- `MergeAndMigrateTo` passes its child context cancel function to `LockWithRetry`.
- `AppendMigration` creates a child context and passes its cancel function to both locks acquired in `lockForAppend`.

## Interfaces and Dependencies

In `pkg/objstore/locking.go`, keep the `Locker` primitive unchanged:

    type Locker = func(ctx context.Context, storage storeapi.Storage, path, hint string) (*RemoteLock, error)

Rename exported renewal method:

    func (l *RemoteLock) startRenewal(ctx context.Context, onLeaseLost func())

The method body should be the current `StartRenewal` body with the same panic-on-double-start behavior and the same renewal interval schedule.

Add a validation helper near renewal/acquire helpers:

    func validateOnLeaseLost(onLeaseLost func()) error {
        if onLeaseLost == nil {
            return errors.New("onLeaseLost callback is required for lease lock renewal")
        }
        return nil
    }

Change `LockWithRetry` signature:

    func LockWithRetry(
        ctx context.Context,
        locker Locker,
        storage storeapi.Storage,
        lockPath string,
        hint string,
        onLeaseLost func(),
    ) (*RemoteLock, error)

At function entry, call `validateOnLeaseLost(onLeaseLost)` before retry state creation or storage access. After `locker` succeeds and before returning, call:

    lock.startRenewal(ctx, onLeaseLost)
    return lock, nil

Add a truncate lifecycle wrapper while keeping `TryLockRemoteTruncate` primitive:

    func LockRemoteTruncate(
        ctx context.Context,
        storage storeapi.Storage,
        hint string,
        onLeaseLost func(),
    ) (*RemoteLock, error) {
        if err := validateOnLeaseLost(onLeaseLost); err != nil {
            return nil, err
        }
        lock, err := TryLockRemoteTruncate(ctx, storage, hint)
        if err != nil {
            return nil, err
        }
        lock.startRenewal(ctx, onLeaseLost)
        return lock, nil
    }

`LockRemoteTruncate` must not call `CleanUpStaleTruncateLock`, must not retry, and must not call `LockWithRetry`. `RunStreamTruncate` remains responsible for the existing cleanup-once step before calling the wrapper. This preserves the current truncate behavior: cleanup once, acquire once, return on conflict.

Update comments for primitive functions:

    // TryLockRemoteWrite is a single-attempt primitive for LockWithRetry.
    // It does not retry and does not start renewal.

    // TryLockRemoteRead is a single-attempt primitive for LockWithRetry.
    // It does not retry and does not start renewal.

    // TryLockRemoteTruncate is a single-attempt primitive. Production truncate
    // callers should use LockRemoteTruncate so renewal starts after acquire.

In `pkg/objstore/export_test.go`, replace any test-only wording that mentions exported `StartRenewal` and expose unexported `startRenewal` for external package tests:

    func TESTStartRenewal(ctx context.Context, l *RemoteLock, onLeaseLost func()) {
        l.startRenewal(ctx, onLeaseLost)
    }

Existing `TESTStopRenewal`, `TESTTryRenew`, `TESTSetLeaseConstants`, and `TESTSetNow` remain useful.

In `br/pkg/stream/stream_metas.go`, update signatures:

    func (m *MigrationExt) GetReadLock(ctx context.Context, hint string, onLeaseLost func()) (*objstore.RemoteLock, error)

    func (m MigrationExt) lockForAppend(ctx context.Context, hint string, onLeaseLost func()) (
        readLock, appendLock *objstore.RemoteLock, err error)

Call `LockWithRetry` with the new callback argument.

In `br/pkg/restore/log_client/client.go`, update locked resource API:

    type LockedMigrations struct {
        Migs     []*backuppb.Migration
        readLock *objstore.RemoteLock
    }

    func (m *LockedMigrations) Unlock(ctx context.Context) error {
        return m.readLock.Unlock(ctx)
    }

    func (rc *LogClient) GetLockedMigrations(ctx context.Context, onLeaseLost func()) (ret *LockedMigrations, retErr error)

`GetLockedMigrations` should call `ext.GetReadLock(ctx, "restore stream", onLeaseLost)` before `ext.Load(ctx)`. Its error defer should still call `readLock.UnlockOnCleanUp(ctx)` if `retErr != nil`.

## Plan of Work

Milestone 1 changes the objstore API and tests. It makes renewal internal, adds lifecycle validation, introduces `LockRemoteTruncate`, and proves `LockWithRetry` starts renewal without direct business calls to `StartRenewal`.

Milestone 2 migrates BR production call sites. It updates restore, truncate, `MergeAndMigrateTo`, and `AppendMigration` so every production lease lock supplies an `onLeaseLost` callback at acquire time.

Milestone 3 updates docs and runs static API-boundary checks. It records that immediate renew was evaluated and intentionally left unchanged, and verifies production code no longer calls `.StartRenewal(`.

## Milestone 1: Objstore Renewal Ownership API

Scope: update `pkg/objstore/locking.go`, `pkg/objstore/locking_helper.go`, `pkg/objstore/export_test.go`, and `pkg/objstore/locking_test.go`.

After this milestone, `LockWithRetry` requires a non-nil `onLeaseLost`, `LockRemoteTruncate` exists, and objstore tests prove lifecycle acquire starts renewal.

Concrete steps:

- [x] Rename `RemoteLock.StartRenewal` to `RemoteLock.startRenewal` in `pkg/objstore/locking.go`. Keep the current state-machine behavior and panic-on-double-start behavior unchanged.
- [x] Add `validateOnLeaseLost(onLeaseLost func()) error` in `pkg/objstore/locking_helper.go` or near `LockWithRetry` in `pkg/objstore/locking.go`. Use the exact error text `onLeaseLost callback is required for lease lock renewal` so tests can assert it.
- [x] Change `LockWithRetry` to accept `onLeaseLost func()`. Validate it before any retry/acquire work. On successful acquire, call `lock.startRenewal(ctx, onLeaseLost)` before returning.
- [x] Add `LockRemoteTruncate(ctx, storage, hint, onLeaseLost)` and keep `TryLockRemoteTruncate` primitive.
- [x] Add or update comments on `TryLockRemoteRead`, `TryLockRemoteWrite`, and `TryLockRemoteTruncate` to say they do not start renewal.
- [x] Update `pkg/objstore/export_test.go` with `TESTStartRenewal`.
- [x] Update existing renewal tests in `pkg/objstore/locking_test.go` to call `objstore.TESTStartRenewal(ctx, lock, callback)` instead of `lock.StartRenewal(...)`.
- [x] Add `TestLockWithRetryRejectsNilOnLeaseLost` in `pkg/objstore/locking_test.go`. It should call `LockWithRetry(ctx, TryLockRemoteWrite, strg, "v1/LOCK", "owner", nil)`, assert the required-callback error, and assert no `v1/LOCK.WRIT.` object exists.
- [x] Add `TestLockRemoteTruncateRejectsNilOnLeaseLost` in `pkg/objstore/locking_test.go`. It should call `LockRemoteTruncate(ctx, strg, "truncate", nil)`, assert the required-callback error, and assert no `truncating.lock.` object exists.
- [x] Add `TestLockWithRetryStartsRenewal` in `pkg/objstore/locking_test.go`. Use `TESTSetLeaseConstants` to set a short TTL and interval, acquire through `LockWithRetry(..., func(){})`, read the physical lock path, and assert `ExpireAt` advances after at least one renewal interval without direct `TESTStartRenewal`.
- [x] Update all objstore tests that call `LockWithRetry` to pass `func() {}` unless they intentionally test lease-loss callback behavior.

Run:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLockWithRetry|TestLockRemoteTruncate|TestStartRenewal|TestTryRenew|TestUnlock' -count=1

Expected: all selected objstore tests pass. This plan adds new top-level test functions, so `make bazel_prepare` is required before Ready validation.

## Milestone 2: BR Production Call Sites

Scope: update `br/pkg/task/stream.go`, `br/pkg/restore/log_client/client.go`, `br/pkg/restore/log_client/client_test.go`, `br/pkg/stream/stream_metas.go`, and `br/pkg/stream/stream_metas_test.go`.

After this milestone, production BR code no longer calls `.StartRenewal(` and every production `LockWithRetry` call supplies a non-nil callback.

Concrete steps:

- [x] In `br/pkg/task/stream.go::RunStreamTruncate`, replace `TryLockRemoteTruncate` plus `StartRenewal` with `LockRemoteTruncate(ctx, extStorage, hintOnTruncateLock, cancelFn)`. Keep `CleanUpStaleTruncateLock` and the existing cleanup defer behavior unchanged.
- [x] In `br/pkg/restore/log_client/client.go`, change `LockedMigrations.ReadLock` to private `readLock` and add `Unlock(ctx)` as described in `Interfaces and Dependencies`.
- [x] Change `LogClient.GetLockedMigrations` to accept `onLeaseLost func()`. It should call `ext.GetReadLock(ctx, "restore stream", onLeaseLost)` before `ext.Load(ctx)`.
- [x] In `br/pkg/task/stream.go::restoreStream`, call `client.GetLockedMigrations(ctx, cancelFn)`, remove direct `migs.ReadLock.StartRenewal`, and replace the cleanup registration with `defer utils.WithCleanUp(&err, time.Minute, migs.Unlock)` inside the existing `if !skipCleanup` block. Do not make cleanup unconditional; the `skip-migration-read-lock-cleanup` failpoint must keep its current behavior.
- [x] Before replacing `cleanUpWithRetErr`, note the error ordering difference: the local helper combines as `multierr.Combine(*errOut, err)`, while `utils.WithCleanUp` combines as `multierr.Combine(err, *errOut)`. Existing tests should not assert the combined error string order. If an implementation test depends on order, keep the local helper for this cleanup path instead of changing semantics.
- [x] If `cleanUpWithRetErr` in `br/pkg/task/stream.go` has no remaining callers, delete it and remove the now-unused `multierr` import from that file.
- [x] In `br/pkg/stream/stream_metas.go::GetReadLock`, add `onLeaseLost func()` and pass it to `objstore.LockWithRetry`.
- [x] In `br/pkg/stream/stream_metas.go::MergeAndMigrateTo`, create `workCtx, cancel := context.WithCancel(ctx)` before calling `LockWithRetry`. Pass `workCtx` and `cancel` into `LockWithRetry`, remove direct `lock.StartRenewal`, and ensure subsequent critical-section work still uses `workCtx`. This order matters: if `LockWithRetry` receives the parent `ctx`, the renewal goroutine is not tied to the critical-section child context.
- [x] In `br/pkg/stream/stream_metas.go::lockForAppend`, add `onLeaseLost func()` and pass it to both `LockWithRetry` calls.
- [x] In `br/pkg/stream/stream_metas.go::AppendMigration`, create `workCtx, cancel := context.WithCancel(ctx)`, defer `cancel()`, call `m.lockForAppend(workCtx, "AppendMigration", cancel)`, and use `workCtx` for the protected load/sequence/write work after both locks are acquired.
- [x] Update `br/pkg/restore/log_client/client_test.go::TestGetLockedMigrationsReleasesReadLockOnLoadError` to pass `func(){}` or a test callback to `GetLockedMigrations`.
- [x] Add or extend a `GetLockedMigrations` test that proves renewal starts before `ext.Load(ctx)` returns. A return-time-only test is not enough because it would also pass if renewal started after `Load` but before return. Use a test storage wrapper or a narrowly scoped failpoint to pause `Load` while it is walking or reading `v1/migrations/`. While `GetLockedMigrations(ctx, callback)` is still blocked inside `Load`, list the storage for `v1/LOCK.READ.`, read the lock metadata, and use `require.Eventually` with a generous timeout to assert `ExpireAt` advances on the same physical lock path. Then release the pause, let `GetLockedMigrations` return, and unlock the returned `LockedMigrations`.
- [x] Update `br/pkg/stream/stream_metas_test.go::TestMergeAndMigrateToRenewsWriteLock` to use the new `LockWithRetry` signature indirectly through production code. The test should still assert the write lock path is stable and `ExpireAt` advances.
- [x] Add `TestLockForAppendRenewsBothLocks` in `br/pkg/stream/stream_metas_test.go`. Because this test file is in package `stream`, it can call unexported `m.lockForAppend(ctx, "AppendMigration", func(){})` directly. Shorten lease constants with `objstore.SetLeaseConstantsForTest`, acquire both locks, list `v1/LOCK.READ.` and `v1/APPEND_LOCK.WRIT.` objects, read both `ExpireAt` values, and use `require.Eventually` with a generous timeout to assert both advance on the same physical lock paths after at least one renewal interval without direct renewal calls. Defer unlocks in the same order as `AppendMigration`: `defer readLock.UnlockOnCleanUp(ctx)` followed by `defer appendLock.UnlockOnCleanUp(ctx)`, so append write unlocks first.

Run:

    ./tools/check/failpoint-go-test.sh br/pkg/restore/log_client -run 'TestGetLockedMigrationsReleasesReadLockOnLoadError|TestGetLockedMigrations.*Renew' -count=1

Expected: selected log client tests pass.

Run:

    ./tools/check/failpoint-go-test.sh br/pkg/stream -run 'TestMergeAndMigrateToRenewsWriteLock|Test.*AppendMigration' -count=1

Expected: selected stream tests pass.

Run:

    ./tools/check/failpoint-go-test.sh br/pkg/task -run TestNonExistent -count=1

Expected: package compiles; there may be no matching tests.

## Milestone 3: Documentation and Static Boundary Checks

Scope: update `docs/agents/br/lease-lock-business-review-notes.md` and run static searches.

After this milestone, the review notes reflect the new ownership boundary and production code has no direct `StartRenewal` calls.

Concrete steps:

- [x] In `docs/agents/br/lease-lock-business-review-notes.md`, mark `GetLockedMigrations` renewal ownership as resolved in current branch after implementation.
- [x] In the same document, record the decision to keep `StartRenewal` interval behavior: no immediate renew/verify because acquire writes fresh `ExpireAt`; the fix is early ownership start.
- [x] In the business call-site table, update `AppendMigration` and `MergeAndMigrateTo` concerns after implementation. `AppendMigration` should no longer say "no renewal during append critical section" once the code passes `cancel` to both locks.
- [x] Run static search:

    rg -n "\\.StartRenewal\\(" br pkg -g '*.go' -g '!pkg/objstore/*_test.go'

Expected: no production matches. Objstore tests should use `TESTStartRenewal`; `pkg/objstore` implementation may contain `startRenewal`.

- [x] Run static search:

    rg -n "LockWithRetry\\(" br pkg -g '*.go' -g '!**/*_test.go'

Expected: every production call has the new `onLeaseLost` argument and no production call passes nil.

- [x] Run static search:

    rg -n "TryLockRemote(Read|Write|Truncate)\\(" br pkg -g '*.go' -g '!**/*_test.go'

Expected: production direct `TryLockRemoteRead` / `TryLockRemoteWrite` calls are only function definitions or `LockWithRetry` locker arguments. `RunStreamTruncate` should use `LockRemoteTruncate` instead of `TryLockRemoteTruncate`. The new `LockRemoteTruncate` wrapper is allowed to call the primitive `TryLockRemoteTruncate`.

- [x] Follow `docs/agents/agents-review-guide.md` because this plan and the follow-up notes update files under `docs/agents/`. At minimum, verify there are no wrapped normative keywords, referenced paths exist, and no new docs conflict with `AGENTS.md`.

## Validation and Acceptance

Acceptance criteria:

- Production business code under `br/` no longer calls `.StartRenewal(`.
- `RemoteLock.StartRenewal` is no longer exported; renewal is started by lifecycle acquire APIs.
- `TryLockRemoteRead`, `TryLockRemoteWrite`, and `TryLockRemoteTruncate` remain primitive and do not start renewal.
- `LockWithRetry` rejects nil `onLeaseLost` before storage access.
- `LockRemoteTruncate` rejects nil `onLeaseLost` before storage access.
- `LockRemoteTruncate` starts renewal after successful acquire, and a test observes `ExpireAt` advancing without direct renewal calls.
- `LockWithRetry` starts renewal after successful acquire, and a test observes `ExpireAt` advancing without direct renewal calls.
- `RunStreamTruncate` acquires its truncate lock through `LockRemoteTruncate`.
- `GetLockedMigrations(ctx, onLeaseLost)` starts renewal before loading migrations, hides its internal read lock, and exposes `Unlock(ctx)`.
- `restoreStream` passes `cancelFn` to `GetLockedMigrations` and releases the returned locked resource through `utils.WithCleanUp`.
- `MergeAndMigrateTo` and `AppendMigration` pass child-context cancel functions to their lock acquisitions, and critical-section work uses those child contexts.
- `StartRenewal` first-renew schedule is unchanged; no immediate `tryRenew` is added.
- `docs/agents/br/lease-lock-business-review-notes.md` reflects the implemented ownership boundary and the no-immediate-renew decision.

Ready validation commands:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLockWithRetry|TestLockRemoteTruncate|TestStartRenewal|TestTryRenew|TestUnlock' -count=1

    ./tools/check/failpoint-go-test.sh br/pkg/restore/log_client -run 'TestGetLockedMigrationsReleasesReadLockOnLoadError|TestGetLockedMigrations.*Renew' -count=1

    ./tools/check/failpoint-go-test.sh br/pkg/stream -run 'TestMergeAndMigrateToRenewsWriteLock|TestLockForAppendRenewsBothLocks|Test.*AppendMigration' -count=1

    ./tools/check/failpoint-go-test.sh br/pkg/task -run TestNonExistent -count=1

    rg -n "\\.StartRenewal\\(" br pkg -g '*.go' -g '!pkg/objstore/*_test.go'

    rg -n "LockWithRetry\\(" br pkg -g '*.go' -g '!**/*_test.go'

    rg -n "TryLockRemote(Read|Write|Truncate)\\(" br pkg -g '*.go' -g '!**/*_test.go'

    git diff --check

    make bazel_prepare

    rg -n -P '`(MUST(?: NOT)?|SHOULD|MAY)`' AGENTS.md docs/agents/agents-review-guide.md docs/agents/br/lease-lock-renewal-ownership-implementation-plan.md docs/agents/br/lease-lock-business-review-notes.md

    test -f docs/agents/agents-review-guide.md && test -f docs/agents/br/lease-lock-business-review-notes.md

Because this plan changes code, Ready profile also requires:

    make lint

Expected: all targeted commands pass, `git diff --check` prints nothing, `make bazel_prepare` either produces no diff or only required Bazel metadata, the docs review-guide checks pass, and `make lint` passes.

## Idempotence and Recovery

All tests and static searches are safe to rerun.

If a failpoint test is interrupted and leaves failpoints enabled, run:

    make failpoint-disable

If `make bazel_prepare` changes files unexpectedly, inspect:

    git diff --name-status

Keep only metadata changes caused by newly added Go tests or source file changes in this branch. Do not revert unrelated user changes.

If nil-callback validation tests fail because a lock object was created before validation, move `validateOnLeaseLost` earlier in the public lifecycle function and rerun the targeted objstore tests.

If static search still finds production `.StartRenewal(` calls, either move that call behind a lifecycle acquire wrapper or record why it is not production business code before finishing. The intended final state is no direct production business call.

## Artifacts and Notes

Relevant existing files:

- `pkg/objstore/locking.go`: `RemoteLock`, `tryRenew`, renewal loop, `LockWithRetry`, and `TryLockRemote*`.
- `pkg/objstore/locking_helper.go`: generation, family classification, test timing helpers, and exact acquire helper.
- `pkg/objstore/export_test.go`: test-only access to unexported renewal helpers.
- `pkg/objstore/locking_test.go`: objstore lock acquire, renewal, cleanup, and retry tests.
- `br/pkg/task/stream.go`: `RunStreamTruncate` and `restoreStream`.
- `br/pkg/restore/log_client/client.go`: `LockedMigrations` and `GetLockedMigrations`.
- `br/pkg/restore/log_client/client_test.go`: existing `GetLockedMigrations` cleanup-on-load-error test.
- `br/pkg/stream/stream_metas.go`: `GetReadLock`, `lockForAppend`, `AppendMigration`, and `MergeAndMigrateTo`.
- `br/pkg/stream/stream_metas_test.go`: existing `TestMergeAndMigrateToRenewsWriteLock`.

The implementation should preserve the previously completed instance-generation behavior. This plan does not reopen fixed-path cleanup semantics, family verifier behavior, or truncate retry behavior.
