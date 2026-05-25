# Lease Lock 物理实例化 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把当前 lease-based remote lock 从固定写入路径改成物理实例路径，避免 stale cleanup 删除后来新 acquire 的 lock，同时保持现有业务互斥语义不扩大。

**Architecture:** 业务调用方继续传入逻辑锁路径，例如 `v1/LOCK`、`v1/APPEND_LOCK`、`truncating.lock`。`pkg/objstore/locking.go` 在 acquire 时生成 32 hex generation 并写入物理实例路径；family verifier 负责逻辑互斥判断，cleanup 只删除可确认 stale 的新 32 hex committed instance。renewal 和 unlock 继续操作 `RemoteLock.path` 保存的物理路径，renewal 不迁移 generation。

**Tech Stack:** Go, `pkg/objstore`, `storeapi.Storage`, `conditionalPut`, failpoint-enabled package tests, TiDB `make bazel_prepare` / `make lint` validation.

---

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan must be maintained according to it.

## Purpose / Big Picture

当前 lease lock 已经有 stale cleanup。固定路径 lock 的问题是 cleanup 先读 metadata，再删除同一个 path；如果另一个 waiter 在中间删除旧 lock 并在同一路径 acquire 新 lock，旧 cleanup 可能误删新 holder 的 lock。

完成本计划后，新 acquire 会写入不同的物理实例路径，例如 `v1/LOCK.WRIT.<32hex>`。cleanup 只删除它已经读取并确认 stale 的旧实例路径，因此不会删除后来新 acquire 出来的不同实例。用户可通过 `go test` 中的 cleanup regression case 观察该行为：同一 family 下 stale instance 被 cleanup 后，另一个 alive generated instance 仍存在。

## Progress

- [x] (2026-05-25) 已根据 `docs/agents/br/lease-lock-instance-generation-design.md` 和 `docs/agents/br/lease-lock-family-acquire-protocol.md` 起草执行计划。
- [x] (2026-05-25 11:10+02:00) 编写第一组 objstore regression tests，覆盖 generation 命名和 acquire family conflict 主路径。
- [x] (2026-05-25 11:10+02:00) 实现 generation、family classifier、family conflict error、family verifier。
- [x] (2026-05-25 11:10+02:00) 替换 write/read/truncate acquire 物理目标路径。
- [x] (2026-05-25 11:20+02:00) Slice 2 实现 family-aware cleanup：`LockWithRetry` 改为 logical-family cleanup，新增 `CleanUpStaleTruncateLock`，cleanup 只 reclaim 新 32hex committed instance。
- [x] (2026-05-25 11:45+02:00) 移除 exported generic cleanup/acquire API，并更新 objstore tests。
- [x] (2026-05-25 11:45+02:00) 更新 truncate 调用点和 stream renewal 相关测试。
- [x] (2026-05-25 11:45+02:00) 更新 S3 root-prefix test，覆盖 family prefix 与 generated physical target prefix 两次 listing。
- [x] (2026-05-25 11:20+02:00) 运行 WIP targeted tests：`./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestCleanUp|TestLockWithRetry|TestTryLockRemote|TestRWLock|TestConcurrentLock' -count=1` passed。
- [x] (2026-05-25 11:46+02:00) 运行 API 迁移 WIP tests：objstore failpoint targeted tests、S3 root-prefix test、BR stream renewal test、BR task compile 均 passed。
- [x] (2026-05-25 11:52+02:00) 根据新增 Go test function 运行 `make bazel_prepare` 并检查 Bazel metadata；无 metadata diff。
- [x] (2026-05-25 11:56+02:00) 运行 Ready profile：targeted tests + `make lint` passed。
- [x] (2026-05-25 12:35+02:00) 根据独立审查补充 robustness/race regression tests，并增强 cleanup 诊断日志。
- [x] (2026-05-25 12:40+02:00) 补充 append writer interleaving 与 same physical target collision race tests，直接覆盖 family verifier 和 `assertOnlyMyIntent()` 的并发边界。

## Surprises & Discoveries

- Observation: `pkg/objstore` 使用 failpoint，运行该 package 的 targeted tests 时必须使用 `./tools/check/failpoint-go-test.sh pkg/objstore ...`。
  Evidence: `pkg/objstore/locking.go` 和 `pkg/objstore/local.go` 引入 `github.com/pingcap/failpoint`，`pkg/objstore/BUILD.bazel` 包含 failpoint 依赖。
- Observation: 现有 truncate 调用点在 `br/pkg/task/stream.go` 中先 `CleanUpStaleLock(ctx, extStorage, truncateLockPath)`，再 `TryLockRemote(ctx, extStorage, truncateLockPath, ...)`，没有走 `LockWithRetry`。
  Evidence: `RunStreamTruncate` 当前直接调用这两个 exported generic API。
- Observation: `br/pkg/stream/stream_metas_test.go` 中有测试直接读取 `lockPrefix+".WRIT"`，实现 instance path 后需要改为通过 prefix listing 找到唯一 write instance。
  Evidence: `TestMergeAndMigrateToRenewsWriteLock` 的 `readLockMeta` 读取固定 `v1/LOCK.WRIT`。
- Observation: slice 1 spec 和 quality review 均通过；后续 slice 应补充旧 committed member 与 unknown protected-prefix object 的显式测试，并考虑收敛 lock family 字符串常量。
  Evidence: reviewers reported no blocking gaps; local WIP test command passed after rerunning outside sandbox because Go build cache was read-only inside sandbox.
- Observation: exported generic `CleanUpStaleLock` 仍是 exact-path cleanup；由于 `storeapi.Storage` 没有 compare-and-delete，它不能保证 fixed path 上的 read/delete race 安全。Slice 2 的安全边界是 `LockWithRetry` 不再用它做 family prefix cleanup，family cleanup 只删除 generated 32hex instance path。
  Evidence: new tests cover fixed path, 16hex read, intent, and unknown protected-prefix objects remaining after family cleanup; old generic exact cleanup tests still cover stale/alive/zero-ExpireAt behavior.

## Decision Log

- Decision: `conditionalPut.assertOnlyMyIntent()` 保持 exact physical target 语义，不改成 family-aware。
  Rationale: exact-target 检查仍负责同 target 并发写、generation 碰撞和 unknown target-prefix object；逻辑锁互斥由 acquire-specific `Verify()` 负责。
  Date/Author: 2026-05-25 / design session.
- Decision: 新 generation 固定为 32 hex，格式为 16 hex unix nano 加 16 hex crypto random。
  Rationale: 保持单字段 suffix，便于人工按时间粗略排查，同时降低并发碰撞概率。
  Date/Author: 2026-05-25 / design session.
- Decision: cleanup 只 reclaim 新 32 hex committed instance，不自动删除旧固定路径、旧 16 hex read lock、intent 或 unknown protected-prefix object。
  Rationale: 宁可错误失败，不要错误成功；旧格式无法安全判断 stale，且当前分支未发布，不需要兼容中间态 fixed-path lease lock。
  Date/Author: 2026-05-25 / design session.
- Decision: acquire verifier 不读取 `LockMeta`，不做 stale 判断，不删除 object，并忽略当前事务自己的 intent。
  Rationale: stale reclaim 属于 cleanup 层；第二次 `Verify()` 会看到自己刚写的 intent，不忽略会自冲突。
  Date/Author: 2026-05-25 / design session.
- Decision: `RunStreamTruncate` 保持 cleanup 一次、acquire 一次，不改成 `LockWithRetry` 长重试。
  Rationale: 这是现有业务行为；本次只修 fixed-path stale cleanup race，不扩大 truncate 重试语义。
  Date/Author: 2026-05-25 / design session.

## Outcomes & Retrospective

- 2026-05-25: implementation completed. Follow-up review hardening added direct false-success regressions for writer-held/read-blocked, append writer conflict, legacy fixed truncate conflict, read/write interleaving, append writer interleaving, same physical target collision, and cleanup preserving an alive generated instance while reclaiming a stale sibling. Cleanup diagnostics now warn for unknown protected-prefix objects and use non-legacy wording for generated instances missing `ExpireAt`.

## Context and Orientation

主要实现文件是 `pkg/objstore/locking.go`。这个文件包含：

- `conditionalPut`：通过 intent 文件和两次 `Verify()` 实现 object-store 上的 put-and-verify 写入。
- `VerifyWriteContext.assertOnlyMyIntent()`：检查当前 physical target prefix 下除了自己的 intent 以外没有其他 object。
- `RemoteLock`：保存 `txnID`、`storage` 和 `path`。本计划完成后，`path` 必须是物理实例路径。
- `CleanUpStaleLock`：当前 exported generic fixed-path cleanup。本计划应把它收敛为 unexported instance helper，并新增 truncate 专用 exported cleanup。
- `LockWithRetry` / `tryCleanUpStaleLocks`：当前按 base path prefix 扫描并 cleanup。本计划应改成 family-aware cleanup。
- `TryLockRemoteWrite` / `TryLockRemoteRead` / `TryLockRemote`：当前 acquire 入口。本计划应保留 read/write 入口签名但限制 logical family，删除 exported generic `TryLockRemote`，新增 `TryLockRemoteTruncate`。

主要测试文件是 `pkg/objstore/locking_test.go`。它已经有 lock acquire、concurrent intent、renewal、cleanup、LockWithRetry 测试。本计划优先扩展这个文件，不新增测试文件。

业务调用点：

- `br/pkg/stream/stream_metas.go` 调用 `LockWithRetry(ctx, TryLockRemoteRead, ..., "v1/LOCK", ...)` 和 `LockWithRetry(ctx, TryLockRemoteWrite, ..., "v1/APPEND_LOCK", ...)`。
- `br/pkg/task/stream.go` 的 `RunStreamTruncate` 当前调用 generic cleanup/acquire，需要替换成 truncate 专用 API。
- `pkg/objstore/s3store/s3_test.go` 有 root path prefix test，当前直接调用 `TryLockRemote("truncating.lock")`，需要改为 `TryLockRemoteTruncate` 并校验 listing prefix 仍是 `truncating.lock`。

术语：

- logical path：业务传入的逻辑锁路径，如 `v1/LOCK`。
- physical instance path：实际写入 object store 的 key，如 `v1/LOCK.WRIT.0000018f...`。
- family：同一个 logical path 下参与冲突判断的一组 object key。
- committed member：已经写成 lock metadata 的 lock object。
- intent member：`conditionalPut` 写入的 `<target>.INTENT.<txnID>`。
- cleanup-eligible：classifier 明确确认可以被 stale cleanup 删除的新 32 hex committed instance。

## Interfaces and Dependencies

在 `pkg/objstore/locking.go` 中新增或调整以下内部接口。名称可以在实现时微调，但职责不能合并到 verify 或 cleanup 的错误层里。

    type lockAcquireKind uint8

    const (
        lockAcquireRead lockAcquireKind = iota
        lockAcquireWrite
        lockAcquireTruncate
    )

    type lockMemberKind uint8

    const (
        lockMemberUnknown lockMemberKind = iota
        lockMemberRead
        lockMemberWrite
        lockMemberTruncate
    )

    type lockFamilyMember struct {
        Path            string
        Kind            lockMemberKind
        IsIntent        bool
        IsUnknown       bool
        CleanupEligible bool
        OwnIntent       bool
    }

    type lockFamilyConflictError struct {
        Family      string
        AcquireKind lockAcquireKind
        MemberPath  string
        MemberKind  lockMemberKind
        IsIntent    bool
    }

    func (e lockFamilyConflictError) Error() string {
        return fmt.Sprintf("conflicting lock family member exists: family=%s acquire=%v member=%s kind=%v intent=%t",
            e.Family, e.AcquireKind, e.MemberPath, e.MemberKind, e.IsIntent)
    }

Expected helper signatures:

    func newLockGeneration() (string, error)
    func makeLockContent(path string, hint string) func(uuid.UUID) []byte
    func tryLockRemoteExact(ctx context.Context, storage storeapi.Storage, physicalPath, hint string, verify func(VerifyWriteContext) error) (*RemoteLock, error)
    func TryLockRemoteTruncate(ctx context.Context, storage storeapi.Storage, hint string) (*RemoteLock, error)
    func CleanUpStaleTruncateLock(ctx context.Context, storage storeapi.Storage) (bool, error)
    func listLockFamilyCandidates(ctx context.Context, storage storeapi.Storage, logicalPath string, includeTombstone bool) ([]string, error)
    func classifyLockFamilyMember(logicalPath string, acquireKind lockAcquireKind, objectPath string, ownIntent string) lockFamilyMember
    func verifyLockFamily(ctx VerifyWriteContext, logicalPath string, acquireKind lockAcquireKind) error
    func tryCleanUpStaleLockFamily(ctx context.Context, storage storeapi.Storage, logicalPath string) bool
    func cleanUpStaleLockInstance(ctx context.Context, storage storeapi.Storage, physicalPath string) (bool, error)

`TryLockRemoteRead(ctx, storage, path, hint)` must return an error if `path != "v1/LOCK"`.

`TryLockRemoteWrite(ctx, storage, path, hint)` must return an error unless `path == "v1/LOCK"` or `path == "v1/APPEND_LOCK"`.

`TryLockRemoteTruncate` must not accept an arbitrary path. It always uses logical family `truncating.lock`.

## Plan of Work

The work is implemented test-first. Each milestone should leave the tree in a useful state and run a targeted test command. Because new top-level Go test functions will be added to existing `*_test.go` files, run `make bazel_prepare` before final validation and include any generated Bazel metadata changes.

## Milestone 1: Add focused tests for physical instance names

Scope: update `pkg/objstore/locking_test.go` so the new desired naming behavior is explicit before implementation.

After this milestone, tests should fail because production code still writes fixed paths and 16 hex read locks.

Files:

- Modify: `pkg/objstore/locking_test.go`

Concrete steps:

- [x] Add helper functions near existing test helpers:

    func listPathsWithPrefix(t *testing.T, strg storeapi.Storage, subDir, objPrefix string) []string {
        t.Helper()
        var paths []string
        require.NoError(t, strg.WalkDir(context.Background(), &storeapi.WalkOption{
            SubDir:    subDir,
            ObjPrefix: objPrefix,
        }, func(p string, _ int64) error {
            paths = append(paths, p)
            return nil
        }))
        return paths
    }

    func requireSinglePathWithPrefix(t *testing.T, strg storeapi.Storage, subDir, objPrefix string) string {
        t.Helper()
        paths := listPathsWithPrefix(t, strg, subDir, objPrefix)
        require.Len(t, paths, 1)
        return paths[0]
    }

    func requireHas32HexSuffix(t *testing.T, p, prefix string) {
        t.Helper()
        require.True(t, strings.HasPrefix(p, prefix), "path %s must start with %s", p, prefix)
        suffix := strings.TrimPrefix(p, prefix)
        require.Len(t, suffix, 32)
        _, err := hex.DecodeString(suffix)
        require.NoError(t, err)
    }

- [x] Add imports `encoding/hex` and `strings` if they are not already present.

- [x] Replace or extend `TestTryLockRemote` with truncate-specific expectations:

    func TestTryLockRemoteTruncateCreatesInstancePath(t *testing.T) {
        ctx := context.Background()
        strg, pth := createMockStorage(t)

        lock, err := objstore.TryLockRemoteTruncate(ctx, strg, "truncate owner")
        require.NoError(t, err)

        physicalPath := requireSinglePathWithPrefix(t, strg, "", "truncating.lock.")
        requireHas32HexSuffix(t, physicalPath, "truncating.lock.")
        requireFileExists(t, filepath.Join(pth, physicalPath))

        require.NoError(t, lock.Unlock(ctx))
        requireFileNotExists(t, filepath.Join(pth, physicalPath))
    }

- [x] Replace or extend `TestRWLock` with instance path expectations for `v1/LOCK`:

    func TestTryLockRemoteReadWriteCreateInstancePaths(t *testing.T) {
        ctx := context.Background()
        strg, pth := createMockStorage(t)

        readLock, err := objstore.TryLockRemoteRead(ctx, strg, "v1/LOCK", "reader")
        require.NoError(t, err)
        readPath := requireSinglePathWithPrefix(t, strg, "v1", "LOCK.READ.")
        requireHas32HexSuffix(t, readPath, "v1/LOCK.READ.")

        _, err = objstore.TryLockRemoteWrite(ctx, strg, "v1/LOCK", "writer")
        require.Error(t, err)

        require.NoError(t, readLock.Unlock(ctx))
        requireFileNotExists(t, filepath.Join(pth, readPath))

        writeLock, err := objstore.TryLockRemoteWrite(ctx, strg, "v1/LOCK", "writer")
        require.NoError(t, err)
        writePath := requireSinglePathWithPrefix(t, strg, "v1", "LOCK.WRIT.")
        requireHas32HexSuffix(t, writePath, "v1/LOCK.WRIT.")
        require.NoError(t, writeLock.Unlock(ctx))
    }

- [x] Add append write instance path test:

    func TestTryLockRemoteAppendWriteCreatesInstancePath(t *testing.T) {
        ctx := context.Background()
        strg, _ := createMockStorage(t)

        lock, err := objstore.TryLockRemoteWrite(ctx, strg, "v1/APPEND_LOCK", "append writer")
        require.NoError(t, err)
        physicalPath := requireSinglePathWithPrefix(t, strg, "v1", "APPEND_LOCK.WRIT.")
        requireHas32HexSuffix(t, physicalPath, "v1/APPEND_LOCK.WRIT.")
        require.NoError(t, lock.Unlock(ctx))
    }

- [x] Add invalid logical path tests:

    func TestTryLockRemoteRejectsUnknownLogicalFamily(t *testing.T) {
        ctx := context.Background()
        strg, _ := createMockStorage(t)

        _, err := objstore.TryLockRemoteRead(ctx, strg, "test.lock", "reader")
        require.ErrorContains(t, err, "unknown lock family")

        _, err = objstore.TryLockRemoteWrite(ctx, strg, "test.lock", "writer")
        require.ErrorContains(t, err, "unknown lock family")
    }

- [x] Run the targeted test and confirm it fails for the expected reason:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemoteTruncateCreatesInstancePath|TestTryLockRemoteReadWriteCreateInstancePaths|TestTryLockRemoteAppendWriteCreatesInstancePath|TestTryLockRemoteRejectsUnknownLogicalFamily' -count=1

Expected: fail before implementation because `TryLockRemoteTruncate` does not exist and existing read/write allow arbitrary paths.

## Milestone 2: Implement generation and exact-path acquire primitive

Scope: add physical target generation and an internal exact-path primitive. This milestone should make the naming tests compile, but family conflict tests are not complete yet.

Files:

- Modify: `pkg/objstore/locking.go`

Concrete steps:

- [x] Replace `math/rand` import usage carefully. Keep it because `LockWithRetry` uses jitter. Add `crypto/rand` as `cryptorand`, add `encoding/binary`, and keep `encoding/hex`.

- [x] Add constants near `writeLockName`:

    const (
        lockPathTruncate  = "truncating.lock"
        lockPathMigration = "v1/LOCK"
        lockPathAppend    = "v1/APPEND_LOCK"
        lockGenerationHexLen = 32
    )

- [x] Add `newLockGeneration`:

    func newLockGeneration() (string, error) {
        var random [8]byte
        if _, err := cryptorand.Read(random[:]); err != nil {
            return "", errors.Annotate(err, "generate lock generation random suffix")
        }
        return fmt.Sprintf("%016x%016x", nowFunc().UnixNano(), binary.BigEndian.Uint64(random[:])), nil
    }

- [x] Change `newReadLockName` to take a generated suffix:

    func newReadLockName(path, generation string) string {
        return fmt.Sprintf("%s.READ.%s", path, generation)
    }

- [x] Add `newWriteLockInstanceName` and `newTruncateLockInstanceName`:

    func newWriteLockInstanceName(path, generation string) string {
        return fmt.Sprintf("%s.WRIT.%s", path, generation)
    }

    func newTruncateLockInstanceName(generation string) string {
        return fmt.Sprintf("%s.%s", lockPathTruncate, generation)
    }

- [x] Extract lock content creation so all acquire paths use the same metadata:

    func makeLockContent(lockPath, hint string) func(uuid.UUID) []byte {
        return func(txnID uuid.UUID) []byte {
            meta := MakeLockMeta(hint)
            meta.TxnID = txnID[:]
            res, err := json.Marshal(meta)
            if err != nil {
                log.Panic("Unreachable: a plain object cannot be marshaled to JSON.",
                    zap.String("path", lockPath),
                    logutil.ShortError(err))
            }
            return res
        }
    }

- [x] Rename exported `TryLockRemote` implementation into unexported `tryLockRemoteExact`:

    func tryLockRemoteExact(
        ctx context.Context,
        storage storeapi.Storage,
        physicalPath string,
        hint string,
        verify func(VerifyWriteContext) error,
    ) (*RemoteLock, error) {
        writer := conditionalPut{
            Target:  physicalPath,
            Content: makeLockContent(physicalPath, hint),
            Verify:  verify,
        }
        txnID, err := writer.CommitTo(ctx, storage)
        if err != nil {
            return nil, errors.Annotatef(err, "failed to acquire lock on '%s'", physicalPath)
        }
        return &RemoteLock{txnID: txnID, storage: storage, path: physicalPath}, nil
    }

- [x] Add `TryLockRemoteTruncate`:

    func TryLockRemoteTruncate(ctx context.Context, storage storeapi.Storage, hint string) (*RemoteLock, error) {
        generation, err := newLockGeneration()
        if err != nil {
            return nil, err
        }
        target := newTruncateLockInstanceName(generation)
        return tryLockRemoteExact(ctx, storage, target, hint, func(ctx VerifyWriteContext) error {
            return verifyLockFamily(ctx, lockPathTruncate, lockAcquireTruncate)
        })
    }

    `verifyLockFamily` can initially call existing exact prefix check as a temporary compile step, but it must be replaced in Milestone 3 before tests are allowed to pass.

- [x] Update `TryLockRemoteRead` and `TryLockRemoteWrite` to reject unknown logical paths and to use generated physical target names. Until Milestone 3, they may still use existing prefix verify:

    func TryLockRemoteWrite(ctx context.Context, storage storeapi.Storage, path, hint string) (*RemoteLock, error) {
        if path != lockPathMigration && path != lockPathAppend {
            return nil, errors.Errorf("unknown lock family %s for write lock", path)
        }
        generation, err := newLockGeneration()
        if err != nil {
            return nil, err
        }
        target := newWriteLockInstanceName(path, generation)
        return tryLockRemoteExact(ctx, storage, target, hint, func(ctx VerifyWriteContext) error {
            return verifyLockFamily(ctx, path, lockAcquireWrite)
        })
    }

    func TryLockRemoteRead(ctx context.Context, storage storeapi.Storage, path, hint string) (*RemoteLock, error) {
        if path != lockPathMigration {
            return nil, errors.Errorf("unknown lock family %s for read lock", path)
        }
        generation, err := newLockGeneration()
        if err != nil {
            return nil, err
        }
        target := newReadLockName(path, generation)
        return tryLockRemoteExact(ctx, storage, target, hint, func(ctx VerifyWriteContext) error {
            return verifyLockFamily(ctx, path, lockAcquireRead)
        })
    }

- [x] Run the Milestone 1 targeted test command. Expected: compile may still fail if `verifyLockFamily` is missing; after adding a temporary skeleton, naming tests should start passing or fail due to family conflict logic.

## Milestone 3: Implement family classifier and acquire verifier

Scope: implement the core acquire protocol. `Verify()` must list family candidates with tombstones, classify them, ignore own intent, and return family conflict without reading `LockMeta`.

Files:

- Modify: `pkg/objstore/locking.go`
- Modify: `pkg/objstore/locking_test.go`

Concrete steps:

- [x] Add helper predicates:

    func isHexLen(s string, n int) bool {
        if len(s) != n {
            return false
        }
        _, err := hex.DecodeString(s)
        return err == nil
    }

    func splitIntentPath(p string) (target string, isIntent bool) {
        idx := strings.Index(p, ".INTENT.")
        if idx < 0 {
            return p, false
        }
        return p[:idx], true
    }

- [x] Add family listing helper. It must preserve current verify conservatism by setting `IncludeTombstone: true` for acquire:

    func listLockFamilyCandidates(ctx context.Context, storage storeapi.Storage, logicalPath string, includeTombstone bool) ([]string, error) {
        prefixes, err := lockFamilyPrefixes(logicalPath)
        if err != nil {
            return nil, err
        }
        var candidates []string
        for _, pfx := range prefixes {
            fileName := path.Base(pfx)
            dirName := path.Dir(pfx)
            if dirName == "." {
                dirName = ""
            }
            err := storage.WalkDir(ctx, &storeapi.WalkOption{
                SubDir:           dirName,
                ObjPrefix:        fileName,
                IncludeTombstone: includeTombstone,
            }, func(p string, _ int64) error {
                candidates = append(candidates, p)
                return nil
            })
            if err != nil {
                return nil, err
            }
        }
        return candidates, nil
    }

- [x] Add `lockFamilyPrefixes`:

    func lockFamilyPrefixes(logicalPath string) ([]string, error) {
        switch logicalPath {
        case lockPathTruncate:
            return []string{lockPathTruncate}, nil
        case lockPathMigration:
            return []string{writeLockName(lockPathMigration), lockPathMigration + ".READ"}, nil
        case lockPathAppend:
            return []string{writeLockName(lockPathAppend)}, nil
        default:
            return nil, errors.Errorf("unknown lock family %s", logicalPath)
        }
    }

- [x] Implement classifier using committed target first, then intent wrapper. Required classification:

    func classifyLockFamilyMember(logicalPath string, acquireKind lockAcquireKind, objectPath string, ownIntent string) lockFamilyMember {
        committedPath, isIntent := splitIntentPath(objectPath)
        member := classifyCommittedLockFamilyMember(logicalPath, committedPath)
        if member.Path == "" {
            return lockFamilyMember{}
        }
        member.Path = objectPath
        member.IsIntent = isIntent
        member.OwnIntent = isIntent && objectPath == ownIntent
        if isIntent {
            member.CleanupEligible = false
        }
        return member
    }

    func classifyCommittedLockFamilyMember(logicalPath string, committedPath string) lockFamilyMember {
        switch logicalPath {
        case lockPathTruncate:
            return classifyTruncateMember(committedPath)
        case lockPathMigration:
            return classifyMigrationMember(committedPath)
        case lockPathAppend:
            return classifyAppendMember(committedPath)
        default:
            return lockFamilyMember{}
        }
    }

- [x] Implement `classifyTruncateMember`, `classifyMigrationMember`, and `classifyAppendMember` with these exact outcomes:

    `truncating.lock` is committed truncate member, cleanup ineligible.

    `truncating.lock.<32hex>` is committed truncate member, cleanup eligible.

    `truncating.lock.backup` is unknown protected-prefix member, cleanup ineligible.

    `v1/LOCK.WRIT` is committed write member, cleanup ineligible.

    `v1/LOCK.WRIT.<32hex>` is committed write member, cleanup eligible.

    `v1/LOCK.READ.<16hex>` is committed read member, cleanup ineligible.

    `v1/LOCK.READ.<32hex>` is committed read member, cleanup eligible.

    `v1/LOCK.WRITER_NOTES` is unknown protected-prefix member, cleanup ineligible.

    `v1/APPEND_LOCK.WRIT` is committed write member, cleanup ineligible.

    `v1/APPEND_LOCK.WRIT.<32hex>` is committed write member, cleanup eligible.

- [x] Add conflict predicate:

    func isConflictForAcquire(acquireKind lockAcquireKind, member lockFamilyMember) bool {
        if member.Path == "" || member.OwnIntent {
            return false
        }
        if member.IsUnknown {
            return true
        }
        switch acquireKind {
        case lockAcquireTruncate:
            return true
        case lockAcquireWrite:
            return member.Kind == lockMemberRead || member.Kind == lockMemberWrite
        case lockAcquireRead:
            return member.Kind == lockMemberWrite
        default:
            return true
        }
    }

- [x] Add `verifyLockFamily`:

    func verifyLockFamily(ctx VerifyWriteContext, logicalPath string, acquireKind lockAcquireKind) error {
        candidates, err := listLockFamilyCandidates(ctx, ctx.Storage, logicalPath, true)
        if err != nil {
            return err
        }
        ownIntent := ctx.IntentFileName()
        for _, p := range candidates {
            member := classifyLockFamilyMember(logicalPath, acquireKind, p, ownIntent)
            if isConflictForAcquire(acquireKind, member) {
                return lockFamilyConflictError{
                    Family:      logicalPath,
                    AcquireKind: acquireKind,
                    MemberPath:  member.Path,
                    MemberKind:  member.Kind,
                    IsIntent:    member.IsIntent,
                }
            }
        }
        return nil
    }

- [x] Add tests:

    func TestLockFamilyVerifierIgnoresOwnIntent(t *testing.T) {
        ctx := context.Background()
        strg, _ := createMockStorage(t)

        lock, err := objstore.TryLockRemoteWrite(ctx, strg, "v1/LOCK", "writer")
        require.NoError(t, err)
        require.NoError(t, lock.Unlock(ctx))
    }

    This test primarily protects the second `Verify()` in `conditionalPut.CommitTo`, because it must see and ignore its own intent.

- [x] Add tests for read/write intent compatibility. Use existing failpoints `exclusive-write-commit-to-1` and `exclusive-write-commit-to-2` to pause one acquire after intent creation, then assert:

    read acquire is blocked by write intent.

    write acquire is blocked by read intent.

    read acquire is not blocked by another read intent.

  Keep each case as a separate `t.Run` under one top-level test if possible, to reduce Bazel test metadata churn.

- [x] Run targeted tests:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemoteReadWriteCreateInstancePaths|TestTryLockRemoteAppendWriteCreatesInstancePath|TestLockFamilyVerifierIgnoresOwnIntent|TestRWLock|TestConcurrentLock' -count=1

Expected: pass after classifier/verifier implementation.

## Milestone 4: Implement family-aware cleanup

Scope: replace generic fixed-path cleanup with instance cleanup and family cleanup. Keep cleanup and verify separated.

Files:

- Modify: `pkg/objstore/locking.go`
- Modify: `pkg/objstore/locking_test.go`

Concrete steps:

- [x] Rename `CleanUpStaleLock` to unexported `cleanUpStaleLockInstance`. Keep the metadata logic mostly intact:

    func cleanUpStaleLockInstance(ctx context.Context, storage storeapi.Storage, path string) (reclaimed bool, err error) {
        meta, err := getLockMeta(ctx, storage, path)
        if err != nil {
            exists, existsErr := storage.FileExists(ctx, path)
            if existsErr != nil {
                return false, multierr.Append(err, errors.Annotatef(existsErr, "cleanUpStaleLockInstance: FileExists %s", path))
            }
            if !exists {
                return false, nil
            }
            return false, err
        }
        if meta.ExpireAt.IsZero() {
            return false, errors.Errorf("cleanup candidate %s has zero ExpireAt", path)
        }
        now := nowFunc()
        reclaimAfter := meta.ExpireAt.Add(LeaseTTL)
        if !now.After(reclaimAfter) {
            return false, nil
        }
        if err := storage.DeleteFile(ctx, path); err != nil {
            return false, errors.Annotatef(err, "cleanUpStaleLockInstance: DeleteFile %s", path)
        }
        log.Info("Reclaimed stale lock.",
            zap.String("path", path),
            zap.Time("reclaim_after", reclaimAfter),
            zap.Stringer("original_meta", meta))
        return true, nil
    }

  Note: zero `ExpireAt` is an error only here because this helper must be called only after classifier confirms a new 32 hex cleanup-eligible instance.

- [x] Implement `tryCleanUpStaleLockFamily`. It should list with `includeTombstone=false`, classify each candidate, skip intent and unknown objects, call `cleanUpStaleLockInstance` only for `CleanupEligible`, log candidate errors, continue, and return true if any candidate was reclaimed.

    func tryCleanUpStaleLockFamily(ctx context.Context, storage storeapi.Storage, logicalPath string) bool {
        candidates, err := listLockFamilyCandidates(ctx, storage, logicalPath, false)
        if err != nil {
            log.Warn("Stale-lock cleanup: WalkDir failed; skipping reclaim attempt.",
                zap.String("base", logicalPath), logutil.ShortError(err))
            return false
        }
        anyReclaimed := false
        for _, p := range candidates {
            member := classifyLockFamilyMember(logicalPath, 0, p, "")
            if member.Path == "" || member.IsIntent || member.IsUnknown || !member.CleanupEligible {
                if member.IsUnknown {
                    log.Warn("Stale-lock cleanup: unknown protected-prefix object; skipping.",
                        zap.String("path", p), zap.String("family", logicalPath))
                }
                continue
            }
            reclaimed, err := cleanUpStaleLockInstance(ctx, storage, p)
            if err != nil {
                log.Warn("Stale-lock cleanup: candidate cleanup failed; continuing with next candidate.",
                    zap.String("path", p), logutil.ShortError(err))
                continue
            }
            if reclaimed {
                anyReclaimed = true
            }
        }
        return anyReclaimed
    }

- [x] Update `LockWithRetry` to call `tryCleanUpStaleLockFamily` unconditionally after acquire error. Do not gate on error type.

- [x] Add exported truncate cleanup:

    func CleanUpStaleTruncateLock(ctx context.Context, storage storeapi.Storage) (bool, error) {
        candidates, err := listLockFamilyCandidates(ctx, storage, lockPathTruncate, false)
        if err != nil {
            return false, err
        }
        anyReclaimed := false
        for _, p := range candidates {
            member := classifyLockFamilyMember(lockPathTruncate, lockAcquireTruncate, p, "")
            if member.Path == "" || member.IsIntent || member.IsUnknown || !member.CleanupEligible {
                continue
            }
            reclaimed, err := cleanUpStaleLockInstance(ctx, storage, p)
            if err != nil {
                log.Warn("Stale truncate lock cleanup: candidate cleanup failed; continuing with next candidate.",
                    zap.String("path", p), logutil.ShortError(err))
                continue
            }
            if reclaimed {
                anyReclaimed = true
            }
        }
        return anyReclaimed, nil
    }

  This function returns WalkDir errors but logs and continues candidate cleanup errors, matching the design that malformed 32 hex instances should not block the cleanup pass itself.

- [x] Update cleanup tests. Existing tests expecting `CleanUpStaleLock` should be changed to call either `CleanUpStaleTruncateLock` or exercise cleanup through `LockWithRetry`.

Required test cases:

    stale truncating.lock.<32hex> is deleted by CleanUpStaleTruncateLock
    stale truncating.lock is not deleted by CleanUpStaleTruncateLock
    stale v1/LOCK.WRIT.<32hex> is deleted by LockWithRetry cleanup pass
    stale v1/LOCK.READ.<32hex> is deleted by LockWithRetry cleanup pass
    stale v1/APPEND_LOCK.WRIT.<32hex> is deleted by LockWithRetry cleanup pass
    stale v1/LOCK.WRIT is not deleted
    stale v1/LOCK.READ.<16hex> is not deleted
    unknown protected-prefix object is not deleted
    intent object is not deleted
    malformed v1/LOCK.WRIT.<32hex> logs/skips and a later stale eligible candidate is still deleted

- [x] Add a regression test for the original fixed-path race, now using two different instances:

    write stale v1/LOCK.WRIT.<32hex-old>
    wrap storage DeleteFile so that before deleting old instance, another holder acquires v1/LOCK.WRIT.<32hex-new>
    call cleanup on old instance through LockWithRetry or direct family cleanup test hook
    assert new instance still exists and has hint "fresh-holder"

  If no direct test hook exists for family cleanup, expose only test helper through `pkg/objstore/export_test.go`, not production API.

- [x] Run targeted cleanup tests:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestCleanUp|TestLockWithRetry' -count=1

Expected: pass after cleanup implementation. Existing tests with old generic `CleanUpStaleLock` names should be renamed to new behavior names.

## Milestone 5: Remove exported generic APIs and update business call sites

Scope: remove the exported generic fixed-path API surface, then update callers.

Files:

- Modify: `pkg/objstore/locking.go`
- Modify: `pkg/objstore/locking_test.go`
- Modify: `br/pkg/task/stream.go`
- Modify: `br/pkg/stream/stream_metas_test.go`
- Modify: `pkg/objstore/s3store/s3_test.go`

Concrete steps:

- [x] Delete exported `TryLockRemote` and `CleanUpStaleLock` from `pkg/objstore/locking.go`.

- [x] Keep `tryLockRemoteExact` unexported for shared implementation and tests.

- [x] Replace truncate call site in `br/pkg/task/stream.go`:

    if _, err := objstore.CleanUpStaleTruncateLock(ctx, extStorage); err != nil {
        return err
    }
    lock, err := objstore.TryLockRemoteTruncate(ctx, extStorage, hintOnTruncateLock)
    if err != nil {
        return err
    }

- [x] Update `pkg/objstore/s3store/s3_test.go::TestTryLockRemoteRootPathPrefix`:

    Call `TryLockRemoteTruncate(context.Background(), storage, "hint")`.

    Keep expected S3 `Prefix` as `truncating.lock`; physical target generation should not change listing prefix.

- [x] Update `br/pkg/stream/stream_metas_test.go::TestMergeAndMigrateToRenewsWriteLock`:

    Replace direct read of `lockPrefix+".WRIT"` with prefix listing under `v1/LOCK.WRIT.` and read the single returned path.

    Replace direct cleanup call `CleanUpStaleLock(ctx, s, lockPrefix+".WRIT")` with competing acquire assertion only, or with `LockWithRetry` behavior if the test specifically needs cleanup. Since the test verifies active renewal, the important assertion is that a competing writer fails while the original holder is alive.

- [x] Run search to ensure exported generic APIs are gone:

    rg -n "TryLockRemote\\(|CleanUpStaleLock\\(" . -g '*.go'

Expected: no production references to exported generic APIs. Test references should either be removed or use new APIs/test helpers.

## Milestone 6: Validate renewal remains in-place

Scope: adjust renewal tests so they do not assume fixed paths and add explicit generation-stability assertions.

Files:

- Modify: `pkg/objstore/locking_test.go`

Concrete steps:

- [x] Update tests that currently acquire `TryLockRemote(ctx, ..., "test.lock", ...)` for renewal to use `TryLockRemoteTruncate` or `TryLockRemoteWrite("v1/LOCK")`.

- [x] In `TestTryRenewSuccess`, record the physical path returned by prefix listing before renewal and after renewal. Assert they are equal:

    physicalPath := requireSinglePathWithPrefix(t, strg, "v1", "LOCK.WRIT.")
    require.NoError(t, objstore.TESTTryRenew(ctx, lock))
    afterPath := requireSinglePathWithPrefix(t, strg, "v1", "LOCK.WRIT.")
    require.Equal(t, physicalPath, afterPath)

- [x] Keep existing renewal behavior assertions:

    TxnID mismatch returns `objstore.TESTRenewTxnIDMismatch`.

    expired lease returns `objstore.TESTRenewLeaseExpired`.

    transient write error is not classified as permanent.

    `StartRenewal` calls onLeaseLost on permanent loss or retry exhaustion.

- [x] Run targeted renewal tests:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryRenew|TestStartRenewal|TestUnlock' -count=1

Expected: pass. These tests prove renewal refreshes the same physical path instead of moving generation.

## Milestone 7: Run package and call-site validation

Scope: run the smallest checks that cover changed packages and then the Ready gate.

Concrete steps:

- [x] Before final validation, run Bazel prepare because this plan adds new top-level Go test functions in an existing `*_test.go`:

    make bazel_prepare

Expected: either no diff, or `BUILD.bazel` metadata updated consistently. Include resulting metadata changes in the final commit.

- [x] Run objstore targeted tests with failpoint runner:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemote|TestConflictLock|TestRWLock|TestConcurrentLock|TestCleanUp|TestLockWithRetry|TestTryRenew|TestStartRenewal|TestUnlock' -count=1

Expected: pass.

- [x] Run the S3 root-prefix test:

    go test ./pkg/objstore/s3store -run TestTryLockRemoteRootPathPrefix -count=1

Expected: pass, and mock expects prefix `truncating.lock`.

- [x] Run the BR stream renewal test:

    go test ./br/pkg/stream -run TestMergeAndMigrateToRenewsWriteLock -count=1

Expected: pass.

- [x] Run targeted task package test if a local test exists for `RunStreamTruncate`; otherwise record that truncate call-site behavior was covered by compile plus objstore unit tests. Search command:

    rg -n "RunStreamTruncate|StreamTruncate" br/pkg/task -g '*_test.go'

- [x] Run `go test` compile for the changed BR task package:

    go test ./br/pkg/task -run TestNonExistent -count=1

Expected: package compiles; no tests run or package-specific output indicates no matching tests.

- [x] Run Ready profile lint because code changed:

    make lint

Expected: pass.

## Validation and Acceptance

The change is acceptable only when all of these are true:

- New acquire paths create `truncating.lock.<32hex>`, `v1/LOCK.WRIT.<32hex>`, `v1/LOCK.READ.<32hex>`, and `v1/APPEND_LOCK.WRIT.<32hex>`.
- `RemoteLock.Unlock` deletes the physical instance path that was acquired.
- Renewal refreshes `ExpireAt` in place and does not change generation.
- Write acquire is blocked by read/write committed members and read/write intents, except its own intent.
- Read acquire is blocked by write committed members and write intents, but not by read locks or read intents.
- Acquire verifier does not read `LockMeta`, does not check `ExpireAt`, and does not cleanup.
- `LockWithRetry` cleanup only deletes stale new 32 hex committed instances.
- Old fixed-path locks, old 16 hex read locks, intents, and unknown protected-prefix objects are not auto-deleted.
- Malformed new 32 hex cleanup candidate does not become `LockWithRetry` final error and does not stop cleanup of later candidates.
- `RunStreamTruncate` still performs cleanup once and acquire once; it does not enter `LockWithRetry`.
- Exported generic `TryLockRemote` and `CleanUpStaleLock` are removed.
- `rg -n "TryLockRemote\\(|CleanUpStaleLock\\(" . -g '*.go'` finds no accidental production use of removed APIs.

## Idempotence and Recovery

All tests and validation commands are safe to rerun.

If `make bazel_prepare` changes unrelated Bazel files, inspect `git diff --name-status` and keep only metadata changes caused by newly added tests or source file changes in this branch. Do not revert user changes.

If failpoint tests leave failpoints enabled after an interrupted run, run:

    make failpoint-disable

If a milestone creates too much diff, split implementation by these commit boundaries:

1. classifier and instance naming tests.
2. acquire implementation.
3. cleanup implementation.
4. business call-site and S3 test updates.
5. Bazel metadata and final lint fixes.

## Artifacts and Notes

Use these commands during implementation to inspect state:

    git status --short
    git diff -- pkg/objstore/locking.go pkg/objstore/locking_test.go
    rg -n "TryLockRemote\\(|CleanUpStaleLock\\(|TryLockRemoteTruncate|CleanUpStaleTruncateLock" . -g '*.go'

Before final response, report:

1. Files changed.
2. Validation profile used. For completion, use `Ready`.
3. Correctness, compatibility, and performance risks.
4. Exact validation commands run.
5. What was not verified locally.
