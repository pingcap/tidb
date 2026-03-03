# WORK17: 结果集缓存失效 — 与 lease 和 write lock 联动

## 背景

结果集缓存的正确性依赖于"数据不变"这个前提。只要 cached table 的数据可能变化（lease 过期、write lock 获取），必须清空结果集缓存。本任务在所有相关路径插入 `invalidateResultCache()` 调用。

## 依赖

WORK16（resultSetCache 数据结构已就位）

## 修改文件

- `pkg/table/tables/cache.go`

## 修改内容

### 1. `updateLockForRead` 重新加载数据时清空

在 `updateLockForRead()` 中，`LockForRead` 成功后、设置新的 `cacheData` 之前：

```go
func (c *cachedTable) updateLockForRead(...) {
    // ...
    succ, err := handle.LockForRead(ctx, tid, lease)
    if err != nil { ... }
    if succ {
        c.invalidateResultCache() // ← 新增：数据即将重新加载，旧结果集失效
        c.cacheData.Store(&cacheData{
            Start: ts, Lease: lease, MemBuffer: nil,
        })
        // ...
    }
}
```

### 2. `lockForWrite` 时清空

写入意味着数据要变，结果集不再有效：

```go
func (c *cachedTable) lockForWrite(ctx context.Context) (uint64, error) {
    handle := c.TakeStateRemoteHandle()
    defer c.PutStateRemoteHandle(handle)
    c.invalidateResultCache() // ← 新增
    return handle.LockForWrite(ctx, c.Meta().ID, cacheTableWriteLease)
}
```

### 3. `renewLease` 失败时清空

`renewLease` 中如果 `newLease == 0`（续约失败），数据可能已被修改：

```go
func (c *cachedTable) renewLease(...) {
    // ...
    newLease, err := handle.RenewReadLease(...)
    if err != nil {
        c.invalidateResultCache() // ← 续约出错，保守清空
        return
    }
    if newLease > 0 {
        // 续约成功，resultCache 继续有效，不清空
        c.cacheData.Store(...)
    } else {
        c.invalidateResultCache() // ← 续约返回 0，数据可能变了
    }
}
```

### 4. `TryReadFromCache` 返回 nil 时的隐含清空

当 `TryReadFromCache` 返回 `nil, false`（cache 完全失效），此时 `cacheData` 已过期。后续 `updateLockForRead` 会重建，走到上面第 1 点的清空逻辑，所以无需额外处理。

## 测试

### 新增测试用例

在 `cache_test.go` 或新建 `result_cache_invalidation_test.go` 中：

1. **TestResultCacheInvalidateOnWrite** — 模拟 write lock 获取后，resultCache 被清空
2. **TestResultCacheInvalidateOnReload** — 模拟 lease 过期后重新 LockForRead，resultCache 被清空
3. **TestResultCacheSurvivesRenew** — renewLease 成功时，resultCache 保持不变

```bash
cd pkg/table/tables && go test -run 'TestResultCacheInvalidate' -count=1 -v
cd pkg/table/tables && go test -run 'TestResultCacheSurvivesRenew' -count=1 -v
```

### 回归测试

```bash
cd pkg/table/tables && go test -count=1 -timeout 300s
cd pkg/executor && go test -run 'TestCacheTable' -count=1 -v
```

## 预期收益

保证结果集缓存的正确性，任何数据变更路径都会清空缓存。
