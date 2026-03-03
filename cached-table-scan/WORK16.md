# WORK16: 结果集缓存基础设施 — ResultCacheKey + resultSetCache 数据结构

## 背景

当前 cached table 缓存的是原始 KV 数据（`kv.MemBuffer`），每次查询仍需 range scan → decode → filter → sort/agg。对于读多写少的 cached table（配置表、字典表），查询模式高度重复，直接缓存 `(planDigest, paramHash) → []chunk.Chunk` 可以跳过所有中间计算。

本任务建立结果集缓存的核心数据结构，不改变查询路径（后续任务接入）。

## 修改文件

- `pkg/table/tables/cache.go`（修改 `cachedTable` struct）
- `pkg/table/tables/result_cache.go`（新文件）
- `pkg/table/tables/result_cache_test.go`（新文件）

## 修改内容

### 1. 新建 `result_cache.go`

```go
package tables

import (
    "sync"
    "sync/atomic"

    "github.com/pingcap/tidb/pkg/types"
    "github.com/pingcap/tidb/pkg/util/chunk"
)

// ResultCacheKey 是结果集缓存的查找 key
type ResultCacheKey struct {
    PlanDigest [16]byte // normalized plan 的 digest
    ParamHash  uint64   // prepared stmt 参数值的 hash，非 prepared 时为 0
}

// cachedResult 是单条缓存的结果集
type cachedResult struct {
    chunks     []*chunk.Chunk
    fieldTypes []*types.FieldType // 用于校验 schema 兼容性
    memSize    int64
    hitCount   atomic.Int64
}

// resultSetCache 挂在 cachedTable 上，生命周期与 cacheData lease 绑定
type resultSetCache struct {
    mu       sync.RWMutex
    items    map[ResultCacheKey]*cachedResult
    totalMem int64

    maxEntries int
    maxMemory  int64
}

const (
    defaultMaxResultCacheEntries = 256
    defaultMaxResultCacheMemory  = 64 << 20 // 64MB
)

func newResultSetCache() *resultSetCache {
    return &resultSetCache{
        items:      make(map[ResultCacheKey]*cachedResult),
        maxEntries: defaultMaxResultCacheEntries,
        maxMemory:  defaultMaxResultCacheMemory,
    }
}

// Get 查找缓存，命中时递增 hitCount
func (c *resultSetCache) Get(key ResultCacheKey) ([]*chunk.Chunk, []*types.FieldType, bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()
    if r, ok := c.items[key]; ok {
        r.hitCount.Add(1)
        return r.chunks, r.fieldTypes, true
    }
    return nil, nil, false
}

// Put 写入缓存，超出限制时放弃（不淘汰，因为 lease 失效会整体清空）
func (c *resultSetCache) Put(key ResultCacheKey, chunks []*chunk.Chunk, fieldTypes []*types.FieldType) bool {
    memSize := estimateChunksMemory(chunks)
    c.mu.Lock()
    defer c.mu.Unlock()
    if len(c.items) >= c.maxEntries || c.totalMem+memSize > c.maxMemory {
        return false
    }
    c.items[key] = &cachedResult{
        chunks:     chunks,
        fieldTypes: fieldTypes,
        memSize:    memSize,
    }
    c.totalMem += memSize
    return true
}

func (c *resultSetCache) Len() int {
    c.mu.RLock()
    defer c.mu.RUnlock()
    return len(c.items)
}

func (c *resultSetCache) MemoryUsage() int64 {
    c.mu.RLock()
    defer c.mu.RUnlock()
    return c.totalMem
}

func estimateChunksMemory(chunks []*chunk.Chunk) int64 {
    var total int64
    for _, chk := range chunks {
        total += int64(chk.MemoryUsage())
    }
    return total
}
```

### 2. 在 `cachedTable` struct 中添加字段

在 `cache.go` 的 `cachedTable` struct 新增：

```go
resultCache atomic.Pointer[resultSetCache]
```

### 3. 添加存取和失效方法

```go
func (c *cachedTable) getResultCache() *resultSetCache {
    return c.resultCache.Load()
}

func (c *cachedTable) getOrCreateResultCache() *resultSetCache {
    if rc := c.resultCache.Load(); rc != nil {
        return rc
    }
    rc := newResultSetCache()
    if c.resultCache.CompareAndSwap(nil, rc) {
        return rc
    }
    return c.resultCache.Load()
}

func (c *cachedTable) invalidateResultCache() {
    c.resultCache.Store(nil)
}
```

## 测试

### 新增 `result_cache_test.go`

测试用例：
1. **TestResultCacheGetMiss** — 空缓存 Get 返回 false
2. **TestResultCachePutAndGet** — Put 后 Get 命中，chunks/fieldTypes 一致
3. **TestResultCacheHitCount** — 多次 Get 后 hitCount 正确递增
4. **TestResultCacheMaxEntries** — 超出 maxEntries 时 Put 返回 false
5. **TestResultCacheMaxMemory** — 超出 maxMemory 时 Put 返回 false
6. **TestResultCacheConcurrency** — 并发 Get/Put 无 race

```bash
cd pkg/table/tables && go test -run 'TestResultCache' -count=1 -v -race
```

### 确保不破坏现有测试

```bash
cd pkg/table/tables && go test -count=1 -timeout 300s
```

## 预期收益

无直接性能收益，为 WORK17-WORK20 提供基础设施。
