# WORK20: 查询路径接入 — 结果集缓存读取与回填

## 背景

前置任务已完成数据结构（WORK16）、失效机制（WORK17）、可缓存判定（WORK18）、key 构建（WORK19）。本任务将结果集缓存接入实际查询路径：cache hit 时直接返回 chunks，miss 时正常执行并回填缓存。

## 依赖

WORK16, WORK17, WORK18, WORK19

## 修改文件

- `pkg/executor/builder.go`
- `pkg/executor/cached_result_exec.go`（新文件）
- `pkg/table/table.go`（CachedTable 接口扩展）

## 修改内容

### 1. 扩展 CachedTable 接口

在 `table.CachedTable` 接口中添加结果集缓存方法：

```go
type CachedTable interface {
    Table
    // 现有方法...
    TryReadFromCache(ts uint64, leaseDuration time.Duration) (kv.MemBuffer, bool)
    UpdateLockForRead(ctx context.Context, store kv.Storage, ts uint64, leaseDuration time.Duration)
    WriteLockAndKeepAlive(ctx context.Context, exit chan struct{}, leasePtr *uint64, wg chan error)

    // 新增：结果集缓存
    GetCachedResult(key ResultCacheKey) ([]*chunk.Chunk, []*types.FieldType, bool)
    PutCachedResult(key ResultCacheKey, chunks []*chunk.Chunk, fieldTypes []*types.FieldType) bool
}
```

### 2. 新建 `cached_result_exec.go`

包装现有 executor，在其上层添加结果集缓存逻辑：

```go
type CachedResultExec struct {
    exec.BaseExecutor

    original    exec.Executor        // 被包装的原始 executor
    cachedTable table.CachedTable
    cacheKey    ResultCacheKey
    canCache    bool

    // cache hit 状态
    hitCache     bool
    cachedChunks []*chunk.Chunk
    chunkIdx     int

    // cache miss 状态：收集结果用于回填
    collecting    bool
    collectedChunks []*chunk.Chunk
    resultSchema  []*types.FieldType
}

func (e *CachedResultExec) Open(ctx context.Context) error {
    if e.canCache {
        chunks, fieldTypes, ok := e.cachedTable.GetCachedResult(e.cacheKey)
        if ok && schemaMatch(fieldTypes, e.RetFieldTypes()) {
            e.hitCache = true
            e.cachedChunks = chunks
            e.chunkIdx = 0
            return nil
        }
    }

    // Cache miss，打开原始 executor
    if err := e.original.Open(ctx); err != nil {
        return err
    }
    if e.canCache {
        e.collecting = true
        e.resultSchema = e.RetFieldTypes()
    }
    return nil
}

func (e *CachedResultExec) Next(ctx context.Context, req *chunk.Chunk) error {
    if e.hitCache {
        return e.nextFromCache(req)
    }

    err := e.original.Next(ctx, req)
    if err != nil {
        e.collecting = false
        return err
    }

    if e.collecting && req.NumRows() > 0 {
        // 拷贝一份用于缓存（原始 chunk 会被 session 复用）
        copied := req.CopyConstruct()
        e.collectedChunks = append(e.collectedChunks, copied)
    }

    return nil
}

func (e *CachedResultExec) Close() error {
    if e.hitCache {
        return nil // 没打开原始 executor
    }

    // 执行完毕，回填缓存
    if e.collecting && len(e.collectedChunks) >= 0 {
        e.cachedTable.PutCachedResult(e.cacheKey, e.collectedChunks, e.resultSchema)
    }

    return e.original.Close()
}

func (e *CachedResultExec) nextFromCache(req *chunk.Chunk) error {
    req.Reset()
    if e.chunkIdx >= len(e.cachedChunks) {
        return nil // EOF
    }
    src := e.cachedChunks[e.chunkIdx]
    e.chunkIdx++
    // 拷贝到 req（不能直接给 session 共享的 chunk 指针）
    req.Append(src, 0, src.NumRows())
    return nil
}

func schemaMatch(cached, current []*types.FieldType) bool {
    if len(cached) != len(current) {
        return false
    }
    for i := range cached {
        if cached[i].GetType() != current[i].GetType() {
            return false
        }
    }
    return true
}
```

### 3. 在 `builder.go` 中接入

在 `handleCachedTable()` 和 `getCacheTable()` 的 cache 命中路径中，用 `CachedResultExec` 包装最终的 executor：

```go
// 在 executorBuilder 中，当 cached table 的 KV cache 命中后
// 检查是否可以进一步使用结果集缓存
func (b *executorBuilder) wrapWithResultCache(e exec.Executor, cachedTbl table.CachedTable) exec.Executor {
    if !b.canCacheResult {
        return e
    }
    key, ok := BuildResultCacheKey(b.ctx)
    if !ok {
        return e
    }
    return &CachedResultExec{
        original:    e,
        cachedTable: cachedTbl,
        cacheKey:    key,
        canCache:    true,
    }
}
```

### 4. 安全考虑

- **Chunk 拷贝**：缓存写入和读取时都做深拷贝，避免多 session 共享 chunk 导致 data race
- **Schema 校验**：DDL 后 plan digest 可能不变但 schema 变了，通过 fieldType 校验防止返回错误数据
- **内存控制**：依赖 WORK16 中 resultSetCache 的 maxEntries 和 maxMemory 限制

## 测试

### 新增集成测试

1. **TestResultCacheHit** — 同一 prepared stmt 执行两次，第二次命中结果集缓存
2. **TestResultCacheMiss_DifferentParams** — 不同参数不命中
3. **TestResultCacheInvalidateAfterWrite** — 写入后结果集缓存失效，重新执行得到新数据
4. **TestResultCacheWithJoin** — 两张 cached table join 的结果集缓存
5. **TestResultCacheSkip_NonDeterministic** — 含 NOW() 的查询不走结果集缓存
6. **TestResultCacheSkip_ForUpdate** — FOR UPDATE 不走结果集缓存
7. **TestResultCacheSchemaChange** — DDL 改列后旧缓存 schema 不匹配，走 miss 路径
8. **TestResultCacheCorrectness** — 验证缓存结果与直接执行结果完全一致

```bash
cd pkg/executor && go test -run 'TestResultCache' -count=1 -v
```

### 回归测试

```bash
cd pkg/executor && go test -count=1 -timeout 600s
cd pkg/table/tables && go test -count=1 -timeout 300s
cd tests/integrationtest && go test -run 'TestCacheTable' -count=1 -v
```

## 预期收益

对于重复查询 cached table 的场景，跳过 KV scan + decode + filter + sort 全部开销，直接返回预计算好的 chunks。预期重复查询延迟降低一个数量级（从微秒级 scan 到纳秒级拷贝）。
