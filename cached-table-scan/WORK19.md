# WORK19: ResultCacheKey 构建 — plan digest + 参数 hash

## 背景

结果集缓存的 key 由 `(planDigest, paramHash)` 组成。plan digest 用于区分不同的查询模式，paramHash 用于区分同一 prepared statement 的不同参数值。本任务实现 key 的构建逻辑。

## 依赖

WORK16, WORK18

## 修改文件

- `pkg/table/tables/result_cache.go`（扩展 key 构建方法）
- `pkg/planner/core/result_cache_key.go`（新文件）

## 修改内容

### 1. Plan Digest 获取

TiDB 的 plan cache 已经有 plan digest 的基础设施。`stmtctx.StatementContext` 中有 `PlanDigest` 和 `NormalizedPlan`。直接复用：

```go
func BuildResultCacheKey(sctx sessionctx.Context) (ResultCacheKey, bool) {
    stmtCtx := sctx.GetSessionVars().StmtCtx

    // 获取 plan digest
    _, planDigest := stmtCtx.GetPlanDigest()
    if planDigest == nil {
        return ResultCacheKey{}, false
    }

    var key ResultCacheKey
    copy(key.PlanDigest[:], planDigest.Bytes())

    // 获取参数 hash（prepared statement 场景）
    if params := sctx.GetSessionVars().PlanCacheParams.AllParamValues(); len(params) > 0 {
        key.ParamHash = hashParams(params)
    }

    return key, true
}
```

### 2. 参数 Hash 计算

```go
func hashParams(params []types.Datum) uint64 {
    h := fnv.New64a()
    for _, p := range params {
        // 利用 Datum.HashCode 获取稳定的 hash 输入
        b, _ := codec.EncodeKey(nil, nil, p)
        h.Write(b)
    }
    return h.Sum64()
}
```

### 3. 非 Prepared Statement 的处理

对于普通 SQL（非 prepared），plan digest 已经包含了 SQL 中的常量值（因为 normalized plan 会把常量替换为 `?`，但 digest 包含了实际值）。

**注意**：需要确认 TiDB 的 plan digest 行为——如果 normalized plan 把常量替换了，则需要额外把 SQL 中的常量也 hash 进 key。这取决于 `PlanDigest` 的实现：
- 如果 `PlanDigest` 对 `SELECT * FROM t WHERE id = 1` 和 `SELECT * FROM t WHERE id = 2` 产生不同的 digest → 直接用 digest 做 key，paramHash = 0
- 如果产生相同的 digest → 需要把常量值 hash 到 paramHash 中

需要在实现时验证并选择正确策略。

## 测试

### 新增测试

1. **TestBuildResultCacheKey_Prepared** — prepared stmt 不同参数产生不同 key
2. **TestBuildResultCacheKey_SameParams** — 相同参数产生相同 key
3. **TestBuildResultCacheKey_NoPlan** — 没有 plan digest 时返回 false
4. **TestBuildResultCacheKey_NonPrepared** — 普通 SQL 的 key 构建
5. **TestHashParams_DifferentTypes** — 不同类型参数的 hash 稳定性

```bash
cd pkg/planner/core && go test -run 'TestBuildResultCacheKey' -count=1 -v
cd pkg/table/tables && go test -run 'TestHashParams' -count=1 -v
```

### 回归测试

```bash
cd pkg/planner/core && go test -count=1 -timeout 600s
```

## 预期收益

提供稳定、高效的缓存 key 构建，确保相同查询命中、不同查询隔离。
