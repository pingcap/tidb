# WORK21: 结果集缓存可观测性 — metrics、SHOW 状态、日志

## 背景

结果集缓存上线后需要可观测性支持：用户和开发者需要知道缓存是否生效、命中率如何、内存占用多少。本任务添加 metrics、session 变量、EXPLAIN 标记等。

## 依赖

WORK20

## 修改文件

- `pkg/metrics/executor.go`（新增 metrics）
- `pkg/executor/cached_result_exec.go`（添加 metrics 上报）
- `pkg/sessionctx/stmtctx/stmtctx.go`（新增标记）
- `pkg/executor/explain.go` 或相关 EXPLAIN 逻辑

## 修改内容

### 1. Prometheus Metrics

```go
var (
    ResultCacheHitCounter = prometheus.NewCounter(prometheus.CounterOpts{
        Namespace: "tidb",
        Subsystem: "executor",
        Name:      "result_cache_hit_total",
        Help:      "Total number of result cache hits on cached tables",
    })
    ResultCacheMissCounter = prometheus.NewCounter(prometheus.CounterOpts{
        Namespace: "tidb",
        Subsystem: "executor",
        Name:      "result_cache_miss_total",
        Help:      "Total number of result cache misses on cached tables",
    })
    ResultCacheMemoryGauge = prometheus.NewGauge(prometheus.GaugeOpts{
        Namespace: "tidb",
        Subsystem: "executor",
        Name:      "result_cache_memory_bytes",
        Help:      "Memory usage of result cache on cached tables",
    })
    ResultCacheEvictCounter = prometheus.NewCounter(prometheus.CounterOpts{
        Namespace: "tidb",
        Subsystem: "executor",
        Name:      "result_cache_evict_total",
        Help:      "Total number of result cache evictions (lease expiry)",
    })
)
```

### 2. StmtCtx 标记

在 `StatementContext` 中添加：

```go
ReadFromResultCache bool // 是否命中了结果集缓存
```

这样 `EXPLAIN ANALYZE` 和慢日志中可以体现。

### 3. EXPLAIN ANALYZE 输出

当命中结果集缓存时，在 EXPLAIN ANALYZE 的根节点添加：

```
result_cache: hit, cached_rows: 100
```

### 4. 慢日志集成

在慢日志输出中添加 `Result_cache_hit: true/false` 字段，方便排查。

### 5. `CachedResultExec` 中上报

```go
func (e *CachedResultExec) Open(ctx context.Context) error {
    if e.canCache {
        chunks, fieldTypes, ok := e.cachedTable.GetCachedResult(e.cacheKey)
        if ok && schemaMatch(fieldTypes, e.RetFieldTypes()) {
            ResultCacheHitCounter.Inc()
            e.ctx.GetSessionVars().StmtCtx.ReadFromResultCache = true
            // ...
        } else {
            ResultCacheMissCounter.Inc()
        }
    }
    // ...
}
```

## 测试

### 新增测试

1. **TestResultCacheExplainAnalyze** — EXPLAIN ANALYZE 输出包含 result_cache 信息
2. **TestResultCacheSlowLog** — 慢日志中包含 Result_cache_hit 字段
3. **TestResultCacheStmtCtxFlag** — ReadFromResultCache 标记正确设置

```bash
cd pkg/executor && go test -run 'TestResultCacheExplain' -count=1 -v
cd pkg/executor && go test -run 'TestResultCacheSlowLog' -count=1 -v
```

### 回归测试

```bash
cd pkg/executor && go test -count=1 -timeout 600s
```

## 预期收益

提供完整的可观测性，方便上线后监控和调优。
