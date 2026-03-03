# WORK05: Cached Table 跳过 UnionScanExec merge 逻辑

## 背景

`UnionScanExec.getOneRow()` 每行都调用 `getSnapshotRow()` 和 `getAddedRow()`，然后做 merge。但对于 cached table，`getSnapshotRow()` 永远返回 nil（因为 `cacheTable != nil` 直接 return nil），merge 比较是无用功。

Profile 显示 `UnionScanExec.getOneRow` 累计 333.42s，其中 `getAddedRow` 占 332.64s，snapshot 端为 0。每次 `getOneRow` 调用仍要走完整的分支判断。

## 修改文件

- `pkg/executor/union_scan.go`

## 修改内容

在 `UnionScanExec.Next()` 中，当 `us.cacheTable != nil` 时，走专用 fast path，直接从 `addedRowsIter` 取行，跳过 `getOneRow` 的 merge 逻辑：

```go
func (us *UnionScanExec) Next(ctx context.Context, req *chunk.Chunk) error {
    us.memBuf.RLock()
    defer us.memBuf.RUnlock()
    req.GrowAndReset(us.MaxChunkSize())

    if us.cacheTable != nil {
        return us.nextForCacheTable(ctx, req)
    }
    // ... 原有逻辑不变
}

func (us *UnionScanExec) nextForCacheTable(ctx context.Context, req *chunk.Chunk) error {
    mutableRow := chunk.MutRowFromTypes(exec.RetTypes(us))
    for batchSize := req.Capacity(); req.NumRows() < batchSize; {
        row, err := us.addedRowsIter.Next()
        if err != nil {
            return err
        }
        if row == nil {
            return nil
        }
        mutableRow.SetDatums(row...)

        // virtual column 处理（与原逻辑一致）
        sctx := us.Ctx()
        for _, idx := range us.virtualColumnIndex {
            datum, err := us.Schema().Columns[idx].EvalVirtualColumn(sctx.GetExprCtx().GetEvalCtx(), mutableRow.ToRow())
            if err != nil {
                return err
            }
            castDatum, err := table.CastValue(us.Ctx(), datum, us.columns[idx], false, true)
            if err != nil {
                return err
            }
            if (mysql.HasNotNullFlag(us.columns[idx].GetFlag()) || mysql.HasPreventNullInsertFlag(us.columns[idx].GetFlag())) && castDatum.IsNull() {
                castDatum = table.GetZeroValue(us.columns[idx])
            }
            mutableRow.SetDatum(idx, castDatum)
        }

        matched, _, err := expression.EvalBool(us.Ctx().GetExprCtx().GetEvalCtx(), us.conditionsWithVirCol, mutableRow.ToRow())
        if err != nil {
            return err
        }
        if matched {
            req.AppendRow(mutableRow.ToRow())
        }
    }
    return nil
}
```

### 注意事项

- virtual column 处理逻辑需要与原 `Next()` 保持一致
- `cursor4AddRows` 在 cache table 路径不再需要
- 可考虑抽取 virtual column 处理为公共方法，避免代码重复

## 测试

### 单元测试
```bash
cd pkg/executor && go test -run 'TestUnionScan' -count=1 -v
cd pkg/executor && go test -run 'TestDirtyTransaction' -count=1 -v
```

### Cached table 相关测试
```bash
cd pkg/executor && go test -run 'TestCacheTable' -count=1 -v
cd tests/integrationtest && go test -run 'TestCacheTable' -count=1 -v
```

### 全量 executor 测试
```bash
cd pkg/executor && go test -count=1 -timeout 600s
```

## 预期收益

消除 cached table 路径上 merge 分支判断开销。单独收益约 1-2%，但为 WORK06 批量化打基础。
