# WORK15: 缓存 index scan per-row 不变量 — 减少 decodeIndexKeyValue 和 Next 中的重复计算

## 背景

memRowsIterForIndex.Next()（mem_reader.go:1153）和 decodeIndexKeyValue（mem_reader.go:193）在每行迭代时重复计算多个不变值：

1. **hdStatus**（line 194-198）：每行重新检查 tps[len(m.index.Columns)].GetFlag()，但类型 flags 在 scan 期间不变
2. **m.ctx.GetSessionVars().Location()**（line 233）：每列每行调用 GetSessionVars() + Location() 方法链，5 列的 scan 每行调用 5 次
3. **m.ctx.GetExprCtx().GetEvalCtx()**（line 1184）：每行调用用于 EvalBool
4. **DecodeColumnValue 返回 Datum by value**（line 233）：已有 DecodeColumnValueWithDatum（tablecodec.go:447）可写入 *Datum，避免拷贝

Profile 数据：
- decodeIndexKeyValue flat 9.86s (2.30%)
- MutRow.SetDatums 9.26s cum (2.16%)
- DecodeColumnValue 39.69s cum (9.25%)

## 修改范围

- pkg/executor/mem_reader.go

## 阅读范围

- pkg/executor/mem_reader.go：memIndexReader struct(53)、buildMemIndexReader(75)、decodeIndexKeyValue(193)、memRowsIterForIndex struct(1141)、memRowsIterForIndex.Next(1153)、getMemRows(151)
- pkg/tablecodec/tablecodec.go：DecodeColumnValueWithDatum(447)、HandleStatus 类型

## 实现方案

### 1) memIndexReader 新增缓存字段

```go
type memIndexReader struct {
    // ... existing fields ...
    hdStatus tablecodec.HandleStatus // cached, computed once
    loc      *time.Location          // cached from session vars
}
```

### 2) 在 buildMemIndexReader 中预计算 hdStatus

```go
func buildMemIndexReader(ctx context.Context, us *UnionScanExec, idxReader *IndexReaderExecutor) *memIndexReader {
    // ...
    tps := // ... 获取 retFieldTypes
    hdStatus := tablecodec.HandleDefault
    if mysql.HasUnsignedFlag(tps[len(idxReader.index.Columns)].GetFlag()) {
        hdStatus = tablecodec.HandleIsUnsigned
    }
    return &memIndexReader{
        // ... existing fields ...
        hdStatus: hdStatus,
        loc:      us.Ctx().GetSessionVars().Location(),
    }
}
```

注意：tps 在 buildMemIndexReader 时尚未构建（getTypes() 在 getMemRows/getMemRowsIter 中调用）。可以改为在 getMemRows/getMemRowsIter 中首次使用时初始化（lazy init）。

### 3) 简化 decodeIndexKeyValue

```go
func (m *memIndexReader) decodeIndexKeyValue(key, value []byte, tps []*types.FieldType, colInfos []rowcodec.ColInfo) ([]types.Datum, error) {
    // Lazy init hdStatus and loc
    if m.loc == nil {
        m.loc = m.ctx.GetSessionVars().Location()
        m.hdStatus = tablecodec.HandleDefault
        if mysql.HasUnsignedFlag(tps[len(m.index.Columns)].GetFlag()) {
            m.hdStatus = tablecodec.HandleIsUnsigned
        }
    }

    // ... buffer management unchanged ...
    values, err := tablecodec.DecodeIndexKVEx(key, value, colsLen, m.hdStatus, colInfos, buf, m.decodeBuff, m.restoredDec)
    // ...

    ds := m.resultRows[:len(m.outputOffset)]  // pre-size instead of append
    for i, offset := range m.outputOffset {
        if m.physTblIDIdx == i {
            tid, _, _, _ := tablecodec.DecodeKeyHead(key)
            ds[i] = types.NewIntDatum(tid)
            continue
        }
        if offset > physTblIDColumnIdx {
            offset = offset - 1
        }
        // Use DecodeColumnValueWithDatum to write directly into ds[i]
        if err := tablecodec.DecodeColumnValueWithDatum(values[offset], tps[offset], m.loc, &ds[i]); err != nil {
            return nil, err
        }
    }
    return ds, nil
}
```

### 4) 在 memRowsIterForIndex 中缓存 evalCtx

```go
type memRowsIterForIndex struct {
    // ... existing fields ...
    evalCtx expression.EvalContext  // cached
}
```

在初始化时设置：
```go
evalCtx: iter.memIndexReader.ctx.GetExprCtx().GetEvalCtx(),
```

在 Next() 中使用：
```go
matched, _, err := expression.EvalBool(iter.evalCtx, iter.memIndexReader.conditions, iter.mutableRow.ToRow())
```

## 必须新增的测试

在 pkg/executor/mem_reader_test.go（如果存在）或相关测试文件中确认已有 index scan 覆盖。主要依赖已有集成测试验证正确性。

新增 benchmark（可选）：
- BenchmarkDecodeIndexKeyValue — 对比缓存前后的 ns/op

## 需要运行的测试

```bash
# 正确性（依赖已有测试覆盖）
go test ./pkg/executor -run 'Test(UnionScan|CacheTable|Index)' -count=1
go test ./pkg/tablecodec -count=1

# 集成回归
go test ./pkg/executor -count=1 -timeout 600s
```

## 验收标准

- 所有已有测试通过
- decodeIndexKeyValue 中不再有 per-row 的 hdStatus 计算和 Location() 调用
- memRowsIterForIndex.Next 中不再有 per-row 的 GetExprCtx().GetEvalCtx() 调用
