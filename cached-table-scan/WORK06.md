# WORK06: memRowsIterForTable 批量 DecodeToChunk + Vectorized Filter

## 背景

这是本轮最大收益的优化。当前 `memRowsIterForTable.Next()` 逐行处理：
1. 每行调用 `DecodeToChunk` 到 `chunk.New(types, 1, 1)` 的单行 chunk（175.84s, 38.15%）
2. 每行调用 `EvalBool` 做标量过滤（76.05s, 16.50%）
3. 每行 `chunk.Reset()`（9.54s）
4. 每行 `GetDatumRowWithBuffer` 转回 `[]types.Datum`（3s）

合计 ~264s，占总采样 57%。

## 修改文件

- `pkg/executor/mem_reader.go`

## 修改内容

### 新增 `memRowsBatchIterForTable`

替换当前逐行的 `memRowsIterForTable`，改为批量处理：

```go
type memRowsBatchIterForTable struct {
    kvIter         *txnMemBufferIter
    cd             *rowcodec.ChunkDecoder
    batchChk       *chunk.Chunk       // 批量 chunk，容量 batchSize
    sel            []int              // EvalBool 匹配的行号
    cursor         int                // 当前返回到 sel 的哪一行
    datumRow       []types.Datum
    retFieldTypes  []*types.FieldType
    *memTableReader
}

const cachedTableBatchSize = 64

func (iter *memRowsBatchIterForTable) Next() ([]types.Datum, error) {
    for {
        // 如果 batch 中还有未返回的匹配行，直接返回
        if iter.cursor < len(iter.sel) {
            row := iter.batchChk.GetRow(iter.sel[iter.cursor])
            iter.cursor++
            return row.GetDatumRowWithBuffer(iter.retFieldTypes, iter.datumRow), nil
        }

        // 填充新的 batch
        iter.batchChk.Reset()
        iter.sel = iter.sel[:0]
        iter.cursor = 0

        curr := iter.kvIter
        for iter.batchChk.NumRows() < cachedTableBatchSize && curr.Valid() {
            key := curr.Key()
            value := curr.Value()
            if err := curr.Next(); err != nil {
                return nil, err
            }
            if len(value) == 0 {
                continue
            }

            handle, err := tablecodec.DecodeRowKey(key)
            if err != nil {
                return nil, err
            }

            if !rowcodec.IsNewFormat(value) {
                // 老格式 fallback 到逐行处理
                // ... (单独处理这一行)
                continue
            }

            err = iter.cd.DecodeToChunk(value, handle, iter.batchChk)
            if err != nil {
                return nil, err
            }
        }

        if iter.batchChk.NumRows() == 0 {
            return nil, nil // 迭代结束
        }

        // 批量过滤：用 VecEvalBool 或逐行 EvalBool
        // VecEvalBool 需要所有 condition 都支持 vectorized
        // 如果不支持，fallback 到逐行 EvalBool
        for i := 0; i < iter.batchChk.NumRows(); i++ {
            row := iter.batchChk.GetRow(i)
            matched, _, err := expression.EvalBool(
                iter.ctx.GetExprCtx().GetEvalCtx(),
                iter.conditions, row,
            )
            if err != nil {
                return nil, err
            }
            if matched {
                iter.sel = append(iter.sel, i)
            }
        }
    }
}
```

### 修改 `memTableReader.getMemRowsIter`

当 `cacheTable != nil` 时，返回 `memRowsBatchIterForTable`：

```go
func (m *memTableReader) getMemRowsIter(ctx context.Context) (memRowsIter, error) {
    // ... 保持现有的 partition table 分支

    kvIter, err := newTxnMemBufferIter(m.ctx, m.cacheTable, m.kvRanges, m.desc)
    if err != nil {
        return nil, errors.Trace(err)
    }

    if m.cacheTable != nil {
        return &memRowsBatchIterForTable{
            kvIter:         kvIter,
            cd:             m.buffer.cd,
            batchChk:       chunk.New(m.retFieldTypes, cachedTableBatchSize, cachedTableBatchSize),
            sel:            make([]int, 0, cachedTableBatchSize),
            datumRow:       make([]types.Datum, len(m.retFieldTypes)),
            retFieldTypes:  m.retFieldTypes,
            memTableReader: m,
        }, nil
    }

    // 原有逐行 iter 逻辑不变
    return &memRowsIterForTable{ ... }, nil
}
```

### 注意事项

1. `batchChk` 容量选择：64 行是个好的起点，平衡 decode 批量效率和内存占用
2. 老格式（`!IsNewFormat`）的行需要单独处理或 fallback
3. `sel` 缓存需要在 `Close()` 中清理
4. 返回的 `[]types.Datum` 通过 `datumRow` buffer 复用，但调用者（`UnionScanExec.getAddedRow`）会持有引用直到下一次 `Next()` 调用，需要确认安全性
5. 后续可进一步优化为 VecEvalBool，但先做 chunk-level decode 就已经有显著收益

## 测试

### 单元测试
```bash
cd pkg/executor && go test -run 'TestUnionScan' -count=1 -v
cd pkg/executor && go test -run 'TestDirtyTransaction' -count=1 -v
```

### Cached table 测试
```bash
cd pkg/executor && go test -run 'TestCacheTable' -count=1 -v
cd tests/integrationtest && go test -run 'TestCacheTable' -count=1 -v
```

### 新增 Benchmark

在 `pkg/executor/mem_reader_test.go` 中新增：

```go
func BenchmarkMemRowsIterForTable(b *testing.B) {
    // 构造一个含大量 KV 的 cacheTable MemBuffer
    // 对比逐行 iter vs batch iter 的吞吐量
}
```

```bash
cd pkg/executor && go test -bench='BenchmarkMemRowsIterForTable' -benchmem -count=5
```

### 全量测试
```bash
cd pkg/executor && go test -count=1 -timeout 600s
```

## 预期收益

- `DecodeToChunk` 的 per-row 固定开销（`fromBytes` header 解析等）被摊薄
- `chunk.Reset()` 从每行一次变为每 batch 一次，节省 ~9s
- 为后续 VecEvalBool 优化打基础
- 预计整体 CPU 降低 15-25%
