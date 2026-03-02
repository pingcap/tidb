# WORK10: 优化 chunk fixed-len 追加路径（降低 finishAppendFixed/appendNullBitmap 热点）

## 背景

在 profile 中，`rowcodec.DecodeToChunk` 的大量 CPU 会被归因到 `chunk.(*Column).finishAppendFixed` 以及 `appendNullBitmap`，说明当前瓶颈已经从 “找列/批量化” 转移到 “向 chunk 追加数据的 per-cell 固定开销”。

`finishAppendFixed` 当前实现是：
1. 把值写入 `elemBuf`
2. `append(c.data, c.elemBuf...)` 做一次小拷贝（每个 cell 都会发生）
3. 维护 null bitmap

本 WORK 目标是：对 fixed-len 类型的 `Append*` 走 **直接写入 `data` 尾部** 的 fast path，减少第 (2) 步的频繁小拷贝。

## 修改范围（核心文件）

- `pkg/util/chunk/column.go`
- （测试/基准）`pkg/util/chunk/chunk_test.go`

## 阅读范围（读到完全理解为止）

- `pkg/util/chunk/column.go`：fixed-len 与 var-len 的数据布局、`Reset/reset/Resize` 行为
- `pkg/util/chunk/row.go`：`Row.Get*` 读取逻辑（注意它不检查 null）
- `pkg/util/chunk/chunk.go`：`Chunk.Append*` 对 column 的调用路径

## 实现方案（决策已定，避免过度设计）

### 1) fixed-len `Append*`：直接写入 `data` 尾部

对下列函数改为：
- 先扩展 `c.data` 长度（优先走 reslice，不触发 copy）
- 直接把值写进 `c.data[oldLen:oldLen+typeSize]`
- `appendNullBitmap(true)` + `c.length++`

覆盖范围：
- `AppendInt64/AppendUint64/AppendFloat32/AppendFloat64`
- `AppendTime/AppendMyDecimal`
- `AppendNull`（fixed-len 场景下可只扩展长度，不必 copy `elemBuf`）

### 2) 基准：新增 `BenchmarkAppendMixedColumns`

在 `pkg/util/chunk/chunk_test.go` 增加混合列追加 benchmark，用于更真实地评估“固定类型追加 + bytes 追加 + time 追加”的组合成本。

## 必须新增的单元测试

新增测试（建议放在 `pkg/util/chunk/chunk_test.go`，保持就近）：

1. `TestColumnAppendFixedValuesConsistency`
   - 构造含 int/uint/float/time/decimal 的 chunk
   - 追加多行值后，逐行读取校验（使用 `Row.Get*`）
2. `TestColumnNullBitmapAfterAppend`
   - 交替追加 null 与非 null
   - 校验 `IsNull` 为真时不会影响后续非 null 行的读取正确性

## 需要运行的测试

```bash
go test ./pkg/util/chunk -run 'TestColumn(AppendFixedValuesConsistency|NullBitmapAfterAppend)$' -count=1
go test ./pkg/util/chunk -count=1
go test ./pkg/util/chunk -run '^$' -bench 'BenchmarkAppend(Int|Bytes|MixedColumns)$' -benchmem -count=5
```

## 验收标准

- `BenchmarkAppendInt` 与 `BenchmarkAppendMixedColumns` 的 `ns/op` 明显下降（目标先按 10%+ 看齐，最终以实际为准）
- `allocs/op` 维持为 0
- `go test ./pkg/util/chunk -count=1` 全通过

