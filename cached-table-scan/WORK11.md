# WORK11: rowcodec 按列预编译解码器（降低 decodeColToChunk 分发与构造开销）

## 背景

在 `DecodeToChunk` 已经做了 `colMapping` 优化后，`decodeColToChunk` 的 per-cell 成本（例如每次 `switch col.Ft.GetType()`、反复读取 flag/decimal、timestamp 的 TZ 判断与 time 构造）依然会放大到非常可观的 CPU。

本 WORK 的目标是：对 `ChunkDecoder` 的列元信息做一次性编译（per-decoder），把 per-cell 的重复工作降到最低，同时保持语义完全一致。

## 修改范围（核心文件）

- `pkg/util/rowcodec/decoder.go`
- （测试/基准）`pkg/util/rowcodec/decoder_test.go`、`pkg/util/rowcodec/bench_test.go`

## 阅读范围（读到完全理解为止）

- `pkg/util/rowcodec/decoder.go`：`ChunkDecoder` 生命周期、`DecodeToChunk` 主循环、`decodeColToChunk`
- `pkg/types` 内 time 相关实现：`types.Time` 的 `FromPackedUint/ConvertTimeZone` 行为约束
- `pkg/parser/mysql`：类型枚举与 unsigned/timestamp 语义

## 实现方案（决策已定，避免过度设计）

### 1) 增加 compiled 列信息

在 `ChunkDecoder` 中增加一份与 `columns` 同长度的编译结果（例如 `compiled []compiledCol`），初始化时预存：

- `tp byte`：`col.Ft.GetType()`
- `unsigned bool`：`mysql.HasUnsignedFlag(col.Ft.GetFlag())`
- `fsp int`：`col.Ft.GetDecimal()`（time/duration 用）
- `needTZConvert bool`：`tp==mysql.TypeTimestamp && decoder.loc!=nil`
- （可选）一个轻量分发枚举：int/uint/float/bytes/time/decimal/...，避免每次 switch

### 2) decodeColToChunk 改为基于 compiled 信息走分支

- 常见类型（int/uint/bytes/time）走直接分支，避免反复取 `FieldType` 元信息
- timestamp：
  - `decoder.loc == nil` 或 `t.IsZero()` 时不做 TZ 转换
  - `needTZConvert` 预先计算

## 必须新增的测试

在 `pkg/util/rowcodec/decoder_test.go` 增加：

- `TestChunkDecoderCompiledColsCorrectness`
  - 覆盖：int/uint、bytes、datetime、timestamp（loc=nil 与 loc!=nil 两档）
  - 校验：DecodeToChunk 结果与期望一致（可通过构造 rowData + 读回 row 值验证）

## 需要运行的测试与基准

```bash
go test ./pkg/util/rowcodec -run 'TestChunkDecoderCompiledColsCorrectness$' -count=1
go test ./pkg/util/rowcodec -count=1
go test ./pkg/util/rowcodec -run '^$' -bench 'BenchmarkDecode(WideRowToChunk|Decode)$' -benchmem -count=5

# cached table / union scan 相关回归（按实际存在的 test 名匹配）
go test ./pkg/executor -run 'Test(UnionScan|CacheTable)' -count=1
```

## 验收标准

- `BenchmarkDecodeWideRowToChunk` 有可解释下降（目标先按 5%+ 看齐，最终以实际为准），alloc 不上升
- `go test` 回归全部通过

