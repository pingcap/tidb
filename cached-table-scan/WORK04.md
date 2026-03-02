# WORK04: memRowsIterForTable.Next 批量化优化（可选，中长期）

## 背景

`memRowsIterForTable.Next()` 是逐行处理模式：
1. 从 memBuffer 迭代器取一行 KV
2. `DecodeToChunk` 解码到单行 Chunk
3. `EvalBool` 对单行做条件过滤
4. 如果不匹配继续下一行

当 memBuffer 中脏数据量大时（本 profile 中这个路径占 63%），逐行 decode + eval 的开销很高。

## 修改文件

- `pkg/executor/mem_reader.go`

## 修改思路

将逐行处理改为批量处理：

```go
func (iter *memRowsIterForTable) Next() ([]types.Datum, error) {
    // 当前：每次 decode 1 行到 chunk，eval 1 行
    // 优化：decode N 行到 chunk，批量 EvalBool（利用向量化），返回第一个匹配行
}
```

### 具体步骤

1. 在 `memRowsIterForTable` 中增加一个 batch chunk（比如 32 行）
2. 循环填充 batch chunk 直到满或迭代器耗尽
3. 用向量化的 `VecEvalBool` 一次过滤整个 batch
4. 缓存匹配的行，逐个返回

### 注意事项

- 需要处理 `cursor4AddRows` 的语义不变（UnionScanExec 依赖有序返回）
- 老格式（`!rowcodec.IsNewFormat`）的 fallback 路径暂不处理
- batch size 需要权衡：太大浪费内存，太小没有向量化收益

## 测试

```bash
cd pkg/executor && go test -run 'TestUnionScan' -count=1
cd pkg/executor && go test -run 'TestDirtyTransaction' -count=1
```

### Benchmark

需要构造一个大事务场景的 benchmark：在事务中插入大量行，然后执行带条件的查询，对比逐行 vs 批量的性能。

## 预期收益

减少函数调用开销和 decode 的 per-row 固定成本。但实现复杂度较高，建议在 WORK01-03 完成后评估是否仍有必要。

## 优先级

**低** — 这是架构层面的优化，风险较高，收益依赖于 WORK01-03 完成后的残余 profile。建议先完成 WORK01-03，重新采集 profile 后再决定。
