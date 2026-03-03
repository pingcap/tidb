# WORK09: 建立可复现基准 + 锁定 DecodeToChunk 热点构成（profile_1.pb.gz）

## 背景与结论（来自 profile_1.pb.gz）

在 `memRowsBatchIterForTable.Next()` 已经做了 batch decode + vectorized filter、并且 `rowcodec.(*ChunkDecoder).DecodeToChunk()` 已经有列映射缓存（`colMapping`）之后，profile 里 `DecodeToChunk` 仍然很重，且热点主要集中在：

- `pkg/util/rowcodec/decoder.go`
  - `(*ChunkDecoder).DecodeToChunk`
  - `(*ChunkDecoder).decodeColToChunk`
- `pkg/util/chunk/column.go`
  - `(*Column).finishAppendFixed`
  - `(*Column).appendNullBitmap`
  - 以及 `AppendInt64/AppendBytes/AppendTime` 等追加路径

因此本轮优化的前置条件是：把 “pprof 上看到的热点” 变成 “本地可重复跑的 benchmark + 指标基线”，确保后续改动不跑偏。

## 阅读范围（读到完全理解为止）

- `pkg/util/rowcodec/decoder.go`：`DecodeToChunk` 主循环、`decodeColToChunk` 各类型分支
- `pkg/util/rowcodec/row.go`：`fromBytes/getData/findColID`
- `pkg/util/chunk/column.go`：`finishAppendFixed/appendNullBitmap`、fixed/varlen 追加实现
- `pkg/util/chunk/chunk.go`：`Chunk.Append*` 是否有额外开销/检查

## 基线采集（固定命令，写入本文件便于对比）

### pprof（profile 文件不变）

```bash
go tool pprof -top -nodecount=30 $HOME/tmp/shein-poc/profiles/profile_1.pb.gz
go tool pprof -list DecodeToChunk $HOME/tmp/shein-poc/profiles/profile_1.pb.gz
go tool pprof -list decodeColToChunk $HOME/tmp/shein-poc/profiles/profile_1.pb.gz
go tool pprof -list finishAppendFixed $HOME/tmp/shein-poc/profiles/profile_1.pb.gz
go tool pprof -list appendNullBitmap $HOME/tmp/shein-poc/profiles/profile_1.pb.gz
```

### benchmark 基线（必须保存一次输出）

#### rowcodec：宽表 + batch 模拟

新增 benchmark：`BenchmarkDecodeWideRowToChunk`（见 `pkg/util/rowcodec/bench_test.go`）。

```bash
go test ./pkg/util/rowcodec -run '^$' -bench 'BenchmarkDecodeWideRowToChunk' -benchmem -count=5
```

#### chunk：混合列追加（贴近 rowcodec decode 的追加形态）

新增 benchmark：`BenchmarkAppendMixedColumns`（见 `pkg/util/chunk/chunk_test.go`）。

```bash
go test ./pkg/util/chunk -run '^$' -bench 'BenchmarkAppendMixedColumns' -benchmem -count=5
```

## 验收标准

- `BenchmarkDecodeWideRowToChunk` 与 `BenchmarkAppendMixedColumns` 在本机可稳定复现（`count=5` 抖动可接受）
- 后续每个 WORK 的优化必须：
  1) 先跑 WORK09 的基线命令
  2) 再跑同一组命令对比，并把“前后”结果贴回对应 WORK 末尾

## 需要运行的回归测试（确保后续优化不破坏语义）

```bash
go test ./pkg/util/rowcodec -count=1
go test ./pkg/util/chunk -count=1
```

