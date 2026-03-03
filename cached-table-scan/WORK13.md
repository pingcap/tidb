# WORK13: 缓存 decodeRestoredValues 的 map/decoder/编码 buffer（消除 per-row 重复构建）

## 背景

`decodeRestoredValues`（tablecodec.go:844）是 `decodeIndexKvGeneral` 下最大的 CPU 消耗者（34.66s，9.56%），每行调用时：

1. **`make(map[int64]int, len(columns))`** + mapassign 循环构建 colIDs map — 约 6.72s（makemap 0.78s + mapassign 3.74s + mapaccess 2.20s）
2. **`rowcodec.NewByteDecoder(...)`** 每行构造新 decoder — 小开销但不必要
3. **`decodeToBytesInternal` 内 `make([][]byte, len(outputOffset))`** — per-row makeslice
4. **`encodeOldDatum` 内 `make([]byte, 0, ...)`** — per-column makeslice，17.05s cum

这些对象在同一个 index scan 的所有行之间完全相同（columns/colIDs 不变），应该只构建一次。

## 修改范围

- `pkg/tablecodec/tablecodec.go`
- `pkg/util/rowcodec/decoder.go`
- `pkg/executor/mem_reader.go`
- 测试：`pkg/tablecodec/tablecodec_test.go` 或 `bench_test.go`、`pkg/util/rowcodec/decoder_test.go`

## 阅读范围

- `pkg/tablecodec/tablecodec.go`：`decodeRestoredValues`(844)、`decodeIndexKvGeneral`(1892)
- `pkg/util/rowcodec/decoder.go`：`BytesDecoder` struct、`NewByteDecoder`(520)、`decodeToBytesInternal`(533)、`encodeOldDatum`(623)
- `pkg/executor/mem_reader.go`：`memIndexReader` struct(53)、`decodeIndexKeyValue`(192)

## 实现方案

### 1) 新增 `IndexRestoredDecoder` 结构体缓存 map/decoder

在 `pkg/tablecodec/tablecodec.go` 中新增：

```go
// IndexRestoredDecoder caches the colIDs map and BytesDecoder across rows
// to avoid per-row allocations in decodeRestoredValues.
type IndexRestoredDecoder struct {
    colIDs  map[int64]int
    rd      *rowcodec.BytesDecoder
    values  [][]byte  // pre-allocated, reused across rows
}

func NewIndexRestoredDecoder(columns []rowcodec.ColInfo) *IndexRestoredDecoder {
    colIDs := make(map[int64]int, len(columns))
    for i, col := range columns {
        colIDs[col.ID] = i
    }
    rd := rowcodec.NewByteDecoder(columns, []int64{-1}, nil, nil)
    return &IndexRestoredDecoder{
        colIDs: colIDs,
        rd:     rd,
        values: make([][]byte, len(columns)),
    }
}
```

### 2) 新增 `decodeToBytesInto` 方法，接收 pre-allocated values

在 `pkg/util/rowcodec/decoder.go` 中新增 `BytesDecoder.DecodeToBytesNoHandleInto`：

```go
// DecodeToBytesNoHandleInto is like DecodeToBytesNoHandle but writes into
// a caller-provided values slice instead of allocating a new one.
// The arena is used for encodeOldDatum allocations; caller should reset it between rows.
func (decoder *BytesDecoder) DecodeToBytesNoHandleInto(
    outputOffset map[int64]int, value []byte, values [][]byte, arena []byte,
) ([][]byte, []byte, error)
```

内部逻辑与 `decodeToBytesInternal` 相同，但：
- 使用 caller 传入的 `values`（清零后复用）而非 `make`
- `encodeOldDatum` 改为写入 `arena`（append 式，返回 sub-slice），而非每列 `make`

### 3) 改写 `encodeOldDatum` 为 arena 模式

新增接受 arena 参数的版本：

```go
func (*BytesDecoder) encodeOldDatumToArena(tp byte, val []byte, arena []byte) (result []byte, newArena []byte)
```

- `arena = append(arena, flag)` + encode into arena
- `result = arena[startOffset:]`
- 返回 `(result, arena)` 让 caller 继续追加

这消除了 `encodeOldDatum` 每列的 `make([]byte, 0, ...)` 分配（17.05s 中的分配部分）。

### 4) 在 `decodeIndexKvGeneral` 中使用 `IndexRestoredDecoder`

修改 `decodeIndexKvGeneral` 签名，增加 `*IndexRestoredDecoder` 参数（可选，nil 时 fallback 到原逻辑）：

```go
func decodeIndexKvGeneral(key, value []byte, colsLen int, hdStatus HandleStatus,
    columns []rowcodec.ColInfo, buf []byte, preAlloc [][]byte, restoredDec *IndexRestoredDecoder) ([][]byte, error) {
    // ...
    if segs.RestoredValues != nil {
        if restoredDec != nil {
            resultValues, err = restoredDec.Decode(segs.RestoredValues)
        } else {
            resultValues, err = decodeRestoredValues(columns[:colsLen], segs.RestoredValues)
        }
    }
    // ...
}
```

### 5) 修改 `DecodeIndexKVEx` 签名，传递 `IndexRestoredDecoder`

```go
func DecodeIndexKVEx(key, value []byte, colsLen int, hdStatus HandleStatus,
    columns []rowcodec.ColInfo, buf []byte, preAlloc [][]byte, restoredDec *IndexRestoredDecoder) ([][]byte, error)
```

### 6) 在 `memIndexReader` 中缓存 `IndexRestoredDecoder`

在 `memIndexReader` struct 中新增字段：

```go
type memIndexReader struct {
    // ... existing fields ...
    restoredDec *tablecodec.IndexRestoredDecoder  // cached, lazily initialized
}
```

在 `decodeIndexKeyValue` 中懒初始化并传入。

## 必须新增的测试

在 `pkg/tablecodec/tablecodec_test.go` 新增：

- `TestIndexRestoredDecoderCorrectness` — 验证 `IndexRestoredDecoder.Decode` 与原始 `decodeRestoredValues` 输出一致
  - 多行数据复用同一 decoder，验证无状态泄漏
  - 覆盖 int/uint/bytes/nil 列类型

在 `pkg/tablecodec/bench_test.go` 新增：

- `BenchmarkDecodeRestoredValues` — 对比优化前后的 allocs 和 ns/op

在 `pkg/util/rowcodec/decoder_test.go` 新增：

- `TestEncodeOldDatumArena` — 验证 arena 模式 `encodeOldDatumToArena` 与原始 `encodeOldDatum` 输出一致

## 需要运行的测试

```bash
# 正确性
go test ./pkg/tablecodec -run 'TestIndexRestoredDecoder' -count=1
go test ./pkg/util/rowcodec -run 'TestEncodeOldDatumArena' -count=1
go test ./pkg/tablecodec -count=1
go test ./pkg/util/rowcodec -count=1

# benchmark
go test ./pkg/tablecodec -run '^$' -bench 'BenchmarkDecodeIndexKV|BenchmarkDecodeRestoredValues' -benchmem -count=5

# 集成回归
go test ./pkg/table/tables -run 'TestIndex' -count=1
go test ./pkg/executor -run 'Test(UnionScan|CacheTable)' -count=1
```

## 验收标准

- `BenchmarkDecodeRestoredValues` allocs/op 显著降低（目标：map/decoder/values 的 3 次 alloc → 0，encodeOldDatum per-column alloc → 0）
- `BenchmarkDecodeIndexKVGeneral`（WORK12 添加）ns/op 和 allocs 进一步下降
- 所有已有测试通过
