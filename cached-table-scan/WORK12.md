# WORK12: 将 preAlloc/buf 贯穿到 decodeIndexKvGeneral（消除 CutIndexKeyNew 和 reEncodeHandle 的 per-row 分配）

## 背景

`DecodeIndexKVEx` 接收 `buf []byte` 和 `preAlloc [][]byte` 参数用于减少分配，`decodeIndexKvOldCollation` 正确使用了它们，但 `decodeIndexKvGeneral`（line 983）完全没有接收这些参数，导致每行解码都产生多余的 `makeslice` 和 `growslice`。

Profile 数据（focus=decodeIndexKvGeneral）：
- `CutIndexKeyNew` → `makeslice`: 每行分配 `[][]byte` (colsLen)，约 9.32s
- `reEncodeHandle` → `makeslice`: 每行分配 `[][]byte` (handleColLen)，约 6.95s
- `growslice` 8.26s：`resultValues = append(resultValues, handleBytes...)` 因 cap 不够触发

而调用方 `memIndexReader.decodeIndexKeyValue`（mem_reader.go:200-206）已经做好了 preAlloc：
```go
m.decodeBuff = make([][]byte, colsLen, colsLen+len(colInfos)) // cap 包含 handle 列
```
这个 preAlloc 传入 `DecodeIndexKVEx` 后却被 `decodeIndexKvGeneral` 忽略了。

## 修改范围

- `pkg/tablecodec/tablecodec.go`
- `pkg/tablecodec/bench_test.go`（新增 benchmark）

## 阅读范围

- `pkg/tablecodec/tablecodec.go`：`DecodeIndexKVEx`(976)、`decodeIndexKvGeneral`(1892)、`decodeIndexKvOldCollation`(934)、`CutIndexKeyNew`(761)、`CutIndexKeyTo`(744)、`reEncodeHandle`(799)、`reEncodeHandleTo`(808)
- `pkg/executor/mem_reader.go`：`memIndexReader.decodeIndexKeyValue`(192)、`memRowsIterForIndex`(1141)

## 实现方案

### 1) 修改 `decodeIndexKvGeneral` 签名，接收 buf 和 preAlloc

```go
// before:
func decodeIndexKvGeneral(key, value []byte, colsLen int, hdStatus HandleStatus, columns []rowcodec.ColInfo) ([][]byte, error)

// after:
func decodeIndexKvGeneral(key, value []byte, colsLen int, hdStatus HandleStatus, columns []rowcodec.ColInfo, buf []byte, preAlloc [][]byte) ([][]byte, error)
```

### 2) 在 `DecodeIndexKVEx` 中传递参数

```go
// line 983, before:
return decodeIndexKvGeneral(key, value, colsLen, hdStatus, columns)

// after:
return decodeIndexKvGeneral(key, value, colsLen, hdStatus, columns, buf, preAlloc)
```

### 3) 用 `CutIndexKeyTo` 替代 `CutIndexKeyNew`

在 `decodeIndexKvGeneral` 内部：
```go
// before:
resultValues, keySuffix, err = CutIndexKeyNew(key, colsLen)

// after:
resultValues = preAlloc[:colsLen]
keySuffix, err = CutIndexKeyTo(key, resultValues)
```

这避免了每行一次的 `make([][]byte, length)` 分配。

**注意**：当 `segs.RestoredValues != nil` 时，line 1903 会用 `decodeRestoredValues` 的返回值覆盖 `resultValues`。此时 preAlloc 中的 cut 值被丢弃，但至少避免了 `CutIndexKeyNew` 的 makeslice。这部分的进一步优化留给 WORK13。

### 4) 用 `reEncodeHandleTo` 替代 `reEncodeHandle`，传入 buf 和 resultValues

```go
// before:
handleBytes, err := reEncodeHandle(handle, hdStatus == HandleIsUnsigned)
resultValues = append(resultValues, handleBytes...)

// after:
resultValues, err = reEncodeHandleTo(handle, hdStatus == HandleIsUnsigned, buf, resultValues)
```

由于 preAlloc 的 cap 已包含 handle 列空间（`make([][]byte, colsLen, colsLen+len(colInfos))`），当 resultValues 仍指向 preAlloc 时（非 restored 路径），append 不会触发 growslice。

对于 restored 路径（`segs.RestoredValues != nil`），`decodeRestoredValues` 返回的 `resultValues` 没有额外 cap，仍会 growslice。这部分优化留给 WORK13。

### 5) 同样处理 `decodeIndexKvForClusteredIndexVersion1`

该函数（line 1842）有相同问题，一并修改签名并传入 buf/preAlloc。

## 必须新增的测试

在 `pkg/tablecodec/bench_test.go` 新增：

- `BenchmarkDecodeIndexKVGeneral` — 构造 new-collation index KV（version 0, 有 RestoredValues），测量 `DecodeIndexKVEx` 性能和 alloc
- `BenchmarkDecodeIndexKVGeneralIntHandle` — 构造 new-collation + int handle 的 index KV，测量相同

在 `pkg/tablecodec/tablecodec_test.go` 新增：

- `TestDecodeIndexKVExGeneral` — 对 `DecodeIndexKVEx` 的 general 路径做正确性验证：
  - 包含 new collation (restored values) 场景
  - 包含 int handle 和 common handle 场景
  - 验证返回的 column values 和 handle 与期望一致

## 需要运行的测试

```bash
# 正确性
go test ./pkg/tablecodec -run 'TestDecodeIndexKVExGeneral' -count=1
go test ./pkg/tablecodec -count=1

# benchmark（改前改后对比）
go test ./pkg/tablecodec -run '^$' -bench 'BenchmarkDecodeIndexKV' -benchmem -count=5

# 集成回归
go test ./pkg/table/tables -run 'TestIndex' -count=1
go test ./pkg/executor -run 'Test(UnionScan|CacheTable)' -count=1
```

## 验收标准

- `BenchmarkDecodeIndexKVGeneral` allocs/op 减少（至少减少 2 次 makeslice：CutIndexKeyNew + reEncodeHandle）
- 所有已有测试通过
