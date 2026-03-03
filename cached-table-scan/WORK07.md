# WORK07: findColID 跳过二分查找 — 利用 schema 稳定性预计算映射

## 背景

`row.findColID()` 对每列做二分查找，Profile 中占 23.09s (5.01%)。Cached table 场景下，每行的 colID 排列相同（同一张表 schema 不变），但每行仍然重复做二分查找。

另外 `DecodeToChunk` 对每列依次调用 `findColID`，如果列数为 N，则每行要做 N 次二分查找（O(N log M)，M 是 row 中的列数）。

## 修改文件

- `pkg/util/rowcodec/decoder.go`

## 修改内容

在 `ChunkDecoder` 中增加列映射缓存：

```go
type ChunkDecoder struct {
    decoder
    defDatum func(i int, chk *chunk.Chunk) error

    // 列映射缓存：columns[i] 在 row 中的 index（not-null 列）
    // -1 表示未初始化，-2 表示 not found（需要走 handle/default 路径）
    colMapping     []int
    mappingInited  bool
}
```

### 修改 `DecodeToChunk`

```go
func (decoder *ChunkDecoder) DecodeToChunk(rowData []byte, handle kv.Handle, chk *chunk.Chunk) error {
    err := decoder.fromBytes(rowData)
    if err != nil {
        return err
    }

    // 首行建立映射，后续行直接复用
    if !decoder.mappingInited {
        decoder.buildColMapping()
        decoder.mappingInited = true
    }

    for colIdx := range decoder.columns {
        col := &decoder.columns[colIdx]
        if col.VirtualGenCol {
            chk.AppendNull(colIdx)
            continue
        }
        if col.ID == model.ExtraRowChecksumID {
            chk.AppendNull(colIdx)
            continue
        }

        mappedIdx := decoder.colMapping[colIdx]
        if mappedIdx >= 0 {
            // fast path: 直接用缓存的 index
            colData := decoder.getData(mappedIdx)
            err := decoder.decodeColToChunk(colIdx, col, colData, chk)
            if err != nil {
                return err
            }
            continue
        }

        // slow path: 列不在 row 中，走 handle/nil/default 逻辑
        // 这里仍需要区分 isNil vs notFound
        // mappedIdx == -2: not found
        // mappedIdx == -3: is nil
        if mappedIdx == -3 {
            chk.AppendNull(colIdx)
            continue
        }

        if decoder.tryAppendHandleColumn(colIdx, col, handle, chk) {
            continue
        }

        if decoder.defDatum == nil {
            chk.AppendNull(colIdx)
            continue
        }
        err := decoder.defDatum(colIdx, chk)
        if err != nil {
            return err
        }
    }
    return nil
}

func (decoder *ChunkDecoder) buildColMapping() {
    decoder.colMapping = make([]int, len(decoder.columns))
    for i := range decoder.columns {
        col := &decoder.columns[i]
        idx, isNil, notFound := decoder.row.findColID(col.ID)
        if !notFound && !isNil {
            decoder.colMapping[i] = idx
        } else if isNil {
            decoder.colMapping[i] = -3
        } else {
            decoder.colMapping[i] = -2
        }
    }
}
```

### 重要注意事项

1. **映射有效性**：只要 row format 的 colIDs 排列不变，映射就有效。对于同一张表的所有行，这是成立的（colIDs 按 ID 排序存储）。但如果存在 schema change 导致不同行有不同的列集合，映射可能失效。
2. **安全措施**：可以在每行检查 `numNotNullCols + numNullCols` 是否与建立映射时一致，不一致则重建映射。
3. **NULL 列问题**：NULL 列的集合每行可能不同（某些行某列为 NULL，某些行不为 NULL）。因此 **不能** 简单缓存 null 映射。需要对 not-null 列做缓存，对可能为 null 的列仍走 `findColID`。

### 修正方案（考虑 NULL 变化）

由于每行的 null 列集合不同，完整跳过 `findColID` 不安全。改为：

**只缓存 colID 在 row 中的排序位置**，用于加速二分查找的起始点（hint），或者：

**更简单的方案**：对于每列，如果该列在所有行中都是 not-null 的（通过 schema 的 NOT NULL 约束判断），则缓存其 index。对于可 null 列，仍走 `findColID`。

```go
func (decoder *ChunkDecoder) buildColMapping() {
    decoder.colMapping = make([]int, len(decoder.columns))
    for i := range decoder.columns {
        col := &decoder.columns[i]
        if col.Ft != nil && mysql.HasNotNullFlag(col.Ft.GetFlag()) {
            idx, isNil, notFound := decoder.row.findColID(col.ID)
            if !notFound && !isNil {
                decoder.colMapping[i] = idx
                continue
            }
        }
        decoder.colMapping[i] = -1 // 需要每行查找
    }
}
```

在 `DecodeToChunk` 中：
```go
mappedIdx := decoder.colMapping[colIdx]
if mappedIdx >= 0 {
    colData := decoder.getData(mappedIdx)
    // ... fast path
} else {
    // 原有的 findColID 逻辑
}
```

## 测试

### 单元测试
```bash
cd pkg/util/rowcodec && go test -run 'TestDecoder|TestChunkDecoder' -count=1 -v
```

### 新增测试

在 `pkg/util/rowcodec/decoder_test.go` 中新增：

```go
func TestChunkDecoderColMapping(t *testing.T) {
    // 1. 构造多行数据，包含 NOT NULL 列和 NULLABLE 列
    // 2. 验证 NOT NULL 列走 fast path（映射缓存）
    // 3. 验证 NULLABLE 列正确处理 NULL 和非 NULL 值
    // 4. 验证不同行 NULL 集合不同时结果正确
}

func TestChunkDecoderColMappingSchemaChange(t *testing.T) {
    // 验证 row 列数变化时映射正确重建
}
```

### Benchmark
```bash
cd pkg/util/rowcodec && go test -bench='BenchmarkChunkDecoder' -benchmem -count=5
```

### 集成测试
```bash
cd pkg/executor && go test -run 'TestUnionScan|TestCacheTable' -count=1 -v
```

### 全量测试
```bash
cd pkg/util/rowcodec && go test -count=1
cd pkg/executor && go test -count=1 -timeout 600s
```

## 预期收益

对于 NOT NULL 列（通常是表中大多数列），`findColID` 的二分查找被跳过。预计节省 10-15s（`findColID` 的 19.65s flat 中的大部分），约 3-4% CPU。
