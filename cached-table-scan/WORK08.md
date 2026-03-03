# WORK08: DecodeRowKey 优化 — cached table 路径避免重复 key 解码

## 背景

`tablecodec.DecodeRowKey` 在 `memRowsIterForTable.Next` 中每行调用一次，累计 21.50s (4.66%)。它从 KV key 中提取 handle，涉及 bytes decode 和内存分配。

对于 cached table 的全表扫描，如果 table 使用 int handle（PKIsHandle 或 auto rowid），可以用更轻量的方式直接从 key 尾部提取 int handle，跳过完整的 `DecodeRowKey`。

## 修改文件

- `pkg/executor/mem_reader.go`

## 修改内容

在 `memRowsIterForTable.Next()` 中，对 int handle 表做 fast path：

```go
// 在 memRowsIterForTable 结构体中增加字段
type memRowsIterForTable struct {
    // ...existing fields...
    intHandle bool // 是否为 int handle 表
}

// 初始化时设置
func (m *memTableReader) getMemRowsIter(ctx context.Context) (memRowsIter, error) {
    // ...
    intHandle := m.table.PKIsHandle || !m.table.IsCommonHandle
    return &memRowsIterForTable{
        // ...
        intHandle: intHandle,
    }, nil
}
```

在 `Next()` 中：

```go
if iter.intHandle {
    // fast path: 直接从 key 尾部 8 bytes 提取 int handle
    // key format: tablePrefix(1) + tableID(8) + recordPrefix(2) + handle(8)
    // 等价于 DecodeRowKey 但避免了完整的 codec.DecodeInt 调用
    handle, err := tablecodec.DecodeRowKey(key)
    // TODO: 如果 DecodeRowKey 内部可以优化为直接读尾部 8 bytes 就更好
    // 或者用 kv.IntHandle(codec.DecodeInt(key[len(key)-8:]))
}
```

### 更实际的优化：`DecodeRowKey` 本身的快速路径

查看 `tablecodec.DecodeRowKey` 实现，如果它已经足够快则不需要改调用方，而是优化函数本身。

实际上，对 int handle 表来说，关键开销可能在 `codec.DecodeInt` 和 `kv.IntHandle` 的分配。可以：

1. 在 `memRowsIterForTable` 中复用 handle 对象
2. 或直接将 handle int64 传给 `DecodeToChunk`，避免 `kv.Handle` 接口的 boxing

```go
// 在 Next() 中复用 intHandle
var h kv.Handle
if iter.intHandle {
    _, handleVal, _ := codec.DecodeInt(key[len(tablecodec.TablePrefix())+8+len(tablecodec.RecordPrefix()):])
    h = kv.IntHandle(handleVal) // IntHandle 是 int64 alias，不分配堆
}
```

## 测试

### 单元测试
```bash
cd pkg/executor && go test -run 'TestUnionScan' -count=1 -v
cd pkg/executor && go test -run 'TestCacheTable' -count=1 -v
```

### 新增测试

在 `pkg/executor/mem_reader_test.go` 中：

```go
func TestMemRowsIterFastDecodeRowKey(t *testing.T) {
    // 验证 fast path 和 slow path (common handle) 结果一致
    // 构造 int handle 和 common handle 的 key，对比解码结果
}
```

### 集成测试
```bash
cd tests/integrationtest && go test -run 'TestCacheTable' -count=1 -v
```

## 预期收益

减少 `DecodeRowKey` 的 overhead，预计节省 5-10s（`DecodeRowKey` 21.50s 中的部分），约 1-2% CPU。收益不如 WORK06/07 大，但改动简单低风险。

## 优先级

**中** — 改动简单，可以在 WORK05/06 之后顺手做。
