# WORK03: Constant.GetType 避免堆分配（核心优化）

## 背景

`Constant.GetType()` 对 ParamMarker 场景每次调用都执行：
1. `NewFieldType(mysql.TypeUnspecified)` → `NewFieldTypeBuilder()` 分配 builder 到堆上 → `BuildP()` 返回堆指针
2. `GetParamValue()` 获取参数值
3. `InferParamTypeFromDatum()` 设置类型字段

Profile 显示 `GetType` 累计占 26.21%，其中 `NewFieldType` 占 16.54%。

## 修改文件

- `pkg/expression/constant.go`
- `pkg/types/field_type.go`（可选）

## 修改内容

### 方案 A：接受 `*FieldType` 参数避免分配（推荐）

新增方法 `GetTypeTo`，让调用者传入预分配的 `FieldType`：

```go
func (c *Constant) GetTypeTo(ctx EvalContext, tp *types.FieldType) {
    if c.ParamMarker != nil {
        tp.SetType(mysql.TypeUnspecified)
        // 重置其他字段为默认值...
        dt, err := c.ParamMarker.GetUserVar(ctx)
        if err != nil {
            return
        }
        types.InferParamTypeFromDatum(&dt, tp)
        return
    }
    *tp = *c.RetType
}
```

然后在 `EvalInt` 等热路径中使用栈上的 `FieldType`：

```go
var tp types.FieldType
c.GetTypeTo(ctx, &tp)
if tp.GetType() == mysql.TypeNull || dt.IsNull() {
```

### 方案 B：NewFieldType 对 TypeUnspecified 特化

在 `types/field_type.go` 中为 `TypeUnspecified` 提供轻量构造：

```go
func InitUnspecifiedFieldType(tp *FieldType) {
    tp.SetType(mysql.TypeUnspecified)
    tp.SetCharset(charset.CharsetBin)
    tp.SetCollate(charset.CollationBin)
    tp.SetFlen(0)
    tp.SetDecimal(0)
}
```

避免 Builder 模式的间接开销和堆分配。

### 方案 C：在 GetType 中直接栈分配

```go
func (c *Constant) GetType(ctx EvalContext) *FieldType {
    if c.ParamMarker != nil {
        var tp types.FieldType
        types.InitUnspecifiedFieldType(&tp)
        dt, _ := c.ParamMarker.GetUserVar(ctx)
        types.InferParamTypeFromDatum(&dt, &tp)
        return &tp  // 逃逸到堆取决于调用者是否持有引用
    }
    return c.RetType
}
```

注意：如果返回 `&tp`，Go 逃逸分析会将 `tp` 分配到堆上（因为返回了指针）。所以方案 A（传入指针）是最干净的。

### 推荐路径

1. 先实现方案 B 的 `InitUnspecifiedFieldType`
2. 在 `EvalInt`/`EvalReal` 等方法中用栈变量 + `InitUnspecifiedFieldType` + `InferParamTypeFromDatum` 替代 `GetType` 调用
3. `GetType` 本身保持不变（它是接口方法，改签名影响面太大）

## 测试

### 单元测试
```bash
cd pkg/expression && go test -run 'TestConstant|TestSpecificConstant' -count=1
cd pkg/types && go test -run 'TestFieldType' -count=1
```

### Benchmark
```bash
cd pkg/expression && go test -bench='BenchmarkConstantEvalInt' -benchmem -count=5
```

重点关注 `allocs/op` 的变化，预期从 2 allocs/op 降到 0。

### 集成测试
```bash
cd pkg/executor && go test -run 'TestUnionScan' -count=1
```

## 预期收益

彻底消除 ParamMarker Constant eval 路径上的堆分配，预计减少 ~16% 的 CPU 占用（对应 `NewFieldType` 的 16.54% cum）。同时大幅降低 GC 压力（`mallocgc` 19.08% 中相当一部分来自这里）。
