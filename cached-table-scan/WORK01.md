# WORK01: Constant.EvalInt 消除重复 GetType 调用

## 背景

`Constant.EvalInt()` 在 ParamMarker 场景下调用了两次 `GetType(ctx)`（第345行和第349行），每次都会触发 `NewFieldType` 堆分配。这是 profile 中最大的单点浪费。

## 修改文件

- `pkg/expression/constant.go`

## 修改内容

### `EvalInt` (第336行)

当前代码：
```go
if c.GetType(ctx).GetType() == mysql.TypeNull || dt.IsNull() {
    return 0, true, nil
} else if c.GetType(ctx).Hybrid() || dt.Kind() == types.KindString {
```

改为：
```go
tp := c.GetType(ctx)
if tp.GetType() == mysql.TypeNull || dt.IsNull() {
    return 0, true, nil
} else if tp.Hybrid() || dt.Kind() == types.KindString {
```

注意：`EvalReal`（第369行）和 `EvalDecimal`（第405行）已经用了局部变量 `tp`，不需要改。

`EvalString`、`EvalTime`、`EvalDuration`、`EvalJSON` 只调了一次 `GetType`，不需要改。但建议统一风格也提取为局部变量。

## 测试

### 单元测试
```bash
cd pkg/expression && go test -run 'TestConstant|TestSpecificConstant|TestVectorizedConstant' -count=1
```

### 验证正确性
```bash
cd pkg/expression && go test -run 'TestCompare' -count=1
```

### Benchmark（验证性能提升）

在 `pkg/expression/constant_test.go` 中新增 benchmark：

```go
func BenchmarkConstantEvalIntWithParamMarker(b *testing.B) {
    // 构造一个带 ParamMarker 的 Constant，循环调用 EvalInt
    // 对比修改前后的 allocs/op
}
```

运行：
```bash
cd pkg/expression && go test -bench='BenchmarkConstantEvalIntWithParamMarker' -benchmem -count=5
```

## 预期收益

减少 ~50% 的 `Constant.GetType` 调用（在 EvalInt 路径上），直接减少堆分配和 GC 压力。
