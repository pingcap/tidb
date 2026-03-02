# WORK02: CompareInt 消除额外 GetType 调用

## 背景

`CompareInt()` 在调用 `EvalInt` 之后，又对 lhsArg 和 rhsArg 各调一次 `GetType(sctx)` 来检查 unsigned flag。当参数是 ParamMarker Constant 时，`EvalInt` 内部已经调过 `GetType`，这里再调等于第3次（改完 WORK01 后是第2次）。

## 修改文件

- `pkg/expression/builtin_compare.go`

## 修改内容

### `CompareInt` (第3161行)

当前代码（第3178行）：
```go
isUnsigned0, isUnsigned1 := mysql.HasUnsignedFlag(lhsArg.GetType(sctx).GetFlag()), mysql.HasUnsignedFlag(rhsArg.GetType(sctx).GetFlag())
```

方案：对于 `*Constant` 类型的参数，ParamMarker 场景下参数值是 int64，`InferParamTypeFromDatum` 推断出来的 `TypeUnspecified` 不带 unsigned flag。可以考虑：

1. **简单方案**：把 `GetType` 结果缓存——但 `CompareInt` 的调用者不控制 arg 类型，无法避免
2. **推荐方案**：在 `Constant` 上增加一个 `GetFlag` 快捷方法，对于非 ParamMarker 的情况直接返回 `RetType.GetFlag()` 避免完整的 `GetType` 调用；对于 ParamMarker，由于 `InferParamTypeFromUnderlyingValue` 只设置 Type 不设置 Flag，可以直接返回 0

实际上更简单的做法：检查 ParamMarker 的 `InferParamTypeFromDatum` 逻辑——它不会设置 unsigned flag（只设置 type 字段），所以对 ParamMarker Constant，unsigned 永远为 false。可以在 `CompareInt` 中对 `*Constant` 做 fast path：

```go
getFlag := func(arg Expression) uint {
    if c, ok := arg.(*Constant); ok && c.ParamMarker == nil {
        return c.RetType.GetFlag()
    }
    return arg.GetType(sctx).GetFlag()
}
isUnsigned0 := mysql.HasUnsignedFlag(getFlag(lhsArg))
isUnsigned1 := mysql.HasUnsignedFlag(getFlag(rhsArg))
```

## 测试

```bash
cd pkg/expression && go test -run 'TestCompare' -count=1
cd pkg/expression && go test -run 'TestBuiltinCompare' -count=1
```

## 预期收益

在 `EQ int` 比较路径上，对每个非 ParamMarker 常量消除一次 `GetType` 调用。对 ParamMarker 常量仍需调用但无法避免（需要 flag 信息）。
