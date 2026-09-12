# UNION ALL 隐式整数 CAST 对齐修复

Rust 之前用完整 `FieldType::equal` 判断 UNION 分支是否需要 cast，因 flen/NULL flag 差异保留了 `cast_signed(test.u.b)`。Go `BuildCastFunctionWithCheck` 随后折叠同 EvalType 的隐式 cast。

现在当 source/target `EvalType` 相同时直接保留原列表达式，同时继续使用 UNION 的目标 schema 类型。Go 对照计划与 Rust 均为 `gt(test.u.b, 0)`、`estRows=3333.33`；Go 的 `EliminateUnionAllDualItem` 还会删除常量 false 分支，因此测试不再错误要求输出 `TableDual`。

验证：目标 session 回归通过；未放宽谓词断言。
