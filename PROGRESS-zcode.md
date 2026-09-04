# zcode 会话独立进度（避免与并发实例竞写 PROGRESS.md）

> 层 1 已推送（cce32f7301 上游 rebase 后）：build_cast_function 加 in_union 参数（unsigned-int+in_union → 内部名 cast_unsigned_in_union）、build_cast_to 保持 false、新增 build_cast_to_in_union 包装 true。
> 层 2 待做（下轮）：scalar_function.rs cast_ 前缀 dispatch（~:1365 strip_prefix("cast_") 处）加 `name == "cast_unsigned_in_union"` 早退臂：eval arg → to_i64_signed_with_warnings → 负数钳 0 → Datum::UInt；set_opr.rs :352 的 cast 站点改调 build_cast_to_in_union；回归 = set_opr 形状 pin 断言 cast Func 名为 cast_unsigned_in_union（pre-fix 名为 cast_unsigned，FAIL 基线）+ tidb-expr 直eval -1→UInt(0)。
> > inUnion 实现规格（下周期照做；已勘察的精确落点：求值名→CastType 映射在 scalar_function.rs:2004-2005 `("uint",_) => CastType::Unsigned`——in_union 变体在该 match 加 `("cast_unsigned_in_union",_) => CastType::UnsignedInUnion` 臂 + eval_cast 的 UnsignedInUnion 分支钳 0；build_cast_function 的 name match 在 simple_expr.rs:551）：
> > inUnion 实现规格（下周期照做）：①tidb-ast CastType 加 UnsignedInUnion 变体（镜像 Unsigned）；②tidb-expr cast.rs eval_cast 加臂：负输入钳 0（Go builtin_cast.go:998）；③simple_expr.rs build_cast_function 加 in_union 参数（unsigned-int 目标+in_union → 内部名 cast_unsigned_in_union）；④build_cast_to 保持 false 包装；新增 build_cast_to_in_union 传 true；⑤set_opr.rs build_projection4_union 的 cast 站点改调 in-union 变体；⑥回归：in-union CAST(-1 AS UNSIGNED)=0+warn；pre-fix 基线=现有 wrap。

## 已完成（已推送 hparser-integration）
- temporal 复合单元 pins（632d55f3f2）、两分组形状 pins（608dda6d29）
- 审计对账：expr-builtin item 1/2/3/4/6/7 全闭环（466d4e6120/bdf90f7245）；chunk A-3 核实过期（0b8f2de438）

## 队列
1. 按 above 规格实现 inUnion（tidb-ast→tidb-expr→planner 三层，一个 commit）
2. chunk A-1（datum 决策）
3. parser #11（结构性）
4. 分区裁剪验证（等用户对照查询）
