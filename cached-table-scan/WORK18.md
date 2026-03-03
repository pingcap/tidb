# WORK18: 查询可缓存性判定 — canCacheResultSet()

## 背景

不是所有查询的结果集都能缓存。含非确定性函数（NOW/RAND/UUID）、FOR UPDATE、引用非 cached table 的子查询等，结果集不稳定，不能缓存。本任务实现判定逻辑，供后续查询路径使用。

## 依赖

WORK16

## 修改文件

- `pkg/planner/core/result_cache_check.go`（新文件）
- `pkg/planner/core/result_cache_check_test.go`（新文件）

## 修改内容

### 1. 新建 `result_cache_check.go`

```go
package core

import (
    "github.com/pingcap/tidb/pkg/parser/model"
    "github.com/pingcap/tidb/pkg/planner/core/base"
)

// CanCacheResultSet 判定一个物理计划的结果集是否可以缓存
// 仅当以下条件全部满足时返回 true：
//   1. 纯 SELECT 语句（非 DML 中的子查询）
//   2. 不含 FOR UPDATE
//   3. 不含非确定性函数（NOW, RAND, UUID, CURRENT_TIMESTAMP, SYSDATE, CONNECTION_ID 等）
//   4. 不含 session 变量引用（@@xxx）
//   5. 所有涉及的表都是 cached table（join 场景）
//   6. 不含 uncorrelated subquery 引用非 cached table
func CanCacheResultSet(plan base.PhysicalPlan, inDML bool) bool {
    if inDML {
        return false
    }
    return checkPlanTree(plan)
}

func checkPlanTree(plan base.PhysicalPlan) bool {
    // 检查当前节点
    if !checkNode(plan) {
        return false
    }
    // 递归检查所有子节点
    for _, child := range plan.Children() {
        if !checkPlanTree(child) {
            return false
        }
    }
    return true
}

func checkNode(plan base.PhysicalPlan) bool {
    // 1. 检查 FOR UPDATE
    if hasForUpdate(plan) {
        return false
    }
    // 2. 检查涉及的表是否都是 cached table
    if tblScan, ok := plan.(*PhysicalTableScan); ok {
        if tblScan.Table.TableCacheStatusType != model.TableCacheStatusEnable {
            return false
        }
    }
    if idxScan, ok := plan.(*PhysicalIndexScan); ok {
        if idxScan.Table.TableCacheStatusType != model.TableCacheStatusEnable {
            return false
        }
    }
    // 3. 检查 Selection/Projection 中的表达式是否含非确定性函数
    if sel, ok := plan.(*PhysicalSelection); ok {
        for _, cond := range sel.Conditions {
            if !cond.ConstLevel() >= expression.ConstStrict {
                // 非确定性
                // 注意：这里需要更精确的判断，ConstLevel 不适用
            }
        }
    }
    // 实际实现：遍历 plan 中所有 expression，检查 expression.HasNonDeterministicFunc()
    return true
}
```

**注意**：上面是伪代码框架，实际实现需要：
- 遍历 plan 树中所有 `expression.Expression`
- 对每个 expression 调用其 `HasCoercibility` 或自定义的 `hasSideEffect` 检查
- 利用 `expression.Expression` 的 `FuncName` 判断是否为非确定性函数
- TiDB 中已有 `expression.IsMutableEffectsExpr()` 可直接复用

### 2. 判定非确定性函数

复用 `expression.IsMutableEffectsExpr(expr)`，该函数已经覆盖了 NOW/RAND/UUID/SLEEP 等。遍历 plan 中所有 ScalarFunction 即可。

### 3. 在 plan 上标记

在 `PhysicalPlan` 接口或 `basePhysicalPlan` 中添加一个 `canCacheResult bool` 标记，在 `postOptimize` 阶段计算一次，避免执行时重复判定。

## 测试

### 新增 `result_cache_check_test.go`

测试用例：
1. **TestCanCache_SimpleSelect** — `SELECT * FROM cached_t WHERE id = 1` → true
2. **TestCanCache_WithNow** — `SELECT NOW(), id FROM cached_t` → false
3. **TestCanCache_WithRand** — `SELECT * FROM cached_t WHERE v > RAND()` → false
4. **TestCanCache_ForUpdate** — `SELECT * FROM cached_t FOR UPDATE` → false
5. **TestCanCache_JoinAllCached** — 两张 cached table join → true
6. **TestCanCache_JoinMixed** — cached table join 普通表 → false
7. **TestCanCache_SubqueryNonCached** — subquery 引用非 cached table → false
8. **TestCanCache_InDML** — DML 中的子查询 → false
9. **TestCanCache_SessionVar** — `SELECT @@sql_mode, id FROM cached_t` → false

```bash
cd pkg/planner/core && go test -run 'TestCanCache' -count=1 -v
```

### 回归测试

```bash
cd pkg/planner/core && go test -count=1 -timeout 600s
```

## 预期收益

提供准确的可缓存性判定，防止错误缓存导致结果不一致。
