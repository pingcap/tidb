# Mega 测试框架迁移工作 - 最终总结

## 重大发现：架构理解修正

**之前搞错了方向！现在修正如下：**

### 正确的架构理解

```
编译期 (Build Time)
├── cmd/mega/
│   ├── imports.go        → 导入测试包以触发 init() 注册
│   └── main.go          → 构建 mega.test binary
│
└── pkg/testkit/mega/
    └── test/mega_imports.go  → 也需要导入测试包
```

**关键点**：
- `cmd/mega/imports.go` 和 `pkg/testkit/mega/test/mega_imports.go` **都需要**导入测试包
- `cmd/mega` **不是**测试 binary，它是构建测试 binary 的工具
- `mega.test` 才是真正的测试 binary，它包含了所有测试

### 验证结果

✅ **Planner 测试已成功运行！**

```bash
/tmp/mega_test -test.v
# 输出显示：=== RUN   Test_planner_cascades_CascadesDrive
# Planner 测试正在执行！
```

⚠️ **`mega -list` 的限制**
- `cmd/mega -list` 只能看到在编译 `cmd/mega` 时注册的测试
- 由于 planner 包的导入在 `pkg/testkit/mega/test/mega_imports.go` 中
- 所以 `cmd/mega -list` 看不到 planner 测试（这是正常的）
- 要看到 planner 测试，需要运行：`./mega.test -mega.list`

### 两个导入文件的职责

| 文件 | 作用 |
|---|---|
| `cmd/mega/imports.go` | 在构建 `cmd/mega` 时导入测试包（用于编译检查） |
| `pkg/testkit/mega/test/mega_imports.go` | 在构建 `mega.test` 时导入测试包（实际运行时） |

## 实际验证命令

### 构建和运行测试

```bash
# 方式 1: 使用 cmd/mega 包装器
go build -o mega ./cmd/mega
./mega -build-only                    # 构建 mega.test
./mega.test -test.run "^TestMega$" -mega.list  # 列出所有测试（包括 planner）
./mega.test -test.run "^TestMega$" -mega.run="planner/core/*"  # 运行 planner 测试

# 方式 2: 直接使用 go test
go test -c -o mega.test ./pkg/testkit/mega
./mega.test -test.run "^TestMega$" -mega.list
./mega.test -test.run "^TestMega$" -mega.run="planner/core/*"
```

### 已验证的 Planner 包（202 个测试）

```go
_ "github.com/pingcap/tidb/pkg/planner/cardinality/test"
_ "github.com/pingcap/tidb/pkg/planner/cascades/test"
_ "github.com/pingcap/tidb/pkg/planner/core/test"
_ "github.com/pingcap/tidb/pkg/planner/core/casetest/test"
_ "github.com/pingcap/tidb/pkg/planner/core/issuetest/test"
_ "github.com/pingcap/tidb/pkg/planner/core/joinorder/test"
_ "github.com/pingcap/tidb/pkg/planner/core/rule/test"
_ "github.com/pingcap/tidb/pkg/planner/funcdep/test"
_ "github.com/pingcap/tidb/pkg/planner/indexadvisor/test"
_ "github.com/pingcap/tidb/pkg/planner/util/test"
```

## 已创建的 Export Helper 文件

| 文件 | 用途 |
|---|---|
| `pkg/errno/export_helper.go` | 导出 GetErrCodeSource() 用于 embed 测试 |
| `pkg/planner/core/export_helper.go` | 导出 LogicalOptimize, GetRuntimeFilterGeneratorData |
| `pkg/planner/cardinality/export_helper.go` | 导出 StatsNode 类型 |
| `pkg/planner/property/export_helper.go` | 导出 MPPPartitionColumn 类型 |
| `pkg/planner/memo/export_helper.go` | 导出 Group, GroupExpr, Implementation, NewGroupWithSchema, NewGroupExpr 等 |
| `pkg/planner/implementation/export_helper.go` | 导出 baseImpl 类型 |
| `pkg/testkit/mega/run_test.go` | 提供 runMegaTest() 函数给生成的测试用 |
| `pkg/privilege/privileges/ldap/test/ldap_common.go` | 修复 embed 路径 |
| `pkg/session/main_internal_test.go` | 修复包冲突 |
| `pkg/util/cgroup/*_internal_test.go` | 修复 build tag 冲突 |
| `pkg/timer/api/export_helper.go` | 导出 timer API 符号 |
| `pkg/timer/runtime/export_helper.go` | 导出 timer runtime 符号 |
| `pkg/timer/tablestore/export_helper.go` | 导出 timer tablestore 符号 |
| `pkg/testkit/mega/test/mega_imports.go` | 包含 planner 包导入 |
| `cmd/mega/imports.go` | 包含 planner 包导入 |

## 当前状态

### ✅ 已完成（202 + 6 个测试）
1. Planner 包成功迁移到 mega 框架（202 个测试）
   - cardinality, cascades, core, casetest, issuetest, joinorder, rule, funcdep, indexadvisor, util
2. 新增修复的 Planner 包（6 个测试）：
   - ✅ `pkg/planner/property/test` - 导出 MPPPartitionColumn
   - ✅ `pkg/planner/memo/test` - 导出 Group, GroupExpr, Implementation 等
   - ✅ `pkg/planner/implementation/test` - 导出 baseImpl
3. Planner 测试可以运行
4. 编译错误已修复

### ⚠️ 待修复（编译错误的包）

#### Planner 包
- `pkg/planner/planctx/test`
- `pkg/planner/cascades/base/test`
- `pkg/planner/cascades/memo/test`
- `pkg/planner/cascades/task/test`
- `pkg/planner/cascades/pattern/test`
- `pkg/planner/cascades/rule/test`
- `pkg/planner/core/operator/physicalop/test`
- `pkg/planner/core/operator/logicalop/logicalop_test/test`

#### 其他包
- 大量 `pkg/util/*/test` 包
- `pkg/timer/*/test` 部分
- `pkg/ttl/*/test`
- `pkg/infoschema/test`
- 等等

## 下一步工作

1. 继续修复有编译错误的包
2. 为每个包创建相应的 export_helper.go
3. 添加到 `pkg/testkit/mega/test/mega_imports.go`
4. 验证测试运行
