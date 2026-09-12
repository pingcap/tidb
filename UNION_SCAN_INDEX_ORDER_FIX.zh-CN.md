# UnionScan 修复结论更正

## 根因

提交 `7ca9565ce2` 的全局索引排序不符合 Go master，已移除。Go 的 `getOneRow` 只比较 snapshot 与 added 的当前行，只推进获胜的一路；一路耗尽后直接输出另一条流，不全局重排 snapshot。

## 修复

Rust session 路径还需要保留 snapshot 与 staged 两路输入，并按 Go 逐行比较和 shadow suppression 合并。不能用 dirty-table 全局索引排序替代。

## 验证

已有 oracle SQL：`/tmp/union-scan-oracle.sql`；结果：`/tmp/union-scan-go.out`。
Go IndexLookUp 的子节点为 `keep order:false`，实际 handle 顺序如下：

| mutation | Go 顺序 |
| --- | --- |
| insert + update + delete | 5,1,4,2 |
| insert | 5,1,2,3,4 |
| update | 2,1,3,4 |
| delete | 2,3,4 |
| index key tie insert | 6,1,2,3,4 |

此前 `11 passed` 只证明旧断言通过，不能证明 Go 对齐；五个失败不能计为已修复。
现有测试的部分顺序断言与 Go 记录冲突，仍需真实两路合并实现和 SQL 对照验证。
