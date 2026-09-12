# GROUP BY 回归与 Go master 契约一致

基线 `a5da79cfd8`，固定 Go master `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85`。

## 已复现的测试问题

group_by_resolves_against_the_select_list 对没有 ORDER BY 的 `SELECT a,COUNT(*) FROM t GROUP BY 1` 断言固定顺序。Rust 独立复现为 1/2、3/1、2/1，测试期望 1/2、2/1、3/1，日志 `/tmp/group-contract-red.log`。

Go 同样数据 (1,30),(1,31),(2,20),(3,10)，计划使用 HashAgg，无 Sort：`/tmp/group-unordered-go.out`。连续执行 15 次得到多种顺序，`/tmp/group-unordered-repeated-go.out`，因此固定行序不是 Go 契约。

同一用例还将 GROUP BY 0/3 的错误码写成 1054。Go `pkg/planner/core/logical_plan_builder.go:3478` 使用 errors.Errorf；真实 GROUP BY 0 返回 1105/HY000，`/tmp/group-position-current-go.out`。

## 修正

保留原无 ORDER BY SQL，按完整行排序后比较，仍检查所有分组、值与重复次数。已有 ORDER BY 1/2 的严格顺序断言不变。越界位置改为 1105，保留错误文本检查和聚合位置 1056 检查。没有修改生产 SQL 行为。

## Ready 验证

Cargo 环境 `RUSTUP_TOOLCHAIN=1.97 RUSTFLAGS='' RUST_MIN_STACK=33554432`。

```bash
cargo test --manifest-path rust/Cargo.toml -p tidb-session --lib group_by_resolves_against_the_select_list
cargo test --manifest-path rust/Cargo.toml -p tidb-session --test all
make lint
```

结果：原始定向回归 1 passed / 0 failed（`/tmp/group-contract-green.log`），session 集成 310 passed / 0 failed（`/tmp/group-contract-integration.log`），make lint 退出 0（`/tmp/group-contract-lint.log`）。两项测试的原进程均已回收并确认退出 0。

## 分区失败的本轮证据

同一个 Go unistore 实例确认分区表 fast path：`/tmp/partition-point-go.out` 中 a IN(1,2) 为单个 Batch_Point_Get，access object 为 table:t, partition:p1,P2；IN(1,2,1) 保持 estRows=3。Rust fast planner 因 partition.is_some() 直接退出，普通计划包含独立分区节点。恢复需要正确传递分区选择和路由信息，未在本次测试契约提交中实现，不应仅改期望。

Go 实例日志 `/tmp/partition-point-oracle.log`，已正常退出。全量 Rust、RealTiKV、Go/Bazel 门禁未完成，整体目标保持进行中。
