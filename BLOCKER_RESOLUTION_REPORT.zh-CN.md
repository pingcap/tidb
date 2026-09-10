# Rust 集成测试 Blocker Resolution

## 2026-09-10 scan-pushdown

提交 `53fc7659a2` 在 Rust 1.97 下通过完整 Real TiKV scan-pushdown：

```text
RUSTUP_TOOLCHAIN=1.97 bash rust/scripts/run-realtikv-scan-pushdown.sh
EXIT:0
```

- `PI()` 与 Go 返回完全相同的 `id` 行集合。
- residual predicate 不再触发 `Chunk::column` 越界、1105 或断连接。
- COT predicate/projection 保持 Go errno `1690`。
- PD shutdown request-handle 警告仅影响 receipt 观察，不影响 SQL 结果比较。

## PD bootstrap 继续验证（2026-09-10）

新增的确定性 gRPC 回归先返回启动错误、下次返回合法 membership；无需 sleep 改写 fixture。
原始错误类型 `1` 是 UNKNOWN，`NOT_BOOTSTRAPPED` 是 `2`，之前将两者混同的分析不成立。
回归在仅重试 NOT_BOOTSTRAPPED 的实现下失败，错误精确为 `[PD:server:ErrServerNotStarted]server not started`。
现在同时重试该明确启动错误，保留其他错误及跨集群检查。退避上限参照 Go `pkg/store/store.go` / `pkg/util/misc.go`，启动重试依据 go.mod 固定 PD client 的 `servicediscovery.serviceDiscovery.initRetry`。

验证：`RUSTUP_TOOLCHAIN=nightly-2026-08-22 cargo test --manifest-path rust/Cargo.toml -p tidb-pd-client --test all`：45 passed。
日志：`/tmp/pd-bootstrap-red.log`、`/tmp/pd-bootstrap-green.log`。这是 WIP 范围验证，未宣称全部门禁通过。

lock-recovery 最近保留日志显示 `the cluster catalog has no table mysql.tidb`，不是测试成功。
进一步阅读脚本发现它使用 Go `beforeCommitSecondaries` failpoint 构造已提交 primary / 未提交 secondary，再由 Rust 集成测试验证锁恢复。
此前强行添加 Rust read-table wrapper 是 harness 方向错误；下一步恢复 Go fixture server，仍由 Rust 测试验证核心行为。不能把缺失系统表当成这条测试必须移植的前置条件。
