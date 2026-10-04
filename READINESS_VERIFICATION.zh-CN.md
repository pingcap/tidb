# Cluster-session readiness 验证入口

TCP 端口开放不等于应用已完成初始化。保留的 `rust/scripts/cluster-session-readiness.sh` 等待真实 `cluster_session_node_ready` 事件，检查进程退出，并保留180秒超时。access-path、analyze、convergence、repeatable-read 四个 runner 使用同一个等待函数；不要提前伪造 ready 或删除 SQL 对照。

从仓库根目录执行已有回归：

    for runner in access-path analyze convergence repeatable-read; do
      bash rust/scripts/test-access-path-readiness.sh "run-realtikv-${runner}.sh" || exit
    done

该检查覆盖延迟 ready、提前退出和永久没有 ready。它是 Rust 集成 runner 的检查，不是 Go master 原生套件，也不能证明四套 RealTiKV SQL 集成已通过。

2026-09-12 的历史 access-path 通过记录使用 Go `fdfadb96b2cfdc5a7c26b8eb7b2a3da5f3038d85` 和 PD/TiKV `v9.0.0-beta.2.pre-nightly`。完整命令、结果和当时未解决的问题可读取：

    git show 880246825c:READINESS_CURRENT_STATUS.zh-CN.md
    git show 880246825c:CLUSTER_SESSION_READINESS_CURRENT.zh-CN.md
    git show 880246825c:READINESS_VERIFICATION.zh-CN.md

这些记录不是本轮的实时集群验证；旧临时日志不保证存在。当前源码与验证边界见 [审计索引](rust/docs/parity/current-audit/README.md)。本轮清理没有重新运行多节点集成，也没有关闭相关未解决项。
