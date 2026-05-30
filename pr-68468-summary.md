# PR #68468 Summary

PR: `*: Meta service group basic`

1. 新增了一个统一的 `pkg/metaservice` 抽象层，用来管理 PD / etcd 相关的元信息访问。它把几件原来分散处理的事情收拢到一起：获取 PD 地址、解析 keyspace 对应的 meta service group、以及决定当前 keyspace 应该连哪组 meta service 节点。

2. `pkg/store/driver/tikv_driver.go` 在创建 TiKV store 时，开始根据 keyspace metadata 计算 `MetaServiceInfo`。工作方式是：先从 PD client 拿到 PD 地址，再读取 keyspace meta 里的 `meta_service_group_id` 和 `meta_service_group_addrs`；如果 keyspace 没配独立 group，就回退到全局 meta service。之后 GC safepoint KV、AutoID service 等需要访问 etcd / meta service 的逻辑，就统一使用这组解析后的地址。

3. 上层调用方被切到这个新抽象上了，而不是各自直接猜 PD / etcd 地址。包括 `server` 的 AutoID service 注册、`lightning` 获取 etcd client、`infoschema` / `executor` / `store helper` 查询 PD 信息、`infosync` 初始化 meta service client 等。这样 keyspace 场景下这些功能会自动走到正确的 meta service group，测试也补到了新的解析逻辑和关键错误路径。
