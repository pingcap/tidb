# DDL Ingest: 谁把 addrs 传进 NewEtcdSafePointKV，以及 addrs 从哪里来

本文档说明 DDL 分布式回填（add index）创建 Lightning local backend 时，传给 `tikv.NewEtcdSafePointKV(addrs, ...)` 的 `addrs` 的完整调用链与数据来源。用于排查 “dial tcp: missing address” 等与空 endpoints 相关的问题。

## 1. 谁把 addrs 传进 NewEtcdSafePointKV？

**TiDB 侧**：`pkg/lightning/backend/local/local.go` 的 `NewBackend()`。

- 从 `pdSvcDiscovery.GetServiceURLs()` 得到 `pdAddrs`，然后调用：
  - `tikvclient.NewEtcdSafePointKV(pdAddrs, tls.TLSConfig())`
- 即：**传进去的 `addrs` 就是这里的 `pdAddrs`。**

```go
// local.go
pdAddrs = pdSvcDiscovery.GetServiceURLs()
// ...
spkv, err = tikvclient.NewEtcdSafePointKV(pdAddrs, tls.TLSConfig())
```

## 2. pdSvcDiscovery 是谁传进 NewBackend 的？

**调用方**：`pkg/ddl/ingest/backend_mgr.go` 的 `CreateLocalBackend()`。

- 从当前 `store` 取出 PD client，再取它的 ServiceDiscovery，传给 `local.NewBackend`：

```go
pdCli := store.(tikv.Storage).GetRegionCache().PDClient()
be, err := local.NewBackend(ctx, tls, *cfg, pdCli.GetServiceDiscovery())
```

因此：**`pdSvcDiscovery` = `store` 对应的 TiKV store 的 RegionCache 里的 PDClient 的 ServiceDiscovery。**

## 3. 这个 store 从哪里来？

**来源分两种**（都在 `pkg/ddl/backfilling_dist_executor.go` 的 `newBackfillStepExecutor()` 里）：

1. **默认**：`store := ddlObj.store`
   - 即 DDL 启动时绑定的主 store（当前实例的默认 keyspace 的 kv.Storage）。
2. **跨 keyspace**：当 `ddlObj.store.GetKeyspace() != task.Keyspace` 时：
   - 通过 `se.GetSQLServer().GetKSStore(taskKS)` 得到该 task 所在 keyspace 的 store。
   - 内部是 `domain.GetKSStore(taskKS)` → `crossKSSessMgr.GetOrCreate(taskKS, ...)` → `SessionManager.Store()`。

所以：**传进 `CreateLocalBackend` 的 `store` 要么是 DDL 主 store，要么是该 task 的 keyspace 对应的 cross-KS store。**

## 4. store 的 PD 地址最初从哪里来？

- **DDL 主 store**：在 TiDB 启动时通过 `NewDDL(..., Option{Store: opt.Store})` 传入；`opt.Store` 一般来自 domain 的默认 store，其 path 为 `config.GetGlobalConfig().Path`（例如 `tikv://pd1:2379,pd2:2379` 或带 `?keyspaceName=xxx`）。
- **Cross-KS store**：`GetKSStore(taskKS)` 内部对非 system keyspace 会调 `store.InitStorage(keyspaceName)`，而 `InitStorage` 用**同一份**全局配置拼 path：

```go
// pkg/store/store.go
func InitStorage(keyspaceName string) (kv.Storage, error) {
    cfg := config.GetGlobalConfig()
    if keyspaceName == "" {
        fullPath = fmt.Sprintf("%s://%s", cfg.Store, cfg.Path)
    } else {
        fullPath = fmt.Sprintf("%s://%s?keyspaceName=%s", cfg.Store, cfg.Path, keyspaceName)
    }
    return New(fullPath)
}
```

- **New(fullPath)** 会调到 TiKV driver 的 `Open(path)`，里面用 `config.ParsePath(path)` 解析出 `etcdAddrs`，再用这些地址创建 PD client 和 EtcdSafePointKV（driver 自己也会建一份 EtcdSafePointKV，和 DDL ingest 里再建的那份是两回事）。

因此：**所有这里用到的 store，其 PD/etcd 的“初始地址”都来自 `config.GetGlobalConfig().Path`（即 TiDB 配置里的 `path`）。**

## 5. GetServiceURLs() 返回的到底是哪份数据？

- PD client（tikv/pd/client）里的 ServiceDiscovery 会维护一份 URL 列表 `urls`：
  - 创建时：用 `ParsePath(path)` 得到的地址列表初始化 `urls`。
  - 运行后：通过 `updateMember()` 从 PD 的 GetMembers 拉取 member 的 client URLs，并更新 `urls`。
- `GetServiceURLs()` 直接返回当前内存里的这份 `urls`（见 pd client 的 `service_discovery.go` 的 `GetServiceURLs()`）。

所以：**正常情况**下，`addrs` 要么是启动时的 Path 解析结果，要么是之后从 PD 拉到的 member 列表；**若出现空或含空串**，可能是：

1. **配置**：`config.Path` 为空或解析后得到空列表（少见）。
2. **时序/竞态**：在 ServiceDiscovery 尚未完成首次 `updateMember`、或某次异常后 `urls` 被清空/未正确更新时，就调用了 `GetServiceURLs()`。
3. **跨 keyspace**：若 cross-KS 的 store 刚创建不久，其 PD client 的 discovery 可能尚未就绪，此时在 DDL ingest 里立刻用该 store 去 `CreateLocalBackend`，也可能拿到不完整或空的 URLs。

## 6. 小结（谁传进来 + 从哪获取）

| 环节 | 位置 | 说明 |
|------|------|------|
| 传进 NewEtcdSafePointKV 的 addrs | `pkg/lightning/backend/local/local.go` | `pdAddrs`，来自 `pdSvcDiscovery.GetServiceURLs()` |
| pdSvcDiscovery 从哪来 | `pkg/ddl/ingest/backend_mgr.go` | `store.(tikv.Storage).GetRegionCache().PDClient().GetServiceDiscovery()` |
| store 从哪来 | `pkg/ddl/backfilling_dist_executor.go` | `ddlObj.store` 或 `GetKSStore(taskKS)` |
| DDL 的 store 从哪来 | `pkg/ddl/ddl.go` | `NewDDL` 时 `Option.Store`，一般为 domain 默认 store |
| 默认 / cross-KS store 的 Path | `pkg/store/store.go` / `pkg/domain/crossks/cross_ks.go` | `config.GetGlobalConfig().Path`，拼成 `tikv://{Path}?keyspaceName=...` |
| PD client 的初始 URLs | `pkg/store/driver/tikv_driver.go` | `config.ParsePath(path)` → `etcdAddrs`，再创建 PD client |
| GetServiceURLs() 的运行时值 | PD client（tikv/pd/client） | 内存中的 `urls`（初始为 ParsePath 结果，之后由 updateMember 从 PD GetMembers 更新） |

排查 “missing address” 时，可重点确认：

1. 该 TiDB 实例的 `path` 配置是否非空且能被正确解析为至少一个 PD 地址。
2. 是否在 PD 切主或 store 刚创建后不久就执行了 DDL ingest（可配合之前加的 debug 日志看 `pdAddrs` 的 `count` 和 `empty_indices`）。

---

## 7. path 和 discovery 的差异：etcd 有没有“直接用 path”？

- **path**：TiDB 配置里的 `path`（如你日志里的 `dev-artem-test-pd-peer....:2379,...`）是「入口」地址，一般是 PD 的 peer/service DNS，会解析到多个 PD 节点。
- **discovery 更新后**：PD client 连上后通过 GetMembers 拿到**真实 member 的 client_urls**（如 `https://dev-artem-test-pd-0...:2379`, `https://dev-artem-test-pd-1...:2379`），并更新 ServiceDiscovery 里的 `urls`。所以你看到 `[pd] update member urls` 里 old-urls 是 path 风格，new-urls 是 pd-0/pd-1 风格，这是预期行为（DNS 跳转 / 发现真实 member）。

**Etcd 是否直接用 path？**

- **不会**。在 DDL ingest 这条路径里，`local.NewBackend()` 只有在 `pdSvcDiscovery == nil` 时才会用 `config.PDAddr`（path）：
  - `if pdSvcDiscovery != nil { pdAddrs = pdSvcDiscovery.GetServiceURLs() } else { pdAddrs = strings.Split(config.PDAddr, ",") }`
- 我们这里传了 `pdSvcDiscovery`，所以 **etcd 拿到的永远是 `GetServiceURLs()` 的返回值**，即当前 discovery 里维护的那份 `urls`（可能是 path 解析出的初始列表，也可能是 updateMember 之后的 member 列表），**不会绕过 discovery 去读 path**。

因此：path 和 discovery 的 URL 可以不一样（peer DNS vs 具体 member），但传给 `NewEtcdSafePointKV` 的 `addrs` 一定是「当时」`GetServiceURLs()` 的结果；不会出现“etcd 单独用 path、discovery 单独用 member 列表”的分叉。

**为何日志里既有 “switch leader” 又有 “missing address”？**

- 日志里的 `[pd] switch leader` 来自 **NewBackend 里新创建的那个 PD client**（`pd.NewClientWithContext(..., pdAddrs, ...)`），说明当时 `pdAddrs` 非空且新 client 已经连上并完成了一次 leader 切换。
- 同一份 `pdAddrs` 随后传给了 `NewEtcdSafePointKV`；etcd client 用这份列表去 `Sync()`，若之后 **context 被 cancel**（例如 step 超时），etcd 内部重试或 balancer 在清理时有可能报出 `dial tcp: missing address`（例如 endpoints 被清空或选到空串）。
- 所以 “missing address” 有可能不是「一开始就传了空 addrs」，而是 **context 取消后的重试/清理路径** 里 etcd 看到空 endpoints。要区分这两种情况，需要看复现时 debug 日志里 `pd_addrs` 的 `count` 和 `empty_indices`：若当时非空，则更可能是取消/超时导致的后续错误。
