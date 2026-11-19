# TiDB 现有认证能力与 MySQL 对照实测

日期：2026-11-18。目的：在确认 MFA 用户文档前，验证当前分支已经具备哪些能力，明确需要开发的增量。本次没有修改产品代码、切换分支或创建 worktree。

## 结论

现有 `authentication_ldap_simple` 可以直接复用。真实 OpenLDAP 已验证显式 DN、搜索 DN、PR #43582 的多值 DN、密码轮换、连接池复用、StartTLS、LDAPS 和服务故障行为。缺少的是原生 MFA 框架：多因子 SQL、账号配置及策略、协议串联等，不能把 LDAP 单因子已支持写成 MFA 已支持。

`REQUIRE SSL` 只要求客户端到 TiDB 的连接加密，不要求客户端证书，不是认证因子，也不是 MFA 的固有前提。对 LDAP Simple，生产部署应保护原始密码传输：客户端验证 TiDB 的 TLS 身份，TiDB 另行验证 LDAP 的 TLS 身份。账号级 REQUIRE SSL 是独立的服务端限制。

MySQL 8.4.9 即使启用 native 插件，仍以 4052 拒绝把 native 或 SHA-2 放到第二因子。用户已确认不增加两个 native 的 TiDB 特有组合。

另复现一个已有缺陷：`SET GLOBAL authentication_ldap_simple_bind_root_pwd` 会将服务账户密码写入常规 info 日志。以下“断言符合预期”包含对这个缺陷存在性的断言，不表示产品安全验收通过。

## 环境、次数与证据边界

| 项目 | 实际环境 |
| --- | --- |
| TiDB | `release-7.1.9-0`，`5c980fb645c8f3ae76f006967a4bc8b28ce8ba26`，从当前提交构建，Enterprise，unistore |
| TiDB 版本显示 | 构建输出 Release Version 为 `v8.4.0-this-is-a-placeholder`、Core Version 为 `v8.5.5`；本报告以 Git Commit Hash 和分支固定基线，不将占位版本号当作发布版本 |
| MySQL 客户端及对照服务端 | Homebrew MySQL 8.4.9，macOS arm64 |
| LDAP | Alpine 3.22，OpenLDAP 2.6.8，MDB，隔离 Docker 容器 |
| 网络与凭据 | 随机 loopback 端口；临时 CA 和服务端证书；随机密码，经 SQL stdin 或权限 0600 文件传入 |
| TiDB 第一轮 | 41 项断言符合预期，其中 1 项确认已有密码日志缺陷 |
| TiDB 第二轮 | 前述 41 项全部复现，另补“目录改密后原 SQL 会话继续可用”，共 42 项符合预期 |
| MySQL | 两个独立 datadir 各运行 12 项，结果一致 |

测试进程、LDAP 容器、数据目录、证书、凭据与原始日志均已清理。保留脱敏结果、验证脚本及不含测试凭据的 LDAP 镜像以便复跑。未使用已有数据库服务或真实业务凭据。

本机 Apple OpenLDAP 2.4.28 最初无法完成 StartTLS，报 `unsupported extended operation`。该次属于测试环境建立失败，未计为 TiDB 功能失败；最终两轮使用 OpenLDAP 2.6.8，并先验证目录服务自身能完成 StartTLS Bind。

## TiDB 实测结果

下表按行为归组；完整逐项记录包含 42 项断言。拒绝用例与对应成功用例一起运行，避免将客户端未配置或测试环境错误误判为认证拒绝。

| 行为 | 实际结果 |
| --- | --- |
| native 正确密码，TLS/未加密连接 | 两者均成功；错误密码报 1045 |
| SHA-2 正确/错误密码，TLS | 正确成功；错误报 1045 |
| SHA-2 关闭 TLS，本机 MySQL 客户端 | 客户端报 2061，要求安全连接；不能把它说成 TiDB 的 REQUIRE SSL 拒绝 |
| native 账号 REQUIRE SSL | TLS、不配置客户端证书时成功；关闭 TLS 报 1045 |
| 客户端不信任 TiDB CA | 客户端报 2026，证书验证失败 |
| LDAP 显式 Alice DN，正确→错误→正确密码 | 成功→1045→成功，错误尝试未破坏后续正确认证 |
| LDAP 空密码或不存在的 DN | 1045 |
| Bob DN＋Alice 密码 / Bob 密码 | 前者 1045，后者成功 |
| LDAP `AS ''` | 按 `uid=alice` 在配置的 base DN 下搜索并 Bind 成功 |
| PR #43582 多值 DN | `cn=yangkeao+uid=yangkeao,dc=example,dc=org` 的真实 Bind 及 SQL 登录成功 |
| LDAP 不设 REQUIRE SSL，客户端关闭 TLS | 成功，证明现有功能本身未强制这一账号选项；这不是生产部署建议 |
| LDAP 设置 REQUIRE SSL | TLS、不配置客户端证书时成功；关闭 TLS 报 1045 |
| 禁用 mysql_clear_password 客户端插件 | 客户端报 2059，plugin not enabled |
| LDAP pool size=1，多个用户连续登录 | LDAP 服务日志显示同一连接 ID 被复用；各用户仍分别验证凭据 |
| 在 LDAP 直接修改 Alice 密码，不修改/刷新/重启 TiDB | 旧密码立即被后续登录拒绝，新密码连续登录成功 |
| 目录改密前已建立的 SQL 会话 | 改密后仍可查询，CONNECTION_ID 相同；客户端关闭自动重连，排除重新登录 |
| LDAP 服务账户密码空 / 错误 / 恢复 | 前两者 1045，恢复后成功，即便账号使用显式 DN 仍依赖服务账户预绑定 |
| TiDB 到 LDAP 的 StartTLS | 真实登录成功 |
| 改为实际 LDAPS 端口，保持 ldap tls=ON | 真实登录成功，验证当前分支已有同端口 LDAPS fallback |
| LDAP CA 改为不受信任 CA，强制重建池 | 登录 1045；恢复 CA 并重建池后成功 |
| 停止本测试 LDAP 服务，并强制新建连接 | 登录 1045，不放行 |
| auth_socket，匹配 / 不匹配当前 OS 用户 | 匹配成功；不匹配报 1045 |
| CREATE native＋native 或 native＋LDAP | 均报 1064，多因子语法尚未支持 |
| ALTER ADD 2 FACTOR | 1064 |
| 查询 authentication_policy | 1193 |
| SET GLOBAL 的常规 info 日志 | 两轮均包含本次随机 LDAP 服务账户密码，确认已有缺陷；结果文件只保留存在性，不保留密码或原始日志 |

目录连接复用的判断基于服务端 Bind 日志中的连接 ID，以及 Alice/Bob 正确、错误凭据的实际结果。它不代表做过并发、压力或连接池耗尽测试。

## MySQL 8.4.9 对照

启动时明确设置 `--mysql-native-password=ON`、`--authentication-policy=*,,`。先验证 native 单因子登录正常，排除 8.4 默认禁用 native 的干扰。

| 行为 | 实际结果 |
| --- | --- |
| native 单因子；SHA-2 单因子 TLS | 均成功 |
| CREATE native＋native | 4052：`Invalid plugin "mysql_native_password" specified as 2 factor during "CREATE USER".` |
| CREATE native＋SHA-2；SHA-2＋native | 均 4052 |
| ALTER ADD 2 FACTOR native | 4052；失败后原账号仍能登录 |
| REQUIRE SSL，正确密码、TLS、无客户端证书 | 成功，协商 TLS_AES_128_GCM_SHA256 |
| REQUIRE SSL，关闭 TLS或密码错误 | 均 1045 |
| CREATE native＋LDAP | 1524，LDAP 服务端插件未安装 |
| 上述失败 CREATE 的账号残留 | 0 个 |

本机 MySQL 没有 Enterprise LDAP 服务端插件，因此没有完成 MySQL LDAP MFA 正向登录。LDAP 作为后续因子、相同 LDAP 插件配置两个后续因子的允许行为，仍依据 MySQL 官方文档及源码；不能将 TiDB 的 LDAP 单因子实测替代这项验证。

## 已有功能的复现方式

按 [PR #43582](https://github.com/pingcap/tidb/pull/43582) 的 Simple 账号写法配置了真实多值 DN，使用随机测试密码，未照搬其固定端口和 host 网络模式。当前分支的 Simple 设置与 PR 中 SASL 设置分别核对；PR 初期关于不支持 LDAPS 的说明不适用于本次基线。

以下是当前版本已运行成功的单因子示例，变量中的地址、端口、CA 和秘密由隔离测试环境提供：

```sql
SET GLOBAL authentication_ldap_simple_tls = ON;
SET GLOBAL authentication_ldap_simple_ca_path = '<LDAP CA 路径>';
SET GLOBAL authentication_ldap_simple_server_host = '127.0.0.1';
SET GLOBAL authentication_ldap_simple_server_port = <随机 LDAP 端口>;
SET GLOBAL authentication_ldap_simple_bind_root_dn = 'cn=tidb-bind,dc=example,dc=org';
SET GLOBAL authentication_ldap_simple_bind_root_pwd = '<服务账户随机密码>';

CREATE USER 'yangkeao'@'127.0.0.1'
  IDENTIFIED WITH authentication_ldap_simple
  AS 'cn=yangkeao+uid=yangkeao,dc=example,dc=org';
```

```sh
mysql --protocol=TCP --host=127.0.0.1 --port=<随机 TiDB 端口> \
  --user=yangkeao --password --enable-cleartext-plugin \
  --ssl-mode=VERIFY_IDENTITY --ssl-ca=<TiDB CA 路径>
```

目录服务必须先有对应条目及密码。这个账号只验证 LDAP 密码，没有第一份本地数据库密码；把它扩展成有序的本地密码＋LDAP 认证，才是本次 MFA 的开发增量。

本机可复跑材料：

- [TiDB＋LDAP 验证脚本](/tmp/tidb-ldap-current.2Sak5F/check.py)、[镜像 Dockerfile](/tmp/tidb-ldap-current.2Sak5F/Dockerfile)、[第一轮结果](/tmp/tidb-ldap-current.2Sak5F/results.json)、[第二轮结果](/tmp/tidb-ldap-current.2Sak5F/results-repeat.json)。执行 `python3 /tmp/tidb-ldap-current.2Sak5F/check.py --output /tmp/tidb-ldap-current.2Sak5F/results-rerun.json`，会重新建立并清理独立环境。
- [MySQL 验证脚本](/tmp/tidb-mfa-mysql-d7973mh5/run_mysql_probe.py)、[两轮结果](/tmp/tidb-mfa-mysql-d7973mh5/results.json)。

这些可执行材料位于本机 `/tmp`，不是发布测试资产，可能随系统清理而消失。持久化结论、环境和关键行为记录在本文；开发阶段需在正式测试体系中实现 MFA 回归测试。

## 仍未覆盖与范围决定

本轮未实测 LDAP SASL、SM3、JWT 完整外部认证链路、多节点传播、代理、会话迁移、其他驱动、并发负载、证书过期及主机名不匹配。TLS 本轮覆盖了正确证书与不受信任 CA，不将其扩大为所有 TLS 边界均已测试。已有证书方案的独立测试见[前期报告](2026-11-18-multi-factor-authentication-validation.md)，不混入本轮计数。

本次新增能力聚焦 MFA 框架，复用现有 LDAP、密码和 TLS 基础。两个 native 因子已按用户决定排除。Q14 已确认纳入 LDAP 服务账户密码日志脱敏的最小修复，LDAP 认证与配置语义保持。Q15 后已形成[原生 MFA 技术 spec](2026-11-18-native-mfa-spec.md)及补充 MySQL 管理对照，后者证据见其[审阅记录](2026-11-18-native-mfa-spec-review.md)。当前没有执行产品修复；本文计数仍仅对应前述基线实测。

## Q16 系统表专项补测

技术设计复核阶段另完成 MySQL 8.4.9 的 28 项元数据观测和 1 项逆序后立即修改的补测，覆盖列定义、MFA JSON 缺省/未知字段、删除与属性保留、逆序 MODIFY 重载及坏配置匹配回退。它们使用 mysql_no_login 验证管理和存储，不属于上表 TiDB LDAP 登录断言，也不代表新 MFA 实现已完成。脚本、结果和准确结论见[原生 MFA 设计审阅记录](2026-11-18-native-mfa-spec-review.md)。
