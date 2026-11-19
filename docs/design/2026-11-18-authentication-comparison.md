# TiDB 与 MySQL 认证方式源码核对

核对日期：2026-11-18。结论以本地 checkout 为准。首轮核对源码；后续使用同提交的隔离 TiDB 实例验证 authentication_policy 与多因素 SQL，结果见文末。未修改已有数据库的账号或配置。

当前 TiDB 支持 `mysql_native_password` 和 `caching_sha2_password`，默认仍为前者；但 SHA-2 未实现快速认证缓存和 RSA 交换。本地 MySQL 为 8.4.9 LTS，默认 SHA-2，完整实现缓存与 RSA，native 默认关闭。其他认证方式要区分内置账号插件、客户端插件、扩展和企业版插件。

## TiDB 当前分支的实现与用法

TiDB 基线为 `release-7.1.9-0`，提交 `5c980fb645c8f3ae76f006967a4bc8b28ce8ba26`。下面是对当前代码的判断，不把分支名等同于上游 TiDB 7.1 的功能集合。没有读取或修改运行中集群的账号与配置。

### 两种密码认证都支持，但 SHA-2 没有快速认证缓存

`CREATE USER` 与 `ALTER USER` 明确接受 `mysql_native_password` 和 `caching_sha2_password`，握手、密码生成和登录校验都有对应实现。证据：[CREATE USER 插件选择](/Users/cs/code/tidb/pkg/executor/simple.go:1224)、[ALTER USER](/Users/cs/code/tidb/pkg/executor/simple.go:1900)、[握手分派](/Users/cs/code/tidb/pkg/server/conn.go:704)、[密码校验](/Users/cs/code/tidb/pkg/privilege/privileges/privileges.go:794)。

| 项目 | mysql_native_password | caching_sha2_password |
| --- | --- | --- |
| 持久化凭据 | `* + HEX(SHA1(SHA1(password)))`，41 字符；存储哈希不加盐 | MySQL `$A$005$...` 格式，20 字节随机盐、默认 5000 轮 SHA-256 派生 |
| 握手凭据 | 客户端按每连接随机挑战计算 SHA-1 scramble；服务端用存储哈希验证，不需要接收密码原文 | 对非空密码，TiDB 始终返回 `fastAuthFail`，要求完整认证；服务端接收密码后重算哈希 |
| 快速认证缓存 | 不涉及 SHA-2 的 fast-auth 缓存 | 当前未实现；用户权限缓存不等于该认证缓存 |
| RSA 密码交换 | 不使用这条交换路径 | 当前未实现，因此不能套用 MySQL 的公钥获取配置绕过 TLS |
| TCP 连接使用条件 | 老客户端兼容性更好，协议本身无需发送密码原文；仍建议 TLS | 使用支持此插件的客户端并配置 TLS；完整认证中的密码原文由 TLS 保护 |
| 安全与成本 | 无盐、快速 SHA-1 派生对离线猜测的抵抗较弱 | 加盐和迭代提高离线猜测成本；当前 TiDB 每次建立密码认证连接都付出完整校验成本，未做性能量化 |

实现证据：[native 哈希与 challenge-response](/Users/cs/code/tidb/pkg/parser/auth/mysql_native_password.go:27)、[SHA-2 格式与校验](/Users/cs/code/tidb/pkg/parser/auth/caching_sha2.go:192)、[随机盐与固定 5000 轮](/Users/cs/code/tidb/pkg/parser/auth/caching_sha2.go:222)、[总是完整认证且不支持 RSA](/Users/cs/code/tidb/pkg/server/conn.go:779)。

传输条件需要准确区分：`authSha` 本身没有逐次检查 TLS，也没有 RSA 分支。标准客户端在不安全 TCP 上通常转入 RSA，而该路径在 TiDB 不可用；因此实际部署应使用 TLS，或客户端明确视作安全的本地 Unix socket。要让服务端强制 TLS，可对账号指定 `REQUIRE SSL`，或配置 `require_secure_transport`。不要把插件名称本身当作服务端已强制 TLS 的证明。证据：[全局传输限制](/Users/cs/code/tidb/pkg/server/conn.go:678)、[账号 SSL 条件](/Users/cs/code/tidb/pkg/privilege/privileges/privileges.go:733)、[SSL 检查实现](/Users/cs/code/tidb/pkg/privilege/privileges/privileges.go:858)。

### 使用方式与默认值

```sql
-- 以下仅为示例，没有执行。
CREATE USER 'app_sha2'@'%'
  IDENTIFIED WITH caching_sha2_password BY '<设置强密码>'
  REQUIRE SSL;

CREATE USER 'app_native'@'%'
  IDENTIFIED WITH mysql_native_password BY '<设置强密码>'
  REQUIRE SSL;

ALTER USER 'app_native'@'%'
  IDENTIFIED WITH caching_sha2_password BY '<重新提供密码>'
  REQUIRE SSL;

SELECT User, Host, plugin FROM mysql.user;
SHOW VARIABLES LIKE 'default_authentication_plugin';
```

`BY` 接收密码并生成对应插件的哈希；`AS` 接收已经按该插件生成的认证串。native 与 SHA-2 哈希不能互相直接转换，切换插件需要重新提供密码或目标插件对应的哈希。证据：[EncodedPassword](/Users/cs/code/tidb/pkg/parser/ast/misc.go:1531)。

当前默认值是 `mysql_native_password`。这个分支还有一个使用差异：`default_authentication_plugin` 被读取来生成初始握手，而未指定插件的 `CREATE USER` 路径直接选择 native；不要假设改全局变量会把新建账号或已有账号自动改为 SHA-2。建议在建账号语句中显式写 `IDENTIFIED WITH`。证据：[变量默认值](/Users/cs/code/tidb/pkg/sessionctx/variable/sysvar.go:1134)、[握手读取变量](/Users/cs/code/tidb/pkg/server/conn.go:526)、[建用户默认选择](/Users/cs/code/tidb/pkg/executor/simple.go:1224)、[无插件的语法节点](/Users/cs/code/tidb/pkg/parser/parser.y:14925)。

### 其他认证方式

| 方式 | 当前支持与使用边界 | 证据 |
| --- | --- | --- |
| `tidb_sm3_password` | 使用 SM3 的密码认证；同样采用完整认证交换，需要客户端支持此插件 | [SM3 握手](/Users/cs/code/tidb/pkg/server/conn.go:815)、[哈希分派](/Users/cs/code/tidb/pkg/parser/auth/caching_sha2.go:239) |
| `auth_socket` | Unix socket 对端操作系统身份认证；可按 TiDB 用户名或 `AS 'os_user'` 匹配，不用于 TCP | [读取对端 UID](/Users/cs/code/tidb/pkg/server/server.go:583)、[获取系统用户](/Users/cs/code/tidb/pkg/server/conn.go:965)、[身份匹配](/Users/cs/code/tidb/pkg/privilege/privileges/privileges.go:782) |
| `authentication_ldap_simple` | LDAP Simple Bind；使用 `mysql_clear_password` 客户端交换，需配置 LDAP，并保护客户端到 TiDB 和 TiDB 到 LDAP 两段传输 | [客户端插件映射](/Users/cs/code/tidb/pkg/server/conn.go:272)、[LDAP Bind](/Users/cs/code/tidb/pkg/privilege/privileges/ldap/simple.go:25) |
| `authentication_ldap_sasl` | 通过 LDAP SASL 验证；配置允许 SCRAM-SHA-1、SCRAM-SHA-256、GSSAPI，需匹配的客户端插件和 LDAP 服务配置；不是独立的 MySQL Kerberos 服务端插件 | [SASL 多轮交换](/Users/cs/code/tidb/pkg/privilege/privileges/ldap/sasl.go:31)、[机制选项](/Users/cs/code/tidb/pkg/sessionctx/variable/sysvar.go:3396) |
| `tidb_auth_token` | JWT Token 认证；配置 JWKS，校验签名以及 sub、email、iat、exp、iss 条件，适合令牌接入；不等于通用 MySQL OpenID Connect 插件 | [Token 校验分派](/Users/cs/code/tidb/pkg/privilege/privileges/privileges.go:753)、[Claims 条件](/Users/cs/code/tidb/pkg/privilege/privileges/privileges.go:551)、[JWKS 加载](/Users/cs/code/tidb/pkg/server/server.go:386) |
| `tidb_session_token` | 会话迁移专用的短期签名 Token，默认有效期一分钟；不属于普通 `CREATE USER` 可选插件 | [会话迁移用途](/Users/cs/code/tidb/pkg/sessionctx/sessionstates/session_token.go:37)、[登录校验](/Users/cs/code/tidb/pkg/privilege/privileges/privileges.go:747) |
| 自定义 extension 认证 | Go 扩展接口支持自定义密码生成、认证串校验、认证过程及授权回调；需要实际注册实现 | [AuthPlugin 接口](/Users/cs/code/tidb/pkg/extension/auth.go:27)、[CREATE USER 扩展检查](/Users/cs/code/tidb/pkg/executor/simple.go:1243) |
| TLS 客户端证书约束 | 可用 `REQUIRE X509 / ISSUER / SUBJECT` 与账号认证一起检查；属于附加条件，不是独立密码插件 | [认证前证书约束](/Users/cs/code/tidb/pkg/privilege/privileges/privileges.go:733)、[证书检查](/Users/cs/code/tidb/pkg/privilege/privileges/privileges.go:876) |

`mysql_clear_password` 在这里是客户端传输插件，不能直接作为 TiDB 内置账号认证方式创建。已有测试预期明确返回 `Plugin 'mysql_clear_password' is not loaded`。当前内置账号插件列表也不含 MySQL 的 `sha256_password`、PAM、独立 Kerberos/Windows 或 WebAuthn 服务端插件。证据：[允许列表](/Users/cs/code/tidb/pkg/executor/simple.go:1240)、[clear-password 反例](/Users/cs/code/tidb/tests/integrationtest/r/executor/simple.result:200)。

当前 TiDB 没有 MySQL 的有序多插件 MFA 链：`UserSpec` 为单认证项，握手未公布 MFA 能力。TLS 证书与密码联合检查属于已有的另一条路径。证据：[单认证项](/Users/cs/code/tidb/pkg/parser/ast/misc.go:1495)、[服务端能力集合](/Users/cs/code/tidb/pkg/server/server.go:110)。

### 本次核查边界

首轮沿建用户、握手、插件分派、哈希与外部认证调用链阅读代码，并阅读已有 native/SHA-2 哈希和插件切换测试。后续新增的隔离实例验证见文末；没有运行 LDAP/Kerberos/JWT 环境或驱动兼容性测试，没有修改业务源码或已有集群设置。上述源码支持不表示特定部署已经配置好相关服务，也不构成登录性能基准。

## MySQL 核对基线

`/Users/cs/code/mysql-server` 为 detached HEAD，commit `f9c88132a87a7d9e740e50bce2621999695bd3fe`；[MYSQL_VERSION](/Users/cs/code/mysql-server/MYSQL_VERSION:1) 声明 **8.4.9 LTS**。核对时工作区干净，未发现适用的额外 `AGENTS.md`。下文“本地 MySQL”均指这个版本，不代表所有 MySQL 版本。

## 本地 MySQL 的认证插件范围

| 方式 | 8.4.9 状态与适用范围 | 依据 |
|---|---|---|
| `caching_sha2_password` | 内置；默认密码认证；支持快速缓存认证和完整认证 | [内置注册](/Users/cs/code/mysql-server/sql/sql_builtin.cc.in:43)、[默认策略](/Users/cs/code/mysql-server/sql/sys_vars.cc:7381)、[认证入口](/Users/cs/code/mysql-server/sql/auth/sha2_password.cc:924) |
| `mysql_native_password` | 内置，但默认关闭；已废弃；使用前需显式启动 `--mysql-native-password=ON` 或配置 `mysql_native_password=ON` | [静态构建](/Users/cs/code/mysql-server/sql/CMakeLists.txt:923)、[默认关闭标志](/Users/cs/code/mysql-server/sql/auth/mysql_native_password.cc:327)、[废弃警告](/Users/cs/code/mysql-server/sql/auth/mysql_native_password.cc:88)、[官方说明](https://dev.mysql.com/doc/refman/8.4/en/native-pluggable-authentication.html) |
| `sha256_password` | 内置；不带快速认证缓存的 SHA-256 密码认证；已废弃，建议迁往 `caching_sha2_password` | [注册与回调](/Users/cs/code/mysql-server/sql/auth/sql_authentication.cc:5908)、[废弃警告](/Users/cs/code/mysql-server/sql/auth/sql_authentication.cc:4691)、[官方说明](https://dev.mysql.com/doc/refman/8.4/en/sha256-pluggable-authentication.html) |
| `auth_socket` | 社区源码提供可加载模块；要求平台支持 `SO_PEERCRED`；通过 Unix socket 对端 OS 用户认证，不使用密码，可匹配数据库用户名或映射到 `authentication_string` 指定的 OS 用户名 | [条件构建](/Users/cs/code/mysql-server/plugin/auth/CMakeLists.txt:59)、[认证实现](/Users/cs/code/mysql-server/plugin/auth/auth_socket.cc:44) |
| `mysql_no_login` | 社区源码提供可加载模块；所有外部登录都失败，用于拥有 DEFINER 对象等用途的专用账号 | [构建](/Users/cs/code/mysql-server/plugin/auth/CMakeLists.txt:54)、[实现](/Users/cs/code/mysql-server/plugin/auth/mysql_no_login.cc:24) |
| `authentication_ldap_simple` / `authentication_ldap_sasl` | MySQL Enterprise 服务端插件；本地社区源码只有 SASL 客户端及通用 cleartext 客户端，不能据此声称社区服务端已支持 LDAP | [LDAP 官方版本说明](https://dev.mysql.com/doc/refman/8.4/en/ldap-pluggable-authentication.html)、[CLIENT_ONLY 构建](/Users/cs/code/mysql-server/libmysql/authentication_ldap/CMakeLists.txt:77) |
| `authentication_pam` | MySQL Enterprise 服务端插件；通过 PAM 接入系统认证；客户端使用 `mysql_clear_password` | [PAM 官方说明](https://dev.mysql.com/doc/refman/8.4/en/pam-pluggable-authentication.html)、[客户端协议说明](/Users/cs/code/mysql-server/sql-common/client.cc:9522) |
| `authentication_windows` | MySQL Enterprise for Windows 服务端插件；本地提供 Windows 客户端实现 | [Windows 官方说明](https://dev.mysql.com/doc/refman/8.4/en/windows-pluggable-authentication.html)、[客户端构建](/Users/cs/code/mysql-server/libmysql/CMakeLists.txt:288) |
| `authentication_kerberos` | MySQL Enterprise 服务端插件；官方 8.4 文档要求服务端 Linux，客户端还支持 Windows；本地只有客户端实现 | [Kerberos 官方说明](https://dev.mysql.com/doc/refman/8.4/en/kerberos-pluggable-authentication.html)、[CLIENT_ONLY 构建](/Users/cs/code/mysql-server/libmysql/authentication_kerberos/CMakeLists.txt:76) |
| `authentication_webauthn` | MySQL Enterprise 服务端插件；支持 FIDO/FIDO2 硬件设备；本地只含 `authentication_webauthn_client` | [WebAuthn 官方说明](https://dev.mysql.com/doc/refman/8.4/en/webauthn-pluggable-authentication.html)、[CLIENT_ONLY 构建](/Users/cs/code/mysql-server/libmysql/fido_client/authentication_webauthn/CMakeLists.txt:51) |

`mysql_clear_password` 是**客户端协议插件**，不能与服务端认证方式混为一谈。它把 NUL 结尾的明文密码交给服务端插件，常用于 LDAP simple/PAM。客户端默认关闭，需启用；传输时应使用 TLS。[插件启用检查](/Users/cs/code/mysql-server/sql-common/client.cc:5552)、[发送逻辑](/Users/cs/code/mysql-server/sql-common/client.cc:9570)。

`auth`、`auth_test_plugin`、`qa_auth_interface`、`qa_auth_server`、`qa_auth_client` 在构建脚本中标记为 `TEST_ONLY`，不应列入生产认证方式。[测试插件构建](/Users/cs/code/mysql-server/plugin/auth/CMakeLists.txt:26)。

版本边界：

- `authentication_fido` 和 `authentication_fido_client` 在 **8.4.0 已移除**，应改用 WebAuthn。不能因为还存在 `libmysql/fido_client/` 目录或旧注释就判定 FIDO 插件仍可用。[8.4.0 release notes](https://dev.mysql.com/doc/relnotes/mysql/8.4/en/news-8-4-0.html)、[当前目录构建](/Users/cs/code/mysql-server/libmysql/fido_client/CMakeLists.txt:30)。
- `authentication_openid_connect` 是 **9.1.0 新增的 Enterprise 插件**，不属于本地 8.4.9 能力。[9.1.0 release notes](https://dev.mysql.com/doc/relnotes/mysql/9.1/en/news-9-1-0.html)；官网该链接可能重定向到当前 9.x release notes 汇编中的 9.1.0 章节。
- `mysql_native_password` 在 8.4 默认关闭，在 9.0 已移除，因此“本地 8.4 还能显式开启”不能外推到 9.x。[Native 官方说明](https://dev.mysql.com/doc/refman/8.4/en/native-pluggable-authentication.html)。
- 本地虽有 OCI 客户端，官方 8.4 文档明确 `authentication_oci` 服务端为 MySQL HeatWave Service 专用，不算本地社区或普通 Enterprise Server 插件。[认证插件总表](https://dev.mysql.com/doc/refman/8.4/en/authentication-plugins.html)。

## native 与 caching SHA-2：MySQL 的实际实现

| 维度 | `mysql_native_password` | `caching_sha2_password` |
|---|---|---|
| 持久化凭据 | `*` 加 `SHA1(SHA1(password))` 的十六进制字符串；无每账号随机盐 | 带随机盐、轮次和 SHA-256 派生摘要；默认格式 `$A$005$<20 字节 salt><43 字符 digest>`，共 70 字节；默认 5000 轮 |
| 客户端初始响应 | 20 字节 SHA-1 challenge-response | 32 字节 SHA-256 challenge-response |
| 服务端认证过程 | 直接用持久化的二次 SHA-1 摘要验证响应，不需要获得明文密码 | 先查内存缓存；成功则快速通过，缺失或校验失败则要求完整认证 |
| 完整认证密码传输 | 无对应阶段 | TLS、Unix socket、shared memory 连接中传输密码；非安全 TCP 使用 RSA 加密密码 |
| 缓存内容 | 无此认证缓存 | 账号对应 `SHA256(SHA256(password))`，不是保存明文密码 |
| 用途选择 | 旧客户端兼容，8.4 已默认关闭 | 8.4 默认选择；落盘凭据的随机盐和多轮计算提高离线猜密码成本 |

native 的官方源码说明直接给出算法：`SHA1(password) XOR SHA1(nonce || SHA1(SHA1(password)))`，服务端持久化二次 SHA-1；认证回调调用 `check_scramble()`。[算法说明](/Users/cs/code/mysql-server/sql/auth/mysql_native_password.cc:50)、[摘要格式校验](/Users/cs/code/mysql-server/sql/auth/mysql_native_password.cc:137)、[服务端校验](/Users/cs/code/mysql-server/sql/auth/mysql_native_password.cc:284)。密码 challenge-response 本身不要求 TLS，也不代表随后的 SQL 会话已加密。

caching SHA-2 的持久化凭据与内存快速认证摘要是两套数据：

1. 建立/更新密码时生成随机盐，执行多轮 SHA-256，再序列化写入认证字符串。[凭据生成](/Users/cs/code/mysql-server/sql/auth/sha2_password.cc:1150)、[序列化](/Users/cs/code/mysql-server/sql/auth/sha2_password.cc:541)、[5000 轮与 20 字节盐常量](/Users/cs/code/mysql-server/include/crypt_genhash_impl.h:30)、[43 字符摘要常量](/Users/cs/code/mysql-server/sql/auth/i_sha2_password.h:68)。`005` 表示 `5 × 1000` 轮，轮次可由服务端参数调整；不能一律认为所有已存密码都是 5000 轮。
2. 客户端发送基于随机 challenge 的 32 字节响应，服务端先用内存快速摘要校验。缓存命中并校验成功发送 fast-auth success；缓存缺失或校验失败发送 full-auth request。[握手处理](/Users/cs/code/mysql-server/sql/auth/sha2_password.cc:987)。
3. 完整认证必须让服务端取得原始密码用于验证带盐多轮摘要。安全传输直接交付密码；其他传输先校验 RSA 密钥，支持客户端请求公钥，RSA 解密后再去除 challenge XOR。[安全传输分类](/Users/cs/code/mysql-server/sql/auth/sql_authentication.cc:4418)、[完整认证 RSA 分支](/Users/cs/code/mysql-server/sql/auth/sha2_password.cc:1025)、[取公钥、解密、验证](/Users/cs/code/mysql-server/sql/auth/sha2_password.cc:1054)。RSA 只保护密码交换，不加密之后的 SQL 流量。
4. 完整认证成功后产生快速摘要并写入账号缓存。默认快速摘要为二次 SHA-256。[缓存写入](/Users/cs/code/mysql-server/sql/auth/sha2_password.cc:289)、[快速摘要生成](/Users/cs/code/mysql-server/sql/auth/sha2_password.cc:601)、[快速摘要默认轮次](/Users/cs/code/mysql-server/sql/auth/i_sha2_password.h:48)。
5. 成功的 `FLUSH PRIVILEGES` 清整个缓存；成功的改密、账号 rename/drop 删除相应缓存项。缓存是进程内对象，重启会重新建立。因此即使一段时间都能走 fast auth，仍须保证客户端能够完成 TLS/RSA 完整认证。[失效处理](/Users/cs/code/mysql-server/sql/auth/sha2_password.cc:1391)、[FLUSH 事件来源](/Users/cs/code/mysql-server/sql/server_component/mysql_server_event_tracking_bridge_imp.cc:102)、[缓存初始化](/Users/cs/code/mysql-server/sql/auth/sha2_password.cc:1229)。

`sha256_password` 同样会验证带盐 SHA-256 凭据，但没有上述 fast-auth/cache 路径；入口直接读取密码或 RSA 密文，未加密的传输要求 RSA。[直接完整认证入口](/Users/cs/code/mysql-server/sql/auth/sql_authentication.cc:4674)、[RSA 分支](/Users/cs/code/mysql-server/sql/auth/sql_authentication.cc:4749)。它与 `caching_sha2_password` 是不同插件、不同认证流程，不应按名称都含 SHA-256 就视为同一种方式。

## MySQL 默认认证与多因素

8.4 的 `authentication_policy` 默认是 `*,,`：第一个因子必需，第二、第三个可选；未指定第一个因子的具体默认插件时使用 `caching_sha2_password`。服务器初始握手插件默认也为 `caching_sha2_password`。[策略变量](/Users/cs/code/mysql-server/sql/sys_vars.cc:7381)、[初始默认插件](/Users/cs/code/mysql-server/sql/auth/sql_authentication.cc:1168)。

最多支持三个认证因子，但不能把任意三个密码插件串联：第二、第三因子不能使用 `AUTH_FLAG_USES_INTERNAL_STORAGE` 的插件，因此 native/caching SHA-2/sha256 这几个内部密码插件不能用作第二、第三因子。外部认证/设备注册等插件仍须满足相应限制和服务端安装条件。[MFA 插件检查](/Users/cs/code/mysql-server/sql/auth/sql_mfa.cc:575)。

## 核对限制

仅核对当前源码、构建声明和 Oracle 官方文档；没有编译或运行 MySQL，也没有验证本机安装的插件清单、企业插件实现、客户端组合或性能。源码包含插件不等于实例已经加载插件；企业客户端源码不等于社区服务端具备对应认证能力。

## TiDB SM3 与 LDAP 的引入历史

本节的作者、合入时间来自 GitHub PR API 的 `user.login`、`merged_at`，发布版本及日期来自 PingCAP 官方 release notes，并以本地上游发布 tag 的代码交叉核对。合入日期不等于发布日期；自动 cherry-pick PR 的机器人账号不等于功能原作者。

| 能力 | 原始 PR / 作者 | 原始 PR 合入时间（北京时间） | 首次官方发布 |
|---|---|---|---|
| `tidb_sm3_password` | [#36193](https://github.com/pingcap/tidb/pull/36193)，[CbcWestwolf](https://github.com/CbcWestwolf) | 2022-09-08 12:00:57（UTC `2022-09-08T04:00:57Z`） | **v6.3.0-DMR，2022-09-30**；[官方 release notes](https://docs.pingcap.com/tidb/stable/release-6.3.0/) 明确新增 SM3 认证并署名 CbcWestwolf |
| `authentication_ldap_simple` 与 `authentication_ldap_sasl` | **同一个** [#43582](https://github.com/pingcap/tidb/pull/43582)，[YangKeao](https://github.com/YangKeao) | 2023-05-10 22:51:21（UTC `2023-05-10T14:51:21Z`） | **v7.1.0 LTS，2023-05-31**；[官方 release notes](https://docs.pingcap.com/tidb/stable/release-7.1.0/) 明确两插件均从 v7.1.0 引入并署名 YangKeao |

原始提交分别为 [`1d482db4dd9a5657b9bd9b72ab155edd15adbe71`](https://github.com/pingcap/tidb/commit/1d482db4dd9a5657b9bd9b72ab155edd15adbe71) 与 [`7caffd9ba428161e5ec9eed05fc448afffcf4cdb`](https://github.com/pingcap/tidb/commit/7caffd9ba428161e5ec9eed05fc448afffcf4cdb)。[SM3 PR 元数据](https://api.github.com/repos/pingcap/tidb/pulls/36193)、[LDAP PR 元数据](https://api.github.com/repos/pingcap/tidb/pulls/43582) 可直接复核作者与时间。

LDAP 有一个容易误判的版本点：原始 master 提交首次被 `v7.2.0` 的祖先历史包含，但它已通过自动 cherry-pick [#43696](https://github.com/pingcap/tidb/pull/43696) 于 2023-05-11 16:11:22（北京时间）合入 `release-7.1`，形成提交 [`00b50a30314c5470ea5f029235d2b2bba9e01a3f`](https://github.com/pingcap/tidb/commit/00b50a30314c5470ea5f029235d2b2bba9e01a3f)。因此只对原始提交运行 `git tag --contains` 会误判为 v7.2.0；正确首次发布是 v7.1.0。[cherry-pick PR 元数据](https://api.github.com/repos/pingcap/tidb/pulls/43696)、[v7.1.0 两个插件常量](https://github.com/pingcap/tidb/blob/v7.1.0/parser/mysql/const.go#L186)。本地验证：`v7.0.0` 没有这两个 LDAP 插件，`v7.1.0` 已包含；SM3 在 `v6.2.0` 不存在，在 `v6.3.0` 已存在。

发布状态不能只看版本号：SM3 的首次发布属于 **6.3.0-DMR**，LDAP 首次发布属于 **7.1.0 LTS**。对应 release notes 没有把这两项标记为 experimental 或 Enterprise Edition only；它们的实现直接进入公开 TiDB 仓库。官方版本文档也分别列为 “Yes, since 6.3.0” 和 “Yes, since 7.1.0”。SM3 的使用条件是客户端支持该 TiDB 专属插件，官方文档推荐支持 SM3 的 TiDB-JDBC；这不是 MySQL 自带认证插件。[6.3 版本文档源码](https://github.com/pingcap/docs/blob/release-6.3/security-compatibility-with-mysql.md)、[7.1 版本文档源码](https://github.com/pingcap/docs/blob/release-7.1/security-compatibility-with-mysql.md)。

需要与首次引入分开的后续改动：

- SM3 原始 #36193 曾为了客户端兼容，在握手切换时将 SM3 账号映射为 `caching_sha2_password`；同一作者的 [#37909](https://github.com/pingcap/tidb/pull/37909) 于 2022-09-19 合入，取消该映射，改用 `tidb_sm3_password` 握手。两个提交都已在首次发布的 `v6.3.0` 中，不能用原始 PR 的过渡行为解释已发布版本。[实际差异](https://github.com/pingcap/tidb/commit/ff873bbaf9d0d43e5789d1841c4c8845eb0cfbb9)。
- LDAP simple 和 SASL 是一起加入的；原始 #43582 就已有 SCRAM-SHA-1、SCRAM-SHA-256、GSSAPI 的 SASL 机制配置，及 TiDB 到 LDAP 的 **StartTLS**。这里的 GSSAPI 是 LDAP SASL 的机制名，不代表 TiDB 支持 MariaDB 的独立 GSSAPI 认证插件。[原始机制常量](https://github.com/pingcap/tidb/blob/7caffd9ba428161e5ec9eed05fc448afffcf4cdb/privilege/privileges/ldap/const.go)、[官方变量说明](https://docs.pingcap.com/tidb/stable/system-variables/#authentication_ldap_sasl_auth_method_name-new-in-v710)。
- 直接连接 **LDAPS / LDAP over SSL** 是 YangKeao 后续的 [#45754](https://github.com/pingcap/tidb/pull/45754)，于 2023-08-07 18:30:21（北京时间）合入。它新增 `ldap.DialTLS` 路径，不能倒推为 #43582 已支持 LDAPS。本地上游 `v7.1.0`、`v7.1.1`、`v7.2.0`、`v7.3.0` 没有该直接 TLS 路径，`v7.4.0` 已包含；自定义分支仍可能另行回移。[LDAPS 提交差异](https://github.com/pingcap/tidb/commit/0d308613cf1fbefcbc619386d52b59fb08dbd1a7)。

历史核查仅覆盖上述上游引入与相关边界；未穷举各厂商分支回移，也没有启动旧版本实测。

## 当前分支 authentication_policy 与多因素 SQL 实测

2026-11-18 使用二进制 `/tmp/tidb-mfa-check.q9HXlG/tidb-server` 启动新的隔离 unistore 实例；`-V` 的 Git Commit Hash 为 `5c980fb645c8f3ae76f006967a4bc8b28ce8ba26`，与当前 checkout 一致。实例只绑定 `127.0.0.1`，使用临时数据目录和随机端口，未修改已有数据库。10 项断言全部符合预期，结束后已停止实例并删除其运行目录。

| 操作 | 实测结果 |
| --- | --- |
| `SHOW GLOBAL VARIABLES LIKE 'authentication_policy'` | 空结果 |
| `SELECT @@GLOBAL.authentication_policy` | 1193：Unknown system variable 'authentication_policy' |
| `SET GLOBAL authentication_policy=...` | 1193：Unknown system variable 'authentication_policy' |
| 下方完整三因素 CREATE USER | 1064：在第一个 `AND IDENTIFIED` 处语法错误 |
| 查询失败语句是否留下 alice 账号 | 0 行账号，未创建 |
| 单独用 `authentication_fido` 创建账号 | 1524：Plugin 'authentication_fido' is not loaded |
| 分别用 SHA-2、SM3、LDAP Simple、LDAP SASL 创建单插件账号 | 四项均成功；只验证建账号，不代表 LDAP 登录已验证 |

完整失败语句：

```sql
CREATE USER 'alice'@'localhost'
  IDENTIFIED WITH caching_sha2_password
    BY 'sha2_password'
  AND IDENTIFIED WITH authentication_ldap_sasl
    AS 'uid=u1_ldap,ou=People,dc=example,dc=com'
  AND IDENTIFIED WITH authentication_fido;
```

实际错误核心为：

```text
ERROR 1064 (42000): ... line 4 column 6 near "AND IDENTIFIED WITH authentication_ldap_sasl ..."
```

这不是只有开关缺失。当前 [UserSpec 语法](/Users/cs/code/tidb/pkg/parser/parser.y:14601) 接收一个 `AuthOption`，[AST](/Users/cs/code/tidb/pkg/parser/ast/misc.go:1495) 和 [UserRecord](/Users/cs/code/tidb/pkg/privilege/privileges/cache.go:117) 都是单认证项；[登录验证](/Users/cs/code/tidb/pkg/privilege/privileges/privileges.go:747) 按账号插件分支执行，没有顺序遍历多个因子的链；[服务端能力集合](/Users/cs/code/tidb/pkg/server/server.go:110) 没有公布 MFA 能力。虽然存在 [MultiFactorAuthentication 常量](/Users/cs/code/tidb/pkg/parser/mysql/const.go:178)，它本身不构成实现。

因此当前分支不支持 MySQL `authentication_policy` 或示例中的多插件 MFA。现有 `REQUIRE X509 / ISSUER / SUBJECT` 可叠加客户端证书检查，但与 MySQL 的 `AND IDENTIFIED` / `AuthNextFactor` 机制不同。上节所述本地 MySQL 8.4.9 虽支持 MFA 框架，但已删除旧 FIDO 插件；相应企业设备插件是 WebAuthn，使用仍依赖安装与注册条件。

可复现脚本：[check.py](/tmp/tidb-auth-support.Yx87ZR/check.py)。结果：[results.json](/tmp/tidb-auth-support.Yx87ZR/results.json)。未连接真实 LDAP、Kerberos 或硬件认证设备，也未对 MySQL 实例运行此 SQL。
