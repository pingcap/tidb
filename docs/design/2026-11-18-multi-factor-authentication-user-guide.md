# TiDB 原生多因素认证用户文档（用户确认稿 v0.5）

状态：用户已在 Q15 整体确认 v0.5，Q19 已确认技术 spec 并完成当前分支实现。Q10 至 Q12 确定原生 MFA、数据库密码＋LDAP Simple、最多三个因子和基础策略范围；已有能力实测完成，两个 native 因子排除，Q14 确认纳入最小日志脱敏修复。本文 SQL 和登录示例已完成 parser、TiDB 实例及 MySQL CLI 8.4.9 直连复核；真实 LDAP TLS、COM_CHANGE_USER、多节点和其他接入路径仍按文末边界单独验收。

目标基线：`release-7.1.9-0` / `5c980fb645c8f3ae76f006967a4bc8b28ce8ba26`。行为参考 MySQL 8.0.27 与 8.4 共有的 MFA 语义，具体 SQL、账号管理和部署行为见本文及[原生 MFA 技术 spec](2026-11-18-native-mfa-spec.md)。

## 已有能力与本次增量

现有 LDAP Simple 已完成真实目录登录验证，直接复用，不新增一套 LDAP 认证实现。实测细节见[现有认证能力验证报告](2026-11-18-authentication-runtime-validation.md)，其中包含用户提供的 [PR #43582](https://github.com/pingcap/tidb/pull/43582) 多值 DN 示例。

| 能力 | 当前状态 | 本次处理 |
| --- | --- | --- |
| native、SHA-2 单因子登录 | 正确密码成功、错误密码拒绝；本机 SHA-2 客户端的完整认证需要安全连接 | 复用 |
| LDAP Simple 单因子登录 | 显式 DN、空 DN 搜索、多值 DN、正确/错误/空密码均已实测 | 复用 |
| LDAP 连接池、目录改密、服务故障 | 复用连接仍重新 Bind；目录改密影响新登录，原 SQL 会话继续可用；服务不可用时拒绝新登录 | 复用 |
| TLS 与 REQUIRE SSL | 两段 TLS 已分别验证；REQUIRE SSL 仅限制客户端到 TiDB 的加密连接 | 复用，不计入因子数量 |
| MFA 账号与登录 | 开发前基线中 AND IDENTIFIED、ADD FACTOR 报 1064；authentication_policy 报 1193 | 已实现 MFA 框架及其账号管理、策略、协议和持久化能力；运行验收见下文 |

本轮没有实测 LDAP SASL、SM3、JWT 的完整外部认证流程，也没有验证多节点传播、代理和全部客户端；这些不属于本轮已经证明的能力。LDAP Simple 的 StartTLS/LDAPS 单插件验证见既有报告，本轮 MFA 直连链路使用明文 LDAP 测试服务。单因子登录成功不代表原生 MFA 串联已经可用。

## 实现后示例复核状态

2026-11-19 对本文 7 个 SQL 代码块、共 17 条 SQL 语句执行当前 parser 复核，全部解析成功，覆盖 LDAP 全局变量、CREATE、ADD/MODIFY/DROP FACTOR、SET PASSWORD 和 authentication_policy 示例。随后在隔离 TiDB 实例中执行了对应账号管理语句，并使用 MySQL CLI 8.4.9 验证 native/LDAP、SHA-2/LDAP、三因子、错误或缺失凭据、TLS 和 REQUIRE SSL 场景。完整结果见[原生 MFA 端到端验证报告](2026-11-19-multi-factor-authentication-e2e-validation.md)。

`pkg/executor/mfa_test.go` 已覆盖 CREATE、ADD、逆序 MODIFY、双 DROP 的 JSON 生命周期；使用临时 modfile 固定本机 etcd/otelgrpc 依赖后，executor 集成测试已通过。实现后的 MySQL CLI 直连已真实完成 LDAP Bind 和两/三因子认证；仍未宣称通过的范围是 COM_CHANGE_USER 真实客户端调用、多节点传播、代理/会话迁移、其他驱动，以及 LDAP TLS 与 MFA 的组合。

## 交付范围

账号可以配置一至三个有序认证插件因子。登录时按账号定义的顺序认证，每个因子都必须按其插件规则验证成功。任一因子失败、无法完成或所需服务不可用时拒绝登录，不自动退回较少因子的认证。

本次采用 MySQL 产品定义的多插件 MFA：数据库密码与 LDAP 密码分别验证。它们仍属于密码，不能仅据此宣称满足要求两种不同安全因素类别的验收；不把插件数量等同于安全因素类别数量。

| 能力 | 首版范围 |
| --- | --- |
| MFA 组合中的第一因子 | `mysql_native_password` 或 `caching_sha2_password` |
| 第二、第三因子 | `authentication_ldap_simple`，各自配置 LDAP 身份并提供相应密码 |
| 创建与管理 | CREATE 的 AND IDENTIFIED；ALTER 的 ADD/MODIFY/DROP FACTOR；SHOW CREATE USER |
| 因子数量 | 最多 3 个；拒绝超过上限、没有第二因子却单独添加第三因子等非法配置 |
| 账号策略 | 基础 authentication_policy 的数量、指定插件约束及策略管理员权限语义 |
| 版本差异 | 保留 TiDB 现有第一因子默认插件；不加入 MySQL 8.4 的 *:plugin 默认插件和自动补齐语义，未支持写法明确拒绝 |
| 接入及验收客户端 | MySQL CLI 8.4.9 直连；其他驱动须单独验证后再进入支持清单 |
| 原有账号 | 不自动给单因子账号追加因子；保留已有单因子插件和客户端路径 |

MySQL 不允许 native、caching SHA-2、sha256 等使用内部密码存储的插件充当第二或第三因子。本地 MySQL 8.4.9 在明确启用 native 且单因子登录成功的条件下，仍以 4052 拒绝 native＋native、native＋SHA-2 和 SHA-2＋native；用户已确认遵循此限制。两个 LDAP 因子可以使用相同插件并分别指定 DN，这与 MySQL 源码及文档的允许行为一致；本地 MySQL 没有 Enterprise LDAP 服务端插件，尚未实测其 LDAP MFA 正向登录。

首版不包含 WebAuthn、设备注册、无密码登录、LDAP SASL 的 MFA 组合，或任意 extension 插件组合。现有 SM3、LDAP SASL、token、socket、extension 等单因子账号继续按原路径使用。创建或修改未支持的 MFA 组合应明确失败，不得接受配置后跳过相应因子。

代理和会话迁移不纳入首版支持承诺。任何接入路径都不得为已配置 MFA 的账号跳过额外因子；未支持的重新认证或迁移路径应拒绝该账号，而不是以单因子凭据放行。

## 推荐部署与两因子使用

以下示例假定外部 LDAP 服务已由目录管理员部署；已有 LDAP Simple 部署可以沿用现有目录、身份和连接设置。本文负责 TiDB 接入与账号使用，不替代目录产品的安装文档。两段 TLS 是本例保护 LDAP 密码传输的部署选择，不是 MFA 框架新增的认证因子或语法前提。实施验收必须准备真实 LDAP 目录，不能仅用插件切换测试代替成功 Bind。

### 1. 准备目录身份和证书

准备以下身份和凭据：

| 身份 | 用途 |
| --- | --- |
| LDAP 服务账户，例如 `cn=tidb-bind,ou=service,dc=example,dc=com` | TiDB 连接池预绑定；本例使用明确用户 DN，只需该路径要求的 Bind 能力 |
| LDAP 用户，例如 `uid=mfa_user,ou=People,dc=example,dc=com` | 第二因子身份及其非空 LDAP 密码 |
| TiDB 数据库账号 | 第一因子的数据库密码及原有数据库权限 |

示例使用独立、非空的数据库密码和 LDAP 密码，按各自系统的密码策略维护。TiDB 不验证两个系统的密码是否互不相同。LDAP 密码由目录系统管理；账号的 LDAP 认证串保存 DN，不保存一份用户的 LDAP 密码。

为每个 TiDB 节点配置客户端连接所需的服务端证书和私钥，证书应覆盖连接域名。例如：

```toml
[security]
ssl-cert = "/etc/tidb/certs/tidb-server.pem"
ssl-key = "/etc/tidb/certs/tidb-server-key.pem"
```

示例只列服务端身份所需配置，合入已有配置时保留原设置。客户端准备签发 TiDB 服务端证书的 CA。另在每个 TiDB 节点的同一路径部署 LDAP CA 文件，供 TiDB 验证 LDAP 服务端；这与客户端验证 TiDB 的 CA 是两套独立配置。

### 2. 配置 TiDB 到 LDAP 的连接

LDAP Simple 设置由集群全局变量管理，所有使用该插件的账号和因子共用。已有 LDAP 部署应复用并核对现有设置；更换这些全局设置会影响既有 LDAP Simple 账号，不能把它当成仅对某个 MFA 账号生效的修改。

使用具有 `SYSTEM_VARIABLES_ADMIN` 或 `SUPER` 权限的管理员，通过安全连接设置。示例选择支持 StartTLS 的 389 端口：

```sql
SET GLOBAL authentication_ldap_simple_tls = ON;
SET GLOBAL authentication_ldap_simple_ca_path = '/etc/tidb/certs/ldap-ca-v1.pem';
SET GLOBAL authentication_ldap_simple_server_host = 'ldap.example.com';
SET GLOBAL authentication_ldap_simple_server_port = 389;
SET GLOBAL authentication_ldap_simple_bind_root_dn = 'cn=tidb-bind,ou=service,dc=example,dc=com';
SET GLOBAL authentication_ldap_simple_bind_root_pwd = '<服务账户的非空密码>';
```

主机名不带 `ldap://` 前缀，并应与 LDAP 服务端证书匹配。方法默认是 SIMPLE，连接池容量可保留现有值。显式完整 DN 跳过用户搜索，因此本例不要求另设搜索基准和属性；但当前连接池仍先以服务账户预绑定，必须配置有效的服务账户及非空密码。

`tls=ON` 会在指定端口尝试 StartTLS，失败后尝试同一端口的 LDAPS，均校验服务端身份；没有明文回退，也不会自动切换端口。`tls=OFF` 即使端口写 636 也不代表已启用 TLS。选择 LDAPS 时应填写该服务实际监听的端口并验证连接。

这些 SET GLOBAL 会持久化并传播到其他节点，不需要逐节点重复设置；文件必须预先逐节点部署，配置传播也不是瞬时完成。SET 成功和 SHOW 显示配置值均不能证明网络、CA、服务账户和用户 Bind 已成功，应连接每个节点进行真实登录验证。

现有 CA 加载机制只在路径字符串改变时读取，原路径覆盖文件不会自动重载；改变 CA 路径也不会让已有池连接重新握手。更新 LDAP CA 后，应使用新文件路径，并在维护流程中重建连接池或重启相关节点，确认新的 LDAP 连接使用新信任配置。

服务账户密码属于敏感配置。本轮两次实测确认，基线会将 SET GLOBAL 设置的服务账户密码记录到内置 info 日志。Q14 已确认纳入该日志脱敏缺陷的最小修复：配置操作的内置常规日志不输出真实服务账户密码，LDAP 认证和配置语义保持不变。该修复不要求重新实现 LDAP；代码已加入脱敏逻辑，日志验证仍属于实现后的验收项。

### 3. 创建和检查账号

由具备账号管理权限的管理员配置本地密码及 LDAP 第二因子：

```sql
CREATE USER 'mfa_user'@'<账号Host>'
  IDENTIFIED WITH caching_sha2_password BY '<数据库密码>'
  AND IDENTIFIED WITH authentication_ldap_simple
    AS 'uid=mfa_user,ou=People,dc=example,dc=com';

SHOW CREATE USER 'mfa_user'@'<账号Host>';
```

`<账号Host>` 使用实际账号定义。示例显式选择 SHA-2；也可以采用首版支持的 native 第一因子。新增 MFA 不改变原有默认第一因子插件。账号业务授权继续按现有方式维护。

如需由账号设置拒绝未加密连接，可在 CREATE USER 语句末尾追加 `REQUIRE SSL`。它是可独立选择的既有账号条件，不是 MFA 语法必选项，不要求客户端提供证书，也不作为认证插件因子计数。TiDB 与 MySQL 均已实测：指定 REQUIRE SSL 后，TLS 连接不提供客户端证书仍能登录，禁用 TLS 则失败。

现有 LDAP Simple 在不指定 REQUIRE SSL 时也能通过未加密的客户端连接登录，但 mysql_clear_password 会传输原始密码。推荐部署仍使用下一节的客户端 TLS 配置；账号级 REQUIRE SSL 不能代替客户端发送密码前验证 TiDB 身份。TiDB 到 LDAP 的 TLS 另由 LDAP 变量控制。

### 4. 提供两项凭据登录

```sh
mysql --protocol=TCP --host=tidb.example.com --port=4000 \
  --user=mfa_user --password1 --password2 \
  --enable-cleartext-plugin \
  --ssl-mode=VERIFY_IDENTITY --ssl-ca=/path/to/tidb-ca.pem
```

客户端依次提示输入数据库密码和 LDAP 密码；不要把密码写入命令行参数。LDAP Simple 使用内置 `mysql_clear_password` 客户端插件，需要显式允许；该插件本身不强制 TLS。客户端的 VERIFY_IDENTITY 负责在发送密码前验证 TiDB 证书，TiDB 的 LDAP TLS 设置保护另一段连接。

两项验证全部成功后才允许执行普通 SQL。登录后通过 `SELECT CURRENT_USER();` 检查账号。任一密码错误、所需凭据未完成、LDAP 不可用或客户端缺少协议/插件能力时，登录失败。

## 因子管理与三因子示例

以下管理语句是分别说明操作的示例，不是一组要求连续执行的脚本。管理员操作受账号管理权限和对应因子策略约束；普通用户不能自行增加、替换或删除额外因子。

### 给现有账号增加或替换第二因子

```sql
-- 用于原有单因子账号。
ALTER USER 'mfa_user'@'<账号Host>'
  ADD 2 FACTOR IDENTIFIED WITH authentication_ldap_simple
    AS 'uid=mfa_user,ou=People,dc=example,dc=com';

-- 用于已有第二因子的账号，替换其目录身份。
ALTER USER 'mfa_user'@'<账号Host>'
  MODIFY 2 FACTOR IDENTIFIED WITH authentication_ldap_simple
    AS 'uid=mfa_user_v2,ou=People,dc=example,dc=com';
```

每次修改后检查 SHOW CREATE USER，并使用新连接验证新凭据成功、已替换的旧凭据失败。更换 DN 修改的是身份绑定，不是修改 LDAP 密码。

### 增加第三因子

准备另一 LDAP 身份及其独立密码；确认策略允许三个因子。它与第二因子复用同一套 LDAP Simple 全局连接配置，不为每个因子配置独立 LDAP 服务器：

```sql
ALTER USER 'mfa_user'@'<账号Host>'
  ADD 3 FACTOR IDENTIFIED WITH authentication_ldap_simple
    AS 'uid=mfa_user_factor3,ou=People,dc=example,dc=com';

SHOW CREATE USER 'mfa_user'@'<账号Host>';
```

登录命令增加 `--password3`：

```sh
mysql --protocol=TCP --host=tidb.example.com --port=4000 \
  --user=mfa_user --password1 --password2 --password3 \
  --enable-cleartext-plugin \
  --ssl-mode=VERIFY_IDENTITY --ssl-ca=/path/to/tidb-ca.pem
```

三项密码分别对应本地账号、第二因子 DN、第三因子 DN。MySQL 不要求各因子的插件名或 DN 互不相同，本方案也不新增这类限制；示例使用不同身份和密码，不能把重复验证相同凭据描述为增加了安全因素类别。

### 删除因子

```sql
ALTER USER 'mfa_user'@'<账号Host>' DROP 3 FACTOR;
ALTER USER 'mfa_user'@'<账号Host>' DROP 2 FACTOR;
```

这些是独立操作示例。删除必须通过当前对应策略检查；如策略要求第二因子，两因子账号不能直接删掉第二因子，具备策略例外权限的管理员另按下节处理。

对三因子账号直接删除第二因子时，原第三因子前移为第二因子。之后客户端原第三位置的密码应改在 `--password2` 提供，不保留空缺序号。

## 改密与故障恢复

普通用户按既有自助权限修改自身第一因子密码，例如：

```sql
SET PASSWORD = '<新的数据库密码>';
```

管理员或有权自助改密的用户也可以使用只修改密码的 `ALTER USER ... IDENTIFIED BY ...`。两者都应保留第一因子插件、已有第二/第三因子，以及未显式修改的账号条件。改密仍受适用的密码策略和第一因子插件策略约束。

LDAP 用户密码在目录系统修改或恢复，TiDB 无需同步保存该密码。数据库账号重命名保留已有显式 DN，不自动重命名 LDAP 用户。目录密码变更后，用新连接验证新密码可用、旧密码失败。

LDAP 服务暂时不可用时恢复服务，不自动跳过因子。用户遗忘密码时由对应系统的管理员核验身份并重置；目录身份永久变更时，由数据库管理员替换对应因子的 DN。不得把静默删除因子作为恢复成功。若业务管理员决定临时降低账号要求，应显式执行有权限且符合策略的因子删除，并接受期间认证要求降低的结果。

账号配置与目录变更用于后续认证，不会自动断开已有 SQL 会话。需要立即收回访问时，管理员另外识别并终止连接。配置传播后应逐节点验证，不能承诺瞬时生效。维护通道及所需管理权限应在启用 MFA 前由管理员准备好。

## authentication_policy 与权限

首版采用 MySQL 8.0.27 与 8.4 共有的基础语义：

| policy 示例 | 新建账号及相关因子操作采用的约束 |
| --- | --- |
| `'*,,'` | 默认允许 1、2、3 个因子 |
| `'*'` | 仅允许 1 个因子 |
| `'*,*'` | 要求 2 个因子 |
| `'*,*,'` | 至少 2 个因子，允许第 3 个 |
| `'*,*,*'` | 要求 3 个因子 |
| `'*,authentication_ldap_simple'` | 第二位置必须为 LDAP Simple |

星号表示该位置必需但插件不限，空项表示可选，未出现的位置表示不允许。实际 MFA 插件组合仍受首版清单限制；单因子可用插件沿用原有能力。

管理员可查看策略；需要至少两因子且允许第三因子时，可选择第二条语句：

```sql
SHOW GLOBAL VARIABLES LIKE 'authentication_policy';
SET GLOBAL authentication_policy = '*,*,';
```

默认保持 `'*,,'`，不自动改变旧账号。该变量不是登录时强制补足因子的开关，也不是每次账号操作都全量重审最终配置：

- 新建账号须满足数量及插件约束。
- 增加、修改、删除后续因子时执行相应策略检查。
- 策略改为 `'*,*'` 后，旧单因子账号仍可登录并修改第一因子密码，不因缺少第二因子就被要求自动补齐；但第一因子插件仍须满足对应修改策略。若第一位置被强制为另一插件，改密可能被拒绝。
- DROP USER、RENAME USER 按原有账号管理权限处理，不受 MFA policy 限制；重命名保留认证配置。
- 旧账号是否已采用 MFA，应检查实际账号定义，不能只查看全局策略值。

本节的策略入口为 CREATE USER、ALTER USER 和 SET PASSWORD。为保留 TiDB 旧行为，关闭 NO_AUTO_CREATE_USER 时的 GRANT 隐式建用户不进入该检查；直接系统表写入及恢复也不执行这些账号语句。实际登录始终验证账号已存的因子，不能用 policy 变量代替逐账号配置核查。

`AUTHENTICATION_POLICY_ADMIN` 允许有相应账号管理权限的管理员越过策略限制，操作成功并产生警告。该权限本身不授予账号管理权限，也不豁免登录时验证账号已配置的因子。一般的 CREATE USER 权限不等于策略例外权限；修改全局变量还需相应的变量管理权限。

首版不支持 MySQL 8.4 的 `*:plugin` 默认插件及自动补齐语义，使用该写法明确失败。现有密码策略继续作用于适用的本地密码修改，LDAP 密码策略由目录维护；authentication_policy 管理因子配置，三者分别生效。

## 启用与回退

1. 升级所有可能接收目标账号登录及管理语句的 TiDB 节点，再启用 MFA。旧节点可能不理解或强制额外因子，混合版本期间不承诺完整 MFA 保障。
2. 准备 LDAP、两段 TLS、配置权限、账号管理权限及恢复通道；核对全局 LDAP 设置对既有账号的影响。
3. 先配置测试账号，在每个节点完成正确凭据、错误/缺失凭据、外部服务故障、管理操作和三因子验证，再逐步启用目标账号。
4. 核查原有单因子账号。升级本身不改变它们的默认插件或追加额外因子；管理员后续设置的账号策略及显式账号变更分别生效。

回退到不支持 MFA 的版本前，先停止这些账号的新增访问，锁定受影响账号并按需终止已有连接，保留安全存储的账号定义和目录身份映射。回退后不能继续宣称该版本强制执行额外因子；恢复 MFA 服务前，重新升级、检查账号配置并逐节点验收后再解锁。若选择回退为单因子服务，必须由管理员明确决定并按权限操作，不能把它当作自动兼容行为。

## 验收清单

以下为实现后的验收状态；已完成项以“已验证”标记，剩余项仍需正式集成环境补测：

| 场景 | 预期 |
| --- | --- |
| native/SHA-2 第一因子＋LDAP 第二因子 | 已验证：两项验证都成功才能进入正常会话 |
| 两个不同 DN 的 LDAP 后续因子 | 已验证：三项验证都成功；分别输错第二或第三密码均拒绝 |
| 凭据缺失、错误或 LDAP 不可用 | 已验证错误/缺失凭据拒绝；LDAP 服务不可用仍需集成环境补测 |
| 两段 TLS 与可选 REQUIRE SSL | 已验证客户端到 TiDB 的 VERIFY_IDENTITY、MFA＋REQUIRE SSL；LDAP StartTLS/LDAPS 与 MFA 组合仍需补测，不受信任/过期/主机名边界仍沿既有报告和正式环境验收 |
| MFA 客户端能力或必需插件缺失 | 已验证缺少 MFA capability 的协议客户端被拒绝；其他插件缺失仍需补测 |
| 创建、增删改因子、SHOW CREATE USER | 已验证：顺序和配置一致，非法数量/序号/插件组合及删除后的前移语义由自动化和实例测试覆盖 |
| 普通用户修改额外因子 | 已有权限测试覆盖；真实客户端权限验收仍需正式环境补测 |
| 只修改密码、锁定/解锁或无关属性 | 已验证 SET PASSWORD 保留后续因子；其他属性组合沿既有回归继续验收 |
| 多账号语句中某项失败 | 仍需正式集成环境补测完整原子性矩阵 |
| policy 普通检查和有权管理员例外 | 已验证：普通账号的数量检查、策略管理员例外及旧单因子改密边界 |
| LDAP 配置与密码管理 | 已验证显式 DN、服务账户和真实 Bind；LDAP TLS、目录改密及配置传播沿既有报告和正式环境补测 |
| 配置变更和认证相关内置常规日志 | 已验证 LDAP 服务账户密码以 `******` 记录，日志不含明文；正确 MFA 凭据同时完成登录 |
| COM_CHANGE_USER | 重新验证目标账号全部因子；失败不取得目标账号身份 |
| 多节点和已有会话 | 新认证按生效配置校验；旧 SQL 会话按既有语义处理，不因变更自动断开 |
| 升级、回退和旧账号 | 所有支持入口遵守 MFA 要求，普通单因子路径保持，旧节点不作为 MFA 服务入口 |

## 文档确认与下一阶段

Q15 已整体确认本稿的使用流程、权限与策略语义、兼容边界、部署回退和验收要求。本次交付为原生 MFA 框架，复用现有 LDAP Simple；REQUIRE SSL 为独立可选条件，遵循 MySQL 对后续因子插件的限制，并纳入 Q14 确认的最小日志修复。

已形成并按 Q19 确认[原生 MFA 技术 spec v0.4](2026-11-18-native-mfa-spec.md)，包括真实调用链、协议、持久化、缓存、旧路径兼容及测试方案。实现已在当前目录 `mfa-native-support` 分支完成；真实 LDAP 端到端直连验收已完成，边界和未覆盖项记录在[端到端验证报告](2026-11-19-multi-factor-authentication-e2e-validation.md)。

### Q16/Q17 技术复核补充

本用户稿 v0.5 的已确认范围保持；Q17 已单独确认下述因子编号规则，其余技术细化边界随技术稿 v0.4 整体确认：新增 MFA 沿用 MySQL SQL、协议和 mysql.user 的 MFA JSON 格式；TiDB 原有变量持久化、TLS 存储、失败锁定与密码过期规则继续复用。策略例外权限需要管理员显式 GRANT，本次不自动为已有 root 或 SYSTEM_VARIABLES_ADMIN 补授。

目标 MFA 账号的 COM_CHANGE_USER 认证失败按 MySQL 关闭连接；Q17 已确认所有 MODIFY 按语句开始时的因子编号更新；例如先 MODIFY 3、再 MODIFY 2，仍分别替换第三和第二因子，SHOW 与登录顺序始终为 1、2、3，重载不改变后续操作含义。MySQL 8.4.9 在该场景存在已复现的重载相关异常，本版记录此差异并采用上述稳定行为。损坏或超出支持范围的 MFA 配置使该账号拒绝认证，不通过同名更宽 Host 的账号降级。具体证据及兼容边界见技术稿第 4–6、11 节；本版已按 Q19 确认的边界实现。

### Q18 旧行为保护补充（已纳入 Q19 确认）

未配置 MFA 且 policy 为默认时，旧账号的登录、密码管理、权限、TLS、错误和锁定行为保持；管理员显式配置非默认 policy 后，相关单因子管理语句也受新策略约束。新增握手能力位对所有客户端可见，但旧单因子不增加认证轮次或要求 MFA 客户端。Q14 已批准的 LDAP 服务账户密码日志脱敏仍保留。

系统表结构不变；MFA 仅使用现有 JSON 的同名成员，不对旧账号做整体属性清洗。普通 CHANGE_USER 及会话导出保留原行为，完整 SHA-2 CHANGE_USER 交换和新增迁移拒绝仅用于 MFA 范围。BR 不恢复 mysql.global_variables，恢复账号后还须核对目标集群 authentication_policy；不改变原恢复机制。
