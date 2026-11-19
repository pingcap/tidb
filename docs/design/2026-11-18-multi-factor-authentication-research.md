# 多因素认证源码调研与需求决策记录

更新日期：2026-11-18。最新状态：Q15 已整体确认[用户文档 v0.5](2026-11-18-multi-factor-authentication-user-guide.md)，Q19 已确认[原生 MFA 技术 spec v0.4](2026-11-18-native-mfa-spec.md)并完成当前目录 `mfa-native-support` 分支实现。Q17 已确认按因子编号稳定更新。LDAP Simple 复用，开发增量聚焦原生 MFA 框架及 Q14 的最小日志修复；不支持两个 native 因子。前期证书方案及修复 spec 为历史材料。

## Q18 后：旧行为保护与设计精简

用户要求以 TiDB 现有正确性、行为和兼容为边界；允许增加代码/字段，但修改原逻辑或表结构须慎重。本轮继续只读源码并修改文档，没有切分支或开发。系统表仍无 DDL/列/索引/默认值变更，也无 bootstrap 数据迁移。

v0.4 已撤回共用 prepareAuthResponse，首次握手和普通 CHANGE_USER 保留原块，仅 MFA 的 SHA-2 补完整交换；源端迁移只拒绝含 MFA/无效 MFA，缺失账号等旧导出不变。失败计数保留旧字段/条件，新增 CountFailedMFA；HasMultiFactorAuth 收敛为 bool，Auth 预查仅保留自动锁定前置失败所需部分。

账号管理保留原锁行前权限、逐账号 JSON/TLS 边界；仅实际因子操作生成 MFA patch，SET PASSWORD 不写原属性。默认 policy 无 MFA 时沿旧路径，非默认 policy 的新约束由管理员显式启用；extension 的额外动态权限拒绝保持。GRANT 隐式建用户及 BR 不恢复 global_variables 保持，文档明确 policy 的覆盖入口。握手 capability 位是必要的全局协议扩展，Q14 日志脱敏是已批准的旧输出变化。证据、取舍与实现验收门槛见[审阅记录](2026-11-18-native-mfa-spec-review.md)。

## Q15 后的技术方案与补充证据

技术稿选择沿用 MySQL 的 User_attributes.multi_factor_authentication 格式，在一次 session.Auth/ConnectionVerification 内完成所有因子；通过可选传输接口发送 AuthNextFactor，复用现有 LDAP Simple。坏 MFA 配置发布为账号拒绝状态，避免旧缓存继续按单因子放行；跨节点通知和账号事务沿现有机制。

补充 30 项 MySQL 管理/策略检查，使用本机已有 mysql_no_login 插件，只验证管理框架、不替代 LDAP 登录。实测确认多操作/多账号失败回滚、双 DROP 的原始序号、显式策略权限与 SUPER 区别，并发现合法逆序 MODIFY 的持久化数组反转。v0.1 曾拟按明确序号保存；Q16 强调对齐 MySQL 后进一步验证连续操作，发现结果依赖是否重载；v0.2 将其保留为明确待决策项，Q17 随后确认按明确因子编号稳定更新并记录差异，v0.3 已落实。详细证据、已修订的设计问题及未测边界见[审阅记录](2026-11-18-native-mfa-spec-review.md)。

## Q16 后：MySQL 兼容性专项复核

用户要求新增 MFA 的用户行为、设计实现及系统表均参考 MySQL，避免自创语法或持久化格式。已沿源码、28 项隔离元数据检查和 1 项连续操作补测复核，结论为：第一因子原列、后续四字段 JSON、SQL 与 0x02 协议均有 MySQL 依据；完整 mysql.user、全局变量/TLS 存储及账号锁定生命周期仍含 TiDB 既有差异。

Q17 已确认逆序 MODIFY 按明确因子编号稳定更新并记录 MySQL 重载相关异常差异；已将未知因子字段改为忽略并规范写回，补齐 NULL/属性保留规则及目标 MFA CHANGE_USER 失败断连。坏数据拒绝记录与 MySQL 跳过账号不同，能防止同名宽 Host 单因子接替，作为安全例外明确提交确认。权限需显式 GRANT，不复制 MySQL 升级自动补授。证据与差异清单见[技术审阅记录](2026-11-18-native-mfa-spec-review.md)。本轮仍只改文档。

## Q13 后的实测与范围修正

完整结果、基线、复现材料和未测边界见[现有认证能力实测报告](2026-11-18-authentication-runtime-validation.md)。下文前期调查保留其当时的证据范围，以本节及实测报告为最新状态。

- TiDB 当前提交＋真实 OpenLDAP：两轮分别 41、42 项断言符合预期，第二轮补测目录改密后已建立 SQL 会话继续可用。LDAP Simple 已完成真实 Bind，无需重新开发；PR #43582 的多值 DN 示例也已实测。
- 已验证 LDAP 正确/错误/空密码、不同用户、显式 DN、空 DN 搜索、连接复用、目录改密、服务凭据错误及服务不可用、StartTLS、LDAPS、错误 CA。仅使用 loopback 隔离服务和临时凭据，未更改产品代码。
- REQUIRE SSL 是独立的客户端连接加密条件。MySQL 与 TiDB 均验证 TLS 无客户端证书可登录、未加密连接被拒绝；现有 LDAP Simple 未设置 REQUIRE SSL 时也能使用未加密客户端连接。用户稿将其从核心 MFA SQL 中分离为可选账号设置，生产示例仍推荐两段 TLS。
- MySQL 8.4.9 明确启用 native 后，native 单因子成功，native＋native、native＋SHA-2、SHA-2＋native 及 ADD 2 FACTOR native 均报 4052。用户确认不支持两个 native 的自定义组合。MySQL 本地无 Enterprise LDAP 服务端插件，LDAP MFA 正向组合仍是官方文档及源码依据，不能声称已完成实测。
- 当前 TiDB 的 AND IDENTIFIED、ADD FACTOR 报 1064，authentication_policy 报 1193，原生 MFA 框架确实尚需开发。
- 两轮均复现 LDAP 服务账户密码进入 SET GLOBAL info 日志。Q14 已确认纳入最小修复，验收为该配置操作的内置常规日志不输出真实服务账户密码，LDAP 认证与配置语义保持。范围确认不等于允许立即开发，仍须完成用户文档和技术 spec 两次整体确认。
- LDAP SASL、SM3、JWT 完整外部认证、多节点、代理、其他驱动及性能未在本轮实测；不能把局部验证称为全部认证能力测试完成。

## Q11/Q12 确认后的使用流程核对

Q11 选择数据库密码＋LDAP Simple。MFA 第一因子清单为 native/caching SHA-2，第二及第三因子为 LDAP Simple；其他已有单因子认证路径保留。Q12 选择最多三个有序因子、创建/增删改查、基础 authentication_policy 及策略管理员语义，保留 TiDB 现有第一因子默认，不包含 MySQL 8.4 的默认插件与自动补齐语义。完整使用流程由用户文档整体确认。

### LDAP 部署与配置生效

- [LDAP Simple 变量注册](/Users/cs/code/tidb/pkg/sessionctx/variable/sysvar.go:3479)均为 ScopeGlobal。SET GLOBAL 持久化并通过系统变量缓存/通知传播；依据为 [SetGlobalSysVar](/Users/cs/code/tidb/pkg/session/session.go:1336)、[缓存刷新](/Users/cs/code/tidb/pkg/domain/sysvar_cache.go:124)。不能写成每个节点都要重复 SET，但必须逐节点部署同一路径 CA 文件并验证配置及登录生效。
- [连接工厂](/Users/cs/code/tidb/pkg/privilege/privileges/ldap/ldap_common.go:130)在 tls=ON 时先 StartTLS，失败后同一端口尝试 LDAPS；都验证 CA 和 server_host，没有明文回退或端口自动切换。tls=OFF 不会因为端口为 636 自动使用 TLS。
- 显式用户 DN 跳过搜索，但仍经过[服务账户预绑定](/Users/cs/code/tidb/pkg/privilege/privileges/ldap/ldap_common.go:204)。因此最小安全示例配置有效 bind_root_dn 和非空 bind_root_pwd；搜索基准/属性可以沿用默认，因为该示例不执行搜索。
- 当前 LDAP fork 的 [SimpleBind](/Users/cs/build/go/pkg/mod/github.com/!yang!keao/ldap/v3@v3.4.5-0.20230421065457-369a3bab1117/bind.go:59)拒绝空密码，服务账户预绑定也没有启用空密码例外。AS 空串仍会存储并在登录时走搜索，不能声称 DDL 禁止空 DN。用户文档采用显式非空 DN，未扩大修改此旧路径。
- [CA 与配置 setter](/Users/cs/code/tidb/pkg/privilege/privileges/ldap/ldap_common.go:247)只有路径改变时重新读 CA，更新路径不清理连接池，原路径替换文件也不会重新读取。CA 读取失败可能只记录日志，SET/SHOW 成功不是信任配置正确的证据；需要真实新连接验收。文档保留这一既有运维限制，不声称已新增 CA 热更新能力。
- 客户端 mysql_clear_password 本身不要求 TLS；当前账号 REQUIRE SSL 校验发生在认证响应读取之后。用户流程必须保留客户端 VERIFY_IDENTITY，不能用账号条件代替发送密码前的身份验证。LDAP tls 变量只保护 TiDB 到目录这一段。
- [SET GLOBAL 通用日志](/Users/cs/code/tidb/pkg/executor/set.go:268)把变量值放入常规日志，仅对云存储 URI 特判；LDAP 服务账户密码存在记录风险。此处为已读源码结论，未用真实秘密运行复现。日志脱敏作为当前接入流程相关的交付验收要求，后续技术 spec 定义最小修复及回归边界；不能把 SHOW 的掩码误写成全链路已脱敏。

本轮只核对代码及已有工具，没有设置变量、配置目录服务、安装软件或修改产品代码。用户文档中的部署命令仍是交付目标示例。

### 同插件三因子、改密与 policy 边界

MySQL 允许第二、第三因子都使用 LDAP Simple 并保存各自 DN。逐因子[校验](/Users/cs/code/mysql-server/sql/auth/sql_mfa.cc:566)、[序列化](/Users/cs/code/mysql-server/sql/auth/sql_mfa.cc:731)及[认证串切换](/Users/cs/code/mysql-server/sql/auth/sql_authentication.cc:3654)没有按插件名或 DN 去重。ER_MFA_METHODS_IDENTICAL 针对同一语句重复操作相同因子序号，不表示不能重复插件。用户示例采用两个不同 DN 和独立密码，服务端不新增“DN/密码必须互异”的 MySQL 外限制。

ALTER USER 只改第一因子密码时[沿用原 MFA 列表](/Users/cs/code/mysql-server/sql/auth/sql_user.cc:2010)；SET PASSWORD 使用原第一因子插件并[保留 MFA](/Users/cs/code/mysql-server/sql/auth/sql_user.cc:2163)。LDAP 密码由外部目录管理，修改因子 DN 不会修改目录密码。

| policy 已改为 '*,*'，无策略例外权限 | MySQL 行为 |
| --- | --- |
| 创建单因子账号 | 拒绝缺少第二因子 |
| 旧单因子账号 ALTER IDENTIFIED BY 或 SET PASSWORD | 不补查缺少第二因子；第一位置插件仍执行相应检查 |
| 两因子账号 DROP 2 FACTOR | 拒绝删除必选第二因子 |
| 三因子账号 DROP 2 FACTOR | 原第三因子前移，对新的第二位置执行相应策略检查 |
| 三因子账号 DROP 3 FACTOR | 可保留两个符合要求的因子 |
| DROP USER、RENAME USER | 不受 MFA policy 检查，保留账号管理权限等既有规则；重命名不重命名 LDAP 用户 |

依据为 [第一因子和 MFA 操作的策略检查](/Users/cs/code/mysql-server/sql/auth/sql_user.cc:1123)、[仅创建时的缺少因子检查](/Users/cs/code/mysql-server/sql/auth/sql_user.cc:1187)、[SET PASSWORD 调用共同校验](/Users/cs/code/mysql-server/sql/auth/sql_user.cc:2146)、[后续因子复核](/Users/cs/code/mysql-server/sql/auth/sql_mfa.cc:318)。8.0.27 也保留旧单因子改密边界。

后续因子复核并非每次都全量审查最终账号。例如旧三因子账号在 policy 变为 '*,*' 后只 MODIFY 第二因子，不会统一因为未改第三因子超过策略长度而拒绝。因此用户文档使用“新建账号及相关认证配置变更受策略检查”，不声称任意 ALTER 成功后整个账号必然满足新 policy。第一位置强制插件改变时，SET PASSWORD 也可能被拒；不能笼统写它完全不受 policy 影响。

## Q10 后：原生 MFA 的首版范围调查

本节保留 Q10 后、Q11/Q12 确认前的范围调查，最终决定见上节及用户确认稿。

| 新核实事实 | 证据及产品影响 |
| --- | --- |
| LDAP Simple 可以作为后续因子 | [MySQL 官方 Pluggable Authentication](https://docs.oracle.com/cd/E17952_01/mysql-8.4-en/pluggable-authentication.html)明确给出密码加 authentication_ldap_simple 的 2FA 示例。SASL 是另一种合法组合，不是所有后续 LDAP 因子的必选协议 |
| 第一因子密码与 LDAP 密码可以分开提供 | [客户端当前因子密码切换](/Users/cs/code/mysql-server/sql-common/client.cc:6064)和 [cleartext 插件取密码](/Users/cs/code/mysql-server/sql-common/client.cc:9570)；CLI 提供 --password1/2/3，使用不带值的选项交互输入 |
| TiDB 有 Simple 单插件基础 | [LDAP Bind](/Users/cs/code/tidb/pkg/privilege/privileges/ldap/simple.go:27)和 [客户端插件映射](/Users/cs/code/tidb/pkg/server/conn.go:271)已存在；当前没有 MFA 串联，不能把可复用基础写成组合已支持 |
| TiDB 的 SASL 范围 | [SASL 交换](/Users/cs/code/tidb/pkg/privilege/privileges/ldap/sasl.go:32)支持多轮中继，配置可选 SCRAM-SHA-1、SCRAM-SHA-256、GSSAPI；实际目录、客户端库及 Kerberos 等依赖仍需分别准备 |
| 本机客户端 | MySQL CLI 8.4.9，mysql_config --plugindir 返回 /opt/homebrew/Cellar/mysql@8.4/8.4.9/lib/plugin；安装目录未找到 LDAP SASL 或 WebAuthn 客户端模块，mysql_clear_password 为内置客户端插件 |
| 已有 LDAP 测试边界 | [TestLDAPAuthSwitch](/Users/cs/code/tidb/pkg/server/conn_test.go:2222)用 failpoint 验证插件切换；LDAP 包测试包含 DN/TLS/超时，但本轮检索未找到可直接复用的真实目录成功登录环境。实现阶段必须补真实 LDAP 与 MFA 全链路验收 |
| 第二/第三因子插件限制 | [MySQL 校验](/Users/cs/code/mysql-server/sql/auth/sql_mfa.cc:583)拒绝使用内部密码存储的插件；native、caching SHA-2、sha256 均不能放在后续位置 |
| 管理权限 | [MySQL 自助/管理区分](/Users/cs/code/mysql-server/sql/sql_parse.cc:4787)中，单独 ADD/MODIFY/DROP FACTOR 需要账户管理权限；本人注册/注销的豁免另行处理，不能与配置因子混为一谈 |

Q11 已选择数据库密码＋LDAP Simple，明确交付为 MySQL 多插件联合认证并接受外部 LDAP 依赖。这是两个密码，不自动满足要求不同安全因素类别的验收。WebAuthn 及注册流程不进入首版。

### 版本与策略边界

[官方 authentication_policy](https://docs.oracle.com/cd/E17952_01/mysql-8.4-en/server-system-variables.html#sysvar_authentication_policy)定义因子必选、可选、禁止和指定插件约束。默认 '*,,' 允许 1/2/3 因子；'*,*' 要求 2 因子；'*,*,' 至少 2 个、允许第 3 个。变量约束创建与修改，不追溯给现有账号追加因子或改变登录要求。

AUTHENTICATION_POLICY_ADMIN 的策略例外不替代账户管理权限，也不跳过登录因子。全局变量管理权限与策略例外权限分别检查。

MySQL 8.4 比 8.0.27 增加 *:plugin 默认插件和某些必选因子的自动补齐路径，证据为 [8.4 policy 解析](/Users/cs/code/mysql-server/sql/auth/authentication_policy.cc:91)、[CREATE 默认补齐](/Users/cs/code/mysql-server/sql/auth/sql_user.cc:1587)及 [8.0.27 解析](https://github.com/mysql/mysql-server/blob/mysql-8.0.27/sql/mysqld.cc#L4516)。Q12 已确认首版不包含该新增语义，保留 TiDB 原有第一因子默认行为。

### WebAuthn 的条件性范围（历史调查，Q11 已排除）

[官方 WebAuthn](https://docs.oracle.com/cd/E17952_01/mysql-8.4-en/webauthn-pluggable-authentication.html)需要服务端插件、客户端插件、libfido2、设备、RP ID 和注册流程；MySQL 服务端实现属于 Enterprise，不能把本地公开客户端源码当成可移植服务端。多节点使用同一账号时需相容的 RP ID。

选择该方案后，需要另外确认首次注册沙箱、本人注册、本人/管理员注销、设备遗失恢复。UNREGISTER 清除注册材料但保留因子，源码见 [sql_mfa.cc](/Users/cs/code/mysql-server/sql/auth/sql_mfa.cc:714)。MySQL 文档还说明成功注册可让当前连接退出沙箱，即使后续非 WebAuthn 因子尚未验证；不能将“所有因子成功后才能进入正常会话”不加区分地套用于该特殊连接。

无密码 WebAuthn 单因子另有 INITIAL AUTHENTICATION、临时密码和 PASSWORDLESS_USER_ADMIN，不因 Q10 自动纳入。Q11 若选择 LDAP，以上设备决策不进入本轮必答项。

本轮直接访问 dev.mysql.com 页面遇到 403，已通过 Oracle 官方 8.4 文档镜像核对相关内容。只读核查了本机客户端和测试资产，未安装软件、未启动目录服务，也未实现或运行原生 MFA。

## MySQL MFA 与证书方案的区别及结论修正

用户阅读 MySQL MFA 文档后指出，前期示例与其文档功能不同。重新核对官方说明，确认两者分别是有序认证插件链与额外 TLS 账号条件：

- [MySQL 8.4 Multifactor Authentication](https://dev.mysql.com/doc/refman/8.4/en/multifactor-authentication.html)：“The server invokes authentication plugins in the order listed in the account definition.”，任一插件失败即拒绝连接。使用 `AND IDENTIFIED` 配置后续因子，并有 `ADD/MODIFY/DROP FACTOR`、`authentication_policy` 及客户端协议语义。
- [CREATE USER SSL/TLS Options](https://dev.mysql.com/doc/refman/8.4/en/create-user.html#create-user-ssl)：“MySQL can check X.509 certificate attributes in addition to the usual authentication that is based on the user name and credentials.”。`REQUIRE ISSUER/SUBJECT` 是此类附加条件，不把证书配置为原生 MFA 的第二或第三插件因子。
- MySQL MFA 官方示例使用 caching SHA-2 加 LDAP SASL，并明确说用户需提供两个密码。这说明 MySQL 产品的插件因子数量与“知识、持有、生物特征”等安全因素类别数量需要分别描述，不能直接画等号。
- [NIST SP 800-63B-4](https://pages.nist.gov/800-63-4/sp800-63b.html) 第 2.2.1、3.1.6 节支持密码与密码学私钥持有证明作为两类因素；这只能支持候选方案的技术分类，不能替代客户对交付能力和验收方式的认可。

修正前期结论：不能仅依据客户当前的一句话需求确认其接受证书方案。若客户要求 MySQL 文档中的原生 MFA，前期方案不满足该功能要求；若客户接受密码与独立管理的客户端证书联合认证，证书方案才可作为候选继续验证。已复现的账号维护问题是真实问题，但修复它们不等于完成原生 MFA。

Q10 已确认以 MySQL 原生 MFA 为后续用户方案的讨论主线；实际组合和兼容边界由新一轮确认。原 Q9 不继续作为开发确认入口。

## 已确认的工作约束

- 基于 TiDB `release-7.1.9-0` 开发。
- 先分析 `/Users/cs/code/mysql-server`，再讨论并确认 [用户文档](2026-11-18-multi-factor-authentication-user-guide.md)，随后编写并确认技术 spec。
- 两份文档均得到用户明确确认后，才在 `/Users/cs/code/tidb` 创建开发分支并修改实现；不新建 worktree。
- 新分支名不得包含 `codex`、`ai`、`claude`。
- 采用 `grill-with-docs` 要求的逐轮访谈和领域建模：查清事实，按依赖讨论决策；术语确定后记录词汇表，有必要的架构取舍再记录 ADR。
- 第一轮用户已明确：优先满足客户原文“支持身份验证，支持多因素用户认证。需支持根据IP网段或者IP地址设置访问权限。”；MySQL MFA 完整兼容不是硬要求，但尽量沿用 MySQL 行为。
- 用户没有更多客户场景、具体因子或客户端版本信息，要求先参考 MySQL；不重复索要未知信息，也不把推荐默认项视为已确认需求。
- Q4 用户确认 IP 来源控制与多因素认证是独立需求；已有功能若符合要求就不开发。用户以黑白名单举例，没有明确新增全局拒绝规则、规则优先级或其他 ACL 能力。
- Q3 用户明确选择“按这个口径继续”：指定账号登录必须同时通过非空密码与绑定账号的客户端证书身份验证，任一缺失或错误均拒绝登录。这是方案方向确认，不等于完整用户文档或技术 spec 已获批准。
- Q5 用户确认首版覆盖直连，以 MySQL 命令行客户端作为标准示例和验收客户端；代理与会话迁移不在首版支持承诺内。
- Q6 用户确认采用外部 CA 签发、管理员维护证书绑定与人工恢复；变更按 MySQL 习惯对后续认证生效，已有连接由管理员按需另行终止。首版不新增内置签发、自助恢复或自动吊销系统。
- Q7 用户确认接受现有全局密码策略：启用校验并保持正最小长度，配合 MySQL 风格的自助改密权限限制；接受其他账号后续设密和改密也受全局策略约束，不自动修改既有密码或其现有登录结果。此处为部署前提确认，当前未改动任何系统变量。
- Q8 用户要求：先检查当前版本是否已经满足；若全部已有且无需开发，则不写技术 spec。验证在独立临时实例中完成，仅该实例修改了测试所需系统变量，现有部署未改动。

## 源码基线与证据限制

| 项目 | 本地位置 | 固定版本 |
| --- | --- | --- |
| TiDB | `/Users/cs/code/tidb` | `release-7.1.9-0`，`5c980fb645c8f3ae76f006967a4bc8b28ce8ba26` |
| MySQL | `/Users/cs/code/mysql-server` | `mysql-8.4.9`，`f9c88132a87a7d9e740e50bce2621999695bd3fe` |
| MySQL 历史对照 | 同一本地仓库，通过 `git show` / `git grep` 读取 | `mysql-8.0.26`、`mysql-8.0.27` |

未执行 fetch；本地已具备所需参考版本。下文保留源码调查阶段的证据和决策过程，其中“尚未运行”等表述记录当时状态；后续 TiDB 运行验证以[验证报告](2026-11-18-multi-factor-authentication-validation.md)为准。MySQL 服务端尚未运行对照实例。风险推断单独标注；建议范围不视作用户决定。此处的 MySQL 参考版本不等于已选定的兼容目标。

## MySQL 已核实的行为

| 主题 | 源码结论 | 主要证据（MySQL 8.4.9） |
| --- | --- | --- |
| 数量与顺序 | 最多三个有序因子，前一因子成功后执行下一因子；后续失败即停止 | [因子数量](/Users/cs/code/mysql-server/include/mysql_com.h:87)、[认证链](/Users/cs/code/mysql-server/sql/auth/sql_authentication.cc:3640) |
| 客户端能力 | 无 MFA 配置的账号走原路径；有 MFA 配置但客户端未声明相应能力时认证失败 | [能力检查](/Users/cs/code/mysql-server/sql/auth/sql_authentication.cc:3643) |
| 协议 | `AuthNextFactor` 包头为 `0x02`，携带客户端插件名和插件数据；客户端按当前因子选择认证材料 | [协议定义](/Users/cs/code/mysql-server/sql/auth/sql_authentication.cc:2020)、[客户端处理](/Users/cs/code/mysql-server/sql-common/client.cc:6043) |
| 创建与修改 | CREATE 可追加 `AND IDENTIFIED ...`；ALTER 可 ADD、MODIFY、DROP 第 2/3 因子 | [CREATE 语法](/Users/cs/code/mysql-server/sql/sql_yacc.yy:16848)、[ALTER 语法](/Users/cs/code/mysql-server/sql/sql_yacc.yy:17061) |
| 连续编号 | 没有第二因子时不能单独添加第三因子；删除第二因子会使原第三因子前移 | [存在性检查](/Users/cs/code/mysql-server/sql/auth/sql_mfa.cc:63)、[前移处理](/Users/cs/code/mysql-server/sql/auth/sql_mfa.cc:291) |
| 修改与展示密码 | `SET PASSWORD` 修改第一因子并保留 MFA；`SHOW CREATE USER` 顺序输出额外认证项 | [修改密码](/Users/cs/code/mysql-server/sql/auth/sql_user.cc:2104)、[保留 MFA](/Users/cs/code/mysql-server/sql/auth/sql_user.cc:2164)、[展示](/Users/cs/code/mysql-server/sql/sql_rewrite.cc:1177) |
| 插件限制 | 使用内部密码存储的插件不能用于第 2/3 因子，包括 MySQL 的 native 和 caching SHA-2 插件 | [校验](/Users/cs/code/mysql-server/sql/auth/sql_mfa.cc:583)、[native 标志](/Users/cs/code/mysql-server/sql/auth/mysql_native_password.cc:315)、[SHA-2 标志](/Users/cs/code/mysql-server/sql/auth/sha2_password.cc:1334) |
| 账号策略 | `authentication_policy` 默认 `*,,`，约束 CREATE/ALTER 的因子设置；不是给现存账号登录动态追加因子 | [变量说明](/Users/cs/code/mysql-server/sql/sys_vars.cc:7381) |
| 策略例外权限 | `AUTHENTICATION_POLICY_ADMIN` 可使违反配置策略的 DDL 产生 warning，不豁免账号登录时的因子验证 | [策略权限检查](/Users/cs/code/mysql-server/sql/auth/sql_user.cc:1222) |
| 持久化 | 第一因子保留原字段；额外因子存在 `User_attributes.multi_factor_authentication` 数组中 | [读取](/Users/cs/code/mysql-server/sql/auth/acl_table_user.cc:216)、[序列化](/Users/cs/code/mysql-server/sql/auth/sql_mfa.cc:731) |
| 注册例外 | 需要注册的插件可使连接进入注册沙箱；普通 SQL 被拒绝，放行的命令仍需权限检查 | [注册认证](/Users/cs/code/mysql-server/sql/auth/sql_authentication.cc:3680)、[命令限制](/Users/cs/code/mysql-server/sql/sql_parse.cc:5387) |
| 注销与删除 | `UNREGISTER` 清除认证材料并恢复待注册状态，`DROP FACTOR` 删除整个认证阶段 | [注销处理](/Users/cs/code/mysql-server/sql/auth/sql_mfa.cc:708) |

本地源码包含 LDAP SASL 和 WebAuthn 客户端以及通用注册机制；本轮检索未在公开插件源码中找到相应 LDAP/WebAuthn/FIDO 服务端实现，也未在所查认证目录发现 TOTP 实现。这一检索结论不能用来推断所有发行版或外部插件的能力，也不足以承诺直接移植这些服务端功能。

版本对照已确认：`mysql-8.0.26` 的 `include/mysql_com.h` 尚无 MFA 能力常量，`mysql-8.0.27` 已有，且其 `client/include/multi_factor_passwordopt-longopts.h` 已声明三个因子的密码参数。8.0.27 与 8.4.9 的策略默认值均为 `*,,`，但 8.4.9 还定义了 `*:plugin` 及第一因子默认插件规则；不能将所有版本的配置语义直接混用。

已阅读的测试证据是 [authentication_policy-t.cc](/Users/cs/code/mysql-server/unittest/gunit/authentication_policy-t.cc:121)，包含可选/必需因子、第三因子及非法数量等解析边界。本 checkout 未找到完整 MFA SQL/登录集成测试；上述行为尚未做运行验证。

## TiDB 当前支持与旧路径

对于 MySQL 原生多因子插件链，当前只有 [MFA 能力位常量](/Users/cs/code/tidb/pkg/parser/mysql/const.go:178)，[服务端能力集合](/Users/cs/code/tidb/pkg/server/server.go:110) 未公布它。[UserSpec](/Users/cs/code/tidb/pkg/parser/ast/misc.go:1495)、[用户缓存](/Users/cs/code/tidb/pkg/privilege/privileges/cache.go:117)、[CREATE USER](/Users/cs/code/tidb/pkg/executor/simple.go:1224)、[ALTER USER](/Users/cs/code/tidb/pkg/executor/simple.go:1900) 与 [SHOW CREATE USER](/Users/cs/code/tidb/pkg/executor/show.go:1989) 均按单插件、单凭据处理。检索未发现 `authentication_policy`、`AuthNextFactor` 或 MFA JSON 解析实现。此结论不否认下文已有的密码与 TLS 客户端证书联合检查能力。

真实登录调用链是握手与插件协商 → 插件协议交互 → `openSessionAndDoAuth` → `session.Auth` → `ConnectionVerification` → 成功后设置账号身份与角色。参见 [conn.go](/Users/cs/code/tidb/pkg/server/conn.go:692)、[session.go](/Users/cs/code/tidb/pkg/session/session.go:2922)、[privileges.go](/Users/cs/code/tidb/pkg/privilege/privileges/privileges.go:712)。

| 需要保留或明确的边界 | 已读代码事实及影响 |
| --- | --- |
| 普通单因子登录 | 现有客户端兼容路径、空密码、socket、native、SHA-2、SM3、LDAP、token 及 extension 都有既有语义；实现前要建立旧路径验证清单 |
| LDAP 与 extension | 服务端插件和客户端插件可能不同，并可能多轮交换；extension 成功后还能安装授权回调，多因子组合不能只循环覆盖回调。见 [插件接口](/Users/cs/code/tidb/pkg/extension/auth.go:28)、[认证回调](/Users/cs/code/tidb/pkg/privilege/privileges/privileges.go:320) |
| 失败计数 | 当前只有标记 `FailedDueToWrongPassword` 的失败才计入锁定；成功认证会清零。后续因子失败时不能在第一因子成功处提前清零，也不能无意扩大单因子计数范围。见 [session.Auth](/Users/cs/code/tidb/pkg/session/session.go:2970) |
| 密码过期 | 当前有密码过期 sandbox；后续因子与重置密码的关系需要明确。见 [过期处理](/Users/cs/code/tidb/pkg/session/session.go:3010) |
| 切换账号与连接重置 | `COM_CHANGE_USER` 重新认证且失败恢复旧身份；`COM_RESET_CONNECTION` 保留已认证身份，两者不能混同。见 [切换账号](/Users/cs/code/tidb/pkg/server/conn.go:2689)、[重置连接](/Users/cs/code/tidb/pkg/server/conn.go:2767) |
| 会话迁移 | `tidb_session_token` 跳过账号插件认证；当前 token 没有 MFA 完成标志或认证配置版本。见 [token 认证分支](/Users/cs/code/tidb/pkg/privilege/privileges/privileges.go:747)、[token 结构](/Users/cs/code/tidb/pkg/sessionctx/sessionstates/session_token.go:69) |
| 变更生效 | 账号管理提交后通过 `NotifyUpdatePrivilege` 刷新权限缓存；需要明确多节点与进行中连接的行为。见 [通知实现](/Users/cs/code/tidb/pkg/domain/domain.go:3393) |

升级风险推断：旧节点 [解析 User_attributes](/Users/cs/code/tidb/pkg/privilege/privileges/cache.go:1202) 时只读取已知字段，登录又只校验主凭据。如果仅在 JSON 中加入额外因子而允许旧节点接收登录，旧节点可能仍只验证第一因子。这是基于调用链的风险推断，尚未运行复现。部署启用条件和回退要求需要进入用户文档，防护机制留到技术 spec 设计。

## 第二轮补充：密码、客户端证书与来源限制

MySQL 明确将 [客户端证书检查](/Users/cs/code/mysql-server/sql/auth/sql_authentication.cc:4244) 作为密码认证之外的附加条件，`REQUIRE X509`、`ISSUER`、`SUBJECT` 的具体检查在 [acl_check_ssl](/Users/cs/code/mysql-server/sql/auth/sql_authentication.cc:2448)。现有 [grant_alter_user_qa.test](/Users/cs/code/mysql-server/mysql-test/t/grant_alter_user_qa.test:132) 包含密码与 X509 的组合，以及 issuer/subject 与连接示例。这套机制与 `AuthNextFactor` 插件链相互独立。

TiDB 真实链路也会同时执行两类检查：

1. [加载 TLS 配置](/Users/cs/code/tidb/pkg/util/misc.go:489) 将 `security.ssl-ca` 加入客户端证书信任池；`require_secure_transport` 与 CA 配置共同决定 TLS 层的客户端证书策略，不能不经兼容验证就改变现有部署配置。
2. [建立 session](/Users/cs/code/tidb/pkg/server/driver_tidb.go:308) 保存真实 TLS 连接状态。
3. [ConnectionVerification](/Users/cs/code/tidb/pkg/privilege/privileges/privileges.go:712) 在 `mysql.global_priv` 有账号要求时执行 `checkSSL`，随后验证账号插件凭据。
4. [checkSSL](/Users/cs/code/tidb/pkg/privilege/privileges/privileges.go:860) 检查可信证书链，按配置匹配 issuer 和 subject；它不代替密码校验。

TiDB [证书认证测试](/Users/cs/code/tidb/pkg/privilege/privileges/privileges_test.go:410) 覆盖缺少 TLS、缺少已验证证书链、issuer/subject 不匹配等边界；[TLS 集成测试](/Users/cs/code/tidb/pkg/server/tests/tls/tls_test.go:184) 建立真实证书连接。本轮只读测试，未运行；尚未执行密码、证书和源网段三者组合的集成验收。

已选方案是独立账号密码与账号绑定的客户端证书/私钥共同验证，另配来源限制。Q3 已确认该验收口径；不将其写成已实现 MySQL 原生 MFA 或已通过运行验收。

## 第二轮补充：IP 与网段能力对照

| 能力 | MySQL 8.4.9 | TiDB 当前基线 |
| --- | --- | --- |
| 精确 IPv4 | 支持 | 支持 |
| IPv4 网络地址/点分掩码 | 支持 | 支持 |
| IPv4 `/24` 简写 | 支持，8.0.27 也已支持 | 不支持；现有测试包含可创建但无法登录的反例 |
| IPv6 单地址 | 可通过 Host 字符串匹配，不承诺任意等价表示自动归一化 | 同样需注意文本表示；本需求未要求将 IPv6 纳入首版 |
| IPv6 网段 | 所查 Host 掩码解析器不支持 | 不支持 |
| 多个同名账号 | 选择一条匹配账号，使用其认证要求及权限 | 同左；不能叠加所有匹配项 |
| 重叠规则顺序 | 有地址类型与掩码排序规则 | 排序有差异，不能承诺与 MySQL 一样或统一最长前缀优先 |
| 普通代理/NAT | 通常看到 TCP 对端地址 | 同左；另有已存在的 PROXY protocol 来源转交机制 |

MySQL 证据：[CIDR 与掩码解析](/Users/cs/code/mysql-server/sql/auth/sql_auth_cache.cc:227)、[地址匹配](/Users/cs/code/mysql-server/sql/auth/sql_auth_cache.cc:323)、[账号选择](/Users/cs/code/mysql-server/sql/auth/sql_authentication.cc:2292)、[排序](/Users/cs/code/mysql-server/sql/auth/sql_auth_cache.cc:3956)、[网段连接测试](/Users/cs/code/mysql-server/mysql-test/t/grant2.test:363)。

TiDB 证据：[网段解析](/Users/cs/code/tidb/pkg/privilege/privileges/cache.go:1141)、[地址匹配](/Users/cs/code/tidb/pkg/privilege/privileges/cache.go:1565)、[匹配测试](/Users/cs/code/tidb/pkg/privilege/privileges/cache_test.go:250)、[CIDR 反例](/Users/cs/code/tidb/pkg/privilege/privileges/cache_test.go:272)、[IPv6 网段反例](/Users/cs/code/tidb/pkg/privilege/privileges/cache_test.go:294)、[账号排序](/Users/cs/code/tidb/pkg/privilege/privileges/cache.go:814)。

来源控制是账号定义的一部分。新增网段账号不会收紧同名 `@'%'` 账号；密码验证失败也不会继续尝试下一个宽范围账号。为避免超出原始需求，候选方案使用无重叠的账号来源配置，不预设需要新增全局 IP ACL、最长前缀排序或 CIDR 功能。

直连源地址见 [PeerHost](/Users/cs/code/tidb/pkg/server/conn.go:1026)。启用 PROXY protocol 后，TiDB 在 [认证前更新客户端地址](/Users/cs/code/tidb/pkg/server/conn.go:625)，相关代理头、fallback 行为已有 [集成测试](/Users/cs/code/tidb/pkg/server/tests/commontest/tidb_test.go:3079)。真实部署未确认，因此仍需单独验证代理信任范围；转交来源 IP 不等于转交原始 TLS 客户端证书身份。

## Q3 补充：双因素定义与适用条件

2026-11-18 直接读取 [NIST SP 800-63B-4](https://pages.nist.gov/800-63-4/sp800-63b.html)，核实以下定义：

- 第 2.2.1 节允许使用密码与单因素密码学认证组成两种因素；该节同时还有其他要求，不能只依据组合种类声称整个部署满足 AAL2。
- 第 3.1.6 节将通过认证协议证明持有和控制密码学密钥归为单因素密码学认证，并明确其属于“something you have”。
- 第 3.1.6.1 节允许在相应保证等级下使用可导出的密钥，但要求保护其免受未授权访问；软件密钥与硬件不可导出密钥不能被描述为相同保护强度。

据此判断：同一账号强制使用非空密码与绑定该账号的客户端证书私钥共同认证，可以构成知识因素与持有因素的双因素认证。前期将此推导为“满足客户原文的通常技术含义”过于肯定，现予修正：技术因素分类、MySQL 产品功能和客户验收是三个不同判断，当前信息不足以确认客户认可该方案。

[MySQL 8.4 CREATE USER — SSL/TLS Options](https://dev.mysql.com/doc/refman/8.4/en/create-user.html#create-user-ssl) 的官方说明也已直接读取，明确证书属性检查附加于通常的用户名及凭据认证；`X509` 本身不限定具体证书的 issuer/subject，客户端需提供证书和私钥。使用 `ISSUER` 与 `SUBJECT` 约束账号接受的证书身份，符合该既有语义。

本方案的必要边界是：非空密码、服务端实际校验客户端证书和私钥持有证明、证书身份与账号关联、支持的登录路径不能省略其中一项。证书内容是公开信息，单独持有证书文件不构成持有私钥的证明；IP 来源条件也不算第二因素。是否有两次交互输入提示不是判断依据。

TiDB [密码校验分支](/Users/cs/code/tidb/pkg/privilege/privileges/privileges.go:794) 允许账号存储密码和认证响应同时为空，因此若将本方案配置成空密码，就不能再据此声称同时验证知识因素。现有 [联合认证检查](/Users/cs/code/tidb/pkg/privilege/privileges/privileges.go:733) 与 [证书检查](/Users/cs/code/tidb/pkg/privilege/privileges/privileges.go:860) 为推荐方案提供实现基础；本轮尚未运行集成验收。

Q4 的处理原则已记录为复用已有账号级允许来源并单独验证。当前证据不支持将该能力直接称作具有独立允许/拒绝规则及优先级的全局黑白名单管理；若以后新增这一明确要求，再评估功能差距。

## Q3 确认后的维护流程调查

本节为源码调查和用户行为依据，不是已批准的实现方案。以下新增问题尚未通过运行复现；实际修复与回归测试需等待用户文档和技术 spec 均确认后执行。

### 省略 REQUIRE 的 ALTER USER 会清除证书要求

TiDB [parser.y:14687](/Users/cs/code/tidb/pkg/parser/parser.y:14687) 在未提供 `REQUIRE` 时生成空 options；[executeAlterUser](/Users/cs/code/tidb/pkg/executor/simple.go:1794) 无条件调用 [tlsOption2GlobalPriv](/Users/cs/code/tidb/pkg/executor/grant.go:522)，空输入返回非空字节串 `{}`。随后 [TLS 属性写入](/Users/cs/code/tidb/pkg/executor/simple.go:2112) 使用 `Priv=values(Priv)` 整体替换 `mysql.global_priv.Priv`，没有保留或合并旧约束的分支；[checkSSL](/Users/cs/code/tidb/pkg/privilege/privileges/privileges.go:867) 接受更新后未指定的 TLS 类型。

因此普通改密、`ACCOUNT LOCK/UNLOCK` 或其他不带 `REQUIRE` 的 ALTER 会丢失原证书要求。`SET PASSWORD` 是独立路径，只更新密码相关字段，见 [simple.go:2711](/Users/cs/code/tidb/pkg/executor/simple.go:2711)。显式的 `REQUIRE TOKEN_ISSUER ...` 则有不同返回分支，见 [grant.go:592](/Users/cs/code/tidb/pkg/executor/grant.go:592)，不能将其与完全省略 REQUIRE 混同。共享转换 helper 同时有 CREATE/GRANT 调用方，后续方案需沿调用点限定行为变化。

MySQL 的 [属性更新判断](/Users/cs/code/mysql-server/sql/auth/sql_user.cc:1308) 仅在明确修改 TLS 选项时更新它们；[TLS 属性写入](/Users/cs/code/mysql-server/sql/auth/acl_table_user.cc:931) 在明确修改时整组替换。用户行为应为“省略 REQUIRE 保留旧值，显式 REQUIRE 替换整组”。TiDB 已查测试没有直接覆盖改密后的证书约束保持性。

### 普通用户自改证书要求缺少管理权限检查

TiDB [executeAlterUser](/Users/cs/code/tidb/pkg/executor/simple.go:1838) 对当前账号走自助分支，跳过其他账号修改所需的管理权限检查；后续 TLS 写入没有对应的权限门槛。已沿 planner、optimizer、executor builder 和 sandbox 分支检查，未发现另有 TLS 属性检查。因此普通未过期账号可以显式取消或替换自己的证书要求的调用链证据完整，但仍待运行复现。

MySQL 在 [SQLCOM_ALTER_USER](/Users/cs/code/mysql-server/sql/sql_parse.cc:4781) 只对“改自己密码且不改锁定状态、也不指定 REQUIRE”的情况给予豁免；否则 [权限检查](/Users/cs/code/mysql-server/sql/sql_parse.cc:4841) 要求 `UPDATE` on `mysql.*` 或全局 `CREATE USER`。普通仅 USAGE 用户不能修改自己的 REQUIRE。对照时应使用完整 `'user'@'host'` 或 MySQL 支持的 `CURRENT_USER()` 语法，避免把 `USER()` 的语法限制误判为权限拒绝。

用户文档据此明确管理员维护证书要求、普通改密保留要求。技术 spec 仍需限定检查范围，保留合法的自助改密和其他旧路径。

### 全局密码策略不保证所有路径都保留非空密码

TiDB [全局密码变量](/Users/cs/code/tidb/pkg/sessionctx/variable/sysvar.go:665) 中的 enable、policy、length 均为全局设置，影响普通单因子账号。开启校验且长度要求大于零时，CREATE、明文 ALTER 和 SET PASSWORD 可阻止空密码；但 [ALTER 的校验条件](/Users/cs/code/tidb/pkg/executor/simple.go:1934) 仅覆盖 `ByAuthString`。

现有 [password_management_test.go:103](/Users/cs/code/tidb/pkg/executor/test/passwordtest/password_management_test.go:103) 明确要求启用校验后 `ALTER USER ... IDENTIFIED WITH ... AS ''` 仍成功。[EncodedPassword](/Users/cs/code/tidb/pkg/parser/ast/misc.go:1559) 接受空的 HashString，只有插件而不提供 BY/AS 的 ALTER 路径也会生成空密码。开启策略前已有的空密码不会被改写，登录时不重新执行该强度策略；将全局最小长度及字符要求配置为零也允许明文空密码。

已查代码未发现每账号最小长度或禁止空密码开关。因此“只打开 validate_password”不是已选双因素要求的充分保障；同时也不能无差别修改所有单因子账号的现有 AS 空串行为。Q7 已选择全局策略配合 MySQL 风格权限边界，不引入新的每账号属性或语法。

MySQL 对照补充：[自身改密豁免条件](/Users/cs/code/mysql-server/sql/sql_parse.cc:4787) 还要求使用 `IDENTIFIED BY` 且未指定 `IDENTIFIED WITH`。因此普通仅 USAGE 用户不能通过 `AS ''` 或仅指定插件绕开明文密码策略；这两条路径需要管理权限。具备管理权限的执行者仍可导入空认证串，MySQL 的策略并非全面禁止管理员执行 `ALTER ... AS ''`，见 [sql_user.cc:1732](/Users/cs/code/mysql-server/sql/auth/sql_user.cc:1732)、[哈希格式检查](/Users/cs/code/mysql-server/sql/auth/sql_user.cc:1805) 与 [空串合法性](/Users/cs/code/mysql-server/sql/auth/sha2_password.cc:710)。

据此沿用已确认的 MySQL 边界：管理员负责配置非空密码，普通用户仅能通过受密码策略约束的自助改密路径维护密码，认证插件和认证串导入由管理权限控制。结合正最小长度的现有全局策略实现普通用户的非空保障；用户已接受全局策略对其他账号后续设密和改密的影响。技术 spec 需保留授权管理员导入认证串的旧能力，并给出完整权限和设密路径的验证矩阵。

### 证书生命周期与现有连接

| 事项 | 已核实边界 |
| --- | --- |
| issuer/subject 续期 | 两端按身份属性匹配，并非绑定指纹、序列号或单一公钥；同身份新旧有效证书都可能被接受 |
| 私钥丢失后的更换 | 换用不同的 subject 并更新账号完整 REQUIRE 可使旧证书不再匹配该账号；这不等同于 CA 全局撤销 |
| 账号锁定与改密 | 修改账号并刷新权限缓存，不自动终止已有连接；MySQL 测试 [grant_user_lock_qa.test:58](/Users/cs/code/mysql-server/mysql-test/t/grant_user_lock_qa.test:58) 验证锁定后原连接仍可执行 SQL |
| 多节点生效 | 本节点刷新，其他节点接收权限通知；不能声称所有节点瞬时同步，需逐节点验证 |
| TLS reload | TiDB [reload 实现](/Users/cs/code/tidb/pkg/server/server.go:1034) 读取配置路径并替换当前实例 TLS 配置；需要管理权限，不广播集群，也不重新握手已有连接 |
| reload 失败 | 默认保留旧配置；`NO ROLLBACK ON ERROR` 有不同失败行为，用户流程使用默认形式。已有 [TLS 测试](/Users/cs/code/tidb/pkg/server/tests/tls/tls_test.go:379) 覆盖相关边界 |
| 撤销检查 | MySQL 有 [CRL 配置与加载](/Users/cs/code/mysql-server/vio/viosslfactories.cc:500)；已读 TiDB SQL TLS 链路没有 CRL/OCSP 回调或序列号/指纹匹配，不能把 MySQL 的该能力直接写成 TiDB 现有支持 |
| 会话迁移 | TiDB 仍检查新后端连接的 TLS 条件，但 token 分支替代密码，不含前端证书身份；代理和迁移需单独确定支持边界 |

Q6 已确认外部 CA 与管理员人工恢复：核验身份、锁定账号、必要时断开已有连接、更换私钥及 subject 绑定、按需重置密码、解锁后验证新旧凭据。操作必须在前述约束丢失问题修复后验证。

## Q7 已确认的密码策略边界

用户已确认复用现有全局密码策略并按 MySQL 修复自助修改权限：普通用户仅能通过受策略约束的合法自身改密路径操作，认证插件、认证串导入和证书要求由管理权限控制；管理员负责将目标账号初始化为非空密码并维持配置。

已有正最小长度的全局策略会影响普通单因子账号后续的设密/改密，但不改写既有密码，也不在登录时重新校验密码强度。相关权限修复可能使此前被错误放行的普通用户自行修改认证插件/证书要求语句变为拒绝，应在兼容性说明中明确；合法自身改密、管理员导入认证串及单因子登录旧路径需分别保护。

本次不采用另设每账号密码强度约束的方案，不增加相应字段或语法，也不自动启用集群策略。用户文档中的配置命令供管理员在修复交付后按部署流程使用；当前未改变任何系统变量。

## 决策树与当前进度

| 顺序 | 决策 | 当前状态 |
| --- | --- | --- |
| 第一轮 Q1 | 交付目标 | 已明确：以客户原文为准，MySQL 原生 MFA 兼容可选，尽量沿用 MySQL 行为 |
| 第一轮 Q2 | 客户端和场景 | 已明确：目前没有更多信息，先参考 MySQL 提出方案 |
| 第二轮 Q3 | 密码＋账号绑定客户端证书，或原生多因子插件链/其他因素 | 前期证书方案已归档，由 Q10 原生 MFA 路线替代 |
| 第二轮 Q4 | 按账号限制 IPv4 地址/网段 | 用户已明确确认满足要求并关闭；不再讨论或开发 |
| Q5 | 直连接入，或首版同时支持代理和迁移 | 已确认首版覆盖直连 |
| Q6 | 证书方案的签发与恢复 | 历史决定，不自动适用于新的认证组合 |
| Q7 | 证书方案的全局密码策略前提 | 历史决定，不自动作为原生 MFA 的新增部署前提 |
| 用户流程 | 升级启用与回退、完整示例、错误结果和验收清单 | 已写入用户文档；基础及缺口实测完成，完整发布验收仍待执行 |
| Q8 | 先检查现有功能，全部满足则跳过 spec 和开发 | 已执行，两轮各 27 项断言，20 通过、7 失败，失败项一致 |
| 原 Q9 | 技术 spec：证书约束保持性与自身修改权限 | 退出当前确认流程；事实保留，先重新明确 MFA 用户方案 |
| Q10 | 原生 MFA 或证书路线 | 已确认按 MySQL 原生 MFA 重新梳理 |
| Q11 | 首版认证组合与依赖 | 已确认：数据库密码＋LDAP Simple，采用 MySQL 多插件联合认证口径 |
| Q12 | 最大因子数与策略兼容边界 | 已确认最多 3 因子、完整基础管理和共同核心 policy，不包含 8.4 默认补齐语义 |
| Q13 | 用户确认稿 v0.3 整体确认 | 因补充实测暂缓；现有能力实测已完成，改由 v0.5 提交整体确认 |
| Q13 后补充 | 两个 native 因子 | 用户明确不支持；遵循 MySQL 的后续因子插件限制 |
| Q14 | LDAP 服务账户密码日志缺陷 | 已确认纳入最小日志脱敏修复，LDAP 功能继续复用 |
| Q15 | 用户确认稿 v0.5 整体确认 | 已确认；原生 MFA 技术 spec 已完成 |
| Q16 | 原生 MFA 技术 spec v0.1 | 未批准；用户要求专项复核 MySQL 用户行为、设计与系统表，已形成 v0.2 和差异清单；后续由 Q17 确定逆序 MODIFY 边界 |
| Q17 | 是否逐项复制 MySQL 逆序双 MODIFY 的重载相关异常 | 已确认按明确因子编号稳定更新，记录兼容差异 |
| Q18 | 原生 MFA 技术 spec v0.3 整体确认 | 未批准；要求复核旧功能正确性/行为/兼容及 clean code，已完成源码复核并修订 v0.4 |
| Q19 | 原生 MFA 技术 spec v0.4 整体确认 | 待答；确认后才在当前目录从指定基线切分支开发，不创建 worktree |
| 技术 spec 完整后 | 用户明确确认技术 spec，随后在当前目录创建开发分支 | 未确认、未切分支、未开发 |

当前完成源码调查、现有能力实测、用户确认及原生 MFA 技术稿和设计审阅；产品实现保持原样。方案选择和理由记入本文，暂不另建 ADR；术语记入[词汇表](../../CONTEXT.md)。
