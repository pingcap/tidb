# TiDB 原生 MFA 技术 spec 审阅记录

日期：2026-11-18。对象：[技术 spec proposal v0.4](2026-11-18-native-mfa-spec.md)。这是源码及基线实测支持的设计审阅，不是实现代码审查或发布验收。此前证书方案的 spec/review 不作为本方案实现设计。

## 结论摘要

审核对象为 v0.3 设计，可信基线仍是未修改的 TiDB release-7.1.9-0 / 5c980fb645c8f3ae76f006967a4bc8b28ce8ba26。Q18 未批准开发，要求优先保护旧功能、行为和兼容性。源码复核确认共享首因子准备器、迁移缺失账号拒绝会扩大旧行为变化，另发现权限检查顺序、extension 权限附加检查及 JSON 写入边界需要收紧。

整改已纳入 v0.4，文档复核通过，待用户整体确认；这不代表实现已验证无回归。Q17 的按因子编号稳定更新仍保持。产品代码未修改，分支未切换。

## Q18 专项复核：旧行为保护与 clean code

### 已修订问题一：共用认证准备器和迁移检查扩大旧行为变化

位置：v0.3 §6.3/6.4/8；严重程度：P2；证据强度：已确认；影响范围：普通 CHANGE_USER、旧会话导出。

**问题是什么？** 共用 prepareAuthResponse 会让普通 CHANGE_USER 增加首因子完整交换或采用首次握手的旧客户端回退；导出时新增“账号不存在即拒绝”会改变原有旧会话行为。

**是否真实存在？** [首次握手](/Users/cs/code/tidb/pkg/server/conn.go:700)有 SHA-2/SM3 完整交换，[CHANGE_USER](/Users/cs/code/tidb/pkg/server/conn.go:2737)只在插件名非空时 checkAuthPlugin 后直接 Auth；[EncodeSessionStates](/Users/cs/code/tidb/pkg/session/session.go:4840)没有账号存在性检查，[SHOW](/Users/cs/code/tidb/pkg/executor/show.go:2558)还允许 User 为空。这些差异来自当前真实调用链。

**当前原因是什么？** v0.3 为复用而合并不同旧入口，并把“避免生成不可用迁移材料”扩展成所有会话的新准入检查，超出 MFA 所需范围。

**改进建议是什么？** 已撤回共用准备器；保留首次握手与普通 CHANGE_USER 原块，仅目标 MFA 使用现有 checkAuthPlugin/authSha 补足 SHA-2。导出只拒绝当前精确账号含 MFA/无效 MFA，其他旧条件保持。

**建议是否合理？** 新 MFA 两种首因子与 CHANGE_USER 范围均保留，没有新插件解析器；源端少做的缺失账号拒绝不削弱目标端 token/MFA 校验。

**能解决什么？** 消除这两项明确的旧行为扩展；仍须实现后验证空 pluginName 的 MFA SHA-2、旧错误恢复、迁移缺失账号和 MFA token 拒绝。

**是否优于替代方案？** 全部 CHANGE_USER 同时修复会扩大范围；关闭 MFA CHANGE_USER 会缩小已确认能力。限定新 MFA 分支更符合当前需求。

### 已修订问题二：权限顺序与 JSON 重写范围未充分保护旧路径

位置：v0.3 §2.1/3.3/4；严重程度：P2；证据强度：已确认；影响范围：账号锁等待、权限错误时机、原有 JSON/TLS 写入。

**问题是什么？** 原稿先锁行后权限检查，可能让原本立即拒绝的请求等待账号行锁；四字段重写和删空 NULL 规则若应用到所有账号维护，会改变无 MFA 或只改第一密码的原属性。

**是否真实存在？** 基线 [ALTER 权限检查](/Users/cs/code/tidb/pkg/executor/simple.go:1858)先于锁行，[SET PASSWORD 权限检查](/Users/cs/code/tidb/pkg/executor/simple.go:2644)也在前；[CREATE](/Users/cs/code/tidb/pkg/executor/simple.go:1172)无属性写 `{}`，[SET PASSWORD 更新](/Users/cs/code/tidb/pkg/executor/simple.go:2716)不写 User_attributes。循环外 TLS patch 与旧 Password_locking 清理也确实共享于多账号操作。

**当前原因是什么？** 原稿按 MFA 需求描述通用流程，没有明确区分“原检查/写入”和“需要新因子数据的补充检查/patch”。

**改进建议是什么？** 已保留原权限检查位置，仅本人豁免的 MFA 判定在锁行后补充；MFA patch 只由实际因子操作产生，逐账号决定 TLS 写入，清空属性只处理 MFA 参与的合并结果。

**建议是否合理？** 继续复用原行锁及事务，userExistsInternal 仅多返回原始 JSON；RENAME 不解码、DROP 不接入新 codec。无新账号存储层或系统表迁移。

**能解决什么？** 规则上保护旧 `{}`/NULL、回调、权限时机及多账号隔离；实现需用锁等待、混合账号交换顺序、MFA 增删与清锁交叉验证，不能仅测试 helper。

**是否优于替代方案？** 对整列统一解码重写更整齐但改变旧数据；复制整套账号执行器重复事务逻辑。局部 patch 与原位置补判更小。

### 已修订问题三：策略例外绕过 extension 附加权限检查

位置：v0.3 §5；严重程度：P2；证据强度：已确认；影响范围：通过 extension 认证的管理员使用新 policy 例外。

**问题是什么？** 仅检查显式 AUTHENTICATION_POLICY_ADMIN 会忽略 extension 对该动态权限的拒绝。

**是否真实存在？** [HasExplicitlyGrantedDynamicPrivilege](/Users/cs/code/tidb/pkg/privilege/privileges/privileges.go:132)只查 SQL 授权，[RequestDynamicVerification](/Users/cs/code/tidb/pkg/privilege/privileges/privileges.go:145)再调用插件附加检查；[extension 契约](/Users/cs/code/tidb/pkg/extension/auth.go:57)及[已有测试](/Users/cs/code/tidb/pkg/extension/auth_test.go:171)均支持这一用途。

**当前原因是什么？** 原稿为排除 SUPER fallback，只用了低层显式授权检查，遗漏原扩展权限边界。

**改进建议是什么？** 已改为只在 policy mismatch 时同时要求显式检查和原 RequestDynamicVerification 通过；不改两个共享权限 API 的全局语义。

**建议是否合理？** 前者确保 MySQL 的显式权限要求，后者保留 extension 的附加拒绝；正常默认策略无 MFA 时不调用新豁免路径。

**能解决什么？** 关闭新增例外绕过插件的路径；实现测试需覆盖显式授权有/无、插件允许/拒绝与默认策略无额外回调。

**是否优于替代方案？** 修改共享权限 API 会波及其他动态权限；另造第三个权限接口没有必要。组合原检查最小且清晰。

### 已修订问题四：可删除的接口改名与查询错误合同

位置：v0.3 §6.2/6.3/8；严重程度：P3；证据强度：已确认；影响范围：失败统计契约、预查和错误处理复杂度。

**问题是什么？** 把 FailedDueToWrongPassword 整体改名、为纯 MFA 范围查询引入缺失/损坏错误，以及每次 Auth 的额外预查，增加了不必要的旧接口改动和分支。

**是否真实存在？** [旧字段](/Users/cs/code/tidb/pkg/privilege/privilege.go:40)的消费点在 [Auth](/Users/cs/code/tidb/pkg/session/session.go:2972)，旧置位条件集中在 [第一因子验证](/Users/cs/code/tidb/pkg/privilege/privileges/privileges.go:795)。身份匹配后到实际验证前，仅自动锁定块有错误早退。

**当前原因是什么？** 原稿把新增 MFA 的失败统计原因当成重命名旧契约的理由，又将迁移“账号不存在拒绝”的额外要求带入查询接口。

**改进建议是什么？** 已保留旧字段与置位，新增 CountFailedMFA 用 OR 进入原计数；HasMultiFactorAuth 收敛为 bool，实际认证报错仍由原校验器负责；Auth 预查仅留在 enableAutoLock 块前。

**建议是否合理？** 两个计数字段分别表达原契约和新增原因，不同值回填、不加适配器；纯查询不需要生成错误。减少范围并保持原 Auth 签名。

**能解决什么？** 旧单因子失败统计不变，少一次普通无自动锁定认证的查询；仍保留 MFA 错误穿过计数事务的可靠分类。

**是否优于替代方案？** 将所有错误都算失败会改变旧插件统计；删除所有分类又会使 MFA CHANGE_USER 在统计错误后恢复旧连接。保留有职责的分类、删除无必要的改名及查询错误更合适。

### 保留的复杂度与能力边界

- JSON codec 在 executor/cache/SHOW 复用；policy parser 在 SET GLOBAL 和账号执行器复用；因子合并承载原始序号与 Q17 规范顺序。保留这些纯函数，不新增 provider、registry 或统一第一密码编码层。
- 可选 NextFactorAuthConn 避免修改 extension 已公开 AuthConn；0x02 不能用原前缀 0x01 写入方法替代。CHANGE_USER preflight、实际验证 MultiFactor 和最终 MFAAuthError 分别承担交换选择、实际快照和统计错误后的失败范围，保留并限制生命周期。
- 握手 capability 位在用户名之前宣告，必然全局可见；旧客户端不要求支持它，单因子不新增包交换。它是应验收的协议能力扩展，不是“所有握手完全不变”。
- [默认密码编码](/Users/cs/code/tidb/pkg/executor/utils.go:130)、[SET PASSWORD 分支](/Users/cs/code/tidb/pkg/executor/simple.go:2675)和原密码强度条件保持，不因新增 policy 统一；默认 policy 无 MFA 时退出新校验，显式非默认 policy 才产生新约束。
- [GRANT 隐式建用户](/Users/cs/code/tidb/pkg/executor/grant.go:252)及 [BR 不恢复 global_variables](/Users/cs/code/tidb/br/pkg/restore/snap_client/systable_restore.go:77)保持。文档相应收窄 policy 入口承诺，并要求恢复后核对目标策略，不借 MFA 改 GRANT 或 BR。
- Q14 已批准两项 LDAP 服务密码配置的内置日志脱敏；[运行值日志](/Users/cs/code/tidb/pkg/executor/set.go:268)仅替换输出副本，[安全文本](/Users/cs/code/tidb/pkg/parser/ast/misc.go:1229)仅克隆匹配的系统变量 assignment。普通 Restore、原 AST、用户变量、旧 URL 脱敏和真实回调值保持。

### 验证结论与限制

本轮完成文档与基线调用链复核，没有编写产品代码或运行不存在的 MFA 实现测试。已有运行报告继续作为基线；实现后的旧行为回归必须按主稿 §10 执行。没有依据可以保证新增代码零风险，因此把原权限时机、旧字段计数、JSON/TLS 隔离、普通 CHANGE_USER、extension 拒绝和迁移旧入口设为具体验收条件。

## Q16 专项复核：用户合同、系统表及实现参考

需求重新表述为：新增 MFA 沿用 MySQL 支持范围内的 SQL、协议及系统表数据合同；实现沿 MySQL 调用链核对，再适配 TiDB 事务和缓存。不能因函数名或用户示例相似就声称完整兼容；也不为本功能把 TiDB 既有系统库重构成 MySQL 物理结构。

### 源码与实测覆盖

本轮沿表定义→账号写入→JSON 序列化→缓存读取→SHOW/重载，以及因子验证→统计→准入→CHANGE_USER 清理两条调用链复核。基线 commit 保持不变。补充[元数据检查脚本](/tmp/tidb-mfa-mysql-d7973mh5/metadata_probe.py)、[28 项观测结果](/tmp/tidb-mfa-mysql-d7973mh5/metadata_results.json)及[1 项逆序后立即修改的补测](/tmp/tidb-mfa-mysql-d7973mh5/reverse_continuation_results.json)，使用隔离 MySQL datadir 和 mysql_no_login，仅验证存储/管理，不替代 Enterprise LDAP 登录。实例已停止，datadir 和临时凭据已清理。

| 复核点 | 源码/实测结果 | 当前处理 |
| --- | --- | --- |
| 第一因子与 MFA 承载列 | 两边已有 user.plugin/authentication_string/User_attributes；MySQL plugin 的 NOT NULL/默认值与 TiDB 不同 | 使用既有列及同名 MFA 子文档，不增加表/列；不称整表同构 |
| 四字段格式 | [serialize/deserialize](/Users/cs/code/mysql-server/sql/auth/sql_mfa.cc:731)写 plugin、authentication_string、整数 requires_registration/passwordless；数组下标决定因子位置 | 对齐四字段及位置，Go 内存标志不落表 |
| 缺省与未知字段 | 缺 flags 为 false，缺认证串为空，未知因子成员被忽略 | 撤回“未知成员一律拒绝”；写回仅产生四个已知字段 |
| 删除后空值 | 无其他属性时 SQL NULL；metadata:{} 仍保留对象，metadata/Password_locking 保留 | 明确键删除及 NULL 规则；保护 TiDB 已有其他属性 |
| 任意顶层属性 | MySQL 重写不保留未知顶层键；TiDB 原 JSON patch 保留未修改的键 | 保持 TiDB 既有 patch，避免删除 resource_group 等原数据；明确差异 |
| 非零标志/超长数组 | MySQL 接受整数非零标志及 3 个后续项，尽管 SQL 只支持 3 个总因子 | 按首版未支持功能/范围拒绝，不误称为 MySQL 格式不合法 |
| 坏配置与账号匹配 | MySQL FLUSH 成功但跳过坏账号；精确 Host 坏账号重载后，实测同名 % 单因子账号接替并登录成功 | 保留该账号的拒绝记录，防止降级，明确列为安全例外待确认 |
| 逆序双 MODIFY | 初次 `[new-3,new-2]`；不重载立即 MODIFY 2 得 `[immediate-2,new-3]`，先重载则得 `[after-reload-2,new-2]` | Q17 已确认按明确序号更新并按 2、3 保存；重载前后行为一致，记录为兼容例外 |
| CHANGE_USER 失败 | [MySQL 命令处理](/Users/cs/code/mysql-server/sql/sql_parse.cc:1952)恢复旧上下文仅为清理，随后 1873/28000 并断连 | 目标 MFA 认证失败采用断连语义，无 MFA 旧路径保持 |
| 失败锁定阶段 | [MySQL](/Users/cs/code/mysql-server/sql/auth/sql_authentication.cc:4039)因子链后立即统计，再检查 TLS/锁定/过期；TiDB 顺序不同且首因子准备在 Auth 外 | 明确复用 TiDB 既有生命周期，仅将后续因子纳入一次统计；删除完全对齐暗示 |
| 密码过期与 LDAP 错误 | MySQL 检查客户端过期能力，且可能保留插件错误；TiDB 原沙箱及 LDAP 1045 映射不同 | 继承 TiDB 行为，Enterprise LDAP 错误码逐项对照仍未验证 |

系统表拓扑参考 [MySQL user/global_grants](/Users/cs/code/mysql-server/scripts/mysql_system_tables.sql:75)、[TiDB user/global_priv](/Users/cs/code/tidb/pkg/session/bootstrap.go:74)、[global_variables](/Users/cs/code/tidb/pkg/session/bootstrap.go:195)、[global_grants](/Users/cs/code/tidb/pkg/session/bootstrap.go:385)。TLS 存储及变量持久化是既有 TiDB 差异，保留它们不会引入新 MFA 格式，但禁止把它们描述为 MySQL 相同布局。

AUTHENTICATION_POLICY_ADMIN 的授权表四列一致，初始化授权不一致：MySQL [升级脚本](/Users/cs/code/mysql-server/scripts/mysql_system_tables_fix.sql:816)可能给 SYSTEM_VARIABLES_ADMIN 补授，新库 [root 初始化](/Users/cs/code/mysql-server/sql/sql_initialize.cc:67)执行 GRANT ALL。TiDB 当前只初始化静态 root 权限；本稿保留显式 GRANT、不做自动补授，故无需新增 bootstrap 数据迁移。它属于须明示的集成边界，不能从同名表推导授权结果完全一致。

### 当前决策状态

正常 SQL/协议/四字段存储以固定 MySQL 基线核对；已确认的插件与 policy 子集保持。Q17 已确认逆序双 MODIFY 按明确因子编号规范保存；持久化、SHOW、登录及后续修改使用同一顺序。该选择复用原事务和缓存模型，不增加内存序号状态，MySQL 异常作为兼容差异记录。坏配置拒绝、既有 TiDB 统计/沙箱/变量/TLS 规则和不自动补授权限作为 v0.4 的具体边界提交整体确认。用户尚未选择扩大为整个账号系统的严格兼容改造，本次不提前做这种范围扩展。

## 必须修改的问题

以下保留先前设计审阅发现及修订原因；Q16/Q17/Q18 后的修订以 Q18 专项复核及 v0.4 正文为准，尚未单独确认的边界随整体技术稿确认。

### 已关闭：致命认证交换错误被失败计数覆盖（P1）

初稿只规定 transport 返回 AuthExchangeError、Run 识别关闭，没有覆盖中间 session.Auth 的返回路径。现有 [失败统计](/Users/cs/code/tidb/pkg/session/session.go:2976)可能返回计数事务错误；正常达到阈值时 [autolockAction](/Users/cs/code/tidb/pkg/session/session.go:3140)也会返回 account-blocked，覆盖原错误。真实后果是 change-user 在包序号已经失步时恢复旧 ctx，再把新错误作为普通 SQL 错误继续处理。

v0.2 改为目标 MFA 认证失败全部关闭，因此删除仅对传输错误分类的 AuthExchangeError。保留问题的根因约束：以精确账号预查和实际验证快照的 MultiFactor 标记，在 session.Auth 最终错误出口包装 MFAAuthError；handleChangeUser 清理、尽力写 1873、返回 EOF，Run 复用原退出路径。作用域不会被计数事务或自动锁定错误覆盖，Unwrap/Cause 同时保留类型识别及原 SQL 码。预查为单因子、实际快照已变 MFA 的错误也必须关闭；不新增跨请求状态。

### 已关闭：完整账号组合校验与坏配置处置边界（P2）

仅检查后续 JSON 无法排除“合法 LDAP 数组＋socket/token/LDAP/extension 第一插件”。现有 [isValidHash](/Users/cs/code/tidb/pkg/privilege/privileges/privileges.go:369)及认证分支会接受这些旧插件，不能依赖它们阻止超出首版范围的组合。

最终设计在完整行可用时校验组合，将不支持的组合发布为 AuthFactorsInvalid；JSON codec 仍只处理后续格式。这样不限制普通单因子，也不把登录责任塞入 codec。

同时收紧“管理报错”的范围：读取锁定行的共用 helper 返回原始 JSON，避免 [RENAME](/Users/cs/code/tidb/pkg/executor/simple.go:2247)因无关解析而失败；[DROP](/Users/cs/code/tidb/pkg/executor/simple.go:2433)继续删除用户行，有权限的纯 ACCOUNT LOCK 保持原配置落锁。只有依赖认证配置的 ALTER/SET PASSWORD/SHOW 拒绝坏状态。它保留既有处置通道，无需增加强制修复命令。

### 已关闭：policy 被简化为只查修改槽（P2）

MySQL 的 [check_for_authentication_policy](/Users/cs/code/mysql-server/sql/auth/sql_user.cc:1101)检查原始操作，而 [validate_against_authentication_policy](/Users/cs/code/mysql-server/sql/auth/sql_mfa.cc:318)还复核合并后的后续位置及缺失必选项。后者到 policy 末尾即停止，不等于全量最终配置检查。

技术稿和测试矩阵已改为两阶段规则，并写明旧三因子在 `'*,*'` 下只 MODIFY 2 可保留第三因子、DROP 3 在 `'*'` 下的前置拒绝。直接“只查修改槽”或“检查整个最终账号”都会造成不同的兼容偏差。

### 已关闭：迁移查询接口和错误文案承诺（P2）

原迁移查询曾按 v0.2 定义为 bool/error 并拒绝缺失账号。Q18 复核后收紧为 Manager.HasMultiFactorAuth(user, host) bool：有效/无效 MFA 为 true，其他含缺失账号为 false；源端只对 true 拒绝，目标端仍拒绝 MFA token。查询用于交换/失败范围，不当作认证证明。

“统一 1045 就能隐藏哪一个因子通过”的表述也已删除。MySQL 客户端收到下一次 0x02 本身即可知道前因子通过；最终承诺只限制错误文案中的凭据及因子细节，不承诺协议阶段不可观察。

## 需要验证的问题

### 实施验收：完整协议与真实 LDAP

当前只实测现有 TiDB LDAP 单因子和 MySQL 管理语义，没有实现后的 TiDB MFA 登录结果。实现必须通过 MySQL CLI 的 password1/2/3、同插件连续因子、两个本地首因子、错误/断包/重认证和真实 LDAP Bind 测试，才能证明可交付。负责阶段为技术稿第 10 节第 3–5 步。

本机 MySQL 缺少 Enterprise LDAP 服务端插件，因此 MySQL LDAP MFA 正向登录仍未实测。官方 LDAP 文档说明 BY 为 DN；公开框架展示 BY 回调、AS 存储及协议控制，但 Enterprise 回调实现不可见。当前依据足以设计复用 TiDB 的已验证 DN/Bind 语义，不能宣称完整 MySQL LDAP E2E 对照已经通过。

### 实施验收：并发、缓存和故障交叉

坏配置发布、完整组合校验、Host 优先级、JSON 属性合并、同时清除 Password_locking、提交后通知失败及跨节点传播需要在实现中验证。当前根据真实锁和缓存发布调用链确定方案，未运行不存在的实现测试。负责模块为 executor、privileges、domain 的对应测试及多节点验收。

### 实施验收：旧插件及客户端回归

已有 LDAP SASL、SM3、JWT 的完整外部认证及全部驱动尚未实测；不能以建账号成功代替登录结果。既有测试须回归，外部依赖不足的用例须在发布验收报告中明确列出并准备环境。v0.4 不再共用首因子准备器；仅 MFA 的 SHA-2 交换需验证，旧 native、extension 及普通失败恢复必须保持。

## 提交确认的设计权衡

- **存储与升级**：复用 User_attributes，避免新系统表和重复的第一因子存储；代价是旧节点忽略 MFA 键，必须先升级所有服务入口再启用。设计没有宣称旧节点会自动拒绝。
- **同一次 Auth**：保留 session 的一次收尾，后续循环放在 privileges；比每个因子调用 Auth 或新增认证会话管理器更直接，且避免中间身份生效。
- **可选 transport 接口**：旧 AuthConn 已进入 extension 契约。新增独立窄接口仅服务 MFA，不要求外部单因子插件实现新方法。
- **坏配置缓存**：发布带无效标志的账号行，而不是整次刷新报错或丢弃账号；这样同时避免旧单因子缓存续用和宽 Host 回退。它不尝试自动修复管理员写坏的数据。
- **最小旧路径变动**：MFA 相关 ALTER 保留未指定 TLS 及收紧自身管理权限；普通旧单因子维护问题不顺带统一修复。SHA-2 change-user 只在目标 MFA 分支补充；两处密码日志入口是 Q14 已批准的输出变化。
- **握手期限**：当前 connect_timeout 为 noop，设计明确保留，不引入新的超时配置或后台认证工作；LDAP 的超时及重试复用现有实现。
- **逆序 MODIFY（Q17 已确认）**：按语句开始时的明确因子编号更新并按 2、3 保存，保持 TiDB 原事务/缓存模型；接受与 MySQL 重载相关异常的结果差异。实现测试须覆盖两种输入顺序以及重载前后的后续修改。

## 审核依据

### 固定基线

TiDB `release-7.1.9-0`，`5c980fb645c8f3ae76f006967a4bc8b28ce8ba26`；用户已在 Q15 确认用户稿 v0.5。没有产品 diff，当前审查对象是 proposed 技术方案，不能把旧分支缺口当成应继续保留的正确行为，也不能借此全面修复所有旧认证问题。

MySQL `mysql-8.4.9`，`f9c88132a87a7d9e740e50bce2621999695bd3fe`；[8.0.27 第一插件解析](https://github.com/mysql/mysql-server/blob/mysql-8.0.27/sql/auth/sql_user.cc#L1461)确认具体第一 policy 插件决定省略 WITH 的行为，属于两版本共同基础语义。

### 数据与管理链路

- Parser [UserSpec/AuthOption](/Users/cs/code/tidb/pkg/parser/ast/misc.go:1495)、[CREATE/ALTER grammar](/Users/cs/code/tidb/pkg/parser/parser.y:14509)，以及共享 GRANT/ROLE 调用者。
- [CREATE 逐账号拼接](/Users/cs/code/tidb/pkg/executor/simple.go:1186)、[ALTER 事务与权限](/Users/cs/code/tidb/pkg/executor/simple.go:1818)、[用户行锁](/Users/cs/code/tidb/pkg/executor/simple.go:2573)、[SET PASSWORD](/Users/cs/code/tidb/pkg/executor/simple.go:2609)、[系统会话释放回滚](/Users/cs/code/tidb/pkg/executor/internal/exec/executor.go:398)。
- [缓存 JSON 读取](/Users/cs/code/tidb/pkg/privilege/privileges/cache.go:1201)、[Broken SSL 先例](/Users/cs/code/tidb/pkg/privilege/privileges/cache.go:1287)、[缓存发布](/Users/cs/code/tidb/pkg/privilege/privileges/cache.go:2653)。
- [SHOW CREATE USER](/Users/cs/code/tidb/pkg/executor/show.go:1839)、[AST SecurityString](/Users/cs/code/tidb/pkg/parser/ast/misc.go:1515)、[SET GLOBAL 值日志](/Users/cs/code/tidb/pkg/executor/set.go:268)。
- [动态权限 SUPER fallback](/Users/cs/code/tidb/pkg/privilege/privileges/cache.go:1803)与 MySQL [has_global_grant](/Users/cs/code/mysql-server/sql/auth/sql_security_ctx.cc:702)；据此选择显式动态授权查询。

### 协议与会话链路

- [握手](/Users/cs/code/tidb/pkg/server/conn.go:328) → [openSessionAndDoAuth](/Users/cs/code/tidb/pkg/server/conn.go:882) → [session.Auth](/Users/cs/code/tidb/pkg/session/session.go:2922) → [ConnectionVerification](/Users/cs/code/tidb/pkg/privilege/privileges/privileges.go:712) → [LDAP Simple](/Users/cs/code/tidb/pkg/privilege/privileges/ldap/simple.go:27)。
- [COM_CHANGE_USER](/Users/cs/code/tidb/pkg/server/conn.go:2687)、[Run 错误处理](/Users/cs/code/tidb/pkg/server/conn.go:1249)、[迁移导出](/Users/cs/code/tidb/pkg/session/session.go:4840)、[SessionToken 字段](/Users/cs/code/tidb/pkg/sessionctx/sessionstates/session_token.go:68)。
- MySQL [AuthNextFactor](/Users/cs/code/mysql-server/sql/auth/sql_authentication.cc:2136)、[客户端因子下标](/Users/cs/code/mysql-server/sql-common/client.cc:6064)、[clear password 回包](/Users/cs/code/mysql-server/sql-common/client.cc:9570)、[MFA 后统一锁定统计](/Users/cs/code/mysql-server/sql/auth/sql_authentication.cc:4055)。

### 新增 MySQL 管理实测

本轮新增 30 项管理/策略检查，使用临时 datadir、随机 loopback 端口及本机已有 `mysql_no_login`。该插件只用于观察框架管理、存储、策略行为，不作为 LDAP 登录的替代。结果位于[脚本](/tmp/tidb-mfa-mysql-d7973mh5/management_probe.py)及[脱敏 JSON](/tmp/tidb-mfa-mysql-d7973mh5/management_results.json)，服务和临时凭据已经清理。

关键结果包括：同插件三因子 JSON；DROP 2 前移；两种顺序双 DROP；单语句及多账号失败回滚；SUPER 与显式策略管理员区别；仅策略权限缺少账号权限仍拒绝；旧单因子改密、旧三因子未改第三位置的保留；RENAME/DROP 不走 policy；第一因子改密保留数组；非法 policy 拒绝。30 项预期错误码均匹配；其中逆序 MODIFY 的存储结果单独观察为以下异常，而非认为产品正确。

```sql
ALTER USER 'reverse_modify'@'127.0.0.1'
  MODIFY 3 FACTOR IDENTIFIED WITH mysql_no_login AS 'new-factor-3'
  MODIFY 2 FACTOR IDENTIFIED WITH mysql_no_login AS 'new-factor-2';
```

MySQL 实际按 `[new-factor-3, new-factor-2]` 保存，读取又按数组下标定义 2/3。原因是 [alter_mfa](/Users/cs/code/mysql-server/sql/auth/sql_mfa.cc:265)在两项都修改时没有进入补齐及排序分支，而 [serialize](/Users/cs/code/mysql-server/sql/auth/sql_mfa.cc:731)沿现有数组顺序写出。v0.1 曾提请确认按编号规范保存，但 Q16 未批准。v0.2 追加“不重载立即 MODIFY 2”的检查，发现输出为 `[immediate-2,new-3]`，不同于先重载后的 `[after-reload-2,new-2]`。结合 [ACL 内存复制](/Users/cs/code/mysql-server/sql/auth/sql_user.cc:1940)证明单纯复制逆序数组仍不等价；Q17 随后确认采用按因子编号稳定更新的兼容例外。

## 接口、职责与闭合结论

- **Parser**只表示语法与安全格式化；权限/策略不进入 parser，GRANT/ROLE 不意外接收 MFA 语法。
- **executor**拥有账号操作、权限、策略和原系统事务；依赖同事务锁定行，不通过权限缓存计算新持久化状态，不调用 LDAP。
- **privilege 数据层/缓存**拥有后续因子解码和只读账号快照；组合检查发生在完整行已知的位置，错误账号仍参与身份匹配。
- **privileges 认证层**拥有因子顺序及 LDAP 调用；不提交会话身份、不生成网络包、不启动异步认证任务。
- **server transport**拥有 0x02 包及原始 I/O 返回；不校验 LDAP DN/密码，不改变 cc.authPlugin 为最后因子。
- **session**保持单次成功/失败收尾，在最终出口保留 MFA 失败作用域；源端迁移查询是明确命名且只读的窄接口，内部无验证身份切换不受全局拦截。
- **变量与日志**纯 policy 语法解析留在 variable；插件检查放在已有 session 写入入口，避免 variable→extension 循环依赖。两处敏感日志只改副本及输出，不改真实设置值和外部回调。

新增接口均在技术稿第 8 节列出调用方及错误动作；新增数据字段有明确的创建、缓存发布、认证/展示消费、修改及删除生命周期。设计没有新增认证工作流管理器、重复密码库、集群策略协调器或通用日志框架。结构、时序图、错误表及测试矩阵已经按上述修订同步核对。

## 剩余不确定性

实现后的协议、并发和外部插件兼容性仍需实际验证；上述限制保留为发布门槛，不被本次设计审阅替代。用户技术确认尚待完成，本文不授权提前开发。
