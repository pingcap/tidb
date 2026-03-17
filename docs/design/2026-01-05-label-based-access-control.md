# 基于标签的访问控制（LBAC）

- 状态：已实现。

## 概述

本文档描述 TiDB 当前代码中的 LBAC 实现。LBAC 在标准 SQL 权限之上，基于安全标签组件（component）、策略（policy）和标签（label）提供行级与列级授权。
本文以当前源码行为为准，属于“实现现状说明”，不代表未来版本承诺。

## 目标

- 基于标签支配关系（dominance）控制行可见性与可写性。
- 对声明 `SECURED WITH <label>` 的列执行列级访问控制。
- 提供组件、策略、标签、标签授权、豁免（exemption）的 DDL 与元数据存储。
- 提供标签编码/解码与支配判断函数。

## 非目标

- `LABELSECURITY_SCHEMA`（由 `ADMIN LBAC ENABLE` 创建）的旧版存储过程式标签安全机制。
- 数据脱敏、格（lattice）运算、超出允许/拒绝语义的策略变换。
- 完整的多表 `UPDATE/DELETE` 行级 LBAC 强制。
- 基于 `ROLE/GROUP` 的 LBAC 主体授权（当前语法仅支持 `USER`）。

## 全局开关与权限模型

### 全局开关

LBAC 由全局系统变量控制：

- `pkdb_lbac`（global，boolean，默认 `OFF`）

当 `pkdb_lbac=OFF` 时：

- 不会自动注入行过滤条件。
- 不会执行 `SECURED WITH` 列访问检查。
- DML 不会强制校验/规范化 `SECURITYLABEL` 写入值。
- `seclabel()` 与 `seclabel_to_char()` 透传其第二个参数。
- 会跳过 `SECURITY POLICY`、`SECURED WITH`、以及“单表最多一个 `SECURITYLABEL` 列”的 DDL 校验。
  - 但 LBAC 元数据语句（`CREATE/DROP SECURITY ...`、`GRANT/REVOKE ...`）仍会校验参数并更新 `mysql.*` 元数据。

当 `pkdb_lbac=ON` 时，上述 LBAC 行为生效。

### LBAC 语句所需 SQL 权限

LBAC DDL 与 GRANT/REVOKE 语句在建计划阶段检查权限：

- 执行以下语句需要具备 `CREATE USER` 或 `CREATE ROLE` 任一权限：
  - `CREATE/DROP SECURITY LABEL COMPONENT`
  - `CREATE/DROP SECURITY POLICY`
  - `CREATE/DROP SECURITY LABEL`
  - `GRANT/REVOKE SECURITY LABEL`
  - `GRANT/REVOKE EXEMPTION`

该检查在 planner 完成；executor 使用 internal/restricted SQL 更新 `mysql.*` 元数据表。

### 绕过规则

- 拥有 `SUPER` 权限会绕过 LBAC 访问检查（行过滤、列检查、DML 行标签访问检查）。
  - 但写入行标签时，标签存在性/编码有效性仍会校验。
- 对某策略授予豁免（`GRANT EXEMPTION ON RULE ALL ...`）会绕过该策略的 LBAC 访问检查。
  - 同样仍会校验标签存在性/编码有效性。

## 核心概念与对象模型

### 安全标签组件（component）

组件定义标签的一个维度。支持类型及支配语义：

- ARRAY：有序值，序号更小（定义更靠前）支配序号更大。
- SET：多值集合，超集支配子集。
- TREE：层级结构，父节点支配后代节点。

组件名/策略名/标签名大小写不敏感（内部按小写处理）；组件值大小写敏感。

### 安全策略（policy）

策略定义组件顺序与写控制行为。

- 组件顺序决定标签字符串格式，以及编码中的组件 ID 顺序（1..N）。
- `RESTRICT NOT AUTHORIZED WRITE SECURITY LABEL`（默认）：拒绝未授权写入标签。
- `OVERRIDE NOT AUTHORIZED WRITE SECURITY LABEL`：改写未授权标签。
- `WITH LBACRULES` 可解析，但不会落库，也不会参与执行。

### 安全标签（label）

标签是策略下组件到值的具体映射。

- ARRAY/TREE 组件必须恰好 1 个值。
- SET 组件可包含多个值，但不允许重复。
- 规范字符串格式（`seclabel()` 与 DML 规范化使用）：按策略组件顺序拼接 `component1|component2|...`。
  - SET 组件内部值会排序后以 `,` 连接（例如 `HR,OPS`）。

**关键区别：** LBAC 同时使用“标签名”和“标签字符串值”。

- 标签名：`CREATE SECURITY LABEL policy.label_name ...` 创建的对象名；用于 `GRANT SECURITY LABEL` 与 `SECURED WITH`。
- 标签字符串值：`v1|v2|...` 的规范值；用于 `seclabel(policy, label_string)` 编码行标签。

当前没有“按标签名直接编码”的内建函数；写行标签时需使用编码字节或规范标签字符串。

### 行标签列

行标签列类型为 `SECURITYLABEL`（底层为二进制 `LONGBLOB`，binary collation）。

- 仅当表同时具备 `SECURITY POLICY <policy>` 且存在 `SECURITYLABEL` 列时，才启用行级强制。
- `pkdb_lbac=ON` 时，`CREATE TABLE` 会校验单表最多一个 `SECURITYLABEL` 列。
- `ALTER TABLE ... ADD COLUMN ... SECURITYLABEL` 目前不会阻止新增多个 `SECURITYLABEL` 列（限制）；运行时使用 `TableInfo.Columns` 中找到的第一个。

### 列标签

列标签通过 `SECURED WITH <label>` 绑定，用于列级访问控制。

### 授权与豁免

- 标签授权按 `user@host` 存储，访问类型为 `READ`、`WRITE`、`ALL`。
- 豁免按 `user@host + policy` 存储，目前仅支持规则 `all`。
- 文中“可达/可访问”统一指：用户某授权标签对目标标签满足 `lbac_dominates(grant, target)=1`。

## DDL 与元数据

### 支持的 DDL

- `CREATE/DROP SECURITY LABEL COMPONENT`
- `CREATE/DROP SECURITY POLICY`
- `CREATE/DROP SECURITY LABEL`
- `GRANT/REVOKE SECURITY LABEL`
- `GRANT/REVOKE EXEMPTION ON RULE all`
- 表选项（`CREATE TABLE`）：`SECURITY POLICY <policy>`
- 列选项：`SECURED WITH <label>`
- 列类型：`SECURITYLABEL`

上述语句需要 `CREATE USER` 或 `CREATE ROLE` 权限（见上文）。

### DDL 校验规则

- 组件值必须非空且唯一。
- TREE 组件要求且仅允许 1 个根节点；所有父节点必须存在；不允许重复节点。
- 策略必须至少包含 1 个组件；组件不可重复；且必须都存在。
- 标签必须且仅能覆盖策略中的每个组件一次；缺失或额外组件都会报错。
- 标签值必须存在于对应组件定义中。
- 当 `pkdb_lbac=ON`：
  - `SECURED WITH <label>` 需要表上有 `SECURITY POLICY`，且标签必须属于该策略。
  - `CREATE TABLE` 拒绝多个 `SECURITYLABEL` 列。
- `GRANT SECURITY LABEL`：
  - 要求目标用户存在。
  - 同一 `user@host + label` 的访问类型会合并（READ/WRITE 合并为 ALL）。
- `REVOKE SECURITY LABEL`：
  - 要求目标用户存在。
  - 不校验标签是否存在，按条件删除授权行（不存在时无影响）。
- `GRANT/REVOKE EXEMPTION`：
  - 要求策略存在且目标用户存在。
  - `GRANT` 遇到重复会报错；`REVOKE` 按条件删除（不存在时无影响）。
- 删除保护：
  - `DROP SECURITY LABEL COMPONENT` 若被任一策略引用则拒绝。
  - `DROP SECURITY POLICY` 若被任一标签、任一表（`SECURITY POLICY`）、或任一用户豁免引用则拒绝。
  - `DROP SECURITY LABEL` 若被任一用户标签授权或任一列 `SECURED WITH` 引用则拒绝。

### 元数据表

LBAC 元数据存储在 `mysql` 库（启动/升级时创建）：

- `mysql.security_label_components`：
  - `name`（PK），`type`（ARRAY/SET/TREE），`component_values`（JSON）
- `mysql.security_policies`：
  - `name`（PK），`component_names`（JSON 数组），`write_control`（RESTRICT/OVERRIDE）
- `mysql.security_labels`：
  - `name`（PK），`policy_name`，`components`（JSON map）
- `mysql.user_security_labels`：
  - `user_name`，`host`，`label_name`，`access_types`（READ/WRITE/ALL）
- `mysql.user_exemptions`：
  - `user_name`，`host`，`policy_name`，`rule`

由于 `mysql.security_labels.name` 是主键，标签名在全局唯一（不区分策略）。

元数据会进入权限缓存；DDL/授权变更会通过 `NotifyUpdatePrivilege` 触发缓存更新。

## 访问控制语义

### 读路径（SELECT）

行过滤（计划阶段）：

- 仅在 `pkdb_lbac=ON` 且表具有 `SECURITY POLICY` 与 `SECURITYLABEL` 列时生效。
- `SUPER` 用户与策略豁免用户跳过行过滤。
- 构建 `UPDATE/DELETE` 语句时会跳过自动行过滤（其内部 SELECT 不自动附加 LBAC 行过滤）。
- 谓词形态为：对用户 READ/ALL 授权标签做 `lbac_dominates(grant_label, row_label)` 的 OR。
  - 构造谓词前会先做授权标签压缩（被其他授权标签支配的标签会被去除）。
- 若用户对该策略无 READ 授权，则谓词退化为常量 false（返回 0 行）。

列访问（计划阶段）：

- 任何被引用且声明 `SECURED WITH <label>` 的列，都要求对该标签具备 READ 可达性（按支配关系，而非名称精确匹配）。
- 检查覆盖 SELECT 列表、WHERE、ORDER BY 等列引用位置。
- 列检查与行过滤彼此独立（即使行过滤结果为空，引用受保护列仍可报错）。
- 失败报 `ER_COLUMNACCESS_DENIED_ERROR`（1143），`command=READ`。
  - prepared plan cache 会在每次执行时重做这类列级 LBAC 检查。

### 写路径（INSERT/UPDATE/DELETE）

DML Guard 初始化：

- 仅当 `pkdb_lbac=ON` 且目标表同时具备 `SECURITY POLICY` 与 `SECURITYLABEL` 列时启用。

列访问：

- `INSERT/UPDATE`（含 `ON DUPLICATE KEY UPDATE`）写入 `SECURED WITH` 列时需要 WRITE 可达性。
- 失败报 `ER_COLUMNACCESS_DENIED_ERROR`（1143），`command=WRITE`。

行标签检查：

- 行标签值会做规范化（接受编码字节或规范标签字符串），并要求在策略下存在。
- `RESTRICT`（默认）：未授权标签写入报 `ErrRowLabelUnAccessible`。
- `OVERRIDE`：未授权标签会被替换为“首选写标签”：
  - 若某个授权标签支配其余所有写标签，则选该标签。
  - 否则选择规范标签字符串字典序最小者。
- `UPDATE/DELETE` 会检查“旧行标签”是否 WRITE 可达；`NULL` 标签视为不可达。
- `INSERT`/`UPDATE` 的“新值标签”若为 `NULL`，LBAC Guard 不做可达性校验（后续是否允许取决于列本身 `NULL/NOT NULL` 约束）。

多表 DML：

- `DELETE`：多表删除路径不执行 LBAC 行标签检查。
- `UPDATE`：LBAC 行标签检查仅覆盖单目标表场景，多表 `UPDATE` 目前未完全覆盖。

### 与 SQL 权限的关系

LBAC 是附加在 SQL 权限之上的检查层。标签授权不会替代表/列权限。

## 函数与表达式

- `seclabel(policy, label_string)`：
  - LBAC ON：对已存在标签字符串返回编码字节。
  - LBAC OFF：直接返回输入字符串（第二参数）。
  - 执行时需要可用的 LBAC cache，否则返回 eval context 错误。
- `seclabel_to_char(policy, label_bytes)`：
  - LBAC ON：对编码字节返回规范标签字符串。
  - LBAC OFF：直接返回输入（第二参数）。
- `lbac_dominates(grant_label, row_label)`：
  - 若 `grant_label` 支配 `row_label` 返回 1，否则返回 0。
  - 两个参数都必须是编码标签字节。
  - 与 `pkdb_lbac` 开关无直接联动（函数本身不做 OFF 透传）。

执行说明：

- `seclabel()` 与 `seclabel_to_char()` 依赖权限上下文属性，不会被常量折叠为纯字面值。
- `seclabel()` / `seclabel_to_char()` 只做编码/解码与存在性校验，不检查当前用户标签授权。
  - 用户访问控制发生在读写表数据阶段（行过滤、列检查、DML 行标签强制）。
- `seclabel`、`seclabel_to_char`、`lbac_dominates` 不下推到存储层，在 TiDB 侧计算。

## 当前行为与限制

- `WITH LBACRULES` 仅语法支持，不生效。
- `GRANT/REVOKE SECURITY LABEL` 与 `GRANT/REVOKE EXEMPTION` 语法当前仅支持 `TO/FROM USER ...`。
- 策略不强制要求存在 `SECURITYLABEL` 列；缺失时仅行级强制失效（列级 `SECURED WITH` 检查仍可生效）。
- 多表 `UPDATE/DELETE` 的 LBAC 行级强制尚不完整。
- 标签名在全局唯一（跨策略）。
- 豁免规则仅支持 `all`。
- `pkdb_lbac=OFF` 时，可写入原始/无效字节到 `SECURITYLABEL` 列；重新开启后可能在查询时触发解码/支配错误。
- `pkdb_lbac=OFF` 时，`SECURITY POLICY` / `SECURED WITH` 的对象存在性校验会跳过；后续开启可能在建计划或访问时报错。
- 开启 `pkdb_lbac=ON` 会全局禁用 point-get fast plan（`TryFastPlan` 路径被跳过）；旧开关 `tidb_enable_label_security=ON` 也有同样效果。
- `pkdb_lbac=ON` 时，`DELETE` 的列裁剪优化会被全局关闭（不仅限于 LBAC 表）。
- prepared plan cache 会在执行时重做列级 LBAC 检查，但行过滤条件在建计划时已固化；授权变更后可能需要重建计划。
- `DROP SECURITY LABEL` 不检查业务表中是否仍存该标签编码值；删除后历史行标签可能变为“未知标签”。
- `SHOW CREATE TABLE` 会输出 `SECURITY POLICY` 与 `SECURED WITH`。
- `SHOW GRANTS` 的 LBAC 输出依赖权限缓存中的 LBAC 元数据；在 `pkdb_lbac` 开关切换或 OFF 期间变更元数据后，可能需要刷新权限缓存（例如 `FLUSH PRIVILEGES`）才能看到最新结果。

### 风险与后续改进

#### 风险提示

- `pkdb_lbac=ON` 时当前实现会全局禁用 point-get fast plan，并全局关闭 `DELETE` 的列裁剪优化；影响范围不仅限于 LBAC 表，可能带来整体性能退化风险。
- prepared plan cache：列级 `SECURED WITH` 检查在执行期会复检，但行过滤条件在建计划时已固化；当授权发生变化时，可能出现可见性与最新授权不一致，需要依赖重建计划/失效计划来收敛风险。

#### 后续改进点

- 收敛全局优化退化范围：仅对需要 LBAC 行过滤的表/语句禁用 fast path 与列裁剪，或补齐 fast path 的 LBAC 行过滤逻辑。
- 针对含 LBAC 行过滤的 prepared statements：考虑禁用 plan cache、在执行期重算 grant labels 并重建过滤条件，或在权限/授权变更时使相关计划缓存失效。
- `lbac_dominates` 的下推计算。
