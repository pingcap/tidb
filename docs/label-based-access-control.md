# 基于标签的访问控制（LBAC）

本文档说明 TiDB 中已经实现的 LBAC 行为，内容基于当前代码路径与测试用例。
本文聚焦“现有实现”，用于解释当前行为与边界，不代表未来版本承诺。

## 全局开关与权限

- `pkdb_lbac`（global，boolean，默认 OFF）控制 LBAC 是否强制生效。
- 当 `pkdb_lbac=OFF` 时，TiDB 会跳过 LBAC 行过滤、`SECURED WITH` 列检查、DML 行标签强制；`seclabel`/`seclabel_to_char` 直接返回输入值。
  - `SECURITY POLICY`、`SECURED WITH`、以及“单表最多一个 `SECURITYLABEL` 列”的 DDL 校验会跳过。
  - LBAC 元数据语句（`CREATE/DROP SECURITY ...`、`GRANT/REVOKE ...`）仍会继续校验并更新 `mysql.*` 元数据。
- 管理 LBAC 对象（component/policy/label 及其 GRANT/REVOKE）需要 `CREATE USER` 或 `CREATE ROLE` 权限。
- `SUPER` 权限或策略豁免（exemption）可绕过该策略的 LBAC 访问检查。
- LBAC 在 SQL 权限之上额外生效；标签授权不替代表权限/列权限。

示例：

```sql
SET GLOBAL pkdb_lbac = ON;
CREATE DATABASE IF NOT EXISTS lbac_doc;
USE lbac_doc;
```

说明：`tidb_enable_label_security` 是旧版“存储过程式标签安全”开关（由 `ADMIN LBAC ENABLE` 启用），与本文 component/policy/label 模型不同。

## 核心概念

### 组件与支配关系

组件是标签的构建单元。组件名大小写不敏感；组件值大小写敏感且必须非空。

- ARRAY：有序值，序号更小（定义更靠前）支配序号更大。
- SET：超集支配子集。
- TREE：父节点支配后代节点。

### 策略与标签字符串格式

策略定义组件顺序；组件顺序决定标签字符串格式。

规范标签字符串：

```
component1|component2|...|componentN
```

SET 组件内部会按字典序排序，再用逗号拼接，例如 `FIN,HR`。

### 标签名 vs 标签字符串（重点）

LBAC 中有两个容易混淆的概念：

- **标签名（label name）**：`CREATE SECURITY LABEL policy.label_name ...` 创建的对象名。
  - 用于 `SECURED WITH <label_name>`、`GRANT/REVOKE SECURITY LABEL policy.label_name ...`。
- **标签字符串（label string）**：按策略顺序拼接的规范值 `v1|v2|...`。
  - 用于 `seclabel(policy, label_string)`，也是 `SECURITYLABEL` 列里逻辑上对应的标签内容。

补充：

- 当前没有“按标签名编码”的内建函数，必须传标签字符串。
- SET 组件标签字符串应使用排序后的 `,` 连接值。
- `seclabel()` 只做“存在性校验 + 编码”，不做当前用户授权判断。
  - 授权判断在读写表数据时发生（行过滤、列检查、DML 行标签检查）。

### 行标签列与受保护列

- 行标签列类型是 `SECURITYLABEL`（二进制 `LONGBLOB`，binary collation）。
- 行级强制仅在下列条件同时满足时生效：
  - `pkdb_lbac=ON`；
  - 表声明 `SECURITY POLICY <policy>`；
  - 表中存在 `SECURITYLABEL` 列。
- `pkdb_lbac=ON` 时，`CREATE TABLE` 会拒绝同表多个 `SECURITYLABEL` 列。
  - `ALTER TABLE ... ADD COLUMN ... SECURITYLABEL` 当前不会阻止新增多个该类型列（限制）；运行时仅使用找到的第一个。
- 列标签由 `SECURED WITH <label>` 定义。
  - `pkdb_lbac=ON` 时，DDL 会校验该表策略存在，且 `SECURED WITH` 标签属于该策略（`CREATE TABLE` / `ALTER TABLE ADD COLUMN` / `ALTER TABLE MODIFY COLUMN`）。

### 授权与豁免

- 标签授权按 `user@host` 记录，访问类型为 `READ`、`WRITE`、`ALL`。
- 豁免按策略记录；目前仅支持规则 `ALL`。
- 本文“可达性”统一表示：授权标签对目标标签满足支配关系（`lbac_dominates(grant, target)=1`）。

由于 `mysql.security_labels.name` 是主键，标签名在全局唯一。

## SQL 语法与示例

以下示例使用上文创建的 `lbac_doc` 库。

### CREATE SECURITY LABEL COMPONENT

语法：

```
CREATE SECURITY LABEL COMPONENT <name> ARRAY ('v1','v2',...);
CREATE SECURITY LABEL COMPONENT <name> SET ('v1','v2',...);
CREATE SECURITY LABEL COMPONENT <name> TREE ('root' ROOT, 'child' UNDER 'root', ...);
```

示例：

```sql
CREATE SECURITY LABEL COMPONENT classif ARRAY ('TOP','CONF','PUB');
CREATE SECURITY LABEL COMPONENT dept SET ('FIN','HR','ENG');
CREATE SECURITY LABEL COMPONENT region TREE ('WORLD' ROOT, 'AMER' UNDER 'WORLD', 'US' UNDER 'AMER');
```

### CREATE SECURITY POLICY

语法：

```
CREATE SECURITY POLICY <policy> COMPONENTS comp1, comp2, ...;
CREATE SECURITY POLICY <policy> COMPONENTS comp1, comp2, ...
  OVERRIDE NOT AUTHORIZED WRITE SECURITY LABEL;
CREATE SECURITY POLICY <policy> COMPONENTS comp1, comp2, ...
  RESTRICT NOT AUTHORIZED WRITE SECURITY LABEL;
```

`WITH LBACRULES` 可被语法接受，但不会落库，也不会参与执行。

示例：

```sql
CREATE SECURITY POLICY corp_policy COMPONENTS classif, dept, region;
CREATE SECURITY POLICY corp_policy_override COMPONENTS classif, dept, region
  OVERRIDE NOT AUTHORIZED WRITE SECURITY LABEL;
```

### CREATE SECURITY LABEL

语法：

```
CREATE SECURITY LABEL <policy>.<label>
  COMPONENT comp 'v1'[,'v2',...]
  COMPONENT comp2 'v1' ...;
```

每个策略组件必须且仅出现一次。ARRAY/TREE 组件必须恰好 1 个值；SET 组件可多值但不可重复。

示例：

```sql
CREATE SECURITY LABEL corp_policy.top_fin_us
  COMPONENT classif 'TOP'
  COMPONENT dept 'FIN'
  COMPONENT region 'US';

CREATE SECURITY LABEL corp_policy.conf_hr_us
  COMPONENT classif 'CONF'
  COMPONENT dept 'HR'
  COMPONENT region 'US';

CREATE SECURITY LABEL corp_policy.top_fin_hr_amer
  COMPONENT classif 'TOP'
  COMPONENT dept 'FIN','HR'
  COMPONENT region 'AMER';

CREATE SECURITY LABEL corp_policy.col_top_fin_us
  COMPONENT classif 'TOP'
  COMPONENT dept 'FIN'
  COMPONENT region 'US';

CREATE SECURITY LABEL corp_policy_override.ov_top_fin_us
  COMPONENT classif 'TOP'
  COMPONENT dept 'FIN'
  COMPONENT region 'US';

CREATE SECURITY LABEL corp_policy_override.ov_top_fin_hr_amer
  COMPONENT classif 'TOP'
  COMPONENT dept 'FIN','HR'
  COMPONENT region 'AMER';

CREATE SECURITY LABEL corp_policy_override.ov_conf_hr_us
  COMPONENT classif 'CONF'
  COMPONENT dept 'HR'
  COMPONENT region 'US';
```

### 表策略、SECURITYLABEL 与 SECURED WITH

语法：

```
CREATE TABLE <table> (
  ...,
  row_label SECURITYLABEL,
  col INT SECURED WITH <label>,
  ...
) SECURITY POLICY <policy>;
```

示例：

```sql
CREATE TABLE sales (
  id INT PRIMARY KEY,
  item VARCHAR(16),
  amount INT SECURED WITH col_top_fin_us,
  row_seclabel SECURITYLABEL NOT NULL
) SECURITY POLICY corp_policy;

CREATE TABLE sales_override (
  id INT PRIMARY KEY,
  row_seclabel SECURITYLABEL NOT NULL
) SECURITY POLICY corp_policy_override;
```

### GRANT/REVOKE SECURITY LABEL

语法：

```
GRANT SECURITY LABEL <policy>.<label>
  TO USER 'u'@'h'[, USER 'u2'@'h2', ...]
  FOR READ ACCESS[, WRITE ACCESS];
-- 或：FOR ALL ACCESS

REVOKE SECURITY LABEL <policy>.<label>
  FROM USER 'u'@'h'[, USER 'u2'@'h2', ...];
```

示例：

```sql
CREATE USER 'alice'@'%';
GRANT SELECT ON lbac_doc.sales TO 'alice'@'%';
GRANT SECURITY LABEL corp_policy.conf_hr_us TO USER 'alice'@'%' FOR READ ACCESS;

CREATE USER 'label_user'@'%';
GRANT SECURITY LABEL corp_policy.top_fin_us TO USER 'label_user'@'%' FOR READ ACCESS, WRITE ACCESS;
REVOKE SECURITY LABEL corp_policy.top_fin_us FROM USER 'label_user'@'%';
```

说明：

- 重复 `GRANT SECURITY LABEL` 会合并访问类型（READ+WRITE => ALL）。
- `REVOKE SECURITY LABEL` 按标签整行删除授权，不是按 access type 细粒度回收。
- 当前语法只支持 `USER` 主体，不支持 `ROLE`/`GROUP`。

### GRANT/REVOKE EXEMPTION

语法：

```
GRANT EXEMPTION ON RULE ALL FOR <policy> TO USER 'u'@'h';
REVOKE EXEMPTION ON RULE ALL FOR <policy> FROM USER 'u'@'h';
```

示例：

```sql
CREATE USER 'exempt_user'@'%';
GRANT SELECT ON lbac_doc.sales TO 'exempt_user'@'%';
GRANT EXEMPTION ON RULE ALL FOR corp_policy TO USER 'exempt_user'@'%';
REVOKE EXEMPTION ON RULE ALL FOR corp_policy FROM USER 'exempt_user'@'%';
```

### DROP 语句

语法：

```
DROP SECURITY LABEL <policy>.<label>;
DROP SECURITY POLICY <policy>;
DROP SECURITY LABEL COMPONENT <component>;
```

示例：

```sql
CREATE SECURITY LABEL COMPONENT tmp_comp ARRAY ('A');
CREATE SECURITY POLICY tmp_policy COMPONENTS tmp_comp;
CREATE SECURITY LABEL tmp_policy.tmp_label COMPONENT tmp_comp 'A';
DROP SECURITY LABEL tmp_policy.tmp_label;
DROP SECURITY POLICY tmp_policy;
DROP SECURITY LABEL COMPONENT tmp_comp;
```

说明：

- `DROP SECURITY LABEL COMPONENT`：若仍被策略引用会拒绝。
- `DROP SECURITY POLICY`：若仍被标签、表（`SECURITY POLICY`）或豁免引用会拒绝。
- `DROP SECURITY LABEL`：若仍被用户授权或列 `SECURED WITH` 引用会拒绝。
- 删除不会扫描业务表数据；删除后历史行标签可能变为 LBAC 无法识别的“未知标签”。

### SHOW CREATE TABLE / SHOW GRANTS

- `SHOW CREATE TABLE` 会输出表级 `SECURITY POLICY` 以及列级 `SECURED WITH <label>`。
- `SHOW GRANTS` 会输出 `GRANT SECURITY LABEL ...` 与 `GRANT EXEMPTION ...`。
  - 若同一标签同时具备 READ+WRITE，则落库后是 `ALL`，展示为 `FOR ALL ACCESS`。

## 内建函数

### seclabel 与 seclabel_to_char

- `seclabel(policy, label_string)`：把已存在标签字符串编码成字节。
- `seclabel_to_char(policy, label_bytes)`：把编码字节解码为规范标签字符串。
- 这两个函数只做编码/解码与存在性校验，不检查当前用户的标签授权。
- 当 `pkdb_lbac=OFF` 时，这两个函数都透传第二个参数。

示例：

```sql
SELECT seclabel_to_char('CORP_POLICY', seclabel('CORP_POLICY','TOP|FIN|US')) AS label;
```

预期结果：

```
TOP|FIN|US
```

### lbac_dominates

- `lbac_dominates(grant_label_bytes, row_label_bytes)`：若 grant 标签支配 row 标签则返回 1，否则返回 0。
- 两个参数都必须是“编码标签字节”。
- 该函数本身不受 `pkdb_lbac` 开关透传逻辑影响。
  - 因此在 `pkdb_lbac=OFF` 时，如果直接传入普通字符串而非编码字节，仍可能报解码/类型错误。

示例：

```sql
SELECT lbac_dominates(
  seclabel('CORP_POLICY','TOP|FIN,HR|AMER'),
  seclabel('CORP_POLICY','CONF|HR|US')
) AS dominates;
```

预期结果：

```
1
```

### label_accessible（旧版存储过程式标签安全）

`label_accessible(row_label, user_label)` 比较冒号分隔标签串 `level:compartment:group`。
第 1 段是数字（数值更大支配更小）；其余段是逗号分隔集合，要求行标签集合被用户标签集合包含。

示例：

```sql
SELECT label_accessible('10:M:R20', '30:M,E:R20,R40') AS accessible;
```

预期结果：

```
1
```

## DML 行为

LBAC 有两层强制：

- 行级强制（SELECT 行过滤 + INSERT/UPDATE/DELETE 行标签检查）生效条件：
  - `pkdb_lbac=ON`；
  - 表声明 `SECURITY POLICY <policy>`；
  - 表含 `SECURITYLABEL` 列。
- 列级强制（`SECURED WITH`）生效条件：
  - `pkdb_lbac=ON`；
  - 表声明 `SECURITY POLICY <policy>`；
  - 被引用/写入的列声明 `SECURED WITH <label>`。

### SELECT

- 系统会注入基于 `lbac_dominates` 的行过滤条件（按当前用户 READ 授权）。
- 若用户对该策略没有 READ 授权，过滤条件会退化为常量 false（返回空集）。
- 引用 `SECURED WITH` 列需要 READ 可达性。
- 列检查与行过滤独立（即使行过滤为空，引用受保护列仍可能报错）。
- `SUPER` 或该策略豁免可绕过行/列 LBAC 检查。

### INSERT

- 行标签若非 `NULL`，必须能在策略下被识别（支持编码字节或规范字符串）。
- `RESTRICT`（默认）：未授权标签写入会报行标签不可达错误。
- `OVERRIDE`：未授权标签写入会被替换为用户可写的“首选标签”。
- 写 `SECURED WITH` 列需要 WRITE 可达性。
- 行标签写入值若为 `NULL`，LBAC guard 本身不拒绝（列约束仍可拒绝）。

### UPDATE

- 旧行标签必须对当前用户 WRITE 可达，否则报错。
- 更新 `SECURED WITH` 列需要 WRITE 可达性。
- 更新行标签时，沿用与 INSERT 相同的 RESTRICT/OVERRIDE 规则。

### DELETE

- 旧行标签必须 WRITE 可达，否则报错。
- `NULL` 行标签视为不可达。

多表 UPDATE/DELETE 的 LBAC 行级强制尚未完整覆盖；当前主要覆盖单表 DML。

示例（基于前文对象）：

```sql
INSERT INTO sales VALUES
  (1, 'Widget', 100, seclabel('CORP_POLICY','TOP|FIN|US')),
  (2, 'Gizmo', 200, seclabel('CORP_POLICY','CONF|HR|US'));
```

```sql
-- As alice:
SELECT id, item FROM sales ORDER BY id;
-- Result:
-- 2 Gizmo
SELECT amount FROM sales;
-- ERROR 1143 (42000): READ command denied to user 'alice'@'%' for column 'amount' in table 'sales'
```

```sql
-- As exempt_user (no labels):
SELECT amount FROM sales ORDER BY id;
-- ERROR 1143 (42000): READ command denied to user 'exempt_user'@'%' for column 'amount' in table 'sales'

-- As root:
GRANT EXEMPTION ON RULE ALL FOR corp_policy TO USER 'exempt_user'@'%';

-- As exempt_user:
SELECT amount FROM sales ORDER BY id;
-- Result:
-- 100
-- 200

-- As root:
REVOKE EXEMPTION ON RULE ALL FOR corp_policy FROM USER 'exempt_user'@'%';
```

```sql
-- As root:
CREATE USER 'writer'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON lbac_doc.sales TO 'writer'@'%';
GRANT SELECT, INSERT ON lbac_doc.sales_override TO 'writer'@'%';

-- As writer (no secured-column grant yet):
INSERT INTO sales (id, item, amount, row_seclabel)
VALUES (3, 'Gadget', 300, seclabel('CORP_POLICY','TOP|FIN|US'));
-- ERROR 1143 (42000): WRITE command denied to user 'writer'@'%' for column 'amount' in table 'sales'

-- As root:
GRANT SECURITY LABEL corp_policy.col_top_fin_us TO USER 'writer'@'%' FOR WRITE ACCESS;

-- As writer:
INSERT INTO sales (id, item, amount, row_seclabel)
VALUES (3, 'Gadget', 300, seclabel('CORP_POLICY','TOP|FIN|US'));
INSERT INTO sales (id, item, amount, row_seclabel)
VALUES (4, 'Widget', 400, seclabel('CORP_POLICY','CONF|HR|US'));
-- ERROR 8800 (HY000): This row has a unaccessible label, row label [CONF|HR|US], user label [corp_policy]

UPDATE sales SET item = 'Widget2' WHERE id = 1;
UPDATE sales SET item = 'Denied' WHERE id = 2;
-- ERROR 8800 (HY000): This row has a unaccessible label, row label [CONF|HR|US], user label [corp_policy]
DELETE FROM sales WHERE id = 2;
-- ERROR 8800 (HY000): This row has a unaccessible label, row label [CONF|HR|US], user label [corp_policy]
```

```sql
-- As root:
GRANT SECURITY LABEL corp_policy_override.ov_top_fin_us TO USER 'writer'@'%' FOR READ ACCESS, WRITE ACCESS;

-- As writer (OVERRIDE policy):
INSERT INTO sales_override VALUES
  (1, seclabel('CORP_POLICY_OVERRIDE','CONF|HR|US'));
SELECT seclabel_to_char('CORP_POLICY_OVERRIDE', row_seclabel)
FROM sales_override WHERE id = 1;
-- Result:
-- TOP|FIN|US
```

## 限制与运维说明

- `UPDATE/DELETE` 不会像 `SELECT` 一样自动注入 LBAC 行过滤；匹配到不可写行时会在执行阶段报错。
- 多表 `DELETE` 路径跳过 LBAC 行标签检查；多表 `UPDATE` 的 LBAC 行级强制尚未完整覆盖。
- `WITH LBACRULES` 仅语法支持，不生效。
- `GRANT/REVOKE SECURITY LABEL` 与 `GRANT/REVOKE EXEMPTION` 当前仅支持 `USER` 主体。
- 开启 `pkdb_lbac=ON` 会全局禁用 point-get fast plan（`TryFastPlan` fast path）。
- 开启 `pkdb_lbac=ON` 时，`DELETE` 的列裁剪优化会被全局关闭（不仅是 LBAC 表）。
- prepared plan cache 行为：
  - 列级 LBAC 访问会在每次 execute 时重检；
  - 行过滤条件是在建计划时按当时授权固化，授权变更后可能需要 re-prepare/replan。
- `pkdb_lbac=OFF` 时，可创建/写入“未来会在 ON 时失败”的对象或值：
  - `SECURITY POLICY`/`SECURED WITH` 可指向缺失策略/标签；
  - `SECURITYLABEL` 列可写入原始/无效字节。
- `DROP SECURITY LABEL` 不扫描业务表历史数据，可能留下无法解码/匹配的行标签值。
- `SHOW GRANTS` 的 LBAC 条目依赖权限缓存中的 LBAC 元数据；若在 OFF 阶段改过 LBAC 元数据，切换 ON 后可能需要刷新权限缓存（例如 `FLUSH PRIVILEGES`）才能看到最新授权展示。

## 旧版存储过程式标签安全（ADMIN LBAC ENABLE）

`ADMIN LBAC ENABLE` 会创建 `LABELSECURITY_SCHEMA` 与一组存储过程（如 `create_policy`、`create_level`、`set_user_labels`、`apply_table_policy` 等）。
该机制操作 `mysql.tidb_ls_*` 表，使用 `label_accessible` 做行过滤，与本文 component/policy/label 模型相互独立。

示例：

```sql
SET GLOBAL tidb_enable_procedure = ON;
ADMIN LBAC ENABLE;
```

## 综合示例（array + set + tree + exemption）

下面场景对应 `TestLBACCompanyScenario`，并包含 TREE 组件与豁免授予/回收。

```sql
SET GLOBAL pkdb_lbac = ON;
CREATE DATABASE IF NOT EXISTS lbac_company_demo;
USE lbac_company_demo;
DROP TABLE IF EXISTS salary;
DROP USER IF EXISTS 'boss'@'%';
DROP USER IF EXISTS 'hr'@'%';
DROP USER IF EXISTS 'emp_b'@'%';
DROP USER IF EXISTS 'guest'@'%';

CREATE SECURITY LABEL COMPONENT classif ARRAY ('TOP','CONF','PUB');
CREATE SECURITY LABEL COMPONENT dept SET ('EXEC','HR','FIN','ENG');
CREATE SECURITY LABEL COMPONENT region TREE ('WORLD' ROOT, 'AMER' UNDER 'WORLD', 'US' UNDER 'AMER', 'EMEA' UNDER 'WORLD');

CREATE SECURITY POLICY corp_access COMPONENTS classif, dept, region;

CREATE SECURITY LABEL corp_access.exec_top_world
  COMPONENT classif 'TOP' COMPONENT dept 'EXEC' COMPONENT region 'WORLD';
CREATE SECURITY LABEL corp_access.hr_conf_us
  COMPONENT classif 'CONF' COMPONENT dept 'HR' COMPONENT region 'US';
CREATE SECURITY LABEL corp_access.fin_conf_amer
  COMPONENT classif 'CONF' COMPONENT dept 'FIN' COMPONENT region 'AMER';
CREATE SECURITY LABEL corp_access.eng_pub_us
  COMPONENT classif 'PUB' COMPONENT dept 'ENG' COMPONENT region 'US';
CREATE SECURITY LABEL corp_access.hr_non_exec
  COMPONENT classif 'CONF' COMPONENT dept 'ENG','HR' COMPONENT region 'AMER';
CREATE SECURITY LABEL corp_access.salary_top_exec
  COMPONENT classif 'TOP' COMPONENT dept 'EXEC' COMPONENT region 'WORLD';

CREATE TABLE salary (
  id INT PRIMARY KEY,
  name VARCHAR(16),
  dept VARCHAR(8),
  region VARCHAR(8),
  salary INT SECURED WITH salary_top_exec,
  row_seclabel SECURITYLABEL NOT NULL
) SECURITY POLICY corp_access;

INSERT INTO salary VALUES
  (1, 'boss', 'EXEC', 'WORLD', 100000, seclabel('CORP_ACCESS','TOP|EXEC|WORLD')),
  (2, 'hr', 'HR', 'US', 8000, seclabel('CORP_ACCESS','CONF|HR|US')),
  (3, 'fin', 'FIN', 'AMER', 9000, seclabel('CORP_ACCESS','CONF|FIN|AMER')),
  (4, 'emp_b', 'ENG', 'US', 5000, seclabel('CORP_ACCESS','PUB|ENG|US'));

CREATE USER 'boss'@'%';
CREATE USER 'hr'@'%';
CREATE USER 'emp_b'@'%';
CREATE USER 'guest'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON lbac_company_demo.salary TO 'boss'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON lbac_company_demo.salary TO 'hr'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON lbac_company_demo.salary TO 'emp_b'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON lbac_company_demo.salary TO 'guest'@'%';

GRANT EXEMPTION ON RULE ALL FOR corp_access TO USER 'boss'@'%';
GRANT SECURITY LABEL corp_access.hr_non_exec TO USER 'hr'@'%' FOR READ ACCESS, WRITE ACCESS;
GRANT SECURITY LABEL corp_access.eng_pub_us TO USER 'emp_b'@'%' FOR READ ACCESS, WRITE ACCESS;
```

```sql
-- As boss (exempt):
SELECT id, salary FROM salary ORDER BY id;
-- Result:
-- 1 100000
-- 2 8000
-- 3 9000
-- 4 5000
INSERT INTO salary VALUES
  (5, 'boss2', 'EXEC', 'WORLD', 110000, seclabel('CORP_ACCESS','TOP|EXEC|WORLD'));
```

```sql
-- As hr:
SELECT id, name FROM salary ORDER BY id;
-- Result:
-- 2 hr
-- 4 emp_b
SELECT salary FROM salary;
-- ERROR 1143 (42000): READ command denied to user 'hr'@'%' for column 'salary' in table 'salary'
INSERT INTO salary VALUES
  (6, 'eng2', 'ENG', 'US', 5200, seclabel('CORP_ACCESS','PUB|ENG|US'));
-- ERROR 1143 (42000): WRITE command denied to user 'hr'@'%' for column 'salary' in table 'salary'
```

```sql
-- As emp_b:
UPDATE salary SET name = 'emp_b2' WHERE id = 4;
UPDATE salary SET name = 'hacked' WHERE id = 2;
-- ERROR 8800 (HY000): This row has a unaccessible label, row label [CONF|HR|US], user label [corp_access]
DELETE FROM salary WHERE id = 2;
-- ERROR 8800 (HY000): This row has a unaccessible label, row label [CONF|HR|US], user label [corp_access]
```

```sql
-- As guest (no labels):
SELECT id FROM salary ORDER BY id;
-- Result: empty
```

```sql
-- As root:
REVOKE EXEMPTION ON RULE ALL FOR corp_access FROM USER 'boss'@'%';

-- As boss:
SELECT id FROM salary ORDER BY id;
-- Result: empty
SELECT salary FROM salary;
-- ERROR 1143 (42000): READ command denied to user 'boss'@'%' for column 'salary' in table 'salary'
```
