<!--
This is a template for TiDB's change proposal process, documented [here](./README.md).
-->

# Proposal: Foreign Data Wrappers


- Author(s):  [iguory](https://github.com/iguoyr)，[wph95](https://github.com/wph95)，[lfkdsk](https://github.com/lfkdsk)
- Last updated: 2020-02-01
- Discussion at: https://github.com/pingcap/tidb/issues/18677

## Abstract

FDW（FOREIGN DATA WRAPPER）是一种让数据库可以访问远程数据存储的外部数据包装器。

本提案将遵循 [SQL/MED](https://wiki.postgresql.org/wiki/SQL/MED#Version_4)，通过对 Table Lifecycle 注入 Hook（复用 TiDB plan，在执行器 builder 时候使用自定义算子逻辑），使用 TiDB Plugin 等手段，使 TiDB 增加 FDW 功能。

## Background

TiDB 访问远程数据源是一个经久不衰的话题。不完全统计，两届 Hackathon 中，有各类远程数据源访问的项目。例如: CSV，Elasticsearch，Prometheus，Presto 等。但这类需求存在：与 TiDB 生态结合度不强，难以进行稳定官方维护，难以合并至 master 分支落地等问题。

本草案结合前人的项目经验，设计了一套 TiDB FDW 方案。同时有极高的落地性和可扩展性，吸引更多人来构建 TiDB 生态。

## Proposal

将 FDW 功能加入到 TiDB 中。严格循序 PostgreSQL FDW 与 [SQL/MED](https://wiki.postgresql.org/wiki/SQL/MED#Version_4)。

用户可以通过以下流程使用 FDW 扩展。

1. 遵守 plugin framework，创建 Engine Plugin。 
2. 实现 engine plugin 接口（具体参考 Implementation 部分 )
3. build plugin 
4. tidb 中加载相应 engine plugin

这里举使用 CSV FDW 作为例子，用户的使用流程为：

```sql
# 标准 FDW 方式创建 使用 csv engine 的table
CREATE EXTENSION csv_fdw;
CREATE SERVER test_logs FOREIGN DATA WRAPPER csv_fdw;
CREATE FOREIGN TABLE people (city int, name char(255)) SERVER test_logs
OPTIONS ( filename '<YOUR_CSV_FILE_PATH>');

# 使用 MySQL 兼容方案创建 table
CREATE TABLE people(city int, name char(255)) ENGINE = csv ENGINE_ATTRIBUTE = "path=<YOUR_CSV_FILE_PATH>"
  
# 支持 insert  
INSERT INTO people values(1, 'wph95');
INSERT INTO people values(3, 'iguory');
INSERT INTO people values(0, 'lfkdsk');

# 支持 select
SELECT * FROM people;
SELECT * FROM people where city = 2;

# 支持与 TiDB 普通 table: city 进行 join 
SELECT city.cityName, people.name FROM city INNER JOIN people ON city.id=people.city


# 使用 EXPLAIN 可看到逻辑有下推到 Engine ！
EXPLAIN SELECT city.cityName, people.name FROM city INNER JOIN people ON city.id=people.city
> ID                 |    task  |
  Project_0             root
  |_ HashRightJoin      root
    |- TableReader        root
      \- TableScan        cop[tikv]
  \_P 12                  root
     \_TableReader        root
        \_Selection       cop[csv]
          \_TableScan     cop[csv]

# 支持 custom string query，这里例如 TSDB
SELECT * FROM TiDB_Metric where q=`avg(rate(TiDB_Index_Size{table=*,app='*'}[15m]) `;


```

在第一阶段 ( Version 1 ) ，着重实现 FDW 的核心功能。包括:

- Create SERVER/ FOREIGN_TABLE

- Scan Foreign Tables

- Planning Post-Scan

  例如 `SELECT * FROM table WHERE city="shanghai"`， 将 `"city=shanghai"` 下推到远程数据端做过滤

- Insert Data to Foreign Table

- EXPLAIN 支持 foreign table 

## Rationale

Mysql 通过内置的方式实现了多个 Engine，尚未有很好的流程支持远程数据存储。MySQL 官方只支持使用 MySQL 协议的远程数据存储。具有很大的局限性。

PostgreSQL 制定 SQL/MED，实现了 FDW，通过 extension 的方式可以动态的开启相关 FDW 功能。

Prometheus 自身 store 支持 [remote read/write](https://prometheus.io/docs/prometheus/latest/storage/#remote-storage-integrations)，proms 生态存在大量的围绕该功能对 proms 进行拓展的第三方库。

TiDB 通过 Mysql 协议的兼容，实现了部分 engine API，但自身 Engine API 和自身生态 (TiKV/TiTan) 强绑定。无良好的插拔性。

## Implementation

###engine plugin

```
type EngineManifest struct {
 Manifest
 
 OnInsertOpen  func(ctx context.Context, meta *ExecutorMeta) error
 OnInsertNext  func(ctx context.Context, rows [][]expression.Expression, meta *ExecutorMeta) error
 OnInsertClose func(meta *ExecutorMeta) error

 OnReaderOpen  func(ctx context.Context, meta *ExecutorMeta) error
 OnReaderNext  func(ctx context.Context, chk *chunk.Chunk, meta *ExecutorMeta) error
 OnReaderClose func(meta ExecutorMeta)

 OnSelectReaderOpen func(ctx context.Context, filter []expression.Expression, meta *ExecutorMeta) error
 OnSelectReaderNext func(ctx context.Context, chk *chunk.Chunk, filter []expression.Expression, meta *ExecutorMeta) error
 OnSelectReaderClose func(meta ExecutorMeta)

 OnCreateServer     func(sv *model.ServerInfo) error
  OnDropServer     func(sv *model.ServerInfo) error

 OnCreateTable      func(sv *model.ServerInfo, tb *model.TableInfo) error
 OnDropTable        func(tb *model.TableInfo) error
}

```

### 算子

- `OnInsert` 用于对 Foreign Table 写入数据

- `OnReader` 用于 scan foreign table

- `OnDelete` 用于删除  foreign table 的数据

- `OnUpdate` 用于更新 foreign table 中的数据

- `OnSelect` 用于下推 scan 算子 

### DDL

- `OnCreateServer` 用于创建 Server, `CREATE SERVER {server_name}` 时被调用
- `OnDropServer` 用于删除 Server, `Drop SERVER {server_name}` 时被调用
- `OnCreateTable` 用于创建 table，`CREATE TABLE {table_name}`时被调用
- `OnDropTable` 用于删除 table，`DROP TABKE {table_name}`时被调用

## Compatibility and Limitations

虽然本提案的计划只对 TiDB 主分支代码进行一些 Hook 埋点。作为一种对 Mysql SQL 的扩展，对已有功能未有任何修改与破坏，不存在兼容性问题。

但因为以下原因，需要额外工作量：

- 部分库和 Plugin 存在循环引用，需要针对性调整。
- Mysql 的语法中并没有针对 `SQL/MED` 的设计，本提案遵循了 PostgreSQL 的语法与实现细节。同时将参考 MYSQL engine 的相关语法，针对性的进行 FDW 兼容。

因为使用到 TiDB plugin framework，若 TiDB source code tree 发生改变，engine plugin 需要重新编译。

## Open issues (if applicable)



