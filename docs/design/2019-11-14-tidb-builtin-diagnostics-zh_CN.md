# RFC: TiDB 内置 SQL 诊断

## Summary

目前 TiDB 获取诊断信息主要依赖外部工具（perf/iosnoop/iotop/vmstat/sar/...）、监控系统（Prometheus/Grafana）、日志文件、HTTP API 和 TiDB 提供的系统表。分散的工具链和繁杂的获取方式导致 TiDB 的集群的使用门槛高、运维难度大、不能提前发现问题以及遇到问题不能及时排查、诊断和恢复集群等。

本提案提出一种新的方法，在 TiDB 中内置获取诊断信息的功能，并将诊断信息使用系统表的形式对外暴露，使用户可以使用 SQL 的方式进行查询。

## Motivation

本提案主要解决 TiDB 在获取诊断信息过程中的以下问题：

- 工具链分散，需要在不同工具之间来回切换，且部分 Linux 发行版未内置相应工具或内置工具的版本不一致。
- 信息获取方式不一致，比如有 SQL、HTTP、导出监控、登录各个节点查看日志等。
- TiDB 集群组件较多，对不同组件的监控进行对比和关联低效且繁琐。
- TiDB 没有集中日志管理组件，没有高效的手段对整个集群的日志进行过滤、检索、分析、聚合。
- 系统表只包含当前节点信息，不能体现整个集群的状态，如：SLOW_QUERY, PROCESSLIST, STATEMENTS_SUMMARY。

在通过提供多维度集群级别系统表和集群诊断规则框架之后，提高全集群信息查询、状态获取、日志检索、一键巡检、故障诊断几个使用场景中的效率，并为后续异常预警功能提供基础数据。

## Detailed Design

### 系统整体概览

本提案的实现分为四层：

- L1: 最底层在各个节点实现信息采集模块，包括 TiDB/TiKV/PD 监控信息、硬件信息、内核中记录的网络 IO、磁盘 IO 信息、CPU 使用率、内存使用率等。
- L2: 第二层通过调用底层信息采集模块并通过对外服务接口(HTTP API/gRPC Service)向上层提供数据，使 TiDB 可以获取当前节点采集到的信息。
- L3: 第三层由 TiDB 拉取各个节点的信息进行聚合和汇总，并以系统表的形式对上层提供数据。
- L4: 第四层实现诊断框架，诊断框架通过查询系统表获取整个集群的状态，并根据诊断规则得到诊断结果。

如下从信息采集到使用使用诊断规则对采集的信息进行分析的数据流向图:

```
+-L1--------------+             +-L3-----+
| +-------------+ |             |        |
| |   Metrics   | |             |        |
| +-------------+ |             |        |
| +-------------+ |             |        |
| |   Disk IO   | +---L2:gRPC-->+        |
| +-------------+ |             |        |
| +-------------+ |             |  TiDB  |
| |  Network IO | |             |        |
| +-------------+ |             |        |
| +-------------+ |             |        |
| |   Hardware  | +---L2:HTTP-->+        |
| +-------------+ |             |        |
| +-------------+ |             |        |
| | System Info | |             |        |
| +-------------+ |             |        |
+-----------------+             +---+----+
                                    | 
                   +---infoschema---+ 
                   |                  
                   v                  
+-L4---------------+---------------------+
|                                        |
|          Diagnosis Framework           |
|                                        |
| +---------+ +---------+  +---------+   |
| | rule1   | |  rule2  |  |  rule3  |   |
| +---------+ +---------+  +---------+   |
+----------------------------------------+
```

### 系统信息收集

TiDB/TiKV/PD 三个组件都需要实现系统信息采集模块，其中 TiDB/PD 使用 Golang 实现并复用逻辑，TiKV 需要使用 Rust 单独实现。

#### 节点硬件信息

各个节点需要获取的硬件信息包括：

- CPU 信息：物理核心数、逻辑核心数量、NUMA 信息、CPU 频率、CPU 供应商、L1/L2/L3 缓存大小
- 网卡信息：网卡设备名、网卡是否启用、生产厂商、型号、带宽、驱动版本、接口队列数（可选）
- 磁盘信息：磁盘名、磁盘容量、磁盘使用量、磁盘分区、挂载信息
- USB 设备列表
- 内存信息

#### 节点系统信息

各个节点需要获取的系统信息包括：

- CPU 使用率、1/5/15 分钟负载
- 内存：Total/Free/Available/Buffers/Cached/Active/Inactive/Swap
- 磁盘 IO：
    - tps: 该设备每秒的传输次数
    - rrqm/s: 每秒这个设备相关的读取请求有多少被 Merge
    - wrqm/s: 每秒这个设备相关的写入请求有多少被 Merge
    - r/s: 每秒从设备读取的数据量
    - w/s: 每秒从设备写入的数据量
    - rsec/s: 每秒读取的扇区数
    - wsec/s: 每秒写取的扇区数
    - avgrq-sz: 平均请求扇区的大小
    - avgqu-sz: 是平均请求队列的长度
    - await: 每一个IO请求的处理的平均时间（单位是微秒毫秒）
    - svctm: 表示平均每次设备I/O操作的服务时间（以毫秒为单位）
    - %util: 在统计时间内所有处理IO时间，除以总共统计时间
- 网络 IO
    - IFACE：LAN接口
    - rxpck/s：每秒钟接收的数据包
    - txpck/s：每秒钟发送的数据包
    - rxbyt/s：每秒钟接收的字节数
    - txbyt/s：每秒钟发送的字节数
    - rxcmp/s：每秒钟接收的压缩数据包
    - txcmp/s：每秒钟发送的压缩数据包
    - rxmcst/s：每秒钟接收的多播数据包
- 常用的系统配置：sysctl -a

#### 节点配置信息

所有节点都包含当前节点的生效配置，不需要额外的步骤既可拿到配置信息。

#### 节点日志信息

TiDB/TiKV/PD 产生的日志都保存在各自的节点上，并且 TiDB 集群部署过程中没有部署额外的日志收集组件，所以在日志检索中有以下问题：

- 日志分布在各个节点，需要单独登陆到每一个节点使用关键字进行搜索
- 日志文件会每天 rotate，所以在单个节点也需要对多个日志文件进行搜索
- 没有简单的方式对多个节点的日志按照时间排序整合到同一个文件

本提案提供以下两种思路来解决以上问题：

- 引入第三方日志收集组件对所有节点的日志进行收集
    - 优势：统一的日志管理，日志可以长时间保存，并易于检索，并且多个组件的日志可以按照时间排序归并
    - 劣势：增加集群运维难度，第三方组件不容易与 TiDB 内部 SQL 集成；日志收集工具会收集全量日志，收集过程占用各个系统资源（磁盘 IO、网络 IO）
- 各个节点提供日志服务，TiDB 通过各个节点的接口将谓词下推到日志检索接口，直接对各个节点返回的日志进行归并
    - 优势：不引入三方组件，谓词下推后只返回过滤后的日志，能轻易的与 TiDB SQL 进行集成，并能复用 SQL 引擎的过滤、聚合等
    - 劣势：如果节点日志删除后，不能检索到对应日志

根据以上的优劣势分析，本提案使用第二种方案，即各个节点提供日志搜索接口，TiDB 将日志搜索的 SQL 中谓词下推到各个节点，日志搜索接口的语义为：搜索本地日志文件，并使用谓词进行过滤，匹配的结果返回。

- `start_time`: 日志检索的开始时间（unix 时间戳，单位毫秒），如果没有该谓词，则默认为 0。
- `end_time`: 日志检索的开始时间（unix 时间戳，单位毫秒），如果没有该谓词，则默认为 `int64::MAX`。
- `pattern`: 如 SELECT * FROM cluster_log WHERE pattern LIKE "%gc%" 中的 %gc% 即为过滤的关键字
- `level`: 日志等级，可以选为 DEBUG/INFO/WARN/WARNING/TRACE/CRITICAL/ERROR
- `limit`: 返回日志的条数，如果没有指定，则限制为 64k 条，防止日质量太大占用大量网络

#### 节点性能采样数据

当前 TiDB 集群中，发现有性能瓶颈时，需要快速定位问题。火焰图 （Flame Graph）是由 Brendan Gregg 发明的，与其他的 trace 和 profiling 方法不同的是，Flame Graph 以一个全局的视野来看待时间分布，它从底部往顶部，列出所有可能的调用栈。其他的呈现方法，一般只能列出单一的调用栈或者非层次化的时间分布。

目前 TiKV 和 TiDB 获取火焰图的方式不同，并且都需要依赖外部工具。

- TiKV 获取火焰图

    ```
    perf record -F 99 -p proc_pid -g -- sleep 60
    perf script > out.perf
    /opt/FlameGraph/stackcollapse-perf.pl out.perf > out.folded
    /opt/FlameGraph/flamegraph.pl out.folded > cpu.svg
    ```

- TiDB 获取火焰图

    ```
    curl http://127.0.0.1:10080/debug/pprof/profile > cpu.pprof
    go tool pprof -svg cpu.svn cpu.pprof
    ```

目前存在的两个主要问题：

- 生产环境中不一定包含对应的外部工具（perf/flamegraph.pl/go）
- TiKV 和 TiDB 没有统一的方式

为了解决以上两个问题，本提案将获取火焰图的方法内置到 TiDB 中，统一使用 SQL 触发采样并将采样数据转换为火焰图作为查询结果显示，一方面降低对外部工具的依赖，同时也极大的提升效率。各个节点实现采样数据采集功能并提供采样接口，对上层输出指定格式的采样数据。暂定输出为 `[pprof](github.com/google/pprof)` 定义的 ProtoBuf 格式。

采样数据获取方式：

- TiDB/PD: 使用 Golang Runtime 内置的采样数据获取接口
- TiKV: 使用 `[pprof-rs](github.com/tikv/pprof-rs)` 库采集采样数据

#### 节点监控信息

监控信息主要是各个组件内部定义的监控指标。目前 TiDB/TiKV/PD 都会提供 `/metrics` HTTP API，然后通过部署的 Prometheus 组件定时（默认配置 15s）的拉取集群各个节点的监控指标。并且部署了 Grafana 组件用于从 Prometheus 拉取监控数据，进行可视化展示。

监控信息不同于实时获取的系统信息，监控数据是一个时序数据。包含各个节点在各个时间点的数据，对于排查问题和诊断问题有非常重要的用途，所以监控信息的保存和查询对于本提案实现 TiDB 内置 SQL 诊断非常重要。为了能够在 TiDB 内使用 SQL 查询监控数据，目前有以下备选方案：

- 使用 Prometheus client 和 PromQL 查询 Prometheus server 的数据
    - 优势：有现成解决方案，只需要将 Prometheus server 的地址注册到 TiDB 即可，实现简单
    - 劣势：增强了 TiDB 对 Prometheus 的依赖，为后续完全移除 Prometheus 增加了困难
- 将最近一段时间内（暂定 1 天）的监控数据保存到 PD，从 PD 中查询监控数据
    - 优势：该方案不依赖 Prometheus server，为后续移除 Prometheus 组件有一定帮助
    - 劣势：需要实现时序保存逻辑，并实现对应的查询引擎，实现难度和工作量大

本提案倾向于方案二，虽然实现难度更大，但是对后续的工作有帮助。为了解决实现 PromQL 和时序数据保存的实现难度大和周期长的问题，将这个功能分为三个阶段实现（第三阶段视具体情况是否实现）：

1. PD 中添加 `remote-metrics-storage` 配置，暂时配置为 Prometheus Server 的地址。PD 作为 proxy，将请求转移到 Prometheus 上执行，主要有以下考量：
    - 后续 PD 实现查询接口实现自举，TiDB 不需要做其他改动
    - 用户不使用 TiDB 部署的 Prometheus 而使用自建的监控服务，依然可以使用 SQL 查询监控信息以及诊断框架
2. 将 Prometheus 时序数据保存和查询相应的模块抽离出来，并嵌入到 PD 中
3. PD 内部实现自己的时序保存与查询（目前 CockroachDB 的方案）

##### PD 性能分析

PD 目前主要承载 TiDB 集群的调度和 TSO 服务，其中：

1. TSO 获取仅对 Leader 内存中的一个原子变量进行累加
2. 调度生成的 Operator 和 OperatorStep 仅保存在内存中，根据 Region 的心跳信息更新内存中的状态

由以上信息可以得出在 PD 上新增监控功能对 PD 的性能影响在绝大部分情况下可以忽略不计。

### 系统信息获取

由于 TiDB/TiKV/PD 组件之前已经可以通过 HTTP API 对外暴露部分系统信息，并且 PD 主要通过 HTTP API 对外提供服务，所以本提案的部分接口会复用已有逻辑，使用 HTTP API 从各个组件获取数据，比如配置信息获取。

由于 TiKV 后续计划完全移除 HTTP API，所以除了已有接口复用之外，不再额外添加新的 HTTP API，所有日志检索、硬件信息、系统信息获取统一定义 gRPC Service，各个组件实现对应的 Service 并在启动过程中注册到 gRPC Server 中。

#### gRPC Service 定义

```proto
// Diagnostics service for TiDB cluster components.
service Diagnostics {
	// Searchs log in the target node
	rpc search_log(SearchLogRequest) returns (SearchLogResponse) {};
	// Retrieves server info in the target node
	rpc server_info(ServerInfoRequest) returns (ServerInfoResponse) {};
}

enum LogLevel {
	Debug = 0;
	Info = 1;
	Warn = 2;
	Trace = 3;
	Critical = 4;
	Error = 5;
}

message SearchLogRequest {
	int64 start_time = 1;
	int64 end_time = 2;
	LogLevel level = 3;
	string pattern = 4;
	int64 limit = 5;
}

message SearchLogResponse {
	repeated LogMessage messages = 1;
}

message LogMessage {
	int64 time = 1;
	LogLevel level = 2;
	string message = 3;
}

enum ServerInfoType {
	All = 0;
	HardwareInfo = 1;
	SystemInfo = 2;
	LoadInfo = 3;
}

message ServerInfoRequest {
	ServerInfoType tp = 1;
}

message ServerInfoItem {
	// cpu, memory, disk, network ...
	string tp = 1;
	// eg. network: lo1/eth0, cpu: core1/core2, disk: sda1/sda2 
	string name = 2;
	string key = 3;
	string value = 4;
}

message ServerInfoResponse {
	repeated ServerInfoItem items = 1;
}
```

#### 可复用的 HTTP API

目前 TiDB/TiKV/PD 包含部分可复用 HTTP API，本提案暂不将对应接口迁移至 gRPC Service，迁移工作由后续其他计划完成。所有 HTTP API 需要以 JSON 格式返回数据，以下是提案中可能用到的 HTTP API 列表：

- 获取配置信息
    - PD: /pd/api/v1/config
    - TiDB/TiKV: /config
- 性能采样接口: TiDB/PD 包含以下所有接口，TiKV 暂时只包含 CPU 性能采样接口
    - CPU: /debug/pprof/profile
    - Memory: /debug/pprof/heap
    - Allocs: /debug/pprof/allocs
    - Mutex: /debug/pprof/mutex
    - Block: /debug/pprof/block

### 集群信息系统表

每个 TiDB 实例均可以通过前两层提供的 HTTP API 或 gRPC Service 访问其他节点的信息，从而实现集群的 Global View。本提案中通过新建一系列相关系统表将采集到的集群信息向上层提供数据，上层包括不限于：

- 终端用户：用户直接通过 SQL 查询获取集群信息排查问题
- 运维系统：TiDB 的使用环境比较多样，客户可以通过 SQL 获取集群信息将 TiDB 集成到自己的运维系统中
- 生态工具：外部工具通过 SQL 拿到集群信息实现功能定制，比如 `[sqltop](https://github.com/ngaut/sqltop)` 可以直接通过集群 `statements_summary` 获取整个集群的 SQL 采样信息

#### 集群拓扑系统表

要为 TiDB 实例提供一个 **Global View**，首先需要为 TiDB 实例提供一个拓扑系统表，可以从拓扑系统表中获取各个节点的 HTTP API Address 和 gRPC Service Address，从而方便的构造出各个远程 API 的 Endpoint，进一步获取目标节点采集的信息。

本提案实现完成可以通过 SQL 查询以下结果：

```
mysql> use information_schema;
Database changed

mysql> desc CLUSTER_INFO;
+----------------+---------------------+------+------+---------+-------+
| Field          | Type                | Null | Key  | Default | Extra |
+----------------+---------------------+------+------+---------+-------+
| TYPE           | varchar(64)         | YES  |      | NULL    |       |
| ADDRESS        | varchar(64)         | YES  |      | NULL    |       |
| STATUS_ADDRESS | varchar(64)         | YES  |      | NULL    |       |
| VERSION        | varchar(64)         | YES  |      | NULL    |       |
| GIT_HASH       | varchar(64)         | YES  |      | NULL    |       |
+----------------+---------------------+------+------+---------+-------+
5 rows in set (0.00 sec)

mysql> select TYPE, ADDRESS, STATUS_ADDRESS,VERSION from CLUSTER_INFO;
+------+-----------------+-----------------+-----------------------------------------------+
| TYPE | ADDRESS         | STATUS_ADDRESS  | VERSION                                       |
+------+-----------------+-----------------+-----------------------------------------------+
| tidb | 127.0.0.1:4000  | 127.0.0.1:10080 | 5.7.25-TiDB-v4.0.0-alpha-793-g79eef48a3-dirty |
| pd   | 127.0.0.1:2379  | 127.0.0.1:2379  | 4.0.0-alpha                                   |
| tikv | 127.0.0.1:20160 | 127.0.0.1:20180 | 4.0.0-alpha                                   |
+------+-----------------+-----------------+-----------------------------------------------+
3 rows in set (0.00 sec)
```

#### 监控信息系统表

由于监控指标会随着程序的迭代添加和删除监控指标，对于同一个监控指标，可能有不同的表达式获取监控不同维度的信息。鉴于以上两个需求，需要设计一个有弹性的监控系统表框架，本提案暂时才采取以下方案：将表达式映射为 `metrics_schema` 数据库中的系统表，表达式与系统表的关系可以通过以下方式关联：

- 定义在配置文件

    ```
    # tidb.toml
    [metrics_schema]
    qps = `sum(rate(tidb_server_query_total[$STEP])) by (result)`
    memory_usage = `process_resident_memory_bytes{job="tidb"}`
    goroutines = `rate(go_gc_duration_seconds_sum{job="tidb"}[$STEP])`
    ```

- HTTP API 注入

    ```
    curl -XPOST http://host:port/metrics_schema?name=distsql_duration&expr=`histogram_quantile(0.999, 
    sum(rate(tidb_distsql_handle_query_duration_seconds_bucket[$STEP])) by (le, type))`
    ```

- 特殊 SQL 命令

    ```
    mysql> admin metrics_schema add parse_duration `histogram_quantile(0.95, sum(rate(tidb_session_parse_duration_seconds_bucket[$STEP])) by (le, sql_type))`
    ```

- 从文件中加载

    ```
    mysql> admin metrics_schema load external_metrics.txt
    #external_metrics.txt
    execution_duration = `histogram_quantile(0.95, sum(rate(tidb_session_execute_duration_seconds_bucket[$STEP])) by (le, sql_type))`
    pd_client_cmd_ops = `sum(rate(pd_client_cmd_handle_cmds_duration_seconds_count{type!="tso"}[$STEP])) by (type)`
    ```

添加以上表之后就可以在 `metrics_schema` 库中查看对应的表：

```
mysql> use metrics_schema;
Database changed

mysql> show tables;
+-------------------------------------+
| Tables_in_metrics_schema            |
+-------------------------------------+
| qps                                 |
| memory_usage                        |
| goroutines                          |
| distsql_duration                    |
| parse_duration                      |
| execution_duration                  |
| pd_client_cmd_ops                   |
+-------------------------------------+
7 rows in set (0.00 sec)
```

表达式映射到系统表时字段的确定方式主要取决与表达式执行结果的数据。以表达式 `sum(rate(pd_client_cmd_handle_cmds_duration_seconds_count{type!="tso"}[1m]offset 0)) by (type)` 为例，查询的结果为：


| Element | Value |
|---------|-------|
| {type="update_gc_safe_point"} | 0 |
| {type="wait"} | 2.910521666666667 |
| {type="get_all_stores"} | 0 |
| {type="get_prev_region"} | 0 |
| {type="get_region"} | 0 |
| {type="get_region_byid"} | 0 |
| {type="scan_regions"} | 0 |
| {type="tso_async_wait"} | 2.910521666666667 |
| {type="get_operator"} | 0 |
| {type="get_store"} | 0 |
| {type="scatter_region"} | 0 |

映射为表结构以及查询结果为：

```
mysql> desc pd_client_cmd_ops;
+------------+-------------+------+-----+-------------------+-------+
| Field      | Type        | Null | Key | Default           | Extra |
+------------+-------------+------+-----+-------------------+-------+
| address    | varchar(32) | YES  |     | NULL              |       |
| type       | varchar(32) | YES  |     | NULL              |       |
| value      | float       | YES  |     | NULL              |       |
| interval   | int         | YES  |     | 60                |       |
| start_time | int         | YES  |     | CURRENT_TIMESTAMP |       |
| end_time   | int         | YES  |     |                   |       |
| end_time   | int         | YES  |     |                   |       |
| step       | int         | YES  |     |                   |       |
+------------+-------------+------+-----+-------------------+-------+
3 rows in set (0.02 sec)

mysql> select address, type, value from pd_client_cmd_ops;
+------------------+----------------------+---------+
| address          | type                 | value   |
+------------------+----------------------+---------+
| 172.16.5.33:2379 | update_gc_safe_point |       0 |
| 172.16.5.33:2379 | wait                 | 2.91052 |
| 172.16.5.33:2379 | get_all_stores       |       0 |
| 172.16.5.33:2379 | get_prev_region      |       0 |
| 172.16.5.33:2379 | get_region           |       0 |
| 172.16.5.33:2379 | get_region_byid      |       0 |
| 172.16.5.33:2379 | scan_regions         |       0 |
| 172.16.5.33:2379 | tso_async_wait       | 2.91052 |
| 172.16.5.33:2379 | get_operator         |       0 |
| 172.16.5.33:2379 | get_store            |       0 |
| 172.16.5.33:2379 | scatter_region       |       0 |
+------------------+----------------------+---------+
11 rows in set (0.00 sec)

mysql> select address, type, value from pd_client_cmd_ops where start_time='2019-11-14 10:00:00' and end_time='2019-11-14 10:05:00';
+------------------+----------------------+---------+
| address          | type                 | value   |
+------------------+----------------------+---------+
| 172.16.5.33:2379 | update_gc_safe_point |       0 |
| 172.16.5.33:2379 | wait                 | 0.82052 |
| 172.16.5.33:2379 | get_all_stores       |       0 |
| 172.16.5.33:2379 | get_prev_region      |       0 |
| 172.16.5.33:2379 | get_region           |       0 |
| 172.16.5.33:2379 | get_region_byid      |       0 |
| 172.16.5.33:2379 | scan_regions         |       0 |
| 172.16.5.33:2379 | tso_async_wait       | 0.82052 |
| 172.16.5.33:2379 | get_operator         |       0 |
| 172.16.5.33:2379 | get_store            |       0 |
| 172.16.5.33:2379 | scatter_region       |       0 |
+------------------+----------------------+---------+
11 rows in set (0.00 sec)
```

对于多个 label 的 PromQL 就会有多个列的数据，可以方便的使用已有的 SQL 执行引擎对数据过滤、聚合得到期望的结果。

#### 节点性能剖析系统表

通过各个节点的 `/debug/pprof/profile` 拿到对应节点性能采样数据，然后对采样数据进行聚合，最终使用 SQL 查询结果的方式向用户输出性能剖析结果。由于 SQL 查询结果不能以 svg 的格式输出，所以需要解决输出内容展示的问题。

火焰图快速定位问题的核心点是：

- 提供全局视野
- 展示全部调用路径
- 层次化展示

本提案提出的解决方案聚焦在解决核心问题的点上，而未拘泥于是图形展示形式。最终的方案为：对采样数据进行聚合，并将所有的调用路径使用树形结构逐行进行展示。

解决方案是通过以下方式契合三个核心点：

- 提供全局视野：对每一个聚合结果使用单独的一列展示在全局的使用比例，可以方便过滤排序
- 展示全部调用路径：将所有的调用路径都作为查询结果，并使用单独的列对各个调用路径的子树进行编号，可以方便的通过过滤只查看某一个子树
- 层次化展示：使用树形结构展示堆栈，使用单独的列记录栈的深度，可以方便的对不同栈的深度进行过滤

本提案需要实现以下性能剖析表：


| 表名 | 描述 |
|------|-----|
| tidb_profile_cpu | TiDB CPU 火焰图 |
| tikv_profile_cpu | TiKV CPU 火焰图 |
| tidb_profile_block | TiDB 阻塞情况火焰图 |
| tidb_profile_memory | TiDB 内存对象火焰图 |
| tidb_profile_allocs | 内存分配火焰图 |
| tidb_profile_mutex | 锁的争用情况火焰图 |
| tidb_profile_goroutines | 系统中已有的 goroutines，排查 goroutine 泄漏、阻塞 |

#### 内存表全局化

目前 `slow_query`/`statements_summary`/`processlist` 只包含单节点数据，本提案通过添加以下三张集群级别系统表使任何一个 TiDB 实例可以查看整个集群的信息：

| 表名 | 描述 |
|------|-----|
| cluster_slow_query | 所有 TiDB 节点的 slow_query 表数据 |
| cluster_statements_summary | 所有 TiDB 节点的 statements summary 表数据 |
| cluster_processlist | 所有 TiDB 节点的 processlist 表数据 |

#### 所有节点的配置信息

对于一个大集群，通过 HTTP API 去每一个节点获取配置的方式较为繁琐和低效，本提案提供全集群配置信息系统表，简化整个集群配置信息的获取、过滤、聚合。

如下示例是实现本提案后的预期结果：

```
mysql> use information_schema;
Database changed

mysql> select * from cluster_config where `key` like 'log%';
+------+-----------------+-----------------------------+---------------+
| TYPE | ADDRESS         | KEY                         | VALUE         |
+------+-----------------+-----------------------------+---------------+
| pd   | 127.0.0.1:2379  | log-file                    |               |
| pd   | 127.0.0.1:2379  | log-level                   |               |
| pd   | 127.0.0.1:2379  | log.development             | false         |
| pd   | 127.0.0.1:2379  | log.disable-caller          | false         |
| pd   | 127.0.0.1:2379  | log.disable-error-verbose   | true          |
| pd   | 127.0.0.1:2379  | log.disable-stacktrace      | false         |
| pd   | 127.0.0.1:2379  | log.disable-timestamp       | false         |
| pd   | 127.0.0.1:2379  | log.file.filename           |               |
| pd   | 127.0.0.1:2379  | log.file.log-rotate         | true          |
| pd   | 127.0.0.1:2379  | log.file.max-backups        | 0             |
| pd   | 127.0.0.1:2379  | log.file.max-days           | 0             |
| pd   | 127.0.0.1:2379  | log.file.max-size           | 0             |
| pd   | 127.0.0.1:2379  | log.format                  | text          |
| pd   | 127.0.0.1:2379  | log.level                   |               |
| pd   | 127.0.0.1:2379  | log.sampling                | <nil>         |
| tidb | 127.0.0.1:4000  | log.disable-error-stack     | <nil>         |
| tidb | 127.0.0.1:4000  | log.disable-timestamp       | <nil>         |
| tidb | 127.0.0.1:4000  | log.enable-error-stack      | <nil>         |
| tidb | 127.0.0.1:4000  | log.enable-timestamp        | <nil>         |
| tidb | 127.0.0.1:4000  | log.expensive-threshold     | 10000         |
| tidb | 127.0.0.1:4000  | log.file.filename           |               |
| tidb | 127.0.0.1:4000  | log.file.max-backups        | 0             |
| tidb | 127.0.0.1:4000  | log.file.max-days           | 0             |
| tidb | 127.0.0.1:4000  | log.file.max-size           | 300           |
| tidb | 127.0.0.1:4000  | log.format                  | text          |
| tidb | 127.0.0.1:4000  | log.level                   | info          |
| tidb | 127.0.0.1:4000  | log.query-log-max-len       | 4096          |
| tidb | 127.0.0.1:4000  | log.record-plan-in-slow-log | 1             |
| tidb | 127.0.0.1:4000  | log.slow-query-file         | tidb-slow.log |
| tidb | 127.0.0.1:4000  | log.slow-threshold          | 300           |
| tikv | 127.0.0.1:20160 | log-file                    |               |
| tikv | 127.0.0.1:20160 | log-level                   | info          |
| tikv | 127.0.0.1:20160 | log-rotation-timespan       | 1d            |
+------+-----------------+-----------------------------+---------------+
33 rows in set (0.00 sec)

mysql> select * from cluster_config where type='tikv' and `key` like 'raftdb.wal%';
+------+-----------------+---------------------------+--------+
| TYPE | ADDRESS         | KEY                       | VALUE  |
+------+-----------------+---------------------------+--------+
| tikv | 127.0.0.1:20160 | raftdb.wal-bytes-per-sync | 512KiB |
| tikv | 127.0.0.1:20160 | raftdb.wal-dir            |        |
| tikv | 127.0.0.1:20160 | raftdb.wal-recovery-mode  | 2      |
| tikv | 127.0.0.1:20160 | raftdb.wal-size-limit     | 0KiB   |
| tikv | 127.0.0.1:20160 | raftdb.wal-ttl-seconds    | 0      |
+------+-----------------+---------------------------+--------+
5 rows in set (0.01 sec)
```

#### 节点硬件/系统/负载信息系统表

根据 `gRPC Service` 的协议定义，每一个 `ServerInfoItem` 包含信息的名字以及对应的键值对，在向用户展示时，需要添加节点的类型以及节点地址。

```
mysql> use information_schema;
Database changed

mysql> select * from cluster_hardware
+------+-----------------+----------+----------+-------------+--------+
| TYPE | ADDRESS         | HW_TYPE  | HW_NAME  | KEY         | VALUE  |
+------+-----------------+----------+----------+-------------+--------+
| tikv | 127.0.0.1:20160 | cpu      | cpu-1    | frequency   | 3.3GHz |
| tikv | 127.0.0.1:20160 | cpu      | cpu-2    | frequency   | 3.6GHz |
| tikv | 127.0.0.1:20160 | cpu      | cpu-1    | core        | 40     |
| tikv | 127.0.0.1:20160 | cpu      | cpu-2    | core        | 48     |
| tikv | 127.0.0.1:20160 | cpu      | cpu-1    | vcore       | 80     |
| tikv | 127.0.0.1:20160 | cpu      | cpu-2    | vcore       | 96     |
| tikv | 127.0.0.1:20160 | network  | memory   | capacity    | 256GB  |
| tikv | 127.0.0.1:20160 | network  | lo0      | bandwidth   | 10000M |
| tikv | 127.0.0.1:20160 | network  | eth0     | bandwidth   | 1000M  |
| tikv | 127.0.0.1:20160 | disk     | /dev/sda | capacity    | 4096GB |
+------+-----------------+----------+----------+-------------+--------+
10 rows in set (0.01 sec)

mysql> select * from cluster_systeminfo
+------+-----------------+----------+--------------+--------+
| TYPE | ADDRESS         | MODULE   | KEY          | VALUE  |
+------+-----------------+----------+--------------+--------+
| tikv | 127.0.0.1:20160 | sysctl   | ktrace.state | 0      |
| tikv | 127.0.0.1:20160 | sysctl   | hw.byteorder | 1234   |
| ...                                                       |
+------+-----------------+----------+--------------+--------+
20 rows in set (0.01 sec)

mysql> select * from cluster_load
+------+-----------------+----------+-------------+--------+
| TYPE | ADDRESS         | MODULE   | KEY         | VALUE  |
+------+-----------------+----------+-------------+--------+
| tikv | 127.0.0.1:20160 | network  | rsec/s      | 1000Kb |
| ...                                                      |
+------+-----------------+----------+-------------+--------+
100 rows in set (0.01 sec)
```

#### 全链路日志系统表

当前日志搜索需要登陆多台机器分别进行检索，并且没有简单的办法对多个机器的检索结果按照时间全排序。本提案新建一个 `cluster_log` 系统表用于提供全链路日志，简化通过日志排查问题的方式以及提高效率。实现方式为：通过 gRPC Diagnosis Service 的 `search_log` 接口，将日志过滤的谓词下推到各个节点，并最终按照时间进行归并。

如下示例是实现本提案后的预期结果：

```
mysql> use information_schema;
Database changed

mysql> desc cluster_log;
+---------+-------------+------+------+---------+-------+
| Field   | Type        | Null | Key  | Default | Extra |
+---------+-------------+------+------+---------+-------+
| type    | varchar(16) | YES  |      | NULL    |       |
| address | varchar(32) | YES  |      | NULL    |       |
| time    | varchar(32) | YES  |      | NULL    |       |
| level   | varchar(8)  | YES  |      | NULL    |       |
| message | text        | YES  |      | NULL    |       |
+---------+-------------+------+------+---------+-------+
5 rows in set (0.00 sec)

mysql> select * from cluster_log where content like '%412134239937495042%'; -- 查询 TSO 为 412134239937495042 全链路日志
+------+--------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| TYPE | ADDRESS                | LEVEL | CONTENT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
+------+------------------------+-------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:501.60574ms txnStartTS:412134239937495042 region_id:180 store_addr:10.9.82.29:20160 kv_process_ms:416 scan_total_write:340807 scan_processed_write:340806 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                             |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:698.095216ms txnStartTS:412134239937495042 region_id:88 store_addr:10.9.1.128:20160 kv_process_ms:583 scan_total_write:491123 scan_processed_write:491122 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                             |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.529574387s txnStartTS:412134239937495042 region_id:112 store_addr:10.9.1.128:20160 kv_process_ms:945 scan_total_write:831931 scan_processed_write:831930 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                            |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.55722114s txnStartTS:412134239937495042 region_id:100 store_addr:10.9.82.29:20160 kv_process_ms:1000 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                            |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.608597018s txnStartTS:412134239937495042 region_id:96 store_addr:10.9.137.171:20160 kv_process_ms:1048 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                          |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.614233631s txnStartTS:412134239937495042 region_id:92 store_addr:10.9.137.171:20160 kv_process_ms:1000 scan_total_write:831931 scan_processed_write:831930 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                          |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.67587146s txnStartTS:412134239937495042 region_id:116 store_addr:10.9.137.171:20160 kv_process_ms:950 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                           |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.693188495s txnStartTS:412134239937495042 region_id:108 store_addr:10.9.1.128:20160 kv_process_ms:949 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                            |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.693383633s txnStartTS:412134239937495042 region_id:120 store_addr:10.9.1.128:20160 kv_process_ms:951 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                            |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.731990066s txnStartTS:412134239937495042 region_id:128 store_addr:10.9.82.29:20160 kv_process_ms:1035 scan_total_write:831931 scan_processed_write:831930 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                           |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.744524732s txnStartTS:412134239937495042 region_id:104 store_addr:10.9.137.171:20160 kv_process_ms:1030 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                         |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.786915459s txnStartTS:412134239937495042 region_id:132 store_addr:10.9.82.29:20160 kv_process_ms:1014 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                           |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.786978732s txnStartTS:412134239937495042 region_id:124 store_addr:10.9.82.29:20160 kv_process_ms:1002 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                           |
| tikv | 10.9.82.29:20180       | WARN  | [tracker.rs:150] [slow-query] [internal_key_skipped_count=831928] [internal_delete_skipped_count=0] [block_cache_hit_count=17] [block_read_count=1810] [block_read_byte=114945337] [scan_first_range="Some(start: 74800000000000002B5F728000000000130A96 end: 74800000000000002B5F728000000000196372)"] [scan_ranges=1] [scan_iter_processed=831928] [scan_iter_ops=831930] [scan_is_desc=false] [tag=select] [table_id=43] [txn_start_ts=412134239937495042] [wait_time=1ms] [total_process_time=1.001s] [peer_id=ipv4:10.9.120.251:47968] [region_id=100] |
| tikv | 10.9.82.29:20180       | WARN  | [tracker.rs:150] [slow-query] [internal_key_skipped_count=831928] [internal_delete_skipped_count=0] [block_cache_hit_count=19] [block_read_count=1793] [block_read_byte=96014381] [scan_first_range="Some(start: 74800000000000002B5F728000000000393526 end: 74800000000000002B5F7280000000003F97A6)"] [scan_ranges=1] [scan_iter_processed=831928] [scan_iter_ops=831930] [scan_is_desc=false] [tag=select] [table_id=43] [txn_start_ts=412134239937495042] [wait_time=1ms] [total_process_time=1.002s] [peer_id=ipv4:10.9.120.251:47994] [region_id=124]  |
| tikv | 10.9.82.29:20180       | WARN  | [tracker.rs:150] [slow-query] [internal_key_skipped_count=831928] [internal_delete_skipped_count=0] [block_cache_hit_count=17] [block_read_count=1811] [block_read_byte=96620574] [scan_first_range="Some(start: 74800000000000002B5F72800000000045F083 end: 74800000000000002B5F7280000000004C51E4)"] [scan_ranges=1] [scan_iter_processed=831928] [scan_iter_ops=831930] [scan_is_desc=false] [tag=select] [table_id=43] [txn_start_ts=412134239937495042] [wait_time=1ms] [total_process_time=1.014s] [peer_id=ipv4:10.9.120.251:47998] [region_id=132]  |
| tikv | 10.9.137.171:20180     | WARN  | [tracker.rs:150] [slow-query] [internal_key_skipped_count=831928] [internal_delete_skipped_count=0] [block_cache_hit_count=17] [block_read_count=1779] [block_read_byte=95095959] [scan_first_range="Some(start: 74800000000000002B5F7280000000004C51E4 end: 74800000000000002B5F72800000000052B456)"] [scan_ranges=1] [scan_iter_processed=831928] [scan_iter_ops=831930] [scan_is_desc=false] [tag=select] [table_id=43] [txn_start_ts=412134239937495042] [wait_time=2ms] [total_process_time=1.025s] [peer_id=ipv4:10.9.120.251:34926] [region_id=136]  |
| tikv | 10.9.137.171:20180     | WARN  | [tracker.rs:150] [slow-query] [internal_key_skipped_count=831928] [internal_delete_skipped_count=0] [block_cache_hit_count=15] [block_read_count=1793] [block_read_byte=114024055] [scan_first_range="Some(start: 74800000000000002B5F728000000000196372 end: 74800000000000002B5F7280000000001FC628)"] [scan_ranges=1] [scan_iter_processed=831928] [scan_iter_ops=831930] [scan_is_desc=false] [tag=select] [table_id=43] [txn_start_ts=412134239937495042] [wait_time=2ms] [total_process_time=1.03s] [peer_id=ipv4:10.9.120.251:34954] [region_id=104]  |
| tikv | 10.9.82.29:20180       | WARN  | [tracker.rs:150] [slow-query] [internal_key_skipped_count=831930] [internal_delete_skipped_count=0] [block_cache_hit_count=18] [block_read_count=1796] [block_read_byte=96116255] [scan_first_range="Some(start: 74800000000000002B5F7280000000003F97A6 end: 74800000000000002B5F72800000000045F083)"] [scan_ranges=1] [scan_iter_processed=831930] [scan_iter_ops=831932] [scan_is_desc=false] [tag=select] [table_id=43] [txn_start_ts=412134239937495042] [wait_time=1ms] [total_process_time=1.035s] [peer_id=ipv4:10.9.120.251:47996] [region_id=128]  |
| tikv | 10.9.137.171:20180     | WARN  | [tracker.rs:150] [slow-query] [internal_key_skipped_count=831928] [internal_delete_skipped_count=0] [block_cache_hit_count=15] [block_read_count=1792] [block_read_byte=113958562] [scan_first_range="Some(start: 74800000000000002B5F7280000000000CB1BA end: 74800000000000002B5F728000000000130A96)"] [scan_ranges=1] [scan_iter_processed=831928] [scan_iter_ops=831930] [scan_is_desc=false] [tag=select] [table_id=43] [txn_start_ts=412134239937495042] [wait_time=1ms] [total_process_time=1.048s] [peer_id=ipv4:10.9.120.251:34924] [region_id=96]  |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.841528722s txnStartTS:412134239937495042 region_id:140 store_addr:10.9.137.171:20160 kv_process_ms:991 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                          |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.410650751s txnStartTS:412134239937495042 region_id:144 store_addr:10.9.82.29:20160 kv_process_ms:1000 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                           |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.930478221s txnStartTS:412134239937495042 region_id:136 store_addr:10.9.137.171:20160 kv_process_ms:1025 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                         |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.26929792s txnStartTS:412134239937495042 region_id:148 store_addr:10.9.82.29:20160 kv_process_ms:901 scan_total_write:831931 scan_processed_write:831930 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                             |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.116672983s txnStartTS:412134239937495042 region_id:152 store_addr:10.9.82.29:20160 kv_process_ms:828 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                            |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.642668083s txnStartTS:412134239937495042 region_id:156 store_addr:10.9.1.128:20160 kv_process_ms:888 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                            |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.537375971s txnStartTS:412134239937495042 region_id:168 store_addr:10.9.137.171:20160 kv_process_ms:728 scan_total_write:831931 scan_processed_write:831930 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                          |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.602765417s txnStartTS:412134239937495042 region_id:164 store_addr:10.9.82.29:20160 kv_process_ms:871 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                            |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.583965975s txnStartTS:412134239937495042 region_id:172 store_addr:10.9.1.128:20160 kv_process_ms:933 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                            |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.712528952s txnStartTS:412134239937495042 region_id:160 store_addr:10.9.1.128:20160 kv_process_ms:959 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                            |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.664343044s txnStartTS:412134239937495042 region_id:220 store_addr:10.9.1.128:20160 kv_process_ms:976 scan_total_write:865647 scan_processed_write:865646 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                            |
| tidb | 10.9.120.251:10080     | INFO  | [coprocessor.go:725] ["[TIME_COP_PROCESS] resp_time:1.713342373s txnStartTS:412134239937495042 region_id:176 store_addr:10.9.1.128:20160 kv_process_ms:950 scan_total_write:831929 scan_processed_write:831928 scan_total_data:0 scan_processed_data:0 scan_total_lock:1 scan_processed_lock:0"]                                                                                                                                                                                                                                                            |
+------+--------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
31 rows in set (0.01 sec) 

mysql> select * from cluster_log where type='pd' and content like '%scheduler%'; -- 查询 PD 的调度日志

mysql> select * from cluster_log where type='tidb' and content like '%ddl%'; -- 查询 TiDB 的 DDL 日志
```

### 集群诊断

在当前的集群拓扑下，各个组件分散，数据源和数据格式异构，不便于通过程序化的手段进行集群诊断，所以需要人工进行问题诊断。通过前面几层提供的数据系统表，每一个 TiDB 节点都有了一个稳定的全集群 Global View，所以可以在这个基础上实现一个问题诊断框架。通过定义诊断规则能够快速发现集群的已有问题和潜在问题。

**诊断规则定义**：诊断规则是通过读入各个系统表的数据，并通过检测异常数据发现问题的逻辑。

诊断规则可以分为三个层次：

- 发现潜在问题：比如通过判断磁盘容量和磁盘使用量的比例发现磁盘容量不足
- 发现已有问题：比如通过查看负载情况，发现 Coprocessor 的线程池已经跑满
- 给出修复建议：比如通过分析磁盘 IO 发现延迟过高，可以给出更换磁盘的建议

本提案主要负责实现诊断框架和部分诊断规则，更多的诊断规则需要根据使用经验逐步沉淀，最终形成一个专家系统，降低使用门槛和运维难度。后续内容不详细探讨具体某条的诊断规则，主要聚焦诊断框架的实现。

#### 诊断框架设计

诊断框架的设计需要考虑多种用户使用场景，包括不限于：

- 用户选择固定版本后，不会轻易升级 TiDB 集群版本
- 用户自定义诊断规则
- 不重启集群加载新的诊断规则
- 诊断框架需要能方便的与已有运维系统集成
- 用户可能会屏蔽部分诊断，比如用户预期是一个异构系统，那么会屏蔽异构诊断规则
- ...

需要实现一个支持规则热加载的诊断系统，目前有以下备选方案：

- Golang Plugin：使用不同的插件来定义诊断规则，并且加载到 TiDB 的进程中
    - 优势：使用 Golang 开发，开发门槛低
    - 劣势：版本管理容易出错，需要和宿主 TiDB 使用同样的版本编译插件
- 内嵌 Lua：在运行时或启动过程中加载 Lua 脚本，脚本从 TiDB 读取系统表数据，并根据诊断规则判断并反馈结果
    - 优势：Lua 是一个完全依赖宿主的语言，语法简单，容易与宿主集成
    - 劣势：依赖另一个脚本语言
- Shell Script：Shell 具备流程控制功能，所以可以用 Shell 定义诊断规则
    - 优势：易于编写、加载和执行，对 TiDB 内部无侵入，只需要外部 Shell 执行对应 SQL 即可
    - 劣势：需要在安装 mysql client 的机器上运行

本提案暂时采用第三种方案，使用 Shell 编写诊断规则。对 TiDB 没有侵入，同时也为后续实现更好的方案提供扩展性。
