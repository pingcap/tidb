# RFC: TiDB Built-in SQL Diagnostics

## Summary

Currently, TiDB obtains diagnostic information mainly relying on external tools (perf/iosnoop/iotop/iostat/vmstat/sar/...), monitoring systems (Prometheus/Grafana), log files, HTTP APIs, and system tables provided by TiDB. The decentralized toolchains and cumbersome acquisition methods lead to high barriers to the use of TiDB clusters, difficulty in operation and maintenance, failure to detect problems in advance, and failure to timely investigate, diagnose, and recover clusters.
This proposal proposes a new method of acquiring diagnostic information in TiDB and exposing diagnostic information by the system tables so that users can query using SQL.

## Motivation

This proposal mainly solves the following problems in TiDB's process of obtaining diagnostic information: 

- The toolchains are scattered, it needs to switch back and forth between different tools, and some Linux distributions do not have built-in corresponding tools or built-in tools don't have versions as expected.
- The information acquisition methods are inconsistent, such as SQL, HTTP, export monitoring, login to each node to view logs, and so on.
- There are many TiDB cluster components, and the comparison and correlation of monitoring of different components is inefficient and cumbersome.
- TiDB does not have centralized log management components, and there is no efficient ways to filter, retrieve, analyze, and aggregate logs of the entire cluster.
- The system table only contains the current node information, and does not reflect the state of the entire cluster, such as: SLOW_QUERY, PROCESSLIST, STATEMENTS_SUMMARY.

The efficiency of the cluster-based information query, state acquisition, log retrieval, one-click inspection, and fault diagnosis will be improved after the multi-dimensional cluster-level system table and the cluster's diagnostic rule framework is provided. And provide basic data for the subsequent abnormal early warning function.

## Detailed Design

### System Overview

The implementation of this proposal is divided into four layers:

- L1: The lowest level implements the information collection module at each node, including TiDB/TiKV/PD monitoring information, hardware information, network IO recorded in the kernel, Disk IO information, CPU usage, memory usage, and more.
- L2: The second layer can obtain the information collected by the current node by calling the underlying information collection module and providing data to the upper layer through the external service interface (HTTP API/gRPC Service).
- L3: The third layer pulls the information of each node by TiDB, aggregates and summarizes it, and provides data to the upper layer in the form of the system table.
- L4: The fourth layer implements the diagnostic framework. The diagnostic framework obtains the status of the entire cluster by querying the system table and obtains the diagnosis result according to the diagnostic rules.

The following is a flow chart from information collection to analysis using the diagnostic rules to analyze the collected information:

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

### System Information Collection

The TiDB/TiKV/PD three components need to implement the system information collection module. TiDB/PD uses Golang to implement and reuse logic, TiKV needs to be implemented separately by Rust.

#### Node Hardware Information

The hardware information that each node needs to obtain includes:

- CPU information: physical core number, logical core number, NUMA information, CPU frequency, CPU vendor, L1/L2/L3 cache size
- NIC information: NIC device name, NIC enabled status, manufacturer, model, bandwidth, driver version, number of interface queues (optional)
- Disk information: disk name, disk capacity, disk usage, disk partition, mount information
- USB device list
- Memory information

#### Node System Information

The hardware information that each node needs to obtain includes:

- CPU Usage, loads in 1/5/15 minutes:
- Memory: Total/Free/Available/Buffers/Cached/Active/Inactive/Swap
- Disk IO:
    - tps: The number of transfers per second that were issued to the device.
    - rrqm/s: The number of read requests merged per second that were queued to the device.
    - wrqm/s: The number of write requests merged per second that were queued to the device.
    - r/s: The number (after merges) of read requests completed per second for the device.
    - w/s: The number (after merges) of write requests completed per second for the device.
    - rsec/s:  The number of sectors (kilobytes, megabytes) read from the device per second.
    - wsec/s: The number of sectors (kilobytes, megabytes) written to the device per second.
    - await: The average time (in milliseconds) for I/O requests issued to the device to be served. 
    - %util: Percentage of elapsed time during which I/O requests were issued to the device (bandwidth utilization for the device)
- Network IO
    - IFACE: name of the network interface for which statistics are reported.
    - rxpck/s: total number of packets received per second.
    - txpck/s: total number of packets transmitted per second.
    - rxkB/s: total number of kilobytes received per second.
    - txkB/s: total number of kilobytes transmitted per second.
    - rxcmp/s: number of compressed packets received per second.
    - txcmp/s: number of compressed packets transmitted per second.
    - rxmcst/s: number of multicast packets received per second.
- System configuration: `sysctl -a`

#### Node Configuration Information

All nodes contain the active configuration of the current node, and no additional steps are required to get the configuration information.

#### Node Log Information

The logs generated by TiDB/TiKV/PD are saved on their respective nodes, and no additional log collection components are deployed during TiDB cluster deployment, so there are the following problems in log retrieval:

- The logs are distributed on each node and need to be logged in to each node to search using keywords.
- The log files will be rotated every day, so we need to search multiple log files on a single node.
-  There is no easy way to combine logs of multiple nodes into a single file which sorted by the time.

This proposal provides the following two ideas to solve the above problems:

- Introduce a third-party log collection component to collect logs from all nodes
    - Advantages: unified log management, logs can be saved for a long time, and easy to retrieve, and logs of multiple components can be sorted by time
    - Disadvantages: increasing the difficulty of cluster operation and maintenance, third-party components are not easy to integrate with TiDB SQL engine; the log collection tool collects the full amount of logs, and the collection process occupies various system resources (Disk IO, Network IO).
- Each node provides a log service. TiDB pushes the predicate to the log retrieval interface through the log service of each node, and directly merges the logs returned by each node.
    - Advantages: does not introduce third-party components, and returns logs that have been filtered by the pushdown predicates, can easily be integrated with TiDB SQL and reuse SQL engine to filter, aggregation, and more.
    - Disadvantages: If the log files are deleted in some nodes, the corresponding log cannot be retrieved

This proposal uses the second way after trading off the above advantages and disadvantages. That is, each node provides a log search service, and TiDB pushes the predicate in the log search SQL to each node. The semantics of the log search service is: search for local log files, and filter using predicates, and the matching results are returned.

The following are the predicates that the log interface needs to process:

- `start_time`: The start time of the log retrieval (unix timestamp, in milliseconds). If there is no such predicate, the default is 0.
- `end_time:` The start time of the log retrieval (unix timestamp, in milliseconds). If there is no such predicate, the default is `int64::MAX`.
- `pattern`: Such as SELECT * FROM tidb_cluster_log WHERE pattern LIKE "%gc%" in %gc% is the filtered keyword
- `level`: log level, can be selected as DEBUG / INFO / WARN / WARNING / TRACE / CRITICAL / ERROR
- `limit`: The number of returning logs items, if not specified, is limited to 64k, preventing the log from being too large to occupy a large number of networks.

#### Node Performance Sampling Data

In a TiDB cluster, when performance bottlenecks occur, we usually need a way to quickly locate the problem. The Flame Graph was invented by Brendan Gregg. Unlike other trace and profiling methods, Flame Graph looks at the time distribution in a global view, listing all possible call stacks from bottom to top. Other rendering methods generally only list a single call stack or a non-hierarchical time distribution.

TiKV and TiDB currently have different ways of obtaining a flame graph and all of them rely on external tools.

- TiKV retrieves the flame graph

    ```
    perf record -F 99 -p proc_pid -g -- sleep 60
    perf script > out.perf
    /opt/FlameGraph/stackcollapse-perf.pl out.perf > out.folded
    /opt/FlameGraph/flamegraph.pl out.folded > cpu.svg
    ```

- TiDB retrieves the flame graph

    ```
    curl http://127.0.0.1:10080/debug/pprof/profile > cpu.pprof
    go tool pprof -svg cpu.svn cpu.pprof
    ```

There are two main problems in currently:

- The production environment may do not contain the corresponding external tool (perf/flamegraph.pl/go)
- There is no uniform way for TiKV and TiDB.

In order to solve the above two problems, this proposal will be built into TiDB in the way of obtaining the flame map. The use of SQL trigger sampling and the conversion of the sampled data into a flame map as a query result display, on the one hand, reduce the dependence on external tools, and at the same time Great improvement efficiency. Each node implements a sampling data acquisition function and provides a sampling interface for outputting sampled data of a specified format to the upper layer. The tentative output is the ProtoBuf format defined by `[pprof](github.com/google/pprof)`.

Sampling data acquisition method:

- TiDB/PD: Use the sample data acquisition interface built-in Golang Runtime
- TiKV: Collect sample data using `[pprof-rs](github.com/tikv/pprof-rs)` library


#### Node Monitoring Information

Monitoring information is mainly defined internally by each component monitoring metrics, At present, TiDB/TiKV/PD will provide the `/metrics` HTTP API, and then through the deployed Prometheus component timing (default configuration 15s) pull the monitoring metrics of each node of the cluster. And the Grafana component is deployed to pull the monitoring data from Prometheus for visualization.

The monitoring information is different from the real-time acquired system information. The monitoring data is a time series data, which contains the data of each node at each time point. It has very important purposes for troubleshooting and diagnosing problems, so the monitoring information is saved and inquired are very important for this proposal that built-in SQL diagnostics in TiDB. In order to be able to use SQL query monitoring data in TiDB, there are currently the following options:

- Use Prometheus client and PromQL to query the data Prometheus server
    - Advantages: there is a ready-made solution, just register the address of Prometheus server to TiDB, which is simple to implement.
    - Disadvantages: enhanced TiDB's reliance on Prometheus, added difficulty for subsequent complete removal of Prometheus.
- Saved monitoring data for the most recent period (tentative 1 day) to PD, querying monitoring data from PD.
    - Advantages: this solution does not depend on Prometheus server, components for subsequent removal of Prometheus some help
    - Disadvantages: the need to save to achieve timing logic, and to achieve the corresponding query engine, realize the difficulty and workload

This proposal tends to option II, although more difficult to implement, but it will help the follow-up work.  In order to solve the problem of realizing the difficulty and long period of implementation of PromQL and time-series data saving, we can temporarily withdraw the Prometheus time-series data and query the corresponding module and embed it into the PD.

### Retrieve System Information

Since the TiDB/TiKV/PD component has previously exposed some system information through the HTTP API, and the PD mainly provides external services through the HTTP API, some interfaces of this proposal reuse existing interfaces and use the HTTP API to obtain data from various components. For example, get configuration information.

Since the TiKV follow-up plan completely removes the HTTP API, in addition to the existing interface reuse, no additional HTTP APIs are added, so the log retrieval, hardware information, and system information acquisition uniformly define the gRPC Service, and each component implements the corresponding Service. It is registered to the gRPC Server during startup.

#### gRPC Service Definition

```proto
service Diagnosis {
  rpc search_log(SearchLogRequest) returns (SearchLogResponse);
  rpc server_info(ServerInfoRequest) returns (ServerInfoResponse);
}

message SearchLogRequest {
  optional uint64 start_time = 1;
  optional uint64 end_time = 2;
  optional uint64 level = 3;
  optional uint64 pattern = 4;
  optional uint64 limit = 5;
}

message SearchLogResponse {
  optional string type = 1;
  optional string address = 2;
  optional uint64 count = 3;
  repeated LogMessage log_message = 4;
}

message LogMessage {
  optional uint64 time = 1;
  optional uint64 level = 2;
  optional uint64 message = 3;
}

enum ServerInfoType {
	Undefined = 0;
	HardwareInfo = 1;
	SystemInfo = 2;
	LoadInfo = 3;
}

message ServerInfoRequest {
	optional ServerInfoType tp = 1;
}

message ServerInfoItem {
	// name is cpu, memory, disk, network ...
	string name = 1;
	string key = 2;
	string value = 3;
}

message ServerInfoResponse {
	repeated ServerInfoItem items = 1;
}
```

#### Reusable HTTP API

Currently, TiDB/TiKV/PD includes a partially reusable HTTP API. This proposal does not migrate the corresponding interface to the gRPC Service. The migration is completed by other subsequent plans. All HTTP APIs need to return data in JSON format. The following is a list of HTTP APIs that may be used in the proposal:

- Retrieve configuration information
    - PD: /pd/api/v1/config
    - TiDB/TiKV: /config
- Performance sampling interface: TiDB/PD contains all the following interfaces, TiKV temporarily only contains CPU performance sampling interface
    - CPU: /debug/pprof/profile
    - Memory: /debug/pprof/heap
    - Allocs: /debug/pprof/allocs
    - Mutex: /debug/pprof/mutex
    - Block: /debug/pprof/block

#### Cluster Information System Tables

Each TiDB instance can access the information of other nodes through the HTTP API or gRPC Service provided by the first two layers to implement the Global View of the cluster. In this proposal, the collected cluster information is provided to the upper layer by creating a series of related system tables. The upper layer includes not limited to:

- End User: Users can obtain cluster information directly through SQL query to troubleshooting problem
- Operation and maintenance system: The ability to obtain cluster information through SQL will make it easier to integrate TiDB into its own operation and maintenance system.
- Ecological tools: The external tools get the cluster information through SQL to realize the function customization. For example, `sqltop` can directly obtain the SQL sampling information of the entire cluster through the cluster `events_statements_summary_by_digest`

#### Cluster Topology System Table

We need to provide a topology system table before providing a **Global View** for the TiDB instance. Then we can obtain the HTTP API Address and gRPC Service Address of each node from the topology system table, so that each remote API can be easily constructed. The Endpoint further acquires the information collected by the target node.

The implementation of this proposal can query the following results through SQL:

```
mysql> use information_schema;
Database changed

mysql> desc TIDB_CLUSTER_INFO;
+----------------+---------------------+------+------+---------+-------+
| Field          | Type                | Null | Key  | Default | Extra |
+----------------+---------------------+------+------+---------+-------+
| ID             | bigint(21) unsigned | YES  |      | NULL    |       |
| TYPE           | varchar(64)         | YES  |      | NULL    |       |
| NAME           | varchar(64)         | YES  |      | NULL    |       |
| ADDRESS        | varchar(64)         | YES  |      | NULL    |       |
| STATUS_ADDRESS | varchar(64)         | YES  |      | NULL    |       |
| VERSION        | varchar(64)         | YES  |      | NULL    |       |
| GIT_HASH       | varchar(64)         | YES  |      | NULL    |       |
+----------------+---------------------+------+------+---------+-------+
7 rows in set (0.00 sec)

mysql> select TYPE, ADDRESS, STATUS_ADDRESS,VERSION from TIDB_CLUSTER_INFO;
+------+-----------------+-----------------+-----------------------------------------------+
| TYPE | ADDRESS         | STATUS_ADDRESS  | VERSION                                       |
+------+-----------------+-----------------+-----------------------------------------------+
| tidb | 127.0.0.1:4000  | 127.0.0.1:10080 | 5.7.25-TiDB-v4.0.0-alpha-793-g79eef48a3-dirty |
| pd   | 127.0.0.1:2379  | 127.0.0.1:2379  | 4.0.0-alpha                                   |
| tikv | 127.0.0.1:20160 | 127.0.0.1:20180 | 4.0.0-alpha                                   |
+------+-----------------+-----------------+-----------------------------------------------+
3 rows in set (0.00 sec)
```

#### Monitoring Information System Table

Since the monitoring metrics are added and deleted as the program is iterated, for the same monitoring metrics, different types of information may be acquired through different PromQL expressions. In view of the above two requirements, it is necessary to design a flexible monitoring system table frame. This proposal temporarily adopts the following scheme: mapping expressions to system tables in the `metrics_schema` database. The relationship between expressions and system tables can be related in the following ways:

- Defined in the configuration file

    ```
    # tidb.toml
    [metrics_schema]
    qps = `sum(rate(tidb_server_query_total[$INTERVAL] offset $OFFSET_TIME)) by (result)`
    memory_usage = `process_resident_memory_bytes{job="tidb"}`
    goroutines = `rate(go_gc_duration_seconds_sum{job="tidb"}[$INTERVAL] offset $OFFSET_TIME)`
    ```

- HTTP API injection

    ```
    curl -XPOST http://host:port/metrics_schema?name=distsql_duration&expr=`histogram_quantile(0.999, 
    sum(rate(tidb_distsql_handle_query_duration_seconds_bucket[$INTERVAL] offset $OFFSET_TIME)) by (le, type))`
    ```

- Special SQL command

    ```
    mysql> admin metrics_schema add parse_duration `histogram_quantile(0.95, sum(rate(tidb_session_parse_duration_seconds_bucket[$INTERVAL] offset $OFFSET_TIME)) by (le, sql_type))`
    ```

- LOAD from file

    ```
    mysql> admin metrics_schema load external_metrics.txt
    #external_metrics.txt
    execution_duration = `histogram_quantile(0.95, sum(rate(tidb_session_execute_duration_seconds_bucket[$INTERVAL] offset $OFFSET_TIME)) by (le, sql_type))`
    pd_client_cmd_ops = `sum(rate(pd_client_cmd_handle_cmds_duration_seconds_count{type!="tso"}[$INTERVAL] offset $OFFSET_TIME)) by (type)`
    ```

After passing the above mapping, you can view the following table in the `metrics_schema` library:

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

The way the field is determined when the expression is mapped to the system table depends mainly on the data of the result of the expression execution. Taking the expression `sum(rate(pd_client_cmd_handle_cmds_duration_seconds_count{type!="tso"}[1m]offset 0)) by (type)` as an example, the result of the query is:

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

The system table schema and the query results are:

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

mysql> select address, type, value from pd_client_cmd_ops where start_time=’2019-11-14 10:00:00’;
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

PromQL query result which have multiple labels will be mapped to multiple columns of data, which can be easily filtered using existing SQL execution engines. Polymerization gives the desired result.

#### Performance Profiling System Table

The corresponding node performance sampling data is obtained by `/debug/pprof/profile` of each node, and then the aggregated performance profiling result is output to the user by using the SQL query result. Since the SQL query results cannot be output in svg format, you need to solve the problem of output content display.

The core of the flame graph to help locate the problem quickly is:

- Provide a global view
- Show all the call paths
- Hierarchical display

The solution proposed by this proposal focuses on solving the core problem, but not in the form of graphic visualization. The final solution is to aggregate the sampled data and display all the call paths on a line-by-line basis using a tree structure.

The solution is to fit the three core points in the following ways:

- Provide a global view: use a separate column for each aggregate result to show the global usage scale, which can be used to facilitate filtering and sorting to
- Show all call paths: all call paths are used as query results. And use a separate column to number the subtrees of each call path, we can easily view only one subtreeby filtering
- Hierarchical display: use the tree structure to display the stack, use a separate column to record the depth of the stack, which is convenient Filtering the depth of different stacks

This proposal needs to implement the following performance profiling table:

| 表名 | 作用 |
|------|-----|
| tidb_profile_cpu | TiDB CPU flame graph |
| tikv_profile_cpu | TiKV CPU flame graph |
| tidb_profile_block | Stack traces that led to blocking on synchronization primitives |
| tidb_profile_memory | A sampling of memory allocations of live objects |
| tidb_profile_allocs | A sampling of all past memory allocations |
| tidb_profile_mutex | Stack traces of holders of contended mutexes |
| tidb_profile_goroutines | Stack traces of all current goroutines |

#### Globalize Memory System Table

Current the `slow_query`/`events_statements_summary_by_digest`/`processlist` memory tables only contains single-node data. This proposal allows any TiDB instance to view information about the entire cluster by adding the following three cluster-level system tables:

| 表名 | 作用 |
|------|-----|
| tidb_cluster_slow_query | all TiDB nodes' slow_query table data |
| tidb_cluster_statements_summary | all TiDB nodes's statements summary table Data |
| tidb_cluster_processlist | processlist table data of all TiDB nodes |

#### Configuration information of all nodes

For a large cluster, the way to obtain configuration by each node through HTTP API is cumbersome and inefficient. This proposal provides a full cluster configuration information system table. Simplify the acquisition, filtering, and aggregation of the entire cluster configuration information.

The following example is the expected result after implementing this proposal:

```
mysql> use information_schema;
Database changed

mysql> select * from tidb_cluster_config where `key` like 'log%';
+------+------+--------+-----------------+-----------------------------+---------------+
| ID   | TYPE | NAME   | ADDRESS         | KEY                         | VALUE         |
+------+------+--------+-----------------+-----------------------------+---------------+
|   21 | pd   | pd-0   | 127.0.0.1:2379  | log-file                    |               |
|   22 | pd   | pd-0   | 127.0.0.1:2379  | log-level                   |               |
|   23 | pd   | pd-0   | 127.0.0.1:2379  | log.development             | false         |
|   24 | pd   | pd-0   | 127.0.0.1:2379  | log.disable-caller          | false         |
|   25 | pd   | pd-0   | 127.0.0.1:2379  | log.disable-error-verbose   | true          |
|   26 | pd   | pd-0   | 127.0.0.1:2379  | log.disable-stacktrace      | false         |
|   27 | pd   | pd-0   | 127.0.0.1:2379  | log.disable-timestamp       | false         |
|   28 | pd   | pd-0   | 127.0.0.1:2379  | log.file.filename           |               |
|   29 | pd   | pd-0   | 127.0.0.1:2379  | log.file.log-rotate         | true          |
|   30 | pd   | pd-0   | 127.0.0.1:2379  | log.file.max-backups        | 0             |
|   31 | pd   | pd-0   | 127.0.0.1:2379  | log.file.max-days           | 0             |
|   32 | pd   | pd-0   | 127.0.0.1:2379  | log.file.max-size           | 0             |
|   33 | pd   | pd-0   | 127.0.0.1:2379  | log.format                  | text          |
|   34 | pd   | pd-0   | 127.0.0.1:2379  | log.level                   |               |
|   35 | pd   | pd-0   | 127.0.0.1:2379  | log.sampling                | <nil>         |
|  114 | tidb | tidb-0 | 127.0.0.1:4000  | log.disable-error-stack     | <nil>         |
|  115 | tidb | tidb-0 | 127.0.0.1:4000  | log.disable-timestamp       | <nil>         |
|  116 | tidb | tidb-0 | 127.0.0.1:4000  | log.enable-error-stack      | <nil>         |
|  117 | tidb | tidb-0 | 127.0.0.1:4000  | log.enable-timestamp        | <nil>         |
|  118 | tidb | tidb-0 | 127.0.0.1:4000  | log.expensive-threshold     | 10000         |
|  119 | tidb | tidb-0 | 127.0.0.1:4000  | log.file.filename           |               |
|  120 | tidb | tidb-0 | 127.0.0.1:4000  | log.file.max-backups        | 0             |
|  121 | tidb | tidb-0 | 127.0.0.1:4000  | log.file.max-days           | 0             |
|  122 | tidb | tidb-0 | 127.0.0.1:4000  | log.file.max-size           | 300           |
|  123 | tidb | tidb-0 | 127.0.0.1:4000  | log.format                  | text          |
|  124 | tidb | tidb-0 | 127.0.0.1:4000  | log.level                   | info          |
|  125 | tidb | tidb-0 | 127.0.0.1:4000  | log.query-log-max-len       | 4096          |
|  126 | tidb | tidb-0 | 127.0.0.1:4000  | log.record-plan-in-slow-log | 1             |
|  127 | tidb | tidb-0 | 127.0.0.1:4000  | log.slow-query-file         | tidb-slow.log |
|  128 | tidb | tidb-0 | 127.0.0.1:4000  | log.slow-threshold          | 300           |
|  213 | tikv | tikv-0 | 127.0.0.1:20160 | log-file                    |               |
|  214 | tikv | tikv-0 | 127.0.0.1:20160 | log-level                   | info          |
|  215 | tikv | tikv-0 | 127.0.0.1:20160 | log-rotation-timespan       | 1d            |
+------+------+--------+-----------------+-----------------------------+---------------+
33 rows in set (0.00 sec)

mysql> select * from tidb_cluster_config where type='tikv' and `key` like 'raftdb.wal%';
+------+------+--------+-----------------+---------------------------+--------+
| ID   | TYPE | NAME   | ADDRESS         | KEY                       | VALUE  |
+------+------+--------+-----------------+---------------------------+--------+
|  292 | tikv | tikv-0 | 127.0.0.1:20160 | raftdb.wal-bytes-per-sync | 512KiB |
|  293 | tikv | tikv-0 | 127.0.0.1:20160 | raftdb.wal-dir            |        |
|  294 | tikv | tikv-0 | 127.0.0.1:20160 | raftdb.wal-recovery-mode  | 2      |
|  295 | tikv | tikv-0 | 127.0.0.1:20160 | raftdb.wal-size-limit     | 0KiB   |
|  296 | tikv | tikv-0 | 127.0.0.1:20160 | raftdb.wal-ttl-seconds    | 0      |
+------+------+--------+-----------------+---------------------------+--------+
5 rows in set (0.01 sec)
```

#### Node Hardware/System/Load Information System Tables

According to the defination of `gRPC Service` protocol. Each `ServerInfoItem` contains the name of the information and the corresponding key-value pair. When presenting to the user, the type of the node and the node address need to be added.

```
mysql> use information_schema;
Database changed

mysql> select * from tidb_cluster_hardware
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

mysql> select * from tidb_cluster_systeminfo
+------+-----------------+----------+--------------+--------+
| TYPE | ADDRESS         | MODULE   | KEY          | VALUE  |
+------+-----------------+----------+--------------+--------+
| tikv | 127.0.0.1:20160 | sysctl   | ktrace.state | 0      |
| tikv | 127.0.0.1:20160 | sysctl   | hw.byteorder | 1234   |
| ...                                                       |
+------+-----------------+----------+--------------+--------+
20 rows in set (0.01 sec)

mysql> select * from tidb_cluster_load
+------+-----------------+----------+-------------+--------+
| TYPE | ADDRESS         | MODULE   | KEY         | VALUE  |
+------+-----------------+----------+-------------+--------+
| tikv | 127.0.0.1:20160 | network  | rsec/s      | 1000Kb |
| ...                                                      |
+------+-----------------+----------+-------------+--------+
100 rows in set (0.01 sec)
```


#### Fullchain Log System Table

The current log search needs to log in to multiple machines for retrieval, and there is no easy way to sort the retrieval results of multiple machines according to time. This proposal creates a new `tidb_cluster_log` system table to provide full-link logs, simplifying the way to troubleshoot problems through logs and improving efficiency. This is achieved by pushing the log-filtered predicates down to the nodes through the `search_log` interface of the gRPC Diagnosis Service and eventually merging them by time.

The following example is the expected result after implementing this proposal:

```
mysql> use information_schema;
Database changed

mysql> desc tidb_cluster_log;
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

mysql> select * from tidb_cluster_log;
+------+-----------------+-------------------------+-------+------------------------------------+
| type | address         | time                    | level | message                            |
+------+-----------------+-------------------------+-------+------------------------------------+
| tidb | 127.0.0.1:4000  | 2019/11/01 15:03:00.033 | INFO  | [BIG_TXN] [conn=10] [table id] ... |
| tidb | 127.0.0.1:4000  | 2019/11/01 15:03:00.033 | INFO  | [BIG_TXN] [conn=10] [table id] ... |
| tidb | 127.0.0.1:4000  | 2019/11/01 15:03:00.033 | INFO  | [BIG_TXN] [conn=10] [table id] ... |
| pd   | 10.0.1.23:2379  | 2019/11/01 15:04:00.033 | WARN  | ...                                |
| pd   | 10.0.1.23:2379  | 2019/11/01 15:04:00.033 | WARN  | ...                                |
| pd   | 10.0.1.23:2379  | 2019/11/01 15:04:00.033 | WARN  | ...                                |
| tikv | 10.0.1.24:20160 | 2019/11/01 15:05:00.033 | ERROR | ...                                |
| tikv | 10.0.1.24:20160 | 2019/11/01 15:05:00.033 | ERROR | ...                                |
| tikv | 10.0.1.24:20160 | 2019/11/01 15:05:00.033 | ERROR | ...                                |
+------+-----------------+-------------------------+-------+------------------------------------+
9 rows in set (0.00 sec)

mysql> select * from tidb_cluster_log where type = 'pd';
+------+----------------+-------------------------+-------+---------+
| type | address        | time                    | level | message |
+------+----------------+-------------------------+-------+---------+
| pd   | 10.0.1.23:2379 | 2019/11/01 15:04:00.033 | WARN  | ...     |
| pd   | 10.0.1.23:2379 | 2019/11/01 15:04:00.033 | WARN  | ...     |
| pd   | 10.0.1.23:2379 | 2019/11/01 15:04:00.033 | WARN  | ...     |
+------+----------------+-------------------------+-------+---------+
3 rows in set (0.00 sec)

mysql> select * from tidb_cluster_log where level = 'ERROR';
+------+-----------------+-------------------------+-------+---------+
| type | address         | time                    | level | message |
+------+-----------------+-------------------------+-------+---------+
| tikv | 10.0.1.24:20160 | 2019/11/01 15:05:00.033 | ERROR | ...     |
| tikv | 10.0.1.24:20160 | 2019/11/01 15:05:00.033 | ERROR | ...     |
| tikv | 10.0.1.24:20160 | 2019/11/01 15:05:00.033 | ERROR | ...     |
+------+-----------------+-------------------------+-------+---------+
3 rows in set (0.00 sec)

mysql> select * from tidb_cluster_log where message like '%table%';
+------+----------------+-------------------------+-------+------------------------------------+
| type | address        | time                    | level | message                            |
+------+----------------+-------------------------+-------+------------------------------------+
| tidb | 127.0.0.1:4000 | 2019/11/01 15:03:00.033 | INFO  | [BIG_TXN] [conn=10] [table id] ... |
| tidb | 127.0.0.1:4000 | 2019/11/01 15:03:00.033 | INFO  | [BIG_TXN] [conn=10] [table id] ... |
| tidb | 127.0.0.1:4000 | 2019/11/01 15:03:00.033 | INFO  | [BIG_TXN] [conn=10] [table id] ... |
+------+----------------+-------------------------+-------+------------------------------------+
3 rows in set (0.00 sec)
```

### Cluster Diagnostics

In the current cluster topology, each component is dispersed, and the data source and data format are heterogeneous. It is not convenient to perform cluster diagnosis through programmatic ways, so manual diagnosis is required. Through the data system tables provided by the previous layers, each TiDB node has a stable global cluster Global View, so a problem diagnosis framework can be implemented on this basis. By defining diagnostic rules, we can quickly discover existing and potential problems with your cluster.

**Diagnostic rule definition**: Diagnostic rules are logic for finding problems by reading data from various system tables and detecting abnormal data.

Diagnostic rules can be divided into three levels:

- Discovery of potential problems: For example, by determining the ratio of disk capacity and disk usage, it is found with insufficient disk capacity
- Locate an existed problem: For example, by looking at the load, it is found that the thread pool of Coprocessor has reached to bottleneck
- Given a fix suggestions: For example, by analyzing the disk IO, the delay is too high, and the recommendation to replace the disk can be given.

This proposal is mainly responsible for implementing the diagnostic framework and some diagnostic rules. More diagnostic rules need to be gradually precipitated according to the experience, and finally form an expert system to reduce use thresholds and operational difficulty. The follow-up content does not discuss in detail the specific diagnostic rules, mainly focusing on the implementation of the diagnostic framework.

#### Diagnostic Framework Design

Diagnostic framework design needs to consider a variety of user scenarios, including but not limited to:

- User selects a fixed version, the TiDB cluster version will not be easily upgraded.
- User-defined diagnostic rules
- Cluster loading new diagnostic rule without restarting
- The diagnosis framework needs to be easily integrated with the existing operation and maintenance system.
- Users may block some diagnostics. For example, if the user expects to be a heterogeneous system, the heterogeneous diagnostic rules will be shielded
- ...

There is a need to implement a diagnostic system that supports regular hot loading, and currently has the following options:

- Golang Plugin: Use different plugins to define diagnostic rules and load them into the TiDB process
     - Advantages: Developed with Golang, low development threshold
     - Disadvantages: Version management is error-prone and requires compiling plugins with the same version as the host TiDB
- Embedded Lua: Loads Lua scripts at runtime or during startup, the script reads system table data from TiDB and evaluates and feeds back results based on diagnostic rules
     - Advantages: Lua is a fully host-dependent language with simple syntax and easy integration with the host
     - Disadvantages: Relying on another scripting language