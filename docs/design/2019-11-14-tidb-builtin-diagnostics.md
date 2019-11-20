# RFC: TiDB Built-in SQL Diagnostics

## Summary

Currently, TiDB obtains diagnostic information mainly relying on external tools (perf/iosnoop/iotop/iostat/vmstat/sar/...), monitoring systems (Prometheus/Grafana), log files, HTTP APIs, and system tables provided by TiDB. The decentralized toolchains and cumbersome acquisition methods lead to high barriers to the use of TiDB clusters, difficulty in operation and maintenance, failure to detect problems in advance, and failure to timely investigate,diagnose, and recover clusters.

This proposal proposes a new method of acquiring diagnostic information in TiDB for exposure in system tables so that users can query diagnostic information using SQL statements.

## Motivation

This proposal mainly solves the following problems in TiDB's process of obtaining diagnostic information:

- The toolchains are scattered, so TiDB needs to switch back and forth between different tools. In addition, some Linux distributions don't have corresponding built-in tools; even if they do, the versions of the tools aren't as expected.
- The information acquisition methods are inconsistent, for example, SQL, HTTP, exported monitoring, viewing logs by logging into each node, and so on.
- There are many TiDB cluster components. and the comparison and correlation of the monitoring information between different components are inefficient and cumbersome.
- TiDB does not have centralized log management components, so there are no efficient ways to filter, retrieve, analyze, and aggregate logs of the entire cluster.
- The system table only contains the information of the current node, which does not reflect the state of the entire cluster, such as: SLOW_QUERY, PROCESSLIST, STATEMENTS_SUMMARY.

The efficiency of cluster-scale information query, state acquisition, log retrieval, one-click inspection, and fault diagnosis can be improved after the multi-dimensional cluster-level system table and the cluster's diagnostic rule framework are provided. And basic data can be provided for subsequent exception alarming.

## Detailed design

### System overview

The implementation of this proposal is divided into four layers:

- L1: The lowest level implements the information collection module at each node, including monitoring information, hardware information, network IO recorded in the kernel, Disk IO information, CPU usage, memory usage, etc, of TiDB/TiKV/PD.
- L2: The second layer can obtain the information collected by the current node by calling the underlying information collection module and providing data to the upper layer through external service interfaces (HTTP API/gRPC Service).
- L3: In the third layer, TiDB pulls the information from each node for aggregation and summarizing, and provides the data to the upper layer in the form of the system table.
- L4: The fourth layer implements the diagnostic framework. The diagnostic framework obtains the status of the entire cluster by querying the system table and obtains the diagnostic result according to the diagnostic rules.

The following chart shows the data flow from information collection to analysis using the diagnostic rules:

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

### System information collection

The system information collection module needs to be implemented for all of TiDB/TiKV/PD components. TiDB/PD uses Golang to implement with reuse of the underlying logic; TiKV needs separation implementation in Rust.

#### Node hardware information

The hardware information that each node needs to obtain includes:

- CPU information: physical core number, logical core number, NUMA information, CPU frequency, CPU vendor, L1/L2/L3 cache
- NIC information: NIC device name, NIC enabled status, manufacturer, model, bandwidth, driver version, number of interface queues (optional)
- Disk information: disk name, disk capacity, disk usage, disk partition, mount information
- USB device list
- Memory information

#### Node System Information

The hardware information that each node needs to obtain includes:

- CPU Usage, loads in 1/5/15 minutes:
- Memory: Total/Free/Available/Buffers/Cached/Active/Inactive/Swap
- Disk IO:
    - tps: number of transfers per second that were issued to the device.
    - rrqm/s: number of read requests merged per second that were queued to the device.
    - wrqm/s: number of write requests merged per second that were queued to the device.
    - r/s: number (after merges) of read requests completed per second for the device.
    - r/s: number (after merges) of read requests completed per second for the device.
    - r/s: number (after merges) of read requests completed per second for the device.
    - r/s: number (after merges) of read requests completed per second for the device.
    - r/s: number (after merges) of read requests completed per second for the device.
    - w/s: number (after merges) of write requests completed per second for the device.
    - rsec/s: number of sectors (kilobytes, megabytes) read from the device per second.
    - wsec/s: number of sectors (kilobytes, megabytes) written to the device per second.
    - await: average time (in milliseconds) for I/O requests issued to the device to be served.
    - %util: percentage of elapsed time during which I/O requests were issued to the device (bandwidth utilization for the device)
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

#### Node configuration information

All nodes contain the effective configuration for the current node, and no additional steps are required to get the configuration information.

#### Node log information

Currently, the logs generated by TiDB/TiKV/PD are saved on their respective nodes, and no additional log collection components are deployed during TiDB cluster deployment, so there are the following problems in log retrieval:

- Logs are distributed on each node. You need to log in to each node to search using keywords.
- Log files are rotated every day, so we need to search among multiple log files even on a single node.
- There is no easy way to combine logs of multiple nodes into a single file which sorted by the time.

This proposal provides the following two solutions to the above problems:

- Introduce a third-party log collection component to collect logs from all nodes
    - Advantages: with a unified log management mechanism, logs can be saved for a long time, and are easy to retrieve; logs of multiple components can be sorted  by time.
    - Disadvantages: third-party components are not easy to integrate with TiDB SQL engine, which may increase the difficulty of cluster operation and maintenance; the log collection tool collects logs fully, so the collection process will take up system resources (Disk IO, Network IO).
- Each node provides a log service. TiDB pushes the predicate to the log retrieval interface through the log service of each node, and directly merges the logs returned by each node.
    - Advantages: no third-party component is introduced. Only logs that have been filtered by the pushdown predicates are returned; the implementation can easily be integrated with TiDB SQL and reuse SQL engine functions such as filter and aggregation.
    - Disadvantages: If the log files are deleted in some nodes, the corresponding log cannot be retrieved.

This proposal uses the second way after weighing in on the above advantages and disadvantages. That is, each node provides a log search service, and TiDB pushes the predicate in the log search SQL to each node. The semantics of the log search service is: search for local log files, and filter using predicates, and then return the matched results.

The following are the predicates that the log interface needs to process:

- `start_time`: start time of the log retrieval (Unix timestamp, in milliseconds). If there is no such predicate, the default is 0.
- `end_time:`: end time of the log retrieval (Unix timestamp, in milliseconds). If there is no such predicate, the default is `int64::MAX`.
- `pattern`: filter pattern determined by the keyword. For example, `SELECT * FROM tidb_cluster_log` WHERE "%gc%" `%gc%` is the filtered keyword.
- `level`: log level; can be selected as DEBUG/INFO/WARN/WARNING/TRACE/CRITICAL/ERROR
- `limit`: the maximum of logs items to return, preventing the log from being too large and occupying a large bandwidth of the network.. If not specified, the default limit is 64k.

#### Node performance sampling data

In a TiDB cluster, when performance bottlenecks occur, we usually need a way to quickly locate the problem. The Flame Graph was invented by Brendan Gregg. Unlike other trace and profiling methods, Flame Graph looks at the time distribution in a global view, listing all possible call stacks from bottom to top. Other rendering methods generally only list a single call stack or a non-hierarchical time distribution.

TiKV and TiDB currently have different ways of obtaining a flame graph and all of them rely on external tools.

- TiKV retrieves the flame graph via:

    ```
    perf record -F 99 -p proc_pid -g -- sleep 60
    perf script > out.perf
    /opt/FlameGraph/stackcollapse-perf.pl out.perf > out.folded
    /opt/FlameGraph/flamegraph.pl out.folded > cpu.svg
    ```

- TiDB retrieves the flame graph via:

    ```
    curl http://127.0.0.1:10080/debug/pprof/profile > cpu.pprof
    go tool pprof -svg cpu.svn cpu.pprof
    ```

There are two main problems currently:

- The production environment may not contain the corresponding external tool (perf/flamegraph.pl/go)
- There is no unified way for TiKV and TiDB.

In order to solve the above two problems, this proposal proposes to build the flame map in TiDB in the flame map, so that TiDB and TiKV can both use SQL to trigger sampling, and the sampled data can be converted into query results in flame maps. This way we can reduce the dependency on external tools, and at the same time improve efficiency greatly. For each node, a sampling data acquisition function is implemented and the sampling interface provided, through which a specified format is output to the upper layer. The tentative output is the ProtoBuf format defined by `[pprof](github.com/google/pprof)`.

Sampling data acquisition method:

- TiDB/PD: Use the sample data acquisition interface built in Golang runtime
- TiKV: Collect sample data using the `[pprof-rs](github.com/tikv/pprof-rs)` library

#### Node monitoring information

Monitoring information mainly includes monitoring metrics defined internally by each component. At present, TiDB/TiKV/PD provides the `/metrics` HTTP API, through which the deployed Prometheus component pulls the monitoring metrics of each node of the cluster in a timing manner (15s interval by default). And the Grafana component is deployed to pull the monitoring data from Prometheus for visualization.

The monitoring information is different from the system information acquired in real time. The monitoring data is time-series data which contains the data of each node at each time point. It is very useful for troubleshooting and diagnosing problems, so how monitoring information is saved and inquired is very important for this proposal. In order to be able to use SQL to query monitoring data in TiDB, there are currently the following options:

- Use Prometheus client and PromQL to query the data from the Prometheus server
    - Advantages: there is a ready-made solution; just register the address of Prometheus server to TiDB, which is simple to implement.
    - Disadvantages: TiDB will rely more on Prometheus, which increases the difficulty for subsequent removal of Prometheus.
- Save the monitoring data for the most recent period (tentative 1 day) to PD and query monitoring data from PD.
    - Advantages: this solution does not depend on the Prometheus server. Easy for subsequent removal of Prometheus.
    - Disadvantages: requires implementation of time-series saving logic in PD and the corresponding query engine. The workload and difficulty are high.

In this proposal, we are opt to the second solution. Although it is more difficult to implement, it will benefit the follow-up work. Considering the difficulty and long development cycle, we can implement this function in three stages (the third stage is implemented depending on the specific situation):

1. Add the `remote-metrics-storage` configuration to the PD and temporarily configure it as the address of the Prometheus Server. PD acts as a proxy, and the request is transferred to Prometheus for execution. The main considerations are as follows:

    - PD will have its own implementation of the query interface to realize bootstraping. No other changes needed for TiDB.
    - With bootstrapping realized, users can still use SQL to query monitoring information and diagnostic frameworks without relying on the Prometheus component deployed by TiDB

2. Extract the modules for persisting and querying Prometheus time series data and embed it in PD.
3. PD internally implements its own module for persisting and query time series data (currently CockroachDB's solution)

##### PD performance analysis

PD mainly handles scheduling and TSO services for TiDB clusters, where:

1. TSO fetching accumulates only one atomic variable in Leader memory
2. The Operator and OperatorStep generated by the schedule are only stored in memory, and the state in memory is updated according to the heartbeat information of the Region.

From the above information, it can be concluded that the performance impact of the new monitoring function on the PD can be ignored in most cases.

### Retrieve system information

Since the TiDB/TiKV/PD component has previously exposed some system information through the HTTP API, and the PD mainly provides external services through the HTTP API, some interfaces of this proposal reuse existing logics and use the HTTP API to obtain data from various components. For example, configuration information.

However, because TiKV plan to completely removes the HTTP API in the future, only the existing interfaces will be reused for TiKV, and no new HTTP APIs will be added. There will be a unified gRPC service defined for log retrieval, hardware information, and system information acquisition. Each component implements their own services, which are registered to the gRPC Server during startup.

#### gRPC service definition

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
	string name = 1;
	string key = 2;
	string value = 3;
}

message ServerInfoResponse {
	repeated ServerInfoItem items = 1;
}
```

#### Reusable HTTP API

Currently, TiDB/TiKV/PD includes a partially reusable HTTP API. This proposal does not migrate the corresponding interface to the gRPC Service. The migration will be completed by other subsequent plans. All HTTP APIs need to return data in JSON format. The following is a list of HTTP APIs that may be used in this proposal:

- Retrieve configuration information
    - PD: /pd/api/v1/config
    - TiDB/TiKV: /config
- Performance sampling interface: TiDB and PD contain all the following interfaces, while TiKV temporarily only contains the CPU performance sampling interface
    - CPU: /debug/pprof/profile
    - Memory: /debug/pprof/heap
    - Allocs: /debug/pprof/allocs
    - Mutex: /debug/pprof/mutex
    - Block: /debug/pprof/block

#### Cluster information system tables

Each TiDB instance can access the information of other nodes through the HTTP API or gRPC Service provided by the first two layers. This way we can implement the Global View forw the cluster. In this proposal, the collected cluster information is provided to the upper layer by creating a series of related system tables. The upper layer includes not limited to:

- End User: Users can obtain cluster information directly through SQL query to troubleshooting problem
- Operation and maintenance system: The ability to obtain cluster information through SQL will make it easier for users to integrate TiDB into their own operation and maintenance systems.
- Eco-system tools: External tools get the cluster information through SQL to realize function customization. For example, `[sqltop](https://github.com/ngaut/sqltop)` can directly obtain the SQL sampling information of the entire cluster through the `events_statements_summary_by_digest` table of the cluster.

#### Cluster Topology System Table

 To realize **Global View** for the TiDB instance, we need to provide a topology system table, where we can obtain the HTTP API Address and gRPC Service Address of each node. This way the Endpoints can be easily constructed for remote APIs. The Endpoint further acquires the information collected by the target node.

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

#### Monitoring information system table

Monitoring metrics are added and deleted as the program is iterated. For this reason, the same monitoring metric might be obtained through different PromQL expressions to monitor information in different dimensions. Therefore, it is necessary to design a flexible monitoring system table frame. This proposal temporarily adopts the this scheme - mapping expressions to system tables in the `metrics_schema` database. The relationship between expressions and system tables can be mapped in the following ways:

- Define in the configuration file

    ```
    # tidb.toml
    [metrics_schema]
    qps = `sum(rate(tidb_server_query_total[$INTERVAL] offset $OFFSET_TIME)) by (result)`
    memory_usage = `process_resident_memory_bytes{job="tidb"}`
    goroutines = `rate(go_gc_duration_seconds_sum{job="tidb"}[$INTERVAL] offset $OFFSET_TIME)`
    ```

- Inject via HTTP API

    ```
    curl -XPOST http://host:port/metrics_schema?name=distsql_duration&expr=`histogram_quantile(0.999,
    sum(rate(tidb_distsql_handle_query_duration_seconds_bucket[$INTERVAL] offset $OFFSET_TIME)) by (le, type))`
    ```

- Use special SQL commands

    ```
    mysql> admin metrics_schema add parse_duration `histogram_quantile(0.95, sum(rate(tidb_session_parse_duration_seconds_bucket[$INTERVAL] offset $OFFSET_TIME)) by (le, sql_type))`
    ```

- Load from file

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

The way the field is determined when the expression is mapped to the system table depends mainly on the result data of the expression execution. Take expression `sum(rate(pd_client_cmd_handle_cmds_duration_seconds_count{type!="tso"}[1m]offset 0)) by (type)` as an example, the result of the query is:

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

The following are the query results mapped to the system table schema:

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

PromQL query statements with multiple labels will be mapped to multiple columns of data, which can be easily filtered and aggregated using existing SQL execution engines.

#### Performance profiling system table

The corresponding node performance sampling data is obtained by `/debug/pprof/profile` of each node, and then aggregated performance profiling results are output to the user in the form of SQL query. Since the SQL query results cannot be output in svg format, we need to solve the problem of output display.

Our proposed solution is to aggregate the sampled data and display all the call paths on a line-by-line basis in a tree structure. This can be implemented by using flamegraph. The core ideas and the corresponding implementations are described as below:

- Provide a global view: use a separate column for each aggregate result to show the global usage scale, which can be used to facilitate filtering and sorting to
- Show all call paths: all call paths are used as query results. And use a separate column to number the subtrees of each call path, we can easily view only one subtreeby filtering
- Hierarchical display: use the tree structure to display the stack, use a separate column to record the depth of the stack, which is convenient Filtering the depth of different stacks

This proposal needs to implement the following performance profiling table:

| Table | Description |
|------|-----|
| tidb_profile_cpu | TiDB CPU flame graph |
| tikv_profile_cpu | TiKV CPU flame graph |
| tidb_profile_block | Stack traces that led to blocking on synchronization primitives |
| tidb_profile_memory | A sampling of memory allocations of live objects |
| tidb_profile_allocs | A sampling of all past memory allocations |
| tidb_profile_mutex | Stack traces of holders of contended mutexes |
| tidb_profile_goroutines | Stack traces of all current goroutines |

#### Globalized memory system table

Current the `slow_query`/`events_statements_summary_by_digest`/`processlist` memory tables only contain single-node data. This proposal allows any TiDB instance to view information about the entire cluster by adding the following three cluster-level system tables:

| Table Name | Description |
|------|-----|
| tidb_cluster_slow_query | slow_query table data for all TiDB nodes |
| tidb_cluster_statements_summary | statements summary table Data for all TiDB nodes  |
| tidb_cluster_processlist | processlist table data for all TiDB nodes |

#### Configuration information of all nodes

For a large cluster, the way to obtain configuration by each node through HTTP API is cumbersome and inefficient. This proposal provides a full cluster configuration information system table, which simplifies the acquisition, filtering, and aggregation of the entire cluster configuration information.

See the following example for some expected results of this proposal:

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

#### Node hardware/system/load information system tables

According to the definition of `gRPC Service` protocol, each `ServerInfoItem` contains the name of the information and the corresponding key-value pair. When presented to the user, the type of the node and the node address need to be added.

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


#### Full-chain log system table

To search in the current log, users need to log in to multiple machines for retrieval respectively, and there is no easy way to sort the retrieval results of multiple machines by time. This proposal creates a new `tidb_cluster_log` system table to provide full-link logs, thereby simplifying the way to troubleshoot problems through logs and improving efficiency. This is achieved by pushing the log-filtering predicates down to the nodes through the `search_log` interface of the gRPC Diagnosis Service. The filtered logs will be eventually merged by time.

The following example shows the expected results of this proposal:

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

### Cluster diagnostics

In the current cluster topology, each component is dispersed. The data sources and data formats are heterogeneous. It is not convenient to perform cluster diagnosis through programmatic ways, so manual diagnosis is required. With the data system tables provided by the previous layers, each TiDB node has a stable Global View for the full cluster. Based on this, a problem diagnosis framework can be implemented. By defining diagnostic rules, we can quickly discover existing and potential problems on your cluster.

**Diagnostic rule definition**: Diagnostic rules are the logics for finding problems by reading data from various system tables and detecting abnormal data.

Diagnostic rules can be divided into three levels:

- Discovery of potential problems: For example, identify insufficient disk capacity by determining the ratio of disk capacity and disk usage
- Locate an existing problem: For example, by looking at the load, we can know that the thread pool of Coprocessor has reached the bottleneck
- Provide fix suggestions: For example, by analyzing the disk IO, we observe a high latency. A recommendation to replace the disk can be provided to you.

This proposal mainly focuses on implementing the diagnostic framework and some diagnostic rules. More diagnostic rules need to be gradually accumulated based on experience, with the ultimate goal of becoming an expert system that reduces lower the bar of using and operation difficulty. The following content does not detail the specific diagnostic rules, mainly focusing on the implementation of the diagnostic framework.

#### Diagnostic framework design

A variety of user scenarios must be considered for the diagnostic framework design  , including but not limited to:

- After selecting a fixed version, users won't upgrade the TiDB cluster version easily.
- User-defined diagnostic rules
- Loading new diagnostic rule without restarting the cluster
- The diagnosis framework needs to be easily integrated with the existing operation and maintenance system.
- Users may block some diagnostics. For example, if the user expects a heterogeneous system, heterogeneous diagnostic rules will be blocked by users.
- ...

Therefore, implementing a diagnostic system that supports hot rule loading is necessary. Currently there are the following options:

- Golang Plugin: Use different plugins to define diagnostic rules and load them into the TiDB processes
     - Advantages: Low development threshold in Golang
     - Disadvantages: Version management is error-prone and it requires compiling plugins with the same Golang version as the host TiDB
- Embedded Lua: Load Lua scripts in runtime or during startup. The script reads system table data from TiDB, evaluates and provide feedback based on diagnostic rules
     - Advantages: Lua is a fully host-dependent language with simple syntaxes; easy integration with the host
     - Disadvantages: Relying on another scripting language
- Shell Script: Shell supports process control, so you can define diagnostic rules with Shell
    - Advantages: easy to write, load and execute
    - Disadvantages: need to run on the machine where the MySQL client is installed

This proposal temporarily adopts the third option to write diagnostic rules using Shell. There is no intrusion into TiDB, and it also provides scalability for subsequent implementations of better solutions.