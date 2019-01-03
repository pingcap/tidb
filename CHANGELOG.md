# TiDB Change Log

All notable changes to this project will be documented in this file. See also [Release Notes](https://github.com/pingcap/docs/blob/master/releases/rn.md), [TiKV changelog](https://github.com/pingcap/tikv/blob/master/CHANGELOG.md) and [PD changelog](https://github.com/pingcap/pd/blob/master/CHANGELOG.md).

## [2.0.11] - 2019-1-3
- Fix the issue that the error is not handled properly when PD is in an abnormal condition [#8764](https://github.com/pingcap/tidb/pull/8764) [#8923](https://github.com/pingcap/tidb/pull/8923)
- Fix the issue that the `Rename` operation on a table in TiDB is not compatible with that in MySQL [#8809](https://github.com/pingcap/tidb/pull/8809) 
- Fix the issue that the error message is wrongly reported when the `ADMIN CHECK TABLE` operation is performed in the process of executing the `ADD INDEX` statement [#8750](https://github.com/pingcap/tidb/pull/8750)
- Fix the issue that the prefix index range is incorrect in some cases [#8877](https://github.com/pingcap/tidb/pull/8877) 
- Fix the panic issue of the `UPDATE` statement when columns are added in some cases [#8904](https://github.com/pingcap/tidb/pull/8904)


## [2.0.10] - 2018-12-18
- Fix the possible issue caused by canceling a DDL job [#8513](https://github.com/pingcap/tidb/pull/8513)
- Fix the issue that the `ORDER BY` and `UNION` clauses cannot quote the column including a table name [#8514](https://github.com/pingcap/tidb/pull/8514)
- Fix the issue that the `UNCOMPRESS` function does not judge the incorrect input length [#8607](https://github.com/pingcap/tidb/pull/8607)
- Fix the issue encountered by `ANSI_QUOTES SQL_MODE` when upgrading TiDB [#8575](https://github.com/pingcap/tidb/pull/8575)
- Fix the issue that `SELECT` returns the wrong result in some cases [#8570](https://github.com/pingcap/tidb/pull/8570)
- Fix the possible issue that TiDB cannot exit when it receives the exit signal [#8501](https://github.com/pingcap/tidb/pull/8501)
- Fix the issue that `IndexLookUpJoin` returns the wrong result in some cases [#8508](https://github.com/pingcap/tidb/pull/8508)
- Avoid pushing down the filter containing `GetVar` or `SetVar` [#8454](https://github.com/pingcap/tidb/pull/8454)
- Fix the issue that the result length of the `UNION` clauses is incorrect in some cases [#8491](https://github.com/pingcap/tidb/pull/8491)
- Fix the issue of `PREPARE FROM @var_name` [#8488](https://github.com/pingcap/tidb/pull/8488)
- Fix the panic issue when dumping statistics information in some cases [#8464](https://github.com/pingcap/tidb/pull/8464)
- Fix the statistics estimation issue of point queries in some cases [#8493](https://github.com/pingcap/tidb/pull/8493)
- Fix the panic issue when the returned default `enum` value is a string #[8476](https://github.com/pingcap/tidb/pull/8476)
- Fix the issue that too much memory is consumed in the scenario of wide tables [#8467](https://github.com/pingcap/tidb/pull/8467)
- Fix the issue encountered when Parser incorrectly formats the mod opcode [#8431](https://github.com/pingcap/tidb/pull/8431)  
- Fix the panic issue caused by adding foreign key constraints in some cases [#8421](https://github.com/pingcap/tidb/pull/8421) [#8410](https://github.com/pingcap/tidb/pull/8410)
- Fix the issue that the `YEAR` column type incorrectly converts the zero value [#8396](https://github.com/pingcap/tidb/pull/8396)
- Fix the panic issue occurred when the argument of the `VALUES` function is not a column [#8404](https://github.com/pingcap/tidb/pull/8404)
- Disable Plan Cache for statements containing subqueries  [#8395](https://github.com/pingcap/tidb/pull/8395)  


## [2.0.9] - 2018-11-19
- Fix the issue caused by the empty statistics histogram [#7927](https://github.com/pingcap/tidb/pull/7927)
- Fix the panic issue of the `UNION ALL` statement in some cases [#7942](https://github.com/pingcap/tidb/pull/7942) 
- Fix the stack overflow issue caused by wrong DDL Jobs [#7959](https://github.com/pingcap/tidb/pull/7959)
- Add the slow log for the `Commit` operation [#7983](https://github.com/pingcap/tidb/pull/7983)
- Fix the panic issue caused by the too large `Limit` value [#8004](https://github.com/pingcap/tidb/pull/8004)
- Support specifying the `utf8mb4` character set in the `USING` clause  [#8048](https://github.com/pingcap/tidb/pull/8048)
- Make the `TRUNCATE` built-in function support parameters of unsigned integer type [#8069](https://github.com/pingcap/tidb/pull/8069)
- Fix the selectivity estimation issue of the primary key for the statistics module in some cases [#8150](https://github.com/pingcap/tidb/pull/8150)
- Add the `Session` variable to control whether `_tidb_rowid` is allowed to be written in [#8126](https://github.com/pingcap/tidb/pull/8126)
- Fix the panic issue of `PhysicalProjection` in some cases [#8154](https://github.com/pingcap/tidb/pull/8154)
- Fix the unstable results of the `Union` statement in some cases [#8168](https://github.com/pingcap/tidb/pull/8168)
- Fix the issue that `NULL` is not returned by `values` in the non-`Insert` statement [#8179](https://github.com/pingcap/tidb/pull/8179)
- Fix the issue that the statistics module cannot clear the outdated data in some cases [#8184](https://github.com/pingcap/tidb/pull/8184)
- Make the maximum allowed running time for a transaction a configurable option [8209](https://github.com/pingcap/tidb/pull/8209) 
- Fix the wrong comparison algorithm of `expression rewriter` in some cases [#8288](https://github.com/pingcap/tidb/pull/8288)
- Eliminate the extra columns generated by the `UNION ORDER BY` statement [#8307](https://github.com/pingcap/tidb/pull/8307)
- Support the `admin show next_row_id` statement [#8274](https://github.com/pingcap/tidb/pull/8274)
- Fix the escape issue of special characters in the `Show Create Table` statement [#8321](https://github.com/pingcap/tidb/pull/8321)
- Fix the unexpected errors in the `UNION` statement in some cases [#8318](https://github.com/pingcap/tidb/pull/8318)
- Fix the issue that canceling a DDL job causes no rollback of a schema in some cases [#8312](https://github.com/pingcap/tidb/pull/8312)
- Change `tidb_max_chunk_size` to a global variable [#8333](https://github.com/pingcap/tidb/pull/8333)
- Add an upper bound to the `Scan` command of ticlient, to avoid overbound scan [#8309](https://github.com/pingcap/tidb/pull/8309), [#8310](https://github.com/pingcap/tidb/pull/8310) 



## [2.0.8] - 2018-10-16
### Improvements
- Slow down the AUTO_INCREMENT ID increasing speed when the `Update` statement does not modify the corresponding AUTO_INCREMENT column [#7846](https://github.com/pingcap/tidb/pull/7846) 
### Bug fixes
- Quickly create a new etcd session to recover the service when the PD leader goes down [#7810](https://github.com/pingcap/tidb/pull/7810)
- Fix the issue that the time zone is not considered when the default value of the `DateTime` type is calculated [#7672](https://github.com/pingcap/tidb/pull/7672)
- Fix the issue that `duplicate key update` inserts values incorrectly in some conditions [#7685](https://github.com/pingcap/tidb/pull/7685)
- Fix the issue that the predicate conditions of `UnionScan` are not pushed down [#7726](https://github.com/pingcap/tidb/pull/7726)
- Fix the issue that the time zone is not correctly handled when you add the `TIMESTAMP` index [#7812](https://github.com/pingcap/tidb/pull/7812)
- Fix the memory leak issue caused by the statistics module in some conditions [#7864](https://github.com/pingcap/tidb/pull/7864) 
- Fix the issue that the results of `ANALYZE` cannot be obtained in some abnormal conditions [#7871](https://github.com/pingcap/tidb/pull/7871) 
- Do not fold the function `SYSDATE`, to ensure the returned results are correct [#7894](https://github.com/pingcap/tidb/pull/7894) 
- Fix the `substring_index` panic issue in some conditions [#7896](https://github.com/pingcap/tidb/pull/7896) 
- Fix the issue that `OUTER JOIN` is mistakenly converted to `INNER JOIN` in some conditions [#7899](https://github.com/pingcap/tidb/pull/7899) 


## [2.0.7] - 2018-09-07
### New Feature
  - Add the `PROCESSLIST` table in `information_schema` [#7286](https://github.com/pingcap/tidb/pull/7286)
### Improvements
  - Collect more details about SQL statement execution and output the information in the `SLOW QUERY` log [#7364](https://github.com/pingcap/tidb/pull/7364)
  - Drop the partition information in `SHOW CREATE TABLE` [#7388](https://github.com/pingcap/tidb/pull/7388)
  - Improve the execution efficiency of the `ANALYZE` statement by setting it to the RC isolation level and low priority [#7500](https://github.com/pingcap/tidb/pull/7500)
  - Speed up adding a unique index [#7562](https://github.com/pingcap/tidb/pull/7562)
  - Add an option of controlling the DDL concurrency [#7563](https://github.com/pingcap/tidb/pull/7563)
### Bug Fixes
  - Fix the issue that `USE INDEX(`PRIMARY`)` cannot be used in a table whose primary key is an integer [#7298](https://github.com/pingcap/tidb/pull/7298)
  - Fix the issue that `Merge Join` and `Index Join` output incorrect results when the inner row is `NULL` [#7301](https://github.com/pingcap/tidb/pull/7301)
  - Fix the issue that `Join` outputs an incorrect result when the chunk size is set too small [#7315](https://github.com/pingcap/tidb/pull/7315)
  - Fix the panic issue caused by a statement of creating a table involving `range column` [#7379](https://github.com/pingcap/tidb/pull/7379)
  - Fix the issue that `admin check table` mistakenly reports an error of a time-type column [#7457](https://github.com/pingcap/tidb/pull/7457)
  - Fix the issue that the data with a default value `current_timestamp` cannot be queried using the `=` condition [#7467](https://github.com/pingcap/tidb/pull/7467)
  - Fix the issue that the zero-length parameter inserted by using the `ComStmtSendLongData` command is mistakenly parsed to NULL [#7508](https://github.com/pingcap/tidb/pull/7508)
  - Fix the issue that `auto analyze` is repeatedly executed in specific scenarios [#7556](https://github.com/pingcap/tidb/pull/7556)
  - Fix the issue that the parser cannot parse a single line comment ended with a newline character [#7635](https://github.com/pingcap/tidb/pull/7635)

## [2.0.6] - 2018-08-06
### Improvements
  - Make “set system variable” log shorter to save disk space [#7031](https://github.com/pingcap/tidb/pull/7031)
  - Record slow operations during the execution of `ADD INDEX` in the log, to make troubleshooting easier [#7083](https://github.com/pingcap/tidb/pull/7083)
  - Reduce transaction conflicts when updating statistics [#7138](https://github.com/pingcap/tidb/pull/7138)
  - Improve the accuracy of row count estimation when the values pending to be estimated exceeds the statistics range [#7185](https://github.com/pingcap/tidb/pull/7185)
  - Choose the table with a smaller estimated row count as the outer table for `Index Join` to improve its execution efficiency [#7277](https://github.com/pingcap/tidb/pull/7277)
  - Add the recover mechanism for panics occurred during the execution of `ANALYZE TABLE`, to avoid that the tidb-server is unavailable caused by abnormal behavior in the process of collecting statistics [#7228](https://github.com/pingcap/tidb/pull/7228)
  - Return `NULL` and the corresponding warning when the results of `RPAD`/`LPAD` exceed the value of the `max_allowed_packet` system variable, compatible with MySQL [#7244](https://github.com/pingcap/tidb/pull/7244)
  - Set the upper limit of placeholders count in the `PREPARE` statement to 65535, compatible with MySQL [#7250](https://github.com/pingcap/tidb/pull/7250)
### Bug Fixes
  - Fix the issue that the `DROP USER` statement is incompatible with MySQL behavior in some cases [#7014](https://github.com/pingcap/tidb/pull/7014)
  - Fix the issue that statements like `INSERT`/`LOAD DATA` meet OOM aftering opening `tidb_batch_insert` [#7092](https://github.com/pingcap/tidb/pull/7092)
  - Fix the issue that the statistics fail to automatically update when the data of a table keeps updating [#7093](https://github.com/pingcap/tidb/pull/7093)
  - Fix the issue that the firewall breaks inactive gPRC connections [#7099](https://github.com/pingcap/tidb/pull/7099)
  - Fix the issue that prefix index returns a wrong result in some scenarios [#7126](https://github.com/pingcap/tidb/pull/7126)
  - Fix the panic issue caused by outdated statistics in some scenarios [#7155](https://github.com/pingcap/tidb/pull/7155)
  - Fix the issue that one piece of index data is missed after the `ADD INDEX` operation in some scenarios [#7156](https://github.com/pingcap/tidb/pull/7156)
  - Fix the wrong result issue when querying `NULL` values using the unique index in some scenarios [#7172](https://github.com/pingcap/tidb/pull/7172)
  - Fix the messy code issue of the `DECIMAL` multiplication result in some scenarios [#7212](https://github.com/pingcap/tidb/pull/7212)
  - Fix the wrong result issue of `DECIMAL` modulo operation in some scenarios [#7245](https://github.com/pingcap/tidb/pull/7245)
  - Fix the issue that the `UPDATE`/`DELETE` statement in a transaction returns a wrong result under some special sequence of statements [#7219](https://github.com/pingcap/tidb/pull/7219)
  - Fix the panic issue of the `UNION ALL`/`UPDATE` statement during the process of building the execution plan in some scenarios [#7225](https://github.com/pingcap/tidb/pull/7225)
  - Fix the issue that the range of prefix index is calculated incorrectly in some scenarios [#7231](https://github.com/pingcap/tidb/pull/7231)
  - Fix the issue that the `LOAD DATA` statement fails to write the binlog in some scenarios [#7242](https://github.com/pingcap/tidb/pull/7242)
  - Fix the wrong result issue of `SHOW CREATE TABLE` during the execution process of `ADD INDEX` in some scenarios [#7243](https://github.com/pingcap/tidb/pull/7243)
  - Fix the issue that panic occurs when `Index Join` does not initialize timestamps in some scenarios [#7246](https://github.com/pingcap/tidb/pull/7246)
  - Fix the false alarm issue when `ADMIN CHECK TABLE` mistakenly uses the timezone in the session [#7258](https://github.com/pingcap/tidb/pull/7258)
  - Fix the issue that `ADMIN CLEANUP INDEX` does not clean up the index in some scenarios [#7265](https://github.com/pingcap/tidb/pull/7265)
  - Disable the Read Committed isolation level [#7282](https://github.com/pingcap/tidb/pull/7282)

## [2.0.5] - 2018-07-06
### New Features
  - Add the `tidb_disable_txn_auto_retry` system variable which is used to disable the automatic retry of transactions [#6877](https://github.com/pingcap/tidb/pull/6877)
### Improvements
  - Optimize the cost calculation of `Selection` to make the result more accurate [#6989](https://github.com/pingcap/tidb/pull/6989)
  - Select the query condition that completely matches the unique index or the primary key as the query path directly [#6966](https://github.com/pingcap/tidb/pull/6966)
  - Execute necessary cleanup when failing to start the service [#6964](https://github.com/pingcap/tidb/pull/6964)
  - Handle `\N` as NULL in the `Load Data` statement [#6962](https://github.com/pingcap/tidb/pull/6962)
  - Optimize the code structure of CBO [#6953](https://github.com/pingcap/tidb/pull/6953)
  - Report the monitoring metrics earlier when starting the service [#6931](https://github.com/pingcap/tidb/pull/6931)
  - Optimize the format of slow queries by removing the line breaks in SQL statements and adding user information [#6931](https://github.com/pingcap/tidb/pull/6931)
  - Support multiple asterisks in comments [#6931](https://github.com/pingcap/tidb/pull/6931)
### Bug Fixes
  - Fix the issue that `KILL QUERY` always requires SUPER privilege [#6931](https://github.com/pingcap/tidb/pull/6931)
  - Fix the issue that users might fail to login when the number of users exceeds 1024 [#6986](https://github.com/pingcap/tidb/pull/6986)
  - Fix an issue about inserting unsigned `float`/`double` data [#6940](https://github.com/pingcap/tidb/pull/6940)
  - Fix the compatibility of the `COM_FIELD_LIST` command to resolve the panic issue in some MariaDB clients [#6929](https://github.com/pingcap/tidb/pull/6929)
  - Fix the `CREATE TABLE IF NOT EXISTS LIKE` behavior [#6928](https://github.com/pingcap/tidb/pull/6928)
  - Fix an issue in the process of TopN pushdown [#6923](https://github.com/pingcap/tidb/pull/6923)
  - Fix the ID record issue of the currently processing row when an error occurs in executing `Add Index` [#6903](https://github.com/pingcap/tidb/pull/6903)


## [2.0.4] - 2018-06-15
### New Features
  - Support the `ALTER TABLE t DROP COLUMN a CASCADE` syntax
  - Support configuring the value of `tidb_snapshot` to TSO
### Improvements
  - Refine the display of statement types in monitoring items
  - Optimize the accuracy of query cost estimation
  - Configure the `backoff max delay` parameter of gRPC
  - Support cofiguring the memory threshold of a single statement in the configuration file
### Bug Fixes
  - Fix the side effects of the `Cast Decimal` data
  - Fix the wrong result issue of the `Merge Join` operator in specific scenarios
  - Fix the issue of converting the Null object to String
  - Fix the issue of casting Json type of to Json type
  - Refactor the error of Optimizer
  - Fix the issue that the result order is not consistent with MySQL in the condition of `Union` + `OrderBy`
  - Fix the compliance rules issue when the `Union` statement checks the `Limit/OrderBy` clause
  - Fix the compatibility issue of the `Union All` result
  - Fix a bug in predicate pushdown
  - Fix the compatibility issue of the `Union` statement with the `For Update` clause
  - Fix the issue that the `concat_ws` function mistakenly truncates the result

## [2.0.3] - 2018-06-01
### New Features
  - Support modifying the log level online
  - Support the `COM_CHANGE_USER` command
  - Support using the `TIME` type parameters under the binary protocol
### Improvements
  - Optimize the cost estimation of query conditions with the `BETWEEN` expression
  - Do not display the `FOREIGN KEY` information in the result of `SHOW CREATE TABLE`
  - Optimize the cost estimation for queries with the `LIMIT` clause
### Bug Fixes
  - Fix the issue about the `YEAR` type as the unique index
  - Fix the issue about `ON DUPLICATE KEY UPDATE` in conditions without the unique index
  - Fix the compatibility issue of the `CEIL` function
  - Fix the accuracy issue of the `DIV` calculation in the `DECIMAL` type
  - Fix the false alarm of `ADMIN CHECK TABLE`
  - Fix the panic issue of `MAX`/`MIN` under specific expression parameters
  - Fix the issue that the result of `JOIN` is null in special conditions
  - Fix the `IN` expression issue when building and querying Range
  - Fix a Range calculation issue when using `Prepare` to query and `Plan Cache` is enabled
  - Fix the issue that the Schema information is frequently loaded in abnormal conditions

## [2.0.2] - 2018-05-21
### New Features
  - Support using the USE INDEX syntax in the `Delete` statement
  - Add the timeout mechanism for writing Binlog
### Bug Fixes
  - Fix the issue of pushing down the Decimal division expression
  - Forbid using the `shard_row_id_bits` feature in columns with Auto-Increment

## [2.0.1] - 2018-05-16
### New Features
  - Add the `tidb_auto_analyze_ratio` session variable to control the threshold value of automatic statistics update
  - Add an option for TiDB to control the behaviour of Binlog failure
### Improvements
  - Update the progress of `Add Index` to the DDL job information in real time
  - Refactor the `Coprocessor` slow log，distinguish the scenario of tasks with long processing time and long waiting time
  - Log nothing when meeting MySQL protocol handshake error. Avoid too many logs caused by load balancer keep alive mechanism
  - Refine “Out of range value for column” error message
  - Change the behaviour of handling `SIGTERM`, do not wait for all queries to terminate anymore
### Bug Fixes
  - Fix an issue that not all residual states are cleaned up when the transaction commit fails
  - Fix a bug about adding indexes in some conditions
  - Fix the correctness related issue when DDL modifies surface operations in some concurrent scenarios
  - Fix a bug that the result of `LIMIT` is incorrect in some conditions
  - Fix a capitalization issue of the `ADMIN CHECK INDEX` statement to make its index name case insensitive
  - Fix a compatibility issue about the `UNION` statement
  - Fix a compatibility issue when inserting data of `TIME` type
  - Fix a goroutine leak issue caused by `copIteratorTaskSender` in some conditions
  - Fix a bug when there is a subquery in an `Update` statement

## [2.0.0] - 2018-04-27
* SQL Optimizer
  - Use more compact data structure to reduce the memory usage of statistics information
  - Speed up the loading statistics information when starting a tidb-server process
  - Support updating statistics information dynamically [experimental]
  - Optimize the cost model to provide more accurate query cost evaluation
  - Use `Count-Min Sketch` to estimate the cost of point queries more accurately
  - Support analyzing more complex conditions to make full use of indexes
  - Support manually specifying the `Join` order using the `STRAIGHT_JOIN` syntax
  - Use the Stream Aggregation operator when the `GROUP BY` clause is empty to improve the performance
  - Support using indexes for the `MAX/MIN` function
  - Optimize the processing algorithms for correlated subqueries to support decorrelating more types of correlated subqueries and transform them to `Left Outer Join`
  - Extend `IndexLookupJoin` to be used in matching the index prefix
* SQL Execution Engine
  - Refactor all operators using the Chunk architecture, improve the execution performance of analytical queries, and reduce memory usage.There is a significant improvement in the TPC-H benchmark result.
  - Support the Streaming Aggregation operators pushdown
  - Optimize the `Insert Into Ignore` statement to improve the performance by over 10 times
  - Optimize the `Insert On Duplicate Key Update` statement to improve the performance by over 10 times
  - Optimize `Load Data` to improve the performance by over 10 times
  - Push down more data types and functions to TiKV
  - Support computing the memory usage of physical operators, and specifying the processing behavior in the configuration file and system variables when the memory usage exceeds the threshold
  - Support limiting the memory usage by a single SQL statement to reduce the risk of OOM
  - Support using implicit RowID in CRUD operations
  - Improve the performance of point queries
* Server
  - Support the Proxy Protocol
  - Add more monitoring metrics and refine the log
  - Support validating the configuration files
  - Support obtaining the information of TiDB parameters through HTTP API
  - Resolve Lock in the Batch mode to speed up garbage collection
  - Support multi-threaded garbage collection
  - Support TLS
* Compatibility
  - Support more MySQL syntaxes
  - Support modifying the `lower_case_table_names` system variable in the configuration file to support the OGG data synchronization tool
  - Improve compatibility with the Navicat management tool
  - Support displaying the table creating time in `Information_Schema`
  - Fix the issue that the return types of some functions/expressions differ from MySQL
  - Improve compatibility with JDBC
  - Support more SQL Modes
* DDL
  - Optimize the `Add Index` operation to greatly improve the execution speed in some scenarios
  - Attach a lower priority to the `Add Index` operation to reduce the impact on online business
  - Output more detailed status information of the DDL jobs in `Admin Show DDL Jobs`
  - Support querying the original statements of currently running DDL jobs using `Admin Show DDL Job Queries JobID`
  - Support recovering the index data using `Admin Recover Index` for disaster recovery
  - Support modifying Table Options using the `Alter` statement

## [2.0.0-rc.5] - 2018-04-17
### New Features
* Support showing memory usage of the executing statements in the Show Process List statement
* Support setting the table comment using the `Alter` statement
### Improvements
* Clean up the written data while rolling back the `Add Index` operation, to reduce consumed space
* Optimize the insert on duplicate key update statement to improve the performance by 10 times
### Bug Fixes
* Fix the issue about applying the Top-N pushdown rule
* Fix the issue that `Alter Table Modify Column` reports an error in extreme conditions
* Fix the issue about the type of the results returned by the `UNIX_TIMESTAMP` function
* Fix the issue that the NULL value is inserted while adding NOT NULL columns
* Fix the estimation of the number of rows for the columns that contain NULL values
* Fix the zero value of the Binary type
* Fix the BatchGet issue within a transaction

## [2.0.0-rc.4] - 2018-04-01
### New Features
* Support `SHOW GRANTS FOR CURRENT_USER();`
* Support the `SET TRANSACTION` syntax
* Support displaying floating point numbers using scientific notation
### Improvements
* Improve the execution performance of DecodeBytes
* Optimize LIMIT 0 to TableDual, to avoid building useless execution plans
### Bug Fixes
* Fix the issue that the Expression in UnionScan is not cloned
* Fix the potential goroutine leak issue in copIterator
* Fix the issue that admin check table misjudges the unique index including null
* Fix the type inference issue during binary literal computing
* Fix the issue in parsing the `CREATE VIEW` statement
* Fix the panic issue when one statement contains both ORDER BY and LIMIT 0

## [2.0.0-rc.3] - 2018-03-23
### New Features
* Support closing the `Join Reorder` optimization in the optimizer using `STRAIGHT_JOIN`
* Output more detailed status information of DDL jobs in `ADMIN SHOW DDL JOBS`
* Support querying the original statements of currently running DDL jobs using ADMIN SHOW DDL JOB QUERIES
* Support recovering the index data using `ADMIN RECOVER INDEX` for disaster recovery
* Attach a lower priority to the `ADD INDEX` operation to reduce the impact on online business
* Support aggregation functions with JSON type parameters, such as SUM/AVG
* Support modifying the `lower_case_table_names` system variable in the configuration file, to support the OGG data synchronization tool
* Support using implicit RowID in CRUD operations
### Improvements
* Improve compatibility with the Navicat management tool
* Use the Stream Aggregation operator when the GROUP BY substatement is empty, to increase the speed
* Optimize the execution speed of `ADD INDEX` to greatly increase the speed in some scenarios
* Optimize checks on length and precision of the floating point type, to improve compatibility with MySQL
* Improve the parsing error log of time type and add more error information
* Improve memory control and add statistics about IndexLookupExecutor memory
### Bug Fixes
* Fix the wrong result issue of `MAX`/`MIN` in some scenarios
* Fix the issue that the result of `Sort Merge Join` does not show in order of Join Key in some scenarios
* Fix the error of comparison between uint and int in boundary conditions

## [2.0.0-rc.2] - 2018-03-15
Only TiKV has this release

## [2.0.0-rc.1] - 2018-03-09
### New Features
* Support limiting the memory usage by a single SQL statement, to reduce the risk of OOM
* Support pushing the Stream Aggregate operator down to TiKV
* Support validating the configuration file
* Support obtaining the information of TiDB configuration through HTTP API
### Improvements
* Improve the compatibility with Navicat
* Improve the optimizer and extract common expressions with multiple OR conditions, to choose better query plan
* Improve the optimizer and convert subqueries to Join operators in more scenarios, to choose better query plan
* Compatible with more MySQL syntax in Parser
* Resolve Lock in the Batch mode to increase the garbage collection speed
* Optimize the Add Index operation and give lower priority to all write and read operations, to reduce the impact on online business
### Bug Fixes
* Fix the length of Boolean field to improve compatibility

## [1.1.0-beta] - 2018-02-24
### New Features
* Add more monitoring metrics and refine the log
* Add the tidb_config session variable to output the current TiDB configuration
* Support displaying the table creating time in information_schema
### Improvements
* Compatible with more MySQL syntax
* Optimize queries containing the MaxOneRow operator
* Configure the size of intermediate result sets generated by Join, to further reduce the memory used by Join
* Optimize the query performance of the SQL engine to improve the test result of the Sysbench Select/OLTP by 10%
* Improve the computing speed of subqueries in the optimizer using the new execution engine; compared with TiDB 1.0, TiDB 1.1 Beta has great improvement in tests like TPC-H and TPC-DS
### Bug Fixes
* Fix the panic issue in the Union and Index Join operators
* Fix the wrong result issue of the Sort Merge Join operator in some scenarios
* Fix the issue that the Show Index statement shows indexes that are in the process of adding
* Fix the failure of the Drop Stats statement

## [1.0.8] - 2018-02-11
### New Features
* Add limitation (Configurable, the default value is 5000) to the DML statements number within a transaction
### Improvements
* Improve the stability of the GC process by ignoring the regions with GC errors
* Run GC concurrently to accelerate the GC process
* Provide syntax support for the CREATE INDEX statement
* Optimize the performance of the InsertIntoIgnore statement
### Bug Fixes
* Fix issues in the `Outer Join` result in some scenarios
* Fix the issue in the `ShardRowID` option
* Fix an issue in the Table/Column aliases returned by the Prepare statement
* Fix an issue in updating statistics delta
* Fix a panic error in the `Drop Column` statement
* Fix a DML issue when running the `Add Column After` statement

## [1.0.7] - 2018-01-22
### Improvements
* Optimize the `FIELD_LIST` command
* Fix data race of the information schema
* Avoid adding read-only statements to history
* Add the session variable to control the log query
* Add schema info API for the http status server
* Update the behavior when RunWorker is false in DDL
* Improve the stability of test results in statistics
* Support PACK_KEYS syntax for the CREATE TABLE statement
* Add row_id column for the null pushdown schema to optimize performance
### Bug Fixes
* Fix the resource leak issue in statistics
* Fix the goroutine leak issue
* Fix an issue about `IndexJoin`

## [1.1.0-alpha] - 2018-01-19
### New Features
* Support the PROXY protocol
### Improvements
* Support more syntax
* Reduce memory usage of statistics info using more compact structure
* Speed up loading statistics info when starting tidb-server
* Provide more accurate query cost evaluation
* Use Count-Min Sketch to evaluate the cost of queries using unique index more accurately
* Support more complex conditions to make full use of index
* Refactor all executor operators using Chunk architecture, improve the execution performance of analytical statements and reduce memory usage
* Optimize performance of the `INSERT INGORE` statement
* Push down more types and functions to TiKV
* Support more `SQL_MODE`
* Optimize the `Load Data` performance to increase the speed by 10 times
* Optimize the `Use Database` performance
* Support statistics on the memory usage of physical operators
