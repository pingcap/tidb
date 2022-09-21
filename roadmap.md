# TiDB Roadmap

This roadmap brings you what's coming in the 1-year future, so you can see the new features or improvements in advance, follow the progress, learn about the key milestones on the way, and give feedback as the development work goes on.

In the course of development, this roadmap is subject to change based on user needs and feedback. If you have a feature request or want to prioritize a feature, please file an issue on [GitHub](https://github.com/pingcap/tidb/issues).

## TiDB kernel

<table>
<thead>
  <tr>
    <th>Scenario</th>
    <th>Feature</th>
    <th>Description</th>
    <th>Estimated Time of Delivery</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td rowspan="2">Support JSON</td>
    <td>Support JSON function.</td>
    <td>In business scenarios that require flexible schema definitions (such as SaaS, Web3, and gaming), the application can use JSON to store information for ODS, transaction indicators, commodities, game characters, and props.</td>
    <td>September 2022</td>
  </tr>
  <tr>
    <td><ul><li>Support expression indexes.</li><li>Support generated columns.</li></ul></td>
    <td>Provide query acceleration for specific field indexes in JSON scenarios.</td>
    <td>October 2022 </td>
  </tr>
  <tr>
    <td>Flashback</td>
    <td>Support cluster-level flashback.</td>
    <td>In game rollback scenarios, the flashback can be used to achieve a fast rollback of the current cluster. This solves the common problems in the gaming industry such as version errors and bugs.</td>
    <td>November 2022</td>
  </tr>
  <tr>
    <td>TiFlash result write-back (supports <code>INSERT INTO SELECT</code>)</td>
    <td><ul><li>Easily write the analysis results in TiFlash back to TiDB.</li><li>Provide complete ACID transactions, more convenient and reliable than general ETL solutions.</li><li>Set a hard limit on the threshold of intermediate result size, and report an error if the threshold is exceeded.</li><li>Support fully distributed transactions, and remove or relax the limit on the intermediate result size.</li></ul></td>
    <td>These features combined enable a way to materialize intermediate results. The analysis results can be easily reused, which reduces unnecessary ad-hoc queries, improves the performance of BI and other applications (by pulling results directly) and reduces system load (by avoiding duplicated computation), thereby improving the overall data pipeline efficiency and reducing costs. It will make TiFlash an online service.</td>
    <td>November 2022</td>
  </tr>
  <tr>
    <td>Time to live (TTL)</td>
    <td>Support automatically deleting expired table data based on custom rules. </td>
    <td>This feature enables automatic data cleanup in limited data archiving scenarios.</td>
    <td>December 2022</td>
  </tr>
  <tr>
    <td>Multi-value Index</td>
    <td>Support array index.</td>
    <td>Array is one of the commonly used data types in JSON scenarios. For inclusive queries in arrays, multi-value indexes can efficiently improve the query speed. </td>
    <td>December 2022</td>
  </tr>
  <tr>
    <td>TiFlash kernel optimization</td>
    <td><ul><li>FastScan provides weak consistency but faster table scan capability.</li><li>Further optimize the join order, shuffle, and exchange algorithms to improve computing efficiency and boost performance for complex queries.</li><li>Add a fine-grained data sharding mechanism to optimize the <code>COUNT(DISTINCT)</code> function and high cardinality aggregation.</li></ul></td>
    <td>Improve the basic computing capability of TiFlash, and optimize the performance and reliability of the underlying algorithms of the columnar storage and MPP engine.</td>
    <td>December 2022</td>
  </tr>
  <tr>
    <td>TiDB proxy</td>
    <td>Implement automatic load balancing so that upgrading a cluster or modifying configurations does not affect the application. After scaling out or scaling in the cluster, the application can automatically rebalance the connection without reconnecting.</td>
    <td>In scenarios such as upgrades and configuration changes, TiDB proxy is more business-friendly.</td>
    <td>December 2022</td>
  </tr>
  <tr>
    <td>PB-level scalability</td>
    <td>Support huge region size</td>
    <td>Scenarios with fast business growth and a large amount of data</td>
    <td>January 2023</td>
  </tr>
  <tr>
    <td>DDL parallel framework</td>
    <td>Implement a single DDL parallel execution framework for DDL acceleration</td>
    <td>Accelerate DDL in scenarios such as creating indexes for large tables, modifying a column's default value, and modifying a column type.</td>
    <td>January 2023</td>
  </tr>
  <tr>
    <td>Non-prepared Plan Cache</td>
    <td>Support plan cache for general SQL statements in a session to save cache resources, improve the hit rate of general execution plans, and improve SQL performance.</td>
    <td>Non-prepared plan cache. Improve real-time and throughputs of OLTP in general scenarios, save PoC time, and increase PoC win rate.</td>
    <td>January 2023</td>
  </tr>
  <tr>
    <td>SQL blocklist</td>
    <td>Support a rule-based SQL blocklist mechanism.</td>
    <td>In multi-service aggregation scenarios, provide SQL management and control capabilities, and improve cluster stability by prohibiting high-resource-consuming SQL statements.</td>
    <td>February 2023</td>
  </tr>
  <tr>
    <td>Resource management</td>
    <td>Provide a basic resource management and control framework to effectively control the resource squeeze of background tasks on front-end tasks (user operations), and improve cluster stability.</td>
    <td>Refine resource management in the multi-service aggregation scenario.</td>
    <td>March 2023</td>
  </tr>
  <tr>
    <td>Prepared Plan Cache</td>
    <td>Support in-session subquery, expression index, and prepared plan cache for Partition.</td>
    <td>Expand the usage scenarios of plan cache.</td>
    <td>April 2023</td>
  </tr>
  <tr>
    <td>PB-level scalability</td>
    <td>Support dynamic region size adjustment (heterogeneous).</td>
    <td>For scenarios with fast business growth and a large amount of data.</td>
    <td>June 2023</td>
  </tr>
  <tr>
    <td>Instance plan cache</td>
    <td>Support cross-session plan cache, save cache resources, improve the hit rate of general execution plans, and improve SQL performance.</td>
    <td>In general scenarios, reuse execution plans to improve memory utilization and to achieve higher throughputs.</td>
    <td>July 2023</td>
  </tr>
</tbody>
</table>

## Diagnosis and maintenance

<table>
<thead>
  <tr>
    <th>Scenario</th>
    <th>Feature</th>
    <th>Description</th>
    <th>Estimated Time of Delivery</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td>SQL tuning for HTAP workloads</td>
    <td><ul><li>Provide SQL execution information from the perspective of applications.</li><li>Provide suggestions on optimizing SQL for TiFlash and TiKV in HTAP workloads.</li></ul></td>
    <td><ul><li>Provide a dashboard that displays a SQL execution overview from the perspective of applications in HTAP workloads.</li><li>For one or several HTAP scenarios, provide suggestions on SQL optimization.</li></ul></td>
    <td>October 2022</td>
  </tr>
</tbody>
</table>

## Data backup and migration

<table>
<thead>
  <tr>
    <th>Scenario</th>
    <th>Feature</th>
    <th>Description</th>
    <th>Estimated Time of Delivery</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td rowspan="2">Backup and restore</td>
    <td>EBS snapshot-based backup</td>
    <td>BR provides an MVP version of EBS snapshot-based backup and restore.</td>
    <td>September 2022</td>
  </tr>
  <tr>
    <td>EBS snapshot-based backup and restore</td>
    <td>BR provides a complete version of EBS snapshot-based backup and restore.</td>
    <td>December 2022</td>
  </tr>
  <tr>
    <td>Point-in-time recovery (PITR)</td>
    <td>Table-level and database-level PITR</td>
    <td>BR supports table-level or database-level PITR.</td>
    <td>October 2022</td>
  </tr>
  <tr>
    <td rowspan="2">Data replication to downstream systems via TiCDC</td>
    <td>Reduce TiCDC replication latency in planned offline scenarios.</td>
    <td>When TiKV, TiDB, PD, or TiCDC nodes are offline in a planned maintenance window, the replication latency of TiCDC can be reduced to less than 10 seconds.</td>
    <td>October 2022</td>
  </tr>
  <tr>
    <td>Support replicating data to object storage such as S3.</td>
    <td>TiCDC supports replicating data changes to common object storage services.</td>
    <td>November 2022</td>
  </tr>
  <tr>
    <td>Data migration</td>
    <td>TiDB Lightning supports table-level and partition-level incremental data import.</td>
    <td>TiDB Lightning provides comprehensive table-level and partition-level data import capabilities.</td>
    <td>December 2022</td>
  </tr>
</tbody>
</table>

## Security

<table>
<thead>
  <tr>
    <th>Scenario</th>
    <th>Feature</th>
    <th>Description</th>
    <th>Estimated Time of Delivery</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td rowspan="2">ShangMi (SM) algorithms</td>
    <td>Encryption-at-rest (TiKV and TiFlash) supports the SM4 algorithm.</td>
    <td>Supports encrypting data stored in TiKV and TiFlash based on the SM4 algorithm.</td>
    <td>September 2022</td>
  </tr>
  <tr>
    <td>TiDB authentication supports the SM3 algorithm. </td>
    <td>Provide a user authentication plugin based on the SM3 algorithm, which encrypts the password using the SM3 algorithm.</td>
    <td>September 2022</td>
  </tr>
  <tr>
    <td>Log redaction</td>
    <td><ul><li>Data redaction in execution plans in TiDB Dashboard</li><li>Data redaction in TiDB-related logs</li></ul></td>
    <td>Redact sensitive information in execution plans and various logs to enhance the security of user data.</td>
    <td>October 2022</td>
  </tr>
  <tr>
    <td>Password complexity check</td>
    <td>A strong password is required.</td>
    <td>To improve security, empty passwords and weak passwords are not allowed.<br>The required password length is not less than 8. The password must contain an uppercase letter, a lowercase letter, a number, and a character.</td>
    <td>November 2022</td>
  </tr>
  <tr>
    <td>Password expiration</td>
    <td>TiDB provides password expiration management and requires users to change passwords regularly.</td>
    <td>Reduce the security risk of password cracking or leakage caused by using the same password for a long time.</td>
    <td>December 2022</td>
  </tr>
  <tr>
    <td>Password policy management</td>
    <td>TiDB provides a password reuse mechanism and brute-force cracking prevention capabilities.</td>
    <td>TiDB supports password policy management to protect password security.</td>
    <td>March 2023</td>
  </tr>
  <tr>
    <td>Column-level access control</td>
    <td>TiDB supports column-level privilege management.</td>
    <td>TiDB already supports cluster-level, database-level, and table-level privilege management. On top of that, TiDB will support column-level privilege management to meet the principle of least privilege and provide fine-grained data access control.</td>
    <td>June 2023</td>
  </tr>
  <tr>
    <td>Audit logging capability refactor</td>
    <td>Support configurable audit log policies, configurable audit filters (filter by objects, users, and operation types), and visual access to audit logs.</td>
    <td>Improve the completeness and usability of the audit log feature.</td>
    <td>July 2023</td>
  </tr>
</tbody>
</table>
