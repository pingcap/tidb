# TiDB Roadmap

This roadmap brings you what's coming in the 1-year future, so you can see the new features or improvements in advance, follow the progress, learn about the key milestones on the way, and give feedback as the development work goes on. In the course of development, this roadmap is subject to change based on user needs and feedback. If you have a feature request or want to prioritize a feature, please file an issue on [GitHub](https://github.com/pingcap/tidb/issues).

> **Safe harbor statement:**
>
> *Any unreleased features discussed or referenced in our documents, roadmaps, blogs, websites, press releases, or public statements that are not currently available ("unreleased features") are subject to change at our discretion and may not be delivered as planned or at all. Customers acknowledge that purchase decisions are solely based on features and functions that are currently available, and that PingCAP is not obliged to deliver aforementioned unreleased features as part of the contractual agreement unless otherwise stated.*

## TiDB kernel

<table>
<thead>
  <tr>
    <th>Domain</th>
    <th>Feature</th>
    <th>Description</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td rowspan="3">Scalability &amp; Stability</td>
    <td>Support resource management framework.</td>
    <td><ul><li>Provide a basic resource management and control framework to effectively control the resource squeeze of background tasks on front-end tasks (user operations), and improve cluster stability.</li><li>Refine resource management in the multi-service aggregation scenario.</li></ul></td>
  </tr>
  <tr>
    <td>Enhance the plan cache feature.</td>
    <td><ul><li>Support in-session subquery, expression index, and prepared plan cache for partitions, which expands the usage scenarios of plan cache.</li><li>Support plan cache for general SQL statements in a session to save cache resources, improve the hit rate of general execution plans, and improve SQL performance.</li><li>Support cross-session plan cache, save cache resources, improve the hit rate of general execution plans, and improve SQL performance. In general scenarios, reusing execution plans can improve memory utilization and to achieve higher throughputs.</li></ul></td>
  </tr>
  <tr>
    <td>Support dynamic region.</td>
    <td>Support dynamic region size adjustment (heterogeneous) and huge region size for scenarios with fast business growth and a large amount of data.</td>
  </tr>
  <tr>
    <td rowspan="4">SQL</td>
    <td>Support the JSON function.<ul><li>Expression index</li><li>Multi-value index</li><li>Partial index</li>
</ul></td>
    <td>In business scenarios that require flexible schema definitions, the application can use JSON to store information for ODS, transaction indicators, commodities, game characters, and props.</td>
  </tr>
  <tr>
    <td>Support cluster-level flashback.</td>
    <td>In game rollback scenarios, the flashback can be used to achieve a fast rollback of the current cluster. This solves the common problems in the gaming industry such as version errors and bugs.</td>
  </tr>
  <tr>
    <td>Support time to live (TTL).</td>
    <td>This feature enables automatic data cleanup in limited data archiving scenarios.</td>
  </tr>
  <tr>
    <td>Implement a DDL parallel execution framework.</td>
    <td>Implement a distributed parallel DDL execution framework, so that DDL tasks executed by only one TiDB Owner node can be coordinated and executed by all TiDB nodes in the cluster. Improve the execution speed of DDL tasks and cluster resource utilization.<br>By converting the execution of DDL tasks to distributed mode, this feature accelerates the execution speed of DDL tasks and improves the utilization of computing resources in the entire cluster. At present, DDL tasks that need to improve the speed include large table indexing and lossy column type modification tasks.</td>
  </tr>
  <tr>
    <td rowspan="2">Hybrid Transactional and Analytical Processing (HTAP)</td>
    <td>Support TiFlash result write-back.</td>
    <td><p>Support <code>INSERT INTO SELECT</code>.</p><ul><li>Easily write analysis results in TiFlash back to TiDB.</li><li>Provide complete ACID transactions, more convenient and reliable than general ETL solutions.</li><li>Set a hard limit on the threshold of intermediate result size, and report an error if the threshold is exceeded.</li><li>Support fully distributed transactions, and remove or relax the limit on the intermediate result size.</li></ul><p>These features combined enable a way to materialize intermediate results. The analysis results can be easily reused, which reduces unnecessary ad-hoc queries, improves the performance of BI and other applications (by pulling results directly) and reduces system load (by avoiding duplicated computation), thereby improving the overall data pipeline efficiency and reducing costs. It will make TiFlash an online service.</p></td>
  </tr>
  <tr>
    <td>Support FastScan for TiFlash.</td>
    <td><ul><li>FastScan provides weak consistency but faster table scan capability.</li><li>Further optimize the join order, shuffle, and exchange algorithms to improve computing efficiency and boost performance for complex queries.</li><li>Add a fine-grained data sharding mechanism to optimize the <code>COUNT(DISTINCT)</code> function and high cardinality aggregation.</li></ul><p>This feature improves the basic computing capability of TiFlash, and optimizes the performance and reliability of the underlying algorithms of the columnar storage and MPP engine.</p></td>
  </tr>
  <tr>
    <td>Proxy</td>
    <td>Support TiDB proxy.</td>
    <td>Implement automatic load balancing so that upgrading a cluster or modifying configurations does not affect the application. After scaling out or scaling in the cluster, the application can automatically rebalance the connection without reconnecting.<br>In scenarios such as upgrades and configuration changes, TiDB proxy is more business-friendly.</td>
  </tr>
  <tr>
    <td>Maintenance</td>
    <td>Support rule-based SQL blocklist.</td>
    <td>In multi-service aggregation scenarios, provide SQL management and control capabilities, and improve cluster stability by prohibiting high-resource-consuming SQL statements.</td>
  </tr>
</tbody>
</table>

## Diagnosis and maintenance

<table>
<thead>
  <tr>
    <th>Domain</th>
    <th>Feature</th>
    <th>Description</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td>SQL tuning for HTAP workloads</td>
    <td><ul><li>Provide SQL execution information from the perspective of applications.</li><li>Provide suggestions on optimizing SQL for TiFlash and TiKV in HTAP workloads.</li></ul></td>
    <td><ul><li>Provide a dashboard that displays a SQL execution overview from the perspective of applications in HTAP workloads.</li><li>For one or several HTAP scenarios, provide suggestions on SQL optimization.</li></ul></td>
  </tr>
</tbody>
</table>

## Data backup and migration

<table>
<thead>
  <tr>
    <th>Domain</th>
    <th>Feature</th>
    <th>Description</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td>Backup and restore</td>
    <td>AWS EBS or GCP persistent disk snapshot-based backup and restore.</td>
    <td>Support backup and restore based on AWS EBS or GCP persistent disk snapshots.</td>
  </tr>
  <tr>
    <td>Point-in-time recovery (PITR)</td>
    <td>Table-level and database-level PITR.</td>
    <td>BR supports table-level or database-level PITR.</td>
  </tr>
  <tr>
    <td rowspan="2">Data replication to downstream systems via TiCDC</td>
    <td>Reduce TiCDC replication latency in daily operations.</td>
    <td>When TiKV, TiDB, PD, or TiCDC nodes are offline in a planned maintenance window, the replication latency of TiCDC can be reduced to less than 10 seconds.</td>
  </tr>
  <tr>
    <td>Support replicating data to object storage such as S3.</td>
    <td>TiCDC supports replicating data changes to common object storage services.</td>
  </tr>
  <tr>
    <td>Data migration</td>
    <td>TiDB Lightning supports table-level and partition-level online data import.</td>
    <td>TiDB Lightning provides comprehensive table-level and partition-level data import capabilities.</td>
  </tr>
</tbody>
</table>

## Security

<table>
<thead>
  <tr>
    <th>Domain</th>
    <th>Feature</th>
    <th>Description</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td>Log redaction</td>
    <td><ul><li>Support data redaction in execution plans in TiDB Dashboard.</li><li>Enhance data redaction in TiDB-related logs.</li></ul></td>
    <td>Redact sensitive information in execution plans and various logs to enhance the security of user data.</td>
  </tr>
  <tr>
    <td>Password complexity check</td>
    <td>A strong password is required.</td>
    <td>To improve security, empty passwords and weak passwords are not allowed.<br>The required password length is not less than 8. The password must contain an uppercase letter, a lowercase letter, a number, and a character.</td>
  </tr>
  <tr>
    <td>Password expiration</td>
    <td>TiDB provides password expiration management and requires users to change passwords regularly.</td>
    <td>Reduce the security risk of password cracking or leakage caused by using the same password for a long time.</td>
  </tr>
  <tr>
    <td>Password policy management</td>
    <td>TiDB provides a password reuse mechanism and brute-force cracking prevention capabilities.</td>
    <td>TiDB supports password policy management to protect password security.</td>
  </tr>
  <tr>
    <td>Column-level access control</td>
    <td>TiDB supports column-level privilege management.</td>
    <td>TiDB already supports cluster-level, database-level, and table-level privilege management. On top of that, TiDB will support column-level privilege management to meet the principle of least privilege and provide fine-grained data access control.</td>
  </tr>
  <tr>
    <td>Audit logging capability enhancement</td>
    <td>Support configurable audit log policies, configurable audit filters (filter by objects, users, and operation types), and visual access to audit logs.</td>
    <td>Improve the completeness and usability of the audit log feature.</td>
  </tr>
</tbody>
</table>
