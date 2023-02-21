# TiDB Roadmap

This roadmap brings you what's coming in the 1-year future, so you can see the new features or improvements in advance, follow the progress, learn about the key milestones on the way, and give feedback as the development work goes on. In the course of development, this roadmap is subject to change based on user needs and feedback. If you have a feature request or want to prioritize a feature, please file an issue on [GitHub](https://github.com/pingcap/tidb/issues).

✅: The feature or improvement is already available in TiDB.

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
    <td><ul><li>✅ Optimize resource isolation in heavy read scenarios.</li><li>✅ Optimize resource isolation in heavy (batch) write scenarios.</li><li>Provide resource management capability for background process.</li><li>✅ Support resource management framework.</ul></td>
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
    <td rowspan="6">SQL</td>
    <td>Support the JSON function.<ul><li>✅ Expression index</li><li>✅ Multi-valued index</li><li>✅ TiFlash supports JSON function pushdown</li></ul></td>
    <td>In business scenarios that require flexible schema definitions, the application can use JSON to store information for ODS, transaction indicators, commodities, game characters, and props.</td>
  </tr>
  <tr>
    <td><ul><li>✅ Support cluster-level flashback.</li><li>✅ Support database-level flashback.</li></ul></td>
    <td>In game rollback scenarios, the flashback can be used to achieve a fast rollback of the current cluster. This solves the common problems in the gaming industry such as version errors and bugs.</td>
  </tr>
  <tr>
    <td>✅ Support time to live (TTL).</td>
    <td>This feature enables automatic data cleanup in limited data archiving scenarios.</td>
  </tr>
  <tr>
    <td>✅ Support foreign key constraints.</td>
    <td>Supports foreign key constraints compatible with MySQL syntax, and provides DB-level referential integrity check capabilities.</td>
  </tr>
  <tr>
    <td>✅ Support non-transactional DML for insert and update operations.</td>
    <td></td>
  </tr>
  <tr>
    <td><ul><li>Implement a DDL parallel execution framework.</li><li>Provide DDL pause/resume capability.</li></ul></td>
    <td>Implement a distributed parallel DDL execution framework, so that DDL tasks executed by only one TiDB Owner node can be coordinated and executed by all TiDB nodes in the cluster. Improve the execution speed of DDL tasks and cluster resource utilization.<br>By converting the execution of DDL tasks to distributed mode, this feature accelerates the execution speed of DDL tasks and improves the utilization of computing resources in the entire cluster. At present, DDL tasks that need to improve the speed include large table indexing and lossy column type modification tasks.</td>
  </tr>
  <tr>
    <td rowspan="2">Hybrid Transactional and Analytical Processing (HTAP)</td>
    <td>✅ Support TiFlash result write-back.</td>
    <td><p>Support <code>INSERT INTO SELECT</code>.</p><ul><li>Easily write analysis results in TiFlash back to TiDB.</li><li>Provide complete ACID transactions, more convenient and reliable than general ETL solutions.</li><li>Set a hard limit on the threshold of intermediate result size, and report an error if the threshold is exceeded.</li><li>Support fully distributed transactions, and remove or relax the limit on the intermediate result size.</li></ul><p>These features combined enable a way to materialize intermediate results. The analysis results can be easily reused, which reduces unnecessary ad-hoc queries, improves the performance of BI and other applications (by pulling results directly) and reduces system load (by avoiding duplicated computation), thereby improving the overall data pipeline efficiency and reducing costs. It will make TiFlash an online service.</p></td>
  </tr>
  <tr>
    <td>✅ Support FastScan for TiFlash.</td>
    <td><ul><li>FastScan provides weak consistency but faster table scan capability.</li><li>Further optimize the join order, shuffle, and exchange algorithms to improve computing efficiency and boost performance for complex queries.</li><li>Add a fine-grained data sharding mechanism to optimize the <code>COUNT(DISTINCT)</code> function and high cardinality aggregation.</li></ul><p>This feature improves the basic computing capability of TiFlash, and optimizes the performance and reliability of the underlying algorithms of the columnar storage and MPP engine.</p></td>
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
    <td rowspan="2">SQL tuning for HTAP workloads</td>
    <td>Provide SQL execution information from the perspective of applications.</td>
    <td>Provide a dashboard that displays a SQL execution overview from the perspective of applications in HTAP workloads.</td>
  </tr>
  <tr>
    <td>Provide suggestions on optimizing SQL for TiFlash and TiKV in HTAP workloads.</td>
    <td>For one or several HTAP scenarios, provide suggestions on SQL optimization.</td>
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
    <td>Improve TiCDC scalability and reduce replication latency.</td>
    <td>Increase TiCDC's scalability by spanning data changes for single table to multiple TiCDC nodes and reduce replication latency by removing sorting stage.</td>
  </tr>
  <tr>
    <td>✅ Support replicating data to object storage such as S3.</td>
    <td>TiCDC supports replicating data changes to common object storage services.</td>
  </tr>
  <tr>
    <td>Data migration</td>
    <td>✅ Continuous data verification during data migration.</td>
    <td>DM supports online data verification during migration from MySQL compatible database to TiDB.</td>
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
    <td>Password complexity check</td>
    <td>✅ A strong password is required.</td>
    <td>To improve security, empty passwords and weak passwords are not allowed.<br>The required password length is not less than 8. The password must contain an uppercase letter, a lowercase letter, a number, and a character.</td>
  </tr>
  <tr>
    <td>Password expiration</td>
    <td>✅ TiDB provides password expiration management and requires users to change passwords regularly.</td>
    <td>Reduce the security risk of password cracking or leakage caused by using the same password for a long time.</td>
  </tr>
  <tr>
    <td>Password reuse policy</td>
    <td>✅ TiDB provides a password reuse policy.</td>
    <td>Restrict password reuse and improve password security.</td>
  </tr>
  <tr>
    <td>Password anti-brute force cracking</td>
    <td>✅ Accounts will be locked in case of consecutive incorrect passwords.</td>
    <td>Lock the account under continuous wrong passwords to prevent the password from being cracked by brute force.</td>
  </tr>
  <tr>
    <td>Log redaction</td>
    <td><ul><li>Support data redaction in execution plans in TiDB Dashboard.</li><li>Enhance data redaction in TiDB-related logs.</li></ul></td>
    <td>Redact sensitive information in execution plans and various logs to enhance the security of user data.</td>
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
