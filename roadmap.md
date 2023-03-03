# TiDB Roadmap

This roadmap brings you what's coming in the 1-year future, so you can see the new features or improvements in advance, follow the progress, learn about the key milestones on the way, and give feedback as the development work goes on. In the course of development, this roadmap is subject to change based on user needs and feedback. If you have a feature request or want to prioritize a feature, please file an issue on [GitHub](https://github.com/pingcap/tidb/issues).

> **Safe harbor statement:**
>
> *Any unreleased features discussed or referenced in our documents, roadmaps, blogs, websites, press releases, or public statements that are not currently available ("unreleased features") are subject to change at our discretion and may not be delivered as planned or at all. Customers acknowledge that purchase decisions are solely based on features and functions that are currently available, and that PingCAP is not obliged to deliver aforementioned unreleased features as part of the contractual agreement unless otherwise stated.*

## Highlights of what we are planning

<table>
<thead>
  <tr>
    <th></th>
    <th>Mid-year stable<br></th>
    <th>End of year stable<br></th>
    <th>2-3 year projection</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td rowspan="4"><b>Scalability and Performance</b><br>Enhance horsepower</td>
    <td><b>General plan cache</b><br><i>Improve general read performance</i></td>
    <td><b>Stability at PB scale</b><br><i>Reliable and consistent performance for tremendous data</i></td>
    <td>Next generation, more powerful storage engine</td>
  </tr>
  <tr>
    <td><b>Partitioned-raft-kv storage engine</b><br><i>Increased write velocity, faster scaling operations, larger clusters</i></td>
    <td><b>Disaggregate TiFlash compute/storage (auto-caling)</b><br><i>Elastic HTAP resource utilization</i></td>
    <td>Unlimited transaction size</td>
  </tr>
  <tr>
    <td><b>TiFlash performance boost</b><br><i>TiFlash optimization such as late materialization, runtime filter, etc</i></td>
    <td><b>TiFlash S3 based storage engine</b><br><i>Shared storage, lower cost</i></td>
    <td>Multi-model support</td>
  </tr>
  <tr>
    <td><b>Fastest online DDL distributed framework</b><br><i>Complete the distributed framework to support fastest online DDL</i></td>
    <td></td>
    <td></td>
  </tr>
  <tr>
    <td rowspan="4"><b>Reliability and Availability</b><br>Enhance dependability</td>
    <td><b>Resource control: quotas and scheduling for resource groups and background tasks</b><br><i>Reliably and efficiently manage workloads and applications sharing the same cluster</i></td>
    <td><b>Multi-tenancy</b><br><i>Fine-grained resource control, isolation to reduce cost</i></td>
    <td>TiDB memory management re-architecture</td>
  </tr>
  <tr>
    <td><b>TiCDC/PiTR recovery objectives enhancements</b><br><i>Increase business continuity and minimize the impact of system failures</i></td>
    <td><b>Improved cluster/node level fault tolerance</b><br><i>Resilience enhancement</i></td>
    <td>Global Table</td>
  </tr>
  <tr>
    <td><b>TiProxy</b><br><i>Keep database connections during cluster upgrade and scale in/out and avoid impact to applications</i></td>
    <td><b>TiFlash spill to disk</b><br><i>Avoid TiFlash OOM</i></td>
    <td></td>
  </tr>
  <tr>
    <td><b>End-to-end data correctness check</b><br><i>Prevents data error or corruptions through TiCDC</i></td>
    <td></td>
    <td></td>
  </tr>
  <tr>
    <td rowspan="4"><b>SQL</b><br>Enhance functionality and compatibility</td>
    <td><b>Production ready TTL (time-to-live) data management</b><br><i>Manage database size and improve performance by automatically expiring outdated data</i></td>
    <td><b>Materialized views</b><br><i>Pre-calculation to boost query performance</i></td>
    <td>Federated Query</td>
  </tr>
  <tr>
    <td><b>Table level flashback</b><br><i>SQL support for traveling a single table to specific time point</i></td>
    <td></td>
    <td>Cascades Optimizer</td>
  </tr>
  <tr>
    <td></td>
    <td></td>
    <td>Full text search &amp; GIS Support</td>
  </tr>
  <tr>
    <td></td>
    <td></td>
    <td>User-defined functions</td>
  </tr>
  <tr>
    <td rowspan="4"><b>Database Operations and Observability</b><br>Enhance DB manageability and its ecosystem</td>
    <td><b>Distributed TiCDC single table replication</b><br><i>Dramatically improve TiCDC throughput by distributing the workload to multiple nodes</i></td>
    <td><b>Import major performance boost </b><br><i>Expecting 3-4 times improvements</i></td>
    <td>AI-indexing</td>
  </tr>
  <tr>
    <td><b>Production ready TiCDC sink to S3 and Azure object store</b><br><i>Enhance ecosystem to better work with big data</i></td>
    <td><b>TiCDC to support multiple upstreams</b><br>(i.e., N:1 TiDB to TiCDC)</td>
    <td>Heterogeneous database migration support</td>
  </tr>
  <tr>
    <td><b>TiDB Operator fast scale-in</b><br><i>From scale-in one by one to scale at once</i></td>
    <td><b>SQL-based data management</b><br><i>for TiCDC, data migration, and backup&amp;restore tools</i></td>
    <td>Re-invented AI-SQL performance advisor</td>
  </tr>
  <tr>
    <td><b>SQL-based data import</b><br><i>User-friendly operational enhancement</i></td>
    <td><b>Automatic pause/resume DDL during upgrade</b><br><i>Ensure a smooth upgrade experience</i></td>
    <td></td>
  </tr>
  <tr>
    <td rowspan="3"><b>Security</b><br>Enhance data safety and privacy</td>
    <td><b>JWT authentication</b><br><i>secure, standard authentication</i></td>
    <td><b>Field-level/row-level access control</b><br><i>Finer-grained control</i></td>
    <td>Enhanced client-side encryption</td>
  </tr>
  <tr>
    <td><b>LDAP integration</b><br><i>Authenticate via LDAP server over TLS</i></td>
    <td><b>Database encryption</b><br><i>Data-at-rest encryption for database files</i></td>
    <td>Enhanced data masking</td>
  </tr>
  <tr>
    <td><b>Audit log enhancement</b><br><i>Enhance with greater details</i></td>
    <td><b>Unified TLS CA/Key rotation policy</b><br><i>Enhanced security and operational efficiency for all TiDB components</i></td>
    <td>Enhanced data lifecycle management</td>
  </tr>
</tbody>
</table>

These are non-exhaustive plans and subject to change. Features may differ per service subscriptions.

## Recently shipped

- [TiDB 6.6.0 Release Notes](https://docs.pingcap.com/tidb/v6.6/release-6.6.0)
- [TiDB 6.5.0 Release Notes](https://docs.pingcap.com/tidb/v6.5/release-6.5.0)
