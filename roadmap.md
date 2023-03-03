# TiDB Roadmap

This roadmap brings you what's coming in the 1-year future, so you can see the new features or improvements in advance, follow the progress, learn about the key milestones on the way, and give feedback as the development work goes on. In the course of development, this roadmap is subject to change based on user needs and feedback. If you have a feature request or want to prioritize a feature, please file an issue on [GitHub](https://github.com/pingcap/tidb/issues).

> **Safe harbor statement:**
>
> *Any unreleased features discussed or referenced in our documents, roadmaps, blogs, websites, press releases, or public statements that are not currently available ("unreleased features") are subject to change at our discretion and may not be delivered as planned or at all. Customers acknowledge that purchase decisions are solely based on features and functions that are currently available, and that PingCAP is not obliged to deliver aforementioned unreleased features as part of the contractual agreement unless otherwise stated.*

## Highlights of what we are planning

<table>
  <thead>
    <tr>
      <th>Category</th>
      <th>Mid-year stable<br /></th>
      <th>End of year stable<br /></th>
      <th>2-3 year projection</th>
    </tr>
  </thead>
  <tbody valign="top">
    <tr>
      <td><b>Scalability and Performance</b><br />Enhance horsepower</td>
      <td>
        <ul>
          <li>
            <b>General plan cache</b><br />Improve general read performance
          </li>
          <li>
            <b>Partitioned-raft-kv storage engine</b><br />Increased write
            velocity, faster scaling operations, larger clusters
          </li>
          <li>
            <b>TiFlash performance boost</b><br />TiFlash optimization such
            as late materialization and runtime filter
          </li>
          <li>
            <b>Fastest online DDL distributed framework</b><br />Complete
            the distributed framework to support the fastest online DDL
          </li>
        </ul>
      </td>
      <td>
        <ul>
          <li>
            <b>Stability at PB scale</b><br />
            Provide reliable and consistent performance for tremendous data
          </li>
          <li>
            <b>Disaggregate TiFlash compute and storage (auto-scaling) </b
            ><br />Realize elastic HTAP resource utilization
          </li>
          <li>
            <b>TiFlash S3 based storage engine</b>
            <br />Provide shared storage at lower cost
          </li>
        </ul>
      </td>
      <td>
        <ul>
          <li>
            <b>Next generation, more powerful storage engine</b>
          </li>
          <li>
            <b>Unlimited transaction size</b>
          </li>
          <li>
            <b>Multi-model support</b>
          </li>
        </ul>
      </td>
    </tr>
    <tr>
      <td>
        <b>Reliability and Availability</b>
        <br />Enhance dependability
      </td>
      <td>
        <ul>
          <li>
            <b
              >Resource control: quotas and scheduling for resource groups
              and background tasks</b
            >
            <br />Reliably and efficiently manage workloads and applications
            sharing the same cluster
          </li>
          <li>
            <b>TiCDC/PITR recovery objective enhancements</b>
            <br />Increase business continuity and minimize the impact of
            system failures
          </li>
          <li>
            <b>TiProxy</b>
            <br />Keep database connections during cluster upgrade and scale
            in/out and avoid impact on applications
          </li>
          <li>
            <b>End-to-end data correctness check</b>
            <br />Prevent data error or corruption through TiCDC
          </li>
        </ul>
      </td>
      <td>
        <ul>
          <li>
            <b>Multi-tenancy</b>
            <br />Provide fine-grained resource control and isolation to reduce cost
          </li>
          <li>
            <b>Improved cluster/node level fault tolerance</b>
            <br />Enhance cluster resilience
          </li>
          <li>
            <b>TiFlash spill to disk</b>
            <br />Avoid TiFlash OOM
          </li>
        </ul>
      </td>
      <td>
        <ul>
          <li>
            <b>TiDB memory management re-architecture</b>
          </li>
          <li>
            <b>Global table</b>
          </li>
        </ul>
      </td>
    </tr>
    <tr>
      <td>
        <b>SQL</b>
        <br />Enhance functionality and compatibility
      </td>
      <td>
        <ul>
          <li>
            <b>Production-ready TTL (time-to-live) data management</b>
            <br />Manage database size and improve performance by
            automatically expiring outdated data
          </li>
          <li>
            <b>Table level flashback</b>
            <br />Support traveling a single table to a specific time via SQL
            point
          </li>
        </ul>
      </td>
      <td>
        <ul>
          <li>
            <b>Materialized views</b>
            <br />Support pre-calculation to boost query performance
          </li>
        </ul>
      </td>
      <td>
        <ul>
          <li>
            <b>Federated query</b>
          </li>
          <li>
            <b>Cascades optimizer</b>
          </li>
          <li>
            <b>Full text search & GIS support</b>
          </li>
          <li>
            <b>User-defined functions</b>
          </li>
        </ul>
      </td>
    </tr>
    <tr>
      <td>
        <b>Database Operations and Observability</b>
        <br />Enhance DB manageability and its ecosystem
      </td>
      <td>
        <ul>
          <li>
            <b>Distributed TiCDC single table replication</b>
            <br />Dramatically improve TiCDC throughput by distributing the
            workload to multiple nodes
          </li>
          <li>
            <b>Production-ready TiCDC sink to Amazon S3 and Azure object storage</b>
            <br />Enhance ecosystem to better work with big data
          </li>
          <li>
            <b>TiDB Operator fast scale-in</b>
            <br />Improve from scaling in one by one to scaling in at once
          </li>
          <li>
            <b>SQL-based data import</b>
            <br />Improve user-friendliness through operational enhancements
          </li>
        </ul>
      </td>
      <td>
        <ul>
          <li>
            <b>Major performance boost for data import</b>
            <br />Expect 3-4 times of improvements
          </li>
          <li>
            <b>Multiple upstreams for TiCDC</b>
            <br />Support N:1 TiDB to TiCDC
          </li>
          <li>
            <b> SQL-based data management </b>
            <br />Improve data management for TiCDC, data migration, and backup and restore tools
          </li>
          <li>
            <b> Automatic pause/resume DDL during upgrade </b>
            <br />Ensure a smooth upgrade experience
          </li>
        </ul>
      </td>
      <td>
        <ul>
          <li>
            <b>AI-indexing</b>
          </li>
          <li>
            <b>Heterogeneous database migration support</b>
          </li>
          <li>
            <b>Re-invented AI-SQL performance advisor</b>
          </li>
        </ul>
      </td>
    </tr>
    <tr>
      <td>
        <b>Security</b>
        <br />Enhance data safety and privacy
      </td>
      <td>
        <ul>
          <li>
            <b>JWT authentication</b>
            <br />Provide secure and standard authentication
          </li>
          <li>
            <b> LDAP integration </b>
            <br />Authenticate via LDAP server over TLS
          </li>
          <li>
            <b> Audit log enhancement </b>
            <br />
            Enhance logs with greater details
          </li>
        </ul>
      </td>
      <td>
        <ul>
          <li>
            <b>Field-level/row-level access control</b>
            <br />
            Provide finer-grained control
          </li>
          <li>
            <b>Database encryption</b>
            <br />Support data-at-rest encryption for database files
          </li>
          <li>
            <b>Unified TLS CA/Key rotation policy</b>
            <br />Enhance security and operational efficiency for all TiDB
            components
          </li>
        </ul>
      </td>
      <td>
        <ul>
          <li>
            <b>Enhanced client-side encryption</b>
          </li>
          <li>
            <b>Enhanced data masking</b>
          </li>
          <li>
            <b>Enhanced data lifecycle management</b>
          </li>
        </ul>
      </td>
    </tr>
  </tbody>
</table>

These are non-exhaustive plans and subject to change. Features may differ per service subscriptions.

## Recently shipped

- [TiDB 6.6.0 Release Notes](https://docs.pingcap.com/tidb/v6.6/release-6.6.0)
- [TiDB 6.5.0 Release Notes](https://docs.pingcap.com/tidb/v6.5/release-6.5.0)
