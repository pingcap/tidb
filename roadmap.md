# TiDB Roadmap

This roadmap brings you what's coming in the future, so you can see the new features or improvements in advance, follow the progress, learn about the key milestones on the way, and give feedback as the development work goes on. In the course of development, this roadmap is subject to change based on user needs and feedback. If you have a feature request or want to prioritize a feature, please file an issue on [GitHub](https://github.com/pingcap/tidb/issues).

## Highlights of what we are planning

<table>
  <thead>
    <tr>
      <th>Category</th>
      <th>Mid-calendar-year LTS release</th>
      <th>End-calendar-year LTS release</th>
      <th>Future releases</th>
    </tr>
  </thead>
  <tbody valign="top">
    <tr>
      <td>
        <b>Scalability and Performance</b><br /><i>Enhance horsepower</i>
      </td>
      <td>
        <ul>
          <li>
            <b>General plan cache</b><br /><i
              >Improve general read performance</i
            >
          </li>
          <br />
          <li>
            <b>Physical isolation of KV keyspaces (regions)</b><br /><i>
              Enables increased write velocity, faster scaling operations, and 
              larger clusters
            </i>
          </li>
          <br />
          <li>
            <b>TiFlash performance boost</b><br /><i>
              Implement TiFlash optimization such as late materialization
              and runtime filter
            </i>
          </li>
          <br />
          <li>
            <b>Distributed online DDL framework</b><br /><i>
              Alrleady fast, this complete the framework to support the fastest
              online DDL
            </i>
          </li>
          <br />
        </ul>
      </td>
      <td>
        <ul>
          <li>
            <b>Stability at PB scale</b><br />
            <i
              >Provide reliable and consistent performance for tremendous
              data</i
            >
          </li>
          <br />
          <li>
            <b>Disaggregate TiFlash compute and storage (auto-scaling) </b
            ><br /><i>Realize elastic HTAP resource utilization</i>
          </li>
          <br />
          <li>
            <b>TiFlash S3 based storage engine</b>
            <br /><i>Provide shared storage at lower cost</i>
          </li>
          <br />
        </ul>
      </td>
      <td>
        <ul>
          <li>
            <b>Next generation, more powerful storage engine</b>
          </li>
          <br />
          <li>
            <b>Unlimited transaction size</b>
          </li>
          <br />
          <li>
            <b>Multi-model support</b>
          </li>
          <br />
        </ul>
      </td>
    </tr>
    <tr>
      <td>
        <b>Reliability and Availability</b>
        <br /><i>Enhance dependability</i>
      </td>
      <td>
        <ul>
          <li>
            <b
              >Resource control: quotas and resource scheduling for resource groups
              and background tasks. Quotas are burstable.</b
            >
            <br /><i>
              Reliably and efficiently manage workloads and applications
              that share the same cluster
            </i>
          </li>
          <br />
          <li>
            <b>TiCDC/PITR recovery objective enhancements</b>
            <br /><i>
              Increase business continuity and minimize the impact of system
              failures
            </i>
          </li>
          <br />
          <li>
            <b>TiProxy</b>
            <br /><i>
              Keep database connections during cluster upgrade and scale
              in/out, and avoid impact on applications
            </i>
          </li>
          <br />
          <li>
            <b>End-to-end data correctness check</b>
            <br /><i>Prevent data error or corruption through TiCDC</i>
          </li>
          <br />
        </ul>
      </td>
      <td>
        <ul>
          <li>
            <b>Multi-tenancy</b>
            <br /><i
              >Provide fine-grained resource control and isolation to reduce
              cost</i
            >
          </li>
          <br />
          <li>
            <b>Improved cluster/node level fault tolerance</b>
            <br /><i>Enhance cluster resilience</i>
          </li>
          <br />
          <li>
            <b>TiFlash spill to disk</b>
            <br /><i>Avoid TiFlash OOM</i>
          </li>
          <br />
        </ul>
      </td>
      <td>
        <ul>
          <li>
            <b>Enhanced TiDB memory management</b>
          </li>
          <br />
          <li>
            <b>Global table</b>
          </li>
          <br />
        </ul>
      </td>
    </tr>
    <tr>
      <td>
        <b>SQL</b>
        <br /><i>Enhance functionality and compatibility</i>
      </td>
      <td>
        <ul>
          <li>
            <b>Production-ready TTL (time-to-live) data management</b>
            <br /><i>
              Manage database size and improve performance by automatically
              expiring outdated data
            </i>
          </li>
          <br />
          <li>
            <b>Table level flashback</b>
            <br /><i>
              Support traveling a single table to a specific time via SQL
            </i>
          </li>
          <br />
        </ul>
      </td>
      <td>
        <ul>
          <li>
            <b>Materialized views</b>
            <br /><i>Support pre-calculation to boost query performance</i>
          </li>
          <br />
        </ul>
      </td>
      <td>
        <ul>
          <li>
            <b>Federated query</b>
          </li>
          <br />
          <li>
            <b>Cascades optimizer</b>
          </li>
          <br />
          <li>
            <b>Full text search & GIS support</b>
          </li>
          <br />
          <li>
            <b>User-defined functions</b>
          </li>
          <br />
        </ul>
      </td>
    </tr>
    <tr>
      <td>
        <b>Database Operations and Observability</b>
        <br /><i>Enhance DB manageability and its ecosystem</i>
      </td>
      <td>
        <ul>
          <li>
            <b>Distributed TiCDC single table replication</b>
            <br /><i>
              Dramatically improve TiCDC throughput by distributing the
              workload to multiple nodes
            </i>
          </li>
          <br />
          <li>
            <b
              >Production-ready TiCDC sink to Amazon S3 and Azure object
              storage</b
            >
            <br /><i>Enhance ecosystem to better work with big data</i>
          </li>
          <br />
          <li>
            <b>TiDB Operator fast scale-in</b>
            <br /><i
              >Improve from scaling in one by one to scaling in at once</i
            >
          </li>
          <br />
          <li>
            <b>SQL-based data import</b>
            <br /><i
              >Improve user-friendliness through operational enhancements</i
            >
          </li>
          <br />
        </ul>
      </td>
      <td>
        <ul>
          <li>
            <b>Major performance boost for data import</b>
            <br /><i>Expect 3-4 times of improvements</i>
          </li>
          <br />
          <li>
            <b>Multiple upstreams for TiCDC</b>
            <br /><i>Support N:1 TiDB to TiCDC</i>
          </li>
          <br />
          <li>
            <b> SQL-based data management </b>
            <br /><i
              >Improve data management for TiCDC, data migration, and backup
              and restore tools</i
            >
          </li>
          <br />
          <li>
            <b> Automatic pause/resume DDL during upgrade </b>
            <br /><i>Ensure a smooth upgrade experience</i>
          </li>
          <br />
        </ul>
      </td>
      <td>
        <ul>
          <li>
            <b>AI-indexing</b>
          </li>
          <br />
          <li>
            <b>Heterogeneous database migration support</b>
          </li>
          <br />
          <li>
            <b>Re-invented AI-SQL performance advisor</b>
          </li>
        </ul>
      </td>
    </tr>
    <tr>
      <td>
        <b>Security</b>
        <br /><i>Enhance data safety and privacy</i>
      </td>
      <td>
        <ul>
          <li>
            <b>JWT authentication</b>
            <br /><i>Provide secure and standard authentication</i>
          </li>
          <br />
          <li>
            <b> LDAP integration </b>
            <br /><i>Authenticate via LDAP server over TLS</i>
          </li>
          <br />
          <li>
            <b> Audit log enhancement </b>
            <br />
            <i>Enhance logs with greater details</i>
          </li>
          <br />
        </ul>
      </td>
      <td>
        <ul>
          <li>
            <b>Column-level/row-level access control</b>
            <br />
            <i>Provide finer-grained control</i>
          </li>
          <br />
          <li>
            <b>Database encryption</b>
            <br /><i>Encryption at rest with more granularity in table-level and column-level</i>
          </li>
          <br />
          <li>
            <b>Unified TLS CA/Key rotation policy</b>
            <br /><i>
              Enhance security and operational efficiency for all TiDB
              components
            </i>
          </li>
          <br />
        </ul>
      </td>
      <td>
        <ul>
          <li>
            <b>Enhanced client-side encryption</b>
          </li>
          <br />
          <li>
            <b>Enhanced data masking</b>
          </li>
          <br />
          <li>
            <b>Enhanced data lifecycle management</b>
          </li>
          <br />
        </ul>
      </td>
    </tr>
  </tbody>
</table>

These are non-exhaustive plans and subject to change. Features may differ per service subscriptions.

## Recently shipped

- [TiDB 6.6.0 Release Notes](https://docs.pingcap.com/tidb/v6.6/release-6.6.0)
- [TiDB 6.5.0 Release Notes](https://docs.pingcap.com/tidb/v6.5/release-6.5.0)

[![Hits](https://hits.seeyoufarm.com/api/count/incr/badge.svg?url=https%3A%2F%2Fgithub.com%2Fpingcap%2Ftidb%2Fblob%2Fmaster%2Froadmap.md&count_bg=%2379C83D&title_bg=%23555555&icon=&icon_color=%23E7E7E7&title=hits&edge_flat=false)](https://hits.seeyoufarm.com)
