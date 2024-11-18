<div align="center">
<a href='https://www.pingcap.com/?utm_source=github&utm_medium=tidb'>
<img src="docs/tidb-logo.png" alt="TiDB, a distributed SQL database" height=100></img>
</a>

---

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://github.com/pingcap/tidb/blob/master/LICENSE)
[![Language](https://img.shields.io/badge/Language-Go-blue.svg)](https://golang.org/)
[![Build Status](https://prow.tidb.net/badge.svg?jobs=pingcap/tidb/merged_build)](https://prow.tidb.net/?repo=pingcap%2Ftidb&type=postsubmit&job=pingcap%2Ftidb%2Fmerged_build)
[![Go Report Card](https://goreportcard.com/badge/github.com/pingcap/tidb)](https://goreportcard.com/report/github.com/pingcap/tidb)
[![GitHub release](https://img.shields.io/github/tag/pingcap/tidb.svg?label=release)](https://github.com/pingcap/tidb/releases)
</div>

# TiDB

TiDB (/’taɪdiːbi:/, "Ti" stands for Titanium) is an open-source, cloud-native, distributed SQL database designed for high availability, horizontal and vertical scalability, strong consistency, and high performance.

- [Key Features](#key-features)
- [Quick Start](#quick-start)
- [Need Help?](#need-help)
- [Architecture](#architecture)
- [Contributing](#contributing)
- [License](#license)
- [See Also](#see-also)
- [Acknowledgments](#acknowledgments)

## Key Features

- **[Distributed Transactions](https://www.pingcap.com/blog/distributed-transactions-tidb?utm_source=github&utm_medium=tidb)**: TiDB uses a two-phase commit protocol to ensure ACID compliance, providing strong consistency. Transactions span multiple nodes, and TiDB's distributed nature ensures data correctness even in the presence of network partitions or node failures.

- **[Horizontal and Vertical Scalability](https://docs.pingcap.com/tidb/stable/scale-tidb-using-tiup?utm_source=github&utm_medium=tidb)**: TiDB can be scaled horizontally by adding more nodes or vertically by increasing resources of existing nodes, all without downtime. TiDB's architecture separates computing from storage, enabling you to adjust both independently as needed for flexibility and growth.

- **[High Availability](https://docs.pingcap.com/tidbcloud/high-availability-with-multi-az?utm_source=github&utm_medium=tidb)**: Built-in Raft consensus protocol ensures reliability and automated failover. Data is stored in multiple replicas, and transactions are committed only after writing to the majority of replicas, guaranteeing strong consistency and availability, even if some replicas fail. Geographic placement of replicas can be configured for different disaster tolerance levels.

- **[Hybrid Transactional/Analytical Processing (HTAP)](https://www.pingcap.com/blog/htap-demystified-defining-modern-data-architecture-tidb?utm_source=github&utm_medium=tidb)**: TiDB provides two storage engines: TiKV, a row-based storage engine, and TiFlash, a columnar storage engine. TiFlash uses the Multi-Raft Learner protocol to replicate data from TiKV in real time, ensuring consistent data between the TiKV row-based storage engine and the TiFlash columnar storage engine. The TiDB Server coordinates query execution across both TiKV and TiFlash to optimize performance.

- **[Cloud-Native](https://www.pingcap.com/cloud-native?utm_source=github&utm_medium=tidb)**: TiDB can be deployed in public clouds, on-premises, or natively in Kubernetes. [TiDB Operator](https://docs.pingcap.com/tidb-in-kubernetes/stable/tidb-operator-overview/?utm_source=github&utm_medium=tidb) helps manage TiDB on Kubernetes, automating cluster operations, while [TiDB Cloud](https://tidbcloud.com/?utm_source=github&utm_medium=tidb) provides a fully-managed service for easy and economical deployment, allowing users to set up clusters with just a few clicks.

- **[MySQL Compatibility](https://docs.pingcap.com/tidb/stable/mysql-compatibility?utm_source=github&utm_medium=tidb)**: TiDB is compatible with MySQL 8.0, allowing you to use familiar protocols, frameworks and tools. You can migrate applications to TiDB without changing any code, or with minimal modifications. Additionally, TiDB provides a suite of [data migration tools](https://docs.pingcap.com/tidb/stable/ecosystem-tool-user-guide?utm_source=github&utm_medium=tidb) to help easily migrate application data into TiDB.

- **[Open Source Commitment](https://www.pingcap.com/blog/open-source-is-in-our-dna-reaffirming-tidb-commitment?utm_source=github&utm_medium=tidb)**: Open source is at the core of TiDB's identity. All source code is available on GitHub under the Apache 2.0 license, including enterprise-grade features. TiDB is built with the belief that open source enables transparency, innovation, and collaboration. We actively encourage contributions from the community to help build a vibrant and inclusive ecosystem, reaffirming our commitment to open development and accessibility for everyone.

## Quick start

> [!Tip]  
> As part of our commitment to open source, we want to reward all GitHub users. In addition to the free tier, you can get up to $2000 in TiDB Cloud Serverless credits for your open-source contributions - [Claim here](https://ossinsight.io/open-source-heroes/?utm_source=ossinsight&utm_medium=referral&utm_campaign=plg_OSScontribution_credit_05).

1. Start a TiDB Cluser

    - **On Local Playground**. To start a local test cluster, please refer to the [TiDB quick start guide](https://docs.pingcap.com/tidb/stable/quick-start-with-tidb#deploy-a-local-test-cluster?utm_source=github&utm_medium=tidb).

    - **On Kubernetes**. TiDB can be easily deployed in a self-managed Kubernetes environment or Kubernetes services on public clouds using TiDB Operator. For more details, please refer to the [TiDB on Kubernetes quick start guide](https://docs.pingcap.com/tidb-in-kubernetes/stable/get-started?utm_source=github&utm_medium=tidb).

    - **Using TiDB Cloud (Recommended)**. TiDB Cloud offers a fully managed version of TiDB with a free tier, no credit card required, so you can get a free cluster in seconds and start easily: [Sign up for TiDB Cloud](https://tidbcloud.com/free-trial?utm_source=github&utm_medium=tidb).

2. Learn About TiDB SQL: To explore the SQL capabilities of TiDB, refer to the [TiDB SQL documentation](https://docs.pingcap.com/tidb/stable/sql-statement-overview?utm_source=github&utm_medium=tidb).

3. Use MySQL Driver or ORM to [Build an App with TiDB with TiDB](https://docs.pingcap.com/tidbcloud/dev-guide-overview?utm_source=github&utm_medium=tidb).

4. Explore key features, such as [data migration](https://docs.pingcap.com/tidbcloud/tidb-cloud-migration-overview?utm_source=github&utm_medium=tidb), [changefeed](https://docs.pingcap.com/tidbcloud/changefeed-overview?utm_source=github&utm_medium=tidb), [vector search](https://docs.pingcap.com/tidbcloud/vector-search-overview?utm_source=github&utm_medium=tidb), [HTAP](https://docs.pingcap.com/tidbcloud/tidb-cloud-htap-quickstart?utm_source=github&utm_medium=tidb), [disaster recovery](https://docs.pingcap.com/tidb/stable/dr-solution-introduction?utm_source=github&utm_medium=tidb), etc.


## Need Help?

- You can connect with TiDB users, ask questions, find answers, and help others on our community platforms: [Discord](https://discord.gg/KVRZBR2DrG?utm_source=github), Slack ([English](https://slack.tidb.io/invite?team=tidb-community&channel=everyone&ref=pingcap-tidb), [Japanese](https://slack.tidb.io/invite?team=tidb-community&channel=tidb-japan&ref=github-tidb)), [Stack Overflow](https://stackoverflow.com/questions/tagged/tidb), TiDB Forum ([English](https://ask.pingcap.com/), [Chinese](https://asktug.com)), X [@PingCAP](https://twitter.com/PingCAP)

- For filing bugs, suggesting improvements, or requesting new features, use [Github Issues](https://github.com/pingcap/tidb/issues) or join discussions on [Github Discussions](https://github.com/orgs/pingcap/discussions).

- To troubleshoot TiDB, refer to [Toubleshooting documentation](https://docs.pingcap.com/tidb/stable/tidb-troubleshooting-map?utm_source=github&utm_medium=tidb).

## Architecture

![TiDB architecture](./docs/tidb-architecture.png)

Learn more details about TiDB architecture in our [Docs](https://docs.pingcap.com/tidb/stable/tidb-architecture?utm_source=github&utm_medium=tidb).

## Contributing

TiDB is built on a commitment to open source, and we welcome contributions from everyone. Whether you are interested in improving documentation, fixing bugs, or developing new features, we invite you to shape the future of TiDB.

- See our [Contributor Guide](https://github.com/pingcap/community/blob/master/contributors/README.md#how-to-contribute) and [TiDB Development Guide](https://pingcap.github.io/tidb-dev-guide/index.html) to get started.

- If you're looking for issues to work on, try looking at the [good first issues](https://github.com/pingcap/tidb/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22) or [help wanted issues](https://github.com/pingcap/tidb/issues?q=is%3Aopen+is%3Aissue+label%3A%22help+wanted%22).

- The [contribution map](https://github.com/pingcap/tidb-map/blob/master/maps/contribution-map.md#a-map-that-guides-what-and-how-contributors-can-contribute) lists everything you can contribute.

- The [community repository](https://github.com/pingcap/community) contains everything else you need.

- Don't forget to claim your contribution swag by filling in and submitting this [form](https://forms.pingcap.com/f/tidb-contribution-swag).


<a href="https://next.ossinsight.io/widgets/official/compose-recent-active-contributors?repo_id=41986369&limit=30" target="_blank" style="display: block" align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://next.ossinsight.io/widgets/official/compose-recent-active-contributors/thumbnail.png?repo_id=41986369&limit=30&image_size=auto&color_scheme=dark" width="655" height="auto">
    <img alt="Active Contributors of pingcap/tidb - Last 28 days" src="https://next.ossinsight.io/widgets/official/compose-recent-active-contributors/thumbnail.png?repo_id=41986369&limit=30&image_size=auto&color_scheme=light" width="655" height="auto">
  </picture>
</a>

## License

TiDB is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.

## See Also

- [TiDB Online Playgroud](https://play.tidbcloud.com/?utm_source=github&utm_medium=tidb_readme)
- TiDB Case Studies: [TiDB Customers](https://www.pingcap.com/customers/?utm_source=github&utm_medium=tidb), [TiDB 事例記事](https://pingcap.co.jp/case-study/?utm_source=github&utm_medium=tidb), [TiDB 中文用户案例](https://cn.pingcap.com/case/?utm_source=github&utm_medium=tidb)
- [TiDB User Documentation](https://docs.pingcap.com/tidb/stable?utm_source=github&utm_medium=tidb)
- [TiDB Design Docs](/docs/design)
- [TiDB Release Notes](https://docs.pingcap.com/tidb/dev/release-notes?utm_source=github&utm_medium=tidb)
- [TiDB Blog](https://www.pingcap.com/blog/?utm_source=github&utm_medium=tidb)
- [TiDB Roadmap](roadmap.md)

## Acknowledgments

- Thanks [cznic](https://github.com/cznic) for providing some great open source tools.
- Thanks [GolevelDB](https://github.com/syndtr/goleveldb), [BoltDB](https://github.com/boltdb/bolt), and [RocksDB](https://github.com/facebook/rocksdb) for their powerful storage engines.
