# TiDB-Lightning

**TiDB-Lightning** is a tool used for importing data at TB scale to TiDB clusters.

## Documentation

[中文文档](https://docs.pingcap.com/zh/tidb/stable/tidb-lightning-overview)

[English Document](https://docs.pingcap.com/tidb/stable/tidb-lightning-overview)

[Import Into Statement](https://docs.pingcap.com/tidbcloud/sql-statement-import-into) is used to import data files or result from select statement into an empty table in TiDB via the Physical Import Mode of TiDB Lightning.

## Building

To build binary:

```bash
$ cd ../tidb
$ make build_lightning
```

Notice TiDB-Lightning supports building with Go version `Go >= 1.16`

When TiDB-Lightning is built successfully, you can find binary in the `bin` directory.

## Running tests

See [this document](../lightning/tests/README.md) for how to run integration tests.

## Quick start
See [Quick Start for TiDB Lightning](https://docs.pingcap.com/tidb/stable/get-started-with-tidb-lightning).

## Contributing

Contributions are welcomed and greatly appreciated. See [CONTRIBUTING](../CONTRIBUTING.md)
for details on submitting patches and the contribution workflow.

## License

TiDB-Lightning is under the Apache 2.0 license. See the [LICENSE](../LICENSE) file for details.
