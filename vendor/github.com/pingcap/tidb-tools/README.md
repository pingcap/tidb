## tidb-tools

tidb-tools are some useful tool collections for [TiDB](https://github.com/pingcap/tidb).


## How to build

```
make build # build all tools

make importer # build importer

make checker # build checker

make sync_diff_inspector # build sync_diff_inspector

make binlogctl  # build binlogctl
```

When build successfully, you can find the binary in bin directory.

## Tool list

[importer](./importer)

A tool for generating and inserting datas to database which is compatible with MySQL protocol, like MySQL, TiDB.

[checker](./checker)

A tool for checking the compatibility of an existed MySQL database with TiDB.

[sync_diff_inspector](./sync_diff_inspector)

A tool for comparing two database's data and outputs a brief report about differences.

[binlogctl](./tidb-binlog/binlogctl)

A tool for performing some tidb-binlog related operations, like query status pump/drainer and unregister someone pump/drainer.

## License
Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
