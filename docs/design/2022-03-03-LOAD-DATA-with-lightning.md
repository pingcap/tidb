# Integrate LOAD DATA with tidb-lightning

- Author: [lance6716](https://github.com/lance6716), [D3Hunter](https://github.com/D3Hunter)
- Tracking Issue: https://github.com/pingcap/tidb/issues/40499

## Summary

This RFC introduce the similarity and difference of SQL statement "LOAD DATA" and [TiDB lightning](https://docs.pingcap.com/tidb/stable/tidb-lightning-overview), and propose a way to integrate them with the advantage from the both side.

## Background and Motivation

The [LOAD DATA statement](https://docs.pingcap.com/tidb/stable/sql-statement-load-data#load-data) of TiDB aims to be fully compatible with MySQL's one in most scenarios. It has advantages such as
- Easy to use no matter how cluster is deployed
- Supporting linux pipe files
- Supporting modifying the data by [input preprocessing](https://dev.mysql.com/doc/refman/8.0/en/load-data.html#load-data-input-preprocessing)

TiDB lightning is a tool used for importing data at TB scale to TiDB clusters. It is widely used to import data into TiDB cluster in many cases, and it has many powerful and complex features:
- Supporting many file formats like mydumper-like dump files, parquet
- Supporting read files from cloud storage like S3
- Very high importing speed when use [physical import mode](https://docs.pingcap.com/tidb/stable/tidb-lightning-physical-import-mode)

Although they both have some advantanges, merely one of them is limited in data loading sceario. For example, the user tends to load large amount of data using TiDB lightning considering the performance, but if the TiDB cluster is deployed in k8s TiDB lightning need to be launched in a k8s pod to access the cluster. This introduces many problems, and LOAD DATA only requires the user to execute a SQL through MySQL client.

## Detailed Design

In short, we will decouple LOAD DATA statement and TiDB lightning into many components, and try to pick the best parts of both and combine them.

### Data source reader

Currently, TiDB's LOAD DATA only supports to read the data file through MySQL client's connection, which is LOAD DATA LOCAL mode. TiDB lightning can use a [library](https://github.com/pingcap/tidb/tree/master/br/pkg/storage) to read cloud storages like s3. We choose `io.ReadSeekCloser` to provide an abstraction for them. Reading from MySQL client's connection can not implement seek invocation other than `seek(0, current)`. Currently we return an error for this case, and in future we can stash the file to some storage to better support seek.

### Load data worker

Reading the file through MySQL client's connection requires TiDB server to [writes extra data to the connection](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_local_infile_request.html) before the OK packet which represent the ending of the command, but in the normal executor routine we can't directly modify the client connection. In order to keep a clear boundary between the protocol layer and executor layer, we use a *load data worker* to encapsulate the real processing logic. The *load data worker* holds all needed members.

```golang
// LoadDataWorker does a LOAD DATA job.
type LoadDataWorker struct {
    *InsertValues // executor package

    Ctx  sessionctx.Context
    ...
}
```

Then *load data worker* can start the main logic with the *data source reader* which is a `io.ReadSeekCloser`, no matter it's invoked in protocol layer or executor layer.

```golang
func (*LoadDataWorker) Load(context.Context, io.ReadSeekCloser) error {
    ...
}
```

### Data parser

TiDB lightning features many supported data file format, like CSV, parquet, etc. And it also supports reading the compressed file like gzip, zstd, etc. To combine this ability with LOAD DATA, we let *load data worker* create the *data parser* with *data source reader* and its configuration like FIELDS TERMINATED BY, and then drive the *data parser* to get the data.

```golang
func (e *LoadDataWorker) Load(ctx context.Context, reader io.ReadSeekCloser) error {
    var (
        parser mydump.Parser
        err    error
    )

    switch strings.ToLower(e.format) {
    case XXX:
        parser, err = mydump.NewXXXParser(
            ctx,
            reader,
            ...
        )
    case YYY:
        ...
    }

    go e.processStream(..., parser)
    ...
}
```

The *data parser* output the data of one row in the type of `[]types.Datum`, the *load data worker* iterate the *data parser* to accumulate a batch to be further processed. An empty batch means the source file has been read totally.

### KV encoder

The *load data worker* owns a `InsertValues`, which can be used to encode the data to KV pairs. `InsertValues` is also used by other INSERT-like statements, it has more maintenance that lightning's separated KV encoder. Reusing it rather than lightning's one can prevent bugs that caused by lightning's encoding behaviour is outdated, for example, [#41454](https://github.com/pingcap/tidb/issues/41454).

### KV writer

## Other Issues
