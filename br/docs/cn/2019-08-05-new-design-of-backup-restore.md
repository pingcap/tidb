Last updated: 2020-03-07

## 新版备份恢复设计方案

背景：由于需要和 TiDB CDC 在技术上整合，这个文档描述了一个基本思路是 kv scan 的新方案。

### 备份原理

备份的原理其实就是 `select *`，但是这个方案和 mydumper 的区别：

1. 分布式导出数据，数据由各个 region 的 leader 生成，理论上备份的吞吐能达到集群极限。
2. 数据是形式是 SST，SST 格式有优点在于能快速恢复，同时可以直接用 rocksdb 自带一些功能比如，数据压缩/加密。
3. 数据直接保存第三方存储，比如 S3, GCS 等。
4. 备份的一致性保证：SI，需要保证能恢复到 point-in-time 的状态。

### 备份流程

整个备份过程由独立于 TiKV 的外部程序推进，整体的基本流程：

1. 外部程序根据用户指定备份范围下发备份命令到 TiKV，这个外部程序最终会整合到 TiDB 中，入口是 SQL。
2. TiKV 接受备份请求后交由 region leader 处理，并使用一个特殊的 iterator 读取数据
3. TiKV 读取的数据会被组织成 SST 文件。这里需要控制 IO 和内存使用。
4. TiKV 会把 SST 文件写到外部存储，比如自身的本地存储，s3 等，这里需要流控。
5. TIKV 把执行信息汇报给外部程序。

外部程序执行备份的基本流程：

1. 下推备份到所有 TiKV
2. 接受 TiKV Streaming 发过来的备份结果
3. 聚合检查是否有范围没有备份到或者发生错误
4. 如果有范围错误或者没有备份到则重试
5. 重试需要精细化，查询 PD 对应的 leader 然后下发
6. 如果外部需要备份 table 或者 database，除了它们的数据外，还需要备份 schema

### 备份下推

分布式备份是新方案的设计目标之一。最简单高效的方式是直接把备份的命令下发到 TiKV，利用 region info accessor 把命令细分到各个 region leader 上，执行 scan。当下推执行完成后，需要查漏补缺，确保所有范围都覆盖到。这个实现方式最大的问题是可能会备份多余的数据，比如 leader 在备份迁移了，不过好在这不是什么正确性问题。还有一个方式是常规的按照 region 下发备份的命令，也很简单但是效率没有前者高，也有备份多余文件的可能，但是概率毕竟低。

### Iterator

由于 TiDB/TiKV 的事务模型是 percolator，数据的存储需要使用 3 个 CF，分别是 default，lock，write，所以如果 TiKV 想把数据保存在 SST 中，那么起码 TiKV 需要扫出 write 和 default 的数据。这意味现有的 iterator 是不适用的，因为：

1. 它只能吐出 default 的 key-value

2. 它吐出的是 point-in-time 的数据。

在全量备份中，TiKV 需要根据一个 ts（称为 backup_ts）扫出 write 和 default 的 key-value。Iterator 吐出的 key 需要是 datakey，也就是 key 的第一个字节是 ‘**z**’，这是为了在恢复的时候可以直接 ingest，不用 rewrite key。

在增量备份中，TiKV 需要扫出**一段时间**的增量数据，一段时间具体来说：

```
(backup_ts, current_ts]
```

由于备份的保证是 SI，所有增量数据可以直接通过扫 write CF 中的记录即可得到。需要吐出的记录有：
- Put，新增的数据。
- Delete，删除的数据。

不需要吐出的记录有：
- Lock，select for update 写的记录，实际上没有任何数据变更。
- Rollback，清理由提交失败造成的垃圾数据。

### 性能

性能是 KV Scan 方案最头疼的问题之一，因为全表扫描势必会对线上业务造成影响。增量的实现也是全表扫，同样有性能问题，这个和传统数据库的增量不一样。好在也有优化的办法，比如学一下 crdb 在 SST 上加上 max ts 的 table property。

### 外部存储

备份存储设计了一个接口，下层可以有不同的实现，比如本地磁盘，s3 等。由于外部存储不一定是真正的磁盘，所以 TiKV 在生成 SST 的时候，不会直接把数据写到本地磁盘上，而是先缓存在内存中，生成完毕后直接写到外部存储中，这样能避免 IO，提高整个备份的吞吐，当然这里需要内存控制。

### 异常处理

异常处理备份期间的异常和 `select *` 一样，可分为两种可恢复和不可恢复，所有的异常都可以直接复用 TiDB 现有的机制。

可恢复异常一般包含：

- RegionError，一般由 region split/merge，not leader 造成。
- KeyLocked，一般由事务冲突造成。
- Server is busy，一般由于 TiKV 太忙造成。

当发生这些异常时，备份的进度不会被打断。

除了以上的其他错误都是不可恢复异常，发生后，它们会打断备份的进度。

### 超出 GC 时间

超出 GC 时间是说，需要备份的数据已经被 GC，这情况一般发生在增量备份上，会导致增量不完整。在发生这个错的时候，外部程序需要重新来一个全量备份。

现在默认的 GC 时间是 10 分钟，实在是太短了，这意味着一旦开启全量备份，增量备份必须马上接上，而且运行的间隔必须在 10 分钟内。这个频率集群肯定受不了。这个问题不只备份会遇到， CDC 也会遇到。在开始备份或者 CDC 的集群上需要适当延长 GC 时间，比如延长到 6 小时或者 12 小时。

### 恢复

恢复所需的工作有以下几个：

1. 创建需要恢复的 database 和 table
2. 根据 table 和 SST 文件元信息，进行 Split & Scatter Region
3. 将备份下来的 SST 文件按需读取到 TiKV 各节点
4. 根据新 table 的 ID 对 SST 进行 Key Rewrite
5. 将处理好的 SST 文件 Ingest 到 TiKV

### Key Rewrite

由于 TiDB 的数据在 TiKV 那保存的形式是：

```
Key: tablePrefix{tableID}_recordPrefixSep{rowID}
Value: [col1, col2, col3, col4]

Key: tablePrefix{tableID}_indexPrefixSep{indexID}_indexedColumnsValue
Value: rowID
```

在 Key 中编码了 tableID，所以我们不能直接把备份下来的 SST 文件不经任何处理恢复到 TiKV 中，否则就有可能因为 tableID 对不上而导致数据错乱。

为了解决该问题，我们必须在恢复前对 SST 文件进行 key 的改写，将原有的 tableID 替换为新创建的 tableID，同样的 indexID 也需要相同的处理。

### Split & Scatter

TiKV 对 Region 的大小是有限制的，默认为 96MB，超出该阈值则需要分裂（Split）。集群刚启动时，只有少量的 Region 的，我们在恢复的时候不能把所有数据都恢复到这些少量的 Region 中，所以需要提前将 Region 分裂并打散（Scatter）。

由于备份 SST 文件是按照 Region 生成的，天然就有合适的大小和对应的数据范围，所以我们可以在根据各个 SST 文件中的范围对集群中的 Region 进行分裂。分裂完成后还需要打散新分裂出来的 Region，防止发生数据倾斜。

### 恢复流程

现在使用的方案已经不再使用 tikv-importer（它也预计会在 4.0 被移除），转而使用了 TiKV 一组能够下载/导入 SST 文件的新 API。相关的信息可以在[这里](./2019-09-17-design-of-reorganize-importSST-to-TiKV.md)找到。

1. 使用备份的 schema 信息创建新表。
2. 通过备份的元数据构建 key-value 的范围，同时依照新表 ID 构建 rewirte rules。
4. 依照 rewrite rules 和 key 范围分割并打散 regions。
5. 调用 download 和 ingest API 完成导入。

### 问题

1. 在线恢复

这里的困境是 ingest SST 失败会导致各个副本之间的数据不一致。

我们通过 [placement rule](https://pingcap.com/docs-cn/stable/how-to/configure/placement-rules/#placement-rules-使用文档) 实现资源隔离。

2. 原集群恢复

原集群恢复涉及到各种 ID 的问题，最简单办法就是 rewrite key，把 key 中老 ID 替换成新 ID。这是 crdb 的方案，我们同样可以借鉴。

我们通过 [Key rewrite](./2019-09-09-BR-key-rewrite-disscussion.md) 在理论上支持原集群恢复。

### 附录
#### Backup gRPC 服务

备份 RPC 的相关内容已经可以在 [kvproto 仓库](https://github.com/pingcap/kvproto/blob/master/proto/backup.proto) 找到。

新的恢复 RPC 见[这里](https://github.com/pingcap/kvproto/blob/master/proto/import_sstpb.proto)。