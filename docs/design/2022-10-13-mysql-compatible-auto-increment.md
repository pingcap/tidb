# Proposal: MySQL compatible `AUTO_INCREMENT`

- Author(s): [tiancaiamao](https://github.com/tiancaiamao)
- Discussion PR: https://github.com/pingcap/tidb/pull/38449
- Tracking Issue: https://github.com/pingcap/tidb/issues/38442

## Abstract

This proposes an implementation of making the `AUTO_INCREMENT` behaviour compatible with MySQL.

## Background

MySQL deploys on a single-machine, and it provides the [`AUTO_INCREMENT`](https://dev.mysql.com/doc/refman/8.0/en/example-auto-increment.html) table attribute to generate a unique identity for new rows.

In TiDB, we support generating `AUTO_INCREMENT` IDs that are **unique**, **monotone increasing**, but the IDs may **not be consecutive**.
The current behaviour is not fully-compatible with MySQL.

TiDB is a distributed database, each TiDB instance caches a batch of IDs for local allocating. When it exhaust its local cached IDs, it ask for another batch. For example, TiDB instance A may get ID range [0-20000), and instance B takes away range [20000, 40000), the next batch for TiDB instance A could be [40000, 60000), that's why the IDs are not **consecutive**. The ID sequence from instance A might be ...19998, 19999, [a hole here], 40000, 40001...

## Proposal

This proposes that we introduce a centralized auto ID allocating service, the `AUTO_INCREMENT` ID is allocated from the centralized service and there is no caching mechanism on the TiDB layer, so the behaviour can be MySQL compatible.

## Compatibility

The old implementation can still be kept, its performance is better and might meet some user's needs.
We can rename the syntax for it, such as `AUTO_INCREMENT_FAST` or `AUTO_INCREMENT_OLD` or `DEFICIT_AUTO_INCREMENT`.

The new `AUTO_INCREMENT` behaviour is still a compatibility-breaker between old and new versioned TiDB, but it's worthy on behalf of MySQL compatibility.

## Implementation

### Centralized auto ID allocating service

This is the core part of the implementation. There will be a single-point process, responsible for allocating auto IDs.

For every `AUTO_INCREMENT` ID allocation, TiDB send a request to this process. The process will `+1` internally, so the ID is **unique**, **monotone increasing**, and **consecutive**.

Grpc is used for the communication protocol. This part is relatively easy so I would not go to much details in this document.

```
message AutoIDRequest {
    int64 dbID = 1;
    int64 tblID = 2;
    uint64 n = 3;
    int64 increment = 4;
    int64 offset = 5;
}

message AutoIDResponse {
    int64 min = 1;
    int64 max = 2;
}

service AutoIDAlloc {
    rpc AllocAutoID(AutoIDRequest) returns (AutoIDResponse) {}
}
```

### HA of the service

To overcome the SPOF(single-point-of-failure) issue, the centralized auto ID allocating service should have at least a primary and a backup process. [Etcd](https://etcd.io/) is used for that.

Both the primary and backup regist themself in the etcd, the elected primay process serves the workload. When it's gone (crash, quit or anything), the backup take up the role and provide the service. We can just use the pd embedded etcd, this is quite convenient.

When the switch happen, the current max allocated ID information is required so as to guarantee the uniqueness. The new primary process allocate IDs begin from the max allocated ID plus one.

We can persist the max allocated ID every time, but that's costly. Another choice is persisting it periodically, it's safe to allocate IDs in range `[base, max persisted)`. When the primary process crash abnormally, the backup process gets the max persisted ID as its base. Etcd is also used as the storage for the max persisted ID. This optimization could still make the ID not be consecutive, but it's not so common (and I believe MySQL may also crash and facing such problem?), so this is not a big issue.

### Client side

In TiDB side, the client should support service discovering, so as to always access the primary process and handle primary-backup switch automatically.

This is done with the help of etcd. All the auto ID service processes regist their address in etcd, so the client can find the information and connect to the primary.

## Performance evaluation

A network round-trip is involved, so the performance is not as good as the old way. But it should still be acceptable. Within the same data center, the latency is below 500 microsecond on average.

Some preliminary test shows that to get getting 20~40K QPS, it cost about 100~200% CPU on my local machine, this should be enough for the basic usage.
`AUTO_INCREMENT` is not of high performance, and is not scalable compare to TiDB alternatives like `AUTO_RANDOM` and `SHARD_ROW_ID`.

| worker count | qps    | avg latency (ms) | server cpu |
| -----        | --     | ---              | ---        |
| 1            | 13484  | 0.074192         | 115        |
| 2            | 21525  | 0.085926         | 135        |
| 4            | 33984  | 0.11579          | 160        |
| 8            | 54488  | 0.133927         | 203        |
| 16           | 77359  | 0.207223         | 248        |
| 32           | 100893 | 0.332895         | 310        |
| 64           | 118471 | 0.470853         | 357        |
| 128          | 125215 | 0.977818         | 368        |
| 256          | 125901 | 2.335869         | 379        |

![image](https://user-images.githubusercontent.com/1420062/195541033-8ec9405e-a309-43c8-baae-392cce1c4df2.png)

