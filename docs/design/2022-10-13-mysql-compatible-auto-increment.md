# Proposal: MySQL-Compatible `AUTO_INCREMENT`

- **Author(s)**: [tiancaiamao](https://github.com/tiancaiamao)
- **Discussion PR**: [#38449](https://github.com/pingcap/tidb/pull/38449)
- **Tracking Issue**: [#38442](https://github.com/pingcap/tidb/issues/38442)

## Abstract

This proposal outlines the implementation of a feature to align the behavior of `AUTO_INCREMENT` in TiDB with MySQL, ensuring compatibility.

## Background

MySQL operates as a single-machine database(for the most cases) and offers the [`AUTO_INCREMENT`](https://dev.mysql.com/doc/refman/8.0/en/example-auto-increment.html) table attribute to generate unique identifiers for new rows.

In TiDB, we support the generation of `AUTO_INCREMENT` IDs that are **unique** and **monotonically increasing**, but they may **not be consecutive**.

The current behavior in TiDB does not fully align with MySQL.

TiDB is a distributed database, where each TiDB instance caches a batch of IDs for local allocation. When it exhausts its local cached IDs, it requests another batch. For example, TiDB instance A may be allocated the ID range [0-20000), and instance B takes the range [20000, 40000). Consequently, the next batch for TiDB instance A could be [40000, 60000), resulting in non-consecutive IDs. The ID sequence from instance A might appear as ...19998, 19999, [a gap here], 40000, 40001...

## Proposal

This proposal suggests the introduction of a centralized auto ID allocation service. `AUTO_INCREMENT` IDs will be allocated from this centralized service without any caching mechanism at the TiDB layer. This change ensures compatibility with MySQL's behavior.

## Syntax

The old implementation can still be retained as it offers better performance and may continue to meet the needs of some users.

There are several ways to handle compatibility:

1. **Option 1**: We can rename the syntax for the old implementation, for instance, using names like `AUTO_INCREMENT_FAST`, `AUTO_INCREMENT_OLD`, or `DEFICIT_AUTO_INCREMENT`. The advantage of this approach is that we can make the default syntax MySQL-compatible. However, the downside is that the new `AUTO_INCREMENT` behavior remains a compatibility breaker between old and new versions of TiDB.

2. **Option 2**: Alternatively, we can keep the old implementation unchanged and introduce a new syntax. The benefit of this approach is that our users will not encounter issues when upgrading TiDB.

3. **Option 3**: Another viable option is to reuse the `AUTO_ID_CACHE 1` table option, like this:

   ```sql
   CREATE TABLE t (id int key AUTO_INCREMENT) AUTO_ID_CACHE 1;
   ```

TiDB extends the syntax to control the AUTO ID CACHE size using the AUTO_ID_CACHE table option. Setting AUTO_ID_CACHE 1 is equivalent to no caching. This approach maintains the same semantics while changing the implementation. We believe this is the most optimal approach.

## Implementation

### Centralized Auto ID Allocation Service

The core of this implementation revolves around the introduction of a centralized auto ID allocation service. This service will consist of a single-point process responsible for the allocation of auto IDs.

(To be precise, the allocation service will be embedded within the TiDB process initially, possibly transitioning to a standalone process in the future. However, the fundamental concept remains the same.)

For every `AUTO_INCREMENT` ID allocation, TiDB will send a request to this central process. Internally, the process will increment the ID by one, ensuring that the generated IDs are **unique**, **monotonically increasing**, and **consecutive**.

### Handling Multiple Auto_INCREMENT IDs and Communication Protocol

In this implementation, the `+1` operation is conceptual, and a single request can allocate more than one ID. Multiple `AUTO_INCREMENT` IDs within one statement correspond to a `+n` operation. For instance, when executing a statement like `INSERT INTO t VALUES (null),(null),(null)`, it requests the allocation of 3 new values within one request.

For communication between components, gRPC is employed as the communication protocol. While this aspect is relatively straightforward, we won't delve into intricate details in this document.

Below is a brief overview of the gRPC message structures used:

```protobuf
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

The fields increment and offset may seem to have no special significance here, but they are used to ensure compatibility with MySQL's behavior after setting @@auto_increment_increment and @@auto_increment_offset. It is worth noting that configuring two TiDB clusters for bidirectional master-master replication via this mechanism is not a common use case. Additional details can be found at [#14245](https://github.com/pingcap/tidb/issues/14245).

### High Availability (HA) of the Service

To address the issue of Single Point of Failure (SPOF), the centralized auto ID allocation service should have at least a primary and a backup process, and [etcd](https://etcd.io/) is used for managing this HA setup.

Both the primary and backup processes register themselves in etcd. The elected primary process serves the workload. In the event of a primary process failure (due to a crash, termination, or other reasons), the backup process takes over and continues providing the service. We can conveniently utilize the embedded etcd within TiDB, specifically the PD embedded etcd.

During a switch from the primary to the backup process, it is essential to preserve the information about the current maximum allocated ID. This ensures the uniqueness of IDs. The new primary process allocates IDs starting from the maximum allocated ID plus one.

While one approach could be to persist the maximum allocated ID every time, this can be resource-intensive. An alternative is to periodically persist the maximum allocated ID, and it is safe to allocate IDs in the range `[base, max persisted)`. In cases of abnormal crashes of the primary process, the backup process retrieves the max persisted ID as its base. This optimization may still result in non-consecutive IDs, but such occurrences are infrequent and considered acceptable.

**Note**: MySQL 8.0 has [introduced changes to auto-increment handling](https://dev.mysql.com/doc/refman/8.0/en/innodb-auto-increment-handling.html), making the current maximum auto-increment counter value persistent across server restarts, thus ensuring similar crash recovery behavior:

> In MySQL 8.0, this behavior is changed. The current maximum auto-increment counter value is written to the redo log each time it changes and saved to the data dictionary on each checkpoint. These changes make the current maximum auto-increment counter value persistent across server restarts.

### Client-Side Implementation

On the TiDB client side, support for service discovery is essential to ensure that clients always access the primary process and handle primary-backup switches automatically.

Service discovery is facilitated through etcd. All auto ID service processes register their addresses in etcd, allowing clients to retrieve this information and connect to the primary process seamlessly.

## Performance Evaluation

The introduction of a network round-trip in the auto ID allocation process does impact performance, although it remains acceptable. Within the same data center, the average latency is below 500 microseconds.

Preliminary testing indicates that to achieve a throughput of 20-40K QPS, it consumes approximately 100-200% CPU utilization on a local machine. This level of performance should be sufficient for typical usage scenarios.

## Performance Comparison

`AUTO_INCREMENT` demonstrates lower performance and scalability compared to TiDB alternatives such as `AUTO_RANDOM` and `SHARD_ROW_ID`. The following table provides performance metrics for different worker counts:

| Worker Count | QPS    | Average Latency (ms) | Server CPU Usage (%) |
| ------------ | ------ | --------------------- | -------------------- |
| 1            | 13,484 | 0.074192              | 115                  |
| 2            | 21,525 | 0.085926              | 135                  |
| 4            | 33,984 | 0.11579               | 160                  |
| 8            | 54,488 | 0.133927              | 203                  |
| 16           | 77,359 | 0.207223              | 248                  |
| 32           | 100,893| 0.332895              | 310                  |
| 64           | 118,471| 0.470853              | 357                  |
| 128          | 125,215| 0.977818              | 368                  |
| 256          | 125,901| 2.335869              | 379                  |

![Performance Chart](https://user-images.githubusercontent.com/1420062/195541033-8ec9405e-a309-43c8-baae-392cce1c4df2.png)


