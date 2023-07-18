# Label tidb workers 

- Author: [Weiqi Yan](https://github.com/ywqzzy)
- Tracking Issue:
  - https://github.com/pingcap/tidb/issues/45430

## Background and Benefits
The distributed background task parallel execution framework(brief: the framework) requires multiple TiDB servers to serve as scheduler and execute real subtasks for task. In the previous implementation, all TiDB servers can be selected as candidate workers by default. However, given the resource-intensive nature of distributed tasks, it is important to ensure smooth operation of both foreground and background tasks without interference.

To address this, we aim to provide users with the flexibility to designate specific TiDB servers as "scheduler"/"worker". These labeled tidb servers will be dedicated to running distributed tasks, while other servers will not be involved in these operations. This approach allows users to manage resource allocation effectively and ensure optimal performance for both foreground and background tasks.

## Goal
The goal of this RFC is to enhance the existing system by leveraging the labels config for each TiDB server. This variable will enable the dispatcher to assign subtasks exclusively to TiDB servers that are marked with specific labels. By doing so, it ensures that subtasks are executed within a designated set of TiDB servers, providing better control and resource allocation for foreground and background tasks.

In the first phase, We will add the "tidb_role" label.
When the "tidb_role" field is "" means that the server will not run distributed tasks.
When the "tidb_role" field is "dist_worker" means that the server will run distributed tasks.

When no TiDB server is "dist_worker", fallback to previous behavior, and use all TiDB servers to run distributed tasks.

## Detail Design

### How to label

The process is as follows:
1. Deploy TiDB server using `config.toml`.
2. Set the `labels` field in `config.toml` into `ServerInfo`, and sync the `ServerInfo` to etcd.
By doing so, the cluster can manage all "tidb_role" of each TiDB server.

### How to select eligible instances
1. In `dispatcher` of dist task framework, fetch all `ServerInfo` in the cluster
2. Filter `ServerInfo` using "tidb_role" label, only TiDB server with "dist_worker" role can run distributed tasks.

## Usage
When deploying TiDB cluster, change the `tidb.toml` of each TiDB servers.
```
[labels]
tidb_role = "dist_worker"
```
All servers with `tidb_role` = "dist_worker" will be used to run distributed tasks.


## Future Work

In phase two, we can use "tidb_role" to let tidb server run different kind of tasks.

