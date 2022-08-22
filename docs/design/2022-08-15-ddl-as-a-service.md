# Proposal: DDL as a Service

- Author(s): [lcwangchao](https://github.com/lcwangchao)

## Introduction

The proposal of this design is to allow DDL jobs from multiple clusters to share the tidb-server nodes from one cluster. The DDL capability can be exposed as a service.

## Background

Currently, the DDL jobs are running in one of the tidb-server node which is elected as the owner. However, other queries are also running in the DDL owner node which will be affected by DDL jobs more or less. Though we can set `tidb_enable_ddl` to `false` to forbid the specified node being elected as the DDL owner, we still need to reserve one or two nodes to run ddl only and this will lead to a waste of resources.
With providing a dedicated cluster to provide DDL service, we can delegate all DDL jobs from different clusters to it. In this way, the computing resources can be reused, and we can avoid DDL and other queries affect each other.

## Detail Design

### Goal

- Support a tidb cluster running as "ddl service mode" which allow executing ddl jobs from other clusters.
- The DDL service cluster should provide APIs to register/unregister a cluster who want to delegate its DDL jobs to it. It should also provide some read APIs to get the current status.
- The DDL service cluster should balance all the delegated DDL owners to its own tidb-servers. When the tidb-servers are scale-in/scale-out, the DDL owners should be rebalanced.

### Non-Goals

- Not for distribute jobs from a same cluster to multiple tidb-server nodes. There is still only one DDL owner for one cluster at one time and the jobs can only be running with DDL owner.

### Metadata

The DDL service cluster has its own PD, and we can store the metadata to it. Some new keys can be added:

- `/tidb/ddl_service/cluster_registers/<clusterID>`: Each key corresponds to a registered cluster. The value stores the cluster's information like the PD address, cluster version, etc.
- `/tidb/ddl_service/worker_nodes/<nodeID>`: Each key corresponds to a ddl-service node which acts as a "worker" to run DDL jobs. A worker node will campaign the DDL owner of the registered clusters assigned to it.
- `/tidb/ddl_service/ddl_owner_assignments/<clusterID>`: Each key corresponds to the DDL owner assignment of a cluster. The value of this key is the worker node ID the cluster's DDL owner is assigned to.
- `/tidb/ddl_service/leader` A key prefix used by worker nodes to campaign the leader of the DDL service.

### Service Leader

A `leader` is needed to do some management works of the DDL service cluster. Like DDL owner, we can use etcd's [Election](https://pkg.go.dev/go.etcd.io/etcd/clientv3/concurrency#Election) to make sure there is only one leader in the cluster at the same time. All worker nodes try to campaign the leader with the key prefix `/tidb/ddl_service/leader` and one of them will win.

### Schedule of DDL Owners

Once a leader is elected, it will create a background task and start to schedule DDL owners to worker nodes. The leader will make sure every registered cluster is assigned to a worker node. After the assignments are determined, the leader persist the result to the keys `/tidb/ddl_service/ddl_owner_assignments/<clusterID>` in PD for each cluster. The leader should also make the workload balanced, that means once a new cluster registered or a cluster unregistered or scale-in/scale-out the worker nodes, the leader should recompute the assignments and update the corresponding keys in PD.

Every worker node should watch the key with prefix `/tidb/ddl_service/ddl_owner_assignments/`. Once any of the keys are changed, the worker  node will:

- If a new assignment to it is created, the worker node will start a new DDL owner for the corresponding cluster. Some components such as `domain` required by DDL should also be created.
- If an old assignment to itself is deleted, the worker node will stop the corresponding DDL owner and the components it depends.

Every worker node should keep alive with PD so that when a worker crashes or network isolation happens the leader can detect it. Then the leader will delete all the assignments about the crashed worker and reschedule these clusters. We can use etcd's [Session](https://pkg.go.dev/go.etcd.io/etcd/clientv3/concurrency#Session) to implement the keep alive mechanism.

### DDL ID

Currently, a `ddl_id` is assigned to each tidb-server. `ddl_id` is used by async online schema changing to identify if all tidb-server's schema versions are synced. For the DDL owner running in a service cluster, we should also assign it a `ddl_id`. But there are some changes:

- The `ddl_id` of the DDL owner running in a service cluster should have a special format like `ddl-service-xxxxx` to separate it with other tidb-servers in the original cluster.
- DDL owners from different clusters in the same worker node should have different ids.

For DDL owner running in a service cluster, it still needs to start the background task `loadSchemaInLoop` and update its own schema version to the etcd key `/tidb/ddl/all_schema_versions/{ddl_id}`.

### API

Several HTTP APIs will be added to the work nodes of DDL service cluster to provide the basic management operations:

**Register/Update Cluster**

Register a cluster to the service or update an exist cluster's info.

```shell
curl -X PUT http://{TiDBIP}:10080/ddlservice/registered_clusters/cluster1 -d '{
  "store_addr": "tikv://192.168.1.100:2379",
}'
```

**Unregister Cluster**

Unregister a cluster

```shell
curl -X DELETE http://{TiDBIP}:10080/ddlservice/registered_clusters/cluster1
```

**List all registered clusters**

This API returns the stats of all the registered clusters. `schedule.status` is an enum with 3 values:

- SCHEDULING: The assigned node is not running the cluster's DDL task, it is still under scheduling.
- CAMPAIGNING: The cluster's DDL task is running in the assigned node, but it is still campaigning the DDL owner.
- OWNER: The cluster's DDL owner is running in the assignment node.

```shell
curl http://{TiDBIP}:10080/ddlservice/registered_clusters
[
  {
    "id": "cluster1",
    "store_addr": "tikv://192.168.1.100:2379",
    "schedule": {
      "assigned_to": "ddl-service-node-1",
      "status": "OWNER"
    }
  }
]
```

**List all worker nodes**

```shell
curl http://{TiDBIP}:10080/ddlservice/worker_nodes
[
  {
    "id": "ddl-service-node-1",
    "assignments": [
      {
        "cluster_id": "cluster1",
        "schedule": {
          "assigned_to": "ddl-service-node-1",
          "status": "OWNER"
        }
      }
    ]
  }
]
```

### Compatibility

#### Compatibility with Different TiDB Cluster Versions

DDL service will be facing different versions of registered clusters. Notice that the DDL job requests are encoded and sent by original clusters code and the job is running by DDL cluster's code, we need to introduce some rules to make them compatible:

- DDL service cluster should have an equal or higher version than the clusters registered in it. The code in DDL should be compatible with the lower version. 
- To reduce the development burden, we do not need to keep the compatibility with the codes that is too old. But the DDL cluster should reject some old version clusters registering to it. We can also do some checks when a job is consumed from the queue, rejecting it if it is sent from an old version tidb-server that will no longer be supported.

### Bootstrap

Currently, `session.BootstrapSession` will be called when a tidb-server starts. When `BootstrapSession` is called, it will compare self version and the already bootstrapped version saved in store, and then it will upgrade the meta to the newest if self version is bigger. In the current implement, some upgrade operations can only be performed when the current tidb-server is the DDL owner to avoid conflicts.

One problem is that if ddl owner is moved to DDL service cluster, no one will upgrade the meta in the original cluster. One way to solve it is to decouple the bootstrap works with DDL owner. The tidb-servers should lock another key in etcd and once one tidb-server succeed, is is permitted to perform the upgrade operations.

#### infosync.Infosyncer

`infosync.Infosyncer` is used by DDL worker to interact with PD like updating placement rule or server infos. However, it is a static variable of golang currently. To run multiple DDL owner in one tidb instance, we must make sure each DDL owner has its own `Infosyncer` in `ddlCtx`. It is easy to make it when `NewDDL` and we can pass a new created `Infosyncer` to the ddl object.

However, sometimes it's hard to access a context which has a `Infosyncer` object, we still need to keep a global reference for the default `Infosyncer`. The "default" `Infosyncer` means it is the `Infosyncer` of the current domain of the service cluster, not any of the registered clusters. The default `Infosyncer` can be used by other components except for DDL.

#### TiDB-Dashboard

`infosync.Infosyncer` will update the keys `/topology/tidb/{host}:{ip}/[tidb|ttl]` in etcd and these keys will be required by TiDB-Dashboard to fetch the tidb's topology and last updated time. For non-global `Infosyncer` used by registered DDL jobs, we should not update these key by:

- Skip update these two keys when `Infosyncer.init`.
- Do not start background task `topologySyncerKeeper` for registered cluster's domain.

#### Server Info in Etcd

`infosync.InfoSyncer` will update the keys `/tidb/server/info/{ddl_id}` store the server infos, currently, they are used in some cases:

- Used by `admin show ddl` to fetch the owner's information like IP and port.
- Used when querying cluster table such as `information_schema.CLUSTER_PROCESSLIST` to retrieve data from all tidb-server instances.

For `admin show ddl`, it's not safe to allow user to see the worker node's IP and port in DDL service cluster, especially in a cloud environment. The simplest way to do it is not to update key `/tidb/server/info/{ddl_id}` when the domain is running in a service cluster. By forbid updating server info key, querying cluster tables will ignore the tidb-server instances in the DDL service cluster.

#### GC

Every tidb-server will update the key `/tidb/server/minstartts/{ddl_id}` with their min start ts of all running transactions. If a DDL owner is running in DDL service cluster, it should also update this key to avoid some records is deleted by mistake.

#### System Variables

In this design, DDL owners from multiple clusters will be running in one tidb-server process. One problem is that which configuration should we use in DDL owner? For system variables, there are 4 scopes: "none", "global", "instance" and "session", we can talk about them separately.

The variables with scope "none" and "instance" store their values in static fields, so different DDL owners in the same tidb-server process will get the same value. But it is safe for their semantics.

The variables with scope "session" store their values in a session context, so different DDL owners will get the value from the different `sessionctx.Context` objects. Notice that a new created `sessionctx.Context` should inherit the domain's global value if a variable also has "global" scope, so we need to keep sync with global value changes like what `LoadSysVarCacheLoop` do.

Some variables which are defined with only one scope "global" store the values in static fields. This may cause a problem that when the DDL owner of registered cluster want to read these variables, they may get the values from other domain by mistake. For these variables, we should avoid storing the value in static fields by moving them to `variable.SessionVars` in `sessionctx.Context`.

#### Metrics

Because DDL owners from multiple clusters will run together in DDL service, it's better to separate the metrics from different clusters. Some of these metrics are listed below:

- tidb_ddl_owner_handle_syncer_duration_seconds
- tidb_ddl_worker_operation_duration_seconds
- tidb_ddl_worker_operation_total
- tidb_ddl_add_index_total
- tidb_ddl_backfill_percentage_progress
- tidb_ddl_job_table_duration_seconds
- tidb_ddl_running_job_count
