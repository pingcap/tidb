# Proposal: Support the Local/Global Transaction in Cross-DC Deployment

- Author(s):     [Haizhi/JmPotato](https://github.com/JmPotato)
- Last updated:  2020-10-22
- Discussion at: https://github.com/pingcap/tidb/issues/20561

## Abstract

Make TiDB support the Local and Global transactions.

## Background

As a part of [tidb/issues/18273](https://github.com/pingcap/tidb/issues/18273), now PD has the ability to allocate the Local/Global TSO to support the corresponding Local/Global linear consistency (for more details, you can look at [pd/issues/2759](https://github.com/tikv/pd/issues/2759)). Based on this, TiDB could start two kinds of transactions: local and global.

* Local transaction: only allow to read & write the local data located in the same zone with local TSO.
* Global transaction: allow to read & write any data in the cluster with Global TSO

By supporting these two kinds of transactions, we can reduce the TSO fetching latency caused by the cross-region deployment in order to improve the transaction performance. Thus we need TiDB to have a mechanism to support this ability.

## Proposal

### Configuration

Since the transaction process is managed on the TiDB side, before initiating a transaction, the tidb-server needs to know the topology information for itself so that we can know the PD with the same location to request. Like PD, TiDB needs to introduce a field such as ***dc-location*** in the label configuration. This label is a string type, the default value is empty, and used to set which DC the TiDB node belongs to, so that it can distinguish local/global transaction needs when performing TSO services.

As the label configuration could be modified for other purposes, we will also provide a switch to enable/disable the Local/Global transaction ability in the configuration. It will be named as enable-local-transaction, bool type, and the default value is false.

For now, PD has a configuration field named ***dc-location*** to specify the DC/Zone/Region a PD instance belonging to.

To do the location configuration change in the PD side now:

* Add a new PD of the DC. Just config the new PDs with the correct ***dc-location*** and make them join in the existing PD cluster.
* Remove a PD of the DC. Reboot the PD cluster without the PDs we want to delete.
* Change the DC of a PD. Stop the PD cluster and re-config the ***dc-location*** of the PD, then boot the PD cluster again.

Since TiDB uses label configuration to configure the instance-level topology rather than configuration, the users should change the label casually. They should make sure the value is corresponding to one of the PD Local TSO Allocators. However, the session created before the change will still keep the old ***dc-location*** label value in the session variables.

### Session Variable

Every time a TiDB connection is established and an initial session is created, we need to check whether the current TiDB instance has ***dc-location*** label configured, and then add a session-level variable, whose name is ***tidb_txn_scope***, the default value is “global”, which means the global transaction needs to be performed, but when the ***dc-location*** is not empty, the value of ***tidb_txn_scope*** will be the content of ***dc-location***, which means the local transaction needs to be performed. The reason we use the value of ***dc-location*** as ***tidb_txn_scope*** here is because the ***dc-location*** is needed when we request the PD server so that PD Client knows which dc this request should be handled. TiDB will specify this when taking the TSO by PD Client and obtain the Local TSO from the corresponding DC.

The ***tidb_txn_scope*** variable is valid for both auto commit and non-auto commit transactions.

***tidb_txn_scope*** = “global”, global transaction by default, all auto commits and non-auto commits will run transactions globally with a Global TSO from the PD leader
***tidb_txn_scope*** = “dc-1”, local transaction by default, all auto commits and non-auto commits will run transactions locally with a Local TSO from a PD node from DC-1

Also, because ***tidb_txn_scope*** is a session variable, the user can set it to “global” or ***dc-location*** manually to perform the global or local transaction during a connection session by default.

Though all sessions will need ***tidb_txn_scope*** to decide which transaction a SQL wants, the internal SQLs TiDB uses need to run as global transactions all the time. We need to do some special treatment for this kind of SQLs.

Also, we can provide a SQL syntax for users like this: SHOW DC-LOCATIONS;

This SQL will return all the ***dc-locations*** available we got from the PD leader to make sure it’s the latest info.

### Data constraint

When performing local transactions, TiDB needs to check whether the physical location of the accessed data satisfies the attribute of "local", i.e., whether the data current transaction access is limited to the same DC as the Local TSO Allocator and 