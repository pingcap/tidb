# Proposal: Support the Local/Global Transaction in Cross-DC Deployment

- Author(s):     [Haizhi/JmPotato](https://github.com/JmPotato)
- Last updated:  2020-10-22
- Discussion at: https://github.com/pingcap/tidb/issues/20561

## Abstract

Make TiDB support the Local and Global transactions.

## Background

As a part of [tidb/issues/18273](https://github.com/pingcap/tidb/issues/18273), now PD has the ability to allocate the Local/Global TSO to support the corresponding Local/Global linear consistency (for more details, you can look at [pd/issues/2759](https://github.com/tikv/pd/issues/2759)). Based on this, TiDB could start two kinds of transactions: local and global.

* Local transaction: only allow to read & write the local data which come from the same region of a local TSO
* Global transaction: allow to read & write any data in a cluster with a Global TSO

By supporting these two kinds of transactions, we can reduce the TSO fetching latency caused by the cross-region deployment to improve the transaction performance. So we need TiDB to have a mechanism to support starting these two kinds of transactions.

## Proposal

### Configuration

Since the transaction process is managed on the TiDB side, when initiating a transaction, we need to know the topology information of the current TiDB instance as a PD Client so that we can know which location of the PD to request. Like PD, TiDB needs to introduce a field such as ***dc-location*** in the label configuration. This label is string type, the default value is empty, and used to set which DC the TiDB node belongs to, so that it can distinguish local/global transaction needs when performing TSO services.

And because this label may be used by Auto Scaling or other components, we need to add a switch to control the Local/Global transaction enable/disable in the configuration which will be named as enable-local-transaction, bool type and the default value is false.

For now, PD has a configuration field named ***dc-location*** to specify the DC/Zone/Region a PD instance belonging to.

To do the location configuration change in the PD side now:

* Add a new PD of the DC. Just config the new PDs with the correct ***dc-location*** and make them join in the existing PD cluster.
* Remove a PD of the DC. Reboot the PD cluster without the PDs we want to delete.
* Change the DC of a PD. Stop the PD cluster and re-config the ***dc-location*** of the PD, then boot the PD cluster again.

Since TiDB uses a label to config the instance-level topology rather than configuration, the users can change the label casually, they just need to make sure the value is corresponding to one of the PD Local TSO Allocators. However, the session created before the change will still keep the old ***dc-location*** label value in the session variables.

### Session Variable

Every time a TiDB connection is established and an initial session is created, we need to check whether the current TiDB instance has ***dc-location*** label configured, and then add a session-level variable, whose name is ***tidb_txn_scope***, the default value is “global”, which means the global transaction needs to be performed, but when the ***dc-location*** is not empty, the value of ***tidb_txn_scope*** will be the content of ***dc-location***, which means the local transaction needs to be performed. The reason we use the value of ***dc-location*** as ***tidb_txn_scope*** here is because the ***dc-location*** is needed when we request the PD server so that PD Client knows which dc this request should be handled to. TiDB will specify this when taking the TSO by PD Client and obtain the Local TSO from the corresponding DC.

The ***tidb_txn_scope*** variable is valid for both auto commit and non-auto commit transactions.

***tidb_txn_scope*** = “global”, global transaction by default, all auto commits and non-auto commits will run transactions globally with a Global TSO from the PD leader
***tidb_txn_scope*** = “dc-1”, local transaction by default, all auto commits and non-auto commits will run transactions locally with a Local TSO from a PD node from DC-1

Also, because ***tidb_txn_scope*** is a session variable, the user can set it to “global” or ***dc-location*** manually to perform the global or local transaction during a connection session by default.

Though all sessions will need ***tidb_txn_scope*** to decide which transaction a SQL wants, the internal SQLs TiDB uses need to run as global transactions all the time. We need to do some special treatment to this kind of SQLs.

Also, we can provide a SQL syntax for users like this: SHOW DC-LOCATIONS;

This SQL will return all the ***dc-locations*** available we got from the PD leader to make sure it’s the latest info.

### Data constraint

When performing local transactions, TiDB needs to check whether the physical location of the accessed data satisfies the attribute of "local", i.e., whether the data current transaction access is limited to the same DC as the Local TSO Allocator and TiDB to ensure the linear consistency is not broken.

This data constraint feature will be satisfied by the Placement Rule. Users need to make their own Placement Rules first to plan which data should be placed at where. Based on this, TiDB can check whether the data a local transaction reads or writes is legal.

### Usage

Based on the above functions, now we can talk about how users will use the Local/Global transaction feature.

* Users need to configure the ***dc-location*** of TiDB/PD correctly according to their topology. The TiDB/PD in the same zone should be configured with the same ***dc-location***.
* Users need to configure the Placement Rules for data to specify their zone locations. So that TiDB can check whether a transaction could be allowed to read and write certain data. But for now, the Placement Rule only supports partition-level location isolation. To make the local transaction more easy to use, we need the Placement Rule to support table-level location isolation.
* After the cluster starts, every connection to the TiDB will have a session variable ***tidb_txn_scope*** which will be set to the value of ***dc-location***, for example, if the ***dc-location*** of the TiDB is “dc-1”, then the value of ***tidb_txn_scope*** will be “dc-1” also.
* Users can set ***tidb_txn_scope*** manually. In the feature, we will provide a SQL syntax to easily start each kind of transaction.
* [Next to have] Once all the things are ready, users can normally write SQL to do auto-commit queries and begin transactions manually. And all these transactions are local by default. In this situation, if you want to start a global transaction. You should use SQL like these, which won’t affect ***tidb_txn_scope*** and later transactions:
    - BEGIN GLOBAL;
    - START GLOBAL TRANSACTION;
    - START GLOBAL TRANSACTION WITH CONSISTENT SNAPSHOT;
* [Next to have] If the users don’t label a TiDB instance with ***dc-location***, then the ***tidb_txn_scope*** should be “global”. At this point, if the users have PD configured with ***dc-location*** = “dc-1”, then we can start a local transaction by using SQL like these, which won’t affect ***tidb_txn_scope*** and later transactions:
    - BEGIN LOCAL dc-1;
    - START LOCAL dc-1 TRANSACTION;
    - START LOCAL dc-1 TRANSACTION WITH CONSISTENT SNAPSHOT;


## Rationale

Basically, the new feature needs us to do two things:

- Provide two kinds of transactions
- Provide data constraint check base on the Placement Rule

After merging pd/pr/3045, the PD Client will support the Global/Local TSO batch acquisition, TiDB can request Local TSO by specifying the right ***dc-location***. With the help of the session variable mentioned above, we can easily get the right TSO and start the right transaction.

## Implementation **(Need Discussion)**

### Configuration

Add a new field named ***dc-location*** into the TiDB configuration.

- Type: string
- Default value: “global”
- Constraints
    - Should not be empty
    - Must be same as one of the PD configuration ***dc-location***, which indicates which DC a TiDB locates on

### Session Variable

When a connection is established and the session is initialized, check current TiDB’s ***dc-location*** configuration to create the session variable ***tidb_txn_scope***.

- If ***dc-location*** is empty, then set ***tidb_txn_scope*** with value “global”, which indicates that the later transactions will all be global transactions.
- If ***dc-location*** is not empty, then set ***tidb_txn_scope*** with the value of ***dc-location***, such as “dc-1”, which indicates that the later transactions will all be local transactions belong to DC-1.

This session variable is valid for both auto commit and non-auto commit transactions. Users can set ***tidb_txn_scope*** manually to get the transaction kind they want during the session. Also, if the ***tidb_txn_scope*** doesn’t exist or is empty, TiDB will start the global transaction by default.

### Data constraint

#### Placement Rule

We can use the label-constraint of the Placement Rule to check the ***dc-location*** label of TiDB to make sure whether the data we access is what we want and we are allowed to read.

#### TiDB

* Read the PartitionID by table name in TiDB, and with the PartitionID, we can get the corresponding Placement Rule.
* With TiDB instance’s ***dc-location*** label, we can use label-constraint of the Placement Rule to check the data constraint.
* Build a checker including the above checks and install it into all read & write operators to check the data location information during a local transaction.
* Once the illegal data accessing is detected, we need to stop and rollback the transaction while throwing out an error.

There is a possible problem here. Because the Placement Rule can be modified at any time. TiDB needs to be aware of these changes and update the location information stored as soon as possible. For example, the user changes a Placement Rule which will move some data from DC-1 to DC-2. But because of the delay, at a certain point, both TiDBs in DC-1 and DC-2 think they can start a local transaction for the same data. How should we prevent this kind of situation from happening?

* [A possible solution](https://docs.google.com/document/d/1VYudmmxRqlO8W3C1mcVojrit5BMaMJKu3HBrqHS-emk) (Chinese)
* [Need confirmation] Since the Placement Rule bundles can be obtained by the DDL info, it seems that we can check this by the scheme version checker or something else to be aware of the changing ASAP.

## Testing Plan

[The testing plan](https://docs.google.com/document/d/173PNdh_1wgK7f_7f9HCjVUXWcTX3w8pG_MN4Zgvy2EI) we composed before (Chinese).