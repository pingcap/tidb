# Proposal: Support the Local/Global Transaction in Cross-DC Deployment

- Author(s):     [Haizhi/JmPotato](https://github.com/JmPotato)
- Last updated:  2020-10-22
- Discussion at: https://github.com/pingcap/tidb/issues/20561

## Abstract

Make TiDB support the Local/Global transactions with the Local/Global TSO feature provided by the PD.

## Background

As a part of [tidb/issues/18273](https://github.com/pingcap/tidb/issues/18273), now PD can allocate the Local/Global TSO to support the corresponding Local/Global linear consistency (for more details, you can look at [pd/issues/2759](https://github.com/tikv/pd/issues/2759)). Based on this, TiDB could start two kinds of transactions: local and global.

* Local transaction: only allow to read & write the local data which come from the same region of a local TSO
* Global transaction: allow to read & write any data in a cluster with a Global TSO

By supporting these two kinds of transactions, we can reduce the TSO fetching latency caused by the cross-region deployment to improve the transaction performance. So we need TiDB to have a mechanism to support starting these two kinds of transactions.

## Proposal

### Configuration

Since the transaction process is managed on the TiDB side, when initiating a transaction, we need to know the topology information of the current TiDB instance as a PD Client so that we can know which location of the PD to request. Like PD, TiDB needs to introduce a field such as **_dc-location_** in the label configuration. This label is a string type, the default value is empty, and used to set which DC the TiDB node belongs to so that it can distinguish local/global transaction needs when performing TSO services.

And because this label may be used by Auto Scaling or other components, we need to add a switch to control the Local/Global transaction enable/disable in the configuration which will be named as **_enable-local-transaction_**, bool type, and the default value is **false**.

For now, PD has a configuration field named **_dc-location_** to specify the DC/Zone/Region a PD instance belonging to.

Since TiDB uses a label to config the instance-level topology rather than configuration, the users can change the label casually, they just need to make sure the value is corresponding to one of the PD Local TSO Allocators. However, the session created before the change will still keep the old **_dc-location_** label value in the session variables.

### Session Variable

Every time a TiDB connection is established and an initial session is created, we need to check whether the current TiDB instance has **_dc-location_** label configured, and then add a **session-level** variable, whose name is **_tidb_txn_scope_**, the default value is **“global”**, which means the global transaction needs to be performed, but when the **_dc-location_** is not empty, the value of **_tidb_txn_scope_** will be the content of **_dc-location_**, which means the local transaction needs to be performed. The reason we use the value of **_dc-location_** as **_tidb_txn_scope_** here is that the **_dc-location_** is needed when we request the PD server so that PD Client knows which dc this request should be handled. TiDB will specify this when taking the TSO by PD Client and obtain the Local TSO from the corresponding DC.

The **_tidb_txn_scope_** variable is valid for both auto commit and non-auto commit transactions.

* **_tidb_txn_scope_** = “global”, global transaction by default, all auto commits and non-auto commits will run transactions globally with a Global TSO from the PD leader
* **_tidb_txn_scope_** = “dc-1”, local transaction by default, all auto commits and non-auto commits will run transactions locally with a Local TSO from a PD node from DC-1

Also, because **_tidb_txn_scope_** is a session variable, the user can set it to **“global”** or **_dc-location_** manually to perform the global or local transaction during a connection session by default.

Though all sessions will need **_tidb_txn_scope_** to decide which transaction a SQL wants, the internal SQLs TiDB uses need to run as global transactions all the time. We need to do some special treatment for this kind of SQLs.

Also, we can provide a SQL syntax for users like this:

```sql
SHOW TRANSACTION SCOPES;
```

This SQL will return all the **_dc-locations_** available we got from the PD leader to make sure it’s the latest info. Also, we should provide the DC typology by the **CLUSTER_INFO** as the SQL shown below.

```sql
SELECT * FROM CLUSTER_INFO;
```

So far, in a word, with configuring the typology of PD correctly, if we want to enable the ability of TiDB to run the Local Transaction, we need to develop these functions:

* Label the TiDB **_dc-location_** with the content of DC location info by TiDB’s `Label` feature and add a switch in configuration to enable/disable the Local Transaction.
* Create the right session scope variable **_tidb_txn_scope_** according to the **_dc-location_** label.
* Users can query all **_dc-location_** available in the cluster by a SQL.

### Data constraint

When performing local transactions, TiDB needs to check whether the physical location of the accessed data satisfies the attribute of "local", i.e., whether the data current transaction access is limited to the same DC as the Local TSO Allocator and TiDB to ensure the linear consistency is not broken.

This data constraint feature will be satisfied by the Placement Rule. Users need to make their own Placement Rules first to plan which data should be placed at where. Based on this, TiDB can check whether the data a local transaction reads or writes is legal.

### Usage

Based on the above functions, now we can talk about how users will use the Local/Global transaction feature.

* Users need to configure the label of TiDB/PD correctly according to their topology. The TiDB/PD in the same zone should be configured with the same **_dc-location_**.
* Users need to configure the Placement Rules for data to specify their zone locations. So that TiDB can check whether a transaction could be allowed to read and write certain data. For example, you could run a SQL like this to add a new Placement Rule:

    ```sql
    ALTER TABLE test_table ALTER PARTITION test_partition
        ADD PLACEMENT LABEL="+dc-location=dc-1" ROLE=voter COUNT=3;
    ```

* This SQL will keep the data of test_partition limited in the dc-1. **But for now, the Placement Rule only supports partition-level location isolation. To make the local transaction easier to use, we need the Placement Rule to support table-level location isolation in the future.**
* After the cluster starts, every connection to the TiDB will have a session variable **_tidb_txn_scope_** which will be set to the value of **_dc-location_**, for example, if the **_dc-location_** of the TiDB is “dc-1”, then the value of **_tidb_txn_scope_** will be “dc-1” also.
* Users can set **_tidb_txn_scope_** manually. In the feature, we will provide a SQL syntax to easily start each kind of transaction.
* [Next to have] Once all the things are ready, users can normally write SQL to do auto-commit queries and begin transactions manually. And all these transactions are local by default. In this situation, if you want to start a global transaction. You should use SQL like these, which won’t affect **_tidb_txn_scope_** and later transactions:

    ```sql
    BEGIN GLOBAL;
    START GLOBAL TRANSACTION;
    START GLOBAL TRANSACTION WITH CONSISTENT SNAPSHOT;
    ```

* [Next to have] If the users don’t label a TiDB instance with **_dc-location_**, then the **_tidb_txn_scope_** should be “global”. At this point, if the users have PD configured with **_dc-location_** = “dc-1”, then we can start a local transaction by using SQL like these, which won’t affect **_tidb_txn_scope_** and later transactions:

    ```sql
    BEGIN LOCAL dc-1;
    START LOCAL dc-1 TRANSACTION;
    START LOCAL dc-1 TRANSACTION WITH CONSISTENT SNAPSHOT;
    ```

## Rationale

The new feature needs us to do two things:

* Provide two kinds of transactions
* Provide data constraint check base on the Placement Rule

After merging [pd/pr/3045](https://github.com/tikv/pd/pull/3045), the PD Client will support the Global/Local TSO batch acquisition, TiDB can request Local TSO by specifying the right **_dc-location_**. With the help of the session variable mentioned above, we can easily get the right TSO and start the right transaction.

## Implementation

### Configuration

Add a new field named **_dc-location_** into the TiDB configuration.

* Type: string
* Default value: “global”
* Constraints
    * Should not be empty
    * Must be same as one of the PD configuration **_dc-location_**, which indicates the zone this TiDB locates on

### Session Variable

When a connection is established and the session is initialized, check current TiDB’s **_dc-location_** configuration to create the session variable **_tidb_txn_scope_**.

* If **_dc-location_** is empty, then set **_tidb_txn_scope_** with value “global”, which indicates that the later transactions will all be global transactions.
* If **_dc-location_** is not empty, then set **_tidb_txn_scope_** with the value of **_dc-location_**, such as “dc-1”, which indicates that the later transactions will all be local transactions belonging to DC-1.

This session variable is valid for both auto commit and non-auto commit transactions. Users can set **_tidb_txn_scope_** manually to get the transaction kind they want during the session. Also, if the **_tidb_txn_scope_** doesn’t exist or is empty, TiDB will start the global transaction by default.

### Data constraint

#### Placement Rule

We can use the **_label-constraint_** of the Placement Rule to check the **_dc-location_** label of TiDB to make sure whether the data we access is what we want and we are allowed to read.

#### TiDB

* Read the PartitionID by table name in TiDB, and with the PartitionID, we can get the corresponding Placement Rule.
* With the TiDB instance’s **_dc-location_** label, we can use the **_label-constraint_** of the Placement Rule to check the data constraint.
* Build a checker including the above checks and install it into all read & write operators to check the data location information during a local transaction.
* Once the illegal data accessing is detected, we need to stop and rollback the transaction while throwing out an error.

There is a possible problem here. Because the Placement Rule can be modified at any time. TiDB needs to be aware of these changes and update the location information stored as soon as possible. For example, the user changes a Placement Rule which will move some data from DC-1 to DC-2. But because of the delay, at a certain point, both TiDBs in DC-1 and DC-2 think they can start a local transaction for the same data. How should we prevent this kind of situation from happening?

* [A possible solution](https://docs.google.com/document/d/1VYudmmxRqlO8W3C1mcVojrit5BMaMJKu3HBrqHS-emk/edit#) (Chinese).
* [Need confirmation] Since the Placement Rule bundles can be obtained by the DDL info, it seems that we can check this by the scheme version checker or something else to be aware of the change ASAP.

## Testing Plan

[The testing plan](https://docs.google.com/document/u/0/d/173PNdh_1wgK7f_7f9HCjVUXWcTX3w8pG_MN4Zgvy2EI/edit) we composed before (Chinese).
