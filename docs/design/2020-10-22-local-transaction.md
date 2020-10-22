# Proposal: Support the Local/Global Transaction in Cross-DC Deployment

- Author(s):     [Haizhi/JmPotato](https://github.com/JmPotato)
- Last updated:  2020-10-22
- Discussion at: https://github.com/pingcap/tidb/issues/20561

## Abstract

Make TiDB support the Local and Global transactions.

## Background

As a part of [tidb/issues/18273](https://github.com/pingcap/tidb/issues/18273), now PD has the ability to allocate the Local/Global TSO to support the corresponding Local/Global linear consistency (for more details, you can look at [pd/issues/2759](https://github.com/tikv/pd/issues/2759)). Based on this, TiDB could start two kinds of transactions: local and global.

Local transaction: only allow to read & write the local data which come from the same region as local TSO
Global transaction: allow to read & write any data in a cluster with a Global TSO

By supporting these two kinds of transactions, we can reduce the TSO fetching latency caused by the cross-region deployment to improve the transaction performance. So we need TiDB to have a mechanism to support starting these two kinds of transactions.

## Proposal

### Configuration

Since the transaction process is managed on the TiDB side, when initiating a transaction, we need to know the topology information of the current TiDB instance as the PD Client and the location of the PD we can request in the same DC. Like PD, TiDB needs to introduce a new field named ***dc-location*** in the configuration which is string type, the default value is empty, and used to set which DC the TiDB node belongs to, so that it can distinguish local/global transaction needs when performing TSO services.

### Session Variable

Every time a TiDB connection is established and an initial session is created, we need to check whether the current TiDB instance has ***dc-location*** configured, and then add a session variable, such as ***tidb_txn_scope***, the default value is “global” which means the global transaction needs to be performed, but when the ***dc-location*** is not empty, the value of ***tidb_txn_scope*** will be the ***dc-location*** content, that is, the local transaction needs to be performed. The ***tidb_txn_scope*** variable is valid for both auto commit and non-auto commit transactions. TiDB will specify this when taking the TSO by PD Client and obtain the Local TSO from the corresponding DC.

Also, because ***tidb_txn_scope*** is a session variable, the user can set it to “global” or ***dc-location*** manually to perform the global or local transaction during a connection session.

### Data constraint

When performing local transactions, TiDB needs to check whether the physical location of the accessed data satisfies the attribute of "local", i.e., whether the data current transaction access is limited to the same DC as the Local TSO Allocator and TiDB to ensure the linear consistency is not broken.

This data constraint feature will be satisfied by the Placement Rule. Users need to make their own Placement Rules first to plan which data should be placed at where. Based on this, TiDB can check whether the data a local transaction read or write is legal.

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

Add a field to Placement Rule named ***dc-location*** to indicate the DC location information of the corresponding data. The reason we don’t just use the current existing label location-label is that this attribute has a certain effect in PD scheduling (for topological isolation degree judgment), so using it may bring some compatibility issues and coupling with other functions.

#### TiDB

First, get all Placement Rules from PD by HTTP API. Then we need to filter out the rules we want.

By ***dc-location***, collect all Placement Rules having the same label.

- According to these Placement Rules, we can get all Table ID, Index ID,  and Partition ID which satisfy these rules.
    - **[Need Discussion]** How can we get all Table ID, Index ID,  and Partition ID which satisfy a certain Placement Rule?
- With this ID information, we need to store it into TiDB so that the later transactions can read this data to check the data location constraint.
    - **[Need Discussion]** How to store this ID information?
        - TiDB Cache
        - Store into the scheme, for example, Table 1 is contained by Placement Rule A and Placement Rule A is labeled with “dc-1”. Then we will write “dc-1” in Table 1’s scheme. The later scheme checker will check this part to make sure the data constraint is satisfied. This solution also needs to consider whether the scheme has changed after writing.
- Build a checker and install it into all read & write operator to check the location information during a local transaction.
- Once the illegal data accessing is detected, we need to stop and rollback the transaction while throwing out an error.

There are two possible problems we need to notice according to the solution above.

- The validity of location information. Because the Placement Rule can be modified at any time. TiDB needs to be aware of these changes and update the location information stored as soon as possible.
- Also caused by the validity of location information. For example, the user changes a Placement Rule which will move some data from DC-1 to DC-2. But because of the delay, at a certain point, both TiDBs in DC-1 and DC-2 think they can start a local transaction for the same data. How should we prevent this kind of situation from happening?
[A possible solution](https://docs.google.com/document/d/1VYudmmxRqlO8W3C1mcVojrit5BMaMJKu3HBrqHS-emk) (Chinese)

## Testing Plan

[The testing plan](https://docs.google.com/document/d/173PNdh_1wgK7f_7f9HCjVUXWcTX3w8pG_MN4Zgvy2EI) composed before (Chinese).