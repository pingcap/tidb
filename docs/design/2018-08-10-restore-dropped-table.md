
# Proposal: 

- Author(s):    [winkyao](https://github.com/winkyao)
- Last updated:  2018-08-10

## Abstract

The proposal proposes to support `ADMIN RESTORE TABLE table_id` command, to restore the table that is dropped by faulty operation.

## Background

At present, if we drop the table in production environment, and realize the operations is faulty immediately. Before we support the proposed command, we can only [Reading Data From History Versions](https://pingcap.com/docs/op-guide/history-read/) to rescue the disaster. But it needs to read all the data in the storage, it spend too much time for our purpose that just restore the dropped table.

## Proposal

We can add a new command `ADMIN RESTORE TABLE table_id` to just make the dropped table be public again. If the data is not deleted by GC worker, this command can work. So it is better to enlarge the gc life time with `update mysql.tidb set variable_value='30h' where variable_name='tikv_gc_life_time';`, before we executing the statement. And the table and the original table data can be restore in a few seconds, it is a lot faster than before. It also can reduce the complexity of the operations, dissolves the artificial operation error.

## Rationale

Let's take a look what `DROP TABLE` statement does. `DROP TABLE` statement first remove the dropping table meta data from the coresponding database meta data. After the schemas are synced by all the TiDB instances, in `worker.deleteRange`, TiDB will insert a delete range that construct from the first row key to end row key of the dropping table into the table `mysql.gc_delete_range`. At most `max(gcDefaultRunInterval, gcLifeTimeKey)` time later, the GC worker will delete the table data finally. 

The meta data of the table is not really deleted, the meta key format is `Table:table_id`, as long as we can find out the id of the dropped table, we can recover the table information. The `admin show ddl jobs` statement can retrive the table id:

```
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------+
| JOBS                                                                                                                                                                                                                          | STATE     |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------+
| ID:44, Type:drop table, State:synced, SchemaState:none, SchemaID:1, TableID:39, RowCount:0, ArgLen:0, start time: 2018-08-11 11:23:53.308 +0800 CST, Err:<nil>, ErrCount:0, SnapshotVersion:0                                 | synced    |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------+
```

As you can see, if we can restore the table before the GC worker delete the table data, we can restore the table completely, if the table data is deleted, we can only restore an empty table.

## Compatibility

It's a new command, will not lead to compatibility issues.

## Implementation

1. `ADMIN RESTORE TABLE table_id` will enqueue a new DDL job to TiDB general DDL queue.
2. Before we start to do this job, we need to check if the table name is already exists(created a new table with the same table name after you drop it), if it is exists, return `ErrTableExists` error.
3. Secondly, find out whether the delete-range of the dropped table is still in the `mysql.gc_delete_range`, if not, means that the GC worker is cleanup the data, we can not restore the table successfully, return a error to the client. If it is still there, we remove the record in the `mysql.gc_delete_range` table, if we successfully remove, continue to step 4, otherwise we return a error to the client to indicate the command can not execute safely.
4. Remove the delete-range record in the `mysql.gc_delete_range` table, to prevent GC worker deleting the real data.
5. Use the previous table meta infomation of the table_id to insert the meta data into the schema meta data, like what `Meta.CreateTable` does. And make the table infomation state to be `model.StatePublic`, then the restore is finished after the schema is synced by all the TiDB instances.
6. If the command is canceled or rollbacked, and the delete-range record is already removed, we need to insert it into the `mysql.gc_delete_range` again, like what `Drop Table` does in `worker.finishDDLJob`.
