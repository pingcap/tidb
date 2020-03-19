
# Proposal: A new command to restore dropped table 

- Author(s):    [winkyao](https://github.com/winkyao)
- Last updated:  2018-08-10

## Abstract

This proposal proposes to support the `ADMIN RESTORE TABLE table_id` command, to restore the table that is dropped by a faulty operation.

## Background

At present, if we drop the table in production environment, we will realize whether the operation is faulty immediately. Before we support the proposed command, we can only [read data from history versions](https://pingcap.com/docs/op-guide/history-read/) to relieve the disaster. But it needs to read all the data in the storage and it takes too much time to just restore the dropped the table.

## Proposal

We can add a new command `ADMIN RESTORE TABLE table_id` to just make the dropped table public again. If the data is not deleted by GC worker, this command can work. So it is better to enlarge the GC life time with `update mysql.tidb set variable_value='30h' where variable_name='tikv_gc_life_time';`, before we execute the statement. The table and the original table data can be restored in a few seconds and it is a lot faster than before. It also can reduce the complexity of the operations and dissolve the artificial operation error.

## Rationale

Let's take a look at the workflow of the `DROP TABLE` statement. The `DROP TABLE` statement first removes the dropping table meta data from the coresponding database meta data. After the schemas are synced by all the TiDB instances, in `worker.deleteRange`, TiDB will insert a deleted range of the first row key to the end row key of the dropping table into the table `mysql.gc_delete_range`. At most `max(gcDefaultRunInterval, gcLifeTimeKey)` time later, the GC worker will delete the table data finally. 

The meta data of the table is not really deleted. The meta key format is `Table:table_id`. As long as we can find out the ID of the dropped table, we can recover the table information. The `admin show ddl jobs` statement can retrieve the table ID:

```
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------+
| JOBS                                                                                                                                                                                                                          | STATE     |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------+
| ID:44, Type:drop table, State:synced, SchemaState:none, SchemaID:1, TableID:39, RowCount:0, ArgLen:0, start time: 2018-08-11 11:23:53.308 +0800 CST, Err:<nil>, ErrCount:0, SnapshotVersion:0                                 | synced    |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------+
```

As you can see, if we can restore the table before the GC worker deletes the table data, we can restore the table completely. If the table data is deleted, we can only restore an empty table.

Before we run `ADMIN RESTORE TABLE table_id`, you must:
* Ensure that no GC task is running and then you can figure this by using TiDB logs and metrics.
* Increase `tikv_gc_life_time` to a sufficient value.

## Compatibility

It's a new command and will not lead to compatibility issues.

## Implementation

1. `ADMIN RESTORE TABLE table_id` will enqueue a new DDL job to TiDB general DDL queue.
2. Before we start to do this job, we need to check if the table name already exists (created a new table with the same table name after you drop it). If it exists, return the `ErrTableExists` error.
3. Find out whether the delete-range of the dropped table is still in `mysql.gc_delete_range`. If not, it means that the GC worker has cleaned up the data. In this situation, we cannot restore the table successfully but return an error to the client. If it is still there, we remove the record in the `mysql.gc_delete_range` table. If we successfully remove the record, continue to Step 4; otherwise, we return an error to the client to indicate the command cannot be executed safely.
4. Use the previous table meta information of the table_id to insert the meta data into the schema meta data, like what `Meta.CreateTable` does. And set the table information state to `model.StatePublic`, then the restoration will be finished after the schema is synced by all the TiDB instances.
5. If the command is canceled or rollbacked, and the delete-range record is already removed, we need to insert it into `mysql.gc_delete_range` again, like what `Drop Table` does in `worker.finishDDLJob`.
