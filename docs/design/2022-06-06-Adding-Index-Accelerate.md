# TiDB Adding Index Acceleration 
- Author(s):     [Bear.C](https://github.com/Benjamin2037) (Benjamin)
- Last updated:  2022-06-06
## Abstract
This article will describe how TiDB do the adding index Acceleration solution.

## Background
In TiDB, all the DDL statements are executed with online mode that will let DDL tasks cause
minimal impaction on user DML transaction. There are still some DDL statements that need
backfill data for new created object, for example: create index. The more data that table contained the more time needed to handle backfill process. Speed up backfill process to accelerate DDL statements execution is also important for TiDB user.

The original solution for adding index will start several workers to do backfill task.
They all have a write reorganization state to back fill data into the rows existed.
Backfilling is time consuming, to accelerate this process, TiDB has built some sub
workers to do this in the DDL owner node.

                            DDL owner thread
                                  ^
                                  | (reorgCtx.doneCh)
                                  |
                            worker master
                                  ^ (waitTaskResults)
                                  |
                                  |
                                  v (sendRangeTask)
    +--------------------+---------+---------+------------------+-----------------+
   |                    |                   |                  |                 |
 backfillworker1     backfillworker2     backfillworker3     backfillworker4     ...

The worker master is responsible for scaling the backfilling workers according to the
system variable "tidb_ddl_reorg_worker_cnt". Essentially, reorg job is mainly based
on the [start, end] range of the table to backfill data. We did not do it all at once,
there were several DDL rounds.

 	[start1---end1 start2---end2 start3---end3 start4---end4 ...         ...         ] 
    |       |     |       |     |       |     |       |
    +-------+     +-------+     +-------+     +-------+      ...         ...
        |             |             |             |
    bfworker1    bfworker2     bfworker3     bfworker4       ...         ...
        |             |             |             |           |            |
        +---------------- (round1)----------------+           +--(round2)--+

The main range [start, end] will be split into small ranges.
Each small range corresponds to a region and it will be delivered to a backfillworker.
Each worker can only be assigned with one range at one round, those remaining ranges
will be cached until all the backfill workers have had their previous range jobs done.

                [ region start --------------------- region end ]
                                        |
                                        v
                [ batch ] [ batch ] [ batch ] [ batch ] ...
                    |         |         |         |
                    v         v         v         v
                (a kv txn)   ->        ->        ->

For a single range, backfill worker doesn't backfill all the data in one kv transaction.
Instead, it is divided into batches, each time a kv transaction completes the backfilling
of a partial batch. the system variable "tidb_ddl_reorg_batch_size" will be used to set records processed in one batch.
 
## Rationale
The enhancement way is use lightning that act as an local data builder. The data fetching from table is same as original solution does. After that the lightning will take over the remain backfill process. The flow is shown in below.

                             DDL owner thread
                                   ^
                                   | (reorgCtx.doneCh)
                                   |
                             worker master
                                   ^ (waitTaskResults)
                                   |
                                   |
                                   v (sendRangeTask)
    +--------------------+---------+---------+------------------+--------------+
    |                    |                   |                  |              |
backfillworker1     backfillworker2     backfillworker3     backfillworker4    ...

---------------------------------------------------------------------------------------

 	[start1---end1 start2---end2 start3---end3 start4---end4  ...         ...         ]
    |       |     |       |     |       |     |       |
    +-------+     +-------+     +-------+     +-------+       ...         ...
        |             |             |             |
    bfworker1    bfworker2     bfworker3     bfworker4       ...         ...
        |             |             |             |            |
    localwriter1 localwriter2  localwriter3  localwriter4      |
        +---------------- (round1)----------------+            +--(round2)--+
                              |                                       |
                      +------------------+                     +--------------
                      |                  |                     |
                 localfcworker1     localfcworker2        localfcwroker1     ...

	|-----------------------------------------------------------------------------------|
                                        |
                                 +------------------+
                                 |        |         |
                             importer1 importer2  importer3     

The new solution will set up a localwriter for each bfwoker, the building index key/value will be firstly writen into localwriter buffer and then be flushed to local storage at the end of every round. The localfcworker will do flush data and compaction tasks for building index data in TiDB Owner local. After all records are processed for index and local index has been done compaction(sorted that is why this solution need local storage for TiDB). The importer splits the local index sst file into small blocks and ingests them into TiKV. 

### Advantage 
1. Performance is good. Because the backfill processing is almostly done in TiDB local. The adding index performance is improved in an outstanding extend.
2. Also since the backfill processing is not directly done on TiKV, theoretically it will impact less on user DML transaction. (Note: According limitations 3, The local CPU's consumption will contribute a negative part on this point. this will be solved in the future).
      
### Limitations
As the first version, we have some limitations. in the feture, we will keep improve the solution.
1. This solution will be an expirement feature released in TiDB 6.2.
2. Checkpoint currently not support, that means we may restart the backfill process in TiDB DDL Owner restart scenario.
3. The lightning need cpu and memory to make backfill faster, so currently it may consume more CPU. We have limited memory to be over used and will continue implement a flexible resource control framework for DDL.

### Enable/Disable
1. Use tidb_fast_ddl to enable and disable new solution.  
```TiDB> set global tidb_fast_ddl = on/off;```

2. Use one config parameter lightning-sort-path to specify the sort path for lightning.  
```lightning-sort-path = “sortPath”```

## Compatibility
No compatibility issue
