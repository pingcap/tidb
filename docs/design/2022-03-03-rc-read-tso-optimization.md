# Read-Committed Read With Timestamp Check

- Author(s): cfzjywxk
- Last updated: March. 3, 2022
- Discussion at:

## Motivation

For the `read-committed` isolation level, each read request in a single transaction will need to fetch a new `ts` to read the latest committed data.
If the workload is a read-heavy one whose read QPS is significantly higher than writes or there're few read-write conflicts, fetching ts each time will increase the query lantecy.

The new ts itself is used to ensure the most recent data will be returned, if the data version does not change frequently then it's unnecessary to fetch a new timestamp every time.
The ts fetching could be processed in an optimistic way that ts fetching happens only when a more recent data version is encoutered, this saves the cost of many unncessary ts fetching.

## Detailed Design

Introduce another system variable `tidb_rc_read_check_ts` to enable the "lazy ts fetch" read mode. It will take effect for all the in-transaction select statements when the transaction mode is `pessimistic` and the isolation level is `read-committed`.

The execution flow is like this:

1. If the query is executed first time, do not fetch a new `for_update_ts` and just use the last valid one(`start_ts` for the first time).
2. Do build the plan and executor like before.
3. If the execution is successful, a `resultSet` will be returned.
4. The connection layer will drive the response procesure and call `Next` function using the returned `resultSet`.
5. The read executor will try to fetch data to fill in a data `chunk`. The bottommost executor could be a `pointGet`, `batchPointGet` or a coprocessor task. What's different is a `RcReadCheckTS` flag will be set on all these read requests.
6. The read executor in the storage layer will check the read results, if a more recent version does exist then a `WriteConflict` error is reported.
7. For data write record, do check if it's newer than the current `read_ts`.
8. For lock record, return `ErrKeyIsLocked` even though the `lock.ts` is greater than the `read_ts` as the `read_ts` could be a stale one.
9. If no error is returned then the query is finished. Otherwise if there's no `chunk` responsed to the client yet, retry the whole query fetching a new global ts to do the read like normal.


## Compatibility

The default behaviours of the `read-committed` isolation level will not change. One thing different is that if the user client uses `COM_STMT_FETCH` like utility to read data from `TiDB`,
there could be problem if the returned first chunk result is already used by the client but an error is reported processing next result chunk.
