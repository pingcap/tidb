# Read-Consistency Read With Tso Check

- Author(s): cfzjywxk
- Last updated: March. 3, 2022
- Discussion at:

## Motivation

For the read-consistency isolation level read requests in a single transaction, each will need to fetch a new `tso` to read the latest committed data.
If the workload is a read-heavy one or the read `qps` is large, fetching tso each time will increase the query lantecy.

The new tso itself is used to ensure the most recent data will be returned, if the data version does not change frequently then it's unnecessary to fetch tso every time.
That is the `rc-read` could be processed in an optimistic way, the tso could be updated only when a new version data is met， then the tso cost will be saved a lot for this case.

## Detailed Design

Introduce another system variable `tidb_rc_read_check_ts` to enable the "lazy tso update" read mode. It will take effect for all the in-transaction select stataments and the
transaction isolation level is `read-consistency`.

The execution flow is like this:

1. If the query is executed first time, do not fetch a newer `for_update_ts` and just use the last valid one(`start_ts` for the first time).
2. Do build the plan and executor like before.
3. If the execution is successful, a `resultSet` will be returned.
4. The connection layer will drive the response and call `Next` function using the `resultSet`.
5. The read executor will try to fetch data to fill in a `chunk`. The bottommost executor could be a `pointGet`, `batchPointGet` or coprocessor task. What's different is a `RcReadCheckTS` flag will be set on the read requests.
6. The read executor in storage layer will check the read results, if more recent version does exist then a `WriteConflict` error will be returned.
7. For data write record, do check if there's new version compared with current `read_ts`.
8. For lock record, return `ErrKeyIsLocked` though the `lock.ts` is greater than the `read_ts` as the `read_ts` could be a stale one.
9. If no error is returned the query is finished. Otherwise if there's no `chunk` responsed to client yet, do retry the whole query and this time a new `for_update_ts` will be fetched.


## Compatibility

The default behaviours of the `read-consistency` isolation level will not change. One thing different is that if the user client uses `COM_STMT_FETCH` like utility to read data from `TiDB`,
there could be problem if the returned first chunk result is already used by the client but an error is reported processing next result chunk.
