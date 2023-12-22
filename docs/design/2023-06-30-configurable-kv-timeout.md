# Proposal: Configurable KV Timeout

* Authors: [cfzjywxk](https://github.com/cfzjywxk)
* Tracking issue: TODO

## Motivation & Background

TiKV requests are typically processed quickly, taking only a few milliseconds. However, if 
there is disk IO or network latency jitter on a specified TiKV node, the duration of these requests may spike 
to several seconds or more. This usually happens when there is an EBS issue like the EBS is repairing its 
replica.

Currently, the timeout limit for the TiKV requests is fixed and cannot be adjusted.  For example:
- `ReadTimeoutShort`: [30 seconds](https://github.com/tikv/client-go/blob/ce9203ef66e99dcc8b14f68777c520830ba99099/internal/client/client.go#L82), which is used for the Get RPC request.
- `ReadTimeoutMedium`: [60 seconds](https://github.com/tikv/client-go/blob/ce9203ef66e99dcc8b14f68777c520830ba99099/internal/client/client.go#L83), which is used for the BatchGet, Cop RPC request.

The kv-client or TiDB may wait for the responses from TiKV nodes until timeout happens, 
it could not retry or redirect the requests to other nodes quickly as the timeout configuration is usually 
tens of seconds. As a result, the application may experience a query latency spike when TiKV node jitter is 
encountered.

For latency sensitive applications, providing predictable sub-second read latency by set fast retrying
is valuable. queries that are usually very fast (such as point-select query), setting the value
of `tikv_client_read_timeout` to short value like  `500ms`, the TiDB cluster would be more tolerable
for network latency or io latency jitter for a single storage node, because the retry are more quickly.

This would be helpful if the requests could be processed by more than 1 candidate targets, for example
follower or stale read requests that could be handled by multiple available peers.

## Improvement Proposal

A possible improvement suggested by [#44771](https://github.com/pingcap/tidb/issues/44771) is to make the
timeout values of specific KV requests configurable. For example:
- Adding a session variable `tikv_client_read_timeout`, which is used to control the timeout for a single 
TiKV read RPC request. When the user sets the value of this variable, all read RPC request timeouts will use this value. 
The default value of this variable is 0, and the timeout of TiKV read RPC requests is still the original 
value of `ReadTimeoutShort` and `ReadTimeoutMedium`.
- Support statement level hint like `SELECT /*+ set_var(tikv_client_read_timeout=500) */ * FROM t where id = ?;` to
set the timeout value of the KV requests of this single query to the certain value.

### Example Usage

Consider the stale read usage case:

```SQL
set @@tidb_read_staleness=-5;
# The unit is miliseconds. The session variable usage.
set @@tidb_tikv_tidb_timeout=500;
select * from t where id = 1;
# The unit is miliseconds. The query hint usage.
select /*+ set_var(tikv_client_read_timeout=500) */ * FROM t where id = 1;
```

### Problems

- Setting the variable `tikv_client_read_timeout ` may not be easy if it affects the timeout for all 
TiKV read requests, such as Get, BatchGet, Cop in this session.A timeout of 1 second may be sufficient for GET requests, 
but may be small for COP requests. Some large COP requests may keep timing out and could not be processed properly.
- If the value of the variable or query hint `tikv_client_read_timeout` is set too small, more retries will occur, 
increasing the load pressure on the TiDB cluster. In the worst case the query would not return until the 
max backoff timeout is reached if the `tikv_client_read_timeout` is set to a value which none of the replicas
could finish processing within that time.


## Detailed Design

The query hint usage would be more flexible and safer as the impact is limited to a single query.

### Support Timeout Configuration For Get And Batch-Get

- Add related filed in the `KVSnapshot` struct like

```go
type KVSnapshot struct {
	...
	KVReadTimeout: Duration
}
```

This change is to be done in the `client-go` repository.

- Add set interface `SetKVReadTimeout` and get interface `GetKVReadTimeout` for `KVSnapshot`
```go
func (s *KVSnapshot) SetKVReadTimeout(KVTimeout Duration) {
	s.KVReadTimeout = KVTimeout
}
```

This change is to be done both in the `client-go` repository and `tidb` repository.

- Call the set interfaces to pass the timeout value from `StmtContext` to the `KVSnapshot` structures, when
the `PointGet` and `BatchPointGet` executors are built.
- Pass the timeout value into the `Context.max_execution_duration_ms` field of the RPC `Context` like
```protobuf
message Context {
  ...
  uint64 max_execution_duration_ms = 14;
  ...
}
```
Note by now this field is used only by write requests like `Prewrite` and `Commit`.

- Support timeout check during the request handling in TiKV. When there's new point get and batch point get 
requests are received, the `kv_read_timeout` value should be read from the `GetReuqest` and `BatchGetRequest`
requests and passed to the `pointGetter`. By now there's no canceling mechanism when the task is scheduled to the
read pool in TiKV, a simpler way is to check the deadline duration before next processing and try to return
directly if the deadline is exceeded, but once the read task is spawned to the read pool it could not be 
canceled anymore.
```rust
pub fn get(
    &self,
    mut ctx: Context,
    key: Key,
    start_ts: TimeStamp,
) -> impl Future<Output = Result<(Option<Value>, KvGetStatistics)>> {
    ...
     let res = self.read_pool.spawn_handle(
            async move {
                // TODO: Add timeout check here.
                let snapshot =
                    Self::with_tls_engine(|engine| Self::snapshot(engine, snap_ctx)).await?;

                {
                    // TODO: Add timeout check here.
                    ...
                    let result = Self::with_perf_context(CMD, || {
                        let _guard = sample.observe_cpu();
                        let snap_store = SnapshotStore::new(
                            snapshot,
                            start_ts,
                            ctx.get_isolation_level(),
                            !ctx.get_not_fill_cache(),
                            bypass_locks,
                            access_locks,
                            false,
                        );
                        // TODO: Add timeout check here.
                        snap_store
                        .get(&key, &mut statistics)
                    });
                 ...   
            ...        
        );
}
```
These changes need to be done in the `tikv` repository.

### Support Timeout Configuration For Coprocessor Tasks

- Add related field in the `copTask` struct like
```go
type copTask struct {
	...
	KVReadTimeout: Duration
}
```
- Pass the timeout value into the `Context.max_execution_duration_ms` field
```protobuf
message Context {
  ...
  uint64 max_execution_duration_ms = 14;
  ...
}
```

- Use the `kv_read_timeout` value passed in to calculate the `deadline` result in `parse_and_handle_unary_request`,
```rust
fn parse_request_and_check_memory_locks(
    &self,
    mut req: coppb::Request,
    peer: Option<String>,
    is_streaming: bool,
) -> Result<(RequestHandlerBuilder<E::Snap>, ReqContext)> {
    ...
     req_ctx = ReqContext::new(
        tag,
        context,
        ranges,
        self.max_handle_duration, // Here use the specified timeout value.
        peer,
        Some(is_desc_scan),
        start_ts.into(),
        cache_match_version,
        self.perf_level,
    );
}
```
This change needs to be done in the `tikv` repository.

### Timeout Retry

- When timeout happens, retry the next available peer for the read requests like stale read and `non-leader-only`
snapshot read requests. The strategy is:
  1. Try the next peer if the first request times out immediately.
  2. If all the responses from all the peers are `timeout` errors, return `timeout` error to the upper layer,
  3. Backoff with the `timeout` error and retry the requests with **the original default** `ReadShortTimeout`
  and `ReadMediumTimeout` values.
  4. Continue to process like before.
- The timeout retry details should be recorded and handled properly in the request tracker, so are the metrics.

### Timeout Task Canceling

- If the timeout value is configured to a small value like hundreds of milliseconds, there could be more
timeout tasks. When task is spawned to the read pool in TiKV, it could not be canceled immediately for 
some reasons:
  - When the read task is polled and executed, there's no timeout checking mechanism in the task scheduler
  by now.
  - The read operations are synchronous, so if the read task is blocked by slow io the task could not be
  scheduled or canceled.
- Though some simple timeout checks could be added in certain places like "after getting snapshot". A 
complete timeout check or scheduling design is still needed in the future. These preconditions are complex and 
require a lot of work including making the read processing asynchronous in TiKV.


### Compatibility

The requests with configurable timeout values would take effect on newer versions. These fields are expected not to take effect when down-grading the
cluster theoretically.

## Alternative Solutions

### Derived Timeouts

There’s already a query level timeout configuration `max_execution_time` which could be configured using 
variables and query hints. 

If the timeout of RPC or TiKV requests could derive the timeout values from the `max_execution_time` in an 
intelligent way, it’s not needed to expose another variable or usage like `tikv_client_read_timeout`.

For example, consider the strategy:
- The  `max_execution_time` is configured to `1s` on the query level
- The first RPC timeout is configured to some proportional value of the `max_execution_time` like `500ms`
- When first RPC times out the retry RPC is configured to left value of the `max_execution_time` like `400ms`

From the application’s perspective, TiDB is trying to finish the requests ASAP considering the 
input `max_execution_time` value, and when TiDB could not finish in time the timeout error is returned 
to the application and the query fails fast.

Though it's easier to use and would not introduce more configurations or variables, it's difficult to make
the timeout calculation intelligent or automatic, considering things like different request types, 
node pressures and latencies, etc. A lot of experiments are needed to figure out the appropriate strategy.
