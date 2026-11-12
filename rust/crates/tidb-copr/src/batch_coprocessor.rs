// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `pkg/store/copr/batch_coprocessor.go`'s `CopClient.sendBatch` and
//! `batchCopIterator`: a TiFlash request sent as one `BatchCoprocessor`
//! stream per store instead of one coprocessor request per region.
//!
//! The tasks are `buildBatchCopTasksCore`'s ([`crate::batch_cop_tasks`]); a
//! worker per task sends it, rebuilds it when its store cannot be reached,
//! and feeds every response to the iterator's channel.

use std::sync::Arc;
use std::time::Duration;

use tidb_proto::coprocessor;
use tidb_txnkv::driver::tikv_store::TikvStore;
use tidb_txnkv::retry::RegionBackoffBudget;
use tidb_txnkv::{
    CancelHandle, KvRequestMetadata, MppContext, MppStoreTask, QueryResponseError,
    QueryResultSubset, TransportRequest,
};
use tikv_client::tikv::{BatchCoprocessorStreamRequest, BatchCoprocessorStreamResponse};

use crate::mpp_client::{
    invalidate_mpp_retry_regions, mpp_setup, on_send_fail_for_batch_regions, send_mpp,
    under_rpc_interceptor, MppTarget, MPP_RECEIVE_TIMEOUT,
};

/// Go `batchCopIterator.respChan`'s capacity.
const RESP_CHAN_SIZE: usize = 2048;

/// Go `handleBatchCopResponse`: the retry regions it invalidates (`idx >= 10`
/// breaks after the eleventh).
const MAX_INVALIDATED_RETRY_REGIONS: usize = 11;

/// The request's ctx as the batch task builder and sender read it: the
/// request's cancellation is its `Done`.
struct RequestContext(Arc<CancelHandle>);

impl MppContext for RequestContext {
    fn check(&self) -> Result<(), tidb_terror::MysqlError> {
        if self.0.is_cancelled() {
            return Err(tidb_terror::MysqlError::from_terror(
                &tidb_txnkv::tikv_errors::ERR_QUERY_INTERRUPTED,
            ));
        }
        Ok(())
    }

    fn consume(&self, _bytes: i64) {}
}

/// A registered storage error as the response carries it.
fn sql_error(error: &tidb_terror::TerrorError) -> QueryResponseError {
    QueryResponseError::Sql {
        code: u16::try_from(error.code().value())
            .expect("registered storage error fits the MySQL protocol"),
        message: error.message().to_owned(),
    }
}

/// Go `batchCopIterator`'s fields its workers read.
struct BatchCopWorker<S: tikv_client::PdClient> {
    /// Go `store`.
    store: TikvStore<S>,
    /// Go `req`.
    req: KvRequestMetadata,
    /// Go `ctx`'s values.
    kv_context: tidb_txnkv::StatementKvContext,
    rpc_interceptor: tidb_txnkv::RpcInterceptorOption,
    /// Go `ctx`'s `Done`.
    cancellation: Arc<CancelHandle>,
    ctx: Arc<dyn MppContext>,
    /// Go `respChan`.
    resp_chan: tokio::sync::mpsc::Sender<Result<QueryResultSubset, QueryResponseError>>,
    /// Go `appendWarning`.
    append_warning: Option<tidb_txnkv::AppendWarning>,
}

use crate::batch_cop_tasks::Ranges;

/// Go `buildBatchCopTasksForNonPartitionedTable` (`partition_ids` empty) or
/// `buildBatchCopTasksForPartitionedTable` as the batch coprocessor calls
/// them: not MPP, no continuity balance, Go's `DispatchPolicyInvalid`.
async fn build_batch_cop_tasks<S: tikv_client::PdClient>(
    store: &TikvStore<S>,
    req: &KvRequestMetadata,
    ctx: &Arc<dyn MppContext>,
    kv_context: &tidb_txnkv::StatementKvContext,
    ranges_for_each_physical_table: &[Ranges],
    partition_ids: &[i64],
) -> Result<(Vec<MppStoreTask>, Vec<String>), String> {
    crate::batch_cop_tasks::build_batch_cop_tasks(
        store,
        ranges_for_each_physical_table,
        partition_ids,
        ctx,
        false,
        false,
        0,
        tidb_tiflashcompute::DispatchPolicy::Invalid,
        req.tiflash_replica_read,
        kv_context,
    )
    .await
    .map_err(|error| error.to_string())
}

/// Go `appendWarning(warn)` for each of a task build's warnings.
fn append_warnings(append_warning: Option<&tidb_txnkv::AppendWarning>, warnings: Vec<String>) {
    if let Some(append_warning) = append_warning {
        warnings.into_iter().for_each(|warning| (append_warning.0)(warning));
    }
}

/// Go `CopClient.sendBatch`.
///
/// # Errors
/// Go's `copErrorResponse`: an ordered request, or a task build that failed.
pub fn send_batch<S: tikv_client::PdClient + 'static>(
    store: &TikvStore<S>,
    request: &TransportRequest,
) -> Result<Box<dyn tidb_txnkv::Response + Send>, String> {
    let cancellation = Arc::clone(
        request
            .request_cancellation()
            .map_err(|error| format!("{error:?}"))?,
    );
    let req = request
        .metadata_for_send()
        .map_err(|error| format!("{error:?}"))?;
    if req.keep_order || req.desc {
        return Err("batch coprocessor cannot prove keep order or desc property".to_owned());
    }
    let kv_context = request.kv_context().with_txn_start_ts(req.start_ts);
    let ctx: Arc<dyn MppContext> = Arc::new(RequestContext(Arc::clone(&cancellation)));
    let to_pairs = |ranges: &[tidb_txnkv::KeyRange]| -> Ranges {
        ranges
            .iter()
            .map(|range| (range.start_key.clone(), range.end_key.clone()))
            .collect()
    };
    let (ranges, partition_ids): (Vec<Ranges>, Vec<i64>) = if req.partition_id_and_ranges.is_empty() {
        // TODO: merge the if branch.
        let first = req
            .key_ranges
            .as_ref()
            .map_or(&[][..], |ranges| ranges.first_partition_ranges());
        (vec![to_pairs(first)], Vec::new())
    } else {
        // For Partition Table Scan
        req.partition_id_and_ranges
            .iter()
            .map(|pi| (to_pairs(&pi.key_ranges), pi.id))
            .unzip()
    };
    let build = build_batch_cop_tasks(store, req, &ctx, &kv_context, &ranges, &partition_ids);
    let tasks = store.block_on(async {
        if req.max_execution_time_ms == 0 {
            return build.await;
        }
        // If MaxExecutionTime is set, we need to set the deadline for the whole batch coprocessor request context.
        tokio::time::timeout(Duration::from_millis(req.max_execution_time_ms), build)
            .await
            .unwrap_or_else(|_| Err("context deadline exceeded".to_owned()))
    })?;
    let append_warning = request.append_warning();
    let (tasks, warnings) = tasks;
    append_warnings(append_warning.as_ref(), warnings);
    let (resp_chan, receiver) = tokio::sync::mpsc::channel(RESP_CHAN_SIZE);
    let worker = Arc::new(BatchCopWorker {
        store: store.clone(),
        req: req.clone(),
        kv_context,
        rpc_interceptor: request.rpc_interceptor(),
        cancellation: Arc::clone(&cancellation),
        ctx,
        resp_chan,
        append_warning,
    });
    // Go `batchCopIterator.run`: a goroutine per task; the channel closes
    // when the last of them returns.
    let workers = tasks
        .into_iter()
        .map(|task| {
            let worker = Arc::clone(&worker);
            store.runtime().spawn(async move { worker.handle_task(task).await })
        })
        .collect();
    Ok(Box::new(BatchCopIterator {
        store: store.clone(),
        start_ts: req.start_ts,
        cancellation,
        resp_chan: receiver,
        workers,
        closed: false,
    }))
}

impl<S: tikv_client::PdClient> BatchCopWorker<S> {
    /// The statement's stop, else `error`.
    fn after_stop(&self, error: QueryResponseError) -> QueryResponseError {
        if self.cancellation.is_cancelled() {
            return QueryResponseError::Cancelled;
        }
        error
    }

    /// Go `batchCopIterator.handleTask`.
    async fn handle_task(&self, task: MppStoreTask) {
        // Go `backoff.NewBackofferWithVars(ctx, CopNextMaxBackoff, b.vars)`.
        let mut bo = RegionBackoffBudget::campaign_default();
        if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/store/copr/ReduceCopNextMaxBackoff").is_some_and(|val| val.bool()) {
            bo = RegionBackoffBudget::new(std::time::Duration::from_millis(2));
        }
        let mut tasks = vec![task];
        let mut idx = 0;
        while idx < tasks.len() {
            let ret = crate::cop_store::with_statement_kv_context(
                &self.kv_context,
                under_rpc_interceptor(
                    &self.rpc_interceptor,
                    self.handle_task_once(&mut bo, &tasks[idx]),
                ),
            )
            .await;
            match ret {
                Ok(ret) => tasks.extend(ret),
                Err(error) => {
                    let _ = self.resp_chan.send(Err(self.after_stop(error))).await;
                    break;
                }
            }
            idx += 1;
        }
    }

    /// Go `batchCopIterator.retryBatchCopTask`: Merge all ranges and split
    /// them into tasks again.
    async fn retry_batch_cop_task(
        &self,
        task: &MppStoreTask,
    ) -> Result<Vec<MppStoreTask>, QueryResponseError> {
        let sorted = |regions: &[coprocessor::RegionInfo]| -> Ranges {
            let mut ranges: Ranges = regions
                .iter()
                .flat_map(|region| &region.ranges)
                .map(|range| (range.start.clone().into(), range.end.clone().into()))
                .collect();
            // need to make sure the key ranges is sorted
            ranges.sort_by(|a, b| a.0.cmp(&b.0));
            ranges
        };
        let (ranges, partition_ids): (Vec<Ranges>, Vec<i64>) = if task.partition_table_regions.is_empty() {
            (vec![sorted(&task.regions)], Vec::new())
        } else {
            task.partition_table_regions
                .iter()
                .map(|trs| (sorted(&trs.regions), trs.physical_table_id))
                .unzip()
        };
        build_batch_cop_tasks(
            &self.store,
            &self.req,
            &self.ctx,
            &self.kv_context,
            &ranges,
            &partition_ids,
        )
        .await
        .map(|(tasks, warnings)| {
            append_warnings(self.append_warning.as_ref(), warnings);
            tasks
        })
        .map_err(QueryResponseError::Source)
    }

    /// Go `batchCopIterator.handleTaskOnce`: the tasks that replace `task`
    /// when its store could not be reached.
    async fn handle_task_once(
        &self,
        bo: &mut RegionBackoffBudget,
        task: &MppStoreTask,
    ) -> Result<Vec<MppStoreTask>, QueryResponseError> {
        let first_key = task
            .regions
            .first()
            .and_then(|region| region.ranges.first())
            .map_or(&[][..], |range| range.start.as_slice());
        let rg_name = if tidb_vardef::ENABLE_RESOURCE_CONTROL.load(std::sync::atomic::Ordering::Relaxed) {
            self.req.resource_group_name.clone()
        } else {
            String::new()
        };
        let cop_req = coprocessor::BatchRequest {
            tp: self.req.request_type.raw(),
            start_ts: self.req.start_ts,
            data: self.req.data.clone().unwrap_or_default(),
            schema_ver: self.req.schema_version,
            regions: task.regions.clone(),
            table_regions: task.partition_table_regions.clone(),
            connection_id: self.req.connection_id,
            connection_alias: self.req.connection_alias.clone(),
            context: Some(tidb_proto::KvrpcContext {
                isolation_level: self.req.isolation_level.raw() as i32,
                priority: self.req.priority.raw() as i32,
                not_fill_cache: self.req.not_fill_cache,
                record_time_stat: true,
                record_scan_stat: true,
                task_id: self.req.task_id,
                resource_control_context: Some(tidb_proto::KvrpcResourceControlContext {
                    resource_group_name: rg_name,
                    ..Default::default()
                }),
                resource_group_tag: self
                    .req
                    .resource_group_tagger
                    .as_ref()
                    .map(|tagger| tagger.encode_tag_with_key(first_key))
                    .unwrap_or_default(),
                // `tikvrpc.NewRequest` fills the process default.
                request_origin: tidb_proto::KvrpcRequestOrigin::TiDb as i32,
                ..Default::default()
            }),
            ..Default::default()
        };
        let target = MppTarget::new(&self.store, &task.address, &self.rpc_interceptor);
        // Go `sender.SendReqToAddr(bo, task.ctx, task.regionInfos, req,
        // readTimeoutUltraLong)`.
        let sent = mpp_setup(
            Some(self.ctx.as_ref()),
            Some(MPP_RECEIVE_TIMEOUT),
            "batch coprocessor",
            send_mpp::<_, _, BatchCoprocessorStreamResponse>(
                &target.store,
                &target.address,
                target.store_type,
                BatchCoprocessorStreamRequest::new(cop_req),
                MPP_RECEIVE_TIMEOUT,
            ),
        )
        .await;
        match sent {
            Ok(response) => self.handle_streamed_batch_cop_response(bo, response, task).await.map(|()| Vec::new()),
            Err(error) => {
                // E.g. RPC error like "connection refused"? retry the task
                if on_send_fail_for_batch_regions(self.ctx.as_ref(), bo, &target, task, &error).await {
                    return self.retry_batch_cop_task(task).await;
                }
                // Go `derr.ToTiDBErr(err)`: a canceled send is its own
                // error; an exhausted TiFlash RPC backoff is the server
                // timeout.
                if error.canceled() {
                    return Err(QueryResponseError::Source(error.cause()));
                }
                Err(sql_error(&tidb_txnkv::tikv_errors::ERR_TI_FLASH_SERVER_TIMEOUT))
            }
        }
    }

    /// Go `batchCopIterator.handleStreamedBatchCopResponse`.
    async fn handle_streamed_batch_cop_response(
        &self,
        bo: &mut RegionBackoffBudget,
        mut response: BatchCoprocessorStreamResponse,
        task: &MppStoreTask,
    ) -> Result<(), QueryResponseError> {
        // streaming request returns io.EOF, so the first Response is nil.
        let Some(mut resp) = response.first.take() else {
            return Ok(());
        };
        loop {
            self.handle_batch_cop_response(resp, task).await?;
            let received = mpp_setup(
                Some(self.ctx.as_ref()),
                None,
                "batch coprocessor recv",
                response.message(),
            )
            .await;
            match received {
                Ok(Some(next)) => resp = next,
                Ok(None) => return Ok(()),
                Err(error) => {
                    // Go `bo.Backoff(tikv.BoTiKVRPC(), errors.Errorf("recv
                    // stream response error: %v, task store addr: %s", ...))`.
                    let Ok(delay) = bo.next_delay(tikv_client::retry::BO_TIKV_RPC) else {
                        return Err(QueryResponseError::Source(error.to_string()));
                    };
                    let waited = crate::mpp_client::mpp_retry_wait(self.ctx.as_ref(), delay).await;
                    bo.finish_wait(waited.is_ok());
                    if waited.is_err() {
                        return Err(QueryResponseError::Source(error.to_string()));
                    }
                    // No coprocessor.Response for network error, rebuild task based on the last success one.
                    return Err(sql_error(&tidb_txnkv::tikv_errors::ERR_TI_FLASH_SERVER_TIMEOUT));
                }
            }
        }
    }

    /// Go `batchCopIterator.handleBatchCopResponse`.
    async fn handle_batch_cop_response(
        &self,
        response: coprocessor::BatchResponse,
        task: &MppStoreTask,
    ) -> Result<(), QueryResponseError> {
        if !response.other_error.is_empty() {
            return Err(QueryResponseError::Source(format!(
                "other error: {}",
                response.other_error
            )));
        }
        if !response.retry_regions.is_empty() {
            // multiple regions are stale and need to be refreshed
            let stale = response.retry_regions.len().min(MAX_INVALIDATED_RETRY_REGIONS);
            invalidate_mpp_retry_regions(&self.store, &response.retry_regions[..stale]).await;
            return Ok(());
        }
        // Go `handleCollectExecutionInfo`: the store that answered.
        let resp = QueryResultSubset {
            data: response.data,
            runtime: Some(tidb_txnkv::ResponseRuntimeStats {
                callee_address: task.address.clone(),
                ..Default::default()
            }),
        };
        // Go `sendToRespCh`: a closed iterator takes no more.
        self.resp_chan
            .send(Ok(resp))
            .await
            .map_err(|_| QueryResponseError::Cancelled)
    }
}

/// Go `batchCopIterator`.
pub struct BatchCopIterator<S: tikv_client::PdClient> {
    /// Go `store`.
    store: TikvStore<S>,
    /// Go `req.StartTs`.
    start_ts: u64,
    /// Go `ctx`'s `Done`.
    cancellation: Arc<CancelHandle>,
    /// Go `respChan`: Batch results are stored in respChan.
    resp_chan: tokio::sync::mpsc::Receiver<Result<QueryResultSubset, QueryResponseError>>,
    /// Go `wg`'s goroutines, which `rpcCancel` stops.
    workers: Vec<tokio::task::JoinHandle<()>>,
    /// Go `closed`.
    closed: bool,
}

impl<S: tikv_client::PdClient> BatchCopIterator<S> {
    /// Go `batchCopIterator.recvFromRespCh`: the next response, `None` once
    /// every worker has returned.
    fn recv_from_resp_ch(
        &mut self,
    ) -> Result<Option<Result<QueryResultSubset, QueryResponseError>>, QueryResponseError> {
        let cancellation = &self.cancellation;
        let resp_chan = &mut self.resp_chan;
        self.store.block_on(async move {
            loop {
                if cancellation.is_cancelled() {
                    return Err(QueryResponseError::Cancelled);
                }
                tokio::select! {
                    biased;
                    resp = resp_chan.recv() => return Ok(resp),
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
            }
        })
    }

    /// Go `kvStore.CheckVisibility` -> `ToTiDBErr`.
    fn check_visibility(&self) -> Result<(), QueryResponseError> {
        self.store.check_visibility(self.start_ts).map_err(|error| {
            match tidb_store_driver_error::driver_error::to_tidb_driver_error(
                &error.into_storage_driver_error(),
            ) {
                tidb_store_driver_error::driver_error::ConvertedDriverError::Terror(converted) => {
                    sql_error(&converted)
                }
                other => QueryResponseError::Source(other.to_string()),
            }
        })
    }
}

impl<S: tikv_client::PdClient> tidb_txnkv::Response for BatchCopIterator<S> {
    /// Go `batchCopIterator.Next`.
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        if self.closed {
            return Ok(None);
        }
        let received = match self.recv_from_resp_ch() {
            Ok(received) => received,
            Err(error) => {
                self.close()?;
                return Err(error);
            }
        };
        let Some(resp) = received else {
            return Ok(None);
        };
        let resp = resp?;
        self.check_visibility()?;
        // NOTE: The batch coprocessor do not need the order of the result, so the
        // responses are returned as they arrive.
        Ok(Some(resp))
    }

    /// Go `batchCopIterator.Close`: the workers' RPCs are canceled and the
    /// workers waited for.
    fn close(&mut self) -> Result<(), QueryResponseError> {
        if std::mem::replace(&mut self.closed, true) {
            return Ok(());
        }
        self.resp_chan.close();
        let workers = std::mem::take(&mut self.workers);
        for worker in &workers {
            worker.abort();
        }
        self.store.block_on(async move {
            for worker in workers {
                let _ = worker.await;
            }
        });
        Ok(())
    }
}

impl<S: tikv_client::PdClient> Drop for BatchCopIterator<S> {
    fn drop(&mut self) {
        let _ = tidb_txnkv::Response::close(self);
    }
}
