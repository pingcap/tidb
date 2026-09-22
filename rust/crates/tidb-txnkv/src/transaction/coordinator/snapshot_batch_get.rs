// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Request-owned BatchGet completion and retry workers. Go boundaries:
//! snapshot.go batchGetKeysByRegions and snapshot_async.go asyncBatchGetByRegions.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use futures::stream::{FuturesUnordered, StreamExt};
use tidb_proto::{KvrpcBatchGetRequest, KvrpcBatchGetResponse, KvrpcContext};

use crate::lock::{
    decode_blocking_lock_observation, LockRecoveryClient, SnapshotLockSet, TimestampSource,
};
use crate::region::{RegionBackoffBudget, RegionRecoveryLoader};
use crate::rpc::{wait_with_call, UnaryCallContext};
use crate::SharedReadRuntime;

use super::super::command_client::{
    PublishedCommand, TransactionBatchGetRequest, TransactionCommandClient,
};
use super::super::region_batches::{group_snapshot_keys, RegionKeyBatch};
use super::snapshot_read::{
    resolve_snapshot_locks, snapshot_recovery_error, wait_snapshot_lock_ttl,
};
use super::{recover_region_error_with, OptimisticCoordinatorError};

type ReadResult = Result<(), OptimisticCoordinatorError>;

#[allow(clippy::too_many_arguments)]
pub(super) fn snapshot_batch_get_with<C, L, T>(
    runtime: &SharedReadRuntime<C, L>,
    timestamps: &T,
    read_ts: u64,
    resource_group_name: Option<&str>,
    stats: Option<&tikv_client::SnapshotRuntimeStats>,
    resolved_locks: &mut SnapshotLockSet,
    rpc_count: &mut u64,
    keys: &[Vec<u8>],
    call: &UnaryCallContext,
) -> Result<HashMap<Vec<u8>, Vec<u8>>, OptimisticCoordinatorError>
where
    C: TransactionCommandClient + LockRecoveryClient + Clone + Send,
    L: RegionRecoveryLoader + Send + Sync,
    T: TimestampSource + Sync,
{
    // Go captures the published setting once per BatchGet, after cache lookup
    // and before grouping. Existing snapshots observe configuration updates.
    let enable_async = tidb_config::tikvcfg::async_batch_get_enabled();
    let mut groups = group_snapshot_keys(runtime, keys).map_err(read_error)?;
    let state = BatchGetState {
        timestamps,
        read_ts,
        resource_group_name,
        stats,
        resolved_locks: Mutex::new(resolved_locks),
        values: Mutex::new(HashMap::new()),
        rpc_count: AtomicU64::new(0),
        call,
    };
    let result = if groups.len() == 1 {
        // Go bypasses the async API for one batch. Do not allocate a future,
        // clone a client capability or create a worker for this common path.
        run_batch(
            runtime,
            &state,
            groups.pop().unwrap(),
            RegionBackoffBudget::campaign_default(),
            None,
        )
    } else if enable_async {
        run_initial_batches(runtime, &state, groups)
    } else {
        run_sync_batches(
            runtime,
            &state,
            groups,
            &RegionBackoffBudget::campaign_default(),
        )
    };
    *rpc_count = rpc_count.wrapping_add(state.rpc_count.load(Ordering::Relaxed));
    result?;
    Ok(state.values.into_inner().unwrap_or_else(|p| p.into_inner()))
}

struct BatchGetState<'a, T> {
    timestamps: &'a T,
    read_ts: u64,
    resource_group_name: Option<&'a str>,
    stats: Option<&'a tikv_client::SnapshotRuntimeStats>,
    resolved_locks: Mutex<&'a mut SnapshotLockSet>,
    values: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
    rpc_count: AtomicU64,
    call: &'a UnaryCallContext,
}

impl<T> BatchGetState<'_, T> {
    fn request(&self, batch: &mut RegionKeyBatch) -> (KvrpcBatchGetRequest, KvrpcContext) {
        let mut context = batch.context().clone();
        if let Some(name) = self.resource_group_name {
            name.clone_into(
                &mut context
                    .resource_control_context
                    .get_or_insert_with(Default::default)
                    .resource_group_name,
            );
        }
        self.resolved_locks
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .stamp(&mut context);
        (
            KvrpcBatchGetRequest {
                keys: batch.take_keys(),
                version: self.read_ts,
                need_commit_ts: true,
                ..Default::default()
            },
            context,
        )
    }
}

struct BatchReply {
    request: KvrpcBatchGetRequest,
    context: KvrpcContext,
    published: PublishedCommand<KvrpcBatchGetResponse>,
}

impl BatchReply {
    fn needs_recovery(&self) -> bool {
        match &self.published {
            PublishedCommand::Response(reply) => {
                let reply = &reply.response;
                reply.region_error.is_some()
                    || reply
                        .error
                        .as_ref()
                        .is_some_and(|error| error.locked.is_some())
                    || reply.pairs.iter().any(|pair| {
                        pair.error
                            .as_ref()
                            .is_some_and(|error| error.locked.is_some())
                    })
            }
            _ => false,
        }
    }
}

enum BatchEvent {
    Response(RegionKeyBatch, BatchReply),
    Retried(ReadResult),
}

fn read_error(error: impl std::fmt::Display) -> OptimisticCoordinatorError {
    OptimisticCoordinatorError::SnapshotGet(error.to_string())
}

fn run_initial_batches<C, L, T>(
    runtime: &SharedReadRuntime<C, L>,
    state: &BatchGetState<'_, T>,
    mut groups: Vec<RegionKeyBatch>,
) -> ReadResult
where
    C: TransactionCommandClient + LockRecoveryClient + Clone + Send,
    L: RegionRecoveryLoader + Send + Sync,
    T: TimestampSource + Sync,
{
    let bodies: Vec<_> = groups
        .iter_mut()
        .map(|batch| state.request(batch))
        .collect();
    let requests: Vec<_> = groups
        .iter()
        .zip(bodies)
        .map(|(batch, (request, context))| TransactionBatchGetRequest {
            address: batch.address(),
            request,
            context,
        })
        .collect();
    let pending = runtime
        .client()
        .try_lock()
        .map_err(|_| read_error("TiKV client is already borrowed"))?
        .begin_transaction_batch_gets(&requests, state.call);
    state
        .rpc_count
        .fetch_add(requests.len() as u64, Ordering::Relaxed);
    if pending.len() != requests.len() {
        return Err(read_error(
            "BatchGet admission returned an incomplete completion set",
        ));
    }
    let bodies: Vec<_> = requests
        .into_iter()
        .map(|request| (request.request, request.context))
        .collect();
    let mut events: FuturesUnordered<futures::future::BoxFuture<'_, BatchEvent>> = groups
        .into_iter()
        .zip(bodies)
        .zip(pending)
        .map(|((batch, (request, context)), pending)| {
            Box::pin(async move {
                BatchEvent::Response(
                    batch,
                    BatchReply {
                        request,
                        context,
                        published: pending.await,
                    },
                )
            }) as futures::future::BoxFuture<'_, BatchEvent>
        })
        .collect();
    std::thread::scope(|scope| {
        let mut error = None;
        // The scope joins every retry worker even if the call is cancelled
        // while a worker is still collecting values or updating lock hints.
        while !events.is_empty() {
            let event = match wait_with_call(events.next(), state.call) {
                Ok(Some(event)) => event,
                Ok(None) => break,
                Err(cancelled) => {
                    error = Some(read_error(cancelled));
                    break;
                }
            };
            let result = match event {
                BatchEvent::Retried(result) => result,
                BatchEvent::Response(batch, reply) if reply.needs_recovery() => {
                    let worker_runtime = runtime.fork_client();
                    let (sender, receiver) = futures::channel::oneshot::channel();
                    match std::thread::Builder::new()
                        .name("snapshot-batch-retry".into())
                        .spawn_scoped(scope, move || {
                            let result = run_batch(
                                &worker_runtime,
                                state,
                                batch,
                                RegionBackoffBudget::campaign_default(),
                                Some(reply),
                            );
                            let _ = sender.send(result);
                        }) {
                        Ok(_) => {
                            events.push(Box::pin(async move {
                                BatchEvent::Retried(receiver.await.unwrap_or_else(|_| {
                                    Err(read_error("BatchGet retry worker ended without a result"))
                                }))
                            }));
                            Ok(())
                        }
                        Err(spawn) => Err(read_error(spawn)),
                    }
                }
                BatchEvent::Response(batch, reply) => handle_response(
                    runtime,
                    state,
                    &batch,
                    reply,
                    &mut RegionBackoffBudget::campaign_default(),
                    &mut None,
                )
                .map(|_| ()),
            };
            // Go waits for sibling batches after an ordinary request error.
            // Only cancellation/deadline stops the completion loop early.
            if let Err(next) = result {
                error = Some(next);
            }
        }
        // Drop unfinished transport futures before the scope waits for workers.
        // Their completion owners cancel the exact abandoned RPCs.
        drop(events);
        error.map_or(Ok(()), Err)
    })
}

fn run_batch<C, L, T>(
    runtime: &SharedReadRuntime<C, L>,
    state: &BatchGetState<'_, T>,
    mut batch: RegionKeyBatch,
    mut backoff: RegionBackoffBudget,
    mut initial: Option<BatchReply>,
) -> ReadResult
where
    C: TransactionCommandClient + LockRecoveryClient + Clone + Send,
    L: RegionRecoveryLoader + Send + Sync,
    T: TimestampSource + Sync,
{
    let mut resolving_record = None;
    loop {
        let reply = if let Some(reply) = initial.take() {
            reply
        } else {
            let (request, context) = state.request(&mut batch);
            state.rpc_count.fetch_add(1, Ordering::Relaxed);
            let published = runtime
                .client()
                .try_lock()
                .map_err(|_| read_error("TiKV client is already borrowed"))?
                .publish_transaction_batch_get(batch.address(), &request, &context, state.call);
            BatchReply {
                request,
                context,
                published,
            }
        };
        let Some(keys) = handle_response(
            runtime,
            state,
            &batch,
            reply,
            &mut backoff,
            &mut resolving_record,
        )?
        else {
            return Ok(());
        };
        let mut groups = group_snapshot_keys(runtime, &keys).map_err(read_error)?;
        if groups.len() == 1 {
            batch = groups.pop().unwrap();
            continue;
        }
        // Go recursively groups a split retry with tryAsyncAPI=false; each
        // child owns a fork of the already charged backoffer, never a new budget.
        return run_sync_batches(runtime, state, groups, &backoff);
    }
}

// Both the disabled-async initial path and split retries use Go's synchronous
// worker path. Fork the existing budget; joining the scope retains all request
// state until every worker has stopped, including after cancellation/errors.
fn run_sync_batches<C, L, T>(
    runtime: &SharedReadRuntime<C, L>,
    state: &BatchGetState<'_, T>,
    groups: Vec<RegionKeyBatch>,
    backoff: &RegionBackoffBudget,
) -> ReadResult
where
    C: TransactionCommandClient + LockRecoveryClient + Clone + Send,
    L: RegionRecoveryLoader + Send + Sync,
    T: TimestampSource + Sync,
{
    std::thread::scope(|scope| {
        let (sender, receiver) = std::sync::mpsc::channel();
        let mut error = None;
        for group in groups {
            let worker_runtime = runtime.fork_client();
            let worker_backoff = backoff.fork();
            let sender = sender.clone();
            if let Err(spawn) = std::thread::Builder::new()
                .name("snapshot-batch-get".into())
                .spawn_scoped(scope, move || {
                    let _ = sender.send(run_batch(
                        &worker_runtime,
                        state,
                        group,
                        worker_backoff,
                        None,
                    ));
                })
            {
                error = Some(read_error(spawn));
            }
        }
        drop(sender);
        for result in receiver {
            if let Err(next) = result {
                error = Some(next);
            }
        }
        error.map_or(Ok(()), Err)
    })
}

fn handle_response<C, L, T>(
    runtime: &SharedReadRuntime<C, L>,
    state: &BatchGetState<'_, T>,
    batch: &RegionKeyBatch,
    reply: BatchReply,
    backoff: &mut RegionBackoffBudget,
    record: &mut Option<crate::ResolvingLocksGuard>,
) -> Result<Option<Vec<Vec<u8>>>, OptimisticCoordinatorError>
where
    C: LockRecoveryClient,
    L: RegionRecoveryLoader,
    T: TimestampSource,
{
    let response = match reply.published {
        PublishedCommand::Response(response) => response.response,
        PublishedCommand::BeforePublication(error)
        | PublishedCommand::AfterPublication { error, .. } => return Err(read_error(error)),
    };
    if let Some(region_error) = &response.region_error {
        recover_region_error_with(runtime, backoff, region_error, batch.attempt(), state.call)
            .map_err(snapshot_recovery_error)?;
        return Ok(Some(reply.request.keys));
    }
    if let Some(stats) = state.stats {
        let payload = if response.error.is_some() {
            0
        } else {
            response
                .pairs
                .iter()
                .filter(|pair| pair.error.is_none())
                .fold(0_u64, |bytes, pair| {
                    bytes
                        .wrapping_add(pair.key.len() as u64)
                        .wrapping_add(pair.value.len() as u64)
                })
        };
        stats.record_point_response(response.exec_details_v2.as_ref(), payload);
    }
    let mut locks = Vec::new();
    let mut keys = Vec::new();
    if let Some(error) = &response.error {
        if let Some(lock) = &error.locked {
            locks.extend(decode_blocking_lock_observation(lock).map_err(read_error)?);
            keys = reply.request.keys;
        } else {
            return Err(read_error(format!("TiKV key error: {error:?}")));
        }
    } else {
        let mut values = state.values.lock().unwrap_or_else(|p| p.into_inner());
        for pair in response.pairs {
            if let Some(error) = &pair.error {
                if let Some(lock) = &error.locked {
                    let observations =
                        decode_blocking_lock_observation(lock).map_err(read_error)?;
                    keys.extend(observations.iter().map(|lock| lock.key().to_vec()));
                    locks.extend(observations);
                    continue;
                }
                return Err(read_error(format!("TiKV key error: {error:?}")));
            }
            if !pair.value.is_empty() {
                values.insert(pair.key, pair.value);
            }
        }
    }
    if !locks.is_empty() {
        let recovery = resolve_snapshot_locks(
            runtime,
            &locks,
            state.read_ts,
            &reply.context,
            state.call,
            state.timestamps,
            backoff,
            true,
            record,
        )?;
        state
            .resolved_locks
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .absorb(&recovery);
        wait_snapshot_lock_ttl(&recovery, backoff, state.call)?;
    }
    Ok((!keys.is_empty()).then_some(keys))
}
