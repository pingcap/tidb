// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use futures::stream::BoxStream;

use super::plan::PreserveShard;
use crate::kv::ReplicaReadConfig;
use crate::locate::ReplicaSelectorState;
use crate::pd::PdClient;
use crate::region::RegionWithLeader;
use crate::request::plan::CleanupLocks;
use crate::request::Dispatch;
use crate::request::KvRequest;
use crate::request::Plan;
use crate::request::Process;
use crate::request::ProcessResponse;
use crate::request::ResolveLock;
use crate::retry::RetryConfig;
use crate::store::RegionStore;
use crate::store::Request;
use crate::Result;
use std::fmt::Debug;

macro_rules! impl_inner_shardable {
    () => {
        type Shard = P::Shard;

        fn shards(
            &self,
            pd_client: &Arc<impl PdClient>,
        ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
            self.inner.shards(pd_client)
        }

        fn apply_shard(&mut self, shard: Self::Shard) {
            self.inner.apply_shard(shard);
        }

        fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
            self.inner.apply_store(store)
        }

        fn replica_read_config(&self) -> ReplicaReadConfig {
            self.inner.replica_read_config()
        }

        fn replica_selector_state(&self) -> ReplicaSelectorState {
            self.inner.replica_selector_state()
        }

        fn region_request_runtime_stats(&self) -> Option<Arc<$crate::RegionRequestRuntimeStats>> {
            self.inner.region_request_runtime_stats()
        }

        fn set_region_request_runtime_stats(
            &mut self,
            stats: Option<Arc<$crate::RegionRequestRuntimeStats>>,
        ) {
            self.inner.set_region_request_runtime_stats(stats);
        }

        fn record_replica_attempt(&mut self, peer_id: u64) {
            self.inner.record_replica_attempt(peer_id);
        }

        fn record_replica_attempted_time(&mut self, peer_id: u64, duration: std::time::Duration) {
            self.inner.record_replica_attempted_time(peer_id, duration);
        }

        fn mark_replica_deadline_exceeded(&mut self, peer_id: u64) {
            self.inner.mark_replica_deadline_exceeded(peer_id);
        }

        fn add_pending_backoff(&mut self, store_id: u64, config: RetryConfig, reason: String) {
            self.inner.add_pending_backoff(store_id, config, reason);
        }

        fn take_pending_backoff(&mut self, store_id: u64) -> Option<(RetryConfig, String)> {
            self.inner.take_pending_backoff(store_id)
        }

        fn largest_pending_backoff(&self) -> Option<(RetryConfig, String)> {
            self.inner.largest_pending_backoff()
        }

        fn mark_retry_request(&mut self) {
            self.inner.mark_retry_request();
        }

        fn mark_replica_data_not_ready(&mut self, peer_id: u64) {
            self.inner.mark_replica_data_not_ready(peer_id);
        }

        fn record_busy_leader(
            &mut self,
            target_peer_id: u64,
            leader_peer_id: u64,
            estimated_wait_ms: u32,
        ) {
            self.inner
                .record_busy_leader(target_peer_id, leader_peer_id, estimated_wait_ms);
        }

        fn record_not_leader(&mut self, target_peer_id: u64, leader_peer_id: u64) {
            self.inner.record_not_leader(target_peer_id, leader_peer_id);
        }

        fn mark_replica_no_leader(&mut self, peer_id: u64) {
            self.inner.mark_replica_no_leader(peer_id);
        }

        fn record_server_busy(&mut self, peer_id: u64) {
            self.inner.record_server_busy(peer_id);
        }

        fn force_leader_after_flashback(&mut self) {
            self.inner.force_leader_after_flashback();
        }

        fn force_leader_after_region_not_found(&mut self, leader_peer_id: u64) -> bool {
            self.inner
                .force_leader_after_region_not_found(leader_peer_id)
        }

        fn is_read_request(&self) -> bool {
            self.inner.is_read_request()
        }

        fn max_execution_duration_ms(&self) -> u64 {
            self.inner.max_execution_duration_ms()
        }

        fn is_batched_coprocessor_read(&self) -> bool {
            self.inner.is_batched_coprocessor_read()
        }

        fn disable_stale_read_after_lock(&mut self) -> bool {
            self.inner.disable_stale_read_after_lock()
        }
    };
}

pub trait Shardable {
    type Shard: Debug + Clone + Send + Sync;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>>;

    fn apply_shard(&mut self, shard: Self::Shard);

    /// Implementation can skip unnecessary fields clone if fields will be overwritten by `apply_shard`.
    fn clone_then_apply_shard(&self, shard: Self::Shard) -> Self
    where
        Self: Sized + Clone,
    {
        let mut cloned = self.clone();
        cloned.apply_shard(shard);
        cloned
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()>;

    /// Stable source replica-selection settings retained across shard clones
    /// and retry wrappers. Attempt/error state remains selector-local.
    fn replica_read_config(&self) -> ReplicaReadConfig {
        ReplicaReadConfig::default()
    }

    fn replica_selector_state(&self) -> ReplicaSelectorState {
        ReplicaSelectorState::default()
    }

    fn region_request_runtime_stats(&self) -> Option<Arc<crate::RegionRequestRuntimeStats>> {
        None
    }

    fn set_region_request_runtime_stats(
        &mut self,
        _stats: Option<Arc<crate::RegionRequestRuntimeStats>>,
    ) {
    }

    fn record_replica_attempt(&mut self, _peer_id: u64) {}

    fn record_replica_attempted_time(&mut self, _peer_id: u64, _duration: std::time::Duration) {}

    fn mark_replica_deadline_exceeded(&mut self, _peer_id: u64) {}

    fn add_pending_backoff(&mut self, _store_id: u64, _config: RetryConfig, _reason: String) {}

    fn take_pending_backoff(&mut self, _store_id: u64) -> Option<(RetryConfig, String)> {
        None
    }

    fn largest_pending_backoff(&self) -> Option<(RetryConfig, String)> {
        None
    }

    /// Source `RegionRequestSender` marks every resend in the wire context.
    /// Plans without a TiKV request retain a no-op implementation.
    fn mark_retry_request(&mut self) {}

    fn mark_replica_data_not_ready(&mut self, _peer_id: u64) {}

    fn record_busy_leader(
        &mut self,
        _target_peer_id: u64,
        _leader_peer_id: u64,
        _estimated_wait_ms: u32,
    ) {
    }

    fn record_not_leader(&mut self, _target_peer_id: u64, _leader_peer_id: u64) {}

    fn mark_replica_no_leader(&mut self, _peer_id: u64) {}

    fn record_server_busy(&mut self, _peer_id: u64) {}

    fn force_leader_after_flashback(&mut self) {}

    fn force_leader_after_region_not_found(&mut self, _leader_peer_id: u64) -> bool {
        false
    }

    fn is_read_request(&self) -> bool {
        false
    }

    /// Source `MaxExecutionDurationMs` used to distinguish a caller's short
    /// configurable read timeout from an ordinary transport deadline.
    fn max_execution_duration_ms(&self) -> u64 {
        0
    }

    fn is_batched_coprocessor_read(&self) -> bool {
        false
    }

    /// Source `DisableStaleReadMeetLock` changes the next retry to a direct
    /// leader read. Plans without replica-routing state retain a no-op.
    fn disable_stale_read_after_lock(&mut self) -> bool {
        false
    }
}

impl<P, Pr> Shardable for ProcessResponse<P, Pr>
where
    P: Plan + Shardable,
    Pr: Process<P::Result>,
{
    impl_inner_shardable!();
}

pub trait Batchable {
    type Item;

    fn batches(items: Vec<Self::Item>, batch_size: u64) -> Vec<Vec<Self::Item>> {
        let mut batches: Vec<Vec<Self::Item>> = Vec::new();
        let mut batch: Vec<Self::Item> = Vec::new();
        let mut size = 0;

        for item in items {
            let item_size = Self::item_size(&item);
            if size >= batch_size {
                batches.push(batch);
                batch = Vec::new();
                size = 0;
            }
            size += item_size;
            batch.push(item);
        }
        if !batch.is_empty() {
            batches.push(batch)
        }
        batches
    }

    fn item_size(item: &Self::Item) -> u64;
}

pub(crate) fn key_batches<T>(items: Vec<T>, limit: isize) -> Vec<Vec<T>> {
    let mut batches = Vec::new();
    let mut batch = Vec::new();
    let mut count = 0_isize;
    for item in items {
        if count > limit {
            batches.push(batch);
            batch = Vec::with_capacity(
                usize::try_from(limit).expect("key batch limit cannot be negative"),
            );
            count = 0;
        }
        batch.push(item);
        count += 1;
    }
    if !batch.is_empty() {
        batches.push(batch);
    }
    batches
}

// Use to iterate in a region for scan requests that have batch size limit.
// HasNextBatch use to get the next batch according to previous response.
pub trait HasNextBatch {
    fn has_next_batch(&self) -> Option<(Vec<u8>, Vec<u8>)>;
}

// NextBatch use to change start key of request by result of `has_next_batch`.
pub trait NextBatch {
    fn next_batch(&mut self, _range: (Vec<u8>, Vec<u8>));
}

impl<Req: KvRequest + Shardable> Shardable for Dispatch<Req> {
    type Shard = Req::Shard;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        self.request.shards(pd_client)
    }

    fn apply_shard(&mut self, shard: Self::Shard) {
        self.request.apply_shard(shard);
    }

    fn clone_then_apply_shard(&self, shard: Self::Shard) -> Self
    where
        Self: Sized + Clone,
    {
        Dispatch {
            request: self.request.clone_then_apply_shard(shard),
            kv_client: self.kv_client.clone(),
            request_timeout: self.request_timeout,
            retry_request_timeout: self.retry_request_timeout,
            read_timestamp_validation: self.read_timestamp_validation.clone(),
            target: self.target.clone(),
            forwarded_host: self.forwarded_host.clone(),
            replica_read_config: self.replica_read_config.clone(),
            replica_selector_state: self.replica_selector_state.clone(),
            store_health: self.store_health.clone(),
            record_client_side_slow_score: self.record_client_side_slow_score,
            physical_endpoint_type: self.physical_endpoint_type,
            resource_control_replica_number: self.resource_control_replica_number,
            resource_control_access_location: self.resource_control_access_location,
            predicted_read_bytes: self.predicted_read_bytes,
            ru_details: self.ru_details.clone(),
            store_token_count: self.store_token_count.clone(),
            store_token_store_id: self.store_token_store_id,
            region_request_runtime_stats: self.region_request_runtime_stats.clone(),
            logical_peer_id: self.logical_peer_id,
            logical_store_id: self.logical_store_id,
            request_stale_read: self.request_stale_read,
            request_replica_read: self.request_replica_read,
            interceptor: self.interceptor.clone(),
            execution_details_trace_handler: self.execution_details_trace_handler.clone(),
            network_traffic_details: self.network_traffic_details.clone(),
            network_stale_read: self.network_stale_read,
            resource_control: self.resource_control.clone(),
            response_codec: self.response_codec,
            v1_response_codec: self.v1_response_codec,
        }
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        self.kv_client = Some(store.client.clone());
        self.target = store.target.clone();
        self.forwarded_host = store.forwarded_host.clone();
        self.store_health = store.health_status.clone();
        self.record_client_side_slow_score = store.record_client_side_slow_score;
        self.physical_endpoint_type = store.physical_endpoint_type;
        self.resource_control_replica_number = store.resource_control_replica_number;
        self.resource_control_access_location = store.resource_control_access_location;
        self.store_token_count = store.store_token_count.clone();
        self.store_token_store_id = store.target_peer.as_ref().map_or(0, |peer| peer.store_id);
        self.logical_peer_id = store.target_peer.as_ref().map(|peer| peer.id);
        self.logical_store_id = store.target_peer.as_ref().map(|peer| peer.store_id);
        self.request_stale_read = store.stale_read;
        self.request_replica_read = store.is_replica_read();
        if store.busy_threshold_disabled {
            self.replica_selector_state.disable_busy_threshold();
        }
        if store.restores_suspect_leader {
            if let Some(leader) = store.region_with_leader.leader.as_ref() {
                self.replica_selector_state
                    .restore_suspect_leader(leader.id);
            }
        }
        self.request.apply_store(store).map(|()| {
            self.request
                .set_buckets_version(store.region_with_leader.buckets_version())
        })
    }

    fn replica_read_config(&self) -> ReplicaReadConfig {
        self.replica_read_config.clone()
    }

    fn replica_selector_state(&self) -> ReplicaSelectorState {
        self.replica_selector_state.clone()
    }

    fn region_request_runtime_stats(&self) -> Option<Arc<crate::RegionRequestRuntimeStats>> {
        self.region_request_runtime_stats.clone()
    }

    fn set_region_request_runtime_stats(
        &mut self,
        stats: Option<Arc<crate::RegionRequestRuntimeStats>>,
    ) {
        self.region_request_runtime_stats = stats;
    }

    fn record_replica_attempt(&mut self, peer_id: u64) {
        self.replica_selector_state.record_attempt(peer_id);
    }

    fn record_replica_attempted_time(&mut self, peer_id: u64, duration: std::time::Duration) {
        self.replica_selector_state
            .record_attempted_time(peer_id, duration);
    }

    fn mark_replica_deadline_exceeded(&mut self, peer_id: u64) {
        self.replica_selector_state.mark_deadline_exceeded(peer_id);
    }

    fn add_pending_backoff(&mut self, store_id: u64, config: RetryConfig, reason: String) {
        self.replica_selector_state
            .add_pending_backoff(store_id, config, reason);
    }

    fn take_pending_backoff(&mut self, store_id: u64) -> Option<(RetryConfig, String)> {
        self.replica_selector_state.take_pending_backoff(store_id)
    }

    fn largest_pending_backoff(&self) -> Option<(RetryConfig, String)> {
        self.replica_selector_state.largest_pending_backoff()
    }

    fn mark_retry_request(&mut self) {
        self.request.set_is_retry_request();
        if let Some(timeout) = self.retry_request_timeout {
            self.request_timeout = Some(timeout);
            self.request.set_max_execution_duration_ms(
                u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
            );
        }
    }

    fn mark_replica_data_not_ready(&mut self, peer_id: u64) {
        self.replica_selector_state.mark_data_is_not_ready(peer_id);
    }

    fn record_busy_leader(
        &mut self,
        target_peer_id: u64,
        leader_peer_id: u64,
        estimated_wait_ms: u32,
    ) {
        if estimated_wait_ms == 0
            && matches!(
                self.replica_read_config.read_type,
                crate::kv::ReplicaReadType::Leader
            )
            && !self.replica_read_config.stale_read
            && !self.replica_read_config.leader_only
            && target_peer_id == leader_peer_id
        {
            self.replica_selector_state
                .record_busy_leader(leader_peer_id);
        }
    }

    fn record_not_leader(&mut self, target_peer_id: u64, leader_peer_id: u64) {
        self.replica_selector_state
            .record_not_leader(target_peer_id, leader_peer_id);
    }

    fn mark_replica_no_leader(&mut self, peer_id: u64) {
        self.replica_selector_state.mark_no_leader(peer_id);
    }

    fn record_server_busy(&mut self, peer_id: u64) {
        self.replica_selector_state.record_server_busy(peer_id);
    }

    fn force_leader_after_flashback(&mut self) {
        self.replica_selector_state.force_leader_after_flashback();
    }

    fn force_leader_after_region_not_found(&mut self, leader_peer_id: u64) -> bool {
        self.replica_selector_state
            .force_leader_after_region_not_found(leader_peer_id)
    }

    fn is_read_request(&self) -> bool {
        KvRequest::is_read_request(&self.request)
    }

    fn max_execution_duration_ms(&self) -> u64 {
        Request::max_execution_duration_ms(&self.request)
    }

    fn is_batched_coprocessor_read(&self) -> bool {
        KvRequest::is_batched_coprocessor_read(&self.request)
    }

    fn disable_stale_read_after_lock(&mut self) -> bool {
        if !self.replica_read_config.stale_read {
            return false;
        }
        self.replica_read_config.stale_read = false;
        self.replica_read_config.read_type = crate::kv::ReplicaReadType::Leader;
        self.replica_read_config.busy_threshold_ms = 0;
        true
    }
}

impl<Req: KvRequest + NextBatch> NextBatch for Dispatch<Req> {
    fn next_batch(&mut self, range: (Vec<u8>, Vec<u8>)) {
        self.request.next_batch(range);
    }
}

impl<P: Plan + Shardable> Shardable for PreserveShard<P> {
    type Shard = P::Shard;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        self.inner.shards(pd_client)
    }

    fn apply_shard(&mut self, shard: Self::Shard) {
        self.shard = Some(shard.clone());
        self.inner.apply_shard(shard)
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        self.inner.apply_store(store)
    }

    fn replica_read_config(&self) -> ReplicaReadConfig {
        self.inner.replica_read_config()
    }

    fn replica_selector_state(&self) -> ReplicaSelectorState {
        self.inner.replica_selector_state()
    }

    fn region_request_runtime_stats(&self) -> Option<Arc<crate::RegionRequestRuntimeStats>> {
        self.inner.region_request_runtime_stats()
    }

    fn set_region_request_runtime_stats(
        &mut self,
        stats: Option<Arc<crate::RegionRequestRuntimeStats>>,
    ) {
        self.inner.set_region_request_runtime_stats(stats);
    }

    fn record_replica_attempt(&mut self, peer_id: u64) {
        self.inner.record_replica_attempt(peer_id);
    }

    fn mark_retry_request(&mut self) {
        self.inner.mark_retry_request();
    }

    fn mark_replica_data_not_ready(&mut self, peer_id: u64) {
        self.inner.mark_replica_data_not_ready(peer_id);
    }

    fn record_busy_leader(
        &mut self,
        target_peer_id: u64,
        leader_peer_id: u64,
        estimated_wait_ms: u32,
    ) {
        self.inner
            .record_busy_leader(target_peer_id, leader_peer_id, estimated_wait_ms);
    }

    fn record_not_leader(&mut self, target_peer_id: u64, leader_peer_id: u64) {
        self.inner.record_not_leader(target_peer_id, leader_peer_id);
    }

    fn record_server_busy(&mut self, peer_id: u64) {
        self.inner.record_server_busy(peer_id);
    }

    fn force_leader_after_flashback(&mut self) {
        self.inner.force_leader_after_flashback();
    }

    fn force_leader_after_region_not_found(&mut self, leader_peer_id: u64) -> bool {
        self.inner
            .force_leader_after_region_not_found(leader_peer_id)
    }

    fn is_read_request(&self) -> bool {
        self.inner.is_read_request()
    }

    fn is_batched_coprocessor_read(&self) -> bool {
        self.inner.is_batched_coprocessor_read()
    }

    fn disable_stale_read_after_lock(&mut self) -> bool {
        self.inner.disable_stale_read_after_lock()
    }
}

impl<P: Plan + Shardable, PdC: PdClient> Shardable for ResolveLock<P, PdC> {
    impl_inner_shardable!();
}

impl<P: Plan + Shardable, PdC: PdClient> Shardable for CleanupLocks<P, PdC> {
    type Shard = P::Shard;

    fn shards(
        &self,
        pd_client: &Arc<impl PdClient>,
    ) -> BoxStream<'static, Result<(Self::Shard, RegionWithLeader)>> {
        self.inner.shards(pd_client)
    }

    fn apply_shard(&mut self, shard: Self::Shard) {
        self.inner.apply_shard(shard)
    }

    fn apply_store(&mut self, store: &RegionStore) -> Result<()> {
        self.store = Some(store.clone());
        self.inner.apply_store(store)
    }

    fn mark_retry_request(&mut self) {
        self.inner.mark_retry_request();
    }

    fn region_request_runtime_stats(&self) -> Option<Arc<crate::RegionRequestRuntimeStats>> {
        self.inner.region_request_runtime_stats()
    }

    fn set_region_request_runtime_stats(
        &mut self,
        stats: Option<Arc<crate::RegionRequestRuntimeStats>>,
    ) {
        self.inner.set_region_request_runtime_stats(stats);
    }
}

#[doc(hidden)]
#[macro_export]
macro_rules! shardable_key {
    ($type_: ty) => {
        impl Shardable for $type_ {
            type Shard = Vec<Vec<u8>>;

            fn shards(
                &self,
                pd_client: &std::sync::Arc<impl $crate::pd::PdClient>,
            ) -> futures::stream::BoxStream<
                'static,
                $crate::Result<(Self::Shard, $crate::region::RegionWithLeader)>,
            > {
                $crate::store::region_stream_for_keys(
                    std::iter::once(self.key.clone()),
                    pd_client.clone(),
                )
            }

            fn apply_shard(&mut self, mut shard: Self::Shard) {
                assert!(shard.len() == 1);
                self.key = shard.pop().unwrap();
            }

            fn apply_store(&mut self, store: &$crate::store::RegionStore) -> $crate::Result<()> {
                self.set_leader(&store.request_region())?;
                self.set_replica_read(store.is_replica_read());
                self.set_stale_read(store.stale_read);
                self.set_busy_threshold_ms(store.busy_threshold_ms);
                Ok(())
            }
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! shardable_keys {
    ($type_: ty) => {
        impl Shardable for $type_ {
            type Shard = Vec<Vec<u8>>;

            fn shards(
                &self,
                pd_client: &std::sync::Arc<impl $crate::pd::PdClient>,
            ) -> futures::stream::BoxStream<
                'static,
                $crate::Result<(Self::Shard, $crate::region::RegionWithLeader)>,
            > {
                let mut keys = self.keys.clone();
                keys.sort();
                $crate::store::region_stream_for_keys(keys.into_iter(), pd_client.clone())
            }

            fn apply_shard(&mut self, shard: Self::Shard) {
                self.keys = shard.into_iter().map(Into::into).collect();
            }

            fn apply_store(&mut self, store: &$crate::store::RegionStore) -> $crate::Result<()> {
                self.set_leader(&store.request_region())?;
                self.set_replica_read(store.is_replica_read());
                self.set_stale_read(store.stale_read);
                self.set_busy_threshold_ms(store.busy_threshold_ms);
                Ok(())
            }
        }
    };
}

pub trait RangeRequest: Request {
    fn is_reverse(&self) -> bool {
        false
    }
}

#[doc(hidden)]
#[macro_export]
macro_rules! range_request {
    ($type_: ty) => {
        impl RangeRequest for $type_ {}
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! reversible_range_request {
    ($type_: ty) => {
        impl RangeRequest for $type_ {
            fn is_reverse(&self) -> bool {
                self.reverse
            }
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! shardable_range {
    ($type_: ty) => {
        impl Shardable for $type_ {
            type Shard = (Vec<u8>, Vec<u8>);

            fn shards(
                &self,
                pd_client: &Arc<impl $crate::pd::PdClient>,
            ) -> BoxStream<'static, $crate::Result<(Self::Shard, $crate::region::RegionWithLeader)>>
            {
                let mut start_key = self.start_key.clone().into();
                let mut end_key = self.end_key.clone().into();
                // In a reverse range request, the range is in the meaning of [end_key, start_key), i.e. end_key <= x < start_key.
                // Therefore, before fetching the regions from PD, it is necessary to swap the values of start_key and end_key.
                if self.is_reverse() {
                    std::mem::swap(&mut start_key, &mut end_key);
                }
                $crate::store::region_stream_for_range((start_key, end_key), pd_client.clone())
            }

            fn apply_shard(&mut self, shard: Self::Shard) {
                // In a reverse range request, the range is in the meaning of [end_key, start_key), i.e. end_key <= x < start_key.
                // As a result, after obtaining start_key and end_key from PD, we need to swap their values when assigning them to the request.
                self.start_key = shard.0;
                self.end_key = shard.1;
                if self.is_reverse() {
                    std::mem::swap(&mut self.start_key, &mut self.end_key);
                }
            }

            fn apply_store(&mut self, store: &$crate::store::RegionStore) -> $crate::Result<()> {
                self.set_leader(&store.request_region())?;
                self.set_replica_read(store.is_replica_read());
                self.set_stale_read(store.stale_read);
                self.set_busy_threshold_ms(store.busy_threshold_ms);
                Ok(())
            }
        }
    };
}

#[cfg(test)]
mod test {
    use rand::thread_rng;
    use rand::Rng;

    use super::{Batchable, Shardable};
    use crate::kv::{AccessLocationType, ReplicaReadConfig, ReplicaReadType};
    use crate::locate::ReplicaSelectorState;
    use crate::mock::MockKvClient;
    use crate::proto::kvrpcpb;
    use crate::proto::metapb;
    use crate::region::RegionWithLeader;
    use crate::request::plan::Dispatch;
    use crate::store::RegionStore;
    use std::sync::Arc;

    #[test]
    fn test_batches() {
        let mut rng = thread_rng();

        let items: Vec<_> = (0..3)
            .map(|_| (0..2).map(|_| rng.gen::<u8>()).collect::<Vec<_>>())
            .collect();

        let batch_size = 5;

        let batches = BatchableTest::batches(items.clone(), batch_size);

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 3);
        assert_eq!(batches[0][0], items[0]);
        assert_eq!(batches[0][1], items[1]);
        assert_eq!(batches[0][2], items[2]);
    }

    #[test]
    fn source_lock_on_stale_read_retries_a_threshold_free_leader() {
        let mut dispatch = Dispatch {
            request: kvrpcpb::GetRequest::default(),
            kv_client: None,
            request_timeout: None,
            retry_request_timeout: None,
            read_timestamp_validation: None,
            target: String::new(),
            forwarded_host: String::new(),
            replica_read_config: ReplicaReadConfig {
                read_type: ReplicaReadType::Mixed,
                stale_read: true,
                busy_threshold_ms: 50,
                ..Default::default()
            },
            replica_selector_state: ReplicaSelectorState::default(),
            store_health: None,
            record_client_side_slow_score: false,
            physical_endpoint_type: crate::store::EndpointType::TiKv,
            resource_control_replica_number: 1,
            resource_control_access_location: AccessLocationType::Unknown,
            predicted_read_bytes: 0,
            ru_details: None,
            store_token_count: Arc::new(std::sync::atomic::AtomicI64::new(0)),
            store_token_store_id: 0,
            region_request_runtime_stats: None,
            logical_peer_id: None,
            logical_store_id: None,
            request_stale_read: false,
            request_replica_read: false,
            interceptor: None,
            execution_details_trace_handler: None,
            network_traffic_details: None,
            network_stale_read: false,
            resource_control: None,
            response_codec: None,
            v1_response_codec: None,
        };
        assert!(dispatch.disable_stale_read_after_lock());
        assert_eq!(
            dispatch.replica_read_config.read_type,
            ReplicaReadType::Leader
        );
        assert!(!dispatch.replica_read_config.stale_read);
        assert_eq!(dispatch.replica_read_config.busy_threshold_ms, 0);
        assert!(!dispatch.disable_stale_read_after_lock());
    }

    #[test]
    fn source_dispatch_applies_route_metadata_and_restores_suspect_leader() {
        let mut region = RegionWithLeader::default();
        region.region.id = 1;
        region.region.region_epoch = Some(metapb::RegionEpoch {
            conf_ver: 1,
            version: 1,
        });
        region.leader = Some(metapb::Peer {
            id: 2,
            store_id: 3,
            ..Default::default()
        });
        region.buckets = Some(metapb::Buckets {
            region_id: 1,
            version: 9,
            ..Default::default()
        });
        let store = RegionStore::new(region, Arc::new(MockKvClient::default()))
            .with_target("proxy:20160")
            .with_forwarded_host("leader:20160")
            .with_restored_suspect_leader();
        let mut dispatch = Dispatch {
            request: kvrpcpb::GetRequest::default(),
            kv_client: None,
            request_timeout: None,
            retry_request_timeout: None,
            read_timestamp_validation: None,
            target: String::new(),
            forwarded_host: String::new(),
            replica_read_config: ReplicaReadConfig::default(),
            replica_selector_state: ReplicaSelectorState::default(),
            store_health: None,
            record_client_side_slow_score: false,
            physical_endpoint_type: crate::store::EndpointType::TiKv,
            resource_control_replica_number: 1,
            resource_control_access_location: AccessLocationType::Unknown,
            predicted_read_bytes: 0,
            ru_details: None,
            store_token_count: Arc::new(std::sync::atomic::AtomicI64::new(0)),
            store_token_store_id: 0,
            region_request_runtime_stats: None,
            logical_peer_id: None,
            logical_store_id: None,
            request_stale_read: false,
            request_replica_read: false,
            interceptor: None,
            execution_details_trace_handler: None,
            network_traffic_details: None,
            network_stale_read: false,
            resource_control: None,
            response_codec: None,
            v1_response_codec: None,
        };

        dispatch.replica_selector_state.record_attempt(2);
        dispatch.replica_selector_state.record_attempt(2);
        dispatch.replica_selector_state.record_busy_leader(2);
        dispatch.replica_selector_state.record_busy_leader(2);
        assert!(dispatch.replica_selector_state.should_probe_busy_leader(2));

        dispatch.apply_store(&store).unwrap();
        assert_eq!(dispatch.request.context.unwrap().buckets_version, 9);
        assert_eq!(dispatch.target, "proxy:20160");
        assert_eq!(dispatch.forwarded_host, "leader:20160");
        assert_eq!(dispatch.store_token_store_id, 3);
        assert!(Arc::ptr_eq(
            &dispatch.store_token_count,
            &store.store_token_count
        ));
        assert!(dispatch.replica_selector_state.is_leader_selectable(2));
    }

    #[test]
    fn size_batches_split_before_the_item_after_the_limit_is_reached() {
        let items = vec![vec![1; 2], vec![2; 2], vec![3; 2], vec![4; 2]];
        let batches = BatchableTest::batches(items.clone(), 5);
        assert_eq!(batches, vec![items[..3].to_vec(), items[3..].to_vec()]);

        let zero_limit = BatchableTest::batches(vec![vec![1], vec![2]], 0);
        assert_eq!(zero_limit, vec![vec![], vec![vec![1]], vec![vec![2]]]);
    }

    #[test]
    fn key_count_batches_preserve_the_client_go_boundary() {
        let items = (0..514).collect::<Vec<_>>();
        let batches = super::key_batches(items.clone(), 512);
        assert_eq!(batches, vec![items[..513].to_vec(), items[513..].to_vec()]);

        assert_eq!(super::key_batches(vec![1, 2], 0), vec![vec![1], vec![2]]);
        assert!(super::key_batches::<i32>(Vec::new(), -1).is_empty());
        assert!(std::panic::catch_unwind(|| super::key_batches(vec![1], -1)).is_err());
    }

    #[test]
    fn test_batches_big_item() {
        let mut rng = thread_rng();

        let items: Vec<_> = (0..3)
            .map(|_| (0..3).map(|_| rng.gen::<u8>()).collect::<Vec<_>>())
            .collect();

        let batch_size = 2;

        let batches = BatchableTest::batches(items.clone(), batch_size);

        assert_eq!(batches.len(), 3);
        for i in 0..items.len() {
            let batch = &batches[i];
            assert_eq!(batch.len(), 1);
            assert_eq!(batch[0], items[i]);
        }
    }

    struct BatchableTest;

    impl Batchable for BatchableTest {
        type Item = Vec<u8>;

        fn item_size(item: &Self::Item) -> u64 {
            item.len() as u64
        }
    }
}
