// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! The complete pinned PD `opt` package. These values describe PD policy;
//! service discovery, TSO and RPC owners decide when to consume them.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::sync::{mpsc, Mutex};
use tonic::transport::Endpoint;

use super::backoff::SharedBackoffer;
use crate::{Error, Result};

/// Native gRPC endpoint configuration, retained in append order like DialOption.
pub type GrpcDialOption = Arc<dyn Fn(Endpoint) -> Endpoint + Send + Sync>;
/// Go's labels map is assigned by reference, not copied by WithMetricsLabels.
pub type MetricsLabels = Arc<RwLock<HashMap<String, String>>>;
/// Shared backing storage preserves WithRangeEnd's slice assignment semantics.
pub type RangeEnd = Arc<[AtomicU8]>;

/// Source dynamic-option discriminants. Updates use the typed methods below.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum DynamicOption {
    MaxTsoBatchWaitInterval,
    EnableTsoFollowerProxy,
    EnableFollowerHandle,
    TsoClientRpcConcurrency,
    EnableRouterClient,
}

/// A capacity-one notification channel. Its value is a wakeup, not a snapshot;
/// consumers reread the corresponding atomic option after receiving it.
/// The receiver mutex adapts Go's channel to Tokio's single-receiver API.
pub struct OptionChange {
    pub sender: mpsc::Sender<()>,
    pub receiver: Mutex<mpsc::Receiver<()>>,
}

impl OptionChange {
    fn new() -> Self {
        let (sender, receiver) = mpsc::channel(1);
        Self {
            sender,
            receiver: Mutex::new(receiver),
        }
    }

    fn notify(&self) {
        // The source's nonblocking send coalesces changes while the slot is full.
        // Closing this channel is a caller error, as sending on a closed Go channel is.
        match self.sender.try_send(()) {
            Ok(()) | Err(mpsc::error::TrySendError::Full(())) => {}
            Err(mpsc::error::TrySendError::Closed(())) => {
                panic!("send on closed PD option channel")
            }
        }
    }
}

/// Static configuration is set before sharing this owner. Runtime settings and
/// their notification channels stay together under the same Arc in consumers.
pub struct Options {
    pub grpc_dial_options: Vec<GrpcDialOption>,
    pub timeout: Duration,
    pub max_retry_times: isize,
    pub enable_forwarding: bool,
    pub use_tso_server_proxy: bool,
    pub metrics_labels: Option<MetricsLabels>,
    pub init_metrics: bool,
    pub backoffer: Option<SharedBackoffer>,
    max_tso_batch_wait_interval: AtomicU64,
    enable_tso_follower_proxy: AtomicBool,
    enable_follower_handle: AtomicBool,
    tso_client_rpc_concurrency: AtomicIsize,
    enable_router_client: AtomicBool,
    pub enable_tso_follower_proxy_ch: OptionChange,
    pub enable_follower_handle_ch: OptionChange,
    pub enable_router_client_ch: OptionChange,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            grpc_dial_options: Vec::new(),
            timeout: Duration::from_secs(3),
            max_retry_times: 100,
            enable_forwarding: false,
            use_tso_server_proxy: false,
            metrics_labels: None,
            init_metrics: true,
            backoffer: None,
            max_tso_batch_wait_interval: AtomicU64::new(0),
            enable_tso_follower_proxy: AtomicBool::new(false),
            enable_follower_handle: AtomicBool::new(false),
            tso_client_rpc_concurrency: AtomicIsize::new(1),
            enable_router_client: AtomicBool::new(true),
            enable_tso_follower_proxy_ch: OptionChange::new(),
            enable_follower_handle_ch: OptionChange::new(),
            enable_router_client_ch: OptionChange::new(),
        }
    }
}

impl Options {
    /// Go NewOption, including fresh, initially empty notification channels.
    pub fn new() -> Self {
        Self::default()
    }

    /// Accept batch waits from zero through ten milliseconds, inclusive.
    pub fn set_max_tso_batch_wait_interval(&self, interval: Duration) -> Result<()> {
        if interval > Duration::from_millis(10) {
            return Err(Error::StringError(
                "[pd] invalid max TSO batch wait interval, should be between 0 and 10ms".into(),
            ));
        }
        // Go uses one CAS, not an unconditional store or retry-until-success loop.
        let old = self.max_tso_batch_wait_interval.load(Ordering::SeqCst);
        let _ = self.max_tso_batch_wait_interval.compare_exchange(
            old,
            interval.as_nanos() as u64,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        Ok(())
    }

    /// Return the current maximum TSO batch wait.
    pub fn get_max_tso_batch_wait_interval(&self) -> Duration {
        Duration::from_nanos(self.max_tso_batch_wait_interval.load(Ordering::SeqCst))
    }

    /// Change follower proxy policy and notify only if the value changes.
    pub fn set_enable_tso_follower_proxy(&self, enable: bool) {
        if self
            .enable_tso_follower_proxy
            .compare_exchange(!enable, enable, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.enable_tso_follower_proxy_ch.notify();
        }
    }

    /// Return whether TSO follower proxying is enabled.
    pub fn get_enable_tso_follower_proxy(&self) -> bool {
        self.enable_tso_follower_proxy.load(Ordering::SeqCst)
    }

    /// Change follower handling policy and notify only if the value changes.
    pub fn set_enable_follower_handle(&self, enable: bool) {
        if self
            .enable_follower_handle
            .compare_exchange(!enable, enable, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.enable_follower_handle_ch.notify();
        }
    }

    /// Return whether followers may handle requests.
    pub fn get_enable_follower_handle(&self) -> bool {
        self.enable_follower_handle.load(Ordering::SeqCst)
    }

    /// Store concurrency with one CAS; validation belongs to the consuming owner.
    pub fn set_tso_client_rpc_concurrency(&self, value: isize) {
        let _ = self.tso_client_rpc_concurrency.compare_exchange(
            self.get_tso_client_rpc_concurrency(),
            value,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }

    /// Return the current TSO RPC concurrency setting.
    pub fn get_tso_client_rpc_concurrency(&self) -> isize {
        self.tso_client_rpc_concurrency.load(Ordering::SeqCst)
    }

    /// Change router policy and notify only if the value changes.
    pub fn set_enable_router_client(&self, enable: bool) {
        if self
            .enable_router_client
            .compare_exchange(!enable, enable, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.enable_router_client_ch.notify();
        }
    }

    /// Return whether the router client is enabled.
    pub fn get_enable_router_client(&self) -> bool {
        self.enable_router_client.load(Ordering::SeqCst)
    }
}

/// Reusable client configuration applied before sharing Options.
pub type ClientOption = Box<dyn Fn(&mut Options) + Send + Sync>;

/// Append gRPC dial options in the order supplied.
pub fn with_grpc_dial_options(options: Vec<GrpcDialOption>) -> ClientOption {
    Box::new(move |op| op.grpc_dial_options.extend(options.iter().cloned()))
}
/// Set the PD request timeout.
pub fn with_custom_timeout_option(timeout: Duration) -> ClientOption {
    Box::new(move |op| op.timeout = timeout)
}
/// Set PD request forwarding policy.
pub fn with_forwarding_option(enable: bool) -> ClientOption {
    Box::new(move |op| op.enable_forwarding = enable)
}
/// Set whether TSO requests use the API leader as a server proxy.
pub fn with_tso_server_proxy_option(enable: bool) -> ClientOption {
    Box::new(move |op| op.use_tso_server_proxy = enable)
}
/// Set the maximum initialization attempts without validating the count.
pub fn with_max_error_retry(count: isize) -> ClientOption {
    Box::new(move |op| op.max_retry_times = count)
}
/// Assign the shared metric labels map, including a nil map.
pub fn with_metrics_labels(labels: Option<MetricsLabels>) -> ClientOption {
    Box::new(move |op| op.metrics_labels = labels.clone())
}
/// Set whether the client initializes metrics.
pub fn with_init_metrics_option(init_metrics: bool) -> ClientOption {
    Box::new(move |op| op.init_metrics = init_metrics)
}
/// Assign the shared RPC retry policy, including a nil policy.
pub fn with_backoffer(backoffer: Option<SharedBackoffer>) -> ClientOption {
    Box::new(move |op| op.backoffer = backoffer.clone())
}
/// Configure router policy through its notifying setter.
pub fn with_enable_router_client(enable: bool) -> ClientOption {
    Box::new(move |op| op.set_enable_router_client(enable))
}
/// Configure follower handling through its notifying setter.
pub fn with_enable_follower_handle(enable: bool) -> ClientOption {
    Box::new(move |op| op.set_enable_follower_handle(enable))
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
/// PD store lookup policy.
pub struct GetStoreOp {
    pub exclude_tombstone: bool,
    pub allow_router_service_handle: bool,
}
/// Reusable per-store request configuration.
pub type GetStoreOption = Box<dyn Fn(&mut GetStoreOp) + Send + Sync>;
/// Exclude tombstone stores from the result.
pub fn with_exclude_tombstone() -> GetStoreOption {
    Box::new(|op| op.exclude_tombstone = true)
}
/// Permit router service handling of this store request.
pub fn with_allow_router_service_handle_store_request() -> GetStoreOption {
    Box::new(|op| op.allow_router_service_handle = true)
}
/// Require PD leader handling of this store request.
pub fn with_pd_leader_handle_store_request_only() -> GetStoreOption {
    Box::new(|op| op.allow_router_service_handle = false)
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
/// PD scatter and split policy.
pub struct RegionsOp {
    pub group: String,
    pub retry_limit: u64,
    pub skip_store_limit: bool,
}
/// Reusable scatter or split request configuration.
pub type RegionsOption = Box<dyn Fn(&mut RegionsOp) + Send + Sync>;
/// Set the group for scatter or split operations.
pub fn with_group(group: String) -> RegionsOption {
    Box::new(move |op| op.group.clone_from(&group))
}
/// Set the retry limit for scatter or split operations.
pub fn with_retry(retry: u64) -> RegionsOption {
    Box::new(move |op| op.retry_limit = retry)
}
/// Skip the store limit check for scatter or split operations.
pub fn with_skip_store_limit() -> RegionsOption {
    Box::new(|op| op.skip_store_limit = true)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
/// Complete PD region lookup and scan policy.
pub struct GetRegionOp {
    pub need_buckets: bool,
    pub allow_follower_handle: bool,
    pub output_must_contain_all_key_range: bool,
    pub allow_router_service_handle: bool,
}
/// Reusable per-region request configuration.
pub type GetRegionOption = Box<dyn Fn(&mut GetRegionOp) + Send + Sync>;
/// Request region bucket metadata.
pub fn with_buckets() -> GetRegionOption {
    Box::new(|op| op.need_buckets = true)
}
/// Permit a follower to handle this region request.
pub fn with_allow_follower_handle() -> GetRegionOption {
    Box::new(|op| op.allow_follower_handle = true)
}
/// Permit the router service to handle this region request.
pub fn with_allow_router_service_handle() -> GetRegionOption {
    Box::new(|op| op.allow_router_service_handle = true)
}
/// Require the output to contain all requested key ranges.
pub fn with_output_must_contain_all_key_range() -> GetRegionOption {
    Box::new(|op| op.output_must_contain_all_key_range = true)
}
/// Clear both follower and router permissions, preserving other region options.
pub fn with_allow_pd_leader_only() -> GetRegionOption {
    Box::new(|op| {
        op.allow_router_service_handle = false;
        op.allow_follower_handle = false;
    })
}

#[derive(Clone, Debug, Default)]
/// PD metadata operation options; shared range bytes retain caller identity.
pub struct MetaStorageOp {
    pub range_end: Option<RangeEnd>,
    pub revision: i64,
    pub prev_kv: bool,
    pub lease: i64,
    pub limit: i64,
    pub is_opts_with_prefix: bool,
}
/// Reusable metadata request configuration.
pub type MetaStorageOption = Box<dyn Fn(&mut MetaStorageOp) + Send + Sync>;
/// Set the metadata result limit.
pub fn with_limit(limit: i64) -> MetaStorageOption {
    Box::new(move |op| op.limit = limit)
}
/// Assign shared range-end bytes, preserving a nil versus empty range.
pub fn with_range_end(range_end: Option<RangeEnd>) -> MetaStorageOption {
    Box::new(move |op| op.range_end = range_end.clone())
}
/// Set the metadata start revision.
pub fn with_rev(revision: i64) -> MetaStorageOption {
    Box::new(move |op| op.revision = revision)
}
/// Request the previous metadata key-value pair.
pub fn with_prev_kv() -> MetaStorageOption {
    Box::new(|op| op.prev_kv = true)
}
/// Set the metadata lease.
pub fn with_lease(lease: i64) -> MetaStorageOption {
    Box::new(move |op| op.lease = lease)
}
/// Mark this metadata operation as a prefix operation.
pub fn with_prefix() -> MetaStorageOption {
    Box::new(|op| op.is_opts_with_prefix = true)
}

#[cfg(test)]
#[path = "opt_tests.rs"]
mod tests;
