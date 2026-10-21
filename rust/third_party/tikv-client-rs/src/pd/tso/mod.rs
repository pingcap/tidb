// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! pd-client's `clients/tso` package: the TSO client (`Cli`, client.go), its
//! request dispatcher (dispatcher.go), streams (stream.go) and requests
//! (request.go), over the `servicediscovery.ServiceDiscovery` facts the TSO
//! client consumes.

use std::time::Duration;

use async_trait::async_trait;
use tonic::transport::Channel;

use crate::{Error, Result};

mod client;
mod dispatcher;
mod request;
mod stream;

pub(crate) use client::{get_ts_async, Cli};
pub(crate) use stream::StreamBuilderFactory;

/// Go `constants.DefaultKeyspaceGroupID`.
pub(crate) const DEFAULT_KEYSPACE_GROUP_ID: u32 = 0;
/// Go `constants.RetryInterval`.
pub(crate) const RETRY_INTERVAL: Duration = Duration::from_millis(500);
/// Go `constants.MaxRetryTimes`.
pub(crate) const MAX_RETRY_TIMES: u64 = 6;
/// Go `defaultMaxTSOBatchSize`.
pub(crate) const DEFAULT_MAX_TSO_BATCH_SIZE: usize = 10_000;
/// Go `servicediscovery.UpdateMemberBackOffBaseTime`.
const UPDATE_MEMBER_BACKOFF_BASE_TIME: Duration = Duration::from_millis(20);
/// Go `servicediscovery.UpdateMemberMaxBackoffTime`.
const UPDATE_MEMBER_MAX_BACKOFF_TIME: Duration = Duration::from_millis(100);
/// Go `servicediscovery.UpdateMemberTimeout`.
const UPDATE_MEMBER_TIMEOUT: Duration = Duration::from_secs(1);

/// The `servicediscovery.ServiceDiscovery` methods the TSO client uses: the
/// PD leader/followers in PD mode, a keyspace group's primary/secondaries in
/// microservice mode. The owner calls [`Cli::update_tso_leader_url`] when the
/// serving URL switches (Go `ExecAndAddLeaderSwitchedCallback`) and
/// [`Cli::schedule_update_tso_connection_ctxs`] when the members change (Go
/// `AddMembersChangedCallback`).
#[async_trait]
pub(crate) trait ServiceDiscovery: Send + Sync {
    /// Go `GetClusterID`.
    fn get_cluster_id(&self) -> u64;
    /// Go `GetKeyspaceID`.
    fn get_keyspace_id(&self) -> u32;
    /// Go `GetKeyspaceGroupID`.
    fn get_keyspace_group_id(&self) -> u32;
    /// Go `GetServiceURLs`.
    fn get_service_urls(&self) -> Vec<String>;
    /// Go `GetServingURL`.
    fn get_serving_url(&self) -> String;
    /// Go `GetBackupURLs`.
    fn get_backup_urls(&self) -> Vec<String>;
    /// Go `GetOrCreateGRPCConn`.
    async fn get_or_create_grpc_conn(&self, url: &str) -> Result<Channel>;
    /// Go `RemoveClientConn`.
    fn remove_client_conn(&self, url: &str);
    /// Go `ScheduleCheckMemberChanged`.
    fn schedule_check_member_changed(&self);
    /// Go `CheckMemberChanged`.
    async fn check_member_changed(&self) -> Result<()>;
}

/// A second owned value of an error that several waiters report (Go shares
/// one `error` value among them).
pub(crate) fn dup_error(error: &Error) -> Error {
    match error {
        Error::Pd(error) => Error::Pd(error.clone()),
        Error::GrpcAPI(status) => Error::GrpcAPI(status.clone()),
        Error::ContextCanceled => Error::ContextCanceled,
        Error::StringError(message) => Error::StringError(message.clone()),
        error => Error::StringError(error.to_string()),
    }
}

/// Go `errs.IsNetworkError(rpcErr.Code()) || rpcErr.Code() == codes.Canceled`
/// for a stream-construction failure.
fn is_network_or_canceled(error: &Error) -> bool {
    match error {
        Error::GrpcAPI(status) => {
            crate::pd::errs::is_network_error(status.code())
                || status.code() == tonic::Code::Cancelled
        }
        Error::ContextCanceled => true,
        _ => false,
    }
}

/// Go `tlsutil.TrimHTTPPrefix`.
fn trim_http_prefix(url: &str) -> &str {
    url.strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .unwrap_or(url)
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
