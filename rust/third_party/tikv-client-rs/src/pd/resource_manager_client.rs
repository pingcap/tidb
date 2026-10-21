// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! The resource manager surface of PD's client package
//! (`resource_manager_client.go`) that the resource group controller needs:
//! the settings path prefixes, the get options, the token dispatcher over
//! the bidirectional `AcquireTokenBuckets` stream, and the
//! `ResourceGroupProvider` implementation for `PdRpcClient`.
//!
//! The token stream is opened lazily: grpc-go servers send stream headers
//! with their first response, so the stream call completes after the first
//! request is written. An open failure therefore fails the request that
//! first uses the stream, and the next request reconnects.

use std::sync::Arc;

use async_trait::async_trait;
use futures::future::BoxFuture;
use tokio::sync::{mpsc, oneshot};
use tonic::transport::Channel;

use super::client::PdRpcClient;
use super::metastorage;
use super::opt::MetaStorageOption;
use super::resource_group::controller::ResourceGroupProvider;
use super::retry::RetryClientTrait;
use crate::async_util::Cancellation;
use crate::proto::meta_storagepb;
use crate::proto::resource_manager::resource_manager_client::ResourceManagerClient;
use crate::proto::resource_manager::{
    ResourceGroup, TokenBucketResponse, TokenBucketsRequest, TokenBucketsResponse,
};
use crate::store::KvConnect;
use crate::{Error, Result};

const GROUP_SETTINGS_PATH_PREFIX: &str = "resource_group/settings";
const CONTROLLER_CONFIG_PATH_PREFIX: &str = "resource_group/controller";

/// Go `GroupSettingsPathPrefixBytes`: the prefix to watch or get the
/// resource groups of a keyspace.
pub fn group_settings_path_prefix_bytes(keyspace_id: u32) -> Vec<u8> {
    if keyspace_id == crate::request::NULL_KEYSPACE_ID {
        return GROUP_SETTINGS_PATH_PREFIX.as_bytes().to_vec();
    }
    format!("resource_group/keyspace/settings/{keyspace_id}").into_bytes()
}

/// Go `ControllerConfigPathPrefixBytes`: the key of the controller config.
pub const CONTROLLER_CONFIG_PATH_PREFIX_BYTES: &[u8] = CONTROLLER_CONFIG_PATH_PREFIX.as_bytes();

/// The options when getting resource groups.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GetResourceGroupOp {
    pub with_ru_stats: bool,
}

/// Configures `GetResourceGroupOp`.
pub type GetResourceGroupOption = fn(&mut GetResourceGroupOp);

/// Returns the resource groups with RU statistics.
pub fn with_ru_stats(op: &mut GetResourceGroupOp) {
    op.with_ru_stats = true;
}

pub(crate) fn apply_get_resource_group_options(
    opts: &[GetResourceGroupOption],
) -> GetResourceGroupOp {
    let mut op = GetResourceGroupOp::default();
    for opt in opts {
        opt(&mut op);
    }
    op
}

/// Go `constants.MaxRetryTimes`.
const MAX_RETRY_TIMES: usize = 6;

/// Opens a resource-manager token client on the current leader.
pub(crate) type ConnectTokenClient = Arc<
    dyn Fn() -> BoxFuture<'static, Result<(ResourceManagerClient<Channel>, String)>> + Send + Sync,
>;
/// The current leader URL, to notice a leader change.
pub(crate) type CurrentLeader = Arc<dyn Fn() -> BoxFuture<'static, Option<String>> + Send + Sync>;

struct TokenRequest {
    request: TokenBucketsRequest,
    done: oneshot::Sender<Result<Vec<TokenBucketResponse>>>,
}

/// Go `tokenDispatcher`: one task owns the token stream and serves token
/// requests one at a time.
pub(crate) struct TokenDispatcher {
    token_request_ch: mpsc::Sender<TokenRequest>,
}

enum TokenResponses {
    Pending(
        BoxFuture<
            'static,
            std::result::Result<
                tonic::Response<tonic::codec::Streaming<TokenBucketsResponse>>,
                tonic::Status,
            >,
        >,
    ),
    Ready(Box<tonic::codec::Streaming<TokenBucketsResponse>>),
}

/// Go `resourceManagerConnectionContext`.
struct TokenStream {
    leader: String,
    requests: futures::channel::mpsc::UnboundedSender<TokenBucketsRequest>,
    responses: TokenResponses,
}

impl TokenDispatcher {
    /// Go `createTokenDispatcher`.
    pub(crate) fn new(
        connect: ConnectTokenClient,
        current_leader: CurrentLeader,
        schedule_check_member_changed: Arc<dyn Fn() + Send + Sync>,
        cancellation: Cancellation,
    ) -> Self {
        let (token_request_ch, token_requests) = mpsc::channel(1);
        tokio::spawn(handle_resource_token_dispatcher(
            token_requests,
            connect,
            current_leader,
            schedule_check_member_changed,
            cancellation,
        ));
        TokenDispatcher { token_request_ch }
    }

    /// Go `AcquireTokenBuckets`: queues the request and waits for its
    /// response or for the client to close.
    pub(crate) async fn acquire_token_buckets(
        &self,
        request: TokenBucketsRequest,
        client_cancellation: &Cancellation,
    ) -> Result<Vec<TokenBucketResponse>> {
        let (done, response) = oneshot::channel();
        let request = TokenRequest { request, done };
        tokio::select! {
            biased;
            _ = client_cancellation.cancelled() => return Err(Error::ContextCanceled),
            sent = self.token_request_ch.send(request) => {
                if sent.is_err() {
                    return Err(Error::ContextCanceled);
                }
            }
        }
        tokio::select! {
            biased;
            _ = client_cancellation.cancelled() => Err(Error::ContextCanceled),
            result = response => match result {
                Ok(result) => result,
                Err(_) => Err(Error::ContextCanceled),
            },
        }
    }
}

async fn handle_resource_token_dispatcher(
    mut token_requests: mpsc::Receiver<TokenRequest>,
    connect: ConnectTokenClient,
    current_leader: CurrentLeader,
    schedule_check_member_changed: Arc<dyn Fn() + Send + Sync>,
    cancellation: Cancellation,
) {
    let mut connection = match try_resource_manager_connect(&connect, &cancellation).await {
        Ok(connection) => Some(connection),
        Err(error) => {
            log::warn!(error:% = error; "[resource_manager] get token stream error");
            None
        }
    };
    loop {
        // Fetch the request from the channel.
        let first_request = tokio::select! {
            biased;
            _ = cancellation.cancelled() => break,
            request = token_requests.recv() => match request {
                Some(request) => request,
                None => break,
            },
        };
        // Reconnect when there is no stream or the leader has changed.
        let leader = current_leader().await;
        let to_reconnect = match &connection {
            None => true,
            Some(stream) => leader.as_deref() != Some(stream.leader.as_str()),
        };
        if to_reconnect {
            connection = None;
            match try_resource_manager_connect(&connect, &cancellation).await {
                Ok(stream) => connection = Some(stream),
                Err(error) => log::error!(
                    error:% = error;
                    "[resource_manager] try to connect token leader failed"
                ),
            }
            log::info!("[resource_manager] token leader may change, try to reconnect the stream");
        }
        let Some(stream) = connection.as_mut() else {
            let _ = first_request.done.send(Err(Error::StringError(
                "failed to get the stream connection".to_owned(),
            )));
            schedule_check_member_changed();
            continue;
        };
        if let Err(error) = process_token_requests(stream, first_request).await {
            schedule_check_member_changed();
            connection = None;
            log::info!(error:% = error; "[resource_manager] token request error");
        }
    }
    log::info!("[resource manager] exit resource token dispatcher");
}

async fn process_token_requests(stream: &mut TokenStream, t: TokenRequest) -> Result<()> {
    if stream.requests.unbounded_send(t.request).is_err() {
        let message = "token stream is closed".to_owned();
        let _ = t.done.send(Err(Error::StringError(message.clone())));
        return Err(Error::StringError(message));
    }
    if let TokenResponses::Pending(call) = &mut stream.responses {
        match call.await {
            Ok(response) => {
                stream.responses = TokenResponses::Ready(Box::new(response.into_inner()))
            }
            Err(status) => {
                let _ = t.done.send(Err(Error::GrpcAPI(status.clone())));
                return Err(Error::GrpcAPI(status));
            }
        }
    }
    let TokenResponses::Ready(responses) = &mut stream.responses else {
        unreachable!("the token stream call completed above");
    };
    let resp = match responses.message().await {
        Ok(Some(resp)) => resp,
        Ok(None) => {
            let message = "token stream is closed".to_owned();
            let _ = t.done.send(Err(Error::StringError(message.clone())));
            return Err(Error::StringError(message));
        }
        Err(status) => {
            let _ = t.done.send(Err(Error::GrpcAPI(status.clone())));
            return Err(Error::GrpcAPI(status));
        }
    };
    if let Some(error) = resp.error {
        let message = format!("[resource_manager] {}", error.message);
        let _ = t.done.send(Err(Error::StringError(message.clone())));
        return Err(Error::StringError(message));
    }
    let _ = t.done.send(Ok(resp.responses));
    Ok(())
}

/// Go `tryResourceManagerConnect`.
async fn try_resource_manager_connect(
    connect: &ConnectTokenClient,
    cancellation: &Cancellation,
) -> Result<TokenStream> {
    let mut last_error = None;
    // Go retries a failed client lookup at once; its retry interval only
    // paces stream-open failures, which surface on first use here.
    for _ in 0..MAX_RETRY_TIMES {
        if cancellation.is_cancelled() {
            return Err(Error::ContextCanceled);
        }
        match connect().await {
            Ok((mut client, leader)) => {
                let (requests, request_stream) = futures::channel::mpsc::unbounded();
                let call =
                    Box::pin(async move { client.acquire_token_buckets(request_stream).await });
                return Ok(TokenStream {
                    leader,
                    requests,
                    responses: TokenResponses::Pending(call),
                });
            }
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error
        .unwrap_or_else(|| Error::StringError("failed to get the stream connection".to_owned())))
}

#[async_trait]
impl<KvC, P> ResourceGroupProvider for PdRpcClient<KvC, P>
where
    KvC: KvConnect + Send + Sync + 'static,
    P: RetryClientTrait + Send + Sync + 'static,
{
    async fn get_resource_group(
        &self,
        resource_group_name: &str,
        opts: &[GetResourceGroupOption],
    ) -> Result<Option<ResourceGroup>> {
        let op = apply_get_resource_group_options(opts);
        PdRpcClient::<KvC, P>::get_resource_group(self, resource_group_name, op.with_ru_stats).await
    }

    async fn list_resource_groups(
        &self,
        opts: &[GetResourceGroupOption],
    ) -> Result<Vec<ResourceGroup>> {
        let op = apply_get_resource_group_options(opts);
        PdRpcClient::<KvC, P>::list_resource_groups(self, op.with_ru_stats).await
    }

    async fn add_resource_group(&self, meta_group: ResourceGroup) -> Result<String> {
        PdRpcClient::<KvC, P>::add_resource_group(self, meta_group).await
    }

    async fn modify_resource_group(&self, meta_group: ResourceGroup) -> Result<String> {
        PdRpcClient::<KvC, P>::modify_resource_group(self, meta_group).await
    }

    async fn delete_resource_group(&self, resource_group_name: &str) -> Result<String> {
        PdRpcClient::<KvC, P>::delete_resource_group(self, resource_group_name).await
    }

    async fn acquire_token_buckets(
        &self,
        request: TokenBucketsRequest,
    ) -> Result<Vec<TokenBucketResponse>> {
        PdRpcClient::<KvC, P>::acquire_token_buckets(self, request).await
    }

    async fn load_resource_groups(&self) -> Result<(Vec<ResourceGroup>, i64)> {
        PdRpcClient::<KvC, P>::load_resource_groups(self).await
    }
}

#[async_trait]
impl<KvC, P> metastorage::Client for PdRpcClient<KvC, P>
where
    KvC: KvConnect + Send + Sync + 'static,
    P: RetryClientTrait + Send + Sync + 'static,
{
    async fn watch(
        &self,
        key: &[u8],
        opts: &[MetaStorageOption],
    ) -> Result<mpsc::Receiver<metastorage::WatchResponse>> {
        PdRpcClient::<KvC, P>::meta_storage_watch(self, key, opts).await
    }

    async fn get(
        &self,
        key: &[u8],
        opts: &[MetaStorageOption],
    ) -> Result<meta_storagepb::GetResponse> {
        PdRpcClient::<KvC, P>::meta_storage_get(self, key, opts).await
    }

    async fn put(
        &self,
        key: &[u8],
        value: &[u8],
        opts: &[MetaStorageOption],
    ) -> Result<meta_storagepb::PutResponse> {
        PdRpcClient::<KvC, P>::meta_storage_put(self, key, value, opts).await
    }
}
