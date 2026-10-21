// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! pd-client `servicediscovery/tso_service_discovery.go`: the client-side
//! discovery of the independent TSO service. In microservice (API) mode the
//! TSO client consumes it as its `ServiceDiscovery`: the serving URL is the
//! primary of the keyspace group the keyspace belongs to, found by asking a
//! TSO server (`FindGroupByKeyspaceID`), and the discovery re-checks it in
//! its own loop every `MemberUpdateInterval` or when asked.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use prost::Message;
use tokio::task::JoinHandle;
use tonic::transport::Channel;

use super::cluster::{grpc_error_text, Connection, TsoDiscoveryHooks};
use super::errs;
use super::service_discovery::{
    callee_id, ChannelCache, LeaderSwitchedCallback, MEMBER_UPDATE_INTERVAL,
};
use crate::async_util::Cancellation;
use crate::proto::{keyspacepb, meta_storagepb, pdpb, tsopb};
use crate::{Error, Result};

/// Go `tsoSvcDiscoveryFormat`: `/ms/<cluster-id>/tso/<group-id>/primary`.
fn tso_svc_discovery_key(cluster_id: u64, group_id: u32) -> String {
    format!("/ms/{cluster_id}/tso/{group_id:05}/primary")
}

/// Go `initRetryInterval`.
const INIT_RETRY_INTERVAL: Duration = Duration::from_secs(1);
/// Go `servicediscovery.UpdateMemberTimeout`.
const UPDATE_MEMBER_TIMEOUT: Duration = Duration::from_secs(1);
/// Go `constants.DefaultKeyspaceGroupID`.
const DEFAULT_KEYSPACE_GROUP_ID: u32 = 0;
/// Go `constants.TSOKeyspaceGroupIDKey`.
const TSO_KEYSPACE_GROUP_ID_KEY: &str = "tso_keyspace_group_id";
/// Go `errNoPrimaryURL`'s text.
const ERR_NO_PRIMARY_URL: &str = "no primary url found in the keyspace group";

fn err_no_primary_url() -> Error {
    Error::StringError(ERR_NO_PRIMARY_URL.to_owned())
}

fn is_no_primary_url(error: &Error) -> bool {
    matches!(error, Error::StringError(message) if message == ERR_NO_PRIMARY_URL)
}

/// Go `keyspaceGroupSvcDiscovery`: the keyspace group the keyspace belongs
/// to, its primary/secondary URLs, and the metadata revision that guards
/// against stale updates (`modRevision`).
#[derive(Default)]
struct KeyspaceGroupSvcDiscovery {
    group: Option<tsopb::KeyspaceGroup>,
    primary_url: String,
    secondary_urls: Vec<String>,
    urls: Vec<String>,
}

/// Go `tsoServerDiscovery`: the TSO servers, picked round robin.
#[derive(Default)]
struct TsoServerDiscovery {
    urls: Vec<String>,
    select_idx: usize,
}

/// Go `tsoServiceDiscovery`.
pub(crate) struct TsoServiceDiscovery {
    /// PD's discovery (Go `serviceDiscovery`): its leader, its connections,
    /// and the connection setup this discovery's own connections share.
    connection: Connection,
    cluster_id: u64,
    keyspace_id: AtomicU32,
    keyspace_meta: Option<keyspacepb::KeyspaceMeta>,
    default_discovery_key: String,
    tso_server_discovery: Mutex<TsoServerDiscovery>,
    keyspace_group_sd: RwLock<KeyspaceGroupSvcDiscovery>,
    /// Go `keyspaceGroupSD.modRevision`.
    mod_revision: AtomicU64,
    /// Go `clientConns`: this discovery's own connections.
    client_conns: ChannelCache,
    /// Go `tsoLeaderUpdatedCb`.
    tso_leader_updated_cb: RwLock<Option<LeaderSwitchedCallback>>,
    /// Go `checkMembershipCh` (capacity one: a stored permit).
    check_membership: tokio::sync::Notify,
    ctx: Cancellation,
    /// Go `wg`: the check member loop.
    workers: Mutex<Vec<JoinHandle<()>>>,
    /// Go `printFallbackLogOnce`.
    print_fallback_log_once: std::sync::Once,
    /// PD's `CheckMemberChanged`, as the owning client implements it.
    pd_hooks: Arc<RwLock<Option<TsoDiscoveryHooks>>>,
}

impl TsoServiceDiscovery {
    /// Go `NewTSOServiceDiscovery`. Starts with the default keyspace group;
    /// [`Self::init`] discovers the keyspace's.
    pub(crate) fn new(
        connection: Connection,
        cluster_id: u64,
        keyspace_id: u32,
        keyspace_meta: Option<keyspacepb::KeyspaceMeta>,
        pd_hooks: Arc<RwLock<Option<TsoDiscoveryHooks>>>,
    ) -> Arc<Self> {
        let discovery = Arc::new(Self {
            connection,
            cluster_id,
            keyspace_id: AtomicU32::new(keyspace_id),
            keyspace_meta,
            default_discovery_key: tso_svc_discovery_key(cluster_id, DEFAULT_KEYSPACE_GROUP_ID),
            tso_server_discovery: Mutex::default(),
            keyspace_group_sd: RwLock::default(),
            mod_revision: AtomicU64::new(0),
            client_conns: ChannelCache::default(),
            tso_leader_updated_cb: RwLock::new(None),
            check_membership: tokio::sync::Notify::new(),
            ctx: Cancellation::default(),
            workers: Mutex::new(Vec::new()),
            print_fallback_log_once: std::sync::Once::new(),
            pd_hooks,
        });
        log::info!(
            "cluster-id" = discovery.cluster_id,
            "keyspace-id" = keyspace_id,
            "keyspace-meta":% = crate::logutil::go_json_protobuf(discovery.keyspace_meta.as_ref()),
            "default-discovery-key" = discovery.default_discovery_key.as_str();
            "created tso service discovery"
        );
        discovery
    }

    /// The discovery's ctx (Go `c.ctx`).
    pub(crate) fn ctx(&self) -> &Cancellation {
        &self.ctx
    }

    fn max_retry_times(&self) -> isize {
        self.connection.options.max_retry_times
    }

    fn context_done(&self) -> impl std::future::Future<Output = Error> + Send + 'static {
        let ctx = self.ctx.clone();
        async move {
            ctx.cancelled().await;
            Error::ContextCanceled
        }
    }

    /// Go `Init`: the first member update, retried; on success, the check
    /// member loop.
    pub(crate) async fn init(self: &Arc<Self>) -> Result<()> {
        log::info!(
            "max-retry-times" = self.max_retry_times(),
            "retry-interval":% = crate::logutil::go_duration(INIT_RETRY_INTERVAL);
            "initializing tso service discovery"
        );
        if let Err(error) = super::backoff::retry(
            self.context_done(),
            self.max_retry_times(),
            INIT_RETRY_INTERVAL.as_nanos() as i64,
            || self.update_member(),
        )
        .await
        {
            log::error!(error:% = error; "failed to update member. initialization failed.");
            self.ctx.cancel();
            return Err(error);
        }
        self.workers
            .lock()
            .unwrap()
            .push(tokio::spawn(Arc::clone(self).start_check_member_loop()));
        Ok(())
    }

    /// Go `Close`.
    pub(crate) async fn close(&self) {
        log::info!("closing tso service discovery");
        self.ctx.cancel();
        let workers = std::mem::take(&mut *self.workers.lock().unwrap());
        for worker in workers {
            let _ = worker.await;
        }
        self.client_conns.close();
        log::info!("tso service discovery is closed");
    }

    /// Go `startCheckMemberLoop`.
    async fn start_check_member_loop(self: Arc<Self>) {
        let ctx = self.ctx.child();
        let mut ticker = tokio::time::interval_at(
            tokio::time::Instant::now() + MEMBER_UPDATE_INTERVAL,
            MEMBER_UPDATE_INTERVAL,
        );
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = self.check_membership.notified() => {}
                _ = ticker.tick() => {}
                _ = ctx.cancelled() => {
                    log::info!("[tso] exit check member loop");
                    return;
                }
            }
            // `MaxRetryTimes * initRetryInterval` is far less than
            // `MemberUpdateInterval`: a failover is found quickly without
            // updating too often.
            if let Err(error) = super::backoff::retry(
                self.context_done(),
                self.max_retry_times(),
                INIT_RETRY_INTERVAL.as_nanos() as i64,
                || self.update_member(),
            )
            .await
            {
                log::error!(error:% = error; "[tso] failed to update member");
            }
        }
    }

    /// Go `GetClusterID`.
    pub(crate) fn get_cluster_id(&self) -> u64 {
        self.cluster_id
    }

    /// Go `GetKeyspaceID`.
    pub(crate) fn get_keyspace_id(&self) -> u32 {
        self.keyspace_id.load(Ordering::Acquire)
    }

    /// Go `GetKeyspaceGroupID`: the default group until one is known.
    pub(crate) fn get_keyspace_group_id(&self) -> u32 {
        self.keyspace_group_sd
            .read()
            .unwrap()
            .group
            .as_ref()
            .map_or(DEFAULT_KEYSPACE_GROUP_ID, |group| group.id)
    }

    /// Go `GetServiceURLs`: the group's primary and secondaries.
    pub(crate) fn get_service_urls(&self) -> Vec<String> {
        self.keyspace_group_sd.read().unwrap().urls.clone()
    }

    /// Go `GetServingURL` (`getPrimaryURL`).
    pub(crate) fn get_serving_url(&self) -> String {
        self.keyspace_group_sd.read().unwrap().primary_url.clone()
    }

    /// Go `GetBackupURLs` (`getSecondaryURLs`).
    pub(crate) fn get_backup_urls(&self) -> Vec<String> {
        self.keyspace_group_sd
            .read()
            .unwrap()
            .secondary_urls
            .clone()
    }

    /// Go `GetOrCreateGRPCConn`.
    pub(crate) async fn get_or_create_grpc_conn(
        &self,
        url: &str,
    ) -> std::result::Result<Channel, tonic::Status> {
        self.client_conns
            .get_or_connect(url, || self.connection.new_channel(url))
            .await
    }

    /// Go `RemoveClientConn`.
    pub(crate) fn remove_client_conn(&self, url: &str) {
        self.client_conns.remove(url);
    }

    /// Go `ScheduleCheckMemberChanged`.
    pub(crate) fn schedule_check_member_changed(&self) {
        self.check_membership.notify_one();
    }

    /// Go `CheckMemberChanged`: PD's members, then this discovery's.
    pub(crate) async fn check_member_changed(&self) -> Result<()> {
        let hooks = self.pd_hooks.read().unwrap().clone();
        if let Some(hooks) = hooks {
            if let Err(error) = (hooks.pd_check_member_changed)().await {
                log::warn!(error:% = error; "[tso] failed to check member changed");
            }
        }
        if let Err(error) =
            super::backoff::with_config(self.context_done(), || self.update_member()).await
        {
            log::error!(error:% = error; "[tso] failed to update member");
            return Err(error);
        }
        Ok(())
    }

    /// Go `ExecAndAddLeaderSwitchedCallback`: one callback, run now with the
    /// current primary.
    pub(crate) fn exec_and_add_leader_switched_callback(&self, callback: LeaderSwitchedCallback) {
        let url = self.get_serving_url();
        if !url.is_empty() {
            if let Err(error) = callback(&url) {
                log::error!(
                    url = url.as_str(),
                    error:% = error;
                    "[tso] failed to call back when tso global service url update"
                );
            }
        }
        *self.tso_leader_updated_cb.write().unwrap() = Some(callback);
    }

    /// Go `afterPrimarySwitched`.
    fn after_primary_switched(&self, old_primary: &str, new_primary: &str) -> Result<()> {
        let callback = self.tso_leader_updated_cb.read().unwrap().clone();
        if let Some(callback) = callback {
            callback(new_primary)?;
        }
        log::info!(
            "new-primary" = new_primary,
            "old-primary" = old_primary;
            "[tso] switch primary"
        );
        Ok(())
    }

    /// Go `hasKeyspaceGroupIDConfig`: the keyspace's assigned group when it
    /// is a non-default one (which forbids falling back to group 0).
    fn has_keyspace_group_id_config(&self) -> (u32, bool) {
        let Some(group_id) = self
            .keyspace_meta
            .as_ref()
            .and_then(|meta| meta.config.get(TSO_KEYSPACE_GROUP_ID_KEY))
            .and_then(|id| id.parse::<u32>().ok())
        else {
            return (0, false);
        };
        if group_id == DEFAULT_KEYSPACE_GROUP_ID {
            return (0, false);
        }
        (group_id, true)
    }

    /// Go `checkAndHandleFallbackInMicroserviceMode`.
    async fn check_and_handle_fallback_in_microservice_mode(&self, keyspace_id: u32) -> Result<()> {
        let cluster_info = match self
            .get_cluster_info(
                &self.connection.membership.leader_url(),
                self.connection.options.timeout,
            )
            .await
        {
            Ok(cluster_info) => cluster_info,
            Err(error) => {
                log::warn!(
                    "keyspace-id" = keyspace_id,
                    error:% = error;
                    "[tso] failed to get cluster info to check service mode"
                );
                return Err(error);
            }
        };
        let service_mode = cluster_info
            .service_modes
            .first()
            .copied()
            .and_then(|mode| pdpb::ServiceMode::try_from(mode).ok());
        if service_mode == Some(pdpb::ServiceMode::ApiSvcMode) {
            let (group_id, should_block_fallback) = self.has_keyspace_group_id_config();
            if should_block_fallback {
                log::warn!(
                    "keyspace-id" = keyspace_id,
                    "keyspace-group-id" = group_id,
                    "service-mode" = "API_SVC_MODE";
                    "[tso] in microservice mode but no TSO server available - cannot fallback to group 0"
                );
                return Err(Error::StringError(
                    "no TSO microservice available in microservice mode".to_owned(),
                ));
            }
            log::info!(
                "keyspace-id" = keyspace_id,
                "keyspace-meta":% = crate::logutil::go_json_protobuf(self.keyspace_meta.as_ref()),
                "service-mode" = "API_SVC_MODE";
                "[tso] keyspace has no tso_keyspace_group_id config or configured as 0, allowing fallback to group 0"
            );
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn update_member_for_test(&self) -> Result<()> {
        self.update_member().await
    }

    /// Go `updateMember`.
    async fn update_member(&self) -> Result<()> {
        let tso_server_url = match self.get_tso_server().await {
            Ok(url) => url,
            Err(error) => {
                log::error!(error:% = error; "[tso] failed to get tso server");
                return Err(error);
            }
        };
        let result = self.update_member_inner(&tso_server_url).await;
        // A server outside the group knows the group but not its primary:
        // ask one of the group's members instead.
        if let Err(error) = &result {
            if is_no_primary_url(error) {
                let urls = self.get_backup_urls();
                if urls.is_empty() {
                    return result;
                }
                use rand::seq::SliceRandom;
                let url = urls
                    .choose(&mut rand::thread_rng())
                    .cloned()
                    .unwrap_or_default();
                return self.update_member_inner(&url).await;
            }
        }
        result
    }

    /// Go `updateMemberInner`.
    async fn update_member_inner(&self, tso_server_url: &str) -> Result<()> {
        let keyspace_id = self.get_keyspace_id();
        let cur_mod_revision = self.mod_revision.load(Ordering::Acquire);
        let (keyspace_group, mod_revision) = if !tso_server_url.is_empty() {
            match self
                .find_group_by_keyspace_id(
                    keyspace_id,
                    tso_server_url,
                    UPDATE_MEMBER_TIMEOUT,
                    self.mod_revision.load(Ordering::Acquire),
                )
                .await
            {
                Ok(found) => found,
                Err(error) => {
                    log::error!(
                        "keyspace-id-in-request" = keyspace_id,
                        "tso-server-url" = tso_server_url,
                        "response-mod-revision" = 0_u64,
                        "current-mod-revision" = cur_mod_revision,
                        error:% = error;
                        "[tso] failed to find the keyspace group"
                    );
                    return Err(error);
                }
            }
        } else {
            // A keyspace assigned to a group must not fall back to the
            // legacy default group even when no TSO server is listed.
            self.check_and_handle_fallback_in_microservice_mode(keyspace_id)
                .await?;
            self.print_fallback_log_once.call_once(|| {
                log::warn!(
                    "keyspace-id-in-request" = keyspace_id,
                    "tso-server-url" = tso_server_url,
                    "discovery-key" = self.default_discovery_key.as_str();
                    "[tso] no tso server URL found, fallback to the legacy path to discover from etcd directly"
                );
            });
            let urls = self.discover_with_legacy_path().await?;
            if urls.is_empty() {
                return Err(Error::StringError("no tso server url found".to_owned()));
            }
            let members = urls
                .into_iter()
                .enumerate()
                .map(|(i, address)| tsopb::KeyspaceGroupMember {
                    address,
                    is_primary: i == 0,
                })
                .collect();
            (
                tsopb::KeyspaceGroup {
                    id: DEFAULT_KEYSPACE_GROUP_ID,
                    members,
                    ..Default::default()
                },
                0,
            )
        };

        let old_group_id = self.get_keyspace_group_id();
        if old_group_id != keyspace_group.id {
            log::info!(
                "keyspace-id" = keyspace_id,
                "new-keyspace-group-id" = keyspace_group.id,
                "old-keyspace-group-id" = old_group_id;
                "[tso] the keyspace group changed"
            );
        }

        let mut primary_url = String::new();
        let mut secondary_urls = Vec::new();
        let mut urls = Vec::with_capacity(keyspace_group.members.len());
        for member in &keyspace_group.members {
            urls.push(member.address.clone());
            if member.is_primary {
                primary_url.clone_from(&member.address);
            } else {
                secondary_urls.push(member.address.clone());
            }
        }

        // Connect to a new primary outside the discovery's critical section.
        if !primary_url.is_empty() && !primary_url.eq_ignore_ascii_case(&self.get_serving_url()) {
            if let Err(error) = self.get_or_create_grpc_conn(&primary_url).await {
                let error = Error::from(error);
                log::warn!(
                    "keyspace-id-in-request" = keyspace_id,
                    "tso-server-url" = tso_server_url,
                    "next-primary" = primary_url.as_str(),
                    error:% = error;
                    "[tso] failed to connect the next primary"
                );
                return Err(error);
            }
        }

        let (old_primary, primary_switched, meta_changed) = self.update_keyspace_group_sd(
            keyspace_group.clone(),
            &primary_url,
            secondary_urls,
            urls,
            mod_revision,
        );

        if primary_switched {
            log::info!(
                "keyspace-id-in-request" = keyspace_id,
                "tso-server-url" = tso_server_url,
                "keyspace-group-service":% = crate::error::protobuf_text(&keyspace_group);
                "[tso] updated keyspace group service discovery info"
            );
            self.after_primary_switched(&old_primary, &primary_url)?;
        }

        // The group and secondaries are updated even without a primary.
        if primary_url.is_empty() {
            return Err(err_no_primary_url());
        }
        if !meta_changed {
            log::warn!(
                modRevision = mod_revision;
                "[tso] failed to update keyspace group, met stale keyspace group info"
            );
            return Err(Error::StringError(format!(
                "keyspace group modRevision is stale, current modRevision: {}, new modRevision: {}",
                self.mod_revision.load(Ordering::Acquire),
                mod_revision
            )));
        }
        Ok(())
    }

    /// Go `keyspaceGroupSvcDiscovery.update`: (old primary, primary
    /// switched, meta changed). An older revision changes nothing; an empty
    /// primary keeps the current one.
    fn update_keyspace_group_sd(
        &self,
        keyspace_group: tsopb::KeyspaceGroup,
        new_primary_url: &str,
        secondary_urls: Vec<String>,
        urls: Vec<String>,
        mod_revision: u64,
    ) -> (String, bool, bool) {
        let mut sd = self.keyspace_group_sd.write().unwrap();
        if self.mod_revision.load(Ordering::Acquire) > mod_revision {
            return (String::new(), false, false);
        }
        let old_primary_url = sd.primary_url.clone();
        let mut primary_switched = false;
        if !new_primary_url.is_empty() {
            primary_switched = !old_primary_url.eq_ignore_ascii_case(new_primary_url);
            sd.primary_url = new_primary_url.to_owned();
        }
        sd.secondary_urls = secondary_urls;
        sd.group = Some(keyspace_group);
        sd.urls = urls;
        self.mod_revision.store(mod_revision, Ordering::Release);
        (old_primary_url, primary_switched, true)
    }

    /// Go `findGroupByKeyspaceID`.
    async fn find_group_by_keyspace_id(
        &self,
        keyspace_id: u32,
        tso_srv_url: &str,
        timeout: Duration,
        mod_revision: u64,
    ) -> Result<(tsopb::KeyspaceGroup, u64)> {
        let target = target(tso_srv_url);
        let find_error = |text: String| -> Error {
            errs::ERR_CLIENT_FIND_GROUP_BY_KEYSPACE_ID
                .wrap(Some(Arc::new(Error::StringError(text))))
                .expect("a cause is given")
                .gen_with_stack_by_cause()
                .into()
        };
        let channel = self.get_or_create_grpc_conn(tso_srv_url).await?;
        let mut request = tonic::Request::new(tsopb::FindGroupByKeyspaceIdRequest {
            header: Some(tsopb::RequestHeader {
                cluster_id: self.cluster_id,
                keyspace: Some(tsopb::request_header::Keyspace::KeyspaceId(keyspace_id)),
                keyspace_group_id: DEFAULT_KEYSPACE_GROUP_ID,
                callee_id: callee_id(tso_srv_url),
                ..Default::default()
            }),
            keyspace: Some(
                tsopb::find_group_by_keyspace_id_request::Keyspace::KeyspaceId(keyspace_id),
            ),
            mod_revision,
        });
        request.set_timeout(timeout);
        let mut client = tsopb::tso_client::TsoClient::new(channel);
        let response = tokio::select! {
            response = tokio::time::timeout(
                timeout,
                client.find_group_by_keyspace_id(request),
            ) => response,
            _ = self.ctx.cancelled() => return Err(Error::ContextCanceled),
        };
        // tonic exposes no connectivity state for Go's `status:` part.
        let response = match response {
            Ok(Ok(response)) => response.into_inner(),
            Ok(Err(status)) => {
                return Err(find_error(format!(
                    "error:{} target:{target}",
                    grpc_error_text(&status)
                )))
            }
            Err(_) => {
                return Err(find_error(format!(
                    "error:context deadline exceeded target:{target}"
                )))
            }
        };
        if let Some(error) = response.header.as_ref().and_then(|h| h.error.as_ref()) {
            if error.message.contains(errs::MISMATCH_CALLEE_ID_ERR) {
                // A stale connection to a reused address: recreate it.
                self.remove_client_conn(tso_srv_url);
            }
            return Err(find_error(format!(
                "error:{} target:{target}",
                crate::error::protobuf_text(error).trim_end()
            )));
        }
        let Some(keyspace_group) = response.keyspace_group else {
            return Err(find_error(format!(
                "error:no keyspace group found target:{target}"
            )));
        };
        if response.mod_revision < mod_revision {
            return Err(find_error(format!(
                "error:response mod revision less than the given mod revision target:{target} response mod revision:{} current mod revision:{mod_revision}",
                response.mod_revision
            )));
        }
        Ok((keyspace_group, response.mod_revision))
    }

    /// Go `getTSOServer`: a TSO server PD lists, round robin; empty when PD
    /// lists none (an older PD: fall back to the legacy path).
    async fn get_tso_server(&self) -> Result<String> {
        let urls = self.discover_microservice_tso().await?;
        if urls.is_empty() {
            return Ok(String::new());
        }
        let mut t = self.tso_server_discovery.lock().unwrap();
        if t.urls.is_empty() || !equal_without_order(&t.urls, &urls) {
            log::info!("sorted-urls":serde = urls; "update tso server URLs");
            t.urls = urls;
            t.select_idx = 0;
        }
        let url = t.urls[t.select_idx].clone();
        t.select_idx = (t.select_idx + 1) % t.urls.len();
        Ok(url)
    }

    /// Go `serviceDiscovery.discoverMicroservice(tsoService)`: the TSO
    /// servers PD's leader lists in its cluster info.
    async fn discover_microservice_tso(&self) -> Result<Vec<String>> {
        let leader_url = self.connection.membership.leader_url();
        if leader_url.is_empty() {
            return Err(Error::StringError("failed to get leader url".to_owned()));
        }
        match self
            .get_cluster_info(&leader_url, self.connection.options.timeout)
            .await
        {
            Ok(cluster_info) => Ok(cluster_info.tso_urls),
            Err(error) => {
                log::error!(
                    "leader-url" = leader_url.as_str(),
                    error:% = error;
                    "[pd] failed to get cluster info"
                );
                Err(error)
            }
        }
    }

    /// Go `serviceDiscovery.getClusterInfo`.
    async fn get_cluster_info(
        &self,
        url: &str,
        timeout: Duration,
    ) -> Result<pdpb::GetClusterInfoResponse> {
        let metrics = super::metrics::global_metrics();
        let start = std::time::Instant::now();
        let target = target(url);
        let cluster_info_error = |text: String| -> Error {
            errs::ERR_CLIENT_GET_CLUSTER_INFO
                .wrap(Some(Arc::new(Error::StringError(text))))
                .expect("a cause is given")
                .gen_with_stack_by_cause()
                .into()
        };
        let channel = self.connection.get_or_create_channel(url).await?;
        let mut request = tonic::Request::new(pdpb::GetClusterInfoRequest::default());
        request.set_timeout(timeout);
        let response = tokio::time::timeout(
            timeout,
            pdpb::pd_client::PdClient::new(channel).get_cluster_info(request),
        )
        .await;
        metrics
            .internal_cmd_duration_get_cluster_info
            .observe(start.elapsed().as_secs_f64());
        let failed = || {
            metrics
                .internal_cmd_failed_duration_get_cluster_info
                .observe(start.elapsed().as_secs_f64())
        };
        match response {
            Ok(Ok(response)) => {
                let cluster_info = response.into_inner();
                if let Some(error) = cluster_info.header.as_ref().and_then(|h| h.error.as_ref()) {
                    failed();
                    return Err(cluster_info_error(format!(
                        "error:{} target:{target}",
                        crate::error::protobuf_text(error).trim_end()
                    )));
                }
                Ok(cluster_info)
            }
            Ok(Err(status)) => {
                failed();
                Err(cluster_info_error(format!(
                    "error:{} target:{target}",
                    grpc_error_text(&status)
                )))
            }
            Err(_) => Err(cluster_info_error(format!(
                "error:context deadline exceeded target:{target}"
            ))),
        }
    }

    /// Go `discoverWithLegacyPath`: the default group's primary, from PD's
    /// meta storage.
    async fn discover_with_legacy_path(&self) -> Result<Vec<String>> {
        let response = match self.meta_storage_get().await {
            Ok(response) => response,
            Err(error) => {
                log::error!(
                    "discovery-key" = self.default_discovery_key.as_str(),
                    error:% = error;
                    "[tso] failed to get the keyspace serving endpoint"
                );
                return Err(error);
            }
        };
        if response.kvs.is_empty() {
            log::error!(
                "primary-key" = self.default_discovery_key.as_str();
                "[tso] didn't find the keyspace serving endpoint"
            );
            return Err(errs::ERR_CLIENT_GET_SERVING_ENDPOINT.error().into());
        } else if response.count > 1 {
            return Err(Error::StringError(format!(
                "[{}]get invalid value response {:?}, must only one",
                errs::ERR_CLIENT_GET_MULTI_RESPONSE
                    .code()
                    .unwrap_or_default(),
                response.kvs
            )));
        }
        let primary =
            tsopb::Participant::decode(response.kvs[0].value.as_slice()).map_err(|error| {
                Error::from(
                    errs::ERR_CLIENT_PROTO_UNMARSHAL
                        .wrap(Some(Arc::new(Error::StringError(error.to_string()))))
                        .expect("a cause is given")
                        .gen_with_stack_by_cause(),
                )
            })?;
        if primary.listen_urls.is_empty() {
            log::error!(
                "discovery-key" = self.default_discovery_key.as_str();
                "[tso] the keyspace serving endpoint list is empty"
            );
            return Err(errs::ERR_CLIENT_GET_SERVING_ENDPOINT.error().into());
        }
        Ok(primary.listen_urls)
    }

    /// Go `metacli.Get(ctx, key)` on PD's leader.
    async fn meta_storage_get(&self) -> Result<meta_storagepb::GetResponse> {
        let leader_url = self.connection.membership.leader_url();
        let channel = self.connection.get_or_create_channel(&leader_url).await?;
        let mut request = tonic::Request::new(meta_storagepb::GetRequest {
            header: Some(meta_storagepb::RequestHeader {
                cluster_id: self.cluster_id,
                ..Default::default()
            }),
            key: self.default_discovery_key.clone().into_bytes(),
            ..Default::default()
        });
        request.set_timeout(self.connection.options.timeout);
        let response = meta_storagepb::meta_storage_client::MetaStorageClient::new(channel)
            .get(request)
            .await?
            .into_inner();
        if let Some(error) = response.header.as_ref().and_then(|h| h.error.as_ref()) {
            return Err(Error::StringError(
                super::meta_storage_client::meta_storage_error_text(error),
            ));
        }
        Ok(response)
    }
}

/// grpc-go's `cc.Target()` for a URL: the address without the scheme.
fn target(url: &str) -> &str {
    url.strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .unwrap_or(url)
}

/// Go `EqualWithoutOrder`.
fn equal_without_order(a: &[String], b: &[String]) -> bool {
    a.len() == b.len() && a.iter().all(|item| b.contains(item))
}

/// The TSO client's view of the discovery.
#[async_trait]
impl super::tso::ServiceDiscovery for TsoServiceDiscovery {
    fn get_cluster_id(&self) -> u64 {
        TsoServiceDiscovery::get_cluster_id(self)
    }

    fn get_keyspace_id(&self) -> u32 {
        TsoServiceDiscovery::get_keyspace_id(self)
    }

    fn get_keyspace_group_id(&self) -> u32 {
        TsoServiceDiscovery::get_keyspace_group_id(self)
    }

    fn get_service_urls(&self) -> Vec<String> {
        TsoServiceDiscovery::get_service_urls(self)
    }

    fn get_serving_url(&self) -> String {
        TsoServiceDiscovery::get_serving_url(self)
    }

    fn get_backup_urls(&self) -> Vec<String> {
        TsoServiceDiscovery::get_backup_urls(self)
    }

    async fn get_or_create_grpc_conn(&self, url: &str) -> Result<Channel> {
        Ok(TsoServiceDiscovery::get_or_create_grpc_conn(self, url).await?)
    }

    fn remove_client_conn(&self, url: &str) {
        TsoServiceDiscovery::remove_client_conn(self, url);
    }

    fn schedule_check_member_changed(&self) {
        TsoServiceDiscovery::schedule_check_member_changed(self);
    }

    async fn check_member_changed(&self) -> Result<()> {
        TsoServiceDiscovery::check_member_changed(self).await
    }
}
