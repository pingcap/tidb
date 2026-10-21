// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! pd-client `servicediscovery/router_service_discovery.go`: the client-side
//! discovery of the independent router service, whose nodes register under
//! `/ms/{cluster_id}/router/registry/` in PD's meta storage. Requests that
//! allow router-service handling go to a healthy node, the rest to PD.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::task::JoinHandle;
use tonic::transport::Channel;

use super::errs;
use crate::async_util::Cancellation;
use crate::proto::meta_storagepb;
use crate::{Error, Result};

/// Go `servicePathFormat`.
fn service_path(cluster_id: u64) -> String {
    format!("/ms/{cluster_id}/router/registry/")
}

/// Go `initRetryInterval`.
const INIT_RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// A meta-storage prefix read (`metastorage.Client.Get(key, WithPrefix())`).
pub(crate) type MetaPrefixGet =
    Arc<dyn Fn(Vec<u8>) -> BoxFuture<'static, Result<meta_storagepb::GetResponse>> + Send + Sync>;
/// Go `GetOrCreateGRPCConn`.
pub(crate) type GetOrCreateConn = Arc<
    dyn Fn(String) -> BoxFuture<'static, std::result::Result<Channel, tonic::Status>> + Send + Sync,
>;
/// Go `RemoveClientConn` (the shared connection cache's).
pub(crate) type RemoveConn = Arc<dyn Fn(&str) + Send + Sync>;

/// One router node: Go `serviceClient` (never the leader), whose
/// availability is its network health.
pub(crate) struct RouterNode {
    pub(crate) url: String,
    pub(crate) channel: Channel,
    network_failure: AtomicBool,
}

impl RouterNode {
    /// Go `serviceClient.Available`.
    pub(crate) fn available(&self) -> bool {
        !self.network_failure.load(Ordering::Acquire)
    }
}

/// Go `serviceBalancer` over the nodes, round robin among available ones.
#[derive(Default)]
struct Balancer {
    nodes: Vec<Arc<RouterNode>>,
    next: usize,
}

impl Balancer {
    /// Go `set`: an empty list keeps the previous nodes.
    fn set(&mut self, nodes: Vec<Arc<RouterNode>>) {
        if nodes.is_empty() {
            return;
        }
        self.nodes = nodes;
        self.next = 0;
    }

    /// Go `get`: the next available node, if any.
    fn get(&mut self) -> Option<Arc<RouterNode>> {
        for _ in 0..self.nodes.len() {
            let node = Arc::clone(&self.nodes[self.next % self.nodes.len()]);
            self.next = (self.next + 1) % self.nodes.len();
            if node.available() {
                return Some(node);
            }
        }
        None
    }

    fn clean(&mut self) {
        self.nodes.clear();
        self.next = 0;
    }
}

/// Go `routerServiceDiscovery`.
pub(crate) struct RouterServiceDiscovery {
    cluster_id: u64,
    default_discovery_key: String,
    tls: bool,
    max_retry_times: isize,
    meta_get: MetaPrefixGet,
    get_or_create_conn: GetOrCreateConn,
    remove_conn: RemoveConn,
    /// Go `sortedUrls` (`None` is Go's nil before the first update).
    sorted_urls: RwLock<Option<Vec<String>>>,
    nodes: Mutex<BTreeMap<String, Arc<RouterNode>>>,
    balancer: Mutex<Balancer>,
    check_membership: tokio::sync::Notify,
    /// Go `ctx`; Go `parentCtx` is the owner's.
    ctx: Cancellation,
    workers: tokio::sync::Mutex<Vec<JoinHandle<()>>>,
}

impl RouterServiceDiscovery {
    /// Go `NewRouterServiceDiscovery` (which also runs `Init`).
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new(
        parent: &Cancellation,
        cluster_id: u64,
        keyspace_id: u32,
        tls: bool,
        max_retry_times: isize,
        meta_get: MetaPrefixGet,
        get_or_create_conn: GetOrCreateConn,
        remove_conn: RemoveConn,
    ) -> Arc<Self> {
        let discovery = Arc::new(Self {
            cluster_id,
            default_discovery_key: service_path(cluster_id),
            tls,
            max_retry_times,
            meta_get,
            get_or_create_conn,
            remove_conn,
            sorted_urls: RwLock::new(None),
            nodes: Mutex::new(BTreeMap::new()),
            balancer: Mutex::new(Balancer::default()),
            check_membership: tokio::sync::Notify::new(),
            ctx: parent.child(),
            workers: tokio::sync::Mutex::new(Vec::new()),
        });
        log::info!(
            "cluster-id" = discovery.cluster_id,
            "keyspace-id" = keyspace_id,
            "default-discovery-key" = discovery.default_discovery_key.as_str();
            "[router service] created router service discovery"
        );
        discovery.init().await;
        discovery
    }

    /// Go `Init`.
    async fn init(self: &Arc<Self>) {
        log::info!(
            "max-retry-times" = self.max_retry_times,
            "retry-interval":% = crate::logutil::go_duration(INIT_RETRY_INTERVAL);
            "[router service] initializing router service discovery"
        );
        if let Err(error) = self.check_member_changed().await {
            log::warn!(
                error:% = error;
                "[router service] failed to initialize router service discovery"
            );
        }
        let mut workers = self.workers.lock().await;
        workers.push(tokio::spawn(Arc::clone(self).start_check_member_loop()));
        workers.push(tokio::spawn(Arc::clone(self).node_health_check_loop()));
    }

    /// Go `GetServiceURLs`.
    pub(crate) fn get_service_urls(&self) -> Vec<String> {
        self.sorted_urls.read().unwrap().clone().unwrap_or_default()
    }

    /// Go `GetServiceClient`: a healthy router node, if any.
    pub(crate) fn get_service_client(&self) -> Option<Arc<RouterNode>> {
        self.balancer.lock().unwrap().get()
    }

    /// Go `ScheduleCheckMemberChanged`.
    pub(crate) fn schedule_check_member_changed(&self) {
        if self.ctx.is_cancelled() {
            log::info!("[router service] service discovery is shutting down");
            return;
        }
        self.check_membership.notify_one();
    }

    /// Go `CheckMemberChanged`: `updateMember` under `retry.WithConfig`.
    pub(crate) async fn check_member_changed(&self) -> Result<()> {
        let ctx = self.ctx.clone();
        let result = super::backoff::with_config(
            async move {
                ctx.cancelled().await;
                Error::ContextCanceled
            },
            || self.update_member(),
        )
        .await;
        if let Err(error) = &result {
            log::warn!(error:% = error; "[router service] failed to update member");
        }
        result
    }

    /// Go `updateMember`.
    async fn update_member(&self) -> Result<()> {
        let sorted_addrs = match self.discovery_ms_addrs().await {
            Ok(addrs) => addrs,
            Err(error) => {
                // Go stops on a meta storage client error instead of retrying;
                // `DiscoveryMSAddrs` reports every read failure as one.
                log::warn!(
                    error:% = error;
                    "[router service] meta storage client error, stopping router service discovery"
                );
                return Ok(());
            }
        };
        if self.sorted_urls.read().unwrap().as_ref() == Some(&sorted_addrs) {
            return Ok(());
        }
        self.update_urls(sorted_addrs.clone());
        self.update_nodes(&sorted_addrs).await;
        Ok(())
    }

    /// Go `metastorage.DiscoveryMSAddrs`.
    async fn discovery_ms_addrs(&self) -> Result<Vec<String>> {
        let response = (self.meta_get)(self.default_discovery_key.clone().into_bytes())
            .await
            .map_err(|error| {
                Error::from(
                    errs::ERR_CLIENT_GET_META_STORAGE_CLIENT
                        .wrap(Some(Arc::new(error)))
                        .expect("a cause is given")
                        .gen_with_stack_by_cause(),
                )
            })?;
        if let Some(error) = response
            .header
            .as_ref()
            .and_then(|header| header.error.as_ref())
        {
            return Err(errs::ERR_CLIENT_GET_PROTO_CLIENT
                .wrap(Some(Arc::new(Error::StringError(error.message.clone()))))
                .expect("a cause is given")
                .gen_with_stack_by_cause()
                .into());
        }
        let mut sorted_addrs = Vec::with_capacity(response.kvs.len());
        for kv in response.kvs {
            match serde_json::from_slice::<ServiceRegistryEntry>(&kv.value) {
                Ok(entry) => sorted_addrs.push(entry.service_addr),
                Err(error) => {
                    log::warn!(error:% = error; "json unmarshal the service registry entry failed");
                    log::warn!(
                        "service-key" = self.default_discovery_key.as_str(),
                        key:% = String::from_utf8_lossy(&kv.key),
                        error:% = error;
                        "try to deserialize service registry entry failed"
                    );
                }
            }
        }
        sorted_addrs.sort();
        Ok(sorted_addrs)
    }

    /// Go `updateURLs`.
    fn update_urls(&self, sorted_addrs: Vec<String>) {
        log::info!(
            "new-sorted-urls":serde = sorted_addrs;
            "[router service] update member sorted urls"
        );
        *self.sorted_urls.write().unwrap() = Some(sorted_addrs);
    }

    /// Go `updateNodes`.
    async fn update_nodes(&self, urls: &[String]) {
        let mut url_set = Vec::with_capacity(urls.len());
        for url in urls {
            if url.is_empty() {
                continue;
            }
            let new_url =
                super::service_discovery::pick_service_url(std::slice::from_ref(url), self.tls)
                    .unwrap_or_else(|| url.clone());
            url_set.push(new_url.clone());
            if self.nodes.lock().unwrap().contains_key(&new_url) {
                continue;
            }
            match (self.get_or_create_conn)(new_url.clone()).await {
                Ok(channel) => {
                    self.nodes
                        .lock()
                        .unwrap()
                        .entry(new_url.clone())
                        .or_insert_with(|| {
                            Arc::new(RouterNode {
                                url: new_url,
                                channel,
                                network_failure: AtomicBool::new(false),
                            })
                        });
                }
                Err(error) => log::warn!(
                    url = new_url.as_str(),
                    error:% = Error::from(error);
                    "[router service] failed to connect router service"
                ),
            }
        }
        let clients = {
            let mut nodes = self.nodes.lock().unwrap();
            let stale = nodes
                .keys()
                .filter(|url| !url_set.contains(url))
                .cloned()
                .collect::<Vec<_>>();
            for url in stale {
                nodes.remove(&url);
                (self.remove_conn)(&url);
                log::info!(url = url.as_str(); "[router service] removed stale node");
            }
            nodes.values().cloned().collect::<Vec<_>>()
        };
        log::info!(
            urls:serde = urls,
            "clients-length" = clients.len();
            "[router service] updating nodes succeeded"
        );
        self.balancer.lock().unwrap().set(clients);
    }

    /// Go `startCheckMemberLoop`.
    async fn start_check_member_loop(self: Arc<Self>) {
        let mut ticker = tokio::time::interval_at(
            tokio::time::Instant::now() + super::service_discovery::MEMBER_UPDATE_INTERVAL,
            super::service_discovery::MEMBER_UPDATE_INTERVAL,
        );
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = self.check_membership.notified() => {}
                _ = ticker.tick() => {}
                _ = self.ctx.cancelled() => {
                    log::info!("[router service] exit check member loop");
                    return;
                }
            }
            let _ = self.check_member_changed().await;
        }
    }

    /// Go `nodeHealthCheckLoop`.
    async fn node_health_check_loop(self: Arc<Self>) {
        let interval = super::region_service::HEALTH_CHECK_INTERVAL;
        let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = self.ctx.cancelled() => {
                    log::info!("[router service] exit health check member loop");
                    return;
                }
                _ = ticker.tick() => self.check_node_health().await,
            }
        }
    }

    /// Go `checkNodeHealth`: each node's network health, probed with a third
    /// of the check interval.
    async fn check_node_health(&self) {
        let nodes = self
            .nodes
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for node in nodes {
            let serving = super::service_discovery::health_check(
                node.channel.clone(),
                super::region_service::HEALTH_CHECK_INTERVAL / 3,
            )
            .await
            .is_ok_and(|status| status == 1);
            node.network_failure.store(!serving, Ordering::Release);
        }
    }

    /// Go `Close`.
    pub(crate) async fn close(&self) {
        log::info!("[router service] closing router service discovery");
        self.ctx.cancel();
        let workers = std::mem::take(&mut *self.workers.lock().await);
        for worker in workers {
            let _ = worker.await;
        }
        let urls = std::mem::take(&mut *self.nodes.lock().unwrap())
            .into_keys()
            .collect::<Vec<_>>();
        for url in urls {
            (self.remove_conn)(&url);
        }
        *self.sorted_urls.write().unwrap() = Some(Vec::new());
        self.balancer.lock().unwrap().clean();
        log::info!("[router service] is closed");
    }
}

/// Go `metastorage.ServiceRegistryEntry`.
#[derive(serde_derive::Deserialize)]
struct ServiceRegistryEntry {
    #[serde(rename = "service-addr", default)]
    service_addr: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kv(key: &str, value: &str) -> crate::proto::meta_storagepb::KeyValue {
        crate::proto::meta_storagepb::KeyValue {
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
            ..Default::default()
        }
    }

    /// pd-client logs the router service discovery's lifecycle and the
    /// members it reads from meta storage, sorted.
    #[tokio::test]
    async fn router_service_discovery_reads_members_and_logs_pd_clients_lines() {
        crate::logutil::test_capture::install();
        let responses = Arc::new(Mutex::new(vec![meta_storagepb::GetResponse {
            kvs: vec![
                kv(
                    "/ms/71/router/registry/b",
                    r#"{"service-addr":"http://127.0.0.1:2"}"#,
                ),
                kv(
                    "/ms/71/router/registry/a",
                    r#"{"service-addr":"http://127.0.0.1:1"}"#,
                ),
                kv("/ms/71/router/registry/c", "not json"),
            ],
            ..Default::default()
        }]));
        let keys = Arc::new(Mutex::new(Vec::new()));
        let meta_get: MetaPrefixGet = {
            let responses = responses.clone();
            let keys = keys.clone();
            Arc::new(move |key| {
                keys.lock().unwrap().push(key);
                let response = responses.lock().unwrap().pop().unwrap_or_default();
                Box::pin(async move { Ok(response) })
            })
        };
        let connect: GetOrCreateConn = Arc::new(|url| {
            Box::pin(async move { Ok(Channel::from_shared(url).unwrap().connect_lazy()) })
        });
        let removed = Arc::new(Mutex::new(Vec::new()));
        let remove: RemoveConn = {
            let removed = removed.clone();
            Arc::new(move |url| removed.lock().unwrap().push(url.to_owned()))
        };
        let parent = Cancellation::default();
        let discovery =
            RouterServiceDiscovery::new(&parent, 71, 4, false, 3, meta_get, connect, remove).await;
        assert_eq!(
            discovery.get_service_urls(),
            vec![
                "http://127.0.0.1:1".to_owned(),
                "http://127.0.0.1:2".to_owned()
            ]
        );
        assert_eq!(keys.lock().unwrap()[0], b"/ms/71/router/registry/".to_vec());
        let first = discovery.get_service_client().unwrap();
        let second = discovery.get_service_client().unwrap();
        assert_ne!(first.url, second.url, "round robin over the nodes");
        discovery.close().await;
        assert_eq!(removed.lock().unwrap().len(), 2);
        assert!(discovery.get_service_client().is_none());
        for expected in [
            "INFO [router service] created router service discovery cluster-id=71 keyspace-id=4 default-discovery-key=/ms/71/router/registry/",
            "INFO [router service] initializing router service discovery max-retry-times=3 retry-interval=1s",
            "INFO [router service] update member sorted urls new-sorted-urls=[\"http://127.0.0.1:1\", \"http://127.0.0.1:2\"]",
            "INFO [router service] updating nodes succeeded urls=[\"http://127.0.0.1:1\", \"http://127.0.0.1:2\"] clients-length=2",
            "INFO [router service] closing router service discovery",
            "INFO [router service] exit check member loop",
            "INFO [router service] exit health check member loop",
            "INFO [router service] is closed",
        ] {
            assert!(
                crate::logutil::test_capture::any(|line| line == expected),
                "{expected:?} not in {:?}",
                crate::logutil::test_capture::lines()
            );
        }
        assert!(crate::logutil::test_capture::any(|line| {
            line.starts_with(
            "WARN try to deserialize service registry entry failed service-key=/ms/71/router/registry/ key=/ms/71/router/registry/c error="
        )
        }));
    }
}
