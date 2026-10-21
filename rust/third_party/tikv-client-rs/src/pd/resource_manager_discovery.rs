// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! pd-client `servicediscovery/resource_manager_service_discovery.go`: in
//! microservice mode, the standalone resource manager's primary is read from
//! `/ms/{cluster_id}/resource_manager/primary` in PD's meta storage. Read and
//! token RPCs go to it once it is known; PD serves them otherwise.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use prost::Message;
use tokio::task::JoinHandle;
use tonic::transport::Channel;

use super::errs;
use super::router_service_discovery::GetOrCreateConn;
use crate::async_util::Cancellation;
use crate::proto::{meta_storagepb, resource_manager};
use crate::{Error, Result};

/// Go `resourceManagerInitRetryTime`.
const RESOURCE_MANAGER_INIT_RETRY_TIME: isize = 3;
/// Go `initRetryInterval`.
const INIT_RETRY_INTERVAL: Duration = Duration::from_secs(1);
/// Go `serviceURLRetryInterval`.
const SERVICE_URL_RETRY_INTERVAL: Duration = Duration::from_secs(3);

/// A meta-storage single-key read (`metastorage.Client.Get(key)`).
pub(crate) type MetaGet =
    Arc<dyn Fn(Vec<u8>) -> BoxFuture<'static, Result<meta_storagepb::GetResponse>> + Send + Sync>;

#[derive(Default)]
struct Service {
    url: String,
    conn: Option<Channel>,
}

/// Go `ResourceManagerDiscovery`.
pub(crate) struct ResourceManagerDiscovery {
    ctx: Cancellation,
    started: AtomicBool,
    discovery_key: String,
    meta_get: MetaGet,
    get_conn: GetOrCreateConn,
    /// Go `onLeaderChanged` (the token connection's reconnect).
    on_leader_changed: Arc<dyn Fn() + Send + Sync>,
    update_service_url: tokio::sync::Notify,
    service: Mutex<Service>,
    worker: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

impl ResourceManagerDiscovery {
    /// Go `NewResourceManagerDiscovery`.
    pub(crate) fn new(
        parent: &Cancellation,
        cluster_id: u64,
        meta_get: MetaGet,
        get_conn: GetOrCreateConn,
        on_leader_changed: Arc<dyn Fn() + Send + Sync>,
    ) -> Arc<Self> {
        let discovery = Arc::new(Self {
            ctx: parent.child(),
            started: AtomicBool::new(false),
            discovery_key: format!("/ms/{cluster_id}/resource_manager/primary"),
            meta_get,
            get_conn,
            on_leader_changed,
            update_service_url: tokio::sync::Notify::new(),
            service: Mutex::new(Service::default()),
            worker: tokio::sync::Mutex::new(None),
        });
        log::info!(
            "cluster-id" = cluster_id,
            "discovery-key" = discovery.discovery_key.as_str();
            "[resource-manager] created resource manager discovery"
        );
        discovery
    }

    /// Go `Init`.
    pub(crate) async fn init(self: &Arc<Self>) {
        if self
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        *self.worker.lock().await = Some(tokio::spawn(Arc::clone(self).init_and_update_loop()));
    }

    /// Go `initAndUpdateLoop`.
    async fn init_and_update_loop(self: Arc<Self>) {
        log::info!(
            "max-retry-times" = RESOURCE_MANAGER_INIT_RETRY_TIME,
            "retry-interval":% = crate::logutil::go_duration(INIT_RETRY_INTERVAL);
            "[resource-manager] initializing service discovery"
        );
        let found = Mutex::new((String::new(), 0_i64));
        let ctx = self.ctx.clone();
        let result = super::backoff::retry(
            async move {
                ctx.cancelled().await;
                Error::ContextCanceled
            },
            RESOURCE_MANAGER_INIT_RETRY_TIME,
            INIT_RETRY_INTERVAL.as_nanos() as i64,
            || async {
                let discovered = self.discover_service_url().await?;
                *found.lock().unwrap() = discovered;
                Ok(())
            },
        )
        .await;
        if let Err(error) = result {
            log::error!(
                error:% = error;
                "[resource-manager] failed to discover service. initialization failed."
            );
        }
        let (url, revision) = found.into_inner().unwrap();
        self.reset_conn(&url).await;
        self.update_service_url_loop(revision).await;
    }

    /// Go `resetConn`.
    async fn reset_conn(&self, url: &str) {
        if url.is_empty() {
            *self.service.lock().unwrap() = Service::default();
            (self.on_leader_changed)();
            return;
        }
        if self.service.lock().unwrap().url == url {
            return;
        }
        let conn = match (self.get_conn)(url.to_owned()).await {
            Ok(conn) => conn,
            Err(error) => {
                // Dialing is lazy, so this should not normally fail.
                log::error!(
                    url = url,
                    error:% = Error::from(error);
                    "[resource-manager] failed to create gRPC connection"
                );
                return;
            }
        };
        *self.service.lock().unwrap() = Service {
            url: url.to_owned(),
            conn: Some(conn),
        };
        log::info!("new-url" = url; "[resource-manager] updated service URL");
        (self.on_leader_changed)();
    }

    /// Go `GetServiceURL`: empty when PD serves the resource manager.
    pub(crate) fn get_service_url(&self) -> String {
        self.service.lock().unwrap().url.clone()
    }

    /// Go `GetConn`.
    pub(crate) fn get_conn(&self) -> Option<Channel> {
        self.service.lock().unwrap().conn.clone()
    }

    /// Go `discoverServiceURL`: the primary's first listen URL and the
    /// response revision.
    async fn discover_service_url(&self) -> Result<(String, i64)> {
        let response = match (self.meta_get)(self.discovery_key.clone().into_bytes()).await {
            Ok(response) => response,
            Err(error) => {
                log::error!(
                    "discovery-key" = self.discovery_key.as_str(),
                    error:% = error;
                    "[resource-manager] failed to get resource-manager serving endpoint"
                );
                return Err(error);
            }
        };
        let revision = response.header.as_ref().map_or(0, |header| header.revision);
        if response.kvs.is_empty() {
            return Ok((String::new(), revision));
        }
        if response.count > 1 {
            return Err(errs::client_get_multi_response(format!("{:?}", response.kvs)).into());
        }
        let primary = resource_manager::Participant::decode(response.kvs[0].value.as_slice())
            .map_err(|error| {
                Error::from(
                    errs::ERR_CLIENT_PROTO_UNMARSHAL
                        .wrap(Some(Arc::new(Error::StringError(error.to_string()))))
                        .expect("a cause is given")
                        .gen_with_stack_by_cause(),
                )
            })?;
        let Some(url) = primary.listen_urls.into_iter().next() else {
            log::warn!(
                "discovery-key" = self.discovery_key.as_str();
                "[resource-manager] the keyspace serving endpoint list is empty"
            );
            return Err(errs::ERR_CLIENT_GET_SERVING_ENDPOINT.error().into());
        };
        Ok((url, revision))
    }

    /// Go `ScheduleUpdateServiceURL`.
    pub(crate) fn schedule_update_service_url(&self) {
        self.update_service_url.notify_one();
    }

    /// Go `updateServiceURLLoop`: without a standalone primary, look for one
    /// every second; on request, rediscover (at most once per
    /// `serviceURLRetryInterval`).
    async fn update_service_url_loop(&self, mut revision: i64) {
        let mut ticker = tokio::time::interval_at(
            tokio::time::Instant::now() + INIT_RETRY_INTERVAL,
            INIT_RETRY_INTERVAL,
        );
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_update_time: Option<Instant> = None;
        loop {
            tokio::select! {
                _ = self.ctx.cancelled() => {
                    log::info!("[resource-manager] exit update service URL loop due to context canceled");
                    return;
                }
                _ = ticker.tick() => {
                    if !self.get_service_url().is_empty() {
                        continue;
                    }
                    self.discover_and_update(&mut revision).await;
                }
                _ = self.update_service_url.notified() => {
                    if let Some(last) = last_update_time {
                        let since = last.elapsed();
                        if since < SERVICE_URL_RETRY_INTERVAL {
                            let wait = SERVICE_URL_RETRY_INTERVAL - since;
                            log::info!(
                                since:% = crate::logutil::go_duration(since),
                                wait:% = crate::logutil::go_duration(wait);
                                "[resource-manager] delay updating service URL due to backoff"
                            );
                            tokio::select! {
                                _ = self.ctx.cancelled() => {
                                    log::info!("[resource-manager] exit update service URL loop due to context canceled");
                                    return;
                                }
                                _ = tokio::time::sleep(wait) => {}
                            }
                        }
                    }
                    last_update_time = Some(Instant::now());
                    log::info!(
                        "old-url" = self.get_service_url().as_str();
                        "[resource-manager] updating service URL"
                    );
                    self.discover_and_update(&mut revision).await;
                }
            }
        }
    }

    /// Go `discoverAndUpdate`.
    async fn discover_and_update(&self, revision: &mut i64) {
        match self.discover_service_url().await {
            Ok((url, new_revision)) => {
                if new_revision > *revision {
                    self.reset_conn(&url).await;
                    *revision = new_revision;
                }
            }
            Err(error) => log::warn!(
                "discovery-key" = self.discovery_key.as_str(),
                error:% = error;
                "[resource-manager] failed to discover service URL"
            ),
        }
    }

    /// Go `Close`.
    pub(crate) async fn close(&self) {
        self.ctx.cancel();
        if let Some(worker) = self.worker.lock().await.take() {
            let _ = worker.await;
        }
        self.service.lock().unwrap().conn = None;
    }
}

/// pd-client `shouldUpdateRMURL`: a leader change, or a network error (or
/// cancellation) of the resource manager's RPC.
pub(crate) fn should_update_rm_url(error: &Error) -> bool {
    let error_ref: &(dyn std::error::Error + 'static) = error;
    if errs::is_leader_change(Some(error_ref)) {
        return true;
    }
    match error {
        Error::GrpcAPI(status) => {
            errs::is_network_error(status.code()) || status.code() == tonic::Code::Cancelled
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// pd-client's resource manager discovery: the standalone primary from
    /// meta storage, its connection, and the lines along the way.
    #[tokio::test]
    async fn resource_manager_discovery_follows_the_primary_and_logs_pd_clients_lines() {
        crate::logutil::test_capture::install();
        let primary = resource_manager::Participant {
            name: "rm-1".to_owned(),
            id: 1,
            listen_urls: vec!["http://127.0.0.1:3".to_owned()],
        };
        let meta_get: MetaGet = Arc::new(move |key| {
            assert_eq!(key, b"/ms/72/resource_manager/primary".to_vec());
            let response = meta_storagepb::GetResponse {
                header: Some(meta_storagepb::ResponseHeader {
                    revision: 5,
                    ..Default::default()
                }),
                kvs: vec![meta_storagepb::KeyValue {
                    key: key.clone(),
                    value: primary.encode_to_vec(),
                    ..Default::default()
                }],
                count: 1,
                ..Default::default()
            };
            Box::pin(async move { Ok(response) })
        });
        let connect: GetOrCreateConn = Arc::new(|url| {
            Box::pin(async move { Ok(Channel::from_shared(url).unwrap().connect_lazy()) })
        });
        let leader_changes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let changes = leader_changes.clone();
        let parent = Cancellation::default();
        let discovery = ResourceManagerDiscovery::new(
            &parent,
            72,
            meta_get,
            connect,
            Arc::new(move || {
                changes.fetch_add(1, Ordering::SeqCst);
            }),
        );
        discovery.init().await;
        for _ in 0..200 {
            if !discovery.get_service_url().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(discovery.get_service_url(), "http://127.0.0.1:3");
        assert!(discovery.get_conn().is_some());
        assert_eq!(leader_changes.load(Ordering::SeqCst), 1);
        discovery.close().await;
        for expected in [
            "INFO [resource-manager] created resource manager discovery cluster-id=72 discovery-key=/ms/72/resource_manager/primary",
            "INFO [resource-manager] initializing service discovery max-retry-times=3 retry-interval=1s",
            "INFO [resource-manager] updated service URL new-url=http://127.0.0.1:3",
            "INFO [resource-manager] exit update service URL loop due to context canceled",
        ] {
            assert!(
                crate::logutil::test_capture::any(|line| line == expected),
                "{expected:?} not in {:?}",
                crate::logutil::test_capture::lines()
            );
        }
        assert!(should_update_rm_url(&Error::GrpcAPI(
            tonic::Status::unavailable("down")
        )));
        assert!(!should_update_rm_url(&Error::GrpcAPI(
            tonic::Status::not_found("x")
        )));
    }
}
