// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Test support shared by the ported controller tests: the mock resource
//! group provider of `global_controller_test.go` and the helpers of
//! `group_controller_test.go`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::future::BoxFuture;
use prometheus::core::Collector;
use prometheus::{Counter, CounterVec, Histogram};
use tokio::sync::mpsc;

use super::config::{default_ru_config, RUConfig};
use super::global_controller::ResourceGroupProvider;
use super::group_controller::{
    new_group_cost_controller, new_request_source_metrics_state, GroupCostController,
};
use super::limiter::NotifyMsg;
use crate::pd::errs::ClientGetResourceGroupError;
use crate::pd::metastorage::{self, WatchResponse};
use crate::pd::opt::MetaStorageOption;
use crate::pd::resource_manager_client::{
    GetResourceGroupOption, CONTROLLER_CONFIG_PATH_PREFIX_BYTES,
};
use crate::proto::meta_storagepb;
use crate::proto::resource_manager::{
    BackgroundSettings, GroupMode, GroupRequestUnitSettings, ResourceGroup, TokenBucket,
    TokenBucketResponse, TokenBucketsRequest, TokenLimitSettings,
};

type GetResourceGroupResult = Arc<dyn Fn() -> crate::Result<Option<ResourceGroup>> + Send + Sync>;
pub(crate) type AcquireTokenBucketsHook = Arc<
    dyn Fn(TokenBucketsRequest) -> BoxFuture<'static, crate::Result<Vec<TokenBucketResponse>>>
        + Send
        + Sync,
>;

struct GetResourceGroupExpectation {
    name: String,
    result: GetResourceGroupResult,
    once: bool,
    used: bool,
}

/// The testify mock of Go's `MockResourceGroupProvider`: expectations match
/// in registration order and a `once` expectation answers a single call.
pub(crate) struct MockResourceGroupProvider {
    get_response: Mutex<meta_storagepb::GetResponse>,
    get_resource_group: Mutex<Vec<GetResourceGroupExpectation>>,
    get_resource_group_calls: AtomicUsize,
    acquire_token_buckets: Mutex<Option<AcquireTokenBucketsHook>>,
    watch_config_channel: Mutex<Option<mpsc::Receiver<WatchResponse>>>,
    // Keeps the never-written watch channels open, like Go's unclosed chans.
    watch_senders: Mutex<Vec<mpsc::Sender<WatchResponse>>>,
}

/// Go `newMockResourceGroupProvider`.
pub(crate) fn new_mock_resource_group_provider() -> Arc<MockResourceGroupProvider> {
    Arc::new(MockResourceGroupProvider {
        get_response: Mutex::new(meta_storagepb::GetResponse::default()),
        get_resource_group: Mutex::new(Vec::new()),
        get_resource_group_calls: AtomicUsize::new(0),
        acquire_token_buckets: Mutex::new(None),
        watch_config_channel: Mutex::new(None),
        watch_senders: Mutex::new(Vec::new()),
    })
}

impl MockResourceGroupProvider {
    pub(crate) fn set_get_response(&self, response: meta_storagepb::GetResponse) {
        *self.get_response.lock().unwrap() = response;
    }

    fn expect(&self, name: &str, result: GetResourceGroupResult, once: bool) {
        self.get_resource_group
            .lock()
            .unwrap()
            .push(GetResourceGroupExpectation {
                name: name.to_owned(),
                result,
                once,
                used: false,
            });
    }

    /// `On("GetResourceGroup", _, name, _).Return(group, nil)`.
    pub(crate) fn on_get_resource_group(&self, name: &str, group: Option<ResourceGroup>) {
        self.expect(name, Arc::new(move || Ok(group.clone())), false);
    }

    /// `On("GetResourceGroup", _, name, _).Return(...).Once()`.
    pub(crate) fn on_get_resource_group_once(
        &self,
        name: &str,
        result: impl Fn() -> crate::Result<Option<ResourceGroup>> + Send + Sync + 'static,
    ) {
        self.expect(name, Arc::new(result), true);
    }

    pub(crate) fn on_acquire_token_buckets(&self, hook: AcquireTokenBucketsHook) {
        *self.acquire_token_buckets.lock().unwrap() = Some(hook);
    }

    /// `On("Watch", _, ControllerConfigPathPrefixBytes, _).Return(channel, nil)`.
    pub(crate) fn on_watch_config(&self, channel: mpsc::Receiver<WatchResponse>) {
        *self.watch_config_channel.lock().unwrap() = Some(channel);
    }

    /// `AssertNumberOfCalls(t, "GetResourceGroup", n)`.
    pub(crate) fn get_resource_group_calls(&self) -> usize {
        self.get_resource_group_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl metastorage::Client for MockResourceGroupProvider {
    async fn watch(
        &self,
        key: &[u8],
        _opts: &[MetaStorageOption],
    ) -> crate::Result<mpsc::Receiver<WatchResponse>> {
        if key == CONTROLLER_CONFIG_PATH_PREFIX_BYTES {
            if let Some(channel) = self.watch_config_channel.lock().unwrap().take() {
                return Ok(channel);
            }
        }
        let (sender, receiver) = mpsc::channel(1);
        self.watch_senders.lock().unwrap().push(sender);
        Ok(receiver)
    }

    async fn get(
        &self,
        _key: &[u8],
        _opts: &[MetaStorageOption],
    ) -> crate::Result<meta_storagepb::GetResponse> {
        Ok(self.get_response.lock().unwrap().clone())
    }

    async fn put(
        &self,
        _key: &[u8],
        _value: &[u8],
        _opts: &[MetaStorageOption],
    ) -> crate::Result<meta_storagepb::PutResponse> {
        Err(crate::Error::Unimplemented)
    }
}

#[async_trait]
impl ResourceGroupProvider for MockResourceGroupProvider {
    async fn get_resource_group(
        &self,
        resource_group_name: &str,
        _opts: &[GetResourceGroupOption],
    ) -> crate::Result<Option<ResourceGroup>> {
        if fail::eval("gerResourceGroupError", |_| ()).is_some() {
            let cause = "fake get resource group error".to_owned();
            return Err(crate::Error::PdResourceGroup(ClientGetResourceGroupError {
                resource_group_name: resource_group_name.to_owned(),
                cause: cause.clone(),
                error: Some(Arc::new(std::io::Error::other(cause))),
            }));
        }
        self.get_resource_group_calls.fetch_add(1, Ordering::SeqCst);
        let result = {
            let mut expectations = self.get_resource_group.lock().unwrap();
            let expectation = expectations
                .iter_mut()
                .find(|expectation| {
                    expectation.name == resource_group_name && !(expectation.once && expectation.used)
                })
                .unwrap_or_else(|| {
                    panic!("mock: I don't know what to return for GetResourceGroup({resource_group_name})")
                });
            expectation.used = true;
            expectation.result.clone()
        };
        result()
    }

    async fn list_resource_groups(
        &self,
        _opts: &[GetResourceGroupOption],
    ) -> crate::Result<Vec<ResourceGroup>> {
        Err(crate::Error::Unimplemented)
    }

    async fn add_resource_group(&self, _meta_group: ResourceGroup) -> crate::Result<String> {
        Err(crate::Error::Unimplemented)
    }

    async fn modify_resource_group(&self, _meta_group: ResourceGroup) -> crate::Result<String> {
        Err(crate::Error::Unimplemented)
    }

    async fn delete_resource_group(&self, _resource_group_name: &str) -> crate::Result<String> {
        Err(crate::Error::Unimplemented)
    }

    async fn acquire_token_buckets(
        &self,
        request: TokenBucketsRequest,
    ) -> crate::Result<Vec<TokenBucketResponse>> {
        let hook = self
            .acquire_token_buckets
            .lock()
            .unwrap()
            .clone()
            .expect("mock: I don't know what to return for AcquireTokenBuckets");
        hook(request).await
    }

    async fn load_resource_groups(&self) -> crate::Result<(Vec<ResourceGroup>, i64)> {
        Ok((Vec::new(), 0))
    }
}

/// An RU-mode resource group with the given token limit settings.
pub(crate) fn ru_group(name: &str, fill_rate: u64, burst_limit: i64) -> ResourceGroup {
    ResourceGroup {
        name: name.to_owned(),
        mode: GroupMode::RuMode as i32,
        r_u_settings: Some(GroupRequestUnitSettings {
            r_u: Some(TokenBucket {
                settings: Some(TokenLimitSettings {
                    fill_rate,
                    burst_limit,
                    ..TokenLimitSettings::default()
                }),
                ..TokenBucket::default()
            }),
        }),
        ..ResourceGroup::default()
    }
}

/// A sender whose receiver is gone: like a Go unbuffered channel nobody
/// receives from, every non-blocking send fails.
pub(crate) fn closed_channel<T>() -> mpsc::Sender<T> {
    mpsc::channel(1).0
}

/// Go `createTestGroupCostController`.
pub(crate) fn create_test_group_cost_controller() -> Arc<GroupCostController> {
    create_test_group_cost_controller_with_config(default_ru_config())
}

/// `createTestGroupCostController` with an adjusted main config, standing in
/// for Go tests that assign `gc.mainCfg` fields after creation.
pub(crate) fn create_test_group_cost_controller_with_config(
    config: RUConfig,
) -> Arc<GroupCostController> {
    let mut group = ru_group("test", 1000, 0);
    group.priority = 1;
    group.background_settings = Some(BackgroundSettings {
        job_types: vec!["lightning".to_owned(), "br".to_owned()],
        ..BackgroundSettings::default()
    });
    new_group_cost_controller(
        Arc::new(group),
        Arc::new(config),
        closed_channel::<NotifyMsg>(),
        closed_channel::<Arc<GroupCostController>>(),
        Some(new_request_source_metrics_state("test")),
    )
    .unwrap()
}

pub(crate) fn counter_value(counter: &Counter) -> f64 {
    counter.get()
}

pub(crate) fn histogram_sample_count(histogram: &Histogram) -> u64 {
    histogram.get_sample_count()
}

/// Go `collectorMetricCount`.
pub(crate) fn collector_metric_count(collector: &CounterVec) -> usize {
    collector
        .collect()
        .iter()
        .map(|family| family.get_metric().len())
        .sum()
}

/// Go `testutil.Eventually` / `require.Eventually`.
pub(crate) async fn eventually(
    mut condition: impl FnMut() -> bool,
    wait_for: Duration,
    tick: Duration,
) -> bool {
    let deadline = Instant::now() + wait_for;
    loop {
        if condition() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(tick).await;
    }
}
