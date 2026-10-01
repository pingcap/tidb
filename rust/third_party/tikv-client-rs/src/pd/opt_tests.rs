// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::pd_backoff::Backoffer;
use crate::pd_options::*;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::Mutex;
use tonic::transport::Endpoint;

fn empty(channel: &OptionChange) {
    assert_eq!(
        channel.receiver.try_lock().unwrap().try_recv(),
        Err(TryRecvError::Empty)
    );
}
fn receive(channel: &OptionChange) {
    assert_eq!(channel.receiver.try_lock().unwrap().try_recv(), Ok(()));
}

// All original TestDynamicOptionChange assertions, with deterministic channel
// observations: setting is synchronous, so no wall-clock Eventually is needed.
#[test]
fn source_dynamic_option_change() {
    let o = Options::new();
    assert_eq!(o.get_max_tso_batch_wait_interval(), Duration::ZERO);
    assert!(!o.get_enable_tso_follower_proxy());
    assert!(!o.get_enable_follower_handle());
    assert_eq!(o.get_tso_client_rpc_concurrency(), 1);
    assert!(o.get_enable_router_client());
    assert!(o
        .set_max_tso_batch_wait_interval(Duration::from_secs(1))
        .is_err());
    assert_eq!(o.get_max_tso_batch_wait_interval(), Duration::ZERO);
    for micros in [1000, 500, 1500, 10000, 0] {
        let interval = Duration::from_micros(micros);
        o.set_max_tso_batch_wait_interval(interval).unwrap();
        assert_eq!(o.get_max_tso_batch_wait_interval(), interval);
    }
    empty(&o.enable_tso_follower_proxy_ch);
    for enable in [true, false] {
        o.set_enable_tso_follower_proxy(enable);
        receive(&o.enable_tso_follower_proxy_ch);
        assert_eq!(o.get_enable_tso_follower_proxy(), enable);
    }
    o.set_enable_tso_follower_proxy(false);
    empty(&o.enable_tso_follower_proxy_ch);
    for enable in [true, false] {
        o.set_enable_follower_handle(enable);
        assert_eq!(o.get_enable_follower_handle(), enable);
    }
    o.set_tso_client_rpc_concurrency(10);
    assert_eq!(o.get_tso_client_rpc_concurrency(), 10);
    empty(&o.enable_router_client_ch);
    for enable in [false, true] {
        o.set_enable_router_client(enable);
        receive(&o.enable_router_client_ch);
        assert_eq!(o.get_enable_router_client(), enable);
    }
    o.set_enable_router_client(true);
    empty(&o.enable_router_client_ch);
}

#[test]
fn source_options() {
    let mut region = GetRegionOp::default();
    assert!(!region.allow_follower_handle);
    with_allow_follower_handle()(&mut region);
    with_allow_router_service_handle()(&mut region);
    assert!(region.allow_follower_handle);
    assert!(region.allow_router_service_handle);
    with_allow_pd_leader_only()(&mut region);
    assert!(!region.allow_follower_handle);
    assert!(!region.allow_router_service_handle);
    let mut store = GetStoreOp::default();
    assert!(!store.allow_router_service_handle);
    with_allow_router_service_handle_store_request()(&mut store);
    assert!(store.allow_router_service_handle);
    with_pd_leader_handle_store_request_only()(&mut store);
    assert!(!store.allow_router_service_handle);
}

#[test]
fn static_options_keep_defaults_order_and_shared_identity() {
    let mut o = Options::new();
    assert_eq!(o.timeout, Duration::from_secs(3));
    assert_eq!(o.max_retry_times, 100);
    assert!(!o.enable_forwarding);
    assert!(!o.use_tso_server_proxy);
    assert!(o.init_metrics);
    assert!(o.grpc_dial_options.is_empty());
    assert!(o.metrics_labels.is_none());
    assert!(o.backoffer.is_none());
    let labels = Arc::new(RwLock::new(Default::default()));
    let backoffer = Arc::new(Mutex::new(Backoffer::new(
        1_000_000,
        10_000_000,
        1_000_000_000,
    )));
    let constructors = [
        with_custom_timeout_option(Duration::from_secs(7)),
        with_forwarding_option(true),
        with_tso_server_proxy_option(true),
        with_max_error_retry(-1),
        with_metrics_labels(Some(labels.clone())),
        with_init_metrics_option(false),
        with_backoffer(Some(backoffer.clone())),
        with_enable_router_client(false),
        with_enable_follower_handle(true),
    ];
    for set in &constructors {
        set(&mut o);
    }
    assert_eq!(o.timeout, Duration::from_secs(7));
    assert_eq!(o.max_retry_times, -1);
    assert!(o.enable_forwarding);
    assert!(o.use_tso_server_proxy);
    assert!(!o.init_metrics);
    assert!(Arc::ptr_eq(o.metrics_labels.as_ref().unwrap(), &labels));
    assert!(Arc::ptr_eq(o.backoffer.as_ref().unwrap(), &backoffer));
    labels.write().unwrap().insert("name".into(), "pd".into());
    assert_eq!(
        o.metrics_labels.as_ref().unwrap().read().unwrap()["name"],
        "pd"
    );
    assert!(!o.get_enable_router_client());
    assert!(o.get_enable_follower_handle());
    receive(&o.enable_router_client_ch);
    receive(&o.enable_follower_handle_ch);
    // Constructor closures are reusable; assigning the same bool does not wake.
    for set in &constructors {
        set(&mut o);
    }
    empty(&o.enable_router_client_ch);
    empty(&o.enable_follower_handle_ch);
    with_backoffer(None)(&mut o);
    with_metrics_labels(None)(&mut o);
    assert!(o.backoffer.is_none());
    assert!(o.metrics_labels.is_none());

    let order = Arc::new(std::sync::Mutex::new(Vec::new()));
    let dial = |i| {
        let order = order.clone();
        Arc::new(move |ep: Endpoint| {
            order.lock().unwrap().push(i);
            ep
        }) as GrpcDialOption
    };
    let append = with_grpc_dial_options(vec![dial(1), dial(2)]);
    append(&mut o);
    with_grpc_dial_options(vec![dial(3)])(&mut o);
    append(&mut o);
    assert!(Arc::ptr_eq(
        &o.grpc_dial_options[0],
        &o.grpc_dial_options[3]
    ));
    o.grpc_dial_options.iter().fold(
        Endpoint::from_static("http://127.0.0.1:2379"),
        |ep, apply| apply(ep),
    );
    assert_eq!(*order.lock().unwrap(), [1, 2, 3, 1, 2]);
}

#[test]
fn every_request_option_preserves_assignment_and_overrides() {
    let mut region = GetRegionOp::default();
    with_buckets()(&mut region);
    with_output_must_contain_all_key_range()(&mut region);
    with_allow_follower_handle()(&mut region);
    with_allow_router_service_handle()(&mut region);
    with_allow_pd_leader_only()(&mut region);
    assert_eq!(
        region,
        GetRegionOp {
            need_buckets: true,
            output_must_contain_all_key_range: true,
            ..Default::default()
        }
    );
    with_allow_router_service_handle()(&mut region);
    assert!(region.allow_router_service_handle);
    assert!(!region.allow_follower_handle);
    let mut store = GetStoreOp::default();
    with_exclude_tombstone()(&mut store);
    with_allow_router_service_handle_store_request()(&mut store);
    with_pd_leader_handle_store_request_only()(&mut store);
    assert_eq!(
        store,
        GetStoreOp {
            exclude_tombstone: true,
            ..Default::default()
        }
    );
    let mut regions = RegionsOp::default();
    with_group("group".into())(&mut regions);
    with_retry(u64::MAX)(&mut regions);
    with_skip_store_limit()(&mut regions);
    assert_eq!(
        regions,
        RegionsOp {
            group: "group".into(),
            retry_limit: u64::MAX,
            skip_store_limit: true
        }
    );
    with_group(String::new())(&mut regions);
    with_retry(0)(&mut regions);
    assert_eq!(regions.group, "");
    assert_eq!(regions.retry_limit, 0);
    let mut meta = MetaStorageOp::default();
    assert!(meta.range_end.is_none());
    assert_eq!((meta.revision, meta.lease, meta.limit), (0, 0, 0));
    assert!(!meta.prev_kv && !meta.is_opts_with_prefix);
    let range: RangeEnd = Arc::new([AtomicU8::new(1), AtomicU8::new(2)]);
    let set_end = with_range_end(Some(range.clone()));
    set_end(&mut meta);
    with_rev(-3)(&mut meta);
    with_lease(-5)(&mut meta);
    with_limit(-7)(&mut meta);
    with_prev_kv()(&mut meta);
    with_prefix()(&mut meta);
    assert_eq!((meta.revision, meta.lease, meta.limit), (-3, -5, -7));
    assert!(meta.prev_kv && meta.is_opts_with_prefix);
    assert!(Arc::ptr_eq(meta.range_end.as_ref().unwrap(), &range));
    range[0].store(9, Ordering::SeqCst);
    assert_eq!(
        meta.range_end.as_ref().unwrap()[0].load(Ordering::SeqCst),
        9
    );
    with_range_end(None)(&mut meta);
    assert!(meta.range_end.is_none());
    set_end(&mut meta);
    assert!(Arc::ptr_eq(meta.range_end.as_ref().unwrap(), &range));
}

#[test]
fn notifications_coalesce_and_only_follow_real_changes() {
    let o = Options::new();
    for (set, get, channel, initial) in [
        (
            Options::set_enable_tso_follower_proxy as fn(&Options, bool),
            Options::get_enable_tso_follower_proxy as fn(&Options) -> bool,
            &o.enable_tso_follower_proxy_ch,
            false,
        ),
        (
            Options::set_enable_follower_handle,
            Options::get_enable_follower_handle,
            &o.enable_follower_handle_ch,
            false,
        ),
        (
            Options::set_enable_router_client,
            Options::get_enable_router_client,
            &o.enable_router_client_ch,
            true,
        ),
    ] {
        empty(channel);
        set(&o, initial);
        empty(channel);
        set(&o, !initial);
        set(&o, initial);
        set(&o, !initial);
        assert_eq!(get(&o), !initial);
        receive(channel);
        empty(channel);
        set(&o, !initial);
        empty(channel);
        set(&o, initial);
        receive(channel);
        empty(channel);
    }
    o.set_tso_client_rpc_concurrency(-10);
    assert_eq!(o.get_tso_client_rpc_concurrency(), -10);
    o.set_tso_client_rpc_concurrency(0);
    assert_eq!(o.get_tso_client_rpc_concurrency(), 0);
    o.set_max_tso_batch_wait_interval(Duration::from_nanos(1))
        .unwrap();
    assert!(o
        .set_max_tso_batch_wait_interval(Duration::from_nanos(10_000_001))
        .is_err());
    assert_eq!(o.get_max_tso_batch_wait_interval(), Duration::from_nanos(1));
}

#[test]
fn options_support_concurrent_changes_without_creating_workers() {
    let o = Arc::new(Options::new());
    let completed = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for i in 0..8 {
            let o = &o;
            let completed = &completed;
            scope.spawn(move || {
                for n in 0..1000 {
                    o.set_enable_follower_handle(n % 2 == 0);
                    o.set_enable_router_client(n % 2 != 0);
                    o.set_enable_tso_follower_proxy(n % 2 == 0);
                    o.set_tso_client_rpc_concurrency(i);
                    o.set_max_tso_batch_wait_interval(Duration::from_micros(n))
                        .unwrap();
                }
                completed.fetch_add(1, Ordering::SeqCst);
            });
        }
    });
    assert_eq!(completed.load(Ordering::SeqCst), 8);
    assert!((0..8).contains(&o.get_tso_client_rpc_concurrency()));
    assert!(o.get_max_tso_batch_wait_interval() <= Duration::from_millis(10));
    for channel in [
        &o.enable_follower_handle_ch,
        &o.enable_router_client_ch,
        &o.enable_tso_follower_proxy_ch,
    ] {
        receive(channel);
        empty(channel);
    }
    assert_eq!(Arc::strong_count(&o), 1);
}

#[tokio::test]
async fn canceled_receiver_does_not_consume_the_next_notification() {
    let o = Options::new();
    {
        let mut receiver = o.enable_follower_handle_ch.receiver.lock().await;
        let receive = receiver.recv();
        tokio::pin!(receive);
        assert!(futures::poll!(&mut receive).is_pending());
    }
    o.set_enable_follower_handle(true);
    assert_eq!(
        o.enable_follower_handle_ch
            .receiver
            .lock()
            .await
            .recv()
            .await,
        Some(())
    );
}

#[test]
fn closed_notification_channel_only_panics_on_a_real_change() {
    let o = Options::new();
    o.enable_follower_handle_ch
        .receiver
        .try_lock()
        .unwrap()
        .close();
    o.set_enable_follower_handle(false);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        o.set_enable_follower_handle(true);
    }))
    .is_err());
    assert!(o.get_enable_follower_handle());
}
