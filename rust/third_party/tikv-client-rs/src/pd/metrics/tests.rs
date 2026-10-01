use super::*;
use prometheus::core::Collector;
use serde_json::{json, Value};
use std::sync::atomic::AtomicUsize;

fn labels(value: &str) -> HashMap<String, String> {
    HashMap::from([("cluster".to_owned(), value.to_owned())])
}

fn normalize(value: &mut Value) {
    match value {
        Value::Object(fields) => {
            // Go Prometheus includes creation timestamps; Rust 0.13 does not.
            fields.remove("created_timestamp");
            for v in fields.values_mut() {
                normalize(v);
            }
        }
        Value::Array(values) => {
            for v in values.iter_mut() {
                normalize(v);
            }
            // Collector/label iteration order is unspecified in both clients.
            values.sort_by_key(|v| v.to_string());
        }
        Value::Number(n) => {
            *value = json!(n.as_f64().unwrap());
        }
        _ => {}
    }
}

fn snapshot(collectors: Vec<(&str, Box<dyn Collector>)>) -> Value {
    let mut result = serde_json::Map::new();
    for (name, collector) in collectors {
        let families = collector.collect();
        assert_eq!(families.len(), 1, "{name}");
        let f = &families[0];
        let metrics: Vec<Value> = f.get_metric().iter().map(|m| {
            let mut value = json!({"label": m.get_label().iter().map(|l| json!({"name":l.get_name(),"value":l.get_value()})).collect::<Vec<_>>()});
            if m.has_histogram() {
                let h=m.get_histogram();
                value["histogram"]=json!({"sample_count":h.get_sample_count(),"sample_sum":h.get_sample_sum(),"bucket":h.get_bucket().iter().map(|b| json!({"cumulative_count":b.get_cumulative_count(),"upper_bound":b.get_upper_bound()})).collect::<Vec<_>>()});
            } else if m.has_counter() {
                value["counter"]=json!({"value":m.get_counter().get_value()});
            } else if m.has_gauge() {
                value["gauge"]=json!({"value":m.get_gauge().get_value()});
            } else { panic!("unexpected metric type for {name}"); }
            value
        }).collect();
        result.insert(name.into(), json!({"name":f.get_name(),"help":f.get_help(),"type":f.get_field_type() as i32,"metric":metrics}));
    }
    Value::Object(result)
}

fn check_oracle(mut actual: Value, expected: &str) {
    let mut expected: Value = serde_json::from_str(expected).unwrap();
    normalize(&mut actual);
    normalize(&mut expected);
    for (name, value) in expected.as_object().unwrap() {
        assert_eq!(&actual[name], value, "Go collector {name}");
    }
    assert_eq!(
        actual.as_object().unwrap().len(),
        expected.as_object().unwrap().len()
    );
}

#[test]
fn pd_collectors_and_all_prebound_observers_match_go_runtime() {
    let metrics = Metrics::new(labels("oracle")).unwrap();
    metrics.exercise_oracle();
    check_oracle(
        snapshot(metrics.all_collectors()),
        include_str!("../../../doc/pd-metrics-oracle/pd.json"),
    );
}

#[test]
fn resource_group_collectors_and_observers_match_go_runtime() {
    let metrics = resource_group::Metrics::new(labels("oracle")).unwrap();
    metrics.exercise_oracle();
    check_oracle(
        snapshot(metrics.all_collectors()),
        include_str!("../../../doc/pd-metrics-oracle/resource_group.json"),
    );
}

#[test]
fn consumers_bind_immediately_then_rebind_before_registration() {
    let owner = MetricsOwner::new(Arc::new(ResourceGroupOwner::new()));
    let registry = Registry::new();
    let events = Arc::new(Mutex::new(Vec::new()));
    let old = owner.snapshot();
    let event_copy = events.clone();
    let registry_copy = registry.clone();
    owner.register_consumer(Box::new(move |metrics| {
        assert!(
            registry_copy.gather().is_empty(),
            "consumers precede registration"
        );
        event_copy.lock().unwrap().push(
            metrics
                .circuit_breaker_counters
                .with_label_values(&["same", "success"]),
        );
    }));
    assert_eq!(events.lock().unwrap().len(), 1);
    events.lock().unwrap()[0].inc();
    owner.init_and_register(labels("first"), &registry);
    assert_eq!(events.lock().unwrap().len(), 2);
    assert_eq!(events.lock().unwrap()[0].get(), 1.0);
    assert_eq!(events.lock().unwrap()[1].get(), 0.0);
    assert!(!Arc::ptr_eq(&old, &owner.snapshot()));
    let current = owner.snapshot();
    owner.init_and_register(labels("ignored"), &registry);
    assert!(Arc::ptr_eq(&current, &owner.snapshot()));
    assert_eq!(events.lock().unwrap().len(), 2);
    let calls = Arc::new(AtomicUsize::new(0));
    let copy = calls.clone();
    owner.register_consumer(Box::new(move |metrics| {
        assert_eq!(
            metrics.tso_batch_size.desc()[0].const_label_pairs[0].get_value(),
            "first"
        );
        copy.fetch_add(1, Ordering::SeqCst);
    }));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(registry
        .gather()
        .iter()
        .all(|f| f.get_metric().iter().all(|m| m
            .get_label()
            .iter()
            .any(|l| l.get_name() == "cluster" && l.get_value() == "first"))));
}

#[test]
fn concurrent_initialization_and_consumer_registration_keep_one_generation() {
    let owner = Arc::new(MetricsOwner::new(Arc::new(ResourceGroupOwner::new())));
    let registry = Registry::new();
    let consumers: Vec<_> = (0..16).map(|_| Arc::new(Mutex::new(None))).collect();
    std::thread::scope(|scope| {
        for (i, handle) in consumers.iter().enumerate() {
            let owner = owner.clone();
            let registry = registry.clone();
            let handle = handle.clone();
            scope.spawn(move || {
                owner.register_consumer(Box::new(move |metrics| {
                    *handle.lock().unwrap() = Some(metrics.tso_batch_size.clone());
                }));
                owner.init_and_register(labels(&i.to_string()), &registry);
            });
        }
    });
    let current = owner.snapshot().tso_batch_size.desc()[0]
        .const_label_pairs
        .clone();
    for handle in consumers {
        assert_eq!(
            handle.lock().unwrap().as_ref().unwrap().desc()[0].const_label_pairs,
            current
        );
    }
}

#[test]
fn registration_preserves_go_omission_and_resource_group_has_no_once_guard() {
    let resource = Arc::new(ResourceGroupOwner::new());
    let owner = MetricsOwner::new(resource.clone());
    let registry = Registry::new();
    assert!(registry.gather().is_empty());
    owner.init_and_register(HashMap::new(), &registry);
    let metrics = owner.snapshot();
    metrics
        .ongoing_request_count_gauge
        .with_label_values(&["stream"])
        .set(1.0);
    metrics
        .estimate_tso_latency_gauge
        .with_label_values(&["stream"])
        .set(2.0);
    let families = registry.gather();
    assert!(!families
        .iter()
        .any(|f| f.get_name() == "pd_client_request_ongoing_requests_count"));
    assert!(families
        .iter()
        .any(|f| f.get_name() == "pd_client_request_estimate_tso_latency"));
    let old = resource.snapshot();
    let duplicate = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        resource.init_and_register(HashMap::new(), &registry)
    }));
    assert!(duplicate.is_err());
    assert!(
        !Arc::ptr_eq(&old, &resource.snapshot()),
        "Go replaces globals before the duplicate registration panic"
    );
}

#[test]
fn first_pd_registration_failure_still_consumes_once_guard() {
    let owner = MetricsOwner::new(Arc::new(ResourceGroupOwner::new()));
    let registry = Registry::new();
    Metrics::new(HashMap::new())
        .unwrap()
        .register_metrics(&registry)
        .unwrap();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
        || owner.init_and_register(HashMap::new(), &registry)
    ))
    .is_err());
    let current = owner.snapshot();
    owner.init_and_register(labels("later"), &registry);
    assert!(Arc::ptr_eq(&current, &owner.snapshot()));
}
