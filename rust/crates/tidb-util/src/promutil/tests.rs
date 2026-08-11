// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;

fn counter(name: &str) -> Counter {
    Counter::with_opts(Opts::new(name, "test counter")).unwrap()
}

// Go TestNoopRegistry.
#[test]
fn noop_registry() {
    let registry = new_noop_registry();

    registry
        .register(Box::new(counter("noop_counter")))
        .unwrap();
    registry
        .register(Box::new(counter("noop_counter")))
        .unwrap();
    registry.must_register(vec![
        Box::new(counter("noop_counter")),
        Box::new(counter("noop_counter")),
    ]);

    assert!(registry.unregister(Box::new(counter("noop_counter"))));
    let gauge_vec = GaugeVec::new(Opts::new("noop_gauge", "test gauge"), &["kind"]).unwrap();
    assert!(registry.unregister(Box::new(gauge_vec)));
}

#[test]
fn default_factory_creates_every_metric_family() {
    let factory = new_default_factory();

    let counter = factory
        .new_counter(Opts::new("factory_counter", "counter"))
        .unwrap();
    counter.inc();
    assert_eq!(counter.get(), 1.0);

    let counter_vec = factory
        .new_counter_vec(Opts::new("factory_counter_vec", "counter vec"), &["kind"])
        .unwrap();
    counter_vec.with_label_values(&["read"]).inc();
    assert_eq!(counter_vec.with_label_values(&["read"]).get(), 1.0);

    let gauge = factory
        .new_gauge(Opts::new("factory_gauge", "gauge"))
        .unwrap();
    gauge.set(2.0);
    assert_eq!(gauge.get(), 2.0);

    let gauge_vec = factory
        .new_gauge_vec(Opts::new("factory_gauge_vec", "gauge vec"), &["kind"])
        .unwrap();
    gauge_vec.with_label_values(&["write"]).set(3.0);
    assert_eq!(gauge_vec.with_label_values(&["write"]).get(), 3.0);

    let histogram = factory
        .new_histogram(HistogramOpts::new("factory_histogram", "histogram"))
        .unwrap();
    histogram.observe(4.0);
    assert_eq!(histogram.get_sample_count(), 1);

    let histogram_vec = factory
        .new_histogram_vec(
            HistogramOpts::new("factory_histogram_vec", "histogram vec"),
            &["kind"],
        )
        .unwrap();
    histogram_vec.with_label_values(&["scan"]).observe(5.0);
    assert_eq!(
        histogram_vec
            .with_label_values(&["scan"])
            .get_sample_count(),
        1
    );
}

#[test]
fn default_registry_rejects_duplicates_and_reports_unregistration() {
    let registry = new_default_registry();
    registry.must_register(vec![Box::new(counter("default_registry_counter"))]);
    assert!(registry
        .register(Box::new(counter("default_registry_counter")))
        .is_err());
    assert!(registry.unregister(Box::new(counter("default_registry_counter"))));
    assert!(!registry.unregister(Box::new(counter("default_registry_counter"))));

    let duplicate_registry = new_default_registry();
    duplicate_registry.must_register(vec![Box::new(counter("must_register_counter"))]);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        duplicate_registry.must_register(vec![Box::new(counter("must_register_counter"))]);
    }));
    assert!(panic.is_err());
}
