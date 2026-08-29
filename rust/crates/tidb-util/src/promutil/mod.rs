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

//! Native Prometheus metric factories and registries from Go
//! `pkg/util/promutil`.

use prometheus::{
    core::Collector, Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec,
};

/// Go `prometheus.CounterOpts` represented by the native Rust option type.
pub type CounterOpts = prometheus::Opts;

/// Go `prometheus.GaugeOpts` represented by the native Rust option type.
pub type GaugeOpts = prometheus::Opts;

/// Creates native Prometheus metric families.
pub trait Factory: Send + Sync {
    /// Go `NewCounter`.
    fn new_counter(&self, opts: CounterOpts) -> Counter;

    /// Go `NewCounterVec`.
    fn new_counter_vec(&self, opts: CounterOpts, label_names: &[&str]) -> CounterVec;

    /// Go `NewGauge`.
    fn new_gauge(&self, opts: GaugeOpts) -> Gauge;

    /// Go `NewGaugeVec`.
    fn new_gauge_vec(&self, opts: GaugeOpts, label_names: &[&str]) -> GaugeVec;

    /// Go `NewHistogram`.
    fn new_histogram(&self, opts: HistogramOpts) -> Histogram;

    /// Go `NewHistogramVec`.
    fn new_histogram_vec(&self, opts: HistogramOpts, label_names: &[&str]) -> HistogramVec;
}

#[derive(Clone, Copy, Debug, Default)]
struct DefaultFactory;

impl Factory for DefaultFactory {
    fn new_counter(&self, opts: CounterOpts) -> Counter {
        Counter::with_opts(opts).unwrap_or_else(|error| panic!("{error}"))
    }

    fn new_counter_vec(&self, opts: CounterOpts, label_names: &[&str]) -> CounterVec {
        CounterVec::new(opts, label_names).unwrap_or_else(|error| panic!("{error}"))
    }

    fn new_gauge(&self, opts: GaugeOpts) -> Gauge {
        Gauge::with_opts(opts).unwrap_or_else(|error| panic!("{error}"))
    }

    fn new_gauge_vec(&self, opts: GaugeOpts, label_names: &[&str]) -> GaugeVec {
        GaugeVec::new(opts, label_names).unwrap_or_else(|error| panic!("{error}"))
    }

    fn new_histogram(&self, opts: HistogramOpts) -> Histogram {
        Histogram::with_opts(opts).unwrap_or_else(|error| panic!("{error}"))
    }

    fn new_histogram_vec(&self, opts: HistogramOpts, label_names: &[&str]) -> HistogramVec {
        HistogramVec::new(opts, label_names).unwrap_or_else(|error| panic!("{error}"))
    }
}

/// Returns the default implementation of [`Factory`].
#[must_use]
pub fn new_default_factory() -> Box<dyn Factory> {
    Box::new(DefaultFactory)
}

/// Registers or unregisters native Prometheus collectors.
pub trait Registry: Send + Sync {
    /// Registers one collector.
    fn register(&self, collector: Box<dyn Collector>) -> prometheus::Result<()>;

    /// Registers every collector, panicking on the first error.
    fn must_register(&self, collectors: Vec<Box<dyn Collector>>);

    /// Unregisters an equivalent collector and reports whether it existed.
    fn unregister(&self, collector: Box<dyn Collector>) -> bool;
}

impl Registry for prometheus::Registry {
    fn register(&self, collector: Box<dyn Collector>) -> prometheus::Result<()> {
        prometheus::Registry::register(self, collector)
    }

    fn must_register(&self, collectors: Vec<Box<dyn Collector>>) {
        for collector in collectors {
            prometheus::Registry::register(self, collector)
                .unwrap_or_else(|error| panic!("failed to register collector: {error}"));
        }
    }

    fn unregister(&self, collector: Box<dyn Collector>) -> bool {
        prometheus::Registry::unregister(self, collector).is_ok()
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct NoopRegistry;

impl Registry for NoopRegistry {
    fn register(&self, _collector: Box<dyn Collector>) -> prometheus::Result<()> {
        Ok(())
    }

    fn must_register(&self, _collectors: Vec<Box<dyn Collector>>) {}

    fn unregister(&self, _collector: Box<dyn Collector>) -> bool {
        true
    }
}

/// Returns a registry that accepts and discards every operation.
#[must_use]
pub fn new_noop_registry() -> Box<dyn Registry> {
    Box::new(NoopRegistry)
}

/// Returns a fresh native Prometheus registry.
#[must_use]
pub fn new_default_registry() -> Box<dyn Registry> {
    Box::new(prometheus::Registry::new())
}

#[cfg(test)]
mod tests;
