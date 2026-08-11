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
//!
//! Construction delegates to the Rust Prometheus client. Unlike the Go
//! client, the Rust client validates descriptors at construction, so Factory
//! methods return its native [`prometheus::Result`] instead of adding panics.

pub use prometheus::{
    core::Collector, Counter, CounterVec, Error as PrometheusError, Gauge, GaugeVec, Histogram,
    HistogramOpts, HistogramVec, Opts, Result as PrometheusResult,
};

/// Go `prometheus.CounterOpts` represented by the native Rust option type.
pub type CounterOpts = Opts;

/// Go `prometheus.GaugeOpts` represented by the native Rust option type.
pub type GaugeOpts = Opts;

/// Creates native Prometheus metric families.
pub trait Factory: Send + Sync {
    /// Go `NewCounter`.
    fn new_counter(&self, opts: CounterOpts) -> PrometheusResult<Counter>;

    /// Go `NewCounterVec`.
    fn new_counter_vec(
        &self,
        opts: CounterOpts,
        label_names: &[&str],
    ) -> PrometheusResult<CounterVec>;

    /// Go `NewGauge`.
    fn new_gauge(&self, opts: GaugeOpts) -> PrometheusResult<Gauge>;

    /// Go `NewGaugeVec`.
    fn new_gauge_vec(&self, opts: GaugeOpts, label_names: &[&str]) -> PrometheusResult<GaugeVec>;

    /// Go `NewHistogram`.
    fn new_histogram(&self, opts: HistogramOpts) -> PrometheusResult<Histogram>;

    /// Go `NewHistogramVec`.
    fn new_histogram_vec(
        &self,
        opts: HistogramOpts,
        label_names: &[&str],
    ) -> PrometheusResult<HistogramVec>;
}

#[derive(Clone, Copy, Debug, Default)]
struct DefaultFactory;

impl Factory for DefaultFactory {
    fn new_counter(&self, opts: CounterOpts) -> PrometheusResult<Counter> {
        Counter::with_opts(opts)
    }

    fn new_counter_vec(
        &self,
        opts: CounterOpts,
        label_names: &[&str],
    ) -> PrometheusResult<CounterVec> {
        CounterVec::new(opts, label_names)
    }

    fn new_gauge(&self, opts: GaugeOpts) -> PrometheusResult<Gauge> {
        Gauge::with_opts(opts)
    }

    fn new_gauge_vec(&self, opts: GaugeOpts, label_names: &[&str]) -> PrometheusResult<GaugeVec> {
        GaugeVec::new(opts, label_names)
    }

    fn new_histogram(&self, opts: HistogramOpts) -> PrometheusResult<Histogram> {
        Histogram::with_opts(opts)
    }

    fn new_histogram_vec(
        &self,
        opts: HistogramOpts,
        label_names: &[&str],
    ) -> PrometheusResult<HistogramVec> {
        HistogramVec::new(opts, label_names)
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
    fn register(&self, collector: Box<dyn Collector>) -> PrometheusResult<()>;

    /// Registers every collector, panicking on the first error.
    fn must_register(&self, collectors: Vec<Box<dyn Collector>>);

    /// Unregisters an equivalent collector and reports whether it existed.
    fn unregister(&self, collector: Box<dyn Collector>) -> bool;
}

impl Registry for prometheus::Registry {
    fn register(&self, collector: Box<dyn Collector>) -> PrometheusResult<()> {
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
    fn register(&self, _collector: Box<dyn Collector>) -> PrometheusResult<()> {
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
