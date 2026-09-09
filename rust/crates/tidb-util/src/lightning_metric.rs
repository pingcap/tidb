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

//! Prometheus collectors from Go `pkg/lightning/metric`.

use std::collections::HashMap;
use std::sync::Arc;

use prometheus::core::{Collector, MetricVec, MetricVecBuilder};
use prometheus::{
    exponential_buckets, proto, Counter, CounterVec, GaugeVec, Histogram, HistogramOpts,
    HistogramVec, Opts,
};

use crate::promutil::{Factory, Registry};

/// The pending table state.
pub const TABLE_STATE_PENDING: &str = "pending";
/// The imported table state.
pub const TABLE_STATE_IMPORTED: &str = "imported";
/// The completed table state.
pub const TABLE_STATE_COMPLETED: &str = "completed";
/// The total-restore metric state.
pub const STATE_TOTAL_RESTORE: &str = "total_restore";
/// The restored metric state.
pub const STATE_RESTORED: &str = "restored";
/// The written metric state.
pub const STATE_RESTORE_WRITTEN: &str = "written";
/// The imported metric state.
pub const STATE_IMPORTED: &str = "imported";
/// The merged metric state.
pub const STATE_MERGED: &str = "merged";
/// The total progress phase.
pub const PROGRESS_PHASE_TOTAL: &str = "total";
/// The restore progress phase.
pub const PROGRESS_PHASE_RESTORE: &str = "restore";
/// The import progress phase.
pub const PROGRESS_PHASE_IMPORT: &str = "import";
/// The successful table result.
pub const TABLE_RESULT_SUCCESS: &str = "success";
/// The failed table result.
pub const TABLE_RESULT_FAILURE: &str = "failure";
/// The estimated chunk state.
pub const CHUNK_STATE_ESTIMATED: &str = "estimated";
/// The pending chunk state.
pub const CHUNK_STATE_PENDING: &str = "pending";
/// The running chunk state.
pub const CHUNK_STATE_RUNNING: &str = "running";
/// The finished chunk state.
pub const CHUNK_STATE_FINISHED: &str = "finished";
/// The failed chunk state.
pub const CHUNK_STATE_FAILED: &str = "failed";
/// The SST split operation.
pub const SST_PROCESS_SPLIT: &str = "split";
/// The SST write operation.
pub const SST_PROCESS_WRITE: &str = "write";
/// The SST ingest operation.
pub const SST_PROCESS_INGEST: &str = "ingest";
/// The index block-delivery kind.
pub const BLOCK_DELIVER_KIND_INDEX: &str = "index";
/// The data block-delivery kind.
pub const BLOCK_DELIVER_KIND_DATA: &str = "data";

const LIGHTNING_NAMESPACE: &str = "lightning";

fn counter_opts(
    namespace: &str,
    subsystem: &str,
    name: &str,
    help: &str,
    labels: &HashMap<String, String>,
) -> Opts {
    Opts::new(name, help)
        .namespace(namespace)
        .subsystem(subsystem)
        .const_labels(labels.clone())
}

fn histogram_opts(
    namespace: &str,
    subsystem: &str,
    name: &str,
    help: &str,
    labels: &HashMap<String, String>,
    buckets: Vec<f64>,
) -> HistogramOpts {
    HistogramOpts::new(name, help)
        .namespace(namespace)
        .subsystem(subsystem)
        .const_labels(labels.clone())
        .buckets(buckets)
}

fn buckets(start: f64, factor: f64, count: usize) -> Vec<f64> {
    exponential_buckets(start, factor, count).unwrap_or_else(|error| panic!("{error}"))
}

/// Metrics shared by Lightning backends.
pub struct Common {
    /// Counts chunks by state.
    pub chunk_counter: CounterVec,
    /// Counts bytes by state.
    pub bytes_counter: CounterVec,
    /// Counts rows by state and table.
    pub rows_counter: CounterVec,
    /// Measures row-read time.
    pub row_read_seconds_histogram: Histogram,
    /// Measures row-encoding time.
    pub row_encode_seconds_histogram: Histogram,
    /// Measures block-delivery time.
    pub block_deliver_seconds_histogram: Histogram,
    /// Measures delivered block sizes.
    pub block_deliver_bytes_histogram: HistogramVec,
    /// Measures delivered block key-value counts.
    pub block_deliver_kv_pairs_histogram: HistogramVec,
}

impl Common {
    /// Creates common metrics through a Prometheus factory.
    pub fn new(
        factory: &dyn Factory,
        namespace: &str,
        subsystem: &str,
        const_labels: HashMap<String, String>,
    ) -> Self {
        Self {
            chunk_counter: factory.new_counter_vec(
                counter_opts(
                    namespace,
                    subsystem,
                    "chunks",
                    "count number of chunks processed",
                    &const_labels,
                ),
                &["state"],
            ),
            bytes_counter: factory.new_counter_vec(
                counter_opts(
                    namespace,
                    subsystem,
                    "bytes",
                    "count of total bytes",
                    &const_labels,
                ),
                &["state"],
            ),
            rows_counter: factory.new_counter_vec(
                counter_opts(
                    namespace,
                    subsystem,
                    "rows",
                    "count of total rows",
                    &const_labels,
                ),
                &["state", "table"],
            ),
            row_read_seconds_histogram: factory.new_histogram(histogram_opts(
                namespace,
                subsystem,
                "row_read_seconds",
                "time needed to parse a row(include time to read and decompress file)",
                &const_labels,
                buckets(0.001, 3.1622776601683795, 7),
            )),
            row_encode_seconds_histogram: factory.new_histogram(histogram_opts(
                namespace,
                subsystem,
                "row_encode_seconds",
                "time needed to encode a row",
                &const_labels,
                buckets(0.001, 3.1622776601683795, 10),
            )),
            block_deliver_seconds_histogram: factory.new_histogram(histogram_opts(
                namespace,
                subsystem,
                "block_deliver_seconds",
                "time needed to deliver a block",
                &const_labels,
                buckets(0.001, 3.1622776601683795, 10),
            )),
            block_deliver_bytes_histogram: factory.new_histogram_vec(
                histogram_opts(
                    namespace,
                    subsystem,
                    "block_deliver_bytes",
                    "number of bytes being sent out to importer",
                    &const_labels,
                    buckets(512.0, 2.0, 10),
                ),
                &["kind"],
            ),
            block_deliver_kv_pairs_histogram: factory.new_histogram_vec(
                histogram_opts(
                    namespace,
                    subsystem,
                    "block_deliver_kv_pairs",
                    "number of KV pairs being sent out to importer",
                    &const_labels,
                    buckets(1.0, 2.0, 10),
                ),
                &["kind"],
            ),
        }
    }

    /// Registers all common collectors.
    pub fn register_to(&self, registry: &dyn Registry) {
        registry.must_register(vec![
            Box::new(self.chunk_counter.clone()),
            Box::new(self.bytes_counter.clone()),
            Box::new(self.rows_counter.clone()),
            Box::new(self.row_read_seconds_histogram.clone()),
            Box::new(self.row_encode_seconds_histogram.clone()),
            Box::new(self.block_deliver_seconds_histogram.clone()),
            Box::new(self.block_deliver_bytes_histogram.clone()),
            Box::new(self.block_deliver_kv_pairs_histogram.clone()),
        ]);
    }

    /// Unregisters all common collectors.
    pub fn unregister_from(&self, registry: &dyn Registry) {
        for collector in self.collectors() {
            registry.unregister(collector);
        }
    }

    fn collectors(&self) -> Vec<Box<dyn Collector>> {
        vec![
            Box::new(self.chunk_counter.clone()),
            Box::new(self.bytes_counter.clone()),
            Box::new(self.rows_counter.clone()),
            Box::new(self.row_read_seconds_histogram.clone()),
            Box::new(self.row_encode_seconds_histogram.clone()),
            Box::new(self.block_deliver_seconds_histogram.clone()),
            Box::new(self.block_deliver_bytes_histogram.clone()),
            Box::new(self.block_deliver_kv_pairs_histogram.clone()),
        ]
    }
}

/// Lightning Prometheus metrics.
pub struct Metrics {
    /// Counts importer engines by type.
    pub importer_engine_counter: CounterVec,
    /// Tracks idle workers by pool name.
    pub idle_workers_gauge: GaugeVec,
    /// Counts key-value encoders by type.
    pub kv_encoder_counter: CounterVec,
    /// Counts processed tables by state and result.
    pub table_counter: CounterVec,
    /// Counts processed engines by state and result.
    pub processed_engine_counter: CounterVec,
    /// Measures table import time.
    pub import_seconds_histogram: Histogram,
    /// Measures chunk-parser block-read time.
    pub chunk_parser_read_block_seconds_histogram: Histogram,
    /// Measures worker acquisition time by pool name.
    pub apply_worker_seconds_histogram: HistogramVec,
    /// Measures key-value delivery time.
    pub row_kv_deliver_seconds_histogram: Histogram,
    /// Measures row-read byte counts.
    pub row_read_bytes_histogram: Histogram,
    /// Measures checksum time.
    pub checksum_seconds_histogram: Histogram,
    /// Measures SST operation time by kind.
    pub sst_seconds_histogram: HistogramVec,
    /// Tracks local-storage usage by medium.
    pub local_storage_usage_bytes_gauge: GaugeVec,
    /// Tracks Lightning progress by phase.
    pub progress_gauge: GaugeVec,
    /// Metrics shared by Lightning backends.
    pub common: Arc<Common>,
}

impl Metrics {
    /// Creates the Lightning metric set.
    pub fn new(factory: &dyn Factory) -> Self {
        let labels = HashMap::new();
        let common = Arc::new(Common::new(
            factory,
            LIGHTNING_NAMESPACE,
            "",
            labels.clone(),
        ));
        let counter = |name, help, vars: &[&str]| {
            factory.new_counter_vec(
                counter_opts(LIGHTNING_NAMESPACE, "", name, help, &labels),
                vars,
            )
        };
        let histogram = |name, help, start, factor, count| {
            factory.new_histogram(histogram_opts(
                LIGHTNING_NAMESPACE,
                "",
                name,
                help,
                &labels,
                buckets(start, factor, count),
            ))
        };
        let histogram_vec = |name, help, start, factor, count, vars: &[&str]| {
            factory.new_histogram_vec(
                histogram_opts(
                    LIGHTNING_NAMESPACE,
                    "",
                    name,
                    help,
                    &labels,
                    buckets(start, factor, count),
                ),
                vars,
            )
        };
        Self {
            importer_engine_counter: counter(
                "importer_engine",
                "counting open and closed importer engines",
                &["type"],
            ),
            idle_workers_gauge: factory.new_gauge_vec(
                counter_opts(
                    LIGHTNING_NAMESPACE,
                    "",
                    "idle_workers",
                    "counting idle workers",
                    &labels,
                ),
                &["name"],
            ),
            kv_encoder_counter: counter(
                "kv_encoder",
                "counting kv open and closed kv encoder",
                &["type"],
            ),
            table_counter: counter(
                "tables",
                "count number of tables processed",
                &["state", "result"],
            ),
            processed_engine_counter: counter(
                "engines",
                "count number of engines processed",
                &["state", "result"],
            ),
            import_seconds_histogram: histogram(
                "import_seconds",
                "time needed to import a table",
                0.125,
                2.0,
                6,
            ),
            chunk_parser_read_block_seconds_histogram: histogram(
                "chunk_parser_read_block_seconds",
                "time needed for chunk parser read a block",
                0.001,
                3.1622776601683795,
                10,
            ),
            apply_worker_seconds_histogram: histogram_vec(
                "apply_worker_seconds",
                "time needed to apply a worker",
                0.001,
                3.1622776601683795,
                10,
                &["name"],
            ),
            row_kv_deliver_seconds_histogram: histogram(
                "row_kv_deliver_seconds",
                "time needed to send kvs to deliver loop",
                0.001,
                3.1622776601683795,
                10,
            ),
            row_read_bytes_histogram: histogram(
                "row_read_bytes",
                "number of bytes being read out from data source",
                1024.0,
                2.0,
                8,
            ),
            checksum_seconds_histogram: histogram(
                "checksum_seconds",
                "time needed to complete the checksum stage",
                1.0,
                2.2679331552660544,
                10,
            ),
            sst_seconds_histogram: histogram_vec(
                "sst_seconds",
                "time needed to complete the sst operations",
                1.0,
                2.2679331552660544,
                10,
                &["kind"],
            ),
            local_storage_usage_bytes_gauge: factory.new_gauge_vec(
                counter_opts(
                    LIGHTNING_NAMESPACE,
                    "",
                    "local_storage_usage_bytes",
                    "disk/memory size currently occupied by intermediate files in local backend",
                    &labels,
                ),
                &["medium"],
            ),
            progress_gauge: factory.new_gauge_vec(
                counter_opts(
                    LIGHTNING_NAMESPACE,
                    "",
                    "progress",
                    "progress of lightning phase",
                    &labels,
                ),
                &["phase"],
            ),
            common,
        }
    }

    /// Registers all Lightning collectors.
    pub fn register_to(&self, registry: &dyn Registry) {
        self.common.register_to(registry);
        registry.must_register(self.collectors());
    }

    /// Unregisters all Lightning collectors.
    pub fn unregister_from(&self, registry: &dyn Registry) {
        self.common.unregister_from(registry);
        for collector in self.collectors() {
            registry.unregister(collector);
        }
    }

    fn collectors(&self) -> Vec<Box<dyn Collector>> {
        vec![
            Box::new(self.importer_engine_counter.clone()),
            Box::new(self.idle_workers_gauge.clone()),
            Box::new(self.kv_encoder_counter.clone()),
            Box::new(self.table_counter.clone()),
            Box::new(self.processed_engine_counter.clone()),
            Box::new(self.import_seconds_histogram.clone()),
            Box::new(self.chunk_parser_read_block_seconds_histogram.clone()),
            Box::new(self.apply_worker_seconds_histogram.clone()),
            Box::new(self.row_kv_deliver_seconds_histogram.clone()),
            Box::new(self.row_read_bytes_histogram.clone()),
            Box::new(self.checksum_seconds_histogram.clone()),
            Box::new(self.sst_seconds_histogram.clone()),
            Box::new(self.local_storage_usage_bytes_gauge.clone()),
            Box::new(self.progress_gauge.clone()),
        ]
    }

    /// Records one table outcome for `status`.
    pub fn record_table_count(&self, status: &str, error: Option<&dyn std::error::Error>) {
        let result = if error.is_some() {
            TABLE_RESULT_FAILURE
        } else {
            TABLE_RESULT_SUCCESS
        };
        self.table_counter
            .with_label_values(&[status, result])
            .inc();
    }

    /// Records one engine outcome for `status`.
    pub fn record_engine_count(&self, status: &str, error: Option<&dyn std::error::Error>) {
        let result = if error.is_some() {
            TABLE_RESULT_FAILURE
        } else {
            TABLE_RESULT_SUCCESS
        };
        self.processed_engine_counter
            .with_label_values(&[status, result])
            .inc();
    }
}

/// Reads the current value of a counter.
pub fn read_counter(counter: &Counter) -> f64 {
    counter.get()
}

/// Reads a histogram as its protocol metric representation.
pub fn read_histogram(histogram: &Histogram) -> Option<proto::Metric> {
    histogram
        .collect()
        .into_iter()
        .next()?
        .take_metric()
        .into_iter()
        .next()
}

fn metric_has_label(pairs: &[proto::LabelPair], labels: &HashMap<String, String>) -> bool {
    pairs.iter().any(|pair| {
        labels
            .get(pair.name())
            .is_some_and(|value| value == pair.value())
    })
}

/// Sums counters having at least one matching label from `labels`.
pub fn read_all_counters<T: MetricVecBuilder>(
    metrics: &MetricVec<T>,
    labels: &HashMap<String, String>,
) -> f64 {
    metrics
        .collect()
        .into_iter()
        .flat_map(|mut family| family.take_metric())
        .filter(|metric| metric_has_label(metric.get_label(), labels))
        .map(|metric| metric.get_counter().get_value())
        .sum()
}

/// Reads the sample sum of a histogram.
pub fn read_histogram_sum(histogram: &Histogram) -> f64 {
    histogram.get_sample_sum()
}

struct AllMetricKey;
struct CommonMetricKey;

/// Returns a context carrying all Lightning metrics.
pub fn with_metric(
    context: &tikv_client::trace::TraceContext,
    metrics: Arc<Metrics>,
) -> tikv_client::trace::TraceContext {
    with_common_metric(context, Arc::clone(&metrics.common)).with_value::<AllMetricKey, _>(metrics)
}

/// Returns a context carrying common Lightning metrics.
pub fn with_common_metric(
    context: &tikv_client::trace::TraceContext,
    metrics: Arc<Common>,
) -> tikv_client::trace::TraceContext {
    context.with_value::<CommonMetricKey, _>(metrics)
}

/// Retrieves all Lightning metrics from a context.
pub fn from_context(context: &tikv_client::trace::TraceContext) -> Option<Arc<Metrics>> {
    context.value::<AllMetricKey, Arc<Metrics>>().cloned()
}

/// Retrieves common Lightning metrics from a context.
pub fn get_common_metric(context: &tikv_client::trace::TraceContext) -> Option<Arc<Common>> {
    context.value::<CommonMetricKey, Arc<Common>>().cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::promutil::new_default_factory;

    #[test]
    fn read_counter() {
        let counter = Counter::new("read_counter", "read counter").unwrap();
        counter.inc_by(1256.0);
        counter.inc_by(2214.0);
        assert_eq!(super::read_counter(&counter), 3470.0);
    }

    #[test]
    fn read_histogram_sum() {
        let histogram =
            Histogram::with_opts(HistogramOpts::new("read_histogram", "read histogram")).unwrap();
        histogram.observe(11131.5);
        histogram.observe(15261.0);
        assert_eq!(super::read_histogram_sum(&histogram), 26392.5);
    }

    #[test]
    fn record_engine_count() {
        let factory = new_default_factory();
        let metrics = Metrics::new(factory.as_ref());
        metrics.record_engine_count("table1", None);
        let error = std::io::Error::other("mock error");
        metrics.record_engine_count("table1", Some(&error));
        assert_eq!(
            super::read_counter(
                &metrics
                    .processed_engine_counter
                    .with_label_values(&["table1", "success"])
            ),
            1.0
        );
        assert_eq!(
            super::read_counter(
                &metrics
                    .processed_engine_counter
                    .with_label_values(&["table1", "failure"])
            ),
            1.0
        );
    }

    #[test]
    fn metrics_register() {
        let factory = new_default_factory();
        let common = Common::new(factory.as_ref(), "test", "", HashMap::new());
        let registry = prometheus::Registry::new();
        assert_eq!(common.collectors().len(), 8);
        common.register_to(&registry);
        for collector in common.collectors() {
            assert!(Registry::unregister(&registry, collector));
        }
        let metrics = Metrics::new(factory.as_ref());
        let registry = prometheus::Registry::new();
        metrics.register_to(&registry);
        assert_eq!(
            metrics.common.collectors().len() + metrics.collectors().len(),
            22
        );
        for collector in metrics
            .common
            .collectors()
            .into_iter()
            .chain(metrics.collectors())
        {
            assert!(Registry::unregister(&registry, collector));
        }
    }

    #[test]
    fn metrics_unregister() {
        let factory = new_default_factory();
        let metrics = Metrics::new(factory.as_ref());
        let registry = prometheus::Registry::new();
        metrics.register_to(&registry);
        metrics.unregister_from(&registry);
        for collector in metrics
            .common
            .collectors()
            .into_iter()
            .chain(metrics.collectors())
        {
            assert!(!Registry::unregister(&registry, collector));
        }
    }

    #[test]
    fn context() {
        let factory = new_default_factory();
        let metrics = Arc::new(Metrics::new(factory.as_ref()));
        let context = with_metric(
            &tikv_client::trace::TraceContext::default(),
            Arc::clone(&metrics),
        );
        assert!(from_context(&context).is_some());
        assert!(get_common_metric(&context).is_some());
        let context = with_common_metric(
            &tikv_client::trace::TraceContext::default(),
            Arc::clone(&metrics.common),
        );
        assert!(from_context(&context).is_none());
        assert!(get_common_metric(&context).is_some());
    }
}
