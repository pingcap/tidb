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

//! Complete Rust owner for Go `pkg/util/topsql/reporter/metrics`.
//!
//! The three metric families mirror Go's `pkg/metrics/topsql.go`; the
//! per-label handles and their startup initialization mirror `metrics.go`.

use prometheus::{Counter, CounterVec, Histogram, HistogramOpts, HistogramVec, Opts};
use std::sync::LazyLock;

fn register<C: prometheus::core::Collector + Clone + 'static>(
    collector: prometheus::Result<C>,
) -> C {
    let collector = collector.expect("valid TopSQL metric definition");
    prometheus::default_registry()
        .register(Box::new(collector.clone()))
        .expect("TopSQL metric registered once");
    collector
}

static TOPSQL_IGNORED_TOTAL: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new(
            "tidb_topsql_ignored_total",
            "Counter of ignored top-sql metrics (register-sql, register-plan, collect-data and report-data), normally it should be 0.",
        ),
        &["type"],
    ))
});

static TOPSQL_REPORT_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_topsql_report_duration_seconds",
            "Bucket histogram of reporting time (s) to the top-sql agent",
        )
        .buckets(prometheus::exponential_buckets(0.001, 2.0, 24).expect("valid buckets")),
        &["type", "result"],
    ))
});

static TOPSQL_REPORT_DATA: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_topsql_report_data_total",
            "Bucket histogram of reporting records/sql/plan count to the top-sql agent.",
        )
        .buckets(prometheus::exponential_buckets(1.0, 2.0, 20).expect("valid buckets")),
        &["type"],
    ))
});

macro_rules! ignore_counter {
    ($name:ident, $label:literal, $doc:literal) => {
        #[doc = $doc]
        pub static $name: LazyLock<Counter> =
            LazyLock::new(|| TOPSQL_IGNORED_TOTAL.with_label_values(&[$label]));
    };
}

ignore_counter!(
    IGNORE_EXCEED_SQL_COUNTER,
    "ignore_exceed_sql",
    "Go `IgnoreExceedSQLCounter`: SQL metadata rejected by the collection cap."
);
ignore_counter!(
    IGNORE_EXCEED_PLAN_COUNTER,
    "ignore_exceed_plan",
    "Go `IgnoreExceedPlanCounter`: plan metadata rejected by the collection cap."
);
ignore_counter!(
    IGNORE_EXCEED_RU_KEYS_COUNTER,
    "ignore_exceed_ru_keys",
    "Go `IgnoreExceedRUKeysCounter`: RU keys rejected by the aggregation cap."
);
ignore_counter!(
    IGNORE_EXCEED_RU_TOTAL_COUNTER,
    "ignore_exceed_ru_total",
    "Go `IgnoreExceedRUTotalCounter`: RU total rejected by the aggregation cap."
);
ignore_counter!(
    IGNORE_LATE_COMPACTED_RU_KEYS_COUNTER,
    "ignore_late_compacted_ru_keys",
    "Go `IgnoreLateCompactedRUKeysCounter`: late RU keys dropped after bucket compaction."
);
ignore_counter!(
    IGNORE_LATE_COMPACTED_RU_TOTAL_COUNTER,
    "ignore_late_compacted_ru_total",
    "Go `IgnoreLateCompactedRUTotalCounter`: RU total dropped after bucket compaction."
);
ignore_counter!(
    IGNORE_COLLECT_CHANNEL_FULL_COUNTER,
    "ignore_collect_channel_full",
    "Go `IgnoreCollectChannelFullCounter`: collection batches dropped because the channel was full."
);
ignore_counter!(
    IGNORE_COLLECT_STMT_CHANNEL_FULL_COUNTER,
    "ignore_collect_stmt_channel_full",
    "Go `IgnoreCollectStmtChannelFullCounter`: statement batches dropped because the channel was full."
);
ignore_counter!(
    IGNORE_COLLECT_RU_CHANNEL_FULL_COUNTER,
    "ignore_collect_ru_channel_full",
    "Go `IgnoreCollectRUChannelFullCounter`: RU batches dropped because the channel was full."
);
ignore_counter!(
    IGNORE_REPORT_CHANNEL_FULL_COUNTER,
    "ignore_report_channel_full",
    "Go `IgnoreReportChannelFullCounter`: report batches dropped because the channel was full."
);
ignore_counter!(
    IGNORE_REPORT_DATA_BY_BACKPRESSURE_COUNTER,
    "ignore_report_data_by_backpressure",
    "Go `IgnoreReportDataByBackpressureCounter`: report windows dropped under backpressure."
);

macro_rules! duration_histogram {
    ($name:ident, $kind:literal, $result:literal, $doc:literal) => {
        #[doc = $doc]
        pub static $name: LazyLock<Histogram> =
            LazyLock::new(|| TOPSQL_REPORT_DURATION.with_label_values(&[$kind, $result]));
    };
}

duration_histogram!(
    REPORT_ALL_DURATION_SUCC_HISTOGRAM,
    "all",
    "ok",
    "Go `ReportAllDurationSuccHistogram`."
);
duration_histogram!(
    REPORT_ALL_DURATION_FAILED_HISTOGRAM,
    "all",
    "error",
    "Go `ReportAllDurationFailedHistogram`."
);
duration_histogram!(
    REPORT_RECORD_DURATION_SUCC_HISTOGRAM,
    "record",
    "ok",
    "Go `ReportRecordDurationSuccHistogram`."
);
duration_histogram!(
    REPORT_RECORD_DURATION_FAILED_HISTOGRAM,
    "record",
    "error",
    "Go `ReportRecordDurationFailedHistogram`."
);
duration_histogram!(
    REPORT_SQL_DURATION_SUCC_HISTOGRAM,
    "sql",
    "ok",
    "Go `ReportSQLDurationSuccHistogram`."
);
duration_histogram!(
    REPORT_SQL_DURATION_FAILED_HISTOGRAM,
    "sql",
    "error",
    "Go `ReportSQLDurationFailedHistogram`."
);
duration_histogram!(
    REPORT_PLAN_DURATION_SUCC_HISTOGRAM,
    "plan",
    "ok",
    "Go `ReportPlanDurationSuccHistogram`."
);
duration_histogram!(
    REPORT_PLAN_DURATION_FAILED_HISTOGRAM,
    "plan",
    "error",
    "Go `ReportPlanDurationFailedHistogram`."
);
duration_histogram!(
    REPORT_RU_RECORD_DURATION_SUCC_HISTOGRAM,
    "ru_record",
    "ok",
    "Go `ReportRURecordDurationSuccHistogram`."
);
duration_histogram!(
    REPORT_RU_RECORD_DURATION_FAILED_HISTOGRAM,
    "ru_record",
    "error",
    "Go `ReportRURecordDurationFailedHistogram`."
);

macro_rules! data_histogram {
    ($name:ident, $kind:literal, $doc:literal) => {
        #[doc = $doc]
        pub static $name: LazyLock<Histogram> =
            LazyLock::new(|| TOPSQL_REPORT_DATA.with_label_values(&[$kind]));
    };
}

data_histogram!(
    TOPSQL_REPORT_RECORD_COUNTER_HISTOGRAM,
    "record",
    "Go `TopSQLReportRecordCounterHistogram`."
);
data_histogram!(
    TOPSQL_REPORT_RU_RECORD_COUNTER_HISTOGRAM,
    "ru_record",
    "Go `TopSQLReportRURecordCounterHistogram`."
);
data_histogram!(
    TOPSQL_REPORT_SQL_COUNT_HISTOGRAM,
    "sql",
    "Go `TopSQLReportSQLCountHistogram`."
);
data_histogram!(
    TOPSQL_REPORT_PLAN_COUNT_HISTOGRAM,
    "plan",
    "Go `TopSQLReportPlanCountHistogram`."
);

/// Go `InitMetricsVars`: binds every reporter metric handle to its labeled
/// process-wide vector. Go calls this from package `init`; Rust calls it when
/// the server initializes its metric families.
pub fn init_metrics_vars() {
    for metric in [
        &IGNORE_EXCEED_SQL_COUNTER,
        &IGNORE_EXCEED_PLAN_COUNTER,
        &IGNORE_EXCEED_RU_KEYS_COUNTER,
        &IGNORE_EXCEED_RU_TOTAL_COUNTER,
        &IGNORE_LATE_COMPACTED_RU_KEYS_COUNTER,
        &IGNORE_LATE_COMPACTED_RU_TOTAL_COUNTER,
        &IGNORE_COLLECT_CHANNEL_FULL_COUNTER,
        &IGNORE_COLLECT_STMT_CHANNEL_FULL_COUNTER,
        &IGNORE_COLLECT_RU_CHANNEL_FULL_COUNTER,
        &IGNORE_REPORT_CHANNEL_FULL_COUNTER,
        &IGNORE_REPORT_DATA_BY_BACKPRESSURE_COUNTER,
    ] {
        LazyLock::force(metric);
    }
    for metric in [
        &REPORT_ALL_DURATION_SUCC_HISTOGRAM,
        &REPORT_ALL_DURATION_FAILED_HISTOGRAM,
        &REPORT_RECORD_DURATION_SUCC_HISTOGRAM,
        &REPORT_RECORD_DURATION_FAILED_HISTOGRAM,
        &REPORT_SQL_DURATION_SUCC_HISTOGRAM,
        &REPORT_SQL_DURATION_FAILED_HISTOGRAM,
        &REPORT_PLAN_DURATION_SUCC_HISTOGRAM,
        &REPORT_PLAN_DURATION_FAILED_HISTOGRAM,
        &REPORT_RU_RECORD_DURATION_SUCC_HISTOGRAM,
        &REPORT_RU_RECORD_DURATION_FAILED_HISTOGRAM,
        &TOPSQL_REPORT_RECORD_COUNTER_HISTOGRAM,
        &TOPSQL_REPORT_RU_RECORD_COUNTER_HISTOGRAM,
        &TOPSQL_REPORT_SQL_COUNT_HISTOGRAM,
        &TOPSQL_REPORT_PLAN_COUNT_HISTOGRAM,
    ] {
        LazyLock::force(metric);
    }
}

/// The histogram families and help strings exposed by Go's `pkg/metrics`.
pub fn histogram_definitions() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "tidb_topsql_report_data_total",
            "Bucket histogram of reporting records/sql/plan count to the top-sql agent.",
        ),
        (
            "tidb_topsql_report_duration_seconds",
            "Bucket histogram of reporting time (s) to the top-sql agent",
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_metrics_vars_registers_every_go_label_series() {
        init_metrics_vars();

        let families = prometheus::default_registry().gather();
        let family = |name: &str| {
            families
                .iter()
                .find(|family| family.name() == name)
                .unwrap_or_else(|| panic!("missing metric family {name}"))
        };
        let labels = |family: &prometheus::proto::MetricFamily| {
            let mut values = family
                .get_metric()
                .iter()
                .map(|metric| {
                    let mut labels = metric
                        .get_label()
                        .iter()
                        .map(|label| (label.name().to_owned(), label.value().to_owned()))
                        .collect::<Vec<_>>();
                    labels.sort();
                    labels
                })
                .collect::<Vec<_>>();
            values.sort();
            values
        };

        assert_eq!(
            labels(family("tidb_topsql_ignored_total")),
            [
                "ignore_collect_channel_full",
                "ignore_collect_ru_channel_full",
                "ignore_collect_stmt_channel_full",
                "ignore_exceed_plan",
                "ignore_exceed_ru_keys",
                "ignore_exceed_ru_total",
                "ignore_exceed_sql",
                "ignore_late_compacted_ru_keys",
                "ignore_late_compacted_ru_total",
                "ignore_report_channel_full",
                "ignore_report_data_by_backpressure",
            ]
            .map(|value| vec![("type".to_owned(), value.to_owned())])
        );
        let mut expected_duration_labels = [
            ("all", "error"),
            ("all", "ok"),
            ("plan", "error"),
            ("plan", "ok"),
            ("record", "error"),
            ("record", "ok"),
            ("ru_record", "error"),
            ("ru_record", "ok"),
            ("sql", "error"),
            ("sql", "ok"),
        ]
        .map(|(kind, result)| {
            let mut labels = vec![
                ("type".to_owned(), kind.to_owned()),
                ("result".to_owned(), result.to_owned()),
            ];
            labels.sort();
            labels
        });
        expected_duration_labels.sort();
        assert_eq!(
            labels(family("tidb_topsql_report_duration_seconds")),
            expected_duration_labels
        );
        assert_eq!(
            labels(family("tidb_topsql_report_data_total")),
            ["plan", "record", "ru_record", "sql"]
                .map(|value| vec![("type".to_owned(), value.to_owned())])
        );

        assert_eq!(
            histogram_definitions(),
            vec![
                (
                    "tidb_topsql_report_data_total",
                    "Bucket histogram of reporting records/sql/plan count to the top-sql agent.",
                ),
                (
                    "tidb_topsql_report_duration_seconds",
                    "Bucket histogram of reporting time (s) to the top-sql agent",
                ),
            ]
        );
    }

    #[test]
    fn ignore_report_data_by_backpressure_counter_increments() {
        init_metrics_vars();
        let counter = LazyLock::force(&IGNORE_REPORT_DATA_BY_BACKPRESSURE_COUNTER);
        let before = counter.get();
        counter.inc();
        assert_eq!(before + 1.0, counter.get());
    }
}
