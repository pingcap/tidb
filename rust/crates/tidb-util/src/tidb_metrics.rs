//! Prometheus metrics whose public names are owned by TiDB's Go server.

use prometheus::{exponential_buckets, Gauge, Histogram, HistogramOpts, HistogramVec, Opts};
use std::sync::OnceLock;

fn register<T: prometheus::core::Collector + Clone + 'static>(metric: &T) {
    let _ = prometheus::default_registry().register(Box::new(metric.clone()));
}

fn session_histogram(name: &str, help: &str, buckets: Vec<f64>) -> HistogramVec {
    let metric = HistogramVec::new(
        HistogramOpts::new(name, help)
            .namespace("tidb")
            .subsystem("session")
            .buckets(buckets),
        &["sql_type"],
    )
    .expect("valid TiDB session histogram");
    register(&metric);
    metric
}

fn server_histogram(name: &str, help: &str, buckets: Vec<f64>) -> Histogram {
    let metric = Histogram::with_opts(
        HistogramOpts::new(name, help)
            .namespace("tidb")
            .subsystem("server")
            .buckets(buckets),
    )
    .expect("valid TiDB server histogram");
    register(&metric);
    metric
}

fn subsystem_histogram_vec(
    subsystem: &str,
    name: &str,
    help: &str,
    labels: &[&str],
    buckets: Vec<f64>,
) -> HistogramVec {
    let metric = HistogramVec::new(
        HistogramOpts::new(name, help)
            .namespace("tidb")
            .subsystem(subsystem)
            .buckets(buckets),
        labels,
    )
    .expect("valid TiDB histogram vector");
    register(&metric);
    metric
}

pub const GENERAL_SQL_TYPE: &str = "general";

pub fn parse_duration() -> &'static HistogramVec {
    static METRIC: OnceLock<HistogramVec> = OnceLock::new();
    METRIC.get_or_init(|| {
        session_histogram(
            "parse_duration_seconds",
            "Bucketed histogram of processing time (s) in parse SQL.",
            exponential_buckets(0.00004, 2.0, 28).unwrap(),
        )
    })
}

pub fn compile_duration() -> &'static HistogramVec {
    static METRIC: OnceLock<HistogramVec> = OnceLock::new();
    METRIC.get_or_init(|| {
        session_histogram(
            "compile_duration_seconds",
            "Bucketed histogram of processing time (s) in query optimize.",
            exponential_buckets(0.00004, 2.0, 28).unwrap(),
        )
    })
}

pub fn execute_duration() -> &'static HistogramVec {
    static METRIC: OnceLock<HistogramVec> = OnceLock::new();
    METRIC.get_or_init(|| {
        session_histogram(
            "execute_duration_seconds",
            "Bucketed histogram of processing time (s) in running executor.",
            exponential_buckets(0.0001, 2.0, 30).unwrap(),
        )
    })
}

pub fn get_token_duration() -> &'static Histogram {
    static METRIC: OnceLock<Histogram> = OnceLock::new();
    METRIC.get_or_init(|| server_histogram("get_token_duration_seconds", "Duration (us) for getting token, it should be small until concurrency limit is reached.", exponential_buckets(1.0, 2.0, 30).unwrap()))
}

pub fn server_tokens() -> &'static Gauge {
    static METRIC: OnceLock<Gauge> = OnceLock::new();
    METRIC.get_or_init(|| {
        let metric = Gauge::with_opts(
            Opts::new("tokens", "Number of available server tokens.")
                .namespace("tidb")
                .subsystem("server"),
        )
        .expect("valid token gauge");
        register(&metric);
        metric
    })
}

pub fn transaction_duration() -> &'static HistogramVec {
    static METRIC: OnceLock<HistogramVec> = OnceLock::new();
    METRIC.get_or_init(|| {
        subsystem_histogram_vec(
            "session",
            "transaction_duration_seconds",
            "Bucketed histogram of a transaction execution duration, including retry.",
            &["txn_mode", "type", "scope"],
            exponential_buckets(0.001, 2.0, 28).unwrap(),
        )
    })
}

pub fn tikv_request_duration() -> &'static HistogramVec {
    static METRIC: OnceLock<HistogramVec> = OnceLock::new();
    METRIC.get_or_init(|| {
        subsystem_histogram_vec(
            "tikvclient",
            "request_seconds",
            "Bucketed histogram of sending request duration.",
            &["type", "store", "stale_read", "scope"],
            exponential_buckets(0.0005, 2.0, 24).unwrap(),
        )
    })
}

pub fn txn_write_size() -> &'static HistogramVec {
    static METRIC: OnceLock<HistogramVec> = OnceLock::new();
    METRIC.get_or_init(|| {
        subsystem_histogram_vec(
            "tikvclient",
            "txn_write_size_bytes",
            "Size of kv pairs to write in a transaction.",
            &["scope"],
            exponential_buckets(16.0, 4.0, 17).unwrap(),
        )
    })
}

pub fn init() {
    parse_duration().with_label_values(&[GENERAL_SQL_TYPE]);
    compile_duration().with_label_values(&[GENERAL_SQL_TYPE]);
    execute_duration().with_label_values(&[GENERAL_SQL_TYPE]);
    let _ = get_token_duration();
    let _ = server_tokens();
    transaction_duration().with_label_values(&["pessimistic", "Query", GENERAL_SQL_TYPE]);
    tikv_request_duration().with_label_values(&["Cop", "0", "false", GENERAL_SQL_TYPE]);
    txn_write_size().with_label_values(&[GENERAL_SQL_TYPE]);
}
