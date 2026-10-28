// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `pkg/metrics/server.go` (plus the server-layer families declared in
//! `pkg/metrics/metrics.go`): the `tidb_server_*`, `tidb_monitor_*`, and
//! `tidb_config_*` families the status server exports for the TiDB Grafana
//! dashboards.
//!
//! Every definition below is transcribed one for one from the Go source —
//! name, help string, labels, and buckets — with the Go symbol named in a
//! comment. Families whose bump sites live in subsystems this node has not
//! transcreated yet (TiFlash, PD HTTP APIs, IA remote reads)
//! are registered exactly as Go registers them at init, so the exported
//! family set matches even where the series stay at Go's init state.
//!
//! The two query families Go also declares here — `tidb_server_query_total`
//! and `tidb_server_handle_query_duration_seconds` — stay in
//! [`crate::query_metrics`], which transcribes `clientConn.addQueryMetrics`
//! together with the dispatch scope guard.

use prometheus::{
    Counter, CounterVec, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounter,
    IntCounterVec, IntGauge, IntGaugeVec, Opts,
};
use std::sync::LazyLock;

/// Go `pkg/metrics` label constants (`session.go:265-303`).
pub mod labels {
    pub const TYPE: &str = "type";
    pub const DB: &str = "db";
    pub const RESULT: &str = "result";
    pub const SQL_TYPE: &str = "sql_type";
    pub const IN_TXN: &str = "in_txn";
    pub const VERSION: &str = "version";
    pub const HASH: &str = "hash";
    pub const ADDRESS: &str = "address";
    pub const MODULE: &str = "module";
    pub const RESOURCE_GROUP: &str = "resource_group";

    /// Go `metrics.LblOK`.
    pub const OK: &str = "ok";
    /// Go `metrics.LblError`.
    pub const ERROR: &str = "error";
    /// Go `pkg/server/metrics`: `DisconnectErrorUndetermined`.
    pub const UNDETERMINED: &str = "undetermined";
}

fn register<C: prometheus::core::Collector + Clone + 'static>(
    collector: prometheus::Result<C>,
) -> C {
    let collector = collector.expect("valid server metric definition");
    if let Err(error) = prometheus::default_registry().register(Box::new(collector.clone())) {
        // A duplicate metric family must not take the node down: the first
        // registration keeps serving the family, so the data Go would
        // export is still exported. Warn once at startup instead.
        let names: Vec<String> = collector
            .desc()
            .iter()
            .map(|desc| desc.fq_name.to_owned())
            .collect();
        eprintln!("[[server metric skipped as already registered: {error:?} for {names:?}]]");
    }
    collector
}

/// Go `metrics.PacketIOCounter`.
pub static PACKET_IO_BYTES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register(IntCounterVec::new(
        Opts::new(
            "tidb_server_packet_io_bytes",
            "Counters of packet IO bytes.",
        ),
        &[labels::TYPE],
    ))
});

/// Go `metrics.QueryRPCHistogram`.
pub static QUERY_STATEMENT_RPC_COUNT: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_server_query_statement_rpc_count",
            "Bucketed histogram of execution rpc count of handled query statements.",
        )
        .buckets(prometheus::exponential_buckets(1.0, 1.5, 23).expect("valid buckets")),
        &[labels::SQL_TYPE, labels::DB],
    ))
});

/// Go `metrics.QueryProcessedKeyHistogram`.
pub static QUERY_STATEMENT_PROCESSED_KEYS: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_server_query_statement_processed_keys",
            "Bucketed histogram of processed key count during the scan of handled query statements.",
        )
        .buckets(prometheus::exponential_buckets(1.0, 2.0, 32).expect("valid buckets")),
        &[labels::SQL_TYPE, labels::DB],
    ))
});

/// Go `metrics.IARemoteReadSegmentCount`.
pub static IA_REMOTE_READ_SEGMENT_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register(IntCounterVec::new(
        Opts::new(
            "tidb_server_ia_remote_read_segment_count",
            "Counter of IA remote read segments observed by TiDB.",
        ),
        &[labels::SQL_TYPE, labels::DB],
    ))
});

/// Go `metrics.IARemoteReadSegmentSize`.
pub static IA_REMOTE_READ_SEGMENT_SIZE_BYTES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register(IntCounterVec::new(
        Opts::new(
            "tidb_server_ia_remote_read_segment_size_bytes",
            "Counter of IA remote read segment bytes observed by TiDB.",
        ),
        &[labels::SQL_TYPE, labels::DB],
    ))
});

/// Go `metrics.IARemoteReadSegmentWaitDuration`.
pub static IA_REMOTE_READ_SEGMENT_WAIT_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_server_ia_remote_read_segment_wait_duration_seconds",
            "Bucketed histogram of IA remote read segment wait time observed by TiDB.",
        )
        .buckets(prometheus::exponential_buckets(0.00005, 2.0, 20).expect("valid buckets")),
        &[labels::SQL_TYPE, labels::DB],
    ))
});

/// Go `metrics.ConnGauge`.
pub static CONN_GAUGE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register(IntGaugeVec::new(
        Opts::new("tidb_server_connections", "Number of connections."),
        &[labels::RESOURCE_GROUP],
    ))
});

/// Go `metrics.DisconnectionCounter`, bound one for one with Go
/// `pkg/server/metrics` `DisconnectNormal` / `DisconnectByClientWithError` /
/// `DisconnectErrorUndetermined`.
pub static DISCONNECTION_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register(IntCounterVec::new(
        Opts::new(
            "tidb_server_disconnection_total",
            "Counter of connections disconnected.",
        ),
        &[labels::RESULT],
    ))
});

/// Go `metrics.PreparedStmtGauge`.
pub static PREPARED_STMTS: LazyLock<IntGauge> = LazyLock::new(|| {
    register(IntGauge::new(
        "tidb_server_prepared_stmts",
        "number of prepared statements.",
    ))
});

/// Go `metrics.ExecuteErrorCounter`.
pub static EXECUTE_ERROR_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register(IntCounterVec::new(
        Opts::new(
            "tidb_server_execute_error_total",
            "Counter of execute errors.",
        ),
        &[labels::TYPE, labels::DB, labels::RESOURCE_GROUP],
    ))
});

/// Go `metrics.CriticalErrorCounter`.
pub static CRITICAL_ERROR_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    register(IntCounter::new(
        "tidb_server_critical_error_total",
        "Counter of critical errors.",
    ))
});

/// Go `metrics.ServerEventCounter`, with Go's event label values
/// `ServerStart` / `ServerStop` / `EventKill`.
pub static EVENT_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register(IntCounterVec::new(
        Opts::new("tidb_server_event_total", "Counter of tidb-server event."),
        &[labels::TYPE],
    ))
});

/// Go `metrics.TimeJumpBackCounter`.
pub static TIME_JUMP_BACK_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    register(IntCounter::new(
        "tidb_monitor_time_jump_back_total",
        "Counter of system time jumps backward.",
    ))
});

/// Go `metrics.ReadFromTableCacheCounter`.
pub static READ_FROM_TABLECACHE_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    register(IntCounter::new(
        "tidb_server_read_from_tablecache_total",
        "Counter of query read from table cache.",
    ))
});

/// Go `metrics.HandShakeErrorCounter`.
pub static HANDSHAKE_ERROR_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    register(IntCounter::new(
        "tidb_server_handshake_error_total",
        "Counter of hand shake error.",
    ))
});

/// Go `metrics.GetTokenDurationHistogram`.
pub static GET_TOKEN_DURATION_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    register(Histogram::with_opts(
        HistogramOpts::new(
            "tidb_server_get_token_duration_seconds",
            "Duration (us) for getting token, it should be small until concurrency limit is reached.",
        )
        .buckets(prometheus::exponential_buckets(1.0, 2.0, 30).expect("valid buckets")),
    ))
});

/// Go `metrics.NumOfMultiQueryHistogram`.
pub static MULTI_QUERY_NUM: LazyLock<Histogram> = LazyLock::new(|| {
    register(Histogram::with_opts(
        HistogramOpts::new(
            "tidb_server_multi_query_num",
            "The number of queries contained in a multi-query statement.",
        )
        .buckets(prometheus::exponential_buckets(1.0, 2.0, 20).expect("valid buckets")),
    ))
});

/// Go `metrics.TotalQueryProcHistogram`.
pub static SLOW_QUERY_PROCESS_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_server_slow_query_process_duration_seconds",
            "Bucketed histogram of processing time (s) of of slow queries.",
        )
        .buckets(prometheus::exponential_buckets(0.001, 2.0, 28).expect("valid buckets")),
        &[labels::SQL_TYPE],
    ))
});

/// Go `metrics.TotalCopProcHistogram`.
pub static SLOW_QUERY_COP_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_server_slow_query_cop_duration_seconds",
            "Bucketed histogram of all cop processing time (s) of of slow queries.",
        )
        .buckets(prometheus::exponential_buckets(0.001, 2.0, 28).expect("valid buckets")),
        &[labels::SQL_TYPE],
    ))
});

/// Go `metrics.TotalCopWaitHistogram`.
pub static SLOW_QUERY_WAIT_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_server_slow_query_wait_duration_seconds",
            "Bucketed histogram of all cop waiting time (s) of of slow queries.",
        )
        .buckets(prometheus::exponential_buckets(0.001, 2.0, 28).expect("valid buckets")),
        &[labels::SQL_TYPE],
    ))
});

/// Go `metrics.CopMVCCRatioHistogram`.
pub static SLOW_QUERY_COP_MVCC_RATIO: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_server_slow_query_cop_mvcc_ratio",
            "Bucketed histogram of all cop total keys / processed keys in slow queries.",
        )
        .buckets(prometheus::exponential_buckets(0.5, 2.0, 21).expect("valid buckets")),
        &[labels::SQL_TYPE],
    ))
});

/// Go `metrics.SlowQueryCounter`.
pub static SLOW_QUERY_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register(IntCounterVec::new(
        Opts::new("tidb_server_slow_query_total", "Counter of slow queries."),
        &[labels::SQL_TYPE],
    ))
});

/// Go `metrics.MaxProcs`.
pub static MAXPROCS: LazyLock<IntGauge> = LazyLock::new(|| {
    register(IntGauge::new(
        "tidb_server_maxprocs",
        "The value of GOMAXPROCS.",
    ))
});

/// Go `metrics.GOGC`.
pub static GOGC: LazyLock<IntGauge> =
    LazyLock::new(|| register(IntGauge::new("tidb_server_gogc", "The value of GOGC")));

/// Go `metrics.ConnIdleDurationHistogram`.
pub static CONN_IDLE_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_server_conn_idle_duration_seconds",
            "Bucketed histogram of connection idle time (s).",
        )
        .buckets(prometheus::exponential_buckets(0.0005, 2.0, 29).expect("valid buckets")),
        &[labels::IN_TXN],
    ))
});

/// Go `metrics.ServerInfo`.
pub static SERVER_INFO: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register(IntGaugeVec::new(
        Opts::new(
            "tidb_server_info",
            "Indicate the tidb server info, and the value is the start timestamp (s).",
        ),
        &[labels::VERSION, labels::HASH],
    ))
});

/// Go `metrics.TokenGauge`.
pub static TOKENS: LazyLock<IntGauge> = LazyLock::new(|| {
    register(IntGauge::new(
        "tidb_server_tokens",
        "The number of concurrent executing session",
    ))
});

/// Go `metrics.ConfigStatus`.
pub static CONFIG_STATUS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register(IntGaugeVec::new(
        Opts::new(
            "tidb_config_status",
            "Status of the TiDB server configurations.",
        ),
        &[labels::TYPE],
    ))
});

/// Go `metrics.TiFlashQueryTotalCounter`.
pub static TIFLASH_QUERY_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register(IntCounterVec::new(
        Opts::new(
            "tidb_server_tiflash_query_total",
            "Counter of TiFlash queries.",
        ),
        &[labels::TYPE, labels::RESULT],
    ))
});

/// Go `metrics.TiFlashFailedMPPStoreState`.
pub static TIFLASH_FAILED_STORE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register(IntGaugeVec::new(
        Opts::new(
            "tidb_server_tiflash_failed_store",
            "Statues of failed tiflash mpp store,-1 means detector heartbeat,0 means reachable,1 means abnormal.",
        ),
        &[labels::ADDRESS],
    ))
});

/// Go `metrics.PDAPIExecutionHistogram`.
pub static PD_API_EXECUTION_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_server_pd_api_execution_duration_seconds",
            "Bucketed histogram of all pd api execution time (s)",
        )
        .buckets(prometheus::exponential_buckets(0.001, 2.0, 20).expect("valid buckets")),
        &[labels::TYPE],
    ))
});

/// Go `metrics.PDAPIRequestCounter`.
pub static PD_API_REQUEST_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register(IntCounterVec::new(
        Opts::new(
            "tidb_server_pd_api_request_total",
            "Counter of the pd http api requests",
        ),
        &[labels::TYPE, labels::RESULT],
    ))
});

/// Go `metrics.CPUProfileCounter`.
pub static CPU_PROFILE_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    register(IntCounter::new(
        "tidb_server_cpu_profile_total",
        "Counter of cpu profile requests.",
    ))
});

/// Go `metrics.LoadTableCacheDurationHistogram`.
pub static LOAD_TABLE_CACHE_SECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    register(Histogram::with_opts(
        HistogramOpts::new(
            "tidb_server_load_table_cache_seconds",
            "Bucketed histogram of loading table cache time (s).",
        )
        .buckets(prometheus::exponential_buckets(0.001, 2.0, 28).expect("valid buckets")),
    ))
});

/// Go `metrics.RCCheckTSWriteConfilictCounter`.
pub static RC_CHECK_TS_CONFLICT_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register(IntCounterVec::new(
        Opts::new(
            "tidb_server_rc_check_ts_conflict_total",
            "Counter of write conflict error on RC check TS.",
        ),
        &[labels::TYPE],
    ))
});

/// Go `metrics.MemoryLimit`.
pub static MEMORY_QUOTA_BYTES: LazyLock<IntGauge> = LazyLock::new(|| {
    register(IntGauge::new(
        "tidb_server_memory_quota_bytes",
        "Memory quota for the server, read from the configuration.",
    ))
});

/// Go `metrics.InternalSessions`.
pub static INTERNAL_SESSIONS: LazyLock<IntGauge> = LazyLock::new(|| {
    register(IntGauge::new(
        "tidb_server_internal_sessions",
        "Number of internal sessions.",
    ))
});

/// Go `metrics.ActiveUser`.
pub static ACTIVE_USERS: LazyLock<IntGauge> = LazyLock::new(|| {
    register(IntGauge::new(
        "tidb_server_active_users",
        "Number of active users.",
    ))
});

/// Go `metrics.TLSVersion`.
pub static TLS_VERSION: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register(IntCounterVec::new(
        Opts::new("tidb_server_tls_version", "TLS version of the connections."),
        &["version"],
    ))
});

/// Go `metrics.TLSCipher`.
pub static TLS_CIPHER: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register(IntCounterVec::new(
        Opts::new("tidb_server_tls_cipher", "TLS cipher of the connections."),
        &["cipher"],
    ))
});

/// Go `metrics.PanicCounter` (`pkg/metrics/metrics.go:122`).
pub use crate::panic::PANIC_TOTAL;

/// Go `metrics.MemoryUsage` (`pkg/metrics/metrics.go:129`).
pub static MEMORY_USAGE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register(IntGaugeVec::new(
        Opts::new("tidb_server_memory_usage", "Memory Usage"),
        &[labels::MODULE, labels::TYPE],
    ))
});

/// Go `ExecuteErrorToLabel` (`pkg/metrics/server.go:483`): the label is the
/// error's RFC code string (`"<pkg>:<code>"`), or `unknown` for a plain
/// error. The Go identity travels on the error (`SqlQueryError::identity`).
pub fn execute_error_to_label(identity: Option<tidb_error::terror::TerrorIdentity>) -> String {
    identity.map_or_else(|| "unknown".to_owned(), |identity| identity.rfc_code())
}

/// Go `BindingCacheHitCounter` (`pkg/metrics`).
pub static TIDB_SERVER_BINDING_CACHE_HIT_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    register(Counter::new(
        "tidb_server_binding_cache_hit_total",
        "Counter of binding cache hit.",
    ))
});

/// Go `BindingCacheMemLimit` (`pkg/metrics`).
pub use crate::bindinfo::BINDING_CACHE_MEM_LIMIT as TIDB_SERVER_BINDING_CACHE_MEM_LIMIT;

/// Go `BindingCacheMemUsage` (`pkg/metrics`).
pub use crate::bindinfo::BINDING_CACHE_MEM_USAGE as TIDB_SERVER_BINDING_CACHE_MEM_USAGE;

/// Go `BindingCacheMissCounter` (`pkg/metrics`).
pub static TIDB_SERVER_BINDING_CACHE_MISS_TOTAL: LazyLock<Counter> = LazyLock::new(|| {
    register(Counter::new(
        "tidb_server_binding_cache_miss_total",
        "Counter of binding cache miss.",
    ))
});

/// Go `BindingCacheNumBindings` (`pkg/metrics`).
pub use crate::bindinfo::BINDING_CACHE_NUM_BINDINGS as TIDB_SERVER_BINDING_CACHE_NUM_BINDINGS;

/// Go `RunawayFlusherAddCounter` (`pkg/metrics`).
pub static TIDB_SERVER_RUNAWAY_FLUSHER_ADD_TOTAL: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new(
            "tidb_server_runaway_flusher_add_total",
            "Counter of records added to runaway flusher.",
        ),
        &["name"],
    ))
});

/// Go `RunawayFlusherCounter` (`pkg/metrics`).
pub static TIDB_SERVER_RUNAWAY_FLUSHER_TOTAL: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new(
            "tidb_server_runaway_flusher_total",
            "Counter of runaway flusher operations.",
        ),
        &["name", "result"],
    ))
});

/// Go `RunawaySyncerCheckpoint` (`pkg/metrics`).
pub static TIDB_SERVER_RUNAWAY_SYNCER_CHECKPOINT: LazyLock<GaugeVec> = LazyLock::new(|| {
    register(GaugeVec::new(
        Opts::new("tidb_server_runaway_syncer_checkpoint", "Current lower-bound checkpoint of runaway syncer: Unix milliseconds of the next scan window for start_time (watch) or done_time (watch_done)."),
        &["type"],
    ))
});

/// Go `RunawaySyncerCounter` (`pkg/metrics`).
pub static TIDB_SERVER_RUNAWAY_SYNCER_TOTAL: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new(
            "tidb_server_runaway_syncer_total",
            "Counter of runaway syncer operations.",
        ),
        &["result", "type"],
    ))
});

/// Go `TTLEventCounter` (`pkg/metrics`).
pub static TIDB_SERVER_TTL_EVENT_COUNT: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new("tidb_server_ttl_event_count", "Counter of ttl event."),
        &["type"],
    ))
});

/// Go `TTLInsertRowsCounter` (`pkg/metrics`).
pub static TIDB_SERVER_TTL_INSERT_ROWS: LazyLock<Counter> = LazyLock::new(|| {
    register(Counter::new(
        "tidb_server_ttl_insert_rows",
        "The count of TTL rows inserted",
    ))
});

/// Go `TTLJobStatus` (`pkg/metrics`).
pub static TIDB_SERVER_TTL_JOB_STATUS: LazyLock<GaugeVec> = LazyLock::new(|| {
    register(GaugeVec::new(
        Opts::new(
            "tidb_server_ttl_job_status",
            "The jobs count in the specified status",
        ),
        &["type"],
    ))
});

/// Go `TTLPhaseTime` (`pkg/metrics`).
pub static TIDB_SERVER_TTL_PHASE_TIME: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new("tidb_server_ttl_phase_time", "The time spent in each phase"),
        &["phase", "type"],
    ))
});

/// Go `TTLProcessedExpiredRowsCounter` (`pkg/metrics`).
pub static TIDB_SERVER_TTL_PROCESSED_EXPIRED_ROWS: LazyLock<CounterVec> = LazyLock::new(|| {
    register(CounterVec::new(
        Opts::new(
            "tidb_server_ttl_processed_expired_rows",
            "The count of expired rows processed in TTL jobs",
        ),
        &["result", "sql_type"],
    ))
});

/// Go `TTLTaskStatus` (`pkg/metrics`).
pub static TIDB_SERVER_TTL_TASK_STATUS: LazyLock<GaugeVec> = LazyLock::new(|| {
    register(GaugeVec::new(
        Opts::new(
            "tidb_server_ttl_task_status",
            "The tasks count in the specified status",
        ),
        &["type"],
    ))
});

/// Go `TTLWatermarkDelay` (`pkg/metrics`).
pub static TIDB_SERVER_TTL_WATERMARK_DELAY: LazyLock<GaugeVec> = LazyLock::new(|| {
    register(GaugeVec::new(
        Opts::new(
            "tidb_server_ttl_watermark_delay",
            "Bucketed delay time in seconds for TTL tables.",
        ),
        &["name", "type"],
    ))
});

/// Go `TTLQueryDuration` (`pkg/metrics/ttl.go`).
pub static TTL_QUERY_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register(HistogramVec::new(
        HistogramOpts::new(
            "tidb_server_ttl_query_duration",
            "Bucketed histogram of processing time (s) of handled TTL queries.",
        )
        .buckets(prometheus::exponential_buckets(0.01, 2.0, 20).expect("valid buckets")),
        &["sql_type", "result"],
    ))
});

/// Go `RegisterMetrics` / `InitServerMetrics` (`pkg/metrics/metrics.go`) for
/// this file's families: every definition registers with the default
/// registry and its plain gauges and counters export their zero series. Vec
/// families gain series on first labeled use, exactly as Go's do.
pub fn register_metrics() {
    LazyLock::force(&PACKET_IO_BYTES);
    LazyLock::force(&QUERY_STATEMENT_RPC_COUNT);
    LazyLock::force(&QUERY_STATEMENT_PROCESSED_KEYS);
    LazyLock::force(&IA_REMOTE_READ_SEGMENT_COUNT);
    LazyLock::force(&IA_REMOTE_READ_SEGMENT_SIZE_BYTES);
    LazyLock::force(&IA_REMOTE_READ_SEGMENT_WAIT_DURATION);
    LazyLock::force(&CONN_GAUGE);
    LazyLock::force(&DISCONNECTION_TOTAL);
    LazyLock::force(&PREPARED_STMTS);
    LazyLock::force(&EXECUTE_ERROR_TOTAL);
    LazyLock::force(&CRITICAL_ERROR_TOTAL);
    LazyLock::force(&EVENT_TOTAL);
    LazyLock::force(&TIME_JUMP_BACK_TOTAL);
    LazyLock::force(&READ_FROM_TABLECACHE_TOTAL);
    LazyLock::force(&HANDSHAKE_ERROR_TOTAL);
    LazyLock::force(&GET_TOKEN_DURATION_SECONDS);
    LazyLock::force(&MULTI_QUERY_NUM);
    LazyLock::force(&SLOW_QUERY_PROCESS_DURATION);
    LazyLock::force(&SLOW_QUERY_COP_DURATION);
    LazyLock::force(&SLOW_QUERY_WAIT_DURATION);
    LazyLock::force(&SLOW_QUERY_COP_MVCC_RATIO);
    LazyLock::force(&SLOW_QUERY_TOTAL);
    LazyLock::force(&MAXPROCS);
    LazyLock::force(&GOGC);
    LazyLock::force(&CONN_IDLE_DURATION);
    LazyLock::force(&SERVER_INFO);
    LazyLock::force(&TOKENS);
    LazyLock::force(&CONFIG_STATUS);
    LazyLock::force(&TIFLASH_QUERY_TOTAL);
    LazyLock::force(&TIFLASH_FAILED_STORE);
    LazyLock::force(&PD_API_EXECUTION_DURATION);
    LazyLock::force(&PD_API_REQUEST_TOTAL);
    LazyLock::force(&CPU_PROFILE_TOTAL);
    LazyLock::force(&LOAD_TABLE_CACHE_SECONDS);
    LazyLock::force(&RC_CHECK_TS_CONFLICT_TOTAL);
    LazyLock::force(&MEMORY_QUOTA_BYTES);
    LazyLock::force(&INTERNAL_SESSIONS);
    LazyLock::force(&ACTIVE_USERS);
    LazyLock::force(&TLS_VERSION);
    LazyLock::force(&TLS_CIPHER);
    LazyLock::force(&PANIC_TOTAL);
    LazyLock::force(&MEMORY_USAGE);
}

/// Registers one collector with the process default registry, once: Go's
/// `pkg/metrics.RegisterMetrics` makes every family visible to `/metrics`.
fn register_once<C>(collector: prometheus::Result<C>, what: &'static str) -> C
where
    C: prometheus::core::Collector + Clone + 'static,
{
    let metric = collector.unwrap_or_else(|error| panic!("{what} is constructible: {error}"));
    prometheus::default_registry()
        .register(Box::new(metric.clone()))
        .unwrap_or_else(|error| panic!("{what} is registered once: {error}"));
    metric
}


/// Go `metrics.PlanCacheCounter`.
pub static PLAN_CACHE_COUNTER: LazyLock<CounterVec> = LazyLock::new(|| {
    register_once(
        CounterVec::new(
            Opts::new("plan_cache_total", "Counter of query using plan cache.")
                .namespace("tidb")
                .subsystem("server"),
            &["type"],
        ),
        "plan cache counter",
    )
});

/// Go `metrics.PlanCacheMissCounter`.
pub static PLAN_CACHE_MISS_COUNTER: LazyLock<CounterVec> = LazyLock::new(|| {
    register_once(
        CounterVec::new(
            Opts::new("plan_cache_miss_total", "Counter of plan cache miss.")
                .namespace("tidb")
                .subsystem("server"),
            &["type"],
        ),
        "plan cache miss counter",
    )
});

/// Go `metrics.PlanCacheInstanceMemoryUsage`.
pub static PLAN_CACHE_INSTANCE_MEMORY_USAGE: LazyLock<GaugeVec> = LazyLock::new(|| {
    register_once(
        GaugeVec::new(
            Opts::new(
                "plan_cache_instance_memory_usage",
                "Total plan cache memory usage of all sessions in a instance",
            )
            .namespace("tidb")
            .subsystem("server"),
            &["type"],
        ),
        "plan cache memory gauge",
    )
});

/// Go `metrics.PlanCacheInstancePlanNumCounter`.
pub static PLAN_CACHE_INSTANCE_PLAN_NUM_COUNTER: LazyLock<GaugeVec> = LazyLock::new(|| {
    register_once(
        GaugeVec::new(
            Opts::new(
                "plan_cache_instance_plan_num_total",
                "Counter of plan of all prepared plan cache in a instance",
            )
            .namespace("tidb")
            .subsystem("server"),
            &["type"],
        ),
        "plan cache plan num gauge",
    )
});

/// Go `metrics.PlanCacheProcessDuration` (1ms ~ 1.5days exponential buckets).
pub static PLAN_CACHE_PROCESS_DURATION: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_once(
        HistogramVec::new(
            HistogramOpts::new(
                "plan_cache_process_duration_seconds",
                "Bucketed histogram of processing time (s) of plan cache operations.",
            )
            .namespace("tidb")
            .subsystem("server")
            .buckets(prometheus::exponential_buckets(0.001, 2.0, 28).expect("28 positive buckets")),
            &["type"],
        ),
        "plan cache duration histogram",
    )
});
