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

//! SEED of Go `pkg/executor/metrics_reader.go`: the three `METRICS_SCHEMA`
//! retrievers -- [`MetricRetriever`] (`metrics_schema.<table>`, one Prometheus
//! range query per quantile), [`MetricsSummaryRetriever`]
//! (`information_schema.metrics_summary`) and
//! [`MetricsSummaryByLabelRetriever`]
//! (`information_schema.metrics_summary_by_label`), the latter two being pure
//! SQL generators that aggregate over the metric tables the first one serves.
//!
//! This is a cluster-facing memtable reader, not an operator: nothing here
//! implements `tidb_executor::Executor`. It lives beside this crate's other
//! `cluster_*` retrievers, which is where `pkg/executor`'s
//! `dummyCloser`-shaped `retrieve(ctx, sctx) ([][]types.Datum, error)`
//! retrievers belong.
//!
//! SEED, not a package claim: `pkg/executor` is a very large Go package and
//! only `metrics_reader.go` is covered here. Two symbols from OTHER Go
//! packages are seeded alongside it because `metrics_reader.go`'s own bodies
//! call them directly and the result is unobservable without them:
//!
//! * [`MetricTableDef::gen_prom_ql`] / [`gen_label_condition_values`] --
//!   `pkg/infoschema/metrics_schema.go:114`, `:121`, `:145`, reached from Go
//!   `queryMetric` (`:123`) and `genRecord` (`:172`).
//! * [`InspectionFilter`] -- `pkg/executor/inspection_result.go:60`, `:75`,
//!   same Go package, used by both summary retrievers (`:212`, `:289`).
//!
//! ## Narrowings (each named)
//!
//! * `*model.TableInfo` -> a lowercase table name [`String`]: the Go bodies
//!   read only `e.table.Name.L`.
//! * `*plannercore.MetricTableExtractor` /
//!   `*plannercore.MetricSummaryTableExtractor` ->
//!   [`MetricTableExtractor`] / [`MetricSummaryTableExtractor`], carrying
//!   exactly the fields the ported bodies read. `tidb-planner`'s extractors
//!   are not wired to this tier.
//! * `map[string]set.StringSet` -> `BTreeMap<String, BTreeSet<String>>`. Go's
//!   `set.StringSet` is a `map[string]struct{}` with NO iteration order, and
//!   both `GenLabelConditionValues` (`:145`) and Go's own
//!   `genLabelCondition` sort before rendering; the `BTreeSet` is already
//!   sorted, so the rendered strings agree.
//! * `promv1.Range` -> [`PromQueryRange`]; `pmodel.Value`/`pmodel.Matrix`/
//!   `pmodel.SamplePair` -> [`PromValue`]/[`SampleStream`]/[`SamplePair`].
//!   The Prometheus client crate is absent, so the query itself is the
//!   [`PromQuerier`] seam (see Boundaries).
//! * `sessionctx.Context` -> explicit scalar parameters
//!   (`metric_schema_step`, `metric_schema_range_duration`,
//!   `has_process_priv`) plus, for the summary retrievers, the
//!   [`RestrictedSqlExecutor`] seam standing in for
//!   `sctx.GetRestrictedSQLExecutor()`.
//! * `chunk.Row` (the restricted executor's result rows) -> `Vec<Datum>`,
//!   read positionally exactly as Go reads `row.GetFloat64(i)` /
//!   `row.GetString(i)` / `row.Len()-1`.
//! * `strconv.FormatFloat(q, 'f', -1, 64)` (`metrics_schema.go:116`) ->
//!   [`format_float_shortest`]: Rust's `{}` is also shortest-round-trip, but
//!   switches to exponent notation for very large/small magnitudes where Go's
//!   `'f'` never does. Quantiles are in `[0, 1]`, where the two agree.
//! * `fmt.Sprintf("%f", q)` (`:228`, `:307`) -> `{:.6}`; both fix six
//!   decimals and round half-to-even.
//! * `time.UnixMilli` (`:162`) resolves in Go's LOCAL zone before
//!   `types.FromGoTime` reads its calendar fields. The zone is an explicit
//!   `chrono::TimeZone` parameter here rather than an ambient global.
//!
//! ## Boundaries (nothing invented)
//!
//! * `infosync.GetPrometheusAddr`, `api.NewClient`, `promv1.NewAPI` and
//!   `promQLAPI.QueryRange` (`:96`-`:134`): no Prometheus client and no
//!   `infosync` at this tier. [`MetricRetriever::retrieve`] takes a
//!   [`PromQuerier`], builds the exact promQL string Go builds, and maps the
//!   querier's error into Go's two `errors.Errorf` spellings. Go's two
//!   five-attempt retry loops with a 100ms sleep, and the 10s
//!   `promReadTimeout` ([`PROM_READ_TIMEOUT`], kept as a constant), are
//!   therefore the querier's business, not ours.
//! * `infoschema.GetMetricTableDef` (`:67`) / `infoschema.MetricTableMap`
//!   (`:204`, `:218`): the generated metric-table catalog is not ported. The
//!   definitions are supplied by the caller as a [`MetricTableDefs`] map,
//!   whose lookup reproduces Go's `can not find metric table: %v` error and
//!   whose sorted iteration reproduces Go's `slices.Sort(tables)` (`:209`,
//!   `:286`).
//! * `plannerutil.QueryTimeRange.Condition()`
//!   (`pkg/planner/util/misc.go:201`): supplied as the already-rendered
//!   `time_condition` string. `tidb-planner`'s `QueryTimeRange` documents its
//!   `From`/`To` as "the caller's own timestamp encoding" and has no
//!   `Condition`.
//! * `hasPriv(sctx, mysql.ProcessPriv)` (`:197`, `:274`) -> the
//!   `has_process_priv` flag; the rendered text of
//!   `plannererrors.ErrSpecificAccessDenied` is the privilege layer's, so
//!   [`MetricsReaderError::SpecificAccessDenied`] carries the argument
//!   (`"PROCESS"`) rather than a guessed message.
//! * The `mockMetricsTableData` / `mockMetricsPromData` failpoints (`:60`,
//!   `:97`) and `MockMetricsPromDataKey`: failpoints are test injection, and
//!   the [`PromQuerier`] seam already lets a test supply canned matrices.
//! * `kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)` (`:211`, `:288`):
//!   context plumbing with no observable effect on the rows produced.
//! * `sctx.GetSessionVars().StmtCtx.AppendWarning` (`:220`, `:297`) -> the
//!   warnings [`Vec`] each summary `retrieve` returns; this tier's retrievers
//!   have no statement context to push into.
//!
//! ## Concurrency
//!
//! Nothing in `metrics_reader.go` is concurrent: all three `retrieve` bodies
//! are straight-line loops on the calling goroutine. The only concurrency Go
//! has is inside the Prometheus HTTP client and the `context.WithTimeout`
//! cancellation, both of which sit behind the [`PromQuerier`] boundary.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use chrono::{DateTime, TimeZone};
use tidb_datatype::{core_time_from_datetime, Datum, Time, TimeType};

/// Go `promReadTimeout` (`metrics_reader.go:43`).
///
/// Kept for the [`PromQuerier`] implementor that owns the HTTP call; nothing
/// on this side can enforce it.
pub const PROM_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Go `metadef.MetricSchemaName.L` (`pkg/meta/metadef/db.go:30`), the schema
/// the summary retrievers' generated SQL reads from.
pub const METRIC_SCHEMA_NAME_LOWER: &str = "metrics_schema";

/// Go `promQLQuantileKey` (`pkg/infoschema/metrics_schema.go:36`).
const PROM_QL_QUANTILE_KEY: &str = "$QUANTILE";
/// Go `promQLLabelConditionKey` (`pkg/infoschema/metrics_schema.go:37`).
const PROM_QL_LABEL_CONDITION_KEY: &str = "$LABEL_CONDITIONS";
/// Go `promQRangeDurationKey` (`pkg/infoschema/metrics_schema.go:38`).
const PROM_Q_RANGE_DURATION_KEY: &str = "$RANGE_DURATION";

/// Go `map[string]set.StringSet`: the extractors' per-label value sets.
///
/// Keys are lowercase label names, as `genRecord` (`:172`) looks them up with
/// `strings.ToLower(label)`.
pub type LabelConditions = BTreeMap<String, BTreeSet<String>>;

/// Go `infoschema.MetricTableMap` (`pkg/infoschema/metrics_schema.go`), keyed
/// by lowercase table name.
pub type MetricTableDefs = BTreeMap<String, MetricTableDef>;

/// Errors the three retrievers return.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricsReaderError {
    /// Go `plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs(arg)`
    /// (`:198`, `:275`). The rendered message belongs to the privilege layer.
    SpecificAccessDenied {
        /// The privilege named in the Go call, always `"PROCESS"` here.
        privilege: String,
    },
    /// Go `infoschema.GetMetricTableDef`'s
    /// `can not find metric table: %v` (`pkg/infoschema/metrics_schema.go:93`),
    /// surfaced from `MetricRetriever.retrieve` (`:67`).
    ///
    /// The summary retrievers do NOT take this path: a missing definition
    /// there is a warning and a `continue` (`:220`, `:297`).
    UnknownMetricTable {
        /// The lowercase table name that was not in the map.
        name: String,
    },
    /// Go `errors.Errorf("query metric error, msg: %v, detail: %v", ...)`
    /// (`:83`) and `errors.Errorf("query metric error: %v", ...)` (`:85`).
    QueryMetric {
        /// The already-formatted Go message.
        message: String,
    },
    /// Go `errors.Errorf("execute '%s' failed: %v", sql, err)` (`:243`,
    /// `:325`).
    ExecuteSql {
        /// The already-formatted Go message.
        message: String,
    },
    /// The `time.UnixMilli` timestamp in a sample pair does not name a real
    /// instant in the supplied zone, or is outside the MySQL `DATETIME`
    /// range.
    ///
    /// Go cannot reach this: `time.UnixMilli` is total and
    /// `types.FromGoTime` does no validation.
    InvalidTimestamp {
        /// The offending `pmodel.SamplePair.Timestamp`, in milliseconds.
        timestamp_ms: i64,
    },
}

// ---------------------------------------------------------------------------
// pkg/infoschema/metrics_schema.go seeds (called directly by the ported bodies)
// ---------------------------------------------------------------------------

/// Go `infoschema.MetricTableDef` (`pkg/infoschema/metrics_schema.go:76`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MetricTableDef {
    /// Go `PromQL`: the template holding `$QUANTILE`, `$LABEL_CONDITIONS` and
    /// `$RANGE_DURATION`.
    pub prom_ql: String,
    /// Go `Labels`, in declaration order -- which is the column order of the
    /// generated metric table and therefore of every record built here.
    pub labels: Vec<String>,
    /// Go `Quantile`. A value `> 0` is what makes the table quantile-shaped:
    /// it adds a `quantile` column and a `quantile` grouping key.
    pub quantile: f64,
    /// Go `Comment`, echoed as the last column of both summary tables.
    pub comment: String,
}

impl MetricTableDef {
    /// Go `MetricTableDef.GenPromQL` (`pkg/infoschema/metrics_schema.go:114`).
    ///
    /// The three substitutions happen in Go's order; since the replacements
    /// never contain another key, the order is not observable.
    #[must_use]
    pub fn gen_prom_ql(
        &self,
        metrics_schema_range_duration: i64,
        labels: &LabelConditions,
        quantile: f64,
    ) -> String {
        let prom_ql = self
            .prom_ql
            .replace(PROM_QL_QUANTILE_KEY, &format_float_shortest(quantile));
        let prom_ql = prom_ql.replace(
            PROM_QL_LABEL_CONDITION_KEY,
            &self.gen_label_condition(labels),
        );
        prom_ql.replace(
            PROM_Q_RANGE_DURATION_KEY,
            &format!("{metrics_schema_range_duration}s"),
        )
    }

    /// Go `MetricTableDef.genLabelCondition`
    /// (`pkg/infoschema/metrics_schema.go:121`).
    ///
    /// Walks the table's OWN label order (not the condition map's), skips
    /// labels with no values, and renders a single value as `label="v"` and
    /// several as `label=~"a|b"`.
    fn gen_label_condition(&self, labels: &LabelConditions) -> String {
        let mut buf = String::new();
        let mut index = 0;
        for label in &self.labels {
            let Some(values) = labels.get(label) else {
                continue;
            };
            if values.is_empty() {
                continue;
            }
            if index > 0 {
                buf.push(',');
            }
            let rendered = gen_label_condition_values(values);
            if values.len() == 1 {
                buf.push_str(&format!("{label}=\"{rendered}\""));
            } else {
                buf.push_str(&format!("{label}=~\"{rendered}\""));
            }
            index += 1;
        }
        buf
    }
}

/// Go `infoschema.GenLabelConditionValues`
/// (`pkg/infoschema/metrics_schema.go:145`): the sorted values joined by `|`.
///
/// Go sorts the map keys explicitly; the `BTreeSet` is already in that order.
#[must_use]
pub fn gen_label_condition_values(values: &BTreeSet<String>) -> String {
    values.iter().cloned().collect::<Vec<_>>().join("|")
}

/// Go `strconv.FormatFloat(v, 'f', -1, 64)`.
///
/// Narrowing: Rust's `{}` is shortest-round-trip like Go's `-1` precision, but
/// falls back to exponent notation outside roughly `1e-5..1e16`, where Go's
/// `'f'` would keep writing digits. Every caller here formats a quantile in
/// `[0, 1]`.
#[must_use]
pub fn format_float_shortest(value: f64) -> String {
    format!("{value}")
}

/// Go `inspectionFilter` (`pkg/executor/inspection_result.go:60`), reduced to
/// the `set` field -- the only one the summary retrievers construct (`:212`,
/// `:289`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InspectionFilter {
    /// Go `set set.StringSet`; empty means "everything passes".
    pub set: BTreeSet<String>,
}

impl InspectionFilter {
    /// Go `inspectionFilter.enable` (`pkg/executor/inspection_result.go:75`).
    #[must_use]
    pub fn enable(&self, name: &str) -> bool {
        self.set.is_empty() || self.set.contains(name)
    }
}

// ---------------------------------------------------------------------------
// Prometheus value shapes (narrowings of prometheus/common/model)
// ---------------------------------------------------------------------------

/// Go `promv1.Range` (`metrics_reader.go:136`, `promQLQueryRange`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromQueryRange<Tz: TimeZone> {
    /// Go `Start`, from the extractor's `StartTime`.
    pub start: DateTime<Tz>,
    /// Go `End`, from the extractor's `EndTime`.
    pub end: DateTime<Tz>,
    /// Go `Step`, `time.Second * MetricSchemaStep`.
    pub step: Duration,
}

/// Go `pmodel.SamplePair`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SamplePair {
    /// Go `Timestamp` (`pmodel.Time`), in milliseconds.
    pub timestamp_ms: i64,
    /// Go `Value` (`pmodel.SampleValue`, a `float64`).
    pub value: f64,
}

/// Go `pmodel.Metric`: the label set attached to one series.
pub type PromMetric = BTreeMap<String, String>;

/// Go `*pmodel.SampleStream`, one element of a `pmodel.Matrix`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SampleStream {
    /// Go `Metric`. `None` reproduces Go's `metric != nil` check (`:168`);
    /// note Go also treats an ABSENT label the same way, which the lookup
    /// below preserves.
    pub metric: Option<PromMetric>,
    /// Go `Values`.
    pub values: Vec<SamplePair>,
}

/// Go `pmodel.Value`, narrowed to the two cases `genRows` distinguishes
/// (`:146`).
#[derive(Debug, Clone, PartialEq)]
pub enum PromValue {
    /// Go `pmodel.ValMatrix` / `pmodel.Matrix`.
    Matrix(Vec<SampleStream>),
    /// Every other `pmodel.ValueType`; `genRows` yields no rows for these.
    NonMatrix,
}

/// The seam standing in for Go `queryMetric`'s Prometheus round trip
/// (`metrics_reader.go:96`).
///
/// Go resolves the address through `infosync.GetPrometheusAddr`, builds an
/// `api.Client`, wraps it in `promv1.NewAPI`, applies a
/// [`PROM_READ_TIMEOUT`] context and retries `QueryRange` up to five times.
/// All of that -- address discovery, transport, timeout and retry -- is the
/// implementor's; this crate only supplies the promQL and the range.
pub trait PromQuerier<Tz: TimeZone> {
    /// Go `promQLAPI.QueryRange(ctx, promQL, queryRange)`.
    ///
    /// The error is already split the way Go's `retrieve` (`:82`) splits it.
    fn query_range(
        &mut self,
        prom_ql: &str,
        range: &PromQueryRange<Tz>,
    ) -> Result<PromValue, PromQueryError>;
}

/// The two error shapes Go's `retrieve` distinguishes at `:82`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromQueryError {
    /// Go `*promv1.Error`, carrying `Msg` and `Detail`.
    Api {
        /// Go `err1.Msg`.
        msg: String,
        /// Go `err1.Detail`.
        detail: String,
    },
    /// Any other `error`; Go renders it with `err.Error()`.
    Other {
        /// Go `err.Error()`.
        message: String,
    },
}

impl PromQueryError {
    /// Go `metrics_reader.go:82`-`:86`.
    fn into_reader_error(self) -> MetricsReaderError {
        let message = match self {
            Self::Api { msg, detail } => {
                format!("query metric error, msg: {msg}, detail: {detail}")
            }
            Self::Other { message } => format!("query metric error: {message}"),
        };
        MetricsReaderError::QueryMetric { message }
    }
}

// ---------------------------------------------------------------------------
// MetricRetriever (metrics_reader.go:46)
// ---------------------------------------------------------------------------

/// Go `plannercore.MetricTableExtractor`, narrowed to the fields
/// `MetricRetriever` reads.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricTableExtractor<Tz: TimeZone> {
    /// Go `SkipRequest`: the predicate is unsatisfiable, so produce nothing.
    pub skip_request: bool,
    /// Go `Quantiles`; empty means "use the table definition's own".
    pub quantiles: Vec<f64>,
    /// Go `LabelConditions`.
    pub label_conditions: LabelConditions,
    /// Go `StartTime`.
    pub start_time: DateTime<Tz>,
    /// Go `EndTime`.
    pub end_time: DateTime<Tz>,
}

/// Go `MetricRetriever` (`metrics_reader.go:46`): reads one metric table by
/// issuing one Prometheus range query per requested quantile.
#[derive(Debug, Clone)]
pub struct MetricRetriever<Tz: TimeZone> {
    /// Go `table.Name.L`.
    table_name: String,
    /// Go `tblDef`, filled by `retrieve` from the catalog (`:67`).
    tbl_def: Option<MetricTableDef>,
    /// Go `extractor`.
    extractor: MetricTableExtractor<Tz>,
    /// Go `retrieved`: this retriever answers exactly once.
    retrieved: bool,
}

impl<Tz: TimeZone> MetricRetriever<Tz> {
    /// Builds the retriever the Go executor builder builds.
    #[must_use]
    pub fn new(table_name: String, extractor: MetricTableExtractor<Tz>) -> Self {
        Self {
            table_name,
            tbl_def: None,
            extractor,
            retrieved: false,
        }
    }

    /// Go `MetricRetriever.getQueryRange` (`metrics_reader.go:138`).
    ///
    /// `metric_schema_step` is Go
    /// `sctx.GetSessionVars().MetricSchemaStep`, in seconds.
    #[must_use]
    pub fn get_query_range(&self, metric_schema_step: i64) -> PromQueryRange<Tz> {
        PromQueryRange {
            start: self.extractor.start_time.clone(),
            end: self.extractor.end_time.clone(),
            // Narrowing: Go's `time.Duration` is signed and a negative
            // `MetricSchemaStep` would yield a negative step; `Duration` is
            // unsigned, so a negative setting saturates to zero. The variable
            // is bounded positive by `sysvar` validation.
            step: Duration::from_secs(metric_schema_step.max(0).unsigned_abs()),
        }
    }

    /// Go `MetricRetriever.retrieve` (`metrics_reader.go:54`).
    ///
    /// `defs` stands in for `infoschema.GetMetricTableDef` (`:67`), `querier`
    /// for the Prometheus round trip, and `zone` for the local zone
    /// `time.UnixMilli` resolves in.
    pub fn retrieve<Q: PromQuerier<Tz>>(
        &mut self,
        defs: &MetricTableDefs,
        querier: &mut Q,
        metric_schema_step: i64,
        metrics_schema_range_duration: i64,
        zone: &Tz,
    ) -> Result<Vec<Vec<Datum>>, MetricsReaderError> {
        if self.retrieved || self.extractor.skip_request {
            return Ok(Vec::new());
        }
        self.retrieved = true;

        let tbl_def = defs.get(&self.table_name).cloned().ok_or_else(|| {
            MetricsReaderError::UnknownMetricTable {
                name: self.table_name.clone(),
            }
        })?;
        self.tbl_def = Some(tbl_def.clone());
        let query_range = self.get_query_range(metric_schema_step);

        let mut quantiles = self.extractor.quantiles.clone();
        if quantiles.is_empty() {
            quantiles = vec![tbl_def.quantile];
        }

        let mut total_rows = Vec::new();
        for quantile in quantiles {
            // Go builds the promQL inside `queryMetric` (`:123`).
            let prom_ql = tbl_def.gen_prom_ql(
                metrics_schema_range_duration,
                &self.extractor.label_conditions,
                quantile,
            );
            let value = querier
                .query_range(&prom_ql, &query_range)
                .map_err(PromQueryError::into_reader_error)?;
            total_rows.extend(self.gen_rows(&value, quantile, zone)?);
        }
        Ok(total_rows)
    }

    /// Go `MetricRetriever.genRows` (`metrics_reader.go:144`).
    ///
    /// A non-matrix value yields no rows, exactly as Go's type switch does.
    pub fn gen_rows(
        &self,
        value: &PromValue,
        quantile: f64,
        zone: &Tz,
    ) -> Result<Vec<Vec<Datum>>, MetricsReaderError> {
        let mut rows = Vec::new();
        if let PromValue::Matrix(matrix) = value {
            for stream in matrix {
                for pair in &stream.values {
                    rows.push(self.gen_record(stream.metric.as_ref(), *pair, quantile, zone)?);
                }
            }
        }
        Ok(rows)
    }

    /// Go `MetricRetriever.genRecord` (`metrics_reader.go:158`).
    ///
    /// Column order is `time`, the definition's labels, `quantile` (only when
    /// the definition is quantile-shaped), `value` -- the order
    /// `genColumnInfos` declares.
    pub fn gen_record(
        &self,
        metric: Option<&PromMetric>,
        pair: SamplePair,
        quantile: f64,
        zone: &Tz,
    ) -> Result<Vec<Datum>, MetricsReaderError> {
        let tbl_def = self
            .tbl_def
            .as_ref()
            .expect("gen_record runs after retrieve resolved the table definition");
        let mut record = Vec::with_capacity(2 + tbl_def.labels.len() + 1);

        let instant = zone
            .timestamp_millis_opt(pair.timestamp_ms)
            .single()
            .ok_or(MetricsReaderError::InvalidTimestamp {
                timestamp_ms: pair.timestamp_ms,
            })?;
        let time = Time::new(
            core_time_from_datetime(instant),
            TimeType::DateTime,
            tidb_datatype::MAX_FSP,
        )
        .map_err(|_| MetricsReaderError::InvalidTimestamp {
            timestamp_ms: pair.timestamp_ms,
        })?;
        record.push(Datum::Time(time));

        for label in &tbl_def.labels {
            // Go: the series' own label value, falling back to the value(s)
            // the WHERE clause pinned when the series carries none.
            let mut value = metric
                .and_then(|m| m.get(label))
                .cloned()
                .unwrap_or_default();
            if value.is_empty() {
                value = self
                    .extractor
                    .label_conditions
                    .get(&label.to_lowercase())
                    .map(gen_label_condition_values)
                    .unwrap_or_default();
            }
            record.push(Datum::new_string(value));
        }

        if tbl_def.quantile > 0.0 {
            record.push(Datum::Real(quantile));
        }

        // Go maps NaN to NULL (`:179`); +/-Inf is passed through as-is.
        if pair.value.is_nan() {
            record.push(Datum::Null);
        } else {
            record.push(Datum::Real(pair.value));
        }
        Ok(record)
    }
}

// ---------------------------------------------------------------------------
// The summary retrievers (metrics_reader.go:188, :265)
// ---------------------------------------------------------------------------

/// Go `plannercore.MetricSummaryTableExtractor`, narrowed to the fields both
/// summary retrievers read.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MetricSummaryTableExtractor {
    /// Go `SkipRequest`.
    pub skip_request: bool,
    /// Go `MetricsNames`, the `metrics_name in (...)` filter.
    pub metrics_names: BTreeSet<String>,
    /// Go `Quantiles`.
    pub quantiles: Vec<f64>,
}

/// The seam standing in for `sctx.GetRestrictedSQLExecutor()` +
/// `ExecRestrictedSQL` (`metrics_reader.go:240`, `:322`).
///
/// The generated SQL reads the metric tables through the ordinary SQL path,
/// which at this tier means whatever the caller wires up.
pub trait RestrictedSqlExecutor {
    /// Go `exec.ExecRestrictedSQL(ctx, nil, sql)`; the returned rows are read
    /// positionally.
    ///
    /// The error is rendered into Go's `execute '%s' failed: %v` by the
    /// caller, so only its `Error()` text is needed here.
    fn exec_restricted_sql(&mut self, sql: &str) -> Result<Vec<Vec<Datum>>, String>;
}

/// Go `fmt.Sprintf("%f", q)` over the extractor's quantiles, or Go's default.
///
/// `default_quantile` is `"0.99"` at `:231` and, for the by-label retriever,
/// the whole `quantile=0.99` clause is spelled out separately at `:311`.
fn format_quantiles(quantiles: &[f64]) -> Vec<String> {
    quantiles.iter().map(|q| format!("{q:.6}")).collect()
}

/// Go `MetricsSummaryRetriever.retrieve`'s per-table SQL (`:233`, `:236`).
#[must_use]
pub fn metrics_summary_sql(
    name: &str,
    def: &MetricTableDef,
    condition: &str,
    quantiles: &[f64],
) -> String {
    let schema = METRIC_SCHEMA_NAME_LOWER;
    if def.quantile > 0.0 {
        let qs = if quantiles.is_empty() {
            vec!["0.99".to_owned()]
        } else {
            format_quantiles(quantiles)
        };
        format!(
            "select sum(value),avg(value),min(value),max(value),quantile from `{schema}`.`{name}` {condition} and quantile in ({}) group by quantile order by quantile",
            qs.join(",")
        )
    } else {
        format!(
            "select sum(value),avg(value),min(value),max(value) from `{schema}`.`{name}` {condition}"
        )
    }
}

/// Go `MetricsSummaryByLabelRetriever.retrieve`'s per-table SQL (`:316`,
/// `:319`).
///
/// Note the argument order flip against [`metrics_summary_sql`]: Go's
/// `%[2]s`/`%[1]s` indexing puts the schema first in one and second in the
/// other, but both render `` `metrics_schema`.`<table>` ``.
#[must_use]
pub fn metrics_summary_by_label_sql(
    name: &str,
    def: &MetricTableDef,
    condition: &str,
    quantiles: &[f64],
) -> String {
    let schema = METRIC_SCHEMA_NAME_LOWER;
    // Go `cols := def.Labels` then `cols = append(cols, "quantile")`; the
    // clone here is what keeps that append from being observable on the
    // shared definition (Go's `append` may or may not alias depending on the
    // slice's spare capacity -- an aliasing hazard, not an intended
    // behavior).
    let mut cols = def.labels.clone();
    let mut cond = condition.to_owned();
    if def.quantile > 0.0 {
        cols.push("quantile".to_owned());
        if quantiles.is_empty() {
            cond.push_str(" and quantile=0.99");
        } else {
            cond.push_str(" and quantile in (");
            cond.push_str(&format_quantiles(quantiles).join(","));
            cond.push(')');
        }
    }
    if cols.is_empty() {
        format!(
            "select sum(value),avg(value),min(value),max(value) from `{schema}`.`{name}` {cond}"
        )
    } else {
        let joined = cols.join("`,`");
        format!(
            "select sum(value),avg(value),min(value),max(value),`{joined}` from `{schema}`.`{name}` {cond} group by `{joined}` order by `{joined}`"
        )
    }
}

/// What a summary `retrieve` produced: the rows plus the warnings Go pushes
/// into `StmtCtx` for metric names with no definition (`:220`, `:297`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SummaryOutcome {
    /// The `[][]types.Datum` Go returns.
    pub rows: Vec<Vec<Datum>>,
    /// Go `fmt.Errorf("metrics table: %s not found", name)`.
    pub warnings: Vec<String>,
}

/// Go `MetricsSummaryRetriever` (`metrics_reader.go:188`).
#[derive(Debug, Clone, Default)]
pub struct MetricsSummaryRetriever {
    /// Go `extractor`.
    extractor: MetricSummaryTableExtractor,
    /// Go `retrieved`.
    retrieved: bool,
}

impl MetricsSummaryRetriever {
    /// Builds the retriever the Go executor builder builds.
    ///
    /// Go also carries `table` and `timeRange`; the former is unread by the
    /// body and the latter arrives as the rendered `time_condition`.
    #[must_use]
    pub fn new(extractor: MetricSummaryTableExtractor) -> Self {
        Self {
            extractor,
            retrieved: false,
        }
    }

    /// Go `MetricsSummaryRetriever.retrieve` (`metrics_reader.go:196`).
    ///
    /// `time_condition` is `e.timeRange.Condition()` (`:213`).
    pub fn retrieve<E: RestrictedSqlExecutor>(
        &mut self,
        defs: &MetricTableDefs,
        exec: &mut E,
        time_condition: &str,
        has_process_priv: bool,
    ) -> Result<SummaryOutcome, MetricsReaderError> {
        if !has_process_priv {
            return Err(MetricsReaderError::SpecificAccessDenied {
                privilege: "PROCESS".to_owned(),
            });
        }
        if self.retrieved || self.extractor.skip_request {
            return Ok(SummaryOutcome::default());
        }
        self.retrieved = true;

        let filter = InspectionFilter {
            set: self.extractor.metrics_names.clone(),
        };
        let mut outcome = SummaryOutcome::default();
        // Go sorts the map keys (`:209`); `MetricTableDefs` iterates sorted.
        for (name, def) in defs {
            if !filter.enable(name) {
                continue;
            }
            let sql = metrics_summary_sql(name, def, time_condition, &self.extractor.quantiles);
            let rows =
                exec.exec_restricted_sql(&sql)
                    .map_err(|err| MetricsReaderError::ExecuteSql {
                        message: format!("execute '{sql}' failed: {err}"),
                    })?;
            for row in rows {
                let quantile = if def.quantile > 0.0 {
                    // Go `row.GetFloat64(row.Len()-1)`.
                    row.last().cloned().unwrap_or(Datum::Null)
                } else {
                    // Go's `var quantile any` stays nil -> a NULL datum.
                    Datum::Null
                };
                outcome.rows.push(vec![
                    Datum::new_string(name.clone()),
                    quantile,
                    row.first().cloned().unwrap_or(Datum::Null),
                    row.get(1).cloned().unwrap_or(Datum::Null),
                    row.get(2).cloned().unwrap_or(Datum::Null),
                    row.get(3).cloned().unwrap_or(Datum::Null),
                    Datum::new_string(def.comment.clone()),
                ]);
            }
        }
        // Go warns for names in `MetricTableMap` without a definition
        // (`:218`), which cannot happen once the map IS the definitions; the
        // warning list stays empty rather than being faked.
        Ok(outcome)
    }
}

/// Go `MetricsSummaryByLabelRetriever` (`metrics_reader.go:265`).
#[derive(Debug, Clone, Default)]
pub struct MetricsSummaryByLabelRetriever {
    /// Go `extractor`.
    extractor: MetricSummaryTableExtractor,
    /// Go `retrieved`.
    retrieved: bool,
}

impl MetricsSummaryByLabelRetriever {
    /// Builds the retriever the Go executor builder builds.
    #[must_use]
    pub fn new(extractor: MetricSummaryTableExtractor) -> Self {
        Self {
            extractor,
            retrieved: false,
        }
    }

    /// Go `MetricsSummaryByLabelRetriever.retrieve` (`metrics_reader.go:273`).
    pub fn retrieve<E: RestrictedSqlExecutor>(
        &mut self,
        defs: &MetricTableDefs,
        exec: &mut E,
        time_condition: &str,
        has_process_priv: bool,
    ) -> Result<SummaryOutcome, MetricsReaderError> {
        if !has_process_priv {
            return Err(MetricsReaderError::SpecificAccessDenied {
                privilege: "PROCESS".to_owned(),
            });
        }
        if self.retrieved || self.extractor.skip_request {
            return Ok(SummaryOutcome::default());
        }
        self.retrieved = true;

        let filter = InspectionFilter {
            set: self.extractor.metrics_names.clone(),
        };
        let mut outcome = SummaryOutcome::default();
        for (name, def) in defs {
            if !filter.enable(name) {
                continue;
            }
            let sql =
                metrics_summary_by_label_sql(name, def, time_condition, &self.extractor.quantiles);
            let rows =
                exec.exec_restricted_sql(&sql)
                    .map_err(|err| MetricsReaderError::ExecuteSql {
                        message: format!("execute '{sql}' failed: {err}"),
                    })?;

            // Go `:328`: when the first label is `instance` it is lifted out
            // of the joined label string into its own column.
            let non_instance_label_index =
                usize::from(def.labels.first().is_some_and(|l| l == "instance"));
            // Go `skipCols`: sum/avg/min/max occupy positions 0..4.
            const SKIP_COLS: usize = 4;

            for row in rows {
                let instance = if non_instance_label_index > 0 {
                    row.get(SKIP_COLS).cloned().unwrap_or(Datum::Null)
                } else {
                    Datum::new_string("")
                };
                let mut labels: Vec<String> = Vec::new();
                for (i, label) in def.labels[non_instance_label_index..].iter().enumerate() {
                    let cell = row.get(SKIP_COLS + non_instance_label_index + i);
                    let mut val = datum_string(cell);
                    if label == "store" || label == "store_id" {
                        val = format!("store_id:{val}");
                    }
                    labels.push(val);
                }
                let quantile = if def.quantile > 0.0 {
                    row.last().cloned().unwrap_or(Datum::Null)
                } else {
                    Datum::Null
                };
                outcome.rows.push(vec![
                    instance,
                    Datum::new_string(name.clone()),
                    Datum::new_string(labels.join(", ")),
                    quantile,
                    row.first().cloned().unwrap_or(Datum::Null),
                    row.get(1).cloned().unwrap_or(Datum::Null),
                    row.get(2).cloned().unwrap_or(Datum::Null),
                    row.get(3).cloned().unwrap_or(Datum::Null),
                    Datum::new_string(def.comment.clone()),
                ]);
            }
        }
        Ok(outcome)
    }
}

/// Go `row.GetString(i)` over a restricted-SQL result cell.
///
/// Go's chunk accessor returns `""` for a NULL or absent cell rather than
/// erroring, which is what the empty string here reproduces.
fn datum_string(cell: Option<&Datum>) -> String {
    match cell {
        Some(Datum::String(s)) => String::from_utf8_lossy(s.bytes()).into_owned(),
        Some(Datum::Bytes(b)) => String::from_utf8_lossy(b).into_owned(),
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    //! Go's coverage of these three retrievers is testkit-bound: every metric
    //! table is served by a live `METRICS_SCHEMA` virtual table over a mocked
    //! Prometheus (`mockMetricsPromData` / `mockMetricsTableData` failpoints)
    //! plus a bootstrapped session for `hasPriv` and the restricted SQL
    //! executor. `metrics_reader.go`'s own `_test.go` file holds only
    //! `TestStmtLabel`, which tests something else entirely. These tests
    //! therefore exercise the ported bodies through the seams directly.

    use super::*;
    use chrono::Utc;

    fn set(values: &[&str]) -> BTreeSet<String> {
        values.iter().map(|v| (*v).to_owned()).collect()
    }

    fn quantile_def() -> MetricTableDef {
        MetricTableDef {
            prom_ql: "histogram_quantile($QUANTILE, sum(rate(x{$LABEL_CONDITIONS}[$RANGE_DURATION])) by (le,instance))".to_owned(),
            labels: vec!["instance".to_owned(), "type".to_owned()],
            quantile: 0.99,
            comment: "the quantile comment".to_owned(),
        }
    }

    fn plain_def() -> MetricTableDef {
        MetricTableDef {
            prom_ql: "sum(rate(y{$LABEL_CONDITIONS}[$RANGE_DURATION]))".to_owned(),
            labels: Vec::new(),
            quantile: 0.0,
            comment: "the plain comment".to_owned(),
        }
    }

    struct FixedQuerier {
        seen: Vec<String>,
        results: Vec<Result<PromValue, PromQueryError>>,
    }

    impl PromQuerier<Utc> for FixedQuerier {
        fn query_range(
            &mut self,
            prom_ql: &str,
            _range: &PromQueryRange<Utc>,
        ) -> Result<PromValue, PromQueryError> {
            self.seen.push(prom_ql.to_owned());
            self.results.remove(0)
        }
    }

    struct FixedExec {
        seen: Vec<String>,
        rows: Vec<Vec<Datum>>,
        err: Option<String>,
    }

    impl RestrictedSqlExecutor for FixedExec {
        fn exec_restricted_sql(&mut self, sql: &str) -> Result<Vec<Vec<Datum>>, String> {
            self.seen.push(sql.to_owned());
            match &self.err {
                Some(err) => Err(err.clone()),
                None => Ok(self.rows.clone()),
            }
        }
    }

    fn extractor() -> MetricTableExtractor<Utc> {
        MetricTableExtractor {
            skip_request: false,
            quantiles: Vec::new(),
            label_conditions: LabelConditions::new(),
            start_time: Utc.timestamp_opt(1_600_000_000, 0).unwrap(),
            end_time: Utc.timestamp_opt(1_600_003_600, 0).unwrap(),
        }
    }

    fn defs(entries: &[(&str, MetricTableDef)]) -> MetricTableDefs {
        entries
            .iter()
            .map(|(name, def)| ((*name).to_owned(), def.clone()))
            .collect()
    }

    /// Go `MetricTableDef.GenPromQL` (`pkg/infoschema/metrics_schema.go:114`).
    #[test]
    fn gen_prom_ql_substitutes_all_three_keys() {
        let mut labels = LabelConditions::new();
        labels.insert("instance".to_owned(), set(&["10.0.0.1:10080"]));
        labels.insert("type".to_owned(), set(&["kv", "cop"]));
        let got = quantile_def().gen_prom_ql(60, &labels, 0.99);
        assert_eq!(
            got,
            "histogram_quantile(0.99, sum(rate(x{instance=\"10.0.0.1:10080\",type=~\"cop|kv\"}[60s])) by (le,instance))"
        );
    }

    /// Go `genLabelCondition` walks `def.Labels`, not the condition map, and
    /// skips labels with no values.
    #[test]
    fn gen_label_condition_follows_definition_order_and_skips_empty() {
        let mut labels = LabelConditions::new();
        labels.insert("type".to_owned(), set(&["kv"]));
        // Not a label of the definition at all: invisible.
        labels.insert("store".to_owned(), set(&["1"]));
        // Present but empty: skipped by the `len(values) == 0` guard.
        labels.insert("instance".to_owned(), BTreeSet::new());
        assert_eq!(quantile_def().gen_label_condition(&labels), "type=\"kv\"");
    }

    /// Go `GenLabelConditionValues` (`:145`) sorts then joins with `|`.
    #[test]
    fn label_condition_values_are_sorted_and_pipe_joined() {
        assert_eq!(gen_label_condition_values(&set(&["b", "a", "c"])), "a|b|c");
        assert_eq!(gen_label_condition_values(&BTreeSet::new()), "");
    }

    /// Go `getQueryRange` (`:138`): step is `MetricSchemaStep` seconds.
    #[test]
    fn query_range_carries_extractor_window_and_step() {
        let retriever = MetricRetriever::new("tidb_query_duration".to_owned(), extractor());
        let range = retriever.get_query_range(30);
        assert_eq!(range.start, Utc.timestamp_opt(1_600_000_000, 0).unwrap());
        assert_eq!(range.end, Utc.timestamp_opt(1_600_003_600, 0).unwrap());
        assert_eq!(range.step, Duration::from_secs(30));
    }

    /// Go `retrieve` (`:54`) issues one query per quantile and concatenates
    /// the per-quantile row batches in quantile order.
    #[test]
    fn retrieve_queries_once_per_quantile() {
        let mut ext = extractor();
        ext.quantiles = vec![0.9, 0.99];
        let mut retriever = MetricRetriever::new("q".to_owned(), ext);
        let mut querier = FixedQuerier {
            seen: Vec::new(),
            results: vec![
                Ok(PromValue::Matrix(vec![SampleStream {
                    metric: None,
                    values: vec![SamplePair {
                        timestamp_ms: 1_600_000_000_000,
                        value: 1.0,
                    }],
                }])),
                Ok(PromValue::Matrix(vec![SampleStream {
                    metric: None,
                    values: vec![SamplePair {
                        timestamp_ms: 1_600_000_000_000,
                        value: 2.0,
                    }],
                }])),
            ],
        };
        let rows = retriever
            .retrieve(&defs(&[("q", quantile_def())]), &mut querier, 30, 60, &Utc)
            .unwrap();
        assert_eq!(querier.seen.len(), 2);
        assert!(querier.seen[0].starts_with("histogram_quantile(0.9,"));
        assert!(querier.seen[1].starts_with("histogram_quantile(0.99,"));
        assert_eq!(rows.len(), 2);
        // time, instance, type, quantile, value.
        assert_eq!(rows[0].len(), 5);
        assert_eq!(rows[0][3], Datum::Real(0.9));
        assert_eq!(rows[0][4], Datum::Real(1.0));
        assert_eq!(rows[1][3], Datum::Real(0.99));
        assert_eq!(rows[1][4], Datum::Real(2.0));
    }

    /// Go `:75`: no explicit quantiles means the definition's own.
    #[test]
    fn retrieve_falls_back_to_the_definition_quantile() {
        let mut retriever = MetricRetriever::new("q".to_owned(), extractor());
        let mut querier = FixedQuerier {
            seen: Vec::new(),
            results: vec![Ok(PromValue::Matrix(Vec::new()))],
        };
        let rows = retriever
            .retrieve(&defs(&[("q", quantile_def())]), &mut querier, 30, 60, &Utc)
            .unwrap();
        assert!(rows.is_empty());
        assert_eq!(querier.seen.len(), 1);
        assert!(querier.seen[0].starts_with("histogram_quantile(0.99,"));
    }

    /// Go `:55`: `retrieved` and `SkipRequest` both short-circuit.
    #[test]
    fn retrieve_answers_at_most_once() {
        let mut retriever = MetricRetriever::new("q".to_owned(), extractor());
        let mut querier = FixedQuerier {
            seen: Vec::new(),
            results: vec![Ok(PromValue::Matrix(Vec::new()))],
        };
        let all = defs(&[("q", quantile_def())]);
        assert!(retriever
            .retrieve(&all, &mut querier, 30, 60, &Utc)
            .unwrap()
            .is_empty());
        assert!(retriever
            .retrieve(&all, &mut querier, 30, 60, &Utc)
            .unwrap()
            .is_empty());
        assert_eq!(querier.seen.len(), 1);

        let mut skipped_ext = extractor();
        skipped_ext.skip_request = true;
        let mut skipped = MetricRetriever::new("q".to_owned(), skipped_ext);
        let mut idle = FixedQuerier {
            seen: Vec::new(),
            results: Vec::new(),
        };
        assert!(skipped
            .retrieve(&all, &mut idle, 30, 60, &Utc)
            .unwrap()
            .is_empty());
        assert!(idle.seen.is_empty());
    }

    /// Go `:67`: an unknown table name surfaces `GetMetricTableDef`'s error.
    #[test]
    fn retrieve_rejects_an_unknown_metric_table() {
        let mut retriever = MetricRetriever::new("nope".to_owned(), extractor());
        let mut querier = FixedQuerier {
            seen: Vec::new(),
            results: Vec::new(),
        };
        let err = retriever
            .retrieve(&defs(&[("q", quantile_def())]), &mut querier, 30, 60, &Utc)
            .unwrap_err();
        assert_eq!(
            err,
            MetricsReaderError::UnknownMetricTable {
                name: "nope".to_owned()
            }
        );
    }

    /// Go `:82`-`:86`: the two error spellings.
    #[test]
    fn retrieve_renders_both_query_error_shapes() {
        let mut retriever = MetricRetriever::new("q".to_owned(), extractor());
        let mut api_err = FixedQuerier {
            seen: Vec::new(),
            results: vec![Err(PromQueryError::Api {
                msg: "bad_data".to_owned(),
                detail: "parse error".to_owned(),
            })],
        };
        let all = defs(&[("q", quantile_def())]);
        assert_eq!(
            retriever
                .retrieve(&all, &mut api_err, 30, 60, &Utc)
                .unwrap_err(),
            MetricsReaderError::QueryMetric {
                message: "query metric error, msg: bad_data, detail: parse error".to_owned()
            }
        );

        let mut other = MetricRetriever::new("q".to_owned(), extractor());
        let mut other_err = FixedQuerier {
            seen: Vec::new(),
            results: vec![Err(PromQueryError::Other {
                message: "dial tcp: refused".to_owned(),
            })],
        };
        assert_eq!(
            retriever_error(&mut other, &all, &mut other_err),
            MetricsReaderError::QueryMetric {
                message: "query metric error: dial tcp: refused".to_owned()
            }
        );
    }

    fn retriever_error(
        retriever: &mut MetricRetriever<Utc>,
        all: &MetricTableDefs,
        querier: &mut FixedQuerier,
    ) -> MetricsReaderError {
        retriever.retrieve(all, querier, 30, 60, &Utc).unwrap_err()
    }

    /// Go `genRecord` (`:158`): column order, the label fallback to the WHERE
    /// clause's pinned values, and NaN -> NULL.
    #[test]
    fn gen_record_column_order_label_fallback_and_nan() {
        let mut ext = extractor();
        ext.label_conditions
            .insert("type".to_owned(), set(&["kv", "cop"]));
        let mut retriever = MetricRetriever::new("q".to_owned(), ext);
        let mut querier = FixedQuerier {
            seen: Vec::new(),
            results: vec![Ok(PromValue::Matrix(vec![SampleStream {
                // `type` is absent from the series, so it falls back.
                metric: Some(
                    [("instance".to_owned(), "10.0.0.1:10080".to_owned())]
                        .into_iter()
                        .collect(),
                ),
                values: vec![
                    SamplePair {
                        timestamp_ms: 1_600_000_000_000,
                        value: 7.5,
                    },
                    SamplePair {
                        timestamp_ms: 1_600_000_030_000,
                        value: f64::NAN,
                    },
                ],
            }]))],
        };
        let rows = retriever
            .retrieve(&defs(&[("q", quantile_def())]), &mut querier, 30, 60, &Utc)
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert!(matches!(rows[0][0], Datum::Time(_)));
        assert_eq!(rows[0][1], Datum::new_string("10.0.0.1:10080"));
        assert_eq!(rows[0][2], Datum::new_string("cop|kv"));
        assert_eq!(rows[0][3], Datum::Real(0.99));
        assert_eq!(rows[0][4], Datum::Real(7.5));
        assert_eq!(rows[1][4], Datum::Null);
    }

    /// Go `:176`: a non-quantile definition emits no `quantile` column, and
    /// `:168`'s nil-metric guard leaves the labels empty when nothing pinned
    /// them.
    #[test]
    fn gen_record_without_quantile_column() {
        let mut retriever = MetricRetriever::new("p".to_owned(), extractor());
        let mut querier = FixedQuerier {
            seen: Vec::new(),
            results: vec![Ok(PromValue::Matrix(vec![SampleStream {
                metric: None,
                values: vec![SamplePair {
                    timestamp_ms: 1_600_000_000_000,
                    value: 3.0,
                }],
            }]))],
        };
        let rows = retriever
            .retrieve(&defs(&[("p", plain_def())]), &mut querier, 30, 60, &Utc)
            .unwrap();
        // time, value only.
        assert_eq!(rows[0].len(), 2);
        assert_eq!(rows[0][1], Datum::Real(3.0));
    }

    /// Go `genRows` (`:146`): a non-matrix value yields nothing.
    #[test]
    fn gen_rows_ignores_non_matrix_values() {
        let mut retriever = MetricRetriever::new("q".to_owned(), extractor());
        let mut querier = FixedQuerier {
            seen: Vec::new(),
            results: vec![Ok(PromValue::NonMatrix)],
        };
        assert!(retriever
            .retrieve(&defs(&[("q", quantile_def())]), &mut querier, 30, 60, &Utc)
            .unwrap()
            .is_empty());
    }

    /// Go `:233` / `:236`.
    #[test]
    fn summary_sql_shapes() {
        let cond = "where time>='2020-01-01 00:00:00' and time<='2020-01-01 01:00:00'";
        assert_eq!(
            metrics_summary_sql("q", &quantile_def(), cond, &[]),
            format!("select sum(value),avg(value),min(value),max(value),quantile from `metrics_schema`.`q` {cond} and quantile in (0.99) group by quantile order by quantile")
        );
        assert_eq!(
            metrics_summary_sql("q", &quantile_def(), cond, &[0.8, 0.95]),
            format!("select sum(value),avg(value),min(value),max(value),quantile from `metrics_schema`.`q` {cond} and quantile in (0.800000,0.950000) group by quantile order by quantile")
        );
        assert_eq!(
            metrics_summary_sql("p", &plain_def(), cond, &[]),
            format!("select sum(value),avg(value),min(value),max(value) from `metrics_schema`.`p` {cond}")
        );
    }

    /// Go `:316` / `:319`, including the `quantile` column appended to the
    /// grouping key list and the `quantile=0.99` default clause.
    #[test]
    fn summary_by_label_sql_shapes() {
        let cond = "where time>='a' and time<='b'";
        assert_eq!(
            metrics_summary_by_label_sql("q", &quantile_def(), cond, &[]),
            format!("select sum(value),avg(value),min(value),max(value),`instance`,`type`,`quantile` from `metrics_schema`.`q` {cond} and quantile=0.99 group by `instance`,`type`,`quantile` order by `instance`,`type`,`quantile`")
        );
        assert_eq!(
            metrics_summary_by_label_sql("q", &quantile_def(), cond, &[0.9]),
            format!("select sum(value),avg(value),min(value),max(value),`instance`,`type`,`quantile` from `metrics_schema`.`q` {cond} and quantile in (0.900000) group by `instance`,`type`,`quantile` order by `instance`,`type`,`quantile`")
        );
        assert_eq!(
            metrics_summary_by_label_sql("p", &plain_def(), cond, &[]),
            format!("select sum(value),avg(value),min(value),max(value) from `metrics_schema`.`p` {cond}")
        );
        // The definition must not observe Go's `append` to `cols`.
        assert_eq!(quantile_def().labels, vec!["instance", "type"]);
    }

    /// Go `:197` / `:274`: PROCESS is required before anything else happens.
    #[test]
    fn summary_retrievers_require_process_priv() {
        let all = defs(&[("q", quantile_def())]);
        let mut exec = FixedExec {
            seen: Vec::new(),
            rows: Vec::new(),
            err: None,
        };
        let denied = MetricsReaderError::SpecificAccessDenied {
            privilege: "PROCESS".to_owned(),
        };
        assert_eq!(
            MetricsSummaryRetriever::default()
                .retrieve(&all, &mut exec, "where 1", false)
                .unwrap_err(),
            denied
        );
        assert_eq!(
            MetricsSummaryByLabelRetriever::default()
                .retrieve(&all, &mut exec, "where 1", false)
                .unwrap_err(),
            denied
        );
        assert!(exec.seen.is_empty());
    }

    /// Go `:245`-`:259`: name, quantile (last column), sum/avg/min/max,
    /// comment.
    #[test]
    fn summary_retrieve_row_shape() {
        let mut retriever = MetricsSummaryRetriever::new(MetricSummaryTableExtractor::default());
        let mut exec = FixedExec {
            seen: Vec::new(),
            rows: vec![vec![
                Datum::Real(10.0),
                Datum::Real(2.5),
                Datum::Real(1.0),
                Datum::Real(4.0),
                Datum::Real(0.99),
            ]],
            err: None,
        };
        let outcome = retriever
            .retrieve(
                &defs(&[("q", quantile_def()), ("p", plain_def())]),
                &mut exec,
                "where 1",
                true,
            )
            .unwrap();
        // Both tables are visited, in sorted name order: `p` then `q`.
        assert_eq!(exec.seen.len(), 2);
        assert!(exec.seen[0].contains("`metrics_schema`.`p`"));
        assert!(exec.seen[1].contains("`metrics_schema`.`q`"));
        assert_eq!(outcome.rows.len(), 2);
        // `p` has no quantile, so column 1 is NULL and the comment is its own.
        assert_eq!(outcome.rows[0][0], Datum::new_string("p"));
        assert_eq!(outcome.rows[0][1], Datum::Null);
        assert_eq!(outcome.rows[0][6], Datum::new_string("the plain comment"));
        // `q` takes the last result column as the quantile.
        assert_eq!(outcome.rows[1][0], Datum::new_string("q"));
        assert_eq!(outcome.rows[1][1], Datum::Real(0.99));
        assert_eq!(outcome.rows[1][2], Datum::Real(10.0));
        assert_eq!(outcome.rows[1][5], Datum::Real(4.0));
        assert_eq!(
            outcome.rows[1][6],
            Datum::new_string("the quantile comment")
        );
    }

    /// Go `:327`-`:361`: the leading `instance` label becomes its own column
    /// and `store`/`store_id` values are prefixed.
    #[test]
    fn summary_by_label_retrieve_lifts_instance_and_prefixes_stores() {
        let def = MetricTableDef {
            prom_ql: String::new(),
            labels: vec!["instance".to_owned(), "store".to_owned(), "type".to_owned()],
            quantile: 0.0,
            comment: "c".to_owned(),
        };
        let mut retriever =
            MetricsSummaryByLabelRetriever::new(MetricSummaryTableExtractor::default());
        let mut exec = FixedExec {
            seen: Vec::new(),
            rows: vec![vec![
                Datum::Real(10.0),
                Datum::Real(2.5),
                Datum::Real(1.0),
                Datum::Real(4.0),
                Datum::new_string("10.0.0.1:20160"),
                Datum::new_string("7"),
                Datum::new_string("kv"),
            ]],
            err: None,
        };
        let outcome = retriever
            .retrieve(&defs(&[("t", def)]), &mut exec, "where 1", true)
            .unwrap();
        let row = &outcome.rows[0];
        assert_eq!(row[0], Datum::new_string("10.0.0.1:20160"));
        assert_eq!(row[1], Datum::new_string("t"));
        assert_eq!(row[2], Datum::new_string("store_id:7, kv"));
        assert_eq!(row[3], Datum::Null);
        assert_eq!(row[4], Datum::Real(10.0));
        assert_eq!(row[8], Datum::new_string("c"));
    }

    /// Go `:243` / `:325`: the failing SQL is echoed in the error.
    #[test]
    fn summary_retrieve_wraps_executor_errors() {
        let mut retriever = MetricsSummaryRetriever::new(MetricSummaryTableExtractor::default());
        let mut exec = FixedExec {
            seen: Vec::new(),
            rows: Vec::new(),
            err: Some("boom".to_owned()),
        };
        let err = retriever
            .retrieve(&defs(&[("p", plain_def())]), &mut exec, "where 1", true)
            .unwrap_err();
        let MetricsReaderError::ExecuteSql { message } = err else {
            panic!("expected ExecuteSql");
        };
        assert!(message.starts_with("execute 'select sum(value)"));
        assert!(message.ends_with("failed: boom"));
    }

    /// Go `:212` / `:289`: an empty `MetricsNames` set passes everything.
    #[test]
    fn inspection_filter_empty_set_passes_all() {
        assert!(InspectionFilter::default().enable("anything"));
        let filter = InspectionFilter { set: set(&["a"]) };
        assert!(filter.enable("a"));
        assert!(!filter.enable("b"));
    }

    /// Go `:200` / `:277`: `SkipRequest` and the one-shot `retrieved` flag.
    #[test]
    fn summary_retrieve_is_one_shot_and_skippable() {
        let all = defs(&[("p", plain_def())]);
        let mut exec = FixedExec {
            seen: Vec::new(),
            rows: Vec::new(),
            err: None,
        };
        let mut once = MetricsSummaryRetriever::default();
        assert!(once
            .retrieve(&all, &mut exec, "where 1", true)
            .unwrap()
            .rows
            .is_empty());
        assert_eq!(exec.seen.len(), 1);
        assert!(once
            .retrieve(&all, &mut exec, "where 1", true)
            .unwrap()
            .rows
            .is_empty());
        assert_eq!(exec.seen.len(), 1);

        let mut skipped = MetricsSummaryByLabelRetriever::new(MetricSummaryTableExtractor {
            skip_request: true,
            ..MetricSummaryTableExtractor::default()
        });
        assert!(skipped
            .retrieve(&all, &mut exec, "where 1", true)
            .unwrap()
            .rows
            .is_empty());
        assert_eq!(exec.seen.len(), 1);
    }
}
