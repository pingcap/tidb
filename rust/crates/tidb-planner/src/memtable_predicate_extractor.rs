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

//! Go `pkg/planner/core/memtable_predicate_extractor.go`: the predicate
//! extractors a memory table owns.
//!
//! `LogicalMemTable.PredicatePushDown` hands its predicates to the table's
//! extractor, which claims the ones it can turn into a request filter (node
//! types, instances, time ranges, names) and returns the rest. The claimed
//! predicates are not evaluated again above the scan: the reader applies them.
//! The information-schema extractors live in
//! [`crate::memtable_infoschema_extractor`].
//!
//! Go's readers request only what the extractor kept. This port's readers
//! materialize a whole virtual table first, so [`MemTablePredicateExtractor::keeps_row`]
//! applies the claimed filters to those rows; its doc names the extractors whose
//! claimed time ranges and patterns have no materialized rows to apply to.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use tidb_datatype::{
    core_time_from_datetime, ConversionFlags, Datum, FieldName, FieldType, FieldTypeCode,
    SessionTimeZone, Time, TimeType,
};
use tidb_expr::constant::Constant;
use tidb_expr::expr_util::normal_form::split_dnf_items;
use tidb_expr::expression::Expression;
use tidb_expr::scalar_function::ScalarFunction;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;
use tidb_hack::{go_to_lower, go_to_upper};

pub use crate::memtable_infoschema_extractor::{InfoSchemaBaseExtractor, InfoSchemaExtractorKind};
use crate::physical::PhysicalPlan;

/// Go `set.StringSet`, ordered so every rendering is deterministic.
pub type StringSet = BTreeSet<String>;

const NANOS_PER_MILLI: i64 = 1_000_000;

/// Go `util.MetricTableTimeFormat` is `"2006-01-02 15:04:05.999"`.
const METRIC_TABLE_TIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

/// The session facts Go's `ExplainInfo` bodies read off `p.SCtx()`.
pub trait MemTableExplainEnv {
    /// Go `StmtCtx.TimeZone()`.
    fn time_zone(&self) -> SessionTimeZone;
    /// Go `SessionVars.SlowQueryFile`.
    fn slow_query_file(&self) -> String;
    /// Go `SessionVars.MetricSchemaStep`, in seconds.
    fn metric_schema_step(&self) -> i64;
    /// Go `SessionVars.MetricSchemaRangeDuration`, in seconds.
    fn metric_schema_range_duration(&self) -> i64;
}

/// The function a `lower(col) = 'x'` / `upper(col) = 'x'` predicate applied to
/// its column (Go `extractHelper.pushedDownFuncs`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PushedDownFunc {
    Lower,
    Upper,
}

impl PushedDownFunc {
    pub(crate) fn apply(self, value: &str) -> String {
        match self {
            Self::Lower => go_to_lower(value),
            Self::Upper => go_to_upper(value),
        }
    }
}

/// Go `extractHelper`: the utility half every extractor embeds.
#[derive(Clone, Debug, Default)]
pub(crate) struct ExtractHelper {
    /// Go `pushedDownFuncs`. Go replaces the whole map on every assignment,
    /// so it holds at most the most recent column.
    pub(crate) pushed_down_funcs: BTreeMap<String, PushedDownFunc>,
    /// Go `extractLowerString`: whether each extracted column's values were
    /// lower-cased.
    pub(crate) extract_lower_string: BTreeMap<String, bool>,
}

/// One constant's value. Go reads a parameter marker's current value through
/// `ParamMarker.GetUserVar`; memory-table plans are never cached, so the value
/// bound at build time is that same value.
fn constant_value(constant: &Constant) -> Option<Datum> {
    if constant.deferred_expr.is_some() {
        return None;
    }
    Some(constant.value.clone())
}

fn function_name(function: &ScalarFunction) -> &str {
    function.func_name.lowercase()
}

/// Go `Datum.ToString`, which fails for NULL and the range sentinels.
fn datum_to_string(datum: &Datum) -> Option<String> {
    if matches!(datum, Datum::Null) {
        return None;
    }
    datum.sql_string().ok()
}

/// Go `Datum.GetString`: the payload of a string datum, empty otherwise.
fn datum_get_string(datum: &Datum) -> String {
    match datum {
        Datum::String(value) => String::from_utf8_lossy(value.bytes()).into_owned(),
        Datum::Bytes(value) => String::from_utf8_lossy(value).into_owned(),
        _ => String::new(),
    }
}

/// Go `regexp.QuoteMeta`.
fn quote_meta(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len());
    for ch in value.chars() {
        if r"\.+*?()|[]{}^$".contains(ch) {
            quoted.push('\\');
        }
        quoted.push(ch);
    }
    quoted
}

impl ExtractHelper {
    /// Go `extractHelper.findColumn`: the schema columns named `col_name`.
    pub(crate) fn find_column(
        schema: &Schema,
        names: &[FieldName],
        col_name: &str,
    ) -> BTreeMap<i64, String> {
        let mut extract_cols = BTreeMap::new();
        for (i, name) in names.iter().enumerate() {
            if name.names.column.lower == col_name {
                if let Some(column) = schema.columns.get(i) {
                    extract_cols.insert(column.unique_id, name.names.column.lower.clone());
                }
            }
        }
        extract_cols
    }

    /// Go `extractHelper.extractColInConsExpr`: `col IN (constants)`.
    fn extract_col_in_cons_expr(
        extract_cols: &BTreeMap<i64, String>,
        function: &ScalarFunction,
    ) -> Option<(String, Vec<Datum>)> {
        let (first, rest) = function.args.split_first()?;
        let Expression::Column(column) = first else {
            return None;
        };
        let name = extract_cols.get(&column.unique_id)?;
        let mut results = Vec::with_capacity(rest.len());
        for arg in rest {
            let Expression::Constant(constant) = arg else {
                return None;
            };
            results.push(constant_value(constant)?);
        }
        Some((name.clone(), results))
    }

    /// Go `extractHelper.setColumnPushedDownFn`.
    fn set_column_pushed_down_fn(
        &mut self,
        col_name: &str,
        extract_cols: &BTreeMap<i64, String>,
        function: &ScalarFunction,
    ) {
        let Some(scalar) = Self::extract_col_binary_op_scalar_func(extract_cols, function) else {
            return;
        };
        let func = match function_name(scalar) {
            "lower" => PushedDownFunc::Lower,
            "upper" => PushedDownFunc::Upper,
            _ => return,
        };
        self.pushed_down_funcs = BTreeMap::from([(col_name.to_owned(), func)]);
    }

    /// Go `extractHelper.extractColBinaryOpScalarFunc`: the one-column
    /// function on the non-constant side, as `lower` in
    /// `eq(lower(col), "constant")`.
    fn extract_col_binary_op_scalar_func<'a>(
        extract_cols: &BTreeMap<i64, String>,
        function: &'a ScalarFunction,
    ) -> Option<&'a ScalarFunction> {
        let args = &function.args;
        if args.len() < 2 {
            return None;
        }
        let const_idx = (0..2)
            .find(|i| matches!(args[*i], Expression::Constant(_)))
            .unwrap_or(0);
        let Expression::ScalarFunction(scalar) = &args[1 - const_idx] else {
            return None;
        };
        let [Expression::Column(column)] = scalar.args.as_slice() else {
            return None;
        };
        extract_cols
            .contains_key(&column.unique_id)
            .then_some(scalar)
    }

    /// Go `extractHelper.tryToFindInnerColAndIdx`: under scalar push-down, the
    /// column inside a `lower`/`upper` argument.
    fn try_to_find_inner_col_and_idx(
        enable_scalar_push_down: bool,
        args: &[Expression],
    ) -> Option<(&tidb_expr::column::Column, usize)> {
        if !enable_scalar_push_down {
            return None;
        }
        let (col_idx, scalar) = args
            .iter()
            .take(2)
            .enumerate()
            .find_map(|(i, arg)| match arg {
                Expression::ScalarFunction(scalar) => Some((i, scalar)),
                _ => None,
            })?;
        let [Expression::Column(column)] = scalar.args.as_slice() else {
            return None;
        };
        matches!(function_name(scalar), "lower" | "upper").then_some((column, col_idx))
    }

    /// Go `extractHelper.extractColBinaryOpConsExpr`: `col op constant` or
    /// `constant op col`, and whether the column is on the left.
    fn extract_col_binary_op_cons_expr(
        enable_scalar_push_down: bool,
        extract_cols: &BTreeMap<i64, String>,
        function: &ScalarFunction,
    ) -> Option<(String, Vec<Datum>, bool)> {
        let args = &function.args;
        if args.len() < 2 {
            return None;
        }
        let mut found = args
            .iter()
            .take(2)
            .enumerate()
            .find_map(|(i, arg)| match arg {
                Expression::Column(column) => Some((column, i)),
                _ => None,
            });
        if let Some(inner) = Self::try_to_find_inner_col_and_idx(enable_scalar_push_down, args) {
            found = Some(inner);
        }
        let (column, col_idx) = found?;
        let name = extract_cols.get(&column.unique_id)?;
        let Expression::Constant(constant) = &args[1 - col_idx] else {
            return None;
        };
        let value = constant_value(constant)?;
        Some((name.clone(), vec![value], col_idx == 0))
    }

    /// Go `extractHelper.extractColOrExpr`: `c='a' OR c='b' OR c IN (...)`
    /// over one column.
    fn extract_col_or_expr(
        extract_cols: &BTreeMap<i64, String>,
        function: &ScalarFunction,
    ) -> Option<(String, Vec<Datum>)> {
        let [Expression::ScalarFunction(lhs), Expression::ScalarFunction(rhs), ..] =
            function.args.as_slice()
        else {
            return None;
        };
        let extract = |function: &ScalarFunction| match function_name(function) {
            "eq" => Self::extract_col_binary_op_cons_expr(false, extract_cols, function)
                .map(|(name, datums, _)| (name, datums)),
            "or" => Self::extract_col_or_expr(extract_cols, function),
            "in" => Self::extract_col_in_cons_expr(extract_cols, function),
            _ => None,
        };
        let (lhs_name, mut lhs_datums) = extract(lhs)?;
        let (rhs_name, rhs_datums) = extract(rhs)?;
        if lhs_name != rhs_name {
            return None;
        }
        lhs_datums.extend(rhs_datums);
        Some((lhs_name, lhs_datums))
    }

    /// Go `extractHelper.merge`: the CNF intersection of `lhs` with the
    /// datums' strings, or the datums alone when `lhs` is empty. A datum Go
    /// cannot stringify empties the result, which reads as a contradiction.
    fn merge(lhs: &StringSet, datums: &[Datum], to_lower: bool) -> StringSet {
        let mut values = StringSet::new();
        for datum in datums {
            let Some(value) = datum_to_string(datum) else {
                return StringSet::new();
            };
            values.insert(if to_lower { go_to_lower(&value) } else { value });
        }
        if lhs.is_empty() {
            values
        } else {
            lhs.intersection(&values).cloned().collect()
        }
    }

    /// Go `extractHelper.extractCol`: claims every `=`, `IN` and same-column
    /// `OR` predicate over `extract_col_name`. Returns the unclaimed
    /// predicates, whether the claimed ones contradict, and their values.
    pub(crate) fn extract_col(
        &mut self,
        schema: &Schema,
        names: &[FieldName],
        predicates: Vec<Expression>,
        extract_col_name: &str,
        value_to_lower: bool,
    ) -> (Vec<Expression>, bool, StringSet) {
        let mut result = StringSet::new();
        let extract_cols = Self::find_column(schema, names, extract_col_name);
        if extract_cols.is_empty() {
            return (predicates, false, result);
        }
        let mut remained = Vec::with_capacity(predicates.len());
        let mut skip_request = false;
        // The predicates are a CNF list, so the values intersect.
        for expr in predicates {
            let Expression::ScalarFunction(function) = &expr else {
                remained.push(expr);
                continue;
            };
            let extracted = match function_name(function) {
                "eq" => {
                    let extracted =
                        Self::extract_col_binary_op_cons_expr(true, &extract_cols, function)
                            .map(|(name, datums, _)| (name, datums));
                    if extracted
                        .as_ref()
                        .is_some_and(|(name, _)| name == extract_col_name)
                    {
                        self.set_column_pushed_down_fn(extract_col_name, &extract_cols, function);
                    }
                    extracted
                }
                "in" => Self::extract_col_in_cons_expr(&extract_cols, function),
                "or" => Self::extract_col_or_expr(&extract_cols, function),
                _ => None,
            };
            match extracted {
                Some((name, datums)) if name == extract_col_name => {
                    result = Self::merge(&result, &datums, value_to_lower);
                    skip_request = result.is_empty();
                }
                _ => remained.push(expr),
            }
            // The reader requests nothing, so no predicate needs to remain.
            if skip_request {
                remained.clear();
                break;
            }
        }
        self.extract_lower_string
            .insert(extract_col_name.to_owned(), value_to_lower);
        (remained, skip_request, result)
    }

    /// Go `extractHelper.extractLikePatternCol`: the `LIKE` / `REGEXP` / `=`
    /// patterns over one string column, one pattern per CNF item.
    pub(crate) fn extract_like_pattern_col(
        schema: &Schema,
        names: &[FieldName],
        predicates: Vec<Expression>,
        extract_col_name: &str,
        to_lower: bool,
        need_like_to_regexp: bool,
    ) -> (Vec<Expression>, Vec<String>) {
        let extract_cols = Self::find_column(schema, names, extract_col_name);
        if extract_cols.is_empty() {
            return (predicates, Vec::new());
        }
        let mut remained = Vec::with_capacity(predicates.len());
        let mut patterns = Vec::new();
        for expr in predicates {
            let Expression::ScalarFunction(function) = &expr else {
                remained.push(expr);
                continue;
            };
            // `c LIKE '%a%' OR c LIKE '%b%'` becomes one DNF regexp.
            let pattern = if function_name(function) == "or" && !to_lower {
                Self::extract_or_like_pattern(
                    function,
                    extract_col_name,
                    &extract_cols,
                    need_like_to_regexp,
                )
            } else {
                Self::extract_like_pattern(
                    function,
                    extract_col_name,
                    &extract_cols,
                    need_like_to_regexp,
                )
            };
            match pattern {
                Some(pattern) => patterns.push(if to_lower {
                    go_to_lower(&pattern)
                } else {
                    pattern
                }),
                None => remained.push(expr),
            }
        }
        (remained, patterns)
    }

    /// Go `extractHelper.extractOrLikePattern`.
    fn extract_or_like_pattern(
        function: &ScalarFunction,
        extract_col_name: &str,
        extract_cols: &BTreeMap<i64, String>,
        need_like_to_regexp: bool,
    ) -> Option<String> {
        let predicates = split_dnf_items(&Expression::ScalarFunction(function.clone()));
        if predicates.is_empty() {
            return None;
        }
        let mut parts = Vec::with_capacity(predicates.len());
        for predicate in &predicates {
            let Expression::ScalarFunction(function) = predicate else {
                return None;
            };
            parts.push(Self::extract_like_pattern(
                function,
                extract_col_name,
                extract_cols,
                need_like_to_regexp,
            )?);
        }
        Some(parts.join("|"))
    }

    /// Go `extractHelper.extractLikePattern`.
    fn extract_like_pattern(
        function: &ScalarFunction,
        extract_col_name: &str,
        extract_cols: &BTreeMap<i64, String>,
        need_like_to_regexp: bool,
    ) -> Option<String> {
        let name = function_name(function);
        if !matches!(name, "eq" | "like" | "ilike" | "regexp" | "regexp_like") {
            return None;
        }
        let (col_name, datums, _) =
            Self::extract_col_binary_op_cons_expr(false, extract_cols, function)?;
        if col_name != extract_col_name {
            return None;
        }
        let value = datum_get_string(&datums[0]);
        match name {
            "eq" => Some(format!("^{}$", quote_meta(&value))),
            "like" | "ilike" if need_like_to_regexp => Some(
                tidb_util::stringutil::compile_like_to_regexp(value.as_bytes(), b'\\'),
            ),
            _ => Some(value),
        }
    }

    /// Go `extractHelper.extractTimeRange`: the time window over one column
    /// as Unix nanoseconds, zero meaning unbounded.
    pub(crate) fn extract_time_range(
        ctx: &dyn Columns,
        schema: &Schema,
        names: &[FieldName],
        predicates: Vec<Expression>,
        extract_col_name: &str,
        timezone: &SessionTimeZone,
    ) -> (Vec<Expression>, i64, i64) {
        let (mut start_time, mut end_time) = (0_i64, 0_i64);
        let extract_cols = Self::find_column(schema, names, extract_col_name);
        if extract_cols.is_empty() {
            return (predicates, start_time, end_time);
        }
        let mut remained = Vec::with_capacity(predicates.len());
        for expr in predicates {
            let Expression::ScalarFunction(function) = &expr else {
                remained.push(expr);
                continue;
            };
            let mut fn_name = function_name(function).to_owned();
            let extracted = if matches!(fn_name.as_str(), "gt" | "ge" | "lt" | "le" | "eq") {
                Self::extract_col_binary_op_cons_expr(false, &extract_cols, function)
            } else {
                None
            };
            let Some((_, datums, col_on_left)) =
                extracted.filter(|(name, _, _)| name == extract_col_name)
            else {
                remained.push(expr);
                continue;
            };
            if !col_on_left {
                fn_name = match fn_name.as_str() {
                    "gt" => "lt",
                    "ge" => "le",
                    "lt" => "gt",
                    "le" => "ge",
                    other => other,
                }
                .to_owned();
            }
            let Some(timestamp) = datum_to_unix_nanos(ctx, &datums[0], timezone) else {
                remained.push(expr);
                continue;
            };
            // Go adds or subtracts one millisecond for a strict bound because
            // the log search precision is a millisecond.
            match fn_name.as_str() {
                "eq" => {
                    start_time = start_time.max(timestamp);
                    end_time = if end_time == 0 {
                        timestamp
                    } else {
                        end_time.min(timestamp)
                    };
                }
                "gt" => start_time = start_time.max(timestamp + NANOS_PER_MILLI),
                "ge" => start_time = start_time.max(timestamp),
                "lt" => {
                    end_time = if end_time == 0 {
                        timestamp - NANOS_PER_MILLI
                    } else {
                        end_time.min(timestamp - NANOS_PER_MILLI)
                    };
                }
                "le" => {
                    end_time = if end_time == 0 {
                        timestamp
                    } else {
                        end_time.min(timestamp)
                    };
                }
                _ => remained.push(expr),
            }
        }
        (remained, start_time, end_time)
    }

    /// Go `extractHelper.parseQuantiles`: the parsable values, sorted.
    fn parse_quantiles(quantile_set: &StringSet) -> Vec<f64> {
        let mut quantiles: Vec<f64> = quantile_set
            .iter()
            .filter_map(|value| value.parse::<f64>().ok())
            .collect();
        quantiles.sort_by(f64::total_cmp);
        quantiles
    }

    /// Go `extractHelper.parseUint64`: the parsable values, sorted.
    fn parse_uint64(uint64_set: &StringSet) -> Vec<u64> {
        let mut values: Vec<u64> = uint64_set
            .iter()
            .filter_map(|value| value.parse::<u64>().ok())
            .collect();
        values.sort_unstable();
        values
    }

    /// Go `extractHelper.extractCols`: every column outside `exclude_cols`.
    fn extract_cols(
        &mut self,
        schema: &Schema,
        names: &[FieldName],
        predicates: Vec<Expression>,
        exclude_cols: &[&str],
        value_to_lower: bool,
    ) -> (Vec<Expression>, bool, BTreeMap<String, StringSet>) {
        let mut cols = BTreeMap::new();
        let mut remained = predicates;
        for name in names {
            let col_name = &name.names.column.lower;
            if exclude_cols.contains(&col_name.as_str()) {
                continue;
            }
            let (rest, skip_request, values) =
                self.extract_col(schema, names, remained, col_name, value_to_lower);
            remained = rest;
            if skip_request {
                return (Vec::new(), true, BTreeMap::new());
            }
            if !values.is_empty() {
                cols.insert(col_name.clone(), values);
            }
        }
        (remained, false, cols)
    }

    /// Go `extractHelper.convertToTime`: zero and `MaxInt64` mean now.
    fn convert_to_time(t: i64) -> DateTime<Utc> {
        if t == 0 || t == i64::MAX {
            Utc::now()
        } else {
            DateTime::from_timestamp_nanos(t)
        }
    }

    /// Go `extractHelper.convertToBoolSlice`: unique, in first-seen order;
    /// both roles when none was named.
    fn convert_to_bool_slice(values: &[u64]) -> Vec<bool> {
        if values.is_empty() {
            return vec![false, true];
        }
        let mut result = Vec::new();
        for value in values {
            let role = *value == 1;
            if !result.contains(&role) {
                result.push(role);
            }
        }
        result
    }
}

/// Converts one comparison constant as Go's `extractTimeRange` does:
/// `ConvertTo(DATETIME(6))` in the statement context, then `time.Date` in
/// `timezone`, as Unix nanoseconds. `None` is Go's "keep the predicate".
fn datum_to_unix_nanos(
    ctx: &dyn Columns,
    datum: &Datum,
    timezone: &SessionTimeZone,
) -> Option<i64> {
    let mut target = FieldType::new(FieldTypeCode::Datetime);
    target.set_decimal(6);
    let converted = datum
        .convert_to_in(&target, ConversionFlags::default(), &ctx.time_zone())
        .ok()?;
    let Datum::Time(time) = converted.value else {
        return None;
    };
    let core = time.core_time();
    let naive =
        NaiveDate::from_ymd_opt(core.year(), u32::from(core.month()), u32::from(core.day()))?
            .and_hms_micro_opt(
                u32::from(core.hour()),
                u32::from(core.minute()),
                u32::from(core.second()),
                core.microsecond(),
            )?;
    let local = timezone.from_local_datetime(&naive);
    let instant = match local.earliest() {
        Some(instant) => instant,
        None => timezone.from_utc_datetime(&naive),
    };
    instant.timestamp_nanos_opt()
}

/// Go `time.Time.In(tz)`: the wall clock of `instant` in `tz`.
fn wall_clock(instant: DateTime<Utc>, tz: &SessionTimeZone) -> DateTime<SessionTimeZone> {
    tz.from_utc_datetime(&instant.naive_utc())
}

/// Go `t.Format(util.MetricTableTimeFormat)`: seconds, then up to three
/// fractional digits with trailing zeros (and a bare dot) dropped.
fn format_metric_time(instant: DateTime<Utc>, tz: &SessionTimeZone) -> String {
    let local = wall_clock(instant, tz);
    let mut text = local.format(METRIC_TABLE_TIME_FORMAT).to_string();
    let millis = local.timestamp_subsec_millis();
    if millis > 0 {
        let fraction = format!("{millis:03}");
        text.push('.');
        text.push_str(fraction.trim_end_matches('0'));
    }
    text
}

/// Go `types.NewTime(types.FromGoTime(t.In(tz)), mysql.TypeDatetime,
/// types.MaxFsp).String()`.
fn format_max_fsp_datetime(instant: DateTime<Utc>, tz: &SessionTimeZone) -> String {
    let core = core_time_from_datetime(wall_clock(instant, tz));
    Time::new(core, TimeType::DateTime, 6).map_or_else(|_| String::new(), |time| time.to_string())
}

fn millis_to_instant(millis: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(millis).unwrap_or_default()
}

/// Go `extractStringFromStringSet`: each value quoted, sorted, comma-joined.
pub(crate) fn extract_string_from_string_set(set: &StringSet) -> String {
    let mut quoted: Vec<String> = set.iter().map(|value| format!("\"{value}\"")).collect();
    quoted.sort();
    quoted.join(",")
}

/// Go `extractStringFromStringSlice`: sorted, comma-joined.
pub(crate) fn extract_string_from_string_slice(values: &[String]) -> String {
    let mut sorted = values.to_vec();
    sorted.sort();
    sorted.join(",")
}

/// Go `extractStringFromUint64Slice`: formatted, THEN sorted as strings.
fn extract_string_from_uint64_slice(values: &[u64]) -> String {
    let mut formatted: Vec<String> = values.iter().map(u64::to_string).collect();
    formatted.sort();
    formatted.join(",")
}

/// Go `extractStringFromBoolSlice`.
fn extract_string_from_bool_slice(values: &[bool]) -> String {
    let mut formatted: Vec<String> = values.iter().map(bool::to_string).collect();
    formatted.sort();
    formatted.join(",")
}

/// Go's `ExplainInfo` buffers end each part with `", "` and cut the last
/// one off, which is joining the parts.
fn trim_explain(parts: &[String]) -> String {
    parts.join(", ")
}

/// Go `ClusterTableExtractor`, for `CLUSTER_CONFIG`, `CLUSTER_LOAD`,
/// `CLUSTER_HARDWARE` and `CLUSTER_SYSTEMINFO`.
#[derive(Clone, Debug, Default)]
pub struct ClusterTableExtractor {
    helper: ExtractHelper,
    /// Go `SkipRequest`: the WHERE clause is always false.
    pub skip_request: bool,
    /// Go `NodeTypes`: the component types to ask, lower-cased.
    pub node_types: StringSet,
    /// Go `Instances`: the instances to ask.
    pub instances: StringSet,
}

impl ClusterTableExtractor {
    fn extract(
        &mut self,
        schema: &Schema,
        names: &[FieldName],
        predicates: Vec<Expression>,
    ) -> Vec<Expression> {
        let (remained, type_skip, node_types) = self
            .helper
            .extract_col(schema, names, predicates, "type", true);
        let (remained, addr_skip, instances) = self
            .helper
            .extract_col(schema, names, remained, "instance", false);
        self.skip_request = type_skip || addr_skip;
        self.node_types = node_types;
        self.instances = instances;
        remained
    }

    fn explain_info(&self) -> String {
        if self.skip_request {
            return "skip_request:true".to_owned();
        }
        let mut parts = Vec::new();
        if !self.node_types.is_empty() {
            parts.push(format!(
                "node_types:[{}]",
                extract_string_from_string_set(&self.node_types)
            ));
        }
        if !self.instances.is_empty() {
            parts.push(format!(
                "instances:[{}]",
                extract_string_from_string_set(&self.instances)
            ));
        }
        trim_explain(&parts)
    }

    /// Go `infoschema.FilterClusterServerInfo`'s test for one server.
    #[must_use]
    pub fn matches(&self, node_type: &str, instance: &str) -> bool {
        (self.node_types.is_empty() || self.node_types.contains(node_type))
            && (self.instances.is_empty() || self.instances.contains(instance))
    }
}

/// Go `ClusterLogTableExtractor`, for `CLUSTER_LOG`.
#[derive(Clone, Debug, Default)]
pub struct ClusterLogTableExtractor {
    helper: ExtractHelper,
    /// Go `SkipRequest`.
    pub skip_request: bool,
    /// Go `NodeTypes`, lower-cased.
    pub node_types: StringSet,
    /// Go `Instances`.
    pub instances: StringSet,
    /// Go `StartTime`, Unix milliseconds; zero when unbounded.
    pub start_time: i64,
    /// Go `EndTime`, Unix milliseconds; zero when unbounded.
    pub end_time: i64,
    /// Go `Patterns`: the message regexps.
    pub patterns: Vec<String>,
    /// Go `LogLevels`, lower-cased.
    pub log_levels: StringSet,
}

impl ClusterLogTableExtractor {
    fn extract(
        &mut self,
        ctx: &dyn Columns,
        schema: &Schema,
        names: &[FieldName],
        predicates: Vec<Expression>,
    ) -> Vec<Expression> {
        let (remained, type_skip, node_types) = self
            .helper
            .extract_col(schema, names, predicates, "type", true);
        let (remained, addr_skip, instances) = self
            .helper
            .extract_col(schema, names, remained, "instance", false);
        let (remained, level_skip, log_levels) = self
            .helper
            .extract_col(schema, names, remained, "level", true);
        self.skip_request = type_skip || addr_skip || level_skip;
        self.node_types = node_types;
        self.instances = instances;
        self.log_levels = log_levels;
        if self.skip_request {
            return Vec::new();
        }
        // Go searches the log in the server's local zone.
        let (remained, start_time, end_time) = ExtractHelper::extract_time_range(
            ctx,
            schema,
            names,
            remained,
            "time",
            &SessionTimeZone::Local,
        );
        // The time unit for searching the log is a millisecond.
        self.start_time = start_time / NANOS_PER_MILLI;
        self.end_time = end_time / NANOS_PER_MILLI;
        if self.start_time != 0 && self.end_time != 0 {
            self.skip_request = self.start_time > self.end_time;
        }
        if self.skip_request {
            return Vec::new();
        }
        let (remained, patterns) = ExtractHelper::extract_like_pattern_col(
            schema, names, remained, "message", false, true,
        );
        self.patterns = patterns;
        remained
    }

    fn explain_info(&self, env: &dyn MemTableExplainEnv) -> String {
        if self.skip_request {
            return "skip_request: true".to_owned();
        }
        let tz = env.time_zone();
        let mut parts = Vec::new();
        if self.start_time > 0 {
            parts.push(format!(
                "start_time:{}",
                format_metric_time(millis_to_instant(self.start_time), &tz)
            ));
        }
        if self.end_time > 0 {
            parts.push(format!(
                "end_time:{}",
                format_metric_time(millis_to_instant(self.end_time), &tz)
            ));
        }
        if !self.node_types.is_empty() {
            parts.push(format!(
                "node_types:[{}]",
                extract_string_from_string_set(&self.node_types)
            ));
        }
        if !self.instances.is_empty() {
            parts.push(format!(
                "instances:[{}]",
                extract_string_from_string_set(&self.instances)
            ));
        }
        if !self.log_levels.is_empty() {
            parts.push(format!(
                "log_levels:[{}]",
                extract_string_from_string_set(&self.log_levels)
            ));
        }
        trim_explain(&parts)
    }
}

/// Go `HotRegionTypeRead`.
pub const HOT_REGION_TYPE_READ: &str = "read";
/// Go `HotRegionTypeWrite`.
pub const HOT_REGION_TYPE_WRITE: &str = "write";

/// Go `HotRegionsHistoryTableExtractor`, for `TIDB_HOT_REGIONS_HISTORY`.
#[derive(Clone, Debug, Default)]
pub struct HotRegionsHistoryTableExtractor {
    helper: ExtractHelper,
    /// Go `SkipRequest`.
    pub skip_request: bool,
    /// Go `StartTime`, Unix milliseconds.
    pub start_time: i64,
    /// Go `EndTime`, Unix milliseconds.
    pub end_time: i64,
    /// Go `RegionIDs`.
    pub region_ids: Vec<u64>,
    /// Go `StoreIDs`.
    pub store_ids: Vec<u64>,
    /// Go `PeerIDs`.
    pub peer_ids: Vec<u64>,
    /// Go `IsLearners`.
    pub is_learners: Vec<bool>,
    /// Go `IsLeaders`.
    pub is_leaders: Vec<bool>,
    /// Go `HotRegionTypes`.
    pub hot_region_types: StringSet,
}

impl HotRegionsHistoryTableExtractor {
    fn extract(
        &mut self,
        ctx: &dyn Columns,
        schema: &Schema,
        names: &[FieldName],
        predicates: Vec<Expression>,
    ) -> Vec<Expression> {
        let h = &mut self.helper;
        let (remained, region_skip, region_ids) =
            h.extract_col(schema, names, predicates, "region_id", false);
        let (remained, store_skip, store_ids) =
            h.extract_col(schema, names, remained, "store_id", false);
        let (remained, peer_skip, peer_ids) =
            h.extract_col(schema, names, remained, "peer_id", false);
        self.region_ids = ExtractHelper::parse_uint64(&region_ids);
        self.store_ids = ExtractHelper::parse_uint64(&store_ids);
        self.peer_ids = ExtractHelper::parse_uint64(&peer_ids);
        self.skip_request = region_skip || store_skip || peer_skip;
        if self.skip_request {
            return Vec::new();
        }
        let (remained, learner_skip, is_learners) =
            h.extract_col(schema, names, remained, "is_learner", false);
        let (remained, leader_skip, is_leaders) =
            h.extract_col(schema, names, remained, "is_leader", false);
        self.skip_request = learner_skip || leader_skip;
        if self.skip_request {
            return Vec::new();
        }
        self.is_learners =
            ExtractHelper::convert_to_bool_slice(&ExtractHelper::parse_uint64(&is_learners));
        self.is_leaders =
            ExtractHelper::convert_to_bool_slice(&ExtractHelper::parse_uint64(&is_leaders));
        let (remained, type_skip, types) = h.extract_col(schema, names, remained, "type", false);
        self.hot_region_types = types;
        self.skip_request = type_skip;
        if self.skip_request {
            return Vec::new();
        }
        // PD keys hot regions by [type, time], so read and write are two
        // requests.
        if self.hot_region_types.is_empty() {
            self.hot_region_types
                .insert(HOT_REGION_TYPE_READ.to_owned());
            self.hot_region_types
                .insert(HOT_REGION_TYPE_WRITE.to_owned());
        }
        let (remained, start_time, end_time) = ExtractHelper::extract_time_range(
            ctx,
            schema,
            names,
            remained,
            "update_time",
            &ctx.time_zone(),
        );
        self.start_time = start_time / NANOS_PER_MILLI;
        self.end_time = end_time / NANOS_PER_MILLI;
        if self.start_time != 0 && self.end_time != 0 {
            self.skip_request = self.start_time > self.end_time;
        }
        if self.skip_request {
            return Vec::new();
        }
        remained
    }

    fn explain_info(&self, env: &dyn MemTableExplainEnv) -> String {
        if self.skip_request {
            return "skip_request: true".to_owned();
        }
        let tz = env.time_zone();
        let mut parts = Vec::new();
        // Go formats these with `time.DateTime`, seconds only.
        let date_time = |millis: i64| {
            wall_clock(millis_to_instant(millis), &tz)
                .format(METRIC_TABLE_TIME_FORMAT)
                .to_string()
        };
        if self.start_time > 0 {
            parts.push(format!("start_time:{}", date_time(self.start_time)));
        }
        if self.end_time > 0 {
            parts.push(format!("end_time:{}", date_time(self.end_time)));
        }
        for (label, ids) in [
            ("region_ids", &self.region_ids),
            ("store_ids", &self.store_ids),
            ("peer_ids", &self.peer_ids),
        ] {
            if !ids.is_empty() {
                parts.push(format!(
                    "{label}:[{}]",
                    extract_string_from_uint64_slice(ids)
                ));
            }
        }
        if !self.is_learners.is_empty() {
            parts.push(format!(
                "learner_roles:[{}]",
                extract_string_from_bool_slice(&self.is_learners)
            ));
        }
        if !self.is_leaders.is_empty() {
            parts.push(format!(
                "leader_roles:[{}]",
                extract_string_from_bool_slice(&self.is_leaders)
            ));
        }
        if !self.hot_region_types.is_empty() {
            parts.push(format!(
                "hot_region_types:[{}]",
                extract_string_from_string_set(&self.hot_region_types)
            ));
        }
        trim_explain(&parts)
    }
}

/// Go `MetricTableExtractor`, for every `metrics_schema` table.
#[derive(Clone, Debug)]
pub struct MetricTableExtractor {
    helper: ExtractHelper,
    /// Go `SkipRequest`.
    pub skip_request: bool,
    /// Go `StartTime`.
    pub start_time: DateTime<Utc>,
    /// Go `EndTime`.
    pub end_time: DateTime<Utc>,
    /// Go `LabelConditions`.
    pub label_conditions: BTreeMap<String, StringSet>,
    /// Go `Quantiles`.
    pub quantiles: Vec<f64>,
}

/// Go `defaultMetricQueryDuration`.
const DEFAULT_METRIC_QUERY_DURATION: chrono::Duration = chrono::Duration::minutes(10);

impl MetricTableExtractor {
    /// Go `newMetricTableExtractor`: the last ten minutes.
    #[must_use]
    pub fn new() -> Self {
        let (start_time, end_time) = Self::get_time_range(0, 0);
        Self {
            helper: ExtractHelper::default(),
            skip_request: false,
            start_time,
            end_time,
            label_conditions: BTreeMap::new(),
            quantiles: Vec::new(),
        }
    }

    fn extract(
        &mut self,
        ctx: &dyn Columns,
        schema: &Schema,
        names: &[FieldName],
        predicates: Vec<Expression>,
    ) -> Vec<Expression> {
        let (remained, skip_request, quantile_set) = self
            .helper
            .extract_col(schema, names, predicates, "quantile", true);
        self.quantiles = ExtractHelper::parse_quantiles(&quantile_set);
        self.skip_request = skip_request;
        if self.skip_request {
            return Vec::new();
        }
        let (remained, start_time, end_time) = ExtractHelper::extract_time_range(
            ctx,
            schema,
            names,
            remained,
            "time",
            &ctx.time_zone(),
        );
        (self.start_time, self.end_time) = Self::get_time_range(start_time, end_time);
        self.skip_request = self.start_time > self.end_time;
        if self.skip_request {
            return Vec::new();
        }
        let (_, skip_request, extract_cols) = self.helper.extract_cols(
            schema,
            names,
            remained.clone(),
            &["quantile", "time", "value"],
            false,
        );
        self.skip_request = skip_request;
        if self.skip_request {
            return Vec::new();
        }
        self.label_conditions = extract_cols;
        // Some metric readers cannot use a label predicate, so every label
        // condition stays.
        remained
    }

    /// Go `MetricTableExtractor.getTimeRange`.
    fn get_time_range(start: i64, end: i64) -> (DateTime<Utc>, DateTime<Utc>) {
        if start == 0 && end == 0 {
            let end_time = Utc::now();
            return (end_time - DEFAULT_METRIC_QUERY_DURATION, end_time);
        }
        let mut start_time = DateTime::<Utc>::default();
        let mut end_time = DateTime::<Utc>::default();
        if start != 0 {
            start_time = ExtractHelper::convert_to_time(start);
        }
        if end != 0 {
            end_time = ExtractHelper::convert_to_time(end);
        }
        if start == 0 {
            start_time = end_time - DEFAULT_METRIC_QUERY_DURATION;
        }
        if end == 0 {
            end_time = start_time + DEFAULT_METRIC_QUERY_DURATION;
        }
        (start_time, end_time)
    }

    fn explain_info(&self, table_name: &str, env: &dyn MemTableExplainEnv) -> String {
        if self.skip_request {
            return "skip_request: true".to_owned();
        }
        let prom_ql = self
            .get_metric_table_prom_ql(&go_to_lower(table_name), env.metric_schema_range_duration());
        let tz = env.time_zone();
        format!(
            "PromQL:{prom_ql}, start_time:{}, end_time:{}, step:{}",
            format_metric_time(self.start_time, &tz),
            format_metric_time(self.end_time, &tz),
            format_go_duration_seconds(env.metric_schema_step()),
        )
    }

    /// Go `MetricTableExtractor.GetMetricTablePromQL`.
    #[must_use]
    pub fn get_metric_table_prom_ql(
        &self,
        lower_table_name: &str,
        metric_schema_range_duration: i64,
    ) -> String {
        let Some(def) = tidb_metadef::metric_table_def::get_metric_table_def(lower_table_name)
        else {
            return String::new();
        };
        let quantiles = if self.quantiles.is_empty() {
            vec![def.quantile]
        } else {
            self.quantiles.clone()
        };
        quantiles
            .iter()
            .map(|quantile| {
                def.gen_prom_ql(
                    metric_schema_range_duration,
                    &self.label_conditions,
                    *quantile,
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    }
}

impl Default for MetricTableExtractor {
    fn default() -> Self {
        Self::new()
    }
}

/// Go `time.Duration.String()` for a whole number of seconds.
fn format_go_duration_seconds(seconds: i64) -> String {
    if seconds == 0 {
        return "0s".to_owned();
    }
    let sign = if seconds < 0 { "-" } else { "" };
    let total = seconds.unsigned_abs();
    let (hours, minutes, secs) = (total / 3600, total % 3600 / 60, total % 60);
    if hours > 0 {
        format!("{sign}{hours}h{minutes}m{secs}s")
    } else if minutes > 0 {
        format!("{sign}{minutes}m{secs}s")
    } else {
        format!("{sign}{secs}s")
    }
}

/// Go `MetricSummaryTableExtractor`, for `METRICS_SUMMARY` and
/// `METRICS_SUMMARY_BY_LABEL`.
#[derive(Clone, Debug, Default)]
pub struct MetricSummaryTableExtractor {
    helper: ExtractHelper,
    /// Go `SkipRequest`.
    pub skip_request: bool,
    /// Go `MetricsNames`, lower-cased.
    pub metrics_names: StringSet,
    /// Go `Quantiles`.
    pub quantiles: Vec<f64>,
}

impl MetricSummaryTableExtractor {
    fn extract(
        &mut self,
        schema: &Schema,
        names: &[FieldName],
        predicates: Vec<Expression>,
    ) -> Vec<Expression> {
        // Go extracts both columns from the ORIGINAL predicates and keeps the
        // second call's remainder, so every quantile predicate stays.
        let (_, quantile_skip, quantiles) =
            self.helper
                .extract_col(schema, names, predicates.clone(), "quantile", false);
        let (remained, metrics_name_skip, metrics_names) =
            self.helper
                .extract_col(schema, names, predicates, "metrics_name", true);
        self.skip_request = quantile_skip || metrics_name_skip;
        self.quantiles = ExtractHelper::parse_quantiles(&quantiles);
        self.metrics_names = metrics_names;
        remained
    }
}

/// Go `InspectionResultTableExtractor`, for `INSPECTION_RESULT`.
#[derive(Clone, Debug, Default)]
pub struct InspectionResultTableExtractor {
    helper: ExtractHelper,
    /// Go `SkipInspection`.
    pub skip_inspection: bool,
    /// Go `Rules`, lower-cased.
    pub rules: StringSet,
    /// Go `Items`, lower-cased.
    pub items: StringSet,
}

impl InspectionResultTableExtractor {
    fn extract(
        &mut self,
        schema: &Schema,
        names: &[FieldName],
        predicates: Vec<Expression>,
    ) -> Vec<Expression> {
        let (remained, rule_skip, rules) = self
            .helper
            .extract_col(schema, names, predicates, "rule", true);
        let (remained, item_skip, items) = self
            .helper
            .extract_col(schema, names, remained, "item", true);
        self.skip_inspection = rule_skip || item_skip;
        self.rules = rules;
        self.items = items;
        remained
    }

    fn explain_info(&self) -> String {
        if self.skip_inspection {
            return "skip_inspection:true".to_owned();
        }
        format!(
            "rules:[{}], items:[{}]",
            extract_string_from_string_set(&self.rules),
            extract_string_from_string_set(&self.items)
        )
    }
}

/// Go `InspectionSummaryTableExtractor`, for `INSPECTION_SUMMARY`.
#[derive(Clone, Debug, Default)]
pub struct InspectionSummaryTableExtractor {
    helper: ExtractHelper,
    /// Go `SkipInspection`.
    pub skip_inspection: bool,
    /// Go `Rules`, lower-cased.
    pub rules: StringSet,
    /// Go `MetricNames`, lower-cased.
    pub metric_names: StringSet,
    /// Go `Quantiles`.
    pub quantiles: Vec<f64>,
}

impl InspectionSummaryTableExtractor {
    fn extract(
        &mut self,
        schema: &Schema,
        names: &[FieldName],
        predicates: Vec<Expression>,
    ) -> Vec<Expression> {
        // Go extracts each column from the ORIGINAL predicates and keeps only
        // the quantile call's remainder, so rule and metric-name predicates
        // stay above the scan.
        let (_, rule_skip, rules) =
            self.helper
                .extract_col(schema, names, predicates.clone(), "rule", true);
        let (_, metric_name_skip, metric_names) =
            self.helper
                .extract_col(schema, names, predicates.clone(), "metrics_name", true);
        let (remained, quantile_skip, quantile_set) = self
            .helper
            .extract_col(schema, names, predicates, "quantile", false);
        self.skip_inspection = rule_skip || quantile_skip || metric_name_skip;
        self.rules = rules;
        self.quantiles = ExtractHelper::parse_quantiles(&quantile_set);
        self.metric_names = metric_names;
        remained
    }

    fn explain_info(&self) -> String {
        if self.skip_inspection {
            return "skip_inspection: true".to_owned();
        }
        let mut parts = Vec::new();
        if !self.rules.is_empty() {
            parts.push(format!(
                "rules:[{}]",
                extract_string_from_string_set(&self.rules)
            ));
        }
        if !self.metric_names.is_empty() {
            parts.push(format!(
                "metric_names:[{}]",
                extract_string_from_string_set(&self.metric_names)
            ));
        }
        if !self.quantiles.is_empty() {
            let quantiles: Vec<String> = self.quantiles.iter().map(|q| format!("{q:.6}")).collect();
            parts.push(format!("quantiles:[{}]", quantiles.join(",")));
        }
        trim_explain(&parts)
    }
}

/// Go `InspectionRuleTableExtractor`, for `INSPECTION_RULES`.
#[derive(Clone, Debug, Default)]
pub struct InspectionRuleTableExtractor {
    helper: ExtractHelper,
    /// Go `SkipRequest`.
    pub skip_request: bool,
    /// Go `Types`, lower-cased.
    pub types: StringSet,
}

impl InspectionRuleTableExtractor {
    fn extract(
        &mut self,
        schema: &Schema,
        names: &[FieldName],
        predicates: Vec<Expression>,
    ) -> Vec<Expression> {
        let (remained, type_skip, types) = self
            .helper
            .extract_col(schema, names, predicates, "type", true);
        self.skip_request = type_skip;
        self.types = types;
        remained
    }

    fn explain_info(&self) -> String {
        if self.skip_request {
            return "skip_request: true".to_owned();
        }
        if self.types.is_empty() {
            return String::new();
        }
        format!(
            "node_types:[{}]",
            extract_string_from_string_set(&self.types)
        )
    }
}

/// Go `TimeRange`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimeRange {
    /// Go `StartTime`.
    pub start_time: DateTime<Utc>,
    /// Go `EndTime`.
    pub end_time: DateTime<Utc>,
}

/// Go `SlowQueryExtractor`, for `SLOW_QUERY`.
#[derive(Clone, Debug, Default)]
pub struct SlowQueryExtractor {
    /// Go `SkipRequest`.
    pub skip_request: bool,
    /// Go `TimeRanges`.
    pub time_ranges: Vec<TimeRange>,
    /// Go `Enable`: the reader locates slow-log files by the time range
    /// instead of reading only the current file.
    pub enable: bool,
    /// Go `Desc`.
    pub desc: bool,
    /// Go `Limit`: a pushed-down `LIMIT`/`TopN` row hint; zero is none.
    pub limit: u64,
}

impl SlowQueryExtractor {
    /// Go `SlowQueryExtractor.SetRowLimitHint`.
    pub fn set_row_limit_hint(&mut self, limit: u64) {
        if limit == 0 {
            return;
        }
        if self.limit == 0 || limit < self.limit {
            self.limit = limit;
        }
    }

    /// Go `SlowQueryExtractor.SetDesc`.
    pub fn set_desc(&mut self, desc: bool) {
        self.desc = desc;
    }

    fn extract(
        &mut self,
        ctx: &dyn Columns,
        schema: &Schema,
        names: &[FieldName],
        predicates: Vec<Expression>,
    ) -> Vec<Expression> {
        let (remained, start_time, end_time) = ExtractHelper::extract_time_range(
            ctx,
            schema,
            names,
            predicates,
            "time",
            &ctx.time_zone(),
        );
        self.set_time_range(start_time, end_time);
        self.skip_request = self.enable
            && self
                .time_ranges
                .first()
                .is_some_and(|range| range.start_time > range.end_time);
        if self.skip_request {
            return Vec::new();
        }
        remained
    }

    /// Go `SlowQueryExtractor.setTimeRange`: an open side reaches
    /// `MinDatetime` / `MaxDatetime` in UTC.
    fn set_time_range(&mut self, start: i64, end: i64) {
        if start == 0 && end == 0 {
            return;
        }
        let start_time = if start == 0 {
            Utc.with_ymd_and_hms(1, 1, 1, 0, 0, 0)
                .single()
                .unwrap_or_default()
        } else {
            ExtractHelper::convert_to_time(start)
        };
        let end_time = if end == 0 {
            Utc.with_ymd_and_hms(9999, 12, 31, 23, 59, 59)
                .single()
                .unwrap_or_default()
        } else {
            ExtractHelper::convert_to_time(end)
        };
        self.time_ranges.push(TimeRange {
            start_time,
            end_time,
        });
        self.enable = true;
    }

    fn explain_info(&self, env: &dyn MemTableExplainEnv) -> String {
        if self.skip_request {
            return "skip_request: true".to_owned();
        }
        let Some(range) = self.time_ranges.first().filter(|_| self.enable) else {
            return format!(
                "only search in the current '{}' file",
                env.slow_query_file()
            );
        };
        let tz = env.time_zone();
        format!(
            "start_time:{}, end_time:{}",
            format_max_fsp_datetime(range.start_time, &tz),
            format_max_fsp_datetime(range.end_time, &tz)
        )
    }
}

/// Go `TableStorageStatsExtractor`, for `TABLE_STORAGE_STATS`.
#[derive(Clone, Debug, Default)]
pub struct TableStorageStatsExtractor {
    helper: ExtractHelper,
    /// Go `SkipRequest`.
    pub skip_request: bool,
    /// Go `TableSchema`, lower-cased.
    pub table_schema: StringSet,
    /// Go `TableName`, lower-cased.
    pub table_name: StringSet,
}

impl TableStorageStatsExtractor {
    fn extract(
        &mut self,
        schema: &Schema,
        names: &[FieldName],
        predicates: Vec<Expression>,
    ) -> Vec<Expression> {
        let (remained, schema_skip, table_schema) =
            self.helper
                .extract_col(schema, names, predicates, "table_schema", true);
        let (remained, table_skip, table_name) =
            self.helper
                .extract_col(schema, names, remained, "table_name", true);
        self.skip_request = schema_skip || table_skip;
        if self.skip_request {
            return Vec::new();
        }
        self.table_schema = table_schema;
        self.table_name = table_name;
        remained
    }

    fn explain_info(&self) -> String {
        if self.skip_request {
            return "skip_request: true".to_owned();
        }
        let mut parts = Vec::new();
        if !self.table_schema.is_empty() {
            parts.push(format!(
                "schema:[{}]",
                extract_string_from_string_set(&self.table_schema)
            ));
        }
        if !self.table_name.is_empty() {
            parts.push(format!(
                "table:[{}]",
                extract_string_from_string_set(&self.table_name)
            ));
        }
        parts.join(", ")
    }
}

/// Go `TiFlashSystemTableExtractor`, for `TIFLASH_TABLES`, `TIFLASH_SEGMENTS`
/// and `TIFLASH_INDEXES`.
#[derive(Clone, Debug, Default)]
pub struct TiFlashSystemTableExtractor {
    helper: ExtractHelper,
    /// Go `SkipRequest`.
    pub skip_request: bool,
    /// Go `TiFlashInstances`.
    pub tiflash_instances: StringSet,
    /// Go `TiDBDatabases`: the quoted, comma-joined lower-cased names.
    pub tidb_databases: String,
    /// Go `TiDBTables`: the quoted, comma-joined lower-cased names.
    pub tidb_tables: String,
    database_set: StringSet,
    table_set: StringSet,
}

impl TiFlashSystemTableExtractor {
    fn extract(
        &mut self,
        schema: &Schema,
        names: &[FieldName],
        predicates: Vec<Expression>,
    ) -> Vec<Expression> {
        let (remained, instance_skip, tiflash_instances) =
            self.helper
                .extract_col(schema, names, predicates, "tiflash_instance", false);
        let (remained, database_skip, tidb_databases) =
            self.helper
                .extract_col(schema, names, remained, "tidb_database", true);
        let (remained, table_skip, tidb_tables) =
            self.helper
                .extract_col(schema, names, remained, "tidb_table", true);
        self.skip_request = instance_skip || database_skip || table_skip;
        if self.skip_request {
            return Vec::new();
        }
        self.tiflash_instances = tiflash_instances;
        self.tidb_databases = extract_string_from_string_set(&tidb_databases);
        self.tidb_tables = extract_string_from_string_set(&tidb_tables);
        self.database_set = tidb_databases;
        self.table_set = tidb_tables;
        remained
    }

    fn explain_info(&self) -> String {
        if self.skip_request {
            return "skip_request:true".to_owned();
        }
        let mut parts = Vec::new();
        if !self.tiflash_instances.is_empty() {
            parts.push(format!(
                "tiflash_instances:[{}]",
                extract_string_from_string_set(&self.tiflash_instances)
            ));
        }
        if !self.tidb_databases.is_empty() {
            parts.push(format!("tidb_databases:[{}]", self.tidb_databases));
        }
        if !self.tidb_tables.is_empty() {
            parts.push(format!("tidb_tables:[{}]", self.tidb_tables));
        }
        trim_explain(&parts)
    }
}

/// Go `StatementsSummaryExtractor`, for `STATEMENTS_SUMMARY`,
/// `STATEMENTS_SUMMARY_HISTORY` and `TIDB_STATEMENTS_STATS`.
#[derive(Clone, Debug, Default)]
pub struct StatementsSummaryExtractor {
    helper: ExtractHelper,
    /// Go `SkipRequest`.
    pub skip_request: bool,
    /// Go `Digests`; `None` is Go's nil set.
    pub digests: Option<StringSet>,
    /// Go `CoarseTimeRange`: `summary_begin_time <= end AND summary_end_time
    /// >= start`, read by the v2 store; the predicates themselves stay.
    pub coarse_time_range: Option<TimeRange>,
}

/// Go `defaultStatementsDuration`.
const DEFAULT_STATEMENTS_DURATION: chrono::Duration = chrono::Duration::hours(1);

impl StatementsSummaryExtractor {
    fn extract(
        &mut self,
        ctx: &dyn Columns,
        schema: &Schema,
        names: &[FieldName],
        predicates: Vec<Expression>,
    ) -> Vec<Expression> {
        let (remained, skip, digests) = self
            .helper
            .extract_col(schema, names, predicates, "digest", false);
        if skip {
            self.skip_request = true;
            return Vec::new();
        }
        if !digests.is_empty() {
            self.digests = Some(digests);
        }
        let Some(range) = Self::find_coarse_time_range(ctx, schema, names, &remained) else {
            return remained;
        };
        if range.start_time > range.end_time {
            self.skip_request = true;
            return Vec::new();
        }
        self.coarse_time_range = Some(range);
        remained
    }

    /// Go `StatementsSummaryExtractor.findCoarseTimeRange`.
    fn find_coarse_time_range(
        ctx: &dyn Columns,
        schema: &Schema,
        names: &[FieldName],
        predicates: &[Expression],
    ) -> Option<TimeRange> {
        let tz = ctx.time_zone();
        let (_, _, end_time) = ExtractHelper::extract_time_range(
            ctx,
            schema,
            names,
            predicates.to_vec(),
            "summary_begin_time",
            &tz,
        );
        let (_, start_time, _) = ExtractHelper::extract_time_range(
            ctx,
            schema,
            names,
            predicates.to_vec(),
            "summary_end_time",
            &tz,
        );
        Self::build_time_range(start_time, end_time)
    }

    /// Go `StatementsSummaryExtractor.buildTimeRange`.
    fn build_time_range(start: i64, end: i64) -> Option<TimeRange> {
        if start == 0 && end == 0 {
            return None;
        }
        let mut start_time = DateTime::<Utc>::default();
        let mut end_time = DateTime::<Utc>::default();
        if start != 0 {
            start_time = ExtractHelper::convert_to_time(start);
        }
        if end != 0 {
            end_time = ExtractHelper::convert_to_time(end);
        }
        if start == 0 {
            start_time = end_time - DEFAULT_STATEMENTS_DURATION;
        }
        if end == 0 {
            end_time = start_time + DEFAULT_STATEMENTS_DURATION;
        }
        Some(TimeRange {
            start_time,
            end_time,
        })
    }

    fn explain_info(&self, env: &dyn MemTableExplainEnv) -> String {
        if self.skip_request {
            return "skip_request: true".to_owned();
        }
        let mut parts = Vec::new();
        if let Some(digests) = self.digests.as_ref().filter(|digests| !digests.is_empty()) {
            parts.push(format!(
                "digests: [{}]",
                extract_string_from_string_set(digests)
            ));
        }
        if let Some(range) = &self.coarse_time_range {
            let tz = env.time_zone();
            parts.push(format!(
                "start_time: {}, end_time: {}",
                format_max_fsp_datetime(range.start_time, &tz),
                format_max_fsp_datetime(range.end_time, &tz)
            ));
        }
        trim_explain(&parts)
    }
}

/// Go `TikvRegionPeersExtractor`, for `TIKV_REGION_PEERS`.
#[derive(Clone, Debug, Default)]
pub struct TikvRegionPeersExtractor {
    helper: ExtractHelper,
    /// Go `SkipRequest`.
    pub skip_request: bool,
    /// Go `RegionIDs`.
    pub region_ids: Vec<u64>,
    /// Go `StoreIDs`.
    pub store_ids: Vec<u64>,
}

impl TikvRegionPeersExtractor {
    fn extract(
        &mut self,
        schema: &Schema,
        names: &[FieldName],
        predicates: Vec<Expression>,
    ) -> Vec<Expression> {
        let (remained, region_skip, region_ids) =
            self.helper
                .extract_col(schema, names, predicates, "region_id", false);
        let (remained, store_skip, store_ids) = self
            .helper
            .extract_col(schema, names, remained, "store_id", false);
        self.region_ids = ExtractHelper::parse_uint64(&region_ids);
        self.store_ids = ExtractHelper::parse_uint64(&store_ids);
        self.skip_request = region_skip || store_skip;
        if self.skip_request {
            return Vec::new();
        }
        remained
    }

    fn explain_info(&self) -> String {
        if self.skip_request {
            return "skip_request:true".to_owned();
        }
        let mut parts = Vec::new();
        if !self.region_ids.is_empty() {
            parts.push(format!(
                "region_ids:[{}]",
                extract_string_from_uint64_slice(&self.region_ids)
            ));
        }
        if !self.store_ids.is_empty() {
            parts.push(format!(
                "store_ids:[{}]",
                extract_string_from_uint64_slice(&self.store_ids)
            ));
        }
        trim_explain(&parts)
    }
}

/// Go `TiKVRegionStatusExtractor`, for `TIKV_REGION_STATUS`.
#[derive(Clone, Debug, Default)]
pub struct TiKVRegionStatusExtractor {
    helper: ExtractHelper,
    /// Go `tablesID`.
    pub tables_id: Vec<i64>,
}

impl TiKVRegionStatusExtractor {
    fn extract(
        &mut self,
        schema: &Schema,
        names: &[FieldName],
        predicates: Vec<Expression>,
    ) -> Vec<Expression> {
        let (remained, _, table_id_set) =
            self.helper
                .extract_col(schema, names, predicates.clone(), "table_id", true);
        if table_id_set.is_empty() {
            return predicates;
        }
        let mut tables_id = Vec::with_capacity(table_id_set.len());
        for key in &table_id_set {
            let Ok(table_id) = key.parse::<i64>() else {
                self.tables_id.clear();
                return predicates;
            };
            tables_id.push(table_id);
        }
        self.tables_id.extend(tables_id);
        remained
    }

    fn explain_info(&self) -> String {
        if self.tables_id.is_empty() {
            return String::new();
        }
        let ids: Vec<String> = self.tables_id.iter().map(i64::to_string).collect();
        format!("table_id in {{{}}}", ids.join(","))
    }
}

/// Go `base.MemTablePredicateExtractor`: the extractor one memory table owns.
#[derive(Clone, Debug)]
pub enum MemTablePredicateExtractor {
    /// Go `*ClusterTableExtractor`.
    ClusterTable(ClusterTableExtractor),
    /// Go `*ClusterLogTableExtractor`.
    ClusterLogTable(ClusterLogTableExtractor),
    /// Go `*HotRegionsHistoryTableExtractor`.
    HotRegionsHistoryTable(HotRegionsHistoryTableExtractor),
    /// Go `*MetricTableExtractor`.
    MetricTable(Box<MetricTableExtractor>),
    /// Go `*MetricSummaryTableExtractor`.
    MetricSummaryTable(MetricSummaryTableExtractor),
    /// Go `*InspectionResultTableExtractor`.
    InspectionResultTable(InspectionResultTableExtractor),
    /// Go `*InspectionSummaryTableExtractor`.
    InspectionSummaryTable(InspectionSummaryTableExtractor),
    /// Go `*InspectionRuleTableExtractor`.
    InspectionRuleTable(InspectionRuleTableExtractor),
    /// Go `*SlowQueryExtractor`.
    SlowQuery(SlowQueryExtractor),
    /// Go `*TableStorageStatsExtractor`.
    TableStorageStats(TableStorageStatsExtractor),
    /// Go `*TiFlashSystemTableExtractor`.
    TiFlashSystemTable(TiFlashSystemTableExtractor),
    /// Go `*StatementsSummaryExtractor`.
    StatementsSummary(StatementsSummaryExtractor),
    /// Go `*TikvRegionPeersExtractor`.
    TikvRegionPeers(TikvRegionPeersExtractor),
    /// Go's `InfoSchema*Extractor` family over `InfoSchemaBaseExtractor`.
    InfoSchema(Box<InfoSchemaBaseExtractor>),
    /// Go `*TiKVRegionStatusExtractor`.
    TiKVRegionStatus(TiKVRegionStatusExtractor),
}

impl MemTablePredicateExtractor {
    /// Go `buildMemTable`'s extractor switch (`logical_plan_builder.go`):
    /// every `metrics_schema` table, and the named `information_schema`
    /// tables by their upper-cased name.
    #[must_use]
    pub fn for_table(db_name: &str, table_name: &str) -> Option<Self> {
        let db = go_to_lower(db_name);
        if db == tidb_metadef::METRIC_SCHEMA_NAME_L {
            return Some(Self::MetricTable(Box::new(MetricTableExtractor::new())));
        }
        if db != tidb_metadef::INFORMATION_SCHEMA_NAME_L {
            return None;
        }
        use InfoSchemaExtractorKind as Kind;
        let info_schema =
            |kind: Kind| Self::InfoSchema(Box::new(InfoSchemaBaseExtractor::new(kind)));
        Some(match go_to_upper(table_name).as_str() {
            "CLUSTER_CONFIG" | "CLUSTER_LOAD" | "CLUSTER_HARDWARE" | "CLUSTER_SYSTEMINFO" => {
                Self::ClusterTable(ClusterTableExtractor::default())
            }
            "CLUSTER_LOG" => Self::ClusterLogTable(ClusterLogTableExtractor::default()),
            "TIDB_HOT_REGIONS_HISTORY" => {
                Self::HotRegionsHistoryTable(HotRegionsHistoryTableExtractor::default())
            }
            "INSPECTION_RESULT" => {
                Self::InspectionResultTable(InspectionResultTableExtractor::default())
            }
            "INSPECTION_SUMMARY" => {
                Self::InspectionSummaryTable(InspectionSummaryTableExtractor::default())
            }
            "INSPECTION_RULES" => {
                Self::InspectionRuleTable(InspectionRuleTableExtractor::default())
            }
            "METRICS_SUMMARY" | "METRICS_SUMMARY_BY_LABEL" => {
                Self::MetricSummaryTable(MetricSummaryTableExtractor::default())
            }
            "SLOW_QUERY" => Self::SlowQuery(SlowQueryExtractor::default()),
            "TABLE_STORAGE_STATS" => Self::TableStorageStats(TableStorageStatsExtractor::default()),
            "TIFLASH_TABLES" | "TIFLASH_SEGMENTS" | "TIFLASH_INDEXES" => {
                Self::TiFlashSystemTable(TiFlashSystemTableExtractor::default())
            }
            "STATEMENTS_SUMMARY" | "STATEMENTS_SUMMARY_HISTORY" | "TIDB_STATEMENTS_STATS" => {
                Self::StatementsSummary(StatementsSummaryExtractor::default())
            }
            "TIKV_REGION_PEERS" => Self::TikvRegionPeers(TikvRegionPeersExtractor::default()),
            "COLUMNS" => info_schema(Kind::Columns),
            "TABLES" => info_schema(Kind::Tables),
            "PARTITIONS" => info_schema(Kind::Partitions),
            "STATISTICS" => info_schema(Kind::Statistics),
            "SCHEMATA" => info_schema(Kind::Schemata),
            "SEQUENCES" => info_schema(Kind::Sequence),
            "TIDB_INDEX_USAGE" => info_schema(Kind::TiDBIndexUsage),
            "DDL_JOBS" => info_schema(Kind::Ddl),
            "CHECK_CONSTRAINTS" => info_schema(Kind::CheckConstraints),
            "TIDB_CHECK_CONSTRAINTS" => info_schema(Kind::TiDBCheckConstraints),
            "REFERENTIAL_CONSTRAINTS" => info_schema(Kind::ReferConst),
            "TIDB_INDEXES" => info_schema(Kind::Indexes),
            "VIEWS" => info_schema(Kind::Views),
            "KEY_COLUMN_USAGE" => info_schema(Kind::KeyColumnUsage),
            "TABLE_CONSTRAINTS" => info_schema(Kind::TableConstraints),
            "TIKV_REGION_STATUS" => Self::TiKVRegionStatus(TiKVRegionStatusExtractor::default()),
            _ => return None,
        })
    }

    /// Go `MemTablePredicateExtractor.Extract(ctx, schema, names, predicates)`:
    /// returns the predicates this extractor does not claim.
    pub fn extract(
        &mut self,
        ctx: &dyn Columns,
        schema: &Schema,
        names: &[FieldName],
        predicates: Vec<Expression>,
    ) -> Vec<Expression> {
        match self {
            Self::ClusterTable(e) => e.extract(schema, names, predicates),
            Self::ClusterLogTable(e) => e.extract(ctx, schema, names, predicates),
            Self::HotRegionsHistoryTable(e) => e.extract(ctx, schema, names, predicates),
            Self::MetricTable(e) => e.extract(ctx, schema, names, predicates),
            Self::MetricSummaryTable(e) => e.extract(schema, names, predicates),
            Self::InspectionResultTable(e) => e.extract(schema, names, predicates),
            Self::InspectionSummaryTable(e) => e.extract(schema, names, predicates),
            Self::InspectionRuleTable(e) => e.extract(schema, names, predicates),
            Self::SlowQuery(e) => e.extract(ctx, schema, names, predicates),
            Self::TableStorageStats(e) => e.extract(schema, names, predicates),
            Self::TiFlashSystemTable(e) => e.extract(schema, names, predicates),
            Self::StatementsSummary(e) => e.extract(ctx, schema, names, predicates),
            Self::TikvRegionPeers(e) => e.extract(schema, names, predicates),
            Self::InfoSchema(e) => e.extract(schema, names, predicates),
            Self::TiKVRegionStatus(e) => e.extract(schema, names, predicates),
        }
    }

    /// Go `MemTablePredicateExtractor.ExplainInfo(p)`, the memory-table
    /// scan's operator info.
    #[must_use]
    pub fn explain_info(&self, table_name: &str, env: &dyn MemTableExplainEnv) -> String {
        match self {
            Self::ClusterTable(e) => e.explain_info(),
            Self::ClusterLogTable(e) => e.explain_info(env),
            Self::HotRegionsHistoryTable(e) => e.explain_info(env),
            Self::MetricTable(e) => e.explain_info(table_name, env),
            Self::MetricSummaryTable(_) => String::new(),
            Self::InspectionResultTable(e) => e.explain_info(),
            Self::InspectionSummaryTable(e) => e.explain_info(),
            Self::InspectionRuleTable(e) => e.explain_info(),
            Self::SlowQuery(e) => e.explain_info(env),
            Self::TableStorageStats(e) => e.explain_info(),
            Self::TiFlashSystemTable(e) => e.explain_info(),
            Self::StatementsSummary(e) => e.explain_info(env),
            Self::TikvRegionPeers(e) => e.explain_info(),
            Self::InfoSchema(e) => e.explain_info(),
            Self::TiKVRegionStatus(e) => e.explain_info(),
        }
    }

    /// Applies the claimed filters to one row of a whole materialized table,
    /// in the table's own column order.
    ///
    /// Go's readers request only the rows the extractor kept; this port's
    /// readers materialize the whole virtual table, so the predicates the
    /// extractor claimed (and removed from the Selection above) are applied
    /// here. A contradiction keeps nothing. The claimed time ranges and
    /// message patterns of `CLUSTER_LOG`, `SLOW_QUERY`,
    /// `TIDB_HOT_REGIONS_HISTORY` and the metric tables are not applied:
    /// those tables have no materialized rows (their readers refuse, or are
    /// not served).
    #[must_use]
    pub fn keeps_row(&self, columns: &[&str], row: &[Datum]) -> bool {
        let value = |name: &str| row_value(columns, row, name);
        match self {
            Self::ClusterTable(e) => {
                !e.skip_request
                    && set_keeps(&e.node_types, value("type"), true)
                    && set_keeps(&e.instances, value("instance"), false)
            }
            Self::ClusterLogTable(e) => {
                !e.skip_request
                    && set_keeps(&e.node_types, value("type"), true)
                    && set_keeps(&e.instances, value("instance"), false)
                    && set_keeps(&e.log_levels, value("level"), true)
            }
            Self::HotRegionsHistoryTable(e) => {
                !e.skip_request
                    && uint_keeps(&e.region_ids, value("region_id"))
                    && uint_keeps(&e.store_ids, value("store_id"))
                    && uint_keeps(&e.peer_ids, value("peer_id"))
                    && bool_keeps(&e.is_learners, value("is_learner"))
                    && bool_keeps(&e.is_leaders, value("is_leader"))
                    && set_keeps(&e.hot_region_types, value("type"), false)
            }
            Self::MetricTable(e) => !e.skip_request,
            Self::MetricSummaryTable(e) => {
                !e.skip_request
                    && set_keeps(&e.metrics_names, value("metrics_name"), true)
                    && float_keeps(&e.quantiles, value("quantile"))
            }
            Self::InspectionResultTable(e) => {
                !e.skip_inspection
                    && set_keeps(&e.rules, value("rule"), true)
                    && set_keeps(&e.items, value("item"), true)
            }
            Self::InspectionSummaryTable(e) => {
                !e.skip_inspection
                    && set_keeps(&e.rules, value("rule"), true)
                    && set_keeps(&e.metric_names, value("metrics_name"), true)
                    && float_keeps(&e.quantiles, value("quantile"))
            }
            Self::InspectionRuleTable(e) => {
                !e.skip_request && set_keeps(&e.types, value("type"), true)
            }
            Self::SlowQuery(e) => !e.skip_request,
            Self::TableStorageStats(e) => {
                !e.skip_request
                    && set_keeps(&e.table_schema, value("table_schema"), true)
                    && set_keeps(&e.table_name, value("table_name"), true)
            }
            Self::TiFlashSystemTable(e) => {
                !e.skip_request
                    && set_keeps(&e.tiflash_instances, value("tiflash_instance"), false)
                    && set_keeps(&e.database_set, value("tidb_database"), true)
                    && set_keeps(&e.table_set, value("tidb_table"), true)
            }
            Self::StatementsSummary(e) => {
                !e.skip_request
                    && e.digests
                        .as_ref()
                        .is_none_or(|digests| set_keeps(digests, value("digest"), false))
            }
            Self::TikvRegionPeers(e) => {
                !e.skip_request
                    && uint_keeps(&e.region_ids, value("region_id"))
                    && uint_keeps(&e.store_ids, value("store_id"))
            }
            Self::InfoSchema(e) => e.keeps_row(columns, row),
            Self::TiKVRegionStatus(e) => {
                e.tables_id.is_empty()
                    || value("table_id")
                        .flatten()
                        .and_then(|value| value.parse::<i64>().ok())
                        .is_some_and(|id| e.tables_id.contains(&id))
            }
        }
    }

    /// The cluster-table extractor, when this is one.
    #[must_use]
    pub fn as_cluster_table(&self) -> Option<&ClusterTableExtractor> {
        match self {
            Self::ClusterTable(e) => Some(e),
            _ => None,
        }
    }

    /// The cluster-log extractor, when this is one.
    #[must_use]
    pub fn as_cluster_log_table(&self) -> Option<&ClusterLogTableExtractor> {
        match self {
            Self::ClusterLogTable(e) => Some(e),
            _ => None,
        }
    }

    /// Go `MemTableRowLimitHintSetter` / `MemTableDescHintSetter`: only the
    /// slow-query extractor takes `LogicalMemTable.PushDownTopN`'s hints.
    pub fn apply_topn_hints(&mut self, hints: crate::logical::mem_table::MemTableTopNHints) {
        let Self::SlowQuery(e) = self else {
            return;
        };
        if let Some(desc) = hints.desc {
            e.set_desc(desc);
        }
        if let Some(limit) = hints.row_limit_hint {
            e.set_row_limit_hint(limit);
        }
    }
}

/// One column of a materialized row by name: `None` when the table has no
/// such column, `Some(None)` for NULL.
pub(crate) fn row_value(columns: &[&str], row: &[Datum], name: &str) -> Option<Option<String>> {
    let index = columns
        .iter()
        .position(|column| column.eq_ignore_ascii_case(name))?;
    Some(row.get(index).and_then(datum_to_string))
}

/// Whether a value passes a claimed value set: an empty set claims nothing,
/// and NULL matches no named value.
fn set_keeps(set: &StringSet, value: Option<Option<String>>, to_lower: bool) -> bool {
    if set.is_empty() {
        return true;
    }
    match value {
        None => true,
        Some(None) => false,
        Some(Some(value)) => set.contains(&if to_lower { go_to_lower(&value) } else { value }),
    }
}

fn uint_keeps(ids: &[u64], value: Option<Option<String>>) -> bool {
    if ids.is_empty() {
        return true;
    }
    match value {
        None => true,
        Some(value) => value
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|id| ids.contains(&id)),
    }
}

fn bool_keeps(roles: &[bool], value: Option<Option<String>>) -> bool {
    match value {
        None => true,
        Some(value) => value
            .and_then(|value| value.parse::<i64>().ok())
            .is_some_and(|role| roles.contains(&(role == 1))),
    }
}

fn float_keeps(values: &[f64], value: Option<Option<String>>) -> bool {
    if values.is_empty() {
        return true;
    }
    match value {
        None => true,
        Some(value) => value
            .and_then(|value| value.parse::<f64>().ok())
            .is_some_and(|value| values.contains(&value)),
    }
}

/// The routing one `CLUSTER_*` scan needs from cluster discovery.
#[derive(Clone, Debug, Default)]
pub struct ClusterTableFilter {
    skip_request: bool,
    node_types: StringSet,
    instances: StringSet,
}

impl ClusterTableFilter {
    /// Whether this scan provably needs no nodes.
    #[must_use]
    pub fn skip_request(&self) -> bool {
        self.skip_request
    }

    /// Go `FilterClusterServerInfo`, before issuing any request.
    #[must_use]
    pub fn matches(&self, node_type: &str, instance: &str) -> bool {
        (self.node_types.is_empty() || self.node_types.contains(node_type))
            && (self.instances.is_empty() || self.instances.contains(instance))
    }
}

/// Every memory-table scan of `table` in `plan`, with its extractor.
fn mem_table_extractors<'a>(
    plan: &'a PhysicalPlan,
    table: &str,
    out: &mut Vec<Option<&'a MemTablePredicateExtractor>>,
) {
    if let PhysicalPlan::MemTable(scan) = plan {
        if scan.table_name.eq_ignore_ascii_case(table) {
            out.push(scan.extractor.as_ref());
        }
    }
    if let PhysicalPlan::CTE(cte) = plan {
        mem_table_extractors(&cte.seed_plan, table, out);
        if let Some(recursive) = &cte.recursive_plan {
            mem_table_extractors(recursive, table, out);
        }
    }
    for child in plan.children() {
        mem_table_extractors(child, table, out);
    }
}

/// Each `table` scan's routing needs, from its `ClusterTableExtractor`. A
/// shared materialization must satisfy their union, self joins included.
#[must_use]
pub fn cluster_table_filters(plan: &PhysicalPlan, table: &str) -> Vec<ClusterTableFilter> {
    let mut extractors = Vec::new();
    mem_table_extractors(plan, table, &mut extractors);
    extractors
        .into_iter()
        .map(|extractor| {
            extractor
                .and_then(MemTablePredicateExtractor::as_cluster_table)
                .map_or_else(ClusterTableFilter::default, |e| ClusterTableFilter {
                    skip_request: e.skip_request,
                    node_types: e.node_types.clone(),
                    instances: e.instances.clone(),
                })
        })
        .collect()
}

/// Each `CLUSTER_LOG` scan's extractor in `plan`.
#[must_use]
pub fn cluster_log_extractors(plan: &PhysicalPlan) -> Vec<ClusterLogTableExtractor> {
    let mut extractors = Vec::new();
    mem_table_extractors(plan, "CLUSTER_LOG", &mut extractors);
    extractors
        .into_iter()
        .map(|extractor| {
            extractor
                .and_then(MemTablePredicateExtractor::as_cluster_log_table)
                .cloned()
                .unwrap_or_default()
        })
        .collect()
}
