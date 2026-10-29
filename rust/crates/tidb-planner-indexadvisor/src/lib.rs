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


//! Go `pkg/planner/indexadvisor`: the index advisor behind
//! `RECOMMEND INDEX`.
//!
//! The advisor runs the AutoAdmin algorithm over a workload -- the statements
//! named by `FOR '...'`, or the last day's `SELECT`s of
//! `statements_summary_history` -- asking the optimizer what each statement
//! would cost under hypothetical indexes. The session it runs on is an
//! [`AdvisorSession`] (Go's `sessionctx.Context`), and the what-if planner an
//! [`Optimizer`] (Go's `Optimizer` interface); the executor implements both.

mod algorithm;
mod model;
mod options;
mod query;

use std::collections::BTreeMap;

use tidb_set::keyset::{KeySet, Keyed};

use tidb_datatype::{Datum, FieldType};
use tidb_driver_error::DriverError;
use tidb_hack::GoToLower;

pub use model::{Column, ImpactedQuery, Index, Query, Recommendation};
pub use options::{description, ALL_OPTIONS};
pub use query::parse_one_sql;

use options::{AdvisorOption, OPT_MODULE};

/// The advisor's result: its errors are the session's driver errors.
pub type Result<T> = std::result::Result<T, DriverError>;

/// A plain Go `error`: 1105 on the wire.
pub fn advisor_error(message: impl Into<String>) -> DriverError {
    DriverError::Mysql(tidb_terror::MysqlError::unknown(message))
}

/// Go `Optimizer`: everything the advisor asks of the optimizer.
pub trait Optimizer {
    /// Go `ColumnType`: the type of a column.
    fn column_type(&mut self, column: &Column) -> Result<FieldType>;
    /// Go `PrefixContainIndex`: whether an existing index has `index` as a prefix.
    fn prefix_contain_index(&mut self, index: &Index) -> Result<bool>;
    /// Go `PossibleColumns`: the tables of `schema` with a column named `column_name`.
    fn possible_columns(&mut self, schema: &str, column_name: &str) -> Result<Vec<Column>>;
    /// Go `TableColumns`: the columns of a table.
    fn table_columns(&mut self, schema: &str, table: &str) -> Result<Vec<Column>>;
    /// Go `IndexNameExist`: whether a table has an index of that name.
    fn index_name_exist(&mut self, schema: &str, table: &str, index_name: &str) -> Result<bool>;
    /// Go `EstIndexSize`: the estimated size of an index on those columns.
    fn est_index_size(&mut self, database: &str, table: &str, columns: &[String]) -> Result<f64>;
    /// Go `QueryPlanCost`: the plan cost of `sql` under exactly the hypothetical indexes given.
    fn query_plan_cost(&mut self, sql: &str, hypo_indexes: &[Index]) -> Result<f64>;
}

/// Go `sessionctx.Context` as the advisor uses it: its internal SQL (`exec`)
/// and the statement's warnings.
pub trait AdvisorSession {
    /// Go `exec` of the `mysql.tidb_kernel_options` read.
    fn option_rows(&mut self, sql: &str) -> Result<Vec<Vec<Datum>>>;
    /// Go `exec` of the `statements_summary_history` read.
    fn stmt_summary_rows(&mut self, sql: &str) -> Result<Vec<Vec<Datum>>>;
    /// Go `exec` of a write.
    fn exec_write(&mut self, sql: &str) -> Result<()>;
    /// `SessionVars.CurrentDB`.
    fn current_db(&self) -> String;
    /// `StmtCtx.AppendWarning`.
    fn append_warning(&mut self, warning: tidb_context::SqlWarn);
}

type QuerySet = KeySet<Query>;

/// Go `ExecuteInternal`'s argument binding (`sqlescape.EscapeSQL`).
fn escape(sql: &str, args: &[&str]) -> Result<String> {
    let args: Vec<tidb_sqlescape::SqlArg<'_>> = args
        .iter()
        .map(|arg| tidb_sqlescape::SqlArg::String(arg.as_bytes()))
        .collect();
    tidb_sqlescape::escape_sql(sql, &args)
        .map(|sql| String::from_utf8_lossy(&sql).into_owned())
        .map_err(|error| advisor_error(error.to_string()))
}

/// Go `GetOptions`: the stored values, defaults for the unset ones.
pub fn get_options(sctx: &mut impl AdvisorSession) -> Result<BTreeMap<String, String>> {
    let names: Vec<String> = ALL_OPTIONS.iter().map(|name| format!("'{name}'")).collect();
    let sql = format!(
        "SELECT name, value FROM mysql.tidb_kernel_options WHERE module = '{OPT_MODULE}' AND name in ({})",
        names.join(",")
    );
    let rows = sctx.option_rows(&sql)?;
    let mut values = BTreeMap::new();
    for row in rows {
        let name = row.first().and_then(tidb_datatype::datum_text::sysvar_datum_text).unwrap_or_default();
        let value = row.get(1).and_then(tidb_datatype::datum_text::sysvar_datum_text).unwrap_or_default();
        values.insert(name, value);
    }
    for name in ALL_OPTIONS {
        values
            .entry(name.to_owned())
            .or_insert_with(|| options::default_val(name).to_owned());
    }
    Ok(values)
}

/// Go `SetOption`.
pub fn set_option(sctx: &mut impl AdvisorSession, name: &str, value: &tidb_ast::Expr) -> Result<()> {
    let value = options::option_val(name, value)?;
    let description = options::description(name);
    let sql = escape(
        "INSERT INTO mysql.tidb_kernel_options VALUES (%?, %?, %?, now(), 'valid', %?) \
         ON DUPLICATE KEY UPDATE value = %?, updated_at=now(), description = %?",
        &[OPT_MODULE, name, &value, description, &value, description],
    )?;
    sctx.exec_write(&sql)
}

/// Go `AdviseIndexes` and `adviseIndexesWithOption`.
pub fn advise_indexes<S: AdvisorSession + Optimizer>(
    sctx: &mut S,
    sqls: Vec<String>,
    user_options: &[tidb_ast::RecommendIndexOption],
) -> Result<Vec<Recommendation>> {
    let values = get_options(sctx)?;
    let option = options::fill_option(values, user_options, sqls)?;

    let default_db = sctx.current_db();
    let query_set = prepare_query_set(sctx, &default_db, &option)?;

    // Identify the indexable columns.
    let indexable = query::collect_indexable_columns_for_query_set(sctx, &query_set)?;

    // Start the advisor.
    let (indexes, all_candidates) =
        algorithm::advise_indexes(&query_set, &indexable, sctx, &option)?;

    let results = prepare_recommendation(&indexes, &query_set, sctx)?;
    save_recommendations(sctx, &results);

    if results.is_empty() {
        let describe = |keys: Vec<String>| {
            let mut shown: Vec<String> = Vec::with_capacity(5);
            for key in keys {
                shown.push(key);
                if shown.len() >= 5 {
                    shown.push("...".to_owned());
                    break;
                }
            }
            shown.join(", ")
        };
        let columns = describe(indexable.to_list().iter().map(Keyed::key).collect());
        let candidates = describe(all_candidates.to_list().iter().map(Keyed::key).collect());
        let explanation = format!(
            " Considered {} indexable columns({}), {} or more index candidates({}), no sufficiently beneficial indexes were found.",
            indexable.size(),
            columns,
            all_candidates.size(),
            candidates
        );
        sctx.append_warning(tidb_context::SqlWarn::with_code(tidb_context::WarningLevel::Warning, 1105, explanation));
    }
    Ok(results)
}

/// Go `saveRecommendations`: a failure is logged, never raised.
fn save_recommendations(sctx: &mut impl AdvisorSession, results: &[Recommendation]) {
    for result in results {
        let queries = impacted_queries_json(&result.top_impacted_queries);
        let workload = format!(
            "{{\"WorkloadImprovement\":{}}}",
            go_json_float(result.workload_improvement)
        );
        let detail = format!(
            "{{\"Reason\":{},\"IndexSize\":{}}}",
            go_json_string(&result.reason),
            result.index_size
        );
        let Ok(sql) = escape(
            "insert into mysql.index_advisor_results (\
             created_at, updated_at, schema_name, table_name, index_name, \
             index_columns, index_details, top_impacted_queries, workload_impact, extra) values \
             (now(), now(), %?, %?, %?, %?, %?, %?, %?, null) \
             on duplicate key update \
             updated_at=now(), index_details=%?, top_impacted_queries=%?, workload_impact=%?",
            &[
                &result.database,
                &result.table,
                &result.index_name,
                &result.index_columns.join(","),
                &detail,
                &queries,
                &workload,
                &detail,
                &queries,
                &workload,
            ],
        ) else {
            continue;
        };
        let _ = sctx.exec_write(&sql);
    }
}

/// Go `loadQuerySetFromStmtSummary`.
fn load_query_set_from_stmt_summary(
    sctx: &mut impl AdvisorSession,
    option: &AdvisorOption,
) -> Result<QuerySet> {
    let sql = format!(
        "SELECT any_value(ifnull(schema_name, \"\")) as schema_name, \
                any_value(query_sample_text) as query_sample_text, \
                sum(cast(exec_count as double)) as exec_count \
         FROM information_schema.statements_summary_history \
         WHERE stmt_type = \"Select\" AND \
               summary_begin_time >= date_sub(now(), interval 1 day) AND \
               prepared = 0 AND \
               upper(ifnull(schema_name, \"\")) not in (\"MYSQL\", \"INFORMATION_SCHEMA\", \"METRICS_SCHEMA\", \"PERFORMANCE_SCHEMA\") \
         GROUP BY digest \
         ORDER BY sum(exec_count) DESC \
         LIMIT {}",
        option.max_num_query
    );
    let rows = sctx.stmt_summary_rows(&sql)?;
    let mut query_set = QuerySet::new();
    for row in rows {
        let schema_name = row.first().and_then(tidb_datatype::datum_text::sysvar_datum_text).unwrap_or_default();
        let text = row.get(1).and_then(tidb_datatype::datum_text::sysvar_datum_text).unwrap_or_default();
        let frequency = match row.get(2) {
            Some(Datum::Real(count)) => *count as i64,
            Some(other) => tidb_datatype::datum_text::sysvar_datum_text(other)
                .and_then(|count| count.parse::<f64>().ok())
                .map_or(0, |count| count as i64),
            None => 0,
        };
        query_set.add(Query {
            schema_name,
            text,
            frequency,
        });
    }
    Ok(query_set)
}

/// Go `prepareQuerySet`.
fn prepare_query_set<S: AdvisorSession + Optimizer>(
    sctx: &mut S,
    default_db: &str,
    option: &AdvisorOption,
) -> Result<QuerySet> {
    let mut query_set = QuerySet::new();
    if !option.specified_sqls.is_empty() {
        // The target queries are specified.
        for sql in &option.specified_sqls {
            query_set.add(Query {
                schema_name: default_db.to_owned(),
                text: sql.clone(),
                frequency: 1,
            });
        }
    } else {
        query_set = load_query_set_from_stmt_summary(sctx, option)?;
        if query_set.size() == 0 {
            return Err(advisor_error(
                "can't get any queries from statements_summary",
            ));
        }
    }

    // Filter the invalid queries.
    let ignore_error = option.specified_sqls.is_empty();
    let query_set = query::restore_schema_name(default_db, &query_set, ignore_error)?;
    let query_set = query::filter_sql_accessing_system_tables(&query_set, ignore_error)?;
    let query_set = query::filter_invalid_queries(sctx, &query_set, ignore_error)?;
    if query_set.size() == 0 {
        return Err(advisor_error(
            "empty query set after filtering invalid queries",
        ));
    }
    Ok(query_set)
}

/// Go `prepareRecommendation`.
fn prepare_recommendation(
    indexes: &KeySet<Index>,
    queries: &QuerySet,
    optimizer: &mut dyn Optimizer,
) -> Result<Vec<Recommendation>> {
    let mut results = Vec::with_capacity(indexes.size());
    for index in indexes.to_list() {
        let columns: Vec<String> = index
            .columns
            .iter()
            .map(|column| {
                column
                    .column_name
                    .trim_matches(|c| c == '\'' || c == '"' || c == ' ')
                    .to_owned()
            })
            .collect();

        // A graceful index name.
        let index_name =
            graceful_index_name(optimizer, &index.schema_name, &index.table_name, &columns);

        // The index size.
        let index_size =
            optimizer.est_index_size(&index.schema_name, &index.table_name, &columns)?;

        // The improvements.
        let (mut cost_before_total, mut cost_after_total) = (0.0_f64, 0.0_f64);
        let mut impacts: Vec<ImpactedQuery> = Vec::with_capacity(queries.size());
        for query in queries.to_list() {
            let mut cost_before = optimizer.query_plan_cost(&query.text, &[])?;
            let mut cost_after =
                optimizer.query_plan_cost(&query.text, std::slice::from_ref(&index))?;
            if cost_before == 0.0 {
                // Avoid NaN.
                cost_before += 0.1;
                cost_after += 0.1;
            }
            cost_before_total += cost_before * query.frequency as f64;
            cost_after_total += cost_after * query.frequency as f64;

            let improvement = round((cost_before - cost_after) / cost_before, 6);
            if improvement < 0.0001 {
                // This query has no benefit.
                continue;
            }
            impacts.push(ImpactedQuery {
                query: query.text.clone(),
                improvement,
            });
        }
        impacts.sort_by(|a, b| b.improvement.total_cmp(&a.improvement));
        impacts.truncate(3);
        if cost_before_total == 0.0 {
            // Avoid NaN.
            cost_before_total += 0.1;
            cost_after_total += 0.1;
        }
        let workload_improvement = round(
            (cost_before_total - cost_after_total) / cost_before_total,
            6,
        );
        if workload_improvement < 0.000001 || impacts.is_empty() {
            // This index has no benefit.
            continue;
        }

        let normalized = query::normalize_digest(&impacts[0].query);
        let reason = format!(
            "Column [{}] appear in Equal or Range Predicate clause(s) in query: {}",
            columns.join(" "),
            normalized
        );
        results.push(Recommendation {
            database: index.schema_name.clone(),
            table: index.table_name.clone(),
            index_name,
            index_columns: columns,
            reason,
            index_size: index_size as u64,
            workload_improvement,
            top_impacted_queries: impacts,
        });
    }
    Ok(results)
}

fn round(value: f64, places: i32) -> f64 {
    let scale = 10_f64.powi(places);
    (value * scale).round() / scale
}

/// Go `gracefulIndexName`.
fn graceful_index_name(
    optimizer: &mut dyn Optimizer,
    schema: &str,
    table: &str,
    columns: &[String],
) -> String {
    let clip = |name: String| -> String {
        if name.len() > 64 {
            name.chars().take(64).collect()
        } else {
            name
        }
    };
    let mut exists = |name: &str| -> bool {
        optimizer
            .index_name_exist(schema, table, &name.go_to_lower())
            .unwrap_or(false)
    };
    let name = clip(format!("idx_{}", columns.join("_")));
    if !exists(&name) {
        return name;
    }
    let name = clip(format!("idx_{}", columns[0]));
    if !exists(&name) {
        return name;
    }
    let mut name = name;
    for i in 0..30 {
        name = clip(format!("idx_{}_{}", columns[0], i));
        if !exists(&name) {
            return name;
        }
    }
    name
}

/// Go `json.Marshal` of a string.
fn go_json_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Go `encoding/json`'s float64 encoding.
fn go_json_float(value: f64) -> String {
    let magnitude = value.abs();
    let exponent_form = magnitude != 0.0 && (magnitude < 1e-6 || magnitude >= 1e21);
    let mut text = tidb_goruntime::go_strconv::format_float(
        value,
        if exponent_form { b'e' } else { b'f' },
        -1,
        64,
    );
    if exponent_form {
        // Clean up e-09 to e-9.
        if let Some(position) = text.find("e-0") {
            text.remove(position + 2);
        }
    }
    text
}

/// Go `json.Marshal` of `[]*ImpactedQuery`.
pub fn impacted_queries_json(queries: &[ImpactedQuery]) -> String {
    let items: Vec<String> = queries
        .iter()
        .map(|query| {
            format!(
                "{{\"Query\":{},\"Improvement\":{}}}",
                go_json_string(&query.query),
                go_json_float(query.improvement)
            )
        })
        .collect();
    format!("[{}]", items.join(","))
}
