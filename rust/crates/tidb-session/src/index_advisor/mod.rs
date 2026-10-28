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

//! Go `pkg/planner/indexadvisor` and `RecommendIndexExec`
//! (`pkg/executor/recommend_index.go`): `RECOMMEND INDEX RUN | SET | SHOW`.
//!
//! The advisor runs the AutoAdmin algorithm over a workload -- the statements
//! named by `FOR '...'`, or the last day's `SELECT`s of
//! `statements_summary_history` -- asking the optimizer what each statement
//! would cost under hypothetical indexes.

mod algorithm;
mod keyset;
mod model;
mod options;
mod query;

use std::sync::Arc;

use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_executor::ddl::HypoIndexes;
use tidb_executor::DriverError;
use tidb_hack::GoToLower;

use crate::{Session, StmtOutput, WarningLevel};
use keyset::{KeySet, Keyed};
use model::{Column, ImpactedQuery, Index, Query, Recommendation};
use options::{AdvisorOption, ALL_OPTIONS, OPT_MODULE};

pub(crate) type Result<T> = std::result::Result<T, DriverError>;

/// A plain Go `error`: 1105 on the wire.
pub(crate) fn advisor_error(message: impl Into<String>) -> DriverError {
    DriverError::Mysql(tidb_executor::MysqlError::unknown(message))
}

/// Go `Optimizer`: everything the advisor asks of the optimizer.
pub(crate) trait Optimizer {
    fn column_type(&mut self, column: &Column) -> Result<FieldType>;
    fn prefix_contain_index(&mut self, index: &Index) -> Result<bool>;
    fn possible_columns(&mut self, schema: &str, column_name: &str) -> Result<Vec<Column>>;
    fn table_columns(&mut self, schema: &str, table: &str) -> Result<Vec<Column>>;
    fn index_name_exist(&mut self, schema: &str, table: &str, index_name: &str) -> Result<bool>;
    fn est_index_size(&mut self, database: &str, table: &str, columns: &[String]) -> Result<f64>;
    fn query_plan_cost(&mut self, sql: &str, hypo_indexes: &[Index]) -> Result<f64>;
}

type QuerySet = KeySet<Query>;

fn varchar(flen: i64) -> FieldType {
    let mut field_type = FieldType::new(FieldTypeCode::Varchar);
    field_type.set_flen(flen);
    field_type.set_charset_name("utf8mb4");
    field_type.set_collation_name("utf8mb4_bin");
    field_type
}

fn blob() -> FieldType {
    let mut field_type = FieldType::new(FieldTypeCode::Blob);
    field_type.set_flen(-1);
    field_type.set_charset_name("utf8mb4");
    field_type.set_collation_name("utf8mb4_bin");
    field_type
}

fn text(value: impl Into<String>) -> Datum {
    Datum::Bytes(value.into().into_bytes())
}

impl Session {
    /// Go `buildRecommendIndex` and `RecommendIndexExec.Next`.
    pub(crate) fn recommend_index_stmt(
        &mut self,
        statement: &tidb_ast::RecommendIndexStmt,
    ) -> Result<StmtOutput> {
        use tidb_ast::RecommendIndexStmt as Stmt;
        match statement {
            Stmt::Run { sql, options } => {
                let columns = vec![
                    ("database".to_owned(), varchar(64)),
                    ("table".to_owned(), varchar(64)),
                    ("index_name".to_owned(), varchar(64)),
                    ("index_columns".to_owned(), varchar(256)),
                    ("est_index_size".to_owned(), varchar(256)),
                    ("reason".to_owned(), varchar(256)),
                    ("top_impacted_query".to_owned(), blob()),
                    ("create_index_statement".to_owned(), blob()),
                ];
                let mut sqls = Vec::new();
                if let Some(text) = sql.as_deref().filter(|text| !text.is_empty()) {
                    for piece in text.split(';') {
                        let piece = piece.trim();
                        if !piece.is_empty() {
                            sqls.push(piece.to_owned());
                        }
                    }
                    if sqls.is_empty() {
                        return Err(advisor_error("empty SQLs"));
                    }
                }
                let results = self.advise_indexes(sqls, options)?;
                let rows = results
                    .iter()
                    .map(|result| {
                        let columns_text = result.index_columns.join(",");
                        vec![
                            text(result.database.clone()),
                            text(result.table.clone()),
                            text(result.index_name.clone()),
                            text(columns_text.clone()),
                            text(result.index_size.to_string()),
                            text(result.reason.clone()),
                            text(impacted_queries_json(&result.top_impacted_queries)),
                            text(format!(
                                "CREATE INDEX {} ON {}({});",
                                result.index_name, result.table, columns_text
                            )),
                        ]
                    })
                    .collect();
                Ok(StmtOutput::Rows { columns, rows })
            }
            Stmt::Set(options) => {
                if options.is_empty() {
                    return Err(advisor_error("option is empty"));
                }
                for option in options {
                    self.set_advisor_option(&option.name, &option.value)?;
                }
                Ok(StmtOutput::Done(false))
            }
            Stmt::ShowOption => {
                let columns = vec![
                    ("option".to_owned(), varchar(64)),
                    ("value".to_owned(), varchar(64)),
                    ("description".to_owned(), varchar(256)),
                ];
                let values = self.get_advisor_options()?;
                let rows = ALL_OPTIONS
                    .iter()
                    .filter_map(|name| {
                        values.get(*name).map(|value| {
                            vec![
                                text(*name),
                                text(value.clone()),
                                text(options::description(name)),
                            ]
                        })
                    })
                    .collect();
                Ok(StmtOutput::Rows { columns, rows })
            }
            Stmt::Apply(_) => Err(advisor_error("unsupported action apply")),
            Stmt::Ignore(_) => Err(advisor_error("unsupported action ignore")),
            Stmt::Status => Err(advisor_error("unsupported action status")),
            Stmt::Cancel => Err(advisor_error("unsupported action cancel")),
            Stmt::Unknown(action) => Err(advisor_error(format!("unsupported action {action}"))),
        }
    }

    /// Go `GetOptions`: the stored values, defaults for the unset ones.
    fn get_advisor_options(&mut self) -> Result<std::collections::BTreeMap<String, String>> {
        let names: Vec<String> = ALL_OPTIONS.iter().map(|name| format!("'{name}'")).collect();
        let sql = format!(
            "SELECT name, value FROM mysql.tidb_kernel_options WHERE module = '{OPT_MODULE}' AND name in ({})",
            names.join(",")
        );
        let rows = self.user_table_rows(&sql)?;
        let mut values = std::collections::BTreeMap::new();
        for row in rows {
            let name = row.first().and_then(crate::datum_text).unwrap_or_default();
            let value = row.get(1).and_then(crate::datum_text).unwrap_or_default();
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
    fn set_advisor_option(&mut self, name: &str, value: &tidb_ast::Expr) -> Result<()> {
        let value = options::option_val(name, value)?;
        let quote = crate::user_table::sql_str;
        let description = options::description(name);
        let sql = format!(
            "INSERT INTO mysql.tidb_kernel_options VALUES ({}, {}, {}, now(), 'valid', {}) \
             ON DUPLICATE KEY UPDATE value = {}, updated_at=now(), description = {}",
            quote(OPT_MODULE),
            quote(name),
            quote(&value),
            quote(description),
            quote(&value),
            quote(description),
        );
        self.run_internal_write(&sql).map(|_| ())
    }

    /// Go `AdviseIndexes` and `adviseIndexesWithOption`.
    fn advise_indexes(
        &mut self,
        sqls: Vec<String>,
        user_options: &[tidb_ast::RecommendIndexOption],
    ) -> Result<Vec<Recommendation>> {
        let values = self.get_advisor_options()?;
        let option = options::fill_option(values, user_options, sqls)?;

        let default_db = self.vars.current_db.clone();
        let mut optimizer = SessionOptimizer { session: self };
        let query_set = prepare_query_set(&mut optimizer, &default_db, &option)?;

        // Identify the indexable columns.
        let indexable = query::collect_indexable_columns_for_query_set(&mut optimizer, &query_set)?;

        // Start the advisor.
        let (indexes, all_candidates) =
            algorithm::advise_indexes(&query_set, &indexable, &mut optimizer, &option)?;

        let results = prepare_recommendation(&indexes, &query_set, &mut optimizer)?;
        optimizer.session.save_recommendations(&results);

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
            self.append_warning(WarningLevel::Warning, 1105, explanation);
        }
        Ok(results)
    }

    /// Go `saveRecommendations`: a failure is logged, never raised.
    fn save_recommendations(&mut self, results: &[Recommendation]) {
        let quote = crate::user_table::sql_str;
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
            let sql = format!(
                "insert into mysql.index_advisor_results (\
                 created_at, updated_at, schema_name, table_name, index_name, \
                 index_columns, index_details, top_impacted_queries, workload_impact, extra) values \
                 (now(), now(), {}, {}, {}, {}, {}, {}, {}, null) \
                 on duplicate key update \
                 updated_at=now(), index_details={}, top_impacted_queries={}, workload_impact={}",
                quote(&result.database),
                quote(&result.table),
                quote(&result.index_name),
                quote(&result.index_columns.join(",")),
                quote(&detail),
                quote(&queries),
                quote(&workload),
                quote(&detail),
                quote(&queries),
                quote(&workload),
            );
            let _ = self.run_internal_write(&sql);
        }
    }
}

/// Go `prepareQuerySet`.
fn prepare_query_set(
    optimizer: &mut SessionOptimizer<'_>,
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
        query_set = optimizer.session.load_query_set_from_stmt_summary(option)?;
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
    let query_set = query::filter_invalid_queries(optimizer, &query_set, ignore_error)?;
    if query_set.size() == 0 {
        return Err(advisor_error(
            "empty query set after filtering invalid queries",
        ));
    }
    Ok(query_set)
}

impl Session {
    /// Go `loadQuerySetFromStmtSummary`.
    fn load_query_set_from_stmt_summary(&mut self, option: &AdvisorOption) -> Result<QuerySet> {
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
        let ctx = self.statement_context(false);
        let rows = self.set_value_rows(&sql, &ctx)?;
        let mut query_set = QuerySet::new();
        for row in rows {
            let schema_name = row.first().and_then(crate::datum_text).unwrap_or_default();
            let text = row.get(1).and_then(crate::datum_text).unwrap_or_default();
            let frequency = match row.get(2) {
                Some(Datum::Real(count)) => *count as i64,
                Some(other) => crate::datum_text(other)
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
}

/// Go `prepareRecommendation`.
fn prepare_recommendation(
    indexes: &KeySet<Index>,
    queries: &QuerySet,
    optimizer: &mut SessionOptimizer<'_>,
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
    optimizer: &mut SessionOptimizer<'_>,
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
    let mut text = tidb_datatype::go_strconv::format_float(
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
fn impacted_queries_json(queries: &[ImpactedQuery]) -> String {
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

/// The what-if optimizer over a live session (Go `optimizerImpl`).
struct SessionOptimizer<'a> {
    session: &'a mut Session,
}

impl SessionOptimizer<'_> {
    /// `TableByName` and the columns it has.
    fn table_columns_of(&mut self, schema: &str, table: &str) -> Result<Vec<(String, FieldType)>> {
        self.session
            .with_catalog_mut(|catalog| match catalog.table_in(schema, table) {
                Some(tidb_executor::TableEntry::Kv(stored)) => Ok(stored
                    .columns()
                    .iter()
                    .map(|column| (column.name.go_to_lower(), column.field_type.clone()))
                    .collect()),
                Some(_) => Ok(Vec::new()),
                None => Err(DriverError::from(
                    tidb_error::catalog::infoschema::ERR_TABLE_NOT_EXISTS
                        .generate_with_stack_by_args(&[(&schema).into(), (&table).into()]),
                )),
            })
    }

    fn plan_cost(&mut self, statement: &tidb_ast::Stmt, hypo_indexes: &[Index]) -> Result<f64> {
        // Go `QueryPlanCost`: hypothetical indexes are exactly the given ones.
        let mut hypo = HypoIndexes::default();
        for index in hypo_indexes {
            let columns: Vec<String> = index
                .columns
                .iter()
                .map(|column| column.column_name.clone())
                .collect();
            self.session.with_catalog_mut(|catalog| {
                tidb_executor::ddl::set_advisor_hypo_index(
                    catalog,
                    &index.schema_name,
                    &index.table_name,
                    &index.index_name,
                    &columns,
                    &mut hypo,
                )
            })?;
        }
        self.session.hypo_indexes = Arc::new(hypo);

        let current_db = self.session.vars.current_db.clone();
        // Go sets \`StmtCtx.InExplainStmt\`, the only planning mode that reads
        // the session's hypothetical indexes.
        let ctx = self
            .session
            .statement_context(false)
            .with_in_explain_stmt(true)
            .with_skip_cte_consumer_counts(true);
        match statement {
            tidb_ast::Stmt::Query(query) => self.session.with_catalog_mut(|catalog| {
                tidb_executor::query_plan_cost(query, catalog, &current_db, &ctx)
            }),
            tidb_ast::Stmt::Dml(dml) => match &**dml {
                tidb_ast::DmlStmt::Insert(_) => Err(advisor_error(
                    "runtime error: invalid memory address or nil pointer dereference",
                )),
                tidb_ast::DmlStmt::Update(_) | tidb_ast::DmlStmt::Delete(_) => {
                    let (verb, table) = match &**dml {
                        tidb_ast::DmlStmt::Update(_) => ("UPDATE", first_table_name(statement)),
                        _ => ("DELETE", first_table_name(statement)),
                    };
                    Err(DriverError::Mysql(tidb_executor::MysqlError::from_terror(
                        &tidb_error::plannererrors::ERR_NON_UPDATABLE_TABLE
                            .generate_with_stack_by_args(&[table.into(), verb.into()]),
                    )))
                }
                _ => Err(advisor_error("plan is not a physical plan: *core.Simple")),
            },
            tidb_ast::Stmt::Admin(admin) => Err(advisor_error(match &**admin {
                tidb_ast::AdminStmt::Explain(_) => "plan is not a physical plan: *core.Explain",
                tidb_ast::AdminStmt::AnalyzeTable(_)
                | tidb_ast::AdminStmt::AnalyzeIncremental(_) => {
                    "runtime error: invalid memory address or nil pointer dereference"
                }
                _ => "plan is not a physical plan: *core.Simple",
            })),
            tidb_ast::Stmt::Ddl(_) => Err(advisor_error("plan is not a physical plan: *core.DDL")),
            tidb_ast::Stmt::Session(_) => {
                Err(advisor_error("plan is not a physical plan: *core.Simple"))
            }
        }
    }
}

/// The name of the first table a statement reads.
fn first_table_name(statement: &tidb_ast::Stmt) -> String {
    let mut statement = statement.clone();
    let mut name = String::new();
    let mut visitor = |node: &mut dyn std::any::Any| {
        if name.is_empty() {
            if let Some(table) = node.downcast_ref::<tidb_ast::TableRef>() {
                name = table.name.last().cloned().unwrap_or_default();
            }
        }
        false
    };
    struct First<'a, F: FnMut(&mut dyn std::any::Any) -> bool>(&'a mut F);
    impl<F: FnMut(&mut dyn std::any::Any) -> bool> tidb_ast::Visitor for First<'_, F> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            (self.0)(node)
        }
        fn leave(&mut self, _: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    tidb_ast::Visitable::accept(&mut statement, &mut First(&mut visitor));
    name
}

impl Optimizer for SessionOptimizer<'_> {
    fn column_type(&mut self, column: &Column) -> Result<FieldType> {
        let wanted = column.column_name.go_to_lower();
        let columns = self.table_columns_of(&column.schema_name, &column.table_name)?;
        columns
            .into_iter()
            .find(|(name, _)| *name == wanted)
            .map(|(_, field_type)| field_type)
            .ok_or_else(|| {
                advisor_error(format!(
                    "column {} not found in table {}.{}",
                    column.column_name, column.schema_name, column.table_name
                ))
            })
    }

    fn prefix_contain_index(&mut self, index: &Index) -> Result<bool> {
        self.session.with_catalog_mut(|catalog| {
            let Some(tidb_executor::TableEntry::Kv(table)) =
                catalog.table_in(&index.schema_name, &index.table_name)
            else {
                return Err(DriverError::from(
                    tidb_error::catalog::infoschema::ERR_TABLE_NOT_EXISTS
                        .generate_with_stack_by_args(&[
                            (&index.schema_name).into(),
                            (&index.table_name).into(),
                        ]),
                ));
            };
            for stored in table.indexes() {
                if stored.column_offsets.len() < index.columns.len() {
                    continue;
                }
                let matched = index.columns.iter().enumerate().all(|(position, column)| {
                    table.columns()[stored.column_offsets[position]]
                        .name
                        .go_to_lower()
                        == column.column_name.go_to_lower()
                });
                if matched {
                    return Ok(true);
                }
            }
            Ok(false)
        })
    }

    fn possible_columns(&mut self, schema: &str, column_name: &str) -> Result<Vec<Column>> {
        // Filtering the system schemas.
        let schema = schema.go_to_lower();
        if tidb_metadef::db::is_mem_db(&schema) || tidb_metadef::db::is_system_db(&schema) {
            return Ok(Vec::new());
        }
        self.session.with_catalog_mut(|catalog| {
            let Some(names) = catalog.table_names(&schema) else {
                return Err(DriverError::from(
                    tidb_error::catalog::infoschema::ERR_DATABASE_NOT_EXISTS
                        .generate_with_stack_by_args(&[(schema.clone()).into()]),
                ));
            };
            let mut found = Vec::new();
            for name in names {
                if let Some(tidb_executor::TableEntry::Kv(table)) = catalog.table_in(&schema, &name)
                {
                    for column in table.columns() {
                        if column.name.go_to_lower() == column_name {
                            found.push(Column {
                                schema_name: schema.clone(),
                                table_name: name.go_to_lower(),
                                column_name: column.name.go_to_lower(),
                            });
                        }
                    }
                }
            }
            Ok(found)
        })
    }

    fn table_columns(&mut self, schema: &str, table: &str) -> Result<Vec<Column>> {
        Ok(self
            .table_columns_of(schema, table)?
            .into_iter()
            .map(|(name, _)| Column {
                schema_name: schema.to_owned(),
                table_name: table.to_owned(),
                column_name: name,
            })
            .collect())
    }

    fn index_name_exist(&mut self, schema: &str, table: &str, index_name: &str) -> Result<bool> {
        self.session.with_catalog_mut(|catalog| {
            let Some(tidb_executor::TableEntry::Kv(stored)) = catalog.table_in(schema, table)
            else {
                return Err(DriverError::from(
                    tidb_error::catalog::infoschema::ERR_TABLE_NOT_EXISTS
                        .generate_with_stack_by_args(&[(&schema).into(), (&table).into()]),
                ));
            };
            Ok(stored
                .indexes()
                .iter()
                .any(|index| index.name.go_to_lower() == index_name))
        })
    }

    fn est_index_size(&mut self, database: &str, table: &str, columns: &[String]) -> Result<f64> {
        let stats_handle = self.session.stats_handle();
        self.session.with_catalog_mut(|catalog| {
            let Some(tidb_executor::TableEntry::Kv(stored)) = catalog.table_in(database, table)
            else {
                return Err(DriverError::from(
                    tidb_error::catalog::infoschema::ERR_TABLE_NOT_EXISTS
                        .generate_with_stack_by_args(&[(&database).into(), (&table).into()]),
                ));
            };
            let statistics = catalog.table_statistics_in(&stats_handle, stored.table_id());
            let row_count = statistics
                .as_ref()
                .map_or(0.0, |stats| stats.row_count as f64);
            let mut size = 0.0_f64;
            for name in columns {
                let wanted = name.go_to_lower();
                let total_size = stored
                    .columns()
                    .iter()
                    .find(|column| column.name.go_to_lower() == wanted)
                    .and_then(|column| {
                        let stats = statistics.as_ref()?;
                        let loaded = stats
                            .column_load_status
                            .get(&column.id)
                            .is_some_and(|status| status.stats_initialized());
                        loaded
                            .then(|| stats.columns.get(&column.id))?
                            .map(|column_stats| column_stats.histogram.tot_col_size as f64)
                    });
                match total_size {
                    // Might be not loaded.
                    None => size += 8.0 * row_count,
                    Some(total) => size += total,
                }
            }
            Ok(size)
        })
    }

    fn query_plan_cost(&mut self, sql: &str, hypo_indexes: &[Index]) -> Result<f64> {
        let statement = query::parse_one_sql(sql)?;
        let saved_warnings = self.session.warnings.clone();
        let saved_hypo = Arc::clone(&self.session.hypo_indexes);
        let result = self.plan_cost(&statement, hypo_indexes);
        self.session.warnings = saved_warnings;
        self.session.hypo_indexes = saved_hypo;
        result
    }
}

#[cfg(test)]
mod tests {
    use crate::tests_support::row_text;
    use crate::Session;

    fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
        row_text(session.run(sql))
    }

    fn error(session: &mut Session, sql: &str) -> (u16, String) {
        let error = session
            .run(sql)
            .err()
            .unwrap_or_else(|| panic!("{sql} was accepted"))
            .to_mysql_error();
        (error.code, error.message)
    }

    /// Go `checkResult` of `indexadvisor_sql_test.go`: `db.table.columns` of
    /// each recommendation, sorted.
    fn recommended(session: &mut Session, statements: &str) -> String {
        let sql = format!(
            "recommend index run for '{}'",
            statements.replace('\'', "''")
        );
        let mut found: Vec<String> = rows(session, &sql)
            .iter()
            .map(|row| format!("{}.{}.{}", row[0], row[1], row[3]))
            .collect();
        found.sort();
        found.join("|")
    }

    /// `TestIndexAdvisorForSQL`, `ForMultipleTables`, `Prefix`, `CoveringIndex`
    /// and `ExistingIndex`: the algorithm's picks, and the plan costs behind
    /// them, follow Go's.
    #[test]
    fn advisor_picks_the_indexes_go_picks() {
        let mut session = Session::new();
        session.run("use test").unwrap();
        session.run("create table t (a int, b int, c int)").unwrap();
        assert_eq!(
            recommended(&mut session, "select a from t where a=1"),
            "test.t.a"
        );
        assert_eq!(
            recommended(&mut session, "select a from t where a=1 and b=1"),
            "test.t.a,b"
        );
        assert_eq!(
            recommended(
                &mut session,
                "select * from t where a=1;select * from t where a=1 and b=1"
            ),
            "test.t.a,b"
        );
        assert_eq!(
            recommended(
                &mut session,
                "select * from t where a=1;select * from t where a=1 and b=1; select * from t where a=1 and b=1 and c=1"
            ),
            "test.t.a,b,c"
        );
        session
            .run("create table tc (a int, b int, c int, d int)")
            .unwrap();
        assert_eq!(
            recommended(&mut session, "select b from tc where a=1"),
            "test.tc.a,b"
        );
        assert_eq!(
            recommended(&mut session, "select b from tc where a=1 and d=1"),
            "test.tc.a,d,b"
        );
        session
            .run("create table te (a int, b int, c int, index ab (a, b))")
            .unwrap();
        assert_eq!(recommended(&mut session, "select * from te where a=1"), "");
        assert_eq!(
            recommended(
                &mut session,
                "select * from te where a=1; select * from te where a=1 and b=1; select * from te where c=1"
            ),
            "test.te.c"
        );
        // A statement the optimizer plans with the DNF/order-by shape of
        // `heuristicMergeIndexes`: hypothetical indexes outside the stats
        // collection, as Go's session-local `IndexInfo`s are.
        assert_eq!(
            recommended(
                &mut session,
                "select * from t where (a=1 or b=1) and c=2 order by c"
            ),
            "test.t.c"
        );
        // A CTE is not inlined: the advisor never ran Go's preprocessor.
        assert_eq!(
            recommended(
                &mut session,
                "with c as (select * from t where a=1) select * from c where b=2"
            ),
            ""
        );
        // A locking read still sees hypothetical indexes.
        assert_eq!(
            recommended(&mut session, "select * from t where a=1 for update"),
            "test.t.a"
        );
    }

    /// The columns `recommend index run` answers, and the statement it prints.
    #[test]
    fn advisor_reports_go_s_columns_and_restored_queries() {
        let mut session = Session::new();
        session.run("use test").unwrap();
        session.run("create table t (a int, b int, c int)").unwrap();
        let answer = rows(
            &mut session,
            "recommend index run for 'select a, count(*) from t where a=1 and (b=2 or c=3)'",
        );
        assert_eq!(answer.len(), 1, "{answer:?}");
        assert_eq!(&answer[0][..5], ["test", "t", "idx_a", "a", "0"]);
        assert_eq!(
            answer[0][5],
            "Column [a] appear in Equal or Range Predicate clause(s) in query: select `a` , count ( ? ) from `test` . `t` where `a` = ? and ( `b` = ? or `c` = ? )"
        );
        assert!(
            answer[0][6].contains(
                "\"Query\":\"SELECT `a`,count(1) FROM `test`.`t` WHERE `a` = 1 AND (`b` = 2 OR `c` = 3)\""
            ),
            "{answer:?}"
        );
        assert_eq!(answer[0][7], "CREATE INDEX idx_a ON t(a);");
        // No recommendation leaves Go's explanation as a warning.
        // An unqualified column names every table of the schema that has it,
        // so the empty case gets a schema of its own.
        session.run("create database kdb").unwrap();
        session.run("use kdb").unwrap();
        session
            .run("create table k (a int, b int, c int, key(a, b, c))")
            .unwrap();
        assert!(rows(
            &mut session,
            "recommend index run for 'select * from k where a=1 and b=1 and c=1'"
        )
        .is_empty());
        let warnings = rows(&mut session, "show warnings");
        assert_eq!(
            warnings,
            vec![vec![
                "Warning".to_owned(),
                "1105".to_owned(),
                " Considered 3 indexable columns(kdb.k.a, kdb.k.b, kdb.k.c), 3 or more index candidates(kdb.k(a), kdb.k(b), kdb.k(c)), no sufficiently beneficial indexes were found.".to_owned()
            ]]
        );
    }

    /// `options.go`: values are validated, stored in
    /// `mysql.tidb_kernel_options`, and read back by SHOW.
    #[test]
    fn advisor_options_follow_go() {
        let mut session = Session::new();
        session.run("use test").unwrap();
        let shown = rows(&mut session, "recommend index show");
        let names: Vec<&str> = shown.iter().map(|row| row[0].as_str()).collect();
        assert_eq!(
            names,
            [
                "max_num_index",
                "max_index_columns",
                "max_num_query",
                "timeout"
            ]
        );
        assert_eq!(shown[0][1], "5");
        session
            .run("recommend index set max_num_index=3, timeout='10s'")
            .unwrap();
        let shown = rows(&mut session, "recommend index show");
        assert_eq!((shown[0][1].as_str(), shown[3][1].as_str()), ("3", "10s"));
        assert_eq!(
            error(&mut session, "recommend index set max_num_index=0"),
            (1105, "invalid value 0 for max_num_index".to_owned())
        );
        assert_eq!(
            error(&mut session, "recommend index set timeout='abc'"),
            (1105, "time: invalid duration \"abc\"".to_owned())
        );
        assert_eq!(
            error(&mut session, "recommend index set max_num_index='x'"),
            (1105, "invalid value type string".to_owned())
        );
        assert_eq!(
            error(&mut session, "recommend index set timeout=10"),
            (
                1105,
                "invalid value type int64 for timeout, expected a duration string".to_owned()
            )
        );
        // A non-literal value makes Go's parser drop the whole list.
        assert_eq!(
            error(&mut session, "recommend index set max_num_index=-1"),
            (1105, "option is empty".to_owned())
        );
        assert_eq!(
            error(&mut session, "recommend index frobnicate"),
            (1105, "unsupported action frobnicate".to_owned())
        );
        assert_eq!(
            error(&mut session, "recommend index apply 1"),
            (1105, "unsupported action apply".to_owned())
        );
    }

    /// The refusals of `FilterInvalidQueries` under fix control 43817 and the
    /// like, for a statement the user named.
    #[test]
    fn advisor_refuses_what_go_refuses() {
        let mut session = Session::new();
        session.run("use test").unwrap();
        session.run("create table t (a int, b int, c int)").unwrap();
        assert_eq!(
            error(
                &mut session,
                "recommend index run for 'select * from t where a=(select max(b) from t)'"
            ),
            (
                1105,
                "evaluate non-correlated sub-queries during optimization phase is not allowed by fix-control 43817"
                    .to_owned()
            )
        );
        assert_eq!(
            error(
                &mut session,
                "recommend index run for 'select * from nosuch where a=1'"
            ),
            (1146, "Table 'test.nosuch' doesn't exist".to_owned())
        );
        assert_eq!(
            error(&mut session, "recommend index run for 'show tables'"),
            (
                1105,
                "empty query set after filtering invalid queries".to_owned()
            )
        );
        assert_eq!(
            error(&mut session, "recommend index run for ';;'"),
            (1105, "empty SQLs".to_owned())
        );
        assert_eq!(
            error(
                &mut session,
                "recommend index run for 'select * from t where a=1' with timeout='1ns'"
            ),
            (1105, "index advisor timeout after 1ns".to_owned())
        );
    }

    /// `RestoreWithDefaultDB` (binding flags): function names stay as written,
    /// the lock clause keeps Go's spelling, and redundant parentheses go.
    #[test]
    fn binding_restore_keeps_written_names_and_drops_redundant_parentheses() {
        let restore = |sql: &str| {
            let statement = tidb_parser::parse(sql).unwrap();
            tidb_parser::util_parser::restore_with_default_db(&statement, "test", "")
        };
        assert_eq!(
            restore("select Max(a), COUNT(*), group_concat(a separator ',') from t for update"),
            "SELECT Max(`a`),COUNT(1),group_concat(`a` SEPARATOR ',') FROM `test`.`t` for update"
        );
        assert_eq!(
            restore("select * from t where a=1 for update of t"),
            "SELECT * FROM `test`.`t` WHERE `a` = 1 for update OF `test`.`t`"
        );
        assert_eq!(
            restore("select * from t where (a=1) and (b=2 and c=3) or a*(b*c)=1"),
            "SELECT * FROM `test`.`t` WHERE `a` = 1 AND `b` = 2 AND `c` = 3 OR `a` * (`b` * `c`) = 1"
        );
        assert_eq!(
            restore("select * from t where (a-1)-2=4 and a-(1-2)=4 and -(a+1)=1"),
            "SELECT * FROM `test`.`t` WHERE `a` - 1 - 2 = 4 AND `a` - (1 - 2) = 4 AND -(`a` + 1) = 1"
        );
        assert_eq!(
            restore("select count(distinct (b)) from t"),
            "SELECT count(DISTINCT `b`) FROM `test`.`t`"
        );
    }

    /// Go's resolver visits a window call's columns, so an aggregated query
    /// must justify them.
    #[test]
    fn window_columns_in_an_aggregated_query_need_justifying() {
        let mut session = Session::new();
        session.run("use test").unwrap();
        session.run("create table t (a int, b int, c int)").unwrap();
        assert_eq!(
            error(
                &mut session,
                "select max(a), row_number() over (order by c) from t"
            )
            .0,
            8123
        );
        assert_eq!(
            error(
                &mut session,
                "select a, max(b), row_number() over (order by c) from t group by a"
            )
            .0,
            1055
        );
        assert_eq!(
            rows(
                &mut session,
                "select a, max(b), row_number() over (order by a) from t group by a"
            ),
            Vec::<Vec<String>>::new()
        );
    }
}
