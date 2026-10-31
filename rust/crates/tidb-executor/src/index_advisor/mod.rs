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


//! `RecommendIndexExec` (`pkg/executor/recommend_index.go`): `RECOMMEND INDEX
//! RUN | SET | SHOW` over the advisor of `tidb-planner-indexadvisor`, and the
//! session's what-if [`Optimizer`] (Go `optimizerImpl`).

use std::sync::Arc;

use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_hack::GoToLower;
use tidb_planner_indexadvisor::{
    advisor_error, AdvisorSession, Column, Index, Optimizer, Result, ALL_OPTIONS,
};
use tidb_stats_handle::catalog_statistics::CatalogStatistics;
use tidb_variable::hypo_index::HypoIndexes;

use crate::DriverError;
use crate::StmtOutput;

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

/// Go `RecommendIndexExec` (`executor/recommend_index.go`) and the
/// advisor's session optimizer.
pub trait IndexAdvisorExec: crate::bind_exec::SqlBindExec {
    /// Replaces `SessionVars.HypoIndexes`.
    fn set_hypo_indexes(&mut self, indexes: Arc<HypoIndexes>);
    /// The statement's warnings, to restore after a what-if plan.
    fn warnings_snapshot(&self) -> Vec<tidb_context::SqlWarn>;
    /// Puts back a [`Self::warnings_snapshot`].
    fn restore_warnings(&mut self, warnings: Vec<tidb_context::SqlWarn>);
    /// An internal write over the system tables.
    fn run_internal_write(&mut self, sql: &str) -> Result<Option<u64>>;

    /// Go `buildRecommendIndex` and `RecommendIndexExec.Next`.
    fn recommend_index_stmt(
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
                let results = tidb_planner_indexadvisor::advise_indexes(
                    &mut SessionOptimizer::<Self> { session: self },
                    sqls,
                    options,
                )?;
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
                            text(tidb_planner_indexadvisor::impacted_queries_json(
                                &result.top_impacted_queries,
                            )),
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
                    tidb_planner_indexadvisor::set_option(
                        &mut SessionOptimizer::<Self> { session: self },
                        &option.name,
                        &option.value,
                    )?;
                }
                Ok(StmtOutput::Done(false))
            }
            Stmt::ShowOption => {
                let columns = vec![
                    ("option".to_owned(), varchar(64)),
                    ("value".to_owned(), varchar(64)),
                    ("description".to_owned(), varchar(256)),
                ];
                let values = tidb_planner_indexadvisor::get_options(&mut SessionOptimizer::<Self> {
                    session: self,
                })?;
                let rows = ALL_OPTIONS
                    .iter()
                    .filter_map(|name| {
                        values.get(*name).map(|value| {
                            vec![
                                text(*name),
                                text(value.clone()),
                                text(tidb_planner_indexadvisor::description(name)),
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

}

/// The what-if optimizer over a live session (Go `optimizerImpl`).
struct SessionOptimizer<'a, S: IndexAdvisorExec> {
    session: &'a mut S,
}

impl<S: IndexAdvisorExec> SessionOptimizer<'_, S> {
    /// `TableByName` and the columns it has.
    fn table_columns_of(&mut self, schema: &str, table: &str) -> Result<Vec<(String, FieldType)>> {
        self.session
            .with_catalog_mut(|catalog| match catalog.table_in(schema, table) {
                Some(crate::TableEntry::Kv(stored)) => Ok(stored
                    .columns()
                    .iter()
                    .map(|column| (column.name.go_to_lower(), column.field_type.clone()))
                    .collect()),
                Some(_) => Ok(Vec::new()),
                None => Err(DriverError::from(
                    tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
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
                crate::ddl::set_advisor_hypo_index(
                    catalog,
                    &index.schema_name,
                    &index.table_name,
                    &index.index_name,
                    &columns,
                    &mut hypo,
                )
            })?;
        }
        self.session.set_hypo_indexes(Arc::new(hypo));

        let current_db = self.session.session_vars().current_db.clone();
        // Go sets \`StmtCtx.InExplainStmt\`, the only planning mode that reads
        // the session's hypothetical indexes.
        let ctx = self
            .session
            .statement_context(false)
            .with_in_explain_stmt(true)
            .with_skip_cte_consumer_counts(true);
        match statement {
            tidb_ast::Stmt::Query(query) => self.session.with_catalog_mut(|catalog| {
                crate::query_plan_cost(query, catalog, &current_db, &ctx)
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
                    Err(DriverError::Mysql(crate::MysqlError::from_terror(
                        &tidb_plannererrors::ERR_NON_UPDATABLE_TABLE
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

impl<S: IndexAdvisorExec> Optimizer for SessionOptimizer<'_, S> {
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
            let Some(crate::TableEntry::Kv(table)) =
                catalog.table_in(&index.schema_name, &index.table_name)
            else {
                return Err(DriverError::from(
                    tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
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
                    tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                        .generate_with_stack_by_args(&[(schema.clone()).into()]),
                ));
            };
            let mut found = Vec::new();
            for name in names {
                if let Some(crate::TableEntry::Kv(table)) = catalog.table_in(&schema, &name)
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
            let Some(crate::TableEntry::Kv(stored)) = catalog.table_in(schema, table)
            else {
                return Err(DriverError::from(
                    tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
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
            let Some(crate::TableEntry::Kv(stored)) = catalog.table_in(database, table)
            else {
                return Err(DriverError::from(
                    tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                        .generate_with_stack_by_args(&[(&database).into(), (&table).into()]),
                ));
            };
            let statistics = catalog.table_statistics_in(&stats_handle, stored.table_id());
            let row_count = statistics
                .as_ref()
                .map_or(0.0, |stats| stats.hist_coll.realtime_count as f64);
            let mut size = 0.0_f64;
            for name in columns {
                let wanted = name.go_to_lower();
                let total_size = stored
                    .columns()
                    .iter()
                    .find(|column| column.name.go_to_lower() == wanted)
                    .and_then(|column| {
                        let column_stats = statistics.as_ref()?.hist_coll.get_column(column.id)?;
                        let column_stats = column_stats
                            .read()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        column_stats
                            .is_stats_initialized()
                            .then(|| column_stats.histogram.tot_col_size as f64)
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
        let statement = tidb_planner_indexadvisor::parse_one_sql(sql)?;
        let saved_warnings = self.session.warnings_snapshot();
        let saved_hypo = self.session.hypo_indexes();
        let result = self.plan_cost(&statement, hypo_indexes);
        self.session.restore_warnings(saved_warnings);
        self.session.set_hypo_indexes(saved_hypo);
        result
    }
}

impl<S: IndexAdvisorExec> AdvisorSession for SessionOptimizer<'_, S> {
    fn option_rows(&mut self, sql: &str) -> Result<Vec<Vec<Datum>>> {
        self.session.user_table_rows(sql)
    }

    fn stmt_summary_rows(&mut self, sql: &str) -> Result<Vec<Vec<Datum>>> {
        let ctx = self.session.statement_context(false);
        self.session.set_value_rows(sql, &ctx)
    }

    fn exec_write(&mut self, sql: &str) -> Result<()> {
        self.session.run_internal_write(sql).map(|_| ())
    }

    fn current_db(&self) -> String {
        self.session.session_vars().current_db.clone()
    }

    fn append_warning(&mut self, warning: tidb_context::SqlWarn) {
        self.session.append_sql_warning(warning);
    }
}
