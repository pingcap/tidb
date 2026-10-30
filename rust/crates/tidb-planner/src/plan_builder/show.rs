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

//! Go `PlanBuilder.buildShow` (`pkg/planner/core/planbuilder.go:3532`).
//!
//! # Narrowings, by name
//!
//! * The SHOW STATS_BUCKETS / HISTOGRAMS / META / LOCKED visit reads
//!   `show.Table`, which those statements never set; the visit names no
//!   table, which is what Go's would name had it a table to read.

use std::collections::BTreeMap;

use tidb_ast::{Expr, ShowStmt, ShowStmtType};
use tidb_expression_aggregation::ByItems;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_exprctx::Columns;
use tidb_model::TempTableType;

use tidb_infoschema::table_source::TableSource;
use super::visit_info::{VisitAuthError, VisitInfo};
use super::PlanBuilder;
use tidb_planner_core_operator_logicalop::projection::LogicalProjection;
use crate::rule::flags;
use tidb_planner_core_operator_logicalop::sort::LogicalSort;
use crate::common_plans::{Plan, ShowNextRowID};
use tidb_planner_core_operator_logicalop::show_ddl_jobs::LogicalShowDDLJobs;
use tidb_planner_core_operator_logicalop::{LogicalPlan, LogicalShow, ShowContents};
use tidb_planner_core_base::PlanError;
use crate::show_predicate_extractor::ShowBaseExtractor;
use crate::show_schema::{build_show_ddl_jobs_fields, build_show_next_row_id, build_show_schema};

/// Go `show.Table.Schema.L` / `show.Table.Name.L`, empty when no table.
fn table_names(show: &ShowStmt) -> (String, String) {
    match show.table.as_deref() {
        Some([schema, name]) => (tidb_mysql::to_lowercase(schema), tidb_mysql::to_lowercase(name)),
        Some([name]) => (String::new(), tidb_mysql::to_lowercase(name)),
        _ => (String::new(), String::new()),
    }
}

/// Go `buildShow`'s `logicalop.ShowContents` literal.
fn show_contents(show: &ShowStmt) -> ShowContents {
    ShowContents {
        tp: show.tp,
        db_name: show.db_name.clone(),
        table_name: show.table.as_ref().and_then(|path| path.last().cloned()),
        table_partitions: show.table_partitions.clone(),
        partition: show.partition.clone(),
        column_name: show.column.as_ref().and_then(|path| path.last().cloned()),
        index_name: show.index_name.clone(),
        resource_group_name: show.resource_group_name.clone(),
        flag: show.flag,
        user: show.user.clone(),
        roles: show.roles.clone(),
        count_warnings_or_errors: show.count_warnings_or_errors,
        full: show.full,
        if_not_exists: show.if_not_exists,
        global_scope: show.global_scope,
        extended: show.extended,
        import_job_id: show.import_job_id,
        import_group_key: show.show_group_key.clone(),
        distribution_job_id: show.distribution_job_id,
    }
}

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// Go `appendVisitInfo(b.visitInfo, priv, db, table, "", err)`.
    fn append_show_visit(
        &mut self,
        privilege: tidb_mysql::PrivilegeType,
        database: &str,
        table: &str,
        error: VisitAuthError,
    ) {
        self.visit_info.push(VisitInfo {
            privilege,
            database: database.to_owned(),
            table: table.to_owned(),
            column: String::new(),
            error,
            dynamic_privs: Vec::new(),
            dynamic_with_grant: false,
        });
    }

    /// Go `appendDynamicVisitInfo(b.visitInfo, privs, withGrant, err)`.
    pub(crate) fn append_dynamic_visit(
        &mut self,
        privs: &[&str],
        with_grant: bool,
        error: VisitAuthError,
    ) {
        self.visit_info.push(VisitInfo {
            privilege: tidb_mysql::ExtendedPriv,
            database: String::new(),
            table: String::new(),
            column: String::new(),
            error,
            dynamic_privs: privs.iter().map(|privilege| (*privilege).to_owned()).collect(),
            dynamic_with_grant: with_grant,
        });
    }

    /// Go `buildShow`: a `LogicalShow` with `buildShowSchema`'s columns, under
    /// a selection for LIKE (unless an extractor claimed it) and WHERE, a
    /// limit, a projection when anything was stacked, and a sort for SHOW
    /// VARIABLES / STATUS. `show` is the preprocessed statement
    /// ([`crate::preprocess::resolve_show_stmt`]).
    ///
    /// # Errors
    ///
    /// `ErrNoDB` for SHOW TABLES / TABLE STATUS without a database, the
    /// missing table of SHOW TABLE REGIONS / DISTRIBUTIONS, the temporary-table
    /// and not-yet-supported refusals, and any LIKE / WHERE build error.
    pub fn build_show(&mut self, show: &ShowStmt) -> Result<Plan, PlanError> {
        use ShowStmtType as T;
        let mut p = LogicalShow::new(self.base(LogicalShow::TYPE), show_contents(show));
        let mut is_view = false;
        let mut is_sequence = false;
        // Go: "It depends on ShowPredicateExtractor now".
        let mut build_pattern = true;
        let (schema_l, table_l) = table_names(show);

        match show.tp {
            T::Databases | T::Variables | T::Tables | T::Columns | T::TableStatus | T::Collation
            | T::Affinity => {
                if matches!(show.tp, T::Tables | T::TableStatus) && show.db_name.is_empty() {
                    return Err(PlanError::from(
                        tidb_plannererrors::ERR_NO_DB.generate_with_stack_by_args(&[]),
                    ));
                }
                if let Some(extractor) = ShowBaseExtractor::extract(show) {
                    p.extractor = Some(Box::new(extractor));
                    build_pattern = false;
                }
            }
            T::CreateTable | T::CreateSequence | T::PlacementForTable | T::PlacementForPartition => {
                // Go `b.is.TableByName` over the session's infoschema, which
                // holds views and sequences beside tables (this source keeps
                // them apart) and in which a local temporary table hides a
                // persistent object of its name.
                let mut is_temp_table_local = false;
                if let Some(table) = self.source.find_table(&schema_l, &table_l) {
                    if let Some(info) = &table.table_info {
                        let info = info.read();
                        is_view = info.is_view();
                        is_sequence = info.is_sequence();
                        is_temp_table_local = info.temp_table_type == TempTableType::LOCAL;
                    }
                } else if self.source.find_view(&schema_l, &table_l).is_some() {
                    is_view = true;
                } else if self.source.find_sequence(&schema_l, &table_l).is_some() {
                    is_sequence = true;
                }
                if is_view {
                    self.append_show_visit(
                        tidb_mysql::ShowViewPriv,
                        &schema_l,
                        &table_l,
                        VisitAuthError::TableAccessDenied {
                            command: "SHOW VIEW",
                            table: table_l.clone(),
                        },
                    );
                } else {
                    let privilege = if is_temp_table_local {
                        tidb_mysql::AllPrivMask
                    } else {
                        tidb_mysql::PrivilegeType(tidb_mysql::AllPrivMask.0 & !tidb_mysql::CreateTMPTablePriv.0)
                    };
                    self.append_show_visit(
                        privilege,
                        &schema_l,
                        &table_l,
                        VisitAuthError::TableAccessDenied {
                            command: "SHOW",
                            table: table_l.clone(),
                        },
                    );
                }
            }
            T::Config => self.append_show_visit(
                tidb_mysql::ConfigPriv,
                "",
                "",
                VisitAuthError::SpecificAccessDenied { privilege: "CONFIG" },
            ),
            T::CreateView => {
                self.append_show_visit(
                    tidb_mysql::SelectPriv,
                    &schema_l,
                    &table_l,
                    VisitAuthError::TableAccessDenied {
                        command: "SELECT",
                        table: table_l.clone(),
                    },
                );
                self.append_show_visit(
                    tidb_mysql::ShowViewPriv,
                    &schema_l,
                    &table_l,
                    VisitAuthError::TableAccessDenied {
                        command: "SHOW VIEW",
                        table: table_l.clone(),
                    },
                );
            }
            T::Backups => self.append_dynamic_visit(
                &["BACKUP_ADMIN"],
                false,
                VisitAuthError::SpecificAccessDenied {
                    privilege: "SUPER or BACKUP_ADMIN",
                },
            ),
            T::Restores => self.append_dynamic_visit(
                &["RESTORE_ADMIN"],
                false,
                VisitAuthError::SpecificAccessDenied {
                    privilege: "SUPER or RESTORE_ADMIN",
                },
            ),
            T::TableNextRowId => {
                let (schema, names) = build_show_next_row_id();
                self.append_show_visit(
                    tidb_mysql::SelectPriv,
                    &schema_l,
                    &table_l,
                    VisitAuthError::PrivilegeCheckFail,
                );
                return Ok(Plan::ShowNextRowID(ShowNextRowID {
                    table_name: show.table.clone().unwrap_or_default(),
                    schema,
                    names,
                }));
            }
            T::StatsExtended | T::StatsHealthy | T::StatsTopN | T::HistogramsInFlight
            | T::ColumnStatsUsage => {
                self.append_show_visit(
                    tidb_mysql::SelectPriv,
                    "mysql",
                    "",
                    VisitAuthError::DbAccessDenied {
                        database: "mysql".to_owned(),
                    },
                );
                if show.tp == T::StatsHealthy {
                    if let Some(extractor) = ShowBaseExtractor::extract(show) {
                        p.extractor = Some(Box::new(extractor));
                        build_pattern = false;
                    }
                }
            }
            T::StatsBuckets | T::StatsHistograms | T::StatsMeta | T::StatsLocked => {
                self.append_show_visit(
                    tidb_mysql::SelectPriv,
                    &schema_l,
                    &table_l,
                    VisitAuthError::TableAccessDenied {
                        command: "SHOW",
                        table: table_l.clone(),
                    },
                );
                if show.tp == T::StatsMeta {
                    if let Some(extractor) = ShowBaseExtractor::extract(show) {
                        p.extractor = Some(Box::new(extractor));
                        build_pattern = false;
                    }
                }
            }
            T::Regions | T::Distributions => {
                let Some(table) = self.source.find_table(&schema_l, &table_l) else {
                    let (schema, name) = match show.table.as_deref() {
                        Some([schema, name]) => (schema.as_str(), name.as_str()),
                        _ => ("", ""),
                    };
                    return Err(PlanError::from(
                        tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                            .fast_generate_by_args(&[schema.into(), name.into()]),
                    ));
                };
                let is_temporary = table
                    .table_info
                    .as_ref()
                    .is_some_and(|info| info.read().temp_table_type != TempTableType::NONE);
                if is_temporary {
                    return Err(PlanError::from(
                        tidb_plannererrors::ERR_OPT_ON_TEMPORARY_TABLE
                            .generate_with_stack_by_args(&[(if show.tp == T::Regions {
                                "show table regions"
                            } else {
                                "show table distributions"
                            })
                            .into()]),
                    ));
                }
            }
            T::ReplicaStatus => {
                return Err(PlanError::from(
                    tidb_plannererrors::ERR_NOT_SUPPORTED_YET
                        .generate_with_stack_by_args(&["SHOW {REPLICA | SLAVE} STATUS".into()]),
                ));
            }
            _ => {}
        }

        let (mut schema, names) = build_show_schema(show, is_view, is_sequence);
        for column in &mut schema.columns {
            column.unique_id = self.column_ids.alloc();
        }
        let first_name = names.first().map(|name| name.names.column.original.clone());
        p.base.base.set_schema(Some(schema));
        p.base.base.set_output_names(names);
        let mut np = LogicalPlan::Show(p);
        let mut stacked = false;
        // Go: "If we have ShowPredicateExtractor, we do not buildSelection
        // with Pattern"; the pattern tests the first output column.
        if let (Some(pattern), true, Some(first_name)) = (&show.pattern, build_pattern, first_name) {
            let like = Expr::Like {
                expr: Box::new(Expr::Column(vec![first_name])),
                pattern: Box::new(pattern.clone()),
                not: false,
                ilike: false,
                escape: None,
            };
            np = self.build_selection(np, &like, &BTreeMap::new())?;
            stacked = true;
        }
        if let Some(r#where) = &show.r#where {
            np = self.build_selection(np, r#where, &BTreeMap::new())?;
            stacked = true;
        }
        if let Some(limit) = &show.limit {
            np = self.build_limit(np, limit)?;
            stacked = true;
        }
        if stacked {
            self.opt_flag |= flags::ELIMINATE_PROJECTION;
            np = self.project_show_columns(np)?;
        }
        if matches!(show.tp, T::Variables | T::Status) {
            let order_by = np
                .schema()
                .and_then(|schema| schema.columns.first().cloned())
                .ok_or_else(|| PlanError::internal("SHOW VARIABLES / STATUS without a column"))?;
            let mut sort = LogicalSort::new(
                self.base(LogicalSort::TYPE),
                vec![ByItems::new(Expression::Column(order_by), false)],
            );
            sort.base.set_children(vec![np]);
            np = LogicalPlan::Sort(sort);
        }
        Ok(Plan::Logical(np))
    }

    /// Go `buildAdmin`'s `AdminShowDDLJobs` arm: a `LogicalShowDDLJobs` with
    /// `buildShowDDLJobsFields`' columns under a selection for WHERE.
    ///
    /// # Errors
    ///
    /// The WHERE's build error.
    pub fn build_admin_show_ddl_jobs(
        &mut self,
        job_number: i64,
        where_clause: Option<&Expr>,
    ) -> Result<LogicalPlan, PlanError> {
        let mut p = LogicalShowDDLJobs::new(self.base(LogicalShowDDLJobs::TYPE), job_number);
        let (mut schema, names) = build_show_ddl_jobs_fields();
        for column in &mut schema.columns {
            column.unique_id = self.column_ids.alloc();
        }
        p.base.base.set_schema(Some(schema));
        p.base.base.set_output_names(names);
        let plan = LogicalPlan::ShowDDLJobs(p);
        match where_clause {
            Some(where_clause) => self.build_selection(plan, where_clause, &BTreeMap::new()),
            None => Ok(plan),
        }
    }

    /// Go `buildShow`'s closing projection: every column of the stacked plan,
    /// re-exported under a fresh unique id with the plan's output names.
    fn project_show_columns(&mut self, plan: LogicalPlan) -> Result<LogicalPlan, PlanError> {
        let columns = plan
            .schema()
            .map(|schema| schema.columns.clone())
            .ok_or_else(|| PlanError::internal("a SHOW plan without a schema"))?;
        let names = plan.output_names().to_vec();
        let exprs = columns.iter().cloned().map(Expression::Column).collect();
        let mut projected = columns;
        for column in &mut projected {
            column.unique_id = self.column_ids.alloc();
        }
        let mut projection = LogicalProjection::new(self.base(LogicalProjection::TYPE), exprs);
        projection.base.set_children(vec![plan]);
        projection.base.base.set_schema(Some(Schema::new(projected)));
        projection.base.base.set_output_names(names);
        Ok(LogicalPlan::Projection(projection))
    }
}
