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

//! The parsed statement retained by PREPARE and its execute-time bound clone.
//!
//! Go stores this as `PlanCacheStmt.PreparedAst`. The retained point-get and
//! general SELECT descriptors retain syntax and key metadata; the session
//! owns cached physical plans and rebuilds execution state for every EXECUTE.

#[allow(unused_imports)]
use super::*;
use crate::compiler::PlanCacheableChecker;
use std::sync::Arc;

use tidb_ast::Stmt;
use tidb_datatype::Datum;
use crate::{
    DriverError, PlanCacheStmt,
};


/// One statement parsed under the SQL mode in force at PREPARE time.
#[derive(Clone, Debug)]
pub struct PreparedAst {
    /// Go `PlanCacheStmt.SchemaVersion`, shared by clones of this prepared definition.
    schema_version: Arc<std::sync::atomic::AtomicU64>,
    sql: String,
    statement: Arc<Stmt>,
    parameter_count: usize,
    /// Go `PlanCacheStmt.VisitInfos`: the table privileges this statement
    /// demands, derived once at PREPARE against the database current then
    /// and checked by every EXECUTE (`checkPreparedPriv`).
    privilege_requests: Arc<[crate::table_privilege::TablePrivilegeRequest]>,
    dml_plan: Option<Arc<PlanCacheStmt>>,
    select_plan: Option<Arc<PlanCacheStmt>>,
    /// Go `PlanCacheStmt.StmtCacheable`: whether PREPARE found the statement
    /// cacheable; the cached plans above exist only while it holds.
    stmt_cacheable: bool,
}

pub use tidb_planner::plan_cache_utils::PreparedPlanCacheEnvironmentCache;

impl PreparedAst {
    pub fn from_parsed(
        schema_version: u64,
        sql: String,
        statement: Stmt,
        parameter_count: usize,
        privilege_requests: Vec<crate::table_privilege::TablePrivilegeRequest>,
        dml_plan: Option<PlanCacheStmt>,
        select_plan: Option<PlanCacheStmt>,
        stmt_cacheable: bool,
    ) -> Self {
        Self {
            schema_version: Arc::new(std::sync::atomic::AtomicU64::new(schema_version)),
            sql,
            statement: Arc::new(statement),
            parameter_count,
            privilege_requests: privilege_requests.into(),
            dml_plan: dml_plan.map(Arc::new),
            select_plan: select_plan.map(Arc::new),
            stmt_cacheable,
        }
    }

    /// Go `PlanCacheStmt.StmtCacheable`.
    #[must_use]
    pub fn stmt_cacheable(&self) -> bool {
        self.stmt_cacheable
    }

    /// Go `PlanCacheStmt.StmtCacheable = cacheable`: an uncacheable statement
    /// plans every EXECUTE afresh, so its cached plans go.
    pub fn set_stmt_cacheable(&mut self, cacheable: bool) {
        self.stmt_cacheable = cacheable;
        if !cacheable {
            self.dml_plan = None;
            self.select_plan = None;
        }
    }

    /// Go `planCachePreprocess`: table lookup failure after schema change is ErrSchemaChanged.
    pub fn schema_change_error(&self, current_version: u64, error: DriverError) -> DriverError {
        let inner = error.clone().to_mysql_error();
        if self
            .schema_version
            .load(std::sync::atomic::Ordering::Relaxed)
            != current_version
            && inner.identity
                == Some(tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS.identity())
        {
            return DriverError::from(tidb_plannererrors::ERR_SCHEMA_CHANGED.gen_with_stack(
                format!("Schema change caused error: {}", inner.error_string()),
            ));
        }
        error
    }

    /// Go `planCachePreprocess`: retain the schema version after successful preprocessing.
    pub fn set_schema_version(&self, version: u64) {
        self.schema_version
            .store(version, std::sync::atomic::Ordering::Relaxed);
    }

    /// The privilege requests PREPARE derived (Go `PlanCacheStmt.VisitInfos`).
    pub fn privilege_requests(&self) -> &[crate::table_privilege::TablePrivilegeRequest] {
        &self.privilege_requests
    }

    /// The original statement text retained for process metadata and routing.
    #[must_use]
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// The number of execute-time values this statement requires.
    #[must_use]
    pub const fn parameter_count(&self) -> usize {
        self.parameter_count
    }

    /// The statement parsed under PREPARE-time SQL semantics.
    #[must_use]
    pub fn statement(&self) -> &Stmt {
        &self.statement
    }


    /// The immutable DML plan compiled while the statement was prepared.
    #[must_use]
    pub fn dml_plan(&self) -> Option<Arc<PlanCacheStmt>> {
        self.dml_plan.clone()
    }

    /// The prepared SELECT descriptor whose full physical tree is generated
    /// on the first EXECUTE and rebuilt on later cache hits.
    #[must_use]
    pub fn select_plan(&self) -> Option<Arc<PlanCacheStmt>> {
        self.select_plan.clone()
    }

    /// Clones the retained tree and installs this execution's values on its
    /// parameter markers, matching Go's immutable prepared definition plus
    /// per-execution marker state.
    pub fn bind(&self, values: &[Datum]) -> Result<Stmt, DriverError> {
        crate::bind_prepared_statement(&self.statement, values)
    }
}

pub use tidb_planner::plan_cache::GenerateNewPlanError;


/// Go `GetPlanFromPlanCache` over the session's prepared statements.
pub trait PreparedAstCompile: super::CompilerSession {
    /// Go `TblInfo2UnionScan`'s dirty tables
    /// ([`tidb_planner::plan_cache_utils::plan_cache_dirty_tables`]).
    fn plan_cache_dirty_tables(&self) -> Vec<i64> {
        tidb_planner::plan_cache_utils::plan_cache_dirty_tables(self)
    }

    /// Go `NewPlanCacheKey`'s session facts
    /// ([`tidb_planner::plan_cache_utils::prepared_plan_cache_environment`]).
    fn prepared_plan_cache_environment(&self) -> Option<Arc<crate::PreparedPlanCacheEnvironment>> {
        tidb_planner::plan_cache_utils::prepared_plan_cache_environment(self)
    }

    /// [`Self::prepared_plan_cache_environment`] with the matched binding's
    /// SQL in the key.
    fn prepared_plan_cache_environment_for_binding(
        &self,
        binding_sql: Option<&str>,
    ) -> Option<crate::PreparedPlanCacheEnvironment> {
        tidb_planner::plan_cache_utils::prepared_plan_cache_environment_for_binding(self, binding_sql)
    }

    /// Go `GeneratePlanCacheStmtWithAST`'s refusal note for a protocol-level
    /// prepare: a statement the plan cache will not hold is answered with
    /// `skip prepared plan-cache: <reason>`, as `PREPARE ... FROM` is.
    fn note_prepared_plan_cache_refusal(&mut self, statement: &Stmt) -> Result<(), DriverError> {
        let refusal = {
            let catalog = self.lock_catalog()?;
            let mut candidate = statement.clone();
            self.prepared_statement_cacheable(&mut candidate, &catalog).err()
        };
        if let Some(reason) = refusal {
            self.append_sql_warning(tidb_context::SqlWarn::with_code(tidb_context::WarningLevel::Warning, 1105,
                format!("skip prepared plan-cache: {reason}")),
            );
        }
        Ok(())
    }

    /// Parses and retains the statement under this session's current SQL mode.
    fn prepare_ast(&mut self, sql: &str) -> Result<PreparedAst, DriverError> {
        let statement = self.parse_statement(sql)?;
        self.prepare_ast_parsed(sql, statement)
    }

    /// [`Self::prepare_ast`] over the statement the front end already parsed
    /// from `sql` (Go `PrepareExec` works on the node `ParseSQL` produced).
    fn prepare_ast_parsed(
        &self,
        sql: &str,
        mut statement: Stmt,
    ) -> Result<PreparedAst, DriverError> {
        // Go `GeneratePlanCacheStmtWithAST` preprocesses the node first
        // (`handleTableName` pins every unqualified name to the current
        // database), then collects the `VisitInfos` from it -- so the retained
        // statement, its requests and every EXECUTE name the same tables.
        tidb_planner::preprocess::preprocess(
            &mut statement,
            self.preprocess_context(),
        )
        .map_err(crate::driver::planner_error_to_driver)?;
        tidb_planner::preprocess::pin_current_database(&mut statement, self.session_vars().current_db.as_str())?;
        let parameter_count = crate::parsed_parameter_count(&statement);
        let privilege_requests = self.collect_table_privileges(&statement, true)?;
        let planner_context = self.statement_context_for_stmt(&statement, false);
        let (dml_plan, select_plan, cacheable) = {
            let catalog = self.lock_catalog()?;
            let cacheable = {
                let mut candidate = statement.clone();
                self.prepared_statement_cacheable(&mut candidate, &catalog)
                    .is_ok()
            };
            (
                if cacheable {
                    crate::build_prepared_dml_plan(
                        &statement,
                        parameter_count,
                        &catalog,
                        self.session_vars().current_db.as_str(),
                    )?
                    .map(|plan| plan.with_sql(sql))
                } else {
                    None
                },
                cacheable
                    .then(|| {
                        crate::build_prepared_select_plan(
                            &statement,
                            parameter_count,
                            &catalog,
                            self.session_vars().current_db.as_str(),
                            &planner_context,
                        )
                        .map(|plan| plan.with_sql(sql))
                    })
                    .flatten(),
                cacheable,
            )
        };
        // Go `GeneratePlanCacheStmtWithAST` ends with `checkPreparedPriv`:
        // PREPARE itself is refused without the statement's privileges.
        self.check_table_privilege_requests(&privilege_requests)?;
        Ok(PreparedAst::from_parsed(
            self.lock_catalog()?.schema_version(),
            sql.to_owned(),
            statement,
            parameter_count,
            privilege_requests,
            dml_plan,
            select_plan,
            cacheable,
        ))
    }






}

impl<T: super::CompilerSession> PreparedAstCompile for T {}
