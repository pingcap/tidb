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
//! Go stores this as `PlanCacheStmt.PreparedAst`. The retained point-get plan
//! is immutable; execute-time handles and mutable executor state are rebuilt
//! for every cache hit.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tidb_ast::Stmt;
use tidb_datatype::Datum;
use tidb_executor::{DriverError, PreparedPointGetExecution, PreparedPointGetPlan};

use tidb_executor::access_path::StatementReadShape;

use crate::{Session, StmtKind, StoredStateChange};

/// One statement parsed under the SQL mode in force at PREPARE time.
#[derive(Clone, Debug)]
pub struct PreparedAst {
    sql: String,
    statement: Stmt,
    parameter_count: usize,
    point_get_plan: Option<Arc<PreparedPointGetPlan>>,
    point_get_cache_ready: Arc<AtomicBool>,
}

impl PreparedAst {
    pub(crate) fn from_parsed(
        sql: String,
        statement: Stmt,
        parameter_count: usize,
        point_get_plan: Option<PreparedPointGetPlan>,
    ) -> Self {
        Self {
            sql,
            statement,
            parameter_count,
            point_get_plan: point_get_plan.map(Arc::new),
            point_get_cache_ready: Arc::new(AtomicBool::new(false)),
        }
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

    /// The protocol answer shape determined by this statement's parsed form.
    #[must_use]
    pub fn statement_kind(&self, session: &Session) -> StmtKind {
        session.statement_kind_parsed(&self.statement)
    }

    /// The immutable fast point-read plan compiled while the statement was
    /// prepared, when its shape is safe to reuse for every EXECUTE.
    #[must_use]
    pub fn point_get_plan(&self) -> Option<Arc<PreparedPointGetPlan>> {
        self.point_get_plan.clone()
    }

    /// The persistent cluster state this statement changes, if any.
    #[must_use]
    pub fn stored_state_change(&self) -> StoredStateChange {
        Session::stored_state_change_parsed(&self.statement)
    }

    /// Clones the retained tree and installs this execution's values on its
    /// parameter markers, matching Go's immutable prepared definition plus
    /// per-execution marker state.
    pub fn bind(
        &self,
        values: &[Datum],
        zone: &tidb_datatype::SessionTimeZone,
    ) -> Result<BoundPreparedAst, DriverError> {
        let statement = tidb_executor::bind_prepared_statement(&self.statement, values)?;
        let point_get = self
            .point_get_plan
            .as_ref()
            .and_then(|plan| plan.bind(values, zone));
        let point_get_cache_hit = self.point_get_cache_ready.load(Ordering::Acquire);
        let execution_sql = if matches!(statement, Stmt::Query(_)) {
            self.sql.clone()
        } else {
            statement.restore()
        };
        Ok(BoundPreparedAst {
            execution_sql,
            statement: Some(statement),
            point_get,
            point_get_cache_hit,
            point_get_cache_ready: Arc::clone(&self.point_get_cache_ready),
            use_cached_point_get: false,
        })
    }

    /// Binds one EXECUTE, taking Go's cached PointGet door before cloning the
    /// complete prepared AST. Every state that can change the plan declines to
    /// [`Self::bind`], so the ordinary path remains the correctness fallback.
    pub fn bind_for_execution(
        &self,
        session: &Session,
        values: &[Datum],
    ) -> Result<BoundPreparedAst, DriverError> {
        if values.len() != self.parameter_count {
            return Err(DriverError::WrongParamCount);
        }
        let zone = session.session_time_zone();
        if self.point_get_cache_ready.load(Ordering::Acquire) {
            if let Some(execution) = self
                .point_get_plan
                .as_ref()
                .filter(|plan| session.can_reuse_prepared_point_get(plan))
                .and_then(|plan| plan.bind(values, &zone))
            {
                return Ok(BoundPreparedAst {
                    execution_sql: self.sql.clone(),
                    statement: None,
                    point_get: Some(execution),
                    point_get_cache_hit: true,
                    point_get_cache_ready: Arc::clone(&self.point_get_cache_ready),
                    use_cached_point_get: true,
                });
            }
        }
        self.bind(values, &zone)
    }
}

/// One execution's parameter values installed on a private AST clone.
#[derive(Debug)]
pub struct BoundPreparedAst {
    pub(crate) execution_sql: String,
    pub(crate) statement: Option<Stmt>,
    point_get: Option<PreparedPointGetExecution>,
    point_get_cache_hit: bool,
    point_get_cache_ready: Arc<AtomicBool>,
    use_cached_point_get: bool,
}

impl BoundPreparedAst {
    pub(crate) fn into_parts(
        self,
    ) -> (
        String,
        Option<Stmt>,
        Option<PreparedPointGetExecution>,
        Option<PreparedPointGetExecution>,
        Arc<AtomicBool>,
    ) {
        let cache_candidate = (!self.use_cached_point_get)
            .then(|| self.point_get.clone())
            .flatten();
        let cached = self
            .use_cached_point_get
            .then_some(self.point_get)
            .flatten();
        (
            self.execution_sql,
            self.statement,
            cached,
            cache_candidate,
            self.point_get_cache_ready,
        )
    }

    /// The read policy chosen from the same bound tree execution will plan.
    #[must_use]
    pub fn statement_read_shape(&mut self, session: &Session) -> StatementReadShape {
        if self.use_cached_point_get {
            return StatementReadShape::AutocommitPointGet;
        }
        self.use_cached_point_get = self.point_get_cache_hit
            && self
                .point_get
                .as_ref()
                .is_some_and(|execution| session.can_reuse_prepared_point_get(execution.plan()));
        if self.use_cached_point_get {
            return StatementReadShape::AutocommitPointGet;
        }
        session.statement_read_shape_bound(
            self.statement
                .as_ref()
                .expect("the ordinary prepared path retains its bound AST"),
        )
    }
}

impl Session {
    /// Parses and retains the statement under this session's current SQL mode.
    pub fn prepare_ast(&self, sql: &str) -> Result<PreparedAst, DriverError> {
        let statement = self.parse_statement(sql)?;
        let parameter_count = tidb_executor::parsed_parameter_count(&statement);
        let point_get_plan = {
            let catalog = self.lock_catalog()?;
            tidb_executor::build_prepared_point_get_plan(
                &statement,
                parameter_count,
                &catalog,
                self.current_database(),
                &self.session_time_zone(),
            )
        };
        Ok(PreparedAst::from_parsed(
            sql.to_owned(),
            statement,
            parameter_count,
            point_get_plan,
        ))
    }

    /// Go `IsSafeToReusePointGetExecutor` plus the plan-cache reuse gates of
    /// `GetPlanFromPlanCache`. Every uncertain state declines to ordinary
    /// planning; no cached lookup is allowed to widen into a multi-read plan.
    ///
    /// An EXPLICIT transaction may reuse the cached read too: its statement
    /// snapshot is already bound before execution, so the lookup reads at the
    /// transaction's timestamp through the session's own storage — the same
    /// visibility the ordinary planner would build for it (Go serves these
    /// from its prepared plan cache inside transactions as well).
    pub(crate) fn can_reuse_prepared_point_get(&self, plan: &PreparedPointGetPlan) -> bool {
        if !self.session_bindings.is_empty() {
            if std::env::var("TIDB_RS_TRACE").is_ok_and(|v| v.contains("decline")) {
                eprintln!("[pg-reuse-no] session_bindings");
            }
            return false;
        }
        if self
            .vars
            .optimizer_fix_control()
            .get_bool_with_default(tidb_planner::fix_control::FIX_52592, false)
            || self.vars.get_system("sql_select_limit").as_deref() != Ok("18446744073709551615")
            || self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_SNAPSHOT)
                .is_ok_and(|value| !value.is_empty())
            || self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_READ_STALENESS)
                .is_ok_and(|value| value.trim().parse::<i64>().is_ok_and(|value| value != 0))
        {
            if std::env::var("TIDB_RS_TRACE").is_ok_and(|v| v.contains("decline")) {
                eprintln!("[pg-reuse-no] vars gate");
            }
            return false;
        }
        let matched = self
            .lock_catalog()
            .is_ok_and(|catalog| plan.matches_catalog(&catalog, self.current_database()));
        if !matched && std::env::var("TIDB_RS_TRACE").is_ok_and(|v| v.contains("decline")) {
            eprintln!("[pg-reuse-no] catalog identity moved");
        }
        matched
    }

    /// Binds a retained point-get plan for a binary EXECUTE after applying
    /// the same autocommit, snapshot, session-binding, and schema gates used
    /// by the Go point-get cache.
    pub fn bind_cached_prepared_point_get(
        &self,
        plan: &Arc<PreparedPointGetPlan>,
        values: &[Datum],
    ) -> Option<PreparedPointGetExecution> {
        if !self.can_reuse_prepared_point_get(plan) {
            return None;
        }
        let bound = plan.bind(values, &self.session_time_zone());
        if bound.is_none()
            && std::env::var("TIDB_RS_TRACE").is_ok_and(|v| v.contains("decline"))
        {
            eprintln!("[pg-bind-none] {}.{}", plan.names().0, plan.names().1);
        }
        bound
    }
}
