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


//! Go `session.ExecutePreparedStmt`: running a prepared statement through
//! the session's statement lifecycle.

use crate::record_set::PendingExecution;
use crate::*;
use tidb_ast::Expr;
use tidb_executor::compiler::PreparedExec;

impl Session {
    /// Go `ExecuteExec`: looks the statement up, binds the `USING` variables'
    /// current values to its markers, and runs the result.
    pub(crate) fn execute_prepared_statement(
        &mut self,
        name: &str,
        using: &[Expr],
    ) -> Result<PendingExecution, DriverError> {
        // Go `planCachePreprocess` step 4: under a new schema version the
        // statement is preprocessed again, and if that fails "the real reason
        // for the error is schema changed" -- `ErrSchemaChanged` over the inner
        // error's `Error()` text, class prefix included. The table-resolution
        // failure is the preprocess failure this tier can see.
        let current = self.with_catalog_mut(|catalog| Ok(catalog.schema_version()))?;
        let stale = self
            .prepared_statements
            .get(name)
            .is_some_and(|prepared| prepared.schema_version != current);
        let result = self.execute_prepared_statement_inner(name, using);
        if !stale {
            return result;
        }
        match result {
            Ok(pending) => {
                if let Some(prepared) = self.prepared_statements.get_mut(name) {
                    prepared.schema_version = current;
                }
                Ok(pending)
            }
            Err(error) => {
                let inner = error.clone().to_mysql_error();
                let table_gone = inner.identity
                    == Some(tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS.identity());
                if table_gone {
                    Err(DriverError::Mysql(tidb_terror::MysqlError::from_terror(
                        &tidb_plannererrors::ERR_SCHEMA_CHANGED.generate_with_stack(
                            format!("Schema change caused error: {}", inner.error_string()),
                        ),
                    )))
                } else {
                    Err(error)
                }
            }
        }
    }

    pub(crate) fn execute_prepared_statement_inner(
        &mut self,
        name: &str,
        using: &[Expr],
    ) -> Result<PendingExecution, DriverError> {
        let prepared = self
            .prepared_statements
            .get(name)
            .cloned()
            .ok_or(DriverError::from(
                tidb_plannererrors::ERR_STMT_NOT_FOUND.generate_with_stack_by_args(&[]),
            ))?;
        // Go `planCachePreprocess` step 1, before any value is evaluated.
        if using.len() != prepared.param_count {
            return Err(DriverError::from(
                tidb_plannererrors::ERR_WRONG_PARAM_COUNT.generate_with_stack_by_args(&[]),
            ));
        }
        // Go `SetParameterValuesIntoSCtx`: each `USING` entry is evaluated to
        // a Datum, and the parameter's TYPE then follows from that value
        // (`InferParamTypeFromDatum`) rather than from anything the PREPARE
        // knew. The parser admits only user variables here, so evaluating one
        // is a lookup in this session's own map.
        let values: Vec<Datum> = using
            .iter()
            .map(|expr| match expr {
                Expr::UserVar(name) => self.using_param_value(name),
                _ => unreachable!("the parser admits only @variables in USING"),
            })
            .collect();
        // Go checks a `LIMIT` parameter's KIND, not just its value, so a
        // string that happens to read as a number is refused as firmly as a
        // negative integer is.
        for &order in &prepared.limit_markers {
            match values.get(order) {
                Some(Datum::Int(value)) if *value >= 0 => {}
                Some(Datum::UInt(_)) => {}
                _ => {
                    return Err(DriverError::from(
                        tidb_plannererrors::ERR_WRONG_ARGUMENTS
                            .generate_with_stack_by_args(&["LIMIT".into()]),
                    ))
                }
            }
        }
        // Go summarizes an EXECUTE under the digest of the statement text as
        // PREPAREd, not under the marker-aliased text the port runs.
        self.retarget_prepared_statement_observation(&prepared.original_sql);
        let (effective_statement, binding_sql) =
            self.prepared_statement_with_binding(&prepared.statement);
        let mut effective_statement = effective_statement.into_owned();
        // EXECUTE and its bound body share one result lifecycle. Keep the
        // binding match while the body applies its own hints and metadata.
        let binding_matched = binding_sql.is_some();
        self.rewrite_fts_for_planning(&mut effective_statement)?;
        // go's plan cache re-checks the stored verdict on every EXECUTE and
        // records the refusal as a statement warning (oracle m21: EXECUTE
        // st3 carries the same skip row as its PREPARE). A cacheable plan
        // whose root is a TableDual is refused with its own reason (oracle
        // m21: EXECUTE st USING @p over `SELECT ? + 1` carries `skip
        // prepared plan-cache: get a TableDual plan`).
        // Reported once the uncached statement's boundary has reset the
        // warnings (`prepare_bound_execution`).
        self.pending_plan_cache_refusal = prepared
            .cacheable
            .is_ok()
            .then(|| self.prepared_plan_cache_refusal(&prepared.statement, &effective_statement))
            .flatten();
        if prepared.cacheable.is_ok()
            && self.prepared_plan_cache_allowed_for_statement(&effective_statement)
        {
            if let Some(cached) = prepared.select_plan.as_ref().and_then(|plan| {
                self.bind_cached_prepared_select_for_statement(
                    plan,
                    &values,
                    &effective_statement,
                    binding_sql.as_deref(),
                )
            }) {
                let result = self.prepare_cached_select_execution(
                    &cached,
                    &prepared.sql,
                    &prepared.privilege_requests,
                );
                if binding_matched {
                    self.vars.found_in_binding = true;
                }
                return result;
            }
            if let Some(cached) = prepared.dml_plan.as_ref().and_then(|plan| {
                self.bind_cached_prepared_dml_for_statement(
                    plan,
                    &values,
                    &effective_statement,
                    binding_sql.as_deref(),
                )
            }) {
                let result = self.execute_cached_prepared_dml(
                    &cached,
                    &prepared.sql,
                    &prepared.privilege_requests,
                );
                if binding_matched {
                    self.vars.found_in_binding = true;
                }
                return result.map(PendingExecution::Complete);
            }
        }
        let bound = tidb_executor::bind_statement(effective_statement, &values)?;
        // Go builds the prepared statement's own plan and runs it as this
        // statement's body, so the inner statement goes through the same
        // dispatch every other statement does -- including DDL's implicit
        // commit, which is why `EXECUTE` of a prepared `CREATE TABLE` works
        // (captured).
        let result =
            self.prepare_bound_execution(&prepared.sql, bound, &prepared.privilege_requests);
        if binding_matched {
            self.vars.found_in_binding = true;
        }
        result
    }
}
