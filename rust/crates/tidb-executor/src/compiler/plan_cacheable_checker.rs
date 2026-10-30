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

//! Go `IsASTCacheable` as a session asks it: the session's variables over
//! the planner's AST check (`tidb_planner::plan_cacheable_checker`).

use tidb_infoschema::Catalog;
use tidb_planner::plan_cacheable_checker::{stmt_cacheable, CacheableContext, MAX_NUM_PARAM};

/// Go `IsASTCacheable` with the session's variables.
pub trait PlanCacheableChecker: super::CompilerSession {
    /// Go `IsASTCacheable` with this session's variables and current
    /// infoschema. This is the single PREPARE-time admission authority for
    /// SQL and binary-protocol prepared statements.
    fn prepared_statement_cacheable(
        &self,
        stmt: &mut tidb_ast::Stmt,
        catalog: &Catalog,
    ) -> Result<(), String> {
        let configured_max = self
            .session_vars()
            .optimizer_fix_control()
            .get_int_with_default(tidb_fixcontrol::FIX_44823, MAX_NUM_PARAM as i64);
        let max_num_param = match configured_max {
            0 => usize::MAX,
            value if value > 0 => usize::try_from(value).unwrap_or(usize::MAX),
            _ => MAX_NUM_PARAM,
        };
        let verdict = stmt_cacheable(
            stmt,
            CacheableContext {
                catalog,
                current_database: self.session_vars().current_db.as_str(),
                enable_param_limit: self.session_vars().system_bool(
                    tidb_vardef::tidb_vars::TIDB_ENABLE_PLAN_CACHE_FOR_PARAM_LIMIT,
                    tidb_vardef::defaults::DEF_TIDB_ENABLE_PLAN_CACHE_FOR_PARAM_LIMIT,
                ),
                enable_subquery: self.session_vars().system_bool(
                    tidb_vardef::tidb_vars::TIDB_ENABLE_PLAN_CACHE_FOR_SUBQUERY,
                    tidb_vardef::defaults::DEF_TIDB_ENABLE_PLAN_CACHE_FOR_SUBQUERY,
                ),
                dynamic_partition_prune: self
                    .session_vars()
                    .get_system(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)
                    .is_ok_and(|mode| mode.eq_ignore_ascii_case("dynamic")),
                disable_partition_cache: self
                    .session_vars()
                    .optimizer_fix_control()
                    .get_bool_with_default(tidb_fixcontrol::FIX_33031, false),
                enable_generated_columns: self
                    .session_vars()
                    .optimizer_fix_control()
                    .get_bool_with_default(tidb_fixcontrol::FIX_45798, true),
                max_num_param,
            },
        );
        if verdict.is_err()
            && self
                .session_vars()
                .optimizer_fix_control()
                .get_bool_with_default(tidb_fixcontrol::FIX_49736, false)
        {
            Ok(())
        } else {
            verdict
        }
    }
}

impl<T: super::CompilerSession> PlanCacheableChecker for T {}
