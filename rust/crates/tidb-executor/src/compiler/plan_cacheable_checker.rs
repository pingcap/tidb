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

use tidb_infoschema::SessionExtendedInfoSchema;

/// Go `IsASTCacheable` with the session's variables.
pub trait PlanCacheableChecker: super::CompilerSession {
    /// Go `IsASTCacheable` with this session's variables and current
    /// infoschema. This is the single PREPARE-time admission authority for
    /// SQL and binary-protocol prepared statements.
    fn prepared_statement_cacheable(
        &self,
        stmt: &mut tidb_ast::Stmt,
        catalog: &SessionExtendedInfoSchema,
    ) -> Result<(), String> {
        tidb_planner::plan_cacheable_checker::prepared_statement_cacheable(self, stmt, catalog)
    }
}

impl<T: super::CompilerSession> PlanCacheableChecker for T {}
