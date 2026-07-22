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

//! Scoped `ADMIN FLUSH PLAN_CACHE` grammar from Go's `parseAdminStmt`.

use tidb_ast::AdminPlanCacheScope;

use crate::{PResult, Parser};

/// Parses every Go plan-cache scope, including the default SESSION spelling.
pub(crate) fn parse(parser: &mut Parser) -> PResult<Option<AdminPlanCacheScope>> {
    if !parser.is_kw("ADMIN") || !parser.is_kw_at(1, "FLUSH") {
        return Ok(None);
    }
    let (scope, scoped) = if parser.is_kw_at(2, "PLAN_CACHE") {
        (AdminPlanCacheScope::Session, false)
    } else if parser.is_kw_at(2, "SESSION") && parser.is_kw_at(3, "PLAN_CACHE") {
        (AdminPlanCacheScope::Session, true)
    } else if parser.is_kw_at(2, "INSTANCE") && parser.is_kw_at(3, "PLAN_CACHE") {
        (AdminPlanCacheScope::Instance, true)
    } else if parser.is_kw_at(2, "GLOBAL") && parser.is_kw_at(3, "PLAN_CACHE") {
        (AdminPlanCacheScope::Global, true)
    } else {
        return Ok(None);
    };
    parser.expect_kw("ADMIN")?;
    parser.expect_kw("FLUSH")?;
    if scoped {
        parser.bump();
    }
    parser.expect_kw("PLAN_CACHE")?;
    Ok(Some(scope))
}
