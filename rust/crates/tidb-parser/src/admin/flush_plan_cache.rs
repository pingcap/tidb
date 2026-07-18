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

/// Claims only the SESSION and GLOBAL forms assigned to this source vertical.
/// Go also accepts a default SESSION spelling and INSTANCE scope; those are
/// intentionally separate parser leaves and remain unclaimed here.
pub(crate) fn parse(parser: &mut Parser) -> PResult<Option<AdminPlanCacheScope>> {
    if !parser.is_kw("ADMIN") || !parser.is_kw_at(1, "FLUSH") {
        return Ok(None);
    }
    let scope = if parser.is_kw_at(2, "SESSION") && parser.is_kw_at(3, "PLAN_CACHE") {
        AdminPlanCacheScope::Session
    } else if parser.is_kw_at(2, "GLOBAL") && parser.is_kw_at(3, "PLAN_CACHE") {
        AdminPlanCacheScope::Global
    } else {
        return Ok(None);
    };
    parser.expect_kw("ADMIN")?;
    parser.expect_kw("FLUSH")?;
    parser.bump(); // SESSION or GLOBAL, fixed by the discriminator above.
    parser.expect_kw("PLAN_CACHE")?;
    Ok(Some(scope))
}
