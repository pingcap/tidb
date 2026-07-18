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

//! `SHOW OPEN TABLES` branch from Go's `parseShowStmt`.

use tidb_ast::ShowOpenTablesStmt;

use crate::{PResult, Parser};

/// Claims only `SHOW OPEN TABLES [IN | FROM schema]`. The source branch also
/// shares LIKE/WHERE handling; that unported filter payload is deliberately
/// left outside this schema-only vertical slice.
pub(crate) fn parse(parser: &mut Parser) -> PResult<Option<ShowOpenTablesStmt>> {
    if !parser.is_kw("OPEN") {
        return Ok(None);
    }
    parser.expect_kw("OPEN")?;
    parser.expect_kw("TABLES")?;
    let database = if parser.is_kw("IN") || parser.is_kw("FROM") {
        parser.bump();
        Some(parser.parse_name()?)
    } else {
        None
    };
    Ok(Some(ShowOpenTablesStmt { database }))
}
