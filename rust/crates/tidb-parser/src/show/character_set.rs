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

//! Go's `SHOW CHARACTER SET` / `SHOW CHARSET` parser leaf.

use tidb_ast::{ShowCharsetFilter, ShowCharsetStmt};

use crate::{prec, PResult, Parser};

/// Parses only Go's `CharsetKw` production:
/// `CHARACTER SET`, `CHAR SET`, or the identifier alias `CHARSET`.
pub(super) fn parse(parser: &mut Parser) -> PResult<Option<ShowCharsetStmt>> {
    if parser.is_kw("CHARSET") {
        parser.bump();
    } else if parser.is_kw("CHARACTER") || parser.is_kw("CHAR") {
        parser.bump();
        parser.expect_kw("SET")?;
    } else {
        return Ok(None);
    }

    let filter = if parser.is_kw("LIKE") {
        parser.bump();
        Some(ShowCharsetFilter::Like(parser.parse_expr(prec::UNARY)?))
    } else if parser.is_kw("WHERE") {
        parser.bump();
        Some(ShowCharsetFilter::Where(parser.parse_expr(prec::NONE)?))
    } else {
        None
    };
    Ok(Some(ShowCharsetStmt { filter }))
}
