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

//! Go's identifier-dispatched `SHOW ENGINES` parser leaf.

use tidb_ast::{ShowEnginesFilter, ShowEnginesStmt};

use crate::{prec, PResult, Parser};

/// Parses `SHOW ENGINES` and the shared Go `LIKE`/`WHERE` suffix.
pub(super) fn parse(parser: &mut Parser) -> PResult<Option<ShowEnginesStmt>> {
    if !parser.is_kw("ENGINES") {
        return Ok(None);
    }
    parser.bump();
    let filter = if parser.is_kw("LIKE") {
        parser.bump();
        Some(ShowEnginesFilter::Like(parser.parse_expr(prec::UNARY)?))
    } else if parser.is_kw("WHERE") {
        parser.bump();
        Some(ShowEnginesFilter::Where(parser.parse_expr(prec::NONE)?))
    } else {
        None
    };
    Ok(Some(ShowEnginesStmt { filter }))
}
