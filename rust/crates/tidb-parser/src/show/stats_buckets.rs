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

//! Go's identifier-dispatched `SHOW STATS_BUCKETS` parser leaf.

use tidb_ast::{ShowStatsBucketsFilter, ShowStatsBucketsStmt};

use crate::{prec, PResult, Parser};

/// Parses only the `STATS_BUCKETS` entry from Go's `showSimpleTypes` map.
pub(super) fn parse(parser: &mut Parser) -> PResult<Option<ShowStatsBucketsStmt>> {
    if !parser.peek().text.eq_ignore_ascii_case("STATS_BUCKETS") {
        return Ok(None);
    }
    parser.bump();
    let filter = if parser.is_kw("LIKE") {
        parser.bump();
        Some(ShowStatsBucketsFilter::Like(
            parser.parse_expr(prec::UNARY)?,
        ))
    } else if parser.is_kw("WHERE") {
        parser.bump();
        Some(ShowStatsBucketsFilter::Where(
            parser.parse_expr(prec::NONE)?,
        ))
    } else {
        None
    };
    Ok(Some(ShowStatsBucketsStmt { filter }))
}
