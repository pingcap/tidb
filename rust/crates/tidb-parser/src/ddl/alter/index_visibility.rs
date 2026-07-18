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

//! Go `HandParser.parseAlterAlter`'s index-visibility branch.

use tidb_ast::{AlterIndexVisibility, AlterTableAction, IndexVisibility};

use crate::{PResult, Parser};

/// Parses only `ALTER INDEX name {VISIBLE|INVISIBLE}`.
///
/// `None` does not consume input, leaving the other `parseAlterAlter`
/// branches (`CHECK`, `CONSTRAINT`, and column defaults) owned by their
/// future typed leaves rather than accidentally accepting their prefixes.
pub(crate) fn parse(parser: &mut Parser) -> PResult<Option<AlterTableAction>> {
    if !(parser.is_kw("ALTER") && parser.is_kw_at(1, "INDEX")) {
        return Ok(None);
    }
    parser.bump();
    parser.bump();
    let name = parser.parse_name()?;
    let visibility = if parser.is_kw("VISIBLE") {
        parser.bump();
        IndexVisibility::Visible
    } else if parser.is_kw("INVISIBLE") {
        parser.bump();
        IndexVisibility::Invisible
    } else {
        return Err(parser.err_here("expected VISIBLE or INVISIBLE"));
    };
    Ok(Some(AlterTableAction::AlterIndexVisibility(
        AlterIndexVisibility { name, visibility },
    )))
}
