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

//! Go `HandParser.parseAlterRename`'s index/key branch.

use tidb_ast::{AlterTableAction, RenameIndex};

use crate::{PResult, Parser};

/// Parses only `RENAME {INDEX|KEY} old TO new`.
///
/// It consumes no token for RENAME TABLE or RENAME COLUMN, which remain
/// independent actions in the outer ALTER parser.
pub(crate) fn parse(parser: &mut Parser) -> PResult<Option<AlterTableAction>> {
    if !(parser.is_kw("RENAME") && (parser.is_kw_at(1, "INDEX") || parser.is_kw_at(1, "KEY"))) {
        return Ok(None);
    }
    parser.bump();
    parser.bump();
    let from = parser.parse_ident_like_name()?;
    parser.expect_kw("TO")?;
    let to = parser.parse_ident_like_name()?;
    Ok(Some(AlterTableAction::RenameIndex(RenameIndex {
        from,
        to,
    })))
}
