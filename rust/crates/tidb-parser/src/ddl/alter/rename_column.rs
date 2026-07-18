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

//! Go `HandParser.parseAlterRename`'s column branch.

use tidb_ast::{AlterTableAction, RenameColumn};

use crate::{PResult, Parser};

/// Parses only `RENAME COLUMN old TO new`.
pub(crate) fn parse(parser: &mut Parser) -> PResult<Option<AlterTableAction>> {
    if !(parser.is_kw("RENAME") && parser.is_kw_at(1, "COLUMN")) {
        return Ok(None);
    }
    parser.bump();
    parser.bump();
    let from = parser.parse_name_or_keyword()?;
    parser.expect_kw("TO")?;
    let to = parser.parse_name_or_keyword()?;
    Ok(Some(AlterTableAction::RenameColumn(RenameColumn {
        from,
        to,
    })))
}
