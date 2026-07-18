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

//! Go `HandParser.parseAlterAlter`'s CHECK-enforcement branch.

use tidb_ast::{AlterCheck, AlterTableAction};

use crate::{PResult, Parser};

/// Parses only `ALTER {CHECK|CONSTRAINT} name {ENFORCED|NOT ENFORCED}`.
///
/// `None` leaves ALTER INDEX and ALTER COLUMN DEFAULT to their independent
/// leaves. This function consumes no token until it recognizes the exact
/// CHECK/CONSTRAINT prefix.
pub(crate) fn parse(parser: &mut Parser) -> PResult<Option<AlterTableAction>> {
    if !(parser.is_kw("ALTER") && (parser.is_kw_at(1, "CHECK") || parser.is_kw_at(1, "CONSTRAINT")))
    {
        return Ok(None);
    }
    parser.bump();
    parser.bump();
    // Go's `expectIdentLike` accepts identifier-class keywords such as
    // `unknown` here, so preserve that grammar boundary rather than using
    // the stricter ordinary-name parser.
    let name = parser.parse_name_or_keyword()?;
    let enforced = if parser.is_kw("ENFORCED") {
        parser.bump();
        true
    } else if parser.is_kw("NOT") {
        parser.bump();
        parser.expect_kw("ENFORCED")?;
        false
    } else {
        return Err(parser.err_here("expected ENFORCED or NOT ENFORCED"));
    };
    Ok(Some(AlterTableAction::AlterCheck(AlterCheck {
        name,
        enforced,
    })))
}
