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

//! Go `parseAlterTableOptions` leaves for AUTO_ID_CACHE and AUTO_RANDOM_BASE.

use tidb_ast::{AlterTableAction, TableOption};

use crate::{PResult, Parser};

/// Parses only Go's FORCE modifier for AUTO_INCREMENT/AUTO_RANDOM_BASE.
/// Ordinary auto-ID options flow through the shared table-option loop so they
/// compose with every adjacent option in one Go `AlterTableOption` spec.
pub(crate) fn parse(parser: &mut Parser) -> PResult<Option<AlterTableAction>> {
    if !parser.is_kw("FORCE")
        || (!parser.is_kw_at(1, "AUTO_INCREMENT") && !parser.is_kw_at(1, "AUTO_RANDOM_BASE"))
    {
        return Ok(None);
    }
    parser.bump();

    let option = if parser.is_kw("AUTO_INCREMENT") {
        parser.bump();
        parser.accept_optional_equals();
        let value = parser.parse_table_option_integer("AUTO_INCREMENT")?;
        TableOption::ForceAutoIncrement(value)
    } else if parser.is_kw("AUTO_RANDOM_BASE") {
        parser.bump();
        parser.accept_optional_equals();
        let value = parser.parse_table_option_integer("AUTO_RANDOM_BASE")?;
        TableOption::ForceAutoRandomBase(value)
    } else {
        unreachable!("FORCE selector checked the auto-ID option")
    };

    Ok(Some(AlterTableAction::SetTableOptions {
        options: vec![option],
    }))
}
