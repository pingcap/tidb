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

//! Go-shaped ALTER TABLE TTL option and REMOVE TTL grammar.

use tidb_ast::{AlterTableAction, AlterTableRemoveTtl, TableOption};

use crate::{PResult, Parser};

/// Parses the alter-only TTL branch. CREATE TABLE owns the same option payload
/// through its separate option loop.
pub(crate) fn parse(parser: &mut Parser) -> PResult<Option<AlterTableAction>> {
    if parser.is_kw("REMOVE") {
        if !parser.is_kw_at(1, "TTL") {
            return Ok(None);
        }
        parser.bump();
        parser.bump();
        return Ok(Some(AlterTableAction::RemoveTtl(AlterTableRemoveTtl)));
    }
    if !matches!(
        parser.peek().text.to_ascii_uppercase().as_str(),
        "TTL" | "TTL_ENABLE" | "TTL_JOB_INTERVAL"
    ) {
        return Ok(None);
    }
    let mut options = Vec::new();
    while let Some(option) = parser.parse_table_option()? {
        if options.is_empty()
            && !matches!(
                option,
                TableOption::Ttl { .. }
                    | TableOption::TtlEnable(_)
                    | TableOption::TtlJobInterval(_)
            )
        {
            unreachable!("TTL parser prefix selected a non-TTL option");
        }
        options.push(option);
    }
    Ok(Some(AlterTableAction::SetTableOptions { options }))
}
