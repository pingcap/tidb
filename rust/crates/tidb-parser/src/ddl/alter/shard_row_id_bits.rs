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

//! Go `HandParser.parseAlterTableOptions`' `SHARD_ROW_ID_BITS` table-option branch.

use tidb_ast::{AlterTableAction, TableOption};

use crate::{PResult, Parser};

/// Parses only `SHARD_ROW_ID_BITS [=] integer`.
///
/// Go stores this generic table option in one `AlterTableOption` spec. Keep
/// that existing Rust envelope rather than creating a second ALTER action for
/// the same source AST shape. This leaf consumes nothing unless its exact
/// keyword is present.
pub(crate) fn parse(parser: &mut Parser) -> PResult<Option<AlterTableAction>> {
    if !parser.is_kw("SHARD_ROW_ID_BITS") {
        return Ok(None);
    }
    parser.bump();
    parser.accept_optional_equals();
    let bits = parser.parse_table_option_integer("SHARD_ROW_ID_BITS")?;
    Ok(Some(AlterTableAction::SetTableOptions {
        options: vec![TableOption::ShardRowIdBits(bits)],
    }))
}
