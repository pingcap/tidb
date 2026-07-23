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

use tidb_ast::{AlterTableAction, AlterTableRemoveTtl};

use crate::{PResult, Parser};

/// Parses only the alter-only `REMOVE TTL` branch. Ordinary TTL options use
/// the same table-option loop as CREATE TABLE and every adjacent ALTER option.
pub(crate) fn parse(parser: &mut Parser) -> PResult<Option<AlterTableAction>> {
    if !(parser.is_kw("REMOVE") && parser.is_kw_at(1, "TTL")) {
        return Ok(None);
    }
    parser.bump();
    parser.bump();
    Ok(Some(AlterTableAction::RemoveTtl(AlterTableRemoveTtl)))
}
