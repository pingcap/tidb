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

//! Go `HandParser.parseAlterDrop`'s payload-free primary-key branch.

use tidb_ast::{AlterTableAction, DropPrimaryKey};

use crate::{PResult, Parser};

/// Parses only `DROP PRIMARY KEY` after the outer dispatcher consumed `DROP`.
///
/// This branch must precede generic `DROP {INDEX|KEY}` and column handling:
/// Go gives primary-key removal a distinct `AlterTableDropPrimaryKey` action.
pub(crate) fn parse(parser: &mut Parser) -> PResult<Option<AlterTableAction>> {
    if !parser.is_kw("PRIMARY") {
        return Ok(None);
    }
    parser.bump();
    parser.expect_kw("KEY")?;
    Ok(Some(AlterTableAction::DropPrimaryKey(DropPrimaryKey)))
}
