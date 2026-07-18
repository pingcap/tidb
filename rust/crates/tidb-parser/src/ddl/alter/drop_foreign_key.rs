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

//! Go `HandParser.parseAlterDrop`'s foreign-key branch.

use tidb_ast::{AlterTableAction, DropForeignKey};

use crate::{PResult, Parser};

/// Parses only `DROP FOREIGN KEY name` after the outer dispatcher consumed
/// `DROP`.
///
/// `None` leaves CHECK/CONSTRAINT, INDEX/KEY, COLUMN, PARTITION, and every
/// other `parseAlterDrop` branch to their independent parsers. Go's hand
/// parser expects `KEY` followed directly by an identifier-like name; in
/// particular, it does not parse `IF EXISTS` here despite the wider AST field.
pub(crate) fn parse(parser: &mut Parser) -> PResult<Option<AlterTableAction>> {
    if !parser.is_kw("FOREIGN") {
        return Ok(None);
    }
    parser.bump();
    parser.expect_kw("KEY")?;
    Ok(Some(AlterTableAction::DropForeignKey(DropForeignKey {
        name: parser.parse_name_or_keyword()?,
    })))
}
