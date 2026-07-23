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

//! Go `HandParser.parseAlterDrop`'s CHECK-constraint branch.

use tidb_ast::{AlterTableAction, DropCheck};

use crate::{PResult, Parser};

/// Parses only `DROP {CHECK|CONSTRAINT} name` after the outer dispatcher
/// consumed `DROP`.
///
/// `None` leaves FOREIGN KEY, INDEX/KEY, COLUMN, PARTITION, and every other
/// `parseAlterDrop` branch to their independent parsers.
pub(crate) fn parse(parser: &mut Parser) -> PResult<Option<AlterTableAction>> {
    if !(parser.is_kw("CHECK") || parser.is_kw("CONSTRAINT")) {
        return Ok(None);
    }
    parser.bump();
    Ok(Some(AlterTableAction::DropCheck(DropCheck {
        name: parser.parse_ident_like_name()?,
    })))
}
