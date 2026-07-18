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

//! Go `parseTableOption`'s `ALTER TABLE AUTO_INCREMENT` leaf.

use tidb_ast::{AlterTableAction, TableOption};
use tidb_lexer::TokenKind;

use crate::{PResult, Parser};

/// Parses only `AUTO_INCREMENT [=] intLit`.
///
/// Go's `HandParser.parseAlterTableOptions` delegates this branch to
/// `parseTableOptionUint`, then stores the result in one
/// `AlterTableOption.Options` entry. Keep that shared envelope while making
/// this physical leaf own neither `AUTO_RANDOM_BASE` nor other generic table
/// options.
pub(crate) fn parse(parser: &mut Parser) -> PResult<Option<AlterTableAction>> {
    if !parser.is_kw("AUTO_INCREMENT") {
        return Ok(None);
    }
    parser.bump();
    parser.accept_optional_equals();
    let token = parser.peek().clone();
    if token.kind != TokenKind::IntLit {
        return Err(parser.err_here("expected an integer after AUTO_INCREMENT"));
    }
    parser.bump();
    Ok(Some(AlterTableAction::SetTableOptions {
        options: vec![TableOption::AutoIncrement(token.text)],
    }))
}
