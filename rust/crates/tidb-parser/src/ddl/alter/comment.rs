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

//! Go's table-level `ALTER TABLE ... COMMENT [=] <string>` option leaf.

use tidb_ast::{AlterTableAction, TableOption};
use tidb_lexer::TokenKind;

use crate::{decode_string, PResult, Parser};

/// Parses only the `TableOptionComment` branch delegated by Go's
/// `HandParser.parseAlterTableOptions` to `parseTableOption`.
///
/// The shared `SetTableOptions` envelope already mirrors Go's
/// `AlterTableOption.Options`; this leaf deliberately owns no sibling table
/// options.
pub(crate) fn parse(parser: &mut Parser) -> PResult<Option<AlterTableAction>> {
    // COMMENT is identifier-class in this lexer even though Go's hand parser
    // dispatches it as a table-option token, so match its source spelling at
    // this grammar boundary instead of requiring a lexer Keyword token.
    if !parser.peek().text.eq_ignore_ascii_case("COMMENT") {
        return Ok(None);
    }
    parser.bump();
    if parser.is_op("=") {
        parser.bump();
    }
    let token = parser.peek().clone();
    if token.kind != TokenKind::Str {
        return Err(parser.err_here("expected ALTER TABLE COMMENT string literal"));
    }
    parser.bump();
    Ok(Some(AlterTableAction::SetTableOptions {
        options: vec![TableOption::Comment(decode_string(&token.text))],
    }))
}
