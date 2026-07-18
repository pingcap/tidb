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

//! Go's `ADMIN ALTER DDL JOBS` parser leaf.

use tidb_ast::{AdminAlterDdlJobOption, AdminAlterDdlJobsStmt, Expr, UnaryOp};
use tidb_lexer::TokenKind;

use crate::{prec, PResult, Parser};

/// Parses the source-owned `ADMIN ALTER DDL JOBS` branch.
pub(super) fn parse(parser: &mut Parser) -> PResult<Option<AdminAlterDdlJobsStmt>> {
    if !parser.is_kw_at(1, "ALTER") || !parser.is_kw_at(2, "DDL") || !parser.is_kw_at(3, "JOBS") {
        return Ok(None);
    }

    parser.expect_kw("ADMIN")?;
    parser.expect_kw("ALTER")?;
    parser.expect_kw("DDL")?;
    parser.expect_kw("JOBS")?;

    let job_token = parser.peek().clone();
    if job_token.kind != TokenKind::IntLit {
        return Err(parser.err_here("expected an ADMIN ALTER DDL JOBS integer ID"));
    }
    parser.bump();
    let job_number = job_token
        .text
        .parse()
        .map_err(|_| parser.err_here("ADMIN ALTER DDL JOBS ID is out of signed 64-bit range"))?;

    let mut options = Vec::new();
    while parser.peek().kind != TokenKind::Eof && !parser.is_op(";") {
        let option_token = parser.peek().clone();
        if !matches!(option_token.kind, TokenKind::Ident | TokenKind::Keyword) {
            return Err(parser.err_here("expected an ADMIN ALTER DDL JOBS option name"));
        }
        parser.bump();
        if parser.is_op("=") || parser.is_op(":=") {
            parser.bump();
        } else {
            return Err(parser.err_here("expected '=' or ':=' after ADMIN ALTER DDL JOBS option"));
        }
        let value = parse_signed_literal(parser)?;
        options.push(AdminAlterDdlJobOption {
            name: option_token.text.to_lowercase(),
            value,
        });
        if parser.is_op(",") {
            parser.bump();
        } else {
            break;
        }
    }

    Ok(Some(AdminAlterDdlJobsStmt {
        job_number,
        options,
    }))
}

/// Go's `SignedLiteral`: one scalar literal, or `+`/`-` directly followed by
/// a numeric literal. This is intentionally literal-only; option expressions
/// are validated by the DDL job subsystem rather than generalized here.
fn parse_signed_literal(parser: &mut Parser) -> PResult<Expr> {
    if parser.is_op("+") || parser.is_op("-") {
        let operator = if parser.is_op("+") {
            UnaryOp::Plus
        } else {
            UnaryOp::Minus
        };
        parser.bump();
        if !matches!(
            parser.peek().kind,
            TokenKind::IntLit | TokenKind::FloatLit | TokenKind::DecLit
        ) {
            return Err(parser.err_here("expected numeric ADMIN ALTER DDL JOBS literal"));
        }
        return Ok(Expr::Unary(
            operator,
            Box::new(parser.parse_expr(prec::UNARY)?),
        ));
    }
    if !matches!(
        parser.peek().kind,
        TokenKind::IntLit
            | TokenKind::FloatLit
            | TokenKind::DecLit
            | TokenKind::Str
            | TokenKind::CharsetIntroducer
            | TokenKind::HexLit
            | TokenKind::BitLit
    ) && !(parser.is_kw("NULL") || parser.is_kw("TRUE") || parser.is_kw("FALSE"))
    {
        return Err(parser.err_here("expected ADMIN ALTER DDL JOBS literal"));
    }
    parser.parse_expr(prec::UNARY)
}
