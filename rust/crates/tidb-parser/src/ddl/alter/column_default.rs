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

//! Go `HandParser.parseAlterAlter`'s ALTER-column-default branch.

use tidb_ast::{AlterColumnDefault, AlterTableAction, Expr, UnaryOp};
use tidb_lexer::TokenKind;

use crate::{prec, PResult, Parser};

/// Parses only `ALTER [COLUMN] name {SET DEFAULT value|DROP DEFAULT}`.
///
/// The unparenthesized form is Go's `SignedLiteral` production; only a
/// parenthesized form may use the general expression grammar. CHECK,
/// CONSTRAINT, and INDEX prefixes remain owned by their preceding leaves.
pub(crate) fn parse(parser: &mut Parser) -> PResult<Option<AlterTableAction>> {
    if !parser.is_kw("ALTER")
        || parser.is_kw_at(1, "INDEX")
        || parser.is_kw_at(1, "CHECK")
        || parser.is_kw_at(1, "CONSTRAINT")
    {
        return Ok(None);
    }
    parser.bump();
    if parser.is_kw("COLUMN") {
        parser.bump();
    }
    let name = parser.parse_name_or_keyword()?;
    let default_value = if parser.is_kw("SET") {
        parser.bump();
        parser.expect_kw("DEFAULT")?;
        Some(if parser.is_op("(") {
            parser.bump();
            let expression = parser.parse_expr(prec::NONE)?;
            parser.expect_op(")")?;
            preserve_parenthesized_expression(expression)
        } else {
            parse_signed_literal(parser)?
        })
    } else if parser.is_kw("DROP") {
        parser.bump();
        parser.expect_kw("DEFAULT")?;
        None
    } else {
        return Err(parser.err_here("expected SET DEFAULT or DROP DEFAULT"));
    };
    Ok(Some(AlterTableAction::AlterColumnDefault(
        AlterColumnDefault {
            name,
            default_value,
        },
    )))
}

/// Go's `SignedLiteral`: one scalar literal, or a `+`/`-` numeric literal.
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
            return Err(parser.err_here("expected numeric default literal"));
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
        return Err(parser.err_here("expected default literal"));
    }
    parser.parse_expr(prec::UNARY)
}

/// Go's parenthesized literal AST restore omits redundant parentheses, while
/// expressions and function calls retain their written grouping. The outer
/// grammar parentheses were consumed before `parse_expr`, so reconstruct a
/// `Paren` node exactly for the latter case.
fn preserve_parenthesized_expression(expression: Expr) -> Expr {
    if is_signed_literal(&expression) {
        expression
    } else {
        Expr::Paren(Box::new(expression))
    }
}

fn is_signed_literal(expression: &Expr) -> bool {
    matches!(
        expression,
        Expr::Int(_)
            | Expr::Decimal(_)
            | Expr::Float(_)
            | Expr::Hex(_)
            | Expr::Bit(_)
            | Expr::String(_)
            | Expr::CharsetString { .. }
            | Expr::Null
            | Expr::Bool(_)
    ) || matches!(
        expression,
        Expr::Unary(UnaryOp::Plus | UnaryOp::Minus, value)
            if matches!(**value, Expr::Int(_) | Expr::Decimal(_) | Expr::Float(_))
    )
}
