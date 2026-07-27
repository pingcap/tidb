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

//! The expression rewriter: builds a planner [`Expression`] from a parsed AST
//! [`Expr`] (Go's `expression_rewriter.go`).
//!
//! This is the bridge from a parsed SQL expression to the evaluable expression
//! tree. It is a SEED: literals become [`Constant`]s and operators become
//! [`ScalarFunction`]s (named so [`ScalarFunction::eval`] dispatches them),
//! which is enough for constant/operator expressions such as `1 + 1` or
//! `2 * 3 - 1`.
//!
//! DEFERRED (documented): column references (need schema/name resolution), the
//! full literal domain (decimal/hex/bit/charset strings, unsigned promotion of
//! large integers), function calls, subqueries, and the result-type inference
//! that Go performs while rewriting (the ScalarFunction result type is a
//! placeholder here; evaluation dispatches on operand kinds, not this type).

use crate::constant::Constant;
use crate::expression::{Expression, ScalarFunction};
use crate::scalar_function::{binary_op_name, unary_op_name};
use crate::EvalError;
use tidb_ast::{CiString, Expr};
use tidb_datatype::{Datum, FieldType, FieldTypeCode};

fn constant(datum: Datum, code: FieldTypeCode) -> Expression {
    Expression::Constant(Constant::new(datum, FieldType::new(code)))
}

fn scalar(name: &str, args: Vec<Expression>) -> Expression {
    // The result type is a placeholder: operator evaluation dispatches on the
    // operand datum kinds, not on this type. Faithful type inference is deferred.
    Expression::ScalarFunction(ScalarFunction::new(
        CiString::new(name),
        FieldType::new(FieldTypeCode::LongLong),
        args,
    ))
}

/// Go `expression_rewriter`: rewrite a parsed AST [`Expr`] into an evaluable
/// [`Expression`].
///
/// Supports integer/float/string/boolean/NULL literals, unary and binary
/// operators, and parentheses. Returns [`EvalError::Unsupported`] for forms not
/// yet handled (column references, function calls, other literal kinds).
pub fn rewrite_expr(expr: &Expr) -> Result<Expression, EvalError> {
    match expr {
        Expr::Int(text) => {
            let value: i64 = text
                .parse()
                .map_err(|_| EvalError::Unsupported("integer literal outside the i64 domain"))?;
            Ok(constant(Datum::Int(value), FieldTypeCode::LongLong))
        }
        Expr::Float(value) => Ok(constant(Datum::Real(*value), FieldTypeCode::Double)),
        Expr::Bool(value) => Ok(constant(
            Datum::Int(i64::from(*value)),
            FieldTypeCode::LongLong,
        )),
        Expr::Null => Ok(constant(Datum::Null, FieldTypeCode::Null)),
        Expr::String(text) => {
            let mut datum = Datum::Null;
            datum.set_bytes(text.clone().into_bytes());
            Ok(Expression::Constant(Constant::new(
                datum,
                FieldType::new(FieldTypeCode::VarString),
            )))
        }
        Expr::Paren(inner) => rewrite_expr(inner),
        Expr::Unary(op, inner) => {
            let arg = rewrite_expr(inner)?;
            Ok(scalar(unary_op_name(*op), vec![arg]))
        }
        Expr::Binary(op, lhs, rhs) => {
            let left = rewrite_expr(lhs)?;
            let right = rewrite_expr(rhs)?;
            Ok(scalar(binary_op_name(*op), vec![left, right]))
        }
        _ => Err(EvalError::Unsupported(
            "expression form is not yet supported by the rewriter",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::NoColumns;
    use tidb_ast::{BinaryOp, UnaryOp};
    use tidb_chunk::chunk::Chunk;

    // Evaluates a rewritten expression over an empty (column-less) row.
    fn eval_const(expr: &Expr) -> Datum {
        let rewritten = rewrite_expr(expr).unwrap();
        let chunk = Chunk::new_empty(&[]);
        // A column-less chunk still yields a virtual row for evaluation.
        let mut c = chunk;
        c.set_num_virtual_rows(1);
        rewritten.eval(&NoColumns, c.get_row(0)).unwrap()
    }

    #[test]
    fn rewrite_and_eval_arithmetic() {
        // 1 + 1
        let one = || Box::new(Expr::Int("1".to_owned()));
        let plus = Expr::Binary(BinaryOp::Plus, one(), one());
        assert_eq!(eval_const(&plus), Datum::Int(2));

        // 2 * 3 - 1  ==  (2*3) - 1  == 5
        let two = Box::new(Expr::Int("2".to_owned()));
        let three = Box::new(Expr::Int("3".to_owned()));
        let mul = Box::new(Expr::Binary(BinaryOp::Mul, two, three));
        let minus = Expr::Binary(BinaryOp::Minus, mul, one());
        assert_eq!(eval_const(&minus), Datum::Int(5));

        // -(1 + 1) == -2, through a paren
        let paren = Box::new(Expr::Paren(Box::new(Expr::Binary(
            BinaryOp::Plus,
            one(),
            one(),
        ))));
        let neg = Expr::Unary(UnaryOp::Minus, paren);
        assert_eq!(eval_const(&neg), Datum::Int(-2));
    }

    #[test]
    fn rewrite_literals() {
        assert_eq!(eval_const(&Expr::Null), Datum::Null);
        assert_eq!(eval_const(&Expr::Bool(true)), Datum::Int(1));
        match eval_const(&Expr::Float(1.5)) {
            Datum::Real(f) => assert_eq!(f, 1.5),
            other => panic!("expected real, got {other:?}"),
        }
    }

    #[test]
    fn unsupported_form_errors() {
        // A column reference is not yet handled.
        let col = Expr::Column(vec!["a".to_owned()]);
        assert!(rewrite_expr(&col).is_err());
    }
}
