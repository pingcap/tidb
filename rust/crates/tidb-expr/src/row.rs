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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Row-value (`ROW(...)`/`(...)`) comparison — `=`/`<>`/`<`/`>`/`<=`/`>=`
//! between two same-arity tuples, called from `crate::eval_in`'s own
//! `Expr::Binary` arm (when both operands are bare `Expr::Row` nodes)
//! and `crate::func::eval_in_list` (a row-value `IN`/`NOT IN` operand).
//! Real MySQL/TiDB restricts `ROW(...)` syntactically to ONLY these
//! positions (confirmed via `gorun`: a bare `SELECT ROW(1,2)` with no
//! comparison is a genuine parse-time ERROR there too) — so this crate
//! deliberately does NOT need a general-purpose `Datum::Row` variant
//! that could appear in `GROUP BY`/`ORDER BY`/`DISTINCT` dedup or be
//! projected as an ordinary column value; AST-level special-casing at
//! exactly these two call sites is enough.
//!
//! `<=>` (NULL-safe equal) is deliberately NOT implemented here —
//! narrower scope, not exercised by the corpus, and its own NULL
//! semantics (never `NULL`, treats `NULL<=>NULL` as `TRUE`) would need
//! a second, differently-shaped combination rule from the `=`/`<>`/
//! ordering operators below, not a trivial extension of them.

use std::cmp::Ordering;

use tidb_ast::BinaryOp;

use crate::coerce::bool_int;
use crate::ops::eval_binary;
use crate::{Datum, EvalError};

/// Total order over two scalar datums — Go `types/datum.go` `Datum.Compare`
/// as used for sorting (`pkg/util/chunk/compare.go` `GetCompareFunc`).
///
/// Semantics are exactly the ones this crate's own `=`/`<` operators use
/// (the ordering behind `IN`/`BETWEEN`/comparison), plus the sort-side NULL
/// rule: NULL orders below every non-NULL value and equal to NULL (Go
/// `chunk.cmpNull` / `Datum.Compare`'s `KindNull` arm) instead of the
/// operators' three-valued NULL propagation. Strings compare under the
/// session `utf8mb4_bin` PAD SPACE collation; numeric kinds compare
/// cross-kind through MySQL's promotion rules (Int/UInt/Decimal exactly,
/// Real via `f64`); string-vs-numeric compares as MySQL real coercion.
/// Errors surface for operand kinds the evaluator does not order.
pub fn compare_datums(l: &Datum, r: &Datum) -> Result<Ordering, EvalError> {
    match (l, r) {
        (Datum::Null, Datum::Null) => return Ok(Ordering::Equal),
        (Datum::Null, _) => return Ok(Ordering::Less),
        (_, Datum::Null) => return Ok(Ordering::Greater),
        _ => {}
    }
    match eval_binary(BinaryOp::Eq, l.clone(), r.clone())? {
        Datum::Int(1) => return Ok(Ordering::Equal),
        Datum::Int(0) => {}
        _ => unreachable!("eval_binary(Eq, non-NULL, non-NULL) only ever returns Int(0/1)"),
    }
    match eval_binary(BinaryOp::Lt, l.clone(), r.clone())? {
        Datum::Int(1) => Ok(Ordering::Less),
        Datum::Int(0) => Ok(Ordering::Greater),
        _ => unreachable!("eval_binary(Lt, non-NULL, non-NULL) only ever returns Int(0/1)"),
    }
}

/// `l = r` for two same-arity row values — three-valued, SQL `AND`-
/// composed equality across ALL positions (confirmed via `gorun`: a
/// definite mismatch at ANY position decides the result `FALSE`
/// outright, even when a LATER position is `NULL` — `ROW(1,2) <>
/// ROW(2,NULL)` is `TRUE`, not `NULL`; only when NO position is a
/// definite mismatch AND at least one is `NULL` does the whole
/// comparison become `NULL`). Every position is checked — NOT stopped
/// at the first `NULL` — matching real SQL `AND`'s own semantics
/// (`FALSE AND NULL` is `FALSE`, not `NULL`, regardless of which
/// operand is evaluated first).
fn row_eq(l: &[Datum], r: &[Datum]) -> Result<Datum, EvalError> {
    let mut any_null = false;
    for (lv, rv) in l.iter().zip(r) {
        match eval_binary(BinaryOp::Eq, lv.clone(), rv.clone())? {
            Datum::Int(0) => return Ok(Datum::Int(0)),
            Datum::Null => any_null = true,
            Datum::Int(1) => {}
            _ => unreachable!("eval_binary(Eq, ...) only ever returns Int(0/1) or Null"),
        }
    }
    Ok(if any_null { Datum::Null } else { Datum::Int(1) })
}

/// `l <op> r` for two same-arity row values, `op` one of
/// `Eq`/`Ne`/`Lt`/`Gt`/`Le`/`Ge` — see [`row_eq`]'s own doc for
/// equality; the four ordering operators are LEXICOGRAPHIC (confirmed
/// via `gorun`: the FIRST position where the two rows differ decides
/// the whole comparison, regardless of what follows — `ROW(2,1) <
/// ROW(1,NULL)` is `FALSE`, not `NULL`, since position 0 alone (`2 <
/// 1` is false) already decides it without ever looking at position 1's
/// own `NULL`). Real TiDB rejects a mismatched row arity outright
/// (confirmed via `gorun`: `ROW(1,2) = ROW(1,2,3)` is a genuine `ERR`)
/// — modelled here as `Unsupported` too, matching this crate's own
/// convention for other rare-but-real SQL error conditions (e.g.
/// `crate::eval_in`'s own scalar-subquery-with-multiple-columns case).
pub(crate) fn row_compare(op: BinaryOp, l: &[Datum], r: &[Datum]) -> Result<Datum, EvalError> {
    if l.len() != r.len() {
        return Err(EvalError::Unsupported("row value arity mismatch"));
    }
    match op {
        BinaryOp::Eq => row_eq(l, r),
        BinaryOp::Ne => Ok(match row_eq(l, r)? {
            Datum::Int(v) => Datum::Int(1 - v),
            Datum::Null => Datum::Null,
            _ => unreachable!("row_eq only ever returns Int or Null"),
        }),
        BinaryOp::Lt | BinaryOp::Gt | BinaryOp::Le | BinaryOp::Ge => {
            for (lv, rv) in l.iter().zip(r) {
                match eval_binary(BinaryOp::Eq, lv.clone(), rv.clone())? {
                    Datum::Null => return Ok(Datum::Null),
                    Datum::Int(1) => continue, // equal here — the deciding position is later
                    Datum::Int(0) => {
                        let is_lt = match eval_binary(BinaryOp::Lt, lv.clone(), rv.clone())? {
                            Datum::Int(v) => v == 1,
                            Datum::Null => return Ok(Datum::Null),
                            _ => unreachable!(),
                        };
                        return Ok(bool_int(match op {
                            BinaryOp::Lt | BinaryOp::Le => is_lt,
                            BinaryOp::Gt | BinaryOp::Ge => !is_lt,
                            _ => unreachable!(),
                        }));
                    }
                    _ => unreachable!("eval_binary(Eq, ...) only ever returns Int(0/1) or Null"),
                }
            }
            // Every position was equal — decides `<=`/`>=` (true) vs `<`/`>` (false).
            Ok(bool_int(matches!(op, BinaryOp::Le | BinaryOp::Ge)))
        }
        _ => Err(EvalError::Unsupported("row value comparison operator")),
    }
}
