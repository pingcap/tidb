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

//! Running guards for the expressions a `corpus/expr/*.txt` topic header names
//! as HELD OUT: rows whose Go answer was captured but which `tidb-expr` cannot
//! evaluate today, so putting them in the corpus would only turn the
//! differential gate red without adding information.
//!
//! Each guard pins TODAY's behavior and carries the Go answer beside it. The
//! point is that the gap cannot close silently: the day `tidb-expr` learns to
//! evaluate one of these, its assertion here fails and the reader is sent back
//! to the corpus topic to move the row into the executable set.
//!
//! Mirrors the hold-out list in `corpus/expr/int_arithmetic_source.txt`, which
//! is itself the tail of `pkg/expression/builtin_arithmetic_test.go`'s tables.

use tidb_ast::{QueryStmt, SelectField, Stmt};

/// The same evaluation path `expr_diff.rs` drives, reduced to "did it produce a
/// label, and which one".
fn eval_label(expr: &str) -> Result<String, String> {
    let stmt = tidb_parser::parse(&format!("select {expr}")).map_err(|e| e.message)?;
    let Stmt::Query(query) = stmt else {
        return Err("not a query".to_owned());
    };
    let QueryStmt::Select(sel) = query.into_inner() else {
        return Err("not a select".to_owned());
    };
    match sel.fields.first() {
        Some(SelectField::Expr { expr, .. }) => tidb_expr::eval(expr)
            .map(|v| v.label())
            .map_err(|e| format!("{e:?}")),
        _ => Err("no field expression".to_owned()),
    }
}

/// A hex or bit literal in an arithmetic context is an INTEGER in MySQL --
/// unsigned for `0x`, signed for `b''` -- which is what
/// `TestArithmeticPlus`'s `types.ParseHexStr`/`types.NewBitLiteral` cases
/// assert. `tidb-expr` carries both as plain bytes and refuses them, so the
/// binary-literal identity is lost before arithmetic ever sees it.
#[test]
fn binary_literal_arithmetic_is_still_unevaluated() {
    // Go: UINT:9007199254740993
    assert!(
        eval_label("0x20000000000000 + 1").is_err(),
        "hex-literal arithmetic now evaluates; move the row into \
         corpus/expr/int_arithmetic_source.txt and regenerate its golden"
    );
    // Go: INT:4
    assert!(
        eval_label("b'00011' + 1").is_err(),
        "bit-literal arithmetic now evaluates; move the row into \
         corpus/expr/int_arithmetic_source.txt and regenerate its golden"
    );
}

/// `TestArithmeticMod`'s `types.Duration{45296s}` row: a TIME operand takes its
/// numeric form (`123456`), so the remainder against 122 is 114. `ops.rs`
/// already routes `Datum::Duration` through `numeric_context_value`; what is
/// missing is only `CAST(... AS TIME)` to produce one from a literal.
#[test]
fn cast_as_time_operand_is_still_unevaluated() {
    // Go: INT:114
    assert!(
        eval_label("mod(cast('12:34:56' as time), 122)").is_err(),
        "CAST AS TIME now evaluates; move the row into \
         corpus/expr/int_arithmetic_source.txt and regenerate its golden"
    );
}
