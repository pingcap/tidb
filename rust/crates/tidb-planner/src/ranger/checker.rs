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

//! Go `pkg/util/ranger/checker.go`, COMPLETE: `conditionChecker` — whether
//! one condition can BUILD a range over the checked column
//! (`is_access_cond`) and whether it must also STAY as a filter
//! (`should_reserve`). A prefix-length column can locate rows but not prove
//! them, so nearly every admission over one reserves the condition; the
//! collation rules (`CompatibleCollate`, the binary-collation EQ
//! approximation, PAD SPACE `LIKE`) are ported arm by arm with Go's issue
//! citations.
//!
//! Upstream has no checker-only unit test (its coverage rides
//! `ranger_test.go`'s integration cases); the focused tests below pin each
//! arm against the Go bodies.

use tidb_datatype::{EvalType, FieldTypeCode};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::scalar_function::ScalarFunction;

/// Go `types.UnspecifiedLength`.
pub const UNSPECIFIED_LENGTH: i64 = -1;

/// Go `conditionChecker`. The Go `ctx` field is an eval context consulted
/// only for types and column identity; both read directly off this port's
/// expressions.
#[derive(Debug)]
pub struct ConditionChecker<'a> {
    /// Go `checkerCol`.
    pub checker_col: Option<&'a Column>,
    /// Go `length`: the index column's prefix length.
    pub length: i64,
    /// Go `optPrefixIndexSingleScan` (`tidb_opt_prefix_index_single_scan`).
    pub opt_prefix_index_single_scan: bool,
}

impl ConditionChecker<'_> {
    /// Go `isFullLengthColumn`.
    fn is_full_length_column(&self) -> bool {
        self.length == UNSPECIFIED_LENGTH
            || self
                .checker_col
                .and_then(|col| col.ret_type.as_ref())
                .is_some_and(|ft| self.length == ft.flen())
    }

    /// Go `check`: `(is_access_cond, should_reserve)`.
    #[must_use]
    pub fn check(&self, condition: &Expression) -> (bool, bool) {
        match condition {
            Expression::ScalarFunction(scalar) => self.check_scalar_function(scalar),
            Expression::Column(column) => {
                if column
                    .ret_type
                    .as_ref()
                    .is_some_and(|ft| ft.eval_type() == EvalType::String)
                {
                    return (false, true);
                }
                self.check_column_expr(condition)
            }
            Expression::Constant(_) => (true, false),
            Expression::CorrelatedColumn(_) => (false, true),
        }
    }

    /// Go `checkScalarFunction`.
    fn check_scalar_function(&self, scalar: &ScalarFunction) -> (bool, bool) {
        let (_, collation) = scalar.collation.charset_and_collation();
        let name = scalar.func_name.lowercase();
        match name {
            "or" | "and" => {
                let (access0, reserve0) = self.check(&scalar.args[0]);
                let (access1, reserve1) = self.check(&scalar.args[1]);
                if access0 && access1 {
                    return (true, reserve0 || reserve1);
                }
                (false, true)
            }
            "eq" | "ne" | "ge" | "gt" | "le" | "lt" | "nulleq" => {
                for (constant_side, column_side) in [
                    (&scalar.args[0], &scalar.args[1]),
                    (&scalar.args[1], &scalar.args[0]),
                ] {
                    if !matches!(constant_side, Expression::Constant(_)) {
                        continue;
                    }
                    if !self.match_column(column_side) {
                        continue;
                    }
                    // The comparison must run under a collation compatible
                    // with the column's own.
                    let column_type = column_side.static_type();
                    let is_string =
                        column_type.is_some_and(|ft| ft.eval_type() == EvalType::String);
                    if is_string
                        && !tidb_datatype::compatible_collate(
                            column_type.map_or("", |ft| ft.collation_name()),
                            collation,
                        )
                    {
                        // A binary-collation constant (col = CAST(x AS
                        // BINARY)) may build an APPROXIMATE range for
                        // EQ/NullEQ, kept as a filter for the binary
                        // semantics.
                        if tidb_datatype::is_bin_collation(collation)
                            && (name == "eq" || name == "nulleq")
                        {
                            return (true, true);
                        }
                        return (false, true);
                    }
                    let is_full_length = self.is_full_length_column();
                    if name == "ne" {
                        return (is_full_length, !is_full_length);
                    }
                    return (true, !is_full_length);
                }
                (false, true)
            }
            "isnull" => {
                if self.match_column(&scalar.args[0]) {
                    // Whether the column is null is knowable from a prefix
                    // of ANY length.
                    let is_null_reserve =
                        !self.opt_prefix_index_single_scan && !self.is_full_length_column();
                    return (true, is_null_reserve);
                }
                (false, true)
            }
            "istrue_with_null" | "istrue" | "isfalse" => {
                if let Expression::Column(column) = &scalar.args[0] {
                    if column
                        .ret_type
                        .as_ref()
                        .is_some_and(|ft| ft.eval_type() == EvalType::String)
                    {
                        return (false, true);
                    }
                }
                self.check_column_expr(&scalar.args[0])
            }
            "not" => {
                // Go's TODO: "not like" does not convert to access
                // conditions.
                let Expression::ScalarFunction(inner) = &scalar.args[0] else {
                    // "not column" or "not constant" can't lead to a range.
                    return (false, true);
                };
                if inner.func_name.lowercase() == "like" || inner.func_name.lowercase() == "nulleq"
                {
                    return (false, true);
                }
                self.check(&scalar.args[0])
            }
            "in" => {
                if !self.match_column(&scalar.args[0]) {
                    return (false, true);
                }
                let column_type = scalar.args[0].static_type();
                let is_string = column_type.is_some_and(|ft| ft.eval_type() == EvalType::String);
                if is_string
                    && !tidb_datatype::compatible_collate(
                        column_type.map_or("", |ft| ft.collation_name()),
                        collation,
                    )
                {
                    if !tidb_datatype::is_bin_collation(collation) {
                        return (false, true);
                    }
                    // Binary-collation mismatch: every IN value must be a
                    // constant before the approximate range is admitted.
                    if scalar.args[1..]
                        .iter()
                        .any(|value| !matches!(value, Expression::Constant(_)))
                    {
                        return (false, true);
                    }
                    return (true, true);
                }
                if scalar.args[1..]
                    .iter()
                    .any(|value| !matches!(value, Expression::Constant(_)))
                {
                    return (false, true);
                }
                (true, !self.is_full_length_column())
            }
            "like" => self.check_like_func(scalar),
            "getparam" => (true, false),
            _ => (false, true),
        }
    }

    /// Go `checkLikeFunc`, with the PAD SPACE reservation and the enum
    /// wildcard refusals (issue 27130).
    fn check_like_func(&self, scalar: &ScalarFunction) -> (bool, bool) {
        let (_, collation) = scalar.collation.charset_and_collation();
        let column_type = scalar.args[0].static_type();
        if !tidb_datatype::compatible_collate(
            column_type.map_or("", |ft| ft.collation_name()),
            collation,
        ) {
            return (false, true);
        }
        if !self.match_column(&scalar.args[0]) {
            return (false, true);
        }
        let Expression::Constant(pattern) = &scalar.args[1] else {
            return (false, true);
        };
        if matches!(pattern.value, tidb_datatype::Datum::Null) {
            return (false, true);
        }
        let Ok(pattern_str) = pattern.value.sql_string() else {
            return (false, true);
        };
        let pattern_bytes = pattern_str.as_bytes();
        let mut like_func_reserve = !self.is_full_length_column();

        // Trailing spaces are significant in `like`; PAD SPACE index keys
        // drop them, so a Selection must re-check.
        if tidb_datatype::is_pad_space_collation(collation) {
            like_func_reserve = true;
        }

        if pattern_bytes.is_empty() {
            return (true, like_func_reserve);
        }
        let Expression::Constant(escape_const) = &scalar.args[2] else {
            return (false, true);
        };
        let tidb_datatype::Datum::Int(escape) = &escape_const.value else {
            return (false, true);
        };
        let escape = *escape as u8;
        let is_enum = column_type.is_some_and(|ft| ft.code() == FieldTypeCode::Enum);
        let mut i = 0;
        while i < pattern_bytes.len() {
            if pattern_bytes[i] == escape {
                i += 1;
                if i < pattern_bytes.len() - 1 {
                    i += 1;
                    continue;
                }
                break;
            }
            if i == 0 && (pattern_bytes[i] == b'%' || pattern_bytes[i] == b'_') {
                return (false, true);
            }
            if pattern_bytes[i] == b'%' {
                // `enum like 'xxx%'` cannot build a range (issue 27130).
                if is_enum {
                    return (false, true);
                }
                if i != pattern_bytes.len() - 1 {
                    like_func_reserve = true;
                }
                break;
            }
            if pattern_bytes[i] == b'_' {
                if is_enum {
                    return (false, true);
                }
                like_func_reserve = true;
                break;
            }
            i += 1;
        }
        (true, like_func_reserve)
    }

    /// Go `matchColumn` (`EqualByExprAndID`).
    fn match_column(&self, expr: &Expression) -> bool {
        self.checker_col
            .is_some_and(|column| column.equal_column(expr))
    }

    /// Go `checkColumn`.
    fn check_column_expr(&self, expr: &Expression) -> (bool, bool) {
        if self.match_column(expr) {
            return (true, !self.is_full_length_column());
        }
        (false, true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_ast::CiString;
    use tidb_datatype::{Collation, Datum, FieldType};
    use tidb_expr::constant::Constant;

    fn int_column(unique_id: i64) -> Column {
        Column::new(unique_id, FieldType::new(FieldTypeCode::LongLong))
    }

    fn string_column(unique_id: i64, collation: &str) -> Column {
        let mut ft = FieldType::new(FieldTypeCode::Varchar);
        ft.set_flen(20);
        ft.set_charset_name("utf8mb4");
        ft.set_collation_name(collation);
        Column::new(unique_id, ft)
    }

    fn int_const(v: i64) -> Expression {
        Expression::Constant(Constant::new(
            Datum::Int(v),
            FieldType::new(FieldTypeCode::LongLong),
        ))
    }

    fn string_const(text: &str) -> Expression {
        let mut ft = FieldType::new(FieldTypeCode::Varchar);
        ft.set_charset_name("utf8mb4");
        ft.set_collation_name("utf8mb4_bin");
        Expression::Constant(Constant::new(
            Datum::String(tidb_datatype::StringDatum::new(
                text.as_bytes().to_vec(),
                Collation::Utf8Mb4Bin,
            )),
            ft,
        ))
    }

    fn func(name: &str, collation: &str, args: Vec<Expression>) -> Expression {
        let mut scalar = ScalarFunction::new(
            CiString::new(name),
            FieldType::new(FieldTypeCode::LongLong),
            args,
        );
        scalar
            .collation
            .set_charset_and_collation("utf8mb4", collation);
        Expression::ScalarFunction(scalar)
    }

    fn checker(column: &Column, length: i64) -> ConditionChecker<'_> {
        ConditionChecker {
            checker_col: Some(column),
            length,
            // Go default: tidb_opt_prefix_index_single_scan = ON.
            opt_prefix_index_single_scan: true,
        }
    }

    /// The comparison arms: full-length admits without reserve; a prefix
    /// column admits WITH reserve; `ne` only over full length.
    #[test]
    fn comparisons_admit_by_length() {
        let col = int_column(1);
        let full = checker(&col, UNSPECIFIED_LENGTH);
        let eq = func(
            "eq",
            "binary",
            vec![Expression::Column(col.clone()), int_const(3)],
        );
        assert_eq!(full.check(&eq), (true, false));
        // Constant on the left admits through Go's mirrored arm.
        let eq_rev = func(
            "eq",
            "binary",
            vec![int_const(3), Expression::Column(col.clone())],
        );
        assert_eq!(full.check(&eq_rev), (true, false));

        let prefix = checker(&col, 5);
        assert_eq!(prefix.check(&eq), (true, true), "prefix reserves");
        let ne = func(
            "ne",
            "binary",
            vec![Expression::Column(col.clone()), int_const(3)],
        );
        assert_eq!(full.check(&ne), (true, false));
        assert_eq!(prefix.check(&ne), (false, true), "prefix ne refuses");

        // A comparison against another column has no constant side.
        let other = int_column(2);
        let col_vs_col = func(
            "eq",
            "binary",
            vec![Expression::Column(col.clone()), Expression::Column(other)],
        );
        assert_eq!(full.check(&col_vs_col), (false, true));
    }

    /// The collation gate: an incompatible collation refuses, EXCEPT the
    /// binary-collation EQ approximation which admits with reserve.
    #[test]
    fn collation_mismatch_gates_string_comparisons() {
        let col = string_column(1, "utf8mb4_general_ci");
        let full = checker(&col, UNSPECIFIED_LENGTH);
        // Function computed under utf8mb4_bin against a general_ci column:
        // incompatible; eq admits approximately, lt refuses.
        // `utf8mb4_bin` IS a bin collation (Go `IsBinCollation`), so the
        // EQ approximation admits with reserve even against general_ci.
        let eq = func(
            "eq",
            "utf8mb4_bin",
            vec![Expression::Column(col.clone()), string_const("x")],
        );
        assert_eq!(full.check(&eq), (true, true));
        // A non-bin incompatible collation refuses outright.
        let eq_ci = func(
            "eq",
            "utf8mb4_unicode_ci",
            vec![Expression::Column(col.clone()), string_const("x")],
        );
        assert_eq!(full.check(&eq_ci), (false, true));
        let eq_binary = func(
            "eq",
            "binary",
            vec![Expression::Column(col.clone()), string_const("x")],
        );
        assert_eq!(
            full.check(&eq_binary),
            (true, true),
            "binary EQ approximates"
        );
        let lt_binary = func(
            "lt",
            "utf8mb4_bin",
            vec![Expression::Column(col.clone()), string_const("x")],
        );
        assert_eq!(
            full.check(&lt_binary),
            (false, true),
            "only EQ/NullEQ approximate"
        );

        // A compatible collation admits cleanly.
        let compatible = string_column(2, "utf8mb4_bin");
        let full_compatible = checker(&compatible, UNSPECIFIED_LENGTH);
        let eq_same = func(
            "eq",
            "utf8mb4_bin",
            vec![Expression::Column(compatible.clone()), string_const("x")],
        );
        assert_eq!(full_compatible.check(&eq_same), (true, false));
    }

    /// `isnull` admits from any prefix; the reserve depends on the
    /// single-scan switch exactly as Go writes it.
    #[test]
    fn isnull_reserve_follows_the_single_scan_switch() {
        let col = int_column(1);
        let isnull = func("isnull", "binary", vec![Expression::Column(col.clone())]);
        assert_eq!(
            checker(&col, UNSPECIFIED_LENGTH).check(&isnull),
            (true, false)
        );
        let mut prefix = checker(&col, 5);
        assert_eq!(
            prefix.check(&isnull),
            (true, false),
            "single-scan ON never reserves"
        );
        prefix.opt_prefix_index_single_scan = false;
        assert_eq!(prefix.check(&isnull), (true, true));
    }

    /// The IN arm: all-constant lists admit; any non-constant refuses.
    #[test]
    fn in_lists_require_constants() {
        let col = int_column(1);
        let full = checker(&col, UNSPECIFIED_LENGTH);
        let all_const = func(
            "in",
            "binary",
            vec![Expression::Column(col.clone()), int_const(1), int_const(2)],
        );
        assert_eq!(full.check(&all_const), (true, false));
        let with_column = func(
            "in",
            "binary",
            vec![
                Expression::Column(col.clone()),
                int_const(1),
                Expression::Column(int_column(9)),
            ],
        );
        assert_eq!(full.check(&with_column), (false, true));
    }

    /// The LIKE arm over Go's pattern walk: leading wildcards refuse,
    /// trailing `%` admits (reserving only when not last or prefix), `_`
    /// reserves, and the PAD SPACE collation always reserves.
    #[test]
    fn like_patterns_walk_as_go() {
        let col = string_column(1, "utf8mb4_bin");
        let full = checker(&col, UNSPECIFIED_LENGTH);
        let like = |pattern: &str, collation: &str| {
            func(
                "like",
                collation,
                vec![
                    Expression::Column(col.clone()),
                    string_const(pattern),
                    int_const(i64::from(b'\\')),
                ],
            )
        };
        // `utf8mb4_bin` is a PAD SPACE collation (Go
        // `IsPadSpaceCollation`: only binary and the 0900 family are NO
        // PAD), so every ADMITTED like reserves for the trailing-space
        // recheck; the refusals are unchanged.
        assert_eq!(full.check(&like("abc%", "utf8mb4_bin")), (true, true));
        assert_eq!(full.check(&like("%abc", "utf8mb4_bin")), (false, true));
        assert_eq!(full.check(&like("_bc", "utf8mb4_bin")), (false, true));
        assert_eq!(full.check(&like("a%c", "utf8mb4_bin")), (true, true));
        assert_eq!(full.check(&like("a_c", "utf8mb4_bin")), (true, true));
        assert_eq!(full.check(&like("abc", "utf8mb4_bin")), (true, true));

        // PAD SPACE collation always reserves (trailing spaces).
        let padded = string_column(2, "utf8mb4_general_ci");
        let full_padded = checker(&padded, UNSPECIFIED_LENGTH);
        let like_padded = func(
            "like",
            "utf8mb4_general_ci",
            vec![
                Expression::Column(padded.clone()),
                string_const("abc"),
                int_const(i64::from(b'\\')),
            ],
        );
        assert_eq!(full_padded.check(&like_padded), (true, true));
    }

    /// `not like` refuses (Go's TODO); `not (a < c)` recurses.
    #[test]
    fn not_recurses_except_like() {
        let col = int_column(1);
        let full = checker(&col, UNSPECIFIED_LENGTH);
        let lt = func(
            "lt",
            "binary",
            vec![Expression::Column(col.clone()), int_const(3)],
        );
        let not_lt = func("not", "binary", vec![lt]);
        assert_eq!(full.check(&not_lt), (true, false));
        let strcol = string_column(2, "utf8mb4_bin");
        let like = func(
            "like",
            "utf8mb4_bin",
            vec![
                Expression::Column(strcol),
                string_const("a%"),
                int_const(i64::from(b'\\')),
            ],
        );
        let not_like = func("not", "binary", vec![like]);
        assert_eq!(full.check(&not_like), (false, true));
    }

    /// Bare expressions: a constant is an access condition, a string
    /// column refuses, an int column admits by length.
    #[test]
    fn bare_expressions_check_as_go() {
        let col = int_column(1);
        let full = checker(&col, UNSPECIFIED_LENGTH);
        assert_eq!(full.check(&int_const(1)), (true, false));
        assert_eq!(full.check(&Expression::Column(col.clone())), (true, false));
        let strcol = string_column(2, "utf8mb4_bin");
        let str_checker = checker(&strcol, UNSPECIFIED_LENGTH);
        assert_eq!(
            str_checker.check(&Expression::Column(strcol.clone())),
            (false, true)
        );
    }
}
