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

use super::*;

#[cfg(test)]
mod round_truncate_type_source_tests {
    use super::*;
    use crate::column::Column;
    use crate::scalar_function::ScalarFunction;
    use tidb_chunk::chunk::Chunk;

    fn column(code: FieldTypeCode, flen: i64, decimal: i64) -> Expression {
        let mut field_type = FieldType::new(code);
        field_type.set_flen(flen);
        field_type.set_decimal(decimal);
        Expression::Column(Column::new(1, field_type))
    }

    #[test]
    fn dynamic_decimal_scale_and_integer_domain_match_the_source_types() {
        let value = column(FieldTypeCode::NewDecimal, 10, 2);
        let mut scale = column(FieldTypeCode::LongLong, 20, 0);
        let Expression::Column(scale_column) = &mut scale else {
            unreachable!()
        };
        scale_column.index = 1;
        for name in ["round", "truncate"] {
            let result = builtin_return_type(name, &[value.clone(), scale.clone()]).unwrap();
            assert_eq!(result.code(), FieldTypeCode::NewDecimal, "{name}");
            assert_eq!(result.flen(), 10, "{name}");
            assert_eq!(result.decimal(), 2, "{name}");

            let mut chunk = Chunk::new_with_capacity(
                &[
                    value.static_type().unwrap().clone(),
                    scale.static_type().unwrap().clone(),
                ],
                1,
            );
            chunk.append_datum(
                0,
                &Datum::Decimal(tidb_datatype::Decimal::from_literal("1.23")),
            );
            chunk.append_int64(1, 5);
            let function = ScalarFunction::new(
                tidb_ast::CiString::new(name),
                result,
                vec![value.clone(), scale.clone()],
            );
            let Datum::Decimal(answer) = function
                .eval(&crate::context::NoColumns, chunk.get_row(0))
                .unwrap()
            else {
                panic!("{name} returned a non-decimal value")
            };
            assert_eq!(answer.to_string(), "1.23", "{name}");
        }

        let integer = column(FieldTypeCode::LongLong, 20, 0);
        assert_eq!(
            builtin_return_type("truncate", &[integer, scale])
                .unwrap()
                .code(),
            FieldTypeCode::LongLong
        );
    }

    #[test]
    fn constant_decimal_scale_fixes_the_source_metadata() {
        let value = column(FieldTypeCode::NewDecimal, 10, 2);
        let scale = |value| {
            Expression::Constant(Constant::new(
                Datum::Int(value),
                FieldType::new(FieldTypeCode::LongLong),
            ))
        };
        for (name, requested, expected_flen, expected_decimal) in [
            ("round", 5, 13, 5),
            ("truncate", 5, 13, 5),
            ("round", -1, 10, 0),
            ("truncate", -1, 8, 0),
            ("round", 100, 38, 30),
            ("truncate", 100, 38, 30),
        ] {
            let result = builtin_return_type(name, &[value.clone(), scale(requested)]).unwrap();
            assert_eq!(result.flen(), expected_flen, "{name}({requested})");
            assert_eq!(result.decimal(), expected_decimal, "{name}({requested})");
        }
    }
}

#[cfg(test)]
mod ceil_floor_type_source_tests {
    use super::*;
    use crate::column::Column;
    use crate::scalar_function::ScalarFunction;
    use tidb_chunk::chunk::Chunk;

    fn decimal(flen: i64, scale: i64) -> Expression {
        let mut field_type = FieldType::new(FieldTypeCode::NewDecimal);
        field_type.set_flen(flen);
        field_type.set_decimal(scale);
        Expression::Column(Column::new(1, field_type))
    }

    #[test]
    fn decimal_result_domain_uses_declared_integer_width() {
        for name in ["ceil", "ceiling", "floor"] {
            let narrow_arg = decimal(20, 2);
            let narrow = builtin_return_type(name, std::slice::from_ref(&narrow_arg)).unwrap();
            assert_eq!(narrow.code(), FieldTypeCode::LongLong, "{name}");

            let wide_arg = decimal(21, 2);
            let wide = builtin_return_type(name, std::slice::from_ref(&wide_arg)).unwrap();
            assert_eq!(wide.code(), FieldTypeCode::NewDecimal, "{name}");
            assert_eq!(wide.flen(), 21, "{name}");
            assert_eq!(wide.decimal(), 0, "{name}");

            for (argument, result_type, expect_decimal) in
                [(narrow_arg, narrow, false), (wide_arg, wide, true)]
            {
                let mut chunk =
                    Chunk::new_with_capacity(&[argument.static_type().unwrap().clone()], 1);
                chunk.append_datum(
                    0,
                    &Datum::Decimal(tidb_datatype::Decimal::from_literal("-1.23")),
                );
                let result =
                    ScalarFunction::new(tidb_ast::CiString::new(name), result_type, vec![argument])
                        .eval(&crate::context::NoColumns, chunk.get_row(0))
                        .unwrap();
                let expected = if name == "floor" { -2 } else { -1 };
                if expect_decimal {
                    let Datum::Decimal(result) = result else {
                        panic!("{name} returned a non-decimal wide result")
                    };
                    assert_eq!(result.to_string(), expected.to_string(), "{name}");
                } else {
                    assert_eq!(result, Datum::Int(expected), "{name}");
                }
            }
        }
    }
}

#[cfg(test)]
mod time_source_tests {
    use super::*;
    use crate::NoColumns;
    use tidb_ast::Expr;
    use tidb_chunk::chunk::Chunk;

    fn string_arg(value: &str) -> Expression {
        let mut ft = FieldType::new(FieldTypeCode::VarString);
        ft.set_flen(value.len() as i64);
        Expression::Constant(Constant::new(Datum::new_string(value.to_owned()), ft))
    }

    fn chunk_time(value: &str) -> Datum {
        let expression = Expr::Func {
            name: "time".to_owned(),
            args: vec![Expr::String(value.to_owned())],
            origin_position: 0,
        };
        let rewritten = crate::rewriter::rewrite_expr(&expression).unwrap();
        let mut chunk = Chunk::new_empty(&[]);
        chunk.set_num_virtual_rows(1);
        rewritten.eval(&NoColumns, chunk.get_row(0)).unwrap()
    }

    /// Exact metadata half of Go `TestTime`: the four positive spellings,
    /// the negative maximum duration, and the integer-zero build boundary.
    #[test]
    fn test_time() {
        for (value, fsp, flen) in [
            ("2003-12-31 01:02:03", 0, 10),
            ("2003-12-31 01:02:03.000123", 6, 17),
            ("01:02:03.000123", 6, 17),
            ("01:02:03", 0, 10),
            ("-838:59:59.000000", 6, 17),
        ] {
            let result = builtin_return_type("time", &[string_arg(value)]).unwrap();
            assert_eq!(result.code(), FieldTypeCode::Duration);
            assert_eq!(result.charset_name(), "binary");
            assert_eq!(result.collation_name(), "binary");
            assert!(result.has_flag(tidb_datatype::FieldTypeFlags::BINARY));
            assert_eq!(result.decimal(), fsp);
            assert_eq!(result.flen(), flen);
            let Datum::Duration(duration) = chunk_time(value) else {
                panic!("TIME must evaluate into its declared duration domain")
            };
            let expected = value
                .split_once(char::is_whitespace)
                .map_or(value, |(_, time)| time);
            assert_eq!(duration.to_string(), expected);
        }

        let zero = Expression::Constant(Constant::new(
            Datum::Int(0),
            FieldType::new(FieldTypeCode::LongLong),
        ));
        let result = builtin_return_type("time", &[zero]).unwrap();
        assert_eq!(result.code(), FieldTypeCode::Duration);
        assert_eq!(result.decimal(), 0);
        assert_eq!(result.flen(), 10);
        assert!(builtin_return_type("time", &[]).is_none());
    }
}

#[cfg(test)]
mod vector_result_type_tests {
    use super::*;

    fn string_arg() -> Expression {
        Expression::Constant(Constant::new(
            Datum::new_string("[1,2]"),
            FieldType::new(FieldTypeCode::VarString),
        ))
    }

    #[test]
    fn vector_builtin_result_domains_match_their_function_classes() {
        let vector = Expression::Constant(Constant::new(
            Datum::Null,
            FieldType::new(FieldTypeCode::VectorFloat32),
        ));
        assert_eq!(
            builtin_return_type("vec_dims", std::slice::from_ref(&vector))
                .expect("VEC_DIMS type")
                .code(),
            FieldTypeCode::LongLong
        );
        assert_eq!(
            builtin_return_type("vec_l2_distance", &[vector.clone(), vector.clone()])
                .expect("VEC_L2_DISTANCE type")
                .code(),
            FieldTypeCode::Double
        );
        assert_eq!(
            builtin_return_type("vec_from_text", &[string_arg()])
                .expect("VEC_FROM_TEXT type")
                .code(),
            FieldTypeCode::VectorFloat32
        );
        assert_eq!(
            builtin_return_type("vec_as_text", &[vector])
                .expect("VEC_AS_TEXT type")
                .code(),
            FieldTypeCode::VarString
        );
    }
}

#[cfg(test)]
mod info_source_tests {
    use std::cell::Cell;

    use super::*;
    use crate::Columns;

    struct LastInsertColumns {
        previous: u64,
        published: Cell<Option<u64>>,
    }

    impl Columns for LastInsertColumns {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn last_insert_id(&self) -> Option<u64> {
            Some(self.previous)
        }

        fn set_last_insert_id(&self, value: u64) {
            self.published.set(Some(value));
        }
    }

    /// Go `TestLastInsertID`: both arities' wire metadata and the complete
    /// source value matrix, including float rounding and two's-complement
    /// publication of negative and maximum unsigned arguments.
    #[test]
    fn test_last_insert_id() {
        let one_arg = [Expression::Constant(Constant::new(
            Datum::Int(1),
            FieldType::new(FieldTypeCode::LongLong),
        ))];
        for args in [&[][..], &one_arg[..]] {
            let result_type = builtin_return_type("last_insert_id", args).unwrap();
            assert_eq!(result_type.code(), FieldTypeCode::LongLong);
            assert_eq!(result_type.charset_name(), "binary");
            assert_eq!(result_type.collation_name(), "binary");
            assert!(result_type.has_flag(tidb_datatype::FieldTypeFlags::BINARY));
            assert!(result_type.is_unsigned());
            assert_eq!(result_type.flen(), 20);
        }

        for (previous, args, expected, published) in [
            (0, vec![Datum::Int(1)], 1, Some(1)),
            (0, vec![Datum::Real(1.1)], 1, Some(1)),
            (0, vec![Datum::UInt(u64::MAX)], u64::MAX, Some(u64::MAX)),
            (0, vec![Datum::Int(-1)], u64::MAX, Some(u64::MAX)),
            (1, vec![], 1, None),
            (u64::MAX, vec![], u64::MAX, None),
        ] {
            let ctx = LastInsertColumns {
                previous,
                published: Cell::new(None),
            };
            assert_eq!(
                crate::func::eval_func_values_in("LAST_INSERT_ID", &args, &ctx)
                    .expect("LAST_INSERT_ID must be dispatched")
                    .expect("source row must evaluate"),
                Datum::UInt(expected)
            );
            assert_eq!(ctx.published.get(), published);
        }
    }
}

#[cfg(test)]
mod concat_flen_tests {
    use super::*;

    fn arg(code: FieldTypeCode, flen: i64, decimal: i64) -> Expression {
        let mut ft = FieldType::new(code);
        ft.set_flen(flen);
        ft.set_decimal(decimal);
        Expression::Constant(Constant::new(Datum::Null, ft))
    }

    fn flen_of(name: &str, args: &[Expression]) -> i64 {
        builtin_return_type(name, args)
            .unwrap_or_else(|| panic!("{name} has no return type"))
            .flen()
    }

    const UNSPECIFIED: i64 = tidb_datatype::UNSPECIFIED_LENGTH;

    fn varchar(flen: i64) -> Expression {
        arg(FieldTypeCode::Varchar, flen, UNSPECIFIED)
    }
    fn bigint() -> Expression {
        arg(FieldTypeCode::LongLong, 11, 0)
    }

    /// Every number below is Go's own `GetFlen()` for the built expression,
    /// captured by running `expression.NewFunction` over these exact argument
    /// FieldTypes. `CONCAT`'s width reaches the client as the result set's
    /// `ColumnLength`, so an unspecified flen here is wire-visible.
    #[test]
    fn concat_sums_the_string_cast_widths_go_sums() {
        assert_eq!(flen_of("concat", &[varchar(16)]), 16);
        assert_eq!(flen_of("concat", &[varchar(16), varchar(16)]), 32);
        // Any integer widens to MaxIntWidth, whatever its display width.
        assert_eq!(flen_of("concat", &[bigint(), bigint()]), 40);
        assert_eq!(flen_of("concat", &[arg(FieldTypeCode::Tiny, 4, 0)]), 20);
        // BIT is the exception: its BYTE count, because TiKV needs the real
        // length when it evaluates things like ASCII(bit).
        assert_eq!(flen_of("concat", &[arg(FieldTypeCode::Bit, 8, 0)]), 1);
        // Sign, decimal point and a leading zero.
        assert_eq!(
            flen_of("concat", &[arg(FieldTypeCode::NewDecimal, 10, 2)]),
            13
        );
        // TiDB prints reals in full, never scientific notation, so the
        // worst-case widths are far wider than MySQL's 12/22.
        assert_eq!(
            flen_of(
                "concat",
                &[arg(FieldTypeCode::Float, UNSPECIFIED, UNSPECIFIED)]
            ),
            87
        );
        assert_eq!(
            flen_of(
                "concat",
                &[arg(FieldTypeCode::Double, UNSPECIFIED, UNSPECIFIED)]
            ),
            370
        );
        assert_eq!(
            flen_of(
                "concat",
                &[
                    varchar(16),
                    arg(FieldTypeCode::Double, UNSPECIFIED, UNSPECIFIED)
                ]
            ),
            386
        );
        assert_eq!(
            flen_of(
                "concat",
                &[arg(FieldTypeCode::Date, UNSPECIFIED, UNSPECIFIED)]
            ),
            10
        );
        assert_eq!(
            flen_of("concat", &[arg(FieldTypeCode::Datetime, UNSPECIFIED, 0)]),
            19
        );
        assert_eq!(
            flen_of("concat", &[arg(FieldTypeCode::Datetime, UNSPECIFIED, 3)]),
            23
        );
        assert_eq!(
            flen_of("concat", &[arg(FieldTypeCode::Duration, UNSPECIFIED, 2)]),
            13
        );
    }

    /// The separator is counted `len(args) - 2` times, so a single value
    /// argument contributes no separator at all.
    #[test]
    fn concat_ws_counts_the_separator_between_values_only() {
        assert_eq!(flen_of("concat_ws", &[varchar(3), varchar(16)]), 16);
        assert_eq!(
            flen_of("concat_ws", &[varchar(3), varchar(16), varchar(16)]),
            35
        );
        assert_eq!(
            flen_of("concat_ws", &[varchar(3), bigint(), bigint(), bigint()]),
            66
        );
        assert_eq!(
            flen_of(
                "concat_ws",
                &[
                    varchar(3),
                    arg(FieldTypeCode::Double, UNSPECIFIED, UNSPECIFIED),
                    varchar(16)
                ]
            ),
            389
        );
    }

    /// Go restarts the running sum at MaxBlobWidth for an argument of unknown
    /// width and then still ADDS that argument's -1, so a lone unsized
    /// argument leaves MaxBlobWidth - 1 rather than MaxBlobWidth. Porting the
    /// arithmetic rather than the intent is what keeps us on Go's number.
    #[test]
    fn concat_clamps_at_max_blob_width_the_way_go_does() {
        let unsized_arg = arg(FieldTypeCode::Varchar, UNSPECIFIED, UNSPECIFIED);
        assert_eq!(
            flen_of("concat", std::slice::from_ref(&unsized_arg)),
            MAX_BLOB_WIDTH - 1
        );
        // A second argument pushes the sum back over the clamp.
        assert_eq!(
            flen_of("concat", &[unsized_arg, varchar(16)]),
            MAX_BLOB_WIDTH
        );
    }
}

/// Go's `createTestCase4StrFuncs` (`pkg/expression/typeinfer_test.go`), which
/// is TiDB's own golden table for what a string builtin reports: it builds a
/// logical plan for `select <expr> from t` over a fixed schema and asserts the
/// output column's type byte, charset, flag, flen and decimal.
///
/// Only the TYPE BYTE and the FLEN are re-asserted here -- the charset and
/// flag are `derive_collation`'s answer, tested with that -- and only for the
/// builtins this rewriter builds. Every expected number below is copied from
/// that table rather than recomputed, so a rule that merely looks right does
/// not pass.
#[cfg(test)]
mod go_string_flen_tests {
    use super::*;

    const UNSPECIFIED: i64 = tidb_datatype::UNSPECIFIED_LENGTH;

    fn typed(code: FieldTypeCode, flen: i64, decimal: i64) -> Expression {
        let mut ft = FieldType::new(code);
        ft.set_flen(flen);
        ft.set_decimal(decimal);
        Expression::Constant(Constant::new(Datum::Null, ft))
    }

    fn literal(value: Datum, code: FieldTypeCode, flen: i64) -> Expression {
        let mut ft = FieldType::new(code);
        ft.set_flen(flen);
        ft.set_decimal(UNSPECIFIED);
        Expression::Constant(Constant::new(value, ft))
    }

    /// `c_char char(20)`.
    fn c_char() -> Expression {
        typed(FieldTypeCode::String, 20, UNSPECIFIED)
    }
    /// `c_binary binary(20)`.
    fn c_binary() -> Expression {
        typed(FieldTypeCode::String, 20, UNSPECIFIED)
    }
    /// `c_int_d int`.
    fn c_int_d() -> Expression {
        typed(FieldTypeCode::Long, 11, 0)
    }
    /// `c_double_d double`.
    fn c_double_d() -> Expression {
        typed(FieldTypeCode::Double, UNSPECIFIED, UNSPECIFIED)
    }
    /// `c_float_d float`.
    fn c_float_d() -> Expression {
        typed(FieldTypeCode::Float, UNSPECIFIED, UNSPECIFIED)
    }
    /// `c_decimal decimal(6, 3)`.
    fn c_decimal() -> Expression {
        typed(FieldTypeCode::NewDecimal, 6, 3)
    }
    /// `c_text_d text`.
    fn c_text_d() -> Expression {
        typed(FieldTypeCode::Blob, 65535, UNSPECIFIED)
    }
    /// `c_datetime datetime(2)`.
    fn c_datetime() -> Expression {
        typed(FieldTypeCode::Datetime, UNSPECIFIED, 2)
    }
    /// `c_set set('a', 'b', 'c')`.
    fn c_set() -> Expression {
        typed(FieldTypeCode::Set, 5, UNSPECIFIED)
    }
    /// `c_enum enum('a', 'b', 'c')`.
    fn c_enum() -> Expression {
        typed(FieldTypeCode::Enum, 1, UNSPECIFIED)
    }

    #[track_caller]
    fn assert_go(name: &str, args: &[Expression], code: FieldTypeCode, flen: i64) {
        let ft =
            builtin_return_type(name, args).unwrap_or_else(|| panic!("{name} has no return type"));
        assert_eq!((ft.code(), ft.flen()), (code, flen), "{name}");
    }

    /// The rule shape that reaches the widest part of the family:
    /// `bf.tp.SetFlen(args[0].GetType().GetFlen())` read AFTER the argument was
    /// wrapped in `WrapWithCastAsString`, which is why an `int` argument is 20
    /// and a `double` one 370.
    #[test]
    fn arg_zero_width_family_matches_go() {
        for name in [
            "lower",
            "upper",
            "lcase",
            "ucase",
            "reverse",
            "ltrim",
            "rtrim",
            "left",
            "right",
            "substr",
            "substring",
            "mid",
            "substring_index",
        ] {
            // Go: lower(c_int_d) / reverse(c_int_d) / left(c_int_d, c_int_d).
            assert_go(name, &[c_int_d(), c_int_d()], FieldTypeCode::VarString, 20);
            assert_go(name, &[c_char(), c_int_d()], FieldTypeCode::VarString, 20);
            assert_go(name, &[c_binary(), c_int_d()], FieldTypeCode::VarString, 20);
        }
        // `TRIM` is the same rule in its one- and three-argument forms; its
        // two-argument form is the exception, asserted separately below.
        assert_go("trim", &[c_int_d()], FieldTypeCode::VarString, 20);
        assert_go(
            "trim",
            &[c_char(), c_char(), c_int_d()],
            FieldTypeCode::VarString,
            20,
        );
        // Go: reverse over the remaining column kinds.
        assert_go("reverse", &[c_float_d()], FieldTypeCode::VarString, 87);
        assert_go("reverse", &[c_double_d()], FieldTypeCode::VarString, 370);
        assert_go("reverse", &[c_decimal()], FieldTypeCode::VarString, 9);
        assert_go("reverse", &[c_set()], FieldTypeCode::VarString, 5);
        assert_go("reverse", &[c_enum()], FieldTypeCode::VarString, 1);
    }

    /// A TEXT argument is the boundary case the expression-index refusals turn
    /// on: 65535 is ONE SHORT of `getRetTp`'s MEDIUM threshold, so Go reports a
    /// `var_string(65535)` and not a `mediumtext`. Go's own row.
    #[test]
    fn a_text_argument_stays_var_string_in_go() {
        assert_go("reverse", &[c_text_d()], FieldTypeCode::VarString, 65535);
        assert_go("lower", &[c_text_d()], FieldTypeCode::VarString, 65535);
        // MEDIUMTEXT and LONGTEXT do cross it, which is what makes
        // `index i((lower(mediumtext_col)))` a 3757 rather than an accept.
        assert_go(
            "lower",
            &[typed(FieldTypeCode::MediumBlob, 16_777_215, UNSPECIFIED)],
            FieldTypeCode::MediumBlob,
            16_777_215,
        );
        assert_go(
            "lower",
            &[typed(
                FieldTypeCode::LongBlob,
                MAX_LONG_BLOB_WIDTH,
                UNSPECIFIED,
            )],
            FieldTypeCode::LongBlob,
            MAX_LONG_BLOB_WIDTH,
        );
        // And a `varchar(0)` argument keeps a ZERO-width result, which is the
        // third refusal (3761).
        assert_go(
            "lower",
            &[typed(FieldTypeCode::Varchar, 0, UNSPECIFIED)],
            FieldTypeCode::VarString,
            0,
        );
    }

    /// `getRetTp`'s two thresholds are both `>=`, and both are exact: a result
    /// of exactly 65536 IS a mediumblob and one of exactly `MaxBlobWidth` IS a
    /// longblob. Asserted on the boundary itself because an off-by-one there
    /// is invisible in every other row -- no column width in Go's own golden
    /// table lands on either number.
    #[test]
    fn the_promotion_thresholds_are_inclusive() {
        let at = |flen: i64| {
            builtin_return_type("lower", &[typed(FieldTypeCode::Varchar, flen, UNSPECIFIED)])
                .unwrap()
                .code()
        };
        assert_eq!(at(65535), FieldTypeCode::VarString);
        assert_eq!(at(65536), FieldTypeCode::MediumBlob);
        assert_eq!(at(MAX_BLOB_WIDTH - 1), FieldTypeCode::MediumBlob);
        assert_eq!(at(MAX_BLOB_WIDTH), FieldTypeCode::LongBlob);
    }

    /// `TRIM(remstr FROM str)` is the one form in the family whose
    /// `getFunction` sets no flen at all.
    #[test]
    fn two_argument_trim_has_no_width_in_go() {
        assert_go(
            "trim",
            &[c_char(), c_char()],
            FieldTypeCode::VarString,
            UNSPECIFIED,
        );
    }

    /// The fixed-width members, each of which `getRetTp` then promotes.
    #[test]
    fn fixed_width_members_match_go() {
        assert_go(
            "space",
            &[c_int_d()],
            FieldTypeCode::LongBlob,
            MAX_BLOB_WIDTH,
        );
        assert_go(
            "repeat",
            &[c_char(), c_int_d()],
            FieldTypeCode::LongBlob,
            MAX_BLOB_WIDTH,
        );
        assert_go(
            "insert_func",
            &[c_char(), c_int_d(), c_int_d(), c_char()],
            FieldTypeCode::LongBlob,
            MAX_BLOB_WIDTH,
        );
        assert_go(
            "format",
            &[c_double_d(), c_double_d()],
            FieldTypeCode::LongBlob,
            MAX_BLOB_WIDTH,
        );
        for name in ["bin", "oct"] {
            assert_go(name, &[c_int_d()], FieldTypeCode::VarString, 64);
            assert_go(name, &[c_text_d()], FieldTypeCode::VarString, 64);
        }
        assert_go(
            "conv",
            &[c_char(), c_int_d(), c_int_d()],
            FieldTypeCode::VarString,
            64,
        );
    }

    /// `replaceFunctionClass.fixLength`. Go's own row is
    /// `replace(1234, 2, 55)` -> 20, where every literal widens to
    /// `MaxIntWidth` first and the excess term is therefore zero.
    #[test]
    fn replace_matches_go() {
        assert_go(
            "replace",
            &[
                literal(Datum::Int(1234), FieldTypeCode::LongLong, 4),
                literal(Datum::Int(2), FieldTypeCode::LongLong, 1),
                literal(Datum::Int(55), FieldTypeCode::LongLong, 2),
            ],
            FieldTypeCode::VarString,
            20,
        );
        assert_go(
            "replace",
            &[c_binary(), c_int_d(), c_int_d()],
            FieldTypeCode::VarString,
            20,
        );
        // The excess term itself: a 20-wide subject, a 2-wide needle and a
        // 5-wide replacement is 20 + (20/2)*3.
        assert_go(
            "replace",
            &[
                c_char(),
                typed(FieldTypeCode::Varchar, 2, UNSPECIFIED),
                typed(FieldTypeCode::Varchar, 5, UNSPECIFIED),
            ],
            FieldTypeCode::VarString,
            50,
        );
    }

    /// `getFlen4LpadAndRpad`: only a constant pad length is knowable, and it is
    /// multiplied by four. Go's rows.
    #[test]
    fn lpad_and_rpad_match_go() {
        let twelve = literal(Datum::Int(12), FieldTypeCode::LongLong, 2);
        let go = literal(Datum::Bytes(b"go".to_vec()), FieldTypeCode::VarString, 2);
        for name in ["lpad", "rpad"] {
            assert_go(
                name,
                &[
                    literal(Datum::Bytes(b"TiDB".to_vec()), FieldTypeCode::VarString, 4),
                    twelve.clone(),
                    go.clone(),
                ],
                FieldTypeCode::VarString,
                48,
            );
            // A NON-constant length is `mysql.MaxBlobWidth` before the times
            // four, and the clamp brings it back to `MaxBlobWidth`.
            assert_go(
                name,
                &[c_char(), c_int_d(), c_char()],
                FieldTypeCode::LongBlob,
                MAX_BLOB_WIDTH,
            );
        }
    }

    /// `eltFunctionClass`: the widest selectable value, where an argument of
    /// unknown width RESETS rather than widens.
    #[test]
    fn elt_matches_go() {
        assert_go(
            "elt",
            &[c_int_d(), c_char(), c_char(), c_char()],
            FieldTypeCode::VarString,
            20,
        );
        assert_go(
            "elt",
            &[c_int_d(), c_char(), c_int_d()],
            FieldTypeCode::VarString,
            20,
        );
        assert_go(
            "elt",
            &[c_int_d(), c_char(), c_double_d(), c_int_d()],
            FieldTypeCode::VarString,
            370,
        );
        assert_go(
            "elt",
            &[c_int_d(), c_char(), c_double_d(), c_int_d(), c_binary()],
            FieldTypeCode::VarString,
            370,
        );
        // The reset: a trailing argument of unknown width takes the result
        // back to unknown, which is Go's `flen == UnspecifiedLength ||` arm.
        assert_go(
            "elt",
            &[
                c_int_d(),
                c_char(),
                typed(FieldTypeCode::Varchar, UNSPECIFIED, UNSPECIFIED),
            ],
            FieldTypeCode::VarString,
            UNSPECIFIED,
        );
    }

    /// `exportSetFunctionClass`: sixty-four values and sixty-three separators,
    /// times four. Go's three rows, all over TEXT columns.
    #[test]
    fn export_set_matches_go() {
        assert_go(
            "export_set",
            &[c_double_d(), c_text_d(), c_text_d()],
            FieldTypeCode::MediumBlob,
            16_777_212,
        );
        assert_go(
            "export_set",
            &[c_double_d(), c_text_d(), c_text_d(), c_text_d()],
            FieldTypeCode::LongBlob,
            33_291_780,
        );
        assert_go(
            "export_set",
            &[c_double_d(), c_text_d(), c_text_d(), c_text_d(), c_int_d()],
            FieldTypeCode::LongBlob,
            33_291_780,
        );
    }

    /// `makeSetFunctionClass.getFlen`, both halves: a constant mask sizes only
    /// the members it selects, and anything else sums them all.
    #[test]
    fn make_set_matches_go() {
        assert_go(
            "make_set",
            &[c_int_d(), c_text_d()],
            FieldTypeCode::VarString,
            65535,
        );
        assert_go(
            "make_set",
            &[c_int_d(), c_text_d(), c_binary()],
            FieldTypeCode::MediumBlob,
            65556,
        );
        // Go's `make_set(1, c_text_d, 0x40)`: mask 1 selects the FIRST member
        // only, so the binary literal contributes nothing.
        assert_go(
            "make_set",
            &[
                literal(Datum::Int(1), FieldTypeCode::LongLong, 1),
                c_text_d(),
                literal(Datum::Bytes(vec![0x40]), FieldTypeCode::VarString, 3),
            ],
            FieldTypeCode::VarString,
            65535,
        );
    }

    /// `charFunctionClass`: four bytes per code point, the trailing USING
    /// charset argument excluded.
    #[test]
    fn char_matches_go() {
        let charset = literal(
            Datum::Bytes(b"binary".to_vec()),
            FieldTypeCode::VarString,
            6,
        );
        assert_go(
            "char_func",
            &[c_int_d(), charset.clone()],
            FieldTypeCode::VarString,
            4,
        );
        assert_go(
            "char_func",
            &[c_int_d(), c_int_d(), charset],
            FieldTypeCode::VarString,
            8,
        );
    }

    /// Go's `CONCAT`/`CONCAT_WS` rows over LITERALS, which only add up once a
    /// literal carries the width `types.DefaultTypeForValue` gives it. They
    /// are the check that the literal rule and the sum rule agree.
    #[test]
    fn concat_over_literals_matches_go() {
        let lit = |bytes: &[u8]| {
            literal(
                Datum::Bytes(bytes.to_vec()),
                FieldTypeCode::VarString,
                bytes.len() as i64,
            )
        };
        // Go: CONCAT('T', 'i', 'DB') -> 4.
        assert_go(
            "concat",
            &[lit(b"T"), lit(b"i"), lit(b"DB")],
            FieldTypeCode::VarString,
            4,
        );
        // Go: CONCAT_WS('-', 'T', 'i', 'DB') -> 6, the separator counted twice.
        assert_go(
            "concat_ws",
            &[lit(b"-"), lit(b"T"), lit(b"i"), lit(b"DB")],
            FieldTypeCode::VarString,
            6,
        );
        // Go: CONCAT_WS(',', 'TiDB', c_binary) -> 25.
        assert_go(
            "concat_ws",
            &[lit(b","), lit(b"TiDB"), c_binary()],
            FieldTypeCode::VarString,
            25,
        );
    }

    /// The INT half of the same file. Go fixes a width on each of these too,
    /// and it is not `MaxIntWidth` -- `LENGTH` is 10, `ASCII` 3, `STRCMP` 2.
    /// Go's own rows, from the same golden table.
    #[test]
    fn string_builtins_returning_an_int_match_go() {
        for (name, args, flen) in [
            ("bit_length", vec![c_char()], 10),
            ("ascii", vec![c_char()], 3),
            ("ord", vec![c_char()], 10),
            ("instr", vec![c_char(), c_char()], 11),
            ("strcmp", vec![c_char(), c_char()], 2),
            ("find_in_set", vec![c_int_d(), c_text_d()], 3),
            // Go's `MaxIntWidth` rows, which the ETInt default already gives.
            ("char_length", vec![c_char()], 20),
            ("character_length", vec![c_char()], 20),
            ("locate", vec![c_char(), c_char()], 20),
            ("field", vec![c_double_d(), c_text_d()], 20),
        ] {
            assert_go(name, &args, FieldTypeCode::LongLong, flen);
        }
        // `LENGTH`'s own row is not in the golden table; `lengthFunctionClass`
        // sets 10 and `OCTET_LENGTH` is the same class.
        assert_go("length", &[c_char()], FieldTypeCode::LongLong, 10);
        assert_go("octet_length", &[c_char()], FieldTypeCode::LongLong, 10);
    }

    /// `quoteFunctionClass`: `2 * flen + 2`. Go's rows.
    #[test]
    fn quote_matches_go() {
        assert_go("quote", &[c_int_d()], FieldTypeCode::VarString, 42);
        assert_go("quote", &[c_float_d()], FieldTypeCode::VarString, 176);
        assert_go("quote", &[c_double_d()], FieldTypeCode::VarString, 742);
    }

    /// `HEX` splits on the argument's eval type: a string is hexed four bytes
    /// per character at two digits each, a number is doubled from its DECLARED
    /// width. Go's `hex(c_char)` 160 and `hex(c_int_d)` 22.
    #[test]
    fn hex_matches_go() {
        assert_go("hex", &[c_char()], FieldTypeCode::VarString, 160);
        assert_go("hex", &[c_int_d()], FieldTypeCode::VarString, 22);
    }

    /// `UNHEX` is the family's one reader of the UNCAST argument type, which is
    /// why Go's `unhex(c_int_d)` is 6 and not 40.
    #[test]
    fn unhex_matches_go() {
        assert_go("unhex", &[c_int_d()], FieldTypeCode::VarString, 6);
        assert_go("unhex", &[c_char()], FieldTypeCode::VarString, 40);
    }

    /// `FROM_BASE64` triples, `TO_BASE64` grows by `base64NeededEncodedLength`.
    /// Go's rows, including the two whose triple crosses the MEDIUM boundary.
    #[test]
    fn base64_matches_go() {
        assert_go("from_base64", &[c_int_d()], FieldTypeCode::VarString, 60);
        assert_go("from_base64", &[c_float_d()], FieldTypeCode::VarString, 261);
        assert_go(
            "from_base64",
            &[c_double_d()],
            FieldTypeCode::VarString,
            1110,
        );
        assert_go("from_base64", &[c_decimal()], FieldTypeCode::VarString, 27);
        assert_go("from_base64", &[c_datetime()], FieldTypeCode::VarString, 66);
        assert_go("from_base64", &[c_char()], FieldTypeCode::VarString, 60);
        assert_go("from_base64", &[c_set()], FieldTypeCode::VarString, 15);
        assert_go("from_base64", &[c_enum()], FieldTypeCode::VarString, 3);
        assert_go(
            "from_base64",
            &[c_text_d()],
            FieldTypeCode::MediumBlob,
            196_605,
        );
        assert_go("to_base64", &[c_binary()], FieldTypeCode::VarString, 28);
    }
}
