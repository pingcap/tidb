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

//! Request-owned expression context, matching Go flagsAndTzToSessionContext.

use std::sync::{Arc, Mutex};
use tidb_datatype::{Datum, SessionTimeZone};
use tidb_expr::{Columns, ErrorLevel};
use tidb_model::flags::*;

#[derive(Debug)]
pub(super) struct RequestEvalContext {
    pub(super) zone: SessionTimeZone,
    pub(super) division_precision: u32,
    pub(super) flags: u64,
    pub(super) column_types: Vec<tidb_datatype::FieldType>,
    warnings: Mutex<Vec<(u16, String)>>,
}

impl RequestEvalContext {
    pub(super) fn new(zone: SessionTimeZone, division_precision: u32, flags: u64) -> Self {
        Self {
            zone,
            division_precision,
            flags,
            column_types: Vec::new(),
            warnings: Mutex::new(Vec::new()),
        }
    }

    pub(super) fn condition_value(&self, value: &Datum) -> Result<i128, String> {
        let converted = value.to_bool().map_err(|err| format!("{err:?}"))?;
        if converted.event.is_some() {
            let text = match value {
                Datum::String(value) => value.as_utf8().map_err(|err| err.to_string())?,
                Datum::Bytes(value) => std::str::from_utf8(value).map_err(|err| err.to_string())?,
                _ => return Err("unsupported condition conversion event".to_owned()),
            };
            self.handle_truncate(&format!(
                "Truncated incorrect DOUBLE value: '{}'",
                tidb_datatype::float_warning_input(text)
            ))
            .map_err(|err| format!("{err:?}"))?;
        }
        Ok(i128::from(converted.value))
    }

    pub(super) fn take_warnings(&self) -> Vec<(u16, String)> {
        std::mem::take(&mut *self.warnings.lock().expect("coprocessor warnings"))
    }
}

impl Columns for RequestEvalContext {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn time_zone(&self) -> SessionTimeZone {
        self.zone.clone()
    }
    fn div_precision_increment(&self) -> u32 {
        self.division_precision
    }
    fn truncate_level(&self) -> ErrorLevel {
        if self.flags & FLAG_IGNORE_TRUNCATE != 0 {
            ErrorLevel::Ignore
        } else if self.flags & FLAG_TRUNCATE_AS_WARNING != 0 {
            ErrorLevel::Warn
        } else {
            ErrorLevel::Error
        }
    }
    fn division_by_zero_level(&self) -> ErrorLevel {
        if self.flags & FLAG_DIVIDED_BY_ZERO_AS_WARNING != 0 {
            ErrorLevel::Warn
        } else {
            ErrorLevel::Error
        }
    }
    fn type_flags(&self) -> tidb_datatype::ConversionFlags {
        tidb_datatype::DEFAULT_STATEMENT_FLAGS
            .with_ignore_truncate_err(self.flags & FLAG_IGNORE_TRUNCATE != 0)
            .with_truncate_as_warning(self.flags & FLAG_TRUNCATE_AS_WARNING != 0)
            .with_ignore_zero_in_date_err(self.flags & FLAG_IGNORE_ZERO_IN_DATE != 0)
            .with_allow_negative_to_unsigned(self.flags & FLAG_IN_INSERT_STMT == 0)
    }
    fn append_warning(&self, code: u16, message: &str) {
        self.warnings
            .lock()
            .expect("coprocessor warnings")
            .push((code, message.to_owned()));
    }
}

/// A decoded builtin and the request that owns its evaluation settings.
#[derive(Clone, Debug)]
pub struct SharedExpression {
    pub(super) expression: tidb_expr::expression::Expression,
    pub(super) context: Arc<RequestEvalContext>,
}

#[cfg(test)]
mod tests {
    use super::super::{convert_expr_with_context, eval_datum, RegionAggregator, TopNSpec};
    use super::*;
    use tidb_proto::tipb;

    fn column(index: i64, tp: i32) -> tipb::Expr {
        let mut val = Vec::new();
        tidb_codec::encode_int(&mut val, index);
        tipb::Expr {
            tp: Some(tipb::ExprType::ColumnRef as i32),
            val: Some(val),
            field_type: Some(tipb::FieldType {
                tp: Some(tp),
                decimal: Some(0),
                ..Default::default()
            }),
            ..Default::default()
        }
    }
    fn call(sig: tipb::ScalarFuncSig, args: Vec<tipb::Expr>, tp: i32) -> tipb::Expr {
        tipb::Expr {
            tp: Some(tipb::ExprType::ScalarFunc as i32),
            sig: Some(sig as i32),
            children: args,
            field_type: Some(tipb::FieldType {
                tp: Some(tp),
                decimal: Some(0),
                ..Default::default()
            }),
            ..Default::default()
        }
    }
    fn conditional(value: tipb::Expr) -> tipb::Expr {
        call(
            tipb::ScalarFuncSig::CaseWhenInt,
            vec![column(0, 8), value],
            8,
        )
    }
    fn context(flags: u64) -> Arc<RequestEvalContext> {
        Arc::new(RequestEvalContext::new(
            SessionTimeZone::Named(chrono_tz::Asia::Shanghai),
            8,
            flags,
        ))
    }

    #[test]
    fn dag_precision_defaults_only_when_absent() {
        use prost::Message;
        for (wire, expected) in [(None, 4), (Some(0), 0), (Some(8), 8)] {
            let dag = tipb::DagRequest {
                div_precision_increment: wire,
                ..Default::default()
            };
            let request = tidb_proto::coprocessor::Request {
                tp: super::super::REQ_TYPE_DAG,
                data: dag.encode_to_vec(),
                ranges: vec![Default::default()],
                ..Default::default()
            };
            let context = super::super::build_dag(&request).unwrap();
            assert_eq!(context.div_precision_increment, i64::from(expected));
            assert_eq!(
                context.expression_context.div_precision_increment(),
                expected
            );
        }
    }

    #[test]
    fn selection_aggregate_and_topn_share_request_context() {
        let mut ctx = context(0);
        Arc::get_mut(&mut ctx).unwrap().column_types = vec![
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Datetime),
        ];
        let expr = conditional(call(
            tipb::ScalarFuncSig::UnixTimestampInt,
            vec![column(1, 12)],
            8,
        ));
        let time = tidb_datatype::Time::from_date_checked(
            2024,
            3,
            5,
            14,
            30,
            0,
            0,
            tidb_datatype::TimeType::DateTime,
            0,
        )
        .unwrap();
        let row = [Datum::Int(1), Datum::Time(time)];
        let expected = Datum::Int(1_709_620_200);
        let selection = convert_expr_with_context(&expr, &ctx).unwrap();
        assert_eq!(eval_datum(&selection, &row).unwrap(), expected);
        let topn = TopNSpec::build(
            &tipb::TopN {
                order_by: vec![tipb::ByItem {
                    expr: Some(expr.clone()),
                    ..Default::default()
                }],
                limit: Some(1),
                ..Default::default()
            },
            &ctx,
        )
        .unwrap();
        assert_eq!(topn.evaluate(&row).unwrap(), [expected.clone()]);
        let mut agg = RegionAggregator::build(
            &tipb::Aggregation {
                agg_func: vec![tipb::Expr {
                    tp: Some(tipb::ExprType::Max as i32),
                    children: vec![expr],
                    ..Default::default()
                }],
                ..Default::default()
            },
            &[],
            &ctx,
        )
        .unwrap();
        agg.update(&row).unwrap();
        assert_eq!(agg.finish(), [vec![expected]]);
    }

    #[test]
    fn shared_arithmetic_uses_request_division_precision() {
        let ctx = context(0);
        let mut division = call(
            tipb::ScalarFuncSig::DivideDecimal,
            vec![column(1, 246), column(2, 246)],
            246,
        );
        division.field_type.as_mut().unwrap().decimal = Some(8);
        let mut expr = call(
            tipb::ScalarFuncSig::CaseWhenDecimal,
            vec![column(0, 8), division],
            246,
        );
        expr.field_type.as_mut().unwrap().decimal = Some(8);
        let converted = convert_expr_with_context(&expr, &ctx).unwrap();
        let row = [
            Datum::Int(1),
            Datum::Decimal(tidb_datatype::Decimal::from_int(1)),
            Datum::Decimal(tidb_datatype::Decimal::from_int(3)),
        ];
        let Datum::Decimal(value) = eval_datum(&converted, &row).unwrap() else {
            panic!("decimal result");
        };
        assert_eq!(value.to_string(), "0.33333333");
    }

    #[test]
    fn shared_noninteger_condition_uses_sql_truth_conversion() {
        let ctx = context(FLAG_TRUNCATE_AS_WARNING);
        for (sig, tp, value, expected) in [
            (
                tipb::ScalarFuncSig::CaseWhenReal,
                5,
                Datum::Real(0.5),
                Some(1),
            ),
            (
                tipb::ScalarFuncSig::CaseWhenReal,
                5,
                Datum::Real(0.0),
                Some(0),
            ),
            (tipb::ScalarFuncSig::CaseWhenReal, 5, Datum::Null, None),
        ] {
            let expr = call(sig, vec![column(0, 8), column(1, tp)], tp);
            let converted = convert_expr_with_context(&expr, &ctx).unwrap();
            assert_eq!(
                super::super::eval_expr(&converted, &[Datum::Int(1), value], 8, &ctx.zone).unwrap(),
                expected
            );
        }
        let expr = call(tipb::ScalarFuncSig::Lower, vec![column(0, 253)], 253);
        let converted = convert_expr_with_context(&expr, &ctx).unwrap();
        assert_eq!(
            super::super::eval_expr(&converted, &[Datum::Bytes(b"1tail".to_vec())], 8, &ctx.zone)
                .unwrap(),
            Some(1)
        );
        let warnings = ctx.take_warnings();
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].0, 1292);
    }

    #[test]
    fn shared_builtin_warnings_belong_to_the_request() {
        let expr = conditional(call(
            tipb::ScalarFuncSig::CastStringAsInt,
            vec![column(1, 253)],
            8,
        ));
        let row = [Datum::Int(1), Datum::Bytes(b"12tail".to_vec())];
        for flags in [0, FLAG_TRUNCATE_AS_WARNING, FLAG_IGNORE_TRUNCATE] {
            let ctx = context(flags);
            let converted = convert_expr_with_context(&expr, &ctx).unwrap();
            let result = eval_datum(&converted, &row);
            if flags == 0 {
                assert!(result.is_err());
            } else {
                assert_eq!(result.unwrap(), Datum::Int(12));
            }
            let warnings = ctx.take_warnings();
            assert_eq!(
                warnings.len(),
                usize::from(flags == FLAG_TRUNCATE_AS_WARNING)
            );
            if let Some((code, _)) = warnings.first() {
                assert_eq!(*code, 1292);
            }
            assert!(ctx.take_warnings().is_empty());
        }
    }
}

#[cfg(test)]
mod timestamp_decode_tests {
    use super::*;
    use tidb_proto::tipb;

    #[test]
    fn timestamp_literal_uses_request_zone_during_decoding() {
        let utc = tidb_datatype::Time::from_date_checked(
            2024,
            1,
            2,
            3,
            4,
            5,
            0,
            tidb_datatype::TimeType::Timestamp,
            0,
        )
        .unwrap();
        let mut val = Vec::new();
        tidb_codec::encode_uint(&mut val, utc.to_packed_uint().unwrap());
        let literal = tipb::Expr {
            tp: Some(tipb::ExprType::MysqlTime as i32),
            val: Some(val),
            field_type: Some(tipb::FieldType {
                tp: Some(7),
                decimal: Some(0),
                ..Default::default()
            }),
            ..Default::default()
        };
        let expr = tipb::Expr {
            tp: Some(tipb::ExprType::ScalarFunc as i32),
            sig: Some(tipb::ScalarFuncSig::UnixTimestampInt as i32),
            children: vec![literal],
            field_type: Some(tipb::FieldType {
                tp: Some(8),
                ..Default::default()
            }),
            ..Default::default()
        };
        let ctx = Arc::new(RequestEvalContext::new(
            SessionTimeZone::Named(chrono_tz::Asia::Shanghai),
            4,
            0,
        ));
        let direct = super::super::convert_expr_with_context(&expr.children[0], &ctx).unwrap();
        let local = tidb_datatype::Time::from_date_checked(
            2024,
            1,
            2,
            11,
            4,
            5,
            0,
            tidb_datatype::TimeType::Timestamp,
            0,
        )
        .unwrap();
        assert_eq!(
            super::super::eval_datum(&direct, &[]).unwrap(),
            Datum::Time(local)
        );
        let expr = super::super::convert_expr_with_context(&expr, &ctx).unwrap();
        assert_eq!(
            super::super::eval_datum(&expr, &[]).unwrap(),
            Datum::Int(1_704_164_645)
        );
    }
}

#[cfg(test)]
mod child_error_tests {
    use super::*;
    use tidb_proto::tipb;
    #[test]
    fn legacy_parent_keeps_typed_child_error() {
        let text = tipb::Expr {
            tp: Some(tipb::ExprType::String as i32),
            val: Some(b"not a number".to_vec()),
            field_type: Some(tipb::FieldType {
                tp: Some(253),
                ..Default::default()
            }),
            ..Default::default()
        };
        let cast = tipb::Expr {
            tp: Some(tipb::ExprType::ScalarFunc as i32),
            sig: Some(tipb::ScalarFuncSig::CastStringAsReal as i32),
            children: vec![text],
            field_type: Some(tipb::FieldType {
                tp: Some(5),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut zero = Vec::new();
        tidb_codec::encode_float(&mut zero, 0.0);
        let zero = tipb::Expr {
            tp: Some(tipb::ExprType::Float64 as i32),
            val: Some(zero),
            field_type: Some(tipb::FieldType {
                tp: Some(5),
                ..Default::default()
            }),
            ..Default::default()
        };
        let expr = tipb::Expr {
            tp: Some(tipb::ExprType::ScalarFunc as i32),
            sig: Some(tipb::ScalarFuncSig::GtReal as i32),
            children: vec![cast, zero],
            field_type: Some(tipb::FieldType {
                tp: Some(8),
                ..Default::default()
            }),
            ..Default::default()
        };
        let ctx = Arc::new(RequestEvalContext::new(
            SessionTimeZone::Named(chrono_tz::UTC),
            4,
            0,
        ));
        let converted = super::super::convert_expr_with_context(&expr, &ctx).unwrap();
        let result = super::super::eval_expr(&converted, &[], 4, &ctx.zone);
        assert!(
            result.is_err(),
            "strict cast error must propagate, got {result:?}"
        );
        // Each legacy channel must preserve errors before inspecting the result's kind.
        let child = super::super::convert_expr_with_context(&expr.children[0], &ctx).unwrap();
        assert!(super::super::eval_real(Some(&child), &[], 4, &ctx.zone).is_err());
        assert!(super::super::eval_decimal(Some(&child), &[], 4, &ctx.zone).is_err());
        assert!(super::super::eval_bytes(Some(&child), &[], 4, &ctx.zone).is_err());
        assert!(super::super::eval_json(Some(&child), &[], 4, &ctx.zone).is_err());
        assert!(super::super::eval_time(Some(&child), &[], 4, &ctx.zone).is_err());
        assert!(super::super::eval_duration(Some(&child), &[], 4, &ctx.zone).is_err());
        for (sig, left, expected) in [
            (super::super::SimpleSig::LogicalAnd, 0, 0),
            (super::super::SimpleSig::LogicalOr, 1, 1),
        ] {
            let lazy = super::super::SimpleExpr::Func(
                sig,
                vec![super::super::SimpleExpr::Int(left), child.clone()],
            );
            assert_eq!(
                super::super::eval_expr(&lazy, &[], 4, &ctx.zone).unwrap(),
                Some(expected)
            );
        }
    }
}
#[cfg(test)]
mod next_boundary_audit {
    #[test]
    fn typed_pb_boundary_matches_go() {
        // The immutable Go capture covers results, error states, and warnings.
        // Full error-text identity and package completeness are separate gates.
        use prost::Message;
        use std::fmt::Write;
        fn unhex(s: &str) -> Vec<u8> {
            s.as_bytes()
                .chunks_exact(2)
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect()
        }
        fn hex(s: &[u8]) -> String {
            let mut out = String::new();
            for b in s {
                write!(&mut out, "{b:02x}").unwrap();
            }
            out
        }
        use std::io::Read;
        let mut input = String::new();
        flate2::read::GzDecoder::new(
            include_bytes!("../../../../docs/planner/pb-boundary-audit-20260929/go.tsv.gz")
                .as_slice(),
        )
        .read_to_string(&mut input)
        .unwrap();
        assert_eq!(input.lines().count(), 23_568);
        let mut differences = Vec::new();
        for line in input.lines() {
            let fields = line.split('\t').collect::<Vec<_>>();
            let name = fields[0];
            let flags = fields[1].parse::<u64>().unwrap();
            let context = RequestEvalContext::new(SessionTimeZone::utc(), 4, flags);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let pb = tidb_proto::tipb::Expr::decode(unhex(fields[2]).as_slice()).unwrap();
                let expr = match tidb_expr::distsql_builtin::pb_to_expr(&pb, &[]) {
                    Ok(expr) => expr,
                    Err(error) => return ("decode_error".to_owned(), error),
                };
                match expr.eval(&context, tidb_chunk::row::Row::empty()) {
                    Err(error) => ("error".to_owned(), format!("{error:?}")),
                    Ok(Datum::Null) => ("null".to_owned(), String::new()),
                    Ok(Datum::Real(value) | Datum::Float32(value)) => {
                        (format!("real:{:016x}", value.to_bits()), String::new())
                    }
                    Ok(value) => (
                        format!("value:{}", hex(&value.sql_bytes().unwrap())),
                        String::new(),
                    ),
                }
            }));
            let (result, detail) = result.unwrap_or_else(|_| ("panic".to_owned(), String::new()));
            let warnings = context.take_warnings();
            if result != fields[3] || warnings.len().to_string() != fields[5] {
                differences.push(format!("{name} flags={flags}: got {result}, {} warnings ({detail}); want {}, {} warnings", warnings.len(), fields[3], fields[5]));
            }
        }
        assert!(
            differences.is_empty(),
            "{} differences; first 30:\n{}",
            differences.len(),
            differences
                .iter()
                .take(30)
                .cloned()
                .collect::<Vec<_>>()
                .join("\n")
        );
    }

    #[test]
    fn typed_pb_conversion_matches_go() {
        use prost::Message;
        use std::fmt::Write;
        fn unhex(s: &str) -> Vec<u8> {
            s.as_bytes()
                .chunks_exact(2)
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect()
        }
        fn hex(s: &[u8]) -> String {
            let mut out = String::new();
            for b in s {
                write!(&mut out, "{b:02x}").unwrap();
            }
            out
        }
        let input = include_str!(
            "../../../../docs/planner/pb-systematic-audit-20260928/pb-systematic-go.tsv"
        );
        let mut differences = Vec::new();
        for line in input.lines() {
            let fields = line.split('\t').collect::<Vec<_>>();
            let name = fields[0];
            let signature = name.split('/').next().unwrap();
            if !signature.starts_with("Cast")
                || !(signature.ends_with("AsInt")
                    || signature.ends_with("AsReal")
                    || signature.ends_with("AsDecimal"))
            {
                continue;
            }
            let flags = fields[1].parse::<u64>().unwrap();
            let context = RequestEvalContext::new(SessionTimeZone::utc(), 4, flags);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let pb = tidb_proto::tipb::Expr::decode(unhex(fields[2]).as_slice()).unwrap();
                let expr = match tidb_expr::distsql_builtin::pb_to_expr(&pb, &[]) {
                    Ok(expr) => expr,
                    Err(error) => return ("decode_error".to_owned(), error),
                };
                match expr.eval(&context, tidb_chunk::row::Row::empty()) {
                    Err(tidb_expr::EvalError::Conversion(error)) => {
                        let expected = String::from_utf8(unhex(fields[4])).unwrap();
                        assert_eq!(error.to_string(), expected, "{name} flags={flags}");
                        ("error".to_owned(), error.to_string())
                    }
                    Err(error) => ("error".to_owned(), format!("{error:?}")),
                    Ok(Datum::Null) => ("null".to_owned(), String::new()),
                    Ok(Datum::Real(value) | Datum::Float32(value)) => {
                        (format!("real:{:016x}", value.to_bits()), String::new())
                    }
                    Ok(value) => (
                        format!("value:{}", hex(&value.sql_bytes().unwrap())),
                        String::new(),
                    ),
                }
            }));
            let (result, detail) = result.unwrap_or_else(|_| ("panic".to_owned(), String::new()));
            let warnings = context.take_warnings();
            if result != fields[3] || warnings.len().to_string() != fields[5] {
                differences.push(format!("{name} flags={flags}: got {result}, {} warnings ({detail}); want {}, {} warnings", warnings.len(), fields[3], fields[5]));
            }
        }
        assert!(differences.is_empty(), "{}", differences.join("\n"));
    }

    use super::*;
    use tidb_proto::tipb;

    #[test]
    fn pb_contract_string_controls() {
        for sig in [
            tipb::ScalarFuncSig::IfString,
            tipb::ScalarFuncSig::CaseWhenString,
        ] {
            let text = |v: &str| tipb::Expr {
                tp: Some(tipb::ExprType::String as i32),
                val: Some(v.as_bytes().to_vec()),
                field_type: Some(ty(253)),
                ..Default::default()
            };
            let mut pb = call(sig, vec![int(1), text("yes"), text("no")]);
            pb.field_type = Some(ty(253));
            let expr = tidb_expr::distsql_builtin::pb_to_expr(&pb, &[]).unwrap();
            assert_eq!(
                expr.eval(ctx().as_ref(), tidb_chunk::row::Row::empty())
                    .unwrap()
                    .sql_bytes()
                    .unwrap(),
                b"yes"
            );
        }
    }
    #[test]
    fn pb_contract_conv_evaluates_bases_before_null_text() {
        let text = tipb::Expr {
            tp: Some(tipb::ExprType::String as i32),
            val: Some(b"invalid".to_vec()),
            field_type: Some(ty(253)),
            ..Default::default()
        };
        let bad = call(tipb::ScalarFuncSig::CastStringAsInt, vec![text]);
        let mut pb = call(
            tipb::ScalarFuncSig::Conv,
            vec![tipb::Expr::default(), bad, int(10)],
        );
        pb.field_type = Some(ty(253));
        let expr = tidb_expr::distsql_builtin::pb_to_expr(&pb, &[]).unwrap();
        assert!(expr
            .eval(ctx().as_ref(), tidb_chunk::row::Row::empty())
            .is_err());
    }
    #[test]
    fn pb_contract_real_cast_keeps_precision() {
        let text = tipb::Expr {
            tp: Some(tipb::ExprType::String as i32),
            val: Some(b"1.26".to_vec()),
            field_type: Some(ty(253)),
            ..Default::default()
        };
        let mut pb = call(tipb::ScalarFuncSig::CastStringAsReal, vec![text]);
        let mut ft = ty(5);
        ft.flen = Some(3);
        ft.decimal = Some(1);
        pb.field_type = Some(ft);
        let expr = tidb_expr::distsql_builtin::pb_to_expr(&pb, &[]).unwrap();
        assert_eq!(
            expr.eval(ctx().as_ref(), tidb_chunk::row::Row::empty())
                .unwrap(),
            Datum::Real(1.3)
        );
    }

    #[test]
    fn pb_contract_conv_order_and_warning_policy() {
        let text = |v: &str| tipb::Expr {
            tp: Some(tipb::ExprType::String as i32),
            val: Some(v.as_bytes().to_vec()),
            field_type: Some(ty(253)),
            ..Default::default()
        };
        let bad_int = call(tipb::ScalarFuncSig::CastStringAsInt, vec![text("invalid")]);
        let bad_text = call(tipb::ScalarFuncSig::CastIntAsString, vec![bad_int.clone()]);
        for (args, errors) in [
            (vec![tipb::Expr::default(), bad_int.clone(), int(10)], true),
            (
                vec![bad_text.clone(), tipb::Expr::default(), bad_int.clone()],
                false,
            ),
            (vec![bad_text, int(10), tipb::Expr::default()], false),
            (vec![tipb::Expr::default(), int(10), bad_int], true),
        ] {
            let mut pb = call(tipb::ScalarFuncSig::Conv, args);
            pb.field_type = Some(ty(253));
            let expr = tidb_expr::distsql_builtin::pb_to_expr(&pb, &[]).unwrap();
            for flags in [0, FLAG_TRUNCATE_AS_WARNING, FLAG_IGNORE_TRUNCATE] {
                let context = RequestEvalContext::new(SessionTimeZone::utc(), 4, flags);
                let result = expr.eval(&context, tidb_chunk::row::Row::empty());
                if errors && flags == 0 {
                    assert!(matches!(
                        result,
                        Err(tidb_expr::EvalError::TruncatedWrongValue(_))
                    ));
                } else {
                    assert_eq!(result.unwrap(), Datum::Null);
                }
                assert_eq!(
                    context.take_warnings().len(),
                    usize::from(errors && flags == FLAG_TRUNCATE_AS_WARNING)
                );
            }
        }
    }
    #[test]
    fn pb_contract_real_production_has_source_specific_errors() {
        for flags in [0, FLAG_TRUNCATE_AS_WARNING, FLAG_IGNORE_TRUNCATE] {
            for (input, unsigned, expected) in [
                ("1.26", false, Some(1.3)),
                ("999", false, None),
                ("-1.26", true, None),
            ] {
                let text = tipb::Expr {
                    tp: Some(tipb::ExprType::String as i32),
                    val: Some(input.as_bytes().to_vec()),
                    field_type: Some(ty(253)),
                    ..Default::default()
                };
                let mut pb = call(tipb::ScalarFuncSig::CastStringAsReal, vec![text]);
                let mut ft = ty(5);
                ft.flen = Some(3);
                ft.decimal = Some(1);
                ft.flag = Some(if unsigned { 32 } else { 0 });
                pb.field_type = Some(ft.clone());
                let context = RequestEvalContext::new(SessionTimeZone::utc(), 4, flags);
                let result = tidb_expr::distsql_builtin::pb_to_expr(&pb, &[])
                    .unwrap()
                    .eval(&context, tidb_chunk::row::Row::empty());
                if let Some(expected) = expected {
                    assert_eq!(result.unwrap(), Datum::Real(expected));
                } else {
                    assert!(matches!(result, Err(tidb_expr::EvalError::Conversion(_))));
                }
                assert!(context.take_warnings().is_empty());
                // Go CastRealAsReal does not run the string producer.
                pb.sig = Some(tipb::ScalarFuncSig::CastRealAsReal as i32);
                pb.children = vec![real(1.26)];
                assert_eq!(
                    tidb_expr::distsql_builtin::pb_to_expr(&pb, &[])
                        .unwrap()
                        .eval(&context, tidb_chunk::row::Row::empty())
                        .unwrap(),
                    Datum::Real(1.26)
                );
            }
        }
    }
    fn ty(tp: i32) -> tipb::FieldType {
        tipb::FieldType {
            tp: Some(tp),
            decimal: Some(0),
            ..Default::default()
        }
    }
    fn int(v: i64) -> tipb::Expr {
        let mut b = Vec::new();
        tidb_codec::encode_int(&mut b, v);
        tipb::Expr {
            tp: Some(tipb::ExprType::Int64 as i32),
            val: Some(b),
            field_type: Some(ty(8)),
            ..Default::default()
        }
    }
    fn real(v: f64) -> tipb::Expr {
        let mut b = Vec::new();
        tidb_codec::encode_float(&mut b, v);
        tipb::Expr {
            tp: Some(tipb::ExprType::Float64 as i32),
            val: Some(b),
            field_type: Some(ty(5)),
            ..Default::default()
        }
    }
    fn call(sig: tipb::ScalarFuncSig, args: Vec<tipb::Expr>) -> tipb::Expr {
        tipb::Expr {
            tp: Some(tipb::ExprType::ScalarFunc as i32),
            sig: Some(sig as i32),
            children: args,
            field_type: Some(ty(8)),
            ..Default::default()
        }
    }
    fn ctx() -> Arc<RequestEvalContext> {
        Arc::new(RequestEvalContext::new(SessionTimeZone::utc(), 4, 0))
    }
    #[test]
    fn a_supported_child_remains_supported_under_a_shared_parent() {
        let ctx = ctx();
        let child = call(tipb::ScalarFuncSig::GtReal, vec![real(1.0), real(0.0)]);
        let alone = super::super::convert_expr_with_context(&child, &ctx).unwrap();
        assert_eq!(
            super::super::eval_expr(&alone, &[], 4, &ctx.zone).unwrap(),
            Some(1)
        );
        let parent = call(tipb::ScalarFuncSig::IfInt, vec![child, int(1), int(0)]);
        let expr = super::super::convert_expr_with_context(&parent, &ctx)
            .expect("Go recursively decodes supported signatures");
        assert_eq!(
            super::super::eval_expr(&expr, &[], 4, &ctx.zone).unwrap(),
            Some(1)
        );
    }
    #[test]
    fn empty_value_list_returns_false_like_go() {
        let ctx = ctx();
        let list = tipb::Expr {
            tp: Some(tipb::ExprType::ValueList as i32),
            val: Some(Vec::new()),
            ..Default::default()
        };
        let pb = call(tipb::ScalarFuncSig::InInt, vec![int(1), list]);
        let expr = super::super::convert_expr_with_context(&pb, &ctx)
            .expect("Go expands ValueList into constant arguments");
        assert_eq!(
            super::super::eval_expr(&expr, &[], 4, &ctx.zone).unwrap(),
            Some(0)
        );
    }
    #[test]
    fn null_comparison_still_reports_the_other_operands_error() {
        let ctx = ctx();
        let bad = tipb::Expr {
            tp: Some(tipb::ExprType::String as i32),
            val: Some(b"invalid".to_vec()),
            field_type: Some(ty(253)),
            ..Default::default()
        };
        let cast = call(tipb::ScalarFuncSig::CastStringAsInt, vec![bad]);
        let alone = super::super::convert_expr_with_context(&cast, &ctx).unwrap();
        assert!(super::super::eval_expr(&alone, &[], 4, &ctx.zone).is_err());
        let null = tipb::Expr {
            tp: Some(tipb::ExprType::Null as i32),
            ..Default::default()
        };
        for sig in [
            tipb::ScalarFuncSig::EqInt,
            tipb::ScalarFuncSig::NeInt,
            tipb::ScalarFuncSig::LtInt,
            tipb::ScalarFuncSig::LeInt,
            tipb::ScalarFuncSig::GtInt,
            tipb::ScalarFuncSig::GeInt,
        ] {
            for args in [
                vec![null.clone(), cast.clone()],
                vec![cast.clone(), null.clone()],
            ] {
                let pb = call(sig, args);
                let expr = super::super::convert_expr_with_context(&pb, &ctx).unwrap();
                let result = super::super::eval_expr(&expr, &[], 4, &ctx.zone);
                assert!(
                    result.is_err(),
                    "Go evaluates both comparison operands, {sig:?}: {result:?}"
                );
            }
        }
        for (sig, left, expected) in [
            (tipb::ScalarFuncSig::LogicalAnd, 0, 0),
            (tipb::ScalarFuncSig::LogicalOr, 1, 1),
        ] {
            let expr = super::super::convert_expr_with_context(
                &call(sig, vec![int(left), cast.clone()]),
                &ctx,
            )
            .unwrap();
            assert_eq!(
                super::super::eval_expr(&expr, &[], 4, &ctx.zone).unwrap(),
                Some(expected)
            );
        }
    }
    #[test]
    fn audit_typed_if_preserves_conversion_errors() {
        let context = ctx();
        let bad = tipb::Expr {
            tp: Some(tipb::ExprType::String as i32),
            val: Some(b"invalid".to_vec()),
            field_type: Some(ty(253)),
            ..Default::default()
        };
        let pb = call(tipb::ScalarFuncSig::IfInt, vec![int(1), bad, int(0)]);
        let expr = tidb_expr::distsql_builtin::pb_to_expr(&pb, &[]).unwrap();
        let result = expr.eval(context.as_ref(), tidb_chunk::row::Row::empty());
        assert!(
            matches!(result, Err(tidb_expr::EvalError::Conversion(_))),
            "Go typed IF returns conversion error: {result:?}"
        );
    }
    #[test]
    fn audit_typed_is_null_evaluates_its_domain() {
        let context = ctx();
        let bad = tipb::Expr {
            tp: Some(tipb::ExprType::String as i32),
            val: Some(b"invalid".to_vec()),
            field_type: Some(ty(253)),
            ..Default::default()
        };
        let pb = call(tipb::ScalarFuncSig::RealIsNull, vec![bad]);
        let expr = tidb_expr::distsql_builtin::pb_to_expr(&pb, &[]).unwrap();
        let result = expr.eval(context.as_ref(), tidb_chunk::row::Row::empty());
        assert!(
            matches!(result, Err(tidb_expr::EvalError::Conversion(_))),
            "Go EvalReal propagates conversion error: {result:?}"
        );
    }
    #[test]
    fn audit_binary_varstring_cast_respects_wire_length() {
        let input = tipb::Expr {
            tp: Some(tipb::ExprType::String as i32),
            val: Some(b"abcd".to_vec()),
            field_type: Some(ty(253)),
            ..Default::default()
        };
        let mut pb = call(tipb::ScalarFuncSig::CastStringAsString, vec![input]);
        pb.field_type = Some(tipb::FieldType {
            tp: Some(253),
            flen: Some(2),
            charset: Some("binary".into()),
            collate: Some(63),
            flag: Some(128),
            ..Default::default()
        });
        let expr = tidb_expr::distsql_builtin::pb_to_expr(&pb, &[]).unwrap();
        let result = expr
            .eval(&tidb_expr::NoColumns, tidb_chunk::row::Row::empty())
            .unwrap();
        assert_eq!(result.sql_bytes().unwrap(), b"ab");
    }
    #[test]
    fn audit_decoded_string_function_derives_coercibility() {
        let text = |v: &[u8]| tipb::Expr {
            tp: Some(tipb::ExprType::String as i32),
            val: Some(v.to_vec()),
            field_type: Some(tipb::FieldType {
                tp: Some(253),
                charset: Some("utf8mb4".into()),
                collate: Some(46),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut pb = call(
            tipb::ScalarFuncSig::IfNullString,
            vec![text(b"x"), text(b"y")],
        );
        pb.field_type = text(b"").field_type;
        let expr = tidb_expr::distsql_builtin::pb_to_expr(&pb, &[]).unwrap();
        assert_eq!(tidb_expr::collation_derive::coercibility_of(&expr).0, 4);
    }

    #[test]
    fn typed_control_families_keep_request_warning_policy_and_laziness() {
        use tipb::ScalarFuncSig::*;
        let text = |v: &str| tipb::Expr {
            tp: Some(tipb::ExprType::String as i32),
            val: Some(v.as_bytes().to_vec()),
            field_type: Some(ty(253)),
            ..Default::default()
        };
        let null = tipb::Expr::default();
        for pb in [
            call(IfInt, vec![int(1), text("invalid"), int(0)]),
            call(IfNullInt, vec![text("invalid"), int(1)]),
            call(CaseWhenInt, vec![int(1), text("invalid"), int(0)]),
            call(IntIsNull, vec![text("invalid")]),
            call(RealIsNull, vec![text("invalid")]),
        ] {
            let expr = tidb_expr::distsql_builtin::pb_to_expr(&pb, &[]).unwrap();
            for (flags, warning_count) in [
                (0, 0),
                (FLAG_TRUNCATE_AS_WARNING, 1),
                (FLAG_IGNORE_TRUNCATE, 0),
            ] {
                let context = RequestEvalContext::new(SessionTimeZone::utc(), 4, flags);
                let result = expr.eval(&context, tidb_chunk::row::Row::empty());
                if flags == 0 {
                    assert!(result.is_err(), "{:?}: {result:?}", pb.sig());
                } else {
                    assert_eq!(result.unwrap(), Datum::Int(0), "{:?}", pb.sig());
                }
                let warnings = context.take_warnings();
                assert_eq!(
                    warnings.len(),
                    warning_count,
                    "{:?}: {warnings:?}",
                    pb.sig()
                );
                if warning_count != 0 {
                    assert_eq!(warnings[0].0, 1292);
                }
            }
        }
        for pb in [
            call(IfInt, vec![int(0), text("invalid"), int(7)]),
            call(IfNullInt, vec![int(7), text("invalid")]),
            call(CaseWhenInt, vec![null.clone(), text("invalid"), int(7)]),
            call(
                CaseWhenInt,
                vec![int(0), text("invalid"), int(1), int(7), text("invalid")],
            ),
        ] {
            let context = ctx();
            let expr = tidb_expr::distsql_builtin::pb_to_expr(&pb, &[]).unwrap();
            assert_eq!(
                expr.eval(context.as_ref(), tidb_chunk::row::Row::empty())
                    .unwrap(),
                Datum::Int(7)
            );
            assert!(context.take_warnings().is_empty());
        }
    }

    #[test]
    fn string_cast_field_types_preserve_bytes_padding_and_diagnostics() {
        let input = |bytes: &[u8]| tipb::Expr {
            tp: Some(tipb::ExprType::String as i32),
            val: Some(bytes.to_vec()),
            field_type: Some(ty(253)),
            ..Default::default()
        };
        for (code, bytes, width, expected, warned) in [
            (253, b"abcd".as_slice(), 2, b"ab".as_slice(), true),
            (253, b"a".as_slice(), 3, b"a".as_slice(), false),
            (254, b"a".as_slice(), 3, b"a\0\0".as_slice(), false),
            (
                253,
                b"\xff\xfe\x00".as_slice(),
                2,
                b"\xff\xfe".as_slice(),
                true,
            ),
            (253, b"abcd".as_slice(), -1, b"abcd".as_slice(), false),
        ] {
            for flags in [0, FLAG_TRUNCATE_AS_WARNING, FLAG_IGNORE_TRUNCATE] {
                let context = RequestEvalContext::new(SessionTimeZone::utc(), 4, flags);
                let mut pb = call(tipb::ScalarFuncSig::CastStringAsString, vec![input(bytes)]);
                pb.field_type = Some(tipb::FieldType {
                    tp: Some(code),
                    flen: Some(width),
                    charset: Some("binary".into()),
                    collate: Some(63),
                    flag: Some(128),
                    ..Default::default()
                });
                let expr = tidb_expr::distsql_builtin::pb_to_expr(&pb, &[]).unwrap();
                let result = expr.eval(&context, tidb_chunk::row::Row::empty());
                if warned && flags == 0 {
                    assert!(result.is_err());
                } else {
                    assert_eq!(result.unwrap().sql_bytes().unwrap(), expected);
                }
                let warnings = context.take_warnings();
                assert_eq!(
                    warnings.len(),
                    usize::from(warned && flags == FLAG_TRUNCATE_AS_WARNING)
                );
                if !warnings.is_empty() {
                    assert_eq!(warnings[0].0, 1406);
                }
            }
        }
    }

    #[test]
    fn fixed_binary_padding_checks_packet_limit_before_allocation() {
        let input = tipb::Expr {
            tp: Some(tipb::ExprType::String as i32),
            val: Some(b"a".to_vec()),
            field_type: Some(ty(253)),
            ..Default::default()
        };
        let mut pb = call(tipb::ScalarFuncSig::CastStringAsString, vec![input]);
        pb.field_type = Some(tipb::FieldType {
            tp: Some(254),
            flen: Some(i32::MAX),
            charset: Some("binary".into()),
            collate: Some(63),
            flag: Some(128),
            ..Default::default()
        });
        let expr = tidb_expr::distsql_builtin::pb_to_expr(&pb, &[]).unwrap();
        let context = RequestEvalContext::new(SessionTimeZone::utc(), 4, FLAG_TRUNCATE_AS_WARNING);
        assert_eq!(
            expr.eval(&context, tidb_chunk::row::Row::empty()).unwrap(),
            Datum::Null
        );
        let warnings = context.take_warnings();
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].0, 1301);
    }
}

#[cfg(test)]
#[path = "region_aggregate_tests.rs"]
mod region_aggregate_tests;
