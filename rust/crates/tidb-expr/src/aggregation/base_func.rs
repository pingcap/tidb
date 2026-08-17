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

//! Go `pkg/expression/aggregation/base_func.go`: the function signature every
//! aggregate and window descriptor embeds, and the type inference that fills
//! its return type.
//!
//! See [`super`] for the symbol table, the COMPLETE-vs-SEED label and every
//! narrowing.

use super::names;
use super::wrap_cast::{
    self, set_bin_chs_cln_flag, type_of, MAX_BLOB_WIDTH, MAX_FIELD_CHAR_LENGTH, MAX_REAL_WIDTH,
    NOT_FIXED_DEC,
};
use crate::context::{Columns, EvalError};
use crate::expression::{ConstLevel, Expression};
use tidb_datatype::{
    Datum, EvalType, FieldType, FieldTypeCode, FieldTypeFlags, MAX_DECIMAL_SCALE,
    MAX_DECIMAL_WIDTH, UNSPECIFIED_LENGTH,
};

/// The errors Go's `baseFuncDesc`/`AggFuncDesc` methods return through
/// `errors.Errorf`/`errors.New`.
///
/// Go has one untyped `error` here; naming each case keeps the caller able to
/// tell "this name is not an aggregate" apart from "this aggregate's argument
/// is wrong", which the planner reports differently.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AggDescError {
    /// Go `errors.Errorf("unsupported agg function: %s", name)`
    /// (`base_func.go:157`, `descriptor.go:379`).
    UnsupportedAggFunction(String),
    /// Go `errors.New("APPROX_PERCENTILE should take 2 arguments")`.
    ApproxPercentileArgCount,
    /// Go `errors.New("APPROX_PERCENTILE should take a constant expression as
    /// percentage argument")`.
    ApproxPercentileNotConstant,
    /// Go `errors.New("APPROX_PERCENTILE: Percentage value cannot be NULL")`.
    ApproxPercentileNullPercentage,
    /// Go `fmt.Errorf("Percentage value %d is out of range [1, 100]", pct)`.
    ApproxPercentileOutOfRange(i64),
    /// Go `fmt.Errorf("APPROX_PERCENTILE: Invalid argument %s", arg)`, raised
    /// when the percentage argument cannot be read as an integer.
    ApproxPercentileInvalidArgument,
    /// Go `errors.New("sum_int should take 1 argument")`.
    SumIntArgCount,
    /// Go `errors.New("sum_int only accepts integer arguments")`.
    SumIntNonInteger,
    /// Go `errors.Errorf("expect sum func, but got %s", name)`
    /// (`TypeInfer4AvgSum`).
    ExpectSumFunc(String),
    /// An aggregate whose inference reads `Args[0]` was built with none. Go
    /// panics with a nil dereference here; a typed error is strictly safer
    /// and reaches the same "this descriptor is malformed" conclusion.
    MissingArgument(&'static str),
    /// A cast wrapper or collation derivation failed.
    Expr(EvalError),
}

impl From<EvalError> for AggDescError {
    fn from(value: EvalError) -> Self {
        AggDescError::Expr(value)
    }
}

impl std::fmt::Display for AggDescError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedAggFunction(name) => {
                write!(f, "unsupported agg function: {name}")
            }
            Self::ApproxPercentileArgCount => {
                write!(f, "APPROX_PERCENTILE should take 2 arguments")
            }
            Self::ApproxPercentileNotConstant => write!(
                f,
                "APPROX_PERCENTILE should take a constant expression as percentage argument"
            ),
            Self::ApproxPercentileNullPercentage => {
                write!(f, "APPROX_PERCENTILE: Percentage value cannot be NULL")
            }
            Self::ApproxPercentileOutOfRange(value) => {
                write!(f, "Percentage value {value} is out of range [1, 100]")
            }
            Self::ApproxPercentileInvalidArgument => {
                write!(f, "APPROX_PERCENTILE: Invalid argument")
            }
            Self::SumIntArgCount => write!(f, "sum_int should take 1 argument"),
            Self::SumIntNonInteger => write!(f, "sum_int only accepts integer arguments"),
            Self::ExpectSumFunc(name) => write!(f, "expect sum func, but got {name}"),
            Self::MissingArgument(name) => {
                write!(f, "agg function {name} requires at least one argument")
            }
            Self::Expr(err) => write!(f, "{err:?}"),
        }
    }
}

impl std::error::Error for AggDescError {}

/// Go `baseFuncDesc` (`base_func.go:34`): a function signature -- name,
/// arguments and inferred return type.
///
/// `RetTp` is a value here, not Go's nullable pointer; see [`super`]'s
/// narrowings.
#[derive(Clone, Debug)]
pub struct BaseFuncDesc {
    /// Go `Name`, always lower-cased by [`BaseFuncDesc::new`].
    pub name: String,
    /// Go `Args`. `TypeInfer` REWRITES entries in place for the aggregates
    /// that wrap their arguments in casts.
    pub args: Vec<Expression>,
    /// Go `RetTp`.
    pub ret_type: FieldType,
}

impl BaseFuncDesc {
    /// Go `newBaseFuncDesc` (`base_func.go:44`): lower-case the name, then
    /// infer.
    pub fn new(
        ctx: &impl Columns,
        name: &str,
        args: Vec<Expression>,
    ) -> Result<Self, AggDescError> {
        let mut desc = BaseFuncDesc {
            name: name.to_ascii_lowercase(),
            args,
            // Go leaves `RetTp` nil until `TypeInfer` assigns it; the
            // `Unspecified` type is the value-typed equivalent and is
            // overwritten by every successful `type_infer` arm.
            ret_type: FieldType::new(FieldTypeCode::Unspecified),
        };
        desc.type_infer(ctx)?;
        Ok(desc)
    }

    /// Builds a descriptor from parts WITHOUT inferring, the shape Go writes
    /// literally in `NewAggFuncDescForWindowFunc` (`descriptor.go:60`) and
    /// `PBExprToAggFuncDesc` (`agg_to_pb.go:214`).
    #[must_use]
    pub fn from_parts(name: String, args: Vec<Expression>, ret_type: FieldType) -> Self {
        BaseFuncDesc {
            name,
            args,
            ret_type,
        }
    }

    /// Go `baseFuncDesc.Equals` (`base_func.go:66`): structural equality, the
    /// `base.Equals` implementation.
    #[must_use]
    pub fn equals(&self, other: &BaseFuncDesc) -> bool {
        if self.name != other.name
            || self.args.len() != other.args.len()
            || !self.ret_type.equal(&other.ret_type)
        {
            return false;
        }
        self.args
            .iter()
            .zip(other.args.iter())
            .all(|(a, b)| a.equal(b))
    }

    /// Go `baseFuncDesc.equal` (`base_func.go:88`): name plus per-argument
    /// `Expression.Equal`, WITHOUT comparing the return type.
    #[must_use]
    pub fn equal(&self, other: &BaseFuncDesc) -> bool {
        self.name == other.name
            && self.args.len() == other.args.len()
            && self
                .args
                .iter()
                .zip(other.args.iter())
                .all(|(a, b)| a.equal(b))
    }

    /// Go `baseFuncDesc.TypeInfer` (`base_func.go:122`): infer the return
    /// type, and for some aggregates rewrite the arguments.
    pub fn type_infer(&mut self, ctx: &impl Columns) -> Result<(), AggDescError> {
        match self.name.as_str() {
            names::COUNT => self.type_infer_4_count(),
            // Go `typeInfer4ApproxCountDistinct` delegates verbatim.
            names::APPROX_COUNT_DISTINCT => self.type_infer_4_count(),
            names::APPROX_PERCENTILE => self.type_infer_4_approx_percentile()?,
            names::SUM => self.type_infer_4_sum()?,
            names::SUM_INT => self.type_infer_4_sum_int()?,
            names::AVG => self.type_infer_4_avg(ctx)?,
            names::GROUP_CONCAT => self.type_infer_4_group_concat(ctx)?,
            names::MAX
            | names::MIN
            | names::FIRST_ROW
            | names::FIRST_VALUE
            | names::LAST_VALUE
            | names::NTH_VALUE => self.type_infer_4_max_min()?,
            names::BIT_AND | names::BIT_OR | names::BIT_XOR => self.type_infer_4_bit_funcs()?,
            names::ROW_NUMBER | names::RANK | names::DENSE_RANK => self.type_infer_4_number_funcs(),
            names::CUME_DIST => self.type_infer_4_cume_dist(),
            names::NTILE => self.type_infer_4_ntile(),
            names::PERCENT_RANK => self.type_infer_4_percent_rank(),
            names::LEAD | names::LAG => self.type_infer_4_lead_lag()?,
            names::VAR_POP | names::STDDEV_POP | names::VAR_SAMP | names::STDDEV_SAMP => {
                self.type_infer_4_pop_or_samp();
            }
            names::JSON_ARRAYAGG => self.type_infer_4_json_array_agg(),
            names::JSON_OBJECTAGG => self.type_infer_4_json_object_agg(ctx)?,
            other => return Err(AggDescError::UnsupportedAggFunction(other.to_owned())),
        }
        Ok(())
    }

    /// The first argument's static type, or `MissingArgument` where Go would
    /// nil-dereference.
    fn arg0_type(&self, who: &'static str) -> Result<FieldType, AggDescError> {
        self.args
            .first()
            .map(type_of)
            .ok_or(AggDescError::MissingArgument(who))
    }

    /// Go `typeInfer4Count` (`base_func.go:161`).
    fn type_infer_4_count(&mut self) {
        let mut ret = FieldType::new(FieldTypeCode::LongLong);
        ret.set_flen(21);
        ret.set_decimal(0);
        // COUNT never returns NULL.
        ret.add_flags(FieldTypeFlags::NOT_NULL);
        set_bin_chs_cln_flag(&mut ret);
        self.ret_type = ret;
    }

    /// Go `typeInfer4ApproxPercentile` (`base_func.go:174`).
    ///
    /// Two narrowings, both named in [`super`]: the constant test is
    /// `ConstLevel == STRICT` (Go: `!= ConstNone`), and the percentage is
    /// read off a `Constant` node rather than through `EvalInt`'s implicit
    /// conversion, so a percentage spelled as a string literal is rejected
    /// where Go would convert it.
    fn type_infer_4_approx_percentile(&mut self) -> Result<(), AggDescError> {
        if self.args.len() != 2 {
            return Err(AggDescError::ApproxPercentileArgCount);
        }
        if self.args[1].const_level() != ConstLevel::STRICT {
            return Err(AggDescError::ApproxPercentileNotConstant);
        }
        let Expression::Constant(percent_const) = &self.args[1] else {
            return Err(AggDescError::ApproxPercentileNotConstant);
        };
        let (percent, is_null) = match &percent_const.value {
            Datum::Null => (0, true),
            Datum::Int(v) => (*v, false),
            Datum::UInt(v) => (i64::try_from(*v).unwrap_or(i64::MAX), false),
            _ => return Err(AggDescError::ApproxPercentileInvalidArgument),
        };
        if percent <= 0 || percent > 100 || is_null {
            if is_null {
                return Err(AggDescError::ApproxPercentileNullPercentage);
            }
            return Err(AggDescError::ApproxPercentileOutOfRange(percent));
        }

        let arg = self.arg0_type(names::APPROX_PERCENTILE)?;
        self.ret_type = match arg.code() {
            FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong => FieldType::new(FieldTypeCode::LongLong),
            FieldTypeCode::Double | FieldTypeCode::Float => FieldType::new(FieldTypeCode::Double),
            FieldTypeCode::NewDecimal => {
                let mut ret = FieldType::new(FieldTypeCode::NewDecimal);
                ret.set_flen(MAX_DECIMAL_WIDTH);
                ret.set_decimal(arg.decimal());
                if ret.decimal() < 0 || ret.decimal() > MAX_DECIMAL_SCALE {
                    ret.set_decimal(MAX_DECIMAL_SCALE);
                }
                ret
            }
            FieldTypeCode::Date
            | FieldTypeCode::Datetime
            | FieldTypeCode::NewDate
            | FieldTypeCode::Timestamp => arg,
            _ => {
                let mut ret = arg;
                ret.del_flags(FieldTypeFlags::NOT_NULL);
                ret
            }
        };
        Ok(())
    }

    /// Go `typeInfer4Sum` (`base_func.go:215`): DECIMAL for integer/decimal
    /// inputs, DOUBLE otherwise.
    fn type_infer_4_sum(&mut self) -> Result<(), AggDescError> {
        let arg = self.arg0_type(names::SUM)?;
        let mut ret = match arg.code() {
            FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong
            | FieldTypeCode::Year => {
                let mut ret = FieldType::new(FieldTypeCode::NewDecimal);
                ret.set_flen_under_limit(arg.flen() + 21);
                ret.set_decimal(0);
                if arg.flen() < 0 {
                    ret.set_flen(MAX_DECIMAL_WIDTH);
                }
                ret
            }
            FieldTypeCode::NewDecimal => {
                let mut ret = FieldType::new(FieldTypeCode::NewDecimal);
                ret.update_flen_and_decimal_under_limit(&arg, 0, 22);
                ret
            }
            FieldTypeCode::Double | FieldTypeCode::Float => {
                let mut ret = FieldType::new(FieldTypeCode::Double);
                ret.set_flen(MAX_REAL_WIDTH);
                ret.set_decimal(arg.decimal());
                ret
            }
            _ => {
                let mut ret = FieldType::new(FieldTypeCode::Double);
                ret.set_flen(MAX_REAL_WIDTH);
                ret.set_decimal(UNSPECIFIED_LENGTH);
                ret
            }
        };
        set_bin_chs_cln_flag(&mut ret);
        self.ret_type = ret;
        Ok(())
    }

    /// Go `typeInfer4SumInt` (`base_func.go:238`).
    fn type_infer_4_sum_int(&mut self) -> Result<(), AggDescError> {
        if self.args.len() != 1 {
            return Err(AggDescError::SumIntArgCount);
        }
        let arg = self.arg0_type(names::SUM_INT)?;
        if !matches!(
            arg.code(),
            FieldTypeCode::Tiny
                | FieldTypeCode::Short
                | FieldTypeCode::Int24
                | FieldTypeCode::Long
                | FieldTypeCode::LongLong
        ) {
            return Err(AggDescError::SumIntNonInteger);
        }
        let mut ret = FieldType::new(FieldTypeCode::LongLong);
        ret.set_flen(21);
        ret.set_decimal(0);
        if arg.flags() & FieldTypeFlags::UNSIGNED != 0 {
            ret.add_flags(FieldTypeFlags::UNSIGNED);
        }
        set_bin_chs_cln_flag(&mut ret);
        self.ret_type = ret;
        Ok(())
    }

    /// Go `TypeInfer4AvgSum` (`base_func.go:258`): the return type of the
    /// PARTIAL `sum` that an `avg` is split into, whose decimal scale must
    /// stay MySQL-compatible.
    pub fn type_infer_4_avg_sum(&mut self, avg_ret_type: &FieldType) -> Result<(), AggDescError> {
        if self.name != names::SUM {
            return Err(AggDescError::ExpectSumFunc(self.name.clone()));
        }
        // A plain column needs no scale upgrade; a computed argument (e.g.
        // `avg(a/b)`) does, because `avg` already widened the scale by 4.
        if matches!(self.args.first(), Some(Expression::Column(_))) {
            self.type_infer_4_sum()?;
        } else if avg_ret_type.code() == FieldTypeCode::NewDecimal {
            self.ret_type
                .set_flen(MAX_DECIMAL_WIDTH.min(self.ret_type.flen() + 22));
        }
        Ok(())
    }

    /// Go `TypeInfer4FinalCount` (`base_func.go:277`).
    pub fn type_infer_4_final_count(&mut self, final_count_ret_type: &FieldType) {
        self.ret_type = final_count_ret_type.clone();
    }

    /// Go `typeInfer4Avg` (`base_func.go:283`).
    fn type_infer_4_avg(&mut self, ctx: &impl Columns) -> Result<(), AggDescError> {
        let div_prec_incre = i64::from(ctx.div_precision_increment());
        let arg = self.arg0_type(names::AVG)?;
        let mut ret = match arg.code() {
            FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong => {
                let mut ret = FieldType::new(FieldTypeCode::NewDecimal);
                ret.set_decimal_under_limit(div_prec_incre);
                let (flen, _) =
                    tidb_mysql::util::default_field_length_and_decimal(arg.code().mysql_type());
                ret.set_flen_under_limit(flen + div_prec_incre);
                ret
            }
            FieldTypeCode::Year | FieldTypeCode::NewDecimal => {
                let mut ret = FieldType::new(FieldTypeCode::NewDecimal);
                ret.update_flen_and_decimal_under_limit(&arg, div_prec_incre, div_prec_incre);
                ret
            }
            FieldTypeCode::Double | FieldTypeCode::Float => {
                let mut ret = FieldType::new(FieldTypeCode::Double);
                ret.set_flen(MAX_REAL_WIDTH);
                ret.set_decimal(arg.decimal());
                ret
            }
            FieldTypeCode::Date
            | FieldTypeCode::Duration
            | FieldTypeCode::Datetime
            | FieldTypeCode::Timestamp => {
                let mut ret = FieldType::new(FieldTypeCode::Double);
                ret.set_flen(MAX_REAL_WIDTH);
                ret.set_decimal(4);
                ret
            }
            _ => {
                let mut ret = FieldType::new(FieldTypeCode::Double);
                ret.set_flen(MAX_REAL_WIDTH);
                ret.set_decimal(UNSPECIFIED_LENGTH);
                ret
            }
        };
        set_bin_chs_cln_flag(&mut ret);
        self.ret_type = ret;
        Ok(())
    }

    /// Go `typeInfer4GroupConcat` (`base_func.go:310`).
    ///
    /// The connection fallback is Go's own: an empty derived charset or
    /// collation is filled from the session, and an empty session value is
    /// filled from `charset.GetDefaultCharsetAndCollate()`.
    fn type_infer_4_group_concat(&mut self, ctx: &impl Columns) -> Result<(), AggDescError> {
        let mut ret = FieldType::new(FieldTypeCode::VarString);
        let mut ec = crate::collation_derive::check_and_derive_collation_from_exprs(
            names::GROUP_CONCAT,
            EvalType::String,
            &self.args,
        )?;
        if ec.charset.is_empty() || ec.collation.is_empty() {
            let (conn_charset, conn_collation) = wrap_cast::connection_charset(ctx);
            if ec.charset.is_empty() {
                ec.charset.clone_from(&conn_charset);
            }
            if ec.collation.is_empty() {
                if ec.charset == conn_charset {
                    ec.collation = conn_collation;
                } else if let Some(coll) = default_collation_for_charset(&ec.charset) {
                    ec.collation = coll;
                } else {
                    ec.collation = conn_collation;
                }
            }
        }
        ret.set_charset_name(ec.charset);
        ret.set_collation_name(ec.collation);
        ret.set_flen(MAX_BLOB_WIDTH);
        ret.set_decimal(0);
        self.ret_type = ret;

        // Go's own TODO keeps this a decimal-only cast rather than
        // `WrapWithCastAsString`; the separator (the last argument) is never
        // wrapped.
        for i in 0..self.args.len().saturating_sub(1) {
            let tp = type_of(&self.args[i]);
            if tp.code() == FieldTypeCode::NewDecimal {
                let arg = std::mem::replace(
                    &mut self.args[i],
                    Expression::Constant(crate::constant::Constant::default()),
                );
                self.args[i] = wrap_cast::build_cast_to(arg, tp)?;
            }
        }
        Ok(())
    }

    /// Go `typeInfer4MaxMin` (`base_func.go:345`), shared by `MAX`/`MIN`/
    /// `FIRST_ROW`/`FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE` and reached again
    /// from `typeInfer4LeadLag`.
    fn type_infer_4_max_min(&mut self) -> Result<(), AggDescError> {
        let arg = self.arg0_type("max/min")?;
        // A scalar function's FLOAT result is stored in the Datum's float64
        // field, so extracting the argument into a Projection would break
        // without an explicit cast to DOUBLE.
        if matches!(self.args.first(), Some(Expression::ScalarFunction(_)))
            && arg.code() == FieldTypeCode::Float
        {
            let mut tp = FieldType::new(FieldTypeCode::Double);
            tp.set_flen(MAX_REAL_WIDTH);
            tp.set_decimal(UNSPECIFIED_LENGTH);
            set_bin_chs_cln_flag(&mut tp);
            let old = std::mem::replace(
                &mut self.args[0],
                Expression::Constant(crate::constant::Constant::default()),
            );
            self.args[0] = wrap_cast::build_cast_to(old, tp)?;
        }
        let arg = self.arg0_type("max/min")?;
        self.ret_type = arg;
        if matches!(
            self.name.as_str(),
            names::MAX | names::MIN | names::LEAD | names::LAG
        ) {
            self.ret_type.del_flags(FieldTypeFlags::NOT_NULL);
        }
        // TiDB issues #13027 and #13961.
        if matches!(
            self.ret_type.code(),
            FieldTypeCode::Enum | FieldTypeCode::Set
        ) && !matches!(
            self.name.as_str(),
            names::FIRST_ROW | names::MAX | names::MIN
        ) {
            let mut ret = FieldType::new(FieldTypeCode::String);
            ret.set_flen(MAX_FIELD_CHAR_LENGTH);
            self.ret_type = ret;
        }
        Ok(())
    }

    /// Go `typeInfer4BitFuncs` (`base_func.go:369`).
    fn type_infer_4_bit_funcs(&mut self) -> Result<(), AggDescError> {
        let mut ret = FieldType::new(FieldTypeCode::LongLong);
        ret.set_flen(21);
        set_bin_chs_cln_flag(&mut ret);
        ret.add_flags(FieldTypeFlags::UNSIGNED | FieldTypeFlags::NOT_NULL);
        self.ret_type = ret;
        if self.args.is_empty() {
            return Err(AggDescError::MissingArgument("bit_and/bit_or/bit_xor"));
        }
        let old = std::mem::replace(
            &mut self.args[0],
            Expression::Constant(crate::constant::Constant::default()),
        );
        self.args[0] = wrap_cast::wrap_with_cast_as_int(old, None)?;
        Ok(())
    }

    /// Go `typeInfer4JsonArrayAgg` (`base_func.go:376`).
    fn type_infer_4_json_array_agg(&mut self) {
        let mut ret = FieldType::new(FieldTypeCode::Json);
        set_bin_chs_cln_flag(&mut ret);
        self.ret_type = ret;
    }

    /// Go `typeInfer4JsonObjectAgg` (`base_func.go:381`).
    fn type_infer_4_json_object_agg(&mut self, ctx: &impl Columns) -> Result<(), AggDescError> {
        let mut ret = FieldType::new(FieldTypeCode::Json);
        set_bin_chs_cln_flag(&mut ret);
        self.ret_type = ret;
        if self.args.is_empty() {
            return Err(AggDescError::MissingArgument(names::JSON_OBJECTAGG));
        }
        let connection = wrap_cast::connection_charset(ctx);
        let old = std::mem::replace(
            &mut self.args[0],
            Expression::Constant(crate::constant::Constant::default()),
        );
        self.args[0] = wrap_cast::wrap_with_cast_as_string(old, (&connection.0, &connection.1))?;
        Ok(())
    }

    /// Go `typeInfer4NumberFuncs` (`base_func.go:388`).
    fn type_infer_4_number_funcs(&mut self) {
        let mut ret = FieldType::new(FieldTypeCode::LongLong);
        ret.set_flen(21);
        set_bin_chs_cln_flag(&mut ret);
        self.ret_type = ret;
    }

    /// Go `typeInfer4CumeDist` (`base_func.go:394`).
    fn type_infer_4_cume_dist(&mut self) {
        let mut ret = FieldType::new(FieldTypeCode::Double);
        ret.set_flen(MAX_REAL_WIDTH);
        ret.set_decimal(NOT_FIXED_DEC);
        self.ret_type = ret;
    }

    /// Go `typeInfer4Ntile` (`base_func.go:400`).
    fn type_infer_4_ntile(&mut self) {
        let mut ret = FieldType::new(FieldTypeCode::LongLong);
        ret.set_flen(21);
        set_bin_chs_cln_flag(&mut ret);
        ret.add_flags(FieldTypeFlags::UNSIGNED);
        self.ret_type = ret;
    }

    /// Go `typeInfer4PercentRank` (`base_func.go:407`).
    ///
    /// NOTE the source calls `SetFlag(mysql.MaxRealWidth)` -- `SetFlag`, not
    /// `SetFlen`, with a WIDTH constant. That is an upstream typo (it stamps
    /// flag bits `23` = `NOT_NULL|PRI_KEY|UNIQUE_KEY|BLOB` onto the type and
    /// leaves flen unset), and it is reproduced exactly, because the flags it
    /// produces are observable in `information_schema` output and in
    /// `UpdateNotNullFlag4RetType`'s behavior.
    fn type_infer_4_percent_rank(&mut self) {
        let mut ret = FieldType::new(FieldTypeCode::Double);
        #[expect(
            clippy::cast_possible_truncation,
            reason = "reproducing the source's SetFlag(MaxRealWidth) verbatim"
        )]
        ret.set_flags(MAX_REAL_WIDTH as u32);
        ret.set_decimal(NOT_FIXED_DEC);
        self.ret_type = ret;
    }

    /// Go `typeInfer4LeadLag` (`base_func.go:413`).
    fn type_infer_4_lead_lag(&mut self) -> Result<(), AggDescError> {
        if self.args.len() < 3 {
            return self.type_infer_4_max_min();
        }
        // Merge the type of the first and third argument.
        let merged = crate::rewriter::infer_type4_control_funcs(
            &self.name,
            &[self.args[0].clone(), self.args[2].clone()],
        );
        // Go ignores the second (error) return and keeps whatever type came
        // back, including a nil one.
        self.ret_type = merged.unwrap_or_else(|| FieldType::new(FieldTypeCode::Unspecified));
        Ok(())
    }

    /// Go `typeInfer4PopOrSamp` (`base_func.go:423`): the variance family
    /// always returns DOUBLE.
    fn type_infer_4_pop_or_samp(&mut self) {
        let mut ret = FieldType::new(FieldTypeCode::Double);
        ret.set_flen(MAX_REAL_WIDTH);
        ret.set_decimal(UNSPECIFIED_LENGTH);
        self.ret_type = ret;
    }

    /// Go `GetDefaultValue` (`base_func.go:447`): the value the aggregate
    /// produces for an EMPTY input.
    #[must_use]
    pub fn get_default_value(&self) -> Datum {
        match self.name.as_str() {
            names::COUNT | names::BIT_OR | names::BIT_XOR => Datum::new_int(0),
            names::APPROX_COUNT_DISTINCT => {
                if self.ret_type.code() == FieldTypeCode::String {
                    Datum::Null
                } else {
                    Datum::new_int(0)
                }
            }
            names::FIRST_ROW
            | names::AVG
            | names::SUM
            | names::SUM_INT
            | names::MAX
            | names::MIN
            | names::GROUP_CONCAT
            | names::APPROX_PERCENTILE => Datum::Null,
            names::BIT_AND => Datum::new_uint(u64::MAX),
            _ => Datum::Null,
        }
    }

    /// Go `WrapCastForAggArgs` (`base_func.go:478`): wrap every argument in
    /// the cast that makes it evaluate as the aggregate's own return type.
    ///
    /// Go PANICS on an eval type with no cast wrapper
    /// (`base_func.go:509`); every [`EvalType`] variant has one here, so the
    /// panic arm has no Rust counterpart.
    pub fn wrap_cast_for_agg_args(&mut self, ctx: &impl Columns) -> Result<(), AggDescError> {
        if self.args.is_empty() {
            return Ok(());
        }
        if NO_NEED_CAST_AGG_FUNCS.contains(&self.name.as_str()) {
            return Ok(());
        }
        let ret_tp = self.ret_type.clone();
        let connection = wrap_cast::connection_charset(ctx);
        for i in 0..self.args.len() {
            // These functions' second argument is a plain non-negative count,
            // not a value in the aggregate's domain.
            if i == 1
                && matches!(
                    self.name.as_str(),
                    names::LEAD | names::LAG | names::NTH_VALUE
                )
            {
                continue;
            }
            if type_of(&self.args[i]).code() == FieldTypeCode::Null {
                continue;
            }
            let old = std::mem::replace(
                &mut self.args[i],
                Expression::Constant(crate::constant::Constant::default()),
            );
            self.args[i] = match ret_tp.eval_type() {
                EvalType::Int => wrap_cast::wrap_with_cast_as_int(old, Some(&ret_tp))?,
                EvalType::Real => wrap_cast::wrap_with_cast_as_real(old)?,
                EvalType::String => {
                    wrap_cast::wrap_with_cast_as_string(old, (&connection.0, &connection.1))?
                }
                EvalType::Decimal => wrap_cast::wrap_with_cast_as_decimal(old)?,
                EvalType::Datetime | EvalType::Timestamp => {
                    wrap_cast::wrap_with_cast_as_time(old, ret_tp.clone())?
                }
                EvalType::Duration => wrap_cast::wrap_with_cast_as_duration(old)?,
                EvalType::Json => wrap_cast::wrap_with_cast_as_json(old)?,
                EvalType::VectorFloat32 => wrap_cast::wrap_with_cast_as_vector_float32(old)?,
            };
        }
        Ok(())
    }
}

/// Go `noNeedCastAggFuncs` (`base_func.go:465`): the aggregates whose
/// evaluation is driven by the ARGUMENT's type, so wrapping a cast would
/// change the answer.
pub(super) const NO_NEED_CAST_AGG_FUNCS: &[&str] = &[
    names::COUNT,
    names::APPROX_COUNT_DISTINCT,
    names::APPROX_PERCENTILE,
    names::MAX,
    names::MIN,
    names::FIRST_ROW,
    names::NTILE,
    names::JSON_ARRAYAGG,
    names::JSON_OBJECTAGG,
];

/// Go `charset.GetDefaultCollation(cs)`, restricted to the charsets this
/// workspace's collation table can name. `None` is Go's error return, which
/// `typeInfer4GroupConcat` treats as "fall back to the connection collation".
fn default_collation_for_charset(charset: &str) -> Option<String> {
    match charset {
        "utf8mb4" => Some("utf8mb4_bin".to_owned()),
        "utf8" => Some("utf8_bin".to_owned()),
        "binary" => Some("binary".to_owned()),
        "latin1" => Some("latin1_bin".to_owned()),
        "ascii" => Some("ascii_bin".to_owned()),
        "gbk" => Some("gbk_chinese_ci".to_owned()),
        _ => None,
    }
}
