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

//! `pkg/expression/builtin_fts.go`: the full-text search function classes.

use crate::builtin::{
    new_base_builtin_func_with_tp, BaseBuiltinFunc, BuildContext, FunctionClass,
};
use crate::expression::Expression;
use crate::wrap_cast::type_of;
use crate::EvalError;
use tidb_datatype::{Datum, EvalType};
use tidb_proto::tipb::ScalarFuncSig;

/// Go `ErrNotSupportedYet.GenWithStackByArgs(feature)`.
fn not_supported_yet(feature: &'static str) -> EvalError {
    EvalError::NotImplemented(feature.into())
}

/// Go `ftsMatchWordFunctionClass`.
pub(crate) struct FtsMatchWordFunctionClass;

impl FunctionClass for FtsMatchWordFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        if !tidb_config_deploymode::is_starter() {
            return Err(not_supported_yet(
                "FTS_MATCH_WORD() is only supported in starter deployment mode",
            ));
        }
        let Expression::Constant(arg_against) = &args[0] else {
            return Err(not_supported_yet("match against a non-constant string"));
        };
        if !matches!(arg_against.value, Datum::String(_)) {
            return Err(not_supported_yet("match against a non-constant string"));
        }
        if args[1..].iter().any(|arg| !matches!(arg, Expression::Column(_))) {
            return Err(not_supported_yet("not matching a column"));
        }
        let mut bf = new_base_builtin_func_with_tp(
            ctx,
            "fts_match_word",
            args,
            EvalType::Real,
            &[EvalType::String, EvalType::String],
        )?;
        bf.pb_code = ScalarFuncSig::FtsMatchWord;
        Ok(bf)
    }
}

/// Go `ftsMysqlMatchAgainstFunctionClass`.
pub(crate) struct FtsMysqlMatchAgainstFunctionClass;

impl FunctionClass for FtsMysqlMatchAgainstFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let Expression::Constant(arg_against) = &args[0] else {
            return Err(not_supported_yet("match against a non-constant string"));
        };
        if !matches!(arg_against.value, Datum::String(_) | Datum::Null) {
            return Err(not_supported_yet("match against a non-string constant"));
        }
        let args_match = &args[1..];
        if args_match.iter().any(|arg| !matches!(arg, Expression::Column(_))) {
            return Err(not_supported_yet("not matching a column"));
        }
        if args_match.iter().any(|arg| type_of(arg).eval_type() != EvalType::String) {
            return Err(not_supported_yet(
                "Doesn't support match search on a non-string column without fulltext index",
            ));
        }
        let arg_tps = vec![EvalType::String; args.len()];
        let mut bf =
            new_base_builtin_func_with_tp(ctx, "match_against", args, EvalType::Real, &arg_tps)?;
        bf.pb_code = ScalarFuncSig::FtsMatchExpression;
        Ok(bf)
    }
}
