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

//! `pkg/expression/builtin_regexp.go`: the regular expression function
//! classes.

use crate::builtin::{
    new_base_builtin_func_with_tp, BaseBuiltinFunc, BuildContext, FunctionClass,
};
use crate::expression::Expression;
use crate::regexp::{err_regexp, err_regexp_wrapping, BINARY_COLLATE_ERR};
use crate::wrap_cast::type_of;
use crate::EvalError;
use tidb_datatype::EvalType;
use tidb_proto::tipb::ScalarFuncSig;

/// Go `mysql.MaxIntWidth`.
const MAX_INT_WIDTH: i64 = 20;

/// Go `regexpBaseFuncSig.isBinaryCollation` over the collation
/// `newBaseBuiltinFuncWithTp` derived.
fn is_binary_collation(bf: &BaseBuiltinFunc) -> bool {
    bf.collation.charset_and_collation() == ("binary", "binary")
}

/// Go `regexpLikeFunctionClass`, registered as `regexp` and `regexp_like`.
pub(crate) struct RegexpLikeFunctionClass {
    pub(crate) func_name: &'static str,
}

impl FunctionClass for RegexpLikeFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let arg_tps = vec![EvalType::String; args.len()];
        let mut bf =
            new_base_builtin_func_with_tp(ctx, self.func_name, args, EvalType::Int, &arg_tps)?;
        bf.tp.set_flen(1);
        bf.pb_code = ScalarFuncSig::RegexpLikeSig;
        Ok(bf)
    }
}

/// Go `regexpSubstrFunctionClass`.
pub(crate) struct RegexpSubstrFunctionClass;

impl FunctionClass for RegexpSubstrFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        const ARG_TPS: [EvalType; 5] = [
            EvalType::String,
            EvalType::String,
            EvalType::Int,
            EvalType::Int,
            EvalType::String,
        ];
        let arg_tps = &ARG_TPS[..args.len()];
        let mut bf =
            new_base_builtin_func_with_tp(ctx, "regexp_substr", args, EvalType::String, arg_tps)?;
        let flen = type_of(&bf.args[0]).flen();
        bf.tp.set_flen(flen);
        bf.pb_code = ScalarFuncSig::RegexpSubstrSig;
        if is_binary_collation(&bf) {
            return Err(err_regexp(BINARY_COLLATE_ERR));
        }
        Ok(bf)
    }
}

/// Go `regexpInStrFunctionClass`.
pub(crate) struct RegexpInStrFunctionClass;

impl FunctionClass for RegexpInStrFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        const ARG_TPS: [EvalType; 6] = [
            EvalType::String,
            EvalType::String,
            EvalType::Int,
            EvalType::Int,
            EvalType::Int,
            EvalType::String,
        ];
        let arg_tps = &ARG_TPS[..args.len()];
        let mut bf =
            new_base_builtin_func_with_tp(ctx, "regexp_instr", args, EvalType::Int, arg_tps)
                .map_err(err_regexp_wrapping)?;
        bf.tp.set_flen(MAX_INT_WIDTH);
        bf.pb_code = ScalarFuncSig::RegexpInStrSig;
        if is_binary_collation(&bf) {
            return Err(err_regexp(BINARY_COLLATE_ERR));
        }
        Ok(bf)
    }
}

/// Go `regexpReplaceFunctionClass`.
pub(crate) struct RegexpReplaceFunctionClass;

impl FunctionClass for RegexpReplaceFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        const ARG_TPS: [EvalType; 6] = [
            EvalType::String,
            EvalType::String,
            EvalType::String,
            EvalType::Int,
            EvalType::Int,
            EvalType::String,
        ];
        let arg_tps = &ARG_TPS[..args.len()];
        let mut bf =
            new_base_builtin_func_with_tp(ctx, "regexp_replace", args, EvalType::String, arg_tps)
                .map_err(err_regexp_wrapping)?;
        let flen = type_of(&bf.args[0]).flen();
        bf.tp.set_flen(flen);
        bf.pb_code = ScalarFuncSig::RegexpReplaceSig;
        if is_binary_collation(&bf) {
            return Err(err_regexp(BINARY_COLLATE_ERR));
        }
        Ok(bf)
    }
}
