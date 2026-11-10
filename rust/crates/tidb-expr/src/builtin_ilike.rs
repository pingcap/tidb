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

//! `pkg/expression/builtin_ilike.go`: the `ILIKE` function class.

use crate::builtin::{
    new_base_builtin_func_with_tp, BaseBuiltinFunc, BuildContext, FunctionClass,
};
use crate::expression::Expression;
use crate::EvalError;
use tidb_datatype::EvalType;
use tidb_proto::tipb::ScalarFuncSig;

/// Go `ilikeFunctionClass`.
pub(crate) struct IlikeFunctionClass;

impl FunctionClass for IlikeFunctionClass {
    fn get_function(
        &self,
        ctx: &dyn BuildContext,
        args: Vec<Expression>,
    ) -> Result<BaseBuiltinFunc, EvalError> {
        let arg_tp = [EvalType::String, EvalType::String, EvalType::Int];
        let mut bf = new_base_builtin_func_with_tp(ctx, "ilike", args, EvalType::Int, &arg_tp)?;
        bf.tp.set_flen(1);
        bf.pb_code = ScalarFuncSig::IlikeSig;
        Ok(bf)
    }
}
