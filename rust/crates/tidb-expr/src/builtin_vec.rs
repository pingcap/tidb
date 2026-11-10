//! Go `pkg/expression/builtin_vec.go`: the function classes of the vector
//! builtins.

use tidb_datatype::EvalType;
use tidb_proto::tipb::ScalarFuncSig;

use crate::builtin::{
    new_base_builtin_func_with_tp, BaseBuiltinFunc, BuildContext, FunctionClass,
};
use crate::builtin_miscellaneous::function_class;
use crate::expression::Expression;
use crate::EvalError;

function_class!(
    /// Go `vecDimsFunctionClass`.
    VecDimsFunctionClass, Int, [VectorFloat32], VecDimsSig
);
function_class!(
    /// Go `vecL1DistanceFunctionClass`.
    VecL1DistanceFunctionClass, Real, [VectorFloat32, VectorFloat32], VecL1DistanceSig
);
function_class!(
    /// Go `vecL2DistanceFunctionClass`.
    VecL2DistanceFunctionClass, Real, [VectorFloat32, VectorFloat32], VecL2DistanceSig
);
function_class!(
    /// Go `vecNegativeInnerProductFunctionClass`.
    VecNegativeInnerProductFunctionClass, Real, [VectorFloat32, VectorFloat32], VecNegativeInnerProductSig
);
function_class!(
    /// Go `vecCosineDistanceFunctionClass`.
    VecCosineDistanceFunctionClass, Real, [VectorFloat32, VectorFloat32], VecCosineDistanceSig
);
function_class!(
    /// Go `vecL2NormFunctionClass`.
    VecL2NormFunctionClass, Real, [VectorFloat32], VecL2NormSig
);
function_class!(
    /// Go `vecFromTextFunctionClass`. `builtinVecFromTextSig` sets no pb code.
    VecFromTextFunctionClass, VectorFloat32, [String], Unspecified => crate::scalar_function::Kernel::Values(crate::scalar_function::class_kernels::vec_from_text)
);
function_class!(
    /// Go `vecAsTextFunctionClass`.
    VecAsTextFunctionClass, String, [VectorFloat32], VecAsTextSig
);
