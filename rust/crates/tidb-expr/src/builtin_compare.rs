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

//! `pkg/expression/builtin_compare.go`: the comparison operators' result-type
//! derivation (`compareFunctionClass.getFunction` / `generateCmpSigs`),
//! transcreated as [`infer_compare_type`].
//!
//! In Go the comparison operand type (`GetAccurateCmpType`) only selects the
//! *signature* (which eval path compares the operands); the *result*
//! `FieldType` is always the ETInt base type from
//! `newReturnFieldTypeForBaseBuiltinFunc` (TypeLonglong, BinaryFlag, flen
//! MaxIntWidth) with `bf.tp.SetFlen(1)`, plus `mysql.IsBooleanFlag` because
//! every comparison name is in the `booleanFunctions` map
//! (`function_traits.go`).
//!
//! DEFERRED (documented): `refineArgs` (constant refinement of int-vs-non-int
//! comparisons) mutates the *arguments*, not the result type, and is not
//! ported here; the JSON `DisableParseJSONFlag4Expr` tweak likewise touches
//! only the args.

use crate::builtin_arithmetic::new_return_field_type;
use tidb_datatype::{EvalType, FieldType, FieldTypeFlags};

/// The result `FieldType` Go's `compareFunctionClass.getFunction` derives, for
/// the comparison scalar-function `name` (`eq`/`nulleq`/`ne`/`lt`/`le`/`gt`/
/// `ge`). Returns `None` for any other function name.
#[must_use]
pub fn infer_compare_type(name: &str) -> Option<FieldType> {
    match name {
        "eq" | "nulleq" | "ne" | "lt" | "le" | "gt" | "ge" => {
            let mut ret = new_return_field_type(EvalType::Int);
            // generateCmpSigs: bf.tp.SetFlen(1).
            ret.set_flen(1);
            // All comparison names are in Go's booleanFunctions map.
            ret.add_flags(FieldTypeFlags::IS_BOOLEAN);
            Some(ret)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::FieldTypeCode;

    #[test]
    fn comparisons_are_boolean_longlong_flen1() {
        for name in ["eq", "nulleq", "ne", "lt", "le", "gt", "ge"] {
            let ret = infer_compare_type(name).unwrap();
            assert_eq!(ret.code(), FieldTypeCode::LongLong, "{name}");
            assert_eq!(ret.flen(), 1, "{name}");
            assert!(!ret.is_unsigned(), "{name}");
            assert_ne!(ret.flags() & FieldTypeFlags::IS_BOOLEAN, 0, "{name}");
            assert_ne!(ret.flags() & FieldTypeFlags::BINARY, 0, "{name}");
        }
    }

    #[test]
    fn non_comparison_name_is_none() {
        assert!(infer_compare_type("plus").is_none());
        assert!(infer_compare_type("and").is_none());
    }
}
