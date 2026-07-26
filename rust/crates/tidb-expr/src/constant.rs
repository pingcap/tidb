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

//! `pkg/expression/constant.go`: the `Constant` expression node and its
//! `ParamMarker`.
//!
//! Ported: the struct and its structural, context-free methods (static type,
//! const-level, correlation, and the lazily-cached `HashCode`). DEFERRED (need
//! `EvalContext`/`chunk.Row` or the param/user-var machinery): all `Eval*`,
//! `GetType(ctx)`'s param-type inference, `Equal` (it evaluates and compares
//! values through a collator), `StringWithCtx`/`ExplainInfo`, `CanonicalHashCode`,
//! and `MemoryUsage`.

use crate::expr_collation::CollationInfo;
use crate::expression::{ConstLevel, Expression, CONSTANT_FLAG, PARAMETER_FLAG};
use tidb_codec::{encode_int, hash_code};
use tidb_datatype::{Datum, FieldType};

/// Go `ParamMarker`: a reference to a placeholder parameter in a prepared
/// statement, by its position in `sessionVars.PreparedParams`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ParamMarker {
    /// Go `order`: the parameter's index.
    pub order: i64,
}

/// Go `Constant`: a literal (or deferred/parameter) constant value.
#[derive(Clone, Debug, Default)]
pub struct Constant {
    /// Go `Value`: the constant's datum.
    pub value: Datum,

    /// Go `RetType` (a `*types.FieldType`; `None` mirrors a nil pointer).
    pub ret_type: Option<FieldType>,

    /// Go `DeferredExpr`: a deferred non-deterministic function, evaluated
    /// lazily when a plan-cache entry is reused.
    pub deferred_expr: Option<Box<Expression>>,

    /// Go `ParamMarker`: set when this constant references an `EXECUTE`
    /// parameter / user variable.
    pub param_marker: Option<ParamMarker>,

    /// Lazily-filled `HashCode` cache (Go `hashcode`).
    hashcode: Vec<u8>,

    /// Go `SubqueryRefID`: the id of the original subquery column, for display.
    pub subquery_ref_id: i64,

    /// Go embedded `collationInfo`.
    pub collation: CollationInfo,
}

impl Constant {
    /// Builds a plain literal constant with the given value and type.
    #[must_use]
    pub fn new(value: Datum, ret_type: FieldType) -> Self {
        Constant {
            value,
            ret_type: Some(ret_type),
            ..Default::default()
        }
    }

    /// Go `GetStaticType`: the declared result type (`None` for a nil pointer).
    /// The context-dependent `GetType` param-type inference is deferred.
    #[must_use]
    pub fn get_static_type(&self) -> Option<&FieldType> {
        self.ret_type.as_ref()
    }

    /// Go `IsCorrelated`: a constant is never correlated.
    #[must_use]
    pub fn is_correlated(&self) -> bool {
        false
    }

    /// Go `ConstLevel`: a deferred or parameter constant is constant only within
    /// one context; a plain literal is strictly constant.
    #[must_use]
    pub fn const_level(&self) -> ConstLevel {
        if self.deferred_expr.is_some() || self.param_marker.is_some() {
            ConstLevel::ONLY_IN_CONTEXT
        } else {
            ConstLevel::STRICT
        }
    }

    /// Go `HashCode` (= `getHashCode(false)`), cached on first call:
    /// - a deferred constant hashes as its deferred expression;
    /// - a parameter hashes as `[parameterFlag, EncodeInt(order)]`;
    /// - a plain literal hashes as `[constantFlag, HashCode(Value)]`.
    pub fn hash_code(&mut self) -> &[u8] {
        if !self.hashcode.is_empty() {
            return &self.hashcode;
        }
        if let Some(deferred) = &mut self.deferred_expr {
            self.hashcode = deferred.hash_code().to_vec();
        } else if let Some(param) = self.param_marker {
            self.hashcode.push(PARAMETER_FLAG);
            encode_int(&mut self.hashcode, param.order);
        } else {
            self.hashcode.push(CONSTANT_FLAG);
            self.hashcode.extend_from_slice(&hash_code(&self.value));
        }
        &self.hashcode
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};

    fn ft() -> FieldType {
        FieldType::new(FieldTypeCode::Long)
    }

    #[test]
    fn const_level_reflects_deferred_and_param() {
        let lit = Constant::new(Datum::Int(1), ft());
        assert_eq!(lit.const_level(), ConstLevel::STRICT);
        assert!(!lit.is_correlated());

        let mut param = Constant::new(Datum::Null, ft());
        param.param_marker = Some(ParamMarker { order: 3 });
        assert_eq!(param.const_level(), ConstLevel::ONLY_IN_CONTEXT);
    }

    #[test]
    fn literal_hash_code_is_flag_plus_value_encoding() {
        let mut c = Constant::new(Datum::Int(1), ft());
        let mut expected = vec![CONSTANT_FLAG];
        expected.extend_from_slice(&hash_code(&Datum::Int(1)));
        assert_eq!(c.hash_code(), expected.as_slice());
        // Cached.
        assert_eq!(c.hash_code(), expected.as_slice());
    }

    #[test]
    fn param_hash_code_is_flag_plus_order() {
        let mut c = Constant::new(Datum::Null, ft());
        c.param_marker = Some(ParamMarker { order: 5 });
        let mut expected = vec![PARAMETER_FLAG];
        encode_int(&mut expected, 5);
        assert_eq!(c.hash_code(), expected.as_slice());
    }

    #[test]
    fn distinct_literals_hash_differently() {
        let mut a = Constant::new(Datum::Int(1), ft());
        let mut b = Constant::new(Datum::Int(2), ft());
        assert_ne!(a.hash_code(), b.hash_code());
    }

    #[test]
    fn clone_is_deep() {
        let mut c = Constant::new(Datum::Int(9), ft());
        c.subquery_ref_id = 7;
        let d = c.clone();
        assert_eq!(d.value, Datum::Int(9));
        assert_eq!(d.subquery_ref_id, 7);
    }
}
