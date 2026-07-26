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

//! `pkg/expression/column.go`: the `Column` expression node.
//!
//! Ported: the struct and its structural, context-free methods (static type,
//! column identity/equality, lazily-cached hash code, correlation/const-level).
//! DEFERRED (need `EvalContext`/`chunk.Row`, or reproduce Go struct byte sizes):
//! all `Eval*`, `StringWithCtx`/`ExplainInfo`, `ResolveIndices`, `RemapColumn`,
//! `Decorrelate`, and `MemoryUsage`. `CorrelatedColumn` is deferred with the
//! other node variants.

use crate::expr_collation::CollationInfo;
use crate::expression::{ConstLevel, Expression, COLUMN_FLAG};
use tidb_codec::encode_int;
use tidb_datatype::FieldType;

/// Go `Column`: a reference to a column, by unique id, within a plan.
#[derive(Clone, Debug, Default)]
pub struct Column {
    /// The column's result type (Go `RetType`, a `*types.FieldType`; `None`
    /// mirrors a nil pointer, though a valid column always carries a type).
    pub ret_type: Option<FieldType>,

    /// Go `ID`: distinguishes `ExtraHandleColumn` and indexes histograms; legacy.
    pub id: i64,

    /// Go `UniqueID`: the identity of this column reference (equality key).
    pub unique_id: i64,

    /// Go `Index`: the column's position in the evaluated row (may be unset/-1).
    pub index: i64,

    /// Lazily-filled `HashCode` cache (Go `hashcode`).
    hashcode: Vec<u8>,

    /// Go `VirtualExpr`: the generating expression for a virtual column.
    pub virtual_expr: Option<Box<Expression>>,

    /// Go `OrigName`.
    pub orig_name: String,

    /// Go `IsHidden`.
    pub is_hidden: bool,

    /// Go `IsPrefix`: whether this is a prefix column in an index.
    pub is_prefix: bool,

    /// Go `InOperand`: inner operand of a column-equal condition rewritten from
    /// `[not] in (subq)`.
    pub in_operand: bool,

    /// Go embedded `collationInfo`.
    pub collation: CollationInfo,

    /// Go `CorrelatedColUniqueID`.
    pub correlated_col_unique_id: i64,
}

impl Column {
    /// Builds a column with the given `UniqueID` and result type; all other
    /// fields take their defaults. (The `hashcode` cache is private, so columns
    /// outside this crate are built through constructors like this rather than
    /// struct literals.)
    #[must_use]
    pub fn new(unique_id: i64, ret_type: FieldType) -> Self {
        Column {
            unique_id,
            ret_type: Some(ret_type),
            ..Default::default()
        }
    }

    /// Go `GetStaticType` / `GetType` (the latter ignores its `EvalContext`),
    /// returning the column's result type (`None` for a nil `RetType`).
    #[must_use]
    pub fn get_static_type(&self) -> Option<&FieldType> {
        self.ret_type.as_ref()
    }

    /// Go `EqualColumn`: two columns are equal iff they share a `UniqueID`; any
    /// non-column expression is unequal.
    #[must_use]
    pub fn equal_column(&self, expr: &Expression) -> bool {
        match expr.as_column() {
            Some(other) => other.unique_id == self.unique_id,
            None => false,
        }
    }

    /// Go `HashCode`: `[columnFlag, EncodeInt(UniqueID)]`, cached on first call.
    pub fn hash_code(&mut self) -> &[u8] {
        if self.hashcode.is_empty() {
            self.hashcode.reserve(9);
            self.hashcode.push(COLUMN_FLAG);
            encode_int(&mut self.hashcode, self.unique_id);
        }
        &self.hashcode
    }

    /// Go `IsCorrelated`: a plain column is never correlated.
    #[must_use]
    pub fn is_correlated(&self) -> bool {
        false
    }

    /// Go `ConstLevel`: a column is not constant.
    #[must_use]
    pub fn const_level(&self) -> ConstLevel {
        ConstLevel::NONE
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{FieldType, FieldTypeCode};

    fn col(unique_id: i64) -> Column {
        Column {
            unique_id,
            ret_type: Some(FieldType::new(FieldTypeCode::Long)),
            ..Default::default()
        }
    }

    #[test]
    fn equality_is_by_unique_id() {
        let a = col(7);
        let a2 = col(7);
        let b = col(8);
        assert!(a.equal_column(&Expression::Column(a2)));
        assert!(!a.equal_column(&Expression::Column(b)));
    }

    #[test]
    fn hash_code_is_flag_plus_encoded_unique_id() {
        let mut c = col(42);
        let mut expected = vec![COLUMN_FLAG];
        encode_int(&mut expected, 42);
        assert_eq!(c.hash_code(), expected.as_slice());
        // Cached: a second call returns the same bytes.
        assert_eq!(c.hash_code(), expected.as_slice());
        assert_eq!(expected.len(), 9);
    }

    #[test]
    fn distinct_unique_ids_hash_differently() {
        let mut c1 = col(1);
        let mut c2 = col(2);
        assert_ne!(c1.hash_code(), c2.hash_code());
    }

    #[test]
    fn plain_column_is_not_correlated_or_const() {
        let c = col(1);
        assert!(!c.is_correlated());
        assert_eq!(c.const_level(), ConstLevel::NONE);
    }
}
