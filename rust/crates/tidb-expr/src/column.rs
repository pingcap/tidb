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

use crate::context::EvalError;
use crate::expr_collation::CollationInfo;
use crate::expression::{ConstLevel, Expression, COLUMN_FLAG};
use tidb_chunk::row::Row;
use tidb_codec::encode_int;
use tidb_datatype::{Datum, FieldType};

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

    /// Go `InColumnArray`: membership is column identity (`UniqueID`), not
    /// pointer identity or the legacy table-column `ID`.
    #[must_use]
    pub fn in_column_array(&self, columns: &[Column]) -> bool {
        columns
            .iter()
            .any(|column| column.unique_id == self.unique_id)
    }

    /// Go `Column.Eval`: read this column's cell (`row.GetDatum(Index, RetType)`).
    /// The `EvalContext` is unused, as in Go.
    pub fn eval(&self, row: Row<'_>) -> Result<Datum, EvalError> {
        let ret_type = self
            .ret_type
            .as_ref()
            .ok_or(EvalError::Unsupported("column has no result type"))?;
        Ok(row.get_datum(self.index as usize, ret_type))
    }
}

/// Go `Column2Exprs`: widen a column slice to the expression domain. Rust's
/// expression enum owns its variants, so this is the source operation with
/// the pointer-sharing detail translated into a clone.
#[must_use]
pub fn columns_to_expressions(columns: &[Column]) -> Vec<Expression> {
    columns.iter().cloned().map(Expression::Column).collect()
}

/// The identity operation inside Go `ColInfo2Col`, separated from the model
/// wrapper: return the first plan column whose legacy table-column `ID`
/// equals the requested `ColumnInfo.ID`.
#[must_use]
pub fn column_by_id(columns: &[Column], id: i64) -> Option<&Column> {
    columns.iter().find(|column| column.id == id)
}

/// Go `CorrelatedColumn`: a column reference bound to a value supplied by an
/// outer query (the correlated value lives in `Data`).
#[derive(Clone, Debug, Default)]
pub struct CorrelatedColumn {
    /// Go embedded `Column`.
    pub column: Column,
    /// Go `Data` (a `*types.Datum`): the current bound value, if any.
    pub data: Option<Datum>,
}

impl CorrelatedColumn {
    /// Go `IsCorrelated`: always true.
    #[must_use]
    pub fn is_correlated(&self) -> bool {
        true
    }

    /// Go `ConstLevel`: a correlated column is not constant.
    #[must_use]
    pub fn const_level(&self) -> ConstLevel {
        ConstLevel::NONE
    }

    /// Go `Equal` -> promoted `Column.EqualColumn`: equal to a plain [`Column`]
    /// with the same `UniqueID`; any other expression (including another
    /// correlated column) is unequal, matching Go's `expr.(*Column)` type assert.
    #[must_use]
    pub fn equal_column(&self, expr: &Expression) -> bool {
        self.column.equal_column(expr)
    }

    /// Go promoted `Column.HashCode`: hashes as the embedded column.
    pub fn hash_code(&mut self) -> &[u8] {
        self.column.hash_code()
    }

    /// Go promoted `Column.GetStaticType`.
    #[must_use]
    pub fn get_static_type(&self) -> Option<&FieldType> {
        self.column.get_static_type()
    }

    /// Go `CorrelatedColumn.Eval`: returns the bound outer value (`*Data`).
    /// Go dereferences the `Data` pointer; a not-yet-bound column yields NULL.
    #[must_use]
    pub fn eval(&self) -> Datum {
        self.data.clone().unwrap_or(Datum::Null)
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

    /// Exact Go `TestColumnHashCode` vectors from `column_test.go`.
    #[test]
    fn test_column_hash_code() {
        for (unique_id, expected) in [
            (12, [0x01, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c]),
            (2, [0x01, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02]),
        ] {
            let mut column = col(unique_id);
            assert_eq!(column.hash_code(), expected.as_slice());
            // Go caches the encoded bytes; a second call must be unchanged.
            assert_eq!(column.hash_code(), expected.as_slice());
        }
    }

    /// Exact Go `TestColumn2Expr`: five columns preserve their position and
    /// `EqualColumn` identity when widened to the expression domain.
    #[test]
    fn test_column2_expr() {
        let columns: Vec<_> = (0..5).map(col).collect();
        let expressions = columns_to_expressions(&columns);
        assert_eq!(expressions.len(), columns.len());
        for (column, expression) in columns.iter().zip(&expressions) {
            assert!(column.equal_column(expression));
        }
    }

    /// Go `TestColInfo2Col`: lookup uses `ColumnInfo.ID` and returns the first
    /// matching plan column; an unknown ID returns nil/None.
    #[test]
    fn test_col_info2_col() {
        let columns = [
            Column {
                id: 0,
                unique_id: 10,
                ..Default::default()
            },
            Column {
                id: 1,
                unique_id: 11,
                ..Default::default()
            },
        ];
        let found = column_by_id(&columns, 0).expect("ID 0 must exist");
        assert!(std::ptr::eq(found, &columns[0]));
        assert!(column_by_id(&columns, 3).is_none());
    }

    /// Exact Go `TestInColumnArray`: present, absent and empty/nil shapes.
    #[test]
    fn test_in_column_array() {
        let column0 = Column {
            id: 0,
            unique_id: 0,
            ..Default::default()
        };
        let column1 = Column {
            id: 1,
            unique_id: 1,
            ..Default::default()
        };
        assert!(column0.in_column_array(&[column0.clone(), column1.clone()]));
        assert!(!column0.in_column_array(&[column1]));
        assert!(!column0.in_column_array(&[]));
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

    #[test]
    fn correlated_column_delegates_and_is_correlated() {
        let mut cc = CorrelatedColumn {
            column: col(5),
            data: Some(Datum::Int(3)),
        };
        assert!(cc.is_correlated());
        assert_eq!(cc.const_level(), ConstLevel::NONE);
        // Equal to a plain column with the same UniqueID...
        assert!(cc.equal_column(&Expression::Column(col(5))));
        assert!(!cc.equal_column(&Expression::Column(col(6))));
        // ...but not to another correlated column (Go's expr.(*Column) fails).
        let other = Expression::CorrelatedColumn(CorrelatedColumn {
            column: col(5),
            data: None,
        });
        assert!(!cc.equal_column(&other));
        // HashCode matches the embedded column's.
        let mut plain = col(5);
        assert_eq!(cc.hash_code(), plain.hash_code());
    }
}
