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

use std::hash::{Hash, Hasher};

use crate::context::EvalError;
use crate::expr_collation::CollationInfo;
use crate::expression::{ConstLevel, Expression, COLUMN_FLAG};
use crate::schema::Schema;
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

    /// Go `EqualByExprAndID`: identity by `UniqueID`, or for generated columns,
    /// equality by matching scalar virtual expression plus result type.
    #[must_use]
    pub fn equal_by_expr_and_id(&self, expr: &Expression) -> bool {
        let Expression::Column(other) = expr else {
            return false;
        };
        let is_virtual_expr_matched = self.ret_type == other.ret_type
            && crate::constant::optional_expression_equals(&self.virtual_expr, &other.virtual_expr);
        other.unique_id == self.unique_id || is_virtual_expr_matched
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

    /// Go `Column.Hash64`: structural plan-key hash over every source field
    /// except the lazy `HashCode` cache.
    #[must_use]
    pub fn hash64(&self) -> u64 {
        let mut hasher = Fnv64::default();
        hash_column(self, &mut hasher);
        hasher.finish()
    }

    /// Go `Column.Equals`: structural plan-key equality. This is deliberately
    /// stricter than [`Self::equal_column`], whose execution identity is only
    /// `UniqueID`.
    #[must_use]
    pub fn equals(&self, other: &Self) -> bool {
        self.ret_type == other.ret_type
            && optional_expression_equals(&self.virtual_expr, &other.virtual_expr)
            && self.id == other.id
            && self.unique_id == other.unique_id
            && self.index == other.index
            && self.orig_name == other.orig_name
            && self.is_hidden == other.is_hidden
            && self.is_prefix == other.is_prefix
            && self.in_operand == other.in_operand
            && self.collation == other.collation
            && self.correlated_col_unique_id == other.correlated_col_unique_id
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

    /// Go `Column.Decorrelate`: a plain column is already decorrelated.
    #[must_use]
    pub fn decorrelate(&self) -> Expression {
        Expression::Column(self.clone())
    }

    /// Go `InColumnArray`: membership is column identity (`UniqueID`), not
    /// pointer identity or the legacy table-column `ID`.
    #[must_use]
    pub fn in_column_array(&self, columns: &[Column]) -> bool {
        columns
            .iter()
            .any(|column| column.unique_id == self.unique_id)
    }

    /// Go `ResolveIndicesByVirtualExpr`: resolve to an exact schema column by
    /// `UniqueID`; if no exact match exists, fall back to a matching generated
    /// column virtual expression.
    #[must_use]
    pub fn resolve_indices_by_virtual_expr(&self, schema: &Schema) -> Option<Column> {
        let mut resolved = self.clone();
        if resolved.resolve_indices_by_virtual_expr_in_place(schema) {
            Some(resolved)
        } else {
            None
        }
    }

    fn resolve_indices_by_virtual_expr_in_place(&mut self, schema: &Schema) -> bool {
        let mut fallback_idx = None;
        for (idx, schema_column) in schema.columns.iter().enumerate() {
            let target = Expression::Column(self.clone());
            if schema_column.equal_column(&target) {
                self.index = idx as i64;
                return true;
            }
            if fallback_idx.is_none() && schema_column.equal_by_expr_and_id(&target) {
                fallback_idx = Some(idx as i64);
            }
        }
        if let Some(idx) = fallback_idx {
            self.index = idx;
            true
        } else {
            false
        }
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

const FNV_OFFSET_64: u64 = 14_695_981_039_346_656_037;
const FNV_PRIME_64: u64 = 1_099_511_628_211;

struct Fnv64(u64);

impl Default for Fnv64 {
    fn default() -> Self {
        Self(FNV_OFFSET_64)
    }
}

impl Hasher for Fnv64 {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(FNV_PRIME_64);
        }
    }
}

fn hash_column(column: &Column, hasher: &mut Fnv64) {
    column.ret_type.hash(hasher);
    column.id.hash(hasher);
    column.unique_id.hash(hasher);
    column.index.hash(hasher);
    match &column.virtual_expr {
        Some(expression) => {
            1_u8.hash(hasher);
            expression_hash64(expression).hash(hasher);
        }
        None => 0_u8.hash(hasher),
    }
    column.orig_name.hash(hasher);
    column.is_hidden.hash(hasher);
    column.is_prefix.hash(hasher);
    column.in_operand.hash(hasher);
    column.collation.hash64().hash(hasher);
    column.correlated_col_unique_id.hash(hasher);
}

fn expression_hash64(expression: &Expression) -> u64 {
    match expression {
        Expression::Constant(constant) => constant.hash64(),
        Expression::Column(column) => column.hash64(),
        Expression::CorrelatedColumn(column) => {
            let mut hasher = Fnv64::default();
            6_u8.hash(&mut hasher);
            hash_column(&column.column, &mut hasher);
            hasher.finish()
        }
        Expression::ScalarFunction(function) => {
            let mut hasher = Fnv64::default();
            3_u8.hash(&mut hasher);
            function.func_name.lowercase().hash(&mut hasher);
            function.ret_type.hash(&mut hasher);
            function.args.len().hash(&mut hasher);
            for argument in &function.args {
                expression_hash64(argument).hash(&mut hasher);
            }
            hasher.finish()
        }
    }
}

fn optional_expression_equals(
    left: &Option<Box<Expression>>,
    right: &Option<Box<Expression>>,
) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => expression_equals(left, right),
        (None, None) => true,
        _ => false,
    }
}

fn expression_equals(left: &Expression, right: &Expression) -> bool {
    match (left, right) {
        (Expression::Constant(left), Expression::Constant(right)) => left.equals(right),
        (Expression::Column(left), Expression::Column(right)) => left.equals(right),
        (Expression::CorrelatedColumn(left), Expression::CorrelatedColumn(right)) => {
            left.column.equals(&right.column)
        }
        (Expression::ScalarFunction(left), Expression::ScalarFunction(right)) => {
            left.func_name.lowercase() == right.func_name.lowercase()
                && left.ret_type == right.ret_type
                && left.args.len() == right.args.len()
                && left
                    .args
                    .iter()
                    .zip(&right.args)
                    .all(|(left, right)| expression_equals(left, right))
        }
        _ => false,
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

/// Go `GcColumnExprIsTidbShard`: whether a generated-column virtual expression
/// is exactly a `tidb_shard(...)` scalar function.
#[must_use]
pub fn gc_column_expr_is_tidb_shard(virtual_expr: Option<&Expression>) -> bool {
    match virtual_expr {
        Some(Expression::ScalarFunction(function)) => {
            function.func_name.lowercase() == "tidb_shard"
        }
        _ => false,
    }
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

    /// Go `CorrelatedColumn.EqualColumn`: equal only to another correlated
    /// column whose embedded [`Column`] has the same `UniqueID`.
    #[must_use]
    pub fn equal_column(&self, expr: &Expression) -> bool {
        match expr {
            Expression::CorrelatedColumn(other) => self.column.unique_id == other.column.unique_id,
            _ => false,
        }
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

    /// Go `CorrelatedColumn.Decorrelate`: replace with the embedded plain
    /// column only when the outer schema contains that column.
    #[must_use]
    pub fn decorrelate(&self, schema: &Schema) -> Expression {
        if schema.contains(&self.column) {
            Expression::Column(self.column.clone())
        } else {
            Expression::CorrelatedColumn(self.clone())
        }
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
    use crate::scalar_function::ScalarFunction;
    use chrono::TimeZone;
    use tidb_ast::CiString;
    use tidb_chunk::chunk::Chunk;
    use tidb_datatype::{
        core_time_from_datetime, parse_bit_str, Decimal, FieldType, FieldTypeCode, MySqlDuration,
        MysqlEnum, MysqlSet, Time, TimeType,
    };

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

    /// Source: `pkg/expression/column_test.go::TestColumn` virtual-expression
    /// resolution branch.
    #[test]
    fn resolve_indices_by_virtual_expr_prefers_exact_unique_id() {
        let string_type = FieldType::new(FieldTypeCode::Varchar);
        let base = Column::new(10, string_type.clone());
        let virtual_expr = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("lower"),
            string_type.clone(),
            vec![Expression::Column(base.clone())],
        ));

        let expr_only = Column {
            unique_id: 12,
            ret_type: Some(string_type.clone()),
            virtual_expr: Some(Box::new(virtual_expr.clone())),
            ..Default::default()
        };
        let exact = Column {
            unique_id: 11,
            ret_type: Some(string_type.clone()),
            virtual_expr: Some(Box::new(virtual_expr.clone())),
            ..Default::default()
        };

        let target = Column {
            unique_id: 11,
            ret_type: Some(string_type.clone()),
            virtual_expr: Some(Box::new(virtual_expr.clone())),
            ..Default::default()
        };
        let resolved = target
            .resolve_indices_by_virtual_expr(&Schema::new(vec![expr_only.clone(), exact.clone()]))
            .expect("exact column id must resolve");
        assert_eq!(resolved.index, 1);

        let ambiguous = Column {
            unique_id: 13,
            ret_type: Some(string_type),
            virtual_expr: Some(Box::new(virtual_expr)),
            ..Default::default()
        };
        let resolved = ambiguous
            .resolve_indices_by_virtual_expr(&Schema::new(vec![expr_only, exact]))
            .expect("virtual expr fallback must resolve");
        assert_eq!(resolved.index, 0);
    }

    /// Source: `pkg/expression/column_test.go::TestColHybird`.
    #[test]
    fn column_eval_preserves_hybrid_datums_from_rows() {
        fn eval_one(field_type: FieldType, datum: &Datum) -> Datum {
            let fields = [field_type.clone()];
            let mut chunk = Chunk::new(&fields, 1, 1);
            chunk.append_datum(0, datum);

            let mut column = Column::new(1, field_type);
            column.index = 0;
            column.eval(chunk.get_row(0)).expect("column eval")
        }

        let bit_type = FieldType::new(FieldTypeCode::Bit);
        for value in [0, 1, 7, 255, 1023] {
            let literal = parse_bit_str(&format!("0b{value:b}")).expect("source bit literal");
            let datum = Datum::new_mysql_bit(literal);
            assert_eq!(eval_one(bit_type.clone(), &datum), datum, "bit {value}");
        }

        let enum_type = FieldType::new(FieldTypeCode::Enum);
        for value in [0, 1, 7, 255, 1023] {
            let datum = Datum::new_enum(
                MysqlEnum::new(value.to_string(), value),
                enum_type.collation(),
            );
            assert_eq!(eval_one(enum_type.clone(), &datum), datum, "enum {value}");
        }

        let set_type = FieldType::new(FieldTypeCode::Set);
        for value in [0, 1, 7, 255, 1023] {
            let datum = Datum::new_set(
                MysqlSet::new(value.to_string(), value),
                set_type.collation(),
            );
            assert_eq!(eval_one(set_type.clone(), &datum), datum, "set {value}");
        }
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

    /// Source: `pkg/expression/column_test.go::TestGcColumnExprIsTidbShard`.
    #[test]
    fn gc_column_expr_is_tidb_shard_matches_source() {
        let field_type = FieldType::new(FieldTypeCode::LongLong);
        let column = Expression::Column(Column::new(1, field_type.clone()));

        assert!(!gc_column_expr_is_tidb_shard(None));
        assert!(!gc_column_expr_is_tidb_shard(Some(&column)));

        let eq = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("eq"),
            field_type.clone(),
            vec![column.clone()],
        ));
        assert!(!gc_column_expr_is_tidb_shard(Some(&eq)));

        let shard = Expression::ScalarFunction(ScalarFunction::new(
            CiString::new("TiDB_ShArD"),
            field_type,
            vec![column],
        ));
        assert!(gc_column_expr_is_tidb_shard(Some(&shard)));
    }

    #[test]
    fn distinct_unique_ids_hash_differently() {
        let mut c1 = col(1);
        let mut c2 = col(2);
        assert_ne!(c1.hash_code(), c2.hash_code());
    }

    /// Source: `pkg/expression/column_test.go::TestColumnHashEquals`.
    #[test]
    fn column_hash_equals_matches_source() {
        let col1 = Column {
            unique_id: 1,
            ..Default::default()
        };
        let mut col2 = col1.clone();
        assert_eq!(col1.hash64(), col2.hash64());
        assert!(col1.equals(&col2));

        col2.unique_id = 2;
        assert_ne!(col1.hash64(), col2.hash64());
        assert!(!col1.equals(&col2));

        col2.unique_id = col1.unique_id;
        col2.id = 2;
        assert_ne!(col1.hash64(), col2.hash64());
        assert!(!col1.equals(&col2));

        col2.id = col1.id;
        col2.ret_type = Some(FieldType::new(FieldTypeCode::LongLong));
        assert_ne!(col1.hash64(), col2.hash64());
        assert!(!col1.equals(&col2));

        col2.ret_type = col1.ret_type.clone();
        col2.index = 1;
        assert_ne!(col1.hash64(), col2.hash64());
        assert!(!col1.equals(&col2));

        col2.index = col1.index;
        col2.orig_name = "a".to_owned();
        assert_ne!(col1.hash64(), col2.hash64());
        assert!(!col1.equals(&col2));

        col2.orig_name.clone_from(&col1.orig_name);
        col2.is_hidden = true;
        assert_ne!(col1.hash64(), col2.hash64());
        assert!(!col1.equals(&col2));

        col2.is_hidden = col1.is_hidden;
        col2.is_prefix = true;
        assert_ne!(col1.hash64(), col2.hash64());
        assert!(!col1.equals(&col2));

        col2.is_prefix = col1.is_prefix;
        col2.in_operand = true;
        assert_ne!(col1.hash64(), col2.hash64());
        assert!(!col1.equals(&col2));

        col2.in_operand = col1.in_operand;
        col2.collation.set_charset_and_collation("", "aa");
        assert_ne!(col1.hash64(), col2.hash64());
        assert!(!col1.equals(&col2));

        col2.collation = col1.collation.clone();
        col2.correlated_col_unique_id = 1;
        assert_ne!(col1.hash64(), col2.hash64());
        assert!(!col1.equals(&col2));
    }

    /// Source: `pkg/expression/column_test.go::TestColumnHashEuqals4VirtualExpr`.
    #[test]
    fn column_virtual_expression_hash_equals_matches_source() {
        let zero = || Box::new(Expression::Constant(crate::constant::Constant::new_zero()));
        let mut col1 = Column {
            unique_id: 1,
            virtual_expr: Some(zero()),
            ..Default::default()
        };
        let mut col2 = Column {
            unique_id: 1,
            ..Default::default()
        };

        assert_ne!(col1.hash64(), col2.hash64());
        assert!(!col1.equals(&col2));

        col2.virtual_expr = Some(zero());
        assert_eq!(col1.hash64(), col2.hash64());
        assert!(col1.equals(&col2));

        col1.virtual_expr = None;
        col2.virtual_expr = None;
        assert_eq!(col1.hash64(), col2.hash64());
        assert!(col1.equals(&col2));
    }

    #[test]
    fn plain_column_is_not_correlated_or_const() {
        let c = col(1);
        assert!(!c.is_correlated());
        assert_eq!(c.const_level(), ConstLevel::NONE);
        assert!(matches!(c.decorrelate(), Expression::Column(column) if column.unique_id == 1));
    }

    #[test]
    fn correlated_column_decorrelates_and_is_correlated() {
        let mut cc = CorrelatedColumn {
            column: col(5),
            data: Some(Datum::Int(3)),
        };
        assert!(cc.is_correlated());
        assert_eq!(cc.const_level(), ConstLevel::NONE);
        // Equal only to another correlated column with the same UniqueID.
        assert!(
            cc.equal_column(&Expression::CorrelatedColumn(CorrelatedColumn {
                column: col(5),
                data: None,
            }))
        );
        assert!(!cc.equal_column(&Expression::Column(col(5))));
        assert!(
            !cc.equal_column(&Expression::CorrelatedColumn(CorrelatedColumn {
                column: col(6),
                data: None,
            }))
        );
        let contained = Schema::new(vec![col(5)]);
        assert!(
            matches!(cc.decorrelate(&contained), Expression::Column(column) if column.unique_id == 5)
        );
        let missing = Schema::new(vec![col(6)]);
        assert!(matches!(
            cc.decorrelate(&missing),
            Expression::CorrelatedColumn(_)
        ));
        // HashCode matches the embedded column's.
        let mut plain = col(5);
        assert_eq!(cc.hash_code(), plain.hash_code());
    }

    /// Source: `pkg/expression/column_test.go::TestColumn`.
    #[test]
    fn correlated_column_eval_matches_source_datum_kinds() {
        let datetime = Time::new(
            core_time_from_datetime(chrono::Utc.with_ymd_and_hms(2017, 1, 18, 1, 1, 1).unwrap()),
            TimeType::DateTime,
            0,
        )
        .expect("valid source datetime");
        let cases = [
            (FieldTypeCode::LongLong, Datum::Int(1)),
            (FieldTypeCode::Double, Datum::Real(1.2)),
            (
                FieldTypeCode::NewDecimal,
                Datum::Decimal(Decimal::from_literal("1.2")),
            ),
            (FieldTypeCode::Varchar, Datum::new_string("abc")),
            (
                FieldTypeCode::Duration,
                Datum::Duration(MySqlDuration::new(0, 0, 0, 0, 0).expect("valid zero duration")),
            ),
            (FieldTypeCode::Datetime, Datum::Time(datetime)),
        ];

        for (code, datum) in cases {
            let correlated = CorrelatedColumn {
                column: Column::new(1, FieldType::new(code)),
                data: Some(datum.clone()),
            };
            assert_eq!(correlated.eval(), datum, "{code:?}");
        }
    }
}
