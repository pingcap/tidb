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

//! Go `Expression.MemoryUsage`: what a retained expression tree costs, as
//! the plan cache's LRU charges it (`PhysicalPlan.MemoryUsage` sums these
//! for every condition and key a node keeps).
//!
//! The constant terms are Go's `unsafe.Sizeof` of the corresponding struct
//! on 64-bit, the same convention `FieldType::memory_usage` follows, so a
//! cached plan is charged what Go charges it rather than what this port's
//! own layouts happen to weigh. One named departure: a `Datum` payload goes
//! through `Datum::estimated_mem_usage` (see its own note on Go's
//! `MemUsage`).

use tidb_util::size::{SIZE_OF_POINTER, SIZE_OF_SLICE};

use crate::column::{Column, CorrelatedColumn};
use crate::constant::Constant;
use crate::expression::Expression;
use crate::scalar_function::ScalarFunction;
use crate::schema::Schema;

/// Go `emptyColumnSize` (`column.go:866`): `unsafe.Sizeof(Column{})`.
pub const GO_EMPTY_COLUMN_SIZE: i64 = 160;
/// Go `emptyConstantSize` (`constant.go:660`).
pub const GO_EMPTY_CONSTANT_SIZE: i64 = 192;
/// Go `emptyScalarFunctionSize` (`scalar_function.go:956`).
pub const GO_EMPTY_SCALAR_FUNCTION_SIZE: i64 = 104;
/// Go `emptyBaseBuiltinFunc` (`builtin.go:1080`): the builtin object every
/// `ScalarFunction` owns and charges through `Function.MemoryUsage()`.
pub const GO_EMPTY_BASE_BUILTIN_FUNC_SIZE: i64 = 152;
/// Go `emptyLocalColumnPoolSize` (`builtin_vectorized.go:79`), the builtin's
/// `bufAllocator`.
pub const GO_EMPTY_LOCAL_COLUMN_POOL_SIZE: i64 = 40;
/// Go `onceSize` (`builtin.go:1081`): `newBaseBuiltinFunc` always allocates
/// `childrenVectorizedOnce`.
pub const GO_ONCE_SIZE: i64 = 12;
/// Go `types.EmptyDatumSize` (`datum.go:82`).
pub const GO_EMPTY_DATUM_SIZE: i64 = 72;
/// Go `emptySchemaSize` (`schema.go:280`): three slice headers.
pub const GO_EMPTY_SCHEMA_SIZE: i64 = 72;

impl Schema {
    /// Go `Schema.MemoryUsage` (`schema.go:283-303`). Go charges slice
    /// capacities; every list here is built exact-sized, so lengths are the
    /// capacities.
    #[must_use]
    pub fn memory_usage(&self) -> i64 {
        let mut sum = GO_EMPTY_SCHEMA_SIZE
            + self.columns.len() as i64 * SIZE_OF_POINTER
            + (self.pk_or_uk.len() + self.nullable_uk.len()) as i64 * SIZE_OF_SLICE
            + self.columns.iter().map(Column::memory_usage).sum::<i64>();
        for key in self.pk_or_uk.iter().chain(&self.nullable_uk) {
            sum += key.len() as i64 * SIZE_OF_POINTER
                + key.iter().map(Column::memory_usage).sum::<i64>();
        }
        sum
    }
}

impl Expression {
    /// Go `Expression.MemoryUsage`.
    #[must_use]
    pub fn memory_usage(&self) -> i64 {
        match self {
            Self::Column(column) => column.memory_usage(),
            Self::Constant(constant) => constant.memory_usage(),
            Self::CorrelatedColumn(column) => column.memory_usage(),
            Self::ScalarFunction(function) => function.memory_usage(),
        }
    }
}

impl Column {
    /// Go `Column.MemoryUsage` (`column.go:869-883`). Go's `collationInfo`
    /// strings are the ones `SetCharsetAndCollation` copied off `RetType`,
    /// so they are read from it here.
    #[must_use]
    pub fn memory_usage(&self) -> i64 {
        let mut sum =
            GO_EMPTY_COLUMN_SIZE + self.hashcode_capacity() as i64 + self.orig_name.len() as i64;
        if let Some(ret_type) = &self.ret_type {
            sum += (ret_type.charset_name().len() + ret_type.collation_name().len()) as i64
                + ret_type.memory_usage() as i64;
        }
        if let Some(virtual_expr) = &self.virtual_expr {
            sum += virtual_expr.memory_usage();
        }
        sum
    }
}

impl CorrelatedColumn {
    /// Go `CorrelatedColumn.MemoryUsage` (`column.go:233-243`).
    #[must_use]
    pub fn memory_usage(&self) -> i64 {
        self.column.memory_usage()
            + SIZE_OF_POINTER
            + self.data.as_ref().map_or(0, |data| {
                data.read()
                    .map_or(0, |value| value.estimated_mem_usage() as i64)
            })
    }
}

impl Constant {
    /// Go `Constant.MemoryUsage` (`constant.go:663-673`).
    #[must_use]
    pub fn memory_usage(&self) -> i64 {
        GO_EMPTY_CONSTANT_SIZE
            + self.value.estimated_mem_usage() as i64
            + self.hashcode_capacity() as i64
            + self
                .ret_type
                .as_ref()
                .map_or(0, |ret_type| ret_type.memory_usage() as i64)
    }
}

impl ScalarFunction {
    /// Go `ScalarFunction.MemoryUsage` (`scalar_function.go:959-972`) plus
    /// the `baseBuiltinFunc.MemoryUsage` (`builtin.go:1084-1102`) it always
    /// adds: the builtin's own struct, its column pool, its `sync.Once`, its
    /// copy of the return type, its collation strings, and every argument.
    #[must_use]
    pub fn memory_usage(&self) -> i64 {
        let ret_type = self
            .ret_type
            .as_ref()
            .map_or(0, |ret_type| ret_type.memory_usage() as i64);
        let (charset, collation) = self.collation.charset_and_collation();
        GO_EMPTY_SCALAR_FUNCTION_SIZE
            + (self.func_name.original().len() + self.func_name.lowercase().len()) as i64
            + self.hashcode_capacity() as i64
            + ret_type
            + GO_EMPTY_BASE_BUILTIN_FUNC_SIZE
            + (charset.len() + collation.len()) as i64
            + GO_EMPTY_LOCAL_COLUMN_POOL_SIZE
            + ret_type
            + GO_ONCE_SIZE
            + self.args.iter().map(Expression::memory_usage).sum::<i64>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_ast::CiString;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::Long)
    }

    /// The constant terms are Go's, so a bare column weighs exactly what Go
    /// charges for one: `emptyColumnSize` plus its type, and a name adds its
    /// bytes.
    #[test]
    fn a_column_weighs_gos_empty_size_plus_its_type_and_name() {
        let plain = Column::new(1, long());
        let ret_type = long();
        let expected = GO_EMPTY_COLUMN_SIZE
            + (ret_type.charset_name().len() + ret_type.collation_name().len()) as i64
            + ret_type.memory_usage() as i64;
        assert_eq!(plain.memory_usage(), expected);
        let mut named = Column::new(1, long());
        named.orig_name = "test.t.a".to_owned();
        assert_eq!(named.memory_usage(), expected + 8);
    }

    /// A function charges its builtin, its arguments and its name twice
    /// (`CIStr` keeps both spellings), the way Go's does.
    #[test]
    fn a_function_charges_its_builtin_and_every_argument() {
        let left = Column::new(1, long());
        let right = Column::new(2, long());
        let args_usage = left.memory_usage() + right.memory_usage();
        let function = ScalarFunction::new(
            CiString::new("eq"),
            long(),
            vec![Expression::Column(left), Expression::Column(right)],
        );
        let usage = function.memory_usage();
        assert!(
            usage > args_usage + GO_EMPTY_SCALAR_FUNCTION_SIZE + GO_EMPTY_BASE_BUILTIN_FUNC_SIZE
        );
        let constant = Constant::new(Datum::Int(1), long());
        assert!(constant.memory_usage() >= GO_EMPTY_CONSTANT_SIZE);
        assert_eq!(
            Expression::ScalarFunction(function.clone()).memory_usage(),
            function.memory_usage()
        );
    }

    /// A schema charges Go's empty size, one pointer per column, one slice
    /// header per key, and every column it holds, keys included.
    #[test]
    fn a_schema_charges_its_columns_and_keys() {
        let a = Column::new(1, long());
        let b = Column::new(2, long());
        let mut schema = Schema::new(vec![a.clone(), b.clone()]);
        let columns_only =
            GO_EMPTY_SCHEMA_SIZE + 2 * SIZE_OF_POINTER + a.memory_usage() + b.memory_usage();
        assert_eq!(schema.memory_usage(), columns_only);
        schema.pk_or_uk.push(vec![a.clone()]);
        assert_eq!(
            schema.memory_usage(),
            columns_only + SIZE_OF_SLICE + SIZE_OF_POINTER + a.memory_usage()
        );
    }
}
