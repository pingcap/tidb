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

//! Go `pkg/util/schemacmp/table.go`: encoding tables into lattices.

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::rc::Rc;

use tidb_ast::IndexType;
use tidb_ast::{RestoreCtx, RestoreFlags, RestoreWriter};
use tidb_datatype::{FieldType, GoString, UNSPECIFIED_LENGTH};
use tidb_model::column::ColumnInfo;
use tidb_model::index::IndexInfo;
use tidb_model::table_info::TableInfo;
use tidb_mysql::types::{
    AutoIncrementFlag, MultipleKeyFlag, NotNullFlag, PriKeyFlag, UniqueKeyFlag,
};

use crate::lattice::{
    equality_singleton, map_lattice, maybe_singleton_interface, maybe_singleton_string, singleton,
    wrap_map_key_error, Bool, Equality, IncompatibleError, Int64, Lattice, LatticeMap, MapLattice,
    Tuple, Value,
};
use crate::typ::{Typ, ERR_MSG_AUTO_TYPE_WITHOUT_KEY};

const COLUMN_INFO_TUPLE_INDEX_DEFAULT_VALUE: usize = 0;
const COLUMN_INFO_TUPLE_INDEX_GENERATED_EXPR_STRING: usize = 1;
const COLUMN_INFO_TUPLE_INDEX_GENERATED_STORED: usize = 2;
const COLUMN_INFO_TUPLE_INDEX_FIELD_TYPES: usize = 3;

/// Go `encodeColumnInfoToLattice`: collects the necessary information for
/// comparing a column.
fn encode_column_info_to_lattice(ci: &ColumnInfo) -> Tuple {
    Tuple(vec![
        maybe_singleton_interface(&ci.default_value),
        singleton(Value::Str(GoString::from(
            ci.generated_expr_string.as_str(),
        ))),
        singleton(Value::Bool(ci.generated_stored)),
        Box::new(Typ::new(&ci.field_type)),
    ])
}

/// Go `restoreColumnInfoFromUnwrapped`: restores the text representation of a
/// column.
fn restore_column_info_from_unwrapped<W: RestoreWriter>(
    ctx: &mut RestoreCtx<W>,
    col: &[Value],
    col_name: &str,
) {
    let Value::FieldType(typ) = &col[COLUMN_INFO_TUPLE_INDEX_FIELD_TYPES] else {
        unreachable!("a column tuple's last entry is its field type");
    };

    ctx.write_name(col_name);
    ctx.write_plain(" ");
    // Go delegates to `types.FieldType.Restore(ctx)`; the Rust field type
    // owns its default-flag restore rendering, which is identical under the
    // default restore flags this package uses.
    ctx.write_plain(&typ.restore());
    if let Value::Str(gen_expr) = &col[COLUMN_INFO_TUPLE_INDEX_GENERATED_EXPR_STRING] {
        if !gen_expr.as_bytes().is_empty() {
            ctx.write_keyword(" GENERATED ALWAYS AS ");
            ctx.write_plain_fmt(format_args!("({})", gen_expr.to_utf8_lossy_go()));
        }
    }
    if col[COLUMN_INFO_TUPLE_INDEX_GENERATED_STORED] == Value::Bool(true) {
        ctx.write_keyword(" STORED");
    }
    if typ.raw_flags() & NotNullFlag as u64 != 0 {
        ctx.write_keyword(" NOT NULL");
    }
    let def_val = &col[COLUMN_INFO_TUPLE_INDEX_DEFAULT_VALUE];
    if *def_val != Value::Nil {
        ctx.write_keyword(" DEFAULT ");
        ctx.write_plain(&def_val.go_format());
    }
    if typ.raw_flags() & AutoIncrementFlag as u64 != 0 {
        ctx.write_keyword(" AUTO_INCREMENT");
    }
}

const INDEX_INFO_TUPLE_INDEX_COLUMNS: usize = 0;
const INDEX_INFO_TUPLE_INDEX_NOT_UNIQUE: usize = 1;
const INDEX_INFO_TUPLE_INDEX_NOT_PRIMARY: usize = 2;
const INDEX_INFO_TUPLE_INDEX_TYPE: usize = 3;

/// Go's unexported `indexColumn` struct.
#[derive(Clone, Debug, PartialEq, Eq)]
struct IndexColumn {
    col_name: String,
    length: i64,
}

/// Go's unexported `indexColumnSlice` type.
#[derive(Clone, Debug, PartialEq, Eq)]
struct IndexColumnSlice(Vec<IndexColumn>);

impl Equality for IndexColumnSlice {
    fn equals(&self, other: &dyn Equality) -> bool {
        let Some(b) = other.as_any().downcast_ref::<Self>() else {
            return false;
        };
        self == b
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Go `encodeIndexInfoToLattice`.
fn encode_index_info_to_lattice(ii: &IndexInfo) -> Tuple {
    let mut index_columns = Vec::with_capacity(ii.columns.len());
    for column in ii.columns.iter_deref() {
        let column = column.read();
        index_columns.push(IndexColumn {
            col_name: column.name.lowercase().to_owned(),
            length: column.length,
        });
    }

    Tuple(vec![
        equality_singleton(Rc::new(IndexColumnSlice(index_columns))),
        Box::new(Bool(!ii.unique)),
        Box::new(Bool(!ii.primary)),
        singleton(Value::IndexType(ii.tp)),
    ])
}

/// Go `encodeImplicitPrimaryKeyToLattice`.
fn encode_implicit_primary_key_to_lattice(ci: &ColumnInfo) -> Tuple {
    Tuple(vec![
        equality_singleton(Rc::new(IndexColumnSlice(vec![IndexColumn {
            col_name: ci.name.lowercase().to_owned(),
            length: UNSPECIFIED_LENGTH,
        }]))),
        Box::new(Bool(false)),
        Box::new(Bool(false)),
        singleton(Value::IndexType(IndexType::BTREE)),
    ])
}

/// Go `restoreIndexInfoFromUnwrapped`.
fn restore_index_info_from_unwrapped<W: RestoreWriter>(
    ctx: &mut RestoreCtx<W>,
    index: &[Value],
    key_name: &str,
) {
    let is_primary = index[INDEX_INFO_TUPLE_INDEX_NOT_PRIMARY] == Value::Bool(false);

    if is_primary {
        ctx.write_keyword("PRIMARY KEY");
    } else if index[INDEX_INFO_TUPLE_INDEX_NOT_UNIQUE] == Value::Bool(false) {
        ctx.write_keyword("UNIQUE KEY ");
        ctx.write_name(key_name);
    } else {
        ctx.write_keyword("KEY ");
        ctx.write_name(key_name);
    }

    if let Value::IndexType(tp) = index[INDEX_INFO_TUPLE_INDEX_TYPE] {
        if tp != IndexType::BTREE {
            ctx.write_keyword(" USING ");
            ctx.write_keyword(tp.sql());
        }
    }

    ctx.write_plain(" (");
    let Value::Equality(columns) = &index[INDEX_INFO_TUPLE_INDEX_COLUMNS] else {
        unreachable!("an index tuple's first entry is its column list");
    };
    let columns = columns
        .as_any()
        .downcast_ref::<IndexColumnSlice>()
        .expect("an index tuple's column list is an indexColumnSlice");
    for (position, column) in columns.0.iter().enumerate() {
        if position != 0 {
            ctx.write_plain(", ");
        }
        ctx.write_name(&column.col_name);
        if column.length != UNSPECIFIED_LENGTH {
            ctx.write_plain_fmt(format_args!("({})", column.length));
        }
    }
    ctx.write_plain(")");
}

macro_rules! tuple_map {
    ($(#[$doc:meta])* $name:ident, $compare_with_nil:item, $join_with_nil:item, $should_delete:literal) => {
        $(#[$doc])*
        #[derive(Clone, Debug, Default)]
        struct $name(BTreeMap<String, Tuple>);

        impl LatticeMap for $name {
            fn new_empty(&self) -> Box<dyn LatticeMap> {
                Box::new(Self::default())
            }

            fn insert(&mut self, key: &str, value: Box<dyn Lattice>) {
                let tuple = value
                    .into_any()
                    .downcast::<Tuple>()
                    .expect("the map stores column/index tuples");
                self.0.insert(key.to_owned(), *tuple);
            }

            fn get(&self, key: &str) -> Option<Box<dyn Lattice>> {
                self.0.get(key).map(|tuple| tuple.clone_lattice())
            }

            fn for_each(
                &self,
                action: &mut dyn FnMut(&str, &dyn Lattice) -> Result<(), IncompatibleError>,
            ) -> Result<(), IncompatibleError> {
                for (key, value) in &self.0 {
                    action(key, value)?;
                }
                Ok(())
            }

            $compare_with_nil

            $join_with_nil

            fn should_delete_incompatible_join(&self) -> bool {
                $should_delete
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn Any {
                self
            }

            fn clone_map(&self) -> Box<dyn LatticeMap> {
                Box::new(self.clone())
            }
        }
    };
}

tuple_map!(
    /// Go's unexported `columnMap` type.
    ColumnMap,
    fn compare_with_nil(&self, value: &dyn Lattice) -> Result<i32, IncompatibleError> {
        let tuple = value
            .as_any()
            .downcast_ref::<Tuple>()
            .expect("the map stores column tuples");
        let typ = tuple.0[COLUMN_INFO_TUPLE_INDEX_FIELD_TYPES]
            .as_any()
            .downcast_ref::<Typ>()
            .expect("a column tuple's last entry is its field type");
        if typ.has_default() {
            return Ok(1);
        }
        Err(IncompatibleError::raw(
            "column with no default value cannot be missing",
        ))
    },
    fn join_with_nil(
        &self,
        value: &dyn Lattice,
    ) -> Result<Option<Box<dyn Lattice>>, IncompatibleError> {
        let mut col = value
            .as_any()
            .downcast_ref::<Tuple>()
            .expect("the map stores column tuples")
            .clone();
        let mut ty = col.0[COLUMN_INFO_TUPLE_INDEX_FIELD_TYPES]
            .as_any()
            .downcast_ref::<Typ>()
            .expect("a column tuple's last entry is its field type")
            .clone();
        if ty.set_flag_for_missing_column() && ty.is_not_null() {
            col.0[COLUMN_INFO_TUPLE_INDEX_DEFAULT_VALUE] = crate::lattice::maybe(Some(
                singleton(ty.get_standard_default_value()),
            ));
        }
        col.0[COLUMN_INFO_TUPLE_INDEX_FIELD_TYPES] = Box::new(ty);
        Ok(Some(Box::new(col)))
    },
    false
);

tuple_map!(
    /// Go's unexported `indexMap` type.
    IndexMap,
    fn compare_with_nil(&self, _value: &dyn Lattice) -> Result<i32, IncompatibleError> {
        Ok(-1)
    },
    fn join_with_nil(
        &self,
        _value: &dyn Lattice,
    ) -> Result<Option<Box<dyn Lattice>>, IncompatibleError> {
        Ok(None)
    },
    true
);

const TABLE_INFO_TUPLE_INDEX_COLLATE: usize = 0;
const TABLE_INFO_TUPLE_INDEX_COLUMNS: usize = 1;
const TABLE_INFO_TUPLE_INDEX_INDICES: usize = 2;
#[allow(dead_code)]
const TABLE_INFO_TUPLE_INDEX_AUTO_INC_ID: usize = 3;
const TABLE_INFO_TUPLE_INDEX_SHARD_ROW_ID_BITS: usize = 4;
const TABLE_INFO_TUPLE_INDEX_AUTO_RANDOM_BITS: usize = 5;
#[allow(dead_code)]
const TABLE_INFO_TUPLE_INDEX_PRE_SPLIT_REGIONS: usize = 6;
const TABLE_INFO_TUPLE_INDEX_COMPRESSION: usize = 7;

/// Go `encodeTableInfoToLattice`.
fn encode_table_info_to_lattice(ti: &TableInfo) -> Tuple {
    // TODO(from Go): Handle VIEW and PARTITION and SEQUENCE
    let mut has_explicit_primary_key = false;
    let mut indices = IndexMap::default();
    for ii in ti.indices.iter_deref() {
        let ii = ii.read();
        if ii.primary {
            has_explicit_primary_key = true;
        }
        indices.0.insert(
            ii.name.lowercase().to_owned(),
            encode_index_info_to_lattice(&ii),
        );
    }
    let mut columns = ColumnMap::default();
    for ci in ti.columns.iter_deref() {
        let ci = ci.read();
        columns.0.insert(
            ci.name.lowercase().to_owned(),
            encode_column_info_to_lattice(&ci),
        );
        if !has_explicit_primary_key && ci.field_type.raw_flags() & PriKeyFlag as u64 != 0 {
            indices.0.insert(
                "primary".to_owned(),
                encode_implicit_primary_key_to_lattice(&ci),
            );
        }
    }

    Tuple(vec![
        Box::new(crate::charset_collation::collation(&ti.collate)),
        map_lattice(Box::new(columns)),
        map_lattice(Box::new(indices)),
        // TODO(from Go): ForeignKeys?
        Box::new(Int64(ti.auto_inc_id)),
        // TODO(from Go): Relax these?
        singleton(Value::Uint64(ti.shard_row_id_bits)),
        singleton(Value::Uint64(ti.auto_random_bits)),
        singleton(Value::Uint64(ti.pre_split_regions)),
        maybe_singleton_string(&ti.compression),
    ])
}

/// Go `restoreTableInfoFromUnwrapped`. Go sorts its unordered map snapshots
/// by key (`sortedMap`); the [`BTreeMap`] snapshots here are already in that
/// order.
fn restore_table_info_from_unwrapped<W: RestoreWriter>(
    ctx: &mut RestoreCtx<W>,
    table: &[Value],
    table_name: &str,
) {
    ctx.write_keyword("CREATE TABLE ");
    ctx.write_name(table_name);
    ctx.write_plain("(");

    let Value::Map(columns) = &table[TABLE_INFO_TUPLE_INDEX_COLUMNS] else {
        unreachable!("the table tuple's columns entry is a map");
    };
    for (position, (col_name, column)) in columns.iter().enumerate() {
        if position != 0 {
            ctx.write_plain(", ");
        }
        let Value::List(column) = column else {
            unreachable!("the columns map stores column tuples");
        };
        restore_column_info_from_unwrapped(ctx, column, col_name);
    }

    let Value::Map(indices) = &table[TABLE_INFO_TUPLE_INDEX_INDICES] else {
        unreachable!("the table tuple's indices entry is a map");
    };
    for (index_name, index) in indices {
        ctx.write_plain(", ");
        let Value::List(index) = index else {
            unreachable!("the indices map stores index tuples");
        };
        restore_index_info_from_unwrapped(ctx, index, index_name);
    }

    ctx.write_plain(")");

    let Value::Str(collate) = &table[TABLE_INFO_TUPLE_INDEX_COLLATE] else {
        unreachable!("the table tuple's collate entry is a string");
    };
    ctx.write_keyword(" COLLATE ");
    ctx.write_plain(&collate.to_utf8_lossy_go());

    if let Value::Uint64(bits) = table[TABLE_INFO_TUPLE_INDEX_SHARD_ROW_ID_BITS] {
        if bits > 0 {
            ctx.write_keyword(" SHARD_ROW_ID_BITS ");
            ctx.write_plain_fmt(format_args!("{bits}"));
        }
    }
    if let Value::Uint64(bits) = table[TABLE_INFO_TUPLE_INDEX_AUTO_RANDOM_BITS] {
        if bits > 0 {
            ctx.write_plain("/*");
            ctx.write_keyword(" AUTO_RANDOM_BITS ");
            ctx.write_plain_fmt(format_args!("{bits} */"));
        }
    }
    if let Value::Str(compression) = &table[TABLE_INFO_TUPLE_INDEX_COMPRESSION] {
        if !compression.as_bytes().is_empty() {
            ctx.write_keyword(" COMPRESSION ");
            ctx.write_string(&compression.to_utf8_lossy_go());
        }
    }
}

/// Go `Table`: a table in the database.
#[derive(Clone, Debug)]
pub struct Table {
    value: Box<dyn Lattice>,
}

/// Go `Encode`: encodes a table.
#[must_use]
pub fn encode(ti: &TableInfo) -> Table {
    Table {
        value: Box::new(encode_table_info_to_lattice(ti)),
    }
}

/// Go `DecodeColumnFieldTypes`: decodes column field types from the lattice.
/// The map is keyed by the lower-case column name, exactly like Go's.
#[must_use]
pub fn decode_column_field_types(t: &Table) -> BTreeMap<String, FieldType> {
    let Value::List(table) = t.value.unwrap() else {
        unreachable!("a table lattice unwraps to a list");
    };
    let Value::Map(column_maps) = &table[TABLE_INFO_TUPLE_INDEX_COLUMNS] else {
        unreachable!("the table tuple's columns entry is a map");
    };
    let mut cols = BTreeMap::new();
    for (key, value) in column_maps {
        let Value::List(column) = value else {
            unreachable!("the columns map stores column tuples");
        };
        let Value::FieldType(field_type) = &column[COLUMN_INFO_TUPLE_INDEX_FIELD_TYPES] else {
            unreachable!("a column tuple's last entry is its field type");
        };
        cols.insert(key.clone(), (**field_type).clone());
    }
    cols
}

impl Table {
    /// Go `Restore`: for debug use only.
    pub fn restore<W: RestoreWriter>(&self, ctx: &mut RestoreCtx<W>, table_name: &str) {
        let Value::List(table) = self.value.unwrap() else {
            unreachable!("a table lattice unwraps to a list");
        };
        restore_table_info_from_unwrapped(ctx, &table, table_name);
    }

    /// Go `Compare`: the `Lattice` comparison over whole tables.
    pub fn compare(&self, other: &Self) -> Result<i32, IncompatibleError> {
        self.value.compare(other.value.as_ref())
    }

    /// Go `Join`: a helper function to join two tables.
    pub fn join(&self, other: &Self) -> Result<Self, IncompatibleError> {
        let res = self.value.join(other.value.as_ref())?;
        let mut table = *res
            .into_any()
            .downcast::<Tuple>()
            .expect("a table join returns the table tuple");

        // Fix up the type's key flags. Unfortunately we cannot count on the
        // type's own flag joining because an index's joining rule is more
        // complex than 3 bits.
        let mut column_key_flags: BTreeMap<String, u64> = BTreeMap::new();
        {
            let indices = table.0[TABLE_INFO_TUPLE_INDEX_INDICES]
                .as_any()
                .downcast_ref::<MapLattice>()
                .expect("the table tuple's indices entry is a lattice map")
                .inner
                .as_any()
                .downcast_ref::<IndexMap>()
                .expect("the indices lattice map is an indexMap");
            for index in indices.0.values() {
                let Value::Equality(columns) = index.0[INDEX_INFO_TUPLE_INDEX_COLUMNS].unwrap()
                else {
                    unreachable!("an index tuple's first entry is its column list");
                };
                let cols = columns
                    .as_any()
                    .downcast_ref::<IndexColumnSlice>()
                    .expect("an index tuple's column list is an indexColumnSlice");
                if cols.0.is_empty() {
                    continue;
                }
                if index.0[INDEX_INFO_TUPLE_INDEX_NOT_PRIMARY].unwrap() == Value::Bool(false) {
                    for col in &cols.0 {
                        *column_key_flags.entry(col.col_name.clone()).or_insert(0) |=
                            PriKeyFlag as u64;
                    }
                } else if index.0[INDEX_INFO_TUPLE_INDEX_NOT_UNIQUE].unwrap() == Value::Bool(false)
                    && cols.0.len() == 1
                {
                    *column_key_flags
                        .entry(cols.0[0].col_name.clone())
                        .or_insert(0) |= UniqueKeyFlag as u64;
                } else {
                    // Only the first column can be set if index or unique
                    // index has multiple columns.
                    // See https://dev.mysql.com/doc/refman/5.7/en/show-columns.html.
                    *column_key_flags
                        .entry(cols.0[0].col_name.clone())
                        .or_insert(0) |= MultipleKeyFlag as u64;
                }
            }
        }
        let columns = table.0[TABLE_INFO_TUPLE_INDEX_COLUMNS]
            .as_any_mut()
            .downcast_mut::<MapLattice>()
            .expect("the table tuple's columns entry is a lattice map")
            .inner
            .as_any_mut()
            .downcast_mut::<ColumnMap>()
            .expect("the columns lattice map is a columnMap");
        for (name, column) in &mut columns.0 {
            let ty = column.0[COLUMN_INFO_TUPLE_INDEX_FIELD_TYPES]
                .as_any_mut()
                .downcast_mut::<Typ>()
                .expect("a column tuple's last entry is its field type");
            let flag = column_key_flags.get(name).copied();
            if flag.is_none() && ty.in_auto_increment() {
                return Err(wrap_map_key_error(
                    name,
                    &IncompatibleError::raw(ERR_MSG_AUTO_TYPE_WITHOUT_KEY),
                ));
            }
            ty.set_anti_key_flags(flag.unwrap_or(0));
        }

        Ok(Self {
            value: Box::new(table),
        })
    }
}

impl fmt::Display for Table {
    /// Go `String`.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut sb = String::new();
        let mut ctx = RestoreCtx::new(RestoreFlags::DEFAULT, &mut sb);
        self.restore(&mut ctx, "tbl");
        formatter.write_str(&sb)
    }
}
