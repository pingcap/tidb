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

//! Resolving the metadata that names a column, against the column list as it
//! is NOW.
//!
//! Three pieces of a `TableInfo` hold column NAMES rather than positions --
//! a generated column's dependency list, the partition expression's, and a
//! foreign key's referencing columns (Go `ColumnInfo.Dependences`,
//! `PartitionInfo.Columns`, `FKInfo.Cols`, all `CIStr`). That is what makes
//! an `ALTER TABLE` that reorders columns harmless: nothing stored has to be
//! remapped, because nothing stored is positional.
//!
//! The price is paid here, and only here. A name becomes an offset at the
//! moment of use ([`KvTable::foreign_key_offsets`], and
//! `generated_column::dependency_offsets` for the two expressions), and the
//! DDL that could make a name WRONG has to be refused rather than allowed to
//! dangle -- which is what [`KvTable::column_dependent`] is asked before a
//! column is dropped or renamed.

use super::{KvForeignKey, KvTable};

/// The piece of name-keyed metadata that stops a column being dropped or
/// renamed, as [`KvTable::column_dependent`] reports it.
///
/// One value per error code Go raises, because the code is the whole
/// observable difference: all three refusals say the same thing about the
/// same column and only the number tells a client which metadata objected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnDependent {
    /// The hidden generated column an expression index was rewritten into.
    /// Go `ErrDependentByFunctionalIndex` (3837).
    ExpressionIndex,
    /// A visible generated column whose expression reads this one. Go
    /// `ErrDependentByGeneratedColumn` (3108).
    GeneratedColumn,
    /// The partition expression, or the `PARTITION BY ... COLUMNS` list. Go
    /// `ErrDependentByPartitionFunctional` (3855).
    Partition,
}

impl KvTable {
    /// What name-keyed metadata reads the column at `offset`, if any.
    ///
    /// Three pieces of a `TableInfo` name a column rather than pointing at it:
    /// a generated column's expression, the hidden generated column an
    /// expression index was rewritten into, and the partition expression (or
    /// its `COLUMNS` list). None of them can survive the column's NAME going
    /// away, so a DDL that drops or renames the column has to answer for all
    /// three -- and Go answers by REFUSING, with a different code for each.
    ///
    /// Order is Go's, and it is observable: Go's
    /// `checkModifyColumnWithGeneratedColumnsConstraint` walks `t.Cols()` in
    /// offset order and the FIRST generated column that names this one
    /// decides between 3837 and 3108, and only then does
    /// `checkDropColumnWithPartitionConstraint` get to say 3855. Hidden
    /// columns are appended after the visible ones, so walking `self.columns`
    /// is that walk.
    #[must_use]
    pub fn column_dependent(&self, offset: usize) -> Option<ColumnDependent> {
        let name = self
            .columns
            .get(offset)
            .map(|column| column.name.as_str())?;
        let names_it = |dependencies: &[String]| {
            dependencies
                .iter()
                .any(|dependency| dependency.eq_ignore_ascii_case(name))
        };
        for (other, column) in self.columns.iter().enumerate() {
            let Some(generated) = column.generated.as_ref() else {
                continue;
            };
            if names_it(&generated.dependencies) {
                return Some(if self.is_hidden(other) {
                    ColumnDependent::ExpressionIndex
                } else {
                    ColumnDependent::GeneratedColumn
                });
            }
        }
        if self
            .partition()
            .is_some_and(|spec| names_it(&spec.dependencies))
        {
            return Some(ColumnDependent::Partition);
        }
        None
    }

    /// The offsets a foreign key's referencing columns sit at NOW, resolved
    /// from the names the constraint stores.
    ///
    /// `None` when a referencing column is gone, which DDL refuses to do
    /// (1553 keeps the index, and the column beneath it, alive).
    #[must_use]
    pub fn foreign_key_offsets(&self, foreign_key: &KvForeignKey) -> Option<Vec<usize>> {
        crate::generated_column::dependency_offsets(&self.columns, &foreign_key.cols).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv_table::FkAction;
    use crate::kv_table::KvColumn;
    use tidb_datatype::{FieldType, FieldTypeCode};

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::LongLong)
    }

    fn column(name: &str, id: i64) -> KvColumn {
        KvColumn {
            name: name.to_owned(),
            id,
            field_type: long(),
            default_value: None,
            origin_default: None,
            generated: None,
        }
    }

    fn test_table() -> KvTable {
        KvTable::new(42, vec![column("a", 1), column("s", 2)])
    }

    /// A foreign key names its referencing columns, so a column inserted
    /// BEFORE them must not move the constraint onto different columns.
    ///
    /// `ALTER TABLE` refuses to reach a table in a foreign key at all today
    /// (`crate::ddl::alter_table`), so this is the pin at the level the
    /// representation lives: with the offsets stored instead of the names,
    /// `cols = ["s"]` resolved to 1 before the insert and still to 1 after,
    /// which is `z` -- the constraint would have been checking the wrong
    /// column with no error anywhere.
    #[test]
    fn a_foreign_key_follows_its_columns_when_one_is_inserted_before_them() {
        let mut t = test_table();
        t.add_foreign_key(KvForeignKey {
            name: "fk_1".to_owned(),
            cols: vec!["s".to_owned()],
            ref_schema: "test".to_owned(),
            ref_table: "parent".to_owned(),
            ref_cols: vec!["s".to_owned()],
            on_delete: FkAction::Restrict,
            on_update: FkAction::Restrict,
        });
        // Re-read the constraint from the table on both sides: a clone taken
        // BEFORE the insert carries its own `cols` and would answer from
        // those, which is not the question.
        let offsets = |t: &KvTable| t.foreign_key_offsets(&t.foreign_keys()[0]);
        assert_eq!(offsets(&t), Some(vec![1]));
        t.add_column(0, column("z", 3));
        assert_eq!(
            offsets(&t),
            Some(vec![2]),
            "the constraint follows `s`, it does not stay at offset 1"
        );
    }
}
