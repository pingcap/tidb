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

//! Go `getModifyColumnType` (`pkg/ddl/modify_column.go`): how a MODIFY /
//! CHANGE COLUMN that `checkModifyTypes` admitted changes the stored data.
//!
//! The kind decides what the job leaves behind -- a column replaced in place,
//! one whose existing rows were checked first, indexes rebuilt under new ids,
//! or a column rewritten under a new id -- so every builder of the job asks
//! this one function.

use tidb_datatype::{FieldType, FieldTypeCode};

/// Go `model.ModifyType*`, in the order `onModifyColumn` dispatches them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModifyColumnType {
    /// The new type's range covers the old one: metadata only.
    NoReorg,
    /// The new type's range may not cover the old one, but strict SQL mode
    /// lets a check of the existing rows stand in for rewriting them.
    NoReorgWithCheck,
    /// VARCHAR -> CHAR under one of the checked kinds: the rows are checked
    /// first (`precheckForVarcharToChar`), and a value that fails turns the
    /// change into [`ModifyColumnType::Reorg`].
    Precheck,
    /// The rows keep their bytes; every index on the column is rebuilt.
    IndexReorg,
    /// Every row is rewritten under a new column id, and every index on the
    /// column is rebuilt.
    Reorg,
}

/// The table facts `getModifyColumnType` reads besides the two types.
#[derive(Clone, Copy, Debug, Default)]
pub struct ModifyColumnFacts {
    /// Whether some index covers the column (Go `getRelatedIndexIDs`).
    pub column_indexed: bool,
    /// Whether the table is partitioned.
    pub partitioned: bool,
    /// Whether the table has a TiFlash replica.
    pub tiflash_replica: bool,
    /// Whether the job's SQL mode is strict.
    pub strict_sql_mode: bool,
}

/// Go `getModifyColumnType` followed by `onModifyColumn`'s VARCHAR -> CHAR
/// adjustment, which routes the two checked kinds through
/// [`ModifyColumnType::Precheck`].
#[must_use]
pub fn modify_column_type(
    old: &FieldType,
    new: &FieldType,
    facts: ModifyColumnFacts,
) -> ModifyColumnType {
    let kind = get_modify_column_type(old, new, facts);
    if old.code() == FieldTypeCode::Varchar
        && new.code() == FieldTypeCode::String
        && matches!(
            kind,
            ModifyColumnType::NoReorgWithCheck | ModifyColumnType::IndexReorg
        )
    {
        return ModifyColumnType::Precheck;
    }
    kind
}

/// Go `getModifyColumnType`: the kind before the VARCHAR -> CHAR adjustment,
/// which is also what a passed precheck recomputes.
#[must_use]
pub fn get_modify_column_type(
    old: &FieldType,
    new: &FieldType,
    facts: ModifyColumnFacts,
) -> ModifyColumnType {
    if no_reorg_data_strict(old, new, facts.column_indexed) {
        if !is_null_to_not_null_change(old, new) {
            return ModifyColumnType::NoReorg;
        }
        return ModifyColumnType::NoReorgWithCheck;
    }
    // Go: "FIXME(joechenrh): handle partition and TiFlash replica case".
    if facts.partitioned || facts.tiflash_replica {
        return ModifyColumnType::Reorg;
    }
    // Go: statistics may hold bytes `codec.EncodeKey` produced, so a change
    // that encodes the same value differently is reorganized.
    if (is_integer_change(old, new) && old.is_unsigned() != new.is_unsigned())
        || (is_char_change(old, new)
            && !tidb_datatype::compatible_collate(old.collation_name(), new.collation_name()))
    {
        return ModifyColumnType::Reorg;
    }
    if !facts.strict_sql_mode {
        return ModifyColumnType::Reorg;
    }
    if need_row_reorg(old, new) {
        return ModifyColumnType::Reorg;
    }
    if !facts.column_indexed || !need_index_reorg(old, new) {
        return ModifyColumnType::NoReorgWithCheck;
    }
    ModifyColumnType::IndexReorg
}

/// Go `noReorgDataStrict`: whether every value of the old type is a value of
/// the new one, so the change needs neither a rewrite nor a check.
#[must_use]
pub fn no_reorg_data_strict(old: &FieldType, new: &FieldType, column_indexed: bool) -> bool {
    let to_unsigned = new.is_unsigned();
    let origin_unsigned = old.is_unsigned();
    let need_truncation_or_toggle_sign = || {
        (new.flen() > 0 && (new.flen() < old.flen() || new.decimal() < old.decimal()))
            || to_unsigned != origin_unsigned
    };
    // Go ignores an integer's declared display width and compares the
    // types' default widths instead.
    let default_old_flen = old.code().default_field_length_and_decimal().0;
    let default_new_flen = new.code().default_field_length_and_decimal().0;
    let need_truncation_or_toggle_sign_for_integer = || {
        (default_new_flen > 0 && default_new_flen < default_old_flen)
            || to_unsigned != origin_unsigned
    };

    if old.code() == new.code() {
        match old.code() {
            // A decimal encodes its precision, fraction and sign with the
            // value, so there is no shortcut between two decimals.
            FieldTypeCode::NewDecimal => {
                return old.flen() == new.flen()
                    && old.decimal() == new.decimal()
                    && to_unsigned == origin_unsigned;
            }
            FieldTypeCode::Enum | FieldTypeCode::Set => {
                return !is_elems_changed_to_modify_column(
                    &old.elems_snapshot(),
                    &new.elems_snapshot(),
                );
            }
            FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong => return to_unsigned == origin_unsigned,
            // A binary string pads with `\x00`, so any length change rewrites.
            FieldTypeCode::String if old.is_binary_string() => {
                return new.flen() == old.flen();
            }
            FieldTypeCode::VectorFloat32 => {
                return !(new.flen() != tidb_datatype::UNSPECIFIED_LENGTH
                    && old.flen() != new.flen());
            }
            _ => {}
        }
        return !need_truncation_or_toggle_sign();
    }

    let (old_code, new_code) = (old.code(), new.code());
    // VARCHAR -> CHAR may need a reorganization.
    if old_code.is_type_varchar() && new_code == FieldTypeCode::String {
        return false;
    }
    // CHAR -> VARCHAR: an index on the column may need one.
    if old_code == FieldTypeCode::String && new_code.is_type_varchar() && column_indexed {
        return false;
    }
    let is_string_family = |code: FieldTypeCode| {
        matches!(
            code,
            FieldTypeCode::Varchar
                | FieldTypeCode::String
                | FieldTypeCode::VarString
                | FieldTypeCode::Blob
                | FieldTypeCode::TinyBlob
                | FieldTypeCode::MediumBlob
                | FieldTypeCode::LongBlob
        )
    };
    if is_string_family(old_code) && is_string_family(new_code) {
        return !need_truncation_or_toggle_sign();
    }
    if old_code.is_integer_type() && new_code.is_integer_type() {
        return !need_truncation_or_toggle_sign_for_integer();
    }
    // Go: "conversion between float and double needs reorganization".
    false
}

/// Go `needRowReorg`: whether the stored row bytes must be rewritten.
#[must_use]
pub fn need_row_reorg(old: &FieldType, new: &FieldType) -> bool {
    if is_integer_change(old, new) {
        return false;
    }
    if !is_char_change(old, new) {
        return true;
    }
    // The charset was checked before; a binary string pads.
    old.is_binary_string() || new.is_binary_string()
}

/// Go `needIndexReorg`: whether the column's index entries must be rebuilt.
/// Called only for an integer or a char change.
#[must_use]
pub fn need_index_reorg(old: &FieldType, new: &FieldType) -> bool {
    if is_integer_change(old, new) {
        return old.is_unsigned() != new.is_unsigned();
    }
    // The key part (`tablecodec.GenIndexKey`).
    if !tidb_datatype::compatible_collate(old.collation_name(), new.collation_name()) {
        return true;
    }
    // The value part (`tablecodec.GenIndexValuePortal`).
    old.need_restored_data() != new.need_restored_data()
}

/// Go `isNullToNotNullChange`.
#[must_use]
pub fn is_null_to_not_null_change(old: &FieldType, new: &FieldType) -> bool {
    !old.has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL)
        && new.has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL)
}

/// Go `IsElemsChangedToModifyColumn`: an ENUM / SET keeps its stored
/// ordinals only when the new list extends the old one unchanged.
#[must_use]
pub fn is_elems_changed_to_modify_column<T: PartialEq>(old: &[T], new: &[T]) -> bool {
    new.len() < old.len() || old.iter().zip(new).any(|(old, new)| old != new)
}

/// Go `buildCheckSQLFromModifyColumn`'s WHERE clause, one row at a time:
/// whether `value`, the old column's value, is a row
/// `checkModifyColumnData` reports. `check_value_range` is Go's
/// `!noReorgDataStrict`, and `None` means Go builds no query at all.
///
/// The conditions are Go's: an integer outside the new type's range, a
/// string whose `LENGTH` (in bytes, against the new type's character `flen`,
/// as Go compares them) exceeds the new length, a VARCHAR -> CHAR value
/// ending in a space, and a NULL when the column becomes NOT NULL (unless it
/// becomes a TIMESTAMP, whose rewrite substitutes the statement time).
#[must_use]
pub fn modify_column_data_violation(
    old: &FieldType,
    new: &FieldType,
    check_value_range: bool,
    value: &tidb_datatype::Datum,
) -> Option<bool> {
    use tidb_datatype::Datum;
    let check_null = is_null_to_not_null_change(old, new)
        && !(old.code() != FieldTypeCode::Timestamp && new.code() == FieldTypeCode::Timestamp);
    if !check_value_range && !check_null {
        return None;
    }
    if value.is_null() {
        // A NULL satisfies none of the range or length conditions.
        return Some(check_null);
    }
    if !check_value_range {
        return Some(false);
    }
    if old.code().is_integer_type() && new.code().is_integer_type() {
        let out_of_range = if new.is_unsigned() {
            let upper = tidb_datatype::integer_unsigned_upper_bound(new.code());
            match value {
                Datum::Int(value) => *value < 0 || (*value as u64) > upper,
                Datum::UInt(value) => *value > upper,
                _ => false,
            }
        } else {
            let lower = tidb_datatype::integer_signed_lower_bound(new.code());
            let upper = tidb_datatype::integer_signed_upper_bound(new.code());
            match value {
                Datum::Int(value) => *value < lower || *value > upper,
                Datum::UInt(value) => *value > upper as u64,
                _ => false,
            }
        };
        return Some(out_of_range);
    }
    let bytes = value.sql_bytes().unwrap_or_default();
    let too_long = i64::try_from(bytes.len()).unwrap_or(i64::MAX) > new.flen();
    let trailing_space = old.code() == FieldTypeCode::Varchar
        && new.code() == FieldTypeCode::String
        && bytes.last() == Some(&b' ');
    Some(too_long || trailing_space)
}

fn is_integer_change(old: &FieldType, new: &FieldType) -> bool {
    old.code().is_integer_type() && new.code().is_integer_type()
}

fn is_char_change(old: &FieldType, new: &FieldType) -> bool {
    old.code().is_type_char() && new.code().is_type_char()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The declared type of `CREATE TABLE probe (a <declaration>)`, resolved as
    /// a column would be.
    fn column_type(declaration: &str) -> FieldType {
        let sql = format!("CREATE TABLE probe (a {declaration})");
        let stmt = tidb_parser::parse(&sql).expect("the probe parses");
        let tidb_ast::Stmt::Ddl(ddl) = &stmt else {
            panic!("expected a DDL statement");
        };
        let tidb_ast::DdlStmt::CreateTable(create) = &**ddl else {
            panic!("expected CREATE TABLE");
        };
        let column = &create.columns[0];
        let collate = column
            .options
            .iter()
            .find_map(|option| match option {
                tidb_ast::ColumnOption::Collate(name) => Some(name.clone()),
                _ => None,
            })
            .unwrap_or_else(|| "utf8mb4_bin".to_owned());
        let charset = collate
            .split('_')
            .next()
            .expect("a collation names its charset");
        let mut field_type =
            crate::ddl::column_field_type::build_field_type("a", &column.ty, charset, &collate)
                .expect("the probe type resolves");
        if column
            .options
            .iter()
            .any(|option| matches!(option, tidb_ast::ColumnOption::NotNull))
        {
            field_type.add_flags(tidb_datatype::FieldTypeFlags::NOT_NULL);
        }
        field_type
    }

    /// Go `modify_column_test.go::TestGetModifyColumnType`, the strict half:
    /// the kind its failpoint records for each type pair, with and without an
    /// index on the column.
    #[test]
    fn the_strict_mode_kinds_are_gos() {
        use ModifyColumnType::*;
        let cases = [
            ("int", "bigint", false, NoReorg),
            ("bigint", "int", false, NoReorgWithCheck),
            ("bigint", "int", true, NoReorgWithCheck),
            ("bigint", "bigint unsigned", false, Reorg),
            ("bigint", "bigint unsigned", true, Reorg),
            ("int unsigned", "bigint", false, Reorg),
            ("int unsigned", "bigint", true, Reorg),
            ("char(10)", "char(20)", false, NoReorg),
            ("char(20)", "char(10)", false, NoReorgWithCheck),
            (
                "char(20) collate utf8mb4_bin",
                "char(10) collate utf8mb4_bin",
                true,
                NoReorgWithCheck,
            ),
            (
                "char(20) collate utf8mb4_general_ci",
                "char(10) collate utf8mb4_general_ci",
                true,
                NoReorgWithCheck,
            ),
            ("char(10)", "varchar(20)", false, NoReorg),
            ("char(20)", "varchar(10)", false, NoReorgWithCheck),
            (
                "char(20) collate utf8mb4_bin",
                "varchar(10) collate utf8mb4_bin",
                true,
                IndexReorg,
            ),
            (
                "char(20) collate utf8mb4_general_ci",
                "varchar(10) collate utf8mb4_general_ci",
                true,
                NoReorgWithCheck,
            ),
            ("varchar(10)", "varchar(20)", false, NoReorg),
            ("varchar(20)", "varchar(10)", false, NoReorgWithCheck),
            (
                "varchar(20) collate utf8mb4_bin",
                "varchar(10) collate utf8mb4_bin",
                true,
                NoReorgWithCheck,
            ),
            (
                "varchar(20) collate utf8mb4_general_ci",
                "varchar(10) collate utf8mb4_general_ci",
                true,
                NoReorgWithCheck,
            ),
            // VARCHAR -> CHAR: Go's failpoint records the kind the precheck
            // recomputes once the rows pass, which is the unadjusted one.
            ("varchar(10)", "char(20)", false, NoReorgWithCheck),
            ("varchar(20)", "char(10)", false, NoReorgWithCheck),
            (
                "varchar(20) collate utf8mb4_bin",
                "char(10) collate utf8mb4_bin",
                true,
                IndexReorg,
            ),
            (
                "varchar(20) collate utf8mb4_general_ci",
                "char(10) collate utf8mb4_general_ci",
                true,
                NoReorgWithCheck,
            ),
            (
                "char(20) collate utf8mb4_bin",
                "varchar(10) collate utf8_unicode_ci",
                true,
                Reorg,
            ),
            (
                "char(20) collate utf8_unicode_ci",
                "varchar(10) collate utf8mb4_bin",
                true,
                Reorg,
            ),
            (
                "varchar(20) collate utf8mb4_bin",
                "char(10) collate utf8_unicode_ci",
                true,
                Reorg,
            ),
            (
                "varchar(20) collate utf8_unicode_ci",
                "char(10) collate utf8mb4_bin",
                true,
                Reorg,
            ),
        ];
        for (before, after, index, expected) in cases {
            let facts = ModifyColumnFacts {
                column_indexed: index,
                strict_sql_mode: true,
                ..ModifyColumnFacts::default()
            };
            assert_eq!(
                get_modify_column_type(&column_type(before), &column_type(after), facts),
                expected,
                "{before} -> {after}, index {index}"
            );
        }
    }

    /// The non-strict half: every checked kind becomes a rewrite.
    #[test]
    fn outside_strict_mode_a_checked_change_is_reorganized() {
        for (before, after) in [
            ("bigint", "int"),
            ("char(20)", "char(10)"),
            ("varchar(20)", "varchar(10)"),
            ("char(20)", "varchar(10)"),
            ("varchar(20)", "char(10)"),
        ] {
            assert_eq!(
                get_modify_column_type(
                    &column_type(before),
                    &column_type(after),
                    ModifyColumnFacts::default()
                ),
                ModifyColumnType::Reorg,
                "{before} -> {after}"
            );
        }
    }

    /// `onModifyColumn` sends VARCHAR -> CHAR through the precheck, and a
    /// partitioned table always reorganizes.
    #[test]
    fn varchar_to_char_prechecks_and_partitions_reorganize() {
        let strict = ModifyColumnFacts {
            strict_sql_mode: true,
            ..ModifyColumnFacts::default()
        };
        assert_eq!(
            modify_column_type(&column_type("varchar(10)"), &column_type("char(5)"), strict),
            ModifyColumnType::Precheck
        );
        assert_eq!(
            modify_column_type(
                &column_type("bigint"),
                &column_type("int"),
                ModifyColumnFacts {
                    partitioned: true,
                    ..strict
                }
            ),
            ModifyColumnType::Reorg
        );
        assert_eq!(
            modify_column_type(&column_type("int"), &column_type("bigint"), strict),
            ModifyColumnType::NoReorg
        );
        assert_eq!(
            modify_column_type(&column_type("int"), &column_type("int not null"), strict),
            ModifyColumnType::NoReorgWithCheck
        );
    }
}
