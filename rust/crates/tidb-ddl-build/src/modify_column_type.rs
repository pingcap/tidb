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
use tidb_error::mysql::MysqlError;

use tidb_hack::GoToLower;

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

/// Go `checkModifyTypes` (`pkg/ddl/modify_column.go:2262`): the type
/// changes Go refuses outright (`checkTypeChangeSupported`), a reorganizing
/// change on a primary-key column, and the charset / collation moves the
/// change cannot make. `indexed` is whether an index covers the column.
///
/// # Errors
///
/// Go's 8200 / collation errors.
pub fn check_modify_types(
    origin: &FieldType,
    to: &FieldType,
    column_name: &str,
    indexed: bool,
) -> Result<(), MysqlError> {
    check_type_change_supported(origin, to)?;
    let can_reorganize = modify_type_needs_reorganization(origin, to);
    if can_reorganize && origin.has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY) {
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_util::dbterror::ERR_UNSUPPORTED_MODIFY_COLUMN
                .generate_with_stack_by_args(&["this column has primary key flag".into()]),
        ));
    }
    check_modify_types_charset_and_collation(origin, to, can_reorganize, column_name, indexed)
}

/// Go `types.CheckModifyTypeCompatible`'s `canReorg` result for a supported
/// type pair. `checkModifyTypes` uses this bit to distinguish an unsupported
/// metadata-only charset change from a charset change that the row rewrite
/// can perform while converting the type.
fn modify_type_needs_reorganization(origin: &FieldType, to: &FieldType) -> bool {
    if origin.code() == to.code() {
        if matches!(origin.code(), FieldTypeCode::Enum | FieldTypeCode::Set) {
            let old = origin.elems_snapshot();
            let new = to.elems_snapshot();
            if new.len() < old.len() || !new.starts_with(&old) {
                return true;
            }
        }
        if origin.code() == FieldTypeCode::NewDecimal
            && (origin.flen() != to.flen()
                || origin.decimal() != to.decimal()
                || origin.is_unsigned() != to.is_unsigned())
        {
            return true;
        }
    } else if !(origin.code().is_string() && to.code().is_string()
        || origin.code().is_integer_type() && to.code().is_integer_type())
    {
        return true;
    }

    if origin.code().converts_between_char_and_varchar(to.code()) {
        return true;
    }

    let (mut old_flen, mut new_flen) = (origin.flen(), to.flen());
    if origin.code().is_integer_type() && to.code().is_integer_type() {
        old_flen = i64::from(origin.code().default_field_length_and_decimal().0);
        new_flen = i64::from(to.code().default_field_length_and_decimal().0);
    }
    if new_flen > 0 && new_flen != old_flen {
        if new_flen < old_flen {
            return true;
        }
        if origin.code() == FieldTypeCode::String
            && to.code() == FieldTypeCode::String
            && origin.is_binary_string()
            && to.is_binary_string()
        {
            return true;
        }
    }
    (to.decimal() > 0 && to.decimal() < origin.decimal())
        || origin.is_unsigned() != to.is_unsigned()
}

/// Go `checkModifyTypes`' charset/collation step: `checkModifyCharsetAndCollation`
/// with the column's index membership, and the indexed-collation refusal
/// reworded for the column. A type change that reorganizes anyway absorbs
/// either 8200 refusal (Go's `ErrUnsupportedModifyCharset.Equal(err)`
/// compares the shared code, so it matches both) -- except to or from GBK.
fn check_modify_types_charset_and_collation(
    origin: &FieldType,
    to: &FieldType,
    can_reorganize: bool,
    column_name: &str,
    indexed: bool,
) -> Result<(), MysqlError> {
    let Err(refusal) = check_modify_charset_and_collation(
        to.charset_name(),
        to.collation_name(),
        origin.charset_name(),
        origin.collation_name(),
        indexed,
    ) else {
        return Ok(());
    };
    let gbk = origin.charset_name() == "gbk" || to.charset_name() == "gbk";
    if !gbk && can_reorganize && !matches!(refusal, CharsetChangeRefusal::UnknownCharset(_)) {
        return Ok(());
    }
    Err(refusal.into_error(|| {
        if gbk {
            format!(
                "Unsupported modifying collation from {} to {}",
                origin.collation_name(),
                to.collation_name()
            )
        } else {
            format!(
                "Unsupported modifying collation of column '{}' from '{}' to '{}' when index is defined on it.",
                column_name.go_to_lower(),
                origin.collation_name(),
                to.collation_name()
            )
        }
    }))
}

/// Go `checkTypeChangeSupported` (`pkg/types/field_type.go:1569-1603`):
/// five ORIGIN/TARGET type-pair refusals that are unconditional -- each is a
/// TiDB `// TODO: ... not support yet, should fix here after supported`, not
/// a rule about the DATA in any particular row. Reached only when the two
/// types differ (Go's caller, `CheckModifyTypeCompatible`, only calls this in
/// its "different type" branch; the same-type precision/elems checks live
/// beside the caller, not here).
///
/// The five arms, transcribed in Go's own order:
/// 1. `{date/datetime/timestamp, TIME, YEAR, any string type, JSON} -> BIT`.
/// 2. `{date/datetime/timestamp, TIME, YEAR, DECIMAL, FLOAT, DOUBLE, JSON,
///    BIT} -> {ENUM, SET}`. Note the asymmetry with rule 3: TIME/YEAR are
///    origins here but not there, because rule 3's TIME-as-target case is
///    covered by rule 5 instead for DURATION specifically.
/// 3. `{ENUM, SET, BIT, DECIMAL, FLOAT, DOUBLE} -> date/datetime/timestamp`.
///    DURATION and YEAR are deliberately NOT origins of this rule -- rule 5
///    is what refuses DURATION as a target, and YEAR -> date/datetime/
///    timestamp is accepted by Go.
/// 4. TiDB `VECTOR` (`TypeTiDBVectorFloat32`) as EITHER side.
/// 5. `{ENUM, SET, BIT} -> TIME (DURATION)`.
///
/// Everything else this function is asked about is accepted HERE -- the
/// per-row conversion of the column reorganization
/// (`KvTableBackfill::update_column_batch`) still gets the last word for
/// any row that will not fit the new type.
fn check_type_change_supported(origin: &FieldType, to: &FieldType) -> Result<(), MysqlError> {
    let (from_code, to_code) = (origin.code(), to.code());
    if from_code == to_code {
        // Go only reaches `checkTypeChangeSupported` from the "different
        // type" branch of `CheckModifyTypeCompatible`; a same-type MODIFY
        // (e.g. widening a `decimal`'s precision) is judged by other rules
        // this tier applies elsewhere, not by this table.
        return Ok(());
    }

    let is_time_like_origin = |code: FieldTypeCode| {
        code.is_type_time()
            || code == FieldTypeCode::Duration
            || code == FieldTypeCode::Year
            || code.is_string()
            || code == FieldTypeCode::Json
    };
    let refused =
        // Rule 1.
        (is_time_like_origin(from_code) && to_code == FieldTypeCode::Bit)
        // Rule 2.
        || ((from_code.is_type_time()
            || from_code == FieldTypeCode::Duration
            || from_code == FieldTypeCode::Year
            || matches!(
                from_code,
                FieldTypeCode::NewDecimal
                    | FieldTypeCode::Float
                    | FieldTypeCode::Double
                    | FieldTypeCode::Json
                    | FieldTypeCode::Bit
            ))
            && matches!(to_code, FieldTypeCode::Enum | FieldTypeCode::Set))
        // Rule 3.
        || (matches!(
            from_code,
            FieldTypeCode::Enum
                | FieldTypeCode::Set
                | FieldTypeCode::Bit
                | FieldTypeCode::NewDecimal
                | FieldTypeCode::Float
                | FieldTypeCode::Double
        ) && to_code.is_type_time())
        // Rule 4.
        || from_code == FieldTypeCode::VectorFloat32
        || to_code == FieldTypeCode::VectorFloat32
        // Rule 5.
        || (matches!(
            from_code,
            FieldTypeCode::Enum | FieldTypeCode::Set | FieldTypeCode::Bit
        ) && to_code == FieldTypeCode::Duration);

    if refused {
        return Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_util::dbterror::ERR_UNSUPPORTED_MODIFY_COLUMN.generate_with_stack_by_args(&[
                format!(
                    "change from original type {} to {} is currently unsupported yet",
                    (origin.compact_str(false)),
                    (to.compact_str(false))
                )
                .into(),
            ]),
        ));
    }
    Ok(())
}

/// Go `checkModifyCharsetAndCollation`'s refusals.
pub enum CharsetChangeRefusal {
    /// `ErrUnknownCharacterSet`: the target pair is not a valid one.
    UnknownCharset(String),
    /// `ErrUnsupportedModifyCollation`: an indexed column would need its
    /// entries rewritten for an incompatible collation. Every Go caller
    /// rewords it, so the caller supplies the message.
    Collation,
    /// `ErrUnsupportedModifyCharset`, with Go's `modify %s` argument.
    Charset(String),
}

impl CharsetChangeRefusal {
    /// The error, with the indexed-collation message worded by the caller.
    pub fn into_error(self, collation_message: impl FnOnce() -> String) -> MysqlError {
        match self {
            Self::UnknownCharset(message) => MysqlError::from_terror(
                &tidb_util::dbterror::ERR_UNKNOWN_CHARACTER_SET.generate_with_stack(message),
            ),
            Self::Collation => MysqlError::from_terror(
                &tidb_util::dbterror::ERR_UNSUPPORTED_MODIFY_COLLATION
                    .generate_with_stack(collation_message()),
            ),
            Self::Charset(reason) => MysqlError::from_terror(
                &tidb_util::dbterror::ERR_UNSUPPORTED_MODIFY_CHARSET
                    .generate_with_stack_by_args(&[reason.into()]),
            ),
        }
    }
}

/// Go `checkModifyCharsetAndCollation`: a valid target pair; an indexed
/// column may not move to an incompatible collation under the new collation
/// framework; and only utf8/latin1 to utf8mb4, or a collation change within
/// utf8/utf8mb4, is metadata-only.
pub fn check_modify_charset_and_collation(
    to_charset: &str,
    to_collation: &str,
    from_charset: &str,
    from_collation: &str,
    need_rewrite_collation_data: bool,
) -> Result<(), CharsetChangeRefusal> {
    if !tidb_datatype::valid_charset_and_collation(to_charset, to_collation) {
        return Err(CharsetChangeRefusal::UnknownCharset(format!(
            "Unknown character set: '{to_charset}', collation: '{to_collation}'"
        )));
    }
    if need_rewrite_collation_data
        && tidb_datatype::new_collation_enabled()
        && !tidb_datatype::compatible_collate(from_collation, to_collation)
    {
        return Err(CharsetChangeRefusal::Collation);
    }
    if matches!(
        (from_charset, to_charset),
        ("utf8", "utf8mb4") | ("utf8", "utf8") | ("utf8mb4", "utf8mb4") | ("latin1", "utf8mb4")
    ) {
        return Ok(());
    }
    if to_charset != from_charset {
        return Err(CharsetChangeRefusal::Charset(format!(
            "charset from {from_charset} to {to_charset}"
        )));
    }
    if to_collation != from_collation {
        return Err(CharsetChangeRefusal::Charset(format!(
            "change collate from {from_collation} to {to_collation}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod type_change_gate_tests {
    use super::check_type_change_supported;
    use tidb_datatype::{FieldType, FieldTypeCode};

    /// Rule 4 (`field_type.go:1591-1594`, `TypeTiDBVectorFloat32` on either
    /// side): VECTOR cannot be changed to or from another type family.
    #[test]
    fn vector_type_is_refused_on_either_side() {
        let vector = FieldType::new(FieldTypeCode::VectorFloat32);
        let int = FieldType::new(FieldTypeCode::Long);

        assert_eq!(
            check_type_change_supported(&vector, &int),
            Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_UNSUPPORTED_MODIFY_COLUMN.generate_with_stack_by_args(&[
                    format!(
                        "change from original type {} to {} is currently unsupported yet",
                        (vector.compact_str(false)),
                        (int.compact_str(false))
                    )
                    .into()
                ])
            ))
        );
        assert_eq!(
            check_type_change_supported(&int, &vector),
            Err(tidb_error::mysql::MysqlError::from_terror(
                &tidb_util::dbterror::ERR_UNSUPPORTED_MODIFY_COLUMN.generate_with_stack_by_args(&[
                    format!(
                        "change from original type {} to {} is currently unsupported yet",
                        (int.compact_str(false)),
                        (vector.compact_str(false))
                    )
                    .into()
                ])
            ))
        );
    }

    /// MUTATION PROBE for all five rules: with the gate neutered (always
    /// `Ok`), every refusal would incorrectly succeed. This test documents the
    /// probe; see the SQL pins in `tidb-session`'s `tests_alter_column.rs`
    /// for the five empty-table cases this backs.
    #[test]
    fn control_conversion_is_not_refused() {
        let from = FieldType::new(FieldTypeCode::Long);
        let to = FieldType::new(FieldTypeCode::LongLong);
        assert!(check_type_change_supported(&from, &to).is_ok());
    }
}
