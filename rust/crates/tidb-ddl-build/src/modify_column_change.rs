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

//! Go `pkg/ddl/modify_column_change.go` (307fb3d6ead): how a MODIFY /
//! CHANGE COLUMN conversion is classified and validated, for submission and
//! execution alike. The result is Go's `model.ModifyColumnType` byte
//! ([`tidb_model::job_enums::modify_type`]).

use tidb_datatype::{FieldTypeCode, FieldTypeFlags};
use tidb_model::job_enums::modify_type;
use tidb_model::{ColumnInfo, TableInfo};
use tidb_mysql::consts::SqlMode;
use tidb_terror::MysqlError;

/// Go `mysql.TypeNull`: older TiDB versions stored it to mark jobs that
/// could need reorg.
const MYSQL_TYPE_NULL: u8 = 6;

/// Go `utf8.UTFMax`.
const UTF_MAX: i64 = 4;

// =============================================================================
// 1. Entry points
// =============================================================================

/// Go `analyzeModifyColumn`: classifies the conversion and validates that it
/// is supported. Returns the base kind (metadata-only, validation-only, index
/// rewrite, or row rewrite) without execution-time restrictions.
///
/// Shared by submission and execution. Execution must evaluate against
/// current metadata, since another DDL may have changed the column since
/// submission.
///
/// # Errors
///
/// Go's `validateModifyColumn` refusals.
pub fn analyze_modify_column(tbl: &TableInfo, old_col: &ColumnInfo, new_col: &ColumnInfo) -> Result<u8, MysqlError> {
    let change = classify_column_change(tbl, old_col, new_col);
    validate_modify_column(old_col, new_col, change)?;
    Ok(change)
}

/// Go `resolveModifyColumnType`: validates and determines the initial
/// execution kind of a MODIFY/CHANGE COLUMN job against current metadata.
///
/// # Errors
///
/// Go's `validateModifyColumn` refusals.
pub fn resolve_modify_column_type(
    tbl: &TableInfo,
    old_col: &ColumnInfo,
    new_col: &ColumnInfo,
    stored_type: u8,
    sql_mode: SqlMode,
) -> Result<u8, MysqlError> {
    let base_type = analyze_modify_column(tbl, old_col, new_col)?;
    let modify_type = apply_execution_restrictions(tbl, old_col, new_col, stored_type, sql_mode, base_type);
    if needs_varchar_to_char_precheck(old_col, new_col, modify_type) {
        return Ok(modify_type::PRECHECK);
    }
    Ok(modify_type)
}

/// Go `resolveModifyColumnTypeAfterPrecheck`: the execution kind after the
/// VARCHAR -> CHAR data precheck succeeds.
#[must_use]
pub fn resolve_modify_column_type_after_precheck(
    tbl: &TableInfo,
    old_col: &ColumnInfo,
    new_col: &ColumnInfo,
    stored_type: u8,
    sql_mode: SqlMode,
) -> u8 {
    let base_type = classify_column_change(tbl, old_col, new_col);
    apply_execution_restrictions(tbl, old_col, new_col, stored_type, sql_mode, base_type)
}

/// Go `isMetaOnlyChange`: whether the column conversion is strictly
/// metadata-only regardless of existing row data. Nullability is checked
/// separately by callers.
#[must_use]
pub fn is_meta_only_change(tbl: &TableInfo, old_col: &ColumnInfo, new_col: &ColumnInfo) -> bool {
    classify_column_change(tbl, old_col, new_col) == modify_type::NO_REORG
}

/// Go `isNullToNotNullChange`.
#[must_use]
pub fn is_null_to_not_null_change(old_col: &ColumnInfo, new_col: &ColumnInfo) -> bool {
    !old_col.field_type.has_flag(FieldTypeFlags::NOT_NULL) && new_col.field_type.has_flag(FieldTypeFlags::NOT_NULL)
}

// =============================================================================
// 2. Conversion support validation
// =============================================================================

/// Go `validateModifyColumn`: whether modifying `from` -> `to` is supported
/// at all (either metadata-only or via reorganization). Non-GBK charset and
/// collation changes are allowed and routed to the appropriate execution
/// path by [`classify_column_change`].
fn validate_modify_column(from: &ColumnInfo, to: &ColumnInfo, change: u8) -> Result<(), MysqlError> {
    let (from_ft, to_ft) = (&from.field_type, &to.field_type);
    if !tidb_charset::valid_charset_and_collation(to_ft.charset_name(), to_ft.collation_name()) {
        return Err(MysqlError::from_terror(&tidb_dbterror::ERR_UNKNOWN_CHARACTER_SET.gen_with_stack(format!(
            "Unknown character set: '{}', collation: '{}'",
            to_ft.charset_name(),
            to_ft.collation_name()
        ))));
    }

    if from_ft.code() != to_ft.code() && !is_cross_type_change_supported(from_ft.code(), to_ft.code()) {
        let msg = format!(
            "change from original type {} to {} is currently unsupported yet",
            from_ft.compact_str(false),
            to_ft.compact_str(false)
        );
        return Err(MysqlError::from_terror(
            &tidb_dbterror::ERR_UNSUPPORTED_MODIFY_COLUMN.gen_with_stack_by_args(&[msg.into()]),
        ));
    }

    // Modifying charset or collation to/from GBK is not supported yet.
    let (from_chs, to_chs) = (from_ft.charset_name(), to_ft.charset_name());
    if from_chs == "gbk" || to_chs == "gbk" {
        if from_chs != to_chs {
            return Err(MysqlError::from_terror(&tidb_dbterror::ERR_UNSUPPORTED_MODIFY_CHARSET.gen_with_stack_by_args(
                &[format!("charset from {from_chs} to {to_chs}").into()],
            )));
        }
        if from_ft.collation_name() != to_ft.collation_name() {
            return Err(MysqlError::from_terror(&tidb_dbterror::ERR_UNSUPPORTED_MODIFY_CHARSET.gen_with_stack_by_args(
                &[format!("change collate from {} to {}", from_ft.collation_name(), to_ft.collation_name()).into()],
            )));
        }
    }

    if change != modify_type::NO_REORG && from_ft.has_flag(FieldTypeFlags::PRI_KEY) {
        return Err(MysqlError::from_terror(
            &tidb_dbterror::ERR_UNSUPPORTED_MODIFY_COLUMN.gen_with_stack_by_args(&["this column has primary key flag".into()]),
        ));
    }
    Ok(())
}

/// Go `isCrossTypeChangeSupported`: whether cross-type reorganization from
/// `origin_tp` to `to_tp` is implemented. Callers handle `origin_tp == to_tp`
/// separately.
#[must_use]
pub fn is_cross_type_change_supported(origin_tp: FieldTypeCode, to_tp: FieldTypeCode) -> bool {
    use FieldTypeCode as T;
    if origin_tp == T::VectorFloat32 || to_tp == T::VectorFloat32 {
        // TODO: Cross-type conversion involving vector is not supported.
        return false;
    }

    match to_tp {
        // TODO: date/datetime/timestamp/time/year/string/json -> bit are not
        // compatible with MySQL yet.
        T::Bit => match origin_tp {
            T::Date | T::Datetime | T::Timestamp | T::Duration | T::Year | T::Json => false,
            _ => !origin_tp.is_string(),
        },
        // TODO: temporal/decimal/float/double/json/bit -> enum/set are not
        // supported yet.
        T::Enum | T::Set => !matches!(
            origin_tp,
            T::Date
                | T::Datetime
                | T::Timestamp
                | T::Duration
                | T::Year
                | T::NewDecimal
                | T::Float
                | T::Double
                | T::Json
                | T::Bit
        ),
        // TODO: enum/set/bit/decimal/float/double -> date/datetime/timestamp
        // are not supported yet.
        T::Date | T::Datetime | T::Timestamp => !matches!(
            origin_tp,
            T::Enum | T::Set | T::Bit | T::NewDecimal | T::Float | T::Double
        ),
        // TODO: enum/set/bit -> time are not supported yet.
        T::Duration => !matches!(origin_tp, T::Enum | T::Set | T::Bit),
        _ => true,
    }
}

// =============================================================================
// 3. Storage & index conversion classification
// =============================================================================

/// Go `classifyColumnChange`: the base kind from row storage and index
/// encoding requirements. Nullability and execution-time restrictions are
/// applied later in [`apply_execution_restrictions`].
#[must_use]
pub fn classify_column_change(tbl: &TableInfo, old_col: &ColumnInfo, new_col: &ColumnInfo) -> u8 {
    let change = classify_by_row_value(old_col, new_col);
    if change == modify_type::REORG {
        return change;
    }
    if !has_related_index(tbl, old_col.id) {
        return change;
    }
    if needs_index_reorg(old_col, new_col) {
        // Index-only reorg only runs validation SQL for integers and
        // non-binary CHAR/VARCHAR; other indexed string types (e.g. TEXT with
        // a prefix index) fall back to full row+index reorg.
        if is_integer_change(old_col, new_col) || is_non_binary_char_change(old_col, new_col) {
            return modify_type::INDEX_REORG;
        }
        return modify_type::REORG;
    }
    change
}

/// Go `classifyByRowValue`: whether existing row values can be reused
/// as-is, need validation, or must be rewritten.
fn classify_by_row_value(old_col: &ColumnInfo, new_col: &ColumnInfo) -> u8 {
    if !is_charset_metadata_compatible(old_col, new_col) {
        return modify_type::REORG;
    }
    let (old, new) = (&old_col.field_type, &new_col.field_type);
    let same_type = old.code() == new.code();
    let same_sign = old.is_unsigned() == new.is_unsigned();

    if is_integer_change(old_col, new_col) {
        // Display widths do not describe the stored integer range.
        let old_width = old.code().get_default_field_length_and_decimal().0;
        let new_width = new.code().get_default_field_length_and_decimal().0;
        if same_sign && new_width >= old_width {
            return modify_type::NO_REORG;
        }
        return modify_type::NO_REORG_WITH_CHECK;
    }
    if same_type && old.code() == FieldTypeCode::NewDecimal {
        // DECIMAL stores precision and scale alongside its value, even on
        // widening.
        if same_sign && old.flen() == new.flen() && old.decimal() == new.decimal() {
            return modify_type::NO_REORG;
        }
    } else if same_type && matches!(old.code(), FieldTypeCode::Enum | FieldTypeCode::Set) {
        if !is_elems_changed_to_modify_column(&old.elems_snapshot(), &new.elems_snapshot()) {
            return modify_type::NO_REORG;
        }
    } else if same_type && old.code() == FieldTypeCode::VectorFloat32 {
        if new.flen() == tidb_datatype::UNSPECIFIED_LENGTH || old.flen() == new.flen() {
            return modify_type::NO_REORG;
        }
    } else if is_string_storage_type(old.code()) && is_string_storage_type(new.code()) {
        return classify_by_string_row_value(old_col, new_col);
    } else if same_type && preserves_column_range(old_col, new_col) {
        return modify_type::NO_REORG;
    }
    // Cross-family conversions and unsupported validation paths rewrite rows.
    modify_type::REORG
}

/// Go `isCharsetMetadataCompatible`: whether stored values remain valid
/// without row conversion when the column charset changes. ENUM/SET store
/// ordinals rather than encoded strings; UTF8/Latin1 -> UTF8MB4 widening
/// also preserves existing byte representations.
fn is_charset_metadata_compatible(old_col: &ColumnInfo, new_col: &ColumnInfo) -> bool {
    let (old, new) = (&old_col.field_type, &new_col.field_type);
    if old.charset_name() == new.charset_name() {
        return true;
    }
    if matches!(old.code(), FieldTypeCode::Enum | FieldTypeCode::Set) {
        return true;
    }
    new.charset_name() == "utf8mb4" && (old.charset_name() == "utf8" || old.charset_name() == "latin1")
}

/// Go `classifyByStringRowValue`.
fn classify_by_string_row_value(old_col: &ColumnInfo, new_col: &ColumnInfo) -> u8 {
    let (old, new) = (&old_col.field_type, &new_col.field_type);
    if old.code() == FieldTypeCode::String
        && new.code() == FieldTypeCode::String
        && old.is_binary_string()
        && new.is_binary_string()
    {
        // BINARY pads with zero bytes; widening changes existing values too.
        if old.flen() == new.flen() {
            return modify_type::NO_REORG;
        }
        return modify_type::REORG;
    }
    if needs_string_byte_length_check(old_col, new_col) {
        if old.charset_name() == new.charset_name() {
            return modify_type::NO_REORG_WITH_CHECK;
        }
        return modify_type::REORG;
    }
    if !is_varchar_to_char(old_col, new_col) && preserves_column_range(old_col, new_col) {
        return modify_type::NO_REORG;
    }
    if is_non_binary_char_change(old_col, new_col) {
        return modify_type::NO_REORG_WITH_CHECK;
    }
    modify_type::REORG
}

/// Go `needsStringByteLengthCheck`: whether a character limit fails to
/// guarantee that every source value fits a target with a byte limit.
fn needs_string_byte_length_check(old_col: &ColumnInfo, new_col: &ColumnInfo) -> bool {
    if string_length_in_bytes(old_col) || !string_length_in_bytes(new_col) {
        return false;
    }
    let (source_limit, target_limit) = (old_col.field_type.flen(), new_col.field_type.flen());
    // Dividing converts the target byte limit to a guaranteed UTF-8
    // character capacity without overflowing the source limit.
    target_limit >= 0 && (source_limit < 0 || source_limit > target_limit / UTF_MAX)
}

/// Go `stringLengthInBytes`.
#[must_use]
pub fn string_length_in_bytes(col: &ColumnInfo) -> bool {
    col.field_type.charset_name() == "binary" || col.field_type.code().is_type_blob()
}

/// Go `preservesColumnRange`: the default rule for unchanged storage
/// formats -- do not narrow length/scale or change signedness. An
/// unspecified target length retains the historical unrestricted-length
/// behavior.
fn preserves_column_range(old_col: &ColumnInfo, new_col: &ColumnInfo) -> bool {
    let (old, new) = (&old_col.field_type, &new_col.field_type);
    old.is_unsigned() == new.is_unsigned()
        && (new.flen() <= 0 || (new.flen() >= old.flen() && new.decimal() >= old.decimal()))
}

/// Go `needsIndexReorg`.
fn needs_index_reorg(old_col: &ColumnInfo, new_col: &ColumnInfo) -> bool {
    let (old, new) = (&old_col.field_type, &new_col.field_type);
    if is_integer_change(old_col, new_col) {
        return old.is_unsigned() != new.is_unsigned();
    }
    if !is_string_storage_type(old.code()) || !is_string_storage_type(new.code()) {
        return false;
    }

    // Check index key part, ref tablecodec.GenIndexKey.
    if tidb_datatype::new_collation_enabled()
        && !tidb_datatype::compatible_collate(old.collation_name(), new.collation_name())
    {
        return true;
    }

    // Check index value part, ref tablecodec.GenIndexValuePortal.
    // TODO(joechenrh): It's better to check each index here, because not all
    // indexes need reorg even if the below condition is true.
    old.need_restored_data() != new.need_restored_data()
}

/// Go `hasRelatedIndex`.
fn has_related_index(tbl: &TableInfo, col_id: i64) -> bool {
    tbl.indices.iter_deref().any(|idx| idx.read().has_column_in_index_columns(tbl, col_id))
}

// =============================================================================
// 4. Execution-time restrictions & precheck
// =============================================================================

/// Go `applyExecutionRestrictions`: adjusts a base classification for
/// nullability checks, legacy job compatibility, and runtime restrictions
/// (partitions, TiFlash, stats encoding, and non-strict SQL mode).
/// Restrictions can only move a change to a more conservative path.
fn apply_execution_restrictions(
    tbl: &TableInfo,
    old_col: &ColumnInfo,
    new_col: &ColumnInfo,
    stored_type: u8,
    sql_mode: SqlMode,
    base_type: u8,
) -> u8 {
    if base_type == modify_type::NO_REORG {
        if is_null_to_not_null_change(old_col, new_col) {
            return modify_type::NO_REORG_WITH_CHECK;
        }
        return modify_type::NO_REORG;
    }

    // Backward compatibility: older TiDB versions stored mysql.TypeNull to
    // mark jobs that could need reorg; keep them on the conservative
    // full-reorg path.
    if stored_type == MYSQL_TYPE_NULL {
        return modify_type::REORG;
    }

    // FIXME(joechenrh): handle partition and TiFlash replica case.
    if tbl.partition.is_some() || tbl.tiflash_replica.as_ref().is_some_and(|replica| replica.read().count > 0) {
        return modify_type::REORG;
    }

    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/disableLossyDDLOptimization")
        .and_then(|value| value.as_bool())
        == Some(true)
    {
        return modify_type::REORG;
    }

    // FIXME(joechenrh): remove this when stats correctness is resolved.
    // Since stats may store bytes encoded by codec.EncodeKey, disable the
    // optimization if the same data produces different encoded bytes for the
    // old and new types.
    if has_incompatible_stats_encoding(old_col, new_col) {
        return modify_type::REORG;
    }

    if !sql_mode.has_strict_mode() {
        return modify_type::REORG;
    }

    base_type
}

/// Go `hasIncompatibleStatsEncoding`.
fn has_incompatible_stats_encoding(old_col: &ColumnInfo, new_col: &ColumnInfo) -> bool {
    let (old, new) = (&old_col.field_type, &new_col.field_type);
    if is_integer_change(old_col, new_col) {
        return old.is_unsigned() != new.is_unsigned();
    }
    if is_char_or_varchar_change(old_col, new_col) {
        return !tidb_datatype::compatible_collate(old.collation_name(), new.collation_name());
    }
    false
}

/// Go `needsVarcharToCharPrecheck`: whether a VARCHAR -> CHAR conversion
/// should start in the precheck kind to verify no existing values have
/// trailing spaces before deciding between a no-row-reorg path and full
/// reorg.
fn needs_varchar_to_char_precheck(old_col: &ColumnInfo, new_col: &ColumnInfo, modify_type: u8) -> bool {
    old_col.field_type.code() == FieldTypeCode::Varchar
        && new_col.field_type.code() == FieldTypeCode::String
        && (modify_type == modify_type::NO_REORG_WITH_CHECK || modify_type == modify_type::INDEX_REORG)
}

// =============================================================================
// 5. Type predicates
// =============================================================================

/// Go `isIntegerChange`.
#[must_use]
pub fn is_integer_change(from: &ColumnInfo, to: &ColumnInfo) -> bool {
    from.field_type.code().is_integer_type() && to.field_type.code().is_integer_type()
}

/// Go `isCharOrVarcharChange`.
fn is_char_or_varchar_change(from: &ColumnInfo, to: &ColumnInfo) -> bool {
    from.field_type.code().is_type_char() && to.field_type.code().is_type_char()
}

/// Go `isVarcharToChar`.
fn is_varchar_to_char(old_col: &ColumnInfo, new_col: &ColumnInfo) -> bool {
    old_col.field_type.code().is_type_varchar() && new_col.field_type.code() == FieldTypeCode::String
}

/// Go `isNonBinaryCharChange`.
fn is_non_binary_char_change(old_col: &ColumnInfo, new_col: &ColumnInfo) -> bool {
    is_char_or_varchar_change(old_col, new_col)
        && !old_col.field_type.is_binary_string()
        && !new_col.field_type.is_binary_string()
}

/// Go `isStringStorageType`.
const fn is_string_storage_type(tp: FieldTypeCode) -> bool {
    matches!(
        tp,
        FieldTypeCode::String
            | FieldTypeCode::Varchar
            | FieldTypeCode::VarString
            | FieldTypeCode::TinyBlob
            | FieldTypeCode::Blob
            | FieldTypeCode::MediumBlob
            | FieldTypeCode::LongBlob
    )
}

/// Go `ConvertBetweenCharAndVarchar`: whether a CHAR/VARCHAR type change
/// requires reorganization under the legacy rules. The TiFlow DM syncer uses
/// this predicate to classify DDL events.
#[must_use]
pub fn convert_between_char_and_varchar(old_type: FieldTypeCode, new_type: FieldTypeCode) -> bool {
    (old_type.is_type_varchar() && new_type == FieldTypeCode::String)
        || (old_type == FieldTypeCode::String
            && new_type.is_type_varchar()
            && tidb_datatype::new_collation_enabled())
}

/// Go `IsElemsChangedToModifyColumn`: whether modifying ENUM/SET elements
/// removes or changes any existing element ordinal.
#[must_use]
pub fn is_elems_changed_to_modify_column<T: PartialEq>(old_elems: &[T], new_elems: &[T]) -> bool {
    new_elems.len() < old_elems.len() || old_elems.iter().zip(new_elems).any(|(old, new)| old != new)
}
