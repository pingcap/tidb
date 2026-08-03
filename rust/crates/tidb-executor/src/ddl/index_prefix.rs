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

//! The declared length of an index key part: Go `pkg/ddl/index.go`'s
//! `checkIndexColumn` and the normalization at the tail of
//! `buildIndexColumns`.
//!
//! # Why this is shared, when the index lowering is not
//!
//! There are two `CREATE TABLE` metadata builders in this workspace, and they
//! lower an index onto genuinely different shapes -- `KvIndex` column OFFSETS
//! in [`crate::ddl`], `IndexInfo`/`IndexColumn` in
//! `tidb_exec::table_info_build` -- so the lowering itself is deliberately
//! written twice.
//!
//! The length RULES are not part of that difference. Whether `a(10)` is legal,
//! and what length TiDB then stores, is a pure function of the column's
//! `FieldType` and the declared number; neither answer mentions an offset or
//! an `IndexColumn`. Writing it twice is what produced the divergence this
//! module was extracted to close: the `TableInfo` builder validated NOTHING
//! and stored whatever length was written, so
//!
//! ```sql
//! create table t (a int, key idx(a(3)))
//! ```
//!
//! built an `IndexInfo` carrying a 3-byte prefix on an INTEGER -- metadata
//! real TiDB refuses outright with 1089 -- while the executor tier refused the
//! same statement. That is the same accept-then-store-something-else shape as
//! the five declared-type rules unified into
//! [`super::column_field_type`], and it is fixed the same way.
//!
//! # What this module does NOT decide
//!
//! It answers only what happens at CREATE time. A prefix length that is
//! ACCEPTED here still has to be honoured by the index encoding
//! ([`crate::index_prefix_cut`]) and by every read-path decision that would
//! otherwise assume an index entry holds the whole column value
//! ([`crate::kv_table::KvIndex::covers`],
//! [`crate::kv_table::KvIndex::ordered_column_offsets`],
//! [`crate::kv_table::KvIndex::has_prefix`], and the ranger's endpoint
//! cutting). Those exist now for SECONDARY indexes; a prefix on a clustered
//! PRIMARY KEY is a different problem and stays refused, see
//! [`clustered_prefix_unsupported`].

use tidb_datatype::FieldType;

/// Go `types.UnspecifiedLength`: the key part covers the whole column.
pub const UNSPECIFIED_LENGTH: i64 = -1;

/// Go `config.MaxIndexLength`'s default, in bytes.
///
/// TiDB makes this configurable; this tier has no such setting, so the
/// default is the value. A table built under a raised limit would be
/// admitted here and rejected there, which is the safe direction.
pub const MAX_INDEX_LENGTH: i64 = 3072;

/// Why a declared key-part length is not legal. Each variant is one of Go's
/// own errors, kept apart because they have different codes and different
/// message arguments.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrefixError {
    /// Go `dbterror.ErrIncorrectPrefixKey` (1089): a length was written on a
    /// type that cannot carry one, or it is longer than the column itself.
    /// Go names neither the column nor the length in this message.
    IncorrectPrefixKey,
    /// Go `dbterror.ErrBlobKeyWithoutLength` (1170): a BLOB/TEXT key part
    /// must say how much of the column it indexes.
    BlobKeyWithoutLength(String),
    /// Go `dbterror.ErrKeyPart0` (1391): the length was written as zero.
    KeyPart0(String),
    /// Go `dbterror.ErrWrongKeyColumn` (1167): the column cannot carry an
    /// index at all.
    WrongKeyColumn(String),
    /// Go `dbterror.ErrTooLongKey` (1071). Both numbers are BYTES, and the
    /// first has already been multiplied by the charset's maximum bytes per
    /// character, which is what Go reports.
    TooLongKey {
        /// The key part's length in bytes.
        length: i64,
        /// The maximum a key part may reach.
        max: i64,
    },
}

/// Go `checkIndexColumn`'s `ErrJSONUsedAsKey` (3152) arm is NOT here: both
/// tiers already raise it from their own index lowering, where the column
/// name is in hand, and it does not depend on the declared length.
///
/// Validates the declared length of one key part and returns the length TiDB
/// STORES for it, which is not always the one that was written.
///
/// `strict` is Go's `suppressTooLongKeyErr` inverted: outside strict mode Go
/// downgrades only the too-long check to a warning and truncates.
///
/// # Errors
///
/// One of [`PrefixError`], in the order Go checks them -- the order is
/// observable, because a `blob` key part with a zero length is 1391 and not
/// 1170.
pub fn stored_index_length(
    field_type: &FieldType,
    column: &str,
    declared: Option<i64>,
    strict: bool,
) -> Result<i64, PrefixError> {
    let code = field_type.code();
    let declared = declared.unwrap_or(UNSPECIFIED_LENGTH);

    // Go `checkIndexColumn`'s FIRST arm: a NULL-typed column, or a
    // `char(0)`/`varchar(0)`/`binary(0)` whose declared width leaves no bytes
    // to key on, cannot be indexed at all. Captured:
    // `create table t (b char(0), index(b))` is
    // "[ddl:1167]The used storage engine can't index column 'b'".
    if code == tidb_datatype::FieldTypeCode::Null
        || (field_type.flen() == 0
            && (code.is_type_char() || code == tidb_datatype::FieldTypeCode::Varchar))
    {
        return Err(PrefixError::WrongKeyColumn(column.to_owned()));
    }

    // A BLOB/TEXT key part must carry a length, and it may not be zero.
    if code.is_type_blob() {
        if declared == UNSPECIFIED_LENGTH {
            return Err(PrefixError::BlobKeyWithoutLength(column.to_owned()));
        }
        if declared == 0 {
            return Err(PrefixError::KeyPart0(column.to_owned()));
        }
    }

    // A length can only be written on a type that has a prefix at all.
    if declared != UNSPECIFIED_LENGTH && !code.is_type_prefixable() {
        return Err(PrefixError::IncorrectPrefixKey);
    }

    // A CHAR/VARCHAR key part may not claim more than the column holds. Go
    // reports the over-long case and the zero case differently, and checks
    // them in this order.
    if declared != UNSPECIFIED_LENGTH && code.is_type_char() {
        if field_type.flen() < declared {
            return Err(PrefixError::IncorrectPrefixKey);
        }
        if declared == 0 {
            return Err(PrefixError::KeyPart0(column.to_owned()));
        }
    }

    // The limit is on BYTES, so a multi-byte charset reaches it sooner. Go
    // multiplies before comparing and reports the multiplied number.
    let mut length_in_bytes = declared;
    if code.is_string() {
        length_in_bytes *= charset_max_bytes(field_type);
    }
    if length_in_bytes > MAX_INDEX_LENGTH && strict {
        return Err(PrefixError::TooLongKey {
            length: length_in_bytes,
            max: MAX_INDEX_LENGTH,
        });
    }

    // Go `buildIndexColumns`: a prefix covering the WHOLE column is stored as
    // no prefix at all, so `varchar(10) key (a(10))` is an ordinary index and
    // prints back without the `(10)`.
    if declared != UNSPECIFIED_LENGTH && code.is_type_char() && declared == field_type.flen() {
        return Ok(UNSPECIFIED_LENGTH);
    }
    Ok(declared)
}

/// Go `getIndexColumnLength` (`pkg/ddl/index.go:307`): the BYTES one key part
/// occupies in the index.
///
/// This is not the declared length. A key part with no declared length takes
/// the COLUMN's own `flen` -- which is why `KEY(a, b)` over two
/// `varchar(600)` utf8mb4 columns is 4800 bytes even though neither part
/// carries a prefix -- and a fixed-width type takes its storage size from
/// `mysql.DefaultLengthOfMysqlTypes` regardless of its display width.
#[must_use]
pub fn index_column_bytes(field_type: &FieldType, declared: i64) -> i64 {
    use tidb_datatype::FieldTypeCode as Code;

    let length = if declared != UNSPECIFIED_LENGTH {
        declared
    } else {
        field_type.flen()
    };
    match field_type.code() {
        // A BIT column is packed eight values to the byte.
        Code::Bit => (length + 7) >> 3,
        Code::Varchar
        | Code::String
        | Code::VarString
        | Code::TinyBlob
        | Code::MediumBlob
        | Code::Blob
        | Code::LongBlob => charset_max_bytes(field_type) * length,
        Code::Tiny => 1,
        Code::Short => 2,
        Code::Int24 => 3,
        Code::Long => 4,
        Code::LongLong | Code::Double => 8,
        // `mysql.MaxFloatPrecisionLength` is 24: above it a FLOAT is stored
        // with double precision, so it costs a DOUBLE's eight bytes.
        Code::Float => {
            if length <= 24 {
                4
            } else {
                8
            }
        }
        Code::NewDecimal => (length / 9 * 4) + ((length % 9) + 1) / 2,
        Code::Year => 1,
        Code::Date | Code::Duration => 3,
        Code::Datetime => 8,
        Code::Timestamp => 4,
        // Go's `default` arm, which ENUM and SET reach too -- they are absent
        // from its switch even though `DefaultLengthOfMysqlTypes` names them.
        _ => length,
    }
}

/// Go `buildIndexColumns`' running-sum check: the key parts of ONE index
/// together may not exceed `config.MaxIndexLength`.
///
/// This is a separate rule from the per-key-part limit in
/// [`stored_index_length`], and it is the one that refuses
/// `PRIMARY KEY (c01,c02,c03,c04)` over four `varchar(255)` utf8mb4 columns:
/// each part is 1020 bytes and legal on its own, but the fourth takes the sum
/// to 4080. Go reports the running sum at the part that crossed the line, not
/// the total, so the parts are checked in declaration order.
///
/// `strict` is the SQL mode. Go downgrades this to a warning-and-truncate
/// only when the mode is non-strict AND the index is a single non-unique key
/// part; a multi-part or unique index is an error either way.
///
/// # Errors
///
/// [`PrefixError::TooLongKey`] carrying the running sum that crossed the max.
pub fn check_index_key_length<'a>(
    parts: impl IntoIterator<Item = (&'a FieldType, i64)>,
    part_count: usize,
    unique: bool,
    strict: bool,
) -> Result<(), PrefixError> {
    let mut sum = 0;
    for (field_type, declared) in parts {
        sum += index_column_bytes(field_type, declared);
        if sum > MAX_INDEX_LENGTH && (strict || unique || part_count > 1) {
            return Err(PrefixError::TooLongKey {
                length: sum,
                max: MAX_INDEX_LENGTH,
            });
        }
    }
    Ok(())
}

/// Go `charset.GetCharsetInfo(col.GetCharset()).Maxlen`, for the charsets a
/// column may declare here.
fn charset_max_bytes(field_type: &FieldType) -> i64 {
    match field_type.charset_name() {
        "utf8mb4" | "gb18030" => 4,
        "utf8" | "gbk" => 3,
        "ucs2" => 2,
        // `binary`, `latin1` and `ascii` are all one byte per character.
        _ => 1,
    }
}

/// Validates one key part the way the runnable path reports it, and hands
/// back the length TiDB STORES for it.
///
/// This is [`stored_index_length`] with Go's errors mapped onto the driver's,
/// so an illegal length reaches the client as TiDB's own code rather than as
/// a refusal of this tier's own.
///
/// # Errors
///
/// The Go error for an illegal length.
pub fn key_part_length(
    field_type: &FieldType,
    column: &str,
    declared: Option<i64>,
) -> Result<i64, crate::DriverError> {
    // Strict mode is not threaded to this tier's DDL yet, and the strict
    // reading is the one that refuses rather than silently truncating a key,
    // so it is the safe default until it is.
    stored_index_length(field_type, column, declared, true).map_err(driver_error)
}

/// Go's index-length errors as the driver reports them, so an illegal key
/// reaches the client as TiDB's own code rather than as a refusal of this
/// tier's own.
#[must_use]
pub fn driver_error(error: PrefixError) -> crate::DriverError {
    match error {
        PrefixError::IncorrectPrefixKey => crate::DriverError::IncorrectPrefixKey,
        PrefixError::BlobKeyWithoutLength(column) => {
            crate::DriverError::BlobKeyWithoutLength(column)
        }
        PrefixError::KeyPart0(column) => crate::DriverError::KeyPart0(column),
        PrefixError::WrongKeyColumn(column) => crate::DriverError::WrongKeyColumn(column),
        PrefixError::TooLongKey { length, max } => crate::DriverError::TooLongKey { length, max },
    }
}

/// The one prefix form still refused, and why it is a DIFFERENT problem from
/// the secondary-index prefix this module now admits.
///
/// A prefix on a SECONDARY index cuts the entry, and every read that needs
/// the whole value goes back to the row for it -- which is what
/// [`crate::index_prefix_cut`], [`crate::kv_table::KvIndex::covers`] and
/// [`crate::kv_table::KvIndex::ordered_column_offsets`] between them arrange.
///
/// A prefix on a CLUSTERED PRIMARY KEY cuts the ROW IDENTIFIER: captured from
/// real TiDB, `create table p (a varchar(20), primary key (a(3)))` prints
/// `/*T![clustered_index] CLUSTERED */` and then rejects `'abcxyz'` after
/// `'abcdef'` -- two distinct rows are two distinct handles only if the cut
/// values differ. There is no row to go back to when the handle itself is
/// lossy, so this is not the same fix, and admitting it on the strength of
/// the secondary-index work would be the silent-wrong-answer shape all over
/// again.
#[must_use]
pub fn clustered_prefix_unsupported() -> &'static str {
    "a prefix-length primary key is not supported yet"
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::FieldTypeCode;

    fn char_type(code: FieldTypeCode, flen: i64, charset: &str) -> FieldType {
        let mut field_type = FieldType::new(code);
        field_type.set_flen(flen);
        field_type.set_charset_name(charset);
        field_type
    }

    /// Captured from real TiDB: `create table e1 (a int, key idx(a(3)))` is
    /// refused. The `TableInfo` builder used to accept it and store a 3-byte
    /// prefix on an integer.
    #[test]
    fn a_length_on_a_non_prefixable_type_is_1089() {
        let int_type = FieldType::new(FieldTypeCode::Long);
        assert_eq!(
            stored_index_length(&int_type, "a", Some(3), true),
            Err(PrefixError::IncorrectPrefixKey)
        );
        // Without a length the same column is an ordinary key part.
        assert_eq!(
            stored_index_length(&int_type, "a", None, true),
            Ok(UNSPECIFIED_LENGTH)
        );
    }

    /// Captured: `create table e2 (a blob, key idx(a))` is refused, and
    /// `create table e5 (a text, key idx(a(5)))` is accepted.
    #[test]
    fn a_blob_key_part_must_carry_a_length() {
        let blob = FieldType::new(FieldTypeCode::Blob);
        assert_eq!(
            stored_index_length(&blob, "a", None, true),
            Err(PrefixError::BlobKeyWithoutLength("a".to_owned()))
        );
        assert_eq!(stored_index_length(&blob, "a", Some(5), true), Ok(5));
        // Zero is a different error from absent, and is checked first.
        assert_eq!(
            stored_index_length(&blob, "a", Some(0), true),
            Err(PrefixError::KeyPart0("a".to_owned()))
        );
    }

    /// Captured: `create table e3 (a varchar(5), key idx(a(10)))` is refused,
    /// and `create table e6 (a varchar(20), key idx(a(0)))` is refused with
    /// the other error.
    #[test]
    fn a_char_key_part_may_not_outrun_its_column() {
        let short = char_type(FieldTypeCode::Varchar, 5, "utf8mb4");
        assert_eq!(
            stored_index_length(&short, "a", Some(10), true),
            Err(PrefixError::IncorrectPrefixKey)
        );
        assert_eq!(
            stored_index_length(&short, "a", Some(0), true),
            Err(PrefixError::KeyPart0("a".to_owned()))
        );
        assert_eq!(stored_index_length(&short, "a", Some(3), true), Ok(3));
    }

    /// Go `buildIndexColumns` normalizes a full-length CHAR prefix away, so
    /// the index is an ordinary one and `SHOW CREATE TABLE` prints no `(n)`.
    /// A BLOB is NOT normalized: its declared length is kept as written.
    #[test]
    fn a_full_length_char_prefix_is_stored_as_no_prefix() {
        let exact = char_type(FieldTypeCode::Varchar, 10, "utf8mb4");
        assert_eq!(
            stored_index_length(&exact, "a", Some(10), true),
            Ok(UNSPECIFIED_LENGTH)
        );
        // One short of the column is a real prefix.
        assert_eq!(stored_index_length(&exact, "a", Some(9), true), Ok(9));

        let mut text = FieldType::new(FieldTypeCode::Blob);
        text.set_flen(10);
        assert_eq!(stored_index_length(&text, "a", Some(10), true), Ok(10));
    }

    /// Go multiplies the length by the charset's bytes per character before
    /// comparing against `MaxIndexLength`, and reports the MULTIPLIED number.
    /// Outside strict mode the check is a warning rather than an error.
    #[test]
    fn the_length_limit_counts_bytes_not_characters() {
        let wide = char_type(FieldTypeCode::Varchar, 2000, "utf8mb4");
        assert_eq!(
            stored_index_length(&wide, "a", Some(1000), true),
            Err(PrefixError::TooLongKey {
                length: 4000,
                max: MAX_INDEX_LENGTH,
            })
        );
        // The same 1000 characters fit in a single-byte charset.
        let narrow = char_type(FieldTypeCode::Varchar, 2000, "latin1");
        assert_eq!(
            stored_index_length(&narrow, "a", Some(1000), true),
            Ok(1000)
        );
        // Non-strict downgrades the check, as Go's suppressTooLongKeyErr does.
        assert_eq!(stored_index_length(&wide, "a", Some(1000), false), Ok(1000));
    }
}
