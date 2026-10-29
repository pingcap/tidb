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

//! Go `pkg/ddl`'s column DEFAULT checks and the stored form a DEFAULT
//! settles into (`checkColumnDefaultValue`, `setDefaultValue`).

use tidb_datatype::RuntimeCollator;
use tidb_datatype::{Datum, FieldType};
use tidb_terror::MysqlError;


/// Go `checkColumnDefaultValue` (`pkg/ddl/add_column.go:1212`), the BLOB /
/// TEXT / JSON arm, shared by every entry point that Go's `SetDefaultValue`
/// serves: CREATE TABLE, ADD COLUMN, MODIFY/CHANGE COLUMN and
/// ALTER COLUMN ... SET DEFAULT.
///
/// It answers Go's `(hasDefaultValue, value)` pair, and is a SEPARATE step
/// from [`normalize_column_default`] (Go's `getDefaultValue` +
/// `checkDefaultValue`), which runs after it over the value returned here.
///
/// Go, verbatim in shape:
///
/// - non-strict `sql_mode` AND an EMPTY-STRING default: warn 1101 and accept.
///   `BLOB`/`LONGBLOB` (which is where `TEXT`/`LONGTEXT` land) additionally
///   report `hasDefaultValue = false`; `JSON`'s default is rewritten to the
///   text `null`. `TINYBLOB`/`MEDIUMBLOB` and their TEXT spellings keep both
///   the default AND `hasDefaultValue`, which is not an oversight here --
///   Go's `if col.GetType() == mysql.TypeBlob || col.GetType() ==
///   mysql.TypeLongBlob` names only those two.
/// - anything else non-NULL on those types: 1101, in every mode.
///
/// Measured against TiDB (`sql_mode=''`):
///
/// ```text
/// create table n1 (c1 text not null default '')       -> `c1` text NOT NULL
/// create table n4 (c1 tinyblob not null default '')   -> `c1` tinyblob NOT NULL DEFAULT ''
/// create table n3 (c1 json not null default '')       -> `c1` json NOT NULL DEFAULT 'null'
/// create table e1 (c1 text default 'x')               -> ERROR 1101
/// ```
///
/// `hasDefaultValue = false` is not "drop the default": Go still STORES the
/// value, and only a NOT NULL column then takes `NoDefaultValueFlag`, which
/// is what makes `SHOW CREATE TABLE` print no DEFAULT clause. A NULLABLE
/// `text DEFAULT ''` keeps printing `DEFAULT ''`. This tier models that flag
/// as "no default recorded", so the pair maps onto `None` for a NOT NULL
/// column and onto the stored value otherwise.
pub fn check_column_default_value(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    ctx: &tidb_metabuild::Context,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<(bool, Datum), MysqlError> {
    use tidb_datatype::FieldTypeCode;

    if !value.is_null() && field_type.code() == FieldTypeCode::VectorFloat32 {
        return Err(tidb_terror::MysqlError::unknown(format!(
            "VECTOR column '{column}' can't have a literal default. Use expression default instead: ((VEC_FROM_TEXT('...')))"
        )));
    }
    if !value.is_null()
        && ctx.get_sql_mode().has_strict_mode()
        && ctx.get_sql_mode().has_no_zero_date_mode()
        && matches!(
            field_type.code(),
            FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp
        )
    {
        let converted = value
            .convert_to_in(
                field_type,
                ctx.get_expr_ctx().get_eval_ctx().type_ctx_flags(),
                zone,
            )
            .map_err(|_| {
                tidb_terror::MysqlError::from_terror(
                    &tidb_dbterror::ERR_INVALID_DEFAULT_VALUE
                        .generate_with_stack_by_args(&[column.to_owned().into()]),
                )
            })?;
        if matches!(converted.value, Datum::Time(time) if time.is_zero()) {
            return Err(tidb_terror::MysqlError::from_terror(
                &tidb_dbterror::ERR_INVALID_DEFAULT_VALUE
                    .generate_with_stack_by_args(&[column.to_owned().into()]),
            ));
        }
    }
    if value.is_null()
        || !matches!(
            field_type.code(),
            FieldTypeCode::Json
                | FieldTypeCode::TinyBlob
                | FieldTypeCode::MediumBlob
                | FieldTypeCode::LongBlob
                | FieldTypeCode::Blob
        )
    {
        return Ok((true, value));
    }
    let empty = matches!(value.as_raw_bytes(), Some(bytes) if bytes.is_empty());
    if ctx.get_sql_mode().has_strict_mode() || !empty {
        return Err(tidb_terror::MysqlError::from_terror(
            &tidb_dbterror::ERR_BLOB_CANT_HAVE_DEFAULT
                .generate_with_stack_by_args(&[column.to_owned().into()]),
        ));
    }
    // Go `ctx.GetEvalCtx().AppendWarning(dbterror.ErrBlobCantHaveDefault.FastGenByArgs(col.Name.O))`.
    let reported = tidb_terror::MysqlError::from_terror(
        &tidb_dbterror::ERR_BLOB_CANT_HAVE_DEFAULT
            .generate_with_stack_by_args(&[column.to_string().into()]),
    );
    ctx.append_warning(tidb_context::WarnErr::Terror(
        tidb_dbterror::ERR_BLOB_CANT_HAVE_DEFAULT.generate(reported.message),
    ));
    Ok(match field_type.code() {
        FieldTypeCode::Blob | FieldTypeCode::LongBlob => (false, value),
        FieldTypeCode::Json => (true, Datum::new_string("null")),
        _ => (true, value),
    })
}

/// Runs the source storage stages without the final-column-flag validation.
///
/// CREATE TABLE needs this split because Go visits DEFAULT in option order,
/// but checks NULL against NOT NULL / PRIMARY KEY only after every option and
/// table-level key has stamped the final FieldType.
pub fn settle_column_default(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    ctx: &tidb_metabuild::Context,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<SettledColumnDefault, MysqlError> {
    let flags = ctx.get_expr_ctx().get_eval_ctx().type_ctx_flags();
    let stored = column_default_storage_value(value, field_type, column, flags, zone, Some(ctx))?;
    let (has_default, stored) = check_column_default_value(stored, field_type, column, ctx, zone)?;
    let stored = timestamp_default_to_utc(stored, field_type, column, flags, zone)?;
    Ok(SettledColumnDefault {
        has_default,
        stored: pad_fixed_width_binary_default(stored, field_type),
    })
}

/// Go `checkDefaultValue`: validate the persisted spelling against the
/// column's final flags and return the typed value an omitted row receives.
pub fn validate_column_default(
    stored: &Datum,
    field_type: &FieldType,
    column: &str,
    column_info_version: u64,
    flags: tidb_datatype::ConversionFlags,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Datum, MysqlError> {
    let invalid = || {
        tidb_terror::MysqlError::from_terror(
            &tidb_dbterror::ERR_INVALID_DEFAULT_VALUE
                .generate_with_stack_by_args(&[column.to_owned().into()]),
        )
    };
    if stored.is_null() {
        // Inline PRIMARY KEY + DEFAULT NULL was already intercepted by
        // `checkPriKeyConstraint`. At this later `checkDefaultValue` boundary
        // Go checks PRI before NOT NULL, so a table-level key is 1171 even
        // when the column also spelled NOT NULL.
        if field_type.has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY) {
            return Err(tidb_terror::MysqlError::from_terror(
                &tidb_dbterror::ERR_PRIMARY_CANT_HAVE_NULL.generate_with_stack_by_args(&[]),
            ));
        }
        if field_type.has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL) {
            return Err(invalid());
        }
        return Ok(Datum::Null);
    }

    let checked = tidb_table::column_default::materialize_stored_literal(
        stored,
        field_type,
        column_info_version,
        flags,
        zone,
    )
    .map_err(|_| invalid())?;
    if checked
        .event
        .as_ref()
        .is_some_and(|event| !tidb_table::column::conversion_event_is_silent(event))
    {
        return Err(invalid());
    }
    Ok(checked.value)
}

/// Go `getDefaultValue`: settle one evaluated expression to the exact byte
/// string `ColumnInfo.DefaultValue` stores. This is deliberately distinct
/// from [`validate_column_default`], which returns the typed runtime value.
pub fn column_default_storage_value(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    flags: tidb_datatype::ConversionFlags,
    zone: &tidb_datatype::SessionTimeZone,
    ctx: Option<&tidb_metabuild::Context>,
) -> Result<Datum, MysqlError> {
    if value.is_null() {
        return Ok(Datum::Null);
    }
    let invalid = || {
        tidb_terror::MysqlError::from_terror(
            &tidb_dbterror::ERR_INVALID_DEFAULT_VALUE
                .generate_with_stack_by_args(&[column.to_owned().into()]),
        )
    };

    // Go handles binary literals before its target-type switch. The three
    // branches are exhaustive and return immediately, so FLOAT/DOUBLE never
    // round their persisted spelling and DECIMAL/TIME/YEAR never retain the
    // literal's raw control bytes.
    if let Datum::BinaryLiteral(literal) | Datum::Bit(literal) = &value {
        if matches!(
            field_type.code(),
            tidb_datatype::FieldTypeCode::Date
                | tidb_datatype::FieldTypeCode::Datetime
                | tidb_datatype::FieldTypeCode::Timestamp
        ) {
            return Err(invalid());
        }
        let bytes = if matches!(
            field_type.code(),
            tidb_datatype::FieldTypeCode::Blob
                | tidb_datatype::FieldTypeCode::TinyBlob
                | tidb_datatype::FieldTypeCode::MediumBlob
                | tidb_datatype::FieldTypeCode::LongBlob
                | tidb_datatype::FieldTypeCode::Json
                | tidb_datatype::FieldTypeCode::VectorFloat32
        ) {
            literal.as_bytes().to_vec()
        } else if matches!(
            field_type.code(),
            tidb_datatype::FieldTypeCode::Bit
                | tidb_datatype::FieldTypeCode::String
                | tidb_datatype::FieldTypeCode::Varchar
                | tidb_datatype::FieldTypeCode::VarString
                | tidb_datatype::FieldTypeCode::Enum
                | tidb_datatype::FieldTypeCode::Set
        ) {
            let (bytes, error) = value
                .binary_string_decoded(flags, field_type.charset_name())
                .into_parts();
            if error.is_some() {
                return Err(invalid());
            }
            bytes
        } else {
            let outcome = literal.to_int();
            if outcome.is_truncated() {
                return Err(invalid());
            }
            outcome.value().to_string().into_bytes()
        };
        return Ok(Datum::new_collation_string(bytes, field_type.collation()));
    }

    let normalized = match field_type.code() {
        tidb_datatype::FieldTypeCode::Tiny
        | tidb_datatype::FieldTypeCode::Short
        | tidb_datatype::FieldTypeCode::Int24
        | tidb_datatype::FieldTypeCode::Long
        | tidb_datatype::FieldTypeCode::LongLong
        | tidb_datatype::FieldTypeCode::Float
        | tidb_datatype::FieldTypeCode::Double => {
            // Go adopts the converted value only when the conversion itself
            // succeeded (`if temp, err := v.ConvertTo(...); err == nil`), and
            // otherwise keeps the original for the check below to report.
            match value.convert_to_in(field_type, flags, zone) {
                Ok(converted) if converted.event.is_none() => converted.value,
                _ => value.clone(),
            }
        }
        tidb_datatype::FieldTypeCode::Enum | tidb_datatype::FieldTypeCode::Set => {
            enum_set_column_default(&value, field_type).ok_or_else(invalid)?
        }
        tidb_datatype::FieldTypeCode::Date
        | tidb_datatype::FieldTypeCode::Datetime
        | tidb_datatype::FieldTypeCode::Timestamp
        | tidb_datatype::FieldTypeCode::Duration => {
            // Go `GetTimeValue`: a value `ParseTime` refuses is an invalid
            // default, while the truncation `parseDatetime` appends directly
            // (`'2020-03-27 20:20:20 123456'` has a seventh part) is the
            // statement's warning and the default is kept.
            #[derive(Default)]
            struct Warnings(std::cell::RefCell<Vec<tidb_terror::TerrorError>>);
            impl tidb_context::ConversionWarningAppender for Warnings {
                fn append_conversion_warning(&self, warning: tidb_terror::TerrorError) {
                    self.0.borrow_mut().push(warning);
                }
            }
            let warnings = Warnings::default();
            let context = tidb_datatype::ConversionContext::new(
                flags,
                tidb_datatype::ConversionLocation::from_time_zone(zone),
                &warnings,
            );
            let converted = value
                .convert_to_in_context(field_type, &context, zone)
                .map_err(|_| invalid())?;
            if converted.error.is_some() {
                return Err(invalid());
            }
            if let Some(ctx) = ctx {
                for warning in warnings.0.into_inner() {
                    ctx.append_warning(tidb_context::WarnErr::Terror(warning));
                }
            }
            converted.value
        }
        tidb_datatype::FieldTypeCode::Bit => bit_column_default(&value, field_type, column)?,
        _ => value.clone(),
    };
    let bytes = normalized.sql_bytes().map_err(|_| invalid())?;
    Ok(Datum::new_collation_string(bytes, field_type.collation()))
}

/// Go `convertTimestampDefaultValToUTC`: a literal TIMESTAMP is persisted as
/// a UTC wall clock after it has passed the session-zone admission checks.
/// Zero stays zero, and computed `CURRENT_TIMESTAMP` never reaches this path.
pub fn timestamp_default_to_utc(
    stored: Datum,
    field_type: &FieldType,
    column: &str,
    flags: tidb_datatype::ConversionFlags,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Datum, MysqlError> {
    if stored.is_null() || field_type.code() != tidb_datatype::FieldTypeCode::Timestamp {
        return Ok(stored);
    }
    let invalid = || {
        tidb_terror::MysqlError::from_terror(
            &tidb_dbterror::ERR_INVALID_DEFAULT_VALUE
                .generate_with_stack_by_args(&[column.to_owned().into()]),
        )
    };
    let converted = stored
        .convert_to_in(field_type, flags, zone)
        .map_err(|_| invalid())?;
    if converted
        .event
        .as_ref()
        .is_some_and(|event| !tidb_table::column::conversion_event_is_silent(event))
    {
        return Err(invalid());
    }
    let Datum::Time(mut time) = converted.value else {
        return Err(invalid());
    };
    if time.is_zero() {
        return Ok(stored);
    }
    time.convert_time_zone(zone, &tidb_datatype::SessionTimeZone::utc())
        .map_err(|_| invalid())?;
    let bytes = Datum::new_time(time).sql_bytes().map_err(|_| invalid())?;
    Ok(Datum::new_collation_string(bytes, field_type.collation()))
}

/// An `ENUM`/`SET` column's written `DEFAULT`, resolved to a MEMBER of the
/// column's own element list, which is the only thing the column can hold.
///
/// Go `pkg/ddl/add_column.go` reaches the same value by three different
/// routes, and the route is chosen by what the default was WRITTEN as:
///
///  * a hex/bit literal (`DEFAULT 0x61`) is decoded to text in the column's
///    charset and taken verbatim -- `getDefaultValue`'s `KindBinaryLiteral`
///    branch returns before the type switch, so the bytes name the member
///    directly and are never read as a number;
///  * an integer (`DEFAULT 2`) is an INDEX: one-based into the element list
///    for `ENUM` (`getEnumDefaultValue` -> `ParseEnumValue`), a bit mask for
///    `SET` (`ParseSetValue`);
///  * anything else is text matched against the element list under the
///    column's collation, with trailing spaces stripped first for `ENUM`
///    because "trailing spaces are automatically deleted from ENUM member
///    values" (`getEnumDefaultValue` -> `TrimRight` -> `ParseEnumName`), and
///    with the empty string admitted for `SET` as the no-members-set value.
///
/// `None` means no member matches, which the caller reports as 1067. Storing
/// the written literal instead -- the shape this replaced -- leaves a column
/// whose `DEFAULT` is not a value of its own type, so an omitted column on
/// `INSERT` reads something the element list does not contain.
/// Go `setDefaultValueWithBinaryPadding`: a FIXED-width binary column pads its
/// stored `DEFAULT` with NUL bytes out to the declared width, exactly as a
/// value written into `BINARY(n)` is padded. `VARBINARY` and every non-binary
/// charset are variable width and keep the default as written.
///
/// Without it, `BINARY(4) DEFAULT 0x61` records a one-byte default for a
/// column that can only ever hold four.
pub fn pad_fixed_width_binary_default(value: Datum, field_type: &FieldType) -> Datum {
    if field_type.code() != tidb_datatype::FieldTypeCode::String || !field_type.is_binary_string() {
        return value;
    }
    let width = field_type.flen();
    let Some(bytes) = value.as_raw_bytes() else {
        return value;
    };
    if width < 0 || bytes.len() >= width as usize {
        return value;
    }
    let mut padded = bytes.to_vec();
    padded.resize(width as usize, 0);
    Datum::new_collation_string(padded, field_type.collation())
}

/// A `BIT(n)` column's written `DEFAULT`, settled to the BITS it names.
///
/// Go `pkg/ddl/add_column.go` `getDefaultValue` reaches the same bits from two
/// spellings, and keeps neither of them verbatim:
///
///  * a bit or hex literal (`DEFAULT b'1100110111001'`, `DEFAULT 0x19b9`) is
///    read with `GetBinaryStringDecoded` in the column's charset, which for a
///    `BIT` column is `binary` and so hands back the literal's own bytes;
///  * an INTEGER (`DEFAULT 250`) becomes `NewBinaryLiteralFromUint(v, -1)`,
///    the number's minimal big-endian bytes.
///
/// Go then stores those bytes as the column's `DefaultValue` string, and every
/// surface that prints a default -- `SHOW CREATE TABLE`, `SHOW COLUMNS`,
/// `information_schema.columns` -- renders them with
/// `BinaryLiteral.ToBitLiteralString(true)`, so both spellings print back as
/// `b'11111010'`. Keeping the WRITTEN datum instead prints `DEFAULT '250'`,
/// which re-reads as the three characters `250` and not as the bits.
pub fn bit_column_default(
    value: &Datum,
    field_type: &FieldType,
    column: &str,
) -> Result<Datum, MysqlError> {
    let invalid = || {
        tidb_terror::MysqlError::from_terror(
            &tidb_dbterror::ERR_INVALID_DEFAULT_VALUE
                .generate_with_stack_by_args(&[column.to_owned().into()]),
        )
    };
    let bits = match value {
        Datum::BinaryLiteral(_) | Datum::Bit(_) => {
            let (bytes, error) = value
                .binary_string_decoded(tidb_datatype::STRICT_FLAGS, field_type.charset_name())
                .into_parts();
            if error.is_some() {
                return Err(invalid());
            }
            tidb_datatype::BinaryLiteral::from(bytes)
        }
        Datum::Int(_) | Datum::UInt(_) => {
            let number = value
                .as_uint()
                .or_else(|| value.as_int().map(|value| value as u64))
                .ok_or_else(invalid)?;
            tidb_datatype::BinaryLiteral::from_uint(number, None)
        }
        // Go falls through to `v.ToString()` for every other kind and lets
        // the check phase decide, so the written value is kept here too.
        _ => return Ok(value.clone()),
    };
    Ok(Datum::BinaryLiteral(bits))
}

pub fn enum_set_column_default(value: &Datum, field_type: &FieldType) -> Option<Datum> {
    let collator = field_type.runtime_collator();
    let datum_collation = field_type.collation();
    let is_set = field_type.code() == tidb_datatype::FieldTypeCode::Set;
    let member = match value {
        Datum::BinaryLiteral(_) | Datum::Bit(_) => {
            let (bytes, error) = value
                .binary_string_decoded(
                    tidb_datatype::ConversionFlags::default(),
                    field_type.charset_name(),
                )
                .into_parts();
            if error.is_some() {
                return None;
            }
            bytes
        }
        Datum::Int(index) => {
            if is_set {
                let element_count = field_type.elems().len();
                let upper = if element_count >= i64::BITS as usize {
                    -1
                } else {
                    (1_i64 << element_count).wrapping_sub(1)
                };
                if *index < 1 || *index > upper {
                    return None;
                }
            }
            let index = u64::try_from(*index).ok()?;
            if is_set {
                field_type.with_elems_visible(|elements| {
                    tidb_datatype::parse_set_value(elements, index)
                        .ok()
                        .map(|members| members.name_bytes().to_vec())
                })?
            } else {
                field_type.with_elems_visible(|elements| {
                    tidb_datatype::parse_enum_value(elements, index)
                        .ok()
                        .map(|member| member.name_bytes().to_vec())
                })?
            }
        }
        _ => {
            let mut text = value.sql_bytes().ok()?;
            if is_set {
                field_type.with_elems_visible(|elements| {
                    tidb_datatype::parse_set_name(elements, text.as_slice(), collator)
                        .ok()
                        .map(|members| members.name_bytes().to_vec())
                })?
            } else {
                while text.last() == Some(&b' ') {
                    text.pop();
                }
                field_type.with_elems_visible(|elements| {
                    tidb_datatype::parse_enum_name(elements, text.as_slice(), collator)
                        .ok()
                        .map(|member| member.name_bytes().to_vec())
                })?
            }
        }
    };
    Some(Datum::new_collation_string(member, datum_collation))
}

/// One `ADD COLUMN`.
/// The result of Go `getDefaultValue` -> `checkColumnDefaultValue` ->
/// `setDefaultValueWithBinaryPadding`, before final column flags validate it.
#[derive(Clone, Debug)]
pub struct SettledColumnDefault {
    /// Go `hasDefaultValue`; an empty non-strict BLOB/TEXT default may be
    /// stored while this is false.
    pub has_default: bool,
    /// The exact persisted `ColumnInfo.DefaultValue` string, represented as a
    /// byte-capable Datum, or NULL.
    pub stored: Datum,
}

/// Compatibility entrypoint for callers that need only the typed value and
/// have already applied `checkColumnDefaultValue` themselves.
pub fn normalize_column_default(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<Datum, MysqlError> {
    let stored = column_default_storage_value(
        value,
        field_type,
        column,
        tidb_datatype::STRICT_FLAGS,
        zone,
        None,
    )?;
    let stored = pad_fixed_width_binary_default(stored, field_type);
    validate_column_default(
        &stored,
        field_type,
        column,
        tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
        tidb_datatype::STRICT_FLAGS,
        zone,
    )
}

#[cfg(test)]
mod normalize_default_tests {
    use super::{enum_set_column_default, normalize_column_default};
    use tidb_datatype::{BinaryLiteral, Datum, FieldType, FieldTypeCode, SessionTimeZone};
use tidb_goruntime::GoString;

    #[test]
    fn enum_set_defaults_preserve_raw_member_bytes() {
        let mut enum_type = FieldType::new(FieldTypeCode::Enum);
        enum_type.set_elems(vec![GoString::from([0xff])]);
        let value = enum_set_column_default(&Datum::Bytes(vec![0xff]), &enum_type)
            .expect("raw ENUM default matches its declaration");
        match value {
            Datum::String(value) => assert_eq!(value.bytes(), [0xff]),
            other => panic!("expected a string default, got {other:?}"),
        }
        let value = normalize_column_default(
            Datum::new_binary_literal(BinaryLiteral::from(vec![0xff])),
            &enum_type,
            "e",
            &SessionTimeZone::utc(),
        )
        .expect("the raw ENUM member passes final strict validation");
        assert_eq!(value.sql_bytes().unwrap(), [0xff]);

        let mut set_type = FieldType::new(FieldTypeCode::Set);
        set_type.set_elems(vec![GoString::from([0xfe])]);
        let value = enum_set_column_default(&Datum::Bytes(vec![0xfe]), &set_type)
            .expect("raw SET default matches its declaration");
        match value {
            Datum::String(value) => assert_eq!(value.bytes(), [0xfe]),
            other => panic!("expected a string default, got {other:?}"),
        }
        let value = normalize_column_default(
            Datum::new_binary_literal(BinaryLiteral::from(vec![0xfe])),
            &set_type,
            "s",
            &SessionTimeZone::utc(),
        )
        .expect("the raw SET member passes final strict validation");
        assert_eq!(value.sql_bytes().unwrap(), [0xfe]);
    }

    #[test]
    fn enum_set_uint_defaults_use_member_names_not_signed_indexes() {
        let number = 9_223_372_036_854_775_808_u64;
        let member = GoString::from(number.to_string());

        for code in [FieldTypeCode::Enum, FieldTypeCode::Set] {
            let mut field_type = FieldType::new(code);
            field_type.set_elems(vec![member.clone()]);
            let value = enum_set_column_default(&Datum::UInt(number), &field_type)
                .expect("a uint default follows Go's string-name branch");
            match value {
                Datum::String(value) => assert_eq!(value.bytes(), member.as_bytes()),
                other => panic!("expected a string default, got {other:?}"),
            }
        }
    }
}
