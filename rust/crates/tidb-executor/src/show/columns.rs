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

//! Go `NewColDesc` and `ShowExec.fetchShowColumns`' cells: the column
//! description rows SHOW COLUMNS / DESCRIBE answer and the cells
//! `information_schema.COLUMNS` shares with them.

use tidb_datatype::{Datum, STRICT_INTEGER_DISPLAY_WIDTH};

use crate::simple::account_tables::sysvar_datum_text as datum_text;
use crate::DriverError;

/// The `Type` cell of a `SHOW COLUMNS`/`DESCRIBE` row: Go `NewColDesc`'s
/// `col.GetTypeDesc()`.
fn type_desc_cell(field_type: &tidb_datatype::FieldType) -> Datum {
    Datum::Bytes(
        field_type
            .type_desc(STRICT_INTEGER_DISPLAY_WIDTH)
            .into_bytes(),
    )
}

/// The text of a column's SETTLED `DEFAULT`, as every surface that prints one
/// renders it.
///
/// Go stores the default as a string and each printer -- `pkg/executor/show.go`
/// for both `SHOW CREATE TABLE` and `SHOW COLUMNS`, and
/// `pkg/infoschema`'s `COLUMN_DEFAULT` -- carries the SAME `TypeBit` branch:
/// a `BIT` column's stored bytes print through
/// `BinaryLiteral.ToBitLiteralString(true)`, so `DEFAULT 250` and
/// `DEFAULT b'11111010'` both read back as `b'11111010'`. Every other type
/// prints its stored text.
pub fn column_default_text(
    value: &Datum,
    field_type: &tidb_datatype::FieldType,
) -> Option<String> {
    if field_type.code() == tidb_datatype::FieldTypeCode::Bit {
        return match value {
            Datum::Null => None,
            Datum::String(_)
            | Datum::Bytes(_)
            | Datum::Raw(_)
            | Datum::BinaryLiteral(_)
            | Datum::Bit(_) => Some(
                tidb_datatype::BinaryLiteral::from(value.go_bytes()).to_bit_literal_string(true),
            ),
            // Anything else never settled into the column's own domain;
            // rendering it as bits would invent a value it does not hold.
            other => datum_text(other),
        };
    }
    datum_text(value)
}

/// The SQL-visible text of one LITERAL column default.
///
/// Go stores a version-1-and-later `TIMESTAMP` default as a UTC wall clock,
/// then `GetColDefaultValue` projects it into the reading session before any
/// metadata surface prints it. Version 0 uses the system zone as the source;
/// [`crate::column_default::materialize_stored_literal`] owns that
/// version boundary. Other types do not pass through this cast here: their
/// stored metadata spelling is the spelling SHOW and INFORMATION_SCHEMA
/// report.
pub fn literal_column_default_text(
    value: &Datum,
    column: &crate::KvColumn,
    flags: tidb_datatype::ConversionFlags,
    session_zone: &tidb_datatype::SessionTimeZone,
) -> Result<Option<String>, tidb_datatype::DatumValueError> {
    if column.field_type.code() != tidb_datatype::FieldTypeCode::Timestamp {
        return Ok(column_default_text(value, &column.field_type));
    }
    let converted = crate::column_default::materialize_stored_literal(
        value,
        &column.field_type,
        column.column_info_version,
        flags,
        session_zone,
    )?;
    Ok(match converted.value {
        Datum::Time(time) => Some(time.to_string()),
        other => column_default_text(&other, &column.field_type),
    })
}

/// Go `NewColDesc`'s `Collation` cell: the column's own collation name, and
/// NULL for anything with no character set -- a numeric or temporal column,
/// and a binary-charset string column alike (captured: `varbinary`, `binary`,
/// `blob` and `longblob` all report NULL).
fn column_collation_cell(field_type: &tidb_datatype::FieldType) -> Datum {
    if field_type.has_charset() {
        Datum::Bytes(field_type.collation_name().as_bytes().to_vec())
    } else {
        Datum::Null
    }
}

/// Go's mock session's fixed grant string for every column of every table
/// (`fetchShowColumns`): this tier grants no per-column privileges of its
/// own, so it reports the same static capture MySQL/TiDB print for a column
/// the current user can select, insert, update, and reference.
const FULL_COL_DESC_PRIVILEGES: &str = "select,insert,update,references";

/// Go `table.NewColDesc`, restricted to the facts this seed's metadata holds.
///
/// `Null` is NO when the column carries `NotNullFlag`; `Key` is PRI for a
/// primary-key column, UNI for a column that is the whole of a unique index,
/// and MUL for one that leads a non-unique index -- Go reads those from the
/// column's key flags, which the DDL sets from the same index definitions.
///
/// `Default` is the column's stored `DEFAULT`, or NULL when none was written.
///
/// `Extra` follows Go `NewColDesc`'s ordered precedence: auto increment,
/// ON UPDATE CURRENT_TIMESTAMP, generated-column kind, then expression
/// default.
pub fn column_description(
    column: &crate::KvColumn,
    offset: usize,
    table: &crate::KvTable,
    full: bool,
    ctx: &crate::StmtContext,
) -> Result<Vec<Datum>, DriverError> {
    let null_flag = if column.field_type.flags() & NOT_NULL_FLAG != 0 {
        "NO"
    } else {
        "YES"
    };
    let extra = column_extra(
        &column.field_type,
        table.auto_increment_offset() == Some(offset),
        column.generated.as_ref().map(|generated| generated.stored),
        column
            .default_value
            .as_ref()
            .is_some_and(crate::column_default::ColumnDefault::is_default_generated),
    );
    let key_flag = column_key_flag(table, offset);
    // Go `NewColDesc`: a column carrying `NoDefaultValueFlag` reports no
    // default, whatever value its metadata still stores (a NOT NULL TEXT/BLOB
    // given `DEFAULT ''` under a non-strict sql_mode).
    let no_default = column
        .field_type
        .has_flag(tidb_datatype::FieldTypeFlags::NO_DEFAULT_VALUE);
    let default = match &column.default_value {
        _ if no_default => Datum::Null,
        Some(crate::column_default::ColumnDefault::Value(value)) => {
            match literal_column_default_text(
                value,
                column,
                ctx.show_default_conversion_flags(),
                &ctx.session_zone(),
            )
            .map_err(|_| {
                DriverError::from(
                    tidb_error::catalog::table::ERR_GET_DEFAULT_FAILED
                        .generate_with_stack_by_args(&[column.name.clone().into()]),
                )
            })? {
                Some(text) => Datum::Bytes(text.into_bytes()),
                None => Datum::Null,
            }
        }
        // Go `NewColDesc` reports a computed default's STORED string here,
        // which is not the parenthesised form `SHOW CREATE TABLE` prints.
        Some(computed) => match computed.column_desc_text(&column.field_type) {
            Some(text) => Datum::Bytes(text.into_bytes()),
            None => Datum::Null,
        },
        None => Datum::Null,
    };
    if !full {
        return Ok(vec![
            Datum::Bytes(column.name.clone().into_bytes()),
            type_desc_cell(&column.field_type),
            Datum::Bytes(null_flag.as_bytes().to_vec()),
            Datum::Bytes(key_flag.into_bytes()),
            default,
            Datum::Bytes(extra.clone().into_bytes()),
        ]);
    }
    let collation = column_collation_cell(&column.field_type);
    Ok(vec![
        Datum::Bytes(column.name.clone().into_bytes()),
        type_desc_cell(&column.field_type),
        collation,
        Datum::Bytes(null_flag.as_bytes().to_vec()),
        Datum::Bytes(key_flag.into_bytes()),
        default,
        Datum::Bytes(extra.into_bytes()),
        Datum::Bytes(FULL_COL_DESC_PRIVILEGES.as_bytes().to_vec()),
        // Go `table.NewColDesc`'s `Comment`, from `ColumnInfo.Comment`.
        Datum::Bytes(column.comment.clone().into_bytes()),
    ])
}

/// Go `table.NewColDesc`'s `Extra` field, kept as a pure formatter so every
/// metadata producer supplies the same four source facts in the same order.
pub fn column_extra(
    field_type: &tidb_datatype::FieldType,
    auto_increment: bool,
    generated_stored: Option<bool>,
    default_is_expr: bool,
) -> String {
    if auto_increment {
        return "auto_increment".to_owned();
    }
    if field_type.has_flag(tidb_datatype::FieldTypeFlags::ON_UPDATE_NOW) {
        let fsp = field_type.decimal();
        return if fsp > 0 {
            format!("DEFAULT_GENERATED on update CURRENT_TIMESTAMP({fsp})")
        } else {
            "DEFAULT_GENERATED on update CURRENT_TIMESTAMP".to_owned()
        };
    }
    if let Some(stored) = generated_stored {
        return if stored {
            "STORED GENERATED".to_owned()
        } else {
            "VIRTUAL GENERATED".to_owned()
        };
    }
    if default_is_expr {
        return "DEFAULT_GENERATED".to_owned();
    }
    String::new()
}

/// A view column's `SHOW COLUMNS` row.
///
/// A view carries no storage metadata, so Go reports no key, no default and
/// no extra for every one of its columns; only the name, the type the body
/// produced, and nullability come from the definition.
///
/// # Why this disagrees with `information_schema.columns`, on purpose
///
/// Go's `tryFillViewColumnType` (`pkg/executor/show.go`) OVERWRITES the stored
/// column's `FieldType` with the re-planned one and then rewrites `VarString`
/// to `Varchar` in place, so every cell this row prints -- type text, charset,
/// collation, nullability -- is read off the PLAN's type.
/// `dataForColumnsInTable` does not: it keeps the re-planned type for
/// `COLUMN_TYPE`/`DATA_TYPE` only and builds the rest from the STORED column,
/// and it does the `VarString` remap for `DATA_TYPE` alone.
///
/// One captured view makes both halves visible at once:
///
/// ```text
/// desc v                            ->  event_id | varchar(32)    | NO
/// information_schema.columns for v  ->  event_id | var_string(32) | YES
/// ```
///
/// Making the two surfaces agree would be the regression, not the fix.
pub fn view_column_description(
    name: &str,
    field_type: &tidb_datatype::FieldType,
    full: bool,
) -> Vec<Datum> {
    let field_type = &show_columns_view_type(field_type);
    let null_flag = if field_type.flags() & NOT_NULL_FLAG != 0 {
        "NO"
    } else {
        "YES"
    };
    // Go `NewColDesc`: the key column reads the type's own flags, which a
    // view column inherits from the base column it projects.
    let key_flag = if field_type.has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY) {
        "PRI"
    } else if field_type.has_flag(tidb_datatype::FieldTypeFlags::UNIQUE_KEY) {
        "UNI"
    } else if field_type.has_flag(tidb_datatype::FieldTypeFlags::MULTIPLE_KEY) {
        "MUL"
    } else {
        ""
    };
    if !full {
        return vec![
            Datum::Bytes(name.as_bytes().to_vec()),
            type_desc_cell(field_type),
            Datum::Bytes(null_flag.as_bytes().to_vec()),
            Datum::Bytes(key_flag.as_bytes().to_vec()),
            Datum::Null,
            Datum::Bytes(Vec::new()),
        ];
    }
    let collation = column_collation_cell(field_type);
    vec![
        Datum::Bytes(name.as_bytes().to_vec()),
        type_desc_cell(field_type),
        collation,
        Datum::Bytes(null_flag.as_bytes().to_vec()),
        Datum::Bytes(key_flag.as_bytes().to_vec()),
        Datum::Null,
        Datum::Bytes(Vec::new()),
        Datum::Bytes(FULL_COL_DESC_PRIVILEGES.as_bytes().to_vec()),
        Datum::Bytes(Vec::new()),
    ]
}

/// Go `tryFillViewColumnType`'s closing rewrite: a re-planned view column
/// whose type came back `VarString` is reported as a `VARCHAR`.
///
/// `CAST(... AS CHAR(32))` yields a `VarString` in the plan, so without this a
/// view over one describes itself with a type name no `CREATE TABLE` can
/// spell. The rewrite is confined to the `SHOW` surface; see
/// [`view_column_description`] for the surface that deliberately does not.
fn show_columns_view_type(field_type: &tidb_datatype::FieldType) -> tidb_datatype::FieldType {
    let mut field_type = field_type.clone();
    if field_type.code() == tidb_datatype::FieldTypeCode::VarString {
        field_type.set_code(tidb_datatype::FieldTypeCode::Varchar);
    }
    field_type
}

/// Go `mysql.NotNullFlag`.
pub(crate) const NOT_NULL_FLAG: u32 = 1;

/// Go `NewColDesc`'s key flag, shared by `SHOW COLUMNS` and
/// `information_schema.COLUMNS`: PRI for a primary key, UNI for a column that
/// is the whole of a unique index, MUL for one that leads a non-unique index.
pub fn column_key_flag(table: &crate::KvTable, offset: usize) -> String {
    if table.columns()[offset]
        .field_type
        .has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY)
    {
        "PRI".to_owned()
    } else if table
        .indexes()
        .iter()
        .any(|index| index.unique && index.column_offsets == [offset])
    {
        "UNI".to_owned()
    } else if table
        .indexes()
        .iter()
        .any(|index| index.column_offsets.first() == Some(&offset))
    {
        "MUL".to_owned()
    } else {
        String::new()
    }
}

#[cfg(test)]
mod column_description_source_tests {
    use super::*;
    use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};

    #[test]
    fn test_desc() {
        // Direct port of pkg/table/column_test.go::TestDesc. The Go test
        // drives NewColDesc by replacing the column flags, then toggles the
        // generated-column storage bit and asks for both result schemas.
        let auto = FieldType::new(FieldTypeCode::Long).with_flags(
            FieldTypeFlags::AUTO_INCREMENT | FieldTypeFlags::NOT_NULL | FieldTypeFlags::PRI_KEY,
        );
        assert_eq!(column_extra(&auto, true, None, false), "auto_increment");

        let multiple = FieldType::new(FieldTypeCode::Long).with_flags(FieldTypeFlags::MULTIPLE_KEY);
        assert_eq!(column_extra(&multiple, false, None, false), "");

        let on_update = FieldType::new(FieldTypeCode::Timestamp)
            .with_flags(FieldTypeFlags::UNIQUE_KEY | FieldTypeFlags::ON_UPDATE_NOW);
        assert_eq!(
            column_extra(&on_update, false, None, false),
            "DEFAULT_GENERATED on update CURRENT_TIMESTAMP"
        );

        let ordinary = FieldType::new(FieldTypeCode::Long);
        assert_eq!(
            column_extra(&ordinary, false, Some(true), false),
            "STORED GENERATED"
        );
        assert_eq!(
            column_extra(&ordinary, false, Some(false), false),
            "VIRTUAL GENERATED"
        );
        assert_eq!(
            column_extra(&ordinary, false, None, true),
            "DEFAULT_GENERATED"
        );

        assert_eq!(
            tidb_planner::show_schema::col_desc_field_names(false),
            ["Field", "Type", "Null", "Key", "Default", "Extra"]
        );
        assert_eq!(
            tidb_planner::show_schema::col_desc_field_names(true),
            [
                "Field",
                "Type",
                "Collation",
                "Null",
                "Key",
                "Default",
                "Extra",
                "Privileges",
                "Comment",
            ]
        );
    }
}
