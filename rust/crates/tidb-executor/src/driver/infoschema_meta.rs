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

//! The `information_schema` tables as CATALOG OBJECTS: their declared
//! columns, and the registration that puts them in every catalog.
//!
//! Go builds these in `pkg/infoschema/tables.go` with `buildTableMeta` at
//! bootstrap and puts the resulting `model.TableInfo`s in the same
//! `infoschema` the user's own tables live in. That is why `SHOW TABLES`,
//! `DESCRIBE` and `SHOW CREATE TABLE` answer for them without any of those
//! statements knowing that the ROWS are computed rather than stored.
//!
//! This module is the schema half, and it lives here rather than beside the
//! row computation (`tidb-session`'s `infoschema`) for exactly one reason:
//! [`Catalog::default`] can then register them itself, so there is no call
//! site left that could forget to. The rows stay in `tidb-session`, which is
//! the only tier that can see sessions and privileges.

use super::Catalog;
use crate::column_default::ColumnDefault;
use crate::{KvColumn, KvTable};
use tidb_datatype::{Datum, FieldType, FieldTypeCode, UNSPECIFIED_LENGTH};

/// Go's schema name for the virtual database.
pub const INFORMATION_SCHEMA: &str = "INFORMATION_SCHEMA";

/// Go `mysql.DefaultCharset` and its default collation, which every
/// string-ish column of these tables carries.
const CHARSET: &str = "utf8mb4";
const COLLATION: &str = "utf8mb4_bin";

/// Go `mysql.NotNullFlag`.
const NOT_NULL_FLAG: u32 = 1;
/// Go `mysql.UnsignedFlag`.
const UNSIGNED_FLAG: u32 = 32;
/// Go `resourcegroup.MaxGroupNameLength`.
const RESOURCE_GROUP_NAME_LENGTH: i64 = 32;

/// Go `infoschema.columnInfo`: one column of a virtual table, as
/// `pkg/infoschema/tables.go` declares it.
///
/// This is the DECLARATION, not the reported type -- [`InfoColumn::field_type`]
/// is the port of Go's `buildColumnInfo`, which is what turns `size` into a
/// flen and picks the charset. Keeping the two apart is what makes
/// `SHOW CREATE TABLE information_schema.COLUMNS` print `bigint(0)` and
/// `longtext` where the declaration says `TypeLonglong` with no size and
/// `TypeLongBlob`: Go's blob widths come from the type, its integer widths
/// come from the declared `size` verbatim -- including the zero.
struct InfoColumn {
    /// Go `columnInfo.name`.
    name: &'static str,
    /// Go `columnInfo.tp`.
    tp: FieldTypeCode,
    /// Go `columnInfo.size`, which is the flen for every type Go does not
    /// override below.
    size: i64,
    /// Go `columnInfo.flag`.
    flag: u32,
    /// Go `columnInfo.deflt`, rendered as the text `SHOW CREATE TABLE`
    /// prints. Go stores `any`, but every default in the tables this tier
    /// serves is a string or a small integer, and both print the same way.
    deflt: Option<&'static str>,
    /// Go `columnInfo.comment`, which `SHOW CREATE TABLE` prints after the
    /// column. Among the tables this tier serves only `CLUSTER_INFO`'s
    /// `SERVER_ID` declares one.
    comment: Option<&'static str>,
}

impl InfoColumn {
    /// Go `infoschema.buildColumnInfo`.
    fn field_type(&self) -> FieldType {
        let mut field_type = FieldType::new(self.tp);
        // Go gives the string-ish types utf8mb4 and every other type the
        // binary metadata `FieldType::new` already installs.
        if matches!(
            self.tp,
            FieldTypeCode::Varchar
                | FieldTypeCode::MediumBlob
                | FieldTypeCode::Blob
                | FieldTypeCode::LongBlob
                | FieldTypeCode::Enum
        ) {
            field_type.set_charset_name(CHARSET);
            field_type.set_collation_name(COLLATION);
            field_type.set_collation(tidb_datatype::Collation::DEFAULT);
        }
        field_type.set_flen(match self.tp {
            FieldTypeCode::Blob => 1 << 16,
            FieldTypeCode::MediumBlob => 1 << 24,
            FieldTypeCode::LongBlob => 1 << 32,
            _ => self.size,
        });
        // OCCUR_TIME is the one FSP-6 timestamp in the tables this tier
        // serves. Every other current declaration has decimal zero.
        field_type.set_decimal(
            if self.name == "OCCUR_TIME" && self.tp == FieldTypeCode::Timestamp {
                6
            } else {
                0
            },
        );
        field_type.set_flags(self.flag);
        field_type
    }
}

/// Go `infoschema.tableSchemataCols`.
/// Go `charsetCols` (`infoschema/tables.go:568`).
const CHARACTER_SETS_COLUMNS: &[InfoColumn] = &[
    InfoColumn {
        name: "CHARACTER_SET_NAME",
        tp: FieldTypeCode::Varchar,
        size: 32,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "DEFAULT_COLLATE_NAME",
        tp: FieldTypeCode::Varchar,
        size: 32,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "DESCRIPTION",
        tp: FieldTypeCode::Varchar,
        size: 60,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "MAXLEN",
        tp: FieldTypeCode::LongLong,
        size: 3,
        flag: 0,
        deflt: None,
        comment: None,
    },
];

/// Go `collationsCols` (`infoschema/tables.go:575`).
const COLLATIONS_COLUMNS: &[InfoColumn] = &[
    InfoColumn {
        name: "COLLATION_NAME",
        tp: FieldTypeCode::Varchar,
        size: 32,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "CHARACTER_SET_NAME",
        tp: FieldTypeCode::Varchar,
        size: 32,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "ID",
        tp: FieldTypeCode::LongLong,
        size: 11,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "IS_DEFAULT",
        tp: FieldTypeCode::Varchar,
        size: 3,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "IS_COMPILED",
        tp: FieldTypeCode::Varchar,
        size: 3,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "SORTLEN",
        tp: FieldTypeCode::LongLong,
        size: 3,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "PAD_ATTRIBUTE",
        tp: FieldTypeCode::Varchar,
        size: 9,
        flag: 0,
        deflt: None,
        comment: None,
    },
];

/// Go `tableCollationCharacterSetApplicabilityCols`
/// (`infoschema/tables.go:852`).
const COLLATION_CHARACTER_SET_APPLICABILITY_COLUMNS: &[InfoColumn] = &[
    InfoColumn {
        name: "COLLATION_NAME",
        tp: FieldTypeCode::Varchar,
        size: 32,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "CHARACTER_SET_NAME",
        tp: FieldTypeCode::Varchar,
        size: 32,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
];

const SCHEMATA_COLUMNS: &[InfoColumn] = &[
    InfoColumn {
        name: "CATALOG_NAME",
        tp: FieldTypeCode::Varchar,
        size: 512,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "SCHEMA_NAME",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "DEFAULT_CHARACTER_SET_NAME",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "DEFAULT_COLLATION_NAME",
        tp: FieldTypeCode::Varchar,
        size: 32,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "SQL_PATH",
        tp: FieldTypeCode::Varchar,
        size: 512,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TIDB_PLACEMENT_POLICY_NAME",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
];

/// Go `infoschema.tableTablesCols`.
const TABLES_COLUMNS: &[InfoColumn] = &[
    InfoColumn {
        name: "TABLE_CATALOG",
        tp: FieldTypeCode::Varchar,
        size: 512,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TABLE_SCHEMA",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TABLE_NAME",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TABLE_TYPE",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "ENGINE",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "VERSION",
        tp: FieldTypeCode::LongLong,
        size: 21,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "ROW_FORMAT",
        tp: FieldTypeCode::Varchar,
        size: 10,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TABLE_ROWS",
        tp: FieldTypeCode::LongLong,
        size: 21,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "AVG_ROW_LENGTH",
        tp: FieldTypeCode::LongLong,
        size: 21,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "DATA_LENGTH",
        tp: FieldTypeCode::LongLong,
        size: 21,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "MAX_DATA_LENGTH",
        tp: FieldTypeCode::LongLong,
        size: 21,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "INDEX_LENGTH",
        tp: FieldTypeCode::LongLong,
        size: 21,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "DATA_FREE",
        tp: FieldTypeCode::LongLong,
        size: 21,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "AUTO_INCREMENT",
        tp: FieldTypeCode::LongLong,
        size: 21,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "CREATE_TIME",
        tp: FieldTypeCode::Datetime,
        size: 19,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "UPDATE_TIME",
        tp: FieldTypeCode::Datetime,
        size: 19,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "CHECK_TIME",
        tp: FieldTypeCode::Datetime,
        size: 19,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TABLE_COLLATION",
        tp: FieldTypeCode::Varchar,
        size: 32,
        flag: 0,
        deflt: Some(COLLATION),
        comment: None,
    },
    InfoColumn {
        name: "CHECKSUM",
        tp: FieldTypeCode::LongLong,
        size: 21,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "CREATE_OPTIONS",
        tp: FieldTypeCode::Varchar,
        size: 255,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TABLE_COMMENT",
        tp: FieldTypeCode::Varchar,
        size: 2048,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TIDB_TABLE_ID",
        tp: FieldTypeCode::LongLong,
        size: 21,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TIDB_ROW_ID_SHARDING_INFO",
        tp: FieldTypeCode::Varchar,
        size: 255,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TIDB_PK_TYPE",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TIDB_PLACEMENT_POLICY_NAME",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TIDB_TABLE_MODE",
        tp: FieldTypeCode::Varchar,
        size: 16,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TIDB_AFFINITY",
        tp: FieldTypeCode::Varchar,
        size: 128,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TIDB_STORAGE_CLASS",
        tp: FieldTypeCode::Varchar,
        size: 32,
        flag: 0,
        deflt: None,
        comment: None,
    },
];

/// Go `infoschema.tableViewsCols`.
const VIEWS_COLUMNS: &[InfoColumn] = &[
    InfoColumn {
        name: "TABLE_CATALOG",
        tp: FieldTypeCode::Varchar,
        size: 512,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TABLE_SCHEMA",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TABLE_NAME",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "VIEW_DEFINITION",
        tp: FieldTypeCode::LongBlob,
        size: 0,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "CHECK_OPTION",
        tp: FieldTypeCode::Varchar,
        size: 8,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "IS_UPDATABLE",
        tp: FieldTypeCode::Varchar,
        size: 3,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "DEFINER",
        tp: FieldTypeCode::Varchar,
        size: 77,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "SECURITY_TYPE",
        tp: FieldTypeCode::Varchar,
        size: 7,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "CHARACTER_SET_CLIENT",
        tp: FieldTypeCode::Varchar,
        size: 32,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "COLLATION_CONNECTION",
        tp: FieldTypeCode::Varchar,
        size: 32,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
];

/// Go `infoschema.tableColumnsCols`.
const COLUMNS_COLUMNS: &[InfoColumn] = &[
    InfoColumn {
        name: "TABLE_CATALOG",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TABLE_SCHEMA",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TABLE_NAME",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "COLUMN_NAME",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "ORDINAL_POSITION",
        tp: FieldTypeCode::Long,
        size: 0,
        flag: UNSIGNED_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "COLUMN_DEFAULT",
        tp: FieldTypeCode::Blob,
        size: 0,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "IS_NULLABLE",
        tp: FieldTypeCode::Varchar,
        size: 3,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "DATA_TYPE",
        tp: FieldTypeCode::LongBlob,
        size: 0,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "CHARACTER_MAXIMUM_LENGTH",
        tp: FieldTypeCode::LongLong,
        size: 0,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "CHARACTER_OCTET_LENGTH",
        tp: FieldTypeCode::LongLong,
        size: 0,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "NUMERIC_PRECISION",
        tp: FieldTypeCode::LongLong,
        size: 0,
        flag: UNSIGNED_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "NUMERIC_SCALE",
        tp: FieldTypeCode::LongLong,
        size: 0,
        flag: UNSIGNED_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "DATETIME_PRECISION",
        tp: FieldTypeCode::Long,
        size: 0,
        flag: UNSIGNED_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "CHARACTER_SET_NAME",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "COLLATION_NAME",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "COLUMN_TYPE",
        tp: FieldTypeCode::MediumBlob,
        size: 0,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "COLUMN_KEY",
        tp: FieldTypeCode::Varchar,
        size: 3,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "EXTRA",
        tp: FieldTypeCode::Varchar,
        size: 256,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "PRIVILEGES",
        tp: FieldTypeCode::Varchar,
        size: 154,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "COLUMN_COMMENT",
        tp: FieldTypeCode::Blob,
        size: 0,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "GENERATION_EXPRESSION",
        tp: FieldTypeCode::LongBlob,
        size: 0,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "SRS_ID",
        tp: FieldTypeCode::Long,
        size: 0,
        flag: UNSIGNED_FLAG,
        deflt: None,
        comment: None,
    },
];

/// Whether `name` is the virtual schema, which is matched case-insensitively
/// as every schema name is.
#[must_use]
pub fn is_information_schema(name: &str) -> bool {
    name.eq_ignore_ascii_case(INFORMATION_SCHEMA)
}

/// Go `infoschema.tableKeyColumnUsageCols`, captured from a running TiDB:
/// one row per column of a `PRIMARY KEY` or `UNIQUE` index (a plain, non-unique
/// `KEY` does not appear here -- it only shows up in `STATISTICS`).
const KEY_COLUMN_USAGE_COLUMNS: &[InfoColumn] = &[
    InfoColumn {
        name: "CONSTRAINT_CATALOG",
        tp: FieldTypeCode::Varchar,
        size: 512,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "CONSTRAINT_SCHEMA",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "CONSTRAINT_NAME",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TABLE_CATALOG",
        tp: FieldTypeCode::Varchar,
        size: 512,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TABLE_SCHEMA",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TABLE_NAME",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "COLUMN_NAME",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "ORDINAL_POSITION",
        tp: FieldTypeCode::LongLong,
        size: 10,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "POSITION_IN_UNIQUE_CONSTRAINT",
        tp: FieldTypeCode::LongLong,
        size: 10,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "REFERENCED_TABLE_SCHEMA",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "REFERENCED_TABLE_NAME",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "REFERENCED_COLUMN_NAME",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
];

/// Go `infoschema.tableTableConstraintsCols`, captured: one row per
/// `PRIMARY KEY` or `UNIQUE` constraint (not per column).
const TABLE_CONSTRAINTS_COLUMNS: &[InfoColumn] = &[
    InfoColumn {
        name: "CONSTRAINT_CATALOG",
        tp: FieldTypeCode::Varchar,
        size: 512,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "CONSTRAINT_SCHEMA",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "CONSTRAINT_NAME",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TABLE_SCHEMA",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TABLE_NAME",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "CONSTRAINT_TYPE",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
];

/// Go `infoschema.tableStatisticsCols`, captured: one row per indexed
/// column, the same population `SHOW INDEX` reports (see
/// `show_index_rows` in `lib.rs`), but under this table's own column set
/// -- no `Clustered`/`Global` columns, and `TABLE_CATALOG`/`TABLE_SCHEMA`/
/// `INDEX_SCHEMA` in their place.
const STATISTICS_COLUMNS: &[InfoColumn] = &[
    InfoColumn {
        name: "TABLE_CATALOG",
        tp: FieldTypeCode::Varchar,
        size: 512,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TABLE_SCHEMA",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TABLE_NAME",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "NON_UNIQUE",
        tp: FieldTypeCode::Varchar,
        size: 1,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "INDEX_SCHEMA",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "INDEX_NAME",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "SEQ_IN_INDEX",
        tp: FieldTypeCode::LongLong,
        size: 2,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "COLUMN_NAME",
        tp: FieldTypeCode::Varchar,
        size: 21,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "COLLATION",
        tp: FieldTypeCode::Varchar,
        size: 1,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "CARDINALITY",
        tp: FieldTypeCode::LongLong,
        size: 21,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "SUB_PART",
        tp: FieldTypeCode::LongLong,
        size: 3,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "PACKED",
        tp: FieldTypeCode::Varchar,
        size: 10,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "NULLABLE",
        tp: FieldTypeCode::Varchar,
        size: 3,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "INDEX_TYPE",
        tp: FieldTypeCode::Varchar,
        size: 16,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "COMMENT",
        tp: FieldTypeCode::Varchar,
        size: 16,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "INDEX_COMMENT",
        tp: FieldTypeCode::Varchar,
        size: 1024,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "IS_VISIBLE",
        tp: FieldTypeCode::Varchar,
        size: 3,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "Expression",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
];

/// Go `infoschema.tableReferentialConstraintsCols`. This tier has no foreign
/// keys, so the table always has zero rows; only the header is captured.
const REFERENTIAL_CONSTRAINTS_COLUMNS: &[InfoColumn] = &[
    InfoColumn {
        name: "CONSTRAINT_CATALOG",
        tp: FieldTypeCode::Varchar,
        size: 512,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "CONSTRAINT_SCHEMA",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "CONSTRAINT_NAME",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "UNIQUE_CONSTRAINT_CATALOG",
        tp: FieldTypeCode::Varchar,
        size: 512,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "UNIQUE_CONSTRAINT_SCHEMA",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "UNIQUE_CONSTRAINT_NAME",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "MATCH_OPTION",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "UPDATE_RULE",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "DELETE_RULE",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TABLE_NAME",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "REFERENCED_TABLE_NAME",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
];

/// Go `infoschema.tableTiDBServersInfoCols` (`pkg/infoschema/tables.go:1123`).
/// Every column is declared with only a name, type and size upstream -- no
/// flags and no defaults -- so all eight are nullable with no default here.
///
/// Rows come from the server-info syncer rather than the catalog (Go's
/// `setDataForServersInfo` reads `GetAllServerInfo`), which is why this
/// table has no `*_rows` function beside it in this module; see
/// `Session::tidb_servers_info_table_rows`.
/// Go `tableClusterInfoCols`. One row per node in the cluster.
///
/// Rows come from the server-info syncer, like `TIDB_SERVERS_INFO`; see
/// `Session::cluster_info_table_rows`.
const CLUSTER_INFO_COLUMNS: &[InfoColumn] = &[
    InfoColumn {
        name: "TYPE",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "INSTANCE",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "STATUS_ADDRESS",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "VERSION",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "GIT_HASH",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "START_TIME",
        tp: FieldTypeCode::Datetime,
        size: 19,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "UPTIME",
        tp: FieldTypeCode::Varchar,
        size: 32,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "SERVER_ID",
        tp: FieldTypeCode::LongLong,
        size: 21,
        flag: 0,
        deflt: None,
        comment: Some("invalid if the configuration item `enable-global-kill` is set to FALSE"),
    },
];

const TIDB_SERVERS_INFO_COLUMNS: &[InfoColumn] = &[
    InfoColumn {
        name: "DDL_ID",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "IP",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "PORT",
        tp: FieldTypeCode::LongLong,
        size: 21,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "STATUS_PORT",
        tp: FieldTypeCode::LongLong,
        size: 21,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "LEASE",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "VERSION",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "GIT_HASH",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "LABELS",
        tp: FieldTypeCode::Varchar,
        size: 128,
        flag: 0,
        deflt: None,
        comment: None,
    },
];

/// Go `infoschema.tableProcesslistCols`, CAPTURED from `pkg/infoschema/tables.go`
/// (`ID, USER, HOST, DB, COMMAND, TIME, STATE, INFO` are the `SHOW
/// PROCESSLIST` columns; the rest are this table's own extras). Rows come
/// from session/registry state, not the catalog -- see
/// `Session::process_list_table_rows`, which is why this table has no
/// `*_rows` function alongside it in this module.
const PROCESSLIST_COLUMNS: &[InfoColumn] = &[
    InfoColumn {
        name: "ID",
        tp: FieldTypeCode::LongLong,
        size: 21,
        flag: NOT_NULL_FLAG | UNSIGNED_FLAG,
        deflt: Some("0"),
        comment: None,
    },
    InfoColumn {
        name: "USER",
        tp: FieldTypeCode::Varchar,
        size: 16,
        flag: NOT_NULL_FLAG,
        deflt: Some(""),
        comment: None,
    },
    InfoColumn {
        name: "HOST",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: NOT_NULL_FLAG,
        deflt: Some(""),
        comment: None,
    },
    InfoColumn {
        name: "DB",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "COMMAND",
        tp: FieldTypeCode::Varchar,
        size: 16,
        flag: NOT_NULL_FLAG,
        deflt: Some(""),
        comment: None,
    },
    InfoColumn {
        name: "TIME",
        tp: FieldTypeCode::Long,
        size: 7,
        flag: NOT_NULL_FLAG,
        deflt: Some("0"),
        comment: None,
    },
    InfoColumn {
        name: "STATE",
        tp: FieldTypeCode::Varchar,
        size: 7,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "INFO",
        tp: FieldTypeCode::LongBlob,
        size: UNSPECIFIED_LENGTH,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "DIGEST",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: Some(""),
        comment: None,
    },
    InfoColumn {
        name: "MEM",
        tp: FieldTypeCode::LongLong,
        size: 21,
        flag: UNSIGNED_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "MEM_ARBITRATION",
        tp: FieldTypeCode::Double,
        size: 22,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "MEM_WAIT_ARBITRATE_START",
        tp: FieldTypeCode::Varchar,
        size: 32,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "MEM_WAIT_ARBITRATE_BYTES",
        tp: FieldTypeCode::LongLong,
        size: 21,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "DISK",
        tp: FieldTypeCode::LongLong,
        size: 21,
        flag: UNSIGNED_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TxnStart",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: NOT_NULL_FLAG,
        deflt: Some(""),
        comment: None,
    },
    InfoColumn {
        name: "RESOURCE_GROUP",
        tp: FieldTypeCode::Varchar,
        size: RESOURCE_GROUP_NAME_LENGTH,
        flag: NOT_NULL_FLAG,
        deflt: Some(""),
        comment: None,
    },
    InfoColumn {
        name: "SESSION_ALIAS",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: NOT_NULL_FLAG,
        deflt: Some(""),
        comment: None,
    },
    InfoColumn {
        name: "ROWS_AFFECTED",
        tp: FieldTypeCode::LongLong,
        size: 21,
        flag: UNSIGNED_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TIDB_CPU",
        tp: FieldTypeCode::LongLong,
        size: 21,
        flag: NOT_NULL_FLAG,
        deflt: Some("0"),
        comment: None,
    },
    InfoColumn {
        name: "TIKV_CPU",
        tp: FieldTypeCode::LongLong,
        size: 21,
        flag: NOT_NULL_FLAG,
        deflt: Some("0"),
        comment: None,
    },
];

/// Go `infoschema.tableDeadlocksCols`.
const DEADLOCKS_COLUMNS: &[InfoColumn] = &[
    InfoColumn {
        name: "DEADLOCK_ID",
        tp: FieldTypeCode::LongLong,
        size: 21,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "OCCUR_TIME",
        tp: FieldTypeCode::Timestamp,
        size: 26,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "RETRYABLE",
        tp: FieldTypeCode::Tiny,
        size: 1,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TRY_LOCK_TRX_ID",
        tp: FieldTypeCode::LongLong,
        size: 21,
        flag: NOT_NULL_FLAG | UNSIGNED_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "CURRENT_SQL_DIGEST",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "CURRENT_SQL_DIGEST_TEXT",
        tp: FieldTypeCode::Blob,
        size: UNSPECIFIED_LENGTH,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "KEY",
        tp: FieldTypeCode::Blob,
        size: UNSPECIFIED_LENGTH,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "KEY_INFO",
        tp: FieldTypeCode::Blob,
        size: UNSPECIFIED_LENGTH,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TRX_HOLDING_LOCK",
        tp: FieldTypeCode::LongLong,
        size: 21,
        flag: NOT_NULL_FLAG | UNSIGNED_FLAG,
        deflt: None,
        comment: None,
    },
];

/// Go `infoschema.tableUserPrivilegesCols`.
///
/// CAPTURED: unlike its `SCHEMA_PRIVILEGES`/`TABLE_PRIVILEGES`/
/// `COLUMN_PRIVILEGES` siblings below, this one DOES have a retriever
/// (`MySQLPrivilege.UserPrivilegesTable`) and DOES serve rows -- including
/// one per DYNAMIC privilege, whose `IS_GRANTABLE` comes from that
/// privilege's own `with_grant_option` rather than from the account's
/// `GRANT OPTION`. Rows are built session-side (see
/// `Session::user_privileges_table_rows`), not from the catalog.
const USER_PRIVILEGES_COLUMNS: &[InfoColumn] = &[
    InfoColumn {
        name: "GRANTEE",
        tp: FieldTypeCode::Varchar,
        size: 81,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TABLE_CATALOG",
        tp: FieldTypeCode::Varchar,
        size: 512,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "PRIVILEGE_TYPE",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: 0,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "IS_GRANTABLE",
        tp: FieldTypeCode::Varchar,
        size: 3,
        flag: 0,
        deflt: None,
        comment: None,
    },
];

/// Go `infoschema.tableSchemaPrivilegesCols`.
///
/// CAPTURED: this table -- and its `TABLE_PRIVILEGES` / `COLUMN_PRIVILEGES`
/// siblings below -- is DECLARED in `pkg/infoschema/tables.go` but has NO
/// retriever anywhere in `pkg/executor`, so real TiDB serves the header and
/// never a row. Verified against `testkit.CreateMockStore` with grants
/// actually present (`GRANT SELECT, INSERT ON db1.* TO 'u1'@'%'`,
/// `GRANT ALL PRIVILEGES ON db1.* TO 'u2'@'localhost'`, plus table-scope
/// grants): `SELECT COUNT(*)` returns `0`. RE-VERIFIED for
/// `COLUMN_PRIVILEGES` with COLUMN grants present (`GRANT SELECT (a),
/// INSERT (a,b) ON cg.t TO u` and friends): still `0` rows. Populating these
/// from the
/// privilege registry would be a DIVERGENCE from Go, not a completion --
/// so the emptiness is the behavior being transcreated.
const SCHEMA_PRIVILEGES_COLUMNS: &[InfoColumn] = &[
    InfoColumn {
        name: "GRANTEE",
        tp: FieldTypeCode::Varchar,
        size: 81,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TABLE_CATALOG",
        tp: FieldTypeCode::Varchar,
        size: 512,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TABLE_SCHEMA",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "PRIVILEGE_TYPE",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "IS_GRANTABLE",
        tp: FieldTypeCode::Varchar,
        size: 3,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
];

/// Go `infoschema.tableTablePrivilegesCols`. Always empty -- see
/// `SCHEMA_PRIVILEGES_COLUMNS`.
const TABLE_PRIVILEGES_COLUMNS: &[InfoColumn] = &[
    InfoColumn {
        name: "GRANTEE",
        tp: FieldTypeCode::Varchar,
        size: 81,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TABLE_CATALOG",
        tp: FieldTypeCode::Varchar,
        size: 512,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TABLE_SCHEMA",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TABLE_NAME",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "PRIVILEGE_TYPE",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "IS_GRANTABLE",
        tp: FieldTypeCode::Varchar,
        size: 3,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
];

/// Go `infoschema.tableColumnPrivilegesCols`. Always empty -- see
/// `SCHEMA_PRIVILEGES_COLUMNS`.
const COLUMN_PRIVILEGES_COLUMNS: &[InfoColumn] = &[
    InfoColumn {
        name: "GRANTEE",
        tp: FieldTypeCode::Varchar,
        size: 81,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TABLE_CATALOG",
        tp: FieldTypeCode::Varchar,
        size: 512,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TABLE_SCHEMA",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "TABLE_NAME",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "COLUMN_NAME",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "PRIVILEGE_TYPE",
        tp: FieldTypeCode::Varchar,
        size: 64,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
    InfoColumn {
        name: "IS_GRANTABLE",
        tp: FieldTypeCode::Varchar,
        size: 3,
        flag: NOT_NULL_FLAG,
        deflt: None,
        comment: None,
    },
];

/// Every `information_schema` table this tier serves, in the order Go's
/// `tableNameToColumns` map is enumerated after sorting -- which is the order
/// `SHOW TABLES` reports, so the list is kept sorted here rather than sorted
/// at every use.
///
/// This is the SINGLE registry: it answers `table_columns`, it is what
/// [`register_tables`] turns into catalog objects, and adding a table means
/// adding one row.
///
/// DIVERGENCE (documented, measured): Go serves 94 tables in this schema
/// (captured: `use information_schema; show tables` returns 94 names), of
/// which these are ported. `SHOW TABLES` therefore under-reports rather
/// than reporting nothing, and naming an unported one still refuses with
/// 1146 -- the same honest shape `mysql` has.
const SERVED_TABLES: &[(&str, &[InfoColumn])] = &[
    ("CLUSTER_INFO", CLUSTER_INFO_COLUMNS),
    ("COLUMNS", COLUMNS_COLUMNS),
    ("COLUMN_PRIVILEGES", COLUMN_PRIVILEGES_COLUMNS),
    ("CHARACTER_SETS", CHARACTER_SETS_COLUMNS),
    ("COLLATIONS", COLLATIONS_COLUMNS),
    ("COLLATION_CHARACTER_SET_APPLICABILITY", COLLATION_CHARACTER_SET_APPLICABILITY_COLUMNS),
    ("DEADLOCKS", DEADLOCKS_COLUMNS),
    ("KEY_COLUMN_USAGE", KEY_COLUMN_USAGE_COLUMNS),
    ("PROCESSLIST", PROCESSLIST_COLUMNS),
    ("REFERENTIAL_CONSTRAINTS", REFERENTIAL_CONSTRAINTS_COLUMNS),
    ("SCHEMATA", SCHEMATA_COLUMNS),
    ("SCHEMA_PRIVILEGES", SCHEMA_PRIVILEGES_COLUMNS),
    ("STATISTICS", STATISTICS_COLUMNS),
    ("TABLES", TABLES_COLUMNS),
    ("TABLE_CONSTRAINTS", TABLE_CONSTRAINTS_COLUMNS),
    ("TABLE_PRIVILEGES", TABLE_PRIVILEGES_COLUMNS),
    ("TIDB_SERVERS_INFO", TIDB_SERVERS_INFO_COLUMNS),
    ("USER_PRIVILEGES", USER_PRIVILEGES_COLUMNS),
    ("VIEWS", VIEWS_COLUMNS),
];

/// The declared columns of one `information_schema` table, or `None` when the
/// table is not one this tier implements.
fn table_columns(name: &str) -> Option<&'static [InfoColumn]> {
    SERVED_TABLES
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
        .map(|(_, columns)| *columns)
}

/// Registers every served table in `catalog` as a real object in the
/// `information_schema` schema.
///
/// Go builds these with `buildTableMeta` at bootstrap and puts them in the
/// same `infoschema` the user's own tables live in, which is why `SHOW
/// TABLES`, `DESCRIBE` and `SHOW CREATE TABLE` answer for them without any of
/// those statements knowing that the rows are computed. This does the same:
/// the object carries the schema, and the rows still come from
/// [`table_rows`] at query time.
pub fn register_tables(catalog: &mut Catalog) {
    for (name, columns) in SERVED_TABLES {
        let kv_columns = columns
            .iter()
            .enumerate()
            .map(|(offset, column)| KvColumn {
                name: column.name.to_owned(),
                id: i64::try_from(offset).unwrap_or(0),
                field_type: column.field_type(),
                column_info_version: tidb_model::column::CURR_LATEST_COLUMN_INFO_VERSION,
                comment: column.comment.unwrap_or_default().to_owned(),
                default_value: column
                    .deflt
                    .map(|text| ColumnDefault::Value(Datum::Bytes(text.as_bytes().to_vec()))),
                origin_default: column
                    .deflt
                    .map(|text| Datum::Bytes(text.as_bytes().to_vec())),
                generated: None,
            })
            .collect();
        let mut table = KvTable::new(0, kv_columns);
        table.name = (*name).to_owned();
        // The schema exists in every catalog this is called on (see
        // `Catalog::default`), so the only error `register_kv_in` reports
        // cannot happen here.
        let _ = catalog.register_kv_in(INFORMATION_SCHEMA, name, table);
    }
}

/// The `(name, type)` pairs one served table reports, or `None` when the name
/// is not one this tier implements.
#[must_use]
pub fn table_schema(name: &str) -> Option<Vec<(String, FieldType)>> {
    let columns = table_columns(name)?;
    Some(
        columns
            .iter()
            .map(|column| (column.name.to_owned(), column.field_type()))
            .collect(),
    )
}

/// The names of every served table, in `SHOW TABLES` order.
#[must_use]
pub fn served_table_names() -> Vec<&'static str> {
    SERVED_TABLES.iter().map(|(name, _)| *name).collect()
}
