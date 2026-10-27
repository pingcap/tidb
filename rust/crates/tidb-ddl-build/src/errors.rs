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

//! The errors this package raises, each the [`MysqlError`] Go's
//! `ErrX.GenWithStackByArgs(...)` renders: the raise site names the Go
//! definition (class, code, SQLSTATE) and the message it formats.
//!
//! Go's `pkg/ddl` returns these as plain `error` values; nothing between the
//! raise and `terror.ToSQLError` at the wire changes them. One function per
//! error keeps one spelling of each message for every raise site.

use tidb_error::mysql::MysqlError;
use tidb_error::terror::TerrorError;
use tidb_hack::GoToLower;

pub use tidb_table::partition_errors::{
    parse_error, partition_column_list, partition_column_value_wrong_type,
    partition_const_domain, partition_duplicate_list_value, partition_field_not_found,
    partition_field_type_not_allowed, partition_function_not_allowed, partition_metadata_unknown,
    partition_values_not_int, partition_wrong_values, partitions_must_be_defined, unsupported,
};
pub use tidb_table::column_default::{
    default_function_not_allowed, invalid_default, unknown_column_in_clause,
    unsupported_sequence_default_type,
};

/// Go `ErrOptOnTemporaryTable.GenWithStackByArgs(op)` over
/// `` `%s` is unsupported on temporary tables. ``. The DDL executor raises
/// `dbterror`'s definition for its own operations; the planner's
/// `preprocess` raises `plannererrors`'.
pub fn opt_on_temporary_table(operation: &str) -> MysqlError {
    let proto: &TerrorError = match operation {
        "placement" | "pre split regions" | "set TiFlash replica" | "masking policy" => {
            &tidb_util::dbterror::ERR_OPT_ON_TEMPORARY_TABLE
        }
        _ => &tidb_error::plannererrors::ERR_OPT_ON_TEMPORARY_TABLE,
    };
    MysqlError::terror(proto, format!("`{operation}` is unsupported on temporary tables."))
}

/// Go `dbterror.ErrSequenceInvalidData` (4136).
/// Go names the schema and sequence lower-cased (`ident.Schema.L`,
/// `ident.Name.L`) -- captured as `'test.nosuch'`.
pub fn sequence_values_conflicting(database: &str, name: &str) -> MysqlError {
    MysqlError::from_terror(
        &tidb_util::dbterror::ERR_SEQUENCE_INVALID_DATA.generate_with_stack_by_args(&[
            database.go_to_lower().into(),
            name.go_to_lower().into(),
        ]),
    )
}

/// Go `infoschema.ErrWrongObject` (1347): `name` is not an `expected`.
pub fn wrong_object(database: &str, name: &str, expected: &str) -> MysqlError {
    MysqlError::from_terror(
        &tidb_error::catalog::infoschema::ERR_WRONG_OBJECT.generate_with_stack_by_args(&[
            database.into(),
            name.into(),
            expected.into(),
        ]),
    )
}

/// Go `infoschema.ErrTableNotExists` (1146).
pub fn table_not_exists(database: &str, name: &str) -> MysqlError {
    MysqlError::from_terror(
        &tidb_error::catalog::infoschema::ERR_TABLE_NOT_EXISTS
            .generate_with_stack_by_args(&[database.into(), name.into()]),
    )
}

/// Go `infoschema.ErrTableLocked` (8020): another session's lock of
/// `lock_type`, held by `holder`.
pub fn table_locked(table: &str, lock_type: tidb_ast::TableLockType, holder: &str) -> MysqlError {
    MysqlError::terror(
        &tidb_error::catalog::infoschema::ERR_TABLE_LOCKED,
        format!("Table '{table}' was locked in {} by {holder}", lock_type.sql()),
    )
}

/// Go's plain `errors.New` for partition metadata with neither an
/// expression nor a column list.
pub fn partition_metadata_incomplete() -> MysqlError {
    MysqlError::unknown(
        "Table partition metadata not correct, neither partition expression or list of \
         partition columns"
            .to_owned(),
    )
}

/// The AUTO_INCREMENT counter's home could not be read or written: Go
/// surfaces the underlying storage failure, which is the carried text.
pub fn auto_id_unavailable(detail: impl Into<String>) -> MysqlError {
    MysqlError::unknown(detail)
}
