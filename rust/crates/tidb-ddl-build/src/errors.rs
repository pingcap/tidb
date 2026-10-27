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

use std::fmt::Display;

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

/// Go `dbterror.ErrUnsupportedOnCommitPreserve` (8200).
pub fn unsupported_on_commit_preserve() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_UNSUPPORTED_ON_COMMIT_PRESERVE,
        "TiDB doesn't support ON COMMIT PRESERVE ROWS for now",
    )
}

/// The duplicate index name, Go
/// `dbterror.ErrDupKeyName` (1061): "Duplicate key name '%-.192s'".
pub fn duplicate_key_name(name: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_DUP_KEY_NAME,
        format!("Duplicate key name '{name}'"),
    )
}

/// Go `dbterror.ErrWrongTableName` (1103).
pub fn wrong_table_name(name: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_WRONG_TABLE_NAME,
        format!("Incorrect table name '{name}'"),
    )
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

/// Go `dbterror.ErrSequenceUnsupportedTableOption` (4135).
pub fn sequence_unsupported_table_option(option: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_SEQUENCE_UNSUPPORTED_TABLE_OPTION,
        format!("Unsupported sequence table-option {option}"),
    )
}

/// Go `infoschema.ErrForeignKeyCannotOpenParent` (1824).
pub fn foreign_key_cannot_open_parent(table: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_CANNOT_OPEN_PARENT,
        format!("Failed to open the referenced table '{table}'"),
    )
}

/// Go `dbterror.ErrForeignKeyColumnCannotChange` (1832).
pub fn foreign_key_column_cannot_change(column: impl Display, constraint: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_FOREIGN_KEY_COLUMN_CANNOT_CHANGE,
        format!("Cannot change column '{column}': used in a foreign key constraint '{constraint}'"),
    )
}

/// Go `dbterror.ErrForeignKeyColumnCannotChangeChild` (1833).
pub fn foreign_key_column_cannot_change_child(
    column: impl Display,
    constraint: impl Display,
    child_table: impl Display,
) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_FOREIGN_KEY_COLUMN_CANNOT_CHANGE_CHILD,
        format!(
            "Cannot change column '{column}': used in a foreign key constraint \
             '{constraint}' of table '{child_table}'"
        ),
    )
}

/// Go `table.ErrUnknownPartition` (1735).
pub fn unknown_partition(partition: impl Display, table: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_error::catalog::table::ERR_UNKNOWN_PARTITION,
        format!("Unknown partition '{partition}' in table '{table}'"),
    )
}

/// Go `dbterror.ErrPartitionMgmtOnNonpartitioned` (1505).
pub fn partition_mgmt_on_nonpartitioned() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_PARTITION_MGMT_ON_NONPARTITIONED,
        "Partition management on a not partitioned table is not possible".to_owned(),
    )
}

/// Go `dbterror.ErrDropPartitionNonExistent` (1507).
pub fn drop_partition_non_existent() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_DROP_PARTITION_NON_EXISTENT,
        "Error in list of partitions to DROP".to_owned(),
    )
}

/// Go `dbterror.ErrDropLastPartition` (1508).
pub fn drop_last_partition() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_DROP_LAST_PARTITION,
        "Cannot remove all partitions, use DROP TABLE instead".to_owned(),
    )
}

/// Go `dbterror.ErrOnlyOnRangeListPartition.GenWithStackByArgs(op)` (1512).
pub fn only_on_range_list_partition(operation: &str) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_ONLY_ON_RANGE_LIST_PARTITION,
        format!("{operation} PARTITION can only be used on RANGE/LIST partitions"),
    )
}

/// Go `dbterror.ErrCoalesceOnlyOnHashPartition` (1509).
pub fn coalesce_only_on_hash_partition() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_COALESCE_ONLY_ON_HASH_PARTITION,
        "COALESCE PARTITION can only be used on HASH/KEY partitions",
    )
}

/// Go `ast.ErrCoalescePartitionNoPartition` (1515).
pub fn coalesce_partition_no_partition() -> MysqlError {
    MysqlError::terror(
        &tidb_error::catalog::ast::ERR_COALESCE_PARTITION_NO_PARTITION,
        "At least one partition must be coalesced",
    )
}

/// Go `infoschema.ErrPlacementPolicyNotExists` (8239).
pub fn placement_policy_not_exists(name: &str) -> MysqlError {
    MysqlError::terror(
        &tidb_error::catalog::infoschema::ERR_PLACEMENT_POLICY_NOT_EXISTS,
        format!("Unknown placement policy '{name}'"),
    )
}

/// Go `dbterror.ErrOptOnCacheTable.GenWithStackByArgs(op)` (8242).
pub fn opt_on_cache_table(operation: &str) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_OPT_ON_CACHE_TABLE,
        format!("'{operation}' is unsupported on cache tables."),
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

/// Go `infoschema.ErrTableExists` (1050).
pub fn table_exists(name: &str) -> MysqlError {
    MysqlError::terror(
        &tidb_error::catalog::infoschema::ERR_TABLE_EXISTS,
        format!("Table '{name}' already exists"),
    )
}

/// Go `infoschema.ErrTableNotExists` (1146).
pub fn table_not_exists(database: &str, name: &str) -> MysqlError {
    MysqlError::from_terror(
        &tidb_error::catalog::infoschema::ERR_TABLE_NOT_EXISTS
            .generate_with_stack_by_args(&[database.into(), name.into()]),
    )
}

/// Go `dbterror.ErrErrorOnRename` with `ExtractTblInfos`' fixed errno 168:
/// the target schema of a RENAME does not exist.
pub fn error_on_rename_database_missing(from: &str, to: &str, database: &str) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_ERROR_ON_RENAME,
        format!("Error on rename of '{from}' to '{to}' (errno: 168 - Database `{database}` doesn't exist)"),
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

/// Go `dbterror.ErrPartitionNoTemporary` (1562).
pub fn partition_no_temporary() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_PARTITION_NO_TEMPORARY,
        tidb_error::tidb::errname::ErrPartitionNoTemporary.raw,
    )
}

/// Go `dbterror.ErrTempTableNotAllowedWithTTL` (8151).
pub fn temp_table_not_allowed_with_ttl() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_TEMP_TABLE_NOT_ALLOWED_WITH_TTL,
        tidb_error::tidb::errname::ErrTempTableNotAllowedWithTTL.raw,
    )
}

/// Go `ErrBadField` (1054) against the "TTL config" clause.
pub fn unknown_column_in_ttl_config(column: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_BAD_FIELD,
        format!("Unknown column '{column}' in 'TTL config'"),
    )
}

/// Go `dbterror.ErrUnsupportedColumnInTTLConfig` (8148).
pub fn unsupported_column_in_ttl_config(column: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_UNSUPPORTED_COLUMN_IN_TTL_CONFIG,
        format!(
            "Field '{column}' is of a not supported type for TTL config, \
             expect DATETIME, DATE or TIMESTAMP"
        ),
    )
}

/// Go `dbterror.ErrSetTTLOptionForNonTTLTable` (8150).
pub fn set_ttl_option_for_non_ttl_table(option: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_SET_TTL_OPTION_FOR_NON_TTL_TABLE,
        format!("Cannot set {option} on a table without TTL config"),
    )
}

/// Go `dbterror.ErrUnsupportedPrimaryKeyTypeWithTTL` (8153).
pub fn unsupported_primary_key_type_with_ttl() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_UNSUPPORTED_PRIMARY_KEY_TYPE_WITH_TTL,
        tidb_error::tidb::errname::ErrUnsupportedPrimaryKeyTypeWithTTL.raw,
    )
}

/// Go `dbterror.ErrUnsupportedTTLReferencedByFK` (8152).
pub fn ttl_referenced_by_foreign_key() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_UNSUPPORTED_TTL_REFERENCED_BY_FK,
        tidb_error::tidb::errname::ErrUnsupportedTTLReferencedByFK.raw,
    )
}

/// Go `infoschema.ErrForeignKeyNotMatch`: "Incorrect foreign key definition
/// for '%-.192s': %s".
pub fn wrong_fk_def(name: impl Display, reason: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_NOT_MATCH,
        format!("Incorrect foreign key definition for '{name}': {reason}"),
    )
}

/// Go `infoschema.ErrForeignKeyColumnNotNull`.
pub fn foreign_key_column_not_null(column: impl Display, constraint: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_COLUMN_NOT_NULL,
        format!(
            "Column '{column}' cannot be NOT NULL: needed in a foreign key constraint '{constraint}' SET NULL"
        ),
    )
}

/// Go `infoschema.ErrForeignKeyNoIndexInParent`.
pub fn foreign_key_no_index_in_parent(constraint: impl Display, table: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_NO_INDEX_IN_PARENT,
        format!(
            "Failed to add the foreign key constraint. Missing index for constraint '{constraint}' in the referenced table '{table}'"
        ),
    )
}

/// Go `infoschema.ErrForeignKeyOnPartitioned`.
pub fn foreign_key_on_partitioned() -> MysqlError {
    MysqlError::terror(
        &tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_ON_PARTITIONED,
        "Foreign key clause is not yet supported in conjunction with partitioning".to_owned(),
    )
}

/// Go `infoschema.ErrForeignKeyCannotUseVirtualColumn` (`[schema:3733]`).
pub fn foreign_key_uses_virtual_column(foreign_key: impl Display, column: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_CANNOT_USE_VIRTUAL_COLUMN,
        format!("Foreign key '{foreign_key}' uses virtual column '{column}' which is not supported."),
    )
}

/// Go `dbterror.ErrWrongFKOptionForGeneratedColumn` (`[ddl:3104]`).
pub fn wrong_fk_option_for_generated_column(clause: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_WRONG_FK_OPTION_FOR_GENERATED_COLUMN,
        format!("Cannot define foreign key with {clause} clause on a generated column."),
    )
}

/// Go `dbterror.ErrFKIncompatibleColumns` (`[ddl:3780]`).
pub fn fk_incompatible_columns(
    referencing: impl Display,
    referenced: impl Display,
    constraint: impl Display,
) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_FK_INCOMPATIBLE_COLUMNS,
        format!(
            "Referencing column '{referencing}' and referenced column '{referenced}' in \
             foreign key constraint '{constraint}' are incompatible."
        ),
    )
}

/// Go `infoschema.ErrForeignKeyNoColumnInParent`.
pub fn foreign_key_referenced_column_missing(
    column: impl Display,
    constraint: impl Display,
    table: impl Display,
) -> MysqlError {
    MysqlError::terror(
        &tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_NO_COLUMN_IN_PARENT,
        format!(
            "Failed to add the foreign key constraint. Missing column '{column}' for constraint '{constraint}' in the referenced table '{table}'"
        ),
    )
}

/// Go `dbterror.ErrKeyColumnDoesNotExits`.
pub fn foreign_key_child_column_missing(column: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_KEY_COLUMN_DOES_NOT_EXITS,
        format!("Key column '{column}' doesn't exist in table"),
    )
}

/// Go `dbterror.ErrUnsupportedShardRowIDBits` (8200).
pub fn unsupported_shard_row_id_bits() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_UNSUPPORTED_SHARD_ROW_ID_BITS,
        "Unsupported shard_row_id_bits for table with primary key as row id".to_string(),
    )
}

/// Go `infoschema.ErrColumnExists`: "Duplicate column name '%-.192s'".
pub fn duplicate_column_name(name: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_error::catalog::infoschema::ERR_COLUMN_EXISTS,
        format!("Duplicate column name '{name}'"),
    )
}

/// Go `dbterror.ErrUnsupportedModifyColumn` (8200): "Unsupported modify
/// column: %s".
pub fn unsupported_modify_column(reason: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_UNSUPPORTED_MODIFY_COLUMN,
        format!("Unsupported modify column: {reason}"),
    )
}

/// Go `dbterror.ErrUnsupportedModifyColumn` over "change from original type
/// %v to %v is currently unsupported yet".
pub fn unsupported_modify_column_type(from: impl Display, to: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_UNSUPPORTED_MODIFY_COLUMN,
        format!(
            "Unsupported modify column: change from original type {from} to {to} is \
             currently unsupported yet"
        ),
    )
}

/// Go `plannererrors.ErrWindowInvalidWindowFuncUse` (the trailing quote is in
/// Go's own message text).
pub fn window_invalid_window_func_use(name: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_error::plannererrors::ERR_WINDOW_INVALID_WINDOW_FUNC_USE,
        format!("You cannot use the window function '{name}' in this context.'"),
    )
}

/// Go `plannererrors.ErrNotSupportedYet`.
pub fn not_supported_yet(feature: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_error::plannererrors::ERR_NOT_SUPPORTED_YET,
        format!("This version of TiDB doesn't yet support '{feature}'"),
    )
}

/// Go `plannererrors.ErrInvalidGroupFuncUse`.
pub fn invalid_group_func_use() -> MysqlError {
    MysqlError::terror(
        &tidb_error::plannererrors::ERR_INVALID_GROUP_FUNC_USE,
        "Invalid use of group function".to_owned(),
    )
}

/// Go `dbterror.ErrBlobKeyWithoutLength`.
pub fn blob_key_without_length(column: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_BLOB_KEY_WITHOUT_LENGTH,
        format!("BLOB/TEXT column '{column}' used in key specification without a key length"),
    )
}

/// Go `dbterror.ErrIncorrectPrefixKey`.
pub fn incorrect_prefix_key() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_INCORRECT_PREFIX_KEY,
        "Incorrect prefix key; the used key part isn't a string, the used length is longer \
         than the key part, or the storage engine doesn't support unique prefix keys"
            .to_owned(),
    )
}

/// Go `dbterror.ErrKeyPart0`.
pub fn key_part0(column: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_KEY_PART0,
        format!("Key part '{column}' length cannot be 0"),
    )
}

/// Go `dbterror.ErrWrongKeyColumn`.
pub fn wrong_key_column(column: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_WRONG_KEY_COLUMN,
        format!("The used storage engine can't index column '{column}'"),
    )
}

/// Go `dbterror.ErrTooLongKey`.
pub fn too_long_key(length: i64, max: i64) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_TOO_LONG_KEY,
        format!("Specified key was too long ({length} bytes); max key length is {max} bytes"),
    )
}

/// Go `dbterror.ErrJSONUsedAsKey`.
pub fn json_used_in_key(column: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_JSON_USED_AS_KEY,
        format!("JSON column '{column}' cannot be used in key specification."),
    )
}

/// Go `dbterror.ErrBlobCantHaveDefault`.
pub fn blob_cant_have_default(column: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_BLOB_CANT_HAVE_DEFAULT,
        format!("BLOB/TEXT/JSON column '{column}' can't have a default value"),
    )
}

/// Go `dbterror.ErrGeneratedColumnFunctionIsNotAllowed`.
pub fn generated_column_function_not_allowed(column: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_GENERATED_COLUMN_FUNCTION_IS_NOT_ALLOWED,
        format!("Expression of generated column '{column}' contains a disallowed function."),
    )
}

/// Go `dbterror.ErrWrongExprInPartitionFunc`.
pub fn partition_wrong_expr_in_func() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_WRONG_EXPR_IN_PARTITION_FUNC,
        "Constant, random or timezone-dependent expressions in (sub)partitioning function are \
         not allowed"
            .to_owned(),
    )
}

/// Go `dbterror.ErrPartitionFuncNotAllowed`.
pub fn partition_func_wrong_type() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_PARTITION_FUNC_NOT_ALLOWED,
        "The PARTITION function returns the wrong type".to_owned(),
    )
}

/// Go `dbterror.ErrSameNamePartitionField`.
pub fn partition_duplicate_field(field: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_SAME_NAME_PARTITION_FIELD,
        format!("Duplicate partition field name '{field}'"),
    )
}

/// Go `dbterror.ErrTooManyPartitions`.
pub fn partition_too_many() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_TOO_MANY_PARTITIONS,
        "Too many partitions (including subpartitions) were defined".to_owned(),
    )
}

/// Go `ast.ErrSubpartition`.
pub fn partition_subpartition() -> MysqlError {
    MysqlError::terror(
        &tidb_error::catalog::ast::ERR_SUBPARTITION,
        "It is only possible to mix RANGE/LIST partitioning with HASH/KEY partitioning for \
         subpartitioning"
            .to_owned(),
    )
}

/// Go `dbterror.ErrUniqueKeyNeedAllFieldsInPf`.
pub fn partition_unique_key_need_all_fields(kind: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF,
        format!("A {kind} must include all columns in the table's partitioning function"),
    )
}

/// Go `dbterror.ErrTooLongTablePartitionComment`, raised only under STRICT
/// mode.
pub fn partition_comment_too_long(name: impl Display, limit: usize) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_TOO_LONG_TABLE_PARTITION_COMMENT,
        format!("Comment for table partition '{name}' is too long (max = {limit})"),
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

/// Go `ast.ErrNoParts`.
pub fn partition_no_parts(what: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_error::catalog::ast::ERR_NO_PARTS,
        format!("Number of {what} = 0 is not an allowed value"),
    )
}

/// Go `dbterror.ErrSameNamePartition`.
pub fn partition_same_name(name: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_SAME_NAME_PARTITION,
        format!("Duplicate partition name {name}"),
    )
}

/// Go `dbterror.ErrNullInValuesLessThan`.
pub fn partition_null_in_values_less_than() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_NULL_IN_VALUES_LESS_THAN,
        "Not allowed to use NULL value in VALUES LESS THAN".to_owned(),
    )
}

/// Go `dbterror.ErrGlobalIndexNotExplicitlySet`.
pub fn partition_global_index_needed(index: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_GLOBAL_INDEX_NOT_EXPLICITLY_SET,
        format!(
            "Global Index is needed for index '{index}', since the unique index is not \
             including all partitioning columns, and GLOBAL is not given as IndexOption"
        ),
    )
}

/// Go `dbterror.ErrPartitionMaxvalue`.
pub fn partition_max_value_not_last() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_PARTITION_MAXVALUE,
        "MAXVALUE can only be used in last partition definition".to_owned(),
    )
}

/// Go `dbterror.ErrRangeNotIncreasing`.
pub fn partition_range_not_increasing() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_RANGE_NOT_INCREASING,
        "VALUES LESS THAN value must be strictly increasing for each partition".to_owned(),
    )
}

/// Go `dbterror.ErrWrongKeyColumnFunctionalIndex`.
pub fn wrong_key_column_functional_index(expr: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_WRONG_KEY_COLUMN_FUNCTIONAL_INDEX,
        format!("The used storage engine cannot index the expression '{expr}'"),
    )
}

/// Go `dbterror.ErrFunctionalIndexOnJSONOrGeometryFunction`.
pub fn functional_index_on_json() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_FUNCTIONAL_INDEX_ON_JSON_OR_GEOMETRY_FUNCTION,
        "Cannot create an expression index on a function that returns a JSON or GEOMETRY value"
            .to_owned(),
    )
}

/// Go `dbterror.ErrFunctionalIndexOnBlob`.
pub fn functional_index_on_blob() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_FUNCTIONAL_INDEX_ON_BLOB,
        "Cannot create an expression index on an expression that returns a BLOB or TEXT. \
         Please consider using CAST"
            .to_owned(),
    )
}

/// Go `dbterror.ErrFunctionalIndexOnField`.
pub fn functional_index_on_field() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_FUNCTIONAL_INDEX_ON_FIELD,
        "Expression index on a column is not supported. Consider using a regular index instead"
            .to_owned(),
    )
}

/// Go `dbterror.ErrFunctionalIndexFunctionIsNotAllowed`.
pub fn functional_index_function_not_allowed(index: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_FUNCTIONAL_INDEX_FUNCTION_IS_NOT_ALLOWED,
        format!("Expression of expression index '{index}' contains a disallowed function"),
    )
}

/// Go `dbterror.ErrFunctionalIndexRowValueIsNotAllowed`.
pub fn functional_index_row_value(index: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_FUNCTIONAL_INDEX_ROW_VALUE_IS_NOT_ALLOWED,
        format!("Expression of expression index '{index}' cannot refer to a row value"),
    )
}

/// Go `dbterror.ErrGeneratedColumnRowValueIsNotAllowed`.
pub fn generated_column_row_value(column: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_GENERATED_COLUMN_ROW_VALUE_IS_NOT_ALLOWED,
        format!("Expression of generated column '{column}' cannot refer to a row value"),
    )
}

/// Go `dbterror.ErrExpressionIndexCanNotRefer`.
pub fn expression_index_can_not_refer(index: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_EXPRESSION_INDEX_CAN_NOT_REFER,
        format!("Expression index '{index}' cannot refer to an auto-increment column"),
    )
}

/// The 8200 refusal of an expression index over an unsafe function while
/// `allow-expression-index` is off.
pub fn unsafe_function_in_expression_index() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_UNSUPPORTED_EXPRESSION_INDEX,
        "Unsupported creating expression index containing unsafe functions without \
         allow-expression-index in config"
            .to_owned(),
    )
}

/// Go `expression.ErrIncorrectParameterCount`.
pub fn wrong_param_count_to_native_fct(name: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_error::catalog::expression::ERR_INCORRECT_PARAMETER_COUNT,
        format!("Incorrect parameter count in the call to native function '{name}'"),
    )
}

/// Go `dbterror.ErrTooLongIdent`.
pub fn too_long_ident(ident: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_TOO_LONG_IDENT,
        format!("Identifier name '{ident}' is too long"),
    )
}

/// Go `dbterror.ErrAddColumnWithSequenceAsDefault`.
pub fn add_column_sequence_default(column: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_ADD_COLUMN_WITH_SEQUENCE_AS_DEFAULT,
        format!("Unsupported using sequence as default value in add column '{column}'"),
    )
}

/// Go `dbterror.ErrBinlogUnsafeSystemFunction`.
pub fn binlog_unsafe_system_function() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_BINLOG_UNSAFE_SYSTEM_FUNCTION,
        "Statement is unsafe because it uses a system function that may return a different value on the slave".to_owned(),
    )
}

/// Go `dbterror.ErrPrimaryCantHaveNull`.
pub fn primary_cant_have_null() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_PRIMARY_CANT_HAVE_NULL,
        "All parts of a PRIMARY KEY must be NOT NULL; if you need NULL in a key, \
         use UNIQUE instead"
            .to_owned(),
    )
}

/// Go `types.overflow`: "constant %v overflows %s".
pub fn constant_overflows(value: impl Display, type_name: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_error::catalog::types::ERR_OVERFLOW,
        format!("constant {value} overflows {type_name}"),
    )
}

/// Go `autoid.ErrAutoincReadFailed` (1467).
pub fn autoinc_read_failed() -> MysqlError {
    MysqlError::terror(
        &tidb_error::catalog::autoid::ERR_AUTOINC_READ_FAILED,
        "Failed to read auto-increment value from storage engine".to_owned(),
    )
}

/// The AUTO_INCREMENT counter's home could not be read or written: Go
/// surfaces the underlying storage failure, which is the carried text.
pub fn auto_id_unavailable(detail: impl Into<String>) -> MysqlError {
    MysqlError::unknown(detail)
}

/// Go `dbterror.ErrInvalidAutoRandom` (8216).
pub fn invalid_auto_random(reason: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_INVALID_AUTO_RANDOM,
        format!("Invalid auto random: {reason}"),
    )
}

