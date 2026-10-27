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

//! The errors Go's partition code raises on both of its paths: the DDL's
//! validation of a written `PARTITION BY` (`pkg/ddl/partition.go`) and the
//! table layer's rebuild of stored partition metadata
//! (`pkg/table/tables/partition.go`).

use std::fmt::Display;

use tidb_error::mysql::MysqlError;

/// A refusal of what this tier does not implement: Go's catch-all 1105 with
/// the carried text as the whole message.
pub fn unsupported(reason: impl Into<String>) -> MysqlError {
    MysqlError::unknown(reason)
}

/// Go `util.SyntaxError`: `ErrParse.GenWithStackByArgs(SyntaxErrorPrefix,
/// err.Error())` over ErrParse's `"%s %s"`.
pub fn parse_error(message: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_error::catalog::parser::ERR_PARSE,
        format!(
            "You have an error in your SQL syntax; check the manual that corresponds \
             to your TiDB version for the right syntax to use {message}"
        ),
    )
}

/// Go `dbterror.ErrWrongTypeColumnValue`.
pub fn partition_column_value_wrong_type() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_WRONG_TYPE_COLUMN_VALUE,
        "Partition column values of incorrect type".to_owned(),
    )
}

/// Go `ast.ErrPartitionColumnList`.
pub fn partition_column_list() -> MysqlError {
    MysqlError::terror(
        &tidb_error::catalog::ast::ERR_PARTITION_COLUMN_LIST,
        "Inconsistency in usage of column lists for partitioning".to_owned(),
    )
}

/// Go `dbterror.ErrPartitionConstDomain`.
pub fn partition_const_domain() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_PARTITION_CONST_DOMAIN,
        "Partition constant is out of partition function domain".to_owned(),
    )
}

/// Go `table.ErrUnknownPartition`, raised bare: the placeholders are the
/// message.
pub fn partition_metadata_unknown() -> MysqlError {
    MysqlError::terror(
        &tidb_error::catalog::table::ERR_UNKNOWN_PARTITION,
        "Unknown partition '%-.64s' in table '%-.64s'".to_owned(),
    )
}

/// Go `dbterror.ErrPartitionFunctionIsNotAllowed`.
pub fn partition_function_not_allowed() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED,
        "This partition function is not allowed".to_owned(),
    )
}

/// Go `dbterror.ErrNotAllowedTypeInPartition`.
pub fn partition_field_type_not_allowed(column: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_NOT_ALLOWED_TYPE_IN_PARTITION,
        format!("Field '{column}' is of a not allowed type for this type of partitioning"),
    )
}

/// Go `dbterror.ErrFieldNotFoundPart`.
pub fn partition_field_not_found() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_FIELD_NOT_FOUND_PART,
        "Field in list of fields for partition function not found in table".to_owned(),
    )
}

/// Go `ast.ErrPartitionWrongValues`.
pub fn partition_wrong_values(method: impl Display, clause: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_error::catalog::ast::ERR_PARTITION_WRONG_VALUES,
        format!("Only {method} PARTITIONING can use {clause} in partition definition"),
    )
}

/// Go `ast.ErrPartitionsMustBeDefined`.
pub fn partitions_must_be_defined(method: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_error::catalog::ast::ERR_PARTITIONS_MUST_BE_DEFINED,
        format!("For {method} partitions each partition must be defined"),
    )
}

/// Go `dbterror.ErrMultipleDefConstInListPart`.
pub fn partition_duplicate_list_value() -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_MULTIPLE_DEF_CONST_IN_LIST_PART,
        "Multiple definition of same constant in list partitioning".to_owned(),
    )
}

/// Go `dbterror.ErrValuesIsNotIntType`.
pub fn partition_values_not_int(partition: impl Display) -> MysqlError {
    MysqlError::terror(
        &tidb_util::dbterror::ERR_VALUES_IS_NOT_INT_TYPE,
        format!("VALUES value for partition '{partition}' must have type INT"),
    )
}
