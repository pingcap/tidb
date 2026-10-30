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

//! Error prototypes defined by the Go package(s) named per entry.
//!
//! GENERATED mechanically from the Go sources by scanning every
//! package-level `Class*.NewStd` / `NewStdErr` definition; the (class,
//! code) set of every module is cross-checked against the real Go
//! registry (`terror.ErrClassToMySQLCodes`) by
//! `tests/catalog_source.rs`. Do not hand-edit entries.

use std::sync::LazyLock;

use tidb_dbterror::catalog_support::{std_err, Class};
use tidb_terror::TerrorError;
use tidb_errno::errcode;

/// Source `infoschema.ErrDatabaseExists` (`pkg/infoschema/error.go:24`).
pub static ERR_DATABASE_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrDBCreateExists));
/// Source `infoschema.ErrDatabaseDropExists` (`pkg/infoschema/error.go:26`).
pub static ERR_DATABASE_DROP_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrDBDropExists));
/// Source `infoschema.ErrAccessDenied` (`pkg/infoschema/error.go:28`).
pub static ERR_ACCESS_DENIED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrAccessDenied));
/// Source `infoschema.ErrDatabaseNotExists` (`pkg/infoschema/error.go:30`).
pub static ERR_DATABASE_NOT_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrBadDB));
/// Source `infoschema.ErrPlacementPolicyExists` (`pkg/infoschema/error.go:32`).
pub static ERR_PLACEMENT_POLICY_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrPlacementPolicyExists));
/// Source `infoschema.ErrPlacementPolicyNotExists` (`pkg/infoschema/error.go:34`).
pub static ERR_PLACEMENT_POLICY_NOT_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrPlacementPolicyNotExists));
/// Source `infoschema.ErrResourceGroupExists` (`pkg/infoschema/error.go:36`).
pub static ERR_RESOURCE_GROUP_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrResourceGroupExists));
/// Source `infoschema.ErrResourceGroupNotExists` (`pkg/infoschema/error.go:38`).
pub static ERR_RESOURCE_GROUP_NOT_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrResourceGroupNotExists));
/// Source `infoschema.ErrResourceGroupInvalidBackgroundTaskName` (`pkg/infoschema/error.go:40`).
pub static ERR_RESOURCE_GROUP_INVALID_BACKGROUND_TASK_NAME: LazyLock<TerrorError> =
    LazyLock::new(|| {
        std_err(
            Class::Executor,
            errcode::ErrResourceGroupInvalidBackgroundTaskName,
        )
    });
/// Source `infoschema.ErrResourceGroupInvalidForRole` (`pkg/infoschema/error.go:42`).
pub static ERR_RESOURCE_GROUP_INVALID_FOR_ROLE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrResourceGroupInvalidForRole));
/// Source `infoschema.ErrReservedSyntax` (`pkg/infoschema/error.go:44`).
pub static ERR_RESERVED_SYNTAX: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrReservedSyntax));
/// Source `infoschema.ErrTableExists` (`pkg/infoschema/error.go:46`).
pub static ERR_TABLE_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrTableExists));
/// Source `infoschema.ErrTableDropExists` (`pkg/infoschema/error.go:48`).
pub static ERR_TABLE_DROP_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrBadTable));
/// Source `infoschema.ErrSequenceDropExists` (`pkg/infoschema/error.go:50`).
pub static ERR_SEQUENCE_DROP_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrUnknownSequence));
/// Source `infoschema.ErrColumnNotExists` (`pkg/infoschema/error.go:52`).
pub static ERR_COLUMN_NOT_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrBadField));
/// Source `infoschema.ErrColumnExists` (`pkg/infoschema/error.go:54`).
pub static ERR_COLUMN_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrDupFieldName));
/// Source `infoschema.ErrKeyNameDuplicate` (`pkg/infoschema/error.go:56`).
pub static ERR_KEY_NAME_DUPLICATE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrDupKeyName));
/// Source `infoschema.ErrNonuniqTable` (`pkg/infoschema/error.go:58`).
pub static ERR_NONUNIQ_TABLE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrNonuniqTable));
/// Source `infoschema.ErrMultiplePriKey` (`pkg/infoschema/error.go:60`).
pub static ERR_MULTIPLE_PRI_KEY: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrMultiplePriKey));
/// Source `infoschema.ErrTooManyKeyParts` (`pkg/infoschema/error.go:62`).
pub static ERR_TOO_MANY_KEY_PARTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrTooManyKeyParts));
/// Source `infoschema.ErrForeignKeyNotExists` (`pkg/infoschema/error.go:64`).
pub static ERR_FOREIGN_KEY_NOT_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrCantDropFieldOrKey));
/// Source `infoschema.ErrTableNotLockedForWrite` (`pkg/infoschema/error.go:66`).
pub static ERR_TABLE_NOT_LOCKED_FOR_WRITE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrTableNotLockedForWrite));
/// Source `infoschema.ErrTableNotLocked` (`pkg/infoschema/error.go:68`).
pub static ERR_TABLE_NOT_LOCKED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrTableNotLocked));
/// Source `infoschema.ErrTableNotExists` (`pkg/infoschema/error.go:70`).
pub static ERR_TABLE_NOT_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrNoSuchTable));
/// Source `infoschema.ErrKeyNotExists` (`pkg/infoschema/error.go:72`).
pub static ERR_KEY_NOT_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrKeyDoesNotExist));
/// Source `infoschema.ErrCannotAddForeign` (`pkg/infoschema/error.go:74`).
pub static ERR_CANNOT_ADD_FOREIGN: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrCannotAddForeign));
/// Source `infoschema.ErrForeignKeyOnPartitioned` (`pkg/infoschema/error.go:76`).
pub static ERR_FOREIGN_KEY_ON_PARTITIONED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrForeignKeyOnPartitioned));
/// Source `infoschema.ErrForeignKeyNotMatch` (`pkg/infoschema/error.go:78`).
pub static ERR_FOREIGN_KEY_NOT_MATCH: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrWrongFkDef));
/// Source `infoschema.ErrIndexExists` (`pkg/infoschema/error.go:80`).
pub static ERR_INDEX_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrDupIndex));
/// Source `infoschema.ErrUserDropExists` (`pkg/infoschema/error.go:82`).
pub static ERR_USER_DROP_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrBadUser));
/// Source `infoschema.ErrUserAlreadyExists` (`pkg/infoschema/error.go:84`).
pub static ERR_USER_ALREADY_EXISTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrUserAlreadyExists));
/// Source `infoschema.ErrTableLocked` (`pkg/infoschema/error.go:86`).
pub static ERR_TABLE_LOCKED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrTableLocked));
/// Source `infoschema.ErrWrongObject` (`pkg/infoschema/error.go:88`).
pub static ERR_WRONG_OBJECT: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrWrongObject));
/// Source `infoschema.ErrAdminCheckTable` (`pkg/infoschema/error.go:90`).
pub static ERR_ADMIN_CHECK_TABLE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrAdminCheckTable));
/// Source `infoschema.ErrEmptyDatabase` (`pkg/infoschema/error.go:92`).
pub static ERR_EMPTY_DATABASE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrBadDB));
/// Source `infoschema.ErrForbidSchemaChange` (`pkg/infoschema/error.go:94`).
pub static ERR_FORBID_SCHEMA_CHANGE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrForbidSchemaChange));
/// Source `infoschema.ErrTableWithoutPrimaryKey` (`pkg/infoschema/error.go:96`).
pub static ERR_TABLE_WITHOUT_PRIMARY_KEY: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrTableWithoutPrimaryKey));
/// Source `infoschema.ErrForeignKeyCannotUseVirtualColumn` (`pkg/infoschema/error.go:98`).
pub static ERR_FOREIGN_KEY_CANNOT_USE_VIRTUAL_COLUMN: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrForeignKeyCannotUseVirtualColumn));
/// Source `infoschema.ErrForeignKeyCannotOpenParent` (`pkg/infoschema/error.go:100`).
pub static ERR_FOREIGN_KEY_CANNOT_OPEN_PARENT: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrForeignKeyCannotOpenParent));
/// Source `infoschema.ErrForeignKeyNoColumnInParent` (`pkg/infoschema/error.go:102`).
pub static ERR_FOREIGN_KEY_NO_COLUMN_IN_PARENT: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrForeignKeyNoColumnInParent));
/// Source `infoschema.ErrForeignKeyNoIndexInParent` (`pkg/infoschema/error.go:104`).
pub static ERR_FOREIGN_KEY_NO_INDEX_IN_PARENT: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrForeignKeyNoIndexInParent));
/// Source `infoschema.ErrForeignKeyColumnNotNull` (`pkg/infoschema/error.go:106`).
pub static ERR_FOREIGN_KEY_COLUMN_NOT_NULL: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrForeignKeyColumnNotNull));
/// Source `infoschema.ErrResourceGroupSupportDisabled` (`pkg/infoschema/error.go:108`).
pub static ERR_RESOURCE_GROUP_SUPPORT_DISABLED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrResourceGroupSupportDisabled));
/// Source `infoschema.ErrCheckConstraintDupName` (`pkg/infoschema/error.go:110`).
pub static ERR_CHECK_CONSTRAINT_DUP_NAME: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrCheckConstraintDupName));
/// Source `infoschema.ErrProtectedTableMode` (`pkg/infoschema/error.go:112`).
pub static ERR_PROTECTED_TABLE_MODE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrProtectedTableMode));
/// Source `infoschema.ErrInvalidTableModeSet` (`pkg/infoschema/error.go:114`).
pub static ERR_INVALID_TABLE_MODE_SET: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Schema, errcode::ErrInvalidTableModeSet));

#[cfg(test)]
mod go_class_code_tests {
    use tidb_errno::errcode;
    use tidb_terror::TerrorClass;

    /// Each prototype carries the class and code of the Go definition it
    /// transcribes (`Class.NewStd(code)`), so `Error()` prints Go's
    /// `[class:code]`.
    #[test]
    fn prototypes_carry_their_go_class_and_code() {
        assert_eq!(
            super::ERR_DATABASE_EXISTS.class(),
            TerrorClass::Schema,
            "infoschema::ERR_DATABASE_EXISTS"
        );
        assert_eq!(
            super::ERR_DATABASE_EXISTS.code().value(),
            errcode::ErrDBCreateExists as isize,
            "infoschema::ERR_DATABASE_EXISTS"
        );
        assert_eq!(
            super::ERR_DATABASE_DROP_EXISTS.class(),
            TerrorClass::Schema,
            "infoschema::ERR_DATABASE_DROP_EXISTS"
        );
        assert_eq!(
            super::ERR_DATABASE_DROP_EXISTS.code().value(),
            errcode::ErrDBDropExists as isize,
            "infoschema::ERR_DATABASE_DROP_EXISTS"
        );
        assert_eq!(
            super::ERR_ACCESS_DENIED.class(),
            TerrorClass::Schema,
            "infoschema::ERR_ACCESS_DENIED"
        );
        assert_eq!(
            super::ERR_ACCESS_DENIED.code().value(),
            errcode::ErrAccessDenied as isize,
            "infoschema::ERR_ACCESS_DENIED"
        );
        assert_eq!(
            super::ERR_DATABASE_NOT_EXISTS.class(),
            TerrorClass::Schema,
            "infoschema::ERR_DATABASE_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_DATABASE_NOT_EXISTS.code().value(),
            errcode::ErrBadDB as isize,
            "infoschema::ERR_DATABASE_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_PLACEMENT_POLICY_EXISTS.class(),
            TerrorClass::Schema,
            "infoschema::ERR_PLACEMENT_POLICY_EXISTS"
        );
        assert_eq!(
            super::ERR_PLACEMENT_POLICY_EXISTS
                .code()
                .value(),
            errcode::ErrPlacementPolicyExists as isize,
            "infoschema::ERR_PLACEMENT_POLICY_EXISTS"
        );
        assert_eq!(
            super::ERR_PLACEMENT_POLICY_NOT_EXISTS.class(),
            TerrorClass::Schema,
            "infoschema::ERR_PLACEMENT_POLICY_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_PLACEMENT_POLICY_NOT_EXISTS
                .code()
                .value(),
            errcode::ErrPlacementPolicyNotExists as isize,
            "infoschema::ERR_PLACEMENT_POLICY_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_RESOURCE_GROUP_EXISTS.class(),
            TerrorClass::Schema,
            "infoschema::ERR_RESOURCE_GROUP_EXISTS"
        );
        assert_eq!(
            super::ERR_RESOURCE_GROUP_EXISTS
                .code()
                .value(),
            errcode::ErrResourceGroupExists as isize,
            "infoschema::ERR_RESOURCE_GROUP_EXISTS"
        );
        assert_eq!(
            super::ERR_RESOURCE_GROUP_NOT_EXISTS.class(),
            TerrorClass::Schema,
            "infoschema::ERR_RESOURCE_GROUP_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_RESOURCE_GROUP_NOT_EXISTS
                .code()
                .value(),
            errcode::ErrResourceGroupNotExists as isize,
            "infoschema::ERR_RESOURCE_GROUP_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_RESOURCE_GROUP_INVALID_BACKGROUND_TASK_NAME.class(),
            TerrorClass::Executor,
            "infoschema::ERR_RESOURCE_GROUP_INVALID_BACKGROUND_TASK_NAME"
        );
        assert_eq!(
            super::ERR_RESOURCE_GROUP_INVALID_BACKGROUND_TASK_NAME
                .code()
                .value(),
            errcode::ErrResourceGroupInvalidBackgroundTaskName as isize,
            "infoschema::ERR_RESOURCE_GROUP_INVALID_BACKGROUND_TASK_NAME"
        );
        assert_eq!(
            super::ERR_RESOURCE_GROUP_INVALID_FOR_ROLE.class(),
            TerrorClass::Schema,
            "infoschema::ERR_RESOURCE_GROUP_INVALID_FOR_ROLE"
        );
        assert_eq!(
            super::ERR_RESOURCE_GROUP_INVALID_FOR_ROLE
                .code()
                .value(),
            errcode::ErrResourceGroupInvalidForRole as isize,
            "infoschema::ERR_RESOURCE_GROUP_INVALID_FOR_ROLE"
        );
        assert_eq!(
            super::ERR_RESERVED_SYNTAX.class(),
            TerrorClass::Schema,
            "infoschema::ERR_RESERVED_SYNTAX"
        );
        assert_eq!(
            super::ERR_RESERVED_SYNTAX.code().value(),
            errcode::ErrReservedSyntax as isize,
            "infoschema::ERR_RESERVED_SYNTAX"
        );
        assert_eq!(
            super::ERR_TABLE_EXISTS.class(),
            TerrorClass::Schema,
            "infoschema::ERR_TABLE_EXISTS"
        );
        assert_eq!(
            super::ERR_TABLE_EXISTS.code().value(),
            errcode::ErrTableExists as isize,
            "infoschema::ERR_TABLE_EXISTS"
        );
        assert_eq!(
            super::ERR_TABLE_DROP_EXISTS.class(),
            TerrorClass::Schema,
            "infoschema::ERR_TABLE_DROP_EXISTS"
        );
        assert_eq!(
            super::ERR_TABLE_DROP_EXISTS.code().value(),
            errcode::ErrBadTable as isize,
            "infoschema::ERR_TABLE_DROP_EXISTS"
        );
        assert_eq!(
            super::ERR_SEQUENCE_DROP_EXISTS.class(),
            TerrorClass::Schema,
            "infoschema::ERR_SEQUENCE_DROP_EXISTS"
        );
        assert_eq!(
            super::ERR_SEQUENCE_DROP_EXISTS.code().value(),
            errcode::ErrUnknownSequence as isize,
            "infoschema::ERR_SEQUENCE_DROP_EXISTS"
        );
        assert_eq!(
            super::ERR_COLUMN_NOT_EXISTS.class(),
            TerrorClass::Schema,
            "infoschema::ERR_COLUMN_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_COLUMN_NOT_EXISTS.code().value(),
            errcode::ErrBadField as isize,
            "infoschema::ERR_COLUMN_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_COLUMN_EXISTS.class(),
            TerrorClass::Schema,
            "infoschema::ERR_COLUMN_EXISTS"
        );
        assert_eq!(
            super::ERR_COLUMN_EXISTS.code().value(),
            errcode::ErrDupFieldName as isize,
            "infoschema::ERR_COLUMN_EXISTS"
        );
        assert_eq!(
            super::ERR_KEY_NAME_DUPLICATE.class(),
            TerrorClass::Schema,
            "infoschema::ERR_KEY_NAME_DUPLICATE"
        );
        assert_eq!(
            super::ERR_KEY_NAME_DUPLICATE.code().value(),
            errcode::ErrDupKeyName as isize,
            "infoschema::ERR_KEY_NAME_DUPLICATE"
        );
        assert_eq!(
            super::ERR_NONUNIQ_TABLE.class(),
            TerrorClass::Schema,
            "infoschema::ERR_NONUNIQ_TABLE"
        );
        assert_eq!(
            super::ERR_NONUNIQ_TABLE.code().value(),
            errcode::ErrNonuniqTable as isize,
            "infoschema::ERR_NONUNIQ_TABLE"
        );
        assert_eq!(
            super::ERR_MULTIPLE_PRI_KEY.class(),
            TerrorClass::Schema,
            "infoschema::ERR_MULTIPLE_PRI_KEY"
        );
        assert_eq!(
            super::ERR_MULTIPLE_PRI_KEY.code().value(),
            errcode::ErrMultiplePriKey as isize,
            "infoschema::ERR_MULTIPLE_PRI_KEY"
        );
        assert_eq!(
            super::ERR_TOO_MANY_KEY_PARTS.class(),
            TerrorClass::Schema,
            "infoschema::ERR_TOO_MANY_KEY_PARTS"
        );
        assert_eq!(
            super::ERR_TOO_MANY_KEY_PARTS.code().value(),
            errcode::ErrTooManyKeyParts as isize,
            "infoschema::ERR_TOO_MANY_KEY_PARTS"
        );
        assert_eq!(
            super::ERR_FOREIGN_KEY_NOT_EXISTS.class(),
            TerrorClass::Schema,
            "infoschema::ERR_FOREIGN_KEY_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_FOREIGN_KEY_NOT_EXISTS
                .code()
                .value(),
            errcode::ErrCantDropFieldOrKey as isize,
            "infoschema::ERR_FOREIGN_KEY_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_TABLE_NOT_LOCKED_FOR_WRITE.class(),
            TerrorClass::Schema,
            "infoschema::ERR_TABLE_NOT_LOCKED_FOR_WRITE"
        );
        assert_eq!(
            super::ERR_TABLE_NOT_LOCKED_FOR_WRITE
                .code()
                .value(),
            errcode::ErrTableNotLockedForWrite as isize,
            "infoschema::ERR_TABLE_NOT_LOCKED_FOR_WRITE"
        );
        assert_eq!(
            super::ERR_TABLE_NOT_LOCKED.class(),
            TerrorClass::Schema,
            "infoschema::ERR_TABLE_NOT_LOCKED"
        );
        assert_eq!(
            super::ERR_TABLE_NOT_LOCKED.code().value(),
            errcode::ErrTableNotLocked as isize,
            "infoschema::ERR_TABLE_NOT_LOCKED"
        );
        assert_eq!(
            super::ERR_TABLE_NOT_EXISTS.class(),
            TerrorClass::Schema,
            "infoschema::ERR_TABLE_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_TABLE_NOT_EXISTS.code().value(),
            errcode::ErrNoSuchTable as isize,
            "infoschema::ERR_TABLE_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_KEY_NOT_EXISTS.class(),
            TerrorClass::Schema,
            "infoschema::ERR_KEY_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_KEY_NOT_EXISTS.code().value(),
            errcode::ErrKeyDoesNotExist as isize,
            "infoschema::ERR_KEY_NOT_EXISTS"
        );
        assert_eq!(
            super::ERR_CANNOT_ADD_FOREIGN.class(),
            TerrorClass::Schema,
            "infoschema::ERR_CANNOT_ADD_FOREIGN"
        );
        assert_eq!(
            super::ERR_CANNOT_ADD_FOREIGN.code().value(),
            errcode::ErrCannotAddForeign as isize,
            "infoschema::ERR_CANNOT_ADD_FOREIGN"
        );
        assert_eq!(
            super::ERR_FOREIGN_KEY_ON_PARTITIONED.class(),
            TerrorClass::Schema,
            "infoschema::ERR_FOREIGN_KEY_ON_PARTITIONED"
        );
        assert_eq!(
            super::ERR_FOREIGN_KEY_ON_PARTITIONED
                .code()
                .value(),
            errcode::ErrForeignKeyOnPartitioned as isize,
            "infoschema::ERR_FOREIGN_KEY_ON_PARTITIONED"
        );
        assert_eq!(
            super::ERR_FOREIGN_KEY_NOT_MATCH.class(),
            TerrorClass::Schema,
            "infoschema::ERR_FOREIGN_KEY_NOT_MATCH"
        );
        assert_eq!(
            super::ERR_FOREIGN_KEY_NOT_MATCH
                .code()
                .value(),
            errcode::ErrWrongFkDef as isize,
            "infoschema::ERR_FOREIGN_KEY_NOT_MATCH"
        );
        assert_eq!(
            super::ERR_INDEX_EXISTS.class(),
            TerrorClass::Schema,
            "infoschema::ERR_INDEX_EXISTS"
        );
        assert_eq!(
            super::ERR_INDEX_EXISTS.code().value(),
            errcode::ErrDupIndex as isize,
            "infoschema::ERR_INDEX_EXISTS"
        );
        assert_eq!(
            super::ERR_USER_DROP_EXISTS.class(),
            TerrorClass::Schema,
            "infoschema::ERR_USER_DROP_EXISTS"
        );
        assert_eq!(
            super::ERR_USER_DROP_EXISTS.code().value(),
            errcode::ErrBadUser as isize,
            "infoschema::ERR_USER_DROP_EXISTS"
        );
        assert_eq!(
            super::ERR_USER_ALREADY_EXISTS.class(),
            TerrorClass::Schema,
            "infoschema::ERR_USER_ALREADY_EXISTS"
        );
        assert_eq!(
            super::ERR_USER_ALREADY_EXISTS.code().value(),
            errcode::ErrUserAlreadyExists as isize,
            "infoschema::ERR_USER_ALREADY_EXISTS"
        );
        assert_eq!(
            super::ERR_TABLE_LOCKED.class(),
            TerrorClass::Schema,
            "infoschema::ERR_TABLE_LOCKED"
        );
        assert_eq!(
            super::ERR_TABLE_LOCKED.code().value(),
            errcode::ErrTableLocked as isize,
            "infoschema::ERR_TABLE_LOCKED"
        );
        assert_eq!(
            super::ERR_WRONG_OBJECT.class(),
            TerrorClass::Schema,
            "infoschema::ERR_WRONG_OBJECT"
        );
        assert_eq!(
            super::ERR_WRONG_OBJECT.code().value(),
            errcode::ErrWrongObject as isize,
            "infoschema::ERR_WRONG_OBJECT"
        );
        assert_eq!(
            super::ERR_ADMIN_CHECK_TABLE.class(),
            TerrorClass::Schema,
            "infoschema::ERR_ADMIN_CHECK_TABLE"
        );
        assert_eq!(
            super::ERR_ADMIN_CHECK_TABLE.code().value(),
            errcode::ErrAdminCheckTable as isize,
            "infoschema::ERR_ADMIN_CHECK_TABLE"
        );
        assert_eq!(
            super::ERR_EMPTY_DATABASE.class(),
            TerrorClass::Schema,
            "infoschema::ERR_EMPTY_DATABASE"
        );
        assert_eq!(
            super::ERR_EMPTY_DATABASE.code().value(),
            errcode::ErrBadDB as isize,
            "infoschema::ERR_EMPTY_DATABASE"
        );
        assert_eq!(
            super::ERR_FORBID_SCHEMA_CHANGE.class(),
            TerrorClass::Schema,
            "infoschema::ERR_FORBID_SCHEMA_CHANGE"
        );
        assert_eq!(
            super::ERR_FORBID_SCHEMA_CHANGE.code().value(),
            errcode::ErrForbidSchemaChange as isize,
            "infoschema::ERR_FORBID_SCHEMA_CHANGE"
        );
        assert_eq!(
            super::ERR_TABLE_WITHOUT_PRIMARY_KEY.class(),
            TerrorClass::Schema,
            "infoschema::ERR_TABLE_WITHOUT_PRIMARY_KEY"
        );
        assert_eq!(
            super::ERR_TABLE_WITHOUT_PRIMARY_KEY
                .code()
                .value(),
            errcode::ErrTableWithoutPrimaryKey as isize,
            "infoschema::ERR_TABLE_WITHOUT_PRIMARY_KEY"
        );
        assert_eq!(
            super::ERR_FOREIGN_KEY_CANNOT_USE_VIRTUAL_COLUMN.class(),
            TerrorClass::Schema,
            "infoschema::ERR_FOREIGN_KEY_CANNOT_USE_VIRTUAL_COLUMN"
        );
        assert_eq!(
            super::ERR_FOREIGN_KEY_CANNOT_USE_VIRTUAL_COLUMN
                .code()
                .value(),
            errcode::ErrForeignKeyCannotUseVirtualColumn as isize,
            "infoschema::ERR_FOREIGN_KEY_CANNOT_USE_VIRTUAL_COLUMN"
        );
        assert_eq!(
            super::ERR_FOREIGN_KEY_CANNOT_OPEN_PARENT.class(),
            TerrorClass::Schema,
            "infoschema::ERR_FOREIGN_KEY_CANNOT_OPEN_PARENT"
        );
        assert_eq!(
            super::ERR_FOREIGN_KEY_CANNOT_OPEN_PARENT
                .code()
                .value(),
            errcode::ErrForeignKeyCannotOpenParent as isize,
            "infoschema::ERR_FOREIGN_KEY_CANNOT_OPEN_PARENT"
        );
        assert_eq!(
            super::ERR_FOREIGN_KEY_NO_COLUMN_IN_PARENT.class(),
            TerrorClass::Schema,
            "infoschema::ERR_FOREIGN_KEY_NO_COLUMN_IN_PARENT"
        );
        assert_eq!(
            super::ERR_FOREIGN_KEY_NO_COLUMN_IN_PARENT
                .code()
                .value(),
            errcode::ErrForeignKeyNoColumnInParent as isize,
            "infoschema::ERR_FOREIGN_KEY_NO_COLUMN_IN_PARENT"
        );
        assert_eq!(
            super::ERR_FOREIGN_KEY_NO_INDEX_IN_PARENT.class(),
            TerrorClass::Schema,
            "infoschema::ERR_FOREIGN_KEY_NO_INDEX_IN_PARENT"
        );
        assert_eq!(
            super::ERR_FOREIGN_KEY_NO_INDEX_IN_PARENT
                .code()
                .value(),
            errcode::ErrForeignKeyNoIndexInParent as isize,
            "infoschema::ERR_FOREIGN_KEY_NO_INDEX_IN_PARENT"
        );
        assert_eq!(
            super::ERR_FOREIGN_KEY_COLUMN_NOT_NULL.class(),
            TerrorClass::Schema,
            "infoschema::ERR_FOREIGN_KEY_COLUMN_NOT_NULL"
        );
        assert_eq!(
            super::ERR_FOREIGN_KEY_COLUMN_NOT_NULL
                .code()
                .value(),
            errcode::ErrForeignKeyColumnNotNull as isize,
            "infoschema::ERR_FOREIGN_KEY_COLUMN_NOT_NULL"
        );
        assert_eq!(
            super::ERR_RESOURCE_GROUP_SUPPORT_DISABLED.class(),
            TerrorClass::Schema,
            "infoschema::ERR_RESOURCE_GROUP_SUPPORT_DISABLED"
        );
        assert_eq!(
            super::ERR_RESOURCE_GROUP_SUPPORT_DISABLED
                .code()
                .value(),
            errcode::ErrResourceGroupSupportDisabled as isize,
            "infoschema::ERR_RESOURCE_GROUP_SUPPORT_DISABLED"
        );
        assert_eq!(
            super::ERR_CHECK_CONSTRAINT_DUP_NAME.class(),
            TerrorClass::Schema,
            "infoschema::ERR_CHECK_CONSTRAINT_DUP_NAME"
        );
        assert_eq!(
            super::ERR_CHECK_CONSTRAINT_DUP_NAME
                .code()
                .value(),
            errcode::ErrCheckConstraintDupName as isize,
            "infoschema::ERR_CHECK_CONSTRAINT_DUP_NAME"
        );
        assert_eq!(
            super::ERR_PROTECTED_TABLE_MODE.class(),
            TerrorClass::Schema,
            "infoschema::ERR_PROTECTED_TABLE_MODE"
        );
        assert_eq!(
            super::ERR_PROTECTED_TABLE_MODE.code().value(),
            errcode::ErrProtectedTableMode as isize,
            "infoschema::ERR_PROTECTED_TABLE_MODE"
        );
        assert_eq!(
            super::ERR_INVALID_TABLE_MODE_SET.class(),
            TerrorClass::Schema,
            "infoschema::ERR_INVALID_TABLE_MODE_SET"
        );
        assert_eq!(
            super::ERR_INVALID_TABLE_MODE_SET
                .code()
                .value(),
            errcode::ErrInvalidTableModeSet as isize,
            "infoschema::ERR_INVALID_TABLE_MODE_SET"
        );
    }
}
