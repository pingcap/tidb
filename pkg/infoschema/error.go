// Copyright 2020 PingCAP, Inc.
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

package infoschema

import (
	mysql "github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

var (
	// ErrDatabaseExists returns for database already exists.
	ErrDatabaseExists = dbterror.ClassSchema.NewStd(mysql.ErrDBCreateExists)
	// ErrDatabaseDropExists returns for dropping a non-existent database.
	ErrDatabaseDropExists = dbterror.ClassSchema.NewStd(mysql.ErrDBDropExists)
	// ErrAccessDenied return when the user doesn't have the permission to access the table.
	ErrAccessDenied = dbterror.ClassSchema.NewStd(mysql.ErrAccessDenied)
	// ErrDatabaseNotExists returns for database not exists.
	ErrDatabaseNotExists = dbterror.ClassSchema.NewStd(mysql.ErrBadDB)
	// ErrPlacementPolicyExists returns for placement_policy policy already exists.
	ErrPlacementPolicyExists = dbterror.ClassSchema.NewStd(mysql.ErrPlacementPolicyExists)
	// ErrPlacementPolicyNotExists return for placement_policy policy not exists.
	ErrPlacementPolicyNotExists = dbterror.ClassSchema.NewStd(mysql.ErrPlacementPolicyNotExists)
	// ErrResourceGroupExists return for resource group already exists.
	ErrResourceGroupExists = dbterror.ClassSchema.NewStd(mysql.ErrResourceGroupExists)
	// ErrResourceGroupNotExists return for resource group not exists.
	ErrResourceGroupNotExists = dbterror.ClassSchema.NewStd(mysql.ErrResourceGroupNotExists)
	// ErrResourceGroupInvalidBackgroundTaskName return for unknown resource group background task name.
	ErrResourceGroupInvalidBackgroundTaskName = dbterror.ClassExecutor.NewStd(mysql.ErrResourceGroupInvalidBackgroundTaskName)
	// ErrResourceGroupInvalidForRole return for invalid resource group for role.
	ErrResourceGroupInvalidForRole = dbterror.ClassSchema.NewStd(mysql.ErrResourceGroupInvalidForRole)
	// ErrReservedSyntax for internal syntax.
	ErrReservedSyntax = dbterror.ClassSchema.NewStd(mysql.ErrReservedSyntax)
	// ErrTableExists returns for table already exists.
	ErrTableExists = dbterror.ClassSchema.NewStd(mysql.ErrTableExists)
	// ErrTableDropExists returns for dropping a non-existent table.
	ErrTableDropExists = dbterror.ClassSchema.NewStd(mysql.ErrBadTable)
	// ErrSequenceDropExists returns for dropping a non-exist sequence.
	ErrSequenceDropExists = dbterror.ClassSchema.NewStd(mysql.ErrUnknownSequence)
	// ErrColumnNotExists returns for column not exists.
	ErrColumnNotExists = dbterror.ClassSchema.NewStd(mysql.ErrBadField)
	// ErrColumnExists returns for column already exists.
	ErrColumnExists = dbterror.ClassSchema.NewStd(mysql.ErrDupFieldName)
	// ErrKeyNameDuplicate returns for index duplicate when rename index.
	ErrKeyNameDuplicate = dbterror.ClassSchema.NewStd(mysql.ErrDupKeyName)
	// ErrNonuniqTable returns when none unique tables errors.
	ErrNonuniqTable = dbterror.ClassSchema.NewStd(mysql.ErrNonuniqTable)
	// ErrMultiplePriKey returns for multiple primary keys.
	ErrMultiplePriKey = dbterror.ClassSchema.NewStd(mysql.ErrMultiplePriKey)
	// ErrTooManyKeyParts returns for too many key parts.
	ErrTooManyKeyParts = dbterror.ClassSchema.NewStd(mysql.ErrTooManyKeyParts)
	// ErrForeignKeyNotExists returns for foreign key not exists.
	ErrForeignKeyNotExists = dbterror.ClassSchema.NewStd(mysql.ErrCantDropFieldOrKey)
	// ErrTableNotLockedForWrite returns for write tables when only hold the table read lock.
	ErrTableNotLockedForWrite = dbterror.ClassSchema.NewStd(mysql.ErrTableNotLockedForWrite)
	// ErrTableNotLocked returns when session has explicitly lock tables, then visit unlocked table will return this error.
	ErrTableNotLocked = dbterror.ClassSchema.NewStd(mysql.ErrTableNotLocked)
	// ErrTableNotExists returns for table not exists.
	ErrTableNotExists = dbterror.ClassSchema.NewStd(mysql.ErrNoSuchTable)
	// ErrKeyNotExists returns for index not exists.
	ErrKeyNotExists = dbterror.ClassSchema.NewStd(mysql.ErrKeyDoesNotExist)
	// ErrCannotAddForeign returns for foreign key exists.
	ErrCannotAddForeign = dbterror.ClassSchema.NewStd(mysql.ErrCannotAddForeign)
	// ErrForeignKeyOnPartitioned returns for foreign key on partition table.
	ErrForeignKeyOnPartitioned = dbterror.ClassSchema.NewStd(mysql.ErrForeignKeyOnPartitioned)
	// ErrForeignKeyNotMatch returns for foreign key not match.
	ErrForeignKeyNotMatch = dbterror.ClassSchema.NewStd(mysql.ErrWrongFkDef)
	// ErrIndexExists returns for index already exists.
	ErrIndexExists = dbterror.ClassSchema.NewStd(mysql.ErrDupIndex)
	// ErrUserDropExists returns for dropping a non-existent user.
	ErrUserDropExists = dbterror.ClassSchema.NewStd(mysql.ErrBadUser)
	// ErrUserAlreadyExists return for creating a existent user.
	ErrUserAlreadyExists = dbterror.ClassSchema.NewStd(mysql.ErrUserAlreadyExists)
	// ErrTableLocked returns when the table was locked by other session.
	ErrTableLocked = dbterror.ClassSchema.NewStd(mysql.ErrTableLocked)
	// ErrWrongObject returns when the table/view/sequence is not the expected object.
	ErrWrongObject = dbterror.ClassSchema.NewStd(mysql.ErrWrongObject)
	// ErrAdminCheckTable returns when the check table in temporary mode.
	ErrAdminCheckTable = dbterror.ClassSchema.NewStd(mysql.ErrAdminCheckTable)
	// ErrEmptyDatabase returns when the database is unexpectedly empty.
	ErrEmptyDatabase = dbterror.ClassSchema.NewStd(mysql.ErrBadDB)
	// ErrForbidSchemaChange returns when the schema change is illegal
	ErrForbidSchemaChange = dbterror.ClassSchema.NewStd(mysql.ErrForbidSchemaChange)
	// ErrTableWithoutPrimaryKey returns when there is no primary key on a table and sql_require_primary_key is set
	ErrTableWithoutPrimaryKey = dbterror.ClassSchema.NewStd(mysql.ErrTableWithoutPrimaryKey)
	// ErrForeignKeyCannotUseVirtualColumn returns when foreign key refer virtual generated column.
	ErrForeignKeyCannotUseVirtualColumn = dbterror.ClassSchema.NewStd(mysql.ErrForeignKeyCannotUseVirtualColumn)
	// ErrForeignKeyCannotOpenParent returns when foreign key refer table not exists.
	ErrForeignKeyCannotOpenParent = dbterror.ClassSchema.NewStd(mysql.ErrForeignKeyCannotOpenParent)
	// ErrForeignKeyNoColumnInParent returns when foreign key refer columns don't exist in parent table.
	ErrForeignKeyNoColumnInParent = dbterror.ClassSchema.NewStd(mysql.ErrForeignKeyNoColumnInParent)
	// ErrForeignKeyNoIndexInParent returns when foreign key refer columns don't have related index in parent table.
	ErrForeignKeyNoIndexInParent = dbterror.ClassSchema.NewStd(mysql.ErrForeignKeyNoIndexInParent)
	// ErrForeignKeyColumnNotNull returns when foreign key with SET NULL constrain and the related column has not null.
	ErrForeignKeyColumnNotNull = dbterror.ClassSchema.NewStd(mysql.ErrForeignKeyColumnNotNull)
	// ErrResourceGroupSupportDisabled returns for resource group feature is disabled
	ErrResourceGroupSupportDisabled = dbterror.ClassSchema.NewStd(mysql.ErrResourceGroupSupportDisabled)
	// ErrCheckConstraintDupName returns for duplicate constraint names.
	ErrCheckConstraintDupName = dbterror.ClassSchema.NewStd(mysql.ErrCheckConstraintDupName)
)
