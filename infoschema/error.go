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
// See the License for the specific language governing permissions and
// limitations under the License.

package infoschema

import (
	"github.com/pingcap/parser/terror"
	mysql "github.com/pingcap/tidb/errno"
)

var (
	// ErrDatabaseExists returns for database already exists.
	ErrDatabaseExists = terror.ClassSchema.NewStd(mysql.ErrDBCreateExists)
	// ErrDatabaseDropExists returns for dropping a non-existent database.
	ErrDatabaseDropExists = terror.ClassSchema.NewStd(mysql.ErrDBDropExists)
	// ErrAccessDenied return when the user doesn't have the permission to access the table.
	ErrAccessDenied = terror.ClassSchema.NewStd(mysql.ErrAccessDenied)
	// ErrDatabaseNotExists returns for database not exists.
	ErrDatabaseNotExists = terror.ClassSchema.NewStd(mysql.ErrBadDB)
	// ErrTableExists returns for table already exists.
	ErrTableExists = terror.ClassSchema.NewStd(mysql.ErrTableExists)
	// ErrTableDropExists returns for dropping a non-existent table.
	ErrTableDropExists = terror.ClassSchema.NewStd(mysql.ErrBadTable)
	// ErrSequenceDropExists returns for dropping a non-exist sequence.
	ErrSequenceDropExists = terror.ClassSchema.NewStd(mysql.ErrUnknownSequence)
	// ErrColumnNotExists returns for column not exists.
	ErrColumnNotExists = terror.ClassSchema.NewStd(mysql.ErrBadField)
	// ErrColumnExists returns for column already exists.
	ErrColumnExists = terror.ClassSchema.NewStd(mysql.ErrDupFieldName)
	// ErrKeyNameDuplicate returns for index duplicate when rename index.
	ErrKeyNameDuplicate = terror.ClassSchema.NewStd(mysql.ErrDupKeyName)
	// ErrNonuniqTable returns when none unique tables errors.
	ErrNonuniqTable = terror.ClassSchema.NewStd(mysql.ErrNonuniqTable)
	// ErrMultiplePriKey returns for multiple primary keys.
	ErrMultiplePriKey = terror.ClassSchema.NewStd(mysql.ErrMultiplePriKey)
	// ErrTooManyKeyParts returns for too many key parts.
	ErrTooManyKeyParts = terror.ClassSchema.NewStd(mysql.ErrTooManyKeyParts)
	// ErrForeignKeyNotExists returns for foreign key not exists.
	ErrForeignKeyNotExists = terror.ClassSchema.NewStd(mysql.ErrCantDropFieldOrKey)
	// ErrTableNotLockedForWrite returns for write tables when only hold the table read lock.
	ErrTableNotLockedForWrite = terror.ClassSchema.NewStd(mysql.ErrTableNotLockedForWrite)
	// ErrTableNotLocked returns when session has explicitly lock tables, then visit unlocked table will return this error.
	ErrTableNotLocked = terror.ClassSchema.NewStd(mysql.ErrTableNotLocked)
	// ErrTableNotExists returns for table not exists.
	ErrTableNotExists = terror.ClassSchema.NewStd(mysql.ErrNoSuchTable)
	// ErrKeyNotExists returns for index not exists.
	ErrKeyNotExists = terror.ClassSchema.NewStd(mysql.ErrKeyDoesNotExist)
	// ErrCannotAddForeign returns for foreign key exists.
	ErrCannotAddForeign = terror.ClassSchema.NewStd(mysql.ErrCannotAddForeign)
	// ErrForeignKeyNotMatch returns for foreign key not match.
	ErrForeignKeyNotMatch = terror.ClassSchema.NewStd(mysql.ErrWrongFkDef)
	// ErrIndexExists returns for index already exists.
	ErrIndexExists = terror.ClassSchema.NewStd(mysql.ErrDupIndex)
	// ErrUserDropExists returns for dropping a non-existent user.
	ErrUserDropExists = terror.ClassSchema.NewStd(mysql.ErrBadUser)
	// ErrUserAlreadyExists return for creating a existent user.
	ErrUserAlreadyExists = terror.ClassSchema.NewStd(mysql.ErrUserAlreadyExists)
	// ErrTableLocked returns when the table was locked by other session.
	ErrTableLocked = terror.ClassSchema.NewStd(mysql.ErrTableLocked)
	// ErrWrongObject returns when the table/view/sequence is not the expected object.
	ErrWrongObject = terror.ClassSchema.NewStd(mysql.ErrWrongObject)
)
