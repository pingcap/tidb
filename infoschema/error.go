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
	ErrDatabaseExists = terror.ClassSchema.New(mysql.ErrDBCreateExists, mysql.MySQLErrName[mysql.ErrDBCreateExists])
	// ErrDatabaseDropExists returns for dropping a non-existent database.
	ErrDatabaseDropExists = terror.ClassSchema.New(mysql.ErrDBDropExists, mysql.MySQLErrName[mysql.ErrDBDropExists])
	// ErrAccessDenied return when the user doesn't have the permission to access the table.
	ErrAccessDenied = terror.ClassSchema.New(mysql.ErrAccessDenied, mysql.MySQLErrName[mysql.ErrAccessDenied])
	// ErrDatabaseNotExists returns for database not exists.
	ErrDatabaseNotExists = terror.ClassSchema.New(mysql.ErrBadDB, mysql.MySQLErrName[mysql.ErrBadDB])
	// ErrTableExists returns for table already exists.
	ErrTableExists = terror.ClassSchema.New(mysql.ErrTableExists, mysql.MySQLErrName[mysql.ErrTableExists])
	// ErrTableDropExists returns for dropping a non-existent table.
	ErrTableDropExists = terror.ClassSchema.New(mysql.ErrBadTable, mysql.MySQLErrName[mysql.ErrBadTable])
	// ErrSequenceDropExists returns for dropping a non-exist sequence.
	ErrSequenceDropExists = terror.ClassSchema.New(mysql.ErrUnknownSequence, mysql.MySQLErrName[mysql.ErrUnknownSequence])
	// ErrColumnNotExists returns for column not exists.
	ErrColumnNotExists = terror.ClassSchema.New(mysql.ErrBadField, mysql.MySQLErrName[mysql.ErrBadField])
	// ErrColumnExists returns for column already exists.
	ErrColumnExists = terror.ClassSchema.New(mysql.ErrDupFieldName, mysql.MySQLErrName[mysql.ErrDupFieldName])
	// ErrKeyNameDuplicate returns for index duplicate when rename index.
	ErrKeyNameDuplicate = terror.ClassSchema.New(mysql.ErrDupKeyName, mysql.MySQLErrName[mysql.ErrDupKeyName])
	// ErrNonuniqTable returns when none unique tables errors.
	ErrNonuniqTable = terror.ClassSchema.New(mysql.ErrNonuniqTable, mysql.MySQLErrName[mysql.ErrNonuniqTable])
	// ErrMultiplePriKey returns for multiple primary keys.
	ErrMultiplePriKey = terror.ClassSchema.New(mysql.ErrMultiplePriKey, mysql.MySQLErrName[mysql.ErrMultiplePriKey])
	// ErrTooManyKeyParts returns for too many key parts.
	ErrTooManyKeyParts = terror.ClassSchema.New(mysql.ErrTooManyKeyParts, mysql.MySQLErrName[mysql.ErrTooManyKeyParts])
	// ErrForeignKeyNotExists returns for foreign key not exists.
	ErrForeignKeyNotExists = terror.ClassSchema.New(mysql.ErrCantDropFieldOrKey, mysql.MySQLErrName[mysql.ErrCantDropFieldOrKey])
	// ErrTableNotLockedForWrite returns for write tables when only hold the table read lock.
	ErrTableNotLockedForWrite = terror.ClassSchema.New(mysql.ErrTableNotLockedForWrite, mysql.MySQLErrName[mysql.ErrTableNotLockedForWrite])
	// ErrTableNotLocked returns when session has explicitly lock tables, then visit unlocked table will return this error.
	ErrTableNotLocked = terror.ClassSchema.New(mysql.ErrTableNotLocked, mysql.MySQLErrName[mysql.ErrTableNotLocked])
	// ErrTableNotExists returns for table not exists.
	ErrTableNotExists = terror.ClassSchema.New(mysql.ErrNoSuchTable, mysql.MySQLErrName[mysql.ErrNoSuchTable])
	// ErrKeyNotExists returns for index not exists.
	ErrKeyNotExists = terror.ClassSchema.New(mysql.ErrKeyDoesNotExist, mysql.MySQLErrName[mysql.ErrKeyDoesNotExist])
	// ErrCannotAddForeign returns for foreign key exists.
	ErrCannotAddForeign = terror.ClassSchema.New(mysql.ErrCannotAddForeign, mysql.MySQLErrName[mysql.ErrCannotAddForeign])
	// ErrForeignKeyNotMatch returns for foreign key not match.
	ErrForeignKeyNotMatch = terror.ClassSchema.New(mysql.ErrWrongFkDef, mysql.MySQLErrName[mysql.ErrWrongFkDef])
	// ErrIndexExists returns for index already exists.
	ErrIndexExists = terror.ClassSchema.New(mysql.ErrDupIndex, mysql.MySQLErrName[mysql.ErrDupIndex])
	// ErrUserDropExists returns for dropping a non-existent user.
	ErrUserDropExists = terror.ClassSchema.New(mysql.ErrBadUser, mysql.MySQLErrName[mysql.ErrBadUser])
	// ErrUserAlreadyExists return for creating a existent user.
	ErrUserAlreadyExists = terror.ClassSchema.New(mysql.ErrUserAlreadyExists, mysql.MySQLErrName[mysql.ErrUserAlreadyExists])
	// ErrTableLocked returns when the table was locked by other session.
	ErrTableLocked = terror.ClassSchema.New(mysql.ErrTableLocked, mysql.MySQLErrName[mysql.ErrTableLocked])
	// ErrWrongObject returns when the table/view/sequence is not the expected object.
	ErrWrongObject = terror.ClassSchema.New(mysql.ErrWrongObject, mysql.MySQLErrName[mysql.ErrWrongObject])
)
