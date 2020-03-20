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

package ddl

import (
	"fmt"

	"github.com/pingcap/parser/terror"
	mysql "github.com/pingcap/tidb/errno"
)

var (
	// errWorkerClosed means we have already closed the DDL worker.
	errInvalidWorker = terror.ClassDDL.New(mysql.ErrInvalidDDLWorker, mysql.MySQLErrName[mysql.ErrInvalidDDLWorker])
	// errNotOwner means we are not owner and can't handle DDL jobs.
	errNotOwner              = terror.ClassDDL.New(mysql.ErrNotOwner, mysql.MySQLErrName[mysql.ErrNotOwner])
	errCantDecodeIndex       = terror.ClassDDL.New(mysql.ErrCantDecodeIndex, mysql.MySQLErrName[mysql.ErrCantDecodeIndex])
	errInvalidDDLJob         = terror.ClassDDL.New(mysql.ErrInvalidDDLJob, mysql.MySQLErrName[mysql.ErrInvalidDDLJob])
	errCancelledDDLJob       = terror.ClassDDL.New(mysql.ErrCancelledDDLJob, mysql.MySQLErrName[mysql.ErrCancelledDDLJob])
	errFileNotFound          = terror.ClassDDL.New(mysql.ErrFileNotFound, mysql.MySQLErrName[mysql.ErrFileNotFound])
	errRunMultiSchemaChanges = terror.ClassDDL.New(mysql.ErrUnsupportedDDLOperation, fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation], "multi schema change"))
	errWaitReorgTimeout      = terror.ClassDDL.New(mysql.ErrLockWaitTimeout, mysql.MySQLErrName[mysql.ErrWaitReorgTimeout])
	errInvalidStoreVer       = terror.ClassDDL.New(mysql.ErrInvalidStoreVersion, mysql.MySQLErrName[mysql.ErrInvalidStoreVersion])
	// ErrRepairTableFail is used to repair tableInfo in repair mode.
	ErrRepairTableFail = terror.ClassDDL.New(mysql.ErrRepairTable, mysql.MySQLErrName[mysql.ErrRepairTable])

	// We don't support dropping column with index covered now.
	errCantDropColWithIndex      = terror.ClassDDL.New(mysql.ErrUnsupportedDDLOperation, fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation], "drop column with index"))
	errUnsupportedAddColumn      = terror.ClassDDL.New(mysql.ErrUnsupportedDDLOperation, fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation], "add column"))
	errUnsupportedModifyColumn   = terror.ClassDDL.New(mysql.ErrUnsupportedDDLOperation, fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation], "modify column: %s"))
	errUnsupportedModifyCharset  = terror.ClassDDL.New(mysql.ErrUnsupportedDDLOperation, fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation], "modify %s"))
	errUnsupportedPKHandle       = terror.ClassDDL.New(mysql.ErrUnsupportedDDLOperation, fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation], "drop integer primary key"))
	errUnsupportedCharset        = terror.ClassDDL.New(mysql.ErrUnsupportedDDLOperation, fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation], "charset %s and collate %s"))
	errUnsupportedShardRowIDBits = terror.ClassDDL.New(mysql.ErrUnsupportedDDLOperation, fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation], "shard_row_id_bits for table with primary key as row id"))
	errBlobKeyWithoutLength      = terror.ClassDDL.New(mysql.ErrBlobKeyWithoutLength, mysql.MySQLErrName[mysql.ErrBlobKeyWithoutLength])
	errKeyPart0                  = terror.ClassDDL.New(mysql.ErrKeyPart0, mysql.MySQLErrName[mysql.ErrKeyPart0])
	errIncorrectPrefixKey        = terror.ClassDDL.New(mysql.ErrWrongSubKey, mysql.MySQLErrName[mysql.ErrWrongSubKey])
	errTooLongKey                = terror.ClassDDL.New(mysql.ErrTooLongKey, mysql.MySQLErrName[mysql.ErrTooLongKey])
	errKeyColumnDoesNotExits     = terror.ClassDDL.New(mysql.ErrKeyColumnDoesNotExits, mysql.MySQLErrName[mysql.ErrKeyColumnDoesNotExits])
	errUnknownTypeLength         = terror.ClassDDL.New(mysql.ErrUnknownTypeLength, mysql.MySQLErrName[mysql.ErrUnknownTypeLength])
	errUnknownFractionLength     = terror.ClassDDL.New(mysql.ErrUnknownFractionLength, mysql.MySQLErrName[mysql.ErrUnknownFractionLength])
	errInvalidDDLJobVersion      = terror.ClassDDL.New(mysql.ErrInvalidDDLJobVersion, mysql.MySQLErrName[mysql.ErrInvalidDDLJobVersion])
	errInvalidUseOfNull          = terror.ClassDDL.New(mysql.ErrInvalidUseOfNull, mysql.MySQLErrName[mysql.ErrInvalidUseOfNull])
	errTooManyFields             = terror.ClassDDL.New(mysql.ErrTooManyFields, mysql.MySQLErrName[mysql.ErrTooManyFields])
	errInvalidSplitRegionRanges  = terror.ClassDDL.New(mysql.ErrInvalidSplitRegionRanges, mysql.MySQLErrName[mysql.ErrInvalidSplitRegionRanges])
	errReorgPanic                = terror.ClassDDL.New(mysql.ErrReorgPanic, mysql.MySQLErrName[mysql.ErrReorgPanic])
	errFkColumnCannotDrop        = terror.ClassDDL.New(mysql.ErrFkColumnCannotDrop, mysql.MySQLErrName[mysql.ErrFkColumnCannotDrop])
	errFKIncompatibleColumns     = terror.ClassDDL.New(mysql.ErrFKIncompatibleColumns, mysql.MySQLErrName[mysql.ErrFKIncompatibleColumns])

	errOnlyOnRangeListPartition = terror.ClassDDL.New(mysql.ErrOnlyOnRangeListPartition, mysql.MySQLErrName[mysql.ErrOnlyOnRangeListPartition])
	// errWrongKeyColumn is for table column cannot be indexed.
	errWrongKeyColumn = terror.ClassDDL.New(mysql.ErrWrongKeyColumn, mysql.MySQLErrName[mysql.ErrWrongKeyColumn])
	// errWrongFKOptionForGeneratedColumn is for wrong foreign key reference option on generated columns.
	errWrongFKOptionForGeneratedColumn = terror.ClassDDL.New(mysql.ErrWrongFKOptionForGeneratedColumn, mysql.MySQLErrName[mysql.ErrWrongFKOptionForGeneratedColumn])
	// errUnsupportedOnGeneratedColumn is for unsupported actions on generated columns.
	errUnsupportedOnGeneratedColumn = terror.ClassDDL.New(mysql.ErrUnsupportedOnGeneratedColumn, mysql.MySQLErrName[mysql.ErrUnsupportedOnGeneratedColumn])
	// errGeneratedColumnNonPrior forbids to refer generated column non prior to it.
	errGeneratedColumnNonPrior = terror.ClassDDL.New(mysql.ErrGeneratedColumnNonPrior, mysql.MySQLErrName[mysql.ErrGeneratedColumnNonPrior])
	// errDependentByGeneratedColumn forbids to delete columns which are dependent by generated columns.
	errDependentByGeneratedColumn = terror.ClassDDL.New(mysql.ErrDependentByGeneratedColumn, mysql.MySQLErrName[mysql.ErrDependentByGeneratedColumn])
	// errJSONUsedAsKey forbids to use JSON as key or index.
	errJSONUsedAsKey = terror.ClassDDL.New(mysql.ErrJSONUsedAsKey, mysql.MySQLErrName[mysql.ErrJSONUsedAsKey])
	// errBlobCantHaveDefault forbids to give not null default value to TEXT/BLOB/JSON.
	errBlobCantHaveDefault = terror.ClassDDL.New(mysql.ErrBlobCantHaveDefault, mysql.MySQLErrName[mysql.ErrBlobCantHaveDefault])
	errTooLongIndexComment = terror.ClassDDL.New(mysql.ErrTooLongIndexComment, mysql.MySQLErrName[mysql.ErrTooLongIndexComment])
	// ErrInvalidDefaultValue returns for invalid default value for columns.
	ErrInvalidDefaultValue = terror.ClassDDL.New(mysql.ErrInvalidDefault, mysql.MySQLErrName[mysql.ErrInvalidDefault])
	// ErrGeneratedColumnRefAutoInc forbids to refer generated columns to auto-increment columns .
	ErrGeneratedColumnRefAutoInc = terror.ClassDDL.New(mysql.ErrGeneratedColumnRefAutoInc, mysql.MySQLErrName[mysql.ErrGeneratedColumnRefAutoInc])
	// ErrUnsupportedAddPartition returns for does not support add partitions.
	ErrUnsupportedAddPartition = terror.ClassDDL.New(mysql.ErrUnsupportedDDLOperation, fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation], "add partitions"))
	// ErrUnsupportedCoalescePartition returns for does not support coalesce partitions.
	ErrUnsupportedCoalescePartition = terror.ClassDDL.New(mysql.ErrUnsupportedDDLOperation, fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation], "coalesce partitions"))
	// ErrGeneratedColumnFunctionIsNotAllowed returns for unsupported functions for generated columns.
	ErrGeneratedColumnFunctionIsNotAllowed = terror.ClassDDL.New(mysql.ErrGeneratedColumnFunctionIsNotAllowed, mysql.MySQLErrName[mysql.ErrGeneratedColumnFunctionIsNotAllowed])
	// ErrUnsupportedPartitionByRangeColumns returns for does unsupported partition by range columns.
	ErrUnsupportedPartitionByRangeColumns = terror.ClassDDL.New(mysql.ErrUnsupportedDDLOperation, fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation], "partition by range columns"))
	errUnsupportedCreatePartition         = terror.ClassDDL.New(mysql.ErrUnsupportedDDLOperation, fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation], "partition type, treat as normal table"))
	errTablePartitionDisabled             = terror.ClassDDL.New(mysql.ErrUnsupportedDDLOperation, "Partitions are ignored because Table Partition is disabled, please set 'tidb_enable_table_partition' if you need to need to enable it")
	errUnsupportedIndexType               = terror.ClassDDL.New(mysql.ErrUnsupportedDDLOperation, fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation], "index type"))

	// ErrDupKeyName returns for duplicated key name
	ErrDupKeyName = terror.ClassDDL.New(mysql.ErrDupKeyName, mysql.MySQLErrName[mysql.ErrDupKeyName])
	// ErrInvalidDDLState returns for invalid ddl model object state.
	ErrInvalidDDLState = terror.ClassDDL.New(mysql.ErrInvalidDDLState, fmt.Sprintf(mysql.MySQLErrName[mysql.ErrInvalidDDLState]))
	// ErrUnsupportedModifyPrimaryKey returns an error when add or drop the primary key.
	// It's exported for testing.
	ErrUnsupportedModifyPrimaryKey = terror.ClassDDL.New(mysql.ErrUnsupportedDDLOperation, fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation], "%s primary key"))

	// ErrColumnBadNull returns for a bad null value.
	ErrColumnBadNull = terror.ClassDDL.New(mysql.ErrBadNull, mysql.MySQLErrName[mysql.ErrBadNull])
	// ErrBadField forbids to refer to unknown column.
	ErrBadField = terror.ClassDDL.New(mysql.ErrBadField, mysql.MySQLErrName[mysql.ErrBadField])
	// ErrCantRemoveAllFields returns for deleting all columns.
	ErrCantRemoveAllFields = terror.ClassDDL.New(mysql.ErrCantRemoveAllFields, mysql.MySQLErrName[mysql.ErrCantRemoveAllFields])
	// ErrCantDropFieldOrKey returns for dropping a non-existent field or key.
	ErrCantDropFieldOrKey = terror.ClassDDL.New(mysql.ErrCantDropFieldOrKey, mysql.MySQLErrName[mysql.ErrCantDropFieldOrKey])
	// ErrInvalidOnUpdate returns for invalid ON UPDATE clause.
	ErrInvalidOnUpdate = terror.ClassDDL.New(mysql.ErrInvalidOnUpdate, mysql.MySQLErrName[mysql.ErrInvalidOnUpdate])
	// ErrTooLongIdent returns for too long name of database/table/column/index.
	ErrTooLongIdent = terror.ClassDDL.New(mysql.ErrTooLongIdent, mysql.MySQLErrName[mysql.ErrTooLongIdent])
	// ErrWrongDBName returns for wrong database name.
	ErrWrongDBName = terror.ClassDDL.New(mysql.ErrWrongDBName, mysql.MySQLErrName[mysql.ErrWrongDBName])
	// ErrWrongTableName returns for wrong table name.
	ErrWrongTableName = terror.ClassDDL.New(mysql.ErrWrongTableName, mysql.MySQLErrName[mysql.ErrWrongTableName])
	// ErrWrongColumnName returns for wrong column name.
	ErrWrongColumnName = terror.ClassDDL.New(mysql.ErrWrongColumnName, mysql.MySQLErrName[mysql.ErrWrongColumnName])
	// ErrInvalidGroupFuncUse returns for using invalid group functions.
	ErrInvalidGroupFuncUse = terror.ClassDDL.New(mysql.ErrInvalidGroupFuncUse, mysql.MySQLErrName[mysql.ErrInvalidGroupFuncUse])
	// ErrTableMustHaveColumns returns for missing column when creating a table.
	ErrTableMustHaveColumns = terror.ClassDDL.New(mysql.ErrTableMustHaveColumns, mysql.MySQLErrName[mysql.ErrTableMustHaveColumns])
	// ErrWrongNameForIndex returns for wrong index name.
	ErrWrongNameForIndex = terror.ClassDDL.New(mysql.ErrWrongNameForIndex, mysql.MySQLErrName[mysql.ErrWrongNameForIndex])
	// ErrUnknownCharacterSet returns unknown character set.
	ErrUnknownCharacterSet = terror.ClassDDL.New(mysql.ErrUnknownCharacterSet, mysql.MySQLErrName[mysql.ErrUnknownCharacterSet])
	// ErrUnknownCollation returns unknown collation.
	ErrUnknownCollation = terror.ClassDDL.New(mysql.ErrUnknownCollation, mysql.MySQLErrName[mysql.ErrUnknownCollation])
	// ErrCollationCharsetMismatch returns when collation not match the charset.
	ErrCollationCharsetMismatch = terror.ClassDDL.New(mysql.ErrCollationCharsetMismatch, mysql.MySQLErrName[mysql.ErrCollationCharsetMismatch])
	// ErrConflictingDeclarations return conflict declarations.
	ErrConflictingDeclarations = terror.ClassDDL.New(mysql.ErrConflictingDeclarations, fmt.Sprintf(mysql.MySQLErrName[mysql.ErrConflictingDeclarations], "CHARACTER SET ", "%s", "CHARACTER SET ", "%s"))
	// ErrPrimaryCantHaveNull returns All parts of a PRIMARY KEY must be NOT NULL; if you need NULL in a key, use UNIQUE instead
	ErrPrimaryCantHaveNull = terror.ClassDDL.New(mysql.ErrPrimaryCantHaveNull, mysql.MySQLErrName[mysql.ErrPrimaryCantHaveNull])
	// ErrErrorOnRename returns error for wrong database name in alter table rename
	ErrErrorOnRename = terror.ClassDDL.New(mysql.ErrErrorOnRename, mysql.MySQLErrName[mysql.ErrErrorOnRename])
	// ErrViewSelectClause returns error for create view with select into clause
	ErrViewSelectClause = terror.ClassDDL.New(mysql.ErrViewSelectClause, mysql.MySQLErrName[mysql.ErrViewSelectClause])

	// ErrNotAllowedTypeInPartition returns not allowed type error when creating table partition with unsupported expression type.
	ErrNotAllowedTypeInPartition = terror.ClassDDL.New(mysql.ErrFieldTypeNotAllowedAsPartitionField, mysql.MySQLErrName[mysql.ErrFieldTypeNotAllowedAsPartitionField])
	// ErrPartitionMgmtOnNonpartitioned returns it's not a partition table.
	ErrPartitionMgmtOnNonpartitioned = terror.ClassDDL.New(mysql.ErrPartitionMgmtOnNonpartitioned, mysql.MySQLErrName[mysql.ErrPartitionMgmtOnNonpartitioned])
	// ErrDropPartitionNonExistent returns error in list of partition.
	ErrDropPartitionNonExistent = terror.ClassDDL.New(mysql.ErrDropPartitionNonExistent, mysql.MySQLErrName[mysql.ErrDropPartitionNonExistent])
	// ErrSameNamePartition returns duplicate partition name.
	ErrSameNamePartition = terror.ClassDDL.New(mysql.ErrSameNamePartition, mysql.MySQLErrName[mysql.ErrSameNamePartition])
	// ErrRangeNotIncreasing returns values less than value must be strictly increasing for each partition.
	ErrRangeNotIncreasing = terror.ClassDDL.New(mysql.ErrRangeNotIncreasing, mysql.MySQLErrName[mysql.ErrRangeNotIncreasing])
	// ErrPartitionMaxvalue returns maxvalue can only be used in last partition definition.
	ErrPartitionMaxvalue = terror.ClassDDL.New(mysql.ErrPartitionMaxvalue, mysql.MySQLErrName[mysql.ErrPartitionMaxvalue])
	//ErrDropLastPartition returns cannot remove all partitions, use drop table instead.
	ErrDropLastPartition = terror.ClassDDL.New(mysql.ErrDropLastPartition, mysql.MySQLErrName[mysql.ErrDropLastPartition])
	//ErrTooManyPartitions returns too many partitions were defined.
	ErrTooManyPartitions = terror.ClassDDL.New(mysql.ErrTooManyPartitions, mysql.MySQLErrName[mysql.ErrTooManyPartitions])
	//ErrPartitionFunctionIsNotAllowed returns this partition function is not allowed.
	ErrPartitionFunctionIsNotAllowed = terror.ClassDDL.New(mysql.ErrPartitionFunctionIsNotAllowed, mysql.MySQLErrName[mysql.ErrPartitionFunctionIsNotAllowed])
	// ErrPartitionFuncNotAllowed returns partition function returns the wrong type.
	ErrPartitionFuncNotAllowed = terror.ClassDDL.New(mysql.ErrPartitionFuncNotAllowed, mysql.MySQLErrName[mysql.ErrPartitionFuncNotAllowed])
	// ErrUniqueKeyNeedAllFieldsInPf returns must include all columns in the table's partitioning function.
	ErrUniqueKeyNeedAllFieldsInPf = terror.ClassDDL.New(mysql.ErrUniqueKeyNeedAllFieldsInPf, mysql.MySQLErrName[mysql.ErrUniqueKeyNeedAllFieldsInPf])
	errWrongExprInPartitionFunc   = terror.ClassDDL.New(mysql.ErrWrongExprInPartitionFunc, mysql.MySQLErrName[mysql.ErrWrongExprInPartitionFunc])
	// ErrWarnDataTruncated returns data truncated error.
	ErrWarnDataTruncated = terror.ClassDDL.New(mysql.WarnDataTruncated, mysql.MySQLErrName[mysql.WarnDataTruncated])
	// ErrCoalesceOnlyOnHashPartition returns coalesce partition can only be used on hash/key partitions.
	ErrCoalesceOnlyOnHashPartition = terror.ClassDDL.New(mysql.ErrCoalesceOnlyOnHashPartition, mysql.MySQLErrName[mysql.ErrCoalesceOnlyOnHashPartition])
	// ErrViewWrongList returns create view must include all columns in the select clause
	ErrViewWrongList = terror.ClassDDL.New(mysql.ErrViewWrongList, mysql.MySQLErrName[mysql.ErrViewWrongList])
	// ErrAlterOperationNotSupported returns when alter operations is not supported.
	ErrAlterOperationNotSupported = terror.ClassDDL.New(mysql.ErrAlterOperationNotSupportedReason, mysql.MySQLErrName[mysql.ErrAlterOperationNotSupportedReason])
	// ErrWrongObject returns for wrong object.
	ErrWrongObject = terror.ClassDDL.New(mysql.ErrWrongObject, mysql.MySQLErrName[mysql.ErrWrongObject])
	// ErrTableCantHandleFt returns FULLTEXT keys are not supported by table type
	ErrTableCantHandleFt = terror.ClassDDL.New(mysql.ErrTableCantHandleFt, mysql.MySQLErrName[mysql.ErrTableCantHandleFt])
	// ErrFieldNotFoundPart returns an error when 'partition by columns' are not found in table columns.
	ErrFieldNotFoundPart = terror.ClassDDL.New(mysql.ErrFieldNotFoundPart, mysql.MySQLErrName[mysql.ErrFieldNotFoundPart])
	// ErrWrongTypeColumnValue returns 'Partition column values of incorrect type'
	ErrWrongTypeColumnValue = terror.ClassDDL.New(mysql.ErrWrongTypeColumnValue, mysql.MySQLErrName[mysql.ErrWrongTypeColumnValue])
	// ErrFunctionalIndexPrimaryKey returns 'The primary key cannot be a functional index'
	ErrFunctionalIndexPrimaryKey = terror.ClassDDL.New(mysql.ErrFunctionalIndexPrimaryKey, mysql.MySQLErrName[mysql.ErrFunctionalIndexPrimaryKey])
	// ErrFunctionalIndexOnField returns 'Functional index on a column is not supported. Consider using a regular index instead'
	ErrFunctionalIndexOnField = terror.ClassDDL.New(mysql.ErrFunctionalIndexOnField, mysql.MySQLErrName[mysql.ErrFunctionalIndexOnField])
	// ErrInvalidAutoRandom returns when auto_random is used incorrectly.
	ErrInvalidAutoRandom = terror.ClassDDL.New(mysql.ErrInvalidAutoRandom, mysql.MySQLErrName[mysql.ErrInvalidAutoRandom])

	// ErrSequenceRunOut returns when the sequence has been run out.
	ErrSequenceRunOut = terror.ClassDDL.New(mysql.ErrSequenceRunOut, mysql.MySQLErrName[mysql.ErrSequenceRunOut])
	// ErrSequenceInvalidData returns when sequence values are conflicting.
	ErrSequenceInvalidData = terror.ClassDDL.New(mysql.ErrSequenceInvalidData, mysql.MySQLErrName[mysql.ErrSequenceInvalidData])
	// ErrSequenceAccessFail returns when sequences are not able to access.
	ErrSequenceAccessFail = terror.ClassDDL.New(mysql.ErrSequenceAccessFail, mysql.MySQLErrName[mysql.ErrSequenceAccessFail])
	// ErrNotSequence returns when object is not a sequence.
	ErrNotSequence = terror.ClassDDL.New(mysql.ErrNotSequence, mysql.MySQLErrName[mysql.ErrNotSequence])
	// ErrUnknownSequence returns when drop / alter unknown sequence.
	ErrUnknownSequence = terror.ClassDDL.New(mysql.ErrUnknownSequence, mysql.MySQLErrName[mysql.ErrUnknownSequence])
	// ErrSequenceUnsupportedTableOption returns when unsupported table option exists in sequence.
	ErrSequenceUnsupportedTableOption = terror.ClassDDL.New(mysql.ErrSequenceUnsupportedTableOption, mysql.MySQLErrName[mysql.ErrSequenceUnsupportedTableOption])
	// ErrColumnTypeUnsupportedNextValue is returned when sequence next value is assigned to unsupported column type.
	ErrColumnTypeUnsupportedNextValue = terror.ClassDDL.New(mysql.ErrColumnTypeUnsupportedNextValue, mysql.MySQLErrName[mysql.ErrColumnTypeUnsupportedNextValue])
	// ErrUnsupportedExpressionIndex is returned when create an expression index without allow-expression-index.
	ErrUnsupportedExpressionIndex = terror.ClassDDL.New(mysql.ErrUnsupportedDDLOperation, fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation], "creating expression index without allow-expression-index in config"))
)
