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

	parser_mysql "github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	mysql "github.com/pingcap/tidb/errno"
)

var (
	// errWorkerClosed means we have already closed the DDL worker.
	errInvalidWorker = terror.ClassDDL.NewStd(mysql.ErrInvalidDDLWorker)
	// errNotOwner means we are not owner and can't handle DDL jobs.
	errNotOwner              = terror.ClassDDL.NewStd(mysql.ErrNotOwner)
	errCantDecodeRecord      = terror.ClassDDL.NewStd(mysql.ErrCantDecodeRecord)
	errInvalidDDLJob         = terror.ClassDDL.NewStd(mysql.ErrInvalidDDLJob)
	errCancelledDDLJob       = terror.ClassDDL.NewStd(mysql.ErrCancelledDDLJob)
	errFileNotFound          = terror.ClassDDL.NewStd(mysql.ErrFileNotFound)
	errRunMultiSchemaChanges = terror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "multi schema change"), nil), "", "")
	errWaitReorgTimeout      = terror.ClassDDL.NewStdErr(mysql.ErrLockWaitTimeout, mysql.MySQLErrName[mysql.ErrWaitReorgTimeout], "", "")
	errInvalidStoreVer       = terror.ClassDDL.NewStd(mysql.ErrInvalidStoreVersion)
	// ErrRepairTableFail is used to repair tableInfo in repair mode.
	ErrRepairTableFail = terror.ClassDDL.NewStd(mysql.ErrRepairTable)

	// We don't support dropping column with index covered now.
	errCantDropColWithIndex                   = terror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "drop column with index"), nil), "", "")
	errUnsupportedAddColumn                   = terror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "add column"), nil), "", "")
	errUnsupportedModifyColumn                = terror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "modify column: %s"), nil), "", "")
	errUnsupportedModifyCharset               = terror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "modify %s"), nil), "", "")
	errUnsupportedModifyCollation             = terror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "modifying collation from %s to %s"), nil), "", "")
	errUnsupportedPKHandle                    = terror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "drop integer primary key"), nil), "", "")
	errUnsupportedCharset                     = terror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "charset %s and collate %s"), nil), "", "")
	errUnsupportedShardRowIDBits              = terror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "shard_row_id_bits for table with primary key as row id"), nil), "", "")
	errUnsupportedAlterTableWithValidation    = terror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message("ALTER TABLE WITH VALIDATION is currently unsupported", nil), "", "")
	errUnsupportedAlterTableWithoutValidation = terror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message("ALTER TABLE WITHOUT VALIDATION is currently unsupported", nil), "", "")
	errBlobKeyWithoutLength                   = terror.ClassDDL.NewStd(mysql.ErrBlobKeyWithoutLength)
	errKeyPart0                               = terror.ClassDDL.NewStd(mysql.ErrKeyPart0)
	errIncorrectPrefixKey                     = terror.ClassDDL.NewStd(mysql.ErrWrongSubKey)
	errTooLongKey                             = terror.ClassDDL.NewStd(mysql.ErrTooLongKey)
	errKeyColumnDoesNotExits                  = terror.ClassDDL.NewStd(mysql.ErrKeyColumnDoesNotExits)
	errUnknownTypeLength                      = terror.ClassDDL.NewStd(mysql.ErrUnknownTypeLength)
	errUnknownFractionLength                  = terror.ClassDDL.NewStd(mysql.ErrUnknownFractionLength)
	errInvalidDDLJobVersion                   = terror.ClassDDL.NewStd(mysql.ErrInvalidDDLJobVersion)
	errInvalidUseOfNull                       = terror.ClassDDL.NewStd(mysql.ErrInvalidUseOfNull)
	errTooManyFields                          = terror.ClassDDL.NewStd(mysql.ErrTooManyFields)
	errInvalidSplitRegionRanges               = terror.ClassDDL.NewStd(mysql.ErrInvalidSplitRegionRanges)
	errReorgPanic                             = terror.ClassDDL.NewStd(mysql.ErrReorgPanic)
	errFkColumnCannotDrop                     = terror.ClassDDL.NewStd(mysql.ErrFkColumnCannotDrop)
	errFKIncompatibleColumns                  = terror.ClassDDL.NewStd(mysql.ErrFKIncompatibleColumns)

	errOnlyOnRangeListPartition = terror.ClassDDL.NewStd(mysql.ErrOnlyOnRangeListPartition)
	// errWrongKeyColumn is for table column cannot be indexed.
	errWrongKeyColumn = terror.ClassDDL.NewStd(mysql.ErrWrongKeyColumn)
	// errWrongFKOptionForGeneratedColumn is for wrong foreign key reference option on generated columns.
	errWrongFKOptionForGeneratedColumn = terror.ClassDDL.NewStd(mysql.ErrWrongFKOptionForGeneratedColumn)
	// ErrUnsupportedOnGeneratedColumn is for unsupported actions on generated columns.
	ErrUnsupportedOnGeneratedColumn = terror.ClassDDL.NewStd(mysql.ErrUnsupportedOnGeneratedColumn)
	// errGeneratedColumnNonPrior forbids to refer generated column non prior to it.
	errGeneratedColumnNonPrior = terror.ClassDDL.NewStd(mysql.ErrGeneratedColumnNonPrior)
	// errDependentByGeneratedColumn forbids to delete columns which are dependent by generated columns.
	errDependentByGeneratedColumn = terror.ClassDDL.NewStd(mysql.ErrDependentByGeneratedColumn)
	// errJSONUsedAsKey forbids to use JSON as key or index.
	errJSONUsedAsKey = terror.ClassDDL.NewStd(mysql.ErrJSONUsedAsKey)
	// errBlobCantHaveDefault forbids to give not null default value to TEXT/BLOB/JSON.
	errBlobCantHaveDefault = terror.ClassDDL.NewStd(mysql.ErrBlobCantHaveDefault)
	errTooLongIndexComment = terror.ClassDDL.NewStd(mysql.ErrTooLongIndexComment)
	// ErrInvalidDefaultValue returns for invalid default value for columns.
	ErrInvalidDefaultValue = terror.ClassDDL.NewStd(mysql.ErrInvalidDefault)
	// ErrGeneratedColumnRefAutoInc forbids to refer generated columns to auto-increment columns .
	ErrGeneratedColumnRefAutoInc = terror.ClassDDL.NewStd(mysql.ErrGeneratedColumnRefAutoInc)
	// ErrUnsupportedAddPartition returns for does not support add partitions.
	ErrUnsupportedAddPartition = terror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "add partitions"), nil), "", "")
	// ErrUnsupportedCoalescePartition returns for does not support coalesce partitions.
	ErrUnsupportedCoalescePartition   = terror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "coalesce partitions"), nil), "", "")
	errUnsupportedReorganizePartition = terror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "reorganize partition"), nil), "", "")
	errUnsupportedCheckPartition      = terror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "check partition"), nil), "", "")
	errUnsupportedOptimizePartition   = terror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "optimize partition"), nil), "", "")
	errUnsupportedRebuildPartition    = terror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "rebuild partition"), nil), "", "")
	errUnsupportedRemovePartition     = terror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "remove partitioning"), nil), "", "")
	errUnsupportedRepairPartition     = terror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "repair partition"), nil), "", "")
	errUnsupportedExchangePartition   = terror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "exchange partition"), nil), "", "")
	// ErrGeneratedColumnFunctionIsNotAllowed returns for unsupported functions for generated columns.
	ErrGeneratedColumnFunctionIsNotAllowed = terror.ClassDDL.NewStd(mysql.ErrGeneratedColumnFunctionIsNotAllowed)
	// ErrUnsupportedPartitionByRangeColumns returns for does unsupported partition by range columns.
	ErrUnsupportedPartitionByRangeColumns = terror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "partition by range columns"), nil), "", "")
	errUnsupportedCreatePartition         = terror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "partition type, treat as normal table"), nil), "", "")
	errTablePartitionDisabled             = terror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message("Partitions are ignored because Table Partition is disabled, please set 'tidb_enable_table_partition' if you need to need to enable it", nil), "", "")
	errUnsupportedIndexType               = terror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "index type"), nil), "", "")

	// ErrDupKeyName returns for duplicated key name
	ErrDupKeyName = terror.ClassDDL.NewStd(mysql.ErrDupKeyName)
	// ErrInvalidDDLState returns for invalid ddl model object state.
	ErrInvalidDDLState = terror.ClassDDL.NewStdErr(mysql.ErrInvalidDDLState, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrInvalidDDLState].Raw), nil), "", "")
	// ErrUnsupportedModifyPrimaryKey returns an error when add or drop the primary key.
	// It's exported for testing.
	ErrUnsupportedModifyPrimaryKey = terror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "%s primary key"), nil), "", "")
	// ErrPKIndexCantBeInvisible return an error when primary key is invisible index
	ErrPKIndexCantBeInvisible = terror.ClassDDL.NewStd(mysql.ErrPKIndexCantBeInvisible)

	// ErrColumnBadNull returns for a bad null value.
	ErrColumnBadNull = terror.ClassDDL.NewStd(mysql.ErrBadNull)
	// ErrBadField forbids to refer to unknown column.
	ErrBadField = terror.ClassDDL.NewStd(mysql.ErrBadField)
	// ErrCantRemoveAllFields returns for deleting all columns.
	ErrCantRemoveAllFields = terror.ClassDDL.NewStd(mysql.ErrCantRemoveAllFields)
	// ErrCantDropFieldOrKey returns for dropping a non-existent field or key.
	ErrCantDropFieldOrKey = terror.ClassDDL.NewStd(mysql.ErrCantDropFieldOrKey)
	// ErrInvalidOnUpdate returns for invalid ON UPDATE clause.
	ErrInvalidOnUpdate = terror.ClassDDL.NewStd(mysql.ErrInvalidOnUpdate)
	// ErrTooLongIdent returns for too long name of database/table/column/index.
	ErrTooLongIdent = terror.ClassDDL.NewStd(mysql.ErrTooLongIdent)
	// ErrWrongDBName returns for wrong database name.
	ErrWrongDBName = terror.ClassDDL.NewStd(mysql.ErrWrongDBName)
	// ErrWrongTableName returns for wrong table name.
	ErrWrongTableName = terror.ClassDDL.NewStd(mysql.ErrWrongTableName)
	// ErrWrongColumnName returns for wrong column name.
	ErrWrongColumnName = terror.ClassDDL.NewStd(mysql.ErrWrongColumnName)
	// ErrInvalidGroupFuncUse returns for using invalid group functions.
	ErrInvalidGroupFuncUse = terror.ClassDDL.NewStd(mysql.ErrInvalidGroupFuncUse)
	// ErrTableMustHaveColumns returns for missing column when creating a table.
	ErrTableMustHaveColumns = terror.ClassDDL.NewStd(mysql.ErrTableMustHaveColumns)
	// ErrWrongNameForIndex returns for wrong index name.
	ErrWrongNameForIndex = terror.ClassDDL.NewStd(mysql.ErrWrongNameForIndex)
	// ErrUnknownCharacterSet returns unknown character set.
	ErrUnknownCharacterSet = terror.ClassDDL.NewStd(mysql.ErrUnknownCharacterSet)
	// ErrUnknownCollation returns unknown collation.
	ErrUnknownCollation = terror.ClassDDL.NewStd(mysql.ErrUnknownCollation)
	// ErrCollationCharsetMismatch returns when collation not match the charset.
	ErrCollationCharsetMismatch = terror.ClassDDL.NewStd(mysql.ErrCollationCharsetMismatch)
	// ErrConflictingDeclarations return conflict declarations.
	ErrConflictingDeclarations = terror.ClassDDL.NewStdErr(mysql.ErrConflictingDeclarations, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrConflictingDeclarations].Raw, "CHARACTER SET ", "%s", "CHARACTER SET ", "%s"), nil), "", "")
	// ErrPrimaryCantHaveNull returns All parts of a PRIMARY KEY must be NOT NULL; if you need NULL in a key, use UNIQUE instead
	ErrPrimaryCantHaveNull = terror.ClassDDL.NewStd(mysql.ErrPrimaryCantHaveNull)
	// ErrErrorOnRename returns error for wrong database name in alter table rename
	ErrErrorOnRename = terror.ClassDDL.NewStd(mysql.ErrErrorOnRename)
	// ErrViewSelectClause returns error for create view with select into clause
	ErrViewSelectClause = terror.ClassDDL.NewStd(mysql.ErrViewSelectClause)

	// ErrNotAllowedTypeInPartition returns not allowed type error when creating table partition with unsupported expression type.
	ErrNotAllowedTypeInPartition = terror.ClassDDL.NewStd(mysql.ErrFieldTypeNotAllowedAsPartitionField)
	// ErrPartitionMgmtOnNonpartitioned returns it's not a partition table.
	ErrPartitionMgmtOnNonpartitioned = terror.ClassDDL.NewStd(mysql.ErrPartitionMgmtOnNonpartitioned)
	// ErrDropPartitionNonExistent returns error in list of partition.
	ErrDropPartitionNonExistent = terror.ClassDDL.NewStd(mysql.ErrDropPartitionNonExistent)
	// ErrSameNamePartition returns duplicate partition name.
	ErrSameNamePartition = terror.ClassDDL.NewStd(mysql.ErrSameNamePartition)
	// ErrRangeNotIncreasing returns values less than value must be strictly increasing for each partition.
	ErrRangeNotIncreasing = terror.ClassDDL.NewStd(mysql.ErrRangeNotIncreasing)
	// ErrPartitionMaxvalue returns maxvalue can only be used in last partition definition.
	ErrPartitionMaxvalue = terror.ClassDDL.NewStd(mysql.ErrPartitionMaxvalue)
	// ErrDropLastPartition returns cannot remove all partitions, use drop table instead.
	ErrDropLastPartition = terror.ClassDDL.NewStd(mysql.ErrDropLastPartition)
	// ErrTooManyPartitions returns too many partitions were defined.
	ErrTooManyPartitions = terror.ClassDDL.NewStd(mysql.ErrTooManyPartitions)
	// ErrPartitionFunctionIsNotAllowed returns this partition function is not allowed.
	ErrPartitionFunctionIsNotAllowed = terror.ClassDDL.NewStd(mysql.ErrPartitionFunctionIsNotAllowed)
	// ErrPartitionFuncNotAllowed returns partition function returns the wrong type.
	ErrPartitionFuncNotAllowed = terror.ClassDDL.NewStd(mysql.ErrPartitionFuncNotAllowed)
	// ErrUniqueKeyNeedAllFieldsInPf returns must include all columns in the table's partitioning function.
	ErrUniqueKeyNeedAllFieldsInPf = terror.ClassDDL.NewStd(mysql.ErrUniqueKeyNeedAllFieldsInPf)
	errWrongExprInPartitionFunc   = terror.ClassDDL.NewStd(mysql.ErrWrongExprInPartitionFunc)
	// ErrWarnDataTruncated returns data truncated error.
	ErrWarnDataTruncated = terror.ClassDDL.NewStd(mysql.WarnDataTruncated)
	// ErrCoalesceOnlyOnHashPartition returns coalesce partition can only be used on hash/key partitions.
	ErrCoalesceOnlyOnHashPartition = terror.ClassDDL.NewStd(mysql.ErrCoalesceOnlyOnHashPartition)
	// ErrViewWrongList returns create view must include all columns in the select clause
	ErrViewWrongList = terror.ClassDDL.NewStd(mysql.ErrViewWrongList)
	// ErrAlterOperationNotSupported returns when alter operations is not supported.
	ErrAlterOperationNotSupported = terror.ClassDDL.NewStd(mysql.ErrAlterOperationNotSupportedReason)
	// ErrWrongObject returns for wrong object.
	ErrWrongObject = terror.ClassDDL.NewStd(mysql.ErrWrongObject)
	// ErrTableCantHandleFt returns FULLTEXT keys are not supported by table type
	ErrTableCantHandleFt = terror.ClassDDL.NewStd(mysql.ErrTableCantHandleFt)
	// ErrFieldNotFoundPart returns an error when 'partition by columns' are not found in table columns.
	ErrFieldNotFoundPart = terror.ClassDDL.NewStd(mysql.ErrFieldNotFoundPart)
	// ErrWrongTypeColumnValue returns 'Partition column values of incorrect type'
	ErrWrongTypeColumnValue = terror.ClassDDL.NewStd(mysql.ErrWrongTypeColumnValue)
	// ErrFunctionalIndexPrimaryKey returns 'The primary key cannot be a functional index'
	ErrFunctionalIndexPrimaryKey = terror.ClassDDL.NewStd(mysql.ErrFunctionalIndexPrimaryKey)
	// ErrFunctionalIndexOnField returns 'Functional index on a column is not supported. Consider using a regular index instead'
	ErrFunctionalIndexOnField = terror.ClassDDL.NewStd(mysql.ErrFunctionalIndexOnField)
	// ErrInvalidAutoRandom returns when auto_random is used incorrectly.
	ErrInvalidAutoRandom = terror.ClassDDL.NewStd(mysql.ErrInvalidAutoRandom)
	// ErrUnsupportedConstraintCheck returns when use ADD CONSTRAINT CHECK
	ErrUnsupportedConstraintCheck = terror.ClassDDL.NewStd(mysql.ErrUnsupportedConstraintCheck)

	// ErrSequenceRunOut returns when the sequence has been run out.
	ErrSequenceRunOut = terror.ClassDDL.NewStd(mysql.ErrSequenceRunOut)
	// ErrSequenceInvalidData returns when sequence values are conflicting.
	ErrSequenceInvalidData = terror.ClassDDL.NewStd(mysql.ErrSequenceInvalidData)
	// ErrSequenceAccessFail returns when sequences are not able to access.
	ErrSequenceAccessFail = terror.ClassDDL.NewStd(mysql.ErrSequenceAccessFail)
	// ErrNotSequence returns when object is not a sequence.
	ErrNotSequence = terror.ClassDDL.NewStd(mysql.ErrNotSequence)
	// ErrUnknownSequence returns when drop / alter unknown sequence.
	ErrUnknownSequence = terror.ClassDDL.NewStd(mysql.ErrUnknownSequence)
	// ErrSequenceUnsupportedTableOption returns when unsupported table option exists in sequence.
	ErrSequenceUnsupportedTableOption = terror.ClassDDL.NewStd(mysql.ErrSequenceUnsupportedTableOption)
	// ErrColumnTypeUnsupportedNextValue is returned when sequence next value is assigned to unsupported column type.
	ErrColumnTypeUnsupportedNextValue = terror.ClassDDL.NewStd(mysql.ErrColumnTypeUnsupportedNextValue)
	// ErrAddColumnWithSequenceAsDefault is returned when the new added column with sequence's nextval as it's default value.
	ErrAddColumnWithSequenceAsDefault = terror.ClassDDL.NewStd(mysql.ErrAddColumnWithSequenceAsDefault)
	// ErrUnsupportedExpressionIndex is returned when create an expression index without allow-expression-index.
	ErrUnsupportedExpressionIndex = terror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "creating expression index without allow-expression-index in config"), nil), "", "")
	// ErrPartitionExchangePartTable is returned when exchange table partition with another table is partitioned.
	ErrPartitionExchangePartTable = terror.ClassDDL.NewStd(mysql.ErrPartitionExchangePartTable)
	// ErrTablesDifferentMetadata is returned when exchanges tables is not compatible.
	ErrTablesDifferentMetadata = terror.ClassDDL.NewStd(mysql.ErrTablesDifferentMetadata)
	// ErrRowDoesNotMatchPartition is returned when the row record of exchange table does not match the partition rule.
	ErrRowDoesNotMatchPartition = terror.ClassDDL.NewStd(mysql.ErrRowDoesNotMatchPartition)
	// ErrPartitionExchangeForeignKey is returned when exchanged normal table has foreign keys.
	ErrPartitionExchangeForeignKey = terror.ClassDDL.NewStd(mysql.ErrPartitionExchangeForeignKey)
	// ErrCheckNoSuchTable is returned when exchanged normal table is view or sequence.
	ErrCheckNoSuchTable         = terror.ClassDDL.NewStd(mysql.ErrCheckNoSuchTable)
	errUnsupportedPartitionType = terror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "partition type of table %s when exchanging partition"), nil), "", "")
	// ErrPartitionExchangeDifferentOption is returned when attribute does not match between partition table and normal table.
	ErrPartitionExchangeDifferentOption = terror.ClassDDL.NewStd(mysql.ErrPartitionExchangeDifferentOption)
	// ErrTableOptionUnionUnsupported is returned when create/alter table with union option.
	ErrTableOptionUnionUnsupported = terror.ClassDDL.NewStd(mysql.ErrTableOptionUnionUnsupported)
	// ErrTableOptionInsertMethodUnsupported is returned when create/alter table with insert method option.
	ErrTableOptionInsertMethodUnsupported = terror.ClassDDL.NewStd(mysql.ErrTableOptionInsertMethodUnsupported)

	// ErrInvalidPlacementSpec is returned when add/alter an invalid placement rule
	ErrInvalidPlacementSpec = terror.ClassDDL.NewStd(mysql.ErrInvalidPlacementSpec)
)
