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

package ddl

import (
	"fmt"

	mysql "github.com/pingcap/tidb/errno"
	parser_mysql "github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/dbterror"
)

var (
	// errWorkerClosed means we have already closed the DDL worker.
	errInvalidWorker = dbterror.ClassDDL.NewStd(mysql.ErrInvalidDDLWorker)
	// errNotOwner means we are not owner and can't handle DDL jobs.
	errNotOwner              = dbterror.ClassDDL.NewStd(mysql.ErrNotOwner)
	errCantDecodeRecord      = dbterror.ClassDDL.NewStd(mysql.ErrCantDecodeRecord)
	errInvalidDDLJob         = dbterror.ClassDDL.NewStd(mysql.ErrInvalidDDLJob)
	errCancelledDDLJob       = dbterror.ClassDDL.NewStd(mysql.ErrCancelledDDLJob)
	errFileNotFound          = dbterror.ClassDDL.NewStd(mysql.ErrFileNotFound)
	errRunMultiSchemaChanges = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "multi schema change"), nil))
	errWaitReorgTimeout      = dbterror.ClassDDL.NewStdErr(mysql.ErrLockWaitTimeout, mysql.MySQLErrName[mysql.ErrWaitReorgTimeout])
	errInvalidStoreVer       = dbterror.ClassDDL.NewStd(mysql.ErrInvalidStoreVersion)
	// ErrRepairTableFail is used to repair tableInfo in repair mode.
	ErrRepairTableFail = dbterror.ClassDDL.NewStd(mysql.ErrRepairTable)

	// We don't support dropping column with index covered now.
	errCantDropColWithIndex                   = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "drop column with index"), nil))
	errUnsupportedAddColumn                   = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "add column"), nil))
	errUnsupportedModifyColumn                = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "modify column: %s"), nil))
	errUnsupportedModifyCharset               = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "modify %s"), nil))
	errUnsupportedModifyCollation             = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "modifying collation from %s to %s"), nil))
	errUnsupportedPKHandle                    = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "drop integer primary key"), nil))
	errUnsupportedCharset                     = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "charset %s and collate %s"), nil))
	errUnsupportedShardRowIDBits              = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "shard_row_id_bits for table with primary key as row id"), nil))
	errUnsupportedAlterTableWithValidation    = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message("ALTER TABLE WITH VALIDATION is currently unsupported", nil))
	errUnsupportedAlterTableWithoutValidation = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message("ALTER TABLE WITHOUT VALIDATION is currently unsupported", nil))
	errUnsupportedAlterTableOption            = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message("This type of ALTER TABLE is currently unsupported", nil))
	errUnsupportedAlterReplicaForSysTable     = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message("ALTER table replica for tables in system database is currently unsupported", nil))
	errUnsupportedAlterCacheForSysTable       = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message("ALTER table cache for tables in system database is currently unsupported", nil))
	errBlobKeyWithoutLength                   = dbterror.ClassDDL.NewStd(mysql.ErrBlobKeyWithoutLength)
	errKeyPart0                               = dbterror.ClassDDL.NewStd(mysql.ErrKeyPart0)
	errIncorrectPrefixKey                     = dbterror.ClassDDL.NewStd(mysql.ErrWrongSubKey)
	errTooLongKey                             = dbterror.ClassDDL.NewStd(mysql.ErrTooLongKey)
	errKeyColumnDoesNotExits                  = dbterror.ClassDDL.NewStd(mysql.ErrKeyColumnDoesNotExits)
	errInvalidDDLJobVersion                   = dbterror.ClassDDL.NewStd(mysql.ErrInvalidDDLJobVersion)
	errInvalidUseOfNull                       = dbterror.ClassDDL.NewStd(mysql.ErrInvalidUseOfNull)
	errTooManyFields                          = dbterror.ClassDDL.NewStd(mysql.ErrTooManyFields)
	errTooManyKeys                            = dbterror.ClassDDL.NewStd(mysql.ErrTooManyKeys)
	errInvalidSplitRegionRanges               = dbterror.ClassDDL.NewStd(mysql.ErrInvalidSplitRegionRanges)
	errReorgPanic                             = dbterror.ClassDDL.NewStd(mysql.ErrReorgPanic)
	errFkColumnCannotDrop                     = dbterror.ClassDDL.NewStd(mysql.ErrFkColumnCannotDrop)
	errFKIncompatibleColumns                  = dbterror.ClassDDL.NewStd(mysql.ErrFKIncompatibleColumns)

	errAlterReplicaForUnsupportedCharsetTable = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "ALTER table replica for table contain %s charset"), nil))

	errOnlyOnRangeListPartition = dbterror.ClassDDL.NewStd(mysql.ErrOnlyOnRangeListPartition)
	// errWrongKeyColumn is for table column cannot be indexed.
	errWrongKeyColumn = dbterror.ClassDDL.NewStd(mysql.ErrWrongKeyColumn)
	// errWrongKeyColumnFunctionalIndex is for expression cannot be indexed.
	errWrongKeyColumnFunctionalIndex = dbterror.ClassDDL.NewStd(mysql.ErrWrongKeyColumnFunctionalIndex)
	// errWrongFKOptionForGeneratedColumn is for wrong foreign key reference option on generated columns.
	errWrongFKOptionForGeneratedColumn = dbterror.ClassDDL.NewStd(mysql.ErrWrongFKOptionForGeneratedColumn)
	// ErrUnsupportedOnGeneratedColumn is for unsupported actions on generated columns.
	ErrUnsupportedOnGeneratedColumn = dbterror.ClassDDL.NewStd(mysql.ErrUnsupportedOnGeneratedColumn)
	// errGeneratedColumnNonPrior forbids to refer generated column non prior to it.
	errGeneratedColumnNonPrior = dbterror.ClassDDL.NewStd(mysql.ErrGeneratedColumnNonPrior)
	// errDependentByGeneratedColumn forbids to delete columns which are dependent by generated columns.
	errDependentByGeneratedColumn = dbterror.ClassDDL.NewStd(mysql.ErrDependentByGeneratedColumn)
	// errJSONUsedAsKey forbids to use JSON as key or index.
	errJSONUsedAsKey = dbterror.ClassDDL.NewStd(mysql.ErrJSONUsedAsKey)
	// errBlobCantHaveDefault forbids to give not null default value to TEXT/BLOB/JSON.
	errBlobCantHaveDefault = dbterror.ClassDDL.NewStd(mysql.ErrBlobCantHaveDefault)
	errTooLongIndexComment = dbterror.ClassDDL.NewStd(mysql.ErrTooLongIndexComment)
	// ErrInvalidDefaultValue returns for invalid default value for columns.
	ErrInvalidDefaultValue = dbterror.ClassDDL.NewStd(mysql.ErrInvalidDefault)
	// ErrGeneratedColumnRefAutoInc forbids to refer generated columns to auto-increment columns .
	ErrGeneratedColumnRefAutoInc = dbterror.ClassDDL.NewStd(mysql.ErrGeneratedColumnRefAutoInc)
	// ErrExpressionIndexCanNotRefer forbids to refer expression index to auto-increment column.
	ErrExpressionIndexCanNotRefer = dbterror.ClassDDL.NewStd(mysql.ErrFunctionalIndexRefAutoIncrement)
	// ErrUnsupportedAddPartition returns for does not support add partitions.
	ErrUnsupportedAddPartition = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "add partitions"), nil))
	// ErrUnsupportedCoalescePartition returns for does not support coalesce partitions.
	ErrUnsupportedCoalescePartition   = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "coalesce partitions"), nil))
	errUnsupportedReorganizePartition = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "reorganize partition"), nil))
	errUnsupportedCheckPartition      = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "check partition"), nil))
	errUnsupportedOptimizePartition   = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "optimize partition"), nil))
	errUnsupportedRebuildPartition    = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "rebuild partition"), nil))
	errUnsupportedRemovePartition     = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "remove partitioning"), nil))
	errUnsupportedRepairPartition     = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "repair partition"), nil))
	// ErrGeneratedColumnFunctionIsNotAllowed returns for unsupported functions for generated columns.
	ErrGeneratedColumnFunctionIsNotAllowed = dbterror.ClassDDL.NewStd(mysql.ErrGeneratedColumnFunctionIsNotAllowed)
	// ErrGeneratedColumnRowValueIsNotAllowed returns for generated columns referring to row values.
	ErrGeneratedColumnRowValueIsNotAllowed = dbterror.ClassDDL.NewStd(mysql.ErrGeneratedColumnRowValueIsNotAllowed)
	// ErrUnsupportedPartitionByRangeColumns returns for does unsupported partition by range columns.
	ErrUnsupportedPartitionByRangeColumns = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "partition by range columns"), nil))
	// ErrFunctionalIndexFunctionIsNotAllowed returns for unsupported functions for functional index.
	ErrFunctionalIndexFunctionIsNotAllowed = dbterror.ClassDDL.NewStd(mysql.ErrFunctionalIndexFunctionIsNotAllowed)
	// ErrFunctionalIndexRowValueIsNotAllowed returns for functional index referring to row values.
	ErrFunctionalIndexRowValueIsNotAllowed = dbterror.ClassDDL.NewStd(mysql.ErrFunctionalIndexRowValueIsNotAllowed)
	errUnsupportedCreatePartition          = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "partition type, treat as normal table"), nil))
	errTablePartitionDisabled              = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message("Partitions are ignored because Table Partition is disabled, please set 'tidb_enable_table_partition' if you need to need to enable it", nil))
	errUnsupportedIndexType                = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "index type"), nil))
	errWindowInvalidWindowFuncUse          = dbterror.ClassDDL.NewStd(mysql.ErrWindowInvalidWindowFuncUse)

	// ErrDupKeyName returns for duplicated key name.
	ErrDupKeyName = dbterror.ClassDDL.NewStd(mysql.ErrDupKeyName)
	// ErrFkDupName returns for duplicated FK name.
	ErrFkDupName = dbterror.ClassDDL.NewStd(mysql.ErrFkDupName)
	// ErrInvalidDDLState returns for invalid ddl model object state.
	ErrInvalidDDLState = dbterror.ClassDDL.NewStdErr(mysql.ErrInvalidDDLState, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrInvalidDDLState].Raw), nil))
	// ErrUnsupportedModifyPrimaryKey returns an error when add or drop the primary key.
	// It's exported for testing.
	ErrUnsupportedModifyPrimaryKey = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "%s primary key"), nil))
	// ErrPKIndexCantBeInvisible return an error when primary key is invisible index
	ErrPKIndexCantBeInvisible = dbterror.ClassDDL.NewStd(mysql.ErrPKIndexCantBeInvisible)

	// ErrColumnBadNull returns for a bad null value.
	ErrColumnBadNull = dbterror.ClassDDL.NewStd(mysql.ErrBadNull)
	// ErrBadField forbids to refer to unknown column.
	ErrBadField = dbterror.ClassDDL.NewStd(mysql.ErrBadField)
	// ErrCantRemoveAllFields returns for deleting all columns.
	ErrCantRemoveAllFields = dbterror.ClassDDL.NewStd(mysql.ErrCantRemoveAllFields)
	// ErrCantDropFieldOrKey returns for dropping a non-existent field or key.
	ErrCantDropFieldOrKey = dbterror.ClassDDL.NewStd(mysql.ErrCantDropFieldOrKey)
	// ErrInvalidOnUpdate returns for invalid ON UPDATE clause.
	ErrInvalidOnUpdate = dbterror.ClassDDL.NewStd(mysql.ErrInvalidOnUpdate)
	// ErrTooLongIdent returns for too long name of database/table/column/index.
	ErrTooLongIdent = dbterror.ClassDDL.NewStd(mysql.ErrTooLongIdent)
	// ErrWrongDBName returns for wrong database name.
	ErrWrongDBName = dbterror.ClassDDL.NewStd(mysql.ErrWrongDBName)
	// ErrWrongTableName returns for wrong table name.
	ErrWrongTableName = dbterror.ClassDDL.NewStd(mysql.ErrWrongTableName)
	// ErrWrongColumnName returns for wrong column name.
	ErrWrongColumnName = dbterror.ClassDDL.NewStd(mysql.ErrWrongColumnName)
	// ErrWrongUsage returns for wrong ddl syntax usage.
	ErrWrongUsage = dbterror.ClassDDL.NewStd(mysql.ErrWrongUsage)
	// ErrInvalidGroupFuncUse returns for using invalid group functions.
	ErrInvalidGroupFuncUse = dbterror.ClassDDL.NewStd(mysql.ErrInvalidGroupFuncUse)
	// ErrTableMustHaveColumns returns for missing column when creating a table.
	ErrTableMustHaveColumns = dbterror.ClassDDL.NewStd(mysql.ErrTableMustHaveColumns)
	// ErrWrongNameForIndex returns for wrong index name.
	ErrWrongNameForIndex = dbterror.ClassDDL.NewStd(mysql.ErrWrongNameForIndex)
	// ErrUnknownCharacterSet returns unknown character set.
	ErrUnknownCharacterSet = dbterror.ClassDDL.NewStd(mysql.ErrUnknownCharacterSet)
	// ErrUnknownCollation returns unknown collation.
	ErrUnknownCollation = dbterror.ClassDDL.NewStd(mysql.ErrUnknownCollation)
	// ErrCollationCharsetMismatch returns when collation not match the charset.
	ErrCollationCharsetMismatch = dbterror.ClassDDL.NewStd(mysql.ErrCollationCharsetMismatch)
	// ErrConflictingDeclarations return conflict declarations.
	ErrConflictingDeclarations = dbterror.ClassDDL.NewStdErr(mysql.ErrConflictingDeclarations, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrConflictingDeclarations].Raw, "CHARACTER SET ", "%s", "CHARACTER SET ", "%s"), nil))
	// ErrPrimaryCantHaveNull returns All parts of a PRIMARY KEY must be NOT NULL; if you need NULL in a key, use UNIQUE instead
	ErrPrimaryCantHaveNull = dbterror.ClassDDL.NewStd(mysql.ErrPrimaryCantHaveNull)
	// ErrErrorOnRename returns error for wrong database name in alter table rename
	ErrErrorOnRename = dbterror.ClassDDL.NewStd(mysql.ErrErrorOnRename)
	// ErrViewSelectClause returns error for create view with select into clause
	ErrViewSelectClause = dbterror.ClassDDL.NewStd(mysql.ErrViewSelectClause)

	// ErrNotAllowedTypeInPartition returns not allowed type error when creating table partition with unsupported expression type.
	ErrNotAllowedTypeInPartition = dbterror.ClassDDL.NewStd(mysql.ErrFieldTypeNotAllowedAsPartitionField)
	// ErrPartitionMgmtOnNonpartitioned returns it's not a partition table.
	ErrPartitionMgmtOnNonpartitioned = dbterror.ClassDDL.NewStd(mysql.ErrPartitionMgmtOnNonpartitioned)
	// ErrDropPartitionNonExistent returns error in list of partition.
	ErrDropPartitionNonExistent = dbterror.ClassDDL.NewStd(mysql.ErrDropPartitionNonExistent)
	// ErrSameNamePartition returns duplicate partition name.
	ErrSameNamePartition = dbterror.ClassDDL.NewStd(mysql.ErrSameNamePartition)
	// ErrSameNamePartitionField returns duplicate partition field.
	ErrSameNamePartitionField = dbterror.ClassDDL.NewStd(mysql.ErrSameNamePartitionField)
	// ErrRangeNotIncreasing returns values less than value must be strictly increasing for each partition.
	ErrRangeNotIncreasing = dbterror.ClassDDL.NewStd(mysql.ErrRangeNotIncreasing)
	// ErrPartitionMaxvalue returns maxvalue can only be used in last partition definition.
	ErrPartitionMaxvalue = dbterror.ClassDDL.NewStd(mysql.ErrPartitionMaxvalue)
	// ErrDropLastPartition returns cannot remove all partitions, use drop table instead.
	ErrDropLastPartition = dbterror.ClassDDL.NewStd(mysql.ErrDropLastPartition)
	// ErrTooManyPartitions returns too many partitions were defined.
	ErrTooManyPartitions = dbterror.ClassDDL.NewStd(mysql.ErrTooManyPartitions)
	// ErrPartitionConstDomain returns partition constant is out of partition function domain.
	ErrPartitionConstDomain = dbterror.ClassDDL.NewStd(mysql.ErrPartitionConstDomain)
	// ErrPartitionFunctionIsNotAllowed returns this partition function is not allowed.
	ErrPartitionFunctionIsNotAllowed = dbterror.ClassDDL.NewStd(mysql.ErrPartitionFunctionIsNotAllowed)
	// ErrPartitionFuncNotAllowed returns partition function returns the wrong type.
	ErrPartitionFuncNotAllowed = dbterror.ClassDDL.NewStd(mysql.ErrPartitionFuncNotAllowed)
	// ErrUniqueKeyNeedAllFieldsInPf returns must include all columns in the table's partitioning function.
	ErrUniqueKeyNeedAllFieldsInPf = dbterror.ClassDDL.NewStd(mysql.ErrUniqueKeyNeedAllFieldsInPf)
	// ErrWrongExprInPartitionFunc Constant, random or timezone-dependent expressions in (sub)partitioning function are not allowed.
	ErrWrongExprInPartitionFunc = dbterror.ClassDDL.NewStd(mysql.ErrWrongExprInPartitionFunc)
	// ErrWarnDataTruncated returns data truncated error.
	ErrWarnDataTruncated = dbterror.ClassDDL.NewStd(mysql.WarnDataTruncated)
	// ErrCoalesceOnlyOnHashPartition returns coalesce partition can only be used on hash/key partitions.
	ErrCoalesceOnlyOnHashPartition = dbterror.ClassDDL.NewStd(mysql.ErrCoalesceOnlyOnHashPartition)
	// ErrViewWrongList returns create view must include all columns in the select clause
	ErrViewWrongList = dbterror.ClassDDL.NewStd(mysql.ErrViewWrongList)
	// ErrAlterOperationNotSupported returns when alter operations is not supported.
	ErrAlterOperationNotSupported = dbterror.ClassDDL.NewStd(mysql.ErrAlterOperationNotSupportedReason)
	// ErrWrongObject returns for wrong object.
	ErrWrongObject = dbterror.ClassDDL.NewStd(mysql.ErrWrongObject)
	// ErrTableCantHandleFt returns FULLTEXT keys are not supported by table type
	ErrTableCantHandleFt = dbterror.ClassDDL.NewStd(mysql.ErrTableCantHandleFt)
	// ErrFieldNotFoundPart returns an error when 'partition by columns' are not found in table columns.
	ErrFieldNotFoundPart = dbterror.ClassDDL.NewStd(mysql.ErrFieldNotFoundPart)
	// ErrWrongTypeColumnValue returns 'Partition column values of incorrect type'
	ErrWrongTypeColumnValue = dbterror.ClassDDL.NewStd(mysql.ErrWrongTypeColumnValue)
	// ErrValuesIsNotIntType returns 'VALUES value for partition '%-.64s' must have type INT'
	ErrValuesIsNotIntType = dbterror.ClassDDL.NewStd(mysql.ErrValuesIsNotIntType)
	// ErrFunctionalIndexPrimaryKey returns 'The primary key cannot be a functional index'
	ErrFunctionalIndexPrimaryKey = dbterror.ClassDDL.NewStd(mysql.ErrFunctionalIndexPrimaryKey)
	// ErrFunctionalIndexOnField returns 'Functional index on a column is not supported. Consider using a regular index instead'
	ErrFunctionalIndexOnField = dbterror.ClassDDL.NewStd(mysql.ErrFunctionalIndexOnField)
	// ErrInvalidAutoRandom returns when auto_random is used incorrectly.
	ErrInvalidAutoRandom = dbterror.ClassDDL.NewStd(mysql.ErrInvalidAutoRandom)
	// ErrUnsupportedConstraintCheck returns when use ADD CONSTRAINT CHECK
	ErrUnsupportedConstraintCheck = dbterror.ClassDDL.NewStd(mysql.ErrUnsupportedConstraintCheck)
	// ErrDerivedMustHaveAlias returns when a sub select statement does not have a table alias.
	ErrDerivedMustHaveAlias = dbterror.ClassDDL.NewStd(mysql.ErrDerivedMustHaveAlias)

	// ErrSequenceRunOut returns when the sequence has been run out.
	ErrSequenceRunOut = dbterror.ClassDDL.NewStd(mysql.ErrSequenceRunOut)
	// ErrSequenceInvalidData returns when sequence values are conflicting.
	ErrSequenceInvalidData = dbterror.ClassDDL.NewStd(mysql.ErrSequenceInvalidData)
	// ErrSequenceAccessFail returns when sequences are not able to access.
	ErrSequenceAccessFail = dbterror.ClassDDL.NewStd(mysql.ErrSequenceAccessFail)
	// ErrNotSequence returns when object is not a sequence.
	ErrNotSequence = dbterror.ClassDDL.NewStd(mysql.ErrNotSequence)
	// ErrUnknownSequence returns when drop / alter unknown sequence.
	ErrUnknownSequence = dbterror.ClassDDL.NewStd(mysql.ErrUnknownSequence)
	// ErrSequenceUnsupportedTableOption returns when unsupported table option exists in sequence.
	ErrSequenceUnsupportedTableOption = dbterror.ClassDDL.NewStd(mysql.ErrSequenceUnsupportedTableOption)
	// ErrColumnTypeUnsupportedNextValue is returned when sequence next value is assigned to unsupported column type.
	ErrColumnTypeUnsupportedNextValue = dbterror.ClassDDL.NewStd(mysql.ErrColumnTypeUnsupportedNextValue)
	// ErrAddColumnWithSequenceAsDefault is returned when the new added column with sequence's nextval as it's default value.
	ErrAddColumnWithSequenceAsDefault = dbterror.ClassDDL.NewStd(mysql.ErrAddColumnWithSequenceAsDefault)
	// ErrUnsupportedExpressionIndex is returned when create an expression index without allow-expression-index.
	ErrUnsupportedExpressionIndex = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "creating expression index containing unsafe functions without allow-expression-index in config"), nil))
	// ErrPartitionExchangePartTable is returned when exchange table partition with another table is partitioned.
	ErrPartitionExchangePartTable = dbterror.ClassDDL.NewStd(mysql.ErrPartitionExchangePartTable)
	// ErrTablesDifferentMetadata is returned when exchanges tables is not compatible.
	ErrTablesDifferentMetadata = dbterror.ClassDDL.NewStd(mysql.ErrTablesDifferentMetadata)
	// ErrRowDoesNotMatchPartition is returned when the row record of exchange table does not match the partition rule.
	ErrRowDoesNotMatchPartition = dbterror.ClassDDL.NewStd(mysql.ErrRowDoesNotMatchPartition)
	// ErrPartitionExchangeForeignKey is returned when exchanged normal table has foreign keys.
	ErrPartitionExchangeForeignKey = dbterror.ClassDDL.NewStd(mysql.ErrPartitionExchangeForeignKey)
	// ErrCheckNoSuchTable is returned when exchanged normal table is view or sequence.
	ErrCheckNoSuchTable         = dbterror.ClassDDL.NewStd(mysql.ErrCheckNoSuchTable)
	errUnsupportedPartitionType = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message(fmt.Sprintf(mysql.MySQLErrName[mysql.ErrUnsupportedDDLOperation].Raw, "partition type of table %s when exchanging partition"), nil))
	// ErrPartitionExchangeDifferentOption is returned when attribute does not match between partition table and normal table.
	ErrPartitionExchangeDifferentOption = dbterror.ClassDDL.NewStd(mysql.ErrPartitionExchangeDifferentOption)
	// ErrTableOptionUnionUnsupported is returned when create/alter table with union option.
	ErrTableOptionUnionUnsupported = dbterror.ClassDDL.NewStd(mysql.ErrTableOptionUnionUnsupported)
	// ErrTableOptionInsertMethodUnsupported is returned when create/alter table with insert method option.
	ErrTableOptionInsertMethodUnsupported = dbterror.ClassDDL.NewStd(mysql.ErrTableOptionInsertMethodUnsupported)

	// ErrInvalidPlacementPolicyCheck is returned when txn_scope and commit data changing do not meet the placement policy
	ErrInvalidPlacementPolicyCheck = dbterror.ClassDDL.NewStd(mysql.ErrPlacementPolicyCheck)

	// ErrPlacementPolicyWithDirectOption is returned when create/alter table with both placement policy and placement options existed.
	ErrPlacementPolicyWithDirectOption = dbterror.ClassDDL.NewStd(mysql.ErrPlacementPolicyWithDirectOption)

	// ErrPlacementPolicyInUse is returned when placement policy is in use in drop/alter.
	ErrPlacementPolicyInUse = dbterror.ClassDDL.NewStd(mysql.ErrPlacementPolicyInUse)

	// ErrMultipleDefConstInListPart returns multiple definition of same constant in list partitioning.
	ErrMultipleDefConstInListPart = dbterror.ClassDDL.NewStd(mysql.ErrMultipleDefConstInListPart)

	// ErrTruncatedWrongValue is returned when data has been truncated during conversion.
	ErrTruncatedWrongValue = dbterror.ClassDDL.NewStd(mysql.ErrTruncatedWrongValue)

	// ErrWarnDataOutOfRange is returned when the value in a numeric column that is outside the permissible range of the column data type.
	// See https://dev.mysql.com/doc/refman/5.5/en/out-of-range-and-overflow.html for details
	ErrWarnDataOutOfRange = dbterror.ClassDDL.NewStd(mysql.ErrWarnDataOutOfRange)

	// ErrTooLongValueForType is returned when the individual enum element length is too long.
	ErrTooLongValueForType = dbterror.ClassDDL.NewStd(mysql.ErrTooLongValueForType)

	// ErrUnknownEngine is returned when the table engine is unknown.
	ErrUnknownEngine = dbterror.ClassDDL.NewStd(mysql.ErrUnknownStorageEngine)

	errExchangePartitionDisabled = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message("Exchange Partition is disabled, please set 'tidb_enable_exchange_partition' if you need to need to enable it", nil))

	// ErrPartitionNoTemporary returns when partition at temporary mode
	ErrPartitionNoTemporary = dbterror.ClassDDL.NewStd(mysql.ErrPartitionNoTemporary)

	// ErrOptOnTemporaryTable returns when exec unsupported opt at temporary mode
	ErrOptOnTemporaryTable = dbterror.ClassDDL.NewStd(mysql.ErrOptOnTemporaryTable)
	// ErrOptOnCacheTable returns when exec unsupported opt at cache mode
	ErrOptOnCacheTable                  = dbterror.ClassDDL.NewStd(mysql.ErrOptOnCacheTable)
	errUnsupportedOnCommitPreserve      = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message("TiDB doesn't support ON COMMIT PRESERVE ROWS for now", nil))
	errUnsupportedClusteredSecondaryKey = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message("CLUSTERED/NONCLUSTERED keyword is only supported for primary key", nil))

	// ErrUnsupportedLocalTempTableDDL returns when ddl operation unsupported for local temporary table
	ErrUnsupportedLocalTempTableDDL = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message("TiDB doesn't support %s for local temporary table", nil))
	// ErrInvalidAttributesSpec is returned when meeting invalid attributes.
	ErrInvalidAttributesSpec = dbterror.ClassDDL.NewStd(mysql.ErrInvalidAttributesSpec)
	// errFunctionalIndexOnJSONOrGeometryFunction returns when creating expression index and the type of the expression is JSON.
	errFunctionalIndexOnJSONOrGeometryFunction = dbterror.ClassDDL.NewStd(mysql.ErrFunctionalIndexOnJSONOrGeometryFunction)
	// errDependentByFunctionalIndex returns when the dropped column depends by expression index.
	errDependentByFunctionalIndex = dbterror.ClassDDL.NewStd(mysql.ErrDependentByFunctionalIndex)
	// errFunctionalIndexOnBlob when the expression of expression index returns blob or text.
	errFunctionalIndexOnBlob = dbterror.ClassDDL.NewStd(mysql.ErrFunctionalIndexOnBlob)
	// ErrIncompatibleTiFlashAndPlacement when placement and tiflash replica options are set at the same time
	ErrIncompatibleTiFlashAndPlacement = dbterror.ClassDDL.NewStdErr(mysql.ErrUnsupportedDDLOperation, parser_mysql.Message("Placement and tiflash replica options cannot be set at the same time", nil))

	// ErrAutoConvert when auto convert happens
	ErrAutoConvert = dbterror.ClassDDL.NewStd(mysql.ErrAutoConvert)
)
