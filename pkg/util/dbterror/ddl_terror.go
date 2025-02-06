// Copyright 2022 PingCAP, Inc.
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

package dbterror

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/terror"
)

var (
	// ErrInvalidWorker means the worker is invalid.
	ErrInvalidWorker = ClassDDL.NewStd(errno.ErrInvalidDDLWorker)
	// ErrNotOwner means we are not owner and can't handle DDL jobs.
	ErrNotOwner = ClassDDL.NewStd(errno.ErrNotOwner)
	// ErrCantDecodeRecord means we can't decode the record.
	ErrCantDecodeRecord = ClassDDL.NewStd(errno.ErrCantDecodeRecord)
	// ErrInvalidDDLJob means the DDL job is invalid.
	ErrInvalidDDLJob = ClassDDL.NewStd(errno.ErrInvalidDDLJob)
	// ErrCancelledDDLJob means the DDL job is cancelled.
	ErrCancelledDDLJob = ClassDDL.NewStd(errno.ErrCancelledDDLJob)
	// ErrPausedDDLJob returns when the DDL job cannot be paused.
	ErrPausedDDLJob = ClassDDL.NewStd(errno.ErrPausedDDLJob)
	// ErrBDRRestrictedDDL means the DDL is restricted in BDR mode.
	ErrBDRRestrictedDDL = ClassDDL.NewStd(errno.ErrBDRRestrictedDDL)
	// ErrRunMultiSchemaChanges means we run multi schema changes.
	ErrRunMultiSchemaChanges = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "multi schema change for %s"), nil))
	// ErrOperateSameColumn means we change the same columns multiple times in a DDL.
	ErrOperateSameColumn = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "operate same column '%s'"), nil))
	// ErrOperateSameIndex means we change the same indexes multiple times in a DDL.
	ErrOperateSameIndex = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "operate same index '%s'"), nil))
	// ErrWaitReorgTimeout means we wait for reorganization timeout.
	ErrWaitReorgTimeout = ClassDDL.NewStdErr(errno.ErrLockWaitTimeout, errno.MySQLErrName[errno.ErrWaitReorgTimeout])
	// ErrInvalidStoreVer means invalid store version.
	ErrInvalidStoreVer = ClassDDL.NewStd(errno.ErrInvalidStoreVersion)
	// ErrRepairTableFail is used to repair tableInfo in repair mode.
	ErrRepairTableFail = ClassDDL.NewStd(errno.ErrRepairTable)

	// ErrUnsupportedAddVectorIndex means add vector index is unsupported
	ErrUnsupportedAddVectorIndex = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "add vector index: %s"), nil))
	// ErrCantDropColWithIndex means can't drop the column with index. We don't support dropping column with index covered now.
	ErrCantDropColWithIndex = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "drop column with index"), nil))
	// ErrCantDropColWithAutoInc means can't drop column with auto_increment
	ErrCantDropColWithAutoInc = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "can't remove column with auto_increment when @@tidb_allow_remove_auto_inc disabled"), nil))
	// ErrCantDropColWithCheckConstraint means can't drop column with check constraint
	ErrCantDropColWithCheckConstraint = ClassDDL.NewStd(errno.ErrDependentByCheckConstraint)
	// ErrUnsupportedEngineAttribute means engine attribute option is unsupported
	ErrUnsupportedEngineAttribute = ClassDDL.NewStd(errno.ErrEngineAttributeNotSupported)
	// ErrUnsupportedAddColumn means add columns is unsupported
	ErrUnsupportedAddColumn = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "add column"), nil))
	// ErrUnsupportedModifyColumn means modify columns is unsupoorted
	ErrUnsupportedModifyColumn = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "modify column: %s"), nil))
	// ErrUnsupportedModifyCharset means modify charset is unsupoorted
	ErrUnsupportedModifyCharset = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "modify %s"), nil))
	// ErrUnsupportedModifyCollation means modify collation is unsupoorted
	ErrUnsupportedModifyCollation = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "modifying collation from %s to %s"), nil))
	// ErrUnsupportedPKHandle is used to indicate that we can't support this PK handle.
	ErrUnsupportedPKHandle = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "drop integer primary key"), nil))
	// ErrUnsupportedCharset means we don't support the charset.
	ErrUnsupportedCharset = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "charset %s and collate %s"), nil))
	// ErrUnsupportedShardRowIDBits means we don't support the shard_row_id_bits.
	ErrUnsupportedShardRowIDBits = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "shard_row_id_bits for table with primary key as row id"), nil))
	// ErrUnsupportedAlterTableWithValidation means we don't support the alter table with validation.
	ErrUnsupportedAlterTableWithValidation = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message("ALTER TABLE WITH VALIDATION is currently unsupported", nil))
	// ErrUnsupportedAlterTableWithoutValidation means we don't support the alter table without validation.
	ErrUnsupportedAlterTableWithoutValidation = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message("ALTER TABLE WITHOUT VALIDATION is currently unsupported", nil))
	// ErrUnsupportedAlterTableOption means we don't support the alter table option.
	ErrUnsupportedAlterTableOption = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message("This type of ALTER TABLE is currently unsupported", nil))
	// ErrUnsupportedAlterCacheForSysTable means we don't support the alter cache for system table.
	ErrUnsupportedAlterCacheForSysTable = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message("ALTER table cache for tables in system database is currently unsupported", nil))
	// ErrBlobKeyWithoutLength is used when BLOB is used as key but without a length.
	ErrBlobKeyWithoutLength = ClassDDL.NewStd(errno.ErrBlobKeyWithoutLength)
	// ErrKeyPart0 is used when key parts length is 0.
	ErrKeyPart0 = ClassDDL.NewStd(errno.ErrKeyPart0)
	// ErrIncorrectPrefixKey is used when the prefix length is incorrect for a string key.
	ErrIncorrectPrefixKey = ClassDDL.NewStd(errno.ErrWrongSubKey)
	// ErrTooLongKey is used when the column key is too long.
	ErrTooLongKey = ClassDDL.NewStd(errno.ErrTooLongKey)
	// ErrKeyColumnDoesNotExits is used when the key column doesn't exist.
	ErrKeyColumnDoesNotExits = ClassDDL.NewStd(errno.ErrKeyColumnDoesNotExits)
	// ErrInvalidDDLJobVersion is used when the DDL job version is invalid.
	ErrInvalidDDLJobVersion = ClassDDL.NewStd(errno.ErrInvalidDDLJobVersion)
	// ErrInvalidUseOfNull is used when the column is not null.
	ErrInvalidUseOfNull = ClassDDL.NewStd(errno.ErrInvalidUseOfNull)
	// ErrTooManyFields is used when too many columns are used in a select statement.
	ErrTooManyFields = ClassDDL.NewStd(errno.ErrTooManyFields)
	// ErrTooManyKeys is used when too many keys used.
	ErrTooManyKeys = ClassDDL.NewStd(errno.ErrTooManyKeys)
	// ErrInvalidSplitRegionRanges is used when split region ranges is invalid.
	ErrInvalidSplitRegionRanges = ClassDDL.NewStd(errno.ErrInvalidSplitRegionRanges)
	// ErrReorgPanic is used when reorg process is panic.
	ErrReorgPanic = ClassDDL.NewStd(errno.ErrReorgPanic)
	// ErrFkColumnCannotDrop is used when foreign key column can't be dropped.
	ErrFkColumnCannotDrop = ClassDDL.NewStd(errno.ErrFkColumnCannotDrop)
	// ErrFkColumnCannotDropChild is used when foreign key column can't be dropped.
	ErrFkColumnCannotDropChild = ClassDDL.NewStd(errno.ErrFkColumnCannotDropChild)
	// ErrFKIncompatibleColumns is used when foreign key column type is incompatible.
	ErrFKIncompatibleColumns = ClassDDL.NewStd(errno.ErrFKIncompatibleColumns)
	// ErrOnlyOnRangeListPartition is used when the partition type is range list.
	ErrOnlyOnRangeListPartition = ClassDDL.NewStd(errno.ErrOnlyOnRangeListPartition)
	// ErrWrongKeyColumn is for table column cannot be indexed.
	ErrWrongKeyColumn = ClassDDL.NewStd(errno.ErrWrongKeyColumn)
	// ErrWrongKeyColumnFunctionalIndex is for expression cannot be indexed.
	ErrWrongKeyColumnFunctionalIndex = ClassDDL.NewStd(errno.ErrWrongKeyColumnFunctionalIndex)
	// ErrWrongFKOptionForGeneratedColumn is for wrong foreign key reference option on generated columns.
	ErrWrongFKOptionForGeneratedColumn = ClassDDL.NewStd(errno.ErrWrongFKOptionForGeneratedColumn)
	// ErrUnsupportedOnGeneratedColumn is for unsupported actions on generated columns.
	ErrUnsupportedOnGeneratedColumn = ClassDDL.NewStd(errno.ErrUnsupportedOnGeneratedColumn)
	// ErrGeneratedColumnNonPrior forbids to refer generated column non prior to it.
	ErrGeneratedColumnNonPrior = ClassDDL.NewStd(errno.ErrGeneratedColumnNonPrior)
	// ErrDependentByGeneratedColumn forbids to delete columns which are dependent by generated columns.
	ErrDependentByGeneratedColumn = ClassDDL.NewStd(errno.ErrDependentByGeneratedColumn)
	// ErrJSONUsedAsKey forbids to use JSON as key or index.
	ErrJSONUsedAsKey = ClassDDL.NewStd(errno.ErrJSONUsedAsKey)
	// ErrBlobCantHaveDefault forbids to give not null default value to TEXT/BLOB/JSON.
	ErrBlobCantHaveDefault = ClassDDL.NewStd(errno.ErrBlobCantHaveDefault)
	// ErrTooLongIndexComment means the comment for index is too long.
	ErrTooLongIndexComment = ClassDDL.NewStd(errno.ErrTooLongIndexComment)
	// ErrTooLongTableComment means the comment for table is too long.
	ErrTooLongTableComment = ClassDDL.NewStd(errno.ErrTooLongTableComment)
	// ErrTooLongFieldComment means the comment for field/column is too long.
	ErrTooLongFieldComment = ClassDDL.NewStd(errno.ErrTooLongFieldComment)
	// ErrTooLongTablePartitionComment means the comment for table partition is too long.
	ErrTooLongTablePartitionComment = ClassDDL.NewStd(errno.ErrTooLongTablePartitionComment)
	// ErrInvalidDefaultValue returns for invalid default value for columns.
	ErrInvalidDefaultValue = ClassDDL.NewStd(errno.ErrInvalidDefault)
	// ErrDefValGeneratedNamedFunctionIsNotAllowed returns for disallowed function as default value expression of column.
	ErrDefValGeneratedNamedFunctionIsNotAllowed = ClassDDL.NewStd(errno.ErrDefValGeneratedNamedFunctionIsNotAllowed)
	// ErrGeneratedColumnRefAutoInc forbids to refer generated columns to auto-increment columns .
	ErrGeneratedColumnRefAutoInc = ClassDDL.NewStd(errno.ErrGeneratedColumnRefAutoInc)
	// ErrExpressionIndexCanNotRefer forbids to refer expression index to auto-increment column.
	ErrExpressionIndexCanNotRefer = ClassDDL.NewStd(errno.ErrFunctionalIndexRefAutoIncrement)
	// ErrUnsupportedAddPartition returns for does not support add partitions.
	ErrUnsupportedAddPartition = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "add partitions"), nil))
	// ErrUnsupportedCoalescePartition returns for does not support coalesce partitions.
	ErrUnsupportedCoalescePartition = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "coalesce partitions"), nil))
	// ErrUnsupportedReorganizePartition returns for does not support reorganize partitions.
	ErrUnsupportedReorganizePartition = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "reorganize partition"), nil))
	// ErrUnsupportedCheckPartition returns for does not support check partitions.
	ErrUnsupportedCheckPartition = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "check partition"), nil))
	// ErrUnsupportedOptimizePartition returns for does not support optimize partitions.
	ErrUnsupportedOptimizePartition = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "optimize partition"), nil))
	// ErrUnsupportedRebuildPartition returns for does not support rebuild partitions.
	ErrUnsupportedRebuildPartition = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "rebuild partition"), nil))
	// ErrUnsupportedRemovePartition returns for does not support remove partitions.
	ErrUnsupportedRemovePartition = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "remove partitioning"), nil))
	// ErrUnsupportedRepairPartition returns for does not support repair partitions.
	ErrUnsupportedRepairPartition = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "repair partition"), nil))
	// ErrGeneratedColumnFunctionIsNotAllowed returns for unsupported functions for generated columns.
	ErrGeneratedColumnFunctionIsNotAllowed = ClassDDL.NewStd(errno.ErrGeneratedColumnFunctionIsNotAllowed)
	// ErrGeneratedColumnRowValueIsNotAllowed returns for generated columns referring to row values.
	ErrGeneratedColumnRowValueIsNotAllowed = ClassDDL.NewStd(errno.ErrGeneratedColumnRowValueIsNotAllowed)
	// ErrUnsupportedPartitionByRangeColumns returns for does unsupported partition by range columns.
	ErrUnsupportedPartitionByRangeColumns = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "partition by range columns"), nil))
	// ErrFunctionalIndexFunctionIsNotAllowed returns for unsupported functions for functional index.
	ErrFunctionalIndexFunctionIsNotAllowed = ClassDDL.NewStd(errno.ErrFunctionalIndexFunctionIsNotAllowed)
	// ErrFunctionalIndexRowValueIsNotAllowed returns for functional index referring to row values.
	ErrFunctionalIndexRowValueIsNotAllowed = ClassDDL.NewStd(errno.ErrFunctionalIndexRowValueIsNotAllowed)
	// ErrUnsupportedCreatePartition returns for does not support create partitions.
	ErrUnsupportedCreatePartition = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "partition type, treat as normal table"), nil))
	// ErrUnsupportedIndexType returns for unsupported index type.
	ErrUnsupportedIndexType = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "index type"), nil))
	// ErrWindowInvalidWindowFuncUse returns for invalid window function use.
	ErrWindowInvalidWindowFuncUse = ClassDDL.NewStd(errno.ErrWindowInvalidWindowFuncUse)

	// ErrDupKeyName returns for duplicated key name.
	ErrDupKeyName = ClassDDL.NewStd(errno.ErrDupKeyName)
	// ErrFkDupName returns for duplicated FK name.
	ErrFkDupName = ClassDDL.NewStd(errno.ErrFkDupName)
	// ErrInvalidDDLState returns for invalid ddl model object state.
	ErrInvalidDDLState = ClassDDL.NewStdErr(errno.ErrInvalidDDLState, errno.Message(errno.MySQLErrName[errno.ErrInvalidDDLState].Raw, nil))
	// ErrUnsupportedModifyPrimaryKey returns an error when add or drop the primary key.
	// It's exported for testing.
	ErrUnsupportedModifyPrimaryKey = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "%s primary key"), nil))
	// ErrPKIndexCantBeInvisible return an error when primary key is invisible index
	ErrPKIndexCantBeInvisible = ClassDDL.NewStd(errno.ErrPKIndexCantBeInvisible)

	// ErrColumnBadNull returns for a bad null value.
	ErrColumnBadNull = ClassDDL.NewStd(errno.ErrBadNull)
	// ErrBadField forbids to refer to unknown column.
	ErrBadField = ClassDDL.NewStd(errno.ErrBadField)
	// ErrCantRemoveAllFields returns for deleting all columns.
	ErrCantRemoveAllFields = ClassDDL.NewStd(errno.ErrCantRemoveAllFields)
	// ErrCantDropFieldOrKey returns for dropping a non-existent field or key.
	ErrCantDropFieldOrKey = ClassDDL.NewStd(errno.ErrCantDropFieldOrKey)
	// ErrInvalidOnUpdate returns for invalid ON UPDATE clause.
	ErrInvalidOnUpdate = ClassDDL.NewStd(errno.ErrInvalidOnUpdate)
	// ErrTooLongIdent returns for too long name of database/table/column/index.
	ErrTooLongIdent = ClassDDL.NewStd(errno.ErrTooLongIdent)
	// ErrWrongDBName returns for wrong database name.
	ErrWrongDBName = ClassDDL.NewStd(errno.ErrWrongDBName)
	// ErrWrongTableName returns for wrong table name.
	ErrWrongTableName = ClassDDL.NewStd(errno.ErrWrongTableName)
	// ErrWrongColumnName returns for wrong column name.
	ErrWrongColumnName = ClassDDL.NewStd(errno.ErrWrongColumnName)
	// ErrWrongPartitionName returns for wrong partition name.
	ErrWrongPartitionName = ClassDDL.NewStd(errno.ErrWrongPartitionName)
	// ErrWrongUsage returns for wrong ddl syntax usage.
	ErrWrongUsage = ClassDDL.NewStd(errno.ErrWrongUsage)
	// ErrInvalidGroupFuncUse returns for using invalid group functions.
	ErrInvalidGroupFuncUse = ClassDDL.NewStd(errno.ErrInvalidGroupFuncUse)
	// ErrTableMustHaveColumns returns for missing column when creating a table.
	ErrTableMustHaveColumns = ClassDDL.NewStd(errno.ErrTableMustHaveColumns)
	// ErrWrongNameForIndex returns for wrong index name.
	ErrWrongNameForIndex = ClassDDL.NewStd(errno.ErrWrongNameForIndex)
	// ErrUnknownCharacterSet returns unknown character set.
	ErrUnknownCharacterSet = ClassDDL.NewStd(errno.ErrUnknownCharacterSet)
	// ErrUnknownCollation returns unknown collation.
	ErrUnknownCollation = ClassDDL.NewStd(errno.ErrUnknownCollation)
	// ErrCollationCharsetMismatch returns when collation not match the charset.
	ErrCollationCharsetMismatch = ClassDDL.NewStd(errno.ErrCollationCharsetMismatch)
	// ErrConflictingDeclarations return conflict declarations.
	ErrConflictingDeclarations = ClassDDL.NewStdErr(errno.ErrConflictingDeclarations, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrConflictingDeclarations].Raw, "CHARACTER SET ", "%s", "CHARACTER SET ", "%s"), nil))
	// ErrPrimaryCantHaveNull returns All parts of a PRIMARY KEY must be NOT NULL; if you need NULL in a key, use UNIQUE instead
	ErrPrimaryCantHaveNull = ClassDDL.NewStd(errno.ErrPrimaryCantHaveNull)
	// ErrErrorOnRename returns error for wrong database name in alter table rename
	ErrErrorOnRename = ClassDDL.NewStd(errno.ErrErrorOnRename)
	// ErrViewSelectClause returns error for create view with select into clause
	ErrViewSelectClause = ClassDDL.NewStd(errno.ErrViewSelectClause)
	// ErrViewSelectVariable returns error for create view with select into clause
	ErrViewSelectVariable = ClassDDL.NewStd(errno.ErrViewSelectVariable)

	// ErrNotAllowedTypeInPartition returns not allowed type error when creating table partition with unsupported expression type.
	ErrNotAllowedTypeInPartition = ClassDDL.NewStd(errno.ErrFieldTypeNotAllowedAsPartitionField)
	// ErrPartitionMgmtOnNonpartitioned returns it's not a partition table.
	ErrPartitionMgmtOnNonpartitioned = ClassDDL.NewStd(errno.ErrPartitionMgmtOnNonpartitioned)
	// ErrDropPartitionNonExistent returns error in list of partition.
	ErrDropPartitionNonExistent = ClassDDL.NewStd(errno.ErrDropPartitionNonExistent)
	// ErrSameNamePartition returns duplicate partition name.
	ErrSameNamePartition = ClassDDL.NewStd(errno.ErrSameNamePartition)
	// ErrSameNamePartitionField returns duplicate partition field.
	ErrSameNamePartitionField = ClassDDL.NewStd(errno.ErrSameNamePartitionField)
	// ErrRangeNotIncreasing returns values less than value must be strictly increasing for each partition.
	ErrRangeNotIncreasing = ClassDDL.NewStd(errno.ErrRangeNotIncreasing)
	// ErrPartitionMaxvalue returns maxvalue can only be used in last partition definition.
	ErrPartitionMaxvalue = ClassDDL.NewStd(errno.ErrPartitionMaxvalue)
	// ErrMaxvalueInValuesIn returns maxvalue cannot be used in values in.
	ErrMaxvalueInValuesIn = ClassDDL.NewStd(errno.ErrMaxvalueInValuesIn)
	// ErrDropLastPartition returns cannot remove all partitions, use drop table instead.
	ErrDropLastPartition = ClassDDL.NewStd(errno.ErrDropLastPartition)
	// ErrTooManyPartitions returns too many partitions were defined.
	ErrTooManyPartitions = ClassDDL.NewStd(errno.ErrTooManyPartitions)
	// ErrPartitionConstDomain returns partition constant is out of partition function domain.
	ErrPartitionConstDomain = ClassDDL.NewStd(errno.ErrPartitionConstDomain)
	// ErrPartitionFunctionIsNotAllowed returns this partition function is not allowed.
	ErrPartitionFunctionIsNotAllowed = ClassDDL.NewStd(errno.ErrPartitionFunctionIsNotAllowed)
	// ErrPartitionFuncNotAllowed returns partition function returns the wrong type.
	ErrPartitionFuncNotAllowed = ClassDDL.NewStd(errno.ErrPartitionFuncNotAllowed)
	// ErrUniqueKeyNeedAllFieldsInPf returns must include all columns in the table's partitioning function.
	ErrUniqueKeyNeedAllFieldsInPf = ClassDDL.NewStd(errno.ErrUniqueKeyNeedAllFieldsInPf)
	// ErrWrongExprInPartitionFunc Constant, random or timezone-dependent expressions in (sub)partitioning function are not allowed.
	ErrWrongExprInPartitionFunc = ClassDDL.NewStd(errno.ErrWrongExprInPartitionFunc)
	// ErrWarnDataTruncated returns data truncated error.
	ErrWarnDataTruncated = ClassDDL.NewStd(errno.WarnDataTruncated)
	// ErrCoalesceOnlyOnHashPartition returns coalesce partition can only be used on hash/key partitions.
	ErrCoalesceOnlyOnHashPartition = ClassDDL.NewStd(errno.ErrCoalesceOnlyOnHashPartition)
	// ErrViewWrongList returns create view must include all columns in the select clause
	ErrViewWrongList = ClassDDL.NewStd(errno.ErrViewWrongList)
	// ErrAlterOperationNotSupported returns when alter operations is not supported.
	ErrAlterOperationNotSupported = ClassDDL.NewStd(errno.ErrAlterOperationNotSupportedReason)
	// ErrWrongObject returns for wrong object.
	ErrWrongObject = ClassDDL.NewStd(errno.ErrWrongObject)
	// ErrTableCantHandleFt returns FULLTEXT keys are not supported by table type
	ErrTableCantHandleFt = ClassDDL.NewStd(errno.ErrTableCantHandleFt)
	// ErrFieldNotFoundPart returns an error when 'partition by columns' are not found in table columns.
	ErrFieldNotFoundPart = ClassDDL.NewStd(errno.ErrFieldNotFoundPart)
	// ErrWrongTypeColumnValue returns 'Partition column values of incorrect type'
	ErrWrongTypeColumnValue = ClassDDL.NewStd(errno.ErrWrongTypeColumnValue)
	// ErrValuesIsNotIntType returns 'VALUES value for partition '%-.64s' must have type INT'
	ErrValuesIsNotIntType = ClassDDL.NewStd(errno.ErrValuesIsNotIntType)
	// ErrFunctionalIndexPrimaryKey returns 'The primary key cannot be a functional index'
	ErrFunctionalIndexPrimaryKey = ClassDDL.NewStd(errno.ErrFunctionalIndexPrimaryKey)
	// ErrFunctionalIndexOnField returns 'Functional index on a column is not supported. Consider using a regular index instead'
	ErrFunctionalIndexOnField = ClassDDL.NewStd(errno.ErrFunctionalIndexOnField)
	// ErrInvalidAutoRandom returns when auto_random is used incorrectly.
	ErrInvalidAutoRandom = ClassDDL.NewStd(errno.ErrInvalidAutoRandom)
	// ErrUnsupportedConstraintCheck returns when use ADD CONSTRAINT CHECK
	ErrUnsupportedConstraintCheck = ClassDDL.NewStd(errno.ErrUnsupportedConstraintCheck)
	// ErrDerivedMustHaveAlias returns when a sub select statement does not have a table alias.
	ErrDerivedMustHaveAlias = ClassDDL.NewStd(errno.ErrDerivedMustHaveAlias)
	// ErrNullInValuesLessThan returns when a range partition LESS THAN expression includes a NULL
	ErrNullInValuesLessThan = ClassDDL.NewStd(errno.ErrNullInValuesLessThan)

	// ErrSequenceRunOut returns when the sequence has been run out.
	ErrSequenceRunOut = ClassDDL.NewStd(errno.ErrSequenceRunOut)
	// ErrSequenceInvalidData returns when sequence values are conflicting.
	ErrSequenceInvalidData = ClassDDL.NewStd(errno.ErrSequenceInvalidData)
	// ErrSequenceAccessFail returns when sequences are not able to access.
	ErrSequenceAccessFail = ClassDDL.NewStd(errno.ErrSequenceAccessFail)
	// ErrNotSequence returns when object is not a sequence.
	ErrNotSequence = ClassDDL.NewStd(errno.ErrNotSequence)
	// ErrUnknownSequence returns when drop / alter unknown sequence.
	ErrUnknownSequence = ClassDDL.NewStd(errno.ErrUnknownSequence)
	// ErrSequenceUnsupportedTableOption returns when unsupported table option exists in sequence.
	ErrSequenceUnsupportedTableOption = ClassDDL.NewStd(errno.ErrSequenceUnsupportedTableOption)
	// ErrColumnTypeUnsupportedNextValue is returned when sequence next value is assigned to unsupported column type.
	ErrColumnTypeUnsupportedNextValue = ClassDDL.NewStd(errno.ErrColumnTypeUnsupportedNextValue)
	// ErrAddColumnWithSequenceAsDefault is returned when the new added column with sequence's nextval as it's default value.
	ErrAddColumnWithSequenceAsDefault = ClassDDL.NewStd(errno.ErrAddColumnWithSequenceAsDefault)
	// ErrUnsupportedExpressionIndex is returned when create an expression index without allow-expression-index.
	ErrUnsupportedExpressionIndex = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "creating expression index containing unsafe functions without allow-expression-index in config"), nil))
	// ErrPartitionExchangePartTable is returned when exchange table partition with another table is partitioned.
	ErrPartitionExchangePartTable = ClassDDL.NewStd(errno.ErrPartitionExchangePartTable)
	// ErrPartitionExchangeTempTable is returned when exchange table partition with a temporary table
	ErrPartitionExchangeTempTable = ClassDDL.NewStd(errno.ErrPartitionExchangeTempTable)
	// ErrTablesDifferentMetadata is returned when exchanges tables is not compatible.
	ErrTablesDifferentMetadata = ClassDDL.NewStd(errno.ErrTablesDifferentMetadata)
	// ErrRowDoesNotMatchPartition is returned when the row record of exchange table does not match the partition rule.
	ErrRowDoesNotMatchPartition = ClassDDL.NewStd(errno.ErrRowDoesNotMatchPartition)
	// ErrPartitionExchangeForeignKey is returned when exchanged normal table has foreign keys.
	ErrPartitionExchangeForeignKey = ClassDDL.NewStd(errno.ErrPartitionExchangeForeignKey)
	// ErrCheckNoSuchTable is returned when exchanged normal table is view or sequence.
	ErrCheckNoSuchTable = ClassDDL.NewStd(errno.ErrCheckNoSuchTable)
	// ErrUnsupportedPartitionType is returned when exchange table partition type is not supported.
	ErrUnsupportedPartitionType = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "partition type of table %s when exchanging partition"), nil))
	// ErrPartitionExchangeDifferentOption is returned when attribute does not match between partition table and normal table.
	ErrPartitionExchangeDifferentOption = ClassDDL.NewStd(errno.ErrPartitionExchangeDifferentOption)
	// ErrTableOptionUnionUnsupported is returned when create/alter table with union option.
	ErrTableOptionUnionUnsupported = ClassDDL.NewStd(errno.ErrTableOptionUnionUnsupported)
	// ErrTableOptionInsertMethodUnsupported is returned when create/alter table with insert method option.
	ErrTableOptionInsertMethodUnsupported = ClassDDL.NewStd(errno.ErrTableOptionInsertMethodUnsupported)

	// ErrInvalidPlacementPolicyCheck is returned when txn_scope and commit data changing do not meet the placement policy
	ErrInvalidPlacementPolicyCheck = ClassDDL.NewStd(errno.ErrPlacementPolicyCheck)

	// ErrPlacementPolicyWithDirectOption is returned when create/alter table with both placement policy and placement options existed.
	ErrPlacementPolicyWithDirectOption = ClassDDL.NewStd(errno.ErrPlacementPolicyWithDirectOption)

	// ErrPlacementPolicyInUse is returned when placement policy is in use in drop/alter.
	ErrPlacementPolicyInUse = ClassDDL.NewStd(errno.ErrPlacementPolicyInUse)

	// ErrMultipleDefConstInListPart returns multiple definition of same constant in list partitioning.
	ErrMultipleDefConstInListPart = ClassDDL.NewStd(errno.ErrMultipleDefConstInListPart)

	// ErrTruncatedWrongValue is returned when data has been truncated during conversion.
	ErrTruncatedWrongValue = ClassDDL.NewStd(errno.ErrTruncatedWrongValue)

	// ErrWarnDataOutOfRange is returned when the value in a numeric column that is outside the permissible range of the column data type.
	// See https://dev.mysql.com/doc/refman/5.5/en/out-of-range-and-overflow.html for details
	ErrWarnDataOutOfRange = ClassDDL.NewStd(errno.ErrWarnDataOutOfRange)

	// ErrTooLongValueForType is returned when the individual enum element length is too long.
	ErrTooLongValueForType = ClassDDL.NewStd(errno.ErrTooLongValueForType)

	// ErrUnknownEngine is returned when the table engine is unknown.
	ErrUnknownEngine = ClassDDL.NewStd(errno.ErrUnknownStorageEngine)

	// ErrExchangePartitionDisabled is returned when exchange partition is disabled.
	ErrExchangePartitionDisabled = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message("Exchange Partition is disabled, please set 'tidb_enable_exchange_partition' if you need to need to enable it", nil))

	// ErrPartitionNoTemporary returns when partition at temporary mode
	ErrPartitionNoTemporary = ClassDDL.NewStd(errno.ErrPartitionNoTemporary)

	// ErrOptOnTemporaryTable returns when exec unsupported opt at temporary mode
	ErrOptOnTemporaryTable = ClassDDL.NewStd(errno.ErrOptOnTemporaryTable)
	// ErrOptOnCacheTable returns when exec unsupported opt at cache mode
	ErrOptOnCacheTable = ClassDDL.NewStd(errno.ErrOptOnCacheTable)
	// ErrUnsupportedOnCommitPreserve returns when exec unsupported opt on commit preserve
	ErrUnsupportedOnCommitPreserve = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message("TiDB doesn't support ON COMMIT PRESERVE ROWS for now", nil))
	// ErrUnsupportedClusteredSecondaryKey returns when exec unsupported clustered secondary key
	ErrUnsupportedClusteredSecondaryKey = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message("CLUSTERED/NONCLUSTERED keyword is only supported for primary key", nil))

	// ErrUnsupportedLocalTempTableDDL returns when ddl operation unsupported for local temporary table
	ErrUnsupportedLocalTempTableDDL = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message("TiDB doesn't support %s for local temporary table", nil))
	// ErrInvalidAttributesSpec is returned when meeting invalid attributes.
	ErrInvalidAttributesSpec = ClassDDL.NewStd(errno.ErrInvalidAttributesSpec)
	// ErrFunctionalIndexOnJSONOrGeometryFunction returns when creating expression index and the type of the expression is JSON.
	ErrFunctionalIndexOnJSONOrGeometryFunction = ClassDDL.NewStd(errno.ErrFunctionalIndexOnJSONOrGeometryFunction)
	// ErrDependentByFunctionalIndex returns when the dropped column depends by expression index.
	ErrDependentByFunctionalIndex = ClassDDL.NewStd(errno.ErrDependentByFunctionalIndex)
	// ErrFunctionalIndexOnBlob when the expression of expression index returns blob or text.
	ErrFunctionalIndexOnBlob = ClassDDL.NewStd(errno.ErrFunctionalIndexOnBlob)
	// ErrDependentByPartitionFunctional returns when the dropped column depends by expression partition.
	ErrDependentByPartitionFunctional = ClassDDL.NewStd(errno.ErrDependentByPartitionFunctional)

	// ErrUnsupportedAlterTableSpec means we don't support this alter table specification (i.e. unknown)
	ErrUnsupportedAlterTableSpec = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "Unsupported/unknown ALTER TABLE specification"), nil))
	// ErrGeneralUnsupportedDDL as a generic error to customise by argument
	ErrGeneralUnsupportedDDL = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "%s"), nil))

	// ErrAutoConvert when auto convert happens
	ErrAutoConvert = ClassDDL.NewStd(errno.ErrAutoConvert)
	// ErrWrongStringLength when UserName or HostName is too long
	ErrWrongStringLength = ClassDDL.NewStd(errno.ErrWrongStringLength)

	// ErrBinlogUnsafeSystemFunction when use a system function that may return a different value on the slave.
	ErrBinlogUnsafeSystemFunction = ClassDDL.NewStd(errno.ErrBinlogUnsafeSystemFunction)

	// ErrDDLJobNotFound indicates the job id was not found.
	ErrDDLJobNotFound = ClassDDL.NewStd(errno.ErrDDLJobNotFound)
	// ErrCancelFinishedDDLJob returns when cancel a finished ddl job.
	ErrCancelFinishedDDLJob = ClassDDL.NewStd(errno.ErrCancelFinishedDDLJob)
	// ErrCannotCancelDDLJob returns when cancel a almost finished ddl job, because cancel in now may cause data inconsistency.
	ErrCannotCancelDDLJob = ClassDDL.NewStd(errno.ErrCannotCancelDDLJob)
	// ErrCannotPauseDDLJob returns when the State is not qualified to be paused.
	ErrCannotPauseDDLJob = ClassDDL.NewStd(errno.ErrCannotPauseDDLJob)
	// ErrCannotResumeDDLJob returns  when the State is not qualified to be resumed.
	ErrCannotResumeDDLJob = ClassDDL.NewStd(errno.ErrCannotResumeDDLJob)
	// ErrDDLSetting returns when failing to enable/disable DDL.
	ErrDDLSetting = ClassDDL.NewStd(errno.ErrDDLSetting)
	// ErrIngestFailed returns when the DDL ingest job is failed.
	ErrIngestFailed = ClassDDL.NewStd(errno.ErrIngestFailed)
	// ErrIngestCheckEnvFailed returns when the DDL ingest env is failed to init.
	ErrIngestCheckEnvFailed = ClassDDL.NewStd(errno.ErrIngestCheckEnvFailed)

	// ErrColumnInChange indicates there is modification on the column in parallel.
	ErrColumnInChange = ClassDDL.NewStd(errno.ErrColumnInChange)

	// ErrAlterTiFlashModeForTableWithoutTiFlashReplica returns when set tiflash mode on table whose tiflash_replica is null or tiflash_replica_count = 0
	ErrAlterTiFlashModeForTableWithoutTiFlashReplica = ClassDDL.NewStdErr(0, errno.Message("TiFlash mode will take effect after at least one TiFlash replica is set for the table", nil))
	// ErrUnsupportedTiFlashOperationForSysOrMemTable means we don't support the alter tiflash related action(e.g. set tiflash mode, set tiflash replica) for system table.
	ErrUnsupportedTiFlashOperationForSysOrMemTable = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "`set TiFlash replica` settings for system table and memory table"), nil))
	// ErrUnsupportedTiFlashOperationForUnsupportedCharsetTable is used when alter alter tiflash related action(e.g. set tiflash mode, set tiflash replica) with unsupported charset.
	ErrUnsupportedTiFlashOperationForUnsupportedCharsetTable = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation, errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw, "`set TiFlash replica` settings for table contains %s charset"), nil))
	// ErrTiFlashBackfillIndex is the error that tiflash backfill the index failed.
	ErrTiFlashBackfillIndex = ClassDDL.NewStdErr(errno.ErrTiFlashBackfillIndex,
		errno.Message(errno.MySQLErrName[errno.ErrTiFlashBackfillIndex].Raw, nil))

	// ErrDropIndexNeededInForeignKey returns when drop index which is needed in foreign key.
	ErrDropIndexNeededInForeignKey = ClassDDL.NewStd(errno.ErrDropIndexNeededInForeignKey)
	// ErrForeignKeyCannotDropParent returns when drop table which has foreign key referred.
	ErrForeignKeyCannotDropParent = ClassDDL.NewStd(errno.ErrForeignKeyCannotDropParent)
	// ErrTruncateIllegalForeignKey returns when truncate table which has foreign key referred.
	ErrTruncateIllegalForeignKey = ClassDDL.NewStd(errno.ErrTruncateIllegalForeignKey)
	// ErrForeignKeyColumnCannotChange returns when change column which used by foreign key.
	ErrForeignKeyColumnCannotChange = ClassDDL.NewStd(errno.ErrForeignKeyColumnCannotChange)
	// ErrForeignKeyColumnCannotChangeChild returns when change child table's column which used by foreign key.
	ErrForeignKeyColumnCannotChangeChild = ClassDDL.NewStd(errno.ErrForeignKeyColumnCannotChangeChild)
	// ErrNoReferencedRow2 returns when there are rows in child table don't have related foreign key value in refer table.
	ErrNoReferencedRow2 = ClassDDL.NewStd(errno.ErrNoReferencedRow2)

	// ErrUnsupportedColumnInTTLConfig returns when a column type is not expected in TTL config
	ErrUnsupportedColumnInTTLConfig = ClassDDL.NewStd(errno.ErrUnsupportedColumnInTTLConfig)
	// ErrTTLColumnCannotDrop returns when a column is dropped while referenced by TTL config
	ErrTTLColumnCannotDrop = ClassDDL.NewStd(errno.ErrTTLColumnCannotDrop)
	// ErrSetTTLOptionForNonTTLTable returns when the `TTL_ENABLE` or `TTL_JOB_INTERVAL` option is set on a non-TTL table
	ErrSetTTLOptionForNonTTLTable = ClassDDL.NewStd(errno.ErrSetTTLOptionForNonTTLTable)
	// ErrTempTableNotAllowedWithTTL returns when setting TTL config for a temp table
	ErrTempTableNotAllowedWithTTL = ClassDDL.NewStd(errno.ErrTempTableNotAllowedWithTTL)
	// ErrUnsupportedTTLReferencedByFK returns when the TTL config is set for a table referenced by foreign key
	ErrUnsupportedTTLReferencedByFK = ClassDDL.NewStd(errno.ErrUnsupportedTTLReferencedByFK)
	// ErrUnsupportedPrimaryKeyTypeWithTTL returns when create or alter a table with TTL options but the primary key is not supported
	ErrUnsupportedPrimaryKeyTypeWithTTL = ClassDDL.NewStd(errno.ErrUnsupportedPrimaryKeyTypeWithTTL)

	// ErrNotSupportedYet returns when tidb does not support this feature.
	ErrNotSupportedYet = ClassDDL.NewStd(errno.ErrNotSupportedYet)

	// ErrColumnCheckConstraintReferOther is returned when create column check constraint referring other column.
	ErrColumnCheckConstraintReferOther = ClassDDL.NewStd(errno.ErrColumnCheckConstraintReferencesOtherColumn)
	// ErrTableCheckConstraintReferUnknown is returned when create table check constraint referring non-existing column.
	ErrTableCheckConstraintReferUnknown = ClassDDL.NewStd(errno.ErrTableCheckConstraintReferUnknown)
	// ErrConstraintNotFound is returned for dropping a non-existent constraint.
	ErrConstraintNotFound = ClassDDL.NewStd(errno.ErrConstraintNotFound)
	// ErrCheckConstraintIsViolated is returned for violating an existent check constraint.
	ErrCheckConstraintIsViolated = ClassDDL.NewStd(errno.ErrCheckConstraintViolated)
	// ErrCheckConstraintNamedFuncIsNotAllowed is returned for not allowed function with name.
	ErrCheckConstraintNamedFuncIsNotAllowed = ClassDDL.NewStd(errno.ErrCheckConstraintNamedFunctionIsNotAllowed)
	// ErrCheckConstraintFuncIsNotAllowed is returned for not allowed function.
	ErrCheckConstraintFuncIsNotAllowed = ClassDDL.NewStd(errno.ErrCheckConstraintFunctionIsNotAllowed)
	// ErrCheckConstraintVariables is returned for referring user or system variables.
	ErrCheckConstraintVariables = ClassDDL.NewStd(errno.ErrCheckConstraintVariables)
	// ErrCheckConstraintRefersAutoIncrementColumn is returned for referring auto-increment columns.
	ErrCheckConstraintRefersAutoIncrementColumn = ClassDDL.NewStd(errno.ErrCheckConstraintRefersAutoIncrementColumn)
	// ErrCheckConstraintUsingFKReferActionColumn is returned for referring foreign key columns.
	ErrCheckConstraintUsingFKReferActionColumn = ClassDDL.NewStd(errno.ErrCheckConstraintClauseUsingFKReferActionColumn)
	// ErrNonBooleanExprForCheckConstraint is returned for non bool expression.
	ErrNonBooleanExprForCheckConstraint = ClassDDL.NewStd(errno.ErrNonBooleanExprForCheckConstraint)
	// ErrWarnDeprecatedIntegerDisplayWidth share the same code 1681, and it will be returned when length is specified in integer.
	ErrWarnDeprecatedIntegerDisplayWidth = ClassDDL.NewStdErr(
		errno.ErrWarnDeprecatedSyntaxNoReplacement,
		errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrWarnDeprecatedSyntaxNoReplacement].Raw,
			"Integer display width", "",
		), nil),
	)
	// ErrWarnDeprecatedZerofill is for when the deprectated zerofill attribute is used
	ErrWarnDeprecatedZerofill = ClassDDL.NewStdErr(
		errno.ErrWarnDeprecatedSyntaxNoReplacement,
		errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrWarnDeprecatedSyntaxNoReplacement].Raw,
			"The ZEROFILL attribute",
			" Use the LPAD function to zero-pad numbers, or store the formatted numbers in a CHAR column.",
		), nil),
	)
	// ErrCheckConstraintDupName is for duplicate check constraint names
	ErrCheckConstraintDupName = ClassDDL.NewStd(errno.ErrCheckConstraintDupName)
	// ErrUnsupportedDistTask is for `tidb_enable_dist_task enabled` but `tidb_ddl_enable_fast_reorg` disabled.
	ErrUnsupportedDistTask = ClassDDL.NewStdErr(errno.ErrUnsupportedDDLOperation,
		errno.Message(fmt.Sprintf(errno.MySQLErrName[errno.ErrUnsupportedDDLOperation].Raw,
			"tidb_enable_dist_task setting. To utilize distributed task execution, please enable tidb_ddl_enable_fast_reorg first."), nil))
	// ErrGlobalIndexNotExplicitlySet is for Global index when not explicitly said GLOBAL, including UPDATE INDEXES
	ErrGlobalIndexNotExplicitlySet = ClassDDL.NewStd(errno.ErrGlobalIndexNotExplicitlySet)
	// ErrWarnGlobalIndexNeedManuallyAnalyze is used for global indexes,
	// which cannot trigger automatic analysis when it contains prefix columns or virtual generated columns.
	ErrWarnGlobalIndexNeedManuallyAnalyze = ClassDDL.NewStd(errno.ErrWarnGlobalIndexNeedManuallyAnalyze)

	// ErrEngineAttributeInvalidFormat is returned when meeting invalid format of engine attribute.
	ErrEngineAttributeInvalidFormat = ClassDDL.NewStd(errno.ErrEngineAttributeInvalidFormat)
	// ErrStorageClassInvalidSpec is reserved for future use.
	ErrStorageClassInvalidSpec = ClassDDL.NewStd(errno.ErrStorageClassInvalidSpec)
)

// ReorgRetryableErrCodes are the error codes that are retryable for reorganization.
var ReorgRetryableErrCodes = map[uint16]struct{}{
	errno.ErrPDServerTimeout:           {},
	errno.ErrTiKVServerTimeout:         {},
	errno.ErrTiKVServerBusy:            {},
	errno.ErrResolveLockTimeout:        {},
	errno.ErrRegionUnavailable:         {},
	errno.ErrGCTooEarly:                {},
	errno.ErrWriteConflict:             {},
	errno.ErrTiKVStoreLimit:            {},
	errno.ErrTiKVStaleCommand:          {},
	errno.ErrTiKVMaxTimestampNotSynced: {},
	errno.ErrTiFlashServerTimeout:      {},
	errno.ErrTiFlashServerBusy:         {},
	errno.ErrInfoSchemaExpired:         {},
	errno.ErrInfoSchemaChanged:         {},
	errno.ErrWriteConflictInTiDB:       {},
	errno.ErrTxnRetryable:              {},
	errno.ErrNotOwner:                  {},

	// Temporary network partitioning may cause pk commit failure.
	uint16(terror.CodeResultUndetermined): {},
}

// ReorgRetryableErrMsgs are the error messages that are retryable for reorganization.
var ReorgRetryableErrMsgs = []string{
	"context deadline exceeded",
	"requested lease not found",
}
