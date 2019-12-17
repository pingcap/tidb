// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
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
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	pumpcli "github.com/pingcap/tidb-tools/tidb-binlog/pump_client"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	tidbutil "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	// currentVersion is for all new DDL jobs.
	currentVersion = 1
	// DDLOwnerKey is the ddl owner path that is saved to etcd, and it's exported for testing.
	DDLOwnerKey = "/tidb/ddl/fg/owner"
	ddlPrompt   = "ddl"

	shardRowIDBitsMax = 15

	// PartitionCountLimit is limit of the number of partitions in a table.
	// Mysql maximum number of partitions is 8192, our maximum number of partitions is 1024.
	// Reference linking https://dev.mysql.com/doc/refman/5.7/en/partitioning-limitations.html.
	PartitionCountLimit = 1024
)

var (
	// TableColumnCountLimit is limit of the number of columns in a table.
	// It's exported for testing.
	TableColumnCountLimit = uint32(512)
	// EnableSplitTableRegion is a flag to decide whether to split a new region for
	// a newly created table. It takes effect only if the Storage supports split
	// region.
	EnableSplitTableRegion = uint32(0)
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
	errIncorrectPrefixKey        = terror.ClassDDL.New(mysql.ErrWrongSubKey, mysql.MySQLErrName[mysql.ErrWrongSubKey])
	errTooLongKey                = terror.ClassDDL.New(mysql.ErrTooLongKey,
		fmt.Sprintf(mysql.MySQLErrName[mysql.ErrTooLongKey], maxPrefixLength))
	errKeyColumnDoesNotExits    = terror.ClassDDL.New(mysql.ErrKeyColumnDoesNotExits, mysql.MySQLErrName[mysql.ErrKeyColumnDoesNotExits])
	errUnknownTypeLength        = terror.ClassDDL.New(mysql.ErrUnknownTypeLength, mysql.MySQLErrName[mysql.ErrUnknownTypeLength])
	errUnknownFractionLength    = terror.ClassDDL.New(mysql.ErrUnknownFractionLength, mysql.MySQLErrName[mysql.ErrUnknownFractionLength])
	errInvalidDDLJobVersion     = terror.ClassDDL.New(mysql.ErrInvalidDDLJobVersion, mysql.MySQLErrName[mysql.ErrInvalidDDLJobVersion])
	errInvalidUseOfNull         = terror.ClassDDL.New(mysql.ErrInvalidUseOfNull, mysql.MySQLErrName[mysql.ErrInvalidUseOfNull])
	errTooManyFields            = terror.ClassDDL.New(mysql.ErrTooManyFields, mysql.MySQLErrName[mysql.ErrTooManyFields])
	errInvalidSplitRegionRanges = terror.ClassDDL.New(mysql.ErrInvalidSplitRegionRanges, mysql.MySQLErrName[mysql.ErrInvalidSplitRegionRanges])
	errReorgPanic               = terror.ClassDDL.New(mysql.ErrReorgPanic, mysql.MySQLErrName[mysql.ErrReorgPanic])
	errFkColumnCannotDrop       = terror.ClassDDL.New(mysql.ErrFkColumnCannotDrop, mysql.MySQLErrName[mysql.ErrFkColumnCannotDrop])
	errFKIncompatibleColumns    = terror.ClassDDL.New(mysql.ErrFKIncompatibleColumns, mysql.MySQLErrName[mysql.ErrFKIncompatibleColumns])

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
	// ErrWrongSequenceName returns for wrong sequence name.
	ErrWrongSequenceName = terror.ClassDDL.New(1, "wrong sequence name")
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
	// ErrInvalidAutoRandom returns when auto_random is used incorrectly.
	ErrInvalidAutoRandom = terror.ClassDDL.New(mysql.ErrInvalidAutoRandom, mysql.MySQLErrName[mysql.ErrInvalidAutoRandom])
)

// DDL is responsible for updating schema in data store and maintaining in-memory InfoSchema cache.
type DDL interface {
	CreateSchema(ctx sessionctx.Context, name model.CIStr, charsetInfo *ast.CharsetOpt) error
	AlterSchema(ctx sessionctx.Context, stmt *ast.AlterDatabaseStmt) error
	DropSchema(ctx sessionctx.Context, schema model.CIStr) error
	CreateTable(ctx sessionctx.Context, stmt *ast.CreateTableStmt) error
	CreateView(ctx sessionctx.Context, stmt *ast.CreateViewStmt) error
	CreateTableWithLike(ctx sessionctx.Context, ident, referIdent ast.Ident, ifNotExists bool) error
	DropTable(ctx sessionctx.Context, tableIdent ast.Ident) (err error)
	RecoverTable(ctx sessionctx.Context, tbInfo *model.TableInfo, schemaID, autoID, dropJobID int64, snapshotTS uint64) (err error)
	DropView(ctx sessionctx.Context, tableIdent ast.Ident) (err error)
	CreateIndex(ctx sessionctx.Context, tableIdent ast.Ident, keyType ast.IndexKeyType, indexName model.CIStr,
		columnNames []*ast.IndexPartSpecification, indexOption *ast.IndexOption, ifNotExists bool) error
	DropIndex(ctx sessionctx.Context, tableIdent ast.Ident, indexName model.CIStr, ifExists bool) error
	AlterTable(ctx sessionctx.Context, tableIdent ast.Ident, spec []*ast.AlterTableSpec) error
	TruncateTable(ctx sessionctx.Context, tableIdent ast.Ident) error
	RenameTable(ctx sessionctx.Context, oldTableIdent, newTableIdent ast.Ident, isAlterTable bool) error
	LockTables(ctx sessionctx.Context, stmt *ast.LockTablesStmt) error
	UnlockTables(ctx sessionctx.Context, lockedTables []model.TableLockTpInfo) error
	CleanupTableLock(ctx sessionctx.Context, tables []*ast.TableName) error
	UpdateTableReplicaInfo(ctx sessionctx.Context, tid int64, available bool) error
	RepairTable(ctx sessionctx.Context, table *ast.TableName, createStmt *ast.CreateTableStmt) error
	CreateSequence(ctx sessionctx.Context, stmt *ast.CreateSequenceStmt) error

	// GetLease returns current schema lease time.
	GetLease() time.Duration
	// Stats returns the DDL statistics.
	Stats(vars *variable.SessionVars) (map[string]interface{}, error)
	// GetScope gets the status variables scope.
	GetScope(status string) variable.ScopeFlag
	// Stop stops DDL worker.
	Stop() error
	// RegisterEventCh registers event channel for ddl.
	RegisterEventCh(chan<- *util.Event)
	// SchemaSyncer gets the schema syncer.
	SchemaSyncer() util.SchemaSyncer
	// OwnerManager gets the owner manager.
	OwnerManager() owner.Manager
	// GetID gets the ddl ID.
	GetID() string
	// GetTableMaxRowID gets the max row ID of a normal table or a partition.
	GetTableMaxRowID(startTS uint64, tbl table.PhysicalTable) (int64, bool, error)
	// SetBinlogClient sets the binlog client for DDL worker. It's exported for testing.
	SetBinlogClient(*pumpcli.PumpsClient)
	// GetHook gets the hook. It's exported for testing.
	GetHook() Callback
}

// ddl is used to handle the statements that define the structure or schema of the database.
type ddl struct {
	m      sync.RWMutex
	quitCh chan struct{}

	*ddlCtx
	workers     map[workerType]*worker
	sessPool    *sessionPool
	delRangeMgr delRangeManager
}

// ddlCtx is the context when we use worker to handle DDL jobs.
type ddlCtx struct {
	uuid         string
	store        kv.Storage
	ownerManager owner.Manager
	schemaSyncer util.SchemaSyncer
	ddlJobDoneCh chan struct{}
	ddlEventCh   chan<- *util.Event
	lease        time.Duration        // lease is schema lease.
	binlogCli    *pumpcli.PumpsClient // binlogCli is used for Binlog.
	infoHandle   *infoschema.Handle

	// hook may be modified.
	mu struct {
		sync.RWMutex
		hook        Callback
		interceptor Interceptor
	}
}

func (dc *ddlCtx) isOwner() bool {
	isOwner := dc.ownerManager.IsOwner()
	logutil.BgLogger().Debug("[ddl] check whether is the DDL owner", zap.Bool("isOwner", isOwner), zap.String("selfID", dc.uuid))
	if isOwner {
		metrics.DDLCounter.WithLabelValues(metrics.DDLOwner + "_" + mysql.TiDBReleaseVersion).Inc()
	}
	return isOwner
}

// RegisterEventCh registers passed channel for ddl Event.
func (d *ddl) RegisterEventCh(ch chan<- *util.Event) {
	d.ddlEventCh = ch
}

// asyncNotifyEvent will notify the ddl event to outside world, say statistic handle. When the channel is full, we may
// give up notify and log it.
func asyncNotifyEvent(d *ddlCtx, e *util.Event) {
	if d.ddlEventCh != nil {
		if d.lease == 0 {
			// If lease is 0, it's always used in test.
			select {
			case d.ddlEventCh <- e:
			default:
			}
			return
		}
		for i := 0; i < 10; i++ {
			select {
			case d.ddlEventCh <- e:
				return
			default:
				logutil.BgLogger().Warn("[ddl] fail to notify DDL event", zap.String("event", e.String()))
				time.Sleep(time.Microsecond * 10)
			}
		}
	}
}

// NewDDL creates a new DDL.
func NewDDL(ctx context.Context, options ...Option) DDL {
	return newDDL(ctx, options...)
}

func newDDL(ctx context.Context, options ...Option) *ddl {
	opt := &Options{
		Hook: &BaseCallback{},
	}
	for _, o := range options {
		o(opt)
	}

	id := uuid.New().String()
	ctx, cancelFunc := context.WithCancel(ctx)
	var manager owner.Manager
	var syncer util.SchemaSyncer
	if etcdCli := opt.EtcdCli; etcdCli == nil {
		// The etcdCli is nil if the store is localstore which is only used for testing.
		// So we use mockOwnerManager and MockSchemaSyncer.
		manager = owner.NewMockManager(id, cancelFunc)
		syncer = NewMockSchemaSyncer()
	} else {
		manager = owner.NewOwnerManager(etcdCli, ddlPrompt, id, DDLOwnerKey, cancelFunc)
		syncer = util.NewSchemaSyncer(etcdCli, id, manager)
	}

	ddlCtx := &ddlCtx{
		uuid:         id,
		store:        opt.Store,
		lease:        opt.Lease,
		ddlJobDoneCh: make(chan struct{}, 1),
		ownerManager: manager,
		schemaSyncer: syncer,
		binlogCli:    binloginfo.GetPumpsClient(),
		infoHandle:   opt.InfoHandle,
	}
	ddlCtx.mu.hook = opt.Hook
	ddlCtx.mu.interceptor = &BaseInterceptor{}
	d := &ddl{
		ddlCtx: ddlCtx,
	}

	d.start(ctx, opt.ResourcePool)
	variable.RegisterStatistics(d)

	metrics.DDLCounter.WithLabelValues(metrics.CreateDDLInstance).Inc()
	return d
}

// Stop implements DDL.Stop interface.
func (d *ddl) Stop() error {
	d.m.Lock()
	defer d.m.Unlock()

	d.close()
	logutil.BgLogger().Info("[ddl] stop DDL", zap.String("ID", d.uuid))
	return nil
}

func (d *ddl) newDeleteRangeManager(mock bool) delRangeManager {
	var delRangeMgr delRangeManager
	if !mock {
		delRangeMgr = newDelRangeManager(d.store, d.sessPool)
		logutil.BgLogger().Info("[ddl] start delRangeManager OK", zap.Bool("is a emulator", !d.store.SupportDeleteRange()))
	} else {
		delRangeMgr = newMockDelRangeManager()
	}

	delRangeMgr.start()
	return delRangeMgr
}

// start campaigns the owner and starts workers.
// ctxPool is used for the worker's delRangeManager and creates sessions.
func (d *ddl) start(ctx context.Context, ctxPool *pools.ResourcePool) {
	logutil.BgLogger().Info("[ddl] start DDL", zap.String("ID", d.uuid), zap.Bool("runWorker", RunWorker))
	d.quitCh = make(chan struct{})

	// If RunWorker is true, we need campaign owner and do DDL job.
	// Otherwise, we needn't do that.
	if RunWorker {
		err := d.ownerManager.CampaignOwner(ctx)
		terror.Log(errors.Trace(err))

		d.workers = make(map[workerType]*worker, 2)
		d.sessPool = newSessionPool(ctxPool)
		d.delRangeMgr = d.newDeleteRangeManager(ctxPool == nil)
		d.workers[generalWorker] = newWorker(generalWorker, d.store, d.sessPool, d.delRangeMgr)
		d.workers[addIdxWorker] = newWorker(addIdxWorker, d.store, d.sessPool, d.delRangeMgr)
		for _, worker := range d.workers {
			worker.wg.Add(1)
			w := worker
			go tidbutil.WithRecovery(
				func() { w.start(d.ddlCtx) },
				func(r interface{}) {
					if r != nil {
						logutil.Logger(w.logCtx).Error("[ddl] DDL worker meet panic", zap.String("ID", d.uuid))
						metrics.PanicCounter.WithLabelValues(metrics.LabelDDL).Inc()
					}
				})
			metrics.DDLCounter.WithLabelValues(fmt.Sprintf("%s_%s", metrics.CreateDDL, worker.String())).Inc()

			// When the start function is called, we will send a fake job to let worker
			// checks owner firstly and try to find whether a job exists and run.
			asyncNotify(worker.ddlJobCh)
		}

		go tidbutil.WithRecovery(
			func() { d.schemaSyncer.StartCleanWork() },
			func(r interface{}) {
				if r != nil {
					logutil.BgLogger().Error("[ddl] DDL syncer clean worker meet panic",
						zap.String("ID", d.uuid), zap.Reflect("r", r), zap.Stack("stack trace"))
					metrics.PanicCounter.WithLabelValues(metrics.LabelDDLSyncer).Inc()
				}
			})
		metrics.DDLCounter.WithLabelValues(metrics.StartCleanWork).Inc()
	}
}

func (d *ddl) close() {
	if isChanClosed(d.quitCh) {
		return
	}

	startTime := time.Now()
	close(d.quitCh)
	d.ownerManager.Cancel()
	d.schemaSyncer.CloseCleanWork()
	err := d.schemaSyncer.RemoveSelfVersionPath()
	if err != nil {
		logutil.BgLogger().Error("[ddl] remove self version path failed", zap.Error(err))
	}

	for _, worker := range d.workers {
		worker.close()
	}
	// d.delRangeMgr using sessions from d.sessPool.
	// Put it before d.sessPool.close to reduce the time spent by d.sessPool.close.
	if d.delRangeMgr != nil {
		d.delRangeMgr.clear()
	}
	if d.sessPool != nil {
		d.sessPool.close()
	}

	logutil.BgLogger().Info("[ddl] DDL closed", zap.String("ID", d.uuid), zap.Duration("take time", time.Since(startTime)))
}

// GetLease implements DDL.GetLease interface.
func (d *ddl) GetLease() time.Duration {
	d.m.RLock()
	lease := d.lease
	d.m.RUnlock()
	return lease
}

// GetInfoSchemaWithInterceptor gets the infoschema binding to d. It's exported for testing.
// Please don't use this function, it is used by TestParallelDDLBeforeRunDDLJob to intercept the calling of d.infoHandle.Get(), use d.infoHandle.Get() instead.
// Otherwise, the TestParallelDDLBeforeRunDDLJob will hang up forever.
func (d *ddl) GetInfoSchemaWithInterceptor(ctx sessionctx.Context) infoschema.InfoSchema {
	is := d.infoHandle.Get()

	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.mu.interceptor.OnGetInfoSchema(ctx, is)
}

func (d *ddl) genGlobalIDs(count int) ([]int64, error) {
	var ret []int64
	err := kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
		failpoint.Inject("mockGenGlobalIDFail", func(val failpoint.Value) {
			if val.(bool) {
				failpoint.Return(errors.New("gofail genGlobalIDs error"))
			}
		})

		m := meta.NewMeta(txn)
		var err error
		ret, err = m.GenGlobalIDs(count)
		return err
	})

	return ret, err
}

// SchemaSyncer implements DDL.SchemaSyncer interface.
func (d *ddl) SchemaSyncer() util.SchemaSyncer {
	return d.schemaSyncer
}

// OwnerManager implements DDL.OwnerManager interface.
func (d *ddl) OwnerManager() owner.Manager {
	return d.ownerManager
}

// GetID implements DDL.GetID interface.
func (d *ddl) GetID() string {
	return d.uuid
}

func checkJobMaxInterval(job *model.Job) time.Duration {
	// The job of adding index takes more time to process.
	// So it uses the longer time.
	if job.Type == model.ActionAddIndex || job.Type == model.ActionAddPrimaryKey {
		return 3 * time.Second
	}
	if job.Type == model.ActionCreateTable || job.Type == model.ActionCreateSchema {
		return 500 * time.Millisecond
	}
	return 1 * time.Second
}

func (d *ddl) asyncNotifyWorker(jobTp model.ActionType) {
	// If the workers don't run, we needn't to notify workers.
	if !RunWorker {
		return
	}

	if jobTp == model.ActionAddIndex || jobTp == model.ActionAddPrimaryKey {
		asyncNotify(d.workers[addIdxWorker].ddlJobCh)
	} else {
		asyncNotify(d.workers[generalWorker].ddlJobCh)
	}
}

func (d *ddl) doDDLJob(ctx sessionctx.Context, job *model.Job) error {
	// Get a global job ID and put the DDL job in the queue.
	err := d.addDDLJob(ctx, job)
	if err != nil {
		return errors.Trace(err)
	}
	ctx.GetSessionVars().StmtCtx.IsDDLJobInQueue = true

	// Notice worker that we push a new job and wait the job done.
	d.asyncNotifyWorker(job.Type)
	logutil.BgLogger().Info("[ddl] start DDL job", zap.String("job", job.String()), zap.String("query", job.Query))

	var historyJob *model.Job
	jobID := job.ID
	// For a job from start to end, the state of it will be none -> delete only -> write only -> reorganization -> public
	// For every state changes, we will wait as lease 2 * lease time, so here the ticker check is 10 * lease.
	// But we use etcd to speed up, normally it takes less than 0.5s now, so we use 0.5s or 1s or 3s as the max value.
	ticker := time.NewTicker(chooseLeaseTime(10*d.lease, checkJobMaxInterval(job)))
	startTime := time.Now()
	metrics.JobsGauge.WithLabelValues(job.Type.String()).Inc()
	defer func() {
		ticker.Stop()
		metrics.JobsGauge.WithLabelValues(job.Type.String()).Dec()
		metrics.HandleJobHistogram.WithLabelValues(job.Type.String(), metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()
	for {
		select {
		case <-d.ddlJobDoneCh:
		case <-ticker.C:
		}

		historyJob, err = d.getHistoryDDLJob(jobID)
		if err != nil {
			logutil.BgLogger().Error("[ddl] get history DDL job failed, check again", zap.Error(err))
			continue
		} else if historyJob == nil {
			logutil.BgLogger().Debug("[ddl] DDL job is not in history, maybe not run", zap.Int64("jobID", jobID))
			continue
		}

		// If a job is a history job, the state must be JobStateSynced or JobStateRollbackDone or JobStateCancelled.
		if historyJob.IsSynced() {
			logutil.BgLogger().Info("[ddl] DDL job is finished", zap.Int64("jobID", jobID))
			return nil
		}

		if historyJob.Error != nil {
			return errors.Trace(historyJob.Error)
		}
		panic("When the state is JobStateRollbackDone or JobStateCancelled, historyJob.Error should never be nil")
	}
}

func (d *ddl) callHookOnChanged(err error) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	err = d.mu.hook.OnChanged(err)
	return errors.Trace(err)
}

// SetBinlogClient implements DDL.SetBinlogClient interface.
func (d *ddl) SetBinlogClient(binlogCli *pumpcli.PumpsClient) {
	d.binlogCli = binlogCli
}

// GetHook implements DDL.GetHook interface.
func (d *ddl) GetHook() Callback {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.mu.hook
}

func init() {
	ddlMySQLErrCodes := map[terror.ErrCode]uint16{
		mysql.ErrAlterOperationNotSupportedReason:     mysql.ErrAlterOperationNotSupportedReason,
		mysql.ErrBadField:                             mysql.ErrBadField,
		mysql.ErrBadNull:                              mysql.ErrBadNull,
		mysql.ErrBlobCantHaveDefault:                  mysql.ErrBlobCantHaveDefault,
		mysql.ErrBlobKeyWithoutLength:                 mysql.ErrBlobKeyWithoutLength,
		mysql.ErrCancelledDDLJob:                      mysql.ErrCancelledDDLJob,
		mysql.ErrCantDecodeIndex:                      mysql.ErrCantDecodeIndex,
		mysql.ErrCantDropFieldOrKey:                   mysql.ErrCantDropFieldOrKey,
		mysql.ErrCantRemoveAllFields:                  mysql.ErrCantRemoveAllFields,
		mysql.ErrCoalesceOnlyOnHashPartition:          mysql.ErrCoalesceOnlyOnHashPartition,
		mysql.ErrCollationCharsetMismatch:             mysql.ErrCollationCharsetMismatch,
		mysql.ErrConflictingDeclarations:              mysql.ErrConflictingDeclarations,
		mysql.ErrDependentByGeneratedColumn:           mysql.ErrDependentByGeneratedColumn,
		mysql.ErrDropLastPartition:                    mysql.ErrDropLastPartition,
		mysql.ErrDropPartitionNonExistent:             mysql.ErrDropPartitionNonExistent,
		mysql.ErrDupKeyName:                           mysql.ErrDupKeyName,
		mysql.ErrErrorOnRename:                        mysql.ErrErrorOnRename,
		mysql.ErrFieldNotFoundPart:                    mysql.ErrFieldNotFoundPart,
		mysql.ErrFieldTypeNotAllowedAsPartitionField:  mysql.ErrFieldTypeNotAllowedAsPartitionField,
		mysql.ErrFileNotFound:                         mysql.ErrFileNotFound,
		mysql.ErrGeneratedColumnFunctionIsNotAllowed:  mysql.ErrGeneratedColumnFunctionIsNotAllowed,
		mysql.ErrGeneratedColumnNonPrior:              mysql.ErrGeneratedColumnNonPrior,
		mysql.ErrGeneratedColumnRefAutoInc:            mysql.ErrGeneratedColumnRefAutoInc,
		mysql.ErrInvalidAutoRandom:                    mysql.ErrInvalidAutoRandom,
		mysql.ErrInvalidDDLJob:                        mysql.ErrInvalidDDLJob,
		mysql.ErrInvalidDDLState:                      mysql.ErrInvalidDDLState,
		mysql.ErrInvalidDDLWorker:                     mysql.ErrInvalidDDLWorker,
		mysql.ErrInvalidDefault:                       mysql.ErrInvalidDefault,
		mysql.ErrInvalidGroupFuncUse:                  mysql.ErrInvalidGroupFuncUse,
		mysql.ErrInvalidDDLJobFlag:                    mysql.ErrInvalidDDLJobFlag,
		mysql.ErrInvalidDDLJobVersion:                 mysql.ErrInvalidDDLJobVersion,
		mysql.ErrInvalidOnUpdate:                      mysql.ErrInvalidOnUpdate,
		mysql.ErrInvalidSplitRegionRanges:             mysql.ErrInvalidSplitRegionRanges,
		mysql.ErrInvalidStoreVersion:                  mysql.ErrInvalidStoreVersion,
		mysql.ErrInvalidUseOfNull:                     mysql.ErrInvalidUseOfNull,
		mysql.ErrJSONUsedAsKey:                        mysql.ErrJSONUsedAsKey,
		mysql.ErrKeyColumnDoesNotExits:                mysql.ErrKeyColumnDoesNotExits,
		mysql.ErrLockWaitTimeout:                      mysql.ErrLockWaitTimeout,
		mysql.ErrNoParts:                              mysql.ErrNoParts,
		mysql.ErrNotOwner:                             mysql.ErrNotOwner,
		mysql.ErrOnlyOnRangeListPartition:             mysql.ErrOnlyOnRangeListPartition,
		mysql.ErrPartitionColumnList:                  mysql.ErrPartitionColumnList,
		mysql.ErrPartitionFuncNotAllowed:              mysql.ErrPartitionFuncNotAllowed,
		mysql.ErrPartitionFunctionIsNotAllowed:        mysql.ErrPartitionFunctionIsNotAllowed,
		mysql.ErrPartitionMaxvalue:                    mysql.ErrPartitionMaxvalue,
		mysql.ErrPartitionMgmtOnNonpartitioned:        mysql.ErrPartitionMgmtOnNonpartitioned,
		mysql.ErrPartitionRequiresValues:              mysql.ErrPartitionRequiresValues,
		mysql.ErrPartitionWrongNoPart:                 mysql.ErrPartitionWrongNoPart,
		mysql.ErrPartitionWrongNoSubpart:              mysql.ErrPartitionWrongNoSubpart,
		mysql.ErrPartitionWrongValues:                 mysql.ErrPartitionWrongValues,
		mysql.ErrPartitionsMustBeDefined:              mysql.ErrPartitionsMustBeDefined,
		mysql.ErrPrimaryCantHaveNull:                  mysql.ErrPrimaryCantHaveNull,
		mysql.ErrRangeNotIncreasing:                   mysql.ErrRangeNotIncreasing,
		mysql.ErrRowSinglePartitionField:              mysql.ErrRowSinglePartitionField,
		mysql.ErrSameNamePartition:                    mysql.ErrSameNamePartition,
		mysql.ErrSubpartition:                         mysql.ErrSubpartition,
		mysql.ErrSystemVersioningWrongPartitions:      mysql.ErrSystemVersioningWrongPartitions,
		mysql.ErrTableCantHandleFt:                    mysql.ErrTableCantHandleFt,
		mysql.ErrTableMustHaveColumns:                 mysql.ErrTableMustHaveColumns,
		mysql.ErrTooLongIdent:                         mysql.ErrTooLongIdent,
		mysql.ErrTooLongIndexComment:                  mysql.ErrTooLongIndexComment,
		mysql.ErrTooLongKey:                           mysql.ErrTooLongKey,
		mysql.ErrTooManyFields:                        mysql.ErrTooManyFields,
		mysql.ErrTooManyPartitions:                    mysql.ErrTooManyPartitions,
		mysql.ErrTooManyValues:                        mysql.ErrTooManyValues,
		mysql.ErrUniqueKeyNeedAllFieldsInPf:           mysql.ErrUniqueKeyNeedAllFieldsInPf,
		mysql.ErrUnknownCharacterSet:                  mysql.ErrUnknownCharacterSet,
		mysql.ErrUnknownCollation:                     mysql.ErrUnknownCollation,
		mysql.ErrUnknownPartition:                     mysql.ErrUnknownPartition,
		mysql.ErrUnsupportedDDLOperation:              mysql.ErrUnsupportedDDLOperation,
		mysql.ErrUnsupportedOnGeneratedColumn:         mysql.ErrUnsupportedOnGeneratedColumn,
		mysql.ErrViewWrongList:                        mysql.ErrViewWrongList,
		mysql.ErrWrongColumnName:                      mysql.ErrWrongColumnName,
		mysql.ErrWrongDBName:                          mysql.ErrWrongDBName,
		mysql.ErrWrongExprInPartitionFunc:             mysql.ErrWrongExprInPartitionFunc,
		mysql.ErrWrongFKOptionForGeneratedColumn:      mysql.ErrWrongFKOptionForGeneratedColumn,
		mysql.ErrWrongKeyColumn:                       mysql.ErrWrongKeyColumn,
		mysql.ErrWrongNameForIndex:                    mysql.ErrWrongNameForIndex,
		mysql.ErrWrongObject:                          mysql.ErrWrongObject,
		mysql.ErrWrongPartitionTypeExpectedSystemTime: mysql.ErrWrongPartitionTypeExpectedSystemTime,
		mysql.ErrWrongSubKey:                          mysql.ErrWrongSubKey,
		mysql.ErrWrongTableName:                       mysql.ErrWrongTableName,
		mysql.ErrWrongTypeColumnValue:                 mysql.ErrWrongTypeColumnValue,
		mysql.WarnDataTruncated:                       mysql.WarnDataTruncated,
		mysql.ErrFkColumnCannotDrop:                   mysql.ErrFkColumnCannotDrop,
		mysql.ErrFKIncompatibleColumns:                mysql.ErrFKIncompatibleColumns,
	}
	terror.ErrClassToMySQLCodes[terror.ClassDDL] = ddlMySQLErrCodes
}
