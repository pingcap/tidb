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
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	// currentVersion is for all new DDL jobs.
	currentVersion = 1
	// DDLOwnerKey is the ddl owner path that is saved to etcd, and it's exported for testing.
	DDLOwnerKey = "/tidb/ddl/fg/owner"
	ddlPrompt   = "ddl"

	shardRowIDBitsMax = 15

	batchAddingJobs = 10

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

	// ddlLogCtx uses for log.
	ddlLogCtx = context.Background()
)

var (
	// errWorkerClosed means we have already closed the DDL worker.
	errInvalidWorker = terror.ClassDDL.New(codeInvalidWorker, "invalid worker")
	// errNotOwner means we are not owner and can't handle DDL jobs.
	errNotOwner              = terror.ClassDDL.New(codeNotOwner, "not Owner")
	errCantDecodeIndex       = terror.ClassDDL.New(codeCantDecodeIndex, "cannot decode index value, because %s")
	errInvalidDDLJob         = terror.ClassDDL.New(codeInvalidDDLJob, "invalid DDL job")
	errCancelledDDLJob       = terror.ClassDDL.New(codeCancelledDDLJob, "cancelled DDL job")
	errInvalidJobFlag        = terror.ClassDDL.New(codeInvalidJobFlag, "invalid job flag")
	errRunMultiSchemaChanges = terror.ClassDDL.New(codeRunMultiSchemaChanges, "can't run multi schema change")
	errWaitReorgTimeout      = terror.ClassDDL.New(codeWaitReorgTimeout, "wait for reorganization timeout")
	errInvalidStoreVer       = terror.ClassDDL.New(codeInvalidStoreVer, "invalid storage current version")

	// We don't support dropping column with index covered now.
	errCantDropColWithIndex     = terror.ClassDDL.New(codeCantDropColWithIndex, "can't drop column with index")
	errUnsupportedAddColumn     = terror.ClassDDL.New(codeUnsupportedAddColumn, "unsupported add column")
	errUnsupportedModifyColumn  = terror.ClassDDL.New(codeUnsupportedModifyColumn, "unsupported modify column %s")
	errUnsupportedModifyCharset = terror.ClassDDL.New(codeUnsupportedModifyCharset, "unsupported modify %s")
	errUnsupportedPKHandle      = terror.ClassDDL.New(codeUnsupportedDropPKHandle,
		"unsupported drop integer primary key")
	errUnsupportedCharset = terror.ClassDDL.New(codeUnsupportedCharset, "unsupported charset %s collate %s")

	errUnsupportedShardRowIDBits = terror.ClassDDL.New(codeUnsupportedShardRowIDBits, "unsupported shard_row_id_bits for table with primary key as row id.")
	errBlobKeyWithoutLength      = terror.ClassDDL.New(codeBlobKeyWithoutLength, "BLOB/TEXT column '%-.192s' used in key specification without a key length")
	errIncorrectPrefixKey        = terror.ClassDDL.New(codeIncorrectPrefixKey, "Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys")
	errTooLongKey                = terror.ClassDDL.New(codeTooLongKey, mysql.MySQLErrName[mysql.ErrTooLongKey])
	errKeyColumnDoesNotExits     = terror.ClassDDL.New(codeKeyColumnDoesNotExits, mysql.MySQLErrName[mysql.ErrKeyColumnDoesNotExits])
	errUnknownTypeLength         = terror.ClassDDL.New(codeUnknownTypeLength, "Unknown length for type tp %d")
	errUnknownFractionLength     = terror.ClassDDL.New(codeUnknownFractionLength, "Unknown Length for type tp %d and fraction %d")
	errInvalidJobVersion         = terror.ClassDDL.New(codeInvalidJobVersion, "DDL job with version %d greater than current %d")
	errFileNotFound              = terror.ClassDDL.New(codeFileNotFound, "Can't find file: './%s/%s.frm'")
	errErrorOnRename             = terror.ClassDDL.New(codeErrorOnRename, "Error on rename of './%s/%s' to './%s/%s'")
	errInvalidUseOfNull          = terror.ClassDDL.New(codeInvalidUseOfNull, "Invalid use of NULL value")
	errTooManyFields             = terror.ClassDDL.New(codeTooManyFields, "Too many columns")
	errInvalidSplitRegionRanges  = terror.ClassDDL.New(codeInvalidRanges, "Failed to split region ranges")
	errReorgPanic                = terror.ClassDDL.New(codeReorgWorkerPanic, "reorg worker panic.")
	errFkColumnCannotDrop        = terror.ClassDDL.New(mysql.ErrFkColumnCannotDrop, mysql.MySQLErrName[mysql.ErrFkColumnCannotDrop])
	errFKIncompatibleColumns     = terror.ClassDDL.New(mysql.ErrFKIncompatibleColumns, mysql.MySQLErrName[mysql.ErrFKIncompatibleColumns])

	errOnlyOnRangeListPartition = terror.ClassDDL.New(codeOnlyOnRangeListPartition, mysql.MySQLErrName[mysql.ErrOnlyOnRangeListPartition])
	// errWrongKeyColumn is for table column cannot be indexed.
	errWrongKeyColumn = terror.ClassDDL.New(codeWrongKeyColumn, mysql.MySQLErrName[mysql.ErrWrongKeyColumn])
	// errUnsupportedOnGeneratedColumn is for unsupported actions on generated columns.
	errUnsupportedOnGeneratedColumn = terror.ClassDDL.New(codeUnsupportedOnGeneratedColumn, mysql.MySQLErrName[mysql.ErrUnsupportedOnGeneratedColumn])
	// errGeneratedColumnNonPrior forbids to refer generated column non prior to it.
	errGeneratedColumnNonPrior = terror.ClassDDL.New(codeGeneratedColumnNonPrior, mysql.MySQLErrName[mysql.ErrGeneratedColumnNonPrior])
	// errDependentByGeneratedColumn forbids to delete columns which are dependent by generated columns.
	errDependentByGeneratedColumn = terror.ClassDDL.New(codeDependentByGeneratedColumn, mysql.MySQLErrName[mysql.ErrDependentByGeneratedColumn])
	// errJSONUsedAsKey forbids to use JSON as key or index.
	errJSONUsedAsKey = terror.ClassDDL.New(codeJSONUsedAsKey, mysql.MySQLErrName[mysql.ErrJSONUsedAsKey])
	// errBlobCantHaveDefault forbids to give not null default value to TEXT/BLOB/JSON.
	errBlobCantHaveDefault = terror.ClassDDL.New(codeBlobCantHaveDefault, mysql.MySQLErrName[mysql.ErrBlobCantHaveDefault])
	errTooLongIndexComment = terror.ClassDDL.New(codeErrTooLongIndexComment, mysql.MySQLErrName[mysql.ErrTooLongIndexComment])
	// ErrInvalidDefaultValue returns for invalid default value for columns.
	ErrInvalidDefaultValue = terror.ClassDDL.New(codeInvalidDefaultValue, mysql.MySQLErrName[mysql.ErrInvalidDefault])
	// ErrGeneratedColumnRefAutoInc forbids to refer generated columns to auto-increment columns .
	ErrGeneratedColumnRefAutoInc = terror.ClassDDL.New(codeErrGeneratedColumnRefAutoInc, mysql.MySQLErrName[mysql.ErrGeneratedColumnRefAutoInc])
	// ErrUnsupportedAddPartition returns for does not support add partitions.
	ErrUnsupportedAddPartition = terror.ClassDDL.New(codeUnsupportedAddPartition, "unsupported add partitions")
	// ErrUnsupportedCoalescePartition returns for does not support coalesce partitions.
	ErrUnsupportedCoalescePartition = terror.ClassDDL.New(codeUnsupportedCoalescePartition, "unsupported coalesce partitions")
	// ErrGeneratedColumnFunctionIsNotAllowed returns for unsupported functions for generated columns.
	ErrGeneratedColumnFunctionIsNotAllowed = terror.ClassDDL.New(codeErrGeneratedColumnFunctionIsNotAllowed, "Expression of generated column '%s' contains a disallowed function.")
	// ErrUnsupportedPartitionByRangeColumns returns for does unsupported partition by range columns.
	ErrUnsupportedPartitionByRangeColumns = terror.ClassDDL.New(codeUnsupportedPartitionByRangeColumns,
		"unsupported partition by range columns")
	errUnsupportedCreatePartition = terror.ClassDDL.New(codeUnsupportedCreatePartition, "unsupported partition type, treat as normal table")
	errTablePartitionDisabled     = terror.ClassDDL.New(codeUnsupportedCreatePartition, "Partitions are ignored because Table Partition is disabled, please set 'tidb_enable_table_partition' if you need to need to enable it")

	// ErrDupKeyName returns for duplicated key name
	ErrDupKeyName = terror.ClassDDL.New(codeDupKeyName, "duplicate key name")
	// ErrInvalidDBState returns for invalid database state.
	ErrInvalidDBState = terror.ClassDDL.New(codeInvalidDBState, "invalid database state")
	// ErrInvalidTableState returns for invalid Table state.
	ErrInvalidTableState = terror.ClassDDL.New(codeInvalidTableState, "invalid table state")
	// ErrInvalidColumnState returns for invalid column state.
	ErrInvalidColumnState = terror.ClassDDL.New(codeInvalidColumnState, "invalid column state")
	// ErrInvalidIndexState returns for invalid index state.
	ErrInvalidIndexState = terror.ClassDDL.New(codeInvalidIndexState, "invalid index state")
	// ErrInvalidForeignKeyState returns for invalid foreign key state.
	ErrInvalidForeignKeyState = terror.ClassDDL.New(codeInvalidForeignKeyState, "invalid foreign key state")
	// ErrInvalidTableLockState returns for invalid table state.
	ErrInvalidTableLockState = terror.ClassDDL.New(codeInvalidTableLockState, "invalid table lock state")
	// ErrUnsupportedModifyPrimaryKey returns an error when add or drop the primary key.
	// It's exported for testing.
	ErrUnsupportedModifyPrimaryKey = terror.ClassDDL.New(codeUnsupportedModifyPrimaryKey, "unsupported %s primary key")

	// ErrColumnBadNull returns for a bad null value.
	ErrColumnBadNull = terror.ClassDDL.New(codeBadNull, "column cann't be null")
	// ErrBadField forbids to refer to unknown column.
	ErrBadField = terror.ClassDDL.New(codeBadField, "Unknown column '%s' in '%s'")
	// ErrCantRemoveAllFields returns for deleting all columns.
	ErrCantRemoveAllFields = terror.ClassDDL.New(codeCantRemoveAllFields, "can't delete all columns with ALTER TABLE")
	// ErrCantDropFieldOrKey returns for dropping a non-existent field or key.
	ErrCantDropFieldOrKey = terror.ClassDDL.New(codeCantDropFieldOrKey, "can't drop field; check that column/key exists")
	// ErrInvalidOnUpdate returns for invalid ON UPDATE clause.
	ErrInvalidOnUpdate = terror.ClassDDL.New(codeInvalidOnUpdate, mysql.MySQLErrName[mysql.ErrInvalidOnUpdate])
	// ErrTooLongIdent returns for too long name of database/table/column/index.
	ErrTooLongIdent = terror.ClassDDL.New(codeTooLongIdent, mysql.MySQLErrName[mysql.ErrTooLongIdent])
	// ErrWrongDBName returns for wrong database name.
	ErrWrongDBName = terror.ClassDDL.New(codeWrongDBName, mysql.MySQLErrName[mysql.ErrWrongDBName])
	// ErrWrongTableName returns for wrong table name.
	ErrWrongTableName = terror.ClassDDL.New(codeWrongTableName, mysql.MySQLErrName[mysql.ErrWrongTableName])
	// ErrWrongColumnName returns for wrong column name.
	ErrWrongColumnName = terror.ClassDDL.New(codeWrongColumnName, mysql.MySQLErrName[mysql.ErrWrongColumnName])
	// ErrTableMustHaveColumns returns for missing column when creating a table.
	ErrTableMustHaveColumns = terror.ClassDDL.New(codeTableMustHaveColumns, mysql.MySQLErrName[mysql.ErrTableMustHaveColumns])
	// ErrWrongNameForIndex returns for wrong index name.
	ErrWrongNameForIndex = terror.ClassDDL.New(codeWrongNameForIndex, mysql.MySQLErrName[mysql.ErrWrongNameForIndex])
	// ErrUnknownCharacterSet returns unknown character set.
	ErrUnknownCharacterSet = terror.ClassDDL.New(codeUnknownCharacterSet, "Unknown character set: '%s'")
	// ErrUnknownCollation returns unknown collation.
	ErrUnknownCollation = terror.ClassDDL.New(codeUnknownCollation, "Unknown collation: '%s'")
	// ErrCollationCharsetMismatch returns when collation not match the charset.
	ErrCollationCharsetMismatch = terror.ClassDDL.New(codeCollationCharsetMismatch, mysql.MySQLErrName[mysql.ErrCollationCharsetMismatch])
	// ErrConflictingDeclarations return conflict declarations.
	ErrConflictingDeclarations = terror.ClassDDL.New(codeConflictingDeclarations, "Conflicting declarations: 'CHARACTER SET %s' and 'CHARACTER SET %s'")
	// ErrPrimaryCantHaveNull returns All parts of a PRIMARY KEY must be NOT NULL; if you need NULL in a key, use UNIQUE instead
	ErrPrimaryCantHaveNull = terror.ClassDDL.New(codePrimaryCantHaveNull, mysql.MySQLErrName[mysql.ErrPrimaryCantHaveNull])

	// ErrNotAllowedTypeInPartition returns not allowed type error when creating table partiton with unsupport expression type.
	ErrNotAllowedTypeInPartition = terror.ClassDDL.New(codeErrFieldTypeNotAllowedAsPartitionField, mysql.MySQLErrName[mysql.ErrFieldTypeNotAllowedAsPartitionField])
	// ErrPartitionMgmtOnNonpartitioned returns it's not a partition table.
	ErrPartitionMgmtOnNonpartitioned = terror.ClassDDL.New(codePartitionMgmtOnNonpartitioned, "Partition management on a not partitioned table is not possible")
	// ErrDropPartitionNonExistent returns error in list of partition.
	ErrDropPartitionNonExistent = terror.ClassDDL.New(codeDropPartitionNonExistent, " Error in list of partitions to %s")
	// ErrSameNamePartition returns duplicate partition name.
	ErrSameNamePartition = terror.ClassDDL.New(codeSameNamePartition, "Duplicate partition name %s")
	// ErrRangeNotIncreasing returns values less than value must be strictly increasing for each partition.
	ErrRangeNotIncreasing = terror.ClassDDL.New(codeRangeNotIncreasing, "VALUES LESS THAN value must be strictly increasing for each partition")
	// ErrPartitionMaxvalue returns maxvalue can only be used in last partition definition.
	ErrPartitionMaxvalue = terror.ClassDDL.New(codePartitionMaxvalue, "MAXVALUE can only be used in last partition definition")
	//ErrDropLastPartition returns cannot remove all partitions, use drop table instead.
	ErrDropLastPartition = terror.ClassDDL.New(codeDropLastPartition, mysql.MySQLErrName[mysql.ErrDropLastPartition])
	//ErrTooManyPartitions returns too many partitions were defined.
	ErrTooManyPartitions = terror.ClassDDL.New(codeTooManyPartitions, mysql.MySQLErrName[mysql.ErrTooManyPartitions])
	//ErrPartitionFunctionIsNotAllowed returns this partition function is not allowed.
	ErrPartitionFunctionIsNotAllowed = terror.ClassDDL.New(codePartitionFunctionIsNotAllowed, mysql.MySQLErrName[mysql.ErrPartitionFunctionIsNotAllowed])
	// ErrPartitionFuncNotAllowed returns partition function returns the wrong type.
	ErrPartitionFuncNotAllowed = terror.ClassDDL.New(codeErrPartitionFuncNotAllowed, mysql.MySQLErrName[mysql.ErrPartitionFuncNotAllowed])
	// ErrUniqueKeyNeedAllFieldsInPf returns must include all columns in the table's partitioning function.
	ErrUniqueKeyNeedAllFieldsInPf = terror.ClassDDL.New(codeUniqueKeyNeedAllFieldsInPf, mysql.MySQLErrName[mysql.ErrUniqueKeyNeedAllFieldsInPf])
	errWrongExprInPartitionFunc   = terror.ClassDDL.New(codeWrongExprInPartitionFunc, mysql.MySQLErrName[mysql.ErrWrongExprInPartitionFunc])
	// ErrWarnDataTruncated returns data truncated error.
	ErrWarnDataTruncated = terror.ClassDDL.New(codeWarnDataTruncated, mysql.MySQLErrName[mysql.WarnDataTruncated])
	// ErrCoalesceOnlyOnHashPartition returns coalesce partition can only be used on hash/key partitions.
	ErrCoalesceOnlyOnHashPartition = terror.ClassDDL.New(codeCoalesceOnlyOnHashPartition, mysql.MySQLErrName[mysql.ErrCoalesceOnlyOnHashPartition])
	// ErrViewWrongList returns create view must include all columns in the select clause
	ErrViewWrongList = terror.ClassDDL.New(codeViewWrongList, mysql.MySQLErrName[mysql.ErrViewWrongList])
	// ErrAlterOperationNotSupported returns when alter operations is not supported.
	ErrAlterOperationNotSupported = terror.ClassDDL.New(codeNotSupportedAlterOperation, mysql.MySQLErrName[mysql.ErrAlterOperationNotSupportedReason])
	// ErrWrongObject returns for wrong object.
	ErrWrongObject = terror.ClassDDL.New(codeErrWrongObject, mysql.MySQLErrName[mysql.ErrWrongObject])
	// ErrTableCantHandleFt returns FULLTEXT keys are not supported by table type
	ErrTableCantHandleFt = terror.ClassDDL.New(codeErrTableCantHandleFt, mysql.MySQLErrName[mysql.ErrTableCantHandleFt])
	// ErrFieldNotFoundPart returns an error when 'partition by columns' are not found in table columns.
	ErrFieldNotFoundPart = terror.ClassDDL.New(codeFieldNotFoundPart, mysql.MySQLErrName[mysql.ErrFieldNotFoundPart])
	// ErrWrongTypeColumnValue returns 'Partition column values of incorrect type'
	ErrWrongTypeColumnValue = terror.ClassDDL.New(codeWrongTypeColumnValue, mysql.MySQLErrName[mysql.ErrWrongTypeColumnValue])
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
	CreateIndex(ctx sessionctx.Context, tableIdent ast.Ident, unique bool, indexName model.CIStr,
		columnNames []*ast.IndexColName, indexOption *ast.IndexOption) error
	DropIndex(ctx sessionctx.Context, tableIdent ast.Ident, indexName model.CIStr) error
	AlterTable(ctx sessionctx.Context, tableIdent ast.Ident, spec []*ast.AlterTableSpec) error
	TruncateTable(ctx sessionctx.Context, tableIdent ast.Ident) error
	RenameTable(ctx sessionctx.Context, oldTableIdent, newTableIdent ast.Ident, isAlterTable bool) error
	LockTables(ctx sessionctx.Context, stmt *ast.LockTablesStmt) error
	UnlockTables(ctx sessionctx.Context, lockedTables []model.TableLockTpInfo) error
	CleanupTableLock(ctx sessionctx.Context, tables []*ast.TableName) error

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

type limitJobTask struct {
	job *model.Job
	err chan error
}

// ddl is used to handle the statements that define the structure or schema of the database.
type ddl struct {
	m          sync.RWMutex
	quitCh     chan struct{}
	wg         sync.WaitGroup // It's only used to deal with data race in state_test and schema_test.
	limitJobCh chan *limitJobTask

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
	logutil.Logger(ddlLogCtx).Debug("[ddl] check whether is the DDL owner", zap.Bool("isOwner", isOwner), zap.String("selfID", dc.uuid))
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
				logutil.Logger(ddlLogCtx).Warn("[ddl] fail to notify DDL event", zap.String("event", e.String()))
				time.Sleep(time.Microsecond * 10)
			}
		}
	}
}

// NewDDL creates a new DDL.
func NewDDL(ctx context.Context, etcdCli *clientv3.Client, store kv.Storage,
	infoHandle *infoschema.Handle, hook Callback, lease time.Duration, ctxPool *pools.ResourcePool) DDL {
	return newDDL(ctx, etcdCli, store, infoHandle, hook, lease, ctxPool)
}

func newDDL(ctx context.Context, etcdCli *clientv3.Client, store kv.Storage,
	infoHandle *infoschema.Handle, hook Callback, lease time.Duration, ctxPool *pools.ResourcePool) *ddl {
	if hook == nil {
		hook = &BaseCallback{}
	}

	id := uuid.New().String()
	ctx, cancelFunc := context.WithCancel(ctx)
	var manager owner.Manager
	var syncer util.SchemaSyncer
	if etcdCli == nil {
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
		store:        store,
		lease:        lease,
		ddlJobDoneCh: make(chan struct{}, 1),
		ownerManager: manager,
		schemaSyncer: syncer,
		binlogCli:    binloginfo.GetPumpsClient(),
		infoHandle:   infoHandle,
	}
	ddlCtx.mu.hook = hook
	ddlCtx.mu.interceptor = &BaseInterceptor{}
	d := &ddl{
		ddlCtx:     ddlCtx,
		limitJobCh: make(chan *limitJobTask, batchAddingJobs),
	}

	d.start(ctx, ctxPool)
	variable.RegisterStatistics(d)

	metrics.DDLCounter.WithLabelValues(metrics.CreateDDLInstance).Inc()
	return d
}

// Stop implements DDL.Stop interface.
func (d *ddl) Stop() error {
	d.m.Lock()
	defer d.m.Unlock()

	d.close()
	logutil.Logger(ddlLogCtx).Info("[ddl] stop DDL", zap.String("ID", d.uuid))
	return nil
}

func (d *ddl) newDeleteRangeManager(mock bool) delRangeManager {
	var delRangeMgr delRangeManager
	if !mock {
		delRangeMgr = newDelRangeManager(d.store, d.sessPool)
		logutil.Logger(ddlLogCtx).Info("[ddl] start delRangeManager OK", zap.Bool("is a emulator", !d.store.SupportDeleteRange()))
	} else {
		delRangeMgr = newMockDelRangeManager()
	}

	delRangeMgr.start()
	return delRangeMgr
}

// start campaigns the owner and starts workers.
// ctxPool is used for the worker's delRangeManager and creates sessions.
func (d *ddl) start(ctx context.Context, ctxPool *pools.ResourcePool) {
	logutil.Logger(ddlLogCtx).Info("[ddl] start DDL", zap.String("ID", d.uuid), zap.Bool("runWorker", RunWorker))
	d.quitCh = make(chan struct{})

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		tidbutil.WithRecovery(
			func() { d.limitDDLJobs() },
			func(r interface{}) {
				if r != nil {
					logutil.Logger(ddlLogCtx).Error("[ddl] limit DDL jobs meet panic",
						zap.String("ID", d.uuid), zap.Reflect("r", r), zap.Stack("stack trace"))
					metrics.PanicCounter.WithLabelValues(metrics.LabelDDL).Inc()
				}
			})
	}()

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
						metrics.PanicCounter.WithLabelValues(metrics.LabelDDLWorker).Inc()
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
					logutil.Logger(ddlLogCtx).Error("[ddl] DDL syncer clean worker meet panic",
						zap.String("ID", d.uuid), zap.Reflect("r", r), zap.Stack("stack trace"))
					metrics.PanicCounter.WithLabelValues(metrics.LabelDDLSyncer).Inc()
				}
			})
		metrics.DDLCounter.WithLabelValues(fmt.Sprintf("%s", metrics.StartCleanWork)).Inc()
	}
}

func (d *ddl) close() {
	if isChanClosed(d.quitCh) {
		return
	}

	startTime := time.Now()
	close(d.quitCh)
	d.wg.Wait()
	d.ownerManager.Cancel()
	d.schemaSyncer.CloseCleanWork()
	err := d.schemaSyncer.RemoveSelfVersionPath()
	if err != nil {
		logutil.Logger(ddlLogCtx).Error("[ddl] remove self version path failed", zap.Error(err))
	}

	for _, worker := range d.workers {
		worker.close()
	}
	if d.sessPool != nil {
		d.sessPool.close()
	}
	if d.delRangeMgr != nil {
		d.delRangeMgr.clear()
	}

	logutil.Logger(ddlLogCtx).Info("[ddl] DDL closed", zap.String("ID", d.uuid), zap.Duration("take time", time.Since(startTime)))
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
	ret := make([]int64, count)
	err := kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
		var err error

		failpoint.Inject("mockGenGlobalIDFail", func(val failpoint.Value) {
			if val.(bool) {
				failpoint.Return(errors.New("gofail genGlobalIDs error"))
			}
		})

		m := meta.NewMeta(txn)
		for i := 0; i < count; i++ {
			ret[i], err = m.GenGlobalID()
			if err != nil {
				return err
			}
		}
		return nil
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
	job.Query, _ = ctx.Value(sessionctx.QueryString).(string)
	task := &limitJobTask{job, make(chan error)}
	d.limitJobCh <- task
	err := <-task.err
	if err != nil {
		return errors.Trace(err)
	}
	ctx.GetSessionVars().StmtCtx.IsDDLJobInQueue = true

	// Notice worker that we push a new job and wait the job done.
	d.asyncNotifyWorker(job.Type)
	logutil.Logger(ddlLogCtx).Info("[ddl] start DDL job", zap.String("job", job.String()), zap.String("query", job.Query))

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
			logutil.Logger(ddlLogCtx).Error("[ddl] get history DDL job failed, check again", zap.Error(err))
			continue
		} else if historyJob == nil {
			logutil.Logger(ddlLogCtx).Debug("[ddl] DDL job is not in history, maybe not run", zap.Int64("jobID", jobID))
			continue
		}

		// If a job is a history job, the state must be JobStateSynced or JobStateRollbackDone or JobStateCancelled.
		if historyJob.IsSynced() {
			logutil.Logger(ddlLogCtx).Info("[ddl] DDL job is finished", zap.Int64("jobID", jobID))
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

// DDL error codes.
const (
	codeInvalidWorker         terror.ErrCode = 1
	codeNotOwner                             = 2
	codeInvalidDDLJob                        = 3
	codeInvalidJobFlag                       = 5
	codeRunMultiSchemaChanges                = 6
	codeWaitReorgTimeout                     = 7
	codeInvalidStoreVer                      = 8
	codeUnknownTypeLength                    = 9
	codeUnknownFractionLength                = 10
	codeInvalidJobVersion                    = 11
	codeCancelledDDLJob                      = 12
	codeInvalidRanges                        = 13
	codeReorgWorkerPanic                     = 14
	codeCantDecodeIndex                      = 15

	codeInvalidDBState         = 100
	codeInvalidTableState      = 101
	codeInvalidColumnState     = 102
	codeInvalidIndexState      = 103
	codeInvalidForeignKeyState = 104
	codeInvalidTableLockState  = 105

	codeCantDropColWithIndex               = 201
	codeUnsupportedAddColumn               = 202
	codeUnsupportedModifyColumn            = 203
	codeUnsupportedDropPKHandle            = 204
	codeUnsupportedCharset                 = 205
	codeUnsupportedModifyPrimaryKey        = 206
	codeUnsupportedShardRowIDBits          = 207
	codeUnsupportedAddPartition            = 208
	codeUnsupportedCoalescePartition       = 209
	codeUnsupportedModifyCharset           = 210
	codeUnsupportedPartitionByRangeColumns = 211
	codeUnsupportedCreatePartition         = 212

	codeFileNotFound                           = 1017
	codeErrorOnRename                          = 1025
	codeBadNull                                = mysql.ErrBadNull
	codeBadField                               = 1054
	codeTooLongIdent                           = 1059
	codeDupKeyName                             = 1061
	codeInvalidDefaultValue                    = mysql.ErrInvalidDefault
	codeTooLongKey                             = 1071
	codeKeyColumnDoesNotExits                  = mysql.ErrKeyColumnDoesNotExits
	codeIncorrectPrefixKey                     = 1089
	codeCantRemoveAllFields                    = 1090
	codeCantDropFieldOrKey                     = 1091
	codeBlobCantHaveDefault                    = 1101
	codeWrongDBName                            = 1102
	codeWrongTableName                         = 1103
	codeTooManyFields                          = 1117
	codeInvalidUseOfNull                       = 1138
	codeWrongColumnName                        = 1166
	codeWrongKeyColumn                         = 1167
	codeBlobKeyWithoutLength                   = 1170
	codeInvalidOnUpdate                        = 1294
	codeErrWrongObject                         = terror.ErrCode(mysql.ErrWrongObject)
	codeViewWrongList                          = 1353
	codeUnsupportedOnGeneratedColumn           = 3106
	codeGeneratedColumnNonPrior                = 3107
	codeDependentByGeneratedColumn             = 3108
	codeJSONUsedAsKey                          = 3152
	codeWrongNameForIndex                      = terror.ErrCode(mysql.ErrWrongNameForIndex)
	codeErrTooLongIndexComment                 = terror.ErrCode(mysql.ErrTooLongIndexComment)
	codeUnknownCharacterSet                    = terror.ErrCode(mysql.ErrUnknownCharacterSet)
	codeUnknownCollation                       = terror.ErrCode(mysql.ErrUnknownCollation)
	codeCollationCharsetMismatch               = terror.ErrCode(mysql.ErrCollationCharsetMismatch)
	codeConflictingDeclarations                = terror.ErrCode(mysql.ErrConflictingDeclarations)
	codeCantCreateTable                        = terror.ErrCode(mysql.ErrCantCreateTable)
	codeTableMustHaveColumns                   = terror.ErrCode(mysql.ErrTableMustHaveColumns)
	codePartitionsMustBeDefined                = terror.ErrCode(mysql.ErrPartitionsMustBeDefined)
	codePartitionMgmtOnNonpartitioned          = terror.ErrCode(mysql.ErrPartitionMgmtOnNonpartitioned)
	codeDropPartitionNonExistent               = terror.ErrCode(mysql.ErrDropPartitionNonExistent)
	codeSameNamePartition                      = terror.ErrCode(mysql.ErrSameNamePartition)
	codeRangeNotIncreasing                     = terror.ErrCode(mysql.ErrRangeNotIncreasing)
	codePartitionMaxvalue                      = terror.ErrCode(mysql.ErrPartitionMaxvalue)
	codeErrTooManyValues                       = terror.ErrCode(mysql.ErrTooManyValues)
	codeDropLastPartition                      = terror.ErrCode(mysql.ErrDropLastPartition)
	codeTooManyPartitions                      = terror.ErrCode(mysql.ErrTooManyPartitions)
	codeNoParts                                = terror.ErrCode(mysql.ErrNoParts)
	codePartitionFunctionIsNotAllowed          = terror.ErrCode(mysql.ErrPartitionFunctionIsNotAllowed)
	codeErrPartitionFuncNotAllowed             = terror.ErrCode(mysql.ErrPartitionFuncNotAllowed)
	codeErrFieldTypeNotAllowedAsPartitionField = terror.ErrCode(mysql.ErrFieldTypeNotAllowedAsPartitionField)
	codeUniqueKeyNeedAllFieldsInPf             = terror.ErrCode(mysql.ErrUniqueKeyNeedAllFieldsInPf)
	codePrimaryCantHaveNull                    = terror.ErrCode(mysql.ErrPrimaryCantHaveNull)
	codeWrongExprInPartitionFunc               = terror.ErrCode(mysql.ErrWrongExprInPartitionFunc)
	codeWarnDataTruncated                      = terror.ErrCode(mysql.WarnDataTruncated)
	codeErrTableCantHandleFt                   = terror.ErrCode(mysql.ErrTableCantHandleFt)
	codeCoalesceOnlyOnHashPartition            = terror.ErrCode(mysql.ErrCoalesceOnlyOnHashPartition)
	codeUnknownPartition                       = terror.ErrCode(mysql.ErrUnknownPartition)
	codeErrGeneratedColumnFunctionIsNotAllowed = terror.ErrCode(mysql.ErrGeneratedColumnFunctionIsNotAllowed)
	codeErrGeneratedColumnRefAutoInc           = terror.ErrCode(mysql.ErrGeneratedColumnRefAutoInc)
	codeNotSupportedAlterOperation             = terror.ErrCode(mysql.ErrAlterOperationNotSupportedReason)
	codeFieldNotFoundPart                      = terror.ErrCode(mysql.ErrFieldNotFoundPart)
	codePartitionColumnList                    = terror.ErrCode(mysql.ErrPartitionColumnList)
	codeOnlyOnRangeListPartition               = terror.ErrCode(mysql.ErrOnlyOnRangeListPartition)
	codePartitionRequiresValues                = terror.ErrCode(mysql.ErrPartitionRequiresValues)
	codePartitionWrongNoPart                   = terror.ErrCode(mysql.ErrPartitionWrongNoPart)
	codePartitionWrongNoSubpart                = terror.ErrCode(mysql.ErrPartitionWrongNoSubpart)
	codePartitionWrongValues                   = terror.ErrCode(mysql.ErrPartitionWrongValues)
	codeRowSinglePartitionField                = terror.ErrCode(mysql.ErrRowSinglePartitionField)
	codeSubpartition                           = terror.ErrCode(mysql.ErrSubpartition)
	codeSystemVersioningWrongPartitions        = terror.ErrCode(mysql.ErrSystemVersioningWrongPartitions)
	codeWrongPartitionTypeExpectedSystemTime   = terror.ErrCode(mysql.ErrWrongPartitionTypeExpectedSystemTime)
	codeWrongTypeColumnValue                   = terror.ErrCode(mysql.ErrWrongTypeColumnValue)
)

func init() {
	ddlMySQLErrCodes := map[terror.ErrCode]uint16{
		codeBadNull:                                mysql.ErrBadNull,
		codeCantRemoveAllFields:                    mysql.ErrCantRemoveAllFields,
		codeCantDropFieldOrKey:                     mysql.ErrCantDropFieldOrKey,
		codeInvalidOnUpdate:                        mysql.ErrInvalidOnUpdate,
		codeBlobKeyWithoutLength:                   mysql.ErrBlobKeyWithoutLength,
		codeIncorrectPrefixKey:                     mysql.ErrWrongSubKey,
		codeTooLongIdent:                           mysql.ErrTooLongIdent,
		codeTooLongKey:                             mysql.ErrTooLongKey,
		codeKeyColumnDoesNotExits:                  mysql.ErrKeyColumnDoesNotExits,
		codeDupKeyName:                             mysql.ErrDupKeyName,
		codeWrongDBName:                            mysql.ErrWrongDBName,
		codeWrongTableName:                         mysql.ErrWrongTableName,
		codeFileNotFound:                           mysql.ErrFileNotFound,
		codeErrorOnRename:                          mysql.ErrErrorOnRename,
		codeBadField:                               mysql.ErrBadField,
		codeInvalidUseOfNull:                       mysql.ErrInvalidUseOfNull,
		codeUnsupportedOnGeneratedColumn:           mysql.ErrUnsupportedOnGeneratedColumn,
		codeGeneratedColumnNonPrior:                mysql.ErrGeneratedColumnNonPrior,
		codeDependentByGeneratedColumn:             mysql.ErrDependentByGeneratedColumn,
		codeJSONUsedAsKey:                          mysql.ErrJSONUsedAsKey,
		codeBlobCantHaveDefault:                    mysql.ErrBlobCantHaveDefault,
		codeWrongColumnName:                        mysql.ErrWrongColumnName,
		codeWrongKeyColumn:                         mysql.ErrWrongKeyColumn,
		codeWrongNameForIndex:                      mysql.ErrWrongNameForIndex,
		codeTableMustHaveColumns:                   mysql.ErrTableMustHaveColumns,
		codeTooManyFields:                          mysql.ErrTooManyFields,
		codeErrTooLongIndexComment:                 mysql.ErrTooLongIndexComment,
		codeViewWrongList:                          mysql.ErrViewWrongList,
		codeUnknownCharacterSet:                    mysql.ErrUnknownCharacterSet,
		codeUnknownCollation:                       mysql.ErrUnknownCollation,
		codeCollationCharsetMismatch:               mysql.ErrCollationCharsetMismatch,
		codeConflictingDeclarations:                mysql.ErrConflictingDeclarations,
		codePartitionsMustBeDefined:                mysql.ErrPartitionsMustBeDefined,
		codePartitionMgmtOnNonpartitioned:          mysql.ErrPartitionMgmtOnNonpartitioned,
		codeDropPartitionNonExistent:               mysql.ErrDropPartitionNonExistent,
		codeSameNamePartition:                      mysql.ErrSameNamePartition,
		codeRangeNotIncreasing:                     mysql.ErrRangeNotIncreasing,
		codePartitionMaxvalue:                      mysql.ErrPartitionMaxvalue,
		codeErrTooManyValues:                       mysql.ErrTooManyValues,
		codeDropLastPartition:                      mysql.ErrDropLastPartition,
		codeTooManyPartitions:                      mysql.ErrTooManyPartitions,
		codeNoParts:                                mysql.ErrNoParts,
		codePartitionFunctionIsNotAllowed:          mysql.ErrPartitionFunctionIsNotAllowed,
		codeErrPartitionFuncNotAllowed:             mysql.ErrPartitionFuncNotAllowed,
		codeErrFieldTypeNotAllowedAsPartitionField: mysql.ErrFieldTypeNotAllowedAsPartitionField,
		codeUniqueKeyNeedAllFieldsInPf:             mysql.ErrUniqueKeyNeedAllFieldsInPf,
		codePrimaryCantHaveNull:                    mysql.ErrPrimaryCantHaveNull,
		codeWrongExprInPartitionFunc:               mysql.ErrWrongExprInPartitionFunc,
		codeWarnDataTruncated:                      mysql.WarnDataTruncated,
		codeErrTableCantHandleFt:                   mysql.ErrTableCantHandleFt,
		codeCoalesceOnlyOnHashPartition:            mysql.ErrCoalesceOnlyOnHashPartition,
		codeUnknownPartition:                       mysql.ErrUnknownPartition,
		codeNotSupportedAlterOperation:             mysql.ErrAlterOperationNotSupportedReason,
		codeErrWrongObject:                         mysql.ErrWrongObject,
		codeFieldNotFoundPart:                      mysql.ErrFieldNotFoundPart,
		codePartitionColumnList:                    mysql.ErrPartitionColumnList,
		codeInvalidDefaultValue:                    mysql.ErrInvalidDefault,
		codeErrGeneratedColumnRefAutoInc:           mysql.ErrGeneratedColumnRefAutoInc,
		codeOnlyOnRangeListPartition:               mysql.ErrOnlyOnRangeListPartition,
		codePartitionRequiresValues:                mysql.ErrPartitionRequiresValues,
		codePartitionWrongNoPart:                   mysql.ErrPartitionWrongNoPart,
		codePartitionWrongNoSubpart:                mysql.ErrPartitionWrongNoSubpart,
		codePartitionWrongValues:                   mysql.ErrPartitionWrongValues,
		codeRowSinglePartitionField:                mysql.ErrRowSinglePartitionField,
		codeSubpartition:                           mysql.ErrSubpartition,
		codeSystemVersioningWrongPartitions:        mysql.ErrSystemVersioningWrongPartitions,
		codeWrongPartitionTypeExpectedSystemTime:   mysql.ErrWrongPartitionTypeExpectedSystemTime,
		codeWrongTypeColumnValue:                   mysql.ErrWrongTypeColumnValue,
		mysql.ErrFkColumnCannotDrop:                mysql.ErrFkColumnCannotDrop,
		mysql.ErrFKIncompatibleColumns:             mysql.ErrFKIncompatibleColumns,
	}
	terror.ErrClassToMySQLCodes[terror.ClassDDL] = ddlMySQLErrCodes
}
