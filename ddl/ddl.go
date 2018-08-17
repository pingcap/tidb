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
	"fmt"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/juju/errors"
	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/terror"
	log "github.com/sirupsen/logrus"
	"github.com/twinj/uuid"
	"golang.org/x/net/context"
)

const (
	// currentVersion is for all new DDL jobs.
	currentVersion = 1
	// DDLOwnerKey is the ddl owner path that is saved to etcd, and it's exported for testing.
	DDLOwnerKey = "/tidb/ddl/fg/owner"
	ddlPrompt   = "ddl"

	shardRowIDBitsMax = 15
)

var (
	// TableColumnCountLimit is limit of the number of columns in a table.
	// It's exported for testing.
	TableColumnCountLimit = 512
	// EnableSplitTableRegion is a flag to decide whether to split a new region for
	// a newly created table. It takes effect only if the Storage supports split
	// region.
	EnableSplitTableRegion = false

	// PartitionCountLimit is limit of the number of partitions in a table.
	// Mysql maximum number of partitions is 8192, our maximum number of partitions is 1024.
	// Reference linking https://dev.mysql.com/doc/refman/5.7/en/partitioning-limitations.html.
	PartitionCountLimit = 1024
)

var (
	// errWorkerClosed means we have already closed the DDL worker.
	errInvalidWorker = terror.ClassDDL.New(codeInvalidWorker, "invalid worker")
	// errNotOwner means we are not owner and can't handle DDL jobs.
	errNotOwner              = terror.ClassDDL.New(codeNotOwner, "not Owner")
	errInvalidDDLJob         = terror.ClassDDL.New(codeInvalidDDLJob, "invalid DDL job")
	errCancelledDDLJob       = terror.ClassDDL.New(codeCancelledDDLJob, "cancelled DDL job")
	errInvalidJobFlag        = terror.ClassDDL.New(codeInvalidJobFlag, "invalid job flag")
	errRunMultiSchemaChanges = terror.ClassDDL.New(codeRunMultiSchemaChanges, "can't run multi schema change")
	errWaitReorgTimeout      = terror.ClassDDL.New(codeWaitReorgTimeout, "wait for reorganization timeout")
	errInvalidStoreVer       = terror.ClassDDL.New(codeInvalidStoreVer, "invalid storage current version")

	// We don't support dropping column with index covered now.
	errCantDropColWithIndex    = terror.ClassDDL.New(codeCantDropColWithIndex, "can't drop column with index")
	errUnsupportedAddColumn    = terror.ClassDDL.New(codeUnsupportedAddColumn, "unsupported add column")
	errUnsupportedModifyColumn = terror.ClassDDL.New(codeUnsupportedModifyColumn, "unsupported modify column %s")
	errUnsupportedPKHandle     = terror.ClassDDL.New(codeUnsupportedDropPKHandle,
		"unsupported drop integer primary key")
	errUnsupportedCharset = terror.ClassDDL.New(codeUnsupportedCharset, "unsupported charset %s collate %s")

	errUnsupportedShardRowIDBits = terror.ClassDDL.New(codeUnsupportedShardRowIDBits, "unsupported shard_row_id_bits for table with auto_increment column.")

	errBlobKeyWithoutLength = terror.ClassDDL.New(codeBlobKeyWithoutLength, "index for BLOB/TEXT column must specify a key length")
	errIncorrectPrefixKey   = terror.ClassDDL.New(codeIncorrectPrefixKey, "Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys")
	errTooLongKey           = terror.ClassDDL.New(codeTooLongKey,
		fmt.Sprintf("Specified key was too long; max key length is %d bytes", maxPrefixLength))
	errKeyColumnDoesNotExits    = terror.ClassDDL.New(codeKeyColumnDoesNotExits, "this key column doesn't exist in table")
	errUnknownTypeLength        = terror.ClassDDL.New(codeUnknownTypeLength, "Unknown length for type tp %d")
	errUnknownFractionLength    = terror.ClassDDL.New(codeUnknownFractionLength, "Unknown Length for type tp %d and fraction %d")
	errInvalidJobVersion        = terror.ClassDDL.New(codeInvalidJobVersion, "DDL job with version %d greater than current %d")
	errFileNotFound             = terror.ClassDDL.New(codeFileNotFound, "Can't find file: './%s/%s.frm'")
	errErrorOnRename            = terror.ClassDDL.New(codeErrorOnRename, "Error on rename of './%s/%s' to './%s/%s'")
	errBadField                 = terror.ClassDDL.New(codeBadField, "Unknown column '%s' in '%s'")
	errInvalidUseOfNull         = terror.ClassDDL.New(codeInvalidUseOfNull, "Invalid use of NULL value")
	errTooManyFields            = terror.ClassDDL.New(codeTooManyFields, "Too many columns")
	errInvalidSplitRegionRanges = terror.ClassDDL.New(codeInvalidRanges, "Failed to split region ranges")
	errReorgPanic               = terror.ClassDDL.New(codeReorgWorkerPanic, "reorg worker panic.")

	// errWrongKeyColumn is for table column cannot be indexed.
	errWrongKeyColumn = terror.ClassDDL.New(codeWrongKeyColumn, mysql.MySQLErrName[mysql.ErrWrongKeyColumn])
	// errUnsupportedOnGeneratedColumn is for unsupported actions on generated columns.
	errUnsupportedOnGeneratedColumn = terror.ClassDDL.New(codeUnsupportedOnGeneratedColumn, mysql.MySQLErrName[mysql.ErrUnsupportedOnGeneratedColumn])
	// errGeneratedColumnNonPrior forbiddens to refer generated column non prior to it.
	errGeneratedColumnNonPrior = terror.ClassDDL.New(codeGeneratedColumnNonPrior, mysql.MySQLErrName[mysql.ErrGeneratedColumnNonPrior])
	// errDependentByGeneratedColumn forbiddens to delete columns which are dependent by generated columns.
	errDependentByGeneratedColumn = terror.ClassDDL.New(codeDependentByGeneratedColumn, mysql.MySQLErrName[mysql.ErrDependentByGeneratedColumn])
	// errJSONUsedAsKey forbiddens to use JSON as key or index.
	errJSONUsedAsKey = terror.ClassDDL.New(codeJSONUsedAsKey, mysql.MySQLErrName[mysql.ErrJSONUsedAsKey])
	// errBlobCantHaveDefault forbiddens to give not null default value to TEXT/BLOB/JSON.
	errBlobCantHaveDefault = terror.ClassDDL.New(codeBlobCantHaveDefault, mysql.MySQLErrName[mysql.ErrBlobCantHaveDefault])
	errTooLongIndexComment = terror.ClassDDL.New(codeErrTooLongIndexComment, mysql.MySQLErrName[mysql.ErrTooLongIndexComment])

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
	// ErrUnsupportedModifyPrimaryKey returns an error when add or drop the primary key.
	// It's exported for testing.
	ErrUnsupportedModifyPrimaryKey = terror.ClassDDL.New(codeUnsupportedModifyPrimaryKey, "unsupported %s primary key")

	// ErrColumnBadNull returns for a bad null value.
	ErrColumnBadNull = terror.ClassDDL.New(codeBadNull, "column cann't be null")
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
	// ErrPrimaryCantHaveNull returns All parts of a PRIMARY KEY must be NOT NULL; if you need NULL in a key, use UNIQUE instead
	ErrPrimaryCantHaveNull = terror.ClassDDL.New(codePrimaryCantHaveNull, mysql.MySQLErrName[mysql.ErrPrimaryCantHaveNull])

	// ErrNotAllowedTypeInPartition returns not allowed type error when creating table partiton with unsupport expression type.
	ErrNotAllowedTypeInPartition = terror.ClassDDL.New(codeErrFieldTypeNotAllowedAsPartitionField, mysql.MySQLErrName[mysql.ErrFieldTypeNotAllowedAsPartitionField])
	// ErrPartitionsMustBeDefined returns each partition must be defined.
	ErrPartitionsMustBeDefined = terror.ClassDDL.New(codePartitionsMustBeDefined, "For RANGE partitions each partition must be defined")
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
	// ErrTooManyValues returns cannot have more than one value for this type of partitioning.
	ErrTooManyValues = terror.ClassDDL.New(codeErrTooManyValues, mysql.MySQLErrName[mysql.ErrTooManyValues])
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
)

// DDL is responsible for updating schema in data store and maintaining in-memory InfoSchema cache.
type DDL interface {
	CreateSchema(ctx sessionctx.Context, name model.CIStr, charsetInfo *ast.CharsetOpt) error
	DropSchema(ctx sessionctx.Context, schema model.CIStr) error
	CreateTable(ctx sessionctx.Context, stmt *ast.CreateTableStmt) error
	CreateTableWithLike(ctx sessionctx.Context, ident, referIdent ast.Ident, ifNotExists bool) error
	DropTable(ctx sessionctx.Context, tableIdent ast.Ident) (err error)
	CreateIndex(ctx sessionctx.Context, tableIdent ast.Ident, unique bool, indexName model.CIStr,
		columnNames []*ast.IndexColName, indexOption *ast.IndexOption) error
	DropIndex(ctx sessionctx.Context, tableIdent ast.Ident, indexName model.CIStr) error
	AlterTable(ctx sessionctx.Context, tableIdent ast.Ident, spec []*ast.AlterTableSpec) error
	TruncateTable(ctx sessionctx.Context, tableIdent ast.Ident) error
	RenameTable(ctx sessionctx.Context, oldTableIdent, newTableIdent ast.Ident) error

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
	SchemaSyncer() SchemaSyncer
	// OwnerManager gets the owner manager.
	OwnerManager() owner.Manager
	// GetID gets the ddl ID.
	GetID() string
	// GetTableMaxRowID gets the max row ID of a normal table or a partition.
	GetTableMaxRowID(startTS uint64, tbl table.PhysicalTable) (int64, bool, error)
	// SetBinlogClient sets the binlog client for DDL worker. It's exported for testing.
	SetBinlogClient(interface{})
}

// ddl is used to handle the statements that define the structure or schema of the database.
type ddl struct {
	m          sync.RWMutex
	infoHandle *infoschema.Handle
	quitCh     chan struct{}

	*ddlCtx
	workers map[workerType]*worker
}

// ddlCtx is the context when we use worker to handle DDL jobs.
type ddlCtx struct {
	uuid         string
	store        kv.Storage
	ownerManager owner.Manager
	schemaSyncer SchemaSyncer
	ddlJobDoneCh chan struct{}
	ddlEventCh   chan<- *util.Event
	lease        time.Duration // lease is schema lease.
	binlogCli    interface{}   // binlogCli is used for Binlog.

	// hook may be modified.
	mu struct {
		sync.RWMutex
		hook        Callback
		interceptor Interceptor
	}
}

func (dc *ddlCtx) isOwner() bool {
	isOwner := dc.ownerManager.IsOwner()
	log.Debugf("[ddl] it's the DDL owner %v, self ID %s", isOwner, dc.uuid)
	if isOwner {
		metrics.DDLCounter.WithLabelValues(metrics.IsDDLOwner).Inc()
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
				log.Warnf("Fail to notify event %s.", e)
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
	id := uuid.NewV4().String()
	ctx, cancelFunc := context.WithCancel(ctx)
	var manager owner.Manager
	var syncer SchemaSyncer
	if etcdCli == nil {
		// The etcdCli is nil if the store is localstore which is only used for testing.
		// So we use mockOwnerManager and mockSchemaSyncer.
		manager = owner.NewMockManager(id, cancelFunc)
		syncer = NewMockSchemaSyncer()
	} else {
		manager = owner.NewOwnerManager(etcdCli, ddlPrompt, id, DDLOwnerKey, cancelFunc)
		syncer = NewSchemaSyncer(etcdCli, id)
	}

	ddlCtx := &ddlCtx{
		uuid:         id,
		store:        store,
		lease:        lease,
		ddlJobDoneCh: make(chan struct{}, 1),
		ownerManager: manager,
		schemaSyncer: syncer,
		binlogCli:    binloginfo.GetPumpClient(),
	}
	ddlCtx.mu.hook = hook
	ddlCtx.mu.interceptor = &BaseInterceptor{}
	d := &ddl{
		infoHandle: infoHandle,
		ddlCtx:     ddlCtx,
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
	log.Infof("[ddl] stop DDL:%s", d.uuid)
	return nil
}

// start campaigns the owner and starts workers.
// ctxPool is used for the worker's delRangeManager and creates sessions.
func (d *ddl) start(ctx context.Context, ctxPool *pools.ResourcePool) {
	log.Infof("[ddl] start DDL:%s, run worker %v", d.uuid, RunWorker)
	d.quitCh = make(chan struct{})

	// If RunWorker is true, we need campaign owner and do DDL job.
	// Otherwise, we needn't do that.
	if RunWorker {
		err := d.ownerManager.CampaignOwner(ctx)
		terror.Log(errors.Trace(err))

		d.workers = make(map[workerType]*worker, 2)
		d.workers[generalWorker] = newWorker(generalWorker, d.store, ctxPool)
		d.workers[addIdxWorker] = newWorker(addIdxWorker, d.store, ctxPool)
		for _, worker := range d.workers {
			worker.wg.Add(1)
			go worker.start(d.ddlCtx)
			metrics.DDLCounter.WithLabelValues(fmt.Sprintf("%s_%s", metrics.CreateDDL, worker.String())).Inc()

			// When the start function is called, we will send a fake job to let worker
			// checks owner firstly and try to find whether a job exists and run.
			asyncNotify(worker.ddlJobCh)
		}
	}
}

func (d *ddl) close() {
	if isChanClosed(d.quitCh) {
		return
	}

	startTime := time.Now()
	close(d.quitCh)
	d.ownerManager.Cancel()
	err := d.schemaSyncer.RemoveSelfVersionPath()
	if err != nil {
		log.Errorf("[ddl] remove self version path failed %v", err)
	}

	for _, worker := range d.workers {
		worker.close()
	}
	log.Infof("[ddl] closing DDL:%s takes time %v", d.uuid, time.Since(startTime))
}

// GetLease implements DDL.GetLease interface.
func (d *ddl) GetLease() time.Duration {
	d.m.RLock()
	lease := d.lease
	d.m.RUnlock()
	return lease
}

// GetInformationSchema gets the infoschema binding to d. It's expoted for testing.
func (d *ddl) GetInformationSchema(ctx sessionctx.Context) infoschema.InfoSchema {
	is := d.infoHandle.Get()

	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.mu.interceptor.OnGetInfoSchema(ctx, is)
}

func (d *ddl) genGlobalID() (int64, error) {
	var globalID int64
	err := kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
		var err error
		globalID, err = meta.NewMeta(txn).GenGlobalID()
		return errors.Trace(err)
	})

	return globalID, errors.Trace(err)
}

// SchemaSyncer implements DDL.SchemaSyncer interface.
func (d *ddl) SchemaSyncer() SchemaSyncer {
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
	if job.Type == model.ActionAddIndex {
		return 3 * time.Second
	}
	return 1 * time.Second
}

func (d *ddl) asyncNotifyWorker(jobTp model.ActionType) {
	// If the workers don't run, we needn't to notify workers.
	if !RunWorker {
		return
	}

	if jobTp == model.ActionAddIndex {
		asyncNotify(d.workers[addIdxWorker].ddlJobCh)
	} else {
		asyncNotify(d.workers[generalWorker].ddlJobCh)
	}
}

func (d *ddl) doDDLJob(ctx sessionctx.Context, job *model.Job) error {
	// For every DDL, we must commit current transaction.
	if err := ctx.NewTxn(); err != nil {
		return errors.Trace(err)
	}

	// Get a global job ID and put the DDL job in the queue.
	err := d.addDDLJob(ctx, job)
	if err != nil {
		return errors.Trace(err)
	}
	ctx.GetSessionVars().StmtCtx.IsDDLJobInQueue = true

	// Notice worker that we push a new job and wait the job done.
	d.asyncNotifyWorker(job.Type)
	log.Infof("[ddl] start DDL job %s, Query:%s", job, job.Query)

	var historyJob *model.Job
	jobID := job.ID
	// For a job from start to end, the state of it will be none -> delete only -> write only -> reorganization -> public
	// For every state changes, we will wait as lease 2 * lease time, so here the ticker check is 10 * lease.
	// But we use etcd to speed up, normally it takes less than 1s now, so we use 1s or 3s as the max value.
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
			log.Errorf("[ddl] get history DDL job err %v, check again", err)
			continue
		} else if historyJob == nil {
			log.Debugf("[ddl] DDL job ID:%d is not in history, maybe not run", jobID)
			continue
		}

		// If a job is a history job, the state must be JobSynced or JobCancel.
		if historyJob.IsSynced() {
			log.Infof("[ddl] DDL job ID:%d is finished", jobID)
			return nil
		}

		if historyJob.Error != nil {
			return errors.Trace(historyJob.Error)
		}
		panic("When the state is JobCancel, historyJob.Error should never be nil")
	}
}

func (d *ddl) callHookOnChanged(err error) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	err = d.mu.hook.OnChanged(err)
	return errors.Trace(err)
}

// SetBinlogClient implements DDL.SetBinlogClient interface.
func (d *ddl) SetBinlogClient(binlogCli interface{}) {
	d.binlogCli = binlogCli
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

	codeInvalidDBState         = 100
	codeInvalidTableState      = 101
	codeInvalidColumnState     = 102
	codeInvalidIndexState      = 103
	codeInvalidForeignKeyState = 104

	codeCantDropColWithIndex        = 201
	codeUnsupportedAddColumn        = 202
	codeUnsupportedModifyColumn     = 203
	codeUnsupportedDropPKHandle     = 204
	codeUnsupportedCharset          = 205
	codeUnsupportedModifyPrimaryKey = 206
	codeUnsupportedShardRowIDBits   = 207

	codeFileNotFound                           = 1017
	codeErrorOnRename                          = 1025
	codeBadNull                                = mysql.ErrBadNull
	codeBadField                               = 1054
	codeTooLongIdent                           = 1059
	codeDupKeyName                             = 1061
	codeTooLongKey                             = 1071
	codeKeyColumnDoesNotExits                  = 1072
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
	codeUnsupportedOnGeneratedColumn           = 3106
	codeGeneratedColumnNonPrior                = 3107
	codeDependentByGeneratedColumn             = 3108
	codeJSONUsedAsKey                          = 3152
	codeWrongNameForIndex                      = terror.ErrCode(mysql.ErrWrongNameForIndex)
	codeErrTooLongIndexComment                 = terror.ErrCode(mysql.ErrTooLongIndexComment)
	codeUnknownCharacterSet                    = terror.ErrCode(mysql.ErrUnknownCharacterSet)
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
	codePartitionFunctionIsNotAllowed          = terror.ErrCode(mysql.ErrPartitionFunctionIsNotAllowed)
	codeErrPartitionFuncNotAllowed             = terror.ErrCode(mysql.ErrPartitionFuncNotAllowed)
	codeErrFieldTypeNotAllowedAsPartitionField = terror.ErrCode(mysql.ErrFieldTypeNotAllowedAsPartitionField)
	codeUniqueKeyNeedAllFieldsInPf             = terror.ErrCode(mysql.ErrUniqueKeyNeedAllFieldsInPf)
	codePrimaryCantHaveNull                    = terror.ErrCode(mysql.ErrPrimaryCantHaveNull)
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
		codeUnknownCharacterSet:                    mysql.ErrUnknownCharacterSet,
		codePartitionsMustBeDefined:                mysql.ErrPartitionsMustBeDefined,
		codePartitionMgmtOnNonpartitioned:          mysql.ErrPartitionMgmtOnNonpartitioned,
		codeDropPartitionNonExistent:               mysql.ErrDropPartitionNonExistent,
		codeSameNamePartition:                      mysql.ErrSameNamePartition,
		codeRangeNotIncreasing:                     mysql.ErrRangeNotIncreasing,
		codePartitionMaxvalue:                      mysql.ErrPartitionMaxvalue,
		codeErrTooManyValues:                       mysql.ErrTooManyValues,
		codeDropLastPartition:                      mysql.ErrDropLastPartition,
		codeTooManyPartitions:                      mysql.ErrTooManyPartitions,
		codePartitionFunctionIsNotAllowed:          mysql.ErrPartitionFunctionIsNotAllowed,
		codeErrPartitionFuncNotAllowed:             mysql.ErrPartitionFuncNotAllowed,
		codeErrFieldTypeNotAllowedAsPartitionField: mysql.ErrFieldTypeNotAllowedAsPartitionField,
		codeUniqueKeyNeedAllFieldsInPf:             mysql.ErrUniqueKeyNeedAllFieldsInPf,
		codePrimaryCantHaveNull:                    mysql.ErrPrimaryCantHaveNull,
	}
	terror.ErrClassToMySQLCodes[terror.ClassDDL] = ddlMySQLErrCodes
}
