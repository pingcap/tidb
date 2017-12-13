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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/terror"
	log "github.com/sirupsen/logrus"
	"github.com/twinj/uuid"
	goctx "golang.org/x/net/context"
)

const (
	// currentVersion is for all new DDL jobs.
	currentVersion = 1
	// DDLOwnerKey is the ddl owner path that is saved to etcd, and it's exported for testing.
	DDLOwnerKey = "/tidb/ddl/fg/owner"
	ddlPrompt   = "ddl"
)

var (
	// TableColumnCountLimit is limit of the number of columns in a table.
	// It's exported for testing.
	TableColumnCountLimit = 512
	// EnableSplitTableRegion is a flag to decide whether to split a new region for
	// a newly created table. It takes effect only if the Storage supports split
	// region.
	EnableSplitTableRegion = true
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

	errBlobKeyWithoutLength = terror.ClassDDL.New(codeBlobKeyWithoutLength, "index for BLOB/TEXT column must specify a key length")
	errIncorrectPrefixKey   = terror.ClassDDL.New(codeIncorrectPrefixKey, "Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys")
	errTooLongKey           = terror.ClassDDL.New(codeTooLongKey,
		fmt.Sprintf("Specified key was too long; max key length is %d bytes", maxPrefixLength))
	errKeyColumnDoesNotExits = terror.ClassDDL.New(codeKeyColumnDoesNotExits, "this key column doesn't exist in table")
	errDupKeyName            = terror.ClassDDL.New(codeDupKeyName, "duplicate key name")
	errUnknownTypeLength     = terror.ClassDDL.New(codeUnknownTypeLength, "Unknown length for type tp %d")
	errUnknownFractionLength = terror.ClassDDL.New(codeUnknownFractionLength, "Unknown Length for type tp %d and fraction %d")
	errInvalidJobVersion     = terror.ClassDDL.New(codeInvalidJobVersion, "DDL job with version %d greater than current %d")
	errFileNotFound          = terror.ClassDDL.New(codeFileNotFound, "Can't find file: './%s/%s.frm'")
	errErrorOnRename         = terror.ClassDDL.New(codeErrorOnRename, "Error on rename of './%s/%s' to './%s/%s'")
	errBadField              = terror.ClassDDL.New(codeBadField, "Unknown column '%s' in '%s'")
	errInvalidUseOfNull      = terror.ClassDDL.New(codeInvalidUseOfNull, "Invalid use of NULL value")
	errTooManyFields         = terror.ClassDDL.New(codeTooManyFields, "Too many columns")

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
	ErrInvalidOnUpdate = terror.ClassDDL.New(codeInvalidOnUpdate, "invalid ON UPDATE clause for the column")
	// ErrTooLongIdent returns for too long name of database/table/column/index.
	ErrTooLongIdent = terror.ClassDDL.New(codeTooLongIdent, mysql.MySQLErrName[mysql.ErrTooLongIdent])
	// ErrWrongDBName returns for wrong database name.
	ErrWrongDBName = terror.ClassDDL.New(codeWrongDBName, mysql.MySQLErrName[mysql.ErrWrongDBName])
	// ErrWrongTableName returns for wrong table name.
	ErrWrongTableName = terror.ClassDDL.New(codeWrongTableName, mysql.MySQLErrName[mysql.ErrWrongTableName])
	// ErrWrongColumnName returns for wrong column name.
	ErrWrongColumnName = terror.ClassDDL.New(codeWrongColumnName, mysql.MySQLErrName[mysql.ErrWrongColumnName])
	// ErrWrongNameForIndex returns for wrong index name.
	ErrWrongNameForIndex = terror.ClassDDL.New(codeWrongNameForIndex, mysql.MySQLErrName[mysql.ErrWrongNameForIndex])
)

// DDL is responsible for updating schema in data store and maintaining in-memory InfoSchema cache.
type DDL interface {
	CreateSchema(ctx context.Context, name model.CIStr, charsetInfo *ast.CharsetOpt) error
	DropSchema(ctx context.Context, schema model.CIStr) error
	CreateTable(ctx context.Context, ident ast.Ident, cols []*ast.ColumnDef,
		constrs []*ast.Constraint, options []*ast.TableOption) error
	CreateTableWithLike(ctx context.Context, ident, referIdent ast.Ident) error
	DropTable(ctx context.Context, tableIdent ast.Ident) (err error)
	CreateIndex(ctx context.Context, tableIdent ast.Ident, unique bool, indexName model.CIStr,
		columnNames []*ast.IndexColName, indexOption *ast.IndexOption) error
	DropIndex(ctx context.Context, tableIdent ast.Ident, indexName model.CIStr) error
	GetInformationSchema() infoschema.InfoSchema
	AlterTable(ctx context.Context, tableIdent ast.Ident, spec []*ast.AlterTableSpec) error
	TruncateTable(ctx context.Context, tableIdent ast.Ident) error
	RenameTable(ctx context.Context, oldTableIdent, newTableIdent ast.Ident) error
	// SetLease will reset the lease time for online DDL change,
	// it's a very dangerous function and you must guarantee that all servers have the same lease time.
	SetLease(ctx goctx.Context, lease time.Duration)
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
	// OwnerManager gets the owner manager, and it's used for testing.
	OwnerManager() owner.Manager
	// WorkerVars gets the session variables for DDL worker.
	WorkerVars() *variable.SessionVars
	// SetHook sets the hook. It's exported for testing.
	SetHook(h Callback)
	// GetHook gets the hook. It's exported for testing.
	GetHook() Callback
}

// ddl represents the statements which are used to define the database structure or schema.
type ddl struct {
	m sync.RWMutex

	infoHandle   *infoschema.Handle
	hook         Callback
	hookMu       sync.RWMutex
	store        kv.Storage
	ownerManager owner.Manager
	schemaSyncer SchemaSyncer
	// lease is schema seconds.
	lease        time.Duration
	uuid         string
	ddlJobCh     chan struct{}
	ddlJobDoneCh chan struct{}
	ddlEventCh   chan<- *util.Event
	// reorgCtx is for reorganization.
	reorgCtx *reorgCtx

	quitCh chan struct{}
	wait   sync.WaitGroup

	workerVars      *variable.SessionVars
	delRangeManager delRangeManager
}

// RegisterEventCh registers passed channel for ddl Event.
func (d *ddl) RegisterEventCh(ch chan<- *util.Event) {
	d.ddlEventCh = ch
}

// asyncNotifyEvent will notify the ddl event to outside world, say statistic handle. When the channel is full, we may
// give up notify and log it.
func (d *ddl) asyncNotifyEvent(e *util.Event) {
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
func NewDDL(ctx goctx.Context, etcdCli *clientv3.Client, store kv.Storage,
	infoHandle *infoschema.Handle, hook Callback, lease time.Duration, ctxPool *pools.ResourcePool) DDL {
	return newDDL(ctx, etcdCli, store, infoHandle, hook, lease, ctxPool)
}

func newDDL(ctx goctx.Context, etcdCli *clientv3.Client, store kv.Storage,
	infoHandle *infoschema.Handle, hook Callback, lease time.Duration, ctxPool *pools.ResourcePool) *ddl {
	if hook == nil {
		hook = &BaseCallback{}
	}

	id := uuid.NewV4().String()
	ctx, cancelFunc := goctx.WithCancel(ctx)
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
	d := &ddl{
		infoHandle:   infoHandle,
		hook:         hook,
		store:        store,
		uuid:         id,
		lease:        lease,
		ddlJobCh:     make(chan struct{}, 1),
		ddlJobDoneCh: make(chan struct{}, 1),
		reorgCtx:     &reorgCtx{notifyCancelReorgJob: make(chan struct{}, 1)},
		ownerManager: manager,
		schemaSyncer: syncer,
		workerVars:   variable.NewSessionVars(),
	}
	d.workerVars.BinlogClient = binloginfo.GetPumpClient()

	if ctxPool != nil {
		supportDelRange := store.SupportDeleteRange()
		d.delRangeManager = newDelRangeManager(d, ctxPool, supportDelRange)
		log.Infof("[ddl] start delRangeManager OK, with emulator: %t", !supportDelRange)
	} else {
		d.delRangeManager = newMockDelRangeManager()
	}

	d.start(ctx)
	variable.RegisterStatistics(d)
	log.Infof("[ddl] start DDL:%s", d.uuid)
	return d
}

func (d *ddl) Stop() error {
	d.m.Lock()
	defer d.m.Unlock()

	d.close()
	log.Infof("stop DDL:%s", d.uuid)

	return nil
}

func (d *ddl) start(ctx goctx.Context) {
	d.quitCh = make(chan struct{})
	err := d.ownerManager.CampaignOwner(ctx)
	terror.Log(errors.Trace(err))
	d.wait.Add(1)
	go d.onDDLWorker()

	// For every start, we will send a fake job to let worker
	// check owner firstly and try to find whether a job exists and run.
	asyncNotify(d.ddlJobCh)

	d.delRangeManager.start()
}

func (d *ddl) close() {
	if d.isClosed() {
		return
	}
	close(d.quitCh)
	d.ownerManager.Cancel()
	err := d.schemaSyncer.RemoveSelfVersionPath()
	if err != nil {
		log.Errorf("[ddl] remove self version path failed %v", err)
	}
	d.wait.Wait()
	d.delRangeManager.clear()
	log.Infof("close DDL:%s", d.uuid)
}

func (d *ddl) isClosed() bool {
	select {
	case <-d.quitCh:
		return true
	default:
		return false
	}
}

func (d *ddl) SetLease(ctx goctx.Context, lease time.Duration) {
	d.m.Lock()
	defer d.m.Unlock()

	if lease == d.lease {
		return
	}

	log.Warnf("[ddl] change schema lease %s -> %s", d.lease, lease)

	if d.isClosed() {
		// If already closed, just set lease and return.
		d.lease = lease
		return
	}

	// Close the running worker and start again.
	d.close()
	d.lease = lease
	d.start(ctx)
}

func (d *ddl) GetLease() time.Duration {
	d.m.RLock()
	lease := d.lease
	d.m.RUnlock()
	return lease
}

func (d *ddl) GetInformationSchema() infoschema.InfoSchema {
	return d.infoHandle.Get()
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

func checkJobMaxInterval(job *model.Job) time.Duration {
	// The job of adding index takes more time to process.
	// So it uses the longer time.
	if job.Type == model.ActionAddIndex {
		return 3 * time.Second
	}
	return 1 * time.Second
}

func (d *ddl) doDDLJob(ctx context.Context, job *model.Job) error {
	// For every DDL, we must commit current transaction.
	if err := ctx.NewTxn(); err != nil {
		return errors.Trace(err)
	}

	// Get a global job ID and put the DDL job in the queue.
	err := d.addDDLJob(ctx, job)
	if err != nil {
		return errors.Trace(err)
	}

	// Notice worker that we push a new job and wait the job done.
	asyncNotify(d.ddlJobCh)
	log.Infof("[ddl] start DDL job %s, Query:\n%s", job, job.Query)

	var historyJob *model.Job
	jobID := job.ID
	// For a job from start to end, the state of it will be none -> delete only -> write only -> reorganization -> public
	// For every state changes, we will wait as lease 2 * lease time, so here the ticker check is 10 * lease.
	// But we use etcd to speed up, normally it takes less than 1s now, so we use 1s or 3s as the max value.
	ticker := time.NewTicker(chooseLeaseTime(10*d.lease, checkJobMaxInterval(job)))
	startTime := time.Now()
	jobsGauge.WithLabelValues(job.Type.String()).Inc()
	defer func() {
		ticker.Stop()
		jobsGauge.WithLabelValues(job.Type.String()).Dec()
		retLabel := handleJobSucc
		if err != nil {
			retLabel = handleJobFailed
		}
		handleJobHistogram.WithLabelValues(job.Type.String(), retLabel).Observe(time.Since(startTime).Seconds())
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
			log.Debugf("[ddl] DDL job %d is not in history, maybe not run", jobID)
			continue
		}

		// If a job is a history job, the state must be JobSynced or JobCancel.
		if historyJob.IsSynced() {
			log.Infof("[ddl] DDL job %d is finished", jobID)
			return nil
		}

		return errors.Trace(historyJob.Error)
	}
}

func (d *ddl) callHookOnChanged(err error) error {
	d.hookMu.Lock()
	defer d.hookMu.Unlock()

	err = d.hook.OnChanged(err)
	return errors.Trace(err)
}

// SetHook implements DDL.SetHook interface.
func (d *ddl) SetHook(h Callback) {
	d.hookMu.Lock()
	defer d.hookMu.Unlock()

	d.hook = h
}

// GetHook implements DDL.GetHook interface.
func (d *ddl) GetHook() Callback {
	d.hookMu.RLock()
	defer d.hookMu.RUnlock()

	return d.hook
}

func (d *ddl) WorkerVars() *variable.SessionVars {
	return d.workerVars
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

	codeFileNotFound                 = 1017
	codeErrorOnRename                = 1025
	codeBadNull                      = 1048
	codeBadField                     = 1054
	codeTooLongIdent                 = 1059
	codeDupKeyName                   = 1061
	codeTooLongKey                   = 1071
	codeKeyColumnDoesNotExits        = 1072
	codeIncorrectPrefixKey           = 1089
	codeCantRemoveAllFields          = 1090
	codeCantDropFieldOrKey           = 1091
	codeBlobCantHaveDefault          = 1101
	codeWrongDBName                  = 1102
	codeWrongTableName               = 1103
	codeTooManyFields                = 1117
	codeInvalidUseOfNull             = 1138
	codeWrongColumnName              = 1166
	codeWrongKeyColumn               = 1167
	codeBlobKeyWithoutLength         = 1170
	codeInvalidOnUpdate              = 1294
	codeUnsupportedOnGeneratedColumn = 3106
	codeGeneratedColumnNonPrior      = 3107
	codeDependentByGeneratedColumn   = 3108
	codeJSONUsedAsKey                = 3152
	codeWrongNameForIndex            = terror.ErrCode(mysql.ErrWrongNameForIndex)
	codeErrTooLongIndexComment       = terror.ErrCode(mysql.ErrTooLongIndexComment)
)

func init() {
	ddlMySQLErrCodes := map[terror.ErrCode]uint16{
		codeBadNull:                      mysql.ErrBadNull,
		codeCantRemoveAllFields:          mysql.ErrCantRemoveAllFields,
		codeCantDropFieldOrKey:           mysql.ErrCantDropFieldOrKey,
		codeInvalidOnUpdate:              mysql.ErrInvalidOnUpdate,
		codeBlobKeyWithoutLength:         mysql.ErrBlobKeyWithoutLength,
		codeIncorrectPrefixKey:           mysql.ErrWrongSubKey,
		codeTooLongIdent:                 mysql.ErrTooLongIdent,
		codeTooLongKey:                   mysql.ErrTooLongKey,
		codeKeyColumnDoesNotExits:        mysql.ErrKeyColumnDoesNotExits,
		codeDupKeyName:                   mysql.ErrDupKeyName,
		codeWrongDBName:                  mysql.ErrWrongDBName,
		codeWrongTableName:               mysql.ErrWrongTableName,
		codeFileNotFound:                 mysql.ErrFileNotFound,
		codeErrorOnRename:                mysql.ErrErrorOnRename,
		codeBadField:                     mysql.ErrBadField,
		codeInvalidUseOfNull:             mysql.ErrInvalidUseOfNull,
		codeUnsupportedOnGeneratedColumn: mysql.ErrUnsupportedOnGeneratedColumn,
		codeGeneratedColumnNonPrior:      mysql.ErrGeneratedColumnNonPrior,
		codeDependentByGeneratedColumn:   mysql.ErrDependentByGeneratedColumn,
		codeJSONUsedAsKey:                mysql.ErrJSONUsedAsKey,
		codeBlobCantHaveDefault:          mysql.ErrBlobCantHaveDefault,
		codeWrongColumnName:              mysql.ErrWrongColumnName,
		codeWrongKeyColumn:               mysql.ErrWrongKeyColumn,
		codeWrongNameForIndex:            mysql.ErrWrongNameForIndex,
		codeTooManyFields:                mysql.ErrTooManyFields,
		codeErrTooLongIndexComment:       mysql.ErrTooLongIndexComment,
	}
	terror.ErrClassToMySQLCodes[terror.ClassDDL] = ddlMySQLErrCodes
}
