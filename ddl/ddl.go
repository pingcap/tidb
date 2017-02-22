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

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/terror"
	"github.com/twinj/uuid"
)

var (
	// errWorkerClosed means we have already closed the DDL worker.
	errInvalidWorker = terror.ClassDDL.New(codeInvalidWorker, "invalid worker")
	// errNotOwner means we are not owner and can't handle DDL jobs.
	errNotOwner              = terror.ClassDDL.New(codeNotOwner, "not Owner")
	errInvalidDDLJob         = terror.ClassDDL.New(codeInvalidDDLJob, "invalid ddl job")
	errInvalidBgJob          = terror.ClassDDL.New(codeInvalidBgJob, "invalid background job")
	errInvalidJobFlag        = terror.ClassDDL.New(codeInvalidJobFlag, "invalid job flag")
	errRunMultiSchemaChanges = terror.ClassDDL.New(codeRunMultiSchemaChanges, "can't run multi schema change")
	errWaitReorgTimeout      = terror.ClassDDL.New(codeWaitReorgTimeout, "wait for reorganization timeout")
	errInvalidStoreVer       = terror.ClassDDL.New(codeInvalidStoreVer, "invalid storage current version")

	// We don't support dropping column with index covered now.
	errCantDropColWithIndex    = terror.ClassDDL.New(codeCantDropColWithIndex, "can't drop column with index")
	errUnsupportedAddColumn    = terror.ClassDDL.New(codeUnsupportedAddColumn, "unsupported add column")
	errUnsupportedModifyColumn = terror.ClassDDL.New(codeUnsupportedModifyColumn, "unsupported modify column")
	errUnsupportedPKHandle     = terror.ClassDDL.New(codeUnsupportedDropPKHandle,
		"unsupported drop integer primary key")

	errBlobKeyWithoutLength = terror.ClassDDL.New(codeBlobKeyWithoutLength, "index for BLOB/TEXT column must specificate a key length")
	errIncorrectPrefixKey   = terror.ClassDDL.New(codeIncorrectPrefixKey, "Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys")
	errTooLongKey           = terror.ClassDDL.New(codeTooLongKey,
		fmt.Sprintf("Specified key was too long; max key length is %d bytes", maxPrefixLength))
	errKeyColumnDoesNotExits = terror.ClassDDL.New(codeKeyColumnDoesNotExits, "this key column doesn't exist in table")
	errDupKeyName            = terror.ClassDDL.New(codeDupKeyName, "duplicate key name")
	errWrongDBName           = terror.ClassDDL.New(codeWrongDBName, "Incorrect database name '%s'")
	errWrongTableName        = terror.ClassDDL.New(codeWrongTableName, "Incorrect table name '%s'")
	errUnknownTypeLength     = terror.ClassDDL.New(codeUnknownTypeLength, "Unknown length for type tp %d")
	errUnknownFractionLength = terror.ClassDDL.New(codeUnknownFractionLength, "Unknown Length for type tp %d and fraction %d")
	errFileNotFound          = terror.ClassDDL.New(codeFileNotFound, "Can't find file: './%s/%s.frm'")
	errErrorOnRename         = terror.ClassDDL.New(codeErrorOnRename, "Error on rename of './%s/%s' to './%s/%s'")
	errBadField              = terror.ClassDDL.New(codeBadField, "Unknown column '%s' in '%s'")
	errInvalidDefault        = terror.ClassDDL.New(codeInvalidDefault, "Invalid default value for '%s'")
	errInvalidUseOfNull      = terror.ClassDDL.New(codeInvalidUseOfNull, "Invalid use of NULL value")

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

	// ErrColumnBadNull returns for a bad null value.
	ErrColumnBadNull = terror.ClassDDL.New(codeBadNull, "column cann't be null")
	// ErrCantRemoveAllFields returns for deleting all columns.
	ErrCantRemoveAllFields = terror.ClassDDL.New(codeCantRemoveAllFields, "can't delete all columns with ALTER TABLE")
	// ErrCantDropFieldOrKey returns for dropping a non-existent field or key.
	ErrCantDropFieldOrKey = terror.ClassDDL.New(codeCantDropFieldOrKey, "can't drop field; check that column/key exists")
	// ErrInvalidOnUpdate returns for invalid ON UPDATE clause.
	ErrInvalidOnUpdate = terror.ClassDDL.New(codeInvalidOnUpdate, "invalid ON UPDATE clause for the column")
	// ErrTooLongIdent returns for too long name of database/table/column.
	ErrTooLongIdent = terror.ClassDDL.New(codeTooLongIdent, "Identifier name too long")
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
		columnNames []*ast.IndexColName) error
	DropIndex(ctx context.Context, tableIdent ast.Ident, indexName model.CIStr) error
	GetInformationSchema() infoschema.InfoSchema
	AlterTable(ctx context.Context, tableIdent ast.Ident, spec []*ast.AlterTableSpec) error
	TruncateTable(ctx context.Context, tableIdent ast.Ident) error
	RenameTable(ctx context.Context, oldTableIdent, newTableIdent ast.Ident) error
	// SetLease will reset the lease time for online DDL change,
	// it's a very dangerous function and you must guarantee that all servers have the same lease time.
	SetLease(lease time.Duration)
	// GetLease returns current schema lease time.
	GetLease() time.Duration
	// Stats returns the DDL statistics.
	Stats() (map[string]interface{}, error)
	// GetScope gets the status variables scope.
	GetScope(status string) variable.ScopeFlag
	// Stop stops DDL worker.
	Stop() error
	// Start starts DDL worker.
	Start() error
}

type ddl struct {
	m sync.RWMutex

	infoHandle *infoschema.Handle
	hook       Callback
	hookMu     sync.RWMutex
	store      kv.Storage
	// Schema lease seconds.
	lease        time.Duration
	uuid         string
	ddlJobCh     chan struct{}
	ddlJobDoneCh chan struct{}
	// Drop database/table job that runs in the background.
	bgJobCh chan struct{}
	// reorgDoneCh is for reorganization, if the reorganization job is done,
	// we will use this channel to notify outer.
	// TODO: Now we use goroutine to simulate reorganization jobs, later we may
	// use a persistent job list.
	reorgDoneCh chan error
	// reorgRowCount is for reorganization, it uses to simulate a job's row count.
	reorgRowCount int64

	quitCh chan struct{}
	wait   sync.WaitGroup
}

// NewDDL creates a new DDL.
func NewDDL(store kv.Storage, infoHandle *infoschema.Handle, hook Callback, lease time.Duration) DDL {
	return newDDL(store, infoHandle, hook, lease)
}

func newDDL(store kv.Storage, infoHandle *infoschema.Handle, hook Callback, lease time.Duration) *ddl {
	if hook == nil {
		hook = &BaseCallback{}
	}

	d := &ddl{
		infoHandle:   infoHandle,
		hook:         hook,
		store:        store,
		lease:        lease,
		uuid:         uuid.NewV4().String(),
		ddlJobCh:     make(chan struct{}, 1),
		ddlJobDoneCh: make(chan struct{}, 1),
		bgJobCh:      make(chan struct{}, 1),
	}

	d.start()

	variable.RegisterStatistics(d)
	log.Infof("start DDL:%s", d.uuid)

	return d
}

func (d *ddl) Stop() error {
	d.m.Lock()
	defer d.m.Unlock()

	d.close()

	err := kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		owner, err1 := t.GetDDLJobOwner()
		if err1 != nil {
			return errors.Trace(err1)
		}
		if owner == nil || owner.OwnerID != d.uuid {
			return nil
		}

		// DDL job's owner is me, clean it so other servers can complete it quickly.
		return t.SetDDLJobOwner(&model.Owner{})
	})
	if err != nil {
		return errors.Trace(err)
	}

	err = kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		owner, err1 := t.GetBgJobOwner()
		if err1 != nil {
			return errors.Trace(err1)
		}
		if owner == nil || owner.OwnerID != d.uuid {
			return nil
		}

		// Background job's owner is me, clean it so other servers can complete it quickly.
		return t.SetBgJobOwner(&model.Owner{})
	})
	log.Infof("stop DDL:%s", d.uuid)

	return errors.Trace(err)
}

func (d *ddl) Start() error {
	d.m.Lock()
	defer d.m.Unlock()

	if !d.isClosed() {
		return nil
	}

	d.start()

	return nil
}

func (d *ddl) start() {
	d.quitCh = make(chan struct{})
	d.wait.Add(2)
	go d.onBackgroundWorker()
	go d.onDDLWorker()
	// For every start, we will send a fake job to let worker
	// check owner firstly and try to find whether a job exists and run.
	asyncNotify(d.ddlJobCh)
	asyncNotify(d.bgJobCh)
}

func (d *ddl) close() {
	if d.isClosed() {
		return
	}

	close(d.quitCh)

	d.wait.Wait()
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

func (d *ddl) SetLease(lease time.Duration) {
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
	d.start()
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
	ticker := time.NewTicker(chooseLeaseTime(10*d.lease, 10*time.Second))
	startTime := time.Now()
	jobsGauge.WithLabelValues(JobType(ddlJobFlag).String(), job.Type.String()).Inc()
	defer func() {
		ticker.Stop()
		jobsGauge.WithLabelValues(JobType(ddlJobFlag).String(), job.Type.String()).Dec()
		retLabel := handleJobSucc
		if err != nil {
			retLabel = handleJobFailed
		}
		handleJobHistogram.WithLabelValues(JobType(ddlJobFlag).String(), job.Type.String(),
			retLabel).Observe(time.Since(startTime).Seconds())
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
			log.Warnf("[ddl] DDL job %d is not in history, maybe not run", jobID)
			continue
		}

		// If a job is a history job, the state must be JobDone or JobCancel.
		if historyJob.State == model.JobDone {
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

func (d *ddl) setHook(h Callback) {
	d.hookMu.Lock()
	defer d.hookMu.Unlock()

	d.hook = h
}

func filterError(err, exceptErr error) error {
	if terror.ErrorEqual(err, exceptErr) {
		return nil
	}
	return errors.Trace(err)
}

// DDL error codes.
const (
	codeInvalidWorker         terror.ErrCode = 1
	codeNotOwner                             = 2
	codeInvalidDDLJob                        = 3
	codeInvalidBgJob                         = 4
	codeInvalidJobFlag                       = 5
	codeRunMultiSchemaChanges                = 6
	codeWaitReorgTimeout                     = 7
	codeInvalidStoreVer                      = 8
	codeUnknownTypeLength                    = 9
	codeUnknownFractionLength                = 10

	codeInvalidDBState         = 100
	codeInvalidTableState      = 101
	codeInvalidColumnState     = 102
	codeInvalidIndexState      = 103
	codeInvalidForeignKeyState = 104

	codeCantDropColWithIndex    = 201
	codeUnsupportedAddColumn    = 202
	codeUnsupportedModifyColumn = 203
	codeUnsupportedDropPKHandle = 204

	codeFileNotFound          = 1017
	codeErrorOnRename         = 1025
	codeBadNull               = 1048
	codeBadField              = 1054
	codeTooLongIdent          = 1059
	codeDupKeyName            = 1061
	codeInvalidDefault        = 1067
	codeTooLongKey            = 1071
	codeKeyColumnDoesNotExits = 1072
	codeIncorrectPrefixKey    = 1089
	codeCantRemoveAllFields   = 1090
	codeCantDropFieldOrKey    = 1091
	codeWrongDBName           = 1102
	codeWrongTableName        = 1103
	codeInvalidUseOfNull      = 1138
	codeBlobKeyWithoutLength  = 1170
	codeInvalidOnUpdate       = 1294
)

func init() {
	ddlMySQLErrCodes := map[terror.ErrCode]uint16{
		codeBadNull:               mysql.ErrBadNull,
		codeCantRemoveAllFields:   mysql.ErrCantRemoveAllFields,
		codeCantDropFieldOrKey:    mysql.ErrCantDropFieldOrKey,
		codeInvalidOnUpdate:       mysql.ErrInvalidOnUpdate,
		codeBlobKeyWithoutLength:  mysql.ErrBlobKeyWithoutLength,
		codeIncorrectPrefixKey:    mysql.ErrWrongSubKey,
		codeTooLongIdent:          mysql.ErrTooLongIdent,
		codeTooLongKey:            mysql.ErrTooLongKey,
		codeKeyColumnDoesNotExits: mysql.ErrKeyColumnDoesNotExits,
		codeDupKeyName:            mysql.ErrDupKeyName,
		codeWrongDBName:           mysql.ErrWrongDBName,
		codeWrongTableName:        mysql.ErrWrongTableName,
		codeFileNotFound:          mysql.ErrFileNotFound,
		codeErrorOnRename:         mysql.ErrErrorOnRename,
		codeBadField:              mysql.ErrBadField,
		codeInvalidDefault:        mysql.ErrInvalidDefault,
		codeInvalidUseOfNull:      mysql.ErrInvalidUseOfNull,
	}
	terror.ErrClassToMySQLCodes[terror.ClassDDL] = ddlMySQLErrCodes
}
