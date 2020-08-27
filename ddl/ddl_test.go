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
	"os"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
)

type DDLForTest interface {
	// SetHook sets the hook.
	SetHook(h Callback)
	// SetInterceptor sets the interceptor.
	SetInterceptor(h Interceptor)
}

// SetHook implements DDL.SetHook interface.
func (d *ddl) SetHook(h Callback) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.mu.hook = h
}

// SetInterceptor implements DDL.SetInterceptor interface.
func (d *ddl) SetInterceptor(i Interceptor) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.mu.interceptor = i
}

// generalWorker returns the general worker.
func (d *ddl) generalWorker() *worker {
	return d.workers[generalWorker]
}

// restartWorkers is like the function of d.start. But it won't initialize the "workers" and create a new worker.
// It only starts the original workers.
func (d *ddl) restartWorkers(ctx context.Context) {
	d.m.Lock()
	d.ctx, d.cancel = context.WithCancel(ctx)
	d.m.Unlock()

	d.wg.Add(1)
	go d.limitDDLJobs()
	if !RunWorker {
		return
	}

	err := d.ownerManager.CampaignOwner()
	terror.Log(err)
	for _, worker := range d.workers {
		worker.wg.Add(1)
		worker.ctx = d.ctx
		w := worker
		go w.start(d.ddlCtx)
		asyncNotify(worker.ddlJobCh)
	}
}

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	*CustomParallelSuiteFlag = true
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(logutil.NewLogConfig(logLevel, "", "", logutil.EmptyFileLogConfig, false))
	autoid.SetStep(5000)
	ReorgWaitTimeout = 30 * time.Millisecond
	batchInsertDeleteRangeSize = 2

	config.UpdateGlobal(func(conf *config.Config) {
		// Test for table lock.
		conf.EnableTableLock = true
		conf.Log.SlowThreshold = 10000
		// Test for add/drop primary key.
		conf.AlterPrimaryKey = true
	})

	testleak.BeforeTest()
	TestingT(t)
	testleak.AfterTestT(t)()
}

func testNewDDLAndStart(ctx context.Context, c *C, options ...Option) *ddl {
	d := newDDL(ctx, options...)
	err := d.Start(nil)
	c.Assert(err, IsNil)

	return d
}

func testCreateStore(c *C, name string) kv.Storage {
	store, err := mockstore.NewMockStore()
	c.Assert(err, IsNil)
	return store
}

func testNewContext(d *ddl) sessionctx.Context {
	ctx := mock.NewContext()
	ctx.Store = d.store
	return ctx
}

func getSchemaVer(c *C, ctx sessionctx.Context) int64 {
	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	m := meta.NewMeta(txn)
	ver, err := m.GetSchemaVersion()
	c.Assert(err, IsNil)
	return ver
}

type historyJobArgs struct {
	ver    int64
	db     *model.DBInfo
	tbl    *model.TableInfo
	tblIDs map[int64]struct{}
}

func checkEqualTable(c *C, t1, t2 *model.TableInfo) {
	c.Assert(t1.ID, Equals, t2.ID)
	c.Assert(t1.Name, Equals, t2.Name)
	c.Assert(t1.Charset, Equals, t2.Charset)
	c.Assert(t1.Collate, Equals, t2.Collate)
	c.Assert(t1.PKIsHandle, DeepEquals, t2.PKIsHandle)
	c.Assert(t1.Comment, DeepEquals, t2.Comment)
	c.Assert(t1.AutoIncID, DeepEquals, t2.AutoIncID)
}

func checkHistoryJob(c *C, job *model.Job) {
	c.Assert(job.State, Equals, model.JobStateSynced)
}

func checkHistoryJobArgs(c *C, ctx sessionctx.Context, id int64, args *historyJobArgs) {
	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	t := meta.NewMeta(txn)
	historyJob, err := t.GetHistoryDDLJob(id)
	c.Assert(err, IsNil)
	c.Assert(historyJob.BinlogInfo.FinishedTS, Greater, uint64(0))

	if args.tbl != nil {
		c.Assert(historyJob.BinlogInfo.SchemaVersion, Equals, args.ver)
		checkEqualTable(c, historyJob.BinlogInfo.TableInfo, args.tbl)
		return
	}

	// for handling schema job
	c.Assert(historyJob.BinlogInfo.SchemaVersion, Equals, args.ver)
	c.Assert(historyJob.BinlogInfo.DBInfo, DeepEquals, args.db)
	// only for creating schema job
	if args.db != nil && len(args.tblIDs) == 0 {
		return
	}
}

func buildCreateIdxJob(dbInfo *model.DBInfo, tblInfo *model.TableInfo, unique bool, indexName string, colName string) *model.Job {
	return &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionAddIndex,
		BinlogInfo: &model.HistoryInfo{},
		Args: []interface{}{unique, model.NewCIStr(indexName),
			[]*ast.IndexPartSpecification{{
				Column: &ast.ColumnName{Name: model.NewCIStr(colName)},
				Length: types.UnspecifiedLength}}},
	}
}

func testCreatePrimaryKey(c *C, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, colName string) *model.Job {
	job := buildCreateIdxJob(dbInfo, tblInfo, true, "primary", colName)
	job.Type = model.ActionAddPrimaryKey
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func testCreateIndex(c *C, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, unique bool, indexName string, colName string) *model.Job {
	job := buildCreateIdxJob(dbInfo, tblInfo, unique, indexName, colName)
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func testAddColumn(c *C, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, args []interface{}) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionAddColumn,
		Args:       args,
		BinlogInfo: &model.HistoryInfo{},
	}
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func testAddColumns(c *C, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, args []interface{}) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionAddColumns,
		Args:       args,
		BinlogInfo: &model.HistoryInfo{},
	}
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func buildDropIdxJob(dbInfo *model.DBInfo, tblInfo *model.TableInfo, indexName string) *model.Job {
	tp := model.ActionDropIndex
	if indexName == "primary" {
		tp = model.ActionDropPrimaryKey
	}
	return &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       tp,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{model.NewCIStr(indexName)},
	}
}

func testDropIndex(c *C, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, indexName string) *model.Job {
	job := buildDropIdxJob(dbInfo, tblInfo, indexName)
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func buildRebaseAutoIDJobJob(dbInfo *model.DBInfo, tblInfo *model.TableInfo, newBaseID int64) *model.Job {
	return &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionRebaseAutoID,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{newBaseID},
	}
}
