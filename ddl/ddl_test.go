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
	"os"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	"go.uber.org/zap"
)

type DDLForTest interface {
	// SetHook sets the hook.
	SetHook(h Callback)
	// SetInterceptoror sets the interceptor.
	SetInterceptoror(h Interceptor)
}

// SetHook implements DDL.SetHook interface.
func (d *ddl) SetHook(h Callback) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.mu.hook = h
}

// SetInterceptoror implements DDL.SetInterceptoror interface.
func (d *ddl) SetInterceptoror(i Interceptor) {
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
	d.quitCh = make(chan struct{})
	go util.WithRecovery(
		func() { d.addBatchDDLJobs() },
		func(r interface{}) {
			logutil.BgLogger().Error("[ddl] DDL add batch DDL jobs meet paninc",
				zap.String("ID", d.uuid), zap.Reflect("r", r), zap.Stack("stack trace"))
			metrics.PanicCounter.WithLabelValues(metrics.LabelDDL).Inc()
		})
	if !RunWorker {
		return
	}

	err := d.ownerManager.CampaignOwner(ctx)
	terror.Log(err)
	for _, worker := range d.workers {
		worker.wg.Add(1)
		worker.quitCh = make(chan struct{})
		w := worker
		go util.WithRecovery(func() { w.start(d.ddlCtx) },
			func(r interface{}) {
				if r != nil {
					log.Error("[ddl] restart DDL worker meet panic", zap.String("worker", w.String()), zap.String("ID", d.uuid))
				}
			})
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

	cfg := config.GetGlobalConfig()
	newCfg := *cfg
	// Test for table lock.
	newCfg.EnableTableLock = true
	newCfg.Log.SlowThreshold = 10000
	// Test for add/drop primary key.
	newCfg.AlterPrimaryKey = true
	config.StoreGlobalConfig(&newCfg)

	testleak.BeforeTest()
	TestingT(t)
	testleak.AfterTestT(t)()
}

func testCreateStore(c *C, name string) kv.Storage {
	store, err := mockstore.NewMockTikvStore()
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

func testCreateTable1(ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tblInfo},
	}
	job.Query, _ = ctx.Value(sessionctx.QueryString).(string)
	task := &limitJobTask{job, make(chan error)}
	d.limitJobCh <- task
	err := <-task.err
	if err != nil {
		log.Fatal(err.Error())
	}
	return job
}

func testTableInfo1(d *ddl, name string, num int) *model.TableInfo {
	tblInfo := &model.TableInfo{
		Name: model.NewCIStr(name),
	}
	genIDs, err := d.genGlobalIDs(1)
	if err != nil {
		log.Fatal(err.Error())
	}
	tblInfo.ID = genIDs[0]

	cols := make([]*model.ColumnInfo, num)
	for i := range cols {
		col := &model.ColumnInfo{
			Name:         model.NewCIStr(fmt.Sprintf("c%d", i+1)),
			Offset:       i,
			DefaultValue: i + 1,
			State:        model.StatePublic,
		}

		col.FieldType = *types.NewFieldType(mysql.TypeLong)
		col.ID = allocateColumnID(tblInfo)
		cols[i] = col
	}
	tblInfo.Columns = cols
	tblInfo.Charset = "utf8"
	tblInfo.Collate = "utf8_bin"
	return tblInfo
}

func BenchmarkAddDDLs(b *testing.B) {
	store, err := mockstore.NewMockTikvStore()
	if err != nil {
		log.Fatal(err.Error())
	}
	defer store.Close()
	d := newDDL(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	ctx := testNewContext(d)
	defer d.Stop()
	// create database add_ddls;
	dbInfo := &model.DBInfo{
		Name: model.NewCIStr("add_ddls"),
	}
	genIDs, err := d.genGlobalIDs(1)
	if err != nil {
		log.Fatal(err.Error())
	}
	dbInfo.ID = genIDs[0]
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{dbInfo},
	}
	err = d.doDDLJob(ctx, job)
	if err != nil {
		log.Fatal(err.Error())
	}
	// create table t1 (c1 int, c2 int);
	tblInfo1 := testTableInfo1(d, "t1", 2)
	testCreateTable1(ctx, d, dbInfo, tblInfo1)

	count := 200
	taskCnt := 1
	tblInfos := make([]*model.TableInfo, 0, count*taskCnt)
	for i := 0; i < count*taskCnt; i++ {
		tblInfo := testTableInfo1(d, fmt.Sprintf("t_%d", i), 2)
		tblInfos = append(tblInfos, tblInfo)
	}
	wg := sync.WaitGroup{}
	b.ResetTimer()
	runDDLsFunc := func(start, end int) {
		defer wg.Done()
		for i := start; i < end; i++ {
			// tblInfo := testTableInfo1(d, fmt.Sprintf("t_%d", i), 2)
			testCreateTable1(ctx, d, dbInfo, tblInfos[i])
			// se.Execute(ctx, fmt.Sprintf("create table t_%d(a int, b int)", i))
			// se.Execute(ctx, fmt.Sprintf("alter table t%d add index(a)", i))
			// se.Execute(ctx, fmt.Sprintf("alter table t%d add column c%d int", i, i))
		}
		log.Error("xxx-------------", zap.Int("count", end))
	}
	wg.Add(taskCnt)
	for i := 0; i < taskCnt; i++ {
		go runDDLsFunc(i*count, (i+1)*count)
	}
	wg.Wait()
	log.Error("xxx-------------", zap.Int("count", count*taskCnt))
	b.StopTimer()
}
