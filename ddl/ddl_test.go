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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"context"
	"os"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

type DDLForTest interface {
	// SetHook sets the hook.
	SetHook(h Callback)
	// SetInterceptor sets the interceptor.
	SetInterceptor(h Interceptor)
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

// GetMaxRowID is used for test.
func GetMaxRowID(store kv.Storage, priority int, t table.Table, startHandle, endHandle kv.Key) (kv.Key, error) {
	return getRangeEndKey(store, priority, t, startHandle, endHandle)
}

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	*CustomParallelSuiteFlag = true
	logLevel := os.Getenv("log_level")
	err := logutil.InitLogger(logutil.NewLogConfig(logLevel, "", "", logutil.EmptyFileLogConfig, false))
	if err != nil {
		t.Fatal(err)
	}
	autoid.SetStep(5000)
	ReorgWaitTimeout = 30 * time.Millisecond
	batchInsertDeleteRangeSize = 2

	config.UpdateGlobal(func(conf *config.Config) {
		// Test for table lock.
		conf.EnableTableLock = true
		conf.Log.SlowThreshold = 10000
		conf.TiKVClient.AsyncCommit.SafeWindow = 0
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
		conf.Experimental.AllowsExpressionIndex = true
	})
	tikv.EnableFailpoints()

	_, err = infosync.GlobalInfoSyncerInit(context.Background(), "t", func() uint64 { return 1 }, nil, true)
	if err != nil {
		t.Fatal(err)
	}

	testleak.BeforeTest()
	TestingT(t)
	testleak.AfterTestT(t)()
}

func testNewDDLAndStart(ctx context.Context, options ...Option) (*ddl, error) {
	// init infoCache and a stub infoSchema
	ic := infoschema.NewCache(2)
	ic.Insert(infoschema.MockInfoSchemaWithSchemaVer(nil, 0), 0)
	options = append(options, WithInfoCache(ic))
	d := newDDL(ctx, options...)
	err := d.Start(nil)
	return d, err
}

func testCreateStore(t *testing.T, name string) kv.Storage {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	return store
}

func testCreateStoreT(t *testing.T, name string) kv.Storage {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	return store
}

func testNewContext(d *ddl) sessionctx.Context {
	ctx := mock.NewContext()
	ctx.Store = d.store
	return ctx
}

func getSchemaVer(t *testing.T, ctx sessionctx.Context) int64 {
	err := ctx.NewTxn(context.Background())
	require.NoError(t, err)
	txn, err := ctx.Txn(true)
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	ver, err := m.GetSchemaVersion()
	require.NoError(t, err)
	return ver
}

func getSchemaVerT(t *testing.T, ctx sessionctx.Context) int64 {
	err := ctx.NewTxn(context.Background())
	require.NoError(t, err)
	txn, err := ctx.Txn(true)
	require.NoError(t, err)
	m := meta.NewMeta(txn)
	ver, err := m.GetSchemaVersion()
	require.NoError(t, err)
	return ver
}

type historyJobArgs struct {
	ver    int64
	db     *model.DBInfo
	tbl    *model.TableInfo
	tblIDs map[int64]struct{}
}

func checkEqualTable(t *testing.T, t1, t2 *model.TableInfo) {
	require.Equal(t, t1.ID, t2.ID)
	require.Equal(t, t1.Name, t2.Name)
	require.Equal(t, t1.Charset, t2.Charset)
	require.Equal(t, t1.Collate, t2.Collate)
	require.Equal(t, t1.PKIsHandle, t2.PKIsHandle)
	require.Equal(t, t1.Comment, t2.Comment)
	require.Equal(t, t1.AutoIncID, t2.AutoIncID)
}

func checkEqualTableT(t *testing.T, t1, t2 *model.TableInfo) {
	require.Equal(t, t1.ID, t2.ID)
	require.Equal(t, t1.Name, t2.Name)
	require.Equal(t, t1.Charset, t2.Charset)
	require.Equal(t, t1.Collate, t2.Collate)
	require.EqualValues(t, t1.PKIsHandle, t2.PKIsHandle)
	require.EqualValues(t, t1.Comment, t2.Comment)
	require.EqualValues(t, t1.AutoIncID, t2.AutoIncID)
}

func checkHistoryJob(t *testing.T, job *model.Job) {
	require.Equal(t, job.State, model.JobStateSynced)
}

func checkHistoryJobArgs(t *testing.T, ctx sessionctx.Context, id int64, args *historyJobArgs) {
	txn, err := ctx.Txn(true)
	require.NoError(t, err)
	tran := meta.NewMeta(txn)
	historyJob, err := tran.GetHistoryDDLJob(id)
	require.NoError(t, err)
	require.Greater(t, historyJob.BinlogInfo.FinishedTS, uint64(0))

	if args.tbl != nil {
		require.Equal(t, historyJob.BinlogInfo.SchemaVersion, args.ver)
		checkEqualTable(t, historyJob.BinlogInfo.TableInfo, args.tbl)
		return
	}

	// for handling schema job
	require.Equal(t, historyJob.BinlogInfo.SchemaVersion, args.ver)
	require.Equal(t, historyJob.BinlogInfo.DBInfo, args.db)
	// only for creating schema job
	if args.db != nil && len(args.tblIDs) == 0 {
		return
	}
}

func checkHistoryJobArgsT(t *testing.T, ctx sessionctx.Context, id int64, args *historyJobArgs) {
	txn, err := ctx.Txn(true)
	require.NoError(t, err)
	tt := meta.NewMeta(txn)
	historyJob, err := tt.GetHistoryDDLJob(id)
	require.NoError(t, err)
	require.Greater(t, historyJob.BinlogInfo.FinishedTS, uint64(0))

	if args.tbl != nil {
		require.Equal(t, args.ver, historyJob.BinlogInfo.SchemaVersion)
		checkEqualTableT(t, historyJob.BinlogInfo.TableInfo, args.tbl)
		return
	}

	// for handling schema job
	require.Equal(t, args.ver, historyJob.BinlogInfo.SchemaVersion)
	require.EqualValues(t, args.db, historyJob.BinlogInfo.DBInfo)
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

func testCreatePrimaryKey(t *testing.T, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, colName string) *model.Job {
	job := buildCreateIdxJob(dbInfo, tblInfo, true, "primary", colName)
	job.Type = model.ActionAddPrimaryKey
	err := d.doDDLJob(ctx, job)
	require.NoError(t, err)
	v := getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func testCreateIndex(t *testing.T, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, unique bool, indexName string, colName string) *model.Job {
	job := buildCreateIdxJob(dbInfo, tblInfo, unique, indexName, colName)
	err := d.doDDLJob(ctx, job)
	require.NoError(t, err)
	v := getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func testAddColumn(t *testing.T, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, args []interface{}) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionAddColumn,
		Args:       args,
		BinlogInfo: &model.HistoryInfo{},
	}
	err := d.doDDLJob(ctx, job)
	require.NoError(t, err)
	v := getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func testAddColumns(t *testing.T, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, args []interface{}) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionAddColumns,
		Args:       args,
		BinlogInfo: &model.HistoryInfo{},
	}
	err := d.doDDLJob(ctx, job)
	require.NoError(t, err)
	v := getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
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

func testDropIndex(t *testing.T, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, indexName string) *model.Job {
	job := buildDropIdxJob(dbInfo, tblInfo, indexName)
	err := d.doDDLJob(ctx, job)
	require.NoError(t, err)
	v := getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
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

func (s *testDDLSuiteToVerify) TestGetIntervalFromPolicy() {
	policy := []time.Duration{
		1 * time.Second,
		2 * time.Second,
	}
	var (
		val     time.Duration
		changed bool
	)

	val, changed = getIntervalFromPolicy(policy, 0)
	require.Equal(s.T(), val, 1*time.Second)
	require.Equal(s.T(), changed, true)

	val, changed = getIntervalFromPolicy(policy, 1)
	require.Equal(s.T(), val, 2*time.Second)
	require.Equal(s.T(), changed, true)

	val, changed = getIntervalFromPolicy(policy, 2)
	require.Equal(s.T(), val, 2*time.Second)
	require.Equal(s.T(), changed, false)

	val, changed = getIntervalFromPolicy(policy, 3)
	require.Equal(s.T(), val, 2*time.Second)
	require.Equal(s.T(), changed, false)
}
