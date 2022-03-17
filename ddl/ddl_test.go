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
	"testing"
	"time"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

type DDLForTest interface {
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

func testNewDDLAndStart(ctx context.Context, options ...Option) (*ddl, error) {
	// init infoCache and a stub infoSchema
	ic := infoschema.NewCache(2)
	ic.Insert(infoschema.MockInfoSchemaWithSchemaVer(nil, 0), 0)
	options = append(options, WithInfoCache(ic))
	d := newDDL(ctx, options...)
	err := d.Start(nil)
	return d, err
}

func createMockStore(t *testing.T) kv.Storage {
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
	ctx.SetValue(sessionctx.QueryString, "skip")
	err := d.doDDLJob(ctx, job)
	require.NoError(t, err)
	v := getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func testCreateIndex(t *testing.T, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, unique bool, indexName string, colName string) *model.Job {
	job := buildCreateIdxJob(dbInfo, tblInfo, unique, indexName, colName)
	ctx.SetValue(sessionctx.QueryString, "skip")
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
	ctx.SetValue(sessionctx.QueryString, "skip")
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
	ctx.SetValue(sessionctx.QueryString, "skip")
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
	ctx.SetValue(sessionctx.QueryString, "skip")
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

func TestGetIntervalFromPolicy(t *testing.T) {
	policy := []time.Duration{
		1 * time.Second,
		2 * time.Second,
	}
	var (
		val     time.Duration
		changed bool
	)

	val, changed = getIntervalFromPolicy(policy, 0)
	require.Equal(t, val, 1*time.Second)
	require.True(t, changed)

	val, changed = getIntervalFromPolicy(policy, 1)
	require.Equal(t, val, 2*time.Second)
	require.True(t, changed)

	val, changed = getIntervalFromPolicy(policy, 2)
	require.Equal(t, val, 2*time.Second)
	require.False(t, changed)

	val, changed = getIntervalFromPolicy(policy, 3)
	require.Equal(t, val, 2*time.Second)
	require.False(t, changed)
}
