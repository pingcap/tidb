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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/testkit/testutil"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

const testLease = 5 * time.Millisecond

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
	return getRangeEndKey(NewJobContext(), store, priority, t, startHandle, endHandle)
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
	err := sessiontxn.NewTxn(context.Background(), ctx)
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

func colDefStrToFieldType(t *testing.T, str string, ctx sessionctx.Context) *types.FieldType {
	sqlA := "alter table t modify column a " + str
	stmt, err := parser.New().ParseOneStmt(sqlA, "", "")
	require.NoError(t, err)
	colDef := stmt.(*ast.AlterTableStmt).Specs[0].NewColumns[0]
	chs, coll := charset.GetDefaultCharsetAndCollate()
	col, _, err := buildColumnAndConstraint(ctx, 0, colDef, nil, chs, coll)
	require.NoError(t, err)
	return &col.FieldType
}

func TestModifyColumn(t *testing.T) {
	ctx := mock.NewContext()
	tests := []struct {
		origin string
		to     string
		err    error
	}{
		{"int", "bigint", nil},
		{"int", "int unsigned", nil},
		{"varchar(10)", "text", nil},
		{"varbinary(10)", "blob", nil},
		{"text", "blob", dbterror.ErrUnsupportedModifyCharset.GenWithStackByArgs("charset from utf8mb4 to binary")},
		{"varchar(10)", "varchar(8)", nil},
		{"varchar(10)", "varchar(11)", nil},
		{"varchar(10) character set utf8 collate utf8_bin", "varchar(10) character set utf8", nil},
		{"decimal(2,1)", "decimal(3,2)", nil},
		{"decimal(2,1)", "decimal(2,2)", nil},
		{"decimal(2,1)", "decimal(2,1)", nil},
		{"decimal(2,1)", "int", nil},
		{"decimal", "int", nil},
		{"decimal(2,1)", "bigint", nil},
		{"int", "varchar(10) character set gbk", dbterror.ErrUnsupportedModifyCharset.GenWithStackByArgs("charset from binary to gbk")},
		{"varchar(10) character set gbk", "int", dbterror.ErrUnsupportedModifyCharset.GenWithStackByArgs("charset from gbk to binary")},
		{"varchar(10) character set gbk", "varchar(10) character set utf8", dbterror.ErrUnsupportedModifyCharset.GenWithStackByArgs("charset from gbk to utf8")},
		{"varchar(10) character set gbk", "char(10) character set utf8", dbterror.ErrUnsupportedModifyCharset.GenWithStackByArgs("charset from gbk to utf8")},
		{"varchar(10) character set utf8", "char(10) character set gbk", dbterror.ErrUnsupportedModifyCharset.GenWithStackByArgs("charset from utf8 to gbk")},
		{"varchar(10) character set utf8", "varchar(10) character set gbk", dbterror.ErrUnsupportedModifyCharset.GenWithStackByArgs("charset from utf8 to gbk")},
		{"varchar(10) character set gbk", "varchar(255) character set gbk", nil},
	}
	for _, tt := range tests {
		ftA := colDefStrToFieldType(t, tt.origin, ctx)
		ftB := colDefStrToFieldType(t, tt.to, ctx)
		err := checkModifyTypes(ctx, ftA, ftB, false)
		if err == nil {
			require.NoErrorf(t, tt.err, "origin:%v, to:%v", tt.origin, tt.to)
		} else {
			require.EqualError(t, err, tt.err.Error())
		}
	}
}

func TestFieldCase(t *testing.T) {
	var fields = []string{"field", "Field"}
	colObjects := make([]*model.ColumnInfo, len(fields))
	for i, name := range fields {
		colObjects[i] = &model.ColumnInfo{
			Name: model.NewCIStr(name),
		}
	}
	err := checkDuplicateColumn(colObjects)
	require.EqualError(t, err, infoschema.ErrColumnExists.GenWithStackByArgs("Field").Error())
}

func TestIgnorableSpec(t *testing.T) {
	specs := []ast.AlterTableType{
		ast.AlterTableOption,
		ast.AlterTableAddColumns,
		ast.AlterTableAddConstraint,
		ast.AlterTableDropColumn,
		ast.AlterTableDropPrimaryKey,
		ast.AlterTableDropIndex,
		ast.AlterTableDropForeignKey,
		ast.AlterTableModifyColumn,
		ast.AlterTableChangeColumn,
		ast.AlterTableRenameTable,
		ast.AlterTableAlterColumn,
	}
	for _, spec := range specs {
		require.False(t, isIgnorableSpec(spec))
	}

	ignorableSpecs := []ast.AlterTableType{
		ast.AlterTableLock,
		ast.AlterTableAlgorithm,
	}
	for _, spec := range ignorableSpecs {
		require.True(t, isIgnorableSpec(spec))
	}
}

func TestBuildJobDependence(t *testing.T) {
	store := createMockStore(t)
	defer func() {
		require.NoError(t, store.Close())
	}()
	// Add some non-add-index jobs.
	job1 := &model.Job{ID: 1, TableID: 1, Type: model.ActionAddColumn}
	job2 := &model.Job{ID: 2, TableID: 1, Type: model.ActionCreateTable}
	job3 := &model.Job{ID: 3, TableID: 2, Type: model.ActionDropColumn}
	job6 := &model.Job{ID: 6, TableID: 1, Type: model.ActionDropTable}
	job7 := &model.Job{ID: 7, TableID: 2, Type: model.ActionModifyColumn}
	job9 := &model.Job{ID: 9, SchemaID: 111, Type: model.ActionDropSchema}
	job11 := &model.Job{ID: 11, TableID: 2, Type: model.ActionRenameTable, Args: []interface{}{int64(111), "old db name"}}
	err := kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		require.NoError(t, m.EnQueueDDLJob(job1))
		require.NoError(t, m.EnQueueDDLJob(job2))
		require.NoError(t, m.EnQueueDDLJob(job3))
		require.NoError(t, m.EnQueueDDLJob(job6))
		require.NoError(t, m.EnQueueDDLJob(job7))
		require.NoError(t, m.EnQueueDDLJob(job9))
		require.NoError(t, m.EnQueueDDLJob(job11))
		return nil
	})
	require.NoError(t, err)
	job4 := &model.Job{ID: 4, TableID: 1, Type: model.ActionAddIndex}
	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err := buildJobDependence(m, job4)
		require.NoError(t, err)
		require.Equal(t, job4.DependencyID, int64(2))
		return nil
	})
	require.NoError(t, err)
	job5 := &model.Job{ID: 5, TableID: 2, Type: model.ActionAddIndex}
	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err := buildJobDependence(m, job5)
		require.NoError(t, err)
		require.Equal(t, job5.DependencyID, int64(3))
		return nil
	})
	require.NoError(t, err)
	job8 := &model.Job{ID: 8, TableID: 3, Type: model.ActionAddIndex}
	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err := buildJobDependence(m, job8)
		require.NoError(t, err)
		require.Equal(t, job8.DependencyID, int64(0))
		return nil
	})
	require.NoError(t, err)
	job10 := &model.Job{ID: 10, SchemaID: 111, TableID: 3, Type: model.ActionAddIndex}
	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err := buildJobDependence(m, job10)
		require.NoError(t, err)
		require.Equal(t, job10.DependencyID, int64(9))
		return nil
	})
	require.NoError(t, err)
	job12 := &model.Job{ID: 12, SchemaID: 112, TableID: 2, Type: model.ActionAddIndex}
	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err := buildJobDependence(m, job12)
		require.NoError(t, err)
		require.Equal(t, job12.DependencyID, int64(11))
		return nil
	})
	require.NoError(t, err)
}

func TestNotifyDDLJob(t *testing.T) {
	store := createMockStore(t)
	defer func() {
		require.NoError(t, store.Close())
	}()

	getFirstNotificationAfterStartDDL := func(d *ddl) {
		select {
		case <-d.workers[addIdxWorker].ddlJobCh:
		default:
			// The notification may be received by the worker.
		}
		select {
		case <-d.workers[generalWorker].ddlJobCh:
		default:
			// The notification may be received by the worker.
		}
	}

	d, err := testNewDDLAndStart(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, d.Stop())
	}()
	getFirstNotificationAfterStartDDL(d)
	// Ensure that the notification is not handled in workers `start` function.
	d.cancel()
	for _, worker := range d.workers {
		worker.close()
	}

	job := &model.Job{
		SchemaID:   1,
		TableID:    2,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{},
	}
	// Test the notification mechanism of the owner and the server receiving the DDL request on the same TiDB.
	// This DDL request is a general DDL job.
	d.asyncNotifyWorker(job)
	select {
	case <-d.workers[generalWorker].ddlJobCh:
	default:
		require.FailNow(t, "do not get the general job notification")
	}
	// Test the notification mechanism of the owner and the server receiving the DDL request on the same TiDB.
	// This DDL request is a add index DDL job.
	job.Type = model.ActionAddIndex
	d.asyncNotifyWorker(job)
	select {
	case <-d.workers[addIdxWorker].ddlJobCh:
	default:
		require.FailNow(t, "do not get the add index job notification")
	}

	// Test the notification mechanism that the owner and the server receiving the DDL request are not on the same TiDB.
	// And the etcd client is nil.
	d1, err := testNewDDLAndStart(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, d1.Stop())
	}()
	getFirstNotificationAfterStartDDL(d1)
	// Ensure that the notification is not handled by worker's "start".
	d1.cancel()
	for _, worker := range d1.workers {
		worker.close()
	}
	d1.ownerManager.RetireOwner()
	d1.asyncNotifyWorker(job)
	job.Type = model.ActionCreateTable
	d1.asyncNotifyWorker(job)
	require.False(t, d1.OwnerManager().IsOwner())
	select {
	case <-d1.workers[addIdxWorker].ddlJobCh:
		require.FailNow(t, "should not get the add index job notification")
	case <-d1.workers[generalWorker].ddlJobCh:
		require.FailNow(t, "should not get the general job notification")
	default:
	}
}

func testSchemaInfo(d *ddl, name string) (*model.DBInfo, error) {
	dbInfo := &model.DBInfo{
		Name: model.NewCIStr(name),
	}
	genIDs, err := d.genGlobalIDs(1)
	if err != nil {
		return nil, err
	}
	dbInfo.ID = genIDs[0]
	return dbInfo, nil
}

func testCreateSchema(t *testing.T, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{dbInfo},
	}
	ctx.SetValue(sessionctx.QueryString, "skip")
	require.NoError(t, d.DoDDLJob(ctx, job))

	v := getSchemaVer(t, ctx)
	dbInfo.State = model.StatePublic
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, db: dbInfo})
	dbInfo.State = model.StateNone
	return job
}

func buildDropSchemaJob(dbInfo *model.DBInfo) *model.Job {
	return &model.Job{
		SchemaID:   dbInfo.ID,
		Type:       model.ActionDropSchema,
		BinlogInfo: &model.HistoryInfo{},
	}
}

func testDropSchema(t *testing.T, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo) (*model.Job, int64) {
	job := buildDropSchemaJob(dbInfo)
	ctx.SetValue(sessionctx.QueryString, "skip")
	err := d.DoDDLJob(ctx, job)
	require.NoError(t, err)
	ver := getSchemaVer(t, ctx)
	return job, ver
}

func isDDLJobDone(test *testing.T, t *meta.Meta) bool {
	job, err := t.GetDDLJobByIdx(0)
	require.NoError(test, err)
	if job == nil {
		return true
	}

	time.Sleep(testLease)
	return false
}

func testCheckSchemaState(test *testing.T, d *ddl, dbInfo *model.DBInfo, state model.SchemaState) {
	isDropped := true

	for {
		err := kv.RunInNewTxn(context.Background(), d.store, false, func(ctx context.Context, txn kv.Transaction) error {
			t := meta.NewMeta(txn)
			info, err := t.GetDatabase(dbInfo.ID)
			require.NoError(test, err)

			if state == model.StateNone {
				isDropped = isDDLJobDone(test, t)
				if !isDropped {
					return nil
				}
				require.Nil(test, info)
				return nil
			}

			require.Equal(test, info.Name, dbInfo.Name)
			require.Equal(test, info.State, state)
			return nil
		})
		require.NoError(test, err)

		if isDropped {
			break
		}
	}
}

type testCtxKeyType int

func (k testCtxKeyType) String() string {
	return "test_ctx_key"
}

const testCtxKey testCtxKeyType = 0

func TestReorg(t *testing.T) {
	tests := []struct {
		isCommonHandle bool
		handle         kv.Handle
		startKey       kv.Handle
		endKey         kv.Handle
	}{
		{
			false,
			kv.IntHandle(100),
			kv.IntHandle(1),
			kv.IntHandle(0),
		},
		{
			true,
			testutil.MustNewCommonHandle(t, "a", 100, "string"),
			testutil.MustNewCommonHandle(t, 100, "string"),
			testutil.MustNewCommonHandle(t, 101, "string"),
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("isCommandHandle(%v)", test.isCommonHandle), func(t *testing.T) {
			store := createMockStore(t)
			defer func() {
				require.NoError(t, store.Close())
			}()

			d, err := testNewDDLAndStart(
				context.Background(),
				WithStore(store),
				WithLease(testLease),
			)
			require.NoError(t, err)
			defer func() {
				err := d.Stop()
				require.NoError(t, err)
			}()

			time.Sleep(testLease)

			ctx := testNewContext(d)

			ctx.SetValue(testCtxKey, 1)
			require.Equal(t, ctx.Value(testCtxKey), 1)
			ctx.ClearValue(testCtxKey)

			err = sessiontxn.NewTxn(context.Background(), ctx)
			require.NoError(t, err)
			txn, err := ctx.Txn(true)
			require.NoError(t, err)
			err = txn.Set([]byte("a"), []byte("b"))
			require.NoError(t, err)
			err = txn.Rollback()
			require.NoError(t, err)

			err = sessiontxn.NewTxn(context.Background(), ctx)
			require.NoError(t, err)
			txn, err = ctx.Txn(true)
			require.NoError(t, err)
			err = txn.Set([]byte("a"), []byte("b"))
			require.NoError(t, err)
			err = txn.Commit(context.Background())
			require.NoError(t, err)

			rowCount := int64(10)
			handle := test.handle
			f := func() error {
				d.generalWorker().reorgCtx.setRowCount(rowCount)
				d.generalWorker().reorgCtx.setNextKey(handle.Encoded())
				time.Sleep(1*ReorgWaitTimeout + 100*time.Millisecond)
				return nil
			}
			job := &model.Job{
				ID:          1,
				SnapshotVer: 1, // Make sure it is not zero. So the reorgInfo's first is false.
			}
			err = sessiontxn.NewTxn(context.Background(), ctx)
			require.NoError(t, err)
			txn, err = ctx.Txn(true)
			require.NoError(t, err)
			m := meta.NewMeta(txn)
			e := &meta.Element{ID: 333, TypeKey: meta.IndexElementKey}
			rInfo := &reorgInfo{
				Job:         job,
				currElement: e,
			}
			mockTbl := tables.MockTableFromMeta(&model.TableInfo{IsCommonHandle: test.isCommonHandle, CommonHandleVersion: 1})
			err = d.generalWorker().runReorgJob(m, rInfo, mockTbl.Meta(), d.lease, f)
			require.Error(t, err)

			// The longest to wait for 5 seconds to make sure the function of f is returned.
			for i := 0; i < 1000; i++ {
				time.Sleep(5 * time.Millisecond)
				err = d.generalWorker().runReorgJob(m, rInfo, mockTbl.Meta(), d.lease, f)
				if err == nil {
					require.Equal(t, job.RowCount, rowCount)
					require.Equal(t, d.generalWorker().reorgCtx.rowCount, int64(0))

					// Test whether reorgInfo's Handle is update.
					err = txn.Commit(context.Background())
					require.NoError(t, err)
					err = sessiontxn.NewTxn(context.Background(), ctx)
					require.NoError(t, err)

					m = meta.NewMeta(txn)
					info, err1 := getReorgInfo(NewJobContext(), d.ddlCtx, m, job, mockTbl, nil)
					require.NoError(t, err1)
					require.Equal(t, info.StartKey, kv.Key(handle.Encoded()))
					require.Equal(t, info.currElement, e)
					_, doneHandle, _ := d.generalWorker().reorgCtx.getRowCountAndKey()
					require.Nil(t, doneHandle)
					break
				}
			}
			require.NoError(t, err)

			job = &model.Job{
				ID:          2,
				SchemaID:    1,
				Type:        model.ActionCreateSchema,
				Args:        []interface{}{model.NewCIStr("test")},
				SnapshotVer: 1, // Make sure it is not zero. So the reorgInfo's first is false.
			}

			element := &meta.Element{ID: 123, TypeKey: meta.ColumnElementKey}
			info := &reorgInfo{
				Job:             job,
				d:               d.ddlCtx,
				currElement:     element,
				StartKey:        test.startKey.Encoded(),
				EndKey:          test.endKey.Encoded(),
				PhysicalTableID: 456,
			}
			err = kv.RunInNewTxn(context.Background(), d.store, false, func(ctx context.Context, txn kv.Transaction) error {
				m := meta.NewMeta(txn)
				var err1 error
				_, err1 = getReorgInfo(NewJobContext(), d.ddlCtx, m, job, mockTbl, []*meta.Element{element})
				require.True(t, meta.ErrDDLReorgElementNotExist.Equal(err1))
				require.Equal(t, job.SnapshotVer, uint64(0))
				return nil
			})
			require.NoError(t, err)
			job.SnapshotVer = uint64(1)
			err = info.UpdateReorgMeta(info.StartKey)
			require.NoError(t, err)
			err = kv.RunInNewTxn(context.Background(), d.store, false, func(ctx context.Context, txn kv.Transaction) error {
				m := meta.NewMeta(txn)
				info1, err1 := getReorgInfo(NewJobContext(), d.ddlCtx, m, job, mockTbl, []*meta.Element{element})
				require.NoError(t, err1)
				require.Equal(t, info1.currElement, info.currElement)
				require.Equal(t, info1.StartKey, info.StartKey)
				require.Equal(t, info1.EndKey, info.EndKey)
				require.Equal(t, info1.PhysicalTableID, info.PhysicalTableID)
				return nil
			})
			require.NoError(t, err)

			err = d.Stop()
			require.NoError(t, err)
			err = d.generalWorker().runReorgJob(m, rInfo, mockTbl.Meta(), d.lease, func() error {
				time.Sleep(4 * testLease)
				return nil
			})
			require.Error(t, err)
			txn, err = ctx.Txn(true)
			require.NoError(t, err)
			err = txn.Commit(context.Background())
			require.NoError(t, err)
		})
	}
}

func TestGetDDLInfo(t *testing.T) {
	store, clean := newMockStore(t)
	defer clean()

	txn, err := store.Begin()
	require.NoError(t, err)
	m := meta.NewMeta(txn)

	dbInfo2 := &model.DBInfo{
		ID:    2,
		Name:  model.NewCIStr("b"),
		State: model.StateNone,
	}
	job := &model.Job{
		SchemaID: dbInfo2.ID,
		Type:     model.ActionCreateSchema,
		RowCount: 0,
	}
	job1 := &model.Job{
		SchemaID: dbInfo2.ID,
		Type:     model.ActionAddIndex,
		RowCount: 0,
	}

	err = m.EnQueueDDLJob(job)
	require.NoError(t, err)

	info, err := GetDDLInfo(txn)
	require.NoError(t, err)
	require.Len(t, info.Jobs, 1)
	require.Equal(t, job, info.Jobs[0])
	require.Nil(t, info.ReorgHandle)

	// two jobs
	m = meta.NewMeta(txn, meta.AddIndexJobListKey)
	err = m.EnQueueDDLJob(job1)
	require.NoError(t, err)

	info, err = GetDDLInfo(txn)
	require.NoError(t, err)
	require.Len(t, info.Jobs, 2)
	require.Equal(t, job, info.Jobs[0])
	require.Equal(t, job1, info.Jobs[1])
	require.Nil(t, info.ReorgHandle)

	err = txn.Rollback()
	require.NoError(t, err)
}

func TestGetDDLJobs(t *testing.T) {
	store, clean := newMockStore(t)
	defer clean()

	txn, err := store.Begin()
	require.NoError(t, err)

	m := meta.NewMeta(txn)
	cnt := 10
	jobs := make([]*model.Job, cnt)
	var currJobs2 []*model.Job
	for i := 0; i < cnt; i++ {
		jobs[i] = &model.Job{
			ID:       int64(i),
			SchemaID: 1,
			Type:     model.ActionCreateTable,
		}
		err = m.EnQueueDDLJob(jobs[i])
		require.NoError(t, err)

		currJobs, err := GetDDLJobs(txn)
		require.NoError(t, err)
		require.Len(t, currJobs, i+1)

		currJobs2 = currJobs2[:0]
		err = IterAllDDLJobs(txn, func(jobs []*model.Job) (b bool, e error) {
			for _, job := range jobs {
				if job.NotStarted() {
					currJobs2 = append(currJobs2, job)
				} else {
					return true, nil
				}
			}
			return false, nil
		})
		require.NoError(t, err)
		require.Len(t, currJobs2, i+1)
	}

	currJobs, err := GetDDLJobs(txn)
	require.NoError(t, err)

	for i, job := range jobs {
		require.Equal(t, currJobs[i].ID, job.ID)
		require.Equal(t, int64(1), job.SchemaID)
		require.Equal(t, model.ActionCreateTable, job.Type)
	}
	require.Equal(t, currJobs2, currJobs)

	err = txn.Rollback()
	require.NoError(t, err)
}

func TestGetDDLJobsIsSort(t *testing.T) {
	store, clean := newMockStore(t)
	defer clean()

	txn, err := store.Begin()
	require.NoError(t, err)

	// insert 5 drop table jobs to DefaultJobListKey queue
	m := meta.NewMeta(txn)
	enQueueDDLJobs(t, m, model.ActionDropTable, 10, 15)

	// insert 5 create table jobs to DefaultJobListKey queue
	enQueueDDLJobs(t, m, model.ActionCreateTable, 0, 5)

	// insert add index jobs to AddIndexJobListKey queue
	m = meta.NewMeta(txn, meta.AddIndexJobListKey)
	enQueueDDLJobs(t, m, model.ActionAddIndex, 5, 10)

	currJobs, err := GetDDLJobs(txn)
	require.NoError(t, err)
	require.Len(t, currJobs, 15)

	isSort := isJobsSorted(currJobs)
	require.True(t, isSort)

	err = txn.Rollback()
	require.NoError(t, err)
}

func TestCancelJobs(t *testing.T) {
	store, clean := newMockStore(t)
	defer clean()

	txn, err := store.Begin()
	require.NoError(t, err)

	m := meta.NewMeta(txn)
	cnt := 10
	ids := make([]int64, cnt)
	for i := 0; i < cnt; i++ {
		job := &model.Job{
			ID:       int64(i),
			SchemaID: 1,
			Type:     model.ActionCreateTable,
		}
		if i == 0 {
			job.State = model.JobStateDone
		}
		if i == 1 {
			job.State = model.JobStateCancelled
		}
		ids[i] = int64(i)
		err = m.EnQueueDDLJob(job)
		require.NoError(t, err)
	}

	errs, err := CancelJobs(txn, ids)
	require.NoError(t, err)
	for i, err := range errs {
		if i == 0 {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
	}

	errs, err = CancelJobs(txn, []int64{})
	require.NoError(t, err)
	require.Nil(t, errs)

	errs, err = CancelJobs(txn, []int64{-1})
	require.NoError(t, err)
	require.Error(t, errs[0])
	require.Regexp(t, "DDL Job:-1 not found$", errs[0].Error())

	// test cancel finish job.
	job := &model.Job{
		ID:       100,
		SchemaID: 1,
		Type:     model.ActionCreateTable,
		State:    model.JobStateDone,
	}
	err = m.EnQueueDDLJob(job)
	require.NoError(t, err)
	errs, err = CancelJobs(txn, []int64{100})
	require.NoError(t, err)
	require.Error(t, errs[0])
	require.Regexp(t, "This job:100 is finished, so can't be cancelled$", errs[0].Error())

	// test can't cancelable job.
	job.Type = model.ActionDropIndex
	job.SchemaState = model.StateWriteOnly
	job.State = model.JobStateRunning
	job.ID = 101
	err = m.EnQueueDDLJob(job)
	require.NoError(t, err)
	errs, err = CancelJobs(txn, []int64{101})
	require.NoError(t, err)
	require.Error(t, errs[0])
	require.Regexp(t, "This job:101 is almost finished, can't be cancelled now$", errs[0].Error())

	// When both types of jobs exist in the DDL queue,
	// we first cancel the job with a larger ID.
	job = &model.Job{
		ID:       1000,
		SchemaID: 1,
		TableID:  2,
		Type:     model.ActionAddIndex,
	}
	job1 := &model.Job{
		ID:       1001,
		SchemaID: 1,
		TableID:  2,
		Type:     model.ActionAddColumn,
	}
	job2 := &model.Job{
		ID:       1002,
		SchemaID: 1,
		TableID:  2,
		Type:     model.ActionAddIndex,
	}
	job3 := &model.Job{
		ID:       1003,
		SchemaID: 1,
		TableID:  2,
		Type:     model.ActionRepairTable,
	}
	require.NoError(t, m.EnQueueDDLJob(job, meta.AddIndexJobListKey))
	require.NoError(t, m.EnQueueDDLJob(job1))
	require.NoError(t, m.EnQueueDDLJob(job2, meta.AddIndexJobListKey))
	require.NoError(t, m.EnQueueDDLJob(job3))

	errs, err = CancelJobs(txn, []int64{job1.ID, job.ID, job2.ID, job3.ID})
	require.NoError(t, err)
	for _, err := range errs {
		require.NoError(t, err)
	}

	err = txn.Rollback()
	require.NoError(t, err)
}

func TestGetHistoryDDLJobs(t *testing.T) {
	store, clean := newMockStore(t)
	defer clean()

	txn, err := store.Begin()
	require.NoError(t, err)

	m := meta.NewMeta(txn)
	cnt := 11
	jobs := make([]*model.Job, cnt)
	for i := 0; i < cnt; i++ {
		jobs[i] = &model.Job{
			ID:       int64(i),
			SchemaID: 1,
			Type:     model.ActionCreateTable,
		}
		err = m.AddHistoryDDLJob(jobs[i], true)
		require.NoError(t, err)

		historyJobs, err := GetHistoryDDLJobs(txn, DefNumHistoryJobs)
		require.NoError(t, err)

		if i+1 > MaxHistoryJobs {
			require.Len(t, historyJobs, MaxHistoryJobs)
		} else {
			require.Len(t, historyJobs, i+1)
		}
	}

	delta := cnt - MaxHistoryJobs
	historyJobs, err := GetHistoryDDLJobs(txn, DefNumHistoryJobs)
	require.NoError(t, err)
	require.Len(t, historyJobs, MaxHistoryJobs)

	l := len(historyJobs) - 1
	for i, job := range historyJobs {
		require.Equal(t, jobs[delta+l-i].ID, job.ID)
		require.Equal(t, int64(1), job.SchemaID)
		require.Equal(t, model.ActionCreateTable, job.Type)
	}

	var historyJobs2 []*model.Job
	err = IterHistoryDDLJobs(txn, func(jobs []*model.Job) (b bool, e error) {
		for _, job := range jobs {
			historyJobs2 = append(historyJobs2, job)
			if len(historyJobs2) == DefNumHistoryJobs {
				return true, nil
			}
		}
		return false, nil
	})
	require.NoError(t, err)
	require.Equal(t, historyJobs, historyJobs2)

	err = txn.Rollback()
	require.NoError(t, err)
}

func TestIsJobRollbackable(t *testing.T) {
	cases := []struct {
		tp     model.ActionType
		state  model.SchemaState
		result bool
	}{
		{model.ActionDropIndex, model.StateNone, true},
		{model.ActionDropIndex, model.StateDeleteOnly, false},
		{model.ActionDropSchema, model.StateDeleteOnly, false},
		{model.ActionDropColumn, model.StateDeleteOnly, false},
		{model.ActionDropColumns, model.StateDeleteOnly, false},
		{model.ActionDropIndexes, model.StateDeleteOnly, false},
	}
	job := &model.Job{}
	for _, ca := range cases {
		job.Type = ca.tp
		job.SchemaState = ca.state
		re := job.IsRollbackable()
		require.Equal(t, ca.result, re)
	}
}

func TestError(t *testing.T) {
	kvErrs := []*terror.Error{
		dbterror.ErrDDLJobNotFound,
		dbterror.ErrCancelFinishedDDLJob,
		dbterror.ErrCannotCancelDDLJob,
	}
	for _, err := range kvErrs {
		code := terror.ToSQLError(err).Code
		require.NotEqual(t, mysql.ErrUnknown, code)
		require.Equal(t, uint16(err.Code()), code)
	}
}

func newMockStore(t *testing.T) (store kv.Storage, clean func()) {
	var err error
	store, err = mockstore.NewMockStore()
	require.NoError(t, err)

	clean = func() {
		err = store.Close()
		require.NoError(t, err)
	}

	return
}

func isJobsSorted(jobs []*model.Job) bool {
	if len(jobs) <= 1 {
		return true
	}
	for i := 1; i < len(jobs); i++ {
		if jobs[i].ID <= jobs[i-1].ID {
			return false
		}
	}
	return true
}

func enQueueDDLJobs(t *testing.T, m *meta.Meta, jobType model.ActionType, start, end int) {
	for i := start; i < end; i++ {
		job := &model.Job{
			ID:       int64(i),
			SchemaID: 1,
			Type:     jobType,
		}
		err := m.EnQueueDDLJob(job)
		require.NoError(t, err)
	}
}
