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
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

type testCtxKeyType int

func (k testCtxKeyType) String() string {
	return "test_ctx_key"
}

const testCtxKey testCtxKeyType = 0

func (s *testDDLSuiteToVerify) TestReorg() {
	store := testCreateStore(s.T(), "test_reorg")
	defer func() {
		err := store.Close()
		require.NoError(s.T(), err)
	}()

	d, err := testNewDDLAndStart(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	require.NoError(s.T(), err)
	defer func() {
		err := d.Stop()
		require.NoError(s.T(), err)
	}()

	time.Sleep(testLease)

	ctx := testNewContext(d)

	ctx.SetValue(testCtxKey, 1)
	require.Equal(s.T(), ctx.Value(testCtxKey), 1)
	ctx.ClearValue(testCtxKey)

	err = ctx.NewTxn(context.Background())
	require.NoError(s.T(), err)
	txn, err := ctx.Txn(true)
	require.NoError(s.T(), err)
	err = txn.Set([]byte("a"), []byte("b"))
	require.NoError(s.T(), err)
	err = txn.Rollback()
	require.NoError(s.T(), err)

	err = ctx.NewTxn(context.Background())
	require.NoError(s.T(), err)
	txn, err = ctx.Txn(true)
	require.NoError(s.T(), err)
	err = txn.Set([]byte("a"), []byte("b"))
	require.NoError(s.T(), err)
	err = txn.Commit(context.Background())
	require.NoError(s.T(), err)

	rowCount := int64(10)
	handle := s.NewHandle().Int(100).Common("a", 100, "string")
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
	err = ctx.NewTxn(context.Background())
	require.NoError(s.T(), err)
	txn, err = ctx.Txn(true)
	require.NoError(s.T(), err)
	m := meta.NewMeta(txn)
	e := &meta.Element{ID: 333, TypeKey: meta.IndexElementKey}
	rInfo := &reorgInfo{
		Job:         job,
		currElement: e,
	}
	mockTbl := tables.MockTableFromMeta(&model.TableInfo{IsCommonHandle: s.IsCommonHandle, CommonHandleVersion: 1})
	err = d.generalWorker().runReorgJob(m, rInfo, mockTbl.Meta(), d.lease, f)
	require.Error(s.T(), err)

	// The longest to wait for 5 seconds to make sure the function of f is returned.
	for i := 0; i < 1000; i++ {
		time.Sleep(5 * time.Millisecond)
		err = d.generalWorker().runReorgJob(m, rInfo, mockTbl.Meta(), d.lease, f)
		if err == nil {
			require.Equal(s.T(), job.RowCount, rowCount)
			require.Equal(s.T(), d.generalWorker().reorgCtx.rowCount, int64(0))

			// Test whether reorgInfo's Handle is update.
			err = txn.Commit(context.Background())
			require.NoError(s.T(), err)
			err = ctx.NewTxn(context.Background())
			require.NoError(s.T(), err)

			m = meta.NewMeta(txn)
			info, err1 := getReorgInfo(d.ddlCtx, m, job, mockTbl, nil)
			require.NoError(s.T(), err1)
			require.Equal(s.T(), info.StartKey, kv.Key(handle.Encoded()))
			require.Equal(s.T(), info.currElement, e)
			_, doneHandle, _ := d.generalWorker().reorgCtx.getRowCountAndKey()
			require.Nil(s.T(), doneHandle)
			break
		}
	}
	require.NoError(s.T(), err)

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
		StartKey:        s.NewHandle().Int(1).Common(100, "string").Encoded(),
		EndKey:          s.NewHandle().Int(0).Common(101, "string").Encoded(),
		PhysicalTableID: 456,
	}
	err = kv.RunInNewTxn(context.Background(), d.store, false, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err1 error
		_, err1 = getReorgInfo(d.ddlCtx, t, job, mockTbl, []*meta.Element{element})
		require.True(s.T(), meta.ErrDDLReorgElementNotExist.Equal(err1))
		require.Equal(s.T(), job.SnapshotVer, uint64(0))
		return nil
	})
	require.NoError(s.T(), err)
	job.SnapshotVer = uint64(1)
	err = info.UpdateReorgMeta(info.StartKey)
	require.NoError(s.T(), err)
	err = kv.RunInNewTxn(context.Background(), d.store, false, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		info1, err1 := getReorgInfo(d.ddlCtx, t, job, mockTbl, []*meta.Element{element})
		require.NoError(s.T(), err1)
		require.Equal(s.T(), info1.currElement, info.currElement)
		require.Equal(s.T(), info1.StartKey, info.StartKey)
		require.Equal(s.T(), info1.EndKey, info.EndKey)
		require.Equal(s.T(), info1.PhysicalTableID, info.PhysicalTableID)
		return nil
	})
	require.NoError(s.T(), err)

	err = d.Stop()
	require.NoError(s.T(), err)
	err = d.generalWorker().runReorgJob(m, rInfo, mockTbl.Meta(), d.lease, func() error {
		time.Sleep(4 * testLease)
		return nil
	})
	require.Error(s.T(), err)
	txn, err = ctx.Txn(true)
	require.NoError(s.T(), err)
	err = txn.Commit(context.Background())
	require.NoError(s.T(), err)
	s.RerunWithCommonHandleEnabledWithoutCheck(s.TestReorg)
}

func (s *testDDLSuiteToVerify) TestReorgOwner() {
	store := testCreateStore(s.T(), "test_reorg_owner")
	defer func() {
		err := store.Close()
		require.NoError(s.T(), err)
	}()

	d1, err := testNewDDLAndStart(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	require.NoError(s.T(), err)
	defer func() {
		err := d1.Stop()
		require.NoError(s.T(), err)
	}()

	ctx := testNewContext(d1)

	testCheckOwner(s.T(), d1, true)

	d2, err := testNewDDLAndStart(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	require.NoError(s.T(), err)
	defer func() {
		err := d2.Stop()
		require.NoError(s.T(), err)
	}()

	dbInfo, err := testSchemaInfo(d1, "test_reorg")
	require.NoError(s.T(), err)
	testCreateSchema(s.T(), ctx, d1, dbInfo)

	tblInfo, err := testTableInfo(d1, "t", 3)
	require.NoError(s.T(), err)
	testCreateTable(s.T(), ctx, d1, dbInfo, tblInfo)
	t := testGetTable(s.T(), d1, dbInfo.ID, tblInfo.ID)

	num := 10
	for i := 0; i < num; i++ {
		_, err := t.AddRecord(ctx, types.MakeDatums(i, i, i))
		require.NoError(s.T(), err)
	}

	txn, err := ctx.Txn(true)
	require.NoError(s.T(), err)
	err = txn.Commit(context.Background())
	require.NoError(s.T(), err)

	tc := &TestDDLCallback{}
	tc.onJobRunBefore = func(job *model.Job) {
		if job.SchemaState == model.StateDeleteReorganization {
			err = d1.Stop()
			require.NoError(s.T(), err)
		}
	}

	d1.SetHook(tc)

	testDropSchema(s.T(), ctx, d1, dbInfo)

	err = kv.RunInNewTxn(context.Background(), d1.store, false, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		db, err1 := t.GetDatabase(dbInfo.ID)
		require.NoError(s.T(), err1)
		require.Nil(s.T(), db)
		return nil
	})
	require.NoError(s.T(), err)
}
