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

	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/testkit/testutil"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

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

			err = ctx.NewTxn(context.Background())
			require.NoError(t, err)
			txn, err := ctx.Txn(true)
			require.NoError(t, err)
			err = txn.Set([]byte("a"), []byte("b"))
			require.NoError(t, err)
			err = txn.Rollback()
			require.NoError(t, err)

			err = ctx.NewTxn(context.Background())
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
			err = ctx.NewTxn(context.Background())
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
					err = ctx.NewTxn(context.Background())
					require.NoError(t, err)

					m = meta.NewMeta(txn)
					info, err1 := getReorgInfo(d.ddlCtx, m, job, mockTbl, nil)
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
				_, err1 = getReorgInfo(d.ddlCtx, m, job, mockTbl, []*meta.Element{element})
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
				info1, err1 := getReorgInfo(d.ddlCtx, m, job, mockTbl, []*meta.Element{element})
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

func TestReorgOwner(t *testing.T) {
	_, err := infosync.GlobalInfoSyncerInit(context.Background(), "t", func() uint64 { return 1 }, nil, true)
	require.NoError(t, err)

	store := createMockStore(t)
	defer func() {
		require.NoError(t, store.Close())
	}()

	d1, err := testNewDDLAndStart(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	require.NoError(t, err)
	defer func() {
		err := d1.Stop()
		require.NoError(t, err)
	}()

	ctx := testNewContext(d1)

	testCheckOwner(t, d1, true)

	d2, err := testNewDDLAndStart(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	require.NoError(t, err)
	defer func() {
		err := d2.Stop()
		require.NoError(t, err)
	}()

	dbInfo, err := testSchemaInfo(d1, "test_reorg")
	require.NoError(t, err)
	testCreateSchema(t, ctx, d1, dbInfo)

	tblInfo, err := testTableInfo(d1, "t", 3)
	require.NoError(t, err)
	testCreateTable(t, ctx, d1, dbInfo, tblInfo)
	tbl := testGetTable(t, d1, dbInfo.ID, tblInfo.ID)

	num := 10
	for i := 0; i < num; i++ {
		_, err := tbl.AddRecord(ctx, types.MakeDatums(i, i, i))
		require.NoError(t, err)
	}

	txn, err := ctx.Txn(true)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	tc := &TestDDLCallback{}
	tc.onJobRunBefore = func(job *model.Job) {
		if job.SchemaState == model.StateDeleteReorganization {
			err = d1.Stop()
			require.NoError(t, err)
		}
	}

	d1.SetHook(tc)

	testDropSchema(t, ctx, d1, dbInfo)

	err = kv.RunInNewTxn(context.Background(), d1.store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		db, err1 := m.GetDatabase(dbInfo.ID)
		require.NoError(t, err1)
		require.Nil(t, db)
		return nil
	})
	require.NoError(t, err)
}
