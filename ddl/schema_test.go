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

package ddl_test

import (
	"context"
	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/testkit"
	"testing"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/stretchr/testify/require"
)

func genGlobalIDs(store kv.Storage, count int) ([]int64, error) {
	var ret []int64
	err := kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		var err error
		ret, err = m.GenGlobalIDs(count)
		return err
	})
	return ret, err
}

func testSchemaInfo(store kv.Storage, name string) (*model.DBInfo, error) {
	dbInfo := &model.DBInfo{
		Name: model.NewCIStr(name),
	}

	genIDs, err := genGlobalIDs(store, 1)
	if err != nil {
		return nil, err
	}
	dbInfo.ID = genIDs[0]
	return dbInfo, nil
}

func testCreateSchema(t *testing.T, ctx sessionctx.Context, d ddl.DDL, dbInfo *model.DBInfo) *model.Job {
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

func testDropSchema(t *testing.T, ctx sessionctx.Context, d ddl.DDL, dbInfo *model.DBInfo) (*model.Job, int64) {
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

func testCheckSchemaState(test *testing.T, store kv.Storage, dbInfo *model.DBInfo, state model.SchemaState) {
	isDropped := true

	for {
		err := kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
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

func TestSchemaWaitJob(t *testing.T) {
	store, domain, clean := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)
	defer clean()

	testCheckOwner(t, domain.DDL(), true)

	d2 := ddl.NewDDL(context.Background(),
		ddl.WithEtcdClient(domain.EtcdClient()),
		ddl.WithStore(store),
		ddl.WithInfoCache(domain.InfoCache()),
		ddl.WithLease(testLease),
	)
	err := d2.Start(pools.NewResourcePool(func() (pools.Resource, error) {
		return testkit.NewTestKit(t, store).Session(), nil
	}, 2, 2, 5))
	require.NoError(t, err)
	defer func() {
		err := d2.Stop()
		require.NoError(t, err)
	}()

	// d2 must not be owner.
	d2.OwnerManager().RetireOwner()

	dbInfo, err := testSchemaInfo(store, "test_schema")
	require.NoError(t, err)
	testCreateSchema(t, testkit.NewTestKit(t, store).Session(), d2, dbInfo)
	testCheckSchemaState(t, store, dbInfo, model.StatePublic)

	// d2 must not be owner.
	require.False(t, d2.OwnerManager().IsOwner())

	genIDs, err := genGlobalIDs(store, 1)
	require.NoError(t, err)
	schemaID := genIDs[0]
	doDDLJobErr(t, schemaID, 0, model.ActionCreateSchema, []interface{}{dbInfo}, testkit.NewTestKit(t, store).Session(), d2, store)
}

func doDDLJobErr(t *testing.T, schemaID, tableID int64, tp model.ActionType, args []interface{}, ctx sessionctx.Context, d ddl.DDL, store kv.Storage) *model.Job {
	job := &model.Job{
		SchemaID:   schemaID,
		TableID:    tableID,
		Type:       tp,
		Args:       args,
		BinlogInfo: &model.HistoryInfo{},
	}
	// TODO: check error detail
	require.Error(t, d.DoDDLJob(ctx, job))
	testCheckJobCancelled(t, store, job, nil)

	return job
}

func testCheckJobCancelled(t *testing.T, store kv.Storage, job *model.Job, state *model.SchemaState) {
	require.NoError(t, kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		historyJob, err := m.GetHistoryDDLJob(job.ID)
		require.NoError(t, err)
		require.True(t, historyJob.IsCancelled() || historyJob.IsRollbackDone(), "history job %s", historyJob)
		if state != nil {
			require.Equal(t, historyJob.SchemaState, *state)
		}
		return nil
	}))
}

func testCheckOwner(t *testing.T, d ddl.DDL, expectedVal bool) {
	require.Equal(t, d.OwnerManager().IsOwner(), expectedVal)
}
