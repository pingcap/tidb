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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

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
	require.NoError(t, d.doDDLJob(ctx, job))

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
	err := d.doDDLJob(ctx, job)
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

func ExportTestSchema(t *testing.T) {
	store := createMockStore(t)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
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
	ctx := testNewContext(d)
	dbInfo, err := testSchemaInfo(d, "test_schema")
	require.NoError(t, err)

	// create a database.
	job := testCreateSchema(t, ctx, d, dbInfo)
	testCheckSchemaState(t, d, dbInfo, model.StatePublic)
	testCheckJobDone(t, d, job, true)

	/*** to drop the schema with two tables. ***/
	// create table t with 100 records.
	tblInfo1, err := testTableInfo(d, "t", 3)
	require.NoError(t, err)
	tJob1 := testCreateTable(t, ctx, d, dbInfo, tblInfo1)
	testCheckTableState(t, d, dbInfo, tblInfo1, model.StatePublic)
	testCheckJobDone(t, d, tJob1, true)
	tbl1 := testGetTable(t, d, dbInfo.ID, tblInfo1.ID)
	for i := 1; i <= 100; i++ {
		_, err := tbl1.AddRecord(ctx, types.MakeDatums(i, i, i))
		require.NoError(t, err)
	}
	// create table t1 with 1034 records.
	tblInfo2, err := testTableInfo(d, "t1", 3)
	require.NoError(t, err)
	tJob2 := testCreateTable(t, ctx, d, dbInfo, tblInfo2)
	testCheckTableState(t, d, dbInfo, tblInfo2, model.StatePublic)
	testCheckJobDone(t, d, tJob2, true)
	tbl2 := testGetTable(t, d, dbInfo.ID, tblInfo2.ID)
	for i := 1; i <= 1034; i++ {
		_, err := tbl2.AddRecord(ctx, types.MakeDatums(i, i, i))
		require.NoError(t, err)
	}
	job, v := testDropSchema(t, ctx, d, dbInfo)
	testCheckSchemaState(t, d, dbInfo, model.StateNone)
	ids := make(map[int64]struct{})
	ids[tblInfo1.ID] = struct{}{}
	ids[tblInfo2.ID] = struct{}{}
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, db: dbInfo, tblIDs: ids})

	// Drop a non-existent database.
	job = &model.Job{
		SchemaID:   dbInfo.ID,
		Type:       model.ActionDropSchema,
		BinlogInfo: &model.HistoryInfo{},
	}
	ctx.SetValue(sessionctx.QueryString, "skip")
	err = d.doDDLJob(ctx, job)
	require.True(t, terror.ErrorEqual(err, infoschema.ErrDatabaseDropExists), "err %v", err)

	// Drop a database without a table.
	dbInfo1, err := testSchemaInfo(d, "test1")
	require.NoError(t, err)
	job = testCreateSchema(t, ctx, d, dbInfo1)
	testCheckSchemaState(t, d, dbInfo1, model.StatePublic)
	testCheckJobDone(t, d, job, true)
	job, _ = testDropSchema(t, ctx, d, dbInfo1)
	testCheckSchemaState(t, d, dbInfo1, model.StateNone)
	testCheckJobDone(t, d, job, false)
}

func TestSchemaWaitJob(t *testing.T) {
	store := createMockStore(t)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
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

	testCheckOwner(t, d1, true)

	d2, err := testNewDDLAndStart(
		context.Background(),
		WithStore(store),
		WithLease(testLease*4),
	)
	require.NoError(t, err)
	defer func() {
		err := d2.Stop()
		require.NoError(t, err)
	}()
	ctx := testNewContext(d2)

	// d2 must not be owner.
	d2.ownerManager.RetireOwner()

	dbInfo, err := testSchemaInfo(d2, "test_schema")
	require.NoError(t, err)
	testCreateSchema(t, ctx, d2, dbInfo)
	testCheckSchemaState(t, d2, dbInfo, model.StatePublic)

	// d2 must not be owner.
	require.False(t, d2.ownerManager.IsOwner())

	genIDs, err := d2.genGlobalIDs(1)
	require.NoError(t, err)
	schemaID := genIDs[0]
	doDDLJobErr(t, schemaID, 0, model.ActionCreateSchema, []interface{}{dbInfo}, ctx, d2)
}

func testGetSchemaInfoWithError(d *ddl, schemaID int64) (*model.DBInfo, error) {
	var dbInfo *model.DBInfo
	err := kv.RunInNewTxn(context.Background(), d.store, false, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err1 error
		dbInfo, err1 = t.GetDatabase(schemaID)
		if err1 != nil {
			return errors.Trace(err1)
		}
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return dbInfo, nil
}
