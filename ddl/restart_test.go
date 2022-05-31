// Copyright 2020 PingCAP, Inc.
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
//go:build !race

package ddl

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

// this test file include some test that will cause data race, mainly because restartWorkers modify d.ctx

func getDDLSchemaVer(t *testing.T, d *ddl) int64 {
	m, err := d.Stats(nil)
	require.NoError(t, err)
	v := m[ddlSchemaVersion]
	return v.(int64)
}

// restartWorkers is like the function of d.start. But it won't initialize the "workers" and create a new worker.
// It only starts the original workers.
func (d *ddl) restartWorkers(ctx context.Context) {
	d.ctx, d.cancel = context.WithCancel(ctx)

	d.wg.Run(d.limitDDLJobs)
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

// runInterruptedJob should be called concurrently with restartWorkers
func runInterruptedJob(d *ddl, job *model.Job, doneCh chan error) {
	ctx := mock.NewContext()
	ctx.Store = d.store

	var (
		history *model.Job
		err     error
	)

	ctx.SetValue(sessionctx.QueryString, "skip")
	err = d.DoDDLJob(ctx, job)
	if errors.Is(err, context.Canceled) {
		endlessLoopTime := time.Now().Add(time.Minute)
		for history == nil {
			// imitate DoDDLJob's logic, quit only find history
			history, _ = d.getHistoryDDLJob(job.ID)
			if history != nil {
				err = history.Error
			}
			time.Sleep(10 * testLease)
			if time.Now().After(endlessLoopTime) {
				err = errors.New("runInterruptedJob may enter endless loop")
				break
			}
		}
	}

	doneCh <- err
}

func testRunInterruptedJob(t *testing.T, d *ddl, job *model.Job) {
	done := make(chan error, 1)
	go runInterruptedJob(d, job, done)

	ticker := time.NewTicker(d.lease * 1)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			err := d.Stop()
			require.NoError(t, err)
			d.restartWorkers(context.Background())
			time.Sleep(time.Millisecond * 20)
		case err := <-done:
			require.Nil(t, err)
			return
		}
	}
}

func TestSchemaResume(t *testing.T) {
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
		require.NoError(t, d1.Stop())
	}()

	require.True(t, d1.OwnerManager().IsOwner())

	dbInfo, err := testSchemaInfo(d1, "test_restart")
	require.NoError(t, err)
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{dbInfo},
	}
	testRunInterruptedJob(t, d1, job)
	testCheckSchemaState(t, d1, dbInfo, model.StatePublic)

	job = &model.Job{
		SchemaID:   dbInfo.ID,
		Type:       model.ActionDropSchema,
		BinlogInfo: &model.HistoryInfo{},
	}
	testRunInterruptedJob(t, d1, job)
	testCheckSchemaState(t, d1, dbInfo, model.StateNone)
}

func TestStat(t *testing.T) {
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
		require.NoError(t, d.Stop())
	}()

	dbInfo, err := testSchemaInfo(d, "test_restart")
	require.NoError(t, err)
	testCreateSchema(t, testNewContext(d), d, dbInfo)

	// TODO: Get this information from etcd.
	//	m, err := d.Stats(nil)
	//	c.Assert(err, IsNil)
	//	c.Assert(m[ddlOwnerID], Equals, d.uuid)

	job := &model.Job{
		SchemaID:   dbInfo.ID,
		Type:       model.ActionDropSchema,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{dbInfo.Name},
	}

	done := make(chan error, 1)
	go runInterruptedJob(d, job, done)

	ticker := time.NewTicker(d.lease * 1)
	defer ticker.Stop()
	ver := getDDLSchemaVer(t, d)
LOOP:
	for {
		select {
		case <-ticker.C:
			err := d.Stop()
			require.Nil(t, err)
			require.GreaterOrEqual(t, getDDLSchemaVer(t, d), ver)
			d.restartWorkers(context.Background())
			time.Sleep(time.Millisecond * 20)
		case err := <-done:
			// TODO: Get this information from etcd.
			// m, err := d.Stats(nil)
			require.Nil(t, err)
			break LOOP
		}
	}
}

func TestTableResume(t *testing.T) {
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
		require.NoError(t, d.Stop())
	}()

	dbInfo, err := testSchemaInfo(d, "test_table")
	require.NoError(t, err)
	testCreateSchema(t, testNewContext(d), d, dbInfo)
	defer func() {
		testDropSchema(t, testNewContext(d), d, dbInfo)
	}()

	require.True(t, d.OwnerManager().IsOwner())

	tblInfo, err := testTableInfo(d, "t1", 3)
	require.NoError(t, err)
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tblInfo},
	}
	testRunInterruptedJob(t, d, job)
	testCheckTableState(t, d, dbInfo, tblInfo, model.StatePublic)

	job = &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionDropTable,
		BinlogInfo: &model.HistoryInfo{},
	}
	testRunInterruptedJob(t, d, job)
	testCheckTableState(t, d, dbInfo, tblInfo, model.StateNone)
}

// testTableInfo creates a test table with num int columns and with no index.
func testTableInfo(d *ddl, name string, num int) (*model.TableInfo, error) {
	tblInfo := &model.TableInfo{
		Name: model.NewCIStr(name),
	}
	genIDs, err := d.genGlobalIDs(1)

	if err != nil {
		return nil, err
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
	return tblInfo, nil
}

func testCheckTableState(t *testing.T, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, state model.SchemaState) {
	require.NoError(t, kv.RunInNewTxn(context.Background(), d.store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		info, err := m.GetTable(dbInfo.ID, tblInfo.ID)
		require.NoError(t, err)

		if state == model.StateNone {
			require.NoError(t, err)
			return nil
		}

		require.Equal(t, info.Name, tblInfo.Name)
		require.Equal(t, info.State, state)
		return nil
	}))
}
