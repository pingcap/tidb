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
// +build !race

package ddl

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

// this test file include some test that will cause data race, mainly because restartWorkers modify d.ctx

// restartWorkers is like the function of d.start. But it won't initialize the "workers" and create a new worker.
// It only starts the original workers.
func (d *ddl) restartWorkers(ctx context.Context) {
	d.ctx, d.cancel = context.WithCancel(ctx)

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
		worker.ddlJobCtx = context.Background()
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

	err = d.doDDLJob(ctx, job)
	if errors.Is(err, context.Canceled) {
		endlessLoopTime := time.Now().Add(time.Minute)
		for history == nil {
			// imitate doDDLJob's logic, quit only find history
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

func testRunInterruptedJobT(t *testing.T, d *ddl, job *model.Job) {
	done := make(chan error, 1)
	go runInterruptedJob(d, job, done)

	ticker := time.NewTicker(d.lease * 1)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case <-ticker.C:
			err := d.Stop()
			require.NoError(t, err)
			d.restartWorkers(context.Background())
			time.Sleep(time.Millisecond * 20)
		case err := <-done:
			// NOTE: DO NOT use `require.NoError` here.
			require.Nil(t, err)
			break LOOP
		}
	}
}

func testCheckOwnerT(t *testing.T, d *ddl, expectedVal bool) {
	require.Equal(t, expectedVal, d.isOwner())
}

func isDDLJobDoneT(t *testing.T, m *meta.Meta) bool {
	job, err := m.GetDDLJobByIdx(0)
	require.NoError(t, err)
	if job == nil {
		return true
	}

	time.Sleep(testLease)
	return false
}

func getDDLSchemaVerT(t *testing.T, d *ddl) int64 {
	m, err := d.Stats(nil)
	require.NoError(t, err)
	v := m[ddlSchemaVersion]
	return v.(int64)
}

func testCheckSchemaStateT(t *testing.T, d *ddl, dbInfo *model.DBInfo, state model.SchemaState) {
	isDropped := true

	for {
		err := kv.RunInNewTxn(context.Background(), d.store, false, func(ctx context.Context, txn kv.Transaction) error {
			m := meta.NewMeta(txn)
			info, err := m.GetDatabase(dbInfo.ID)
			require.NoError(t, err)

			if state == model.StateNone {
				isDropped = isDDLJobDoneT(t, m)
				if !isDropped {
					return nil
				}
				require.NoError(t, err)
				return nil
			}

			require.Equal(t, dbInfo.Name, info.Name)
			require.Equal(t, state, info.State)
			return nil
		})
		require.NoError(t, err)

		if isDropped {
			break
		}
	}
}

func TestSchemaResume(t *testing.T) {
	store := testCreateStoreT(t, "test_schema_resume")
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

	testCheckOwnerT(t, d1, true)

	dbInfo, err := testSchemaInfo(d1, "test_restart")
	require.NoError(t, err)
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{dbInfo},
	}
	testRunInterruptedJobT(t, d1, job)
	testCheckSchemaStateT(t, d1, dbInfo, model.StatePublic)

	job = &model.Job{
		SchemaID:   dbInfo.ID,
		Type:       model.ActionDropSchema,
		BinlogInfo: &model.HistoryInfo{},
	}
	testRunInterruptedJobT(t, d1, job)
	testCheckSchemaStateT(t, d1, dbInfo, model.StateNone)
}

func TestStat(t *testing.T) {
	store := testCreateStoreT(t, "test_stat")
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

	dbInfo, err := testSchemaInfo(d, "test_restart")
	require.NoError(t, err)
	testCreateSchemaT(t, testNewContext(d), d, dbInfo)

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
	ver := getDDLSchemaVerT(t, d)
LOOP:
	for {
		select {
		case <-ticker.C:
			err := d.Stop()
			require.NoError(t, err)
			require.GreaterOrEqual(t, getDDLSchemaVerT(t, d), ver)
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
	_, err := infosync.GlobalInfoSyncerInit(context.Background(), "t", func() uint64 { return 1 }, nil, true)
	require.NoError(t, err)

	store := testCreateStoreT(t, "test_table")
	ddl, err := testNewDDLAndStart(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	require.NoError(t, err)

	dbInfo, err := testSchemaInfo(ddl, "test_table")
	require.NoError(t, err)
	testCreateSchemaT(t, testNewContext(ddl), ddl, dbInfo)
	testCheckOwnerT(t, ddl, true)

	tblInfo, err := testTableInfo(ddl, "t1", 3)
	require.NoError(t, err)

	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tblInfo},
	}
	testRunInterruptedJobT(t, ddl, job)
	testCheckTableStateT(t, ddl, dbInfo, tblInfo, model.StatePublic)

	job = &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionDropTable,
		BinlogInfo: &model.HistoryInfo{},
	}
	testRunInterruptedJobT(t, ddl, job)
	testCheckTableStateT(t, ddl, dbInfo, tblInfo, model.StateNone)
}
