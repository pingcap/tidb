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
func runInterruptedJob(t *testing.T, d *ddl, job *model.Job, doneCh chan error) {
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

func testRunInterruptedJob(t *testing.T, d *ddl, job *model.Job) {
	done := make(chan error, 1)
	go runInterruptedJob(t, d, job, done)

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
			// Checking for nil instead of NoError because err doesn't seem to be
			// nil, but an object equal to nil.
			require.Nil(t, err)
			break LOOP
		}
	}
}

func TestSchemaResume(t *testing.T) {
	store := testCreateStore(t, "test_schema_resume")
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()

	d1 := testNewDDLAndStart(
		context.Background(),
		t,
		WithStore(store),
		WithLease(testLease),
	)
	defer func() {
		err := d1.Stop()
		require.NoError(t, err)
	}()

	testCheckOwner(t, d1, true)

	dbInfo := testSchemaInfo(t, d1, "test_restart")
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
	s := new(statSuite)
	store := testCreateStore(t, "test_stat")
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()

	d := testNewDDLAndStart(
		context.Background(),
		t,
		WithStore(store),
		WithLease(testLease),
	)
	defer func() {
		err := d.Stop()
		require.NoError(t, err)
	}()

	dbInfo := testSchemaInfo(t, d, "test_restart")
	testCreateSchema(t, testNewContext(d), d, dbInfo)

	// TODO: Get this information from etcd.
	//	m, err := d.Stats(nil)
	//	require.NoError(t, err)
	//	c.Assert(m[ddlOwnerID], Equals, d.uuid)

	job := &model.Job{
		SchemaID:   dbInfo.ID,
		Type:       model.ActionDropSchema,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{dbInfo.Name},
	}

	done := make(chan error, 1)
	go runInterruptedJob(t, d, job, done)

	ticker := time.NewTicker(d.lease * 1)
	defer ticker.Stop()
	ver := s.getDDLSchemaVer(t, d)
LOOP:
	for {
		select {
		case <-ticker.C:
			err := d.Stop()
			require.NoError(t, err)
			require.GreaterOrEqual(t, s.getDDLSchemaVer(t, d), ver)
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
	s, clean := createTableSuite(t)
	defer clean()
	d := s.d

	testCheckOwner(t, d, true)

	tblInfo := testTableInfo(t, d, "t1", 3)
	job := &model.Job{
		SchemaID:   s.dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tblInfo},
	}
	testRunInterruptedJob(t, d, job)
	testCheckTableState(t, d, s.dbInfo, tblInfo, model.StatePublic)

	job = &model.Job{
		SchemaID:   s.dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionDropTable,
		BinlogInfo: &model.HistoryInfo{},
	}
	testRunInterruptedJob(t, d, job)
	testCheckTableState(t, d, s.dbInfo, tblInfo, model.StateNone)
}
