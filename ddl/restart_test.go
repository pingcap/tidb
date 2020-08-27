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
// See the License for the specific language governing permissions and
// limitations under the License.
// +build !race

package ddl

import (
	"context"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/util/mock"
)

// this test file include some test that will cause data race, mainly because restartWorkers modify d.ctx

// restartWorkers is like the function of d.start. But it won't initialize the "workers" and create a new worker.
// It only starts the original workers.
func (d *ddl) restartWorkers(ctx context.Context) {
	d.cancel()
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
		w := worker
		go w.start(d.ddlCtx)
		asyncNotify(worker.ddlJobCh)
	}
}

// runInterruptedJob should be called concurrently with restartWorkers
func runInterruptedJob(c *C, d *ddl, job *model.Job, doneCh chan struct{}) {
	ctx := mock.NewContext()
	ctx.Store = d.store

	var (
		history *model.Job
		err     error
	)

	_ = d.doDDLJob(ctx, job)

	for history == nil {
		history, err = d.getHistoryDDLJob(job.ID)
		c.Assert(err, IsNil)
		time.Sleep(10 * testLease)
	}
	c.Assert(history.Error, IsNil)
	doneCh <- struct{}{}
}

func testRunInterruptedJob(c *C, d *ddl, job *model.Job) {
	done := make(chan struct{}, 1)
	go runInterruptedJob(c, d, job, done)

	ticker := time.NewTicker(d.lease * 1)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case <-ticker.C:
			d.Stop()
			d.restartWorkers(context.Background())
			time.Sleep(time.Millisecond * 20)
		case <-done:
			break LOOP
		}
	}
}

func (s *testSchemaSuite) TestSchemaResume(c *C) {
	store := testCreateStore(c, "test_schema_resume")
	defer store.Close()

	d1 := testNewDDLAndStart(
		context.Background(),
		c,
		WithStore(store),
		WithLease(testLease),
	)
	defer d1.Stop()

	testCheckOwner(c, d1, true)

	dbInfo := testSchemaInfo(c, d1, "test")
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{dbInfo},
	}
	testRunInterruptedJob(c, d1, job)
	testCheckSchemaState(c, d1, dbInfo, model.StatePublic)

	job = &model.Job{
		SchemaID:   dbInfo.ID,
		Type:       model.ActionDropSchema,
		BinlogInfo: &model.HistoryInfo{},
	}
	testRunInterruptedJob(c, d1, job)
	testCheckSchemaState(c, d1, dbInfo, model.StateNone)
}

func (s *testStatSuite) TestStat(c *C) {
	store := testCreateStore(c, "test_stat")
	defer store.Close()

	d := testNewDDLAndStart(
		context.Background(),
		c,
		WithStore(store),
		WithLease(testLease),
	)
	defer d.Stop()

	dbInfo := testSchemaInfo(c, d, "test")
	testCreateSchema(c, testNewContext(d), d, dbInfo)

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

	done := make(chan struct{}, 1)
	go runInterruptedJob(c, d, job, done)

	ticker := time.NewTicker(d.lease * 1)
	defer ticker.Stop()
	ver := s.getDDLSchemaVer(c, d)
LOOP:
	for {
		select {
		case <-ticker.C:
			d.Stop()
			c.Assert(s.getDDLSchemaVer(c, d), GreaterEqual, ver)
			d.restartWorkers(context.Background())
			time.Sleep(time.Millisecond * 20)
		case <-done:
			// TODO: Get this information from etcd.
			// m, err := d.Stats(nil)
			// c.Assert(err, IsNil)
			break LOOP
		}
	}
}

func (s *testTableSuite) TestTableResume(c *C) {
	d := s.d

	testCheckOwner(c, d, true)

	tblInfo := testTableInfo(c, d, "t1", 3)
	job := &model.Job{
		SchemaID:   s.dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tblInfo},
	}
	testRunInterruptedJob(c, d, job)
	testCheckTableState(c, d, s.dbInfo, tblInfo, model.StatePublic)

	job = &model.Job{
		SchemaID:   s.dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionDropTable,
		BinlogInfo: &model.HistoryInfo{},
	}
	testRunInterruptedJob(c, d, job)
	testCheckTableState(c, d, s.dbInfo, tblInfo, model.StateNone)
}
