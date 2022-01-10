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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util/mock"
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
func runInterruptedJob(c *C, d *ddl, job *model.Job, doneCh chan error) {
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

func testRunInterruptedJob(c *C, d *ddl, job *model.Job) {
	done := make(chan error, 1)
	go runInterruptedJob(c, d, job, done)

	ticker := time.NewTicker(d.lease * 1)
	defer ticker.Stop()
LOOP:
	for {
		select {
		case <-ticker.C:
			err := d.Stop()
			c.Assert(err, IsNil)
			d.restartWorkers(context.Background())
			time.Sleep(time.Millisecond * 20)
		case err := <-done:
			c.Assert(err, IsNil)
			break LOOP
		}
	}
}

func (s *testSchemaSuite) TestSchemaResume(c *C) {
	store := testCreateStore(c, "test_schema_resume")
	defer func() {
		err := store.Close()
		c.Assert(err, IsNil)
	}()

	d1, err := testNewDDLAndStart(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	c.Assert(err, IsNil)
	defer func() {
		err := d1.Stop()
		c.Assert(err, IsNil)
	}()

	testCheckOwner(c, d1, true)

	dbInfo, err := testSchemaInfo(d1, "test_restart")
	c.Assert(err, IsNil)
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
	defer func() {
		err := store.Close()
		c.Assert(err, IsNil)
	}()

	d, err := testNewDDLAndStart(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)
	c.Assert(err, IsNil)
	defer func() {
		err := d.Stop()
		c.Assert(err, IsNil)
	}()

	dbInfo, err := testSchemaInfo(d, "test_restart")
	c.Assert(err, IsNil)
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

	done := make(chan error, 1)
	go runInterruptedJob(c, d, job, done)

	ticker := time.NewTicker(d.lease * 1)
	defer ticker.Stop()
	ver := s.getDDLSchemaVer(c, d)
LOOP:
	for {
		select {
		case <-ticker.C:
			err := d.Stop()
			c.Assert(err, IsNil)
			c.Assert(s.getDDLSchemaVer(c, d), GreaterEqual, ver)
			d.restartWorkers(context.Background())
			time.Sleep(time.Millisecond * 20)
		case err := <-done:
			// TODO: Get this information from etcd.
			// m, err := d.Stats(nil)
			c.Assert(err, IsNil)
			break LOOP
		}
	}
}

var _ = Suite(&testTableSuite{})

type testTableSuite struct {
	store  kv.Storage
	dbInfo *model.DBInfo

	d *ddl
}

func (s *testTableSuite) SetUpSuite(c *C) {
	s.store = testCreateStore(c, "test_table")
	ddl, err := testNewDDLAndStart(
		context.Background(),
		WithStore(s.store),
		WithLease(testLease),
	)
	c.Assert(err, IsNil)
	s.d = ddl

	s.dbInfo, err = testSchemaInfo(s.d, "test_table")
	c.Assert(err, IsNil)
	testCreateSchema(c, testNewContext(s.d), s.d, s.dbInfo)
}

func (s *testTableSuite) TearDownSuite(c *C) {
	testDropSchema(c, testNewContext(s.d), s.d, s.dbInfo)
	err := s.d.Stop()
	c.Assert(err, IsNil)
	err = s.store.Close()
	c.Assert(err, IsNil)
}

func (s *testTableSuite) TestTableResume(c *C) {
	d := s.d

	testCheckOwner(c, d, true)

	tblInfo, err := testTableInfo(d, "t1", 3)
	c.Assert(err, IsNil)
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
