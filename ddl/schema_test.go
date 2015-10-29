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
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/errors2"
	"github.com/pingcap/tidb/util/mock"
)

func (s *testDDLSuite) TestSchema(c *C) {
	store := createTestStore(c, "test_schema")
	defer store.Close()

	lease := 100 * time.Millisecond

	d1 := newDDL(store, nil, nil, lease)
	defer d1.close()

	ctx := mock.NewContext()

	schema := model.NewCIStr("test")
	schemaID, err := d1.meta.GenGlobalID()
	c.Assert(err, IsNil)

	job := &model.Job{
		SchemaID: schemaID,
		Type:     model.ActionCreateSchema,
		Args:     []interface{}{schema},
	}

	err = d1.startJob(ctx, job)
	c.Assert(err, IsNil)

	d1.meta.RunInNewTxn(false, func(t *meta.TMeta) error {
		var dbInfo *model.DBInfo
		dbInfo, err = t.GetDatabase(schemaID)
		c.Assert(err, IsNil)
		c.Assert(dbInfo.Name, DeepEquals, model.NewCIStr("test"))
		c.Assert(dbInfo.State, Equals, model.StatePublic)

		var historyJob *model.Job
		historyJob, err = t.GetHistoryDDLJob(job.ID)
		c.Assert(err, IsNil)
		c.Assert(historyJob.State, Equals, model.JobDone)
		c.Assert(historyJob.SchemaState, Equals, model.StatePublic)
		return nil
	})

	job = &model.Job{
		SchemaID: schemaID,
		Type:     model.ActionDropSchema,
	}

	err = d1.startJob(ctx, job)
	c.Assert(err, IsNil)

	d1.meta.RunInNewTxn(false, func(t *meta.TMeta) error {
		dbInfo, err := t.GetDatabase(schemaID)
		c.Assert(err, IsNil)
		c.Assert(dbInfo, IsNil)

		var historyJob *model.Job
		historyJob, err = t.GetHistoryDDLJob(job.ID)
		c.Assert(err, IsNil)
		c.Assert(historyJob.State, Equals, model.JobDone)
		c.Assert(historyJob.SchemaState, Equals, model.StateNone)
		return nil
	})

	job = &model.Job{
		SchemaID: schemaID,
		Type:     model.ActionDropSchema,
	}

	err = d1.startJob(ctx, job)
	c.Assert(errors2.ErrorEqual(err, ErrNotExists), IsTrue)
}

func (s *testDDLSuite) TestSchemaWaitJob(c *C) {
	store := createTestStore(c, "test_schema_wait")
	defer store.Close()

	ctx := mock.NewContext()

	lease := 50 * time.Millisecond

	d1 := newDDL(store, nil, nil, lease)
	defer d1.close()

	testCheckOwner(c, d1, true)

	d2 := newDDL(store, nil, nil, lease)
	defer d2.close()

	// d2 must not be owner.
	testCheckOwner(c, d2, false)

	schema := model.NewCIStr("test")
	schemaID, err := d2.meta.GenGlobalID()
	c.Assert(err, IsNil)

	job := &model.Job{
		SchemaID: schemaID,
		Type:     model.ActionCreateSchema,
		Args:     []interface{}{schema},
	}

	err = d2.startJob(ctx, job)
	c.Assert(err, IsNil)

	d2.meta.RunInNewTxn(false, func(t *meta.TMeta) error {
		var dbInfo *model.DBInfo
		dbInfo, err = t.GetDatabase(schemaID)
		c.Assert(err, IsNil)
		c.Assert(dbInfo.Name, DeepEquals, model.NewCIStr("test"))
		c.Assert(dbInfo.State, Equals, model.StatePublic)
		return nil
	})

	// d2 must not be owner.
	testCheckOwner(c, d2, false)

	schemaID, err = d2.meta.GenGlobalID()
	c.Assert(err, IsNil)

	job = &model.Job{
		SchemaID: schemaID,
		Type:     model.ActionCreateSchema,
		Args:     []interface{}{schema},
	}

	err = d2.startJob(ctx, job)
	c.Assert(err, NotNil)

	d2.meta.RunInNewTxn(false, func(t *meta.TMeta) error {
		var historyJob *model.Job
		historyJob, err = t.GetHistoryDDLJob(job.ID)
		c.Assert(err, IsNil)
		c.Assert(historyJob.State, Equals, model.JobCancelled)
		return nil
	})

	// d2 must not be owner.
	testCheckOwner(c, d2, false)
}

func testRunInterruptedJob(c *C, d *ddl, job *model.Job) {
	ctx := mock.NewContext()
	done := make(chan error, 1)
	go func() {
		done <- d.startJob(ctx, job)
	}()

	ticker := time.NewTicker(d.lease * 1)
	defer ticker.Stop()

LOOP:
	for {
		select {
		case <-ticker.C:
			d.close()
			d.start()
			asyncNotify(d.jobCh)
		case err := <-done:
			c.Assert(err, IsNil)
			break LOOP
		}
	}
}

func (s *testDDLSuite) TestSchemaResume(c *C) {
	store := createTestStore(c, "test_schema_resume")
	defer store.Close()

	lease := 50 * time.Millisecond

	d1 := newDDL(store, nil, nil, lease)
	defer d1.close()

	testCheckOwner(c, d1, true)

	schema := model.NewCIStr("test")
	schemaID, err := d1.meta.GenGlobalID()
	c.Assert(err, IsNil)

	job := &model.Job{
		SchemaID: schemaID,
		Type:     model.ActionCreateSchema,
		Args:     []interface{}{schema},
	}

	testRunInterruptedJob(c, d1, job)

	d1.meta.RunInNewTxn(false, func(t *meta.TMeta) error {
		var dbInfo *model.DBInfo
		dbInfo, err = t.GetDatabase(schemaID)
		c.Assert(err, IsNil)
		c.Assert(dbInfo.Name, DeepEquals, model.NewCIStr("test"))
		c.Assert(dbInfo.State, Equals, model.StatePublic)
		return nil
	})

	job = &model.Job{
		SchemaID: schemaID,
		Type:     model.ActionDropSchema,
	}

	testRunInterruptedJob(c, d1, job)

	d1.meta.RunInNewTxn(false, func(t *meta.TMeta) error {
		var dbInfo *model.DBInfo
		dbInfo, err = t.GetDatabase(schemaID)
		c.Assert(err, IsNil)
		c.Assert(dbInfo, IsNil)
		return nil
	})
}
