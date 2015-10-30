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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/errors2"
	"github.com/pingcap/tidb/util/mock"
)

var _ = Suite(&testSchemaSuite{})

type testSchemaSuite struct{}

func testSchemaInfo(c *C, d *ddl, name string) *model.DBInfo {
	var err error
	dbInfo := &model.DBInfo{
		Name: model.NewCIStr(name),
	}

	dbInfo.ID, err = d.genGlobalID()
	c.Assert(err, IsNil)
	return dbInfo
}

func testCreateSchema(c *C, ctx context.Context, d *ddl, dbInfo *model.DBInfo) *model.Job {
	job := &model.Job{
		SchemaID: dbInfo.ID,
		Type:     model.ActionCreateSchema,
		Args:     []interface{}{dbInfo.Name},
	}

	err := d.startJob(ctx, job)
	c.Assert(err, IsNil)
	return job
}

func testDropSchema(c *C, ctx context.Context, d *ddl, dbInfo *model.DBInfo) *model.Job {
	job := &model.Job{
		SchemaID: dbInfo.ID,
		Type:     model.ActionDropSchema,
	}

	err := d.startJob(ctx, job)
	c.Assert(err, IsNil)
	return job
}

func testCheckSchemaState(c *C, d *ddl, dbInfo *model.DBInfo, state model.SchemaState) {
	kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		info, err := t.GetDatabase(dbInfo.ID)
		c.Assert(err, IsNil)

		if state == model.StateNone {
			c.Assert(info, IsNil)
			return nil
		}

		c.Assert(info.Name, DeepEquals, dbInfo.Name)
		c.Assert(info.State, Equals, state)
		return nil
	})
}

func testCheckJobDone(c *C, d *ddl, job *model.Job, isAdd bool) {
	kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		historyJob, err := t.GetHistoryDDLJob(job.ID)
		c.Assert(err, IsNil)
		c.Assert(historyJob.State, Equals, model.JobDone)
		if isAdd {
			c.Assert(historyJob.SchemaState, Equals, model.StatePublic)
		} else {
			c.Assert(historyJob.SchemaState, Equals, model.StateNone)
		}

		return nil
	})
}

func testCheckJobCancelled(c *C, d *ddl, job *model.Job) {
	kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		historyJob, err := t.GetHistoryDDLJob(job.ID)
		c.Assert(err, IsNil)
		c.Assert(historyJob.State, Equals, model.JobCancelled)
		return nil
	})
}

func (s *testSchemaSuite) TestSchema(c *C) {
	store := testCreateStore(c, "test_schema")
	defer store.Close()

	lease := 100 * time.Millisecond

	d1 := newDDL(store, nil, nil, lease)
	defer d1.close()

	ctx := mock.NewContext()

	dbInfo := testSchemaInfo(c, d1, "test")
	job := testCreateSchema(c, ctx, d1, dbInfo)

	testCheckSchemaState(c, d1, dbInfo, model.StatePublic)
	testCheckJobDone(c, d1, job, true)

	job = testDropSchema(c, ctx, d1, dbInfo)
	testCheckSchemaState(c, d1, dbInfo, model.StateNone)
	testCheckJobDone(c, d1, job, false)

	job = &model.Job{
		SchemaID: dbInfo.ID,
		Type:     model.ActionDropSchema,
	}

	err := d1.startJob(ctx, job)
	c.Assert(errors2.ErrorEqual(err, ErrNotExists), IsTrue)
}

func (s *testSchemaSuite) TestSchemaWaitJob(c *C) {
	store := testCreateStore(c, "test_schema_wait")
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

	dbInfo := testSchemaInfo(c, d2, "test")
	job := testCreateSchema(c, ctx, d2, dbInfo)
	testCheckSchemaState(c, d2, dbInfo, model.StatePublic)

	// d2 must not be owner.
	testCheckOwner(c, d2, false)

	schemaID, err := d2.genGlobalID()
	c.Assert(err, IsNil)

	job = &model.Job{
		SchemaID: schemaID,
		Type:     model.ActionCreateSchema,
		Args:     []interface{}{dbInfo.Name},
	}

	err = d2.startJob(ctx, job)
	c.Assert(err, NotNil)
	testCheckJobCancelled(c, d2, job)

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
		case err := <-done:
			c.Assert(err, IsNil)
			break LOOP
		}
	}
}

func (s *testSchemaSuite) TestSchemaResume(c *C) {
	store := testCreateStore(c, "test_schema_resume")
	defer store.Close()

	lease := 50 * time.Millisecond

	d1 := newDDL(store, nil, nil, lease)
	defer d1.close()

	testCheckOwner(c, d1, true)

	dbInfo := testSchemaInfo(c, d1, "test")

	job := &model.Job{
		SchemaID: dbInfo.ID,
		Type:     model.ActionCreateSchema,
		Args:     []interface{}{dbInfo.Name},
	}

	testRunInterruptedJob(c, d1, job)
	testCheckSchemaState(c, d1, dbInfo, model.StatePublic)

	job = &model.Job{
		SchemaID: dbInfo.ID,
		Type:     model.ActionDropSchema,
	}

	testRunInterruptedJob(c, d1, job)
	testCheckSchemaState(c, d1, dbInfo, model.StateNone)
}
