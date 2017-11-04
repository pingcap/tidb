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
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	goctx "golang.org/x/net/context"
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
		SchemaID:   dbInfo.ID,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{dbInfo},
	}
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)

	v := getSchemaVer(c, ctx)
	dbInfo.State = model.StatePublic
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, db: dbInfo})
	dbInfo.State = model.StateNone
	return job
}

func testDropSchema(c *C, ctx context.Context, d *ddl, dbInfo *model.DBInfo) (*model.Job, int64) {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		Type:       model.ActionDropSchema,
		BinlogInfo: &model.HistoryInfo{},
	}
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)

	ver := getSchemaVer(c, ctx)
	return job, ver
}

func isDDLJobDone(c *C, t *meta.Meta) bool {
	job, err := t.GetDDLJob(0)
	c.Assert(err, IsNil)
	if job == nil {
		return true
	}

	time.Sleep(testLease)
	return false
}

func testCheckSchemaState(c *C, d *ddl, dbInfo *model.DBInfo, state model.SchemaState) {
	isDropped := true

	for {
		kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
			t := meta.NewMeta(txn)
			info, err := t.GetDatabase(dbInfo.ID)
			c.Assert(err, IsNil)

			if state == model.StateNone {
				isDropped = isDDLJobDone(c, t)
				if !isDropped {
					return nil
				}
				c.Assert(info, IsNil)
				return nil
			}

			c.Assert(info.Name, DeepEquals, dbInfo.Name)
			c.Assert(info.State, Equals, state)
			return nil
		})

		if isDropped {
			break
		}
	}
}

func (s *testSchemaSuite) TestSchema(c *C) {
	defer testleak.AfterTest(c)()
	store := testCreateStore(c, "test_schema")
	defer store.Close()
	d := testNewDDL(goctx.Background(), nil, store, nil, nil, testLease)
	defer d.Stop()
	ctx := testNewContext(d)
	dbInfo := testSchemaInfo(c, d, "test")

	// create a database.
	job := testCreateSchema(c, ctx, d, dbInfo)
	testCheckSchemaState(c, d, dbInfo, model.StatePublic)
	testCheckJobDone(c, d, job, true)

	/*** to drop the schema with two tables. ***/
	// create table t with 100 records.
	tblInfo1 := testTableInfo(c, d, "t", 3)
	tJob1 := testCreateTable(c, ctx, d, dbInfo, tblInfo1)
	testCheckTableState(c, d, dbInfo, tblInfo1, model.StatePublic)
	testCheckJobDone(c, d, tJob1, true)
	tbl1 := testGetTable(c, d, dbInfo.ID, tblInfo1.ID)
	for i := 1; i <= 100; i++ {
		_, err := tbl1.AddRecord(ctx, types.MakeDatums(i, i, i))
		c.Assert(err, IsNil)
	}
	// create table t1 with defaultBatchCnt+10 records.
	tblInfo2 := testTableInfo(c, d, "t1", 3)
	tJob2 := testCreateTable(c, ctx, d, dbInfo, tblInfo2)
	testCheckTableState(c, d, dbInfo, tblInfo2, model.StatePublic)
	testCheckJobDone(c, d, tJob2, true)
	tbl2 := testGetTable(c, d, dbInfo.ID, tblInfo2.ID)
	for i := 1; i <= defaultBatchCnt+10; i++ {
		_, err := tbl2.AddRecord(ctx, types.MakeDatums(i, i, i))
		c.Assert(err, IsNil)
	}
	job, v := testDropSchema(c, ctx, d, dbInfo)
	testCheckSchemaState(c, d, dbInfo, model.StateNone)
	ids := make(map[int64]struct{})
	ids[tblInfo1.ID] = struct{}{}
	ids[tblInfo2.ID] = struct{}{}
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, db: dbInfo, tblIDs: ids})

	// Drop a non-existent database.
	job = &model.Job{
		SchemaID:   dbInfo.ID,
		Type:       model.ActionDropSchema,
		BinlogInfo: &model.HistoryInfo{},
	}
	err := d.doDDLJob(ctx, job)
	c.Assert(terror.ErrorEqual(err, infoschema.ErrDatabaseDropExists), IsTrue)

	// Drop a database without a table.
	dbInfo1 := testSchemaInfo(c, d, "test1")
	job = testCreateSchema(c, ctx, d, dbInfo1)
	testCheckSchemaState(c, d, dbInfo1, model.StatePublic)
	testCheckJobDone(c, d, job, true)
	job, _ = testDropSchema(c, ctx, d, dbInfo1)
	testCheckSchemaState(c, d, dbInfo1, model.StateNone)
	testCheckJobDone(c, d, job, false)
}

func (s *testSchemaSuite) TestSchemaWaitJob(c *C) {
	defer testleak.AfterTest(c)()
	store := testCreateStore(c, "test_schema_wait")
	defer store.Close()

	d1 := testNewDDL(goctx.Background(), nil, store, nil, nil, testLease)
	defer d1.Stop()

	testCheckOwner(c, d1, true)

	d2 := testNewDDL(goctx.Background(), nil, store, nil, nil, testLease*4)
	defer d2.Stop()
	ctx := testNewContext(d2)

	// d2 must not be owner.
	d2.ownerManager.SetOwner(false)

	dbInfo := testSchemaInfo(c, d2, "test")
	testCreateSchema(c, ctx, d2, dbInfo)
	testCheckSchemaState(c, d2, dbInfo, model.StatePublic)

	// d2 must not be owner.
	c.Assert(d2.ownerManager.IsOwner(), IsFalse)

	schemaID, err := d2.genGlobalID()
	c.Assert(err, IsNil)
	doDDLJobErr(c, schemaID, 0, model.ActionCreateSchema, []interface{}{dbInfo}, ctx, d2)
}

func testRunInterruptedJob(c *C, d *ddl, job *model.Job) {
	ctx := mock.NewContext()
	ctx.Store = d.store
	done := make(chan error, 1)
	go func() {
		done <- d.doDDLJob(ctx, job)
	}()

	ticker := time.NewTicker(d.lease * 1)
	defer ticker.Stop()

LOOP:
	for {
		select {
		case <-ticker.C:
			d.Stop()
			d.start(goctx.Background())
		case err := <-done:
			c.Assert(err, IsNil)
			break LOOP
		}
	}
}

func (s *testSchemaSuite) TestSchemaResume(c *C) {
	defer testleak.AfterTest(c)()
	store := testCreateStore(c, "test_schema_resume")
	defer store.Close()

	d1 := testNewDDL(goctx.Background(), nil, store, nil, nil, testLease)
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
