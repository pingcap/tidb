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
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testTableSuite{})

type testTableSuite struct {
	store  kv.Storage
	dbInfo *model.DBInfo

	d *ddl
}

// create a test table with num int columns and with no index.
func testTableInfo(c *C, d *ddl, name string, num int) *model.TableInfo {
	var err error
	tblInfo := &model.TableInfo{
		Name: model.NewCIStr(name),
	}
	tblInfo.ID, err = d.genGlobalID()
	c.Assert(err, IsNil)

	cols := make([]*model.ColumnInfo, num)
	for i := range cols {
		col := &model.ColumnInfo{
			Name:         model.NewCIStr(fmt.Sprintf("c%d", i+1)),
			Offset:       i,
			DefaultValue: i + 1,
			State:        model.StatePublic,
		}

		col.FieldType = *types.NewFieldType(mysql.TypeLong)

		col.ID, err = d.genGlobalID()
		c.Assert(err, IsNil)
		cols[i] = col
	}

	tblInfo.Columns = cols

	return tblInfo
}

func testCreateTable(c *C, ctx context.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo) *model.Job {
	job := &model.Job{
		SchemaID: dbInfo.ID,
		TableID:  tblInfo.ID,
		Type:     model.ActionCreateTable,
		Args:     []interface{}{tblInfo},
	}

	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)
	return job
}

func testDropTable(c *C, ctx context.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo) *model.Job {
	job := &model.Job{
		SchemaID: dbInfo.ID,
		TableID:  tblInfo.ID,
		Type:     model.ActionDropTable,
	}

	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)
	return job
}

func testCheckTableState(c *C, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, state model.SchemaState) {
	kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		info, err := t.GetTable(dbInfo.ID, tblInfo.ID)
		c.Assert(err, IsNil)

		if state == model.StateNone {
			c.Assert(info, IsNil)
			return nil
		}

		c.Assert(info.Name, DeepEquals, tblInfo.Name)
		c.Assert(info.State, Equals, state)
		return nil
	})
}

func testGetTable(c *C, d *ddl, schemaID int64, tableID int64) table.Table {
	var tblInfo *model.TableInfo
	kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err error
		tblInfo, err = t.GetTable(schemaID, tableID)
		c.Assert(err, IsNil)
		c.Assert(tblInfo, NotNil)
		return nil
	})
	alloc := autoid.NewAllocator(d.store, schemaID)
	tbl, err := table.TableFromMeta(alloc, tblInfo)
	c.Assert(err, IsNil)
	return tbl
}

func (s *testTableSuite) SetUpSuite(c *C) {
	s.store = testCreateStore(c, "test_table")
	lease := 50 * time.Millisecond
	s.d = newDDL(s.store, nil, nil, lease)

	s.dbInfo = testSchemaInfo(c, s.d, "test")
	testCreateSchema(c, mock.NewContext(), s.d, s.dbInfo)
}

func (s *testTableSuite) TearDownSuite(c *C) {
	testDropSchema(c, mock.NewContext(), s.d, s.dbInfo)
	s.d.close()
	s.store.Close()
}

func testNewContext(c *C, d *ddl) context.Context {
	ctx := d.newReorgContext()
	variable.BindSessionVars(ctx)
	return ctx
}

func (s *testTableSuite) TestTable(c *C) {
	defer testleak.AfterTest(c)()
	d := s.d

	ctx := testNewContext(c, d)
	defer ctx.RollbackTxn()

	tblInfo := testTableInfo(c, d, "t", 3)

	job := testCreateTable(c, ctx, d, s.dbInfo, tblInfo)
	testCheckTableState(c, d, s.dbInfo, tblInfo, model.StatePublic)
	testCheckJobDone(c, d, job, true)

	newTblInfo := testTableInfo(c, d, "t", 3)
	job = &model.Job{
		SchemaID: s.dbInfo.ID,
		TableID:  newTblInfo.ID,
		Type:     model.ActionCreateTable,
		Args:     []interface{}{newTblInfo},
	}

	err := d.doDDLJob(ctx, job)
	c.Assert(err, NotNil)
	testCheckJobCancelled(c, d, job)

	tbl := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)

	_, err = tbl.AddRecord(ctx, types.MakeDatums(1, 1, 1))
	c.Assert(err, IsNil)

	_, err = tbl.AddRecord(ctx, types.MakeDatums(2, 2, 2))
	c.Assert(err, IsNil)

	job = testDropTable(c, ctx, d, s.dbInfo, tblInfo)
	testCheckJobDone(c, d, job, false)
}

func (s *testTableSuite) TestTableResume(c *C) {
	defer testleak.AfterTest(c)()
	d := s.d

	testCheckOwner(c, d, true, ddlJobFlag)

	tblInfo := testTableInfo(c, d, "t1", 3)

	job := &model.Job{
		SchemaID: s.dbInfo.ID,
		TableID:  tblInfo.ID,
		Type:     model.ActionCreateTable,
		Args:     []interface{}{tblInfo},
	}

	testRunInterruptedJob(c, d, job)
	testCheckTableState(c, d, s.dbInfo, tblInfo, model.StatePublic)

	job = &model.Job{
		SchemaID: s.dbInfo.ID,
		TableID:  tblInfo.ID,
		Type:     model.ActionDropTable,
	}

	testRunInterruptedJob(c, d, job)
	testCheckTableState(c, d, s.dbInfo, tblInfo, model.StateNone)
}
