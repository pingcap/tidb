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
	"flag"
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/types"
)

var skipDDL = flag.Bool("skip_ddl", true, "only run simple DDL test")

func trySkipTest(c *C) {
	if *skipDDL {
		c.Skip("skip, only run simple tests")
	}
}

var _ = Suite(&testDDLSuite{})

func testCreateStore(c *C, name string) kv.Storage {
	driver := localstore.Driver{Driver: goleveldb.MemoryDriver{}}
	store, err := driver.Open(fmt.Sprintf("memory:%s", name))
	c.Assert(err, IsNil)
	return store
}

type testDDLSuite struct {
}

func testCheckOwner(c *C, d *ddl, isOwner bool, flag JobType) {
	err := kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		_, err := d.checkOwner(t, flag)
		return err
	})
	if isOwner {
		c.Assert(err, IsNil)
		return
	}

	c.Assert(terror.ErrorEqual(err, errNotOwner), IsTrue)
}

func (s *testDDLSuite) TestCheckOwner(c *C) {
	store := testCreateStore(c, "test_owner")
	defer store.Close()

	lease := 100 * time.Millisecond
	d1 := newDDL(store, nil, nil, lease)
	defer d1.close()

	time.Sleep(lease)

	testCheckOwner(c, d1, true, ddlJobFlag)
	testCheckOwner(c, d1, true, bgJobFlag)

	d2 := newDDL(store, nil, nil, lease)
	defer d2.close()

	testCheckOwner(c, d2, false, ddlJobFlag)
	testCheckOwner(c, d2, false, bgJobFlag)
	d1.close()

	time.Sleep(6 * lease)

	testCheckOwner(c, d2, true, ddlJobFlag)
	testCheckOwner(c, d2, true, bgJobFlag)

	d2.SetLease(1 * time.Second)

	err := d2.Stop()
	c.Assert(err, IsNil)

	err = d1.Start()
	c.Assert(err, IsNil)

	err = d1.Start()
	c.Assert(err, IsNil)

	testCheckOwner(c, d1, true, ddlJobFlag)
	testCheckOwner(c, d1, true, bgJobFlag)

	d2.SetLease(1 * time.Second)
	d2.SetLease(2 * time.Second)
	c.Assert(d2.GetLease(), Equals, 2*time.Second)
}

func (s *testDDLSuite) TestSchemaError(c *C) {
	store := testCreateStore(c, "test_schema_error")
	defer store.Close()

	lease := 50 * time.Millisecond

	d := newDDL(store, nil, nil, lease)
	defer d.close()

	job := &model.Job{
		SchemaID: 1,
		Type:     model.ActionCreateSchema,
		Args:     []interface{}{1},
	}

	ctx := testNewContext(c, d)

	err := d.startDDLJob(ctx, job)
	c.Assert(err, NotNil)
	testCheckJobCancelled(c, d, job)
}

func (s *testDDLSuite) TestTableError(c *C) {
	store := testCreateStore(c, "test_table_error")
	defer store.Close()

	lease := 50 * time.Millisecond

	d := newDDL(store, nil, nil, lease)
	defer d.close()

	job := &model.Job{
		SchemaID: 1,
		TableID:  1,
		Type:     model.ActionCreateTable,
		Args:     []interface{}{1},
	}

	ctx := testNewContext(c, d)

	err := d.startDDLJob(ctx, job)
	c.Assert(err, NotNil)
	testCheckJobCancelled(c, d, job)

	job.SchemaID = -1
	job.State = 0
	tblInfo := testTableInfo(c, d, "t", 3)
	job.Args = []interface{}{tblInfo}

	err = d.startDDLJob(ctx, job)
	c.Assert(err, NotNil)
	testCheckJobCancelled(c, d, job)

	job = &model.Job{
		SchemaID: 1,
		TableID:  1,
		Type:     model.ActionDropTable,
	}

	err = d.startDDLJob(ctx, job)
	c.Assert(err, NotNil)
	testCheckJobCancelled(c, d, job)

	dbInfo := testSchemaInfo(c, d, "test")
	testCreateSchema(c, testNewContext(c, d), d, dbInfo)

	job = &model.Job{
		SchemaID: dbInfo.ID,
		TableID:  -1,
		Type:     model.ActionDropTable,
	}

	err = d.startDDLJob(ctx, job)
	c.Assert(err, NotNil)
	testCheckJobCancelled(c, d, job)

	testCreateTable(c, ctx, d, dbInfo, tblInfo)

	err = kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		job.SchemaID = -1
		job.TableID = -1
		t := meta.NewMeta(txn)
		_, err1 := d.getTableInfo(t, job)
		c.Assert(err1, NotNil)
		job.SchemaID = dbInfo.ID
		_, err1 = d.getTableInfo(t, job)
		c.Assert(err1, NotNil)
		return nil
	})
	c.Assert(err, IsNil)
}

func (s *testDDLSuite) TestIndexError(c *C) {
	store := testCreateStore(c, "test_index_error")
	defer store.Close()

	lease := 50 * time.Millisecond

	d := newDDL(store, nil, nil, lease)
	defer d.close()

	ctx := testNewContext(c, d)

	job := &model.Job{
		SchemaID: -1,
		TableID:  1,
		Type:     model.ActionAddIndex,
	}

	err := d.startDDLJob(ctx, job)
	c.Assert(err, NotNil)
	testCheckJobCancelled(c, d, job)

	job = &model.Job{
		SchemaID: -1,
		TableID:  1,
		Type:     model.ActionDropIndex,
	}

	err = d.startDDLJob(ctx, job)
	c.Assert(err, NotNil)
	testCheckJobCancelled(c, d, job)

	dbInfo := testSchemaInfo(c, d, "test")
	tblInfo := testTableInfo(c, d, "t", 3)

	testCreateSchema(c, ctx, d, dbInfo)
	testCreateTable(c, ctx, d, dbInfo, tblInfo)

	job = &model.Job{
		SchemaID: dbInfo.ID,
		TableID:  tblInfo.ID,
		Type:     model.ActionAddIndex,
		Args:     []interface{}{1},
	}
	err = d.startDDLJob(ctx, job)
	c.Assert(err, NotNil)
	testCheckJobCancelled(c, d, job)

	job = &model.Job{
		SchemaID: dbInfo.ID,
		TableID:  tblInfo.ID,
		Type:     model.ActionAddIndex,
		Args:     []interface{}{false, model.NewCIStr("t"), []*ast.IndexColName{{Column: &ast.ColumnName{Name: model.NewCIStr("c")}, Length: 256}}},
	}
	err = d.startDDLJob(ctx, job)
	c.Assert(err, NotNil)
	testCheckJobCancelled(c, d, job)

	job = &model.Job{
		SchemaID: dbInfo.ID,
		TableID:  tblInfo.ID,
		Type:     model.ActionAddIndex,
		Args:     []interface{}{false, model.NewCIStr("c1_index"), []*ast.IndexColName{{Column: &ast.ColumnName{Name: model.NewCIStr("c")}, Length: 256}}},
	}
	err = d.startDDLJob(ctx, job)
	c.Assert(err, NotNil)
	testCheckJobCancelled(c, d, job)

	testCreateIndex(c, ctx, d, dbInfo, tblInfo, false, "c1_index", "c1")

	job = &model.Job{
		SchemaID: dbInfo.ID,
		TableID:  tblInfo.ID,
		Type:     model.ActionAddIndex,
		Args:     []interface{}{false, model.NewCIStr("c1_index"), []*ast.IndexColName{{Column: &ast.ColumnName{Name: model.NewCIStr("c1")}, Length: 256}}},
	}
	err = d.startDDLJob(ctx, job)
	c.Assert(err, NotNil)
	testCheckJobCancelled(c, d, job)

	job = &model.Job{
		SchemaID: dbInfo.ID,
		TableID:  tblInfo.ID,
		Type:     model.ActionDropIndex,
		Args:     []interface{}{1},
	}
	err = d.startDDLJob(ctx, job)
	c.Assert(err, NotNil)
	testCheckJobCancelled(c, d, job)

	testDropIndex(c, ctx, d, dbInfo, tblInfo, "c1_index")

	job = &model.Job{
		SchemaID: dbInfo.ID,
		TableID:  tblInfo.ID,
		Type:     model.ActionDropIndex,
		Args:     []interface{}{model.NewCIStr("c1_index")},
	}
	err = d.startDDLJob(ctx, job)
	c.Assert(err, NotNil)
	testCheckJobCancelled(c, d, job)
}

func (s *testDDLSuite) TestColumnError(c *C) {
	store := testCreateStore(c, "test_column_error")
	defer store.Close()

	lease := 50 * time.Millisecond

	d := newDDL(store, nil, nil, lease)
	defer d.close()

	ctx := testNewContext(c, d)

	job := &model.Job{
		SchemaID: -1,
		TableID:  1,
		Type:     model.ActionAddColumn,
	}

	err := d.startDDLJob(ctx, job)
	c.Assert(err, NotNil)
	testCheckJobCancelled(c, d, job)

	job = &model.Job{
		SchemaID: -1,
		TableID:  1,
		Type:     model.ActionDropColumn,
	}

	err = d.startDDLJob(ctx, job)
	c.Assert(err, NotNil)
	testCheckJobCancelled(c, d, job)

	dbInfo := testSchemaInfo(c, d, "test")
	tblInfo := testTableInfo(c, d, "t", 3)

	testCreateSchema(c, ctx, d, dbInfo)
	testCreateTable(c, ctx, d, dbInfo, tblInfo)

	col := &model.ColumnInfo{
		Name:         model.NewCIStr("c4"),
		Offset:       len(tblInfo.Columns),
		DefaultValue: 0,
	}

	col.ID, err = d.genGlobalID()
	c.Assert(err, IsNil)

	col.FieldType = *types.NewFieldType(mysql.TypeLong)
	pos := &ast.ColumnPosition{Tp: ast.ColumnPositionAfter, RelativeColumn: &ast.ColumnName{Name: model.NewCIStr("c5")}}

	job = &model.Job{
		SchemaID: dbInfo.ID,
		TableID:  tblInfo.ID,
		Type:     model.ActionAddColumn,
		Args:     []interface{}{col, pos, 0},
	}

	err = d.startDDLJob(ctx, job)
	c.Assert(err, NotNil)
	testCheckJobCancelled(c, d, job)

	job = &model.Job{
		SchemaID: -1,
		TableID:  1,
		Type:     model.ActionDropColumn,
		Args:     []interface{}{1},
	}

	err = d.startDDLJob(ctx, job)
	c.Assert(err, NotNil)
	testCheckJobCancelled(c, d, job)
}
