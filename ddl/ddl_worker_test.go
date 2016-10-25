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
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/store/localstore/goleveldb"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testDDLSuite{})

const testLease = 5 * time.Millisecond

func testCreateStore(c *C, name string) kv.Storage {
	driver := localstore.Driver{Driver: goleveldb.MemoryDriver{}}
	store, err := driver.Open(fmt.Sprintf("memory://%s", name))
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
	defer testleak.AfterTest(c)()
	store := testCreateStore(c, "test_owner")
	defer store.Close()

	d1 := newDDL(store, nil, nil, testLease)
	defer d1.close()

	time.Sleep(testLease)

	testCheckOwner(c, d1, true, ddlJobFlag)
	testCheckOwner(c, d1, true, bgJobFlag)

	d2 := newDDL(store, nil, nil, testLease)
	defer d2.close()

	testCheckOwner(c, d2, false, ddlJobFlag)
	testCheckOwner(c, d2, false, bgJobFlag)
	d1.close()

	// Make sure owner is changed.
	time.Sleep(21 * testLease)

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
	defer testleak.AfterTest(c)()
	store := testCreateStore(c, "test_schema_error")
	defer store.Close()

	d := newDDL(store, nil, nil, testLease)
	defer d.close()
	ctx := testNewContext(c, d)

	doDDLJobErr(c, 1, 0, model.ActionCreateSchema, []interface{}{1}, ctx, d)
}

func (s *testDDLSuite) TestTableError(c *C) {
	defer testleak.AfterTest(c)()
	store := testCreateStore(c, "test_table_error")
	defer store.Close()

	d := newDDL(store, nil, nil, testLease)
	defer d.close()

	ctx := testNewContext(c, d)

	job := doDDLJobErr(c, 1, 1, model.ActionCreateTable, []interface{}{1}, ctx, d)

	job.SchemaID = -1
	job.State = 0
	tblInfo := testTableInfo(c, d, "t", 3)
	job.Args = []interface{}{tblInfo}

	err := d.doDDLJob(ctx, job)
	c.Assert(err, NotNil)
	testCheckJobCancelled(c, d, job)

	doDDLJobErr(c, 1, 1, model.ActionDropTable, nil, ctx, d)

	dbInfo := testSchemaInfo(c, d, "test")
	testCreateSchema(c, testNewContext(c, d), d, dbInfo)

	job = doDDLJobErr(c, dbInfo.ID, -1, model.ActionDropTable, nil, ctx, d)

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
	defer testleak.AfterTest(c)()
	store := testCreateStore(c, "test_index_error")
	defer store.Close()

	d := newDDL(store, nil, nil, testLease)
	defer d.close()

	ctx := testNewContext(c, d)

	doDDLJobErr(c, -1, 1, model.ActionAddIndex, nil, ctx, d)
	doDDLJobErr(c, -1, 1, model.ActionDropIndex, nil, ctx, d)

	dbInfo := testSchemaInfo(c, d, "test")
	tblInfo := testTableInfo(c, d, "t", 3)

	testCreateSchema(c, ctx, d, dbInfo)
	testCreateTable(c, ctx, d, dbInfo, tblInfo)

	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex, []interface{}{1}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex,
		[]interface{}{false, model.NewCIStr("t"), []*ast.IndexColName{{Column: &ast.ColumnName{Name: model.NewCIStr("c")}, Length: 256}}}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex,
		[]interface{}{false, model.NewCIStr("c1_index"), []*ast.IndexColName{{Column: &ast.ColumnName{Name: model.NewCIStr("c")}, Length: 256}}}, ctx, d)

	testCreateIndex(c, ctx, d, dbInfo, tblInfo, false, "c1_index", "c1")

	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionAddIndex,
		[]interface{}{false, model.NewCIStr("c1_index"), []*ast.IndexColName{{Column: &ast.ColumnName{Name: model.NewCIStr("c1")}, Length: 256}}}, ctx, d)
	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionDropIndex, []interface{}{1}, ctx, d)

	testDropIndex(c, ctx, d, dbInfo, tblInfo, "c1_index")

	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionDropIndex, []interface{}{model.NewCIStr("c1_index")}, ctx, d)
}

func (s *testDDLSuite) TestColumnError(c *C) {
	defer testleak.AfterTest(c)()
	store := testCreateStore(c, "test_column_error")
	defer store.Close()

	d := newDDL(store, nil, nil, testLease)
	defer d.close()

	ctx := testNewContext(c, d)

	doDDLJobErr(c, -1, 1, model.ActionAddColumn, nil, ctx, d)
	doDDLJobErr(c, -1, 1, model.ActionDropColumn, nil, ctx, d)

	dbInfo := testSchemaInfo(c, d, "test")
	tblInfo := testTableInfo(c, d, "t", 3)

	testCreateSchema(c, ctx, d, dbInfo)
	testCreateTable(c, ctx, d, dbInfo, tblInfo)

	col := &model.ColumnInfo{
		Name:         model.NewCIStr("c4"),
		Offset:       len(tblInfo.Columns),
		DefaultValue: 0,
	}

	var err error
	col.ID, err = d.genGlobalID()
	c.Assert(err, IsNil)

	col.FieldType = *types.NewFieldType(mysql.TypeLong)
	pos := &ast.ColumnPosition{Tp: ast.ColumnPositionAfter, RelativeColumn: &ast.ColumnName{Name: model.NewCIStr("c5")}}

	doDDLJobErr(c, dbInfo.ID, tblInfo.ID, model.ActionAddColumn, []interface{}{col, pos, 0}, ctx, d)
	doDDLJobErr(c, -1, 1, model.ActionDropColumn, []interface{}{1}, ctx, d)
}

func doDDLJobErr(c *C, schemaID, tableID int64, tp model.ActionType, args []interface{},
	ctx context.Context, d *ddl) *model.Job {
	job := &model.Job{
		SchemaID: schemaID,
		TableID:  tableID,
		Type:     tp,
		Args:     args,
	}
	err := d.doDDLJob(ctx, job)
	c.Assert(err, NotNil)
	testCheckJobCancelled(c, d, job)

	return job
}
