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

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testleak"
	goctx "golang.org/x/net/context"
)

var _ = Suite(&testTableSuite{})

type testTableSuite struct {
	store  kv.Storage
	dbInfo *model.DBInfo

	d *ddl
}

// testTableInfo creates a test table with num int columns and with no index.
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
		col.ID = allocateColumnID(tblInfo)
		cols[i] = col
	}

	tblInfo.Columns = cols

	return tblInfo
}

func testCreateTable(c *C, ctx context.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tblInfo},
	}
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)

	v := getSchemaVer(c, ctx)
	tblInfo.State = model.StatePublic
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	tblInfo.State = model.StateNone
	return job
}

func testRenameTable(c *C, ctx context.Context, d *ddl, newSchemaID, oldSchemaID int64, tblInfo *model.TableInfo) *model.Job {
	job := &model.Job{
		SchemaID:   newSchemaID,
		TableID:    tblInfo.ID,
		Type:       model.ActionRenameTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{oldSchemaID, tblInfo.Name},
	}
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)

	v := getSchemaVer(c, ctx)
	tblInfo.State = model.StatePublic
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	tblInfo.State = model.StateNone
	return job
}

func testDropTable(c *C, ctx context.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionDropTable,
		BinlogInfo: &model.HistoryInfo{},
	}
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)

	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func testTruncateTable(c *C, ctx context.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo) *model.Job {
	newTableID, err := d.genGlobalID()
	c.Assert(err, IsNil)
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionTruncateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{newTableID},
	}
	err = d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)

	v := getSchemaVer(c, ctx)
	tblInfo.ID = newTableID
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
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
	tbl, err := testGetTableWithError(d, schemaID, tableID)
	c.Assert(err, IsNil)
	return tbl
}

func testGetTableWithError(d *ddl, schemaID, tableID int64) (table.Table, error) {
	var tblInfo *model.TableInfo
	err := kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err1 error
		tblInfo, err1 = t.GetTable(schemaID, tableID)
		if err1 != nil {
			return errors.Trace(err1)
		}
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	if tblInfo == nil {
		return nil, errors.New("table not found")
	}
	alloc := autoid.NewAllocator(d.store, schemaID)
	tbl, err := table.TableFromMeta(alloc, tblInfo)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return tbl, nil
}

func (s *testTableSuite) SetUpSuite(c *C) {
	s.store = testCreateStore(c, "test_table")
	s.d = testNewDDL(goctx.Background(), nil, s.store, nil, nil, testLease)

	s.dbInfo = testSchemaInfo(c, s.d, "test")
	testCreateSchema(c, testNewContext(s.d), s.d, s.dbInfo)
}

func (s *testTableSuite) TearDownSuite(c *C) {
	testDropSchema(c, testNewContext(s.d), s.d, s.dbInfo)
	s.d.Stop()
	s.store.Close()
}

func (s *testTableSuite) TestTable(c *C) {
	defer testleak.AfterTest(c)()
	d := s.d

	ctx := testNewContext(d)

	tblInfo := testTableInfo(c, d, "t", 3)
	job := testCreateTable(c, ctx, d, s.dbInfo, tblInfo)
	testCheckTableState(c, d, s.dbInfo, tblInfo, model.StatePublic)
	testCheckJobDone(c, d, job, true)

	// Create an existing table.
	newTblInfo := testTableInfo(c, d, "t", 3)
	doDDLJobErr(c, s.dbInfo.ID, newTblInfo.ID, model.ActionCreateTable, []interface{}{newTblInfo}, ctx, d)

	count := 2000
	tbl := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)
	for i := 1; i <= count; i++ {
		_, err := tbl.AddRecord(ctx, types.MakeDatums(i, i, i), false)
		c.Assert(err, IsNil)
	}

	job = testDropTable(c, ctx, d, s.dbInfo, tblInfo)
	testCheckJobDone(c, d, job, false)

	// for truncate table
	tblInfo = testTableInfo(c, d, "tt", 3)
	job = testCreateTable(c, ctx, d, s.dbInfo, tblInfo)
	testCheckTableState(c, d, s.dbInfo, tblInfo, model.StatePublic)
	testCheckJobDone(c, d, job, true)
	job = testTruncateTable(c, ctx, d, s.dbInfo, tblInfo)
	testCheckTableState(c, d, s.dbInfo, tblInfo, model.StatePublic)
	testCheckJobDone(c, d, job, true)

	// for rename table
	dbInfo1 := testSchemaInfo(c, s.d, "test_rename_table")
	testCreateSchema(c, testNewContext(s.d), s.d, dbInfo1)
	job = testRenameTable(c, ctx, d, dbInfo1.ID, s.dbInfo.ID, tblInfo)
	testCheckTableState(c, d, dbInfo1, tblInfo, model.StatePublic)
	testCheckJobDone(c, d, job, true)
}

func (s *testTableSuite) TestTableResume(c *C) {
	defer testleak.AfterTest(c)()
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
