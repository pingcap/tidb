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
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testColumnSuite{})

type testColumnSuite struct {
	store  kv.Storage
	dbInfo *model.DBInfo

	d *ddl
}

func (s *testColumnSuite) SetUpSuite(c *C) {
	trySkipTest(c)

	s.store = testCreateStore(c, "test_column")
	lease := 50 * time.Millisecond
	s.d = newDDL(s.store, nil, nil, lease)

	s.dbInfo = testSchemaInfo(c, s.d, "test_column")
	testCreateSchema(c, mock.NewContext(), s.d, s.dbInfo)
}

func (s *testColumnSuite) TearDownSuite(c *C) {
	trySkipTest(c)

	testDropSchema(c, mock.NewContext(), s.d, s.dbInfo)
	s.d.close()

	err := s.store.Close()
	c.Assert(err, IsNil)
}

func testCreateColumn(c *C, ctx context.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo,
	colName string, pos *ast.ColumnPosition, defaultValue interface{}) *model.Job {
	col := &model.ColumnInfo{
		Name:         model.NewCIStr(colName),
		Offset:       len(tblInfo.Columns),
		DefaultValue: defaultValue,
	}

	var err error
	col.ID, err = d.genGlobalID()
	c.Assert(err, IsNil)

	col.FieldType = *types.NewFieldType(mysql.TypeLong)

	job := &model.Job{
		SchemaID: dbInfo.ID,
		TableID:  tblInfo.ID,
		Type:     model.ActionAddColumn,
		Args:     []interface{}{col, pos, 0},
	}

	err = d.startDDLJob(ctx, job)
	c.Assert(err, IsNil)
	return job
}

func testDropColumn(c *C, ctx context.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, colName string, isError bool) *model.Job {
	job := &model.Job{
		SchemaID: dbInfo.ID,
		TableID:  tblInfo.ID,
		Type:     model.ActionDropColumn,
		Args:     []interface{}{model.NewCIStr(colName)},
	}

	err := d.startDDLJob(ctx, job)
	if isError {
		c.Assert(err, NotNil)
		return nil
	}

	c.Assert(err, IsNil)
	return job
}

func (s *testColumnSuite) TestColumn(c *C) {
	tblInfo := testTableInfo(c, s.d, "t1", 3)
	ctx := testNewContext(c, s.d)
	defer ctx.FinishTxn(true)

	testCreateTable(c, ctx, s.d, s.dbInfo, tblInfo)

	t := testGetTable(c, s.d, s.dbInfo.ID, tblInfo.ID)

	num := 10
	for i := 0; i < num; i++ {
		_, err := t.AddRecord(ctx, types.MakeDatums(i, 10*i, 100*i))
		c.Assert(err, IsNil)
	}

	err := ctx.FinishTxn(false)
	c.Assert(err, IsNil)

	i := int64(0)
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*column.Col) (bool, error) {
		c.Assert(data, HasLen, 3)
		c.Assert(data[0].GetInt64(), Equals, i)
		c.Assert(data[1].GetInt64(), Equals, 10*i)
		c.Assert(data[2].GetInt64(), Equals, 100*i)
		i++
		return true, nil
	})
	c.Assert(i, Equals, int64(num))

	c.Assert(column.FindCol(t.Cols(), "c4"), IsNil)

	job := testCreateColumn(c, ctx, s.d, s.dbInfo, tblInfo, "c4", &ast.ColumnPosition{Tp: ast.ColumnPositionAfter, RelativeColumn: &ast.ColumnName{Name: model.NewCIStr("c3")}}, 100)
	testCheckJobDone(c, s.d, job, true)

	t = testGetTable(c, s.d, s.dbInfo.ID, tblInfo.ID)
	c.Assert(column.FindCol(t.Cols(), "c4"), NotNil)

	i = int64(0)
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*column.Col) (bool, error) {
		c.Assert(data, HasLen, 4)
		c.Assert(data[0].GetInt64(), Equals, i)
		c.Assert(data[1].GetInt64(), Equals, 10*i)
		c.Assert(data[2].GetInt64(), Equals, 100*i)
		c.Assert(data[3].GetInt64(), Equals, int64(100))
		i++
		return true, nil
	})
	c.Assert(i, Equals, int64(num))

	h, err := t.AddRecord(ctx, types.MakeDatums(11, 12, 13, 14))
	c.Assert(err, IsNil)
	err = ctx.FinishTxn(false)
	c.Assert(err, IsNil)
	values, err := t.RowWithCols(ctx, h, t.Cols())
	c.Assert(err, IsNil)

	c.Assert(values, HasLen, 4)
	c.Assert(values[3].GetInt64(), Equals, int64(14))

	job = testDropColumn(c, ctx, s.d, s.dbInfo, tblInfo, "c4", false)
	testCheckJobDone(c, s.d, job, false)

	t = testGetTable(c, s.d, s.dbInfo.ID, tblInfo.ID)
	values, err = t.RowWithCols(ctx, h, t.Cols())
	c.Assert(err, IsNil)

	c.Assert(values, HasLen, 3)
	c.Assert(values[2].GetInt64(), Equals, int64(13))

	job = testCreateColumn(c, ctx, s.d, s.dbInfo, tblInfo, "c4", &ast.ColumnPosition{Tp: ast.ColumnPositionNone}, 111)
	testCheckJobDone(c, s.d, job, true)

	t = testGetTable(c, s.d, s.dbInfo.ID, tblInfo.ID)
	values, err = t.RowWithCols(ctx, h, t.Cols())
	c.Assert(err, IsNil)

	c.Assert(values, HasLen, 4)
	c.Assert(values[3].GetInt64(), Equals, int64(111))

	job = testCreateColumn(c, ctx, s.d, s.dbInfo, tblInfo, "c5", &ast.ColumnPosition{Tp: ast.ColumnPositionNone}, 101)
	testCheckJobDone(c, s.d, job, true)

	t = testGetTable(c, s.d, s.dbInfo.ID, tblInfo.ID)
	values, err = t.RowWithCols(ctx, h, t.Cols())
	c.Assert(err, IsNil)

	c.Assert(values, HasLen, 5)
	c.Assert(values[4].GetInt64(), Equals, int64(101))

	job = testCreateColumn(c, ctx, s.d, s.dbInfo, tblInfo, "c6", &ast.ColumnPosition{Tp: ast.ColumnPositionFirst}, 202)
	testCheckJobDone(c, s.d, job, true)

	t = testGetTable(c, s.d, s.dbInfo.ID, tblInfo.ID)
	cols := t.Cols()
	c.Assert(cols, HasLen, 6)
	c.Assert(cols[0].Offset, Equals, 0)
	c.Assert(cols[0].Name.L, Equals, "c6")
	c.Assert(cols[1].Offset, Equals, 1)
	c.Assert(cols[1].Name.L, Equals, "c1")
	c.Assert(cols[2].Offset, Equals, 2)
	c.Assert(cols[2].Name.L, Equals, "c2")
	c.Assert(cols[3].Offset, Equals, 3)
	c.Assert(cols[3].Name.L, Equals, "c3")
	c.Assert(cols[4].Offset, Equals, 4)
	c.Assert(cols[4].Name.L, Equals, "c4")
	c.Assert(cols[5].Offset, Equals, 5)
	c.Assert(cols[5].Name.L, Equals, "c5")

	values, err = t.RowWithCols(ctx, h, cols)
	c.Assert(err, IsNil)

	c.Assert(values, HasLen, 6)
	c.Assert(values[0].GetInt64(), Equals, int64(202))
	c.Assert(values[5].GetInt64(), Equals, int64(101))

	job = testDropColumn(c, ctx, s.d, s.dbInfo, tblInfo, "c2", false)
	testCheckJobDone(c, s.d, job, false)

	t = testGetTable(c, s.d, s.dbInfo.ID, tblInfo.ID)

	values, err = t.RowWithCols(ctx, h, t.Cols())
	c.Assert(err, IsNil)

	c.Assert(values, HasLen, 5)
	c.Assert(values[0].GetInt64(), Equals, int64(202))
	c.Assert(values[4].GetInt64(), Equals, int64(101))

	job = testDropColumn(c, ctx, s.d, s.dbInfo, tblInfo, "c1", false)
	testCheckJobDone(c, s.d, job, false)

	job = testDropColumn(c, ctx, s.d, s.dbInfo, tblInfo, "c3", false)
	testCheckJobDone(c, s.d, job, false)

	job = testDropColumn(c, ctx, s.d, s.dbInfo, tblInfo, "c4", false)
	testCheckJobDone(c, s.d, job, false)

	job = testCreateIndex(c, ctx, s.d, s.dbInfo, tblInfo, false, "c5_idx", "c5")
	testCheckJobDone(c, s.d, job, true)

	testDropColumn(c, ctx, s.d, s.dbInfo, tblInfo, "c5", true)

	testDropIndex(c, ctx, s.d, s.dbInfo, tblInfo, "c5_idx")
	testCheckJobDone(c, s.d, job, true)

	job = testDropColumn(c, ctx, s.d, s.dbInfo, tblInfo, "c5", false)
	testCheckJobDone(c, s.d, job, false)

	testDropColumn(c, ctx, s.d, s.dbInfo, tblInfo, "c6", true)

	testDropTable(c, ctx, s.d, s.dbInfo, tblInfo)
}

func (s *testColumnSuite) checkColumnKVExist(c *C, ctx context.Context, t table.Table, handle int64, col *column.Col, columnValue interface{}, isExist bool) {
	txn, err := ctx.GetTxn(true)
	c.Assert(err, IsNil)

	key := t.RecordKey(handle, col)
	data, err := txn.Get(key)

	if isExist {
		c.Assert(err, IsNil)
		v, err1 := tables.DecodeValue(data, &col.FieldType)
		c.Assert(err1, IsNil)
		value, err1 := v.ConvertTo(&col.FieldType)
		c.Assert(err1, IsNil)
		c.Assert(value.GetValue(), Equals, columnValue)
	} else {
		c.Assert(err, NotNil)
	}

	err = ctx.FinishTxn(false)
	c.Assert(err, IsNil)
}

func (s *testColumnSuite) checkNoneColumn(c *C, ctx context.Context, d *ddl, tblInfo *model.TableInfo, handle int64, col *column.Col, columnValue interface{}) {
	t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)
	s.checkColumnKVExist(c, ctx, t, handle, col, columnValue, false)
	s.testGetColumn(c, t, col.Name.L, false)
}

func (s *testColumnSuite) checkDeleteOnlyColumn(c *C, ctx context.Context, d *ddl, tblInfo *model.TableInfo, handle int64, col *column.Col, row []types.Datum, columnValue interface{}, isDropped bool) {
	t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)

	_, err := ctx.GetTxn(true)
	c.Assert(err, IsNil)

	i := int64(0)
	err = t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*column.Col) (bool, error) {
		c.Assert(data, DeepEquals, row)
		i++
		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(i, Equals, int64(1))

	s.checkColumnKVExist(c, ctx, t, handle, col, columnValue, isDropped)

	// Test add a new row.
	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	newRow := types.MakeDatums(int64(11), int64(22), int64(33))
	handle, err = t.AddRecord(ctx, newRow)
	c.Assert(err, IsNil)

	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	rows := [][]types.Datum{row, newRow}

	i = int64(0)
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*column.Col) (bool, error) {
		c.Assert(data, DeepEquals, rows[i])
		i++
		return true, nil
	})
	c.Assert(i, Equals, int64(2))

	s.checkColumnKVExist(c, ctx, t, handle, col, columnValue, false)

	// Test remove a row.
	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	err = t.RemoveRecord(ctx, handle, newRow)
	c.Assert(err, IsNil)

	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	i = int64(0)
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*column.Col) (bool, error) {
		i++
		return true, nil
	})
	c.Assert(i, Equals, int64(1))

	s.checkColumnKVExist(c, ctx, t, handle, col, columnValue, false)
	s.testGetColumn(c, t, col.Name.L, false)
}

func (s *testColumnSuite) checkWriteOnlyColumn(c *C, ctx context.Context, d *ddl, tblInfo *model.TableInfo, handle int64, col *column.Col, row []types.Datum, columnValue interface{}, isDropped bool) {
	t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)

	_, err := ctx.GetTxn(true)
	c.Assert(err, IsNil)

	i := int64(0)
	err = t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*column.Col) (bool, error) {
		c.Assert(data, DeepEquals, row)
		i++
		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(i, Equals, int64(1))

	s.checkColumnKVExist(c, ctx, t, handle, col, columnValue, isDropped)

	// Test add a new row.
	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	newRow := types.MakeDatums(int64(11), int64(22), int64(33))
	handle, err = t.AddRecord(ctx, newRow)
	c.Assert(err, IsNil)

	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	rows := [][]types.Datum{row, newRow}

	i = int64(0)
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*column.Col) (bool, error) {
		c.Assert(data, DeepEquals, rows[i])
		i++
		return true, nil
	})
	c.Assert(i, Equals, int64(2))

	s.checkColumnKVExist(c, ctx, t, handle, col, columnValue, true)

	// Test remove a row.
	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	err = t.RemoveRecord(ctx, handle, newRow)
	c.Assert(err, IsNil)

	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	i = int64(0)
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*column.Col) (bool, error) {
		i++
		return true, nil
	})
	c.Assert(i, Equals, int64(1))

	s.checkColumnKVExist(c, ctx, t, handle, col, columnValue, false)
	s.testGetColumn(c, t, col.Name.L, false)
}

func (s *testColumnSuite) checkReorganizationColumn(c *C, ctx context.Context, d *ddl, tblInfo *model.TableInfo, handle int64, col *column.Col, row []types.Datum, columnValue interface{}, isDropped bool) {
	t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)

	_, err := ctx.GetTxn(true)
	c.Assert(err, IsNil)

	i := int64(0)
	err = t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*column.Col) (bool, error) {
		c.Assert(data, DeepEquals, row)
		i++
		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(i, Equals, int64(1))

	// Test add a new row.
	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	newRow := types.MakeDatums(int64(11), int64(22), int64(33))
	handle, err = t.AddRecord(ctx, newRow)
	c.Assert(err, IsNil)

	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	rows := [][]types.Datum{row, newRow}

	i = int64(0)
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*column.Col) (bool, error) {
		c.Assert(data, DeepEquals, rows[i])
		i++
		return true, nil
	})
	c.Assert(i, Equals, int64(2))

	s.checkColumnKVExist(c, ctx, t, handle, col, columnValue, !isDropped)

	// Test remove a row.
	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	err = t.RemoveRecord(ctx, handle, newRow)
	c.Assert(err, IsNil)

	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	i = int64(0)
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*column.Col) (bool, error) {
		i++
		return true, nil
	})
	c.Assert(i, Equals, int64(1))

	s.testGetColumn(c, t, col.Name.L, false)
}

func (s *testColumnSuite) checkPublicColumn(c *C, ctx context.Context, d *ddl, tblInfo *model.TableInfo, handle int64, col *column.Col, row []types.Datum, columnValue interface{}) {
	t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)

	_, err := ctx.GetTxn(true)
	c.Assert(err, IsNil)

	i := int64(0)
	oldRow := append(row, types.NewDatum(columnValue))
	err = t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*column.Col) (bool, error) {
		c.Assert(data, DeepEquals, oldRow)
		i++
		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(i, Equals, int64(1))

	// Test add a new row.
	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	newRow := types.MakeDatums(int64(11), int64(22), int64(33), int64(44))
	handle, err = t.AddRecord(ctx, newRow)
	c.Assert(err, IsNil)

	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	rows := [][]types.Datum{oldRow, newRow}

	i = int64(0)
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*column.Col) (bool, error) {
		c.Assert(data, DeepEquals, rows[i])
		i++
		return true, nil
	})
	c.Assert(i, Equals, int64(2))

	// Test remove a row.
	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	err = t.RemoveRecord(ctx, handle, newRow)
	c.Assert(err, IsNil)

	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	i = int64(0)
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*column.Col) (bool, error) {
		c.Assert(data, DeepEquals, oldRow)
		i++
		return true, nil
	})
	c.Assert(i, Equals, int64(1))

	err = ctx.FinishTxn(false)
	c.Assert(err, IsNil)
	s.testGetColumn(c, t, col.Name.L, true)
}

func (s *testColumnSuite) checkAddOrDropColumn(c *C, state model.SchemaState, d *ddl, tblInfo *model.TableInfo, handle int64, col *column.Col, row []types.Datum, columnValue interface{}, isDropped bool) {
	ctx := testNewContext(c, d)

	switch state {
	case model.StateNone:
		s.checkNoneColumn(c, ctx, d, tblInfo, handle, col, columnValue)
	case model.StateDeleteOnly:
		s.checkDeleteOnlyColumn(c, ctx, d, tblInfo, handle, col, row, columnValue, isDropped)
	case model.StateWriteOnly:
		s.checkWriteOnlyColumn(c, ctx, d, tblInfo, handle, col, row, columnValue, isDropped)
	case model.StateWriteReorganization, model.StateDeleteReorganization:
		s.checkReorganizationColumn(c, ctx, d, tblInfo, handle, col, row, columnValue, isDropped)
	case model.StatePublic:
		s.checkPublicColumn(c, ctx, d, tblInfo, handle, col, row, columnValue)
	}
}

func (s *testColumnSuite) testGetColumn(c *C, t table.Table, name string, isExist bool) {
	col := column.FindCol(t.Cols(), name)
	if isExist {
		c.Assert(col, NotNil)
	} else {
		c.Assert(col, IsNil)
	}
}

func (s *testColumnSuite) TestAddColumn(c *C) {
	d := newDDL(s.store, nil, nil, 100*time.Millisecond)
	tblInfo := testTableInfo(c, d, "t", 3)
	ctx := testNewContext(c, d)

	_, err := ctx.GetTxn(true)
	c.Assert(err, IsNil)

	testCreateTable(c, ctx, d, s.dbInfo, tblInfo)

	t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)

	row := types.MakeDatums(int64(1), int64(2), int64(3))
	handle, err := t.AddRecord(ctx, row)
	c.Assert(err, IsNil)

	err = ctx.FinishTxn(false)
	c.Assert(err, IsNil)

	colName := "c4"
	defaultColValue := int64(4)
	checkOK := false

	tc := &testDDLCallback{}
	tc.onJobUpdated = func(job *model.Job) {
		if checkOK {
			return
		}

		t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID).(*tables.Table)
		col := column.FindCol(t.Columns, colName)
		if col == nil {
			return
		}

		s.checkAddOrDropColumn(c, col.State, d, tblInfo, handle, col, row, defaultColValue, false)

		if col.State == model.StatePublic {
			checkOK = true
		}
	}

	d.hook = tc

	// Use local ddl for callback test.
	s.d.close()

	d.close()
	d.start()

	job := testCreateColumn(c, ctx, d, s.dbInfo, tblInfo, colName, &ast.ColumnPosition{Tp: ast.ColumnPositionNone}, defaultColValue)
	testCheckJobDone(c, d, job, true)

	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	job = testDropTable(c, ctx, d, s.dbInfo, tblInfo)
	testCheckJobDone(c, d, job, false)

	err = ctx.FinishTxn(false)
	c.Assert(err, IsNil)

	d.close()
	s.d.start()
}

func (s *testColumnSuite) TestDropColumn(c *C) {
	d := newDDL(s.store, nil, nil, 100*time.Millisecond)
	tblInfo := testTableInfo(c, d, "t", 4)
	ctx := testNewContext(c, d)

	_, err := ctx.GetTxn(true)
	c.Assert(err, IsNil)

	testCreateTable(c, ctx, d, s.dbInfo, tblInfo)

	t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)

	colName := "c4"
	defaultColValue := int64(4)
	row := types.MakeDatums(int64(1), int64(2), int64(3))
	handle, err := t.AddRecord(ctx, append(row, types.NewDatum(defaultColValue)))
	c.Assert(err, IsNil)

	err = ctx.FinishTxn(false)
	c.Assert(err, IsNil)

	checkOK := false
	oldCol := &column.Col{}

	tc := &testDDLCallback{}
	tc.onJobUpdated = func(job *model.Job) {
		if checkOK {
			return
		}

		t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID).(*tables.Table)
		col := column.FindCol(t.Columns, colName)
		if col == nil {
			s.checkAddOrDropColumn(c, model.StateNone, d, tblInfo, handle, oldCol, row, defaultColValue, true)
			checkOK = true
			return
		}

		s.checkAddOrDropColumn(c, col.State, d, tblInfo, handle, col, row, defaultColValue, true)
		oldCol = col
	}

	d.hook = tc

	// Use local ddl for callback test.
	s.d.close()

	d.close()
	d.start()

	job := testDropColumn(c, ctx, s.d, s.dbInfo, tblInfo, colName, false)
	testCheckJobDone(c, d, job, false)

	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	job = testDropTable(c, ctx, d, s.dbInfo, tblInfo)
	testCheckJobDone(c, d, job, false)

	err = ctx.FinishTxn(false)
	c.Assert(err, IsNil)

	d.close()
	s.d.start()
}
