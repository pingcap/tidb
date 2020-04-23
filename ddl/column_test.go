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
	"context"
	"reflect"
	"sync"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
)

var _ = Suite(&testColumnSuite{})

type testColumnSuite struct {
	store  kv.Storage
	dbInfo *model.DBInfo
}

func (s *testColumnSuite) SetUpSuite(c *C) {
	s.store = testCreateStore(c, "test_column")
	d := newDDL(
		context.Background(),
		WithStore(s.store),
		WithLease(testLease),
	)

	s.dbInfo = testSchemaInfo(c, d, "test_column")
	testCreateSchema(c, testNewContext(d), d, s.dbInfo)
	d.Stop()
}

func (s *testColumnSuite) TearDownSuite(c *C) {
	err := s.store.Close()
	c.Assert(err, IsNil)
}

func buildCreateColumnJob(dbInfo *model.DBInfo, tblInfo *model.TableInfo, colName string,
	pos *ast.ColumnPosition, defaultValue interface{}) *model.Job {
	col := &model.ColumnInfo{
		Name:               model.NewCIStr(colName),
		Offset:             len(tblInfo.Columns),
		DefaultValue:       defaultValue,
		OriginDefaultValue: defaultValue,
	}
	col.ID = allocateColumnID(tblInfo)
	col.FieldType = *types.NewFieldType(mysql.TypeLong)

	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionAddColumn,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{col, pos, 0},
	}
	return job
}

func testCreateColumn(c *C, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo,
	colName string, pos *ast.ColumnPosition, defaultValue interface{}) *model.Job {
	job := buildCreateColumnJob(dbInfo, tblInfo, colName, pos, defaultValue)
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func buildDropColumnJob(dbInfo *model.DBInfo, tblInfo *model.TableInfo, colName string) *model.Job {
	return &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionDropColumn,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{model.NewCIStr(colName)},
	}
}

func testDropColumn(c *C, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, colName string, isError bool) *model.Job {
	job := buildDropColumnJob(dbInfo, tblInfo, colName)
	err := d.doDDLJob(ctx, job)
	if isError {
		c.Assert(err, NotNil)
		return nil
	}
	c.Assert(errors.ErrorStack(err), Equals, "")
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func (s *testColumnSuite) TestColumn(c *C) {
	d := newDDL(
		context.Background(),
		WithStore(s.store),
		WithLease(testLease),
	)
	defer d.Stop()

	tblInfo := testTableInfo(c, d, "t1", 3)
	ctx := testNewContext(d)

	testCreateTable(c, ctx, d, s.dbInfo, tblInfo)
	t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)

	num := 10
	for i := 0; i < num; i++ {
		_, err := t.AddRecord(ctx, types.MakeDatums(i, 10*i, 100*i))
		c.Assert(err, IsNil)
	}

	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)

	i := int64(0)
	err = t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
		c.Assert(data, HasLen, 3)
		c.Assert(data[0].GetInt64(), Equals, i)
		c.Assert(data[1].GetInt64(), Equals, 10*i)
		c.Assert(data[2].GetInt64(), Equals, 100*i)
		i++
		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(i, Equals, int64(num))

	c.Assert(table.FindCol(t.Cols(), "c4"), IsNil)

	job := testCreateColumn(c, ctx, d, s.dbInfo, tblInfo, "c4", &ast.ColumnPosition{Tp: ast.ColumnPositionAfter, RelativeColumn: &ast.ColumnName{Name: model.NewCIStr("c3")}}, 100)
	testCheckJobDone(c, d, job, true)

	t = testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)
	c.Assert(table.FindCol(t.Cols(), "c4"), NotNil)

	i = int64(0)
	err = t.IterRecords(ctx, t.FirstKey(), t.Cols(),
		func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
			c.Assert(data, HasLen, 4)
			c.Assert(data[0].GetInt64(), Equals, i)
			c.Assert(data[1].GetInt64(), Equals, 10*i)
			c.Assert(data[2].GetInt64(), Equals, 100*i)
			c.Assert(data[3].GetInt64(), Equals, int64(100))
			i++
			return true, nil
		})
	c.Assert(err, IsNil)
	c.Assert(i, Equals, int64(num))

	h, err := t.AddRecord(ctx, types.MakeDatums(11, 12, 13, 14))
	c.Assert(err, IsNil)
	err = ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
	values, err := t.RowWithCols(ctx, h, t.Cols())
	c.Assert(err, IsNil)

	c.Assert(values, HasLen, 4)
	c.Assert(values[3].GetInt64(), Equals, int64(14))

	job = testDropColumn(c, ctx, d, s.dbInfo, tblInfo, "c4", false)
	testCheckJobDone(c, d, job, false)

	t = testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)
	values, err = t.RowWithCols(ctx, h, t.Cols())
	c.Assert(err, IsNil)

	c.Assert(values, HasLen, 3)
	c.Assert(values[2].GetInt64(), Equals, int64(13))

	job = testCreateColumn(c, ctx, d, s.dbInfo, tblInfo, "c4", &ast.ColumnPosition{Tp: ast.ColumnPositionNone}, 111)
	testCheckJobDone(c, d, job, true)

	t = testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)
	values, err = t.RowWithCols(ctx, h, t.Cols())
	c.Assert(err, IsNil)

	c.Assert(values, HasLen, 4)
	c.Assert(values[3].GetInt64(), Equals, int64(111))

	job = testCreateColumn(c, ctx, d, s.dbInfo, tblInfo, "c5", &ast.ColumnPosition{Tp: ast.ColumnPositionNone}, 101)
	testCheckJobDone(c, d, job, true)

	t = testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)
	values, err = t.RowWithCols(ctx, h, t.Cols())
	c.Assert(err, IsNil)

	c.Assert(values, HasLen, 5)
	c.Assert(values[4].GetInt64(), Equals, int64(101))

	job = testCreateColumn(c, ctx, d, s.dbInfo, tblInfo, "c6", &ast.ColumnPosition{Tp: ast.ColumnPositionFirst}, 202)
	testCheckJobDone(c, d, job, true)

	t = testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)
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

	job = testDropColumn(c, ctx, d, s.dbInfo, tblInfo, "c2", false)
	testCheckJobDone(c, d, job, false)

	t = testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)

	values, err = t.RowWithCols(ctx, h, t.Cols())
	c.Assert(err, IsNil)
	c.Assert(values, HasLen, 5)
	c.Assert(values[0].GetInt64(), Equals, int64(202))
	c.Assert(values[4].GetInt64(), Equals, int64(101))

	job = testDropColumn(c, ctx, d, s.dbInfo, tblInfo, "c1", false)
	testCheckJobDone(c, d, job, false)

	job = testDropColumn(c, ctx, d, s.dbInfo, tblInfo, "c3", false)
	testCheckJobDone(c, d, job, false)

	job = testDropColumn(c, ctx, d, s.dbInfo, tblInfo, "c4", false)
	testCheckJobDone(c, d, job, false)

	job = testCreateIndex(c, ctx, d, s.dbInfo, tblInfo, false, "c5_idx", "c5")
	testCheckJobDone(c, d, job, true)

	testDropColumn(c, ctx, d, s.dbInfo, tblInfo, "c5", true)

	testDropIndex(c, ctx, d, s.dbInfo, tblInfo, "c5_idx")
	testCheckJobDone(c, d, job, true)

	job = testDropColumn(c, ctx, d, s.dbInfo, tblInfo, "c5", false)
	testCheckJobDone(c, d, job, false)

	testDropColumn(c, ctx, d, s.dbInfo, tblInfo, "c6", true)

	testDropTable(c, ctx, d, s.dbInfo, tblInfo)
}

func (s *testColumnSuite) checkColumnKVExist(ctx sessionctx.Context, t table.Table, handle int64, col *table.Column, columnValue interface{}, isExist bool) error {
	err := ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		if txn, err1 := ctx.Txn(true); err1 == nil {
			txn.Commit(context.Background())
		}
	}()
	key := t.RecordKey(handle)
	txn, err := ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	data, err := txn.Get(context.TODO(), key)
	if !isExist {
		if terror.ErrorEqual(err, kv.ErrNotExist) {
			return nil
		}
	}
	if err != nil {
		return errors.Trace(err)
	}
	colMap := make(map[int64]*types.FieldType)
	colMap[col.ID] = &col.FieldType
	rowMap, err := tablecodec.DecodeRow(data, colMap, ctx.GetSessionVars().Location())
	if err != nil {
		return errors.Trace(err)
	}
	val, ok := rowMap[col.ID]
	if isExist {
		if !ok || val.GetValue() != columnValue {
			return errors.Errorf("%v is not equal to %v", val.GetValue(), columnValue)
		}
	} else {
		if ok {
			return errors.Errorf("column value should not exists")
		}
	}
	return nil
}

func (s *testColumnSuite) checkNoneColumn(ctx sessionctx.Context, d *ddl, tblInfo *model.TableInfo, handle int64, col *table.Column, columnValue interface{}) error {
	t, err := testGetTableWithError(d, s.dbInfo.ID, tblInfo.ID)
	if err != nil {
		return errors.Trace(err)
	}
	err = s.checkColumnKVExist(ctx, t, handle, col, columnValue, false)
	if err != nil {
		return errors.Trace(err)
	}
	err = s.testGetColumn(t, col.Name.L, false)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *testColumnSuite) checkDeleteOnlyColumn(ctx sessionctx.Context, d *ddl, tblInfo *model.TableInfo, handle int64, col *table.Column, row []types.Datum, columnValue interface{}) error {
	t, err := testGetTableWithError(d, s.dbInfo.ID, tblInfo.ID)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	i := int64(0)
	err = t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
		if !reflect.DeepEqual(data, row) {
			return false, errors.Errorf("%v not equal to %v", data, row)
		}
		i++
		return true, nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	if i != 1 {
		return errors.Errorf("expect 1, got %v", i)
	}
	err = s.checkColumnKVExist(ctx, t, handle, col, columnValue, false)
	if err != nil {
		return errors.Trace(err)
	}
	// Test add a new row.
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	newRow := types.MakeDatums(int64(11), int64(22), int64(33))
	newHandle, err := t.AddRecord(ctx, newRow)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	rows := [][]types.Datum{row, newRow}

	i = int64(0)
	err = t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
		if !reflect.DeepEqual(data, rows[i]) {
			return false, errors.Errorf("%v not equal to %v", data, rows[i])
		}
		i++
		return true, nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	if i != 2 {
		return errors.Errorf("expect 2, got %v", i)
	}

	err = s.checkColumnKVExist(ctx, t, handle, col, columnValue, false)
	if err != nil {
		return errors.Trace(err)
	}
	// Test remove a row.
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	err = t.RemoveRecord(ctx, newHandle, newRow)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	i = int64(0)
	err = t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
		i++
		return true, nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	if i != 1 {
		return errors.Errorf("expect 1, got %v", i)
	}
	err = s.checkColumnKVExist(ctx, t, newHandle, col, columnValue, false)
	if err != nil {
		return errors.Trace(err)
	}
	err = s.testGetColumn(t, col.Name.L, false)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *testColumnSuite) checkWriteOnlyColumn(ctx sessionctx.Context, d *ddl, tblInfo *model.TableInfo, handle int64, col *table.Column, row []types.Datum, columnValue interface{}) error {
	t, err := testGetTableWithError(d, s.dbInfo.ID, tblInfo.ID)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	i := int64(0)
	err = t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
		if !reflect.DeepEqual(data, row) {
			return false, errors.Errorf("%v not equal to %v", data, row)
		}
		i++
		return true, nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	if i != 1 {
		return errors.Errorf("expect 1, got %v", i)
	}

	err = s.checkColumnKVExist(ctx, t, handle, col, columnValue, false)
	if err != nil {
		return errors.Trace(err)
	}

	// Test add a new row.
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	newRow := types.MakeDatums(int64(11), int64(22), int64(33))
	newHandle, err := t.AddRecord(ctx, newRow)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	rows := [][]types.Datum{row, newRow}

	i = int64(0)
	err = t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
		if !reflect.DeepEqual(data, rows[i]) {
			return false, errors.Errorf("%v not equal to %v", data, rows[i])
		}
		i++
		return true, nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	if i != 2 {
		return errors.Errorf("expect 2, got %v", i)
	}

	err = s.checkColumnKVExist(ctx, t, newHandle, col, columnValue, true)
	if err != nil {
		return errors.Trace(err)
	}
	// Test remove a row.
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	err = t.RemoveRecord(ctx, newHandle, newRow)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	i = int64(0)
	err = t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
		i++
		return true, nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	if i != 1 {
		return errors.Errorf("expect 1, got %v", i)
	}

	err = s.checkColumnKVExist(ctx, t, newHandle, col, columnValue, false)
	if err != nil {
		return errors.Trace(err)
	}
	err = s.testGetColumn(t, col.Name.L, false)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *testColumnSuite) checkReorganizationColumn(ctx sessionctx.Context, d *ddl, tblInfo *model.TableInfo, handle int64, col *table.Column, row []types.Datum, columnValue interface{}) error {
	t, err := testGetTableWithError(d, s.dbInfo.ID, tblInfo.ID)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	i := int64(0)
	err = t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
		if !reflect.DeepEqual(data, row) {
			return false, errors.Errorf("%v not equal to %v", data, row)
		}
		i++
		return true, nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	if i != 1 {
		return errors.Errorf("expect 1 got %v", i)
	}

	// Test add a new row.
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	newRow := types.MakeDatums(int64(11), int64(22), int64(33))
	newHandle, err := t.AddRecord(ctx, newRow)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	rows := [][]types.Datum{row, newRow}

	i = int64(0)
	err = t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
		if !reflect.DeepEqual(data, rows[i]) {
			return false, errors.Errorf("%v not equal to %v", data, rows[i])
		}
		i++
		return true, nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	if i != 2 {
		return errors.Errorf("expect 2, got %v", i)
	}

	err = s.checkColumnKVExist(ctx, t, newHandle, col, columnValue, true)
	if err != nil {
		return errors.Trace(err)
	}

	// Test remove a row.
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	err = t.RemoveRecord(ctx, newHandle, newRow)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	i = int64(0)
	err = t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
		i++
		return true, nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	if i != 1 {
		return errors.Errorf("expect 1, got %v", i)
	}
	err = s.testGetColumn(t, col.Name.L, false)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *testColumnSuite) checkPublicColumn(ctx sessionctx.Context, d *ddl, tblInfo *model.TableInfo, _ int64, newCol *table.Column, oldRow []types.Datum, columnValue interface{}) error {
	t, err := testGetTableWithError(d, s.dbInfo.ID, tblInfo.ID)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	i := int64(0)
	updatedRow := append(oldRow, types.NewDatum(columnValue))
	err = t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
		if !reflect.DeepEqual(data, updatedRow) {
			return false, errors.Errorf("%v not equal to %v", data, updatedRow)
		}
		i++
		return true, nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	if i != 1 {
		return errors.Errorf("expect 1, got %v", i)
	}

	// Test add a new row.
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	newRow := types.MakeDatums(int64(11), int64(22), int64(33), int64(44))
	handle, err := t.AddRecord(ctx, newRow)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	rows := [][]types.Datum{updatedRow, newRow}

	i = int64(0)
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
		if !reflect.DeepEqual(data, rows[i]) {
			return false, errors.Errorf("%v not equal to %v", data, rows[i])
		}
		i++
		return true, nil
	})
	if i != 2 {
		return errors.Errorf("expect 2, got %v", i)
	}

	// Test remove a row.
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	err = t.RemoveRecord(ctx, handle, newRow)
	if err != nil {
		return errors.Trace(err)
	}

	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	i = int64(0)
	err = t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
		if !reflect.DeepEqual(data, updatedRow) {
			return false, errors.Errorf("%v not equal to %v", data, updatedRow)
		}
		i++
		return true, nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	if i != 1 {
		return errors.Errorf("expect 1, got %v", i)
	}

	err = s.testGetColumn(t, newCol.Name.L, true)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *testColumnSuite) checkAddColumn(state model.SchemaState, d *ddl, tblInfo *model.TableInfo, handle int64, newCol *table.Column, oldRow []types.Datum, columnValue interface{}) error {
	ctx := testNewContext(d)
	var err error
	switch state {
	case model.StateNone:
		err = errors.Trace(s.checkNoneColumn(ctx, d, tblInfo, handle, newCol, columnValue))
	case model.StateDeleteOnly:
		err = errors.Trace(s.checkDeleteOnlyColumn(ctx, d, tblInfo, handle, newCol, oldRow, columnValue))
	case model.StateWriteOnly:
		err = errors.Trace(s.checkWriteOnlyColumn(ctx, d, tblInfo, handle, newCol, oldRow, columnValue))
	case model.StateWriteReorganization, model.StateDeleteReorganization:
		err = errors.Trace(s.checkReorganizationColumn(ctx, d, tblInfo, handle, newCol, oldRow, columnValue))
	case model.StatePublic:
		err = errors.Trace(s.checkPublicColumn(ctx, d, tblInfo, handle, newCol, oldRow, columnValue))
	}
	return err
}

func (s *testColumnSuite) testGetColumn(t table.Table, name string, isExist bool) error {
	col := table.FindCol(t.Cols(), name)
	if isExist {
		if col == nil {
			return errors.Errorf("column should not be nil")
		}
	} else {
		if col != nil {
			return errors.Errorf("column should be nil")
		}
	}
	return nil
}

func (s *testColumnSuite) TestAddColumn(c *C) {
	d := newDDL(
		context.Background(),
		WithStore(s.store),
		WithLease(testLease),
	)
	tblInfo := testTableInfo(c, d, "t", 3)
	ctx := testNewContext(d)

	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)

	testCreateTable(c, ctx, d, s.dbInfo, tblInfo)
	t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)

	oldRow := types.MakeDatums(int64(1), int64(2), int64(3))
	handle, err := t.AddRecord(ctx, oldRow)
	c.Assert(err, IsNil)

	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	newColName := "c4"
	defaultColValue := int64(4)

	var mu sync.Mutex
	var hookErr error
	checkOK := false

	tc := &TestDDLCallback{}
	tc.onJobUpdated = func(job *model.Job) {
		mu.Lock()
		defer mu.Unlock()
		if checkOK {
			return
		}

		t, err1 := testGetTableWithError(d, s.dbInfo.ID, tblInfo.ID)
		if err1 != nil {
			hookErr = errors.Trace(err1)
			return
		}
		newCol := table.FindCol(t.(*tables.TableCommon).Columns, newColName)
		if newCol == nil {
			return
		}

		err1 = s.checkAddColumn(newCol.State, d, tblInfo, handle, newCol, oldRow, defaultColValue)
		if err1 != nil {
			hookErr = errors.Trace(err1)
			return
		}

		if newCol.State == model.StatePublic {
			checkOK = true
		}
	}

	d.SetHook(tc)

	job := testCreateColumn(c, ctx, d, s.dbInfo, tblInfo, newColName, &ast.ColumnPosition{Tp: ast.ColumnPositionNone}, defaultColValue)

	testCheckJobDone(c, d, job, true)
	mu.Lock()
	hErr := hookErr
	ok := checkOK
	mu.Unlock()
	c.Assert(errors.ErrorStack(hErr), Equals, "")
	c.Assert(ok, IsTrue)

	err = ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)

	job = testDropTable(c, ctx, d, s.dbInfo, tblInfo)
	testCheckJobDone(c, d, job, false)

	txn, err = ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	d.Stop()
}

func (s *testColumnSuite) TestDropColumn(c *C) {
	d := newDDL(
		context.Background(),
		WithStore(s.store),
		WithLease(testLease),
	)
	tblInfo := testTableInfo(c, d, "t2", 4)
	ctx := testNewContext(d)

	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)

	testCreateTable(c, ctx, d, s.dbInfo, tblInfo)
	t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)

	colName := "c4"
	defaultColValue := int64(4)
	row := types.MakeDatums(int64(1), int64(2), int64(3))
	_, err = t.AddRecord(ctx, append(row, types.NewDatum(defaultColValue)))
	c.Assert(err, IsNil)

	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	checkOK := false
	var hookErr error
	var mu sync.Mutex

	tc := &TestDDLCallback{}
	tc.onJobUpdated = func(job *model.Job) {
		mu.Lock()
		defer mu.Unlock()
		if checkOK {
			return
		}
		t, err1 := testGetTableWithError(d, s.dbInfo.ID, tblInfo.ID)
		if err1 != nil {
			hookErr = errors.Trace(err1)
			return
		}
		col := table.FindCol(t.(*tables.TableCommon).Columns, colName)
		if col == nil {
			checkOK = true
			return
		}
	}

	d.SetHook(tc)

	job := testDropColumn(c, ctx, d, s.dbInfo, tblInfo, colName, false)
	testCheckJobDone(c, d, job, false)
	mu.Lock()
	hErr := hookErr
	ok := checkOK
	mu.Unlock()
	c.Assert(hErr, IsNil)
	c.Assert(ok, IsTrue)

	err = ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)

	job = testDropTable(c, ctx, d, s.dbInfo, tblInfo)
	testCheckJobDone(c, d, job, false)

	txn, err = ctx.Txn(true)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	d.Stop()
}

func (s *testColumnSuite) TestModifyColumn(c *C) {
	d := newDDL(
		context.Background(),
		WithStore(s.store),
		WithLease(testLease),
	)
	defer d.Stop()
	tests := []struct {
		origin string
		to     string
		err    error
	}{
		{"int", "bigint", nil},
		{"int", "int unsigned", errUnsupportedModifyColumn.GenWithStackByArgs("length 10 is less than origin 11")},
		{"varchar(10)", "text", nil},
		{"varbinary(10)", "blob", nil},
		{"text", "blob", errUnsupportedModifyCharset.GenWithStackByArgs("charset from utf8mb4 to binary")},
		{"varchar(10)", "varchar(8)", errUnsupportedModifyColumn.GenWithStackByArgs("length 8 is less than origin 10")},
		{"varchar(10)", "varchar(11)", nil},
		{"varchar(10) character set utf8 collate utf8_bin", "varchar(10) character set utf8", nil},
		{"decimal(2,1)", "decimal(3,2)", errUnsupportedModifyColumn.GenWithStackByArgs("can't change decimal column precision")},
		{"decimal(2,1)", "decimal(2,2)", errUnsupportedModifyColumn.GenWithStackByArgs("can't change decimal column precision")},
		{"decimal(2,1)", "decimal(2,1)", nil},
	}
	for _, tt := range tests {
		ftA := s.colDefStrToFieldType(c, tt.origin)
		ftB := s.colDefStrToFieldType(c, tt.to)
		err := checkModifyTypes(ftA, ftB, false)
		if err == nil {
			c.Assert(tt.err, IsNil)
		} else {
			c.Assert(err.Error(), Equals, tt.err.Error())
		}
	}
}

func (s *testColumnSuite) colDefStrToFieldType(c *C, str string) *types.FieldType {
	sqlA := "alter table t modify column a " + str
	stmt, err := parser.New().ParseOneStmt(sqlA, "", "")
	c.Assert(err, IsNil)
	colDef := stmt.(*ast.AlterTableStmt).Specs[0].NewColumns[0]
	chs, coll := charset.GetDefaultCharsetAndCollate()
	col, _, err := buildColumnAndConstraint(nil, 0, colDef, nil, chs, coll)
	c.Assert(err, IsNil)
	return &col.FieldType
}

func (s *testColumnSuite) TestFieldCase(c *C) {
	var fields = []string{"field", "Field"}
	colObjects := make([]*model.ColumnInfo, len(fields))
	for i, name := range fields {
		colObjects[i] = &model.ColumnInfo{
			Name: model.NewCIStr(name),
		}
	}
	err := checkDuplicateColumn(colObjects)
	c.Assert(err.Error(), Equals, infoschema.ErrColumnExists.GenWithStackByArgs("Field").Error())
}
