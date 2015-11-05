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
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
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
	s.store = testCreateStore(c, "test_column")
	lease := 50 * time.Millisecond
	s.d = newDDL(s.store, nil, nil, lease)

	s.dbInfo = testSchemaInfo(c, s.d, "test")
	testCreateSchema(c, mock.NewContext(), s.d, s.dbInfo)
}

func (s *testColumnSuite) TearDownSuite(c *C) {
	testDropSchema(c, mock.NewContext(), s.d, s.dbInfo)
	s.d.close()
	s.store.Close()
}

func testCreateColumn(c *C, ctx context.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo,
	colName string, pos *ColumnPosition, defaultValue interface{}) *model.Job {
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

	err = d.startJob(ctx, job)
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

	err := d.startJob(ctx, job)
	if isError {
		c.Assert(err, NotNil)
		return nil
	}

	c.Assert(err, IsNil)
	return job
}

func (s *testColumnSuite) TestColumn(c *C) {
	tblInfo := testTableInfo(c, s.d, "t1")
	ctx := testNewContext(c, s.d)
	defer ctx.FinishTxn(true)

	testCreateTable(c, ctx, s.d, s.dbInfo, tblInfo)

	t := testGetTable(c, s.d, s.dbInfo.ID, tblInfo.ID)

	num := 10
	for i := 0; i < num; i++ {
		_, err := t.AddRecord(ctx, []interface{}{i, 10 * i, 100 * i})
		c.Assert(err, IsNil)
	}

	err := ctx.FinishTxn(false)
	c.Assert(err, IsNil)

	i := int64(0)
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []interface{}, cols []*column.Col) (bool, error) {
		c.Assert(data, HasLen, 3)
		c.Assert(data[0], Equals, i)
		c.Assert(data[1], Equals, 10*i)
		c.Assert(data[2], Equals, 100*i)
		i++
		return true, nil
	})
	c.Assert(i, Equals, int64(num))

	c.Assert(column.FindCol(t.Cols(), "c4"), IsNil)

	job := testCreateColumn(c, ctx, s.d, s.dbInfo, tblInfo, "c4", &ColumnPosition{Type: ColumnPositionAfter, RelativeColumn: "c3"}, 100)
	testCheckJobDone(c, s.d, job, true)

	t = testGetTable(c, s.d, s.dbInfo.ID, tblInfo.ID)
	c.Assert(column.FindCol(t.Cols(), "c4"), NotNil)

	i = int64(0)
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []interface{}, cols []*column.Col) (bool, error) {
		c.Assert(data, HasLen, 4)
		c.Assert(data[0], Equals, i)
		c.Assert(data[1], Equals, 10*i)
		c.Assert(data[2], Equals, 100*i)
		c.Assert(data[3], Equals, int64(100))
		i++
		return true, nil
	})
	c.Assert(i, Equals, int64(num))

	h, err := t.AddRecord(ctx, []interface{}{11, 12, 13, 14})
	c.Assert(err, IsNil)
	err = ctx.FinishTxn(false)
	c.Assert(err, IsNil)

	values, err := t.RowWithCols(ctx, h, t.Cols())
	c.Assert(err, IsNil)

	c.Assert(values, HasLen, 4)
	c.Assert(values[3], Equals, int64(14))

	job = testDropColumn(c, ctx, s.d, s.dbInfo, tblInfo, "c4", false)
	testCheckJobDone(c, s.d, job, false)

	t = testGetTable(c, s.d, s.dbInfo.ID, tblInfo.ID)
	values, err = t.RowWithCols(ctx, h, t.Cols())
	c.Assert(err, IsNil)

	c.Assert(values, HasLen, 3)
	c.Assert(values[2], Equals, int64(13))

	job = testCreateColumn(c, ctx, s.d, s.dbInfo, tblInfo, "c4", &ColumnPosition{Type: ColumnPositionNone}, 111)
	testCheckJobDone(c, s.d, job, true)

	t = testGetTable(c, s.d, s.dbInfo.ID, tblInfo.ID)
	values, err = t.RowWithCols(ctx, h, t.Cols())
	c.Assert(err, IsNil)

	c.Assert(values, HasLen, 4)
	c.Assert(values[3], Equals, int64(111))

	job = testCreateColumn(c, ctx, s.d, s.dbInfo, tblInfo, "c5", &ColumnPosition{Type: ColumnPositionNone}, 101)
	testCheckJobDone(c, s.d, job, true)

	t = testGetTable(c, s.d, s.dbInfo.ID, tblInfo.ID)
	values, err = t.RowWithCols(ctx, h, t.Cols())
	c.Assert(err, IsNil)

	c.Assert(values, HasLen, 5)
	c.Assert(values[4], Equals, int64(101))

	job = testCreateColumn(c, ctx, s.d, s.dbInfo, tblInfo, "c6", &ColumnPosition{Type: ColumnPositionFirst}, 202)
	testCheckJobDone(c, s.d, job, true)

	t = testGetTable(c, s.d, s.dbInfo.ID, tblInfo.ID)
	values, err = t.RowWithCols(ctx, h, t.Cols())
	c.Assert(err, IsNil)

	c.Assert(values, HasLen, 6)
	c.Assert(values[0], Equals, int64(202))
	c.Assert(values[5], Equals, int64(101))

	job = testDropColumn(c, ctx, s.d, s.dbInfo, tblInfo, "c2", false)
	testCheckJobDone(c, s.d, job, false)

	t = testGetTable(c, s.d, s.dbInfo.ID, tblInfo.ID)
	values, err = t.RowWithCols(ctx, h, t.Cols())
	c.Assert(err, IsNil)

	c.Assert(values, HasLen, 5)
	c.Assert(values[0], Equals, int64(202))
	c.Assert(values[4], Equals, int64(101))

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
}

func (s *testIndexSuite) checkDeleteOnlyColumn(c *C, ctx context.Context, d *ddl, tblInfo *model.TableInfo, handle int64, col *column.Col, row []interface{}) {
	t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)

	i := int64(0)
	err := t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []interface{}, cols []*column.Col) (bool, error) {
		c.Assert(data, DeepEquals, row)
		i++
		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(i, Equals, int64(1))

	// Check if new column value exists.
	txn, err := ctx.GetTxn(false)
	key := t.RecordKey(handle, col)
	_, err = txn.Get(key)
	c.Assert(err, NotNil)

	// Test add a new row.
	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	newRow := []interface{}{int64(11), int64(22), int64(33)}
	handle, err = t.AddRecord(ctx, newRow)
	c.Assert(err, IsNil)

	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	rows := [][]interface{}{row, newRow}

	i = int64(0)
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []interface{}, cols []*column.Col) (bool, error) {
		c.Assert(data, DeepEquals, rows[i])
		i++
		return true, nil
	})
	c.Assert(i, Equals, int64(2))

	// Check if new column value exists.
	txn, err = ctx.GetTxn(true)
	key = t.RecordKey(handle, col)
	_, err = txn.Get(key)
	c.Assert(err, NotNil)

	// Test remove a row.
	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	err = t.RemoveRow(ctx, handle)
	c.Assert(err, IsNil)

	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	i = int64(0)
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []interface{}, cols []*column.Col) (bool, error) {
		i++
		return true, nil
	})
	c.Assert(i, Equals, int64(1))

	// Check if new column value exists.
	txn, err = ctx.GetTxn(true)
	key = t.RecordKey(handle, col)
	_, err = txn.Get(key)
	c.Assert(err, NotNil)

	err = ctx.FinishTxn(false)
	c.Assert(err, IsNil)
}

func (s *testIndexSuite) checkWriteOnlyColumn(c *C, ctx context.Context, d *ddl, tblInfo *model.TableInfo, handle int64, col *column.Col, row []interface{}, columnValue interface{}) {
	t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)

	i := int64(0)
	err := t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []interface{}, cols []*column.Col) (bool, error) {
		c.Assert(data, DeepEquals, row)
		i++
		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(i, Equals, int64(1))

	// Check if new column value exists.
	txn, err := ctx.GetTxn(false)
	key := t.RecordKey(handle, col)
	_, err = txn.Get(key)
	c.Assert(err, NotNil)

	// Test add a new row.
	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	newRow := []interface{}{int64(11), int64(22), int64(33)}
	handle, err = t.AddRecord(ctx, newRow)
	c.Assert(err, IsNil)

	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	rows := [][]interface{}{row, newRow}

	i = int64(0)
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []interface{}, cols []*column.Col) (bool, error) {
		c.Assert(data, DeepEquals, rows[i])
		i++
		return true, nil
	})
	c.Assert(i, Equals, int64(2))

	// Check if new column value exists.
	txn, err = ctx.GetTxn(true)
	key = t.RecordKey(handle, col)
	data, err := txn.Get(key)
	c.Assert(err, IsNil)
	v, err := t.DecodeValue(data, col)
	c.Assert(err, IsNil)
	value, err := types.Convert(v, &col.FieldType)
	c.Assert(err, IsNil)
	c.Assert(value, Equals, columnValue)

	// Test remove a row.
	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	err = t.RemoveRow(ctx, handle)
	c.Assert(err, IsNil)

	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	i = int64(0)
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []interface{}, cols []*column.Col) (bool, error) {
		i++
		return true, nil
	})
	c.Assert(i, Equals, int64(1))

	// Check if new column value exists.
	txn, err = ctx.GetTxn(true)
	key = t.RecordKey(handle, col)
	_, err = txn.Get(key)
	c.Assert(err, NotNil)

	err = ctx.FinishTxn(false)
	c.Assert(err, IsNil)
}

func (s *testIndexSuite) checkPublicColumn(c *C, ctx context.Context, d *ddl, tblInfo *model.TableInfo, handle int64, col *column.Col, row []interface{}, columnValue interface{}) {
	t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)

	i := int64(0)
	oldRow := append(row, columnValue)
	err := t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []interface{}, cols []*column.Col) (bool, error) {
		c.Assert(data, DeepEquals, oldRow)
		i++
		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(i, Equals, int64(1))

	// Test add a new row.
	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	newRow := []interface{}{int64(11), int64(22), int64(33), int64(55)}
	handle, err = t.AddRecord(ctx, newRow)
	c.Assert(err, IsNil)

	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	rows := [][]interface{}{oldRow, newRow}

	i = int64(0)
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []interface{}, cols []*column.Col) (bool, error) {
		c.Assert(data, DeepEquals, rows[i])
		i++
		return true, nil
	})
	c.Assert(i, Equals, int64(2))

	// Test remove a row.
	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	err = t.RemoveRow(ctx, handle)
	c.Assert(err, IsNil)

	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	i = int64(0)
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []interface{}, cols []*column.Col) (bool, error) {
		c.Assert(data, DeepEquals, oldRow)
		i++
		return true, nil
	})
	c.Assert(i, Equals, int64(1))

	err = ctx.FinishTxn(false)
	c.Assert(err, IsNil)
}

func (s *testIndexSuite) checkAddColumn(c *C, state model.SchemaState, ctx context.Context, d *ddl, tblInfo *model.TableInfo, handle int64, col *column.Col, row []interface{}, columnValue interface{}) {
	switch state {
	case model.StateNone:
		// do nothing
	case model.StateDeleteOnly:
		s.checkDeleteOnlyColumn(c, ctx, d, tblInfo, handle, col, row)
	case model.StateWriteOnly:
		s.checkWriteOnlyColumn(c, ctx, d, tblInfo, handle, col, row, columnValue)
	case model.StateReorganization:
		// do nothing
	case model.StatePublic:
		s.checkPublicColumn(c, ctx, d, tblInfo, handle, col, row, columnValue)
	}
}

func testGetColumn(t *tables.Table, name string) *column.Col {
	for _, col := range t.Columns {
		if name == col.Name.L {
			return col
		}
	}

	return nil
}

func (s *testIndexSuite) TestAddColumn(c *C) {
	d := newDDL(s.store, nil, nil, 100*time.Millisecond)
	defer d.close()

	tblInfo := testTableInfo(c, d, "t")
	ctx := testNewContext(c, d)
	defer ctx.FinishTxn(true)

	_, err := ctx.GetTxn(true)
	c.Assert(err, IsNil)

	testCreateTable(c, ctx, d, s.dbInfo, tblInfo)

	t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)

	row := []interface{}{int64(1), int64(2), int64(3)}
	handle, err := t.AddRecord(ctx, row)
	c.Assert(err, IsNil)

	ticker := time.NewTicker(d.lease)
	done := make(chan *model.Job, 1)

	err = ctx.FinishTxn(false)
	c.Assert(err, IsNil)

	colName := "c4"
	defaultColValue := int64(4)

	go func() {
		done <- testCreateColumn(c, ctx, s.d, s.dbInfo, tblInfo, colName, &ColumnPosition{Type: ColumnPositionNone}, defaultColValue)
	}()

	states := []model.SchemaState{model.StateDeleteOnly, model.StateWriteOnly, model.StateReorganization, model.StatePublic}
	stateIndex := 0
	lastStateIndex := -1

	for {
		select {
		case job := <-done:
			testCheckJobDone(c, d, job, true)
			return
		case <-ticker.C:
			d.close()

			t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID).(*tables.Table)
			col := testGetColumn(t, colName)
			if col == nil {
				continue
			}

			// Here means column state is not changed, so just skipped.
			if stateIndex == lastStateIndex {
				continue
			}

			if stateIndex == len(states) {
				return
			}

			c.Assert(col.State, Equals, states[stateIndex])
			lastStateIndex = stateIndex
			stateIndex++

			s.checkAddColumn(c, col.State, ctx, d, tblInfo, handle, col, row, defaultColValue)

			d.start()
		}
	}
}
