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
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testIndexSuite{})

type testIndexSuite struct {
	store  kv.Storage
	dbInfo *model.DBInfo

	d *ddl
}

func (s *testIndexSuite) SetUpSuite(c *C) {
	trySkipTest(c)

	s.store = testCreateStore(c, "test_index")
	lease := 50 * time.Millisecond
	s.d = newDDL(s.store, nil, nil, lease)

	s.dbInfo = testSchemaInfo(c, s.d, "test_index")
	testCreateSchema(c, mock.NewContext(), s.d, s.dbInfo)
}

func (s *testIndexSuite) TearDownSuite(c *C) {
	trySkipTest(c)

	testDropSchema(c, mock.NewContext(), s.d, s.dbInfo)
	s.d.close()

	err := s.store.Close()
	c.Assert(err, IsNil)
}

func testCreateIndex(c *C, ctx context.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, unique bool, indexName string, colName string) *model.Job {
	id, err := d.genGlobalID()
	c.Assert(err, IsNil)
	job := &model.Job{
		SchemaID: dbInfo.ID,
		TableID:  tblInfo.ID,
		Type:     model.ActionAddIndex,
		Args:     []interface{}{unique, model.NewCIStr(indexName), id, []*ast.IndexColName{{Column: &ast.ColumnName{Name: model.NewCIStr(colName)}, Length: 256}}},
	}

	err = d.startDDLJob(ctx, job)
	c.Assert(err, IsNil)
	return job
}

func testDropIndex(c *C, ctx context.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, indexName string) *model.Job {
	job := &model.Job{
		SchemaID: dbInfo.ID,
		TableID:  tblInfo.ID,
		Type:     model.ActionDropIndex,
		Args:     []interface{}{model.NewCIStr(indexName)},
	}

	err := d.startDDLJob(ctx, job)
	c.Assert(err, IsNil)
	return job
}

func (s *testIndexSuite) TestIndex(c *C) {
	tblInfo := testTableInfo(c, s.d, "t1", 3)
	ctx := testNewContext(c, s.d)
	defer ctx.FinishTxn(true)

	txn, err := ctx.GetTxn(true)
	c.Assert(err, IsNil)

	testCreateTable(c, ctx, s.d, s.dbInfo, tblInfo)

	t := testGetTable(c, s.d, s.dbInfo.ID, tblInfo.ID)

	num := 10
	for i := 0; i < num; i++ {
		_, err = t.AddRecord(ctx, types.MakeDatums(i, i, i))
		c.Assert(err, IsNil)
	}

	err = ctx.FinishTxn(false)
	c.Assert(err, IsNil)

	i := int64(0)
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*column.Col) (bool, error) {
		c.Assert(data[0].GetInt64(), Equals, i)
		i++
		return true, nil
	})

	job := testCreateIndex(c, ctx, s.d, s.dbInfo, tblInfo, true, "c1_uni", "c1")
	testCheckJobDone(c, s.d, job, true)

	t = testGetTable(c, s.d, s.dbInfo.ID, tblInfo.ID)
	index := tables.FindIndexByColName(t, "c1")
	c.Assert(index, NotNil)

	h, err := t.AddRecord(ctx, types.MakeDatums(num+1, 1, 1))
	c.Assert(err, IsNil)

	h1, err := t.AddRecord(ctx, types.MakeDatums(num+1, 1, 1))
	c.Assert(err, NotNil)
	c.Assert(h, Equals, h1)

	h, err = t.AddRecord(ctx, types.MakeDatums(1, 1, 1))
	c.Assert(err, NotNil)

	txn, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	exist, _, err := index.X.Exist(txn, types.MakeDatums(1), h)
	c.Assert(err, IsNil)
	c.Assert(exist, IsTrue)

	job = testDropIndex(c, ctx, s.d, s.dbInfo, tblInfo, "c1_uni")
	testCheckJobDone(c, s.d, job, false)

	t = testGetTable(c, s.d, s.dbInfo.ID, tblInfo.ID)
	index1 := tables.FindIndexByColName(t, "c1")
	c.Assert(index1, IsNil)

	txn, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	exist, _, err = index.X.Exist(txn, types.MakeDatums(1), h)
	c.Assert(err, IsNil)
	c.Assert(exist, IsFalse)

	h, err = t.AddRecord(ctx, types.MakeDatums(1, 1, 1))
	c.Assert(err, IsNil)
}

func getIndex(t table.Table, name string) *column.IndexedCol {
	for _, idx := range t.Indices() {
		// only public index can be read.

		if len(idx.Columns) == 1 && strings.EqualFold(idx.Columns[0].Name.L, name) {
			return idx
		}
	}
	return nil
}

func (s *testIndexSuite) testGetIndex(c *C, t table.Table, name string, isExist bool) {
	index := tables.FindIndexByColName(t, name)
	if isExist {
		c.Assert(index, NotNil)
	} else {
		c.Assert(index, IsNil)
	}
}

func (s *testIndexSuite) checkIndexKVExist(c *C, ctx context.Context, t table.Table, handle int64, indexCol *column.IndexedCol, columnValues []types.Datum, isExist bool) {
	c.Assert(len(indexCol.Columns), Equals, len(columnValues))

	txn, err := ctx.GetTxn(true)
	c.Assert(err, IsNil)

	exist, _, err := indexCol.X.Exist(txn, columnValues, handle)
	c.Assert(err, IsNil)
	c.Assert(exist, Equals, isExist)

	err = ctx.FinishTxn(false)
	c.Assert(err, IsNil)
}

func (s *testIndexSuite) checkNoneIndex(c *C, ctx context.Context, d *ddl, tblInfo *model.TableInfo, handle int64, indexCol *column.IndexedCol, row []types.Datum) {
	t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)

	columnValues := make([]types.Datum, len(indexCol.Columns))
	for i, column := range indexCol.Columns {
		columnValues[i] = row[column.Offset]
	}

	s.checkIndexKVExist(c, ctx, t, handle, indexCol, columnValues, false)
	s.testGetIndex(c, t, indexCol.Columns[0].Name.L, false)
}

func (s *testIndexSuite) checkDeleteOnlyIndex(c *C, ctx context.Context, d *ddl, tblInfo *model.TableInfo, handle int64, indexCol *column.IndexedCol, row []types.Datum, isDropped bool) {
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

	columnValues := make([]types.Datum, len(indexCol.Columns))
	for i, column := range indexCol.Columns {
		columnValues[i] = row[column.Offset]
	}

	s.checkIndexKVExist(c, ctx, t, handle, indexCol, columnValues, isDropped)

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

	for i, column := range indexCol.Columns {
		columnValues[i] = newRow[column.Offset]
	}

	s.checkIndexKVExist(c, ctx, t, handle, indexCol, columnValues, false)

	// Test update a new row.
	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	newUpdateRow := types.MakeDatums(int64(44), int64(55), int64(66))
	touched := map[int]bool{0: true, 1: true, 2: true}
	err = t.UpdateRecord(ctx, handle, newRow, newUpdateRow, touched)
	c.Assert(err, IsNil)

	s.checkIndexKVExist(c, ctx, t, handle, indexCol, columnValues, false)

	for i, column := range indexCol.Columns {
		columnValues[i] = newUpdateRow[column.Offset]
	}

	s.checkIndexKVExist(c, ctx, t, handle, indexCol, columnValues, false)

	// Test remove a row.
	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	err = t.RemoveRecord(ctx, handle, newUpdateRow)
	c.Assert(err, IsNil)

	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	i = int64(0)
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*column.Col) (bool, error) {
		i++
		return true, nil
	})
	c.Assert(i, Equals, int64(1))

	s.checkIndexKVExist(c, ctx, t, handle, indexCol, columnValues, false)
	s.testGetIndex(c, t, indexCol.Columns[0].Name.L, false)
}

func (s *testIndexSuite) checkWriteOnlyIndex(c *C, ctx context.Context, d *ddl, tblInfo *model.TableInfo, handle int64, indexCol *column.IndexedCol, row []types.Datum, isDropped bool) {
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

	columnValues := make([]types.Datum, len(indexCol.Columns))
	for i, column := range indexCol.Columns {
		columnValues[i] = row[column.Offset]
	}

	s.checkIndexKVExist(c, ctx, t, handle, indexCol, columnValues, isDropped)

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

	for i, column := range indexCol.Columns {
		columnValues[i] = newRow[column.Offset]
	}

	s.checkIndexKVExist(c, ctx, t, handle, indexCol, columnValues, true)

	// Test update a new row.
	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	newUpdateRow := types.MakeDatums(int64(44), int64(55), int64(66))
	touched := map[int]bool{0: true, 1: true, 2: true}
	err = t.UpdateRecord(ctx, handle, newRow, newUpdateRow, touched)
	c.Assert(err, IsNil)

	s.checkIndexKVExist(c, ctx, t, handle, indexCol, columnValues, false)

	for i, column := range indexCol.Columns {
		columnValues[i] = newUpdateRow[column.Offset]
	}

	s.checkIndexKVExist(c, ctx, t, handle, indexCol, columnValues, true)

	// Test remove a row.
	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	err = t.RemoveRecord(ctx, handle, newUpdateRow)
	c.Assert(err, IsNil)

	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	i = int64(0)
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*column.Col) (bool, error) {
		i++
		return true, nil
	})
	c.Assert(i, Equals, int64(1))

	s.checkIndexKVExist(c, ctx, t, handle, indexCol, columnValues, false)
	s.testGetIndex(c, t, indexCol.Columns[0].Name.L, false)
}

func (s *testIndexSuite) checkReorganizationIndex(c *C, ctx context.Context, d *ddl, tblInfo *model.TableInfo, handle int64, indexCol *column.IndexedCol, row []types.Datum, isDropped bool) {
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

	columnValues := make([]types.Datum, len(indexCol.Columns))
	for i, column := range indexCol.Columns {
		columnValues[i] = newRow[column.Offset]
	}

	s.checkIndexKVExist(c, ctx, t, handle, indexCol, columnValues, !isDropped)

	// Test update a new row.
	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	newUpdateRow := types.MakeDatums(int64(44), int64(55), int64(66))
	touched := map[int]bool{0: true, 1: true, 2: true}
	err = t.UpdateRecord(ctx, handle, newRow, newUpdateRow, touched)
	c.Assert(err, IsNil)

	s.checkIndexKVExist(c, ctx, t, handle, indexCol, columnValues, false)

	for i, column := range indexCol.Columns {
		columnValues[i] = newUpdateRow[column.Offset]
	}

	s.checkIndexKVExist(c, ctx, t, handle, indexCol, columnValues, !isDropped)

	// Test remove a row.
	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	err = t.RemoveRecord(ctx, handle, newUpdateRow)
	c.Assert(err, IsNil)

	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	i = int64(0)
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*column.Col) (bool, error) {
		i++
		return true, nil
	})
	c.Assert(i, Equals, int64(1))

	s.testGetIndex(c, t, indexCol.Columns[0].Name.L, false)
}

func (s *testIndexSuite) checkPublicIndex(c *C, ctx context.Context, d *ddl, tblInfo *model.TableInfo, handle int64, indexCol *column.IndexedCol, row []types.Datum) {
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

	columnValues := make([]types.Datum, len(indexCol.Columns))
	for i, column := range indexCol.Columns {
		columnValues[i] = row[column.Offset]
	}

	s.checkIndexKVExist(c, ctx, t, handle, indexCol, columnValues, true)

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

	for i, column := range indexCol.Columns {
		columnValues[i] = newRow[column.Offset]
	}

	s.checkIndexKVExist(c, ctx, t, handle, indexCol, columnValues, true)

	// Test update a new row.
	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	newUpdateRow := types.MakeDatums(int64(44), int64(55), int64(66))
	touched := map[int]bool{0: true, 1: true, 2: true}
	err = t.UpdateRecord(ctx, handle, newRow, newUpdateRow, touched)
	c.Assert(err, IsNil)

	s.checkIndexKVExist(c, ctx, t, handle, indexCol, columnValues, false)

	for i, column := range indexCol.Columns {
		columnValues[i] = newUpdateRow[column.Offset]
	}

	s.checkIndexKVExist(c, ctx, t, handle, indexCol, columnValues, true)

	// Test remove a row.
	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	err = t.RemoveRecord(ctx, handle, newUpdateRow)
	c.Assert(err, IsNil)

	_, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	i = int64(0)
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*column.Col) (bool, error) {
		i++
		return true, nil
	})
	c.Assert(i, Equals, int64(1))

	s.checkIndexKVExist(c, ctx, t, handle, indexCol, columnValues, false)
	s.testGetIndex(c, t, indexCol.Columns[0].Name.L, true)
}

func (s *testIndexSuite) checkAddOrDropIndex(c *C, state model.SchemaState, d *ddl, tblInfo *model.TableInfo, handle int64, indexCol *column.IndexedCol, row []types.Datum, isDropped bool) {
	ctx := testNewContext(c, d)

	switch state {
	case model.StateNone:
		s.checkNoneIndex(c, ctx, d, tblInfo, handle, indexCol, row)
	case model.StateDeleteOnly:
		s.checkDeleteOnlyIndex(c, ctx, d, tblInfo, handle, indexCol, row, isDropped)
	case model.StateWriteOnly:
		s.checkWriteOnlyIndex(c, ctx, d, tblInfo, handle, indexCol, row, isDropped)
	case model.StateWriteReorganization, model.StateDeleteReorganization:
		s.checkReorganizationIndex(c, ctx, d, tblInfo, handle, indexCol, row, isDropped)
	case model.StatePublic:
		s.checkPublicIndex(c, ctx, d, tblInfo, handle, indexCol, row)
	}
}

func (s *testIndexSuite) TestAddIndex(c *C) {
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

	checkOK := false

	tc := &testDDLCallback{}
	tc.onJobUpdated = func(job *model.Job) {
		if checkOK {
			return
		}

		t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)
		indexCol := getIndex(t, "c1")
		if indexCol == nil {
			return
		}

		s.checkAddOrDropIndex(c, indexCol.State, d, tblInfo, handle, indexCol, row, false)

		if indexCol.State == model.StatePublic {
			checkOK = true
		}
	}

	d.hook = tc

	// Use local ddl for callback test.
	s.d.close()

	d.close()
	d.start()

	job := testCreateIndex(c, ctx, d, s.dbInfo, tblInfo, true, "c1_uni", "c1")
	testCheckJobDone(c, d, job, true)

	job = testCreateIndex(c, ctx, d, s.dbInfo, tblInfo, true, "c1", "c1")
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

func (s *testIndexSuite) TestDropIndex(c *C) {
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

	job := testCreateIndex(c, ctx, s.d, s.dbInfo, tblInfo, true, "c1_uni", "c1")
	testCheckJobDone(c, d, job, true)

	err = ctx.FinishTxn(false)
	c.Assert(err, IsNil)

	checkOK := false
	oldIndexCol := &column.IndexedCol{}

	tc := &testDDLCallback{}
	tc.onJobUpdated = func(job *model.Job) {
		if checkOK {
			return
		}

		t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)
		indexCol := getIndex(t, "c1")
		if indexCol == nil {
			s.checkAddOrDropIndex(c, model.StateNone, d, tblInfo, handle, oldIndexCol, row, true)
			checkOK = true
			return
		}

		s.checkAddOrDropIndex(c, indexCol.State, d, tblInfo, handle, indexCol, row, true)
		oldIndexCol = indexCol
	}

	d.hook = tc

	// Use local ddl for callback test.
	s.d.close()

	d.close()
	d.start()

	job = testDropIndex(c, ctx, d, s.dbInfo, tblInfo, "c1_uni")
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
