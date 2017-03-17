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

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testIndexSuite{})

type testIndexSuite struct {
	store  kv.Storage
	dbInfo *model.DBInfo

	d *ddl
}

func (s *testIndexSuite) SetUpSuite(c *C) {
	s.store = testCreateStore(c, "test_index")
	s.d = newDDL(s.store, nil, nil, testLease)

	s.dbInfo = testSchemaInfo(c, s.d, "test_index")
	testCreateSchema(c, testNewContext(s.d), s.d, s.dbInfo)
}

func (s *testIndexSuite) TearDownSuite(c *C) {
	testDropSchema(c, testNewContext(s.d), s.d, s.dbInfo)
	s.d.Stop()

	err := s.store.Close()
	c.Assert(err, IsNil)
}

func testCreateIndex(c *C, ctx context.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, unique bool, indexName string, colName string) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionAddIndex,
		BinlogInfo: &model.HistoryInfo{},
		Args: []interface{}{unique, model.NewCIStr(indexName),
			[]*ast.IndexColName{{
				Column: &ast.ColumnName{Name: model.NewCIStr(colName)},
				Length: types.UnspecifiedLength}}},
	}

	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func testDropIndex(c *C, ctx context.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, indexName string) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionDropIndex,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{model.NewCIStr(indexName)},
	}

	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)
	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func (s *testIndexSuite) TestIndex(c *C) {
	defer testleak.AfterTest(c)()
	tblInfo := testTableInfo(c, s.d, "t1", 3)
	ctx := testNewContext(s.d)

	testCreateTable(c, ctx, s.d, s.dbInfo, tblInfo)

	t := testGetTable(c, s.d, s.dbInfo.ID, tblInfo.ID)
	err := ctx.NewTxn()
	c.Assert(err, IsNil)
	num := 10
	for i := 0; i < num; i++ {
		_, err = t.AddRecord(ctx, types.MakeDatums(i, i, i))
		c.Assert(err, IsNil)
	}

	c.Assert(ctx.NewTxn(), IsNil)

	i := int64(0)
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
		c.Assert(data[0].GetInt64(), Equals, i)
		i++
		return true, nil
	})

	job := testCreateIndex(c, ctx, s.d, s.dbInfo, tblInfo, true, "c1_uni", "c1")
	testCheckJobDone(c, s.d, job, true)

	t = testGetTable(c, s.d, s.dbInfo.ID, tblInfo.ID)
	index := tables.FindIndexByColName(t, "c1")
	c.Assert(index, NotNil)

	c.Assert(ctx.NewTxn(), IsNil)
	h, err := t.AddRecord(ctx, types.MakeDatums(num+1, 1, 1))
	c.Assert(err, IsNil)

	h1, err := t.AddRecord(ctx, types.MakeDatums(num+1, 1, 1))
	c.Assert(err, NotNil)
	c.Assert(h, Equals, h1)

	h, err = t.AddRecord(ctx, types.MakeDatums(1, 1, 1))
	c.Assert(err, NotNil)

	err = ctx.NewTxn()
	c.Assert(err, IsNil)

	exist, _, err := index.Exist(ctx.Txn(), types.MakeDatums(1), h)
	c.Assert(err, IsNil)
	c.Assert(exist, IsTrue)

	job = testDropIndex(c, ctx, s.d, s.dbInfo, tblInfo, "c1_uni")
	testCheckJobDone(c, s.d, job, false)

	t = testGetTable(c, s.d, s.dbInfo.ID, tblInfo.ID)
	index1 := tables.FindIndexByColName(t, "c1")
	c.Assert(index1, IsNil)

	err = ctx.NewTxn()
	c.Assert(err, IsNil)

	exist, _, err = index.Exist(ctx.Txn(), types.MakeDatums(1), h)
	c.Assert(err, IsNil)
	c.Assert(exist, IsFalse)

	_, err = t.AddRecord(ctx, types.MakeDatums(1, 1, 1))
	c.Assert(err, IsNil)
}

func getIndex(t table.Table, name string) table.Index {
	for _, idx := range t.Indices() {
		// only public index can be read.

		if len(idx.Meta().Columns) == 1 && strings.EqualFold(idx.Meta().Columns[0].Name.L, name) {
			return idx
		}
	}
	return nil
}

func (s *testIndexSuite) testGetIndex(t table.Table, name string, isExist bool) error {
	index := tables.FindIndexByColName(t, name)
	if isExist {
		if index == nil {
			return errors.Errorf("index exist got nil, expected not nil")
		}
		return nil
	}
	if index != nil {
		return errors.Errorf("index exist got %v, expected nil", index)
	}
	return nil
}

func (s *testIndexSuite) checkIndexKVExist(ctx context.Context, t table.Table, handle int64, indexCol table.Index, columnValues []types.Datum, isExist bool) error {
	idxColsLen := len(indexCol.Meta().Columns)
	if idxColsLen != len(columnValues) {
		return errors.Errorf("index columns length got %d, expected %d", idxColsLen, len(columnValues))
	}

	err := ctx.NewTxn()
	if err != nil {
		return errors.Trace(err)
	}

	exist, _, err := indexCol.Exist(ctx.Txn(), columnValues, handle)
	if err != nil {
		return errors.Trace(err)
	}
	if exist != isExist {
		return errors.Errorf("index exist got %v, expected %v", exist, isExist)
	}
	return nil
}

func (s *testIndexSuite) checkNoneIndex(ctx context.Context, d *ddl, tblInfo *model.TableInfo, handle int64, index table.Index, row []types.Datum) error {
	t, err := testGetTableWithError(d, s.dbInfo.ID, tblInfo.ID)
	if err != nil {
		return errors.Trace(err)
	}

	columnValues := make([]types.Datum, len(index.Meta().Columns))
	for i, column := range index.Meta().Columns {
		columnValues[i] = row[column.Offset]
	}

	err = s.checkIndexKVExist(ctx, t, handle, index, columnValues, false)
	if err != nil {
		return errors.Trace(err)
	}
	err = s.testGetIndex(t, index.Meta().Columns[0].Name.L, false)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *testIndexSuite) checkDeleteOnlyIndex(ctx context.Context, d *ddl, tblInfo *model.TableInfo, handle int64, index table.Index, row []types.Datum, isDropped bool) error {
	t, err := testGetTableWithError(d, s.dbInfo.ID, tblInfo.ID)
	if err != nil {
		return errors.Trace(err)
	}

	err = ctx.NewTxn()
	if err != nil {
		return errors.Trace(err)
	}
	err = checkResult(ctx, t, t.Cols(), [][]interface{}{datumsToInterfaces(row)})
	if err != nil {
		return errors.Trace(err)
	}
	columnValues := make([]types.Datum, len(index.Meta().Columns))
	for i, column := range index.Meta().Columns {
		columnValues[i] = row[column.Offset]
	}
	err = s.checkIndexKVExist(ctx, t, handle, index, columnValues, isDropped)
	if err != nil {
		return errors.Trace(err)
	}

	// Test add a new row.
	err = ctx.NewTxn()
	if err != nil {
		return errors.Trace(err)
	}
	newRow := types.MakeDatums(int64(11), int64(22), int64(33))
	handle, err = t.AddRecord(ctx, newRow)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn()
	if err != nil {
		return errors.Trace(err)
	}
	rows := [][]interface{}{datumsToInterfaces(row), datumsToInterfaces(newRow)}
	err = checkResult(ctx, t, t.Cols(), rows)
	if err != nil {
		return errors.Trace(err)
	}
	for i, column := range index.Meta().Columns {
		columnValues[i] = newRow[column.Offset]
	}
	err = s.checkIndexKVExist(ctx, t, handle, index, columnValues, false)
	if err != nil {
		return errors.Trace(err)
	}

	// Test update a new row.
	err = ctx.NewTxn()
	if err != nil {
		return errors.Trace(err)
	}
	newUpdateRow := types.MakeDatums(int64(44), int64(55), int64(66))
	touched := map[int]bool{0: true, 1: true, 2: true}
	err = t.UpdateRecord(ctx, handle, newRow, newUpdateRow, touched)
	if err != nil {
		return errors.Trace(err)
	}
	err = s.checkIndexKVExist(ctx, t, handle, index, columnValues, false)
	if err != nil {
		return errors.Trace(err)
	}
	for i, column := range index.Meta().Columns {
		columnValues[i] = newUpdateRow[column.Offset]
	}
	err = s.checkIndexKVExist(ctx, t, handle, index, columnValues, false)
	if err != nil {
		return errors.Trace(err)
	}

	// Test remove a row.
	err = ctx.NewTxn()
	if err != nil {
		return errors.Trace(err)
	}
	err = t.RemoveRecord(ctx, handle, newUpdateRow)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn()
	if err != nil {
		return errors.Trace(err)
	}
	count := 0
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
		count++
		return true, nil
	})
	if count != 1 {
		return errors.Errorf("count got %d, expected %d", count, 1)
	}

	err = s.checkIndexKVExist(ctx, t, handle, index, columnValues, false)
	if err != nil {
		return errors.Trace(err)
	}
	err = s.testGetIndex(t, index.Meta().Columns[0].Name.L, false)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *testIndexSuite) checkWriteOnlyIndex(ctx context.Context, d *ddl, tblInfo *model.TableInfo, handle int64, index table.Index, row []types.Datum, isDropped bool) error {
	t, err := testGetTableWithError(d, s.dbInfo.ID, tblInfo.ID)
	if err != nil {
		return errors.Trace(err)
	}

	err = ctx.NewTxn()
	if err != nil {
		return errors.Trace(err)
	}
	err = checkResult(ctx, t, t.Cols(), [][]interface{}{datumsToInterfaces(row)})
	if err != nil {
		return errors.Trace(err)
	}
	columnValues := make([]types.Datum, len(index.Meta().Columns))
	for i, column := range index.Meta().Columns {
		columnValues[i] = row[column.Offset]
	}
	err = s.checkIndexKVExist(ctx, t, handle, index, columnValues, isDropped)
	if err != nil {
		return errors.Trace(err)
	}

	// Test add a new row.
	err = ctx.NewTxn()
	if err != nil {
		return errors.Trace(err)
	}
	newRow := types.MakeDatums(int64(11), int64(22), int64(33))
	handle, err = t.AddRecord(ctx, newRow)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn()
	if err != nil {
		return errors.Trace(err)
	}
	rows := [][]interface{}{datumsToInterfaces(row), datumsToInterfaces(newRow)}
	err = checkResult(ctx, t, t.Cols(), rows)
	if err != nil {
		return errors.Trace(err)
	}
	for i, column := range index.Meta().Columns {
		columnValues[i] = newRow[column.Offset]
	}
	err = s.checkIndexKVExist(ctx, t, handle, index, columnValues, true)
	if err != nil {
		return errors.Trace(err)
	}

	// Test update a new row.
	err = ctx.NewTxn()
	if err != nil {
		return errors.Trace(err)
	}
	newUpdateRow := types.MakeDatums(int64(44), int64(55), int64(66))
	touched := map[int]bool{0: true, 1: true, 2: true}
	err = t.UpdateRecord(ctx, handle, newRow, newUpdateRow, touched)
	if err != nil {
		return errors.Trace(err)
	}
	err = s.checkIndexKVExist(ctx, t, handle, index, columnValues, false)
	if err != nil {
		return errors.Trace(err)
	}
	for i, column := range index.Meta().Columns {
		columnValues[i] = newUpdateRow[column.Offset]
	}
	err = s.checkIndexKVExist(ctx, t, handle, index, columnValues, true)
	if err != nil {
		return errors.Trace(err)
	}

	// Test remove a row.
	err = ctx.NewTxn()
	if err != nil {
		return errors.Trace(err)
	}
	err = t.RemoveRecord(ctx, handle, newUpdateRow)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn()
	if err != nil {
		return errors.Trace(err)
	}
	count := 0
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
		count++
		return true, nil
	})
	if count != 1 {
		return errors.Errorf("count got %d, expected %d", count, 1)
	}
	err = s.checkIndexKVExist(ctx, t, handle, index, columnValues, false)
	if err != nil {
		return errors.Trace(err)
	}
	err = s.testGetIndex(t, index.Meta().Columns[0].Name.L, false)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *testIndexSuite) checkReorganizationIndex(ctx context.Context, d *ddl, tblInfo *model.TableInfo, handle int64, index table.Index, row []types.Datum, isDropped bool) error {
	t, err := testGetTableWithError(d, s.dbInfo.ID, tblInfo.ID)
	if err != nil {
		return errors.Trace(err)
	}

	err = ctx.NewTxn()
	if err != nil {
		return errors.Trace(err)
	}
	err = checkResult(ctx, t, t.Cols(), [][]interface{}{datumsToInterfaces(row)})
	if err != nil {
		return errors.Trace(err)
	}

	// Test add a new row.
	err = ctx.NewTxn()
	if err != nil {
		return errors.Trace(err)
	}
	newRow := types.MakeDatums(int64(11), int64(22), int64(33))
	handle, err = t.AddRecord(ctx, newRow)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn()
	if err != nil {
		return errors.Trace(err)
	}
	rows := [][]interface{}{datumsToInterfaces(row), datumsToInterfaces(newRow)}
	err = checkResult(ctx, t, t.Cols(), rows)
	if err != nil {
		return errors.Trace(err)
	}
	columnValues := make([]types.Datum, len(index.Meta().Columns))
	for i, column := range index.Meta().Columns {
		columnValues[i] = newRow[column.Offset]
	}
	err = s.checkIndexKVExist(ctx, t, handle, index, columnValues, !isDropped)
	if err != nil {
		return errors.Trace(err)
	}

	// Test update a new row.
	err = ctx.NewTxn()
	if err != nil {
		return errors.Trace(err)
	}
	newUpdateRow := types.MakeDatums(int64(44), int64(55), int64(66))
	touched := map[int]bool{0: true, 1: true, 2: true}
	err = t.UpdateRecord(ctx, handle, newRow, newUpdateRow, touched)
	if err != nil {
		return errors.Trace(err)
	}
	err = s.checkIndexKVExist(ctx, t, handle, index, columnValues, false)
	if err != nil {
		return errors.Trace(err)
	}
	for i, column := range index.Meta().Columns {
		columnValues[i] = newUpdateRow[column.Offset]
	}
	err = s.checkIndexKVExist(ctx, t, handle, index, columnValues, !isDropped)
	if err != nil {
		return errors.Trace(err)
	}

	// Test remove a row.
	err = ctx.NewTxn()
	if err != nil {
		return errors.Trace(err)
	}
	err = t.RemoveRecord(ctx, handle, newUpdateRow)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn()
	if err != nil {
		return errors.Trace(err)
	}
	count := 0
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
		count++
		return true, nil
	})
	if count != 1 {
		return errors.Errorf("count got %d, expected %d", count, 1)
	}
	err = s.testGetIndex(t, index.Meta().Columns[0].Name.L, false)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *testIndexSuite) checkPublicIndex(ctx context.Context, d *ddl, tblInfo *model.TableInfo, handle int64, index table.Index, row []types.Datum) error {
	t, err := testGetTableWithError(d, s.dbInfo.ID, tblInfo.ID)
	if err != nil {
		return errors.Trace(err)
	}

	err = ctx.NewTxn()
	if err != nil {
		return errors.Trace(err)
	}
	err = checkResult(ctx, t, t.Cols(), [][]interface{}{datumsToInterfaces(row)})
	if err != nil {
		return errors.Trace(err)
	}
	columnValues := make([]types.Datum, len(index.Meta().Columns))
	for i, column := range index.Meta().Columns {
		columnValues[i] = row[column.Offset]
	}
	err = s.checkIndexKVExist(ctx, t, handle, index, columnValues, true)
	if err != nil {
		return errors.Trace(err)
	}

	// Test add a new row.
	err = ctx.NewTxn()
	if err != nil {
		return errors.Trace(err)
	}
	newRow := types.MakeDatums(int64(11), int64(22), int64(33))
	handle, err = t.AddRecord(ctx, newRow)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn()
	if err != nil {
		return errors.Trace(err)
	}
	rows := [][]interface{}{datumsToInterfaces(row), datumsToInterfaces(newRow)}
	err = checkResult(ctx, t, t.Cols(), rows)
	if err != nil {
		return errors.Trace(err)
	}
	for i, column := range index.Meta().Columns {
		columnValues[i] = newRow[column.Offset]
	}
	err = s.checkIndexKVExist(ctx, t, handle, index, columnValues, true)
	if err != nil {
		return errors.Trace(err)
	}

	// Test update a new row.
	err = ctx.NewTxn()
	if err != nil {
		return errors.Trace(err)
	}
	newUpdateRow := types.MakeDatums(int64(44), int64(55), int64(66))
	touched := map[int]bool{0: true, 1: true, 2: true}
	err = ctx.NewTxn()
	if err != nil {
		return errors.Trace(err)
	}
	err = t.UpdateRecord(ctx, handle, newRow, newUpdateRow, touched)
	if err != nil {
		return errors.Trace(err)
	}
	err = s.checkIndexKVExist(ctx, t, handle, index, columnValues, false)
	if err != nil {
		return errors.Trace(err)
	}
	for i, column := range index.Meta().Columns {
		columnValues[i] = newUpdateRow[column.Offset]
	}
	err = s.checkIndexKVExist(ctx, t, handle, index, columnValues, true)
	if err != nil {
		return errors.Trace(err)
	}

	// Test remove a row.
	err = ctx.NewTxn()
	if err != nil {
		return errors.Trace(err)
	}
	err = t.RemoveRecord(ctx, handle, newUpdateRow)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn()
	if err != nil {
		return errors.Trace(err)
	}
	count := 0
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []types.Datum, cols []*table.Column) (bool, error) {
		count++
		return true, nil
	})
	if count != 1 {
		return errors.Errorf("count got %d, expected %d", count, 1)
	}
	err = s.checkIndexKVExist(ctx, t, handle, index, columnValues, false)
	if err != nil {
		return errors.Trace(err)
	}
	err = s.testGetIndex(t, index.Meta().Columns[0].Name.L, true)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *testIndexSuite) checkAddOrDropIndex(state model.SchemaState, d *ddl, tblInfo *model.TableInfo, handle int64, index table.Index, row []types.Datum, isDropped bool) error {
	var err error
	ctx := testNewContext(d)

	switch state {
	case model.StateNone:
		err = s.checkNoneIndex(ctx, d, tblInfo, handle, index, row)
	case model.StateDeleteOnly:
		err = s.checkDeleteOnlyIndex(ctx, d, tblInfo, handle, index, row, isDropped)
	case model.StateWriteOnly:
		err = s.checkWriteOnlyIndex(ctx, d, tblInfo, handle, index, row, isDropped)
	case model.StateWriteReorganization, model.StateDeleteReorganization:
		err = s.checkReorganizationIndex(ctx, d, tblInfo, handle, index, row, isDropped)
	case model.StatePublic:
		err = s.checkPublicIndex(ctx, d, tblInfo, handle, index, row)
	}

	return errors.Trace(err)
}

func (s *testIndexSuite) TestAddIndex(c *C) {
	defer testleak.AfterTest(c)()
	d := newDDL(s.store, nil, nil, testLease)
	tblInfo := testTableInfo(c, d, "t", 3)
	ctx := testNewContext(d)
	testCreateTable(c, ctx, d, s.dbInfo, tblInfo)

	c.Assert(ctx.NewTxn(), IsNil)
	row := types.MakeDatums(int64(1), int64(2), int64(3))
	t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)
	handle, err := t.AddRecord(ctx, row)
	c.Assert(err, IsNil)

	err = ctx.NewTxn()
	c.Assert(err, IsNil)

	checkOK := false
	var checkErr error
	tc := &testDDLCallback{}
	tc.onJobUpdated = func(job *model.Job) {
		if checkOK {
			return
		}

		t, err := testGetTableWithError(d, s.dbInfo.ID, tblInfo.ID)
		if err != nil && checkErr == nil {
			checkErr = errors.Trace(err)
			return
		}
		index := getIndex(t, "c1")
		if index == nil {
			return
		}

		err = s.checkAddOrDropIndex(index.Meta().State, d, tblInfo, handle, index, row, false)
		if err != nil && checkErr == nil {
			checkErr = errors.Trace(err)
			return
		}
		if index.Meta().State == model.StatePublic {
			checkOK = true
		}
	}

	d.setHook(tc)

	// Use local ddl for callback test.
	s.d.Stop()

	d.Stop()
	d.start()

	job := testCreateIndex(c, ctx, d, s.dbInfo, tblInfo, true, "c1_uni", "c1")
	c.Assert(errors.ErrorStack(checkErr), Equals, "")
	testCheckJobDone(c, d, job, true)

	job = testCreateIndex(c, ctx, d, s.dbInfo, tblInfo, true, "c1", "c1")
	c.Assert(errors.ErrorStack(checkErr), Equals, "")
	testCheckJobDone(c, d, job, true)
	err = ctx.NewTxn()
	c.Assert(err, IsNil)

	job = testDropTable(c, ctx, d, s.dbInfo, tblInfo)
	testCheckJobDone(c, d, job, false)
	err = ctx.Txn().Commit()
	c.Assert(err, IsNil)

	d.Stop()
	s.d.start()
}

func (s *testIndexSuite) TestDropIndex(c *C) {
	defer testleak.AfterTest(c)()
	d := newDDL(s.store, nil, nil, testLease)
	tblInfo := testTableInfo(c, d, "t", 3)
	ctx := testNewContext(d)

	err := ctx.NewTxn()
	c.Assert(err, IsNil)

	testCreateTable(c, ctx, d, s.dbInfo, tblInfo)

	row := types.MakeDatums(int64(1), int64(2), int64(3))
	t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)
	handle, err := t.AddRecord(ctx, row)
	c.Assert(err, IsNil)

	err = ctx.NewTxn()
	c.Assert(err, IsNil)

	job := testCreateIndex(c, ctx, s.d, s.dbInfo, tblInfo, true, "c1_uni", "c1")
	testCheckJobDone(c, d, job, true)

	checkOK := false
	var checkErr error
	oldIndexCol := tables.NewIndex(tblInfo, &model.IndexInfo{})
	tc := &testDDLCallback{}
	tc.onJobUpdated = func(job *model.Job) {
		if checkOK {
			return
		}

		t, err := testGetTableWithError(d, s.dbInfo.ID, tblInfo.ID)
		if err != nil && checkErr == nil {
			checkErr = errors.Trace(err)
			return
		}
		index := getIndex(t, "c1")
		if index == nil {
			err = s.checkAddOrDropIndex(model.StateNone, d, tblInfo, handle, oldIndexCol, row, true)
			if err != nil && checkErr == nil {
				checkErr = errors.Trace(err)
				return
			}
			checkOK = true
			return
		}

		err = s.checkAddOrDropIndex(index.Meta().State, d, tblInfo, handle, index, row, true)
		if err != nil && checkErr == nil {
			checkErr = errors.Trace(err)
			return
		}
		oldIndexCol = index
	}

	d.hookMu.Lock()
	d.hook = tc
	d.hookMu.Unlock()

	// Use local ddl for callback test.
	s.d.Stop()

	d.Stop()
	d.start()

	job = testDropIndex(c, ctx, d, s.dbInfo, tblInfo, "c1_uni")
	c.Assert(errors.ErrorStack(checkErr), Equals, "")
	testCheckJobDone(c, d, job, false)

	err = ctx.NewTxn()
	c.Assert(err, IsNil)

	job = testDropTable(c, ctx, d, s.dbInfo, tblInfo)
	c.Assert(errors.ErrorStack(checkErr), Equals, "")
	testCheckJobDone(c, d, job, false)
	err = ctx.Txn().Commit()
	c.Assert(err, IsNil)

	d.Stop()
	s.d.start()
}

func (s *testIndexSuite) TestAddIndexWithNullColumn(c *C) {
	defer testleak.AfterTest(c)()
	d := newDDL(s.store, nil, nil, testLease)
	tblInfo := testTableInfo(c, d, "t", 3)
	// Change c2.DefaultValue to nil
	tblInfo.Columns[1].DefaultValue = nil
	ctx := testNewContext(d)

	err := ctx.NewTxn()
	c.Assert(err, IsNil)

	testCreateTable(c, ctx, d, s.dbInfo, tblInfo)

	// c2 is nil, which is not stored in kv.
	row := types.MakeDatums(int64(1), nil, int(2))
	t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)
	handle, err := t.AddRecord(ctx, row)
	c.Assert(err, IsNil)

	err = ctx.NewTxn()
	c.Assert(err, IsNil)

	checkOK := false
	var checkErr error
	tc := &testDDLCallback{}
	tc.onJobUpdated = func(job *model.Job) {
		if checkOK {
			return
		}

		t, err := testGetTableWithError(d, s.dbInfo.ID, tblInfo.ID)
		if err != nil && checkErr == nil {
			checkErr = errors.Trace(err)
			return
		}
		// Add index on c2.
		index := getIndex(t, "c2")
		if index == nil {
			return
		}
		err = s.checkAddOrDropIndex(index.Meta().State, d, tblInfo, handle, index, row, false)
		if err != nil && checkErr == nil {
			checkErr = errors.Trace(err)
			return
		}
		if index.Meta().State == model.StatePublic {
			checkOK = true
		}
	}

	d.hookMu.Lock()
	d.hook = tc
	d.hookMu.Unlock()

	// Use local ddl for callback test.
	s.d.Stop()
	d.Stop()
	d.start()

	job := testCreateIndex(c, ctx, d, s.dbInfo, tblInfo, true, "c2", "c2")
	c.Assert(errors.ErrorStack(checkErr), Equals, "")
	testCheckJobDone(c, d, job, true)

	c.Assert(ctx.NewTxn(), IsNil)

	job = testDropTable(c, ctx, d, s.dbInfo, tblInfo)
	c.Assert(errors.ErrorStack(checkErr), Equals, "")
	testCheckJobDone(c, d, job, false)

	err = ctx.Txn().Commit()
	c.Assert(err, IsNil)

	d.Stop()
	s.d.start()
}
