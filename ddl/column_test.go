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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type testColumnSuiteToVerify struct {
	suite.Suite
	store  kv.Storage
	dbInfo *model.DBInfo
}

func TestColumnSuite(t *testing.T) {
	suite.Run(t, new(testColumnSuiteToVerify))
}

func (s *testColumnSuiteToVerify) SetupSuite() {
	s.store = createMockStore(s.T())
	d, err := testNewDDLAndStart(
		context.Background(),
		WithStore(s.store),
		WithLease(testLease),
	)
	require.NoError(s.T(), err)

	s.dbInfo, err = testSchemaInfo(d, "test_column")
	require.NoError(s.T(), err)
	testCreateSchema(s.T(), testNewContext(d), d, s.dbInfo)
	require.Nil(s.T(), d.Stop())
}

func (s *testColumnSuiteToVerify) TearDownSuite() {
	err := s.store.Close()
	require.NoError(s.T(), err)
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

func testCreateColumn(t *testing.T, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo,
	colName string, pos *ast.ColumnPosition, defaultValue interface{}) *model.Job {
	job := buildCreateColumnJob(dbInfo, tblInfo, colName, pos, defaultValue)
	ctx.SetValue(sessionctx.QueryString, "skip")
	err := d.doDDLJob(ctx, job)
	require.NoError(t, err)
	v := getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func buildCreateColumnsJob(dbInfo *model.DBInfo, tblInfo *model.TableInfo, colNames []string,
	positions []*ast.ColumnPosition, defaultValue interface{}) *model.Job {
	colInfos := make([]*model.ColumnInfo, len(colNames))
	offsets := make([]int, len(colNames))
	ifNotExists := make([]bool, len(colNames))
	for i, colName := range colNames {
		col := &model.ColumnInfo{
			Name:               model.NewCIStr(colName),
			Offset:             len(tblInfo.Columns),
			DefaultValue:       defaultValue,
			OriginDefaultValue: defaultValue,
		}
		col.ID = allocateColumnID(tblInfo)
		col.FieldType = *types.NewFieldType(mysql.TypeLong)
		colInfos[i] = col
	}

	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionAddColumns,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{colInfos, positions, offsets, ifNotExists},
	}
	return job
}

func testCreateColumns(t *testing.T, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo,
	colNames []string, positions []*ast.ColumnPosition, defaultValue interface{}) *model.Job {
	job := buildCreateColumnsJob(dbInfo, tblInfo, colNames, positions, defaultValue)
	ctx.SetValue(sessionctx.QueryString, "skip")
	err := d.doDDLJob(ctx, job)
	require.NoError(t, err)
	v := getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func buildDropColumnJob(dbInfo *model.DBInfo, tblInfo *model.TableInfo, colName string) *model.Job {
	return &model.Job{
		SchemaID:        dbInfo.ID,
		TableID:         tblInfo.ID,
		Type:            model.ActionDropColumn,
		BinlogInfo:      &model.HistoryInfo{},
		MultiSchemaInfo: &model.MultiSchemaInfo{},
		Args:            []interface{}{model.NewCIStr(colName)},
	}
}

func testDropColumn(t *testing.T, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, colName string, isError bool) *model.Job {
	job := buildDropColumnJob(dbInfo, tblInfo, colName)
	ctx.SetValue(sessionctx.QueryString, "skip")
	err := d.doDDLJob(ctx, job)
	if isError {
		require.Error(t, err)
		return nil
	}
	require.NoError(t, err)
	v := getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func buildDropColumnsJob(dbInfo *model.DBInfo, tblInfo *model.TableInfo, colNames []string) *model.Job {
	columnNames := make([]model.CIStr, len(colNames))
	ifExists := make([]bool, len(colNames))
	for i, colName := range colNames {
		columnNames[i] = model.NewCIStr(colName)
	}
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionDropColumns,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{columnNames, ifExists},
	}
	return job
}

func testDropColumns(t *testing.T, ctx sessionctx.Context, d *ddl, dbInfo *model.DBInfo, tblInfo *model.TableInfo, colNames []string, isError bool) *model.Job {
	job := buildDropColumnsJob(dbInfo, tblInfo, colNames)
	ctx.SetValue(sessionctx.QueryString, "skip")
	err := d.doDDLJob(ctx, job)
	if isError {
		require.Error(t, err)
		return nil
	}
	require.NoError(t, err)
	v := getSchemaVer(t, ctx)
	checkHistoryJobArgs(t, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func (s *testColumnSuiteToVerify) TestColumnBasic() {
	d, err := testNewDDLAndStart(
		context.Background(),
		WithStore(s.store),
		WithLease(testLease),
	)
	require.NoError(s.T(), err)
	defer func() {
		err := d.Stop()
		require.NoError(s.T(), err)
	}()

	tblInfo, err := testTableInfo(d, "t1", 3)
	require.NoError(s.T(), err)
	ctx := testNewContext(d)

	testCreateTable(s.T(), ctx, d, s.dbInfo, tblInfo)
	t := testGetTable(s.T(), d, s.dbInfo.ID, tblInfo.ID)

	num := 10
	for i := 0; i < num; i++ {
		_, err := t.AddRecord(ctx, types.MakeDatums(i, 10*i, 100*i))
		require.NoError(s.T(), err)
	}

	err = ctx.NewTxn(context.Background())
	require.NoError(s.T(), err)

	i := int64(0)
	err = tables.IterRecords(t, ctx, t.Cols(), func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
		require.Len(s.T(), data, 3)
		require.Equal(s.T(), data[0].GetInt64(), i)
		require.Equal(s.T(), data[1].GetInt64(), 10*i)
		require.Equal(s.T(), data[2].GetInt64(), 100*i)
		i++
		return true, nil
	})
	require.NoError(s.T(), err)
	require.Equal(s.T(), i, int64(num))

	require.Nil(s.T(), table.FindCol(t.Cols(), "c4"))

	job := testCreateColumn(s.T(), ctx, d, s.dbInfo, tblInfo, "c4", &ast.ColumnPosition{Tp: ast.ColumnPositionAfter, RelativeColumn: &ast.ColumnName{Name: model.NewCIStr("c3")}}, 100)
	testCheckJobDone(s.T(), d, job, true)

	t = testGetTable(s.T(), d, s.dbInfo.ID, tblInfo.ID)
	require.NotNil(s.T(), table.FindCol(t.Cols(), "c4"))

	i = int64(0)
	err = tables.IterRecords(t, ctx, t.Cols(),
		func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
			require.Len(s.T(), data, 4)
			require.Equal(s.T(), data[0].GetInt64(), i)
			require.Equal(s.T(), data[1].GetInt64(), 10*i)
			require.Equal(s.T(), data[2].GetInt64(), 100*i)
			require.Equal(s.T(), data[3].GetInt64(), int64(100))
			i++
			return true, nil
		})
	require.NoError(s.T(), err)
	require.Equal(s.T(), i, int64(num))

	h, err := t.AddRecord(ctx, types.MakeDatums(11, 12, 13, 14))
	require.NoError(s.T(), err)
	err = ctx.NewTxn(context.Background())
	require.NoError(s.T(), err)
	values, err := tables.RowWithCols(t, ctx, h, t.Cols())
	require.NoError(s.T(), err)

	require.Len(s.T(), values, 4)
	require.Equal(s.T(), values[3].GetInt64(), int64(14))

	job = testDropColumn(s.T(), ctx, d, s.dbInfo, tblInfo, "c4", false)
	testCheckJobDone(s.T(), d, job, false)

	t = testGetTable(s.T(), d, s.dbInfo.ID, tblInfo.ID)
	values, err = tables.RowWithCols(t, ctx, h, t.Cols())
	require.NoError(s.T(), err)

	require.Len(s.T(), values, 3)
	require.Equal(s.T(), values[2].GetInt64(), int64(13))

	job = testCreateColumn(s.T(), ctx, d, s.dbInfo, tblInfo, "c4", &ast.ColumnPosition{Tp: ast.ColumnPositionNone}, 111)
	testCheckJobDone(s.T(), d, job, true)

	t = testGetTable(s.T(), d, s.dbInfo.ID, tblInfo.ID)
	values, err = tables.RowWithCols(t, ctx, h, t.Cols())
	require.NoError(s.T(), err)

	require.Len(s.T(), values, 4)
	require.Equal(s.T(), values[3].GetInt64(), int64(111))

	job = testCreateColumn(s.T(), ctx, d, s.dbInfo, tblInfo, "c5", &ast.ColumnPosition{Tp: ast.ColumnPositionNone}, 101)
	testCheckJobDone(s.T(), d, job, true)

	t = testGetTable(s.T(), d, s.dbInfo.ID, tblInfo.ID)
	values, err = tables.RowWithCols(t, ctx, h, t.Cols())
	require.NoError(s.T(), err)

	require.Len(s.T(), values, 5)
	require.Equal(s.T(), values[4].GetInt64(), int64(101))

	job = testCreateColumn(s.T(), ctx, d, s.dbInfo, tblInfo, "c6", &ast.ColumnPosition{Tp: ast.ColumnPositionFirst}, 202)
	testCheckJobDone(s.T(), d, job, true)

	t = testGetTable(s.T(), d, s.dbInfo.ID, tblInfo.ID)
	cols := t.Cols()
	require.Len(s.T(), cols, 6)
	require.Equal(s.T(), cols[0].Offset, 0)
	require.Equal(s.T(), cols[0].Name.L, "c6")
	require.Equal(s.T(), cols[1].Offset, 1)
	require.Equal(s.T(), cols[1].Name.L, "c1")
	require.Equal(s.T(), cols[2].Offset, 2)
	require.Equal(s.T(), cols[2].Name.L, "c2")
	require.Equal(s.T(), cols[3].Offset, 3)
	require.Equal(s.T(), cols[3].Name.L, "c3")
	require.Equal(s.T(), cols[4].Offset, 4)
	require.Equal(s.T(), cols[4].Name.L, "c4")
	require.Equal(s.T(), cols[5].Offset, 5)
	require.Equal(s.T(), cols[5].Name.L, "c5")

	values, err = tables.RowWithCols(t, ctx, h, cols)
	require.NoError(s.T(), err)

	require.Len(s.T(), values, 6)
	require.Equal(s.T(), values[0].GetInt64(), int64(202))
	require.Equal(s.T(), values[5].GetInt64(), int64(101))

	job = testDropColumn(s.T(), ctx, d, s.dbInfo, tblInfo, "c2", false)
	testCheckJobDone(s.T(), d, job, false)

	t = testGetTable(s.T(), d, s.dbInfo.ID, tblInfo.ID)

	values, err = tables.RowWithCols(t, ctx, h, t.Cols())
	require.NoError(s.T(), err)
	require.Len(s.T(), values, 5)
	require.Equal(s.T(), values[0].GetInt64(), int64(202))
	require.Equal(s.T(), values[4].GetInt64(), int64(101))

	job = testDropColumn(s.T(), ctx, d, s.dbInfo, tblInfo, "c1", false)
	testCheckJobDone(s.T(), d, job, false)

	job = testDropColumn(s.T(), ctx, d, s.dbInfo, tblInfo, "c3", false)
	testCheckJobDone(s.T(), d, job, false)

	job = testDropColumn(s.T(), ctx, d, s.dbInfo, tblInfo, "c4", false)
	testCheckJobDone(s.T(), d, job, false)

	job = testCreateIndex(s.T(), ctx, d, s.dbInfo, tblInfo, false, "c5_idx", "c5")
	testCheckJobDone(s.T(), d, job, true)

	job = testDropColumn(s.T(), ctx, d, s.dbInfo, tblInfo, "c5", false)
	testCheckJobDone(s.T(), d, job, false)

	testDropColumn(s.T(), ctx, d, s.dbInfo, tblInfo, "c6", true)

	testDropTable(s.T(), ctx, d, s.dbInfo, tblInfo)
}

func (s *testColumnSuiteToVerify) checkColumnKVExist(ctx sessionctx.Context, t table.Table, handle kv.Handle, col *table.Column, columnValue interface{}, isExist bool) error {
	err := ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		if txn, err1 := ctx.Txn(true); err1 == nil {
			err = txn.Commit(context.Background())
			if err != nil {
				panic(err)
			}
		}
	}()
	key := tablecodec.EncodeRecordKey(t.RecordPrefix(), handle)
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
	rowMap, err := tablecodec.DecodeRowToDatumMap(data, colMap, ctx.GetSessionVars().Location())
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

func (s *testColumnSuiteToVerify) checkNoneColumn(ctx sessionctx.Context, d *ddl, tblInfo *model.TableInfo, handle kv.Handle, col *table.Column, columnValue interface{}) error {
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

func (s *testColumnSuiteToVerify) checkDeleteOnlyColumn(ctx sessionctx.Context, d *ddl, tblInfo *model.TableInfo, handle kv.Handle, col *table.Column, row []types.Datum, columnValue interface{}) error {
	t, err := testGetTableWithError(d, s.dbInfo.ID, tblInfo.ID)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	i := int64(0)
	err = tables.IterRecords(t, ctx, t.Cols(), func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
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
	err = tables.IterRecords(t, ctx, t.Cols(), func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
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
	err = tables.IterRecords(t, ctx, t.Cols(), func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
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

func (s *testColumnSuiteToVerify) checkWriteOnlyColumn(ctx sessionctx.Context, d *ddl, tblInfo *model.TableInfo, handle kv.Handle, col *table.Column, row []types.Datum, columnValue interface{}) error {
	t, err := testGetTableWithError(d, s.dbInfo.ID, tblInfo.ID)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	i := int64(0)
	err = tables.IterRecords(t, ctx, t.Cols(), func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
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
	err = tables.IterRecords(t, ctx, t.Cols(), func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
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
	err = tables.IterRecords(t, ctx, t.Cols(), func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
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

func (s *testColumnSuiteToVerify) checkReorganizationColumn(ctx sessionctx.Context, d *ddl, tblInfo *model.TableInfo, col *table.Column, row []types.Datum, columnValue interface{}) error {
	t, err := testGetTableWithError(d, s.dbInfo.ID, tblInfo.ID)
	if err != nil {
		return errors.Trace(err)
	}
	err = ctx.NewTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	i := int64(0)
	err = tables.IterRecords(t, ctx, t.Cols(), func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
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
	err = tables.IterRecords(t, ctx, t.Cols(), func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
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
	err = tables.IterRecords(t, ctx, t.Cols(), func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
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

func (s *testColumnSuiteToVerify) checkPublicColumn(ctx sessionctx.Context, d *ddl, tblInfo *model.TableInfo, newCol *table.Column, oldRow []types.Datum, columnValue interface{}) error {
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
	err = tables.IterRecords(t, ctx, t.Cols(), func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
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
	err = tables.IterRecords(t, ctx, t.Cols(), func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
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
	err = tables.IterRecords(t, ctx, t.Cols(), func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
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

func (s *testColumnSuiteToVerify) checkAddColumn(state model.SchemaState, d *ddl, tblInfo *model.TableInfo, handle kv.Handle, newCol *table.Column, oldRow []types.Datum, columnValue interface{}) error {
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
		err = errors.Trace(s.checkReorganizationColumn(ctx, d, tblInfo, newCol, oldRow, columnValue))
	case model.StatePublic:
		err = errors.Trace(s.checkPublicColumn(ctx, d, tblInfo, newCol, oldRow, columnValue))
	}
	return err
}

func (s *testColumnSuiteToVerify) testGetColumn(t table.Table, name string, isExist bool) error {
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

func (s *testColumnSuiteToVerify) TestAddColumn() {
	d, err := testNewDDLAndStart(
		context.Background(),
		WithStore(s.store),
		WithLease(testLease),
	)
	require.NoError(s.T(), err)
	tblInfo, err := testTableInfo(d, "t", 3)
	require.NoError(s.T(), err)
	ctx := testNewContext(d)

	err = ctx.NewTxn(context.Background())
	require.NoError(s.T(), err)

	testCreateTable(s.T(), ctx, d, s.dbInfo, tblInfo)
	t := testGetTable(s.T(), d, s.dbInfo.ID, tblInfo.ID)

	oldRow := types.MakeDatums(int64(1), int64(2), int64(3))
	handle, err := t.AddRecord(ctx, oldRow)
	require.NoError(s.T(), err)

	txn, err := ctx.Txn(true)
	require.NoError(s.T(), err)
	err = txn.Commit(context.Background())
	require.NoError(s.T(), err)

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

	job := testCreateColumn(s.T(), ctx, d, s.dbInfo, tblInfo, newColName, &ast.ColumnPosition{Tp: ast.ColumnPositionNone}, defaultColValue)

	testCheckJobDone(s.T(), d, job, true)
	mu.Lock()
	hErr := hookErr
	ok := checkOK
	mu.Unlock()
	require.NoError(s.T(), hErr)
	require.True(s.T(), ok)

	err = ctx.NewTxn(context.Background())
	require.NoError(s.T(), err)

	job = testDropTable(s.T(), ctx, d, s.dbInfo, tblInfo)
	testCheckJobDone(s.T(), d, job, false)

	txn, err = ctx.Txn(true)
	require.NoError(s.T(), err)
	err = txn.Commit(context.Background())
	require.NoError(s.T(), err)

	err = d.Stop()
	require.NoError(s.T(), err)
}

func (s *testColumnSuiteToVerify) TestAddColumns() {
	d, err := testNewDDLAndStart(
		context.Background(),
		WithStore(s.store),
		WithLease(testLease),
	)
	require.NoError(s.T(), err)
	tblInfo, err := testTableInfo(d, "t", 3)
	require.NoError(s.T(), err)
	ctx := testNewContext(d)

	err = ctx.NewTxn(context.Background())
	require.NoError(s.T(), err)

	testCreateTable(s.T(), ctx, d, s.dbInfo, tblInfo)
	t := testGetTable(s.T(), d, s.dbInfo.ID, tblInfo.ID)

	oldRow := types.MakeDatums(int64(1), int64(2), int64(3))
	handle, err := t.AddRecord(ctx, oldRow)
	require.NoError(s.T(), err)

	txn, err := ctx.Txn(true)
	require.NoError(s.T(), err)
	err = txn.Commit(context.Background())
	require.NoError(s.T(), err)

	newColNames := []string{"c4,c5,c6"}
	positions := make([]*ast.ColumnPosition, 3)
	for i := range positions {
		positions[i] = &ast.ColumnPosition{Tp: ast.ColumnPositionNone}
	}
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
		for _, newColName := range newColNames {
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
	}

	d.SetHook(tc)

	job := testCreateColumns(s.T(), ctx, d, s.dbInfo, tblInfo, newColNames, positions, defaultColValue)

	testCheckJobDone(s.T(), d, job, true)
	mu.Lock()
	hErr := hookErr
	ok := checkOK
	mu.Unlock()
	require.NoError(s.T(), hErr)
	require.True(s.T(), ok)

	job = testDropTable(s.T(), ctx, d, s.dbInfo, tblInfo)
	testCheckJobDone(s.T(), d, job, false)
	err = d.Stop()
	require.NoError(s.T(), err)
}

func (s *testColumnSuiteToVerify) TestDropColumn() {
	d, err := testNewDDLAndStart(
		context.Background(),
		WithStore(s.store),
		WithLease(testLease),
	)
	require.NoError(s.T(), err)
	tblInfo, err := testTableInfo(d, "t2", 4)
	require.NoError(s.T(), err)
	ctx := testNewContext(d)

	err = ctx.NewTxn(context.Background())
	require.NoError(s.T(), err)

	testCreateTable(s.T(), ctx, d, s.dbInfo, tblInfo)
	t := testGetTable(s.T(), d, s.dbInfo.ID, tblInfo.ID)

	colName := "c4"
	defaultColValue := int64(4)
	row := types.MakeDatums(int64(1), int64(2), int64(3))
	_, err = t.AddRecord(ctx, append(row, types.NewDatum(defaultColValue)))
	require.NoError(s.T(), err)

	txn, err := ctx.Txn(true)
	require.NoError(s.T(), err)
	err = txn.Commit(context.Background())
	require.NoError(s.T(), err)

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

	job := testDropColumn(s.T(), ctx, d, s.dbInfo, tblInfo, colName, false)
	testCheckJobDone(s.T(), d, job, false)
	mu.Lock()
	hErr := hookErr
	ok := checkOK
	mu.Unlock()
	require.NoError(s.T(), hErr)
	require.True(s.T(), ok)

	err = ctx.NewTxn(context.Background())
	require.NoError(s.T(), err)

	job = testDropTable(s.T(), ctx, d, s.dbInfo, tblInfo)
	testCheckJobDone(s.T(), d, job, false)

	txn, err = ctx.Txn(true)
	require.NoError(s.T(), err)
	err = txn.Commit(context.Background())
	require.NoError(s.T(), err)

	err = d.Stop()
	require.NoError(s.T(), err)
}

func (s *testColumnSuiteToVerify) TestDropColumns() {
	d, err := testNewDDLAndStart(
		context.Background(),
		WithStore(s.store),
		WithLease(testLease),
	)
	require.NoError(s.T(), err)
	tblInfo, err := testTableInfo(d, "t2", 4)
	require.NoError(s.T(), err)
	ctx := testNewContext(d)

	err = ctx.NewTxn(context.Background())
	require.NoError(s.T(), err)

	testCreateTable(s.T(), ctx, d, s.dbInfo, tblInfo)
	t := testGetTable(s.T(), d, s.dbInfo.ID, tblInfo.ID)

	colNames := []string{"c3", "c4"}
	defaultColValue := int64(4)
	row := types.MakeDatums(int64(1), int64(2), int64(3))
	_, err = t.AddRecord(ctx, append(row, types.NewDatum(defaultColValue)))
	require.NoError(s.T(), err)

	txn, err := ctx.Txn(true)
	require.NoError(s.T(), err)
	err = txn.Commit(context.Background())
	require.NoError(s.T(), err)

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
		for _, colName := range colNames {
			col := table.FindCol(t.(*tables.TableCommon).Columns, colName)
			if col == nil {
				checkOK = true
				return
			}
		}
	}

	d.SetHook(tc)

	job := testDropColumns(s.T(), ctx, d, s.dbInfo, tblInfo, colNames, false)
	testCheckJobDone(s.T(), d, job, false)
	mu.Lock()
	hErr := hookErr
	ok := checkOK
	mu.Unlock()
	require.NoError(s.T(), hErr)
	require.True(s.T(), ok)

	job = testDropTable(s.T(), ctx, d, s.dbInfo, tblInfo)
	testCheckJobDone(s.T(), d, job, false)
	err = d.Stop()
	require.NoError(s.T(), err)
}

func TestModifyColumn(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	d, err := testNewDDLAndStart(
		context.Background(),
		WithStore(store),
		WithLease(testLease),
	)

	require.NoError(t, err)
	ctx := testNewContext(d)

	defer func() {
		err := d.Stop()
		require.NoError(t, err)
		err = store.Close()
		require.NoError(t, err)
	}()

	tests := []struct {
		origin string
		to     string
		err    error
	}{
		{"int", "bigint", nil},
		{"int", "int unsigned", nil},
		{"varchar(10)", "text", nil},
		{"varbinary(10)", "blob", nil},
		{"text", "blob", dbterror.ErrUnsupportedModifyCharset.GenWithStackByArgs("charset from utf8mb4 to binary")},
		{"varchar(10)", "varchar(8)", nil},
		{"varchar(10)", "varchar(11)", nil},
		{"varchar(10) character set utf8 collate utf8_bin", "varchar(10) character set utf8", nil},
		{"decimal(2,1)", "decimal(3,2)", nil},
		{"decimal(2,1)", "decimal(2,2)", nil},
		{"decimal(2,1)", "decimal(2,1)", nil},
		{"decimal(2,1)", "int", nil},
		{"decimal", "int", nil},
		{"decimal(2,1)", "bigint", nil},
		{"int", "varchar(10) character set gbk", dbterror.ErrUnsupportedModifyCharset.GenWithStackByArgs("charset from binary to gbk")},
		{"varchar(10) character set gbk", "int", dbterror.ErrUnsupportedModifyCharset.GenWithStackByArgs("charset from gbk to binary")},
		{"varchar(10) character set gbk", "varchar(10) character set utf8", dbterror.ErrUnsupportedModifyCharset.GenWithStackByArgs("charset from gbk to utf8")},
		{"varchar(10) character set gbk", "char(10) character set utf8", dbterror.ErrUnsupportedModifyCharset.GenWithStackByArgs("charset from gbk to utf8")},
		{"varchar(10) character set utf8", "char(10) character set gbk", dbterror.ErrUnsupportedModifyCharset.GenWithStackByArgs("charset from utf8 to gbk")},
		{"varchar(10) character set utf8", "varchar(10) character set gbk", dbterror.ErrUnsupportedModifyCharset.GenWithStackByArgs("charset from utf8 to gbk")},
		{"varchar(10) character set gbk", "varchar(255) character set gbk", nil},
	}
	for _, tt := range tests {
		ftA := colDefStrToFieldType(t, tt.origin, ctx)
		ftB := colDefStrToFieldType(t, tt.to, ctx)
		err := checkModifyTypes(ctx, ftA, ftB, false)
		if err == nil {
			require.NoErrorf(t, tt.err, "origin:%v, to:%v", tt.origin, tt.to)
		} else {
			require.EqualError(t, err, tt.err.Error())
		}
	}
}

func colDefStrToFieldType(t *testing.T, str string, ctx sessionctx.Context) *types.FieldType {
	sqlA := "alter table t modify column a " + str
	stmt, err := parser.New().ParseOneStmt(sqlA, "", "")
	require.NoError(t, err)
	colDef := stmt.(*ast.AlterTableStmt).Specs[0].NewColumns[0]
	chs, coll := charset.GetDefaultCharsetAndCollate()
	col, _, err := buildColumnAndConstraint(ctx, 0, colDef, nil, chs, coll)
	require.NoError(t, err)
	return &col.FieldType
}

func TestFieldCase(t *testing.T) {
	var fields = []string{"field", "Field"}
	colObjects := make([]*model.ColumnInfo, len(fields))
	for i, name := range fields {
		colObjects[i] = &model.ColumnInfo{
			Name: model.NewCIStr(name),
		}
	}
	err := checkDuplicateColumn(colObjects)
	require.EqualError(t, err, infoschema.ErrColumnExists.GenWithStackByArgs("Field").Error())
}

func (s *testColumnSuiteToVerify) TestAutoConvertBlobTypeByLength() {
	d, err := testNewDDLAndStart(
		context.Background(),
		WithStore(s.store),
		WithLease(testLease),
	)
	require.NoError(s.T(), err)
	// Close the customized ddl(worker goroutine included) after the test is finished, otherwise, it will
	// cause go routine in TiDB leak test.
	defer func() {
		err := d.Stop()
		require.NoError(s.T(), err)
	}()

	sql := fmt.Sprintf("create table t0(c0 Blob(%d), c1 Blob(%d), c2 Blob(%d), c3 Blob(%d))",
		tinyBlobMaxLength-1, blobMaxLength-1, mediumBlobMaxLength-1, longBlobMaxLength-1)
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(s.T(), err)
	tblInfo, err := BuildTableInfoFromAST(stmt.(*ast.CreateTableStmt))
	require.NoError(s.T(), err)
	genIDs, err := d.genGlobalIDs(1)
	require.NoError(s.T(), err)
	tblInfo.ID = genIDs[0]

	ctx := testNewContext(d)
	err = ctx.NewTxn(context.Background())
	require.NoError(s.T(), err)
	testCreateTable(s.T(), ctx, d, s.dbInfo, tblInfo)
	t := testGetTable(s.T(), d, s.dbInfo.ID, tblInfo.ID)

	require.Equal(s.T(), t.Cols()[0].Tp, mysql.TypeTinyBlob)
	require.Equal(s.T(), t.Cols()[0].Flen, tinyBlobMaxLength)
	require.Equal(s.T(), t.Cols()[1].Tp, mysql.TypeBlob)
	require.Equal(s.T(), t.Cols()[1].Flen, blobMaxLength)
	require.Equal(s.T(), t.Cols()[2].Tp, mysql.TypeMediumBlob)
	require.Equal(s.T(), t.Cols()[2].Flen, mediumBlobMaxLength)
	require.Equal(s.T(), t.Cols()[3].Tp, mysql.TypeLongBlob)
	require.Equal(s.T(), t.Cols()[3].Flen, longBlobMaxLength)

	oldRow := types.MakeDatums([]byte("a"), []byte("a"), []byte("a"), []byte("a"))
	_, err = t.AddRecord(ctx, oldRow)
	require.NoError(s.T(), err)

	txn, err := ctx.Txn(true)
	require.NoError(s.T(), err)
	err = txn.Commit(context.Background())
	require.NoError(s.T(), err)
}
