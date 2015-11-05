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
