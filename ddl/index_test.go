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

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/parser/coldef"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/mock"
)

var _ = Suite(&testIndexSuite{})

type testIndexSuite struct {
	store  kv.Storage
	dbInfo *model.DBInfo

	d *ddl
}

func (s *testIndexSuite) SetUpSuite(c *C) {
	s.store = testCreateStore(c, "test_index")
	lease := 50 * time.Millisecond
	s.d = newDDL(s.store, nil, nil, lease)

	s.dbInfo = testSchemaInfo(c, s.d, "test")
	testCreateSchema(c, mock.NewContext(), s.d, s.dbInfo)
}

func (s *testIndexSuite) TearDownSuite(c *C) {
	testDropSchema(c, mock.NewContext(), s.d, s.dbInfo)
	s.d.close()
	s.store.Close()
}

func (s *testIndexSuite) testCreateIndex(c *C, ctx context.Context, d *ddl, tblInfo *model.TableInfo, unique bool, indexName string, colName string) *model.Job {
	job := &model.Job{
		SchemaID: s.dbInfo.ID,
		TableID:  tblInfo.ID,
		Type:     model.ActionAddIndex,
		Args:     []interface{}{unique, model.NewCIStr(indexName), []*coldef.IndexColName{{ColumnName: colName, Length: 256}}},
	}

	err := d.startJob(ctx, job)
	c.Assert(err, IsNil)
	return job
}

func (s *testIndexSuite) testDropIndex(c *C, ctx context.Context, d *ddl, tblInfo *model.TableInfo, indexName string) *model.Job {
	job := &model.Job{
		SchemaID: s.dbInfo.ID,
		TableID:  tblInfo.ID,
		Type:     model.ActionDropIndex,
		Args:     []interface{}{model.NewCIStr(indexName)},
	}

	err := d.startJob(ctx, job)
	c.Assert(err, IsNil)
	return job
}

func (s *testIndexSuite) TestIndex(c *C) {
	tblInfo := testTableInfo(c, s.d, "t1")
	ctx := testNewContext(c, s.d)
	defer ctx.FinishTxn(true)

	txn, err := ctx.GetTxn(true)
	c.Assert(err, IsNil)

	testCreateTable(c, ctx, s.d, s.dbInfo, tblInfo)

	t := testGetTable(c, s.d, s.dbInfo.ID, tblInfo.ID)

	num := 10
	for i := 0; i < num; i++ {
		_, err = t.AddRecord(ctx, []interface{}{i, i, i})
		c.Assert(err, IsNil)
	}

	err = ctx.FinishTxn(false)
	c.Assert(err, IsNil)

	i := int64(0)
	t.IterRecords(ctx, t.FirstKey(), t.Cols(), func(h int64, data []interface{}, cols []*column.Col) (bool, error) {
		c.Assert(data[0], Equals, i)
		i++
		return true, nil
	})

	job := s.testCreateIndex(c, ctx, s.d, tblInfo, true, "c1_uni", "c1")
	testCheckJobDone(c, s.d, job, true)

	t = testGetTable(c, s.d, s.dbInfo.ID, tblInfo.ID)
	index := t.FindIndexByColName("c1")
	c.Assert(index, NotNil)

	h, err := t.AddRecord(ctx, []interface{}{num + 1, 1, 1})
	c.Assert(err, IsNil)

	h1, err := t.AddRecord(ctx, []interface{}{num + 1, 1, 1})
	c.Assert(err, NotNil)
	c.Assert(h, Equals, h1)

	h, err = t.AddRecord(ctx, []interface{}{1, 1, 1})
	c.Assert(err, NotNil)

	txn, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	exist, _, err := index.X.Exist(txn, []interface{}{1}, h)
	c.Assert(err, IsNil)
	c.Assert(exist, IsTrue)

	s.testDropIndex(c, ctx, s.d, tblInfo, "c1_uni")

	t = testGetTable(c, s.d, s.dbInfo.ID, tblInfo.ID)
	index1 := t.FindIndexByColName("c1")
	c.Assert(index1, IsNil)

	txn, err = ctx.GetTxn(true)
	c.Assert(err, IsNil)

	exist, _, err = index.X.Exist(txn, []interface{}{1}, h)
	c.Assert(err, IsNil)
	c.Assert(exist, IsFalse)
}

func testGetIndex(t table.Table, name string) *column.IndexedCol {
	for _, idx := range t.Indices() {
		// only public index can be read.

		if len(idx.Columns) == 1 && strings.EqualFold(idx.Columns[0].Name.L, name) {
			return idx
		}
	}
	return nil
}

func (s *testIndexSuite) TestIndexWait(c *C) {
	d := newDDL(s.store, nil, nil, 100*time.Millisecond)
	defer d.close()

	tblInfo := testTableInfo(c, d, "t")
	ctx := testNewContext(c, d)
	defer ctx.FinishTxn(true)

	txn, err := ctx.GetTxn(true)
	c.Assert(err, IsNil)

	testCreateTable(c, ctx, d, s.dbInfo, tblInfo)

	t := testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)

	var h int64
	h, err = t.AddRecord(ctx, []interface{}{1, 1, 1})
	c.Assert(err, IsNil)

	ticker := time.NewTicker(d.lease)
	done := make(chan *model.Job, 1)

	ctx.FinishTxn(false)
	go func() {
		done <- s.testCreateIndex(c, ctx, d, tblInfo, true, "c1_uni", "c1")
	}()

	var exist bool
LOOP:
	for {
		select {
		case job := <-done:
			testCheckJobDone(c, d, job, true)
			break LOOP
		case <-ticker.C:
			d.close()

			t = testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)
			index := testGetIndex(t, "c1")
			if index == nil {
				d.start()
				continue
			}

			err = t.RemoveRowAllIndex(ctx, h, []interface{}{1})
			c.Assert(err, IsNil)

			err = t.RemoveRow(ctx, h)
			c.Assert(err, IsNil)

			txn, err = ctx.GetTxn(true)
			c.Assert(err, IsNil)

			exist, _, err = index.X.Exist(txn, []interface{}{1}, h)
			c.Assert(err, IsNil)
			c.Assert(exist, IsFalse)

			h, err = t.AddRecord(ctx, []interface{}{1, 1, 1})
			c.Assert(err, IsNil)

			txn, err = ctx.GetTxn(true)
			c.Assert(err, IsNil)

			exist, _, err = index.X.Exist(txn, []interface{}{1}, h)
			c.Assert(err, IsNil)
			switch index.State {
			case model.StateDeleteOnly:
				c.Assert(exist, IsFalse)
			case model.StateNone:
				c.Fatalf("can be none state")
			default:
				c.Assert(exist, IsTrue)
			}

			err = ctx.FinishTxn(false)
			c.Assert(err, IsNil)

			d.start()
		}
	}

	t = testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)
	c.Assert(t.FindIndexByColName("c1"), NotNil)

	ctx.FinishTxn(false)
	go func() {
		done <- s.testDropIndex(c, ctx, d, tblInfo, "c1_uni")
	}()

LOOP1:
	for {
		select {
		case job := <-done:
			testCheckJobDone(c, d, job, false)
			break LOOP1
		case <-ticker.C:
			d.close()

			t = testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)
			index := testGetIndex(t, "c1")
			if index == nil {
				d.start()
				continue
			}

			err = t.RemoveRowAllIndex(ctx, h, []interface{}{1})
			c.Assert(err, IsNil)

			err = t.RemoveRow(ctx, h)
			c.Assert(err, IsNil)

			txn, err = ctx.GetTxn(true)
			c.Assert(err, IsNil)

			exist, _, err = index.X.Exist(txn, []interface{}{1}, h)
			c.Assert(err, IsNil)
			c.Assert(exist, IsFalse)

			h, err = t.AddRecord(ctx, []interface{}{1, 1, 1})
			c.Assert(err, IsNil)

			txn, err = ctx.GetTxn(true)
			c.Assert(err, IsNil)

			exist, _, err = index.X.Exist(txn, []interface{}{1}, h)
			c.Assert(err, IsNil)
			switch index.State {
			case model.StateDeleteOnly:
				c.Assert(exist, IsFalse)
			case model.StateNone:
				c.Fatalf("can be none state")
			default:
				c.Assert(exist, IsTrue)
			}

			err = ctx.FinishTxn(false)
			c.Assert(err, IsNil)

			d.start()
		}
	}

	t = testGetTable(c, d, s.dbInfo.ID, tblInfo.ID)
	c.Assert(t.FindIndexByColName("c1"), IsNil)
}

func init() {
	log.SetLevelByString("error")
}
