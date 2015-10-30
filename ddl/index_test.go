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
	"io"
	"time"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/column"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/parser/coldef"
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

func (s *testIndexSuite) testCreateIndex(c *C, ctx context.Context, tblInfo *model.TableInfo, unique bool, indexName string, colName string) *model.Job {
	job := &model.Job{
		SchemaID: s.dbInfo.ID,
		TableID:  tblInfo.ID,
		Type:     model.ActionAddIndex,
		Args:     []interface{}{unique, model.NewCIStr(indexName), []*coldef.IndexColName{{ColumnName: colName, Length: 256}}},
	}

	err := s.d.startJob(ctx, job)
	c.Assert(err, IsNil)
	return job
}

func (s *testIndexSuite) testDropIndex(c *C, ctx context.Context, tblInfo *model.TableInfo, indexName string) *model.Job {
	job := &model.Job{
		SchemaID: s.dbInfo.ID,
		TableID:  tblInfo.ID,
		Type:     model.ActionDropIndex,
		Args:     []interface{}{model.NewCIStr(indexName)},
	}

	err := s.d.startJob(ctx, job)
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

	for i := 0; i < 10; i++ {
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

	job := s.testCreateIndex(c, ctx, tblInfo, true, "c1_uni", "c1")
	testCheckJobDone(c, s.d, job, true)

	t = testGetTable(c, s.d, s.dbInfo.ID, tblInfo.ID)
	index := t.FindIndexByColName("c1")
	c.Assert(index, NotNil)

	h, err := t.AddRecord(ctx, []interface{}{11, 11, 11})
	c.Assert(err, IsNil)

	h1, err := t.AddRecord(ctx, []interface{}{11, 11, 11})
	c.Assert(err, NotNil)
	c.Assert(h, Equals, h1)

	_, err = t.AddRecord(ctx, []interface{}{1, 1, 1})
	c.Assert(err, NotNil)

	it, _, err := index.X.Seek(txn, []interface{}{1})
	c.Assert(err, IsNil)
	c.Assert(it, NotNil)

	_, h2, err := it.Next()
	c.Assert(err, IsNil)
	c.Assert(h, Equals, h2)

	it.Close()

	s.testDropIndex(c, ctx, tblInfo, "c1_uni")

	t = testGetTable(c, s.d, s.dbInfo.ID, tblInfo.ID)
	index1 := t.FindIndexByColName("c1")
	c.Assert(index1, IsNil)

	it, _, _ = index.X.Seek(txn, []interface{}{1})
	c.Assert(it, NotNil)
	_, _, err = it.Next()
	c.Assert(err.Error(), Equals, io.EOF.Error())
}

func init() {
	log.SetLevelByString("info")
}
