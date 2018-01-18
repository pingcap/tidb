// Copyright 2016 PingCAP, Inc.
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

package tables_test

import (
	"io"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	goctx "golang.org/x/net/context"
)

var _ = Suite(&testIndexSuite{})

type testIndexSuite struct {
	s kv.Storage
}

func (s *testIndexSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	store, err := tikv.NewMockTikvStore()
	c.Assert(err, IsNil)
	s.s = store
}

func (s *testIndexSuite) TearDownSuite(c *C) {
	err := s.s.Close()
	c.Assert(err, IsNil)
	testleak.AfterTest(c)()
}

func (s *testIndexSuite) TestIndex(c *C) {
	tblInfo := &model.TableInfo{
		ID: 1,
		Indices: []*model.IndexInfo{
			{
				ID:   2,
				Name: model.NewCIStr("test"),
				Columns: []*model.IndexColumn{
					{},
					{},
				},
			},
		},
	}
	index := tables.NewIndex(tblInfo, tblInfo.Indices[0])

	// Test ununiq index.
	txn, err := s.s.Begin()
	c.Assert(err, IsNil)

	values := types.MakeDatums(1, 2)
	mockCtx := mock.NewContext()
	_, err = index.Create(mockCtx, txn, values, 1)
	c.Assert(err, IsNil)

	it, err := index.SeekFirst(txn)
	c.Assert(err, IsNil)

	getValues, h, err := it.Next()
	c.Assert(err, IsNil)
	c.Assert(getValues, HasLen, 2)
	c.Assert(getValues[0].GetInt64(), Equals, int64(1))
	c.Assert(getValues[1].GetInt64(), Equals, int64(2))
	c.Assert(h, Equals, int64(1))
	it.Close()
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	exist, _, err := index.Exist(sc, txn, values, 100)
	c.Assert(err, IsNil)
	c.Assert(exist, IsFalse)

	exist, _, err = index.Exist(sc, txn, values, 1)
	c.Assert(err, IsNil)
	c.Assert(exist, IsTrue)

	err = index.Delete(sc, txn, values, 1)
	c.Assert(err, IsNil)

	it, err = index.SeekFirst(txn)
	c.Assert(err, IsNil)

	_, _, err = it.Next()
	c.Assert(terror.ErrorEqual(err, io.EOF), IsTrue)
	it.Close()

	_, err = index.Create(mockCtx, txn, values, 0)
	c.Assert(err, IsNil)

	_, err = index.SeekFirst(txn)
	c.Assert(err, IsNil)

	_, hit, err := index.Seek(sc, txn, values)
	c.Assert(err, IsNil)
	c.Assert(hit, IsTrue)

	err = index.Drop(txn)
	c.Assert(err, IsNil)

	it, hit, err = index.Seek(sc, txn, values)
	c.Assert(err, IsNil)
	c.Assert(hit, IsFalse)

	_, _, err = it.Next()
	c.Assert(terror.ErrorEqual(err, io.EOF), IsTrue)
	it.Close()

	it, err = index.SeekFirst(txn)
	c.Assert(err, IsNil)

	_, _, err = it.Next()
	c.Assert(terror.ErrorEqual(err, io.EOF), IsTrue)
	it.Close()

	err = txn.Commit(goctx.Background())
	c.Assert(err, IsNil)

	tblInfo = &model.TableInfo{
		ID: 2,
		Indices: []*model.IndexInfo{
			{
				ID:     3,
				Name:   model.NewCIStr("test"),
				Unique: true,
				Columns: []*model.IndexColumn{
					{},
					{},
				},
			},
		},
	}
	index = tables.NewIndex(tblInfo, tblInfo.Indices[0])

	// Test uniq index.
	txn, err = s.s.Begin()
	c.Assert(err, IsNil)

	_, err = index.Create(mockCtx, txn, values, 1)
	c.Assert(err, IsNil)

	_, err = index.Create(mockCtx, txn, values, 2)
	c.Assert(err, NotNil)

	it, err = index.SeekFirst(txn)
	c.Assert(err, IsNil)

	getValues, h, err = it.Next()
	c.Assert(err, IsNil)
	c.Assert(getValues, HasLen, 2)
	c.Assert(getValues[0].GetInt64(), Equals, int64(1))
	c.Assert(getValues[1].GetInt64(), Equals, int64(2))
	c.Assert(h, Equals, int64(1))
	it.Close()

	exist, h, err = index.Exist(sc, txn, values, 1)
	c.Assert(err, IsNil)
	c.Assert(h, Equals, int64(1))
	c.Assert(exist, IsTrue)

	exist, h, err = index.Exist(sc, txn, values, 2)
	c.Assert(err, NotNil)
	c.Assert(h, Equals, int64(1))
	c.Assert(exist, IsTrue)

	err = txn.Commit(goctx.Background())
	c.Assert(err, IsNil)

	_, err = index.FetchValues(make([]types.Datum, 0), nil)
	c.Assert(err, NotNil)
}

func (s *testIndexSuite) TestCombineIndexSeek(c *C) {
	tblInfo := &model.TableInfo{
		ID: 1,
		Indices: []*model.IndexInfo{
			{
				ID:   2,
				Name: model.NewCIStr("test"),
				Columns: []*model.IndexColumn{
					{},
					{},
				},
			},
		},
	}
	index := tables.NewIndex(tblInfo, tblInfo.Indices[0])

	txn, err := s.s.Begin()
	c.Assert(err, IsNil)

	mockCtx := mock.NewContext()
	values := types.MakeDatums("abc", "def")
	_, err = index.Create(mockCtx, txn, values, 1)
	c.Assert(err, IsNil)

	index2 := tables.NewIndex(tblInfo, tblInfo.Indices[0])
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	iter, hit, err := index2.Seek(sc, txn, types.MakeDatums("abc", nil))
	c.Assert(err, IsNil)
	defer iter.Close()
	c.Assert(hit, IsFalse)
	_, h, err := iter.Next()
	c.Assert(err, IsNil)
	c.Assert(h, Equals, int64(1))
}
