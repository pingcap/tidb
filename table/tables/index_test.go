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
	"context"
	"github.com/pingcap/parser/mysql"
	"io"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/v4/domain"
	"github.com/pingcap/tidb/v4/kv"
	"github.com/pingcap/tidb/v4/session"
	"github.com/pingcap/tidb/v4/sessionctx/stmtctx"
	"github.com/pingcap/tidb/v4/store/mockstore"
	"github.com/pingcap/tidb/v4/table/tables"
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tidb/v4/util/mock"
	"github.com/pingcap/tidb/v4/util/testleak"
)

var _ = Suite(&testIndexSuite{})

type testIndexSuite struct {
	s   kv.Storage
	dom *domain.Domain
}

func (s *testIndexSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	s.s = store
	s.dom, err = session.BootstrapSession(store)
	c.Assert(err, IsNil)
}

func (s *testIndexSuite) TearDownSuite(c *C) {
	s.dom.Close()
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
					{Offset: 0},
					{Offset: 1},
				},
			},
		},
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: model.NewCIStr("c2"), State: model.StatePublic, Offset: 0, FieldType: *types.NewFieldType(mysql.TypeVarchar)},
			{ID: 2, Name: model.NewCIStr("c2"), State: model.StatePublic, Offset: 1, FieldType: *types.NewFieldType(mysql.TypeString)},
		},
	}
	index := tables.NewIndex(tblInfo.ID, tblInfo, tblInfo.Indices[0])

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
	c.Assert(terror.ErrorEqual(err, io.EOF), IsTrue, Commentf("err %v", err))
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
	c.Assert(terror.ErrorEqual(err, io.EOF), IsTrue, Commentf("err %v", err))
	it.Close()

	it, err = index.SeekFirst(txn)
	c.Assert(err, IsNil)

	_, _, err = it.Next()
	c.Assert(terror.ErrorEqual(err, io.EOF), IsTrue, Commentf("err %v", err))
	it.Close()

	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	tblInfo = &model.TableInfo{
		ID: 2,
		Indices: []*model.IndexInfo{
			{
				ID:     3,
				Name:   model.NewCIStr("test"),
				Unique: true,
				Columns: []*model.IndexColumn{
					{Offset: 0},
					{Offset: 1},
				},
			},
		},
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: model.NewCIStr("c2"), State: model.StatePublic, Offset: 0, FieldType: *types.NewFieldType(mysql.TypeVarchar)},
			{ID: 2, Name: model.NewCIStr("c2"), State: model.StatePublic, Offset: 1, FieldType: *types.NewFieldType(mysql.TypeString)},
		},
	}
	index = tables.NewIndex(tblInfo.ID, tblInfo, tblInfo.Indices[0])

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

	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	_, err = index.FetchValues(make([]types.Datum, 0), nil)
	c.Assert(err, NotNil)

	// Test the function of Next when the value of unique key is nil.
	values2 := types.MakeDatums(nil, nil)
	_, err = index.Create(mockCtx, txn, values2, 2)
	c.Assert(err, IsNil)
	it, err = index.SeekFirst(txn)
	c.Assert(err, IsNil)
	getValues, h, err = it.Next()
	c.Assert(err, IsNil)
	c.Assert(getValues, HasLen, 2)
	c.Assert(getValues[0].GetInterface(), Equals, nil)
	c.Assert(getValues[1].GetInterface(), Equals, nil)
	c.Assert(h, Equals, int64(2))
	it.Close()
}

func (s *testIndexSuite) TestCombineIndexSeek(c *C) {
	tblInfo := &model.TableInfo{
		ID: 1,
		Indices: []*model.IndexInfo{
			{
				ID:   2,
				Name: model.NewCIStr("test"),
				Columns: []*model.IndexColumn{
					{Offset: 1},
					{Offset: 2},
				},
			},
		},
		Columns: []*model.ColumnInfo{
			{Offset: 0},
			{Offset: 1},
			{Offset: 2},
		},
	}
	index := tables.NewIndex(tblInfo.ID, tblInfo, tblInfo.Indices[0])

	txn, err := s.s.Begin()
	c.Assert(err, IsNil)

	mockCtx := mock.NewContext()
	values := types.MakeDatums("abc", "def")
	_, err = index.Create(mockCtx, txn, values, 1)
	c.Assert(err, IsNil)

	index2 := tables.NewIndex(tblInfo.ID, tblInfo, tblInfo.Indices[0])
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	iter, hit, err := index2.Seek(sc, txn, types.MakeDatums("abc", nil))
	c.Assert(err, IsNil)
	defer iter.Close()
	c.Assert(hit, IsFalse)
	_, h, err := iter.Next()
	c.Assert(err, IsNil)
	c.Assert(h, Equals, int64(1))
}
