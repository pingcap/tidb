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
	"io"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testIndexSuite{})

type testIndexSuite struct {
	s   kv.Storage
	dom *domain.Domain
}

func (s *testIndexSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	store, err := mockstore.NewMockStore()
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
	_, err = index.Create(mockCtx, txn.GetUnionStore(), values, kv.IntHandle(1))
	c.Assert(err, IsNil)

	it, err := index.SeekFirst(txn)
	c.Assert(err, IsNil)

	getValues, h, err := it.Next()
	c.Assert(err, IsNil)
	c.Assert(getValues, HasLen, 2)
	c.Assert(getValues[0].GetInt64(), Equals, int64(1))
	c.Assert(getValues[1].GetInt64(), Equals, int64(2))
	c.Assert(h.IntValue(), Equals, int64(1))
	it.Close()
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	exist, _, err := index.Exist(sc, txn.GetUnionStore(), values, kv.IntHandle(100))
	c.Assert(err, IsNil)
	c.Assert(exist, IsFalse)

	exist, _, err = index.Exist(sc, txn.GetUnionStore(), values, kv.IntHandle(1))
	c.Assert(err, IsNil)
	c.Assert(exist, IsTrue)

	err = index.Delete(sc, txn, values, kv.IntHandle(1))
	c.Assert(err, IsNil)

	it, err = index.SeekFirst(txn)
	c.Assert(err, IsNil)

	_, _, err = it.Next()
	c.Assert(terror.ErrorEqual(err, io.EOF), IsTrue, Commentf("err %v", err))
	it.Close()

	_, err = index.Create(mockCtx, txn.GetUnionStore(), values, kv.IntHandle(0))
	c.Assert(err, IsNil)

	_, err = index.SeekFirst(txn)
	c.Assert(err, IsNil)

	_, hit, err := index.Seek(sc, txn, values)
	c.Assert(err, IsNil)
	c.Assert(hit, IsFalse)

	err = index.Drop(txn.GetUnionStore())
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

	_, err = index.Create(mockCtx, txn.GetUnionStore(), values, kv.IntHandle(1))
	c.Assert(err, IsNil)

	_, err = index.Create(mockCtx, txn.GetUnionStore(), values, kv.IntHandle(2))
	c.Assert(err, NotNil)

	it, err = index.SeekFirst(txn)
	c.Assert(err, IsNil)

	getValues, h, err = it.Next()
	c.Assert(err, IsNil)
	c.Assert(getValues, HasLen, 2)
	c.Assert(getValues[0].GetInt64(), Equals, int64(1))
	c.Assert(getValues[1].GetInt64(), Equals, int64(2))
	c.Assert(h.IntValue(), Equals, int64(1))
	it.Close()

	exist, h, err = index.Exist(sc, txn.GetUnionStore(), values, kv.IntHandle(1))
	c.Assert(err, IsNil)
	c.Assert(h.IntValue(), Equals, int64(1))
	c.Assert(exist, IsTrue)

	exist, h, err = index.Exist(sc, txn.GetUnionStore(), values, kv.IntHandle(2))
	c.Assert(err, NotNil)
	c.Assert(h.IntValue(), Equals, int64(1))
	c.Assert(exist, IsTrue)

	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	_, err = index.FetchValues(make([]types.Datum, 0), nil)
	c.Assert(err, NotNil)

	txn, err = s.s.Begin()
	c.Assert(err, IsNil)

	// Test the function of Next when the value of unique key is nil.
	values2 := types.MakeDatums(nil, nil)
	_, err = index.Create(mockCtx, txn.GetUnionStore(), values2, kv.IntHandle(2))
	c.Assert(err, IsNil)
	it, err = index.SeekFirst(txn)
	c.Assert(err, IsNil)
	getValues, h, err = it.Next()
	c.Assert(err, IsNil)
	c.Assert(getValues, HasLen, 2)
	c.Assert(getValues[0].GetInterface(), Equals, nil)
	c.Assert(getValues[1].GetInterface(), Equals, nil)
	c.Assert(h.IntValue(), Equals, int64(2))
	it.Close()

	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
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
	_, err = index.Create(mockCtx, txn.GetUnionStore(), values, kv.IntHandle(1))
	c.Assert(err, IsNil)

	index2 := tables.NewIndex(tblInfo.ID, tblInfo, tblInfo.Indices[0])
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	iter, hit, err := index2.Seek(sc, txn, types.MakeDatums("abc", nil))
	c.Assert(err, IsNil)
	defer iter.Close()
	c.Assert(hit, IsFalse)
	_, h, err := iter.Next()
	c.Assert(err, IsNil)
	c.Assert(h.IntValue(), Equals, int64(1))
}

func (s *testIndexSuite) TestSingleColumnCommonHandle(c *C) {
	tblInfo := buildTableInfo(c, "create table t (a varchar(255) primary key, u int unique, nu int, index nu (nu))")
	var idxUnique, idxNonUnique table.Index
	for _, idxInfo := range tblInfo.Indices {
		idx := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)
		if idxInfo.Name.L == "u" {
			idxUnique = idx
		} else if idxInfo.Name.L == "nu" {
			idxNonUnique = idx
		}
	}
	txn, err := s.s.Begin()
	c.Assert(err, IsNil)

	mockCtx := mock.NewContext()
	sc := mockCtx.GetSessionVars().StmtCtx
	// create index for "insert t values ('abc', 1, 1)"
	idxColVals := types.MakeDatums(1)
	handleColVals := types.MakeDatums("abc")
	encodedHandle, err := codec.EncodeKey(sc, nil, handleColVals...)
	c.Assert(err, IsNil)
	commonHandle, err := kv.NewCommonHandle(encodedHandle)
	c.Assert(err, IsNil)

	for _, idx := range []table.Index{idxUnique, idxNonUnique} {
		key, _, err := idx.GenIndexKey(sc, idxColVals, commonHandle, nil)
		c.Assert(err, IsNil)
		_, err = idx.Create(mockCtx, txn.GetUnionStore(), idxColVals, commonHandle)
		c.Assert(err, IsNil)
		val, err := txn.Get(context.Background(), key)
		c.Assert(err, IsNil)
		colVals, err := tablecodec.DecodeIndexKV(key, val, 1, tablecodec.HandleDefault,
			createRowcodecColInfo(tblInfo, idx.Meta()))
		c.Assert(err, IsNil)
		c.Assert(colVals, HasLen, 2)
		_, d, err := codec.DecodeOne(colVals[0])
		c.Assert(err, IsNil)
		c.Assert(d.GetInt64(), Equals, int64(1))
		_, d, err = codec.DecodeOne(colVals[1])
		c.Assert(err, IsNil)
		c.Assert(d.GetString(), Equals, "abc")
		handle, err := tablecodec.DecodeIndexHandle(key, val, 1)
		c.Assert(err, IsNil)
		c.Assert(handle.IsInt(), IsFalse)
		c.Assert(handle.Encoded(), BytesEquals, commonHandle.Encoded())

		unTouchedVal := append([]byte{1}, val[1:]...)
		unTouchedVal = append(unTouchedVal, kv.UnCommitIndexKVFlag)
		_, err = tablecodec.DecodeIndexKV(key, unTouchedVal, 1, tablecodec.HandleDefault,
			createRowcodecColInfo(tblInfo, idx.Meta()))
		c.Assert(err, IsNil)
	}
}

func (s *testIndexSuite) TestMultiColumnCommonHandle(c *C) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
	tblInfo := buildTableInfo(c, "create table t (a int, b int, u varchar(64) unique, nu varchar(64), primary key (a, b), index nu (nu))")
	var idxUnique, idxNonUnique table.Index
	for _, idxInfo := range tblInfo.Indices {
		idx := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)
		if idxInfo.Name.L == "u" {
			idxUnique = idx
		} else if idxInfo.Name.L == "nu" {
			idxNonUnique = idx
		}
	}

	txn, err := s.s.Begin()
	c.Assert(err, IsNil)
	mockCtx := mock.NewContext()
	sc := mockCtx.GetSessionVars().StmtCtx
	// create index for "insert t values (3, 2, "abc", "abc")
	idxColVals := types.MakeDatums("abc")
	handleColVals := types.MakeDatums(3, 2)
	encodedHandle, err := codec.EncodeKey(sc, nil, handleColVals...)
	c.Assert(err, IsNil)
	commonHandle, err := kv.NewCommonHandle(encodedHandle)
	c.Assert(err, IsNil)
	_ = idxNonUnique
	for _, idx := range []table.Index{idxUnique, idxNonUnique} {
		key, _, err := idx.GenIndexKey(sc, idxColVals, commonHandle, nil)
		c.Assert(err, IsNil)
		_, err = idx.Create(mockCtx, txn.GetUnionStore(), idxColVals, commonHandle)
		c.Assert(err, IsNil)
		val, err := txn.Get(context.Background(), key)
		c.Assert(err, IsNil)
		colVals, err := tablecodec.DecodeIndexKV(key, val, 1, tablecodec.HandleDefault,
			createRowcodecColInfo(tblInfo, idx.Meta()))
		c.Assert(err, IsNil)
		c.Assert(colVals, HasLen, 3)
		_, d, err := codec.DecodeOne(colVals[0])
		c.Assert(err, IsNil)
		c.Assert(d.GetString(), Equals, "abc")
		_, d, err = codec.DecodeOne(colVals[1])
		c.Assert(err, IsNil)
		c.Assert(d.GetInt64(), Equals, int64(3))
		_, d, err = codec.DecodeOne(colVals[2])
		c.Assert(err, IsNil)
		c.Assert(d.GetInt64(), Equals, int64(2))
		handle, err := tablecodec.DecodeIndexHandle(key, val, 1)
		c.Assert(err, IsNil)
		c.Assert(handle.IsInt(), IsFalse)
		c.Assert(handle.Encoded(), BytesEquals, commonHandle.Encoded())
	}
}

func buildTableInfo(c *C, sql string) *model.TableInfo {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	c.Assert(err, IsNil)
	tblInfo, err := ddl.BuildTableInfoFromAST(stmt.(*ast.CreateTableStmt))
	c.Assert(err, IsNil)
	return tblInfo
}

func createRowcodecColInfo(table *model.TableInfo, index *model.IndexInfo) []rowcodec.ColInfo {
	colInfos := make([]rowcodec.ColInfo, 0, len(index.Columns))
	for _, idxCol := range index.Columns {
		col := table.Columns[idxCol.Offset]
		colInfos = append(colInfos, rowcodec.ColInfo{
			ID:         col.ID,
			IsPKHandle: table.PKIsHandle && mysql.HasPriKeyFlag(col.Flag),
			Ft:         rowcodec.FieldTypeFromModelColumn(col),
		})
	}
	return colInfos
}
