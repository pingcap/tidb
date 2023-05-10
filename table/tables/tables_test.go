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

package tables_test

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tipb/go-binlog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func firstKey(t table.Table) kv.Key {
	return tablecodec.EncodeRecordKey(t.RecordPrefix(), kv.IntHandle(math.MinInt64))
}

func indexPrefix(t table.PhysicalTable) kv.Key {
	return tablecodec.GenTableIndexPrefix(t.GetPhysicalID())
}

func seek(t table.PhysicalTable, ctx sessionctx.Context, h kv.Handle) (kv.Handle, bool, error) {
	txn, err := ctx.Txn(true)
	if err != nil {
		return nil, false, err
	}
	recordPrefix := t.RecordPrefix()
	seekKey := tablecodec.EncodeRowKeyWithHandle(t.GetPhysicalID(), h)
	iter, err := txn.Iter(seekKey, recordPrefix.PrefixNext())
	if err != nil {
		return nil, false, err
	}
	if !iter.Valid() || !iter.Key().HasPrefix(recordPrefix) {
		// No more records in the table, skip to the end.
		return nil, false, nil
	}
	handle, err := tablecodec.DecodeRowKey(iter.Key())
	if err != nil {
		return nil, false, err
	}
	return handle, true, nil
}

type mockPumpClient struct{}

func (m mockPumpClient) WriteBinlog(ctx context.Context, in *binlog.WriteBinlogReq, opts ...grpc.CallOption) (*binlog.WriteBinlogResp, error) {
	return &binlog.WriteBinlogResp{}, nil
}

func (m mockPumpClient) PullBinlogs(ctx context.Context, in *binlog.PullBinlogReq, opts ...grpc.CallOption) (binlog.Pump_PullBinlogsClient, error) {
	return nil, nil
}

func TestBasic(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	_, err := tk.Session().Execute(context.Background(), "CREATE TABLE test.t (a int primary key auto_increment, b varchar(255) unique)")
	require.NoError(t, err)
	require.Nil(t, sessiontxn.NewTxn(context.Background(), tk.Session()))
	tb, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	require.Greater(t, tb.Meta().ID, int64(0))
	require.Equal(t, "t", tb.Meta().Name.L)
	require.NotNil(t, tb.Meta())
	require.NotNil(t, tb.Indices())
	require.NotEqual(t, "", string(firstKey(tb)))
	require.NotEqual(t, "", string(indexPrefix(tb.(table.PhysicalTable))))
	require.NotEqual(t, "", string(tb.RecordPrefix()))
	require.NotNil(t, tables.FindIndexByColName(tb, "b"))

	autoID, err := table.AllocAutoIncrementValue(context.Background(), tb, tk.Session())
	require.NoError(t, err)
	require.Greater(t, autoID, int64(0))

	handle, err := tables.AllocHandle(context.Background(), nil, tb)
	require.NoError(t, err)
	require.Greater(t, handle.IntValue(), int64(0))

	ctx := tk.Session()
	rid, err := tb.AddRecord(ctx, types.MakeDatums(1, "abc"))
	require.NoError(t, err)
	require.Greater(t, rid.IntValue(), int64(0))
	row, err := tables.RowWithCols(tb, ctx, rid, tb.Cols())
	require.NoError(t, err)
	require.Equal(t, 2, len(row))
	require.Equal(t, int64(1), row[0].GetInt64())

	_, err = tb.AddRecord(ctx, types.MakeDatums(1, "aba"))
	require.Error(t, err)
	_, err = tb.AddRecord(ctx, types.MakeDatums(2, "abc"))
	require.Error(t, err)

	require.Nil(t, tb.UpdateRecord(context.Background(), ctx, rid, types.MakeDatums(1, "abc"), types.MakeDatums(1, "cba"), []bool{false, true}))

	err = tables.IterRecords(tb, ctx, tb.Cols(), func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
		return true, nil
	})
	require.NoError(t, err)

	indexCnt := func() int {
		cnt, err1 := countEntriesWithPrefix(ctx, indexPrefix(tb.(table.PhysicalTable)))
		require.Nil(t, err1)
		return cnt
	}

	// RowWithCols test
	vals, err := tables.RowWithCols(tb, ctx, kv.IntHandle(1), tb.Cols())
	require.NoError(t, err)
	require.Len(t, vals, 2)
	require.Equal(t, int64(1), vals[0].GetInt64())
	cols := []*table.Column{tb.Cols()[1]}
	vals, err = tables.RowWithCols(tb, ctx, kv.IntHandle(1), cols)
	require.NoError(t, err)
	require.Len(t, vals, 1)
	require.Equal(t, []byte("cba"), vals[0].GetBytes())

	// Make sure there is index data in the storage.
	require.Greater(t, indexCnt(), 0)
	require.Nil(t, tb.RemoveRecord(ctx, rid, types.MakeDatums(1, "cba")))
	// Make sure index data is also removed after tb.RemoveRecord().
	require.Equal(t, 0, indexCnt())
	_, err = tb.AddRecord(ctx, types.MakeDatums(1, "abc"))
	require.NoError(t, err)
	require.Greater(t, indexCnt(), 0)
	handle, found, err := seek(tb.(table.PhysicalTable), ctx, kv.IntHandle(0))
	require.Equal(t, int64(1), handle.IntValue())
	require.Equal(t, true, found)
	require.NoError(t, err)
	_, err = tk.Session().Execute(context.Background(), "drop table test.t")
	require.NoError(t, err)

	table.MockTableFromMeta(tb.Meta())
	alc := tb.Allocators(nil).Get(autoid.RowIDAllocType)
	require.NotNil(t, alc)

	err = alc.Rebase(context.Background(), 0, false)
	require.NoError(t, err)
}

func countEntriesWithPrefix(ctx sessionctx.Context, prefix []byte) (int, error) {
	cnt := 0
	txn, err := ctx.Txn(true)
	if err != nil {
		return 0, errors.Trace(err)
	}
	err = util.ScanMetaWithPrefix(txn, prefix, func(k kv.Key, v []byte) bool {
		cnt++
		return true
	})
	return cnt, err
}

func TestTypes(t *testing.T) {
	ctx := context.Background()
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	_, err := tk.Session().Execute(context.Background(), "CREATE TABLE test.t (c1 tinyint, c2 smallint, c3 int, c4 bigint, c5 text, c6 blob, c7 varchar(64), c8 time, c9 timestamp null default CURRENT_TIMESTAMP, c10 decimal(10,1))")
	require.NoError(t, err)
	_, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	_, err = tk.Session().Execute(ctx, "insert test.t values (1, 2, 3, 4, '5', '6', '7', '10:10:10', null, 1.4)")
	require.NoError(t, err)
	rs, err := tk.Session().Execute(ctx, "select * from test.t where c1 = 1")
	require.NoError(t, err)
	req := rs[0].NewChunk(nil)
	err = rs[0].Next(ctx, req)
	require.NoError(t, err)
	require.False(t, req.NumRows() == 0)
	require.Nil(t, rs[0].Close())
	_, err = tk.Session().Execute(ctx, "drop table test.t")
	require.NoError(t, err)

	_, err = tk.Session().Execute(ctx, "CREATE TABLE test.t (c1 tinyint unsigned, c2 smallint unsigned, c3 int unsigned, c4 bigint unsigned, c5 double, c6 bit(8))")
	require.NoError(t, err)
	_, err = tk.Session().Execute(ctx, "insert test.t values (1, 2, 3, 4, 5, 6)")
	require.NoError(t, err)
	rs, err = tk.Session().Execute(ctx, "select * from test.t where c1 = 1")
	require.NoError(t, err)
	req = rs[0].NewChunk(nil)
	err = rs[0].Next(ctx, req)
	require.NoError(t, err)
	require.False(t, req.NumRows() == 0)
	row := req.GetRow(0)
	require.Equal(t, types.NewBinaryLiteralFromUint(6, -1), types.BinaryLiteral(row.GetBytes(5)))
	require.Nil(t, rs[0].Close())
	_, err = tk.Session().Execute(ctx, "drop table test.t")
	require.NoError(t, err)

	_, err = tk.Session().Execute(ctx, "CREATE TABLE test.t (c1 enum('a', 'b', 'c'))")
	require.NoError(t, err)
	_, err = tk.Session().Execute(ctx, "insert test.t values ('a'), (2), ('c')")
	require.NoError(t, err)
	rs, err = tk.Session().Execute(ctx, "select c1 + 1 from test.t where c1 = 1")
	require.NoError(t, err)
	req = rs[0].NewChunk(nil)
	err = rs[0].Next(ctx, req)
	require.NoError(t, err)
	require.False(t, req.NumRows() == 0)
	require.Equal(t, float64(2), req.GetRow(0).GetFloat64(0))
	require.Nil(t, rs[0].Close())
	_, err = tk.Session().Execute(ctx, "drop table test.t")
	require.NoError(t, err)
}

func TestUniqueIndexMultipleNullEntries(t *testing.T) {
	ctx := context.Background()
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	_, err := tk.Session().Execute(ctx, "drop table if exists test.t")
	require.NoError(t, err)
	_, err = tk.Session().Execute(ctx, "CREATE TABLE test.t (a int primary key auto_increment, b varchar(255) unique)")
	require.NoError(t, err)
	tb, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	require.Greater(t, tb.Meta().ID, int64(0))
	require.Equal(t, "t", tb.Meta().Name.L)
	require.NotNil(t, tb.Meta())
	require.NotNil(t, tb.Indices())
	require.NotEqual(t, "", string(firstKey(tb)))
	require.NotEqual(t, "", string(indexPrefix(tb.(table.PhysicalTable))))
	require.NotEqual(t, "", string(tb.RecordPrefix()))
	require.NotNil(t, tables.FindIndexByColName(tb, "b"))

	handle, err := tables.AllocHandle(context.Background(), nil, tb)
	require.NoError(t, err)
	require.Greater(t, handle.IntValue(), int64(0))

	autoid, err := table.AllocAutoIncrementValue(context.Background(), tb, tk.Session())
	require.NoError(t, err)
	require.Greater(t, autoid, int64(0))

	sctx := tk.Session()
	require.Nil(t, sessiontxn.NewTxn(ctx, sctx))
	_, err = tb.AddRecord(sctx, types.MakeDatums(1, nil))
	require.NoError(t, err)
	_, err = tb.AddRecord(sctx, types.MakeDatums(2, nil))
	require.NoError(t, err)
	txn, err := sctx.Txn(true)
	require.NoError(t, err)
	require.Nil(t, txn.Rollback())
	_, err = tk.Session().Execute(context.Background(), "drop table test.t")
	require.NoError(t, err)
}

func TestRowKeyCodec(t *testing.T) {
	tableVal := []struct {
		tableID int64
		h       int64
		ID      int64
	}{
		{1, 1234567890, 0},
		{2, 1, 0},
		{3, -1, 0},
		{4, -1, 1},
	}

	for _, v := range tableVal {
		b := tablecodec.EncodeRowKeyWithHandle(v.tableID, kv.IntHandle(v.h))
		tableID, handle, err := tablecodec.DecodeRecordKey(b)
		require.NoError(t, err)
		require.Equal(t, v.tableID, tableID)
		require.Equal(t, v.h, handle.IntValue())

		handle, err = tablecodec.DecodeRowKey(b)
		require.NoError(t, err)
		require.Equal(t, v.h, handle.IntValue())
	}

	// test error
	tbl := []string{
		"",
		"x",
		"t1",
		"t12345678",
		"t12345678_i",
		"t12345678_r1",
		"t12345678_r1234567",
	}

	for _, v := range tbl {
		_, err := tablecodec.DecodeRowKey(kv.Key(v))
		require.Error(t, err)
	}
}

func TestUnsignedPK(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	_, err := tk.Session().Execute(context.Background(), "DROP TABLE IF EXISTS test.tPK")
	require.NoError(t, err)
	_, err = tk.Session().Execute(context.Background(), "CREATE TABLE test.tPK (a bigint unsigned primary key, b varchar(255))")
	require.NoError(t, err)
	tb, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("tPK"))
	require.NoError(t, err)
	require.Nil(t, sessiontxn.NewTxn(context.Background(), tk.Session()))
	rid, err := tb.AddRecord(tk.Session(), types.MakeDatums(1, "abc"))
	require.NoError(t, err)
	pt := tb.(table.PhysicalTable)
	row, err := tables.RowWithCols(pt, tk.Session(), rid, tb.Cols())
	require.NoError(t, err)
	require.Equal(t, 2, len(row))
	require.Equal(t, types.KindUint64, row[0].Kind())
	tk.Session().StmtCommit(context.Background())
	txn, err := tk.Session().Txn(true)
	require.NoError(t, err)
	require.Nil(t, txn.Commit(context.Background()))
}

func TestIterRecords(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	_, err := tk.Session().Execute(context.Background(), "DROP TABLE IF EXISTS test.tIter")
	require.NoError(t, err)
	_, err = tk.Session().Execute(context.Background(), "CREATE TABLE test.tIter (a int primary key, b int)")
	require.NoError(t, err)
	_, err = tk.Session().Execute(context.Background(), "INSERT test.tIter VALUES (-1, 2), (2, NULL)")
	require.NoError(t, err)
	require.Nil(t, sessiontxn.NewTxn(context.Background(), tk.Session()))
	tb, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("tIter"))
	require.NoError(t, err)
	totalCount := 0
	err = tables.IterRecords(tb, tk.Session(), tb.Cols(), func(_ kv.Handle, rec []types.Datum, cols []*table.Column) (bool, error) {
		totalCount++
		require.False(t, rec[0].IsNull())
		return true, nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, totalCount)
	txn, err := tk.Session().Txn(true)
	require.NoError(t, err)
	require.Nil(t, txn.Commit(context.Background()))
}

func TestTableFromMeta(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE meta (a int primary key auto_increment, b varchar(255) unique)")
	require.Nil(t, sessiontxn.NewTxn(context.Background(), tk.Session()))
	_, err := tk.Session().Txn(true)
	require.NoError(t, err)
	tb, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("meta"))
	require.NoError(t, err)
	tbInfo := tb.Meta().Clone()

	// For test coverage
	tbInfo.Columns[0].GeneratedExprString = "a"
	_, err = tables.TableFromMeta(autoid.NewAllocators(false), tbInfo)
	require.NoError(t, err)

	tbInfo.Columns[0].GeneratedExprString = "test"
	_, err = tables.TableFromMeta(autoid.NewAllocators(false), tbInfo)
	require.Error(t, err)
	tbInfo.Columns[0].State = model.StateNone
	tb, err = tables.TableFromMeta(autoid.NewAllocators(false), tbInfo)
	require.Nil(t, tb)
	require.Error(t, err)
	tbInfo.State = model.StateNone
	tb, err = tables.TableFromMeta(autoid.NewAllocators(false), tbInfo)
	require.Nil(t, tb)
	require.Error(t, err)

	tk.MustExec(`create table t_mock (id int) partition by range (id) (partition p0 values less than maxvalue)`)
	tb, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t_mock"))
	require.NoError(t, err)
	tt := table.MockTableFromMeta(tb.Meta())
	_, ok := tt.(table.PartitionedTable)
	require.True(t, ok)
	tk.MustExec("drop table t_mock")
	require.Equal(t, table.NormalTable, tt.Type())

	tk.MustExec("create table t_meta (a int) shard_row_id_bits = 15")
	tb, err = domain.GetDomain(tk.Session()).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t_meta"))
	require.NoError(t, err)
	_, err = tables.AllocHandle(context.Background(), tk.Session(), tb)
	require.NoError(t, err)

	maxID := 1<<(64-15-1) - 1
	err = tb.Allocators(tk.Session()).Get(autoid.RowIDAllocType).Rebase(context.Background(), int64(maxID), false)
	require.NoError(t, err)

	_, err = tables.AllocHandle(context.Background(), tk.Session(), tb)
	require.Error(t, err)
}

func TestShardRowIDBitsStep(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists shard_t;")
	tk.MustExec("create table shard_t (a int) shard_row_id_bits = 15;")
	tk.MustExec("set @@tidb_shard_allocate_step=3;")
	tk.MustExec("insert into shard_t values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11);")
	rows := tk.MustQuery("select _tidb_rowid from shard_t;").Rows()
	shards := make(map[int]struct{})
	for _, row := range rows {
		id, err := strconv.ParseUint(row[0].(string), 10, 64)
		require.NoError(t, err)
		shards[int(id>>48)] = struct{}{}
	}
	require.Equal(t, 4, len(shards))
}

func TestHiddenColumn(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("DROP DATABASE IF EXISTS test_hidden;")
	tk.MustExec("CREATE DATABASE test_hidden;")
	tk.MustExec("USE test_hidden;")
	tk.MustExec("CREATE TABLE t (a int primary key, b int as (a+1), c int, d int as (c+1) stored, e int, f tinyint as (a+1));")
	tk.MustExec("insert into t values (1, default, 3, default, 5, default);")
	tb, err := dom.InfoSchema().TableByName(model.NewCIStr("test_hidden"), model.NewCIStr("t"))
	require.NoError(t, err)
	colInfo := tb.Meta().Columns
	// Set column b, d, f to hidden
	colInfo[1].Hidden = true
	colInfo[3].Hidden = true
	colInfo[5].Hidden = true
	tc := tb.(*tables.TableCommon)
	// Reset related caches
	tc.VisibleColumns = nil
	tc.WritableColumns = nil
	tc.HiddenColumns = nil
	tc.FullHiddenColsAndVisibleColumns = nil

	// Basic test
	cols := tb.VisibleCols()
	require.NotNil(t, table.FindCol(cols, "a"))
	require.Nil(t, table.FindCol(cols, "b"))
	require.NotNil(t, table.FindCol(cols, "c"))
	require.Nil(t, table.FindCol(cols, "d"))
	require.NotNil(t, table.FindCol(cols, "e"))
	hiddenCols := tb.HiddenCols()
	require.Nil(t, table.FindCol(hiddenCols, "a"))
	require.NotNil(t, table.FindCol(hiddenCols, "b"))
	require.Nil(t, table.FindCol(hiddenCols, "c"))
	require.NotNil(t, table.FindCol(hiddenCols, "d"))
	require.Nil(t, table.FindCol(hiddenCols, "e"))
	colInfo[1].State = model.StateDeleteOnly
	colInfo[2].State = model.StateDeleteOnly
	fullHiddenColsAndVisibleColumns := tb.FullHiddenColsAndVisibleCols()
	require.NotNil(t, table.FindCol(fullHiddenColsAndVisibleColumns, "a"))
	require.NotNil(t, table.FindCol(fullHiddenColsAndVisibleColumns, "b"))
	require.Nil(t, table.FindCol(fullHiddenColsAndVisibleColumns, "c"))
	require.NotNil(t, table.FindCol(fullHiddenColsAndVisibleColumns, "d"))
	require.NotNil(t, table.FindCol(fullHiddenColsAndVisibleColumns, "e"))
	// Reset schema states.
	colInfo[1].State = model.StatePublic
	colInfo[2].State = model.StatePublic

	// Test show create table
	tk.MustQuery("show create table t;").Check(testkit.Rows(
		"t CREATE TABLE `t` (\n" +
			"  `a` int(11) NOT NULL,\n" +
			"  `c` int(11) DEFAULT NULL,\n" +
			"  `e` int(11) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Test show (extended) columns
	tk.MustQuery("show columns from t").Check(testkit.RowsWithSep("|",
		"a|int(11)|NO|PRI|<nil>|",
		"c|int(11)|YES||<nil>|",
		"e|int(11)|YES||<nil>|"))
	tk.MustQuery("show extended columns from t").Check(testkit.RowsWithSep("|",
		"a|int(11)|NO|PRI|<nil>|",
		"b|int(11)|YES||<nil>|VIRTUAL GENERATED",
		"c|int(11)|YES||<nil>|",
		"d|int(11)|YES||<nil>|STORED GENERATED",
		"e|int(11)|YES||<nil>|",
		"f|tinyint(4)|YES||<nil>|VIRTUAL GENERATED"))

	// `SELECT` statement
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 3 5"))
	tk.MustQuery("select a, c, e from t;").Check(testkit.Rows("1 3 5"))

	// Can't use hidden columns in `SELECT` statement
	tk.MustGetErrMsg("select b from t;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("select b+1 from t;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("select b, c from t;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("select a, d from t;", "[planner:1054]Unknown column 'd' in 'field list'")
	tk.MustGetErrMsg("select d, b from t;", "[planner:1054]Unknown column 'd' in 'field list'")
	tk.MustGetErrMsg("select * from t where b > 1;", "[planner:1054]Unknown column 'b' in 'where clause'")
	tk.MustGetErrMsg("select * from t order by b;", "[planner:1054]Unknown column 'b' in 'order clause'")
	tk.MustGetErrMsg("select * from t group by b;", "[planner:1054]Unknown column 'b' in 'group statement'")

	// Can't use hidden columns in `INSERT` statement
	// 1. insert into ... values ...
	tk.MustGetErrMsg("insert into t values (1, 2, 3, 4, 5, 6);", "[planner:1136]Column count doesn't match value count at row 1")
	tk.MustGetErrMsg("insert into t(b) values (2)", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t(b, c) values (2, 3);", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t(a, d) values (1, 4);", "[planner:1054]Unknown column 'd' in 'field list'")
	tk.MustGetErrMsg("insert into t(d, b) values (4, 2);", "[planner:1054]Unknown column 'd' in 'field list'")
	tk.MustGetErrMsg("insert into t(a) values (b);", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t(a) values (d+1);", "[planner:1054]Unknown column 'd' in 'field list'")
	// 2. insert into ... set ...
	tk.MustGetErrMsg("insert into t set b = 2;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t set b = 2, c = 3;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t set a = 1, d = 4;", "[planner:1054]Unknown column 'd' in 'field list'")
	tk.MustGetErrMsg("insert into t set d = 4, b = 2;", "[planner:1054]Unknown column 'd' in 'field list'")
	tk.MustGetErrMsg("insert into t set a = b;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t set a = d + 1;", "[planner:1054]Unknown column 'd' in 'field list'")
	// 3. insert into ... on duplicated key update ...
	tk.MustGetErrMsg("insert into t set a = 1 on duplicate key update b = 2;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t set a = 1 on duplicate key update b = 2, c = 3;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t set a = 1 on duplicate key update c = 3, d = 4;", "[planner:1054]Unknown column 'd' in 'field list'")
	tk.MustGetErrMsg("insert into t set a = 1 on duplicate key update d = 4, b = 2;", "[planner:1054]Unknown column 'd' in 'field list'")
	tk.MustGetErrMsg("insert into t set a = 1 on duplicate key update c = b;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t set a = 1 on duplicate key update c = d + 1;", "[planner:1054]Unknown column 'd' in 'field list'")
	// 4. replace into ... set ...
	tk.MustGetErrMsg("replace into t set a = 1, b = 2;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("replace into t set a = 1, b = 2, c = 3;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("replace into t set a = 1, d = 4;", "[planner:1054]Unknown column 'd' in 'field list'")
	tk.MustGetErrMsg("replace into t set a = 1, d = 4, b = 2;", "[planner:1054]Unknown column 'd' in 'field list'")
	tk.MustGetErrMsg("replace into t set a = 1, c = b;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("replace into t set a = 1, c = d + 1;", "[planner:1054]Unknown column 'd' in 'field list'")
	// 5. insert into ... select ...
	tk.MustExec("create table t1(a int, b int, c int, d int);")
	tk.MustGetErrMsg("insert into t1 select b from t;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t1 select b+1 from t;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t1 select b, c from t;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("insert into t1 select a, d from t;", "[planner:1054]Unknown column 'd' in 'field list'")
	tk.MustGetErrMsg("insert into t1 select d, b from t;", "[planner:1054]Unknown column 'd' in 'field list'")
	tk.MustGetErrMsg("insert into t1 select a from t where b > 1;", "[planner:1054]Unknown column 'b' in 'where clause'")
	tk.MustGetErrMsg("insert into t1 select a from t order by b;", "[planner:1054]Unknown column 'b' in 'order clause'")
	tk.MustGetErrMsg("insert into t1 select a from t group by b;", "[planner:1054]Unknown column 'b' in 'group statement'")
	tk.MustExec("drop table t1")

	// `UPDATE` statement
	tk.MustGetErrMsg("update t set b = 2;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("update t set b = 2, c = 3;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("update t set a = 1, d = 4;", "[planner:1054]Unknown column 'd' in 'field list'")

	// FIXME: This sql return unknown column 'd' in MySQL
	tk.MustGetErrMsg("update t set d = 4, b = 2;", "[planner:1054]Unknown column 'b' in 'field list'")

	tk.MustGetErrMsg("update t set a = b;", "[planner:1054]Unknown column 'b' in 'field list'")
	tk.MustGetErrMsg("update t set a = d + 1;", "[planner:1054]Unknown column 'd' in 'field list'")
	tk.MustGetErrMsg("update t set a=1 where b=1;", "[planner:1054]Unknown column 'b' in 'where clause'")
	tk.MustGetErrMsg("update t set a=1 where c=3 order by b;", "[planner:1054]Unknown column 'b' in 'order clause'")

	// `DELETE` statement
	tk.MustExec("delete from t;")
	tk.MustQuery("select count(*) from t;").Check(testkit.Rows("0"))
	tk.MustExec("insert into t values (1, 3, 5);")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 3 5"))
	tk.MustGetErrMsg("delete from t where b = 1;", "[planner:1054]Unknown column 'b' in 'where clause'")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 3 5"))
	tk.MustGetErrMsg("delete from t order by d = 1;", "[planner:1054]Unknown column 'd' in 'order clause'")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 3 5"))

	// `DROP COLUMN` statement
	tk.MustGetErrMsg("ALTER TABLE t DROP COLUMN b;", "[ddl:1091]Can't DROP 'b'; check that column/key exists")
	tk.MustQuery("show create table t;").Check(testkit.Rows(
		"t CREATE TABLE `t` (\n" +
			"  `a` int(11) NOT NULL,\n" +
			"  `c` int(11) DEFAULT NULL,\n" +
			"  `e` int(11) DEFAULT NULL,\n" +
			"  PRIMARY KEY (`a`) /*T![clustered_index] CLUSTERED */\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("show extended columns from t").Check(testkit.RowsWithSep("|",
		"a|int(11)|NO|PRI|<nil>|",
		"b|int(11)|YES||<nil>|VIRTUAL GENERATED",
		"c|int(11)|YES||<nil>|",
		"d|int(11)|YES||<nil>|STORED GENERATED",
		"e|int(11)|YES||<nil>|",
		"f|tinyint(4)|YES||<nil>|VIRTUAL GENERATED"))
}

func TestAddRecordWithCtx(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	_, err := tk.Session().Execute(context.Background(), "DROP TABLE IF EXISTS test.tRecord")
	require.NoError(t, err)
	_, err = tk.Session().Execute(context.Background(), "CREATE TABLE test.tRecord (a bigint unsigned primary key, b varchar(255))")
	require.NoError(t, err)
	tb, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("tRecord"))
	require.NoError(t, err)
	defer func() {
		_, err := tk.Session().Execute(context.Background(), "DROP TABLE test.tRecord")
		require.NoError(t, err)
	}()

	require.Nil(t, sessiontxn.NewTxn(context.Background(), tk.Session()))
	_, err = tk.Session().Txn(true)
	require.NoError(t, err)
	recordCtx := tables.NewCommonAddRecordCtx(len(tb.Cols()))
	tables.SetAddRecordCtx(tk.Session(), recordCtx)
	defer tables.ClearAddRecordCtx(tk.Session())

	records := [][]types.Datum{types.MakeDatums(uint64(1), "abc"), types.MakeDatums(uint64(2), "abcd")}
	for _, r := range records {
		rid, err := tb.AddRecord(tk.Session(), r)
		require.NoError(t, err)
		row, err := tables.RowWithCols(tb.(table.PhysicalTable), tk.Session(), rid, tb.Cols())
		require.NoError(t, err)
		require.Equal(t, len(r), len(row))
		require.Equal(t, types.KindUint64, row[0].Kind())
	}

	i := 0
	err = tables.IterRecords(tb, tk.Session(), tb.Cols(), func(_ kv.Handle, rec []types.Datum, cols []*table.Column) (bool, error) {
		i++
		return true, nil
	})
	require.NoError(t, err)
	require.Equal(t, len(records), i)

	tk.Session().StmtCommit(context.Background())
	txn, err := tk.Session().Txn(true)
	require.NoError(t, err)
	require.Nil(t, txn.Commit(context.Background()))
}

func TestConstraintCheckForUniqueIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@autocommit = 1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ttt")
	tk.MustExec("create table ttt(id int(11) NOT NULL AUTO_INCREMENT,k int(11) NOT NULL DEFAULT '0',c char(120) NOT NULL DEFAULT '',PRIMARY KEY (id),UNIQUE KEY k_1 (k,c))")
	tk.MustExec("insert into ttt(k,c) values(1, 'tidb')")
	tk.MustExec("insert into ttt(k,c) values(2, 'tidb')")
	_, err := tk.Exec("update ttt set k=1 where id=2")
	require.Equal(t, "[kv:1062]Duplicate entry '1-tidb' for key 'ttt.k_1'", err.Error())
	tk.MustExec("rollback")

	// no auto-commit
	tk.MustExec("set @@autocommit = 0")
	tk.MustExec("set @@tidb_constraint_check_in_place = 0")
	tk.MustExec("begin")
	_, err = tk.Exec("update ttt set k=1 where id=2")
	require.Equal(t, "[kv:1062]Duplicate entry '1-tidb' for key 'ttt.k_1'", err.Error())
	tk.MustExec("rollback")

	tk.MustExec("set @@tidb_constraint_check_in_place = 1")
	tk.MustExec("begin")
	_, err = tk.Exec("update ttt set k=1 where id=2")
	require.Equal(t, "[kv:1062]Duplicate entry '1-tidb' for key 'ttt.k_1'", err.Error())
	tk.MustExec("rollback")

	// This test check that with @@tidb_constraint_check_in_place = 0, although there is not KV request for the unique index, the pessimistic lock should still be written.
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("set @@tidb_txn_mode = 'pessimistic'")
	tk1.MustExec("set @@tidb_constraint_check_in_place = 0")
	tk2.MustExec("set @@tidb_txn_mode = 'pessimistic'")
	tk1.MustExec("use test")
	tk1.MustExec("begin")
	tk1.MustExec("update ttt set k=3 where id=2")

	ch := make(chan int, 2)
	go func() {
		tk2.MustExec("use test")
		_, err := tk2.Exec("insert into ttt(k,c) values(3, 'tidb')")
		require.Error(t, err)
		ch <- 2
	}()
	// Sleep 100ms for tk2 to execute, if it's not blocked, 2 should have been sent to the channel.
	time.Sleep(100 * time.Millisecond)
	ch <- 1
	_, err = tk1.Exec("commit")
	require.NoError(t, err)
	// The data in channel is 1 means tk2 is blocked, that's the expected behavior.
	require.Equal(t, 1, <-ch)

	// Unrelated to the test logic, just wait for the goroutine to exit to avoid leak in test
	require.Equal(t, 2, <-ch)
}

func TestViewColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b varchar(20))")
	tk.MustExec("drop view if exists v")
	tk.MustExec("create view v as select * from t")
	tk.MustExec("drop view if exists va")
	tk.MustExec("create view va as select count(a) from t")
	testCases := []struct {
		query    string
		expected []string
	}{
		{"select data_type from INFORMATION_SCHEMA.columns where table_name = 'v'", []string{types.TypeToStr(mysql.TypeLong, ""), types.TypeToStr(mysql.TypeVarchar, "")}},
		{"select data_type from INFORMATION_SCHEMA.columns where table_name = 'va'", []string{types.TypeToStr(mysql.TypeLonglong, "")}},
	}
	for _, testCase := range testCases {
		tk.MustQuery(testCase.query).Check(testkit.RowsWithSep("|", testCase.expected...))
	}
	tk.MustExec("drop table if exists t")
	for _, testCase := range testCases {
		require.Len(t, tk.MustQuery(testCase.query).Rows(), 0)
		tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
			"Warning|1356|View 'test.v' references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them",
			"Warning|1356|View 'test.va' references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them"))
	}
}

func TestConstraintCheckForOptimisticUntouched(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists test_optimistic_untouched_flag;")
	tk.MustExec(`create table test_optimistic_untouched_flag(c0 int, c1 varchar(20), c2 varchar(20), unique key uk(c0));`)
	tk.MustExec(`insert into test_optimistic_untouched_flag(c0, c1, c2) values (1, null, 'green');`)

	// Insert a row with duplicated entry on the unique key, the commit should fail.
	tk.MustExec("begin optimistic;")
	tk.MustExec(`insert into test_optimistic_untouched_flag(c0, c1, c2) values (1, 'red', 'white');`)
	tk.MustExec(`delete from test_optimistic_untouched_flag where c1 is null;`)
	tk.MustExec("update test_optimistic_untouched_flag set c2 = 'green' where c2 between 'purple' and 'white';")
	err := tk.ExecToErr("commit")
	require.Error(t, err)

	tk.MustExec("begin optimistic;")
	tk.MustExec(`insert into test_optimistic_untouched_flag(c0, c1, c2) values (1, 'red', 'white');`)
	tk.MustExec("update test_optimistic_untouched_flag set c2 = 'green' where c2 between 'purple' and 'white';")
	err = tk.ExecToErr("commit")
	require.Error(t, err)
}

func TestTxnAssertion(t *testing.T) {
	store := testkit.CreateMockStore(t)

	se, err := session.CreateSession4Test(store)
	se.SetConnectionID(1)
	require.NoError(t, err)
	require.NoError(t, se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk := testkit.NewTestKit(t, store)
	tk.SetSession(se)

	fpAdd := "github.com/pingcap/tidb/table/tables/addRecordForceAssertExist"
	fpUpdate := "github.com/pingcap/tidb/table/tables/updateRecordForceAssertNotExist"
	fpRemove := "github.com/pingcap/tidb/table/tables/removeRecordForceAssertNotExist"

	runStmtInTxn := func(pessimistic bool, stmts ...string) error {
		if pessimistic {
			tk.MustExec("begin pessimistic")
		} else {
			tk.MustExec("begin optimistic")
		}
		for _, stmt := range stmts {
			tk.MustExec(stmt)
		}
		return tk.ExecToErr("commit")
	}

	withFailpoint := func(fp string, f func()) {
		require.NoError(t, failpoint.Enable(fp, "return"))
		defer func() {
			require.NoError(t, failpoint.Disable(fp))
		}()
		f()
	}

	expectAssertionErr := func(assertionLevel string, err error) {
		if assertionLevel == "STRICT" {
			require.NotNil(t, err)
			require.Contains(t, err.Error(), "assertion failed")
		} else {
			require.NoError(t, err)
		}
	}

	testAssertionBasicImpl := func(level string, lock bool, lockIdx bool, useCommonHandle bool) {
		tk.MustExec("set @@tidb_txn_assertion_level = " + level)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		if useCommonHandle {
			tk.MustExec("create table t(id varchar(64) primary key clustered, v int, v2 int, v3 int, v4 varchar(64), index(v2), unique index(v3), index(v4))")
		} else {
			tk.MustExec("create table t(id int primary key, v int, v2 int, v3 int, v4 varchar(64), index(v2), unique index(v3), index(v4))")
		}

		var id1, id2, id3 interface{}
		if useCommonHandle {
			id1, id2, id3 = "1", "2", "3"
		} else {
			id1, id2, id3 = 1, 2, 3
		}

		// Auto commit
		tk.MustExec("insert into t values (?, 10, 100, 1000, '10000')", id1)
		tk.MustExec("update t set v = v + 1 where id = ?", id1)
		tk.MustExec("delete from t where id = 1")

		// Optimistic
		tk.MustExec("insert into t values (?, 20, 200, 2000, '20000'), (?, 30, 300, 3000, '30000')", id2, id3)
		tk.MustExec("begin optimistic")
		if lock {
			tk.MustExec("select * from t where id in (?, ?, ?) for update", id1, id2, id3)
		}
		if lockIdx {
			tk.MustExec("select * from t where v3 in (1000, 2000, 3000) for update")
		}
		tk.MustExec("insert into t values (?, 10, 100, 1000, '10000')", id1)
		tk.MustExec("update t set v = v + 1 where id = ?", id2)
		tk.MustExec("delete from t where id = ?", id3)
		tk.MustExec("commit")

		// Pessimistic
		tk.MustExec("delete from t")
		tk.MustExec("insert into t values (?, 20, 200, 2000, '20000'), (?, 30, 300, 3000, '30000')", id2, id3)
		tk.MustExec("begin pessimistic")
		if lock {
			tk.MustExec("select * from t where id in (?, ?, ?) for update", id1, id2, id3)
		}
		if lockIdx {
			tk.MustExec("select * from t where v3 in (1000, 2000, 3000) for update")
		}
		tk.MustExec("insert into t values (?, 10, 100, 1000, '10000')", id1)
		tk.MustExec("update t set v = v + 1 where id = ?", id2)
		tk.MustExec("delete from t where id = ?", id3)
		tk.MustExec("commit")

		// Inject incorrect assertion so that it must fail.

		// Auto commit
		tk.MustExec("delete from t")
		tk.MustExec("insert into t values (?, 20, 200, 2000, '20000'), (?, 30, 300, 3000, '30000')", id2, id3)
		withFailpoint(fpAdd, func() {
			err = tk.ExecToErr("insert into t values (?, 10, 100, 1000, '10000')", id1)
			expectAssertionErr(level, err)
		})
		withFailpoint(fpUpdate, func() {
			err = tk.ExecToErr("update t set v = v + 1 where id = ?", id2)
			expectAssertionErr(level, err)
		})
		withFailpoint(fpRemove, func() {
			err = tk.ExecToErr("delete from t where id = ?", id3)
			expectAssertionErr(level, err)
		})

		var lockStmts []string = nil
		if lock {
			lockStmts = append(lockStmts, fmt.Sprintf("select * from t where id in (%#v, %#v, %#v) for update", id1, id2, id3))
		}
		if lockIdx {
			lockStmts = append(lockStmts, "select * from t where v3 in (1000, 2000, 3000) for update")
		}

		// Optimistic
		tk.MustExec("delete from t")
		tk.MustExec("insert into t values (?, 20, 200, 2000, '20000'), (?, 30, 300, 3000, '30000')", id2, id3)
		withFailpoint(fpAdd, func() {
			err = runStmtInTxn(false, append(lockStmts, fmt.Sprintf("insert into t values (%#v, 10, 100, 1000, '10000')", id1))...)
			expectAssertionErr(level, err)
		})
		withFailpoint(fpUpdate, func() {
			err = runStmtInTxn(false, append(lockStmts, fmt.Sprintf("update t set v = v + 1 where id = %#v", id2))...)
			expectAssertionErr(level, err)
		})
		withFailpoint(fpRemove, func() {
			err = runStmtInTxn(false, append(lockStmts, fmt.Sprintf("delete from t where id = %#v", id3))...)
			expectAssertionErr(level, err)
		})

		// Pessimistic
		tk.MustExec("delete from t")
		tk.MustExec("insert into t values (?, 20, 200, 2000, '20000'), (?, 30, 300, 3000, '30000')", id2, id3)
		withFailpoint(fpAdd, func() {
			err = runStmtInTxn(true, append(lockStmts, fmt.Sprintf("insert into t values (%#v, 10, 100, 1000, '10000')", id1))...)
			expectAssertionErr(level, err)
		})
		withFailpoint(fpUpdate, func() {
			err = runStmtInTxn(true, append(lockStmts, fmt.Sprintf("update t set v = v + 1 where id = %#v", id2))...)
			expectAssertionErr(level, err)
		})
		withFailpoint(fpRemove, func() {
			err = runStmtInTxn(true, append(lockStmts, fmt.Sprintf("delete from t where id = %#v", id3))...)
			expectAssertionErr(level, err)
		})
	}

	for _, level := range []string{"STRICT", "OFF"} {
		for _, lock := range []bool{false, true} {
			for _, lockIdx := range []bool{false, true} {
				for _, useCommonHandle := range []bool{false, true} {
					t.Logf("testing testAssertionBasicImpl level: %v, lock: %v, lockIdx: %v, useCommonHandle: %v...", level, lock, lockIdx, useCommonHandle)
					testAssertionBasicImpl(level, lock, lockIdx, useCommonHandle)
				}
			}
		}
	}

	testUntouchedIndexImpl := func(level string, pessimistic bool) {
		tk.MustExec("set @@tidb_txn_assertion_level = " + level)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(id int primary key, v int, v2 int, v3 int, index(v2), unique index(v3))")
		tk.MustExec("insert into t values (1, 10, 100, 1000)")

		if pessimistic {
			tk.MustExec("begin pessimistic")
		} else {
			tk.MustExec("begin optimistic")
		}
		tk.MustExec("update t set v = v + 1 where id = 1")
		tk.MustExec("delete from t where id = 1")
		tk.MustExec("insert into t values (1, 11, 101, 1001)")
		tk.MustExec("commit")
	}

	testUntouchedIndexImpl("STRICT", false)
	testUntouchedIndexImpl("STRICT", true)
	testUntouchedIndexImpl("OFF", false)
	testUntouchedIndexImpl("OFF", true)
}

func TestWriteWithChecksums(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	h := helper.NewHelper(store.(helper.Storage))

	tkDDL := testkit.NewTestKit(t, store)
	tkDDL.MustExec("set global tidb_enable_row_level_checksum = 1")
	tkDDL.MustExec("use test")

	tkDML := testkit.NewTestKit(t, store)
	tkDML.MustExec("use test")

	type col struct {
		ID   int64
		Type byte
		Data types.Datum
	}
	isDMLBeforeDDL := func(seq int64) bool { return seq == -1 }
	isDMLAfterDDL := func(seq int64) bool { return seq == -2 }

	for _, tt := range []struct {
		name   string
		init   []string
		schema []col
		ddl    string
		dml    func(seq int64, job *model.Job) ([]byte, [][]col)
	}{
		{
			name: "AddRecord/AddColumn",
			init: []string{"create table t (id int primary key, c1 int)"},
			schema: []col{
				{ID: 1, Type: mysql.TypeLong},
				{ID: 2, Type: mysql.TypeLong},
				{ID: 3, Type: mysql.TypeLong},
			},
			ddl: "alter table t add column c2 int",
			dml: func(seq int64, job *model.Job) ([]byte, [][]col) {
				tkDML.MustExec("insert into t (id, c1) values (?, ?)", seq, seq+1)
				tbl := external.GetTableByName(t, tkDML, "test", "t")
				key := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(seq))
				col1 := []col{
					{1, mysql.TypeLong, types.NewDatum(seq)},
					{2, mysql.TypeLong, types.NewDatum(seq + 1)},
				}
				col2 := []col{
					{1, mysql.TypeLong, types.NewDatum(seq)},
					{2, mysql.TypeLong, types.NewDatum(seq + 1)},
					{3, mysql.TypeLong, types.NewDatum(nil)},
				}
				if isDMLBeforeDDL(seq) {
					return key, [][]col{col1}
				}
				if isDMLAfterDDL(seq) {
					return key, [][]col{col2}
				}
				return key, [][]col{col2, col1}
			},
		},
		{
			name: "AddRecord/AddColumnWithDefault",
			init: []string{"create table t (id int primary key, c1 int)"},
			schema: []col{
				{ID: 1, Type: mysql.TypeLong},
				{ID: 2, Type: mysql.TypeLong},
				{ID: 3, Type: mysql.TypeLong},
			},
			ddl: "alter table t add column c2 int default 42",
			dml: func(seq int64, job *model.Job) ([]byte, [][]col) {
				tkDML.MustExec("insert into t (id, c1) values (?, ?)", seq, seq+1)
				tbl := external.GetTableByName(t, tkDML, "test", "t")
				key := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(seq))
				col1 := []col{
					{1, mysql.TypeLong, types.NewDatum(seq)},
					{2, mysql.TypeLong, types.NewDatum(seq + 1)},
				}
				col2 := []col{
					{1, mysql.TypeLong, types.NewDatum(seq)},
					{2, mysql.TypeLong, types.NewDatum(seq + 1)},
					{3, mysql.TypeLong, types.NewDatum(42)},
				}
				if isDMLBeforeDDL(seq) {
					return key, [][]col{col1}
				}
				if isDMLAfterDDL(seq) {
					return key, [][]col{col2}
				}
				return key, [][]col{col2, col1}
			},
		},
		{
			name: "AddRecord/AddColumnNotNull",
			init: []string{"create table t (id int primary key, c1 int)"},
			schema: []col{
				{ID: 1, Type: mysql.TypeLong},
				{ID: 2, Type: mysql.TypeLong},
				{ID: 3, Type: mysql.TypeLong},
			},
			ddl: "alter table t add column c2 int not null",
			dml: func(seq int64, job *model.Job) ([]byte, [][]col) {
				if isDMLAfterDDL(seq) {
					tkDML.MustExec("insert into t (id, c1, c2) values (?, ?, ?)", seq, seq+1, seq+2)
				} else {
					tkDML.MustExec("insert into t (id, c1) values (?, ?)", seq, seq+1)
				}
				tbl := external.GetTableByName(t, tkDML, "test", "t")
				key := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(seq))
				col1 := []col{
					{1, mysql.TypeLong, types.NewDatum(seq)},
					{2, mysql.TypeLong, types.NewDatum(seq + 1)},
				}
				col2 := []col{
					{1, mysql.TypeLong, types.NewDatum(seq)},
					{2, mysql.TypeLong, types.NewDatum(seq + 1)},
					{3, mysql.TypeLong, types.NewDatum(0)},
				}
				col3 := []col{
					{1, mysql.TypeLong, types.NewDatum(seq)},
					{2, mysql.TypeLong, types.NewDatum(seq + 1)},
					{3, mysql.TypeLong, types.NewDatum(seq + 2)},
				}
				if isDMLBeforeDDL(seq) {
					return key, [][]col{col1}
				}
				if isDMLAfterDDL(seq) {
					return key, [][]col{col3}
				}
				return key, [][]col{col2, col1}
			},
		},
		{
			name: "AddRecord/DropColumn",
			init: []string{"create table t (id int primary key, c1 int, c2 int)"},
			schema: []col{
				{ID: 1, Type: mysql.TypeLong},
				{ID: 2, Type: mysql.TypeLong},
				{ID: 3, Type: mysql.TypeLong},
			},
			ddl: "alter table t drop column c2",
			dml: func(seq int64, job *model.Job) ([]byte, [][]col) {
				tkDML.MustExec("insert into t (id, c1) values (?, ?)", seq, seq+1)
				tbl := external.GetTableByName(t, tkDML, "test", "t")
				key := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(seq))
				col1 := []col{
					{1, mysql.TypeLong, types.NewDatum(seq)},
					{2, mysql.TypeLong, types.NewDatum(seq + 1)},
					{3, mysql.TypeLong, types.NewDatum(nil)},
				}
				col2 := []col{
					{1, mysql.TypeLong, types.NewDatum(seq)},
					{2, mysql.TypeLong, types.NewDatum(seq + 1)},
				}
				if isDMLBeforeDDL(seq) {
					return key, [][]col{col1}
				}
				if isDMLAfterDDL(seq) {
					return key, [][]col{col2}
				}
				return key, [][]col{col1, col2}
			},
		},
		{
			name: "AddRecord/DropColumnWithDefault",
			init: []string{"create table t (id int primary key, c1 int, c2 int default 42)"},
			schema: []col{
				{ID: 1, Type: mysql.TypeLong},
				{ID: 2, Type: mysql.TypeLong},
				{ID: 3, Type: mysql.TypeLong},
			},
			ddl: "alter table t drop column c2",
			dml: func(seq int64, job *model.Job) ([]byte, [][]col) {
				tkDML.MustExec("insert into t (id, c1) values (?, ?)", seq, seq+1)
				tbl := external.GetTableByName(t, tkDML, "test", "t")
				key := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(seq))
				col1 := []col{
					{1, mysql.TypeLong, types.NewDatum(seq)},
					{2, mysql.TypeLong, types.NewDatum(seq + 1)},
					{3, mysql.TypeLong, types.NewDatum(42)},
				}
				col2 := []col{
					{1, mysql.TypeLong, types.NewDatum(seq)},
					{2, mysql.TypeLong, types.NewDatum(seq + 1)},
					{3, mysql.TypeLong, types.NewDatum(nil)},
				}
				col3 := []col{
					{1, mysql.TypeLong, types.NewDatum(seq)},
					{2, mysql.TypeLong, types.NewDatum(seq + 1)},
				}
				if isDMLBeforeDDL(seq) {
					return key, [][]col{col1}
				}
				if isDMLAfterDDL(seq) {
					return key, [][]col{col3}
				}
				return key, [][]col{col2, col3}
			},
		},
		{
			name: "AddRecord/DropColumnNotNull",
			init: []string{"create table t (id int primary key, c1 int, c2 int not null)"},
			schema: []col{
				{ID: 1, Type: mysql.TypeLong},
				{ID: 2, Type: mysql.TypeLong},
				{ID: 3, Type: mysql.TypeLong},
			},
			ddl: "alter table t drop column c2",
			dml: func(seq int64, job *model.Job) ([]byte, [][]col) {
				if isDMLBeforeDDL(seq) {
					tkDML.MustExec("insert into t (id, c1, c2) values (?, ?, ?)", seq, seq+1, seq+2)
				} else {
					tkDML.MustExec("insert into t (id, c1) values (?, ?)", seq, seq+1)
				}
				tbl := external.GetTableByName(t, tkDML, "test", "t")
				key := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(seq))
				col1 := []col{
					{1, mysql.TypeLong, types.NewDatum(seq)},
					{2, mysql.TypeLong, types.NewDatum(seq + 1)},
					{3, mysql.TypeLong, types.NewDatum(seq + 2)},
				}
				col2 := []col{
					{1, mysql.TypeLong, types.NewDatum(seq)},
					{2, mysql.TypeLong, types.NewDatum(seq + 1)},
					{3, mysql.TypeLong, types.NewDatum(0)},
				}
				col3 := []col{
					{1, mysql.TypeLong, types.NewDatum(seq)},
					{2, mysql.TypeLong, types.NewDatum(seq + 1)},
				}
				if isDMLBeforeDDL(seq) {
					return key, [][]col{col1}
				}
				if isDMLAfterDDL(seq) {
					return key, [][]col{col3}
				}
				return key, [][]col{col2, col3}
			},
		},
		{
			name: "AddRecord/ChangeColumnType",
			init: []string{"create table t (id int primary key, c1 int)"},
			schema: []col{
				{ID: 1, Type: mysql.TypeLong},
				{ID: 2, Type: mysql.TypeLong},
				{ID: 3, Type: mysql.TypeVarchar},
			},
			ddl: "alter table t change column c1 c1 varchar(10)",
			dml: func(seq int64, job *model.Job) ([]byte, [][]col) {
				tkDML.MustExec("insert into t (id, c1) values (?, ?)", seq, seq+1)
				tbl := external.GetTableByName(t, tkDML, "test", "t")
				key := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(seq))
				col1 := []col{
					{1, mysql.TypeLong, types.NewDatum(seq)},
					{2, mysql.TypeLong, types.NewDatum(seq + 1)},
				}
				col2 := []col{
					{1, mysql.TypeLong, types.NewDatum(seq)},
					{3, mysql.TypeVarchar, types.NewDatum(strconv.FormatInt(seq+1, 10))},
				}
				if isDMLBeforeDDL(seq) {
					return key, [][]col{col1}
				}
				if isDMLAfterDDL(seq) {
					return key, [][]col{col2}
				}
				return key, [][]col{col1, col2}
			},
		},
		{
			name: "AddRecord/ChangeColumnTypeFloat",
			init: []string{"create table t (id int primary key, c1 float)"},
			schema: []col{
				{ID: 1, Type: mysql.TypeLong},
				{ID: 2, Type: mysql.TypeFloat},
				{ID: 3, Type: mysql.TypeDouble},
			},
			ddl: "alter table t change column c1 c1 double",
			dml: func(seq int64, job *model.Job) ([]byte, [][]col) {
				v := float64(seq) * 3.14
				tkDML.MustExec("insert into t (id, c1) values (?, ?)", seq, v)
				tbl := external.GetTableByName(t, tkDML, "test", "t")
				key := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(seq))
				col1 := []col{
					{1, mysql.TypeLong, types.NewDatum(seq)},
					{2, mysql.TypeFloat, types.NewDatum(float32(v))},
				}
				col2 := []col{
					{1, mysql.TypeLong, types.NewDatum(seq)},
					{2, mysql.TypeFloat, types.NewDatum(float64(float32(v)))},
				}
				col3 := []col{
					{1, mysql.TypeLong, types.NewDatum(seq)},
					{3, mysql.TypeDouble, types.NewDatum(v)},
				}
				if isDMLBeforeDDL(seq) {
					return key, [][]col{col1}
				}
				if isDMLAfterDDL(seq) {
					return key, [][]col{col3}
				}
				return key, [][]col{col1, col2}
			},
		},
		{
			name: "AddRecord/ChangeColumnTypeDouble",
			init: []string{"create table t (id int primary key, c1 double)"},
			schema: []col{
				{ID: 1, Type: mysql.TypeLong},
				{ID: 2, Type: mysql.TypeDouble},
				{ID: 3, Type: mysql.TypeFloat},
			},
			ddl: "alter table t change column c1 c1 float",
			dml: func(seq int64, job *model.Job) ([]byte, [][]col) {
				v := float64(seq) * 3.14
				tkDML.MustExec("insert into t (id, c1) values (?, ?)", seq, v)
				tbl := external.GetTableByName(t, tkDML, "test", "t")
				key := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(seq))
				col1 := []col{
					{1, mysql.TypeLong, types.NewDatum(seq)},
					{2, mysql.TypeDouble, types.NewDatum(v)},
				}
				col2 := []col{
					{1, mysql.TypeLong, types.NewDatum(seq)},
					{3, mysql.TypeFloat, types.NewDatum(float32(v))},
				}
				if isDMLBeforeDDL(seq) {
					return key, [][]col{col1}
				}
				if isDMLAfterDDL(seq) {
					return key, [][]col{col2}
				}
				return key, [][]col{col1, col2}
			},
		},
		{
			name: "AddRecord/SetColumnDefault",
			init: []string{"create table t (id int primary key, c1 int, c2 int default 1)"},
			schema: []col{
				{ID: 1, Type: mysql.TypeLong},
				{ID: 2, Type: mysql.TypeLong},
				{ID: 3, Type: mysql.TypeLong},
			},
			ddl: "alter table t alter column c2 set default 42",
			dml: func(seq int64, job *model.Job) ([]byte, [][]col) {
				tkDML.MustExec("insert into t (id, c1) values (?, ?)", seq, seq+1)
				tbl := external.GetTableByName(t, tkDML, "test", "t")
				key := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(seq))
				col1 := []col{
					{1, mysql.TypeLong, types.NewDatum(seq)},
					{2, mysql.TypeLong, types.NewDatum(seq + 1)},
					{3, mysql.TypeLong, types.NewDatum(1)},
				}
				col2 := []col{
					{1, mysql.TypeLong, types.NewDatum(seq)},
					{2, mysql.TypeLong, types.NewDatum(seq + 1)},
					{3, mysql.TypeLong, types.NewDatum(42)},
				}
				if isDMLBeforeDDL(seq) {
					return key, [][]col{col1}
				}
				if isDMLAfterDDL(seq) {
					return key, [][]col{col2}
				}
				return key, nil
			},
		},
		{
			name: "AddRecord/DropColumnDefault",
			init: []string{"create table t (id int primary key, c1 int, c2 int default 42)"},
			schema: []col{
				{ID: 1, Type: mysql.TypeLong},
				{ID: 2, Type: mysql.TypeLong},
				{ID: 3, Type: mysql.TypeLong},
			},
			ddl: "alter table t alter column c2 drop default",
			dml: func(seq int64, job *model.Job) ([]byte, [][]col) {
				if isDMLAfterDDL(seq) {
					tkDML.MustExec("insert into t (id, c1, c2) values (?, ?, ?)", seq, seq+1, seq+2)
				} else {
					tkDML.MustExec("insert into t (id, c1) values (?, ?)", seq, seq+1)
				}
				tbl := external.GetTableByName(t, tkDML, "test", "t")
				key := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(seq))
				col1 := []col{
					{1, mysql.TypeLong, types.NewDatum(seq)},
					{2, mysql.TypeLong, types.NewDatum(seq + 1)},
					{3, mysql.TypeLong, types.NewDatum(42)},
				}
				col2 := []col{
					{1, mysql.TypeLong, types.NewDatum(seq)},
					{2, mysql.TypeLong, types.NewDatum(seq + 1)},
					{3, mysql.TypeLong, types.NewDatum(seq + 2)},
				}
				if isDMLBeforeDDL(seq) {
					return key, [][]col{col1}
				}
				if isDMLAfterDDL(seq) {
					return key, [][]col{col2}
				}
				return key, nil
			},
		},
		{
			name: "UpdateRecord/AddColumn",
			init: []string{"create table t (id int primary key, c1 int)", "insert into t values (1, 0)"},
			schema: []col{
				{ID: 1, Type: mysql.TypeLong},
				{ID: 2, Type: mysql.TypeLong},
				{ID: 3, Type: mysql.TypeLong},
			},
			ddl: "alter table t add column c2 int",
			dml: func(seq int64, job *model.Job) ([]byte, [][]col) {
				tkDML.MustExec("update t set c1 = ? where id = 1", seq)
				tbl := external.GetTableByName(t, tkDML, "test", "t")
				key := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(1))
				col1 := []col{
					{1, mysql.TypeLong, types.NewDatum(1)},
					{2, mysql.TypeLong, types.NewDatum(seq)},
				}
				col2 := []col{
					{1, mysql.TypeLong, types.NewDatum(1)},
					{2, mysql.TypeLong, types.NewDatum(seq)},
					{3, mysql.TypeLong, types.NewDatum(nil)},
				}
				if isDMLBeforeDDL(seq) {
					return key, [][]col{col1}
				}
				if isDMLAfterDDL(seq) {
					return key, [][]col{col2}
				}
				return key, [][]col{col2, col1}
			},
		},
		{
			name: "UpdateRecord/AddColumnWithDefault",
			init: []string{"create table t (id int primary key, c1 int)", "insert into t values (1, 0)"},
			schema: []col{
				{ID: 1, Type: mysql.TypeLong},
				{ID: 2, Type: mysql.TypeLong},
				{ID: 3, Type: mysql.TypeLong},
			},
			ddl: "alter table t add column c2 int default 42",
			dml: func(seq int64, job *model.Job) ([]byte, [][]col) {
				tkDML.MustExec("update t set c1 = ? where id = 1", seq)
				tbl := external.GetTableByName(t, tkDML, "test", "t")
				key := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(1))
				col1 := []col{
					{1, mysql.TypeLong, types.NewDatum(1)},
					{2, mysql.TypeLong, types.NewDatum(seq)},
				}
				col2 := []col{
					{1, mysql.TypeLong, types.NewDatum(1)},
					{2, mysql.TypeLong, types.NewDatum(seq)},
					{3, mysql.TypeLong, types.NewDatum(42)},
				}
				if isDMLBeforeDDL(seq) {
					return key, [][]col{col1}
				}
				if isDMLAfterDDL(seq) {
					return key, [][]col{col2}
				}
				return key, [][]col{col2, col1}
			},
		},
		{
			name: "UpdateRecord/AddColumnNotNull",
			init: []string{"create table t (id int primary key, c1 int)", "insert into t values (1, 0)"},
			schema: []col{
				{ID: 1, Type: mysql.TypeLong},
				{ID: 2, Type: mysql.TypeLong},
				{ID: 3, Type: mysql.TypeLong},
			},
			ddl: "alter table t add column c2 int not null",
			dml: func(seq int64, job *model.Job) ([]byte, [][]col) {
				tkDML.MustExec("update t set c1 = ? where id = 1", seq)
				tbl := external.GetTableByName(t, tkDML, "test", "t")
				key := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(1))
				col1 := []col{
					{1, mysql.TypeLong, types.NewDatum(1)},
					{2, mysql.TypeLong, types.NewDatum(seq)},
				}
				col2 := []col{
					{1, mysql.TypeLong, types.NewDatum(1)},
					{2, mysql.TypeLong, types.NewDatum(seq)},
					{3, mysql.TypeLong, types.NewDatum(0)},
				}
				if isDMLBeforeDDL(seq) {
					return key, [][]col{col1}
				}
				if isDMLAfterDDL(seq) {
					return key, [][]col{col2}
				}
				return key, [][]col{col2, col1}
			},
		},
		{
			name: "UpdateRecord/DropColumn",
			init: []string{"create table t (id int primary key, c1 int, c2 int)", "insert into t values (1, 0, 0)"},
			schema: []col{
				{ID: 1, Type: mysql.TypeLong},
				{ID: 2, Type: mysql.TypeLong},
				{ID: 3, Type: mysql.TypeLong},
			},
			ddl: "alter table t drop column c2",
			dml: func(seq int64, job *model.Job) ([]byte, [][]col) {
				tkDML.MustExec("update t set c1 = ? where id = 1", seq)
				tbl := external.GetTableByName(t, tkDML, "test", "t")
				key := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(1))
				col1 := []col{
					{1, mysql.TypeLong, types.NewDatum(1)},
					{2, mysql.TypeLong, types.NewDatum(seq)},
					{3, mysql.TypeLong, types.NewDatum(0)},
				}
				col2 := []col{
					{1, mysql.TypeLong, types.NewDatum(1)},
					{2, mysql.TypeLong, types.NewDatum(seq)},
					{3, mysql.TypeLong, types.NewDatum(nil)},
				}
				col3 := []col{
					{1, mysql.TypeLong, types.NewDatum(1)},
					{2, mysql.TypeLong, types.NewDatum(seq)},
				}
				if isDMLBeforeDDL(seq) {
					return key, [][]col{col1}
				}
				if isDMLAfterDDL(seq) {
					return key, [][]col{col3}
				}
				if job.SchemaState == model.StateWriteOnly {
					return key, [][]col{col1, col3}
				}
				return key, [][]col{col2, col3}
			},
		},
		{
			name: "UpdateRecord/DropColumnWithDefault",
			init: []string{"create table t (id int primary key, c1 int, c2 int default 42)", "insert into t values (1, 0, 0)"},
			schema: []col{
				{ID: 1, Type: mysql.TypeLong},
				{ID: 2, Type: mysql.TypeLong},
				{ID: 3, Type: mysql.TypeLong},
			},
			ddl: "alter table t drop column c2",
			dml: func(seq int64, job *model.Job) ([]byte, [][]col) {
				tkDML.MustExec("update t set c1 = ? where id = 1", seq)
				tbl := external.GetTableByName(t, tkDML, "test", "t")
				key := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(1))
				col1 := []col{
					{1, mysql.TypeLong, types.NewDatum(1)},
					{2, mysql.TypeLong, types.NewDatum(seq)},
					{3, mysql.TypeLong, types.NewDatum(0)},
				}
				col2 := []col{
					{1, mysql.TypeLong, types.NewDatum(1)},
					{2, mysql.TypeLong, types.NewDatum(seq)},
					{3, mysql.TypeLong, types.NewDatum(nil)},
				}
				col3 := []col{
					{1, mysql.TypeLong, types.NewDatum(1)},
					{2, mysql.TypeLong, types.NewDatum(seq)},
				}
				if isDMLBeforeDDL(seq) {
					return key, [][]col{col1}
				}
				if isDMLAfterDDL(seq) {
					return key, [][]col{col3}
				}
				if job.SchemaState == model.StateWriteOnly {
					return key, [][]col{col1, col3}
				}
				return key, [][]col{col2, col3}
			},
		},
		{
			name: "UpdateRecord/DropColumnNotNull",
			init: []string{"create table t (id int primary key, c1 int, c2 int not null)", "insert into t values (1, 0, 10)"},
			schema: []col{
				{ID: 1, Type: mysql.TypeLong},
				{ID: 2, Type: mysql.TypeLong},
				{ID: 3, Type: mysql.TypeLong},
			},
			ddl: "alter table t drop column c2",
			dml: func(seq int64, job *model.Job) ([]byte, [][]col) {
				tkDML.MustExec("update t set c1 = ? where id = 1", seq)
				tbl := external.GetTableByName(t, tkDML, "test", "t")
				key := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(1))
				col1 := []col{
					{1, mysql.TypeLong, types.NewDatum(1)},
					{2, mysql.TypeLong, types.NewDatum(seq)},
					{3, mysql.TypeLong, types.NewDatum(10)},
				}
				col2 := []col{
					{1, mysql.TypeLong, types.NewDatum(1)},
					{2, mysql.TypeLong, types.NewDatum(seq)},
					{3, mysql.TypeLong, types.NewDatum(0)},
				}
				col3 := []col{
					{1, mysql.TypeLong, types.NewDatum(1)},
					{2, mysql.TypeLong, types.NewDatum(seq)},
				}
				if isDMLBeforeDDL(seq) {
					return key, [][]col{col1}
				}
				if isDMLAfterDDL(seq) {
					return key, [][]col{col3}
				}
				if job.SchemaState == model.StateWriteOnly {
					return key, [][]col{col1, col3}
				}
				return key, [][]col{col2, col3}
			},
		},
		{
			name: "UpdateRecord/ChangeColumnType",
			init: []string{"create table t (id int primary key, c1 int)", "insert into t values (1, 0)"},
			schema: []col{
				{ID: 1, Type: mysql.TypeLong},
				{ID: 2, Type: mysql.TypeLong},
				{ID: 3, Type: mysql.TypeVarchar},
			},
			ddl: "alter table t change column c1 c1 varchar(10)",
			dml: func(seq int64, job *model.Job) ([]byte, [][]col) {
				tkDML.MustExec("update t set c1 = ? where id = 1", seq)
				tbl := external.GetTableByName(t, tkDML, "test", "t")
				key := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(1))
				col1 := []col{
					{1, mysql.TypeLong, types.NewDatum(1)},
					{2, mysql.TypeLong, types.NewDatum(seq)},
				}
				col2 := []col{
					{1, mysql.TypeLong, types.NewDatum(1)},
					{3, mysql.TypeVarchar, types.NewDatum(strconv.FormatInt(seq, 10))},
				}
				if isDMLBeforeDDL(seq) {
					return key, [][]col{col1}
				}
				if isDMLAfterDDL(seq) {
					return key, [][]col{col2}
				}
				return key, [][]col{col1, col2}
			},
		},
		{
			name: "UpdateRecord/ChangeColumnTypeFloat",
			init: []string{"create table t (id int primary key, c1 float)", "insert into t values (1, 3.14)"},
			schema: []col{
				{ID: 1, Type: mysql.TypeLong},
				{ID: 2, Type: mysql.TypeFloat},
				{ID: 3, Type: mysql.TypeDouble},
			},
			ddl: "alter table t change column c1 c1 double",
			dml: func(seq int64, job *model.Job) ([]byte, [][]col) {
				v := float64(seq) * 3.14
				tkDML.MustExec("update t set c1 = ? where id = 1", v)
				tbl := external.GetTableByName(t, tkDML, "test", "t")
				key := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(1))
				col1 := []col{
					{1, mysql.TypeLong, types.NewDatum(1)},
					{2, mysql.TypeFloat, types.NewDatum(float32(v))},
				}
				col2 := []col{
					{1, mysql.TypeLong, types.NewDatum(1)},
					{2, mysql.TypeFloat, types.NewDatum(float64(float32(v)))},
				}
				col3 := []col{
					{1, mysql.TypeLong, types.NewDatum(1)},
					{3, mysql.TypeDouble, types.NewDatum(v)},
				}
				if isDMLBeforeDDL(seq) {
					return key, [][]col{col1}
				}
				if isDMLAfterDDL(seq) {
					return key, [][]col{col3}
				}
				return key, [][]col{col1, col2}
			},
		},
		{
			name: "UpdateRecord/ChangeColumnTypeDouble",
			init: []string{"create table t (id int primary key, c1 double)", "insert into t values (1, 3.14)"},
			schema: []col{
				{ID: 1, Type: mysql.TypeLong},
				{ID: 2, Type: mysql.TypeDouble},
				{ID: 3, Type: mysql.TypeFloat},
			},
			ddl: "alter table t change column c1 c1 float",
			dml: func(seq int64, job *model.Job) ([]byte, [][]col) {
				v := float64(seq) * 3.14
				tkDML.MustExec("update t set c1 = ? where id = 1", v)
				tbl := external.GetTableByName(t, tkDML, "test", "t")
				key := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(1))
				col1 := []col{
					{1, mysql.TypeLong, types.NewDatum(1)},
					{2, mysql.TypeDouble, types.NewDatum(v)},
				}
				col2 := []col{
					{1, mysql.TypeLong, types.NewDatum(1)},
					{2, mysql.TypeFloat, types.NewDatum(float32(v))},
				}

				if isDMLBeforeDDL(seq) {
					return key, [][]col{col1}
				}
				if isDMLAfterDDL(seq) {
					return key, [][]col{col2}
				}
				return key, [][]col{col1, col2}
			},
		},
	} {
		// build row decoder for extracting checksums from row
		cols := make([]rowcodec.ColInfo, len(tt.schema))
		for i, col := range tt.schema {
			cols[i] = rowcodec.ColInfo{ID: col.ID, Ft: types.NewFieldType(col.Type)}
		}
		dec := rowcodec.NewDatumMapDecoder(cols, time.UTC)
		// build a function for executing dml and validating results
		doDML := func(t *testing.T, seq int64, job *model.Job) {
			key, rows := tt.dml(seq, job)
			// get actualChecksums in row value
			actualChecksums := make([]uint32, 0, 2)
			data, err := h.GetMvccByEncodedKey(key)
			assert.NoError(t, err)
			_, err = dec.DecodeToDatumMap(data.Info.Writes[0].ShortValue, nil)
			assert.NoError(t, err)
			if checksum, ok := dec.GetChecksum(); ok {
				actualChecksums = append(actualChecksums, checksum)
				if checksum, ok := dec.GetExtraChecksum(); ok {
					actualChecksums = append(actualChecksums, checksum)
				}
			}
			// calc expected checksums from row data
			expectChecksums := make([]uint32, 0, 2)
			for _, row := range rows {
				cols := make([]rowcodec.ColData, len(row))
				for i := range row {
					ft := types.NewFieldType(row[i].Type)
					cols[i] = rowcodec.ColData{
						ColumnInfo: &model.ColumnInfo{ID: row[i].ID, FieldType: *ft},
						Datum:      &row[i].Data,
					}
				}
				data := rowcodec.RowData{Cols: cols}
				sort.Sort(data)
				checksum, err := data.Checksum()
				assert.NoError(t, err)
				expectChecksums = append(expectChecksums, checksum)
			}
			// validate checksums
			assert.Equal(t, expectChecksums, actualChecksums)
		}

		// init and run sub test
		tkDDL.MustExec("drop table if exists t")
		for _, sql := range tt.init {
			tkDDL.MustExec(sql)
		}
		t.Run(tt.name, func(t *testing.T) {
			origHook := dom.DDL().GetHook()
			defer dom.DDL().SetHook(origHook)

			var seq int64
			dom.DDL().SetHook(&ddlCallback{
				onJobUpdated: func(job *model.Job) {
					if job.State != model.JobStateRunning {
						return
					}
					doDML(t, atomic.AddInt64(&seq, 1), job)
				},
			})

			doDML(t, -1, nil)
			tkDDL.MustExec(tt.ddl)
			doDML(t, -2, nil)
		})
		tkDDL.MustExec("admin check table t")
	}
}

type ddlCallback struct {
	onChanged            func(err error) error
	onSchemaStateChanged func(schemaVer int64)
	onJobRunBefore       func(job *model.Job)
	onJobRunAfter        func(job *model.Job)
	onJobUpdated         func(job *model.Job)
	onWatched            func(ctx context.Context)
	onGetJobBefore       func(jobType string)
	onGetJobAfter        func(jobType string, job *model.Job)
}

func (cb *ddlCallback) OnChanged(err error) error {
	if cb.onChanged != nil {
		return cb.onChanged(err)
	}
	return err
}

func (cb *ddlCallback) OnSchemaStateChanged(schemaVer int64) {
	if cb.onSchemaStateChanged != nil {
		cb.onSchemaStateChanged(schemaVer)
	}
}

func (cb *ddlCallback) OnJobRunBefore(job *model.Job) {
	if cb.onJobRunBefore != nil {
		cb.onJobRunBefore(job)
	}
}

func (cb *ddlCallback) OnJobRunAfter(job *model.Job) {
	if cb.onJobRunAfter != nil {
		cb.onJobRunAfter(job)
	}
}
func (cb *ddlCallback) OnJobUpdated(job *model.Job) {
	if cb.onJobUpdated != nil {
		cb.onJobUpdated(job)
	}
}
func (cb *ddlCallback) OnWatched(ctx context.Context) {
	if cb.onWatched != nil {
		cb.onWatched(ctx)
	}
}
func (cb *ddlCallback) OnGetJobBefore(jobType string) {
	if cb.onGetJobBefore != nil {
		cb.onGetJobBefore(jobType)
	}
}

func (cb *ddlCallback) OnGetJobAfter(jobType string, job *model.Job) {
	if cb.onGetJobAfter != nil {
		cb.onGetJobAfter(jobType, job)
	}
}
