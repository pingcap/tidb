// Copyright 2021 PingCAP, Inc.
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

package admintest

import (
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/util/callback"
	"github.com/pingcap/tidb/pkg/domain"
	mysql "github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testutil"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/logutil/consistency"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestAdminRecoverIndex(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test")
	tk.MustExec("create table admin_test (c1 int, c2 int, c3 int default 1, index (c1), unique key(c2))")
	tk.MustExec("insert admin_test (c1, c2) values (1, 1), (2, 2), (NULL, NULL)")

	r := tk.MustQuery("admin recover index admin_test c1")
	r.Check(testkit.Rows("0 3"))

	r = tk.MustQuery("admin recover index admin_test c2")
	r.Check(testkit.Rows("0 3"))

	tk.MustExec("admin check index admin_test c1")
	tk.MustExec("admin check index admin_test c2")

	tk.MustExec("drop table if exists admin_test")
	tk.MustExec("create table admin_test (c1 int, c2 int, c3 int default 1, primary key(c1), unique key(c2))")
	tk.MustExec("insert admin_test (c1, c2) values (1, 1), (2, 2), (3, 3), (10, 10), (20, 20)")
	// pk is handle, no additional unique index, no way to recover
	err := tk.ExecToErr("admin recover index admin_test c1")
	// err:index is not found
	require.Error(t, err)

	r = tk.MustQuery("admin recover index admin_test c2")
	r.Check(testkit.Rows("0 5"))
	tk.MustExec("admin check index admin_test c2")

	// Make some corrupted index.
	sctx := mock.NewContext()
	sctx.Store = store
	ctx := sctx.GetTableCtx()
	is := domain.InfoSchema()
	dbName := model.NewCIStr("test")
	tblName := model.NewCIStr("admin_test")
	tbl, err := is.TableByName(dbName, tblName)
	require.NoError(t, err)

	tblInfo := tbl.Meta()
	idxInfo := tblInfo.FindIndexByName("c2")
	indexOpr := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)
	txn, err := store.Begin()
	require.NoError(t, err)
	err = indexOpr.Delete(ctx, txn, types.MakeDatums(1), kv.IntHandle(1))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.True(t, consistency.ErrAdminCheckInconsistent.Equal(err))
	err = tk.ExecToErr("admin check index admin_test c2")
	require.Error(t, err)

	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
	r.Check(testkit.Rows("4"))

	r = tk.MustQuery("admin recover index admin_test c2")
	r.Check(testkit.Rows("1 5"))

	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
	r.Check(testkit.Rows("5"))
	tk.MustExec("admin check index admin_test c2")
	tk.MustExec("admin check table admin_test")

	txn, err = store.Begin()
	require.NoError(t, err)
	err = indexOpr.Delete(ctx, txn, types.MakeDatums(10), kv.IntHandle(10))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	err = tk.ExecToErr("admin check index admin_test c2")
	require.Error(t, err)
	r = tk.MustQuery("admin recover index admin_test c2")
	r.Check(testkit.Rows("1 5"))
	tk.MustExec("admin check index admin_test c2")
	tk.MustExec("admin check table admin_test")

	txn, err = store.Begin()
	require.NoError(t, err)
	err = indexOpr.Delete(ctx, txn, types.MakeDatums(1), kv.IntHandle(1))
	require.NoError(t, err)
	err = indexOpr.Delete(ctx, txn, types.MakeDatums(2), kv.IntHandle(2))
	require.NoError(t, err)
	err = indexOpr.Delete(ctx, txn, types.MakeDatums(3), kv.IntHandle(3))
	require.NoError(t, err)
	err = indexOpr.Delete(ctx, txn, types.MakeDatums(10), kv.IntHandle(10))
	require.NoError(t, err)
	err = indexOpr.Delete(ctx, txn, types.MakeDatums(20), kv.IntHandle(20))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	err = tk.ExecToErr("admin check index admin_test c2")
	require.Error(t, err)

	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
	r.Check(testkit.Rows("0"))

	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX()")
	r.Check(testkit.Rows("5"))

	r = tk.MustQuery("admin recover index admin_test c2")
	r.Check(testkit.Rows("5 5"))

	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
	r.Check(testkit.Rows("5"))

	tk.MustExec("admin check index admin_test c2")
	tk.MustExec("admin check table admin_test")

	tk.MustExec("drop table if exists admin_test")
	tk.MustExec("create table admin_test (c1 int, c2 int, c3 int default 1, primary key(c1), unique key i1((c2+1)))")
	tk.MustExec("insert admin_test (c1, c2) values (1, 1), (2, 2), (3, 3), (10, 10), (20, 20)")
	r = tk.MustQuery("admin recover index admin_test i1")
	r.Check(testkit.Rows("0 5"))
	tk.MustExec("admin check table admin_test")
	sctx = mock.NewContext()
	sctx.Store = store
	ctx = sctx.GetTableCtx()
	is = domain.InfoSchema()
	dbName = model.NewCIStr("test")
	tblName = model.NewCIStr("admin_test")
	tbl, err = is.TableByName(dbName, tblName)
	require.NoError(t, err)

	tblInfo = tbl.Meta()
	idxInfo = tblInfo.FindIndexByName("i1")
	indexOpr = tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)
	txn, err = store.Begin()
	require.NoError(t, err)
	err = indexOpr.Delete(ctx, txn, types.MakeDatums(2), kv.IntHandle(1))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(i1)")
	r.Check(testkit.Rows("4"))
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	r = tk.MustQuery("admin recover index admin_test i1")
	r.Check(testkit.Rows("1 5"))
	tk.MustExec("admin check table admin_test")

	tk.MustExec("drop table if exists admin_test")
	tk.MustExec("create table admin_test (c1 int, c2 int, c3 int default 1, primary key(c1), unique key i1(c1, c2));")
	tk.MustExec("insert admin_test (c1, c2) values (1, 1), (2, 2), (3, 3), (10, 10), (20, 20);")
	tk.MustExec("admin recover index admin_test i1;")
}

func TestAdminRecoverMVIndex(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(pk int primary key, a json, index idx((cast(a as signed array))))")
	tk.MustExec("insert into t values (0, '[0,1,2]')")
	tk.MustExec("insert into t values (1, '[1,2,3]')")
	tk.MustExec("insert into t values (2, '[2,3,4]')")
	tk.MustExec("insert into t values (3, '[3,4,5]')")
	tk.MustExec("insert into t values (4, '[4,5,6]')")
	tk.MustExec("admin check table t")

	sctx := mock.NewContext()
	sctx.Store = store
	ctx := sctx.GetTableCtx()
	is := domain.InfoSchema()
	dbName := model.NewCIStr("test")
	tblName := model.NewCIStr("t")
	tbl, err := is.TableByName(dbName, tblName)
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	idxInfo := tblInfo.Indices[0]
	tk.Session().GetSessionVars().IndexLookupSize = 3
	tk.Session().GetSessionVars().MaxChunkSize = 3

	cpIdx := idxInfo.Clone()
	cpIdx.MVIndex = false
	indexOpr := tables.NewIndex(tblInfo.ID, tblInfo, cpIdx)

	txn, err := store.Begin()
	require.NoError(t, err)
	err = indexOpr.Delete(ctx, txn, types.MakeDatums(2), kv.IntHandle(1))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	err = tk.ExecToErr("admin check table t")
	require.Error(t, err)
	r := tk.MustQuery("admin recover index t idx")
	r.Check(testkit.Rows("1 5"))
	tk.MustExec("admin check table t")
}

func TestAdminCleanupMVIndex(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(pk int primary key, a json, index idx((cast(a as signed array))))")
	tk.MustExec("insert into t values (0, '[0,1,2]')")
	tk.MustExec("insert into t values (1, '[1,2,3]')")
	tk.MustExec("insert into t values (2, '[2,3,4]')")
	tk.MustExec("insert into t values (3, '[3,4,5]')")
	tk.MustExec("insert into t values (4, '[4,5,6]')")
	tk.MustExec("admin check table t")

	// Make some corrupted index. Build the index information.
	sctx := mock.NewContext()
	sctx.Store = store
	ctx := sctx.GetTableCtx()
	is := domain.InfoSchema()
	dbName := model.NewCIStr("test")
	tblName := model.NewCIStr("t")
	tbl, err := is.TableByName(dbName, tblName)
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	idxInfo := tblInfo.Indices[0]
	tk.Session().GetSessionVars().IndexLookupSize = 3
	tk.Session().GetSessionVars().MaxChunkSize = 3

	cpIdx := idxInfo.Clone()
	cpIdx.MVIndex = false
	indexOpr := tables.NewIndex(tblInfo.ID, tblInfo, cpIdx)

	txn, err := store.Begin()
	require.NoError(t, err)
	_, err = indexOpr.Create(ctx, txn, types.MakeDatums(9), kv.IntHandle(9), nil)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	err = tk.ExecToErr("admin check table t")
	require.Error(t, err)

	r := tk.MustQuery("admin cleanup index t idx")
	r.Check(testkit.Rows("1"))
	tk.MustExec("admin check table t")
}

func TestClusteredIndexAdminRecoverIndex(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists test_cluster_index_admin_recover;")
	tk.MustExec("create database test_cluster_index_admin_recover;")
	tk.MustExec("use test_cluster_index_admin_recover;")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	dbName := model.NewCIStr("test_cluster_index_admin_recover")
	tblName := model.NewCIStr("t")

	// Test no corruption case.
	tk.MustExec("create table t (a varchar(255), b int, c char(10), primary key(a, c), index idx(b), index idx1(c));")
	tk.MustExec("insert into t values ('1', 2, '3'), ('1', 2, '4'), ('1', 2, '5');")
	tk.MustQuery("admin recover index t `primary`;").Check(testkit.Rows("0 0"))
	tk.MustQuery("admin recover index t `idx`;").Check(testkit.Rows("0 3"))
	tk.MustExec("admin check table t;")

	sctx := mock.NewContext()
	sctx.Store = store
	ctx := sctx.GetTableCtx()
	is := domain.InfoSchema()
	tbl, err := is.TableByName(dbName, tblName)
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	idxInfo := tblInfo.FindIndexByName("idx")
	indexOpr := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)

	// Some index entries are missed.
	// Recover an index don't covered by clustered index.
	txn, err := store.Begin()
	require.NoError(t, err)
	cHandle := testutil.MustNewCommonHandle(t, "1", "3")
	err = indexOpr.Delete(ctx, txn, types.MakeDatums(2), cHandle)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	tk.MustGetErrCode("admin check table t", mysql.ErrDataInconsistent)
	tk.MustGetErrCode("admin check index t idx", mysql.ErrDataInconsistent)

	tk.MustQuery("SELECT COUNT(*) FROM t USE INDEX(idx)").Check(testkit.Rows("2"))
	tk.MustQuery("admin recover index t idx").Check(testkit.Rows("1 3"))
	tk.MustQuery("SELECT COUNT(*) FROM t USE INDEX(idx)").Check(testkit.Rows("3"))
	tk.MustExec("admin check table t;")

	// Recover an index covered by clustered index.
	idx1Info := tblInfo.FindIndexByName("idx1")
	indexOpr1 := tables.NewIndex(tblInfo.ID, tblInfo, idx1Info)
	txn, err = store.Begin()
	require.NoError(t, err)
	err = indexOpr1.Delete(ctx, txn, types.MakeDatums("3"), cHandle)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	tk.MustGetErrCode("admin check table t", mysql.ErrDataInconsistent)
	tk.MustGetErrCode("admin check index t idx1", mysql.ErrDataInconsistent)

	tk.MustQuery("SELECT COUNT(*) FROM t USE INDEX(idx1)").Check(testkit.Rows("2"))
	tk.MustQuery("admin recover index t idx1").Check(testkit.Rows("1 3"))
	tk.MustQuery("SELECT COUNT(*) FROM t USE INDEX(idx1)").Check(testkit.Rows("3"))
	tk.MustExec("admin check table t;")
}

func TestAdminRecoverPartitionTableIndex(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	getTable := func() table.Table {
		ctx := mock.NewContext()
		ctx.Store = store
		is := domain.InfoSchema()
		dbName := model.NewCIStr("test")
		tblName := model.NewCIStr("admin_test")
		tbl, err := is.TableByName(dbName, tblName)
		require.NoError(t, err)
		return tbl
	}

	checkFunc := func(tbl table.Table, pid int64, idxValue int) {
		idxInfo := tbl.Meta().FindIndexByName("c2")
		indexOpr := tables.NewIndex(pid, tbl.Meta(), idxInfo)
		ctx := mock.NewContext()
		txn, err := store.Begin()
		require.NoError(t, err)
		err = indexOpr.Delete(ctx.GetTableCtx(), txn, types.MakeDatums(idxValue), kv.IntHandle(idxValue))
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)
		err = tk.ExecToErr("admin check table admin_test")
		require.Error(t, err)
		require.True(t, consistency.ErrAdminCheckInconsistent.Equal(err))

		r := tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
		r.Check(testkit.Rows("2"))

		r = tk.MustQuery("admin recover index admin_test c2")
		r.Check(testkit.Rows("1 3"))

		r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
		r.Check(testkit.Rows("3"))
		tk.MustExec("admin check table admin_test")
	}

	// Test for hash partition table.
	tk.MustExec("drop table if exists admin_test")
	tk.MustExec("create table admin_test (c1 int, c2 int, c3 int default 1, primary key (c1), index (c2)) partition by hash(c1) partitions 3;")
	tk.MustExec("insert admin_test (c1, c2) values (0, 0), (1, 1), (2, 2)")
	r := tk.MustQuery("admin recover index admin_test c2")
	r.Check(testkit.Rows("0 3"))
	tbl := getTable()
	pi := tbl.Meta().GetPartitionInfo()
	require.NotNil(t, pi)
	for i, p := range pi.Definitions {
		checkFunc(tbl, p.ID, i)
	}

	// Test for range partition table.
	tk.MustExec("drop table if exists admin_test")
	tk.MustExec(`create table admin_test (c1 int, c2 int, c3 int default 1, primary key (c1), index (c2)) PARTITION BY RANGE ( c1 ) (
		PARTITION p0 VALUES LESS THAN (5),
		PARTITION p1 VALUES LESS THAN (10),
		PARTITION p2 VALUES LESS THAN (MAXVALUE))`)
	tk.MustExec("insert admin_test (c1, c2) values (0, 0), (6, 6), (12, 12)")
	r = tk.MustQuery("admin recover index admin_test c2")
	r.Check(testkit.Rows("0 3"))
	tbl = getTable()
	pi = tbl.Meta().GetPartitionInfo()
	require.NotNil(t, pi)
	for i, p := range pi.Definitions {
		checkFunc(tbl, p.ID, i*6)
	}
}

func TestAdminRecoverIndex1(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	sctx := mock.NewContext()
	sctx.Store = store
	ctx := sctx.GetTableCtx()
	dbName := model.NewCIStr("test")
	tblName := model.NewCIStr("admin_test")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	tk.MustExec("create table admin_test (c1 varchar(255), c2 int, c3 int default 1, primary key(c1), unique key(c2))")
	tk.MustExec("insert admin_test (c1, c2) values ('1', 1), ('2', 2), ('3', 3), ('10', 10), ('20', 20)")

	r := tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(`primary`)")
	r.Check(testkit.Rows("5"))

	is := domain.InfoSchema()
	tbl, err := is.TableByName(dbName, tblName)
	require.NoError(t, err)

	tblInfo := tbl.Meta()
	idxInfo := tblInfo.FindIndexByName("primary")
	require.NotNil(t, idxInfo)
	indexOpr := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)

	txn, err := store.Begin()
	require.NoError(t, err)
	err = indexOpr.Delete(ctx, txn, types.MakeDatums("1"), kv.IntHandle(1))
	require.NoError(t, err)
	err = indexOpr.Delete(ctx, txn, types.MakeDatums("2"), kv.IntHandle(2))
	require.NoError(t, err)
	err = indexOpr.Delete(ctx, txn, types.MakeDatums("3"), kv.IntHandle(3))
	require.NoError(t, err)
	err = indexOpr.Delete(ctx, txn, types.MakeDatums("10"), kv.IntHandle(4))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(`primary`)")
	r.Check(testkit.Rows("1"))

	r = tk.MustQuery("admin recover index admin_test `primary`")
	r.Check(testkit.Rows("4 5"))

	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(`primary`)")
	r.Check(testkit.Rows("5"))

	tk.MustExec("admin check table admin_test")
	tk.MustExec("admin check index admin_test c2")
	tk.MustExec("admin check index admin_test `primary`")
}

func TestAdminCleanupIndex(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test")
	tk.MustExec("create table admin_test (c1 int, c2 int, c3 int default 1, primary key (c1), unique key(c2), key (c3))")
	tk.MustExec("insert admin_test (c1, c2) values (1, 2), (3, 4), (-5, NULL)")
	tk.MustExec("insert admin_test (c1, c3) values (7, 100), (9, 100), (11, NULL)")

	// pk is handle, no need to cleanup
	err := tk.ExecToErr("admin cleanup index admin_test `primary`")
	require.Error(t, err)
	r := tk.MustQuery("admin cleanup index admin_test c2")
	r.Check(testkit.Rows("0"))
	r = tk.MustQuery("admin cleanup index admin_test c3")
	r.Check(testkit.Rows("0"))

	// Make some dangling index.
	sctx := mock.NewContext()
	sctx.Store = store
	ctx := sctx.GetTableCtx()
	is := domain.InfoSchema()
	dbName := model.NewCIStr("test")
	tblName := model.NewCIStr("admin_test")
	tbl, err := is.TableByName(dbName, tblName)
	require.NoError(t, err)

	tblInfo := tbl.Meta()
	idxInfo2 := tblInfo.FindIndexByName("c2")
	indexOpr2 := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo2)
	idxInfo3 := tblInfo.FindIndexByName("c3")
	indexOpr3 := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo3)

	txn, err := store.Begin()
	require.NoError(t, err)
	_, err = indexOpr2.Create(ctx, txn, types.MakeDatums(1), kv.IntHandle(-100), nil)
	require.NoError(t, err)
	_, err = indexOpr2.Create(ctx, txn, types.MakeDatums(6), kv.IntHandle(100), nil)
	require.NoError(t, err)
	_, err = indexOpr2.Create(ctx, txn, types.MakeDatums(8), kv.IntHandle(100), nil)
	require.NoError(t, err)
	_, err = indexOpr2.Create(ctx, txn, types.MakeDatums(nil), kv.IntHandle(101), nil)
	require.NoError(t, err)
	_, err = indexOpr2.Create(ctx, txn, types.MakeDatums(nil), kv.IntHandle(102), nil)
	require.NoError(t, err)
	_, err = indexOpr3.Create(ctx, txn, types.MakeDatums(6), kv.IntHandle(200), nil)
	require.NoError(t, err)
	_, err = indexOpr3.Create(ctx, txn, types.MakeDatums(6), kv.IntHandle(-200), nil)
	require.NoError(t, err)
	_, err = indexOpr3.Create(ctx, txn, types.MakeDatums(8), kv.IntHandle(-200), nil)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	err = tk.ExecToErr("admin check index admin_test c2")
	require.Error(t, err)
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
	r.Check(testkit.Rows("11"))
	r = tk.MustQuery("admin cleanup index admin_test c2")
	r.Check(testkit.Rows("5"))
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
	r.Check(testkit.Rows("6"))
	tk.MustExec("admin check index admin_test c2")

	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	err = tk.ExecToErr("admin check index admin_test c3")
	require.Error(t, err)
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c3)")
	r.Check(testkit.Rows("9"))
	r = tk.MustQuery("admin cleanup index admin_test c3")
	r.Check(testkit.Rows("3"))
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c3)")
	r.Check(testkit.Rows("6"))
	tk.MustExec("admin check index admin_test c3")

	tk.MustExec("admin check table admin_test")
}

func TestAdminCleanupIndexForPartitionTable(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	getTable := func() table.Table {
		ctx := mock.NewContext()
		ctx.Store = store
		is := domain.InfoSchema()
		dbName := model.NewCIStr("test")
		tblName := model.NewCIStr("admin_test")
		tbl, err := is.TableByName(dbName, tblName)
		require.NoError(t, err)
		return tbl
	}

	checkFunc := func(tbl table.Table, pid int64, idxValue, handle int) {
		idxInfo2 := tbl.Meta().FindIndexByName("c2")
		indexOpr2 := tables.NewIndex(pid, tbl.Meta(), idxInfo2)
		idxInfo3 := tbl.Meta().FindIndexByName("c3")
		indexOpr3 := tables.NewIndex(pid, tbl.Meta(), idxInfo3)

		txn, err := store.Begin()
		sctx := mock.NewContext()
		require.NoError(t, err)
		ctx := sctx.GetTableCtx()
		_, err = indexOpr2.Create(ctx, txn, types.MakeDatums(idxValue), kv.IntHandle(handle), nil)
		require.NoError(t, err)
		_, err = indexOpr3.Create(ctx, txn, types.MakeDatums(idxValue), kv.IntHandle(handle), nil)
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)

		err = tk.ExecToErr("admin check table admin_test")
		require.Error(t, err)

		r := tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
		r.Check(testkit.Rows("4"))
		r = tk.MustQuery("admin cleanup index admin_test c2")
		r.Check(testkit.Rows("1"))
		r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
		r.Check(testkit.Rows("3"))

		r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c3)")
		r.Check(testkit.Rows("4"))
		r = tk.MustQuery("admin cleanup index admin_test c3")
		r.Check(testkit.Rows("1"))
		r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c3)")
		r.Check(testkit.Rows("3"))
		tk.MustExec("admin check table admin_test")
	}

	// Test for hash partition table.
	tk.MustExec("drop table if exists admin_test")
	tk.MustExec("create table admin_test (c1 int, c2 int, c3 int default 1, primary key (c2), unique index c2(c2), index c3(c3)) partition by hash(c2) partitions 3;")
	tk.MustExec("insert admin_test (c2, c3) values (0, 0), (1, 1), (2, 2)")
	r := tk.MustQuery("admin cleanup index admin_test c2")
	r.Check(testkit.Rows("0"))
	tbl := getTable()
	pi := tbl.Meta().GetPartitionInfo()
	require.NotNil(t, pi)
	for i, p := range pi.Definitions {
		checkFunc(tbl, p.ID, i+6, i+6)
	}

	// Test for range partition table.
	tk.MustExec("drop table if exists admin_test")
	tk.MustExec(`create table admin_test (c1 int, c2 int, c3 int default 1, primary key (c2), unique index c2 (c2), index c3(c3)) PARTITION BY RANGE ( c2 ) (
		PARTITION p0 VALUES LESS THAN (5),
		PARTITION p1 VALUES LESS THAN (10),
		PARTITION p2 VALUES LESS THAN (MAXVALUE))`)
	tk.MustExec("insert admin_test (c1, c2) values (0, 0), (6, 6), (12, 12)")
	r = tk.MustQuery("admin cleanup index admin_test c2")
	r.Check(testkit.Rows("0"))
	tbl = getTable()
	pi = tbl.Meta().GetPartitionInfo()
	require.NotNil(t, pi)
	for i, p := range pi.Definitions {
		checkFunc(tbl, p.ID, i*6+1, i*6+1)
	}
}

func TestAdminCleanupIndexPKNotHandle(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	tk.MustExec("create table admin_test (c1 int, c2 int, c3 int, primary key (c1, c2))")
	tk.MustExec("insert admin_test (c1, c2) values (1, 2), (3, 4), (-5, 5)")

	r := tk.MustQuery("admin cleanup index admin_test `primary`")
	r.Check(testkit.Rows("0"))

	// Make some dangling index.
	sctx := mock.NewContext()
	sctx.Store = store
	ctx := sctx.GetTableCtx()
	is := domain.InfoSchema()
	dbName := model.NewCIStr("test")
	tblName := model.NewCIStr("admin_test")
	tbl, err := is.TableByName(dbName, tblName)
	require.NoError(t, err)

	tblInfo := tbl.Meta()
	idxInfo := tblInfo.FindIndexByName("primary")
	indexOpr := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)

	txn, err := store.Begin()
	require.NoError(t, err)
	_, err = indexOpr.Create(ctx, txn, types.MakeDatums(7, 10), kv.IntHandle(-100), nil)
	require.NoError(t, err)
	_, err = indexOpr.Create(ctx, txn, types.MakeDatums(4, 6), kv.IntHandle(100), nil)
	require.NoError(t, err)
	_, err = indexOpr.Create(ctx, txn, types.MakeDatums(-7, 4), kv.IntHandle(101), nil)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	err = tk.ExecToErr("admin check index admin_test `primary`")
	require.Error(t, err)
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(`primary`)")
	r.Check(testkit.Rows("6"))
	r = tk.MustQuery("admin cleanup index admin_test `primary`")
	r.Check(testkit.Rows("3"))
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(`primary`)")
	r.Check(testkit.Rows("3"))
	tk.MustExec("admin check index admin_test `primary`")
	tk.MustExec("admin check table admin_test")
}

func TestAdminCleanupIndexMore(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test")
	tk.MustExec("create table admin_test (c1 int, c2 int, unique key (c1, c2), key (c2))")
	tk.MustExec("insert admin_test values (1, 2), (3, 4), (5, 6)")

	tk.MustExec("admin cleanup index admin_test c1")
	tk.MustExec("admin cleanup index admin_test c2")

	// Make some dangling index.
	sctx := mock.NewContext()
	sctx.Store = store
	ctx := sctx.GetTableCtx()
	is := domain.InfoSchema()
	dbName := model.NewCIStr("test")
	tblName := model.NewCIStr("admin_test")
	tbl, err := is.TableByName(dbName, tblName)
	require.NoError(t, err)

	tblInfo := tbl.Meta()
	idxInfo1 := tblInfo.FindIndexByName("c1")
	indexOpr1 := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo1)
	idxInfo2 := tblInfo.FindIndexByName("c2")
	indexOpr2 := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo2)

	txn, err := store.Begin()
	require.NoError(t, err)
	for i := 0; i < 2000; i++ {
		c1 := int64(2*i + 7)
		c2 := int64(2*i + 8)
		_, err = indexOpr1.Create(ctx, txn, types.MakeDatums(c1, c2), kv.IntHandle(c1), nil)
		require.NoErrorf(t, err, errors.ErrorStack(err))
		_, err = indexOpr2.Create(ctx, txn, types.MakeDatums(c2), kv.IntHandle(c1), nil)
		require.NoError(t, err)
	}
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	err = tk.ExecToErr("admin check index admin_test c1")
	require.Error(t, err)
	err = tk.ExecToErr("admin check index admin_test c2")
	require.Error(t, err)
	r := tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX()")
	r.Check(testkit.Rows("3"))
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c1)")
	r.Check(testkit.Rows("2003"))
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
	r.Check(testkit.Rows("2003"))
	r = tk.MustQuery("admin cleanup index admin_test c1")
	r.Check(testkit.Rows("2000"))
	r = tk.MustQuery("admin cleanup index admin_test c2")
	r.Check(testkit.Rows("2000"))
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c1)")
	r.Check(testkit.Rows("3"))
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
	r.Check(testkit.Rows("3"))
	tk.MustExec("admin check index admin_test c1")
	tk.MustExec("admin check index admin_test c2")
	tk.MustExec("admin check table admin_test")
}

func TestClusteredAdminCleanupIndex(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("create table admin_test (c1 varchar(255), c2 int, c3 char(10) default 'c3', primary key (c1, c3), unique key(c2), key (c3))")
	tk.MustExec("insert admin_test (c1, c2) values ('c1_1', 2), ('c1_2', 4), ('c1_3', NULL)")
	tk.MustExec("insert admin_test (c1, c3) values ('c1_4', 'c3_4'), ('c1_5', 'c3_5'), ('c1_6', default)")

	// Normally, there is no dangling index.
	tk.MustQuery("admin cleanup index admin_test `primary`").Check(testkit.Rows("0"))
	tk.MustQuery("admin cleanup index admin_test `c2`").Check(testkit.Rows("0"))
	tk.MustQuery("admin cleanup index admin_test `c3`").Check(testkit.Rows("0"))

	// Make some dangling index.
	sctx := mock.NewContext()
	sctx.Store = store
	ctx := sctx.GetTableCtx()
	tbl, err := domain.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("admin_test"))
	require.NoError(t, err)
	// cleanup clustered primary key takes no effect.

	tblInfo := tbl.Meta()
	idxInfo2 := tblInfo.FindIndexByName("c2")
	indexOpr2 := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo2)
	idxInfo3 := tblInfo.FindIndexByName("c3")
	indexOpr3 := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo3)

	c2DanglingIdx := []struct {
		handle kv.Handle
		idxVal []types.Datum
	}{
		{testutil.MustNewCommonHandle(t, "c1_10", "c3_10"), types.MakeDatums(10)},
		{testutil.MustNewCommonHandle(t, "c1_10", "c3_11"), types.MakeDatums(11)},
		{testutil.MustNewCommonHandle(t, "c1_12", "c3_12"), types.MakeDatums(12)},
	}
	c3DanglingIdx := []struct {
		handle kv.Handle
		idxVal []types.Datum
	}{
		{testutil.MustNewCommonHandle(t, "c1_13", "c3_13"), types.MakeDatums("c3_13")},
		{testutil.MustNewCommonHandle(t, "c1_14", "c3_14"), types.MakeDatums("c3_14")},
		{testutil.MustNewCommonHandle(t, "c1_15", "c3_15"), types.MakeDatums("c3_15")},
	}
	txn, err := store.Begin()
	require.NoError(t, err)
	for _, di := range c2DanglingIdx {
		_, err := indexOpr2.Create(ctx, txn, di.idxVal, di.handle, nil)
		require.NoError(t, err)
	}
	for _, di := range c3DanglingIdx {
		_, err := indexOpr3.Create(ctx, txn, di.idxVal, di.handle, nil)
		require.NoError(t, err)
	}
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	err = tk.ExecToErr("admin check index admin_test c2")
	require.Error(t, err)
	tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)").Check(testkit.Rows("9"))
	tk.MustQuery("admin cleanup index admin_test c2").Check(testkit.Rows("3"))
	tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)").Check(testkit.Rows("6"))
	tk.MustExec("admin check index admin_test c2")

	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	err = tk.ExecToErr("admin check index admin_test c3")
	require.Error(t, err)
	tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c3)").Check(testkit.Rows("9"))
	tk.MustQuery("admin cleanup index admin_test c3").Check(testkit.Rows("3"))
	tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c3)").Check(testkit.Rows("6"))
	tk.MustExec("admin check index admin_test c3")
	tk.MustExec("admin check table admin_test")
}

func TestAdminCheckTableWithMultiValuedIndex(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(pk int primary key, a json, index idx((cast(a as signed array))))")
	tk.MustExec("insert into t values (0, '[0,1,2]')")
	tk.MustExec("insert into t values (1, '[1,2,3]')")
	tk.MustExec("insert into t values (2, '[2,3,4]')")
	tk.MustExec("insert into t values (3, '[3,4,5]')")
	tk.MustExec("insert into t values (4, '[4,5,6]')")
	tk.MustExec("admin check table t")

	// Make some corrupted index. Build the index information.
	sctx := mock.NewContext()
	sctx.Store = store
	ctx := sctx.GetTableCtx()
	is := domain.InfoSchema()
	dbName := model.NewCIStr("test")
	tblName := model.NewCIStr("t")
	tbl, err := is.TableByName(dbName, tblName)
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	idxInfo := tblInfo.Indices[0]
	tk.Session().GetSessionVars().IndexLookupSize = 3
	tk.Session().GetSessionVars().MaxChunkSize = 3

	cpIdx := idxInfo.Clone()
	cpIdx.MVIndex = false
	indexOpr := tables.NewIndex(tblInfo.ID, tblInfo, cpIdx)
	txn, err := store.Begin()
	require.NoError(t, err)
	err = indexOpr.Delete(ctx, txn, types.MakeDatums(0), kv.IntHandle(0))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	err = tk.ExecToErr("admin check table t")
	require.Error(t, err)
	require.True(t, consistency.ErrAdminCheckInconsistent.Equal(err))

	txn, err = store.Begin()
	require.NoError(t, err)
	_, err = indexOpr.Create(ctx, txn, types.MakeDatums(0), kv.IntHandle(0), nil)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	tk.MustExec("admin check table t")

	txn, err = store.Begin()
	require.NoError(t, err)
	_, err = indexOpr.Create(ctx, txn, types.MakeDatums(9), kv.IntHandle(9), nil)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	err = tk.ExecToErr("admin check table t")
	require.Error(t, err)
}

func TestAdminCheckPartitionTableFailed(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test_p")
	tk.MustExec("create table admin_test_p (c1 int key,c2 int,c3 int,index idx(c2)) partition by hash(c1) partitions 4")
	tk.MustExec("insert admin_test_p (c1, c2, c3) values (0,0,0), (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)")
	tk.MustExec("admin check table admin_test_p")

	// Make some corrupted index. Build the index information.
	sctx := mock.NewContext()
	sctx.Store = store
	ctx := sctx.GetTableCtx()
	is := domain.InfoSchema()
	dbName := model.NewCIStr("test")
	tblName := model.NewCIStr("admin_test_p")
	tbl, err := is.TableByName(dbName, tblName)
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	idxInfo := tblInfo.Indices[0]
	tk.Session().GetSessionVars().IndexLookupSize = 3
	tk.Session().GetSessionVars().MaxChunkSize = 3

	// Reduce one row of index on partitions.
	// Table count > index count.
	for i := 0; i <= 5; i++ {
		partitionIdx := i % len(tblInfo.GetPartitionInfo().Definitions)
		indexOpr := tables.NewIndex(tblInfo.GetPartitionInfo().Definitions[partitionIdx].ID, tblInfo, idxInfo)
		txn, err := store.Begin()
		require.NoError(t, err)
		err = indexOpr.Delete(ctx, txn, types.MakeDatums(i), kv.IntHandle(i))
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)
		err = tk.ExecToErr("admin check table admin_test_p")
		require.Error(t, err)
		require.EqualError(t, err, fmt.Sprintf("[admin:8223]data inconsistency in table: admin_test_p, index: idx, handle: %d, index-values:\"\" != record-values:\"handle: %d, values: [KindInt64 %d]\"", i, i, i))
		require.True(t, consistency.ErrAdminCheckInconsistent.Equal(err))
		// TODO: fix admin recover for partition table.
		// r := tk.MustQuery("admin recover index admin_test_p idx")
		// r.Check(testkit.Rows("0 0"))
		// tk.MustExec("admin check table admin_test_p")
		// Manual recover index.
		txn, err = store.Begin()
		require.NoError(t, err)
		_, err = indexOpr.Create(ctx, txn, types.MakeDatums(i), kv.IntHandle(i), nil)
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)
		tk.MustExec("admin check table admin_test_p")
	}

	// Add one row of index on partitions.
	// Table count < index count.
	for i := 0; i <= 5; i++ {
		partitionIdx := i % len(tblInfo.GetPartitionInfo().Definitions)
		indexOpr := tables.NewIndex(tblInfo.GetPartitionInfo().Definitions[partitionIdx].ID, tblInfo, idxInfo)
		txn, err := store.Begin()
		require.NoError(t, err)
		_, err = indexOpr.Create(ctx, txn, types.MakeDatums(i+8), kv.IntHandle(i+8), nil)
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)
		err = tk.ExecToErr("admin check table admin_test_p")
		require.Error(t, err)
		require.EqualError(t, err, fmt.Sprintf("[admin:8223]data inconsistency in table: admin_test_p, index: idx, handle: %d, index-values:\"handle: %d, values: [KindInt64 %d]\" != record-values:\"\"", i+8, i+8, i+8))
		// TODO: fix admin recover for partition table.
		txn, err = store.Begin()
		require.NoError(t, err)
		err = indexOpr.Delete(ctx, txn, types.MakeDatums(i+8), kv.IntHandle(i+8))
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)
		tk.MustExec("admin check table admin_test_p")
	}

	// Table count = index count, but the index value was wrong.
	for i := 0; i <= 5; i++ {
		partitionIdx := i % len(tblInfo.GetPartitionInfo().Definitions)
		indexOpr := tables.NewIndex(tblInfo.GetPartitionInfo().Definitions[partitionIdx].ID, tblInfo, idxInfo)
		txn, err := store.Begin()
		require.NoError(t, err)
		_, err = indexOpr.Create(ctx, txn, types.MakeDatums(i+8), kv.IntHandle(i), nil)
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)
		err = tk.ExecToErr("admin check table admin_test_p")
		require.Error(t, err)
		require.EqualError(t, err, fmt.Sprintf("[admin:8223]data inconsistency in table: admin_test_p, index: idx, handle: %d, index-values:\"handle: %d, values: [KindInt64 %d]\" != record-values:\"handle: %d, values: [KindInt64 %d]\"", i, i, i+8, i, i))
		// TODO: fix admin recover for partition table.
		txn, err = store.Begin()
		require.NoError(t, err)
		err = indexOpr.Delete(ctx, txn, types.MakeDatums(i+8), kv.IntHandle(i))
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)
		tk.MustExec("admin check table admin_test_p")
	}
}

const dbName, tblName = "test", "admin_test"

type inconsistencyTestKit struct {
	*testkit.AsyncTestKit
	uniqueIndex table.Index
	plainIndex  table.Index
	ctx         context.Context
	sctx        sessionctx.Context
	t           *testing.T
}

type kitOpt struct {
	pkColType  string
	idxColType string
	ukColType  string
	clustered  string
}

func newDefaultOpt() *kitOpt {
	return &kitOpt{
		pkColType:  "int",
		idxColType: "int",
		ukColType:  "varchar(255)",
	}
}

func newInconsistencyKit(t *testing.T, tk *testkit.AsyncTestKit, opt *kitOpt) *inconsistencyTestKit {
	ctx := tk.OpenSession(context.Background(), dbName)
	se := testkit.TryRetrieveSession(ctx)
	i := &inconsistencyTestKit{
		AsyncTestKit: tk,
		ctx:          ctx,
		sctx:         se,
		t:            t,
	}
	tk.MustExec(i.ctx, "drop table if exists "+tblName)
	tk.MustExec(i.ctx,
		fmt.Sprintf("create table %s (c1 %s, c2 %s, c3 %s, primary key(c1) %s, index uk1(c2), index k2(c3))",
			tblName, opt.pkColType, opt.idxColType, opt.ukColType, opt.clustered),
	)
	i.rebuild()
	return i
}

func (tk *inconsistencyTestKit) rebuild() {
	tk.MustExec(tk.ctx, "truncate table "+tblName)
	is := domain.GetDomain(testkit.TryRetrieveSession(tk.ctx)).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr(dbName), model.NewCIStr(tblName))
	require.NoError(tk.t, err)
	tk.uniqueIndex = tables.NewIndex(tbl.Meta().ID, tbl.Meta(), tbl.Meta().Indices[0])
	tk.plainIndex = tables.NewIndex(tbl.Meta().ID, tbl.Meta(), tbl.Meta().Indices[1])
}

func TestCheckFailReport(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := newInconsistencyKit(t, testkit.NewAsyncTestKit(t, store), newDefaultOpt())

	rmode := tk.sctx.GetSessionVars().EnableRedactLog

	// row more than unique index
	func() {
		defer tk.rebuild()

		tk.MustExec(tk.ctx, fmt.Sprintf("insert into %s values(1, 1, '10')", tblName))
		txn, err := store.Begin()
		require.NoError(t, err)
		require.NoError(t, tk.uniqueIndex.Delete(tk.sctx.GetTableCtx(), txn, types.MakeDatums(1), kv.IntHandle(1)))
		require.NoError(t, txn.Commit(tk.ctx))

		ctx, hook := testutil.WithLogHook(tk.ctx, t, "inconsistency")
		tk.MustGetErrMsg(ctx, "admin check table admin_test", "[admin:8223]data inconsistency in table: admin_test, index: uk1, handle: 1, index-values:\"\" != record-values:\"handle: 1, values: [KindInt64 1]\"")
		hook.CheckLogCount(t, 1)
		hook.Logs[0].CheckMsg(t, "admin check found data inconsistency")
		hook.Logs[0].CheckField(t,
			zap.String("table_name", "admin_test"),
			zap.String("index_name", "uk1"),
			zap.Stringer("row_id", redact.Stringer(rmode, kv.IntHandle(1))),
		)
		hook.Logs[0].CheckFieldNotEmpty(t, "row_mvcc")
	}()

	// row more than plain index
	func() {
		defer tk.rebuild()

		tk.MustExec(tk.ctx, fmt.Sprintf("insert into %s values(1, 1, '10')", tblName))
		txn, err := store.Begin()
		require.NoError(t, err)
		require.NoError(t, tk.plainIndex.Delete(tk.sctx.GetTableCtx(), txn, []types.Datum{types.NewStringDatum("10")}, kv.IntHandle(1)))
		require.NoError(t, txn.Commit(tk.ctx))

		ctx, hook := testutil.WithLogHook(tk.ctx, t, "inconsistency")
		tk.MustGetErrMsg(ctx, "admin check table admin_test", "[admin:8223]data inconsistency in table: admin_test, index: k2, handle: 1, index-values:\"\" != record-values:\"handle: 1, values: [KindString 10]\"")
		hook.CheckLogCount(t, 1)
		hook.Logs[0].CheckMsg(t, "admin check found data inconsistency")
		hook.Logs[0].CheckField(t,
			zap.String("table_name", "admin_test"),
			zap.String("index_name", "k2"),
			zap.Stringer("row_id", redact.Stringer(rmode, kv.IntHandle(1))),
		)
		hook.Logs[0].CheckFieldNotEmpty(t, "row_mvcc")
	}()

	// row is missed for plain key
	func() {
		defer tk.rebuild()

		txn, err := store.Begin()
		require.NoError(t, err)
		_, err = tk.plainIndex.Create(mock.NewContext().GetTableCtx(), txn, []types.Datum{types.NewStringDatum("100")}, kv.IntHandle(1), nil)
		require.NoError(t, err)
		require.NoError(t, txn.Commit(tk.ctx))

		ctx, hook := testutil.WithLogHook(tk.ctx, t, "inconsistency")
		tk.MustGetErrMsg(ctx, "admin check table admin_test",
			"[admin:8223]data inconsistency in table: admin_test, index: k2, handle: 1, index-values:\"handle: 1, values: [KindString 100]\" != record-values:\"\"")
		hook.CheckLogCount(t, 1)
		logEntry := hook.Logs[0]
		logEntry.CheckMsg(t, "admin check found data inconsistency")
		logEntry.CheckField(t,
			zap.String("table_name", "admin_test"),
			zap.String("index_name", "k2"),
			zap.Stringer("row_id", redact.Stringer(rmode, kv.IntHandle(1))),
		)
		logEntry.CheckFieldNotEmpty(t, "row_mvcc")
		logEntry.CheckFieldNotEmpty(t, "index_mvcc")

		// test inconsistency check in index lookup
		ctx, hook = testutil.WithLogHook(tk.ctx, t, "")
		rs, err := tk.Exec(ctx, "select * from admin_test use index(k2) where c3 = '100'")
		require.NoError(t, err)
		_, err = session.GetRows4Test(ctx, testkit.TryRetrieveSession(ctx), rs)
		require.Error(t, err)
		require.Equal(t, "[executor:8133]data inconsistency in table: admin_test, index: k2, index-count:1 != record-count:0", err.Error())
		hook.CheckLogCount(t, 1)
		logEntry = hook.Logs[0]
		logEntry.CheckMsg(t, "indexLookup found data inconsistency")
		logEntry.CheckField(t,
			zap.String("table_name", "admin_test"),
			zap.String("index_name", "k2"),
			zap.Int64("table_cnt", 0),
			zap.Int64("index_cnt", 1),
			zap.String("missing_handles", `[1]`),
		)
		logEntry.CheckFieldNotEmpty(t, "row_mvcc_0")
	}()

	// row is missed for unique key
	func() {
		defer tk.rebuild()

		txn, err := store.Begin()
		require.NoError(t, err)
		_, err = tk.uniqueIndex.Create(mock.NewContext().GetTableCtx(), txn, []types.Datum{types.NewIntDatum(10)}, kv.IntHandle(1), nil)
		require.NoError(t, err)
		require.NoError(t, txn.Commit(tk.ctx))

		ctx, hook := testutil.WithLogHook(tk.ctx, t, "inconsistency")
		tk.MustGetErrMsg(ctx, "admin check table admin_test",
			"[admin:8223]data inconsistency in table: admin_test, index: uk1, handle: 1, index-values:\"handle: 1, values: [KindInt64 10]\" != record-values:\"\"")
		hook.CheckLogCount(t, 1)
		logEntry := hook.Logs[0]
		logEntry.CheckMsg(t, "admin check found data inconsistency")
		logEntry.CheckField(t,
			zap.String("table_name", "admin_test"),
			zap.String("index_name", "uk1"),
			zap.Stringer("row_id", redact.Stringer(rmode, kv.IntHandle(1))),
		)
		logEntry.CheckFieldNotEmpty(t, "row_mvcc")
		logEntry.CheckFieldNotEmpty(t, "index_mvcc")

		// test inconsistency check in point-get
		ctx, hook = testutil.WithLogHook(tk.ctx, t, "")
		rs, err := tk.Exec(ctx, "select * from admin_test use index(uk1) where c2 = 10")
		require.NoError(t, err)
		_, err = session.GetRows4Test(ctx, testkit.TryRetrieveSession(ctx), rs)
		require.Error(t, err)
		hook.CheckLogCount(t, 1)
		logEntry = hook.Logs[0]
		logEntry.CheckMsg(t, "indexLookup found data inconsistency")
		logEntry.CheckField(t,
			zap.String("table_name", "admin_test"),
			zap.String("index_name", "uk1"),
			zap.Int64("table_cnt", 0),
			zap.Int64("index_cnt", 1),
			zap.String("missing_handles", `[1]`),
		)
		logEntry.CheckFieldNotEmpty(t, "row_mvcc_0")
	}()

	// handle match but value is different for uk
	func() {
		defer tk.rebuild()

		tk.MustExec(tk.ctx, fmt.Sprintf("insert into %s values(1, 10, '100')", tblName))
		txn, err := store.Begin()
		require.NoError(t, err)
		require.NoError(t, tk.uniqueIndex.Delete(tk.sctx.GetTableCtx(), txn, []types.Datum{types.NewIntDatum(10)}, kv.IntHandle(1)))
		_, err = tk.uniqueIndex.Create(mock.NewContext().GetTableCtx(), txn, []types.Datum{types.NewIntDatum(20)}, kv.IntHandle(1), nil)
		require.NoError(t, err)
		require.NoError(t, txn.Commit(tk.ctx))
		ctx, hook := testutil.WithLogHook(tk.ctx, t, "inconsistency")
		tk.MustGetErrMsg(ctx, "admin check table admin_test", "[admin:8223]data inconsistency in table: admin_test, index: uk1, handle: 1, index-values:\"handle: 1, values: [KindInt64 20]\" != record-values:\"handle: 1, values: [KindInt64 10]\"")
		hook.CheckLogCount(t, 1)
		logEntry := hook.Logs[0]
		logEntry.CheckMsg(t, "admin check found data inconsistency")
		logEntry.CheckField(t,
			zap.String("table_name", "admin_test"),
			zap.String("index_name", "uk1"),
			zap.Stringer("row_id", redact.Stringer(rmode, kv.IntHandle(1))),
		)
		logEntry.CheckFieldNotEmpty(t, "row_mvcc")
		logEntry.CheckFieldNotEmpty(t, "index_mvcc")
	}()

	// handle match but value is different for plain key
	func() {
		defer tk.rebuild()

		tk.MustExec(tk.ctx, fmt.Sprintf("insert into %s values(1, 10, '100')", tblName))
		txn, err := store.Begin()
		require.NoError(t, err)
		require.NoError(t, tk.plainIndex.Delete(tk.sctx.GetTableCtx(), txn, []types.Datum{types.NewStringDatum("100")}, kv.IntHandle(1)))
		_, err = tk.plainIndex.Create(mock.NewContext().GetTableCtx(), txn, []types.Datum{types.NewStringDatum("200")}, kv.IntHandle(1), nil)
		require.NoError(t, err)
		require.NoError(t, txn.Commit(tk.ctx))
		ctx, hook := testutil.WithLogHook(tk.ctx, t, "inconsistency")
		tk.MustGetErrMsg(ctx, "admin check table admin_test",
			"[admin:8223]data inconsistency in table: admin_test, index: k2, handle: 1, index-values:\"handle: 1, values: [KindString 200]\" != record-values:\"handle: 1, values: [KindString 100]\"")
		hook.CheckLogCount(t, 1)
		logEntry := hook.Logs[0]
		logEntry.CheckMsg(t, "admin check found data inconsistency")
		logEntry.CheckField(t,
			zap.String("table_name", "admin_test"),
			zap.String("index_name", "k2"),
			zap.Stringer("row_id", redact.Stringer(rmode, kv.IntHandle(1))),
		)
		logEntry.CheckFieldNotEmpty(t, "row_mvcc")
		logEntry.CheckFieldNotEmpty(t, "index_mvcc")
	}()

	// test binary column.
	opt := newDefaultOpt()
	opt.clustered = "clustered"
	opt.pkColType = "varbinary(300)"
	opt.idxColType = "varbinary(300)"
	opt.ukColType = "varbinary(300)"
	tk = newInconsistencyKit(t, testkit.NewAsyncTestKit(t, store), newDefaultOpt())
	func() {
		defer tk.rebuild()

		txn, err := store.Begin()
		require.NoError(t, err)
		encoded, err := codec.EncodeKey(time.UTC, nil, types.NewBytesDatum([]byte{1, 0, 1, 0, 0, 1, 1}))
		require.NoError(t, err)
		hd, err := kv.NewCommonHandle(encoded)
		require.NoError(t, err)
		_, err = tk.uniqueIndex.Create(mock.NewContext().GetTableCtx(), txn, []types.Datum{types.NewBytesDatum([]byte{1, 1, 0, 1, 1, 1, 1, 0})}, hd, nil)
		require.NoError(t, err)
		require.NoError(t, txn.Commit(tk.ctx))

		ctx, hook := testutil.WithLogHook(tk.ctx, t, "inconsistency")

		// TODO(tiancaiamao): admin check doesn't support the chunk protocol.
		// Remove this after https://github.com/pingcap/tidb/issues/35156
		tk.MustExec(ctx, "set @@tidb_enable_chunk_rpc = off")

		tk.MustGetErrMsg(ctx, "admin check table admin_test",
			`[admin:8223]data inconsistency in table: admin_test, index: uk1, handle: 282574488403969, index-values:"handle: 282574488403969, values: [KindInt64 282578800083201]" != record-values:""`)
		hook.CheckLogCount(t, 1)
		logEntry := hook.Logs[0]
		logEntry.CheckMsg(t, "admin check found data inconsistency")
		logEntry.CheckField(t,
			zap.String("table_name", "admin_test"),
			zap.String("index_name", "uk1"),
			zap.Stringer("row_id", redact.Stringer(rmode, kv.IntHandle(282574488403969))),
		)
		logEntry.CheckFieldNotEmpty(t, "row_mvcc")
		logEntry.CheckFieldNotEmpty(t, "index_mvcc")
	}()
}

func TestAdminCheckWithSnapshot(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_t_s")
	tk.MustExec("create table admin_t_s (a int, b int, key(a));")
	tk.MustExec("insert into admin_t_s values (0,0),(1,1);")
	tk.MustExec("admin check table admin_t_s;")
	tk.MustExec("admin check index admin_t_s a;")

	snapshotTime := time.Now()

	sctx := mock.NewContext()
	sctx.Store = store
	ctx := sctx.GetTableCtx()
	is := domain.InfoSchema()
	dbName := model.NewCIStr("test")
	tblName := model.NewCIStr("admin_t_s")
	tbl, err := is.TableByName(dbName, tblName)
	require.NoError(t, err)

	tblInfo := tbl.Meta()
	idxInfo := tblInfo.FindIndexByName("a")
	idxOpr := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)
	txn, err := store.Begin()
	require.NoError(t, err)
	_, err = idxOpr.Create(ctx, txn, types.MakeDatums(2), kv.IntHandle(100), nil)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	err = tk.ExecToErr("admin check table admin_t_s")
	require.Error(t, err)
	err = tk.ExecToErr("admin check index admin_t_s a")
	require.Error(t, err)

	// For mocktikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20060102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)
	// For admin check table when use snapshot.
	tk.MustExec("set @@tidb_snapshot = '" + snapshotTime.Format("2006-01-02 15:04:05.999999") + "'")
	tk.MustExec("admin check table admin_t_s;")
	tk.MustExec("admin check index admin_t_s a;")

	tk.MustExec("set @@tidb_snapshot = ''")
	err = tk.ExecToErr("admin check table admin_t_s")
	require.Error(t, err)
	err = tk.ExecToErr("admin check index admin_t_s a")
	require.Error(t, err)

	r := tk.MustQuery("admin cleanup index admin_t_s a")
	r.Check(testkit.Rows("1"))
	tk.MustExec("admin check table admin_t_s;")
	tk.MustExec("admin check index admin_t_s a;")
	tk.MustExec("drop table if exists admin_t_s")
}

func TestAdminCheckTableFailed(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test")
	tk.MustExec("create table admin_test (c1 int, c2 int, c3 varchar(255) default '1', primary key(c1), key(c3), unique key(c2), key(c2, c3))")
	tk.MustExec("insert admin_test (c1, c2, c3) values (-10, -20, 'y'), (-1, -10, 'z'), (1, 11, 'a'), (2, 12, 'b'), (5, 15, 'c'), (10, 20, 'd'), (20, 30, 'e')")

	// Make some corrupted index. Build the index information.
	sctx := mock.NewContext()
	sctx.Store = store
	ctx := sctx.GetTableCtx()
	is := domain.InfoSchema()
	dbName := model.NewCIStr("test")
	tblName := model.NewCIStr("admin_test")
	tbl, err := is.TableByName(dbName, tblName)
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	idxInfo := tblInfo.Indices[1]
	indexOpr := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)
	tk.Session().GetSessionVars().IndexLookupSize = 3
	tk.Session().GetSessionVars().MaxChunkSize = 3

	// Reduce one row of index.
	// Table count > index count.
	// Index c2 is missing 11.
	txn, err := store.Begin()
	require.NoError(t, err)
	err = indexOpr.Delete(ctx, txn, types.MakeDatums(-10), kv.IntHandle(-1))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.EqualError(t, err, "[admin:8223]data inconsistency in table: admin_test, index: c2, handle: -1, index-values:\"\" != record-values:\"handle: -1, values: [KindInt64 -10]\"")
	require.True(t, consistency.ErrAdminCheckInconsistent.Equal(err))
	tk.MustExec("set @@tidb_redact_log=1;")
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.EqualError(t, err, "[admin:8223]data inconsistency in table: admin_test, index: c2, handle: ?, index-values:\"?\" != record-values:\"?\"")

	tk.MustExec("set @@tidb_redact_log=marker;")
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.EqualError(t, err, "[admin:8223]data inconsistency in table: admin_test, index: c2, handle: ‹-1›, index-values:‹\"\"› != record-values:‹\"handle: -1, values: [KindInt64 -10]\"›")
	tk.MustExec("set @@tidb_redact_log=0;")
	r := tk.MustQuery("admin recover index admin_test c2")
	r.Check(testkit.Rows("1 7"))
	tk.MustExec("admin check table admin_test")

	// Add one row of index.
	// Table count < index count.
	// Index c2 has one more values than table data: 0, and the handle 0 hasn't correlative record.
	txn, err = store.Begin()
	require.NoError(t, err)
	_, err = indexOpr.Create(ctx, txn, types.MakeDatums(0), kv.IntHandle(0), nil)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.EqualError(t, err, "[admin:8223]data inconsistency in table: admin_test, index: c2, handle: 0, index-values:\"handle: 0, values: [KindInt64 0]\" != record-values:\"\"")
	tk.MustExec("set @@tidb_redact_log=1;")
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.EqualError(t, err, "[admin:8223]data inconsistency in table: admin_test, index: c2, handle: ?, index-values:\"?\" != record-values:\"?\"")
	tk.MustExec("set @@tidb_redact_log=marker;")
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.EqualError(t, err, "[admin:8223]data inconsistency in table: admin_test, index: c2, handle: ‹0›, index-values:‹\"handle: 0, values: [KindInt64 0]\"› != record-values:‹\"\"›")
	tk.MustExec("set @@tidb_redact_log=0;")

	// Add one row of index.
	// Table count < index count.
	// Index c2 has two more values than table data: 10, 13, and these handles have correlative record.
	txn, err = store.Begin()
	require.NoError(t, err)
	err = indexOpr.Delete(ctx, txn, types.MakeDatums(0), kv.IntHandle(0))
	require.NoError(t, err)
	// Make sure the index value "19" is smaller "21". Then we scan to "19" before "21".
	_, err = indexOpr.Create(ctx, txn, types.MakeDatums(19), kv.IntHandle(10), nil)
	require.NoError(t, err)
	_, err = indexOpr.Create(ctx, txn, types.MakeDatums(13), kv.IntHandle(2), nil)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.EqualError(t, err, "[admin:8223]data inconsistency in table: admin_test, index: c2, handle: 10, index-values:\"handle: 10, values: [KindInt64 19]\" != record-values:\"handle: 10, values: [KindInt64 20]\"")
	tk.MustExec("set @@tidb_redact_log=1;")
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.EqualError(t, err, "[admin:8223]data inconsistency in table: admin_test, index: c2, handle: ?, index-values:\"?\" != record-values:\"?\"")
	tk.MustExec("set @@tidb_redact_log=marker;")
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.EqualError(t, err, "[admin:8223]data inconsistency in table: admin_test, index: c2, handle: ‹10›, index-values:‹\"handle: 10, values: [KindInt64 19]\"› != record-values:‹\"handle: 10, values: [KindInt64 20]\"›")
	tk.MustExec("set @@tidb_redact_log=0;")

	// Table count = index count.
	// Two indices have the same handle.
	txn, err = store.Begin()
	require.NoError(t, err)
	err = indexOpr.Delete(ctx, txn, types.MakeDatums(13), kv.IntHandle(2))
	require.NoError(t, err)
	err = indexOpr.Delete(ctx, txn, types.MakeDatums(12), kv.IntHandle(2))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.Error(t, err, "[admin:8223]data inconsistency in table: admin_test, index: c2, handle: 2, index-values:\"\" != record-values:\"handle: 2, values: [KindInt64 12]\"")
	tk.MustExec("set @@tidb_redact_log=1;")
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.EqualError(t, err, "[admin:8223]data inconsistency in table: admin_test, index: c2, handle: ?, index-values:\"?\" != record-values:\"?\"")
	tk.MustExec("set @@tidb_redact_log=marker;")
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.EqualError(t, err, "[admin:8223]data inconsistency in table: admin_test, index: c2, handle: ‹10›, index-values:‹\"handle: 10, values: [KindInt64 19]\"› != record-values:‹\"handle: 10, values: [KindInt64 20]\"›")
	tk.MustExec("set @@tidb_redact_log=0;")

	// Table count = index count.
	// Index c2 has one line of data is 19, the corresponding table data is 20.
	txn, err = store.Begin()
	require.NoError(t, err)
	_, err = indexOpr.Create(ctx, txn, types.MakeDatums(12), kv.IntHandle(2), nil)
	require.NoError(t, err)
	err = indexOpr.Delete(ctx, txn, types.MakeDatums(20), kv.IntHandle(10))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.EqualError(t, err, "[admin:8223]data inconsistency in table: admin_test, index: c2, handle: 10, index-values:\"handle: 10, values: [KindInt64 19]\" != record-values:\"handle: 10, values: [KindInt64 20]\"")
	tk.MustExec("set @@tidb_redact_log=1;")
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.EqualError(t, err, "[admin:8223]data inconsistency in table: admin_test, index: c2, handle: ?, index-values:\"?\" != record-values:\"?\"")
	tk.MustExec("set @@tidb_redact_log=marker;")
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.EqualError(t, err, "[admin:8223]data inconsistency in table: admin_test, index: c2, handle: ‹10›, index-values:‹\"handle: 10, values: [KindInt64 19]\"› != record-values:‹\"handle: 10, values: [KindInt64 20]\"›")
	tk.MustExec("set @@tidb_redact_log=0;")

	// Recover records.
	txn, err = store.Begin()
	require.NoError(t, err)
	err = indexOpr.Delete(ctx, txn, types.MakeDatums(19), kv.IntHandle(10))
	require.NoError(t, err)
	_, err = indexOpr.Create(ctx, txn, types.MakeDatums(20), kv.IntHandle(10), nil)
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	tk.MustExec("admin check table admin_test")
}

func TestAdminCheckTableErrorLocate(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)

	executor.CheckTableFastBucketSize.Store(8)

	seed := time.Now().UnixNano()
	rand := rand.New(rand.NewSource(seed))
	logutil.BgLogger().Info("random generator", zap.Int64("seed", seed))

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test")
	tk.MustExec("create table admin_test (c1 int, c2 int, primary key(c1), key(c2))")
	tk.MustExec("set cte_max_recursion_depth=10000;")
	tk.MustExec("insert into admin_test with recursive cte(a, b) as (select 1, 1 union select a+1, b+1 from cte where cte.a< 10000) select * from cte;")

	// Make some corrupted index. Build the index information.
	sctx := mock.NewContext()
	sctx.Store = store
	ctx := sctx.GetTableCtx()
	is := domain.InfoSchema()
	dbName := model.NewCIStr("test")
	tblName := model.NewCIStr("admin_test")
	tbl, err := is.TableByName(dbName, tblName)
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	idxInfo := tblInfo.Indices[0]
	indexOpr := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)

	pattern := "handle:\\s(\\d+)"
	r := regexp.MustCompile(pattern)

	// Delete an index record randomly.
	for i := 0; i < 10; i++ {
		txn, err := store.Begin()
		require.NoError(t, err)
		randomRow := rand.Intn(10000) + 1
		err = indexOpr.Delete(ctx, txn, types.MakeDatums(randomRow), kv.IntHandle(randomRow))
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)
		err = tk.ExecToErr("admin check table admin_test")
		require.Error(t, err)
		match := r.FindStringSubmatch(err.Error())
		require.Greater(t, len(match), 0)
		handle, err := strconv.Atoi(match[1])
		require.NoError(t, err)
		require.Equalf(t, randomRow, handle, "i :%d", i)
		tk.MustQuery("admin recover index admin_test c2")
		tk.MustExec("set @@tidb_enable_fast_table_check = 0")
		tk.MustExec("admin check table admin_test")
		tk.MustExec("set @@tidb_enable_fast_table_check = 1")
	}

	// Add an index record randomly on exists row.
	for i := 0; i < 10; i++ {
		txn, err := store.Begin()
		require.NoError(t, err)
		randomRow := rand.Intn(10000) + 1
		_, err = indexOpr.Create(ctx, txn, types.MakeDatums(randomRow+1), kv.IntHandle(randomRow), nil)
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)
		err = tk.ExecToErr("admin check table admin_test")
		require.Error(t, err)
		match := r.FindStringSubmatch(err.Error())
		require.Greater(t, len(match), 0)
		handle, err := strconv.Atoi(match[1])
		require.NoError(t, err)
		require.Equalf(t, randomRow, handle, "i :%d", i)
		txn, err = store.Begin()
		require.NoError(t, err)
		err = indexOpr.Delete(ctx, txn, types.MakeDatums(randomRow+1), kv.IntHandle(randomRow))
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		tk.MustExec("admin check table admin_test")
	}

	// Add an index record randomly on not exists row.
	for i := 0; i < 10; i++ {
		txn, err := store.Begin()
		require.NoError(t, err)
		randomRow := rand.Intn(10000) + 10000
		_, err = indexOpr.Create(ctx, txn, types.MakeDatums(randomRow+1), kv.IntHandle(randomRow), nil)
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)
		err = tk.ExecToErr("admin check table admin_test")
		require.Error(t, err)
		match := r.FindStringSubmatch(err.Error())
		require.Greater(t, len(match), 0)
		handle, err := strconv.Atoi(match[1])
		require.NoError(t, err)
		require.Equalf(t, randomRow, handle, "i :%d", i)
		txn, err = store.Begin()
		require.NoError(t, err)
		err = indexOpr.Delete(ctx, txn, types.MakeDatums(randomRow+1), kv.IntHandle(randomRow))
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		tk.MustExec("admin check table admin_test")
	}

	// Modify an index record randomly.
	for i := 0; i < 10; i++ {
		txn, err := store.Begin()
		require.NoError(t, err)
		randomRow := rand.Intn(10000) + 1
		err = indexOpr.Delete(ctx, txn, types.MakeDatums(randomRow), kv.IntHandle(randomRow))
		require.NoError(t, err)
		_, err = indexOpr.Create(ctx, txn, types.MakeDatums(randomRow+1), kv.IntHandle(randomRow), nil)
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)
		err = tk.ExecToErr("admin check table admin_test")
		require.Error(t, err)
		match := r.FindStringSubmatch(err.Error())
		require.Greater(t, len(match), 0)
		handle, err := strconv.Atoi(match[1])
		require.NoError(t, err)
		require.Equalf(t, randomRow, handle, "i :%d", i)

		txn, err = store.Begin()
		require.NoError(t, err)
		err = indexOpr.Delete(ctx, txn, types.MakeDatums(randomRow+1), kv.IntHandle(randomRow))
		require.NoError(t, err)
		_, err = indexOpr.Create(ctx, txn, types.MakeDatums(randomRow), kv.IntHandle(randomRow), nil)
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		tk.MustExec("admin check table admin_test")
	}
}

func TestAdminCheckTableErrorLocateForClusterIndex(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)

	executor.CheckTableFastBucketSize.Store(8)

	seed := time.Now().UnixNano()
	rand := rand.New(rand.NewSource(seed))
	logutil.BgLogger().Info("random generator", zap.Int64("seed", seed))

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test")
	tk.MustExec("create table admin_test (c1 mediumint, c2 int, primary key(c1) clustered, key(c2))")
	tk.MustExec("set cte_max_recursion_depth=10000;")
	tk.MustExec("insert into admin_test with recursive cte(a, b) as (select 1, 1 union select a+1, b+1 from cte where cte.a< 10000) select * from cte;")

	// Make some corrupted index. Build the index information.
	sctx := mock.NewContext()
	sctx.Store = store
	ctx := sctx.GetTableCtx()
	is := domain.InfoSchema()
	dbName := model.NewCIStr("test")
	tblName := model.NewCIStr("admin_test")
	tbl, err := is.TableByName(dbName, tblName)
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	idxInfo := tblInfo.Indices[0]
	indexOpr := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)
	sc := ctx.GetSessionVars().StmtCtx

	pattern := "handle:\\s(\\d+)"
	r := regexp.MustCompile(pattern)

	getCommonHandle := func(randomRow int) *kv.CommonHandle {
		h, err := codec.EncodeKey(sc.TimeZone(), nil, types.MakeDatums(randomRow)...)
		require.NoError(t, err)
		ch, err := kv.NewCommonHandle(h)
		require.NoError(t, err)
		return ch
	}

	// Delete an index record randomly.
	for i := 0; i < 10; i++ {
		txn, err := store.Begin()
		require.NoError(t, err)
		randomRow := rand.Intn(10000) + 1
		err = indexOpr.Delete(ctx, txn, types.MakeDatums(randomRow), getCommonHandle(randomRow))
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)
		err = tk.ExecToErr("admin check table admin_test")
		require.Error(t, err)
		match := r.FindStringSubmatch(err.Error())
		require.Greater(t, len(match), 0)
		handle, err := strconv.Atoi(match[1])
		require.NoError(t, err)
		require.Equalf(t, randomRow, handle, "i :%d", i)
		tk.MustQuery("admin recover index admin_test c2")
		tk.MustExec("set @@tidb_enable_fast_table_check = 0")
		tk.MustExec("admin check table admin_test")
		tk.MustExec("set @@tidb_enable_fast_table_check = 1")
	}

	// Add an index record randomly on exists row.
	for i := 0; i < 10; i++ {
		txn, err := store.Begin()
		require.NoError(t, err)
		randomRow := rand.Intn(10000) + 1
		_, err = indexOpr.Create(ctx, txn, types.MakeDatums(randomRow+1), getCommonHandle(randomRow), nil)
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)
		err = tk.ExecToErr("admin check table admin_test")
		require.Error(t, err)
		match := r.FindStringSubmatch(err.Error())
		require.Greater(t, len(match), 0)
		handle, err := strconv.Atoi(match[1])
		require.NoError(t, err)
		require.Equalf(t, randomRow, handle, "i :%d", i)
		txn, err = store.Begin()
		require.NoError(t, err)
		err = indexOpr.Delete(ctx, txn, types.MakeDatums(randomRow+1), getCommonHandle(randomRow))
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		tk.MustExec("admin check table admin_test")
	}

	// Add an index record randomly on not exists row.
	for i := 0; i < 10; i++ {
		txn, err := store.Begin()
		require.NoError(t, err)
		randomRow := rand.Intn(10000) + 10000
		_, err = indexOpr.Create(ctx, txn, types.MakeDatums(randomRow+1), getCommonHandle(randomRow), nil)
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)
		err = tk.ExecToErr("admin check table admin_test")
		require.Error(t, err)
		match := r.FindStringSubmatch(err.Error())
		require.Greater(t, len(match), 0)
		handle, err := strconv.Atoi(match[1])
		require.NoError(t, err)
		require.Equalf(t, randomRow, handle, "i :%d", i)
		txn, err = store.Begin()
		require.NoError(t, err)
		err = indexOpr.Delete(ctx, txn, types.MakeDatums(randomRow+1), getCommonHandle(randomRow))
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		tk.MustExec("admin check table admin_test")
	}

	// Modify an index record randomly.
	for i := 0; i < 10; i++ {
		txn, err := store.Begin()
		require.NoError(t, err)
		randomRow := rand.Intn(10000) + 1
		err = indexOpr.Delete(ctx, txn, types.MakeDatums(randomRow), getCommonHandle(randomRow))
		require.NoError(t, err)
		_, err = indexOpr.Create(ctx, txn, types.MakeDatums(randomRow+1), getCommonHandle(randomRow), nil)
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)
		err = tk.ExecToErr("admin check table admin_test")
		require.Error(t, err)
		match := r.FindStringSubmatch(err.Error())
		require.Greater(t, len(match), 0)
		handle, err := strconv.Atoi(match[1])
		require.NoError(t, err)
		require.Equalf(t, randomRow, handle, "i :%d", i)

		txn, err = store.Begin()
		require.NoError(t, err)
		err = indexOpr.Delete(ctx, txn, types.MakeDatums(randomRow+1), getCommonHandle(randomRow))
		require.NoError(t, err)
		_, err = indexOpr.Create(ctx, txn, types.MakeDatums(randomRow), getCommonHandle(randomRow), nil)
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		tk.MustExec("admin check table admin_test")
	}
}

func TestAdminCleanUpGlobalIndex(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test")

	tk.MustExec("set tidb_enable_global_index = true")
	tk.MustExec("create table admin_test (a int, b int, c int, unique key uidx_a(a)) partition by hash(c) partitions 5")
	tk.MustExec("insert admin_test values (-10, -20, 1), (-1, -10, 2), (1, 11, 3), (2, 12, 0), (5, 15, -1), (10, 20, -2), (20, 30, -3)")
	tk.MustExec("analyze table admin_test")

	// Make some corrupted index. Build the index information.
	sctx := mock.NewContext()
	sctx.Store = store
	is := domain.InfoSchema()
	dbName := model.NewCIStr("test")
	tblName := model.NewCIStr("admin_test")
	tbl, err := is.TableByName(dbName, tblName)
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	idxInfo := tblInfo.Indices[0]
	require.True(t, idxInfo.Global)
	idx := tbl.Indices()[0]
	require.NotNil(t, idx)

	// Reduce one row of table.
	// Index count > table count, (2, 12, 0) is deleted.
	txn, err := store.Begin()
	require.NoError(t, err)
	err = txn.Delete(tablecodec.EncodeRowKey(tblInfo.GetPartitionInfo().Definitions[0].ID, kv.IntHandle(4).Encoded()))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.True(t, consistency.ErrAdminCheckInconsistent.Equal(err))

	r := tk.MustQuery("admin cleanup index admin_test uidx_a")
	r.Check(testkit.Rows("1"))
	err = tk.ExecToErr("admin check table admin_test")
	require.NoError(t, err)
	require.Len(t, tk.MustQuery("select * from admin_test use index(uidx_a)").Rows(), 6)
}

func TestAdminRecoverGlobalIndex(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test")

	tk.MustExec("set tidb_enable_global_index = true")
	tk.MustExec("create table admin_test (a int, b int, c int, unique key uidx_a(a)) partition by hash(c) partitions 5")
	tk.MustExec("insert admin_test values (-10, -20, 1), (-1, -10, 2), (1, 11, 3), (2, 12, 0), (5, 15, -1), (10, 20, -2), (20, 30, -3)")
	tk.MustExec("analyze table admin_test")

	// Make some corrupted index. Build the index information.
	sctx := mock.NewContext()
	sctx.Store = store
	is := domain.InfoSchema()
	dbName := model.NewCIStr("test")
	tblName := model.NewCIStr("admin_test")
	tbl, err := is.TableByName(dbName, tblName)
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	idxInfo := tblInfo.Indices[0]
	require.True(t, idxInfo.Global)
	idx := tbl.Indices()[0]
	require.NotNil(t, idx)

	indexOpr := tables.NewIndex(tblInfo.GetPartitionInfo().Definitions[2].ID, tblInfo, idxInfo)

	// Reduce one row of index.
	// Index count < table count, (-1, -10, 2) is deleted.
	txn, err := store.Begin()
	require.NoError(t, err)
	err = indexOpr.Delete(tk.Session().GetTableCtx(), txn, []types.Datum{types.NewIntDatum(-1)}, kv.IntHandle(2))
	require.NoError(t, err)
	err = txn.Commit(context.Background())
	require.NoError(t, err)
	err = tk.ExecToErr("admin check table admin_test")
	require.Error(t, err)
	require.True(t, consistency.ErrAdminCheckInconsistent.Equal(err))

	r := tk.MustQuery("admin recover index admin_test uidx_a")
	r.Check(testkit.Rows("1 7"))
	err = tk.ExecToErr("admin check table admin_test")
	require.NoError(t, err)
	require.Len(t, tk.MustQuery("select * from admin_test use index(uidx_a)").Rows(), 7)
}

func TestAdminCheckGlobalIndex(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	var enableFastCheck = []bool{false, true}
	for _, enabled := range enableFastCheck {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists admin_test")

		tk.MustExec("set tidb_enable_global_index = true")
		tk.MustExec(fmt.Sprintf("set tidb_enable_fast_table_check = %v", enabled))

		tk.MustExec("create table admin_test (a int, b int, c int, unique key uidx_a(a)) partition by hash(c) partitions 5")
		tk.MustExec("insert admin_test values (-10, -20, 1), (-1, -10, 2), (1, 11, 3), (2, 12, 0), (5, 15, -1), (10, 20, -2), (20, 30, -3)")

		// Make some corrupted index. Build the index information.
		sctx := mock.NewContext()
		sctx.Store = store
		is := domain.InfoSchema()
		dbName := model.NewCIStr("test")
		tblName := model.NewCIStr("admin_test")
		tbl, err := is.TableByName(dbName, tblName)
		require.NoError(t, err)
		tblInfo := tbl.Meta()
		idxInfo := tblInfo.Indices[0]
		require.True(t, idxInfo.Global)
		idx := tbl.Indices()[0]
		require.NotNil(t, idx)

		// Reduce one row of table.
		// Index count > table count, (2, 12, 0) is deleted.
		txn, err := store.Begin()
		require.NoError(t, err)
		err = txn.Delete(tablecodec.EncodeRowKey(tblInfo.GetPartitionInfo().Definitions[0].ID, kv.IntHandle(4).Encoded()))
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)
		err = tk.ExecToErr("admin check table admin_test")
		require.Error(t, err)
		require.True(t, consistency.ErrAdminCheckInconsistent.Equal(err))
		require.ErrorContains(t, err, "[admin:8223]data inconsistency in table: admin_test, index: uidx_a, handle: 4, index-values:\"handle: 4, values: [KindInt64 2")

		indexOpr := tables.NewIndex(tblInfo.GetPartitionInfo().Definitions[0].ID, tblInfo, idxInfo)
		// Remove corresponding index key/value.
		// Admin check table will success.
		txn, err = store.Begin()
		require.NoError(t, err)
		err = indexOpr.Delete(tk.Session().GetTableCtx(), txn, []types.Datum{types.NewIntDatum(2)}, kv.IntHandle(4))
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)
		tk.MustExec("admin check table admin_test")

		indexOpr = tables.NewIndex(tblInfo.GetPartitionInfo().Definitions[2].ID, tblInfo, idxInfo)

		// Reduce one row of index.
		// Index count < table count, (-1, -10, 2) is deleted.
		txn, err = store.Begin()
		require.NoError(t, err)
		err = indexOpr.Delete(tk.Session().GetTableCtx(), txn, []types.Datum{types.NewIntDatum(-1)}, kv.IntHandle(2))
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)
		err = tk.ExecToErr("admin check table admin_test")
		require.Error(t, err)
		require.True(t, consistency.ErrAdminCheckInconsistent.Equal(err))
		require.EqualError(t, err, "[admin:8223]data inconsistency in table: admin_test, index: uidx_a, handle: 2, index-values:\"\" != record-values:\"handle: 2, values: [KindInt64 -1]\"")

		// Add one row of index with inconsistent value.
		// Index count = table count, but data is different.
		txn, err = store.Begin()
		require.NoError(t, err)
		_, err = indexOpr.Create(tk.Session().GetTableCtx(), txn, []types.Datum{types.NewIntDatum(100)}, kv.IntHandle(2), nil)
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)
		err = tk.ExecToErr("admin check table admin_test")
		require.Error(t, err)
		if !enabled {
			require.True(t, consistency.ErrAdminCheckInconsistentWithColInfo.Equal(err))
			require.EqualError(t, err, "[executor:8134]data inconsistency in table: admin_test, index: uidx_a, col: a, handle: \"2\", index-values:\"KindInt64 100\" != record-values:\"KindInt64 -1\", compare err:<nil>")
		} else {
			require.True(t, consistency.ErrAdminCheckInconsistent.Equal(err))
			require.EqualError(t, err, "[admin:8223]data inconsistency in table: admin_test, index: uidx_a, handle: 2, index-values:\"handle: 2, values: [KindInt64 100]\" != record-values:\"handle: 2, values: [KindInt64 -1]\"")
		}
	}
}

func TestAdminCheckGlobalIndexWithClusterIndex(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)

	getCommonHandle := func(row int) *kv.CommonHandle {
		h, err := codec.EncodeKey(tk.Session().GetSessionVars().StmtCtx.TimeZone(), nil, types.MakeDatums(row)...)
		require.NoError(t, err)
		ch, err := kv.NewCommonHandle(h)
		require.NoError(t, err)
		return ch
	}

	var enableFastCheck = []bool{false, true}
	for _, enabled := range enableFastCheck {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists admin_test")

		tk.MustExec("set tidb_enable_global_index = true")
		tk.MustExec(fmt.Sprintf("set tidb_enable_fast_table_check = %v", enabled))

		tk.MustExec("create table admin_test (a int, b int, c int, unique key uidx_a(a), primary key(c)) partition by hash(c) partitions 5")
		tk.MustExec("insert admin_test values (-10, -20, 1), (-1, -10, 2), (1, 11, 3), (2, 12, 0), (5, 15, -1), (10, 20, -2), (20, 30, -3)")

		// Make some corrupted index. Build the index information.
		sctx := mock.NewContext()
		sctx.Store = store
		is := domain.InfoSchema()
		dbName := model.NewCIStr("test")
		tblName := model.NewCIStr("admin_test")
		tbl, err := is.TableByName(dbName, tblName)
		require.NoError(t, err)
		tblInfo := tbl.Meta()
		idxInfo := tblInfo.Indices[0]
		require.True(t, idxInfo.Global)
		df := tblInfo.GetPartitionInfo().Definitions[0]

		// Reduce one row of table.
		// Index count > table count, (2, 12, 0) is deleted.
		txn, err := store.Begin()
		require.NoError(t, err)
		txn.Delete(tablecodec.EncodeRowKey(df.ID, kv.IntHandle(0).Encoded()))
		err = txn.Commit(context.Background())
		require.NoError(t, err)
		err = tk.ExecToErr("admin check table admin_test")
		require.Error(t, err)
		require.True(t, consistency.ErrAdminCheckInconsistent.Equal(err))
		require.ErrorContains(t, err, "[admin:8223]data inconsistency in table: admin_test, index: uidx_a, handle: 0, index-values:\"handle: 0, values: [KindInt64 2")

		indexOpr := tables.NewIndex(tblInfo.GetPartitionInfo().Definitions[0].ID, tblInfo, idxInfo)
		// Remove corresponding index key/value.
		// Admin check table will success.
		txn, err = store.Begin()
		require.NoError(t, err)
		err = indexOpr.Delete(tk.Session().GetTableCtx(), txn, []types.Datum{types.NewIntDatum(2)}, getCommonHandle(0))
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)
		tk.MustExec("admin check table admin_test")

		indexOpr = tables.NewIndex(tblInfo.GetPartitionInfo().Definitions[2].ID, tblInfo, idxInfo)
		// Reduce one row of index.
		// Index count < table count, (-1, -10, 2) is deleted.
		txn, err = store.Begin()
		require.NoError(t, err)
		err = indexOpr.Delete(tk.Session().GetTableCtx(), txn, []types.Datum{types.NewIntDatum(-1)}, getCommonHandle(2))
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)
		err = tk.ExecToErr("admin check table admin_test")
		require.Error(t, err)
		require.True(t, consistency.ErrAdminCheckInconsistent.Equal(err))
		require.EqualError(t, err, "[admin:8223]data inconsistency in table: admin_test, index: uidx_a, handle: 2, index-values:\"\" != record-values:\"handle: 2, values: [KindInt64 -1]\"")

		// Add one row with inconsistent value.
		// Index count = table count, but data is different.
		txn, err = store.Begin()
		require.NoError(t, err)
		_, err = indexOpr.Create(tk.Session().GetTableCtx(), txn, []types.Datum{types.NewIntDatum(100)}, getCommonHandle(2), nil)
		require.NoError(t, err)
		err = txn.Commit(context.Background())
		require.NoError(t, err)
		err = tk.ExecToErr("admin check table admin_test")
		require.Error(t, err)
		if !enabled {
			require.True(t, consistency.ErrAdminCheckInconsistentWithColInfo.Equal(err))
			require.EqualError(t, err, "[executor:8134]data inconsistency in table: admin_test, index: uidx_a, col: a, handle: \"2\", index-values:\"KindInt64 100\" != record-values:\"KindInt64 -1\", compare err:<nil>")
		} else {
			require.True(t, consistency.ErrAdminCheckInconsistent.Equal(err))
			require.EqualError(t, err, "[admin:8223]data inconsistency in table: admin_test, index: uidx_a, handle: 2, index-values:\"handle: 2, values: [KindInt64 100]\" != record-values:\"handle: 2, values: [KindInt64 -1]\"")
		}
	}
}

func TestAdminCheckGlobalIndexDuringDDL(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	originalHook := dom.DDL().GetHook()
	tk := testkit.NewTestKit(t, store)

	var schemaMap = make(map[model.SchemaState]struct{})

	hook := &callback.TestDDLCallback{Do: dom}
	onJobUpdatedExportedFunc := func(job *model.Job) {
		schemaMap[job.SchemaState] = struct{}{}
		_, err := tk.Exec("admin check table admin_test")
		assert.NoError(t, err)
	}
	hook.OnJobUpdatedExported.Store(&onJobUpdatedExportedFunc)

	// check table after delete some index key/value pairs.
	ddl.MockDMLExecution = func() {
		_, err := tk.Exec("admin check table admin_test")
		assert.NoError(t, err)
	}

	batchSize := 32
	tk.MustExec(fmt.Sprintf("set global tidb_ddl_reorg_batch_size = %d", batchSize))

	var enableFastCheck = []bool{false, true}
	for _, enabled := range enableFastCheck {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists admin_test")

		tk.MustExec("set tidb_enable_global_index = true")
		tk.MustExec(fmt.Sprintf("set tidb_enable_fast_table_check = %v", enabled))

		tk.MustExec("create table admin_test (a int, b int, c int, unique key uidx_a(a), primary key(c)) partition by hash(c) partitions 5")
		tk.MustExec("insert admin_test values (-10, -20, 1), (-1, -10, 2), (1, 11, 3), (2, 12, 0), (5, 15, -1), (10, 20, -2), (20, 30, -3)")
		for i := 1; i <= batchSize*2; i++ {
			tk.MustExec(fmt.Sprintf("insert admin_test values (%d, %d, %d)", i*5+1, i, i*5+1))
		}

		dom.DDL().SetHook(hook)
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockDMLExecution", "1*return(true)->return(false)"))
		tk.MustExec("alter table admin_test truncate partition p1")
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockDMLExecution"))
		dom.DDL().SetHook(originalHook)

		// Should have 3 different schema states, `none`, `deleteOnly`, `deleteReorg`
		require.Len(t, schemaMap, 3)
		for ss := range schemaMap {
			delete(schemaMap, ss)
		}
	}
}
