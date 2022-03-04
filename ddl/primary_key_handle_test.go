// Copyright 2019 PingCAP, Inc.
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

package ddl_test

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
)

type testMaxTableRowIDContext struct {
	t   *testing.T
	d   ddl.DDL
	tbl table.Table
}

func newTestMaxTableRowIDContext(t *testing.T, d ddl.DDL, tbl table.Table) *testMaxTableRowIDContext {
	return &testMaxTableRowIDContext{
		t:   t,
		d:   d,
		tbl: tbl,
	}
}

func getMaxTableHandle(ctx *testMaxTableRowIDContext, store kv.Storage) (kv.Handle, bool) {
	t := ctx.t
	d := ctx.d
	tbl := ctx.tbl
	curVer, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	maxHandle, emptyTable, err := d.GetTableMaxHandle(curVer.Ver, tbl.(table.PhysicalTable))
	require.NoError(t, err)
	return maxHandle, emptyTable
}

func checkGetMaxTableRowID(ctx *testMaxTableRowIDContext, store kv.Storage, expectEmpty bool, expectMaxHandle kv.Handle) {
	t := ctx.t
	maxHandle, emptyTable := getMaxTableHandle(ctx, store)
	require.Equal(t, expectEmpty, emptyTable)
	require.EqualValues(t, expectMaxHandle, maxHandle)
}

func TestPrimaryKey(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists test_primary_key;")
	tk.MustExec("create database test_primary_key;")
	tk.MustExec("use test_primary_key;")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly

	// Test add/drop primary key on a plain table.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b varchar(10));")
	tk.MustGetErrCode("alter table t add primary key(a) clustered;", errno.ErrUnsupportedDDLOperation)
	tk.MustExec("alter table t add primary key(a) nonclustered;")
	tk.MustExec("alter table t drop primary key;")
	tk.MustExec("alter table t add primary key(a) nonclustered;")
	tk.MustExec("drop index `primary` on t;")
	tk.MustExec("alter table t add primary key(a);") // implicit nonclustered
	tk.MustExec("drop index `primary` on t;")
	tk.MustGetErrCode("drop index `primary` on t;", errno.ErrCantDropFieldOrKey)

	// Test add/drop primary key on a PKIsHandle table.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b varchar(10), primary key(a) clustered);")
	tk.MustGetErrCode("alter table t drop primary key;", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t add primary key(a) clustered;", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t add primary key(a) nonclustered;", mysql.ErrMultiplePriKey)
	tk.MustGetErrCode("alter table t add primary key(a);", errno.ErrMultiplePriKey) // implicit nonclustered
	tk.MustGetErrCode("alter table t add primary key(b) clustered;", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t add primary key(b) nonclustered;", errno.ErrMultiplePriKey)
	tk.MustGetErrCode("alter table t add primary key(b);", errno.ErrMultiplePriKey) // implicit nonclustered

	// Test add/drop primary key on a nonclustered primary key table.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b varchar(10), primary key(a) nonclustered);")
	tk.MustGetErrCode("alter table t add primary key(a) clustered;", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t add primary key(a) nonclustered;", errno.ErrMultiplePriKey)
	tk.MustGetErrCode("alter table t add primary key(a);", errno.ErrMultiplePriKey) // implicit nonclustered
	tk.MustGetErrCode("alter table t add primary key(b) clustered;", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t add primary key(b) nonclustered;", errno.ErrMultiplePriKey)
	tk.MustGetErrCode("alter table t add primary key(b);", errno.ErrMultiplePriKey) // implicit nonclustered
	tk.MustExec("alter table t drop primary key;")

	// Test add/drop primary key on a CommonHandle key table.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b varchar(10), primary key(b) clustered);")
	tk.MustGetErrCode("alter table t drop primary key;", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t add primary key(a) clustered;", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t add primary key(a) nonclustered;", errno.ErrMultiplePriKey)
	tk.MustGetErrCode("alter table t add primary key(a);", errno.ErrMultiplePriKey) // implicit nonclustered
	tk.MustGetErrCode("alter table t add primary key(b) clustered;", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t add primary key(b) nonclustered;", errno.ErrMultiplePriKey)
	tk.MustGetErrCode("alter table t add primary key(b);", errno.ErrMultiplePriKey) // implicit nonclustered

	// Test add/drop primary key when the column&index name is `primary`.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (`primary` int);")
	tk.MustExec("alter table t add index (`primary`);")
	tk.MustGetErrCode("drop index `primary` on t;", errno.ErrCantDropFieldOrKey)

	// The primary key cannot be invisible, for the case pk_is_handle.
	tk.MustExec("drop table if exists t;")
	tk.MustGetErrCode("create table t(c1 int not null, primary key(c1) invisible);", errno.ErrPKIndexCantBeInvisible)
	tk.MustExec("create table t (a int, b int not null, primary key(a), unique(b) invisible);")
	tk.MustExec("drop table t;")
}

func TestDropAutoIncrementIndex2(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int(11) not null auto_increment key, b int(11), c bigint, unique key (a, b, c))")
	tk.MustExec("alter table t1 drop index a")
}

func TestMultiRegionGetTableEndHandle(t *testing.T) {
	var cluster testutils.Cluster
	store, clean := testkit.CreateMockStore(t, mockstore.WithClusterInspector(func(c testutils.Cluster) {
		mockstore.BootstrapWithSingleStore(c)
		cluster = c
	}))
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists test_get_endhandle")
	tk.MustExec("create database test_get_endhandle")
	tk.MustExec("use test_get_endhandle")

	tk.MustExec("create table t(a bigint PRIMARY KEY, b int)")
	var builder strings.Builder
	fmt.Fprintf(&builder, "insert into t values ")
	for i := 0; i < 1000; i++ {
		fmt.Fprintf(&builder, "(%v, %v),", i, i)
	}
	sql := builder.String()
	tk.MustExec(sql[:len(sql)-1])

	// Get table ID for split.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test_get_endhandle"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblID := tbl.Meta().ID

	d := dom.DDL()
	testCtx := newTestMaxTableRowIDContext(t, d, tbl)

	// Split the table.
	tableStart := tablecodec.GenTableRecordPrefix(tblID)
	cluster.SplitKeys(tableStart, tableStart.PrefixNext(), 100)

	maxHandle, emptyTable := getMaxTableHandle(testCtx, store)
	require.False(t, emptyTable)
	require.Equal(t, kv.IntHandle(999), maxHandle)

	tk.MustExec("insert into t values(10000, 1000)")
	maxHandle, emptyTable = getMaxTableHandle(testCtx, store)
	require.False(t, emptyTable)
	require.Equal(t, kv.IntHandle(10000), maxHandle)

	tk.MustExec("insert into t values(-1, 1000)")
	maxHandle, emptyTable = getMaxTableHandle(testCtx, store)
	require.False(t, emptyTable)
	require.Equal(t, kv.IntHandle(10000), maxHandle)
}

func TestGetTableEndHandle(t *testing.T) {
	// TestGetTableEndHandle test ddl.GetTableMaxHandle method, which will return the max row id of the table.
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists test_get_endhandle")
	tk.MustExec("create database test_get_endhandle")
	tk.MustExec("use test_get_endhandle")
	// Test PK is handle.
	tk.MustExec("create table t(a bigint PRIMARY KEY, b int)")

	is := dom.InfoSchema()
	d := dom.DDL()
	tbl, err := is.TableByName(model.NewCIStr("test_get_endhandle"), model.NewCIStr("t"))
	require.NoError(t, err)

	testCtx := newTestMaxTableRowIDContext(t, d, tbl)
	// test empty table
	checkGetMaxTableRowID(testCtx, store, true, nil)

	tk.MustExec("insert into t values(-1, 1)")
	checkGetMaxTableRowID(testCtx, store, false, kv.IntHandle(-1))

	tk.MustExec("insert into t values(9223372036854775806, 1)")
	checkGetMaxTableRowID(testCtx, store, false, kv.IntHandle(9223372036854775806))

	tk.MustExec("insert into t values(9223372036854775807, 1)")
	checkGetMaxTableRowID(testCtx, store, false, kv.IntHandle(9223372036854775807))

	tk.MustExec("insert into t values(10, 1)")
	tk.MustExec("insert into t values(102149142, 1)")
	checkGetMaxTableRowID(testCtx, store, false, kv.IntHandle(9223372036854775807))

	tk.MustExec("create table t1(a bigint PRIMARY KEY, b int)")

	var builder strings.Builder
	fmt.Fprintf(&builder, "insert into t1 values ")
	for i := 0; i < 1000; i++ {
		fmt.Fprintf(&builder, "(%v, %v),", i, i)
	}
	sql := builder.String()
	tk.MustExec(sql[:len(sql)-1])

	is = dom.InfoSchema()
	testCtx.tbl, err = is.TableByName(model.NewCIStr("test_get_endhandle"), model.NewCIStr("t1"))
	require.NoError(t, err)
	checkGetMaxTableRowID(testCtx, store, false, kv.IntHandle(999))

	// Test PK is not handle
	tk.MustExec("create table t2(a varchar(255))")

	is = dom.InfoSchema()
	testCtx.tbl, err = is.TableByName(model.NewCIStr("test_get_endhandle"), model.NewCIStr("t2"))
	require.NoError(t, err)
	checkGetMaxTableRowID(testCtx, store, true, nil)

	builder.Reset()
	fmt.Fprintf(&builder, "insert into t2 values ")
	for i := 0; i < 1000; i++ {
		fmt.Fprintf(&builder, "(%v),", i)
	}
	sql = builder.String()
	tk.MustExec(sql[:len(sql)-1])

	result := tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxHandle, emptyTable := getMaxTableHandle(testCtx, store)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxHandle.IntValue())))
	require.False(t, emptyTable)

	tk.MustExec("insert into t2 values(100000)")
	result = tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxHandle, emptyTable = getMaxTableHandle(testCtx, store)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxHandle.IntValue())))
	require.False(t, emptyTable)

	tk.MustExec(fmt.Sprintf("insert into t2 values(%v)", math.MaxInt64-1))
	result = tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxHandle, emptyTable = getMaxTableHandle(testCtx, store)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxHandle.IntValue())))
	require.False(t, emptyTable)

	tk.MustExec(fmt.Sprintf("insert into t2 values(%v)", math.MaxInt64))
	result = tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxHandle, emptyTable = getMaxTableHandle(testCtx, store)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxHandle.IntValue())))
	require.False(t, emptyTable)

	tk.MustExec("insert into t2 values(100)")
	result = tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxHandle, emptyTable = getMaxTableHandle(testCtx, store)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxHandle.IntValue())))
	require.False(t, emptyTable)
}

func TestMultiRegionGetTableEndCommonHandle(t *testing.T) {
	var cluster testutils.Cluster
	store, dom, clean := testkit.CreateMockStoreAndDomain(t, mockstore.WithClusterInspector(func(c testutils.Cluster) {
		mockstore.BootstrapWithSingleStore(c)
		cluster = c
	}))
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists test_get_endhandle")
	tk.MustExec("create database test_get_endhandle")
	tk.MustExec("use test_get_endhandle")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn

	tk.MustExec("create table t(a varchar(20), b int, c float, d bigint, primary key (a, b, c))")
	var builder strings.Builder
	fmt.Fprintf(&builder, "insert into t values ")
	for i := 0; i < 1000; i++ {
		fmt.Fprintf(&builder, "('%v', %v, %v, %v),", i, i, i, i)
	}
	sql := builder.String()
	tk.MustExec(sql[:len(sql)-1])

	// Get table ID for split.
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test_get_endhandle"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblID := tbl.Meta().ID

	d := dom.DDL()
	testCtx := newTestMaxTableRowIDContext(t, d, tbl)

	// Split the table.
	tableStart := tablecodec.GenTableRecordPrefix(tblID)
	cluster.SplitKeys(tableStart, tableStart.PrefixNext(), 100)

	maxHandle, emptyTable := getMaxTableHandle(testCtx, store)
	require.False(t, emptyTable)
	require.EqualValues(t, testutil.MustNewCommonHandle(t, "999", 999, 999), maxHandle)

	tk.MustExec("insert into t values('a', 1, 1, 1)")
	maxHandle, emptyTable = getMaxTableHandle(testCtx, store)
	require.False(t, emptyTable)
	require.EqualValues(t, testutil.MustNewCommonHandle(t, "a", 1, 1), maxHandle)

	tk.MustExec("insert into t values('0000', 1, 1, 1)")
	maxHandle, emptyTable = getMaxTableHandle(testCtx, store)
	require.False(t, emptyTable)
	require.EqualValues(t, testutil.MustNewCommonHandle(t, "a", 1, 1), maxHandle)
}

func TestGetTableEndCommonHandle(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists test_get_endhandle")
	tk.MustExec("create database test_get_endhandle")
	tk.MustExec("use test_get_endhandle")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn

	tk.MustExec("create table t(a varchar(15), b bigint, c int, primary key (a, b))")
	tk.MustExec("create table t1(a varchar(15), b bigint, c int, primary key (a(2), b))")

	is := dom.InfoSchema()
	d := dom.DDL()
	tbl, err := is.TableByName(model.NewCIStr("test_get_endhandle"), model.NewCIStr("t"))
	require.NoError(t, err)
	testCtx := newTestMaxTableRowIDContext(t, d, tbl)

	// test empty table
	checkGetMaxTableRowID(testCtx, store, true, nil)
	tk.MustExec("insert into t values('abc', 1, 10)")
	expectedHandle := testutil.MustNewCommonHandle(t, "abc", 1)
	checkGetMaxTableRowID(testCtx, store, false, expectedHandle)
	tk.MustExec("insert into t values('abchzzzzzzzz', 1, 10)")
	expectedHandle = testutil.MustNewCommonHandle(t, "abchzzzzzzzz", 1)
	checkGetMaxTableRowID(testCtx, store, false, expectedHandle)
	tk.MustExec("insert into t values('a', 1, 10)")
	tk.MustExec("insert into t values('ab', 1, 10)")
	checkGetMaxTableRowID(testCtx, store, false, expectedHandle)

	// Test MaxTableRowID with prefixed primary key.

	tbl = tk.GetTableByName("test_get_endhandle", "t1")

	d = dom.DDL()
	testCtx = newTestMaxTableRowIDContext(t, d, tbl)
	checkGetMaxTableRowID(testCtx, store, true, nil)
	tk.MustExec("insert into t1 values('abccccc', 1, 10)")
	expectedHandle = testutil.MustNewCommonHandle(t, "ab", 1)
	checkGetMaxTableRowID(testCtx, store, false, expectedHandle)
	tk.MustExec("insert into t1 values('azzzz', 1, 10)")
	expectedHandle = testutil.MustNewCommonHandle(t, "az", 1)
	checkGetMaxTableRowID(testCtx, store, false, expectedHandle)
}

func TestAutoRandomChangeFromAutoInc(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("set @@tidb_allow_remove_auto_inc = 1;")

	// Basic usages.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a bigint auto_increment primary key);")
	tk.MustExec("insert into t values (), (), ();")
	tk.MustExec("alter table t modify column a bigint auto_random(3);")
	tk.MustExec("insert into t values (), (), ();")
	rows := tk.MustQuery("show table t next_row_id;").Rows()
	require.Lenf(t, rows, 1, "query result: %v", rows)
	require.Lenf(t, rows[0], 5, "query result: %v", rows)
	require.Equal(t, "AUTO_RANDOM", rows[0][4])

	// Changing from auto_inc unique key is not allowed.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a bigint auto_increment unique key);")
	tk.MustGetErrCode("alter table t modify column a bigint auto_random;", errno.ErrInvalidAutoRandom)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a bigint auto_increment unique key, b bigint auto_random primary key);")
	tk.MustGetErrCode("alter table t modify column a bigint auto_random;", errno.ErrInvalidAutoRandom)

	// Changing from non-auto-inc column is not allowed.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a bigint);")
	tk.MustGetErrCode("alter table t modify column a bigint auto_random;", errno.ErrInvalidAutoRandom)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a bigint primary key);")
	tk.MustGetErrCode("alter table t modify column a bigint auto_random;", errno.ErrInvalidAutoRandom)

	// Changing from non BIGINT auto_inc pk column is not allowed.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int auto_increment primary key);")
	tk.MustGetErrCode("alter table t modify column a int auto_random;", errno.ErrInvalidAutoRandom)
	tk.MustGetErrCode("alter table t modify column a bigint auto_random;", errno.ErrInvalidAutoRandom)

	// Changing from auto_random to auto_increment is not allowed.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a bigint auto_random primary key);")
	// "Unsupported modify column: can't set auto_increment"
	tk.MustGetErrCode("alter table t modify column a bigint auto_increment;", errno.ErrUnsupportedDDLOperation)

	// Large auto_increment number overflows auto_random.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a bigint auto_increment primary key);")
	tk.MustExec("insert into t values (1<<(64-5));")
	// "max allowed auto_random shard bits is 3, but got 4 on column `a`"
	tk.MustGetErrCode("alter table t modify column a bigint auto_random(4);", errno.ErrInvalidAutoRandom)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a bigint auto_increment primary key);")
	tk.MustExec("insert into t values (1<<(64-6));")
	tk.MustExec("alter table t modify column a bigint auto_random(4);")
}

func TestAutoRandomExchangePartition(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists auto_random_db")
	defer tk.MustExec("drop database if exists auto_random_db")

	tk.MustExec("use auto_random_db")

	tk.MustExec("set @@tidb_enable_exchange_partition=1")
	defer tk.MustExec("set @@tidb_enable_exchange_partition=0")

	tk.MustExec("drop table if exists e1, e2, e3, e4;")

	tk.MustExec("create table e1 (a bigint primary key clustered auto_random(3)) partition by hash(a) partitions 1;")

	tk.MustExec("create table e2 (a bigint primary key);")
	tk.MustGetErrCode("alter table e1 exchange partition p0 with table e2;", errno.ErrTablesDifferentMetadata)

	tk.MustExec("create table e3 (a bigint primary key auto_random(2));")
	tk.MustGetErrCode("alter table e1 exchange partition p0 with table e3;", errno.ErrTablesDifferentMetadata)
	tk.MustExec("insert into e1 values (), (), ()")

	tk.MustExec("create table e4 (a bigint primary key auto_random(3));")
	tk.MustExec("insert into e4 values ()")
	tk.MustExec("alter table e1 exchange partition p0 with table e4;")

	tk.MustQuery("select count(*) from e1").Check(testkit.Rows("1"))
	tk.MustExec("insert into e1 values ()")
	tk.MustQuery("select count(*) from e1").Check(testkit.Rows("2"))

	tk.MustQuery("select count(*) from e4").Check(testkit.Rows("3"))
	tk.MustExec("insert into e4 values ()")
	tk.MustQuery("select count(*) from e4").Check(testkit.Rows("4"))
}

func TestAutoRandomIncBitsIncrementAndOffset(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists auto_random_db")
	defer tk.MustExec("drop database if exists auto_random_db")
	tk.MustExec("use auto_random_db")
	tk.MustExec("drop table if exists t")

	recreateTable := func() {
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t (a bigint auto_random(6) primary key clustered)")
	}
	truncateTable := func() {
		_, _ = tk.Exec("delete from t")
	}
	insertTable := func() {
		tk.MustExec("insert into t values ()")
	}
	assertIncBitsValues := func(values ...int) {
		mask := strings.Repeat("1", 64-1-6)
		sql := fmt.Sprintf(`select a & b'%s' from t order by a & b'%s' asc`, mask, mask)
		vs := make([]string, len(values))
		for i, value := range values {
			vs[i] = strconv.Itoa(value)
		}
		tk.MustQuery(sql).Check(testkit.Rows(vs...))
	}

	const truncate, recreate = true, false
	expect := func(vs ...int) []int { return vs }
	testCase := []struct {
		setupAction bool  // truncate or recreate
		increment   int   // @@auto_increment_increment
		offset      int   // @@auto_increment_offset
		results     []int // the implicit allocated auto_random incremental-bit part of values
	}{
		{recreate, 5, 10, expect(10, 15, 20)},
		{recreate, 2, 10, expect(10, 12, 14)},
		{truncate, 5, 10, expect(15, 20, 25)},
		{truncate, 10, 10, expect(30, 40, 50)},
		{truncate, 5, 10, expect(55, 60, 65)},
	}
	for _, tc := range testCase {
		switch tc.setupAction {
		case recreate:
			recreateTable()
		case truncate:
			truncateTable()
		}
		tk.Session().GetSessionVars().AutoIncrementIncrement = tc.increment
		tk.Session().GetSessionVars().AutoIncrementOffset = tc.offset
		for range tc.results {
			insertTable()
		}
		assertIncBitsValues(tc.results...)
	}
}

func TestInvisibleIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t,t1,t2,t3,t4,t5,t6")

	// The DDL statement related to invisible index.
	showIndexes := "select index_name, is_visible from information_schema.statistics where table_schema = 'test' and table_name = 't'"
	// 1. Create table with invisible index
	tk.MustExec("create table t (a int, b int, unique (a) invisible)")
	tk.MustQuery(showIndexes).Check(testkit.Rows("a NO"))
	tk.MustExec("insert into t values (1, 2)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2"))
	// 2. Drop invisible index
	tk.MustExec("alter table t drop index a")
	tk.MustQuery(showIndexes).Check(testkit.Rows())
	tk.MustExec("insert into t values (3, 4)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2", "3 4"))
	// 3. Add an invisible index
	tk.MustExec("alter table t add index (b) invisible")
	tk.MustQuery(showIndexes).Check(testkit.Rows("b NO"))
	tk.MustExec("insert into t values (5, 6)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2", "3 4", "5 6"))
	// 4. Drop it
	tk.MustExec("alter table t drop index b")
	tk.MustQuery(showIndexes).Check(testkit.Rows())
	tk.MustExec("insert into t values (7, 8)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2", "3 4", "5 6", "7 8"))
	// 5. Create a multiple-column invisible index
	tk.MustExec("alter table t add index a_b(a, b) invisible")
	tk.MustQuery(showIndexes).Check(testkit.Rows("a_b NO", "a_b NO"))
	tk.MustExec("insert into t values (9, 10)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2", "3 4", "5 6", "7 8", "9 10"))
	// 6. Drop it
	tk.MustExec("alter table t drop index a_b")
	tk.MustQuery(showIndexes).Check(testkit.Rows())
	tk.MustExec("insert into t values (11, 12)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2", "3 4", "5 6", "7 8", "9 10", "11 12"))

	// Limitation: Primary key cannot be invisible index
	tk.MustGetErrCode("create table t1 (a int, primary key (a) nonclustered invisible)", errno.ErrPKIndexCantBeInvisible)
	tk.MustGetErrCode("create table t1 (a int, b int, primary key (a, b) nonclustered invisible)", errno.ErrPKIndexCantBeInvisible)
	tk.MustExec("create table t1 (a int, b int)")
	tk.MustGetErrCode("alter table t1 add primary key(a) nonclustered invisible", errno.ErrPKIndexCantBeInvisible)
	tk.MustGetErrCode("alter table t1 add primary key(a, b) nonclustered invisible", errno.ErrPKIndexCantBeInvisible)

	// Implicit primary key cannot be invisible index
	// Create a implicit primary key
	tk.MustGetErrCode("create table t2(a int not null, unique (a) invisible)", errno.ErrPKIndexCantBeInvisible)
	tk.MustGetErrCode("create table t2(a int auto_increment, unique key (a) invisible);", errno.ErrPKIndexCantBeInvisible)
	// Column `a` become implicit primary key after DDL statement on itself
	tk.MustExec("create table t2(a int not null)")
	tk.MustGetErrCode("alter table t2 add unique (a) invisible", errno.ErrPKIndexCantBeInvisible)
	tk.MustExec("create table t3(a int, unique index (a) invisible)")
	tk.MustGetErrCode("alter table t3 modify column a int not null", errno.ErrPKIndexCantBeInvisible)
	// Only first unique column can be implicit primary
	tk.MustExec("create table t4(a int not null, b int not null, unique (a), unique (b) invisible)")
	showIndexes = "select index_name, is_visible from information_schema.statistics where table_schema = 'test' and table_name = 't4'"
	tk.MustQuery(showIndexes).Check(testkit.Rows("a YES", "b NO"))
	tk.MustExec("insert into t4 values (1, 2)")
	tk.MustQuery("select * from t4").Check(testkit.Rows("1 2"))
	tk.MustGetErrCode("create table t5(a int not null, b int not null, unique (b) invisible, unique (a))", errno.ErrPKIndexCantBeInvisible)
	// Column `b` become implicit primary key after DDL statement on other columns
	tk.MustExec("create table t5(a int not null, b int not null, unique (a), unique (b) invisible)")
	tk.MustGetErrCode("alter table t5 drop index a", errno.ErrPKIndexCantBeInvisible)
	tk.MustGetErrCode("alter table t5 modify column a int null", errno.ErrPKIndexCantBeInvisible)
	// If these is a explicit primary key, no key will become implicit primary key
	tk.MustExec("create table t6 (a int not null, b int, unique (a) invisible, primary key(b) nonclustered)")
	showIndexes = "select index_name, is_visible from information_schema.statistics where table_schema = 'test' and table_name = 't6'"
	tk.MustQuery(showIndexes).Check(testkit.Rows("a NO", "PRIMARY YES"))
	tk.MustExec("insert into t6 values (1, 2)")
	tk.MustQuery("select * from t6").Check(testkit.Rows("1 2"))
	tk.MustGetErrCode("alter table t6 drop primary key", errno.ErrPKIndexCantBeInvisible)
	res := tk.MustQuery("show index from t6 where Key_name='PRIMARY';")
	require.Len(t, res.Rows(), 1)
}

func TestCreateClusteredIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t1 (a int primary key, b int)")
	tk.MustExec("CREATE TABLE t2 (a varchar(255) primary key, b int)")
	tk.MustExec("CREATE TABLE t3 (a int, b int, c int, primary key (a, b))")
	tk.MustExec("CREATE TABLE t4 (a int, b int, c int)")
	ctx := tk.Session().(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	require.True(t, tbl.Meta().PKIsHandle)
	require.False(t, tbl.Meta().IsCommonHandle)
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	require.True(t, tbl.Meta().IsCommonHandle)
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t3"))
	require.NoError(t, err)
	require.True(t, tbl.Meta().IsCommonHandle)
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t4"))
	require.NoError(t, err)
	require.False(t, tbl.Meta().IsCommonHandle)

	tk.MustExec("CREATE TABLE t5 (a varchar(255) primary key nonclustered, b int)")
	tk.MustExec("CREATE TABLE t6 (a int, b int, c int, primary key (a, b) nonclustered)")
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t5"))
	require.NoError(t, err)
	require.False(t, tbl.Meta().IsCommonHandle)
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t6"))
	require.NoError(t, err)
	require.False(t, tbl.Meta().IsCommonHandle)

	tk.MustExec("CREATE TABLE t21 like t2")
	tk.MustExec("CREATE TABLE t31 like t3")
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t21"))
	require.NoError(t, err)
	require.True(t, tbl.Meta().IsCommonHandle)
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t31"))
	require.NoError(t, err)
	require.True(t, tbl.Meta().IsCommonHandle)

	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	tk.MustExec("CREATE TABLE t7 (a varchar(255) primary key, b int)")
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t7"))
	require.NoError(t, err)
	require.False(t, tbl.Meta().IsCommonHandle)
}
