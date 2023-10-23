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
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
)

func getTableMaxHandle(t *testing.T, d ddl.DDL, tbl table.Table, store kv.Storage) (kv.Handle, bool) {
	ver, err := store.CurrentVersion(kv.GlobalTxnScope)
	require.NoError(t, err)
	maxHandle, emptyTable, err := d.GetTableMaxHandle(ddl.NewJobContext(), ver.Ver, tbl.(table.PhysicalTable))
	require.NoError(t, err)
	return maxHandle, emptyTable
}

func checkTableMaxHandle(t *testing.T, d ddl.DDL, tbl table.Table, store kv.Storage, expectedEmpty bool, expectedMaxHandle kv.Handle) {
	maxHandle, emptyHandle := getTableMaxHandle(t, d, tbl, store)
	require.Equal(t, expectedEmpty, emptyHandle)
	if expectedEmpty {
		require.True(t, emptyHandle)
		require.Nil(t, maxHandle)
	} else {
		require.False(t, emptyHandle)
		testutil.HandleEqual(t, expectedMaxHandle, maxHandle)
	}
}

func TestMultiRegionGetTableEndHandle(t *testing.T) {
	var cluster testutils.Cluster
	store := testkit.CreateMockStore(t, mockstore.WithClusterInspector(func(c testutils.Cluster) {
		mockstore.BootstrapWithSingleStore(c)
		cluster = c
	}))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a bigint PRIMARY KEY, b int)")
	var builder strings.Builder
	_, _ = fmt.Fprintf(&builder, "insert into t values ")
	for i := 0; i < 1000; i++ {
		_, _ = fmt.Fprintf(&builder, "(%v, %v),", i, i)
	}
	sql := builder.String()
	tk.MustExec(sql[:len(sql)-1])

	// Get table ID for split.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	d := dom.DDL()

	// Split the table.
	tableStart := tablecodec.GenTableRecordPrefix(tbl.Meta().ID)
	cluster.SplitKeys(tableStart, tableStart.PrefixNext(), 100)
	checkTableMaxHandle(t, d, tbl, store, false, kv.IntHandle(999))

	tk.MustExec("insert into t values(10000, 1000)")
	checkTableMaxHandle(t, d, tbl, store, false, kv.IntHandle(10000))

	tk.MustExec("insert into t values(-1, 1000)")
	checkTableMaxHandle(t, d, tbl, store, false, kv.IntHandle(10000))
}

func TestGetTableEndHandle(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	// TestGetTableEndHandle test ddl.GetTableMaxHandle method, which will return the max row id of the table.
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// PK is handle.
	tk.MustExec("create table t(a bigint PRIMARY KEY, b int)")

	is := dom.InfoSchema()
	d := dom.DDL()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	// test empty table
	checkTableMaxHandle(t, d, tbl, store, true, nil)

	tk.MustExec("insert into t values(-1, 1)")
	checkTableMaxHandle(t, d, tbl, store, false, kv.IntHandle(-1))

	tk.MustExec("insert into t values(9223372036854775806, 1)")
	checkTableMaxHandle(t, d, tbl, store, false, kv.IntHandle(9223372036854775806))

	tk.MustExec("insert into t values(9223372036854775807, 1)")
	checkTableMaxHandle(t, d, tbl, store, false, kv.IntHandle(9223372036854775807))

	tk.MustExec("insert into t values(10, 1)")
	tk.MustExec("insert into t values(102149142, 1)")
	checkTableMaxHandle(t, d, tbl, store, false, kv.IntHandle(9223372036854775807))

	tk.MustExec("create table t1(a bigint PRIMARY KEY, b int)")

	var builder strings.Builder
	_, _ = fmt.Fprintf(&builder, "insert into t1 values ")
	for i := 0; i < 1000; i++ {
		_, _ = fmt.Fprintf(&builder, "(%v, %v),", i, i)
	}
	sql := builder.String()
	tk.MustExec(sql[:len(sql)-1])

	is = dom.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	checkTableMaxHandle(t, d, tbl, store, false, kv.IntHandle(999))

	// Test PK is not handle
	tk.MustExec("create table t2(a varchar(255))")

	is = dom.InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	checkTableMaxHandle(t, d, tbl, store, true, nil)

	builder.Reset()
	_, _ = fmt.Fprintf(&builder, "insert into t2 values ")
	for i := 0; i < 1000; i++ {
		_, _ = fmt.Fprintf(&builder, "(%v),", i)
	}
	sql = builder.String()
	tk.MustExec(sql[:len(sql)-1])

	result := tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxHandle, emptyTable := getTableMaxHandle(t, d, tbl, store)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxHandle.IntValue())))
	require.False(t, emptyTable)

	tk.MustExec("insert into t2 values(100000)")
	result = tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxHandle, emptyTable = getTableMaxHandle(t, d, tbl, store)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxHandle.IntValue())))
	require.False(t, emptyTable)

	tk.MustExec(fmt.Sprintf("insert into t2 values(%v)", math.MaxInt64-1))
	result = tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxHandle, emptyTable = getTableMaxHandle(t, d, tbl, store)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxHandle.IntValue())))
	require.False(t, emptyTable)

	tk.MustExec(fmt.Sprintf("insert into t2 values(%v)", math.MaxInt64))
	result = tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxHandle, emptyTable = getTableMaxHandle(t, d, tbl, store)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxHandle.IntValue())))
	require.False(t, emptyTable)

	tk.MustExec("insert into t2 values(100)")
	result = tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxHandle, emptyTable = getTableMaxHandle(t, d, tbl, store)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxHandle.IntValue())))
	require.False(t, emptyTable)
}

func TestMultiRegionGetTableEndCommonHandle(t *testing.T) {
	var cluster testutils.Cluster
	store := testkit.CreateMockStore(t, mockstore.WithClusterInspector(func(c testutils.Cluster) {
		mockstore.BootstrapWithSingleStore(c)
		cluster = c
	}))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn

	tk.MustExec("create table t(a varchar(20), b int, c float, d bigint, primary key (a, b, c))")
	var builder strings.Builder
	_, _ = fmt.Fprintf(&builder, "insert into t values ")
	for i := 0; i < 1000; i++ {
		_, _ = fmt.Fprintf(&builder, "('%v', %v, %v, %v),", i, i, i, i)
	}
	sql := builder.String()
	tk.MustExec(sql[:len(sql)-1])

	// Get table ID for split.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	d := dom.DDL()

	// Split the table.
	tableStart := tablecodec.GenTableRecordPrefix(tbl.Meta().ID)
	cluster.SplitKeys(tableStart, tableStart.PrefixNext(), 100)
	checkTableMaxHandle(t, d, tbl, store, false, testutil.MustNewCommonHandle(t, "999", 999, 999))

	tk.MustExec("insert into t values('a', 1, 1, 1)")
	checkTableMaxHandle(t, d, tbl, store, false, testutil.MustNewCommonHandle(t, "a", 1, 1))

	tk.MustExec("insert into t values('0000', 1, 1, 1)")
	checkTableMaxHandle(t, d, tbl, store, false, testutil.MustNewCommonHandle(t, "a", 1, 1))
}

func TestGetTableEndCommonHandle(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn

	tk.MustExec("create table t(a varchar(15), b bigint, c int, primary key (a, b))")
	tk.MustExec("create table t1(a varchar(15), b bigint, c int, primary key (a(2), b))")

	is := dom.InfoSchema()
	d := dom.DDL()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	// test empty table
	checkTableMaxHandle(t, d, tbl, store, true, nil)

	tk.MustExec("insert into t values('abc', 1, 10)")
	checkTableMaxHandle(t, d, tbl, store, false, testutil.MustNewCommonHandle(t, "abc", 1))

	tk.MustExec("insert into t values('abchzzzzzzzz', 1, 10)")
	checkTableMaxHandle(t, d, tbl, store, false, testutil.MustNewCommonHandle(t, "abchzzzzzzzz", 1))
	tk.MustExec("insert into t values('a', 1, 10)")
	tk.MustExec("insert into t values('ab', 1, 10)")
	checkTableMaxHandle(t, d, tbl, store, false, testutil.MustNewCommonHandle(t, "abchzzzzzzzz", 1))

	// Test MaxTableRowID with prefixed primary key.
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	d = dom.DDL()
	checkTableMaxHandle(t, d, tbl, store, true, nil)
	tk.MustExec("insert into t1 values('abccccc', 1, 10)")
	checkTableMaxHandle(t, d, tbl, store, false, testutil.MustNewCommonHandle(t, "ab", 1))
	tk.MustExec("insert into t1 values('azzzz', 1, 10)")
	checkTableMaxHandle(t, d, tbl, store, false, testutil.MustNewCommonHandle(t, "az", 1))
}

func TestCreateClusteredIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("CREATE TABLE t1 (a int primary key, b int)")
	tk.MustExec("CREATE TABLE t2 (a varchar(255) primary key, b int)")
	tk.MustExec("CREATE TABLE t3 (a int, b int, c int, primary key (a, b))")
	tk.MustExec("CREATE TABLE t4 (a int, b int, c int)")
	is := domain.GetDomain(tk.Session()).InfoSchema()
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
	is = domain.GetDomain(tk.Session()).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t5"))
	require.NoError(t, err)
	require.False(t, tbl.Meta().IsCommonHandle)
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t6"))
	require.NoError(t, err)
	require.False(t, tbl.Meta().IsCommonHandle)

	tk.MustExec("CREATE TABLE t21 like t2")
	tk.MustExec("CREATE TABLE t31 like t3")
	is = domain.GetDomain(tk.Session()).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t21"))
	require.NoError(t, err)
	require.True(t, tbl.Meta().IsCommonHandle)
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t31"))
	require.NoError(t, err)
	require.True(t, tbl.Meta().IsCommonHandle)

	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	tk.MustExec("CREATE TABLE t7 (a varchar(255) primary key, b int)")
	is = domain.GetDomain(tk.Session()).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t7"))
	require.NoError(t, err)
	require.False(t, tbl.Meta().IsCommonHandle)
}
