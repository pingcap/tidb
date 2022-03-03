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
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/tikv/client-go/v2/testutils"
)

type testMaxTableRowIDContext struct {
	c   *C
	d   ddl.DDL
	tbl table.Table
}

func newTestMaxTableRowIDContext(c *C, d ddl.DDL, tbl table.Table) *testMaxTableRowIDContext {
	return &testMaxTableRowIDContext{
		c:   c,
		d:   d,
		tbl: tbl,
	}
}

func getMaxTableHandle(ctx *testMaxTableRowIDContext, store kv.Storage) (kv.Handle, bool) {
	c := ctx.c
	d := ctx.d
	tbl := ctx.tbl
	curVer, err := store.CurrentVersion(kv.GlobalTxnScope)
	c.Assert(err, IsNil)
	maxHandle, emptyTable, err := d.GetTableMaxHandle(curVer.Ver, tbl.(table.PhysicalTable))
	c.Assert(err, IsNil)
	return maxHandle, emptyTable
}

func checkGetMaxTableRowID(ctx *testMaxTableRowIDContext, store kv.Storage, expectEmpty bool, expectMaxHandle kv.Handle) {
	c := ctx.c
	maxHandle, emptyTable := getMaxTableHandle(ctx, store)
	c.Assert(emptyTable, Equals, expectEmpty)
	c.Assert(maxHandle, testutil.HandleEquals, expectMaxHandle)
}

var _ = Suite(&testIntegrationSuite7{&testIntegrationSuite{}})

type testIntegrationSuite7 struct{ *testIntegrationSuite }

type testIntegrationSuite struct {
	lease   time.Duration
	cluster testutils.Cluster
	store   kv.Storage
	dom     *domain.Domain
	ctx     sessionctx.Context
}

func tearDownIntegrationSuiteTest(s *testIntegrationSuite, c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
}

func (s *testIntegrationSuite) SetUpSuite(c *C) {
	var err error
	s.lease = 50 * time.Millisecond
	ddl.SetWaitTimeWhenErrorOccurred(0)

	s.store, err = mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			s.cluster = c
		}),
	)
	c.Assert(err, IsNil)
	session.SetSchemaLease(s.lease)
	session.DisableStats4Test()
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)

	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	s.ctx = se.(sessionctx.Context)
	_, err = se.Execute(context.Background(), "create database test_db")
	c.Assert(err, IsNil)
}

func (s *testIntegrationSuite) TearDownSuite(c *C) {
	s.dom.Close()
	c.Assert(s.store.Close(), IsNil)
}

func (s *testIntegrationSuite7) TestPrimaryKey(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists test_primary_key;")
	tk.MustExec("create database test_primary_key;")
	tk.MustExec("use test_primary_key;")
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly

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

func (s *testIntegrationSuite7) TestDropAutoIncrementIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int(11) not null auto_increment key, b int(11), c bigint, unique key (a, b, c))")
	tk.MustExec("alter table t1 drop index a")
}

func (s *testIntegrationSuite7) TestMultiRegionGetTableEndHandle(c *C) {
	tk := testkit.NewTestKit(c, s.store)
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
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test_get_endhandle"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblID := tbl.Meta().ID

	d := s.dom.DDL()
	testCtx := newTestMaxTableRowIDContext(c, d, tbl)

	// Split the table.
	tableStart := tablecodec.GenTableRecordPrefix(tblID)
	s.cluster.SplitKeys(tableStart, tableStart.PrefixNext(), 100)

	maxHandle, emptyTable := getMaxTableHandle(testCtx, s.store)
	c.Assert(emptyTable, IsFalse)
	c.Assert(maxHandle, Equals, kv.IntHandle(999))

	tk.MustExec("insert into t values(10000, 1000)")
	maxHandle, emptyTable = getMaxTableHandle(testCtx, s.store)
	c.Assert(emptyTable, IsFalse)
	c.Assert(maxHandle, Equals, kv.IntHandle(10000))

	tk.MustExec("insert into t values(-1, 1000)")
	maxHandle, emptyTable = getMaxTableHandle(testCtx, s.store)
	c.Assert(emptyTable, IsFalse)
	c.Assert(maxHandle, Equals, kv.IntHandle(10000))
}

func (s *testIntegrationSuite7) TestGetTableEndHandle(c *C) {
	// TestGetTableEndHandle test ddl.GetTableMaxHandle method, which will return the max row id of the table.
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists test_get_endhandle")
	tk.MustExec("create database test_get_endhandle")
	tk.MustExec("use test_get_endhandle")
	// Test PK is handle.
	tk.MustExec("create table t(a bigint PRIMARY KEY, b int)")

	is := s.dom.InfoSchema()
	d := s.dom.DDL()
	tbl, err := is.TableByName(model.NewCIStr("test_get_endhandle"), model.NewCIStr("t"))
	c.Assert(err, IsNil)

	testCtx := newTestMaxTableRowIDContext(c, d, tbl)
	// test empty table
	checkGetMaxTableRowID(testCtx, s.store, true, nil)

	tk.MustExec("insert into t values(-1, 1)")
	checkGetMaxTableRowID(testCtx, s.store, false, kv.IntHandle(-1))

	tk.MustExec("insert into t values(9223372036854775806, 1)")
	checkGetMaxTableRowID(testCtx, s.store, false, kv.IntHandle(9223372036854775806))

	tk.MustExec("insert into t values(9223372036854775807, 1)")
	checkGetMaxTableRowID(testCtx, s.store, false, kv.IntHandle(9223372036854775807))

	tk.MustExec("insert into t values(10, 1)")
	tk.MustExec("insert into t values(102149142, 1)")
	checkGetMaxTableRowID(testCtx, s.store, false, kv.IntHandle(9223372036854775807))

	tk.MustExec("create table t1(a bigint PRIMARY KEY, b int)")

	var builder strings.Builder
	fmt.Fprintf(&builder, "insert into t1 values ")
	for i := 0; i < 1000; i++ {
		fmt.Fprintf(&builder, "(%v, %v),", i, i)
	}
	sql := builder.String()
	tk.MustExec(sql[:len(sql)-1])

	is = s.dom.InfoSchema()
	testCtx.tbl, err = is.TableByName(model.NewCIStr("test_get_endhandle"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	checkGetMaxTableRowID(testCtx, s.store, false, kv.IntHandle(999))

	// Test PK is not handle
	tk.MustExec("create table t2(a varchar(255))")

	is = s.dom.InfoSchema()
	testCtx.tbl, err = is.TableByName(model.NewCIStr("test_get_endhandle"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)
	checkGetMaxTableRowID(testCtx, s.store, true, nil)

	builder.Reset()
	fmt.Fprintf(&builder, "insert into t2 values ")
	for i := 0; i < 1000; i++ {
		fmt.Fprintf(&builder, "(%v),", i)
	}
	sql = builder.String()
	tk.MustExec(sql[:len(sql)-1])

	result := tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxHandle, emptyTable := getMaxTableHandle(testCtx, s.store)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxHandle.IntValue())))
	c.Assert(emptyTable, IsFalse)

	tk.MustExec("insert into t2 values(100000)")
	result = tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxHandle, emptyTable = getMaxTableHandle(testCtx, s.store)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxHandle.IntValue())))
	c.Assert(emptyTable, IsFalse)

	tk.MustExec(fmt.Sprintf("insert into t2 values(%v)", math.MaxInt64-1))
	result = tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxHandle, emptyTable = getMaxTableHandle(testCtx, s.store)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxHandle.IntValue())))
	c.Assert(emptyTable, IsFalse)

	tk.MustExec(fmt.Sprintf("insert into t2 values(%v)", math.MaxInt64))
	result = tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxHandle, emptyTable = getMaxTableHandle(testCtx, s.store)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxHandle.IntValue())))
	c.Assert(emptyTable, IsFalse)

	tk.MustExec("insert into t2 values(100)")
	result = tk.MustQuery("select MAX(_tidb_rowid) from t2")
	maxHandle, emptyTable = getMaxTableHandle(testCtx, s.store)
	result.Check(testkit.Rows(fmt.Sprintf("%v", maxHandle.IntValue())))
	c.Assert(emptyTable, IsFalse)
}

func (s *testIntegrationSuite7) TestMultiRegionGetTableEndCommonHandle(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists test_get_endhandle")
	tk.MustExec("create database test_get_endhandle")
	tk.MustExec("use test_get_endhandle")
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn

	tk.MustExec("create table t(a varchar(20), b int, c float, d bigint, primary key (a, b, c))")
	var builder strings.Builder
	fmt.Fprintf(&builder, "insert into t values ")
	for i := 0; i < 1000; i++ {
		fmt.Fprintf(&builder, "('%v', %v, %v, %v),", i, i, i, i)
	}
	sql := builder.String()
	tk.MustExec(sql[:len(sql)-1])

	// Get table ID for split.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test_get_endhandle"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblID := tbl.Meta().ID

	d := s.dom.DDL()
	testCtx := newTestMaxTableRowIDContext(c, d, tbl)

	// Split the table.
	tableStart := tablecodec.GenTableRecordPrefix(tblID)
	s.cluster.SplitKeys(tableStart, tableStart.PrefixNext(), 100)

	maxHandle, emptyTable := getMaxTableHandle(testCtx, s.store)
	c.Assert(emptyTable, IsFalse)
	c.Assert(maxHandle, testutil.HandleEquals, testutil.MustNewCommonHandle(c, "999", 999, 999))

	tk.MustExec("insert into t values('a', 1, 1, 1)")
	maxHandle, emptyTable = getMaxTableHandle(testCtx, s.store)
	c.Assert(emptyTable, IsFalse)
	c.Assert(maxHandle, testutil.HandleEquals, testutil.MustNewCommonHandle(c, "a", 1, 1))

	tk.MustExec("insert into t values('0000', 1, 1, 1)")
	maxHandle, emptyTable = getMaxTableHandle(testCtx, s.store)
	c.Assert(emptyTable, IsFalse)
	c.Assert(maxHandle, testutil.HandleEquals, testutil.MustNewCommonHandle(c, "a", 1, 1))
}

func (s *testIntegrationSuite7) TestGetTableEndCommonHandle(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists test_get_endhandle")
	tk.MustExec("create database test_get_endhandle")
	tk.MustExec("use test_get_endhandle")
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn

	tk.MustExec("create table t(a varchar(15), b bigint, c int, primary key (a, b))")
	tk.MustExec("create table t1(a varchar(15), b bigint, c int, primary key (a(2), b))")

	is := s.dom.InfoSchema()
	d := s.dom.DDL()
	tbl, err := is.TableByName(model.NewCIStr("test_get_endhandle"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	testCtx := newTestMaxTableRowIDContext(c, d, tbl)

	// test empty table
	checkGetMaxTableRowID(testCtx, s.store, true, nil)
	tk.MustExec("insert into t values('abc', 1, 10)")
	expectedHandle := testutil.MustNewCommonHandle(c, "abc", 1)
	checkGetMaxTableRowID(testCtx, s.store, false, expectedHandle)
	tk.MustExec("insert into t values('abchzzzzzzzz', 1, 10)")
	expectedHandle = testutil.MustNewCommonHandle(c, "abchzzzzzzzz", 1)
	checkGetMaxTableRowID(testCtx, s.store, false, expectedHandle)
	tk.MustExec("insert into t values('a', 1, 10)")
	tk.MustExec("insert into t values('ab', 1, 10)")
	checkGetMaxTableRowID(testCtx, s.store, false, expectedHandle)

	// Test MaxTableRowID with prefixed primary key.
	tbl, err = is.TableByName(model.NewCIStr("test_get_endhandle"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	d = s.dom.DDL()
	testCtx = newTestMaxTableRowIDContext(c, d, tbl)
	checkGetMaxTableRowID(testCtx, s.store, true, nil)
	tk.MustExec("insert into t1 values('abccccc', 1, 10)")
	expectedHandle = testutil.MustNewCommonHandle(c, "ab", 1)
	checkGetMaxTableRowID(testCtx, s.store, false, expectedHandle)
	tk.MustExec("insert into t1 values('azzzz', 1, 10)")
	expectedHandle = testutil.MustNewCommonHandle(c, "az", 1)
	checkGetMaxTableRowID(testCtx, s.store, false, expectedHandle)
}

func (s *testIntegrationSuite7) TestAutoRandomChangeFromAutoInc(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("set @@tidb_allow_remove_auto_inc = 1;")

	// Basic usages.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a bigint auto_increment primary key);")
	tk.MustExec("insert into t values (), (), ();")
	tk.MustExec("alter table t modify column a bigint auto_random(3);")
	tk.MustExec("insert into t values (), (), ();")
	rows := tk.MustQuery("show table t next_row_id;").Rows()
	c.Assert(len(rows), Equals, 1, Commentf("query result: %v", rows))
	c.Assert(len(rows[0]), Equals, 5, Commentf("query result: %v", rows))
	c.Assert(rows[0][4], Equals, "AUTO_RANDOM")

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

func (s *testIntegrationSuite7) TestAutoRandomExchangePartition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
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

func (s *testIntegrationSuite7) TestAutoRandomIncBitsIncrementAndOffset(c *C) {
	tk := testkit.NewTestKit(c, s.store)
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
		tk.Se.GetSessionVars().AutoIncrementIncrement = tc.increment
		tk.Se.GetSessionVars().AutoIncrementOffset = tc.offset
		for range tc.results {
			insertTable()
		}
		assertIncBitsValues(tc.results...)
	}
}

func (s *testIntegrationSuite7) TestInvisibleIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)

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
	c.Check(len(res.Rows()), Equals, 1)
}

func (s *testIntegrationSuite7) TestCreateClusteredIndex(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("CREATE TABLE t1 (a int primary key, b int)")
	tk.MustExec("CREATE TABLE t2 (a varchar(255) primary key, b int)")
	tk.MustExec("CREATE TABLE t3 (a int, b int, c int, primary key (a, b))")
	tk.MustExec("CREATE TABLE t4 (a int, b int, c int)")
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().PKIsHandle, IsTrue)
	c.Assert(tbl.Meta().IsCommonHandle, IsFalse)
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().IsCommonHandle, IsTrue)
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t3"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().IsCommonHandle, IsTrue)
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t4"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().IsCommonHandle, IsFalse)

	tk.MustExec("CREATE TABLE t5 (a varchar(255) primary key nonclustered, b int)")
	tk.MustExec("CREATE TABLE t6 (a int, b int, c int, primary key (a, b) nonclustered)")
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t5"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().IsCommonHandle, IsFalse)
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t6"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().IsCommonHandle, IsFalse)

	tk.MustExec("CREATE TABLE t21 like t2")
	tk.MustExec("CREATE TABLE t31 like t3")
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t21"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().IsCommonHandle, IsTrue)
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t31"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().IsCommonHandle, IsTrue)

	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	tk.MustExec("CREATE TABLE t7 (a varchar(255) primary key, b int)")
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t7"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Meta().IsCommonHandle, IsFalse)
}
