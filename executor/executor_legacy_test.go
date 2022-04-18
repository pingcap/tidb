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

package executor_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/tikv/client-go/v2/testutils"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	*CustomParallelSuiteFlag = true

	TestingT(t)
}

var _ = Suite(&testSuite{&baseTestSuite{}})
var _ = Suite(&testSuite3{&baseTestSuite{}})

type testSuite struct{ *baseTestSuite }
type baseTestSuite struct {
	cluster testutils.Cluster
	store   kv.Storage
	domain  *domain.Domain
	*parser.Parser
}

func (s *baseTestSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
	store, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			s.cluster = c
		}),
	)
	c.Assert(err, IsNil)
	s.store = store
	session.SetSchemaLease(0)
	session.DisableStats4Test()
	d, err := session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	se.Close()
	d.SetStatsUpdating(true)
	s.domain = d
}

func (s *baseTestSuite) TearDownSuite(c *C) {
	s.domain.Close()
	c.Assert(s.store.Close(), IsNil)
}

func (s *testSuite) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
}

func (s *testSuite3) TestAdmin(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test")
	tk.MustExec("create table admin_test (c1 int, c2 int, c3 int default 1, index (c1))")
	tk.MustExec("insert admin_test (c1) values (1),(2),(NULL)")

	ctx := context.Background()
	// cancel DDL jobs test
	r, err := tk.Exec("admin cancel ddl jobs 1")
	c.Assert(err, IsNil, Commentf("err %v", err))
	req := r.NewChunk(nil)
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	row := req.GetRow(0)
	c.Assert(row.Len(), Equals, 2)
	c.Assert(row.GetString(0), Equals, "1")
	c.Assert(row.GetString(1), Matches, "*DDL Job:1 not found")

	// show ddl test;
	r, err = tk.Exec("admin show ddl")
	c.Assert(err, IsNil)
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	row = req.GetRow(0)
	c.Assert(row.Len(), Equals, 6)
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	ddlInfo, err := admin.GetDDLInfo(txn)
	c.Assert(err, IsNil)
	c.Assert(row.GetInt64(0), Equals, ddlInfo.SchemaVer)
	// TODO: Pass this test.
	// rowOwnerInfos := strings.Split(row.Data[1].GetString(), ",")
	// ownerInfos := strings.Split(ddlInfo.Owner.String(), ",")
	// c.Assert(rowOwnerInfos[0], Equals, ownerInfos[0])
	serverInfo, err := infosync.GetServerInfoByID(ctx, row.GetString(1))
	c.Assert(err, IsNil)
	c.Assert(row.GetString(2), Equals, serverInfo.IP+":"+
		strconv.FormatUint(uint64(serverInfo.Port), 10))
	c.Assert(row.GetString(3), Equals, "")
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	c.Assert(req.NumRows() == 0, IsTrue)
	err = txn.Rollback()
	c.Assert(err, IsNil)

	// show DDL jobs test
	r, err = tk.Exec("admin show ddl jobs")
	c.Assert(err, IsNil)
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	row = req.GetRow(0)
	c.Assert(row.Len(), Equals, 12)
	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	historyJobs, err := admin.GetHistoryDDLJobs(txn, admin.DefNumHistoryJobs)
	c.Assert(len(historyJobs), Greater, 1)
	c.Assert(len(row.GetString(1)), Greater, 0)
	c.Assert(err, IsNil)
	c.Assert(row.GetInt64(0), Equals, historyJobs[0].ID)
	c.Assert(err, IsNil)

	r, err = tk.Exec("admin show ddl jobs 20")
	c.Assert(err, IsNil)
	req = r.NewChunk(nil)
	err = r.Next(ctx, req)
	c.Assert(err, IsNil)
	row = req.GetRow(0)
	c.Assert(row.Len(), Equals, 12)
	c.Assert(row.GetInt64(0), Equals, historyJobs[0].ID)
	c.Assert(err, IsNil)

	// show DDL job queries test
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test2")
	tk.MustExec("create table admin_test2 (c1 int, c2 int, c3 int default 1, index (c1))")
	result := tk.MustQuery(`admin show ddl job queries 1, 1, 1`)
	result.Check(testkit.Rows())
	result = tk.MustQuery(`admin show ddl job queries 1, 2, 3, 4`)
	result.Check(testkit.Rows())
	historyJobs, err = admin.GetHistoryDDLJobs(txn, admin.DefNumHistoryJobs)
	result = tk.MustQuery(fmt.Sprintf("admin show ddl job queries %d", historyJobs[0].ID))
	result.Check(testkit.Rows(historyJobs[0].Query))
	c.Assert(err, IsNil)

	// check table test
	tk.MustExec("create table admin_test1 (c1 int, c2 int default 1, index (c1))")
	tk.MustExec("insert admin_test1 (c1) values (21),(22)")
	r, err = tk.Exec("admin check table admin_test, admin_test1")
	c.Assert(err, IsNil)
	c.Assert(r, IsNil)
	// error table name
	err = tk.ExecToErr("admin check table admin_test_error")
	c.Assert(err, NotNil)
	// different index values
	sctx := tk.Se.(sessionctx.Context)
	dom := domain.GetDomain(sctx)
	is := dom.InfoSchema()
	c.Assert(is, NotNil)
	tb, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("admin_test"))
	c.Assert(err, IsNil)
	c.Assert(tb.Indices(), HasLen, 1)
	_, err = tb.Indices()[0].Create(mock.NewContext(), txn, types.MakeDatums(int64(10)), kv.IntHandle(1), nil)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	errAdmin := tk.ExecToErr("admin check table admin_test")
	c.Assert(errAdmin, NotNil)

	if config.CheckTableBeforeDrop {
		err = tk.ExecToErr("drop table admin_test")
		c.Assert(err.Error(), Equals, errAdmin.Error())

		// Drop inconsistency index.
		tk.MustExec("alter table admin_test drop index c1")
		tk.MustExec("admin check table admin_test")
	}
	// checksum table test
	tk.MustExec("create table checksum_with_index (id int, count int, PRIMARY KEY(id), KEY(count))")
	tk.MustExec("create table checksum_without_index (id int, count int, PRIMARY KEY(id))")
	r, err = tk.Exec("admin checksum table checksum_with_index, checksum_without_index")
	c.Assert(err, IsNil)
	res := tk.ResultSetToResult(r, Commentf("admin checksum table"))
	// Mocktikv returns 1 for every table/index scan, then we will xor the checksums of a table.
	// For "checksum_with_index", we have two checksums, so the result will be 1^1 = 0.
	// For "checksum_without_index", we only have one checksum, so the result will be 1.
	res.Sort().Check(testkit.Rows("test checksum_with_index 0 2 2", "test checksum_without_index 1 1 1"))

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("CREATE TABLE t1 (c2 BOOL, PRIMARY KEY (c2));")
	tk.MustExec("INSERT INTO t1 SET c2 = '0';")
	tk.MustExec("ALTER TABLE t1 ADD COLUMN c3 DATETIME NULL DEFAULT '2668-02-03 17:19:31';")
	tk.MustExec("ALTER TABLE t1 ADD INDEX idx2 (c3);")
	tk.MustExec("ALTER TABLE t1 ADD COLUMN c4 bit(10) default 127;")
	tk.MustExec("ALTER TABLE t1 ADD INDEX idx3 (c4);")
	tk.MustExec("admin check table t1;")

	// Test admin show ddl jobs table name after table has been droped.
	tk.MustExec("drop table if exists t1;")
	re := tk.MustQuery("admin show ddl jobs 1")
	rows := re.Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][2], Equals, "t1")

	// Test for reverse scan get history ddl jobs when ddl history jobs queue has multiple regions.
	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	historyJobs, err = admin.GetHistoryDDLJobs(txn, 20)
	c.Assert(err, IsNil)

	// Split region for history ddl job queues.
	m := meta.NewMeta(txn)
	startKey := meta.DDLJobHistoryKey(m, 0)
	endKey := meta.DDLJobHistoryKey(m, historyJobs[0].ID)
	s.cluster.SplitKeys(startKey, endKey, int(historyJobs[0].ID/5))

	historyJobs2, err := admin.GetHistoryDDLJobs(txn, 20)
	c.Assert(err, IsNil)
	c.Assert(historyJobs, DeepEquals, historyJobs2)
}

func (s *testSuite3) TestForSelectScopeInUnion(c *C) {
	// A union B for update, the "for update" option belongs to union statement, so
	// it should works on both A and B.
	tk1 := testkit.NewTestKit(c, s.store)
	tk2 := testkit.NewTestKit(c, s.store)
	tk1.MustExec("use test")
	tk1.MustExec("drop table if exists t")
	tk1.MustExec("create table t(a int)")
	tk1.MustExec("insert into t values (1)")

	tk1.MustExec("begin")
	// 'For update' would act on the second select.
	tk1.MustQuery("select 1 as a union select a from t for update")

	tk2.MustExec("use test")
	tk2.MustExec("update t set a = a + 1")

	// As tk1 use select 'for update', it should detect conflict and fail.
	_, err := tk1.Exec("commit")
	c.Assert(err, NotNil)

	tk1.MustExec("begin")
	tk1.MustQuery("select 1 as a union select a from t limit 5 for update")
	tk1.MustQuery("select 1 as a union select a from t order by a for update")

	tk2.MustExec("update t set a = a + 1")

	_, err = tk1.Exec("commit")
	c.Assert(err, NotNil)
}

func (s *testSuite3) TestUnsignedDecimalOverflow(c *C) {
	tests := []struct {
		input  interface{}
		hasErr bool
		err    string
	}{{
		-1,
		true,
		"Out of range value for column",
	}, {
		"-1.1e-1",
		true,
		"Out of range value for column",
	}, {
		-1.1,
		true,
		"Out of range value for column",
	}, {
		-0,
		false,
		"",
	},
	}
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a decimal(10,2) unsigned)")
	for _, t := range tests {
		res, err := tk.Exec("insert into t values (?)", t.input)
		if res != nil {
			defer res.Close()
		}
		if t.hasErr {
			c.Assert(err, NotNil)
			c.Assert(strings.Contains(err.Error(), t.err), IsTrue)
		} else {
			c.Assert(err, IsNil)
		}
		if res != nil {
			c.Assert(res.Close(), IsNil)
		}
	}

	tk.MustExec("set sql_mode=''")
	tk.MustExec("delete from t")
	tk.MustExec("insert into t values (?)", -1)
	r := tk.MustQuery("select a from t limit 1")
	r.Check(testkit.Rows("0.00"))
}

func (s *testSuite3) TestIndexJoinTableDualPanic(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists a")
	tk.MustExec("create table a (f1 int, f2 varchar(32), primary key (f1))")
	tk.MustExec("insert into a (f1,f2) values (1,'a'), (2,'b'), (3,'c')")
	// TODO here: index join cause the data race of txn.
	tk.MustQuery("select /*+ inl_merge_join(a) */ a.* from a inner join (select 1 as k1,'k2-1' as k2) as k on a.f1=k.k1;").
		Check(testkit.Rows("1 a"))
}

func (s *testSuite3) TestSortLeftJoinWithNullColumnInRightChildPanic(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("create table t2(a int)")
	tk.MustExec("insert into t1(a) select 1;")
	tk.MustQuery("select b.n from t1 left join (select a as a, null as n from t2) b on b.a = t1.a order by t1.a").
		Check(testkit.Rows("<nil>"))
}

func (s *testSuite3) TestMaxOneRow(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`drop table if exists t2`)
	tk.MustExec(`create table t1(a double, b double);`)
	tk.MustExec(`create table t2(a double, b double);`)
	tk.MustExec(`insert into t1 values(1, 1), (2, 2), (3, 3);`)
	tk.MustExec(`insert into t2 values(0, 0);`)
	tk.MustExec(`set @@tidb_init_chunk_size=1;`)
	rs, err := tk.Exec(`select (select t1.a from t1 where t1.a > t2.a) as a from t2;`)
	c.Assert(err, IsNil)

	err = rs.Next(context.TODO(), rs.NewChunk(nil))
	c.Assert(err.Error(), Equals, "[executor:1242]Subquery returns more than 1 row")

	c.Assert(rs.Close(), IsNil)
}

func (s *testSuite3) TestRowID(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t`)
	tk.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	tk.MustExec(`create table t(a varchar(10), b varchar(10), c varchar(1), index idx(a, b, c));`)
	tk.MustExec(`insert into t values('a', 'b', 'c');`)
	tk.MustExec(`insert into t values('a', 'b', 'c');`)
	tk.MustQuery(`select b, _tidb_rowid from t use index(idx) where a = 'a';`).Check(testkit.Rows(
		`b 1`,
		`b 2`,
	))
	tk.MustExec(`begin;`)
	tk.MustExec(`select * from t for update`)
	tk.MustQuery(`select distinct b from t use index(idx) where a = 'a';`).Check(testkit.Rows(`b`))
	tk.MustExec(`commit;`)

	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a varchar(5) primary key)`)
	tk.MustExec(`insert into t values('a')`)
	tk.MustQuery("select *, _tidb_rowid from t use index(`primary`) where _tidb_rowid=1").Check(testkit.Rows("a 1"))
}

func (s *testSuite3) TestDoSubquery(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a int)`)
	_, err := tk.Exec(`do 1 in (select * from t)`)
	c.Assert(err, IsNil, Commentf("err %v", err))
	tk.MustExec(`insert into t values(1)`)
	r, err := tk.Exec(`do 1 in (select * from t)`)
	c.Assert(err, IsNil, Commentf("err %v", err))
	c.Assert(r, IsNil, Commentf("result of Do not empty"))
}

func (s *testSuite3) TestSubqueryTableAlias(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t`)

	tk.MustExec("set sql_mode = ''")
	tk.MustGetErrCode("select a, b from (select 1 a) ``, (select 2 b) ``;", mysql.ErrDerivedMustHaveAlias)
	tk.MustGetErrCode("select a, b from (select 1 a) `x`, (select 2 b) `x`;", mysql.ErrNonuniqTable)
	tk.MustGetErrCode("select a, b from (select 1 a), (select 2 b);", mysql.ErrDerivedMustHaveAlias)
	// ambiguous column name
	tk.MustGetErrCode("select a from (select 1 a) ``, (select 2 a) ``;", mysql.ErrDerivedMustHaveAlias)
	tk.MustGetErrCode("select a from (select 1 a) `x`, (select 2 a) `x`;", mysql.ErrNonuniqTable)
	tk.MustGetErrCode("select x.a from (select 1 a) `x`, (select 2 a) `x`;", mysql.ErrNonuniqTable)
	tk.MustGetErrCode("select a from (select 1 a), (select 2 a);", mysql.ErrDerivedMustHaveAlias)

	tk.MustExec("set sql_mode = 'oracle';")
	tk.MustQuery("select a, b from (select 1 a) ``, (select 2 b) ``;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select a, b from (select 1 a) `x`, (select 2 b) `x`;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select a, b from (select 1 a), (select 2 b);").Check(testkit.Rows("1 2"))
	// ambiguous column name
	tk.MustGetErrCode("select a from (select 1 a) ``, (select 2 a) ``;", mysql.ErrNonUniq)
	tk.MustGetErrCode("select a from (select 1 a) `x`, (select 2 a) `x`;", mysql.ErrNonUniq)
	tk.MustGetErrCode("select x.a from (select 1 a) `x`, (select 2 a) `x`;", mysql.ErrNonUniq)
	tk.MustGetErrCode("select a from (select 1 a), (select 2 a);", mysql.ErrNonUniq)
}

func (s *testSuite3) TestSelectHashPartitionTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists th`)
	tk.MustExec("set @@session.tidb_enable_table_partition = '1';")
	tk.MustExec(`create table th (a int, b int) partition by hash(a) partitions 3;`)
	defer tk.MustExec(`drop table if exists th`)
	tk.MustExec(`insert into th values (0,0),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8);`)
	tk.MustExec("insert into th values (-1,-1),(-2,-2),(-3,-3),(-4,-4),(-5,-5),(-6,-6),(-7,-7),(-8,-8);")
	tk.MustQuery("select b from th order by a").Check(testkit.Rows("-8", "-7", "-6", "-5", "-4", "-3", "-2", "-1", "0", "1", "2", "3", "4", "5", "6", "7", "8"))
	tk.MustQuery(" select * from th where a=-2;").Check(testkit.Rows("-2 -2"))
	tk.MustQuery(" select * from th where a=5;").Check(testkit.Rows("5 5"))
}

func (s *testSuite) TestSelectView(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table view_t (a int,b int)")
	tk.MustExec("insert into view_t values(1,2)")
	tk.MustExec("create definer='root'@'localhost' view view1 as select * from view_t")
	tk.MustExec("create definer='root'@'localhost' view view2(c,d) as select * from view_t")
	tk.MustExec("create definer='root'@'localhost' view view3(c,d) as select a,b from view_t")
	tk.MustExec("create definer='root'@'localhost' view view4 as select * from (select * from (select * from view_t) tb1) tb;")
	tk.MustQuery("select * from view1;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view2;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view3;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view4;").Check(testkit.Rows("1 2"))
	tk.MustExec("drop table view_t;")
	tk.MustExec("create table view_t(c int,d int)")
	err := tk.ExecToErr("select * from view1")
	c.Assert(err.Error(), Equals, "[planner:1356]View 'test.view1' references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them")
	err = tk.ExecToErr("select * from view2")
	c.Assert(err.Error(), Equals, "[planner:1356]View 'test.view2' references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them")
	err = tk.ExecToErr("select * from view3")
	c.Assert(err.Error(), Equals, plannercore.ErrViewInvalid.GenWithStackByArgs("test", "view3").Error())
	tk.MustExec("drop table view_t;")
	tk.MustExec("create table view_t(a int,b int,c int)")
	tk.MustExec("insert into view_t values(1,2,3)")
	tk.MustQuery("select * from view1;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view2;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view3;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view4;").Check(testkit.Rows("1 2"))
	tk.MustExec("alter table view_t drop column a")
	tk.MustExec("alter table view_t add column a int after b")
	tk.MustExec("update view_t set a=1;")
	tk.MustQuery("select * from view1;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view2;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view3;").Check(testkit.Rows("1 2"))
	tk.MustQuery("select * from view4;").Check(testkit.Rows("1 2"))
	tk.MustExec("drop table view_t;")
	tk.MustExec("drop view view1,view2,view3,view4;")

	tk.MustExec("set @@tidb_enable_window_function = 1")
	defer func() {
		tk.MustExec("set @@tidb_enable_window_function = 0")
	}()
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values (1,1),(1,2),(2,1),(2,2)")
	tk.MustExec("create definer='root'@'localhost' view v as select a, first_value(a) over(rows between 1 preceding and 1 following), last_value(a) over(rows between 1 preceding and 1 following) from t")
	result := tk.MustQuery("select * from v")
	result.Check(testkit.Rows("1 1 1", "1 1 2", "2 1 2", "2 2 2"))
	tk.MustExec("drop view v;")
}

type testSuite3 struct {
	*baseTestSuite
}

func (s *testSuite3) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show full tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		if tb[1] == "VIEW" {
			tk.MustExec(fmt.Sprintf("drop view %v", tableName))
		} else if tb[1] == "SEQUENCE" {
			tk.MustExec(fmt.Sprintf("drop sequence %v", tableName))
		} else {
			tk.MustExec(fmt.Sprintf("drop table %v", tableName))
		}
	}
}

func (s *testSuite) TestSummaryFailedUpdate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int as(-a))")
	tk.MustExec("insert into t(a) values(1), (3), (7)")
	sm := &mockSessionManager1{
		PS: make([]*util.ProcessInfo, 0),
	}
	tk.Se.SetSessionManager(sm)
	s.domain.ExpensiveQueryHandle().SetSessionManager(sm)
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.OOMAction = config.OOMActionCancel
	})
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	tk.MustExec("set @@tidb_mem_quota_query=1")
	err := tk.ExecToErr("update t set t.a = t.a - 1 where t.a in (select a from t where a < 4)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, "Out Of Memory Quota!.*")
	tk.MustExec("set @@tidb_mem_quota_query=1000000000")
	tk.MustQuery("select stmt_type from information_schema.statements_summary where digest_text = 'update `t` set `t` . `a` = `t` . `a` - ? where `t` . `a` in ( select `a` from `t` where `a` < ? )'").Check(testkit.Rows("Update"))
}
