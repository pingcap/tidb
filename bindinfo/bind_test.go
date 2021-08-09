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
// See the License for the specific language governing permissions and
// limitations under the License.

package bindinfo_test

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/metrics"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/session/txninfo"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	utilparser "github.com/pingcap/tidb/util/parser"
	"github.com/pingcap/tidb/util/stmtsummary"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	dto "github.com/prometheus/client_model/go"
	"github.com/tikv/client-go/v2/testutils"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	logLevel := os.Getenv("log_level")
	err := logutil.InitLogger(logutil.NewLogConfig(logLevel, logutil.DefaultLogFormat, "", logutil.EmptyFileLogConfig, false))
	if err != nil {
		t.Fatal(err)
	}
	autoid.SetStep(5000)
	TestingT(t)
}

var _ = Suite(&testSuite{})
var _ = SerialSuites(&testSerialSuite{})

type testSuite struct {
	cluster testutils.Cluster
	store   kv.Storage
	domain  *domain.Domain
	*parser.Parser
}

type mockSessionManager struct {
	PS []*util.ProcessInfo
}

func (msm *mockSessionManager) ShowTxnList() []*txninfo.TxnInfo {
	panic("unimplemented!")
}

func (msm *mockSessionManager) ShowProcessList() map[uint64]*util.ProcessInfo {
	ret := make(map[uint64]*util.ProcessInfo)
	for _, item := range msm.PS {
		ret[item.ID] = item
	}
	return ret
}

func (msm *mockSessionManager) GetProcessInfo(id uint64) (*util.ProcessInfo, bool) {
	for _, item := range msm.PS {
		if item.ID == id {
			return item, true
		}
	}
	return &util.ProcessInfo{}, false
}

func (msm *mockSessionManager) Kill(cid uint64, query bool) {
}

func (msm *mockSessionManager) KillAllConnections() {
}

func (msm *mockSessionManager) UpdateTLSConfig(cfg *tls.Config) {
}

func (msm *mockSessionManager) ServerID() uint64 {
	return 1
}

var mockTikv = flag.Bool("mockTikv", true, "use mock tikv store in bind test")

func (s *testSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.Parser = parser.New()
	flag.Lookup("mockTikv")
	useMockTikv := *mockTikv
	if useMockTikv {
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
	}
	bindinfo.Lease = 0
	d, err := session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	d.SetStatsUpdating(true)
	s.domain = d
}

func (s *testSuite) TearDownSuite(c *C) {
	s.domain.Close()
	s.store.Close()
	testleak.AfterTest(c)()
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

func (s *testSuite) cleanBindingEnv(tk *testkit.TestKit) {
	tk.MustExec("delete from mysql.bind_info where source != 'builtin'")
	s.domain.BindHandle().Clear()
}

type testSerialSuite struct {
	cluster testutils.Cluster
	store   kv.Storage
	domain  *domain.Domain
}

func (s *testSerialSuite) SetUpSuite(c *C) {
	flag.Lookup("mockTikv")
	useMockTikv := *mockTikv
	if useMockTikv {
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
	}
	bindinfo.Lease = 0
	d, err := session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	d.SetStatsUpdating(true)
	s.domain = d
}

func (s *testSerialSuite) TearDownSuite(c *C) {
	s.domain.Close()
	s.store.Close()
}

func (s *testSerialSuite) cleanBindingEnv(tk *testkit.TestKit) {
	tk.MustExec("delete from mysql.bind_info where source != 'builtin'")
	s.domain.BindHandle().Clear()
}

func normalizeWithDefaultDB(c *C, sql, db string) (string, string) {
	testParser := parser.New()
	stmt, err := testParser.ParseOneStmt(sql, "", "")
	c.Assert(err, IsNil)
	normalized, digest := parser.NormalizeDigest(utilparser.RestoreWithDefaultDB(stmt, "test", ""))
	return normalized, digest.String()
}

func (s *testSuite) TestBindParse(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("create table t(i int)")
	tk.MustExec("create index index_t on t(i)")

	originSQL := "select * from `test` . `t`"
	bindSQL := "select * from `test` . `t` use index(index_t)"
	defaultDb := "test"
	status := "using"
	charset := "utf8mb4"
	collation := "utf8mb4_bin"
	source := bindinfo.Manual
	sql := fmt.Sprintf(`INSERT INTO mysql.bind_info(original_sql,bind_sql,default_db,status,create_time,update_time,charset,collation,source) VALUES ('%s', '%s', '%s', '%s', NOW(), NOW(),'%s', '%s', '%s')`,
		originSQL, bindSQL, defaultDb, status, charset, collation, source)
	tk.MustExec(sql)
	bindHandle := bindinfo.NewBindHandle(tk.Se)
	err := bindHandle.Update(true)
	c.Check(err, IsNil)
	c.Check(bindHandle.Size(), Equals, 1)

	sql, hash := parser.NormalizeDigest("select * from test . t")
	bindData := bindHandle.GetBindRecord(hash.String(), sql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalSQL, Equals, "select * from `test` . `t`")
	bind := bindData.Bindings[0]
	c.Check(bind.BindSQL, Equals, "select * from `test` . `t` use index(index_t)")
	c.Check(bindData.Db, Equals, "test")
	c.Check(bind.Status, Equals, "using")
	c.Check(bind.Charset, Equals, "utf8mb4")
	c.Check(bind.Collation, Equals, "utf8mb4_bin")
	c.Check(bind.CreateTime, NotNil)
	c.Check(bind.UpdateTime, NotNil)
	dur, err := bind.SinceUpdateTime()
	c.Assert(err, IsNil)
	c.Assert(int64(dur), GreaterEqual, int64(0))

	// Test fields with quotes or slashes.
	sql = `CREATE GLOBAL BINDING FOR  select * from t where i BETWEEN "a" and "b" USING select * from t use index(index_t) where i BETWEEN "a\nb\rc\td\0e" and 'x'`
	tk.MustExec(sql)
	tk.MustExec(`DROP global binding for select * from t use index(idx) where i BETWEEN "a\nb\rc\td\0e" and "x"`)

	// Test SetOprStmt.
	tk.MustExec(`create binding for select * from t union all select * from t using select * from t use index(index_t) union all select * from t use index()`)
	tk.MustExec(`drop binding for select * from t union all select * from t using select * from t use index(index_t) union all select * from t use index()`)
	tk.MustExec(`create binding for select * from t INTERSECT select * from t using select * from t use index(index_t) INTERSECT select * from t use index()`)
	tk.MustExec(`drop binding for select * from t INTERSECT select * from t using select * from t use index(index_t) INTERSECT select * from t use index()`)
	tk.MustExec(`create binding for select * from t EXCEPT select * from t using select * from t use index(index_t) EXCEPT select * from t use index()`)
	tk.MustExec(`drop binding for select * from t EXCEPT select * from t using select * from t use index(index_t) EXCEPT select * from t use index()`)
	tk.MustExec(`create binding for (select * from t) union all (select * from t) using (select * from t use index(index_t)) union all (select * from t use index())`)
	tk.MustExec(`drop binding for (select * from t) union all (select * from t) using (select * from t use index(index_t)) union all (select * from t use index())`)

	// Test Update / Delete.
	tk.MustExec("create table t1(a int, b int, c int, key(b), key(c))")
	tk.MustExec("create table t2(a int, b int, c int, key(b), key(c))")
	tk.MustExec("create binding for delete from t1 where b = 1 and c > 1 using delete /*+ use_index(t1, c) */ from t1 where b = 1 and c > 1")
	tk.MustExec("drop binding for delete from t1 where b = 1 and c > 1 using delete /*+ use_index(t1, c) */ from t1 where b = 1 and c > 1")
	tk.MustExec("create binding for delete t1, t2 from t1 inner join t2 on t1.b = t2.b where t1.c = 1 using delete /*+ hash_join(t1, t2), use_index(t1, c) */ t1, t2 from t1 inner join t2 on t1.b = t2.b where t1.c = 1")
	tk.MustExec("drop binding for delete t1, t2 from t1 inner join t2 on t1.b = t2.b where t1.c = 1 using delete /*+ hash_join(t1, t2), use_index(t1, c) */ t1, t2 from t1 inner join t2 on t1.b = t2.b where t1.c = 1")
	tk.MustExec("create binding for update t1 set a = 1 where b = 1 and c > 1 using update /*+ use_index(t1, c) */ t1 set a = 1 where b = 1 and c > 1")
	tk.MustExec("drop binding for update t1 set a = 1 where b = 1 and c > 1 using update /*+ use_index(t1, c) */ t1 set a = 1 where b = 1 and c > 1")
	tk.MustExec("create binding for update t1, t2 set t1.a = 1 where t1.b = t2.b using update /*+ inl_join(t1) */ t1, t2 set t1.a = 1 where t1.b = t2.b")
	tk.MustExec("drop binding for update t1, t2 set t1.a = 1 where t1.b = t2.b using update /*+ inl_join(t1) */ t1, t2 set t1.a = 1 where t1.b = t2.b")
	// Test Insert / Replace.
	tk.MustExec("create binding for insert into t1 select * from t2 where t2.b = 1 and t2.c > 1 using insert into t1 select /*+ use_index(t2,c) */ * from t2 where t2.b = 1 and t2.c > 1")
	tk.MustExec("drop binding for insert into t1 select * from t2 where t2.b = 1 and t2.c > 1 using insert into t1 select /*+ use_index(t2,c) */ * from t2 where t2.b = 1 and t2.c > 1")
	tk.MustExec("create binding for replace into t1 select * from t2 where t2.b = 1 and t2.c > 1 using replace into t1 select /*+ use_index(t2,c) */ * from t2 where t2.b = 1 and t2.c > 1")
	tk.MustExec("drop binding for replace into t1 select * from t2 where t2.b = 1 and t2.c > 1 using replace into t1 select /*+ use_index(t2,c) */ * from t2 where t2.b = 1 and t2.c > 1")
	err = tk.ExecToErr("create binding for insert into t1 values(1,1,1) using insert into t1 values(1,1,1)")
	c.Assert(err.Error(), Equals, "create binding only supports INSERT / REPLACE INTO SELECT")
	err = tk.ExecToErr("create binding for replace into t1 values(1,1,1) using replace into t1 values(1,1,1)")
	c.Assert(err.Error(), Equals, "create binding only supports INSERT / REPLACE INTO SELECT")

	// Test errors.
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec("create table t1(i int, s varchar(20))")
	_, err = tk.Exec("create global binding for select * from t using select * from t1 use index for join(index_t)")
	c.Assert(err, NotNil, Commentf("err %v", err))
}

var testSQLs = []struct {
	createSQL   string
	overlaySQL  string
	querySQL    string
	originSQL   string
	bindSQL     string
	dropSQL     string
	memoryUsage float64
}{
	{
		createSQL:   "binding for select * from t where i>100 using select * from t use index(index_t) where i>100",
		overlaySQL:  "binding for select * from t where i>99 using select * from t use index(index_t) where i>99",
		querySQL:    "select * from t where i          >      30.0",
		originSQL:   "select * from `test` . `t` where `i` > ?",
		bindSQL:     "SELECT * FROM `test`.`t` USE INDEX (`index_t`) WHERE `i` > 99",
		dropSQL:     "binding for select * from t where i>100",
		memoryUsage: float64(126),
	},
	{
		createSQL:   "binding for select * from t union all select * from t using select * from t use index(index_t) union all select * from t use index()",
		overlaySQL:  "",
		querySQL:    "select * from t union all         select * from t",
		originSQL:   "select * from `test` . `t` union all select * from `test` . `t`",
		bindSQL:     "SELECT * FROM `test`.`t` USE INDEX (`index_t`) UNION ALL SELECT * FROM `test`.`t` USE INDEX ()",
		dropSQL:     "binding for select * from t union all select * from t",
		memoryUsage: float64(182),
	},
	{
		createSQL:   "binding for (select * from t) union all (select * from t) using (select * from t use index(index_t)) union all (select * from t use index())",
		overlaySQL:  "",
		querySQL:    "(select * from t) union all         (select * from t)",
		originSQL:   "( select * from `test` . `t` ) union all ( select * from `test` . `t` )",
		bindSQL:     "(SELECT * FROM `test`.`t` USE INDEX (`index_t`)) UNION ALL (SELECT * FROM `test`.`t` USE INDEX ())",
		dropSQL:     "binding for (select * from t) union all (select * from t)",
		memoryUsage: float64(194),
	},
	{
		createSQL:   "binding for select * from t intersect select * from t using select * from t use index(index_t) intersect select * from t use index()",
		overlaySQL:  "",
		querySQL:    "select * from t intersect         select * from t",
		originSQL:   "select * from `test` . `t` intersect select * from `test` . `t`",
		bindSQL:     "SELECT * FROM `test`.`t` USE INDEX (`index_t`) INTERSECT SELECT * FROM `test`.`t` USE INDEX ()",
		dropSQL:     "binding for select * from t intersect select * from t",
		memoryUsage: float64(182),
	},
	{
		createSQL:   "binding for select * from t except select * from t using select * from t use index(index_t) except select * from t use index()",
		overlaySQL:  "",
		querySQL:    "select * from t except         select * from t",
		originSQL:   "select * from `test` . `t` except select * from `test` . `t`",
		bindSQL:     "SELECT * FROM `test`.`t` USE INDEX (`index_t`) EXCEPT SELECT * FROM `test`.`t` USE INDEX ()",
		dropSQL:     "binding for select * from t except select * from t",
		memoryUsage: float64(176),
	},
	{
		createSQL:   "binding for select * from t using select /*+ use_index(t,index_t)*/ * from t",
		overlaySQL:  "",
		querySQL:    "select * from t ",
		originSQL:   "select * from `test` . `t`",
		bindSQL:     "SELECT /*+ use_index(`t` `index_t`)*/ * FROM `test`.`t`",
		dropSQL:     "binding for select * from t",
		memoryUsage: float64(106),
	},
	{
		createSQL:   "binding for delete from t where i = 1 using delete /*+ use_index(t,index_t) */ from t where i = 1",
		overlaySQL:  "",
		querySQL:    "delete    from t where   i = 2",
		originSQL:   "delete from `test` . `t` where `i` = ?",
		bindSQL:     "DELETE /*+ use_index(`t` `index_t`)*/ FROM `test`.`t` WHERE `i` = 1",
		dropSQL:     "binding for delete from t where i = 1",
		memoryUsage: float64(130),
	},
	{
		createSQL:   "binding for delete t, t1 from t inner join t1 on t.s = t1.s where t.i = 1 using delete /*+ use_index(t,index_t), hash_join(t,t1) */ t, t1 from t inner join t1 on t.s = t1.s where t.i = 1",
		overlaySQL:  "",
		querySQL:    "delete t,   t1 from t inner join t1 on t.s = t1.s  where   t.i = 2",
		originSQL:   "delete `test` . `t` , `test` . `t1` from `test` . `t` join `test` . `t1` on `t` . `s` = `t1` . `s` where `t` . `i` = ?",
		bindSQL:     "DELETE /*+ use_index(`t` `index_t`) hash_join(`t`, `t1`)*/ `test`.`t`,`test`.`t1` FROM `test`.`t` JOIN `test`.`t1` ON `t`.`s` = `t1`.`s` WHERE `t`.`i` = 1",
		dropSQL:     "binding for delete t, t1 from t inner join t1 on t.s = t1.s where t.i = 1",
		memoryUsage: float64(297),
	},
	{
		createSQL:   "binding for update t set s = 'a' where i = 1 using update /*+ use_index(t,index_t) */ t set s = 'a' where i = 1",
		overlaySQL:  "",
		querySQL:    "update   t  set s='b' where i=2",
		originSQL:   "update `test` . `t` set `s` = ? where `i` = ?",
		bindSQL:     "UPDATE /*+ use_index(`t` `index_t`)*/ `test`.`t` SET `s`='a' WHERE `i` = 1",
		dropSQL:     "binding for update t set s = 'a' where i = 1",
		memoryUsage: float64(144),
	},
	{
		createSQL:   "binding for update t, t1 set t.s = 'a' where t.i = t1.i using update /*+ inl_join(t1) */ t, t1 set t.s = 'a' where t.i = t1.i",
		overlaySQL:  "",
		querySQL:    "update   t  , t1 set t.s='b' where t.i=t1.i",
		originSQL:   "update ( `test` . `t` ) join `test` . `t1` set `t` . `s` = ? where `t` . `i` = `t1` . `i`",
		bindSQL:     "UPDATE /*+ inl_join(`t1`)*/ (`test`.`t`) JOIN `test`.`t1` SET `t`.`s`='a' WHERE `t`.`i` = `t1`.`i`",
		dropSQL:     "binding for update t, t1 set t.s = 'a' where t.i = t1.i",
		memoryUsage: float64(212),
	},
	{
		createSQL:   "binding for insert into t1 select * from t where t.i = 1 using insert into t1 select /*+ use_index(t,index_t) */ * from t where t.i = 1",
		overlaySQL:  "",
		querySQL:    "insert  into   t1 select * from t where t.i  = 2",
		originSQL:   "insert into `test` . `t1` select * from `test` . `t` where `t` . `i` = ?",
		bindSQL:     "INSERT INTO `test`.`t1` SELECT /*+ use_index(`t` `index_t`)*/ * FROM `test`.`t` WHERE `t`.`i` = 1",
		dropSQL:     "binding for insert into t1 select * from t where t.i = 1",
		memoryUsage: float64(194),
	},
	{
		createSQL:   "binding for replace into t1 select * from t where t.i = 1 using replace into t1 select /*+ use_index(t,index_t) */ * from t where t.i = 1",
		overlaySQL:  "",
		querySQL:    "replace  into   t1 select * from t where t.i  = 2",
		originSQL:   "replace into `test` . `t1` select * from `test` . `t` where `t` . `i` = ?",
		bindSQL:     "REPLACE INTO `test`.`t1` SELECT /*+ use_index(`t` `index_t`)*/ * FROM `test`.`t` WHERE `t`.`i` = 1",
		dropSQL:     "binding for replace into t1 select * from t where t.i = 1",
		memoryUsage: float64(196),
	},
}

func (s *testSuite) TestGlobalBinding(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	for _, testSQL := range testSQLs {
		s.cleanBindingEnv(tk)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("drop table if exists t1")
		tk.MustExec("create table t(i int, s varchar(20))")
		tk.MustExec("create table t1(i int, s varchar(20))")
		tk.MustExec("create index index_t on t(i,s)")

		metrics.BindTotalGauge.Reset()
		metrics.BindMemoryUsage.Reset()

		_, err := tk.Exec("create global " + testSQL.createSQL)
		c.Assert(err, IsNil, Commentf("err %v", err))

		if testSQL.overlaySQL != "" {
			_, err = tk.Exec("create global " + testSQL.overlaySQL)
			c.Assert(err, IsNil)
		}

		pb := &dto.Metric{}
		err = metrics.BindTotalGauge.WithLabelValues(metrics.ScopeGlobal, bindinfo.Using).Write(pb)
		c.Assert(err, IsNil)
		c.Assert(pb.GetGauge().GetValue(), Equals, float64(1))
		err = metrics.BindMemoryUsage.WithLabelValues(metrics.ScopeGlobal, bindinfo.Using).Write(pb)
		c.Assert(err, IsNil)
		c.Assert(pb.GetGauge().GetValue(), Equals, testSQL.memoryUsage)

		sql, hash := normalizeWithDefaultDB(c, testSQL.querySQL, "test")

		bindData := s.domain.BindHandle().GetBindRecord(hash, sql, "test")
		c.Check(bindData, NotNil)
		c.Check(bindData.OriginalSQL, Equals, testSQL.originSQL)
		bind := bindData.Bindings[0]
		c.Check(bind.BindSQL, Equals, testSQL.bindSQL)
		c.Check(bindData.Db, Equals, "test")
		c.Check(bind.Status, Equals, "using")
		c.Check(bind.Charset, NotNil)
		c.Check(bind.Collation, NotNil)
		c.Check(bind.CreateTime, NotNil)
		c.Check(bind.UpdateTime, NotNil)

		rs, err := tk.Exec("show global bindings")
		c.Assert(err, IsNil)
		chk := rs.NewChunk()
		err = rs.Next(context.TODO(), chk)
		c.Check(err, IsNil)
		c.Check(chk.NumRows(), Equals, 1)
		row := chk.GetRow(0)
		c.Check(row.GetString(0), Equals, testSQL.originSQL)
		c.Check(row.GetString(1), Equals, testSQL.bindSQL)
		c.Check(row.GetString(2), Equals, "test")
		c.Check(row.GetString(3), Equals, "using")
		c.Check(row.GetTime(4), NotNil)
		c.Check(row.GetTime(5), NotNil)
		c.Check(row.GetString(6), NotNil)
		c.Check(row.GetString(7), NotNil)

		bindHandle := bindinfo.NewBindHandle(tk.Se)
		err = bindHandle.Update(true)
		c.Check(err, IsNil)
		c.Check(bindHandle.Size(), Equals, 1)

		bindData = bindHandle.GetBindRecord(hash, sql, "test")
		c.Check(bindData, NotNil)
		c.Check(bindData.OriginalSQL, Equals, testSQL.originSQL)
		bind = bindData.Bindings[0]
		c.Check(bind.BindSQL, Equals, testSQL.bindSQL)
		c.Check(bindData.Db, Equals, "test")
		c.Check(bind.Status, Equals, "using")
		c.Check(bind.Charset, NotNil)
		c.Check(bind.Collation, NotNil)
		c.Check(bind.CreateTime, NotNil)
		c.Check(bind.UpdateTime, NotNil)

		_, err = tk.Exec("drop global " + testSQL.dropSQL)
		c.Check(err, IsNil)
		bindData = s.domain.BindHandle().GetBindRecord(hash, sql, "test")
		c.Check(bindData, IsNil)

		err = metrics.BindTotalGauge.WithLabelValues(metrics.ScopeGlobal, bindinfo.Using).Write(pb)
		c.Assert(err, IsNil)
		c.Assert(pb.GetGauge().GetValue(), Equals, float64(0))
		err = metrics.BindMemoryUsage.WithLabelValues(metrics.ScopeGlobal, bindinfo.Using).Write(pb)
		c.Assert(err, IsNil)
		// From newly created global bind handle.
		c.Assert(pb.GetGauge().GetValue(), Equals, testSQL.memoryUsage)

		bindHandle = bindinfo.NewBindHandle(tk.Se)
		err = bindHandle.Update(true)
		c.Check(err, IsNil)
		c.Check(bindHandle.Size(), Equals, 0)

		bindData = bindHandle.GetBindRecord(hash, sql, "test")
		c.Check(bindData, IsNil)

		rs, err = tk.Exec("show global bindings")
		c.Assert(err, IsNil)
		chk = rs.NewChunk()
		err = rs.Next(context.TODO(), chk)
		c.Check(err, IsNil)
		c.Check(chk.NumRows(), Equals, 0)

		_, err = tk.Exec("delete from mysql.bind_info where source != 'builtin'")
		c.Assert(err, IsNil)
	}
}

func (s *testSuite) TestSessionBinding(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	for _, testSQL := range testSQLs {
		s.cleanBindingEnv(tk)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("drop table if exists t1")
		tk.MustExec("create table t(i int, s varchar(20))")
		tk.MustExec("create table t1(i int, s varchar(20))")
		tk.MustExec("create index index_t on t(i,s)")

		metrics.BindTotalGauge.Reset()
		metrics.BindMemoryUsage.Reset()

		_, err := tk.Exec("create session " + testSQL.createSQL)
		c.Assert(err, IsNil, Commentf("err %v", err))

		if testSQL.overlaySQL != "" {
			_, err = tk.Exec("create session " + testSQL.overlaySQL)
			c.Assert(err, IsNil)
		}

		pb := &dto.Metric{}
		err = metrics.BindTotalGauge.WithLabelValues(metrics.ScopeSession, bindinfo.Using).Write(pb)
		c.Assert(err, IsNil)
		c.Assert(pb.GetGauge().GetValue(), Equals, float64(1))
		err = metrics.BindMemoryUsage.WithLabelValues(metrics.ScopeSession, bindinfo.Using).Write(pb)
		c.Assert(err, IsNil)
		c.Assert(pb.GetGauge().GetValue(), Equals, testSQL.memoryUsage)

		handle := tk.Se.Value(bindinfo.SessionBindInfoKeyType).(*bindinfo.SessionHandle)
		bindData := handle.GetBindRecord(testSQL.originSQL, "test")
		c.Check(bindData, NotNil)
		c.Check(bindData.OriginalSQL, Equals, testSQL.originSQL)
		bind := bindData.Bindings[0]
		c.Check(bind.BindSQL, Equals, testSQL.bindSQL)
		c.Check(bindData.Db, Equals, "test")
		c.Check(bind.Status, Equals, "using")
		c.Check(bind.Charset, NotNil)
		c.Check(bind.Collation, NotNil)
		c.Check(bind.CreateTime, NotNil)
		c.Check(bind.UpdateTime, NotNil)

		rs, err := tk.Exec("show global bindings")
		c.Assert(err, IsNil)
		chk := rs.NewChunk()
		err = rs.Next(context.TODO(), chk)
		c.Check(err, IsNil)
		c.Check(chk.NumRows(), Equals, 0)

		rs, err = tk.Exec("show session bindings")
		c.Assert(err, IsNil)
		chk = rs.NewChunk()
		err = rs.Next(context.TODO(), chk)
		c.Check(err, IsNil)
		c.Check(chk.NumRows(), Equals, 1)
		row := chk.GetRow(0)
		c.Check(row.GetString(0), Equals, testSQL.originSQL)
		c.Check(row.GetString(1), Equals, testSQL.bindSQL)
		c.Check(row.GetString(2), Equals, "test")
		c.Check(row.GetString(3), Equals, "using")
		c.Check(row.GetTime(4), NotNil)
		c.Check(row.GetTime(5), NotNil)
		c.Check(row.GetString(6), NotNil)
		c.Check(row.GetString(7), NotNil)

		_, err = tk.Exec("drop session " + testSQL.dropSQL)
		c.Assert(err, IsNil)
		bindData = handle.GetBindRecord(testSQL.originSQL, "test")
		c.Check(bindData, NotNil)
		c.Check(bindData.OriginalSQL, Equals, testSQL.originSQL)
		c.Check(len(bindData.Bindings), Equals, 0)

		err = metrics.BindTotalGauge.WithLabelValues(metrics.ScopeSession, bindinfo.Using).Write(pb)
		c.Assert(err, IsNil)
		c.Assert(pb.GetGauge().GetValue(), Equals, float64(0))
		err = metrics.BindMemoryUsage.WithLabelValues(metrics.ScopeSession, bindinfo.Using).Write(pb)
		c.Assert(err, IsNil)
		c.Assert(pb.GetGauge().GetValue(), Equals, float64(0))
	}
}

func (s *testSuite) TestGlobalAndSessionBindingBothExist(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(id int)")
	tk.MustExec("create table t2(id int)")
	c.Assert(tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "HashJoin"), IsTrue)
	c.Assert(tk.HasPlan("SELECT  /*+ TIDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id", "MergeJoin"), IsTrue)

	tk.MustExec("create global binding for SELECT * from t1,t2 where t1.id = t2.id using SELECT  /*+ TIDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id")

	// Test bindingUsage, which indicates how many times the binding is used.
	metrics.BindUsageCounter.Reset()
	c.Assert(tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "MergeJoin"), IsTrue)
	pb := &dto.Metric{}
	err := metrics.BindUsageCounter.WithLabelValues(metrics.ScopeGlobal).Write(pb)
	c.Assert(err, IsNil)
	c.Assert(pb.GetCounter().GetValue(), Equals, float64(1))

	// Test 'tidb_use_plan_baselines'
	tk.MustExec("set @@tidb_use_plan_baselines = 0")
	c.Assert(tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "HashJoin"), IsTrue)
	tk.MustExec("set @@tidb_use_plan_baselines = 1")

	// Test 'drop global binding'
	c.Assert(tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "MergeJoin"), IsTrue)
	tk.MustExec("drop global binding for SELECT * from t1,t2 where t1.id = t2.id")
	c.Assert(tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "HashJoin"), IsTrue)

	// Test the case when global and session binding both exist
	// PART1 : session binding should totally cover global binding
	// use merge join as session binding here since the optimizer will choose hash join for this stmt in default
	tk.MustExec("create global binding for SELECT * from t1,t2 where t1.id = t2.id using SELECT  /*+ TIDB_HJ(t1, t2) */  * from t1,t2 where t1.id = t2.id")
	c.Assert(tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "HashJoin"), IsTrue)
	tk.MustExec("create binding for SELECT * from t1,t2 where t1.id = t2.id using SELECT  /*+ TIDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id")
	c.Assert(tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "MergeJoin"), IsTrue)
	tk.MustExec("drop global binding for SELECT * from t1,t2 where t1.id = t2.id")
	c.Assert(tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "MergeJoin"), IsTrue)

	// PART2 : the dropped session binding should continue to block the effect of global binding
	tk.MustExec("create global binding for SELECT * from t1,t2 where t1.id = t2.id using SELECT  /*+ TIDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id")
	tk.MustExec("drop binding for SELECT * from t1,t2 where t1.id = t2.id")
	c.Assert(tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "HashJoin"), IsTrue)
	tk.MustExec("drop global binding for SELECT * from t1,t2 where t1.id = t2.id")
	c.Assert(tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "HashJoin"), IsTrue)
}

func (s *testSuite) TestExplain(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(id int)")
	tk.MustExec("create table t2(id int)")

	c.Assert(tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "HashJoin"), IsTrue)
	c.Assert(tk.HasPlan("SELECT  /*+ TIDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id", "MergeJoin"), IsTrue)

	tk.MustExec("create global binding for SELECT * from t1,t2 where t1.id = t2.id using SELECT  /*+ TIDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id")

	c.Assert(tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "MergeJoin"), IsTrue)

	tk.MustExec("drop global binding for SELECT * from t1,t2 where t1.id = t2.id")

	// Add test for SetOprStmt
	tk.MustExec("create index index_id on t1(id)")
	c.Assert(tk.HasPlan("SELECT * from t1 union SELECT * from t1", "IndexReader"), IsFalse)
	c.Assert(tk.HasPlan("SELECT * from t1 use index(index_id) union SELECT * from t1", "IndexReader"), IsTrue)

	tk.MustExec("create global binding for SELECT * from t1 union SELECT * from t1 using SELECT * from t1 use index(index_id) union SELECT * from t1")

	c.Assert(tk.HasPlan("SELECT * from t1 union SELECT * from t1", "IndexReader"), IsTrue)

	tk.MustExec("drop global binding for SELECT * from t1 union SELECT * from t1")
}

// TestBindingSymbolList tests sql with "?, ?, ?, ?", fixes #13871
func (s *testSuite) TestBindingSymbolList(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, INDEX ia (a), INDEX ib (b));")
	tk.MustExec("insert into t value(1, 1);")

	// before binding
	tk.MustQuery("select a, b from t where a = 3 limit 1, 100")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:ia")
	c.Assert(tk.MustUseIndex("select a, b from t where a = 3 limit 1, 100", "ia(a)"), IsTrue)

	tk.MustExec(`create global binding for select a, b from t where a = 1 limit 0, 1 using select a, b from t use index (ib) where a = 1 limit 0, 1`)

	// after binding
	tk.MustQuery("select a, b from t where a = 3 limit 1, 100")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:ib")
	c.Assert(tk.MustUseIndex("select a, b from t where a = 3 limit 1, 100", "ib(b)"), IsTrue)

	// Normalize
	sql, hash := parser.NormalizeDigest("select a, b from test . t where a = 1 limit 0, 1")

	bindData := s.domain.BindHandle().GetBindRecord(hash.String(), sql, "test")
	c.Assert(bindData, NotNil)
	c.Check(bindData.OriginalSQL, Equals, "select `a` , `b` from `test` . `t` where `a` = ? limit ...")
	bind := bindData.Bindings[0]
	c.Check(bind.BindSQL, Equals, "SELECT `a`,`b` FROM `test`.`t` USE INDEX (`ib`) WHERE `a` = 1 LIMIT 0,1")
	c.Check(bindData.Db, Equals, "test")
	c.Check(bind.Status, Equals, "using")
	c.Check(bind.Charset, NotNil)
	c.Check(bind.Collation, NotNil)
	c.Check(bind.CreateTime, NotNil)
	c.Check(bind.UpdateTime, NotNil)
}

func (s *testSuite) TestDMLSQLBind(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int, c int, key idx_b(b), key idx_c(c))")
	tk.MustExec("create table t2(a int, b int, c int, key idx_b(b), key idx_c(c))")

	tk.MustExec("delete from t1 where b = 1 and c > 1")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t1:idx_b")
	c.Assert(tk.MustUseIndex("delete from t1 where b = 1 and c > 1", "idx_b(b)"), IsTrue)
	tk.MustExec("create global binding for delete from t1 where b = 1 and c > 1 using delete /*+ use_index(t1,idx_c) */ from t1 where b = 1 and c > 1")
	tk.MustExec("delete from t1 where b = 1 and c > 1")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t1:idx_c")
	c.Assert(tk.MustUseIndex("delete from t1 where b = 1 and c > 1", "idx_c(c)"), IsTrue)

	c.Assert(tk.HasPlan("delete t1, t2 from t1 inner join t2 on t1.b = t2.b", "HashJoin"), IsTrue)
	tk.MustExec("create global binding for delete t1, t2 from t1 inner join t2 on t1.b = t2.b using delete /*+ inl_join(t1) */ t1, t2 from t1 inner join t2 on t1.b = t2.b")
	c.Assert(tk.HasPlan("delete t1, t2 from t1 inner join t2 on t1.b = t2.b", "IndexJoin"), IsTrue)

	tk.MustExec("update t1 set a = 1 where b = 1 and c > 1")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t1:idx_b")
	c.Assert(tk.MustUseIndex("update t1 set a = 1 where b = 1 and c > 1", "idx_b(b)"), IsTrue)
	tk.MustExec("create global binding for update t1 set a = 1 where b = 1 and c > 1 using update /*+ use_index(t1,idx_c) */ t1 set a = 1 where b = 1 and c > 1")
	tk.MustExec("delete from t1 where b = 1 and c > 1")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t1:idx_c")
	c.Assert(tk.MustUseIndex("update t1 set a = 1 where b = 1 and c > 1", "idx_c(c)"), IsTrue)

	c.Assert(tk.HasPlan("update t1, t2 set t1.a = 1 where t1.b = t2.b", "HashJoin"), IsTrue)
	tk.MustExec("create global binding for update t1, t2 set t1.a = 1 where t1.b = t2.b using update /*+ inl_join(t1) */ t1, t2 set t1.a = 1 where t1.b = t2.b")
	c.Assert(tk.HasPlan("update t1, t2 set t1.a = 1 where t1.b = t2.b", "IndexJoin"), IsTrue)

	tk.MustExec("insert into t1 select * from t2 where t2.b = 2 and t2.c > 2")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t2:idx_b")
	c.Assert(tk.MustUseIndex("insert into t1 select * from t2 where t2.b = 2 and t2.c > 2", "idx_b(b)"), IsTrue)
	tk.MustExec("create global binding for insert into t1 select * from t2 where t2.b = 1 and t2.c > 1 using insert /*+ use_index(t2,idx_c) */ into t1 select * from t2 where t2.b = 1 and t2.c > 1")
	tk.MustExec("insert into t1 select * from t2 where t2.b = 2 and t2.c > 2")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t2:idx_b")
	c.Assert(tk.MustUseIndex("insert into t1 select * from t2 where t2.b = 2 and t2.c > 2", "idx_b(b)"), IsTrue)
	tk.MustExec("drop global binding for insert into t1 select * from t2 where t2.b = 1 and t2.c > 1")
	tk.MustExec("create global binding for insert into t1 select * from t2 where t2.b = 1 and t2.c > 1 using insert into t1 select /*+ use_index(t2,idx_c) */ * from t2 where t2.b = 1 and t2.c > 1")
	tk.MustExec("insert into t1 select * from t2 where t2.b = 2 and t2.c > 2")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t2:idx_c")
	c.Assert(tk.MustUseIndex("insert into t1 select * from t2 where t2.b = 2 and t2.c > 2", "idx_c(c)"), IsTrue)

	tk.MustExec("replace into t1 select * from t2 where t2.b = 2 and t2.c > 2")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t2:idx_b")
	c.Assert(tk.MustUseIndex("replace into t1 select * from t2 where t2.b = 2 and t2.c > 2", "idx_b(b)"), IsTrue)
	tk.MustExec("create global binding for replace into t1 select * from t2 where t2.b = 1 and t2.c > 1 using replace into t1 select /*+ use_index(t2,idx_c) */ * from t2 where t2.b = 1 and t2.c > 1")
	tk.MustExec("replace into t1 select * from t2 where t2.b = 2 and t2.c > 2")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t2:idx_c")
	c.Assert(tk.MustUseIndex("replace into t1 select * from t2 where t2.b = 2 and t2.c > 2", "idx_c(c)"), IsTrue)
}

func (s *testSuite) TestBestPlanInBaselines(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, INDEX ia (a), INDEX ib (b));")
	tk.MustExec("insert into t value(1, 1);")

	// before binding
	tk.MustQuery("select a, b from t where a = 3 limit 1, 100")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:ia")
	c.Assert(tk.MustUseIndex("select a, b from t where a = 3 limit 1, 100", "ia(a)"), IsTrue)

	tk.MustQuery("select a, b from t where b = 3 limit 1, 100")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:ib")
	c.Assert(tk.MustUseIndex("select a, b from t where b = 3 limit 1, 100", "ib(b)"), IsTrue)

	tk.MustExec(`create global binding for select a, b from t where a = 1 limit 0, 1 using select /*+ use_index(@sel_1 test.t ia) */ a, b from t where a = 1 limit 0, 1`)
	tk.MustExec(`create global binding for select a, b from t where b = 1 limit 0, 1 using select /*+ use_index(@sel_1 test.t ib) */ a, b from t where b = 1 limit 0, 1`)

	sql, hash := normalizeWithDefaultDB(c, "select a, b from t where a = 1 limit 0, 1", "test")
	bindData := s.domain.BindHandle().GetBindRecord(hash, sql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalSQL, Equals, "select `a` , `b` from `test` . `t` where `a` = ? limit ...")
	bind := bindData.Bindings[0]
	c.Check(bind.BindSQL, Equals, "SELECT /*+ use_index(@`sel_1` `test`.`t` `ia`)*/ `a`,`b` FROM `test`.`t` WHERE `a` = 1 LIMIT 0,1")
	c.Check(bindData.Db, Equals, "test")
	c.Check(bind.Status, Equals, "using")

	tk.MustQuery("select a, b from t where a = 3 limit 1, 10")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:ia")
	c.Assert(tk.MustUseIndex("select a, b from t where a = 3 limit 1, 100", "ia(a)"), IsTrue)

	tk.MustQuery("select a, b from t where b = 3 limit 1, 100")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:ib")
	c.Assert(tk.MustUseIndex("select a, b from t where b = 3 limit 1, 100", "ib(b)"), IsTrue)
}

func (s *testSuite) TestErrorBind(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustGetErrMsg("create global binding for select * from t using select * from t", "[schema:1146]Table 'test.t' doesn't exist")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(i int, s varchar(20))")
	tk.MustExec("create table t1(i int, s varchar(20))")
	tk.MustExec("create index index_t on t(i,s)")

	_, err := tk.Exec("create global binding for select * from t where i>100 using select * from t use index(index_t) where i>100")
	c.Assert(err, IsNil, Commentf("err %v", err))

	sql, hash := parser.NormalizeDigest("select * from test . t where i > ?")
	bindData := s.domain.BindHandle().GetBindRecord(hash.String(), sql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalSQL, Equals, "select * from `test` . `t` where `i` > ?")
	bind := bindData.Bindings[0]
	c.Check(bind.BindSQL, Equals, "SELECT * FROM `test`.`t` USE INDEX (`index_t`) WHERE `i` > 100")
	c.Check(bindData.Db, Equals, "test")
	c.Check(bind.Status, Equals, "using")
	c.Check(bind.Charset, NotNil)
	c.Check(bind.Collation, NotNil)
	c.Check(bind.CreateTime, NotNil)
	c.Check(bind.UpdateTime, NotNil)

	tk.MustExec("drop index index_t on t")
	_, err = tk.Exec("select * from t where i > 10")
	c.Check(err, IsNil)

	s.domain.BindHandle().DropInvalidBindRecord()

	rs, err := tk.Exec("show global bindings")
	c.Assert(err, IsNil)
	chk := rs.NewChunk()
	err = rs.Next(context.TODO(), chk)
	c.Check(err, IsNil)
	c.Check(chk.NumRows(), Equals, 0)
}

func (s *testSuite) TestPreparedStmt(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)

	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(false) // requires plan cache disabled, or the IndexNames = 1 on first test.

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec(`prepare stmt1 from 'select * from t'`)
	tk.MustExec("execute stmt1")
	c.Assert(len(tk.Se.GetSessionVars().StmtCtx.IndexNames), Equals, 0)

	tk.MustExec("create binding for select * from t using select * from t use index(idx)")
	tk.MustExec("execute stmt1")
	c.Assert(len(tk.Se.GetSessionVars().StmtCtx.IndexNames), Equals, 1)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:idx")

	tk.MustExec("drop binding for select * from t")
	tk.MustExec("execute stmt1")
	c.Assert(len(tk.Se.GetSessionVars().StmtCtx.IndexNames), Equals, 0)

	tk.MustExec("drop table t")
	tk.MustExec("create table t(a int, b int, c int, index idx_b(b), index idx_c(c))")
	tk.MustExec("set @p = 1")

	tk.MustExec("prepare stmt from 'delete from t where b = ? and c > ?'")
	tk.MustExec("execute stmt using @p,@p")
	c.Assert(len(tk.Se.GetSessionVars().StmtCtx.IndexNames), Equals, 1)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:idx_b")
	tk.MustExec("create binding for delete from t where b = 2 and c > 2 using delete /*+ use_index(t,idx_c) */ from t where b = 2 and c > 2")
	tk.MustExec("execute stmt using @p,@p")
	c.Assert(len(tk.Se.GetSessionVars().StmtCtx.IndexNames), Equals, 1)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:idx_c")

	tk.MustExec("prepare stmt from 'update t set a = 1 where b = ? and c > ?'")
	tk.MustExec("execute stmt using @p,@p")
	c.Assert(len(tk.Se.GetSessionVars().StmtCtx.IndexNames), Equals, 1)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:idx_b")
	tk.MustExec("create binding for update t set a = 2 where b = 2 and c > 2 using update /*+ use_index(t,idx_c) */ t set a = 2 where b = 2 and c > 2")
	tk.MustExec("execute stmt using @p,@p")
	c.Assert(len(tk.Se.GetSessionVars().StmtCtx.IndexNames), Equals, 1)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:idx_c")

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 like t")
	tk.MustExec("prepare stmt from 'insert into t1 select * from t where t.b = ? and t.c > ?'")
	tk.MustExec("execute stmt using @p,@p")
	c.Assert(len(tk.Se.GetSessionVars().StmtCtx.IndexNames), Equals, 1)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:idx_b")
	tk.MustExec("create binding for insert into t1 select * from t where t.b = 2 and t.c > 2 using insert into t1 select /*+ use_index(t,idx_c) */ * from t where t.b = 2 and t.c > 2")
	tk.MustExec("execute stmt using @p,@p")
	c.Assert(len(tk.Se.GetSessionVars().StmtCtx.IndexNames), Equals, 1)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:idx_c")

	tk.MustExec("prepare stmt from 'replace into t1 select * from t where t.b = ? and t.c > ?'")
	tk.MustExec("execute stmt using @p,@p")
	c.Assert(len(tk.Se.GetSessionVars().StmtCtx.IndexNames), Equals, 1)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:idx_b")
	tk.MustExec("create binding for replace into t1 select * from t where t.b = 2 and t.c > 2 using replace into t1 select /*+ use_index(t,idx_c) */ * from t where t.b = 2 and t.c > 2")
	tk.MustExec("execute stmt using @p,@p")
	c.Assert(len(tk.Se.GetSessionVars().StmtCtx.IndexNames), Equals, 1)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:idx_c")
}

func (s *testSuite) TestDMLCapturePlanBaseline(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec(" set @@tidb_capture_plan_baselines = on")
	defer func() {
		tk.MustExec(" set @@tidb_capture_plan_baselines = off")
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, key idx_b(b), key idx_c(c))")
	tk.MustExec("create table t1 like t")
	s.domain.BindHandle().CaptureBaselines()
	tk.MustQuery("show global bindings").Check(testkit.Rows())
	tk.MustExec("delete from t where b = 1 and c > 1")
	tk.MustExec("delete from t where b = 1 and c > 1")
	tk.MustExec("update t set a = 1 where b = 1 and c > 1")
	tk.MustExec("update t set a = 1 where b = 1 and c > 1")
	tk.MustExec("insert into t1 select * from t where t.b = 1 and t.c > 1")
	tk.MustExec("insert into t1 select * from t where t.b = 1 and t.c > 1")
	tk.MustExec("replace into t1 select * from t where t.b = 1 and t.c > 1")
	tk.MustExec("replace into t1 select * from t where t.b = 1 and t.c > 1")
	tk.MustExec("insert into t1 values(1,1,1)")
	tk.MustExec("insert into t1 values(1,1,1)")
	tk.MustExec("replace into t1 values(1,1,1)")
	tk.MustExec("replace into t1 values(1,1,1)")
	tk.MustExec("admin capture bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 0)

	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	tk.MustExec("delete from t where b = 1 and c > 1")
	tk.MustExec("delete from t where b = 1 and c > 1")
	tk.MustExec("update t set a = 1 where b = 1 and c > 1")
	tk.MustExec("update t set a = 1 where b = 1 and c > 1")
	tk.MustExec("insert into t1 select * from t where t.b = 1 and t.c > 1")
	tk.MustExec("insert into t1 select * from t where t.b = 1 and t.c > 1")
	tk.MustExec("replace into t1 select * from t where t.b = 1 and t.c > 1")
	tk.MustExec("replace into t1 select * from t where t.b = 1 and t.c > 1")
	tk.MustExec("insert into t1 values(1,1,1)")
	tk.MustExec("insert into t1 values(1,1,1)")
	tk.MustExec("replace into t1 values(1,1,1)")
	tk.MustExec("replace into t1 values(1,1,1)")
	tk.MustExec("admin capture bindings")
	rows = tk.MustQuery("show global bindings").Sort().Rows()
	c.Assert(len(rows), Equals, 4)
	c.Assert(rows[0][0], Equals, "delete from `test` . `t` where `b` = ? and `c` > ?")
	c.Assert(rows[0][1], Equals, "DELETE /*+ use_index(@`del_1` `test`.`t` `idx_b`)*/ FROM `test`.`t` WHERE `b` = 1 AND `c` > 1")
	c.Assert(rows[1][0], Equals, "insert into `test` . `t1` select * from `test` . `t` where `t` . `b` = ? and `t` . `c` > ?")
	c.Assert(rows[1][1], Equals, "INSERT INTO `test`.`t1` SELECT /*+ use_index(@`sel_1` `test`.`t` `idx_b`)*/ * FROM `test`.`t` WHERE `t`.`b` = 1 AND `t`.`c` > 1")
	c.Assert(rows[2][0], Equals, "replace into `test` . `t1` select * from `test` . `t` where `t` . `b` = ? and `t` . `c` > ?")
	c.Assert(rows[2][1], Equals, "REPLACE INTO `test`.`t1` SELECT /*+ use_index(@`sel_1` `test`.`t` `idx_b`)*/ * FROM `test`.`t` WHERE `t`.`b` = 1 AND `t`.`c` > 1")
	c.Assert(rows[3][0], Equals, "update `test` . `t` set `a` = ? where `b` = ? and `c` > ?")
	c.Assert(rows[3][1], Equals, "UPDATE /*+ use_index(@`upd_1` `test`.`t` `idx_b`)*/ `test`.`t` SET `a`=1 WHERE `b` = 1 AND `c` > 1")
}

func (s *testSuite) TestCapturePlanBaseline(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec(" set @@tidb_capture_plan_baselines = on")
	defer func() {
		tk.MustExec(" set @@tidb_capture_plan_baselines = off")
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	s.domain.BindHandle().CaptureBaselines()
	tk.MustQuery("show global bindings").Check(testkit.Rows())
	tk.MustExec("select count(*) from t where a > 10")
	tk.MustExec("select count(*) from t where a > 10")
	tk.MustExec("admin capture bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 0)

	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	tk.MustExec("select * from t where a > 10")
	tk.MustExec("select * from t where a > 10")
	tk.MustExec("admin capture bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][0], Equals, "select * from `test` . `t` where `a` > ?")
	c.Assert(rows[0][1], Equals, "SELECT /*+ use_index(@`sel_1` `test`.`t` )*/ * FROM `test`.`t` WHERE `a` > 10")
}

func (s *testSuite) TestCaptureDBCaseSensitivity(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("drop database if exists SPM")
	tk.MustExec("create database SPM")
	tk.MustExec("use SPM")
	tk.MustExec("create table t(a int, b int, key(b))")
	tk.MustExec("create global binding for select * from t using select /*+ use_index(t) */ * from t")
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	tk.MustExec("select /*+ use_index(t,b) */ * from t")
	tk.MustExec("select /*+ use_index(t,b) */ * from t")
	tk.MustExec("admin capture bindings")
	// The capture should ignore the case sensitivity for DB name when checking if any binding exists,
	// so there would be no new binding captured.
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][1], Equals, "SELECT /*+ use_index(`t` )*/ * FROM `SPM`.`t`")
	c.Assert(rows[0][8], Equals, "manual")
}

func (s *testSuite) TestBaselineDBLowerCase(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("drop database if exists SPM")
	tk.MustExec("create database SPM")
	tk.MustExec("use SPM")
	tk.MustExec("create table t(a int, b int)")
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	tk.MustExec("update t set a = a + 1")
	tk.MustExec("update t set a = a + 1")
	tk.MustExec("admin capture bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][0], Equals, "update `spm` . `t` set `a` = `a` + ?")
	// default_db should have lower case.
	c.Assert(rows[0][2], Equals, "spm")
	tk.MustExec("drop global binding for update t set a = a + 1")
	rows = tk.MustQuery("show global bindings").Rows()
	// DROP GLOBAL BINGING should remove the binding even if we are in SPM database.
	c.Assert(len(rows), Equals, 0)

	tk.MustExec("create global binding for select * from t using select * from t")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][0], Equals, "select * from `spm` . `t`")
	// default_db should have lower case.
	c.Assert(rows[0][2], Equals, "spm")
	tk.MustExec("drop global binding for select * from t")
	rows = tk.MustQuery("show global bindings").Rows()
	// DROP GLOBAL BINGING should remove the binding even if we are in SPM database.
	c.Assert(len(rows), Equals, 0)

	tk.MustExec("create session binding for select * from t using select * from t")
	rows = tk.MustQuery("show session bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][0], Equals, "select * from `spm` . `t`")
	// default_db should have lower case.
	c.Assert(rows[0][2], Equals, "spm")
	tk.MustExec("drop session binding for select * from t")
	rows = tk.MustQuery("show session bindings").Rows()
	// DROP SESSION BINGING should remove the binding even if we are in SPM database.
	c.Assert(len(rows), Equals, 0)

	s.cleanBindingEnv(tk)
	// Simulate existing bindings with upper case default_db.
	tk.MustExec("insert into mysql.bind_info values('select * from `spm` . `t`', 'select * from `spm` . `t`', 'SPM', 'using', '2000-01-01 09:00:00', '2000-01-01 09:00:00', '', '','" +
		bindinfo.Manual + "')")
	tk.MustQuery("select original_sql, default_db from mysql.bind_info where original_sql = 'select * from `spm` . `t`'").Check(testkit.Rows(
		"select * from `spm` . `t` SPM",
	))
	tk.MustExec("admin reload bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][0], Equals, "select * from `spm` . `t`")
	// default_db should have lower case.
	c.Assert(rows[0][2], Equals, "spm")
	tk.MustExec("drop global binding for select * from t")
	rows = tk.MustQuery("show global bindings").Rows()
	// DROP GLOBAL BINGING should remove the binding even if we are in SPM database.
	c.Assert(len(rows), Equals, 0)

	s.cleanBindingEnv(tk)
	// Simulate existing bindings with upper case default_db.
	tk.MustExec("insert into mysql.bind_info values('select * from `spm` . `t`', 'select * from `spm` . `t`', 'SPM', 'using', '2000-01-01 09:00:00', '2000-01-01 09:00:00', '', '','" +
		bindinfo.Manual + "')")
	tk.MustQuery("select original_sql, default_db from mysql.bind_info where original_sql = 'select * from `spm` . `t`'").Check(testkit.Rows(
		"select * from `spm` . `t` SPM",
	))
	tk.MustExec("admin reload bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][0], Equals, "select * from `spm` . `t`")
	// default_db should have lower case.
	c.Assert(rows[0][2], Equals, "spm")
	tk.MustExec("create global binding for select * from t using select * from t")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][0], Equals, "select * from `spm` . `t`")
	// default_db should have lower case.
	c.Assert(rows[0][2], Equals, "spm")
	tk.MustQuery("select original_sql, default_db, status from mysql.bind_info where original_sql = 'select * from `spm` . `t`'").Check(testkit.Rows(
		"select * from `spm` . `t` SPM deleted",
		"select * from `spm` . `t` spm using",
	))
}

func (s *testSuite) TestShowGlobalBindings(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("drop database if exists SPM")
	tk.MustExec("create database SPM")
	tk.MustExec("use SPM")
	tk.MustExec("create table t(a int, b int, key(a))")
	tk.MustExec("create table t0(a int, b int, key(a))")
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 0)
	// Simulate existing bindings in the mysql.bind_info.
	tk.MustExec("insert into mysql.bind_info values('select * from `spm` . `t`', 'select * from `spm` . `t` USE INDEX (`a`)', 'SPM', 'using', '2000-01-01 09:00:00', '2000-01-01 09:00:00', '', '','" +
		bindinfo.Manual + "')")
	tk.MustExec("insert into mysql.bind_info values('select * from `spm` . `t0`', 'select * from `spm` . `t0` USE INDEX (`a`)', 'SPM', 'using', '2000-01-02 09:00:00', '2000-01-02 09:00:00', '', '','" +
		bindinfo.Manual + "')")
	tk.MustExec("insert into mysql.bind_info values('select * from `spm` . `t`', 'select /*+ use_index(`t` `a`)*/ * from `spm` . `t`', 'SPM', 'using', '2000-01-03 09:00:00', '2000-01-03 09:00:00', '', '','" +
		bindinfo.Manual + "')")
	tk.MustExec("insert into mysql.bind_info values('select * from `spm` . `t0`', 'select /*+ use_index(`t0` `a`)*/ * from `spm` . `t0`', 'SPM', 'using', '2000-01-04 09:00:00', '2000-01-04 09:00:00', '', '','" +
		bindinfo.Manual + "')")
	tk.MustExec("admin reload bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 4)
	c.Assert(rows[0][0], Equals, "select * from `spm` . `t0`")
	c.Assert(rows[0][5], Equals, "2000-01-04 09:00:00.000")
	c.Assert(rows[1][0], Equals, "select * from `spm` . `t0`")
	c.Assert(rows[1][5], Equals, "2000-01-02 09:00:00.000")
	c.Assert(rows[2][0], Equals, "select * from `spm` . `t`")
	c.Assert(rows[2][5], Equals, "2000-01-03 09:00:00.000")
	c.Assert(rows[3][0], Equals, "select * from `spm` . `t`")
	c.Assert(rows[3][5], Equals, "2000-01-01 09:00:00.000")

	rows = tk.MustQuery("show session bindings").Rows()
	c.Assert(len(rows), Equals, 0)
	tk.MustExec("create session binding for select a from t using select a from t")
	tk.MustExec("create session binding for select a from t0 using select a from t0")
	tk.MustExec("create session binding for select b from t using select b from t")
	tk.MustExec("create session binding for select b from t0 using select b from t0")
	rows = tk.MustQuery("show session bindings").Rows()
	c.Assert(len(rows), Equals, 4)
	c.Assert(rows[0][0], Equals, "select `b` from `spm` . `t0`")
	c.Assert(rows[1][0], Equals, "select `b` from `spm` . `t`")
	c.Assert(rows[2][0], Equals, "select `a` from `spm` . `t0`")
	c.Assert(rows[3][0], Equals, "select `a` from `spm` . `t`")
}

func (s *testSuite) TestCaptureBaselinesDefaultDB(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec(" set @@tidb_capture_plan_baselines = on")
	defer func() {
		tk.MustExec(" set @@tidb_capture_plan_baselines = off")
	}()
	tk.MustExec("use test")
	tk.MustExec("drop database if exists spm")
	tk.MustExec("create database spm")
	tk.MustExec("create table spm.t(a int, index idx_a(a))")
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	tk.MustExec("select * from spm.t ignore index(idx_a) where a > 10")
	tk.MustExec("select * from spm.t ignore index(idx_a) where a > 10")
	tk.MustExec("admin capture bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	// Default DB should be "" when all columns have explicit database name.
	c.Assert(rows[0][2], Equals, "")
	c.Assert(rows[0][3], Equals, "using")
	tk.MustExec("use spm")
	tk.MustExec("select * from spm.t where a > 10")
	// Should use TableScan because of the "ignore index" binding.
	c.Assert(len(tk.Se.GetSessionVars().StmtCtx.IndexNames), Equals, 0)
}

func (s *testSuite) TestCapturePreparedStmt(c *C) {
	originalVal := config.CheckTableBeforeDrop
	config.CheckTableBeforeDrop = true
	defer func() {
		config.CheckTableBeforeDrop = originalVal
	}()

	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, key idx_b(b), key idx_c(c))")
	c.Assert(tk.MustUseIndex("select * from t where b = 1 and c > 1", "idx_b(b)"), IsTrue)
	tk.MustExec("prepare stmt from 'select /*+ use_index(t,idx_c) */ * from t where b = ? and c > ?'")
	tk.MustExec("set @p = 1")
	tk.MustExec("execute stmt using @p, @p")
	tk.MustExec("execute stmt using @p, @p")

	tk.MustQuery("show global bindings").Check(testkit.Rows())
	tk.MustExec("admin capture bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][0], Equals, "select * from `test` . `t` where `b` = ? and `c` > ?")
	c.Assert(rows[0][1], Equals, "SELECT /*+ use_index(@`sel_1` `test`.`t` `idx_c`)*/ * FROM `test`.`t` WHERE `b` = ? AND `c` > ?")

	c.Assert(tk.MustUseIndex("select /*+ use_index(t,idx_b) */ * from t where b = 1 and c > 1", "idx_c(c)"), IsTrue)
	tk.MustExec("admin flush bindings")
	tk.MustExec("admin evolve bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][0], Equals, "select * from `test` . `t` where `b` = ? and `c` > ?")
	c.Assert(rows[0][1], Equals, "SELECT /*+ use_index(@`sel_1` `test`.`t` `idx_c`)*/ * FROM `test`.`t` WHERE `b` = ? AND `c` > ?")
}

func (s *testSuite) TestDropSingleBindings(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, index idx_a(a), index idx_b(b))")

	// Test drop session bindings.
	tk.MustExec("create binding for select * from t using select * from t use index(idx_a)")
	tk.MustExec("create binding for select * from t using select * from t use index(idx_b)")
	rows := tk.MustQuery("show bindings").Rows()
	// The size of bindings is equal to one. Because for one normalized sql,
	// the `create binding` clears all the origin bindings.
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][1], Equals, "SELECT * FROM `test`.`t` USE INDEX (`idx_b`)")
	tk.MustExec("drop binding for select * from t using select * from t use index(idx_a)")
	rows = tk.MustQuery("show bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][1], Equals, "SELECT * FROM `test`.`t` USE INDEX (`idx_b`)")
	tk.MustExec("drop table t")
	tk.MustExec("drop binding for select * from t using select * from t use index(idx_b)")
	rows = tk.MustQuery("show bindings").Rows()
	c.Assert(len(rows), Equals, 0)

	tk.MustExec("create table t(a int, b int, c int, index idx_a(a), index idx_b(b))")
	// Test drop global bindings.
	tk.MustExec("create global binding for select * from t using select * from t use index(idx_a)")
	tk.MustExec("create global binding for select * from t using select * from t use index(idx_b)")
	rows = tk.MustQuery("show global bindings").Rows()
	// The size of bindings is equal to one. Because for one normalized sql,
	// the `create binding` clears all the origin bindings.
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][1], Equals, "SELECT * FROM `test`.`t` USE INDEX (`idx_b`)")
	tk.MustExec("drop global binding for select * from t using select * from t use index(idx_a)")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][1], Equals, "SELECT * FROM `test`.`t` USE INDEX (`idx_b`)")
	tk.MustExec("drop table t")
	tk.MustExec("drop global binding for select * from t using select * from t use index(idx_b)")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 0)
}

func (s *testSuite) TestDMLEvolveBaselines(c *C) {
	originalVal := config.CheckTableBeforeDrop
	config.CheckTableBeforeDrop = true
	defer func() {
		config.CheckTableBeforeDrop = originalVal
	}()

	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, index idx_b(b), index idx_c(c))")
	tk.MustExec("insert into t values (1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5)")
	tk.MustExec("analyze table t")
	tk.MustExec("set @@tidb_evolve_plan_baselines=1")

	tk.MustExec("create global binding for delete from t where b = 1 and c > 1 using delete /*+ use_index(t,idx_c) */ from t where b = 1 and c > 1")
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	tk.MustExec("delete /*+ use_index(t,idx_b) */ from t where b = 2 and c > 1")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:idx_c")
	tk.MustExec("admin flush bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	tk.MustExec("admin evolve bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)

	tk.MustExec("create global binding for update t set a = 1 where b = 1 and c > 1 using update /*+ use_index(t,idx_c) */ t set a = 1 where b = 1 and c > 1")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 2)
	tk.MustExec("update /*+ use_index(t,idx_b) */ t set a = 2 where b = 2 and c > 1")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:idx_c")
	tk.MustExec("admin flush bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 2)
	tk.MustExec("admin evolve bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 2)

	tk.MustExec("create table t1 like t")
	tk.MustExec("create global binding for insert into t1 select * from t where t.b = 1 and t.c > 1 using insert into t1 select /*+ use_index(t,idx_c) */ * from t where t.b = 1 and t.c > 1")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 3)
	tk.MustExec("insert into t1 select /*+ use_index(t,idx_b) */ * from t where t.b = 2 and t.c > 2")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:idx_c")
	tk.MustExec("admin flush bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 3)
	tk.MustExec("admin evolve bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 3)

	tk.MustExec("create global binding for replace into t1 select * from t where t.b = 1 and t.c > 1 using replace into t1 select /*+ use_index(t,idx_c) */ * from t where t.b = 1 and t.c > 1")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 4)
	tk.MustExec("replace into t1 select /*+ use_index(t,idx_b) */ * from t where t.b = 2 and t.c > 2")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:idx_c")
	tk.MustExec("admin flush bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 4)
	tk.MustExec("admin evolve bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 4)
}

func (s *testSuite) TestAddEvolveTasks(c *C) {
	originalVal := config.CheckTableBeforeDrop
	config.CheckTableBeforeDrop = true
	defer func() {
		config.CheckTableBeforeDrop = originalVal
	}()

	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, index idx_a(a), index idx_b(b), index idx_c(c))")
	tk.MustExec("insert into t values (1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5)")
	tk.MustExec("analyze table t")
	tk.MustExec("create global binding for select * from t where a >= 1 and b >= 1 and c = 0 using select * from t use index(idx_a) where a >= 1 and b >= 1 and c = 0")
	tk.MustExec("set @@tidb_evolve_plan_baselines=1")
	// It cannot choose table path although it has lowest cost.
	tk.MustQuery("select * from t where a >= 4 and b >= 1 and c = 0")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:idx_a")
	tk.MustExec("admin flush bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 2)
	c.Assert(rows[0][1], Equals, "SELECT /*+ use_index(@`sel_1` `test`.`t` )*/ * FROM `test`.`t` WHERE `a` >= 4 AND `b` >= 1 AND `c` = 0")
	c.Assert(rows[0][3], Equals, "pending verify")
	tk.MustExec("admin evolve bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 2)
	c.Assert(rows[0][1], Equals, "SELECT /*+ use_index(@`sel_1` `test`.`t` )*/ * FROM `test`.`t` WHERE `a` >= 4 AND `b` >= 1 AND `c` = 0")
	status := rows[0][3].(string)
	c.Assert(status == "using" || status == "rejected", IsTrue)
}

func (s *testSuite) TestRuntimeHintsInEvolveTasks(c *C) {
	originalVal := config.CheckTableBeforeDrop
	config.CheckTableBeforeDrop = true
	defer func() {
		config.CheckTableBeforeDrop = originalVal
	}()

	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@tidb_evolve_plan_baselines=1")
	tk.MustExec("create table t(a int, b int, c int, index idx_a(a), index idx_b(b), index idx_c(c))")

	tk.MustExec("create global binding for select * from t where a >= 1 and b >= 1 and c = 0 using select * from t use index(idx_a) where a >= 1 and b >= 1 and c = 0")
	tk.MustQuery("select /*+ MAX_EXECUTION_TIME(5000) */ * from t where a >= 4 and b >= 1 and c = 0")
	tk.MustExec("admin flush bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 2)
	c.Assert(rows[0][1], Equals, "SELECT /*+ use_index(@`sel_1` `test`.`t` `idx_c`), max_execution_time(5000)*/ * FROM `test`.`t` WHERE `a` >= 4 AND `b` >= 1 AND `c` = 0")
}

func (s *testSuite) TestBindingCache(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("create global binding for select * from t using select * from t use index(idx);")
	tk.MustExec("create database tmp")
	tk.MustExec("use tmp")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("create global binding for select * from t using select * from t use index(idx);")

	c.Assert(s.domain.BindHandle().Update(false), IsNil)
	c.Assert(s.domain.BindHandle().Update(false), IsNil)
	res := tk.MustQuery("show global bindings")
	c.Assert(len(res.Rows()), Equals, 2)

	tk.MustExec("drop global binding for select * from t;")
	c.Assert(s.domain.BindHandle().Update(false), IsNil)
	c.Assert(len(s.domain.BindHandle().GetAllBindRecord()), Equals, 1)
}

func (s *testSuite) TestDefaultSessionVars(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustQuery(`show variables like "%baselines%"`).Sort().Check(testkit.Rows(
		"tidb_capture_plan_baselines OFF",
		"tidb_evolve_plan_baselines OFF",
		"tidb_use_plan_baselines ON"))
	tk.MustQuery(`show global variables like "%baselines%"`).Sort().Check(testkit.Rows(
		"tidb_capture_plan_baselines OFF",
		"tidb_evolve_plan_baselines OFF",
		"tidb_use_plan_baselines ON"))
}

func (s *testSuite) TestCaptureBaselinesScope(c *C) {
	tk1 := testkit.NewTestKit(c, s.store)
	tk2 := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk1)
	tk1.MustQuery(`show session variables like "tidb_capture_plan_baselines"`).Check(testkit.Rows(
		"tidb_capture_plan_baselines OFF",
	))
	tk1.MustQuery(`show global variables like "tidb_capture_plan_baselines"`).Check(testkit.Rows(
		"tidb_capture_plan_baselines OFF",
	))
	tk1.MustQuery(`select @@session.tidb_capture_plan_baselines`).Check(testkit.Rows(
		"0",
	))
	tk1.MustQuery(`select @@global.tidb_capture_plan_baselines`).Check(testkit.Rows(
		"0",
	))

	tk1.MustExec("set @@session.tidb_capture_plan_baselines = on")
	defer func() {
		tk1.MustExec(" set @@session.tidb_capture_plan_baselines = off")
	}()
	tk1.MustQuery(`show session variables like "tidb_capture_plan_baselines"`).Check(testkit.Rows(
		"tidb_capture_plan_baselines ON",
	))
	tk1.MustQuery(`show global variables like "tidb_capture_plan_baselines"`).Check(testkit.Rows(
		"tidb_capture_plan_baselines OFF",
	))
	tk1.MustQuery(`select @@session.tidb_capture_plan_baselines`).Check(testkit.Rows(
		"1",
	))
	tk1.MustQuery(`select @@global.tidb_capture_plan_baselines`).Check(testkit.Rows(
		"0",
	))
	tk2.MustQuery(`show session variables like "tidb_capture_plan_baselines"`).Check(testkit.Rows(
		"tidb_capture_plan_baselines ON",
	))
	tk2.MustQuery(`show global variables like "tidb_capture_plan_baselines"`).Check(testkit.Rows(
		"tidb_capture_plan_baselines OFF",
	))
	tk2.MustQuery(`select @@session.tidb_capture_plan_baselines`).Check(testkit.Rows(
		"1",
	))
	tk2.MustQuery(`select @@global.tidb_capture_plan_baselines`).Check(testkit.Rows(
		"0",
	))
}

func (s *testSuite) TestDuplicateBindings(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("create global binding for select * from t using select * from t use index(idx);")
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	createTime := rows[0][4]
	time.Sleep(1000000)
	tk.MustExec("create global binding for select * from t using select * from t use index(idx);")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(createTime == rows[0][4], Equals, false)

	tk.MustExec("create session binding for select * from t using select * from t use index(idx);")
	rows = tk.MustQuery("show session bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	createTime = rows[0][4]
	time.Sleep(1000000)
	tk.MustExec("create session binding for select * from t using select * from t use index(idx);")
	rows = tk.MustQuery("show session bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(createTime == rows[0][4], Equals, false)
}

func (s *testSuite) TestStmtHints(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("create global binding for select * from t using select /*+ MAX_EXECUTION_TIME(100), MEMORY_QUOTA(1 GB) */ * from t use index(idx)")
	tk.MustQuery("select * from t")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.MemQuotaQuery, Equals, int64(1073741824))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.MaxExecutionTime, Equals, uint64(100))
	tk.MustQuery("select a, b from t")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.MemQuotaQuery, Equals, int64(0))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.MaxExecutionTime, Equals, uint64(0))
}

func (s *testSuite) TestReloadBindings(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("create global binding for select * from t using select * from t use index(idx)")
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	rows = tk.MustQuery("select * from mysql.bind_info where source != 'builtin'").Rows()
	c.Assert(len(rows), Equals, 1)
	tk.MustExec("delete from mysql.bind_info where source != 'builtin'")
	c.Assert(s.domain.BindHandle().Update(false), IsNil)
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(s.domain.BindHandle().Update(true), IsNil)
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	tk.MustExec("admin reload bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 0)
}

func (s *testSuite) TestDefaultDB(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("create global binding for select * from test.t using select * from test.t use index(idx)")
	tk.MustExec("use mysql")
	tk.MustQuery("select * from test.t")
	// Even in another database, we could still use the bindings.
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:idx")
	tk.MustExec("drop global binding for select * from test.t")
	tk.MustQuery("show global bindings").Check(testkit.Rows())

	tk.MustExec("use test")
	tk.MustExec("create session binding for select * from test.t using select * from test.t use index(idx)")
	tk.MustExec("use mysql")
	tk.MustQuery("select * from test.t")
	// Even in another database, we could still use the bindings.
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:idx")
	tk.MustExec("drop session binding for select * from test.t")
	tk.MustQuery("show session bindings").Check(testkit.Rows())
}

func (s *testSuite) TestEvolveInvalidBindings(c *C) {
	originalVal := config.CheckTableBeforeDrop
	config.CheckTableBeforeDrop = true
	defer func() {
		config.CheckTableBeforeDrop = originalVal
	}()

	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx_a(a))")
	tk.MustExec("create global binding for select * from t where a > 10 using select /*+ USE_INDEX(t) */ * from t where a > 10")
	// Manufacture a rejected binding by hacking mysql.bind_info.
	tk.MustExec("insert into mysql.bind_info values('select * from test . t where a > ?', 'SELECT /*+ USE_INDEX(t,idx_a) */ * FROM test.t WHERE a > 10', 'test', 'rejected', '2000-01-01 09:00:00', '2000-01-01 09:00:00', '', '','" +
		bindinfo.Manual + "')")
	tk.MustQuery("select bind_sql, status from mysql.bind_info where source != 'builtin'").Sort().Check(testkit.Rows(
		"SELECT /*+ USE_INDEX(`t` )*/ * FROM `test`.`t` WHERE `a` > 10 using",
		"SELECT /*+ USE_INDEX(t,idx_a) */ * FROM test.t WHERE a > 10 rejected",
	))
	// Reload cache from mysql.bind_info.
	s.domain.BindHandle().Clear()
	c.Assert(s.domain.BindHandle().Update(true), IsNil)

	tk.MustExec("alter table t drop index idx_a")
	tk.MustExec("admin evolve bindings")
	c.Assert(s.domain.BindHandle().Update(false), IsNil)
	rows := tk.MustQuery("show global bindings").Sort().Rows()
	c.Assert(len(rows), Equals, 2)
	// Make sure this "using" binding is not overrided.
	c.Assert(rows[0][1], Equals, "SELECT /*+ USE_INDEX(`t` )*/ * FROM `test`.`t` WHERE `a` > 10")
	status := rows[0][3].(string)
	c.Assert(status == "using", IsTrue)
	c.Assert(rows[1][1], Equals, "SELECT /*+ USE_INDEX(t,idx_a) */ * FROM test.t WHERE a > 10")
	status = rows[1][3].(string)
	c.Assert(status == "using" || status == "rejected", IsTrue)
}

func (s *testSuite) TestOutdatedInfoSchema(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("create global binding for select * from t using select * from t use index(idx)")
	c.Assert(s.domain.BindHandle().Update(false), IsNil)
	s.cleanBindingEnv(tk)
	tk.MustExec("create global binding for select * from t using select * from t use index(idx)")
}

func (s *testSuite) TestPrivileges(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("create global binding for select * from t using select * from t use index(idx)")
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	tk.MustExec("create user test@'%'")
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "test", Hostname: "%"}, nil, nil), IsTrue)
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 0)
}

func (s *testSuite) TestHintsSetEvolveTask(c *C) {
	originalVal := config.CheckTableBeforeDrop
	config.CheckTableBeforeDrop = true
	defer func() {
		config.CheckTableBeforeDrop = originalVal
	}()

	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, index idx_a(a))")
	tk.MustExec("create global binding for select * from t where a > 10 using select * from t ignore index(idx_a) where a > 10")
	tk.MustExec("set @@tidb_evolve_plan_baselines=1")
	tk.MustQuery("select * from t use index(idx_a) where a > 0")
	bindHandle := s.domain.BindHandle()
	bindHandle.SaveEvolveTasksToStore()
	// Verify the added Binding for evolution contains valid ID and Hint, otherwise, panic may happen.
	sql, hash := normalizeWithDefaultDB(c, "select * from t where a > ?", "test")
	bindData := bindHandle.GetBindRecord(hash, sql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalSQL, Equals, "select * from `test` . `t` where `a` > ?")
	c.Assert(len(bindData.Bindings), Equals, 2)
	bind := bindData.Bindings[1]
	c.Assert(bind.Status, Equals, bindinfo.PendingVerify)
	c.Assert(bind.ID, Not(Equals), "")
	c.Assert(bind.Hint, NotNil)
}

func (s *testSuite) TestHintsSetID(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, index idx_a(a))")
	tk.MustExec("create global binding for select * from t where a > 10 using select /*+ use_index(test.t, idx_a) */ * from t where a > 10")
	bindHandle := s.domain.BindHandle()
	// Verify the added Binding contains ID with restored query block.
	sql, hash := normalizeWithDefaultDB(c, "select * from t where a > ?", "test")
	bindData := bindHandle.GetBindRecord(hash, sql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalSQL, Equals, "select * from `test` . `t` where `a` > ?")
	c.Assert(len(bindData.Bindings), Equals, 1)
	bind := bindData.Bindings[0]
	c.Assert(bind.ID, Equals, "use_index(@`sel_1` `test`.`t` `idx_a`)")

	s.cleanBindingEnv(tk)
	tk.MustExec("create global binding for select * from t where a > 10 using select /*+ use_index(t, idx_a) */ * from t where a > 10")
	bindData = bindHandle.GetBindRecord(hash, sql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalSQL, Equals, "select * from `test` . `t` where `a` > ?")
	c.Assert(len(bindData.Bindings), Equals, 1)
	bind = bindData.Bindings[0]
	c.Assert(bind.ID, Equals, "use_index(@`sel_1` `test`.`t` `idx_a`)")

	s.cleanBindingEnv(tk)
	tk.MustExec("create global binding for select * from t where a > 10 using select /*+ use_index(@sel_1 t, idx_a) */ * from t where a > 10")
	bindData = bindHandle.GetBindRecord(hash, sql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalSQL, Equals, "select * from `test` . `t` where `a` > ?")
	c.Assert(len(bindData.Bindings), Equals, 1)
	bind = bindData.Bindings[0]
	c.Assert(bind.ID, Equals, "use_index(@`sel_1` `test`.`t` `idx_a`)")

	s.cleanBindingEnv(tk)
	tk.MustExec("create global binding for select * from t where a > 10 using select /*+ use_index(@qb1 t, idx_a) qb_name(qb1) */ * from t where a > 10")
	bindData = bindHandle.GetBindRecord(hash, sql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalSQL, Equals, "select * from `test` . `t` where `a` > ?")
	c.Assert(len(bindData.Bindings), Equals, 1)
	bind = bindData.Bindings[0]
	c.Assert(bind.ID, Equals, "use_index(@`sel_1` `test`.`t` `idx_a`)")

	s.cleanBindingEnv(tk)
	tk.MustExec("create global binding for select * from t where a > 10 using select /*+ use_index(T, IDX_A) */ * from t where a > 10")
	bindData = bindHandle.GetBindRecord(hash, sql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalSQL, Equals, "select * from `test` . `t` where `a` > ?")
	c.Assert(len(bindData.Bindings), Equals, 1)
	bind = bindData.Bindings[0]
	c.Assert(bind.ID, Equals, "use_index(@`sel_1` `test`.`t` `idx_a`)")

	s.cleanBindingEnv(tk)
	err := tk.ExecToErr("create global binding for select * from t using select /*+ non_exist_hint() */ * from t")
	c.Assert(terror.ErrorEqual(err, parser.ErrWarnOptimizerHintParseError), IsTrue)
	tk.MustExec("create global binding for select * from t where a > 10 using select * from t where a > 10")
	bindData = bindHandle.GetBindRecord(hash, sql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalSQL, Equals, "select * from `test` . `t` where `a` > ?")
	c.Assert(len(bindData.Bindings), Equals, 1)
	bind = bindData.Bindings[0]
	c.Assert(bind.ID, Equals, "")
}

func (s *testSuite) TestCapturePlanBaselineIgnoreTiFlash(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, key(a), key(b))")
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	tk.MustExec("select * from t")
	tk.MustExec("select * from t")
	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}
	// Here the plan is the TiFlash plan.
	rows := tk.MustQuery("explain select * from t").Rows()
	c.Assert(fmt.Sprintf("%v", rows[len(rows)-1][2]), Equals, "cop[tiflash]")

	tk.MustQuery("show global bindings").Check(testkit.Rows())
	tk.MustExec("admin capture bindings")
	// Don't have the TiFlash plan even we have TiFlash replica.
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][0], Equals, "select * from `test` . `t`")
	c.Assert(rows[0][1], Equals, "SELECT /*+ use_index(@`sel_1` `test`.`t` )*/ * FROM `test`.`t`")
}

func (s *testSuite) TestNotEvolvePlanForReadStorageHint(c *C) {
	originalVal := config.CheckTableBeforeDrop
	config.CheckTableBeforeDrop = true
	defer func() {
		config.CheckTableBeforeDrop = originalVal
	}()

	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx_a(a), index idx_b(b))")
	tk.MustExec("insert into t values (1,1), (2,2), (3,3), (4,4), (5,5), (6,6), (7,7), (8,8), (9,9), (10,10)")
	tk.MustExec("analyze table t")
	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	// Make sure the best plan of the SQL is use TiKV index.
	tk.MustExec("set @@session.tidb_executor_concurrency = 4;")
	rows := tk.MustQuery("explain select * from t where a >= 11 and b >= 11").Rows()
	c.Assert(fmt.Sprintf("%v", rows[len(rows)-1][2]), Equals, "cop[tikv]")

	tk.MustExec("create global binding for select * from t where a >= 1 and b >= 1 using select /*+ read_from_storage(tiflash[t]) */ * from t where a >= 1 and b >= 1")
	tk.MustExec("set @@tidb_evolve_plan_baselines=1")

	// Even if index of TiKV has lower cost, it chooses TiFlash.
	rows = tk.MustQuery("explain select * from t where a >= 11 and b >= 11").Rows()
	c.Assert(fmt.Sprintf("%v", rows[len(rows)-1][2]), Equals, "cop[tiflash]")

	tk.MustExec("admin flush bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	// None evolve task, because of the origin binding is a read_from_storage binding.
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][1], Equals, "SELECT /*+ read_from_storage(tiflash[`t`])*/ * FROM `test`.`t` WHERE `a` >= 1 AND `b` >= 1")
	c.Assert(rows[0][3], Equals, "using")
}

func (s *testSuite) TestBindingWithIsolationRead(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx_a(a), index idx_b(b))")
	tk.MustExec("insert into t values (1,1), (2,2), (3,3), (4,4), (5,5), (6,6), (7,7), (8,8), (9,9), (10,10)")
	tk.MustExec("analyze table t")
	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}
	tk.MustExec("create global binding for select * from t where a >= 1 and b >= 1 using select * from t use index(idx_a) where a >= 1 and b >= 1")
	tk.MustExec("set @@tidb_use_plan_baselines = 1")
	rows := tk.MustQuery("explain select * from t where a >= 11 and b >= 11").Rows()
	c.Assert(rows[len(rows)-1][2], Equals, "cop[tikv]")
	// Even if we build a binding use index for SQL, but after we set the isolation read for TiFlash, it choose TiFlash instead of index of TiKV.
	tk.MustExec("set @@tidb_isolation_read_engines = \"tiflash\"")
	rows = tk.MustQuery("explain select * from t where a >= 11 and b >= 11").Rows()
	c.Assert(rows[len(rows)-1][2], Equals, "cop[tiflash]")
}

func (s *testSuite) TestReCreateBindAfterEvolvePlan(c *C) {
	originalVal := config.CheckTableBeforeDrop
	config.CheckTableBeforeDrop = true
	defer func() {
		config.CheckTableBeforeDrop = originalVal
	}()

	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, index idx_a(a), index idx_b(b), index idx_c(c))")
	tk.MustExec("insert into t values (1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5)")
	tk.MustExec("analyze table t")
	tk.MustExec("create global binding for select * from t where a >= 1 and b >= 1 using select * from t use index(idx_a) where a >= 1 and b >= 1")
	tk.MustExec("set @@tidb_evolve_plan_baselines=1")

	// It cannot choose table path although it has lowest cost.
	tk.MustQuery("select * from t where a >= 0 and b >= 0")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:idx_a")

	tk.MustExec("admin flush bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 2)
	c.Assert(rows[0][1], Equals, "SELECT /*+ use_index(@`sel_1` `test`.`t` )*/ * FROM `test`.`t` WHERE `a` >= 0 AND `b` >= 0")
	c.Assert(rows[0][3], Equals, "pending verify")

	tk.MustExec("create global binding for select * from t where a >= 1 and b >= 1 using select * from t use index(idx_b) where a >= 1 and b >= 1")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	tk.MustQuery("select * from t where a >= 4 and b >= 1")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:idx_b")
}

func (s *testSuite) TestInvisibleIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, unique idx_a(a), index idx_b(b) invisible)")
	tk.MustGetErrMsg(
		"create global binding for select * from t using select * from t use index(idx_b) ",
		"[planner:1176]Key 'idx_b' doesn't exist in table 't'")

	// Create bind using index
	tk.MustExec("create global binding for select * from t using select * from t use index(idx_a) ")

	tk.MustQuery("select * from t")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:idx_a")
	c.Assert(tk.MustUseIndex("select * from t", "idx_a(a)"), IsTrue)

	tk.MustExec(`prepare stmt1 from 'select * from t'`)
	tk.MustExec("execute stmt1")
	c.Assert(len(tk.Se.GetSessionVars().StmtCtx.IndexNames), Equals, 1)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:idx_a")

	// And then make this index invisible
	tk.MustExec("alter table t alter index idx_a invisible")
	tk.MustQuery("select * from t")
	c.Assert(len(tk.Se.GetSessionVars().StmtCtx.IndexNames), Equals, 0)

	tk.MustExec("execute stmt1")
	c.Assert(len(tk.Se.GetSessionVars().StmtCtx.IndexNames), Equals, 0)

	tk.MustExec("drop binding for select * from t")
}

func (s *testSuite) TestBindingSource(c *C) {
	originalVal := config.CheckTableBeforeDrop
	config.CheckTableBeforeDrop = true
	defer func() {
		config.CheckTableBeforeDrop = originalVal
	}()

	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, index idx_a(a))")

	// Test Source for SQL created sql
	tk.MustExec("create global binding for select * from t where a > 10 using select * from t ignore index(idx_a) where a > 10")
	bindHandle := s.domain.BindHandle()
	sql, hash := normalizeWithDefaultDB(c, "select * from t where a > ?", "test")
	bindData := bindHandle.GetBindRecord(hash, sql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalSQL, Equals, "select * from `test` . `t` where `a` > ?")
	c.Assert(len(bindData.Bindings), Equals, 1)
	bind := bindData.Bindings[0]
	c.Assert(bind.Source, Equals, bindinfo.Manual)

	// Test Source for evolved sql
	tk.MustExec("set @@tidb_evolve_plan_baselines=1")
	tk.MustQuery("select * from t where a > 10")
	bindHandle.SaveEvolveTasksToStore()
	sql, hash = normalizeWithDefaultDB(c, "select * from t where a > ?", "test")
	bindData = bindHandle.GetBindRecord(hash, sql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalSQL, Equals, "select * from `test` . `t` where `a` > ?")
	c.Assert(len(bindData.Bindings), Equals, 2)
	bind = bindData.Bindings[1]
	c.Assert(bind.Source, Equals, bindinfo.Evolve)
	tk.MustExec("set @@tidb_evolve_plan_baselines=0")

	// Test Source for captured sqls
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("set @@tidb_capture_plan_baselines = on")
	defer func() {
		tk.MustExec("set @@tidb_capture_plan_baselines = off")
	}()
	tk.MustExec("use test")
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	tk.MustExec("select * from t ignore index(idx_a) where a < 10")
	tk.MustExec("select * from t ignore index(idx_a) where a < 10")
	tk.MustExec("admin capture bindings")
	bindHandle.CaptureBaselines()
	sql, hash = normalizeWithDefaultDB(c, "select * from t where a < ?", "test")
	bindData = bindHandle.GetBindRecord(hash, sql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalSQL, Equals, "select * from `test` . `t` where `a` < ?")
	c.Assert(len(bindData.Bindings), Equals, 1)
	bind = bindData.Bindings[0]
	c.Assert(bind.Source, Equals, bindinfo.Capture)
}

func (s *testSuite) TestSPMHitInfo(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(id int)")
	tk.MustExec("create table t2(id int)")

	c.Assert(tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "HashJoin"), IsTrue)
	c.Assert(tk.HasPlan("SELECT  /*+ TIDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id", "MergeJoin"), IsTrue)

	tk.MustExec("SELECT * from t1,t2 where t1.id = t2.id")
	tk.MustQuery(`select @@last_plan_from_binding;`).Check(testkit.Rows("0"))
	tk.MustExec("create global binding for SELECT * from t1,t2 where t1.id = t2.id using SELECT  /*+ TIDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id")

	c.Assert(tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "MergeJoin"), IsTrue)
	tk.MustExec("SELECT * from t1,t2 where t1.id = t2.id")
	tk.MustQuery(`select @@last_plan_from_binding;`).Check(testkit.Rows("1"))
	tk.MustExec("drop global binding for SELECT * from t1,t2 where t1.id = t2.id")
}

func (s *testSuite) TestIssue19836(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, key (a));")
	tk.MustExec("CREATE SESSION BINDING FOR select * from t where a = 1 limit 5, 5 USING select * from t ignore index (a) where a = 1 limit 5, 5;")
	tk.MustExec("PREPARE stmt FROM 'select * from t where a = 40 limit ?, ?';")
	tk.MustExec("set @a=1;")
	tk.MustExec("set @b=2;")
	tk.MustExec("EXECUTE stmt USING @a, @b;")
	tk.Se.SetSessionManager(&mockSessionManager{
		PS: []*util.ProcessInfo{tk.Se.ShowProcess()},
	})
	explainResult := testkit.Rows(
		"Limit_8 2.00 0 root  time:0s, loops:0 offset:1, count:2 N/A N/A",
		"└─TableReader_13 3.00 0 root  time:0s, loops:0 data:Limit_12 N/A N/A",
		"  └─Limit_12 3.00 0 cop[tikv]   offset:0, count:3 N/A N/A",
		"    └─Selection_11 3.00 0 cop[tikv]   eq(test.t.a, 40) N/A N/A",
		"      └─TableFullScan_10 3000.00 0 cop[tikv] table:t  keep order:false, stats:pseudo N/A N/A",
	)
	tk.MustQuery("explain for connection " + strconv.FormatUint(tk.Se.ShowProcess().ID, 10)).Check(explainResult)
}

func (s *testSuite) TestReCreateBind(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(a))")

	tk.MustQuery("select * from mysql.bind_info where source != 'builtin'").Check(testkit.Rows())
	tk.MustQuery("show global bindings").Check(testkit.Rows())

	tk.MustExec("create global binding for select * from t using select * from t")
	tk.MustQuery("select original_sql, status from mysql.bind_info where source != 'builtin';").Check(testkit.Rows(
		"select * from `test` . `t` using",
	))
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][0], Equals, "select * from `test` . `t`")
	c.Assert(rows[0][3], Equals, "using")

	tk.MustExec("create global binding for select * from t using select * from t")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][0], Equals, "select * from `test` . `t`")
	c.Assert(rows[0][3], Equals, "using")

	rows = tk.MustQuery("select original_sql, status from mysql.bind_info where source != 'builtin';").Rows()
	c.Assert(len(rows), Equals, 2)
	c.Assert(rows[0][1], Equals, "deleted")
	c.Assert(rows[1][1], Equals, "using")
}

func (s *testSuite) TestExplainShowBindSQL(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, key(a))")

	tk.MustExec("create global binding for select * from t using select * from t use index(a)")
	tk.MustQuery("select original_sql, bind_sql from mysql.bind_info where default_db != 'mysql'").Check(testkit.Rows(
		"select * from `test` . `t` SELECT * FROM `test`.`t` USE INDEX (`a`)",
	))

	tk.MustExec("explain format = 'verbose' select * from t")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Using the bindSQL: SELECT * FROM `test`.`t` USE INDEX (`a`)"))
	// explain analyze do not support verbose yet.
}

func (s *testSuite) TestDMLIndexHintBind(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int, key idx_b(b), key idx_c(c))")

	tk.MustExec("delete from t where b = 1 and c > 1")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:idx_b")
	c.Assert(tk.MustUseIndex("delete from t where b = 1 and c > 1", "idx_b(b)"), IsTrue)
	tk.MustExec("create global binding for delete from t where b = 1 and c > 1 using delete from t use index(idx_c) where b = 1 and c > 1")
	tk.MustExec("delete from t where b = 1 and c > 1")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:idx_c")
	c.Assert(tk.MustUseIndex("delete from t where b = 1 and c > 1", "idx_c(c)"), IsTrue)
}

func (s *testSuite) TestCapturedBindingCharset(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	tk.MustExec("use test")
	tk.MustExec("create table t(name varchar(25), index idx(name))")

	tk.MustExec("set character_set_connection = 'ascii'")
	tk.MustExec("update t set name = 'hello' where name <= 'abc'")
	tk.MustExec("update t set name = 'hello' where name <= 'abc'")
	tk.MustExec("set character_set_connection = 'utf8mb4'")
	tk.MustExec("admin capture bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][0], Equals, "update `test` . `t` set `name` = ? where `name` <= ?")
	c.Assert(rows[0][1], Equals, "UPDATE /*+ use_index(@`upd_1` `test`.`t` `idx`)*/ `test`.`t` SET `name`='hello' WHERE `name` <= 'abc'")
	// Charset and Collation are empty now, they are not used currently.
	c.Assert(rows[0][6], Equals, "")
	c.Assert(rows[0][7], Equals, "")
}

func (s *testSuite) TestConcurrentCapture(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	// Simulate an existing binding generated by concurrent CREATE BINDING, which has not been synchronized to current tidb-server yet.
	// Actually, it is more common to be generated by concurrent baseline capture, I use Manual just for simpler test verification.
	tk.MustExec("insert into mysql.bind_info values('select * from `test` . `t`', 'select * from `test` . `t`', '', 'using', '2000-01-01 09:00:00', '2000-01-01 09:00:00', '', '','" +
		bindinfo.Manual + "')")
	tk.MustQuery("select original_sql, source from mysql.bind_info where source != 'builtin'").Check(testkit.Rows(
		"select * from `test` . `t` manual",
	))
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	tk.MustExec("select * from t")
	tk.MustExec("select * from t")
	tk.MustExec("admin capture bindings")
	tk.MustQuery("select original_sql, source, status from mysql.bind_info where source != 'builtin'").Check(testkit.Rows(
		"select * from `test` . `t` manual deleted",
		"select * from `test` . `t` capture using",
	))
}

func (s *testSuite) TestUpdateSubqueryCapture(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int, c int, key idx_b(b))")
	tk.MustExec("create table t2(a int, b int)")
	stmtsummary.StmtSummaryByDigestMap.Clear()
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	tk.MustExec("update t1 set b = 1 where b = 2 and (a in (select a from t2 where b = 1) or c in (select a from t2 where b = 1))")
	tk.MustExec("update t1 set b = 1 where b = 2 and (a in (select a from t2 where b = 1) or c in (select a from t2 where b = 1))")
	tk.MustExec("admin capture bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	bindSQL := "UPDATE /*+ use_index(@`upd_1` `test`.`t1` `idx_b`), use_index(@`sel_1` `test`.`t2` ), hash_join(@`upd_1` `test`.`t1`), use_index(@`sel_2` `test`.`t2` )*/ `test`.`t1` SET `b`=1 WHERE `b` = 2 AND (`a` IN (SELECT `a` FROM `test`.`t2` WHERE `b` = 1) OR `c` IN (SELECT `a` FROM `test`.`t2` WHERE `b` = 1))"
	c.Assert(rows[0][1], Equals, bindSQL)
	tk.MustExec(bindSQL)
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 0)
}

func (s *testSuite) TestIssue20417(c *C) {
	originalVal := config.CheckTableBeforeDrop
	config.CheckTableBeforeDrop = true
	defer func() {
		config.CheckTableBeforeDrop = originalVal
	}()

	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`CREATE TABLE t (
		 pk VARBINARY(36) NOT NULL PRIMARY KEY,
		 b BIGINT NOT NULL,
		 c BIGINT NOT NULL,
		 pad VARBINARY(2048),
		 INDEX idxb(b),
		 INDEX idxc(c)
		)`)

	// Test for create binding
	s.cleanBindingEnv(tk)
	tk.MustExec("create global binding for select * from t using select /*+ use_index(t, idxb) */ * from t")
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][0], Equals, "select * from `test` . `t`")
	c.Assert(rows[0][1], Equals, "SELECT /*+ use_index(`t` `idxb`)*/ * FROM `test`.`t`")
	c.Assert(tk.MustUseIndex("select * from t", "idxb(b)"), IsTrue)
	c.Assert(tk.MustUseIndex("select * from test.t", "idxb(b)"), IsTrue)

	tk.MustExec("create global binding for select * from t WHERE b=2 AND c=3924541 using select /*+ use_index(@sel_1 test.t idxb) */ * from t WHERE b=2 AND c=3924541")
	c.Assert(tk.MustUseIndex("SELECT /*+ use_index(@`sel_1` `test`.`t` `idxc`)*/ * FROM `test`.`t` WHERE `b`=2 AND `c`=3924541", "idxb(b)"), IsTrue)
	c.Assert(tk.MustUseIndex("SELECT /*+ use_index(@`sel_1` `test`.`t` `idxc`)*/ * FROM `t` WHERE `b`=2 AND `c`=3924541", "idxb(b)"), IsTrue)

	// Test for capture baseline
	s.cleanBindingEnv(tk)
	stmtsummary.StmtSummaryByDigestMap.Clear()
	tk.MustExec("set @@tidb_capture_plan_baselines = on")
	s.domain.BindHandle().CaptureBaselines()
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	tk.MustExec("select * from t where b=2 and c=213124")
	tk.MustExec("select * from t where b=2 and c=213124")
	tk.MustExec("admin capture bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][0], Equals, "select * from `test` . `t` where `b` = ? and `c` = ?")
	c.Assert(rows[0][1], Equals, "SELECT /*+ use_index(@`sel_1` `test`.`t` `idxb`)*/ * FROM `test`.`t` WHERE `b` = 2 AND `c` = 213124")
	tk.MustExec("set @@tidb_capture_plan_baselines = off")

	// Test for evolve baseline
	s.cleanBindingEnv(tk)
	tk.MustExec("set @@tidb_evolve_plan_baselines=1")
	tk.MustExec("create global binding for select * from t WHERE c=3924541 using select /*+ use_index(@sel_1 test.t idxb) */ * from t WHERE c=3924541")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][0], Equals, "select * from `test` . `t` where `c` = ?")
	c.Assert(rows[0][1], Equals, "SELECT /*+ use_index(@`sel_1` `test`.`t` `idxb`)*/ * FROM `test`.`t` WHERE `c` = 3924541")
	tk.MustExec("select /*+ use_index(t idxc)*/ * from t where c=3924541")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.IndexNames[0], Equals, "t:idxb")
	tk.MustExec("admin flush bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 2)
	c.Assert(rows[0][0], Equals, "select * from `test` . `t` where `c` = ?")
	c.Assert(rows[0][1], Equals, "SELECT /*+ use_index(@`sel_1` `test`.`t` `idxc`)*/ * FROM `test`.`t` WHERE `c` = 3924541")
	c.Assert(rows[0][3], Equals, "pending verify")
	tk.MustExec("admin evolve bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 2)
	c.Assert(rows[0][0], Equals, "select * from `test` . `t` where `c` = ?")
	c.Assert(rows[0][1], Equals, "SELECT /*+ use_index(@`sel_1` `test`.`t` `idxc`)*/ * FROM `test`.`t` WHERE `c` = 3924541")
	status := rows[0][3].(string)
	c.Assert(status == "using" || status == "rejected", IsTrue)
	tk.MustExec("set @@tidb_evolve_plan_baselines=0")
}

func (s *testSuite) TestForbidEvolvePlanBaseLinesBeforeGA(c *C) {
	originalVal := config.CheckTableBeforeDrop
	config.CheckTableBeforeDrop = false
	defer func() {
		config.CheckTableBeforeDrop = originalVal
	}()

	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	err := tk.ExecToErr("set @@tidb_evolve_plan_baselines=0")
	c.Assert(err, Equals, nil)
	err = tk.ExecToErr("set @@TiDB_Evolve_pLan_baselines=1")
	c.Assert(err, ErrorMatches, "Cannot enable baseline evolution feature, it is not generally available now")
	err = tk.ExecToErr("set @@TiDB_Evolve_pLan_baselines=oN")
	c.Assert(err, ErrorMatches, "Cannot enable baseline evolution feature, it is not generally available now")
	err = tk.ExecToErr("admin evolve bindings")
	c.Assert(err, ErrorMatches, "Cannot enable baseline evolution feature, it is not generally available now")
}

func (s *testSuite) TestCaptureWithZeroSlowLogThreshold(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	stmtsummary.StmtSummaryByDigestMap.Clear()
	c.Assert(tk.Se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil), IsTrue)
	tk.MustExec("set tidb_slow_log_threshold = 0")
	tk.MustExec("select * from t")
	tk.MustExec("select * from t")
	tk.MustExec("set tidb_slow_log_threshold = 300")
	tk.MustExec("admin capture bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][0], Equals, "select * from `test` . `t`")
}

func (s *testSuite) TestExplainTableStmts(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int, value decimal(5,2))")
	tk.MustExec("table t")
	tk.MustExec("explain table t")
	tk.MustExec("desc table t")
}

func (s *testSuite) TestSPMWithoutUseDatabase(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk1 := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	s.cleanBindingEnv(tk1)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, key(a))")
	tk.MustExec("create global binding for select * from t using select * from t force index(a)")

	err := tk1.ExecToErr("select * from t")
	c.Assert(err, ErrorMatches, "*No database selected")
	tk1.MustQuery(`select @@last_plan_from_binding;`).Check(testkit.Rows("0"))
	c.Assert(tk1.MustUseIndex("select * from test.t", "a"), IsTrue)
	tk1.MustExec("select * from test.t")
	tk1.MustQuery(`select @@last_plan_from_binding;`).Check(testkit.Rows("1"))
}

func (s *testSuite) TestBindingWithoutCharset(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(10) CHARACTER SET utf8)")
	tk.MustExec("create global binding for select * from t where a = 'aa' using select * from t where a = 'aa'")
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][0], Equals, "select * from `test` . `t` where `a` = ?")
	c.Assert(rows[0][1], Equals, "SELECT * FROM `test`.`t` WHERE `a` = 'aa'")
}

func (s *testSuite) TestTemporaryTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set tidb_enable_global_temporary_table = true")
	tk.MustExec("create global temporary table t(a int, b int, key(a), key(b)) on commit delete rows")
	tk.MustExec("create table t2(a int, b int, key(a), key(b))")
	tk.MustGetErrCode("create session binding for select * from t where b = 123 using select * from t ignore index(b) where b = 123;", errno.ErrOptOnTemporaryTable)
	tk.MustGetErrCode("create binding for insert into t select * from t2 where t2.b = 1 and t2.c > 1 using insert into t select /*+ use_index(t2,c) */ * from t2 where t2.b = 1 and t2.c > 1", errno.ErrOptOnTemporaryTable)
	tk.MustGetErrCode("create binding for replace into t select * from t2 where t2.b = 1 and t2.c > 1 using replace into t select /*+ use_index(t2,c) */ * from t2 where t2.b = 1 and t2.c > 1", errno.ErrOptOnTemporaryTable)
	tk.MustGetErrCode("create binding for update t set a = 1 where b = 1 and c > 1 using update /*+ use_index(t, c) */ t set a = 1 where b = 1 and c > 1", errno.ErrOptOnTemporaryTable)
	tk.MustGetErrCode("create binding for delete from t where b = 1 and c > 1 using delete /*+ use_index(t, c) */ from t where b = 1 and c > 1", errno.ErrOptOnTemporaryTable)
}

func (s *testSuite) TestBindingLastUpdateTime(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0;")
	tk.MustExec("create table t0(a int, key(a));")
	tk.MustExec("create global binding for select * from t0 using select * from t0 use index(a);")
	tk.MustExec("admin reload bindings;")

	bindHandle := bindinfo.NewBindHandle(tk.Se)
	err := bindHandle.Update(true)
	c.Check(err, IsNil)
	sql, hash := parser.NormalizeDigest("select * from test . t0")
	bindData := bindHandle.GetBindRecord(hash.String(), sql, "test")
	c.Assert(len(bindData.Bindings), Equals, 1)
	bind := bindData.Bindings[0]
	updateTime := bind.UpdateTime.String()

	rows1 := tk.MustQuery("show status like 'last_plan_binding_update_time';").Rows()
	updateTime1 := rows1[0][1]
	c.Assert(updateTime1, Equals, updateTime)

	rows2 := tk.MustQuery("show session status like 'last_plan_binding_update_time';").Rows()
	updateTime2 := rows2[0][1]
	c.Assert(updateTime2, Equals, updateTime)
	tk.MustQuery(`show global status like 'last_plan_binding_update_time';`).Check(testkit.Rows())
}

func (s *testSuite) TestGCBindRecord(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, key(a))")

	tk.MustExec("create global binding for select * from t where a = 1 using select * from t use index(a) where a = 1")
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][0], Equals, "select * from `test` . `t` where `a` = ?")
	c.Assert(rows[0][3], Equals, "using")
	tk.MustQuery("select status from mysql.bind_info where original_sql = 'select * from `test` . `t` where `a` = ?'").Check(testkit.Rows(
		"using",
	))

	h := s.domain.BindHandle()
	// bindinfo.Lease is set to 0 for test env in SetUpSuite.
	c.Assert(h.GCBindRecord(), IsNil)
	rows = tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][0], Equals, "select * from `test` . `t` where `a` = ?")
	c.Assert(rows[0][3], Equals, "using")
	tk.MustQuery("select status from mysql.bind_info where original_sql = 'select * from `test` . `t` where `a` = ?'").Check(testkit.Rows(
		"using",
	))

	tk.MustExec("drop global binding for select * from t where a = 1")
	tk.MustQuery("show global bindings").Check(testkit.Rows())
	tk.MustQuery("select status from mysql.bind_info where original_sql = 'select * from `test` . `t` where `a` = ?'").Check(testkit.Rows(
		"deleted",
	))
	c.Assert(h.GCBindRecord(), IsNil)
	tk.MustQuery("show global bindings").Check(testkit.Rows())
	tk.MustQuery("select status from mysql.bind_info where original_sql = 'select * from `test` . `t` where `a` = ?'").Check(testkit.Rows())
}

func (s *testSerialSuite) TestOptimizeOnlyOnce(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idxa(a))")
	tk.MustExec("create global binding for select * from t using select * from t use index(idxa)")
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/planner/checkOptimizeCountOne", "return"), IsNil)
	tk.MustQuery("select * from t").Check(testkit.Rows())
	c.Assert(failpoint.Disable("github.com/pingcap/tidb/planner/checkOptimizeCountOne"), IsNil)
}
