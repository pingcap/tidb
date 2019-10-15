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
	"flag"
	"fmt"
	"os"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	dto "github.com/prometheus/client_model/go"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(logutil.NewLogConfig(logLevel, logutil.DefaultLogFormat, "", logutil.EmptyFileLogConfig, false))
	autoid.SetStep(5000)
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
	cluster   *mocktikv.Cluster
	mvccStore mocktikv.MVCCStore
	store     kv.Storage
	domain    *domain.Domain
	*parser.Parser
	ctx *mock.Context
}

var mockTikv = flag.Bool("mockTikv", true, "use mock tikv store in bind test")

func (s *testSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.Parser = parser.New()
	flag.Lookup("mockTikv")
	useMockTikv := *mockTikv
	if useMockTikv {
		s.cluster = mocktikv.NewCluster()
		mocktikv.BootstrapWithSingleStore(s.cluster)
		s.mvccStore = mocktikv.MustNewMVCCStore()
		store, err := mockstore.NewMockTikvStore(
			mockstore.WithCluster(s.cluster),
			mockstore.WithMVCCStore(s.mvccStore),
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
	tk.MustExec("truncate table mysql.bind_info")
	s.domain.BindHandle().Clear()
}

func (s *testSuite) TestBindParse(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t(i int)")
	tk.MustExec("create index index_t on t(i)")

	originSQL := "select * from t"
	bindSQL := "select * from t use index(index_t)"
	defaultDb := "test"
	status := "using"
	charset := "utf8mb4"
	collation := "utf8mb4_bin"
	sql := fmt.Sprintf(`INSERT INTO mysql.bind_info(original_sql,bind_sql,default_db,status,create_time,update_time,charset,collation) VALUES ('%s', '%s', '%s', '%s', NOW(), NOW(),'%s', '%s')`,
		originSQL, bindSQL, defaultDb, status, charset, collation)
	tk.MustExec(sql)
	bindHandle := bindinfo.NewBindHandle(tk.Se)
	err := bindHandle.Update(true)
	c.Check(err, IsNil)
	c.Check(bindHandle.Size(), Equals, 1)

	sql, hash := parser.NormalizeDigest("select * from t")
	bindData := bindHandle.GetBindRecord(hash, sql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalSQL, Equals, "select * from t")
	c.Check(bindData.BindSQL, Equals, "select * from t use index(index_t)")
	c.Check(bindData.Db, Equals, "test")
	c.Check(bindData.Status, Equals, "using")
	c.Check(bindData.Charset, Equals, "utf8mb4")
	c.Check(bindData.Collation, Equals, "utf8mb4_bin")
	c.Check(bindData.CreateTime, NotNil)
	c.Check(bindData.UpdateTime, NotNil)

	// Test fields with quotes or slashes.
	sql = `CREATE GLOBAL BINDING FOR  select * from t where a BETWEEN "a" and "b" USING select * from t use index(idx) where a BETWEEN "a\nb\rc\td\0e" and "x"`
	tk.MustExec(sql)
	tk.MustExec(`DROP global binding for select * from t use index(idx) where a BETWEEN "a\nb\rc\td\0e" and "x"`)
}

func (s *testSuite) TestGlobalBinding(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(i int, s varchar(20))")
	tk.MustExec("create table t1(i int, s varchar(20))")
	tk.MustExec("create index index_t on t(i,s)")

	metrics.BindTotalGauge.Reset()
	metrics.BindMemoryUsage.Reset()

	_, err := tk.Exec("create global binding for select * from t where i>100 using select * from t use index(index_t) where i>100")
	c.Assert(err, IsNil, Commentf("err %v", err))

	_, err = tk.Exec("create global binding for select * from t where i>99 using select * from t use index(index_t) where i>99")
	c.Assert(err, IsNil)

	pb := &dto.Metric{}
	metrics.BindTotalGauge.WithLabelValues(metrics.ScopeGlobal, bindinfo.Using).Write(pb)
	c.Assert(pb.GetGauge().GetValue(), Equals, float64(1))
	metrics.BindMemoryUsage.WithLabelValues(metrics.ScopeGlobal, bindinfo.Using).Write(pb)
	c.Assert(pb.GetGauge().GetValue(), Equals, float64(121))

	sql, hash := parser.NormalizeDigest("select * from t where i > ?")

	bindData := s.domain.BindHandle().GetBindRecord(hash, sql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalSQL, Equals, "select * from t where i > ?")
	c.Check(bindData.BindSQL, Equals, "select * from t use index(index_t) where i>99")
	c.Check(bindData.Db, Equals, "test")
	c.Check(bindData.Status, Equals, "using")
	c.Check(bindData.Charset, NotNil)
	c.Check(bindData.Collation, NotNil)
	c.Check(bindData.CreateTime, NotNil)
	c.Check(bindData.UpdateTime, NotNil)

	rs, err := tk.Exec("show global bindings")
	c.Assert(err, IsNil)
	chk := rs.NewChunk()
	err = rs.Next(context.TODO(), chk)
	c.Check(err, IsNil)
	c.Check(chk.NumRows(), Equals, 1)
	row := chk.GetRow(0)
	c.Check(row.GetString(0), Equals, "select * from t where i > ?")
	c.Check(row.GetString(1), Equals, "select * from t use index(index_t) where i>99")
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
	c.Check(bindData.OriginalSQL, Equals, "select * from t where i > ?")
	c.Check(bindData.BindSQL, Equals, "select * from t use index(index_t) where i>99")
	c.Check(bindData.Db, Equals, "test")
	c.Check(bindData.Status, Equals, "using")
	c.Check(bindData.Charset, NotNil)
	c.Check(bindData.Collation, NotNil)
	c.Check(bindData.CreateTime, NotNil)
	c.Check(bindData.UpdateTime, NotNil)

	_, err = tk.Exec("DROP global binding for select * from t where i>100")
	c.Check(err, IsNil)
	bindData = s.domain.BindHandle().GetBindRecord(hash, sql, "test")
	c.Check(bindData, IsNil)

	metrics.BindTotalGauge.WithLabelValues(metrics.ScopeGlobal, bindinfo.Using).Write(pb)
	c.Assert(pb.GetGauge().GetValue(), Equals, float64(0))
	metrics.BindMemoryUsage.WithLabelValues(metrics.ScopeGlobal, bindinfo.Using).Write(pb)
	// From newly created global bind handle.
	c.Assert(pb.GetGauge().GetValue(), Equals, float64(121))

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

	_, err = tk.Exec("delete from mysql.bind_info")
	c.Assert(err, IsNil)

	_, err = tk.Exec("create global binding for select * from t using select * from t1 use index for join(index_t)")
	c.Assert(err, NotNil, Commentf("err %v", err))
}

func (s *testSuite) TestSessionBinding(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(i int, s varchar(20))")
	tk.MustExec("create table t1(i int, s varchar(20))")
	tk.MustExec("create index index_t on t(i,s)")

	metrics.BindTotalGauge.Reset()
	metrics.BindMemoryUsage.Reset()

	_, err := tk.Exec("create session binding for select * from t where i>100 using select * from t use index(index_t) where i>100")
	c.Assert(err, IsNil, Commentf("err %v", err))

	_, err = tk.Exec("create session binding for select * from t where i>99 using select * from t use index(index_t) where i>99")
	c.Assert(err, IsNil)

	pb := &dto.Metric{}
	metrics.BindTotalGauge.WithLabelValues(metrics.ScopeSession, bindinfo.Using).Write(pb)
	c.Assert(pb.GetGauge().GetValue(), Equals, float64(1))
	metrics.BindMemoryUsage.WithLabelValues(metrics.ScopeSession, bindinfo.Using).Write(pb)
	c.Assert(pb.GetGauge().GetValue(), Equals, float64(121))

	handle := tk.Se.Value(bindinfo.SessionBindInfoKeyType).(*bindinfo.SessionHandle)
	bindData := handle.GetBindRecord("select * from t where i > ?", "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalSQL, Equals, "select * from t where i > ?")
	c.Check(bindData.BindSQL, Equals, "select * from t use index(index_t) where i>99")
	c.Check(bindData.Db, Equals, "test")
	c.Check(bindData.Status, Equals, "using")
	c.Check(bindData.Charset, NotNil)
	c.Check(bindData.Collation, NotNil)
	c.Check(bindData.CreateTime, NotNil)
	c.Check(bindData.UpdateTime, NotNil)

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
	c.Check(row.GetString(0), Equals, "select * from t where i > ?")
	c.Check(row.GetString(1), Equals, "select * from t use index(index_t) where i>99")
	c.Check(row.GetString(2), Equals, "test")
	c.Check(row.GetString(3), Equals, "using")
	c.Check(row.GetTime(4), NotNil)
	c.Check(row.GetTime(5), NotNil)
	c.Check(row.GetString(6), NotNil)
	c.Check(row.GetString(7), NotNil)

	_, err = tk.Exec("drop session binding for select * from t where i>99")
	c.Assert(err, IsNil)
	bindData = handle.GetBindRecord("select * from t where i > ?", "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalSQL, Equals, "select * from t where i > ?")
	c.Check(bindData.Status, Equals, "deleted")

	metrics.BindTotalGauge.WithLabelValues(metrics.ScopeSession, bindinfo.Using).Write(pb)
	c.Assert(pb.GetGauge().GetValue(), Equals, float64(0))
	metrics.BindMemoryUsage.WithLabelValues(metrics.ScopeSession, bindinfo.Using).Write(pb)
	c.Assert(pb.GetGauge().GetValue(), Equals, float64(0))
}

func (s *testSuite) TestGlobalAndSessionBindingBothExist(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(id int)")
	tk.MustExec("create table t2(id int)")
	c.Assert(tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "HashLeftJoin"), IsTrue)
	c.Assert(tk.HasPlan("SELECT  /*+ TIDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id", "MergeJoin"), IsTrue)

	tk.MustExec("create global binding for SELECT * from t1,t2 where t1.id = t2.id using SELECT  /*+ TIDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id")

	metrics.BindUsageCounter.Reset()
	c.Assert(tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "MergeJoin"), IsTrue)
	pb := &dto.Metric{}
	metrics.BindUsageCounter.WithLabelValues(metrics.ScopeGlobal).Write(pb)
	c.Assert(pb.GetCounter().GetValue(), Equals, float64(1))
	tk.MustExec("set @@tidb_use_plan_baselines = 0")
	c.Assert(tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "HashLeftJoin"), IsTrue)

	tk.MustExec("drop global binding for SELECT * from t1,t2 where t1.id = t2.id")
	tk.MustExec("set @@tidb_use_plan_baselines = 1")
	c.Assert(tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "HashLeftJoin"), IsTrue)
}

func (s *testSuite) TestExplain(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(id int)")
	tk.MustExec("create table t2(id int)")

	c.Assert(tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "HashLeftJoin"), IsTrue)
	c.Assert(tk.HasPlan("SELECT  /*+ TIDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id", "MergeJoin"), IsTrue)

	tk.MustExec("create global binding for SELECT * from t1,t2 where t1.id = t2.id using SELECT  /*+ TIDB_SMJ(t1, t2) */  * from t1,t2 where t1.id = t2.id")

	c.Assert(tk.HasPlan("SELECT * from t1,t2 where t1.id = t2.id", "MergeJoin"), IsTrue)

	tk.MustExec("drop global binding for SELECT * from t1,t2 where t1.id = t2.id")
}

func (s *testSuite) TestErrorBind(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(i int, s varchar(20))")
	tk.MustExec("create table t1(i int, s varchar(20))")
	tk.MustExec("create index index_t on t(i,s)")

	_, err := tk.Exec("create global binding for select * from t where i>100 using select * from t use index(index_t) where i>100")
	c.Assert(err, IsNil, Commentf("err %v", err))

	sql, hash := parser.NormalizeDigest("select * from t where i > ?")
	bindData := s.domain.BindHandle().GetBindRecord(hash, sql, "test")
	c.Check(bindData, NotNil)
	c.Check(bindData.OriginalSQL, Equals, "select * from t where i > ?")
	c.Check(bindData.BindSQL, Equals, "select * from t use index(index_t) where i>100")
	c.Check(bindData.Db, Equals, "test")
	c.Check(bindData.Status, Equals, "using")
	c.Check(bindData.Charset, NotNil)
	c.Check(bindData.Collation, NotNil)
	c.Check(bindData.CreateTime, NotNil)
	c.Check(bindData.UpdateTime, NotNil)

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
}

func (s *testSuite) TestCapturePlanBaseline(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("set @@tidb_enable_stmt_summary = on")
	tk.MustExec(" set @@tidb_capture_plan_baselines = on")
	defer func() {
		tk.MustExec("set @@tidb_enable_stmt_summary = off")
		tk.MustExec(" set @@tidb_capture_plan_baselines = off")
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	s.domain.BindHandle().CaptureBaselines(s.domain.InfoSchema())
	tk.MustQuery("show global bindings").Check(testkit.Rows())
	tk.MustExec("select * from t")
	tk.MustExec("select * from t")
	s.domain.BindHandle().CaptureBaselines(s.domain.InfoSchema())
	rows := tk.MustQuery("show global bindings").Rows()
	c.Assert(len(rows), Equals, 1)
	c.Assert(rows[0][0], Equals, "select * from t")
	c.Assert(rows[0][1], Equals, "select /*+ USE_INDEX(@`sel_1` `t` )*/ * from t")
}
