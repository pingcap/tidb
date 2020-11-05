package test

import (

	"flag"
	"fmt"

	"os"

	"testing"



	. "github.com/pingcap/check"

	"github.com/pingcap/parser"

	"github.com/pingcap/tidb/config"

	"github.com/pingcap/tidb/domain"

	"github.com/pingcap/tidb/kv"

	"github.com/pingcap/tidb/meta/autoid"

	"github.com/pingcap/tidb/session"

	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/cluster"

	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mock"

	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"

)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	*CustomParallelSuiteFlag = true
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(logutil.NewLogConfig(logLevel, logutil.DefaultLogFormat, "", logutil.EmptyFileLogConfig, false))
	autoid.SetStep(5000)

	config.UpdateGlobal(func(conf *config.Config) {
		conf.Log.SlowThreshold = 30000 // 30s
	})
	tmpDir := config.GetGlobalConfig().TempStoragePath
	_ = os.RemoveAll(tmpDir) // clean the uncleared temp file during the last run.
	_ = os.MkdirAll(tmpDir, 0755)
	testleak.BeforeTest()
	TestingT(t)
	testleak.AfterTestT(t)()
}

type baseTestSuite struct {
	cluster cluster.Cluster
	store   kv.Storage
	domain  *domain.Domain
	*parser.Parser
	ctx *mock.Context
}

type testSuiteWithData struct {
	*baseTestSuite
	testData testutil.TestData
}

var mockTikv = flag.Bool("mockTikv", true, "use mock tikv store in executor test")
var _ = Suite(&testSuiteJoin3{&baseTestSuite{}})

func (s *baseTestSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
	flag.Lookup("mockTikv")
	useMockTikv := *mockTikv
	if useMockTikv {
		store, err := mockstore.NewMockStore(
			mockstore.WithClusterInspector(func(c cluster.Cluster) {
				mockstore.BootstrapWithSingleStore(c)
				s.cluster = c
			}),
		)
		c.Assert(err, IsNil)
		s.store = store
		session.SetSchemaLease(0)
		session.DisableStats4Test()
	}
	d, err := session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	d.SetStatsUpdating(true)
	s.domain = d
	config.UpdateGlobal(func(conf *config.Config) {
		conf.OOMAction = config.OOMActionLog
	})
}

func (s *testSuiteWithData) SetUpSuite(c *C) {
	s.baseTestSuite.SetUpSuite(c)
	var err error
	s.testData, err = testutil.LoadTestSuiteData("testdata", "executor_suite")
	c.Assert(err, IsNil)
}

func (s *testSuiteWithData) TearDownSuite(c *C) {
	s.baseTestSuite.TearDownSuite(c)
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *baseTestSuite) TearDownSuite(c *C) {
	s.domain.Close()
	s.store.Close()
}

type testSuiteJoin3 struct {
	*baseTestSuite
}

func (s *testSuiteJoin3) TestSubquery(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	//tk.MustExec("set @@tidb_hash_join_concurrency=1")
	//tk.MustExec("set @@tidb_hashagg_partial_concurrency=1")
	//tk.MustExec("set @@tidb_hashagg_final_concurrency=1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int, d int)")
	//tk.MustExec("create table test.t1(a bigint, b bigint, index idx_a(a), index idx_b(b));")
	//tk.MustExec("create table test.t2(a bigint, b bigint, index idx_a(a), index idx_b(b));")
	tk.MustExec("insert t values (1, 1)")
	tk.MustExec("insert t values (2, 2)")
	tk.MustExec("insert t values (3, 4)")
	//tk.MustExec("insert t values (4, 4)")
	//tk.MustExec("insert t values (5, 3)")
	tk.MustExec("commit")

	//tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES'")

	//result := tk.MustQuery("select * from t where exists(select * from t k where t.c = k.c having sum(c) = 1)")
	//result.Check(testkit.Rows("1 1"))
	//result = tk.MustQuery("select * from t where exists(select k.c, k.d from t k, t p where t.c = k.d)")
	//result.Check(testkit.Rows("1 1", "2 2"))
	fmt.Println("============================================================")
	//result := tk.MustQuery("select 1 = (select count(*) from t where t.c = k.d) from t k")
	//result := tk.MustQuery("select 1 from (select /*+ HASH_JOIN(t1) */ t1.a in (select t2.a from t2) from t1) x;")
	result := tk.MustQuery("select (select count(*) from t s, t t1 where s.c = t.c and s.c = t1.c) from t;")

	//result := tk.MustQuery("select c from t where 4 in (select max(t1.d) from t as t1 where t.c = t1.d)")
	//result := tk.MustQuery("select t1.d from t t1 where t1.d in (select t2.c from t t2 order by t1.c+t2.c limit 1)")
	fmt.Println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", result.Rows())
	result.Check(testkit.Rows("1", "1", "0"))
}
