package infobind_test

import (
	"context"
	"flag"
	"fmt"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"os"
	"testing"
	"time"
)

// TestLeakCheckCnt is the check count in the pacakge of executor.
// In this package CustomParallelSuiteFlag is true, so we need to increase check count.
const TestLeakCheckCnt = 1000

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(&logutil.LogConfig{
		Level: logLevel,
	})
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
		session.SetStatsLease(0)
	}
	d, err := session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	d.SetStatsUpdating(true)
	s.domain = d
}

func (s *testSuite) TearDownSuite(c *C) {
	s.domain.Close()
	s.store.Close()
	testleak.AfterTest(c, TestLeakCheckCnt)()
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
	tk.MustExec("drop table if exists mysql.bind_info")
	tk.MustExec(session.CreateBindInfoTable)
}
func (s *testSuite) TestGlobalBinding(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(i int, s varchar(20))")
	tk.MustExec("create index index_t on t(i,s)")

	r, err := tk.Exec("create global binding for select * from t using select * from t use index for join(index_t)")
	c.Assert(err, IsNil, Commentf("err %v", err))
	r, err = tk.Exec("create global binding for select * from t using select * from t use index for join(index_t)")
	c.Assert(err, NotNil)
	time.Sleep(6 * time.Second)
	r, err = tk.Exec("show  global bindings")

	ctx := context.Background()
	chk := r.NewChunk()
	err = r.Next(ctx, chk)
	c.Assert(err, IsNil)
	row := chk.GetRow(0)
	c.Assert(row.Len(), Equals, 6)
	c.Assert(row.GetString(0), Equals, "select * from t")
	c.Assert(row.GetString(1), Equals, "select * from t use index for join(index_t)")
	c.Assert(row.GetString(2), Equals, "test")
	var i int64 = 1
	c.Assert(row.GetInt64(3), Equals, i)
	c.Assert(row.GetDatum(4, types.NewFieldType(mysql.TypeTimestamp)), NotNil)
	c.Assert(row.GetDatum(5, types.NewFieldType(mysql.TypeTimestamp)), NotNil)

	tk.MustExec("DROP global binding for select * from t")
	time.Sleep(6 * time.Second)
	r, err = tk.Exec("show  global bindings")
	chk = r.NewChunk()
	err = r.Next(ctx, chk)
	c.Assert(chk.NumRows(), Equals, 0)
	r, err = tk.Exec("create global binding for select * from t using select * from t use index for join(index_t)")
	c.Assert(err, IsNil)

	tk.MustExec("DROP global binding for select * from t")
	time.Sleep(6 * time.Second)
}

func (s *testSuite) TestFullTableSqlBinding(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(i int, s varchar(20))")
	tk.MustExec("create index index_t on t(i,s)")

	r, err := tk.Exec("create global binding for select * from test.t using select * from test.t use index for join(index_t)")
	c.Assert(err, IsNil, Commentf("err %v", err))

	time.Sleep(6 * time.Second)

	r, err = tk.Exec("show  global bindings")
	ctx := context.Background()
	chk := r.NewChunk()
	err = r.Next(ctx, chk)
	c.Assert(err, IsNil)
	row := chk.GetRow(0)
	c.Assert(row.Len(), Equals, 6)
	c.Assert(row.GetString(0), Equals, "select * from test.t")
	c.Assert(row.GetString(1), Equals, "select * from test.t use index for join(index_t)")
	c.Assert(row.GetString(2), Equals, "")
	var i int64 = 1
	c.Assert(row.GetInt64(3), Equals, i)
	c.Assert(row.GetDatum(4, types.NewFieldType(mysql.TypeTimestamp)), NotNil)
	c.Assert(row.GetDatum(5, types.NewFieldType(mysql.TypeTimestamp)), NotNil)

	tk.MustExec("DROP global binding for select * from test.t")
	time.Sleep(6 * time.Second)
}

func (s *testSuite) TestErrorBinding(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.cleanBindingEnv(tk)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(i int, s varchar(20))")
	tk.MustExec("create index index_t on t(i,s)")

	_, err := tk.Exec("create session binding for select * from test.txxxx using select * from test.txxxx use index for join(index_t)")
	c.Assert(err, NotNil)

	_, err = tk.Exec("create session binding for select * from test.txxxx using select * from t use index for join(index_t)")
	c.Assert(err, NotNil)
}
