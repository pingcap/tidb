package cascades_test

import (
	"context"
	"flag"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner/cascades"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/memo"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

var _ = SerialSuites(&testTPCHTransformationSuite{})
type testTPCHTransformationSuite struct {
	*parser.Parser
	testData  testutil.TestData
	is infoschema.InfoSchema
	optimizer *cascades.Optimizer
	sctx sessionctx.Context
}

func (s *testTPCHTransformationSuite) prepareInfoSchema(c *C) {
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	dom, err := session.BootstrapSession(store)
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	tk.MustExec("create database if not exists tpch")
	tk.MustExec("use tpch")
	tk.MustExec(`CREATE TABLE IF NOT EXISTS nation  ( N_NATIONKEY  INTEGER NOT NULL,
                            N_NAME       CHAR(25) NOT NULL,
                            N_REGIONKEY  INTEGER NOT NULL,
                            N_COMMENT    VARCHAR(152),
			    PRIMARY KEY (N_NATIONKEY))`)
	tk.MustExec(`CREATE TABLE IF NOT EXISTS region  ( R_REGIONKEY  INTEGER NOT NULL,
       	               R_NAME       CHAR(25) NOT NULL,
                       R_COMMENT    VARCHAR(152),
	               PRIMARY KEY (R_REGIONKEY))`)
	tk.MustExec(`CREATE TABLE IF NOT EXISTS part  ( P_PARTKEY     INTEGER NOT NULL,
                          P_NAME        VARCHAR(55) NOT NULL,
                          P_MFGR        CHAR(25) NOT NULL,
                          P_BRAND       CHAR(10) NOT NULL,
                          P_TYPE        VARCHAR(25) NOT NULL,
                          P_SIZE        INTEGER NOT NULL,
                          P_CONTAINER   CHAR(10) NOT NULL,
                          P_RETAILPRICE DECIMAL(15,2) NOT NULL,
                          P_COMMENT     VARCHAR(23) NOT NULL,
			  PRIMARY KEY (P_PARTKEY))`)
	tk.MustExec(`CREATE TABLE IF NOT EXISTS supplier  ( S_SUPPKEY     INTEGER NOT NULL,
                             S_NAME        CHAR(25) NOT NULL,
                             S_ADDRESS     VARCHAR(40) NOT NULL,
                             S_NATIONKEY   INTEGER NOT NULL,
                             S_PHONE       CHAR(15) NOT NULL,
                             S_ACCTBAL     DECIMAL(15,2) NOT NULL,
                             S_COMMENT     VARCHAR(101) NOT NULL,
			     PRIMARY KEY (S_SUPPKEY),
			     CONSTRAINT FOREIGN KEY SUPPLIER_FK1 (S_NATIONKEY) references nation(N_NATIONKEY))`)
	tk.MustExec(`CREATE TABLE IF NOT EXISTS partsupp ( PS_PARTKEY     INTEGER NOT NULL,
                             PS_SUPPKEY     INTEGER NOT NULL,
                             PS_AVAILQTY    INTEGER NOT NULL,
                             PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
                             PS_COMMENT     VARCHAR(199) NOT NULL,
			     PRIMARY KEY (PS_PARTKEY,PS_SUPPKEY),
			     CONSTRAINT FOREIGN KEY PARTSUPP_FK1 (PS_SUPPKEY) references supplier(S_SUPPKEY),
			     CONSTRAINT FOREIGN KEY PARTSUPP_FK2 (PS_PARTKEY) references part(P_PARTKEY))`)
	tk.MustExec(`CREATE TABLE IF NOT EXISTS customer  ( C_CUSTKEY     INTEGER NOT NULL,
                             C_NAME        VARCHAR(25) NOT NULL,
                             C_ADDRESS     VARCHAR(40) NOT NULL,
                             C_NATIONKEY   INTEGER NOT NULL,
                             C_PHONE       CHAR(15) NOT NULL,
                             C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
                             C_MKTSEGMENT  CHAR(10) NOT NULL,
                             C_COMMENT     VARCHAR(117) NOT NULL,
			     PRIMARY KEY (C_CUSTKEY),
			     CONSTRAINT FOREIGN KEY CUSTOMER_FK1 (C_NATIONKEY) references nation(N_NATIONKEY))`)
	tk.MustExec(`CREATE TABLE IF NOT EXISTS orders  ( O_ORDERKEY       INTEGER NOT NULL,
                           O_CUSTKEY        INTEGER NOT NULL,
                           O_ORDERSTATUS    CHAR(1) NOT NULL,
                           O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
                           O_ORDERDATE      DATE NOT NULL,
                           O_ORDERPRIORITY  CHAR(15) NOT NULL,  
                           O_CLERK          CHAR(15) NOT NULL, 
                           O_SHIPPRIORITY   INTEGER NOT NULL,
                           O_COMMENT        VARCHAR(79) NOT NULL,
			   PRIMARY KEY (O_ORDERKEY),
			   CONSTRAINT FOREIGN KEY ORDERS_FK1 (O_CUSTKEY) references customer(C_CUSTKEY))`)
	tk.MustExec(`CREATE TABLE IF NOT EXISTS lineitem ( L_ORDERKEY    INTEGER NOT NULL,
                             L_PARTKEY     INTEGER NOT NULL,
                             L_SUPPKEY     INTEGER NOT NULL,
                             L_LINENUMBER  INTEGER NOT NULL,
                             L_QUANTITY    DECIMAL(15,2) NOT NULL,
                             L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
                             L_DISCOUNT    DECIMAL(15,2) NOT NULL,
                             L_TAX         DECIMAL(15,2) NOT NULL,
                             L_RETURNFLAG  CHAR(1) NOT NULL,
                             L_LINESTATUS  CHAR(1) NOT NULL,
                             L_SHIPDATE    DATE NOT NULL,
                             L_COMMITDATE  DATE NOT NULL,
                             L_RECEIPTDATE DATE NOT NULL,
                             L_SHIPINSTRUCT CHAR(25) NOT NULL,
                             L_SHIPMODE     CHAR(10) NOT NULL,
                             L_COMMENT      VARCHAR(44) NOT NULL,
			     PRIMARY KEY (L_ORDERKEY,L_LINENUMBER),
			     CONSTRAINT FOREIGN KEY LINEITEM_FK1 (L_ORDERKEY)  references orders(O_ORDERKEY),
			     CONSTRAINT FOREIGN KEY LINEITEM_FK2 (L_PARTKEY,L_SUPPKEY) references partsupp(PS_PARTKEY, PS_SUPPKEY))`)
	schema := dom.InfoSchema()
	ctx := mock.NewContext()
	ctx.Store = store
	ctx.GetSessionVars().CurrentDB = "tpch"
	dom.CreateStatsHandle(ctx)
	domain.BindDomain(ctx, dom)
	s.sctx = ctx
	s.is = schema
}

func (s *testTPCHTransformationSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
	s.Parser.EnableWindowFunc(true)
	var err error
	s.testData, err = testutil.LoadTestSuiteData("testdata", "tpch_transformation_suite")
	c.Assert(err, IsNil)
	s.prepareInfoSchema(c)
	s.optimizer = cascades.NewOptimizer()
}

func (s *testTPCHTransformationSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

var queryNumber int
func init() {
	flag.IntVar(&queryNumber, "tpch-query", 0, "to specify tpch query")
}

func (s *testTPCHTransformationSuite) TestTPCHQueries(c *C) {
	var input []string
	var output []struct {
		SQL string
		InitMemo []string
		Result []string
	}
	s.testData.GetTestCases(c, &input, &output)
	for i, sql := range input {
		if queryNumber > 0 && queryNumber != i {
			continue
		}
		println(sql)
		stmt, err := s.ParseOneStmt(sql, "", "")
		c.Assert(err, IsNil)
		p, _, err := plannercore.BuildLogicalPlan(context.Background(), s.sctx, stmt, s.is)
		c.Assert(err, IsNil)
		logic, ok := p.(plannercore.LogicalPlan)
		c.Assert(ok, IsTrue)
		initMemo := cascades.ToString(memo.Convert2Group(logic))
		group, err := s.optimizer.LogicalOptimize(s.sctx, logic)
		c.Assert(err, IsNil)
		s.testData.OnRecord(func() {
			output[i].SQL = sql
			output[i].InitMemo = initMemo
			output[i].Result = cascades.ToString(group)
		})
		c.Assert(cascades.ToString(group), DeepEquals, output[i].Result)
	}
}
