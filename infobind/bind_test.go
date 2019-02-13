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

package infobind_test

import (
	"flag"
	"fmt"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infobind"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"os"
	"testing"
)

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
	tk.MustExec("drop table if exists mysql.bind_info")
	tk.MustExec(session.CreateBindInfoTable)
}

func (s *testSuite) TestBindParse(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t(i int)")
	tk.MustExec("create index index_t on t(i)")

	originSQL := "select * from t"
	bindSQL := "select * from t use index(index_t)"
	defaultDb := "test"
	status := 1
	charset := "utf8mb4"
	collation := "utf8mb4_bin"
	sql := fmt.Sprintf(`INSERT INTO mysql.bind_info(original_sql,bind_sql,default_db,status,create_time,update_time,charset,collation) VALUES ('%s', '%s', '%s', %d, NOW(), NOW(),'%s', '%s')`,
		originSQL, bindSQL, defaultDb, status, charset, collation)

	tk.MustExec(sql)

	bindHandle := infobind.NewHandle()

	hu := infobind.NewHandleUpdater(bindHandle, s.Parser, tk.Se)

	err := hu.Update(true)
	c.Check(err, IsNil)

	c.Check(len(bindHandle.Get().Cache), Equals, 1)

	fmt.Println("now cache size:", len(bindHandle.Get().Cache))

	fmt.Println("bindHandle.cache:", bindHandle.Get().Cache)
	hash := parser.DigestHash("select * from t")
	bindData := bindHandle.Get().Cache[hash]

	c.Check(bindData, NotNil)
	c.Check(len(bindData), Equals, 1)
	c.Check(bindData[0].OriginalSQL, Equals, "select * from t")
	c.Check(bindData[0].BindSQL, Equals, "select * from t use index(index_t)")
	c.Check(bindData[0].Db, Equals, "test")
	var i int64 = 1
	c.Check(bindData[0].Status, Equals, i)
	c.Check(bindData[0].Charset, Equals, "utf8mb4")
	c.Check(bindData[0].Collation, Equals, "utf8mb4_bin")
	c.Check(bindData[0].CreateTime, NotNil)
	c.Check(bindData[0].UpdateTime, NotNil)
}
