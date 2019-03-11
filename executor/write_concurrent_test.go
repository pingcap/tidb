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

package executor_test

import (
	"context"
	"flag"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&concurrentWriteSuite{})

type concurrentWriteSuite struct {
	cluster   *mocktikv.Cluster
	mvccStore mocktikv.MVCCStore
	store     kv.Storage
	domain    *domain.Domain
	*parser.Parser
	ctx *mock.Context

	autoIDStep int64
}

func (s *concurrentWriteSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.autoIDStep = autoid.GetStep()
	autoid.SetStep(5000)
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
	s.domain = d
}

func (s *concurrentWriteSuite) TearDownSuite(c *C) {
	s.domain.Close()
	s.store.Close()
	autoid.SetStep(s.autoIDStep)
	testleak.AfterTest(c)()
}

func (s *concurrentWriteSuite) TestBatchInsertWithOnDuplicate(c *C) {
	tk := testkit.NewCTestKit(c, s.store)
	// prepare schema.
	ctx := tk.OpenSessionWitDB(context.Background(), "test")
	tk.MustExec(ctx, "drop table if exists duplicate_test")
	tk.MustExec(ctx, "create table duplicate_test(id int auto_increment, k1 int, primary key(id), unique key uk(k1))")
	tk.MustExec(ctx, "insert into duplicate_test(k1) values(?),(?),(?),(?),(?)", tk.PermInt(5)...)

	tk.ConcurrentRun(c, 3, 2, // concurrent: 3, loops: 2,
		// prepare data for each loop.
		func(ctx context.Context, tk *testkit.CTestKit, concurrent int, currentLoop int) [][][]interface{} {
			var ii [][][]interface{}
			for i := 0; i < concurrent; i++ {
				ii = append(ii, [][]interface{}{tk.PermInt(7)})
			}
			return ii
		},
		// concurrent execute logic.
		func(ctx context.Context, tk *testkit.CTestKit, input [][]interface{}) {
			tk.MustExec(ctx, "set @@session.tidb_batch_insert=1")
			tk.MustExec(ctx, "set @@session.tidb_dml_batch_size=1")
			_, err := tk.Exec(ctx, "insert ignore into duplicate_test(k1) values (?),(?),(?),(?),(?),(?),(?)", input[0]...)
			tk.IgnoreError(err)
		},
		// check after all done.
		func(ctx context.Context, tk *testkit.CTestKit) {
			tk.MustExec(ctx, "admin check table duplicate_test")
			tk.MustQuery(ctx, "select d1.id, d1.k1 from duplicate_test d1 ignore index(uk), duplicate_test d2 use index (uk) where d1.id = d2.id and d1.k1 <> d2.k1").
				Check(testkit.Rows())
		})
}
