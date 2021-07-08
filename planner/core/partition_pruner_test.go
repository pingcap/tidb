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

package core_test

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

var _ = Suite(&testPartitionPruneSuit{})

type testPartitionPruneSuit struct {
	store    kv.Storage
	dom      *domain.Domain
	ctx      sessionctx.Context
	testData testutil.TestData
}

func (s *testPartitionPruneSuit) cleanEnv(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_partition")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
}

func (s *testPartitionPruneSuit) SetUpSuite(c *C) {
	var err error
	s.store, s.dom, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.ctx = mock.NewContext()
}

func (s *testPartitionPruneSuit) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
}

func (s *testPartitionPruneSuit) TestIssue23622(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("USE test;")
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t2 (a int, b int) partition by range (a) (partition p0 values less than (0), partition p1 values less than (5));")
	tk.MustExec("insert into t2(a) values (-1), (1);")
	tk.MustQuery("select * from t2 where a > 10 or b is NULL order by a;").Check(testkit.Rows("-1 <nil>", "1 <nil>"))
}
