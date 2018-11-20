// Copyright 2018 PingCAP, Inc.
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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testExpressionRewriterSuite{})

type testExpressionRewriterSuite struct {
}

func (s *testExpressionRewriterSuite) TestIfNullEliminateColName(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null, b int not null)")
	rs, err := tk.Exec("select ifnull(a,b) from t")
	c.Assert(err, IsNil)
	fields := rs.Fields()
	c.Assert(fields[0].Column.Name.L, Equals, "ifnull(a,b)")
}

func (s *testExpressionRewriterSuite) TestBinaryOpFunction(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t(a int, b int, c int);")
	tk.MustExec("INSERT INTO t VALUES (1, 2, 3), (NULL, 2, 3  ), (1, NULL, 3),(1, 2,   NULL),(NULL, 2, 3+1), (1, NULL, 3+1), (1, 2+1, NULL),(NULL, 2, 3-1), (1, NULL, 3-1), (1, 2-1, NULL)")
	tk.MustQuery("SELECT * FROM t WHERE (a,b,c) <= (1,2,3) order by b").Check(testkit.Rows("1 1 <nil>", "1 2 3"))
	tk.MustQuery("SELECT * FROM t WHERE (a,b,c) > (1,2,3) order by b").Check(testkit.Rows("1 3 <nil>"))
}
