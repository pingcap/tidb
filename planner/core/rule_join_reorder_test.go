// Copyright 2021 PingCAP, Inc.
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
)

var _ = Suite(&testJoinReorderSuite{})

type testJoinReorderSuite struct {
}

func (s*testJoinReorderSuite) TestIssue24095(c *C){
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("select * from t join (select t.* from t join t t1 limit 1);")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (id int, value decimal(10,5));")
	tk.MustExec("desc format = 'brief' select count(*) from t join (select t.id, t.value v1 from t join t t1 on t.id = t1.id order by t.value limit 1) v on v.id = t.id and v.v1 = t.value;")
}