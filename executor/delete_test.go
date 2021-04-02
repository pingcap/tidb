// Copyright 2020 PingCAP, Inc.
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
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite8) TestDeleteLockKey(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec(`drop table if exists t1, t2, t3, t4, t5, t6;`)

	cases := []struct {
		ddl     string
		pre     string
		tk1Stmt string
		tk2Stmt string
	}{
		{
			"create table t1(k int, kk int, val int, primary key(k, kk), unique key(val))",
			"insert into t1 values(1, 2, 3)",
			"delete from t1 where val = 3",
			"insert into t1 values(1, 3, 3)",
		},
		{
			"create table t2(k int, kk int, val int, primary key(k, kk))",
			"insert into t2 values(1, 1, 1)",
			"delete from t2 where k = 1",
			"insert into t2 values(1, 1, 2)",
		},
		{
			"create table t3(k int, kk int, val int, vv int, primary key(k, kk), unique key(val))",
			"insert into t3 values(1, 2, 3, 4)",
			"delete from t3 where vv = 4",
			"insert into t3 values(1, 2, 3, 5)",
		},
		{
			"create table t4(k int, kk int, val int, vv int, primary key(k, kk), unique key(val))",
			"insert into t4 values(1, 2, 3, 4)",
			"delete from t4 where 1",
			"insert into t4 values(1, 2, 3, 5)",
		},
		{
			"create table t5(k int, kk int, val int, vv int, primary key(k, kk), unique key(val))",
			"insert into t5 values(1, 2, 3, 4), (2, 3, 4, 5)",
			"delete from t5 where k in (1, 2, 3, 4)",
			"insert into t5 values(1, 2, 3, 5)",
		},
		{
			"create table t6(k int, kk int, val int, vv int, primary key(k, kk), unique key(val))",
			"insert into t6 values(1, 2, 3, 4), (2, 3, 4, 5)",
			"delete from t6 where kk between 0 and 10",
			"insert into t6 values(1, 2, 3, 5), (2, 3, 4, 6)",
		},
	}
	var wg sync.WaitGroup
	for _, t := range cases {
		wg.Add(1)
		go func(t struct {
			ddl     string
			pre     string
			tk1Stmt string
			tk2Stmt string
		}) {
			tk1, tk2 := testkit.NewTestKit(c, s.store), testkit.NewTestKit(c, s.store)
			tk1.MustExec("use test")
			tk2.MustExec("use test")
			tk1.Se.GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
			tk1.MustExec(t.ddl)
			tk1.MustExec(t.pre)
			tk1.MustExec("begin pessimistic")
			tk2.MustExec("begin pessimistic")
			tk1.MustExec(t.tk1Stmt)
			doneCh := make(chan struct{}, 1)
			go func() {
				tk2.MustExec(t.tk2Stmt)
				doneCh <- struct{}{}
			}()
			time.Sleep(50 * time.Millisecond)
			tk1.MustExec("commit")
			<-doneCh
			tk2.MustExec("commit")
			wg.Done()
		}(t)
	}
	wg.Wait()
}

func (s *testSuite8) TestIssue21200(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists TEST1")
	tk.MustExec("create database TEST1")
	tk.MustExec("use TEST1")
	tk.MustExec("create table t(a int)")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("insert into t1 values(1)")
	tk.MustExec("delete a from t a where exists (select 1 from t1 where t1.a=a.a)")
	tk.MustQuery("select * from t").Check(testkit.Rows())

	tk.MustExec("insert into t values(1), (2)")
	tk.MustExec("insert into t1 values(2)")
	tk.MustExec("prepare stmt from 'delete a from t a where exists (select 1 from t1 where a.a=t1.a and t1.a=?)'")
	tk.MustExec("set @a=1")
	tk.MustExec("execute stmt using @a")
	tk.MustQuery("select * from t").Check(testkit.Rows("2"))
}
