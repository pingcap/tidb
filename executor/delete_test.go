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

package executor_test

import (
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite8) TestDeleteLockKey(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec(`drop table if exists t1, t2, t3, t4;`)

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
			tk1.MustExec("set session tidb_enable_clustered_index=0")
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
			time.Sleep(20 * time.Millisecond)
			tk1.MustExec("commit")
			<-doneCh
			tk2.MustExec("commit")
			wg.Done()
		}(t)
	}
	wg.Wait()
}
