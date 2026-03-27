// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package integration_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestIssue66827OuterJoinWithIsNotTrue(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t0, t1")
	tk.MustExec("create table t0(c0 int)")
	tk.MustExec("create table t1(c0 int)")
	tk.MustExec("insert into t0 values (0)")
	tk.MustExec("insert into t1 values (0)")

	tk.MustQuery("select t0.c0, t1.c0 from t0 left join t1 on t0.c0=t1.c0").Check(testkit.Rows("0 0"))
	tk.MustQuery("select t0.c0, t1.c0 from t0 left join t1 on t0.c0=t1.c0 where (t0.c0=t1.c0) is not true").Check(testkit.Rows())
	tk.MustQuery("select t0.c0, t1.c0 from t0 right join t1 on t0.c0=t1.c0 where (t0.c0=t1.c0) is not true").Check(testkit.Rows())
}
