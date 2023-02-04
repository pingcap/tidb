// Copyright 2015 PingCAP, Inc.
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

package ddl_test

import (
	"testing"

	"github.com/pingcap/tidb/testkit"
)

func TestDropColumnWithCompositeIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int, index i_ab(a, b), index i_a(a))")
	tk.MustExec("insert into t values(1, 1, 1), (2, 2, 2), (3, 3, 3)")
	tk.MustExec("alter table t drop column a")
	query := queryIndexOnTable("test", "t")
	tk.MustQuery(query).Check(testkit.Rows("i_ab YES"))
}

func TestDropColumnWithMultiCompositeIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int, d int, index i_abc(a, b, c), index i_acd(a, c, d))")
	tk.MustExec("insert into t values(1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3)")
	tk.MustExec("alter table t drop column a")
	query := queryIndexOnTable("test", "t")
	tk.MustQuery(query).Check(testkit.Rows("i_abc YES", "i_acd YES"))
	tk.MustQuery("select c, d from t where c > 2").Check(testkit.Rows("3 3"))
	tk.MustQuery("select b, c from t where c > 2").Check(testkit.Rows("3 3"))
}
