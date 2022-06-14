// Copyright 2022 PingCAP, Inc.
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

package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/testkit"
)

func TestIssue33214(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create temporary table t (col enum('a', 'b', 'c') default null)")
	tk.MustExec("insert into t values ('a'), ('b'), ('c'), (null), ('c')")
	tk.MustQuery("select col from t t1 where (select count(*) from t t2 where t2.col = t1.col or t2.col =  'sdf') > 1;").Check(testkit.Rows("c", "c"))
}
