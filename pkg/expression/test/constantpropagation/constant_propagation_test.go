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

package constantpropagation

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestConstantPropagationMissingCastExpr(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table tl50cb7440 (" +
		"  col_43 decimal(30,30) not null," +
		"  primary key (col_43) /*t![clustered_index] clustered */," +
		"  unique key idx_12 (col_43)," +
		"  key idx_13 (col_43)," +
		"  unique key idx_14 (col_43)" +
		") engine=innodb default charset=utf8 collate=utf8_bin;")
	tk.MustExec("insert into tl50cb7440 values(0.000000000000000000000000000000),(0.400000000000000000000000000000);")
	tk.MustQuery("with cte_8911 (col_47665) as" +
		"  (select mid(tl50cb7440.col_43, 6, 9) as r0" +
		"   from tl50cb7440" +
		"   where tl50cb7440.col_43 in (0, 0) and tl50cb7440.col_43 in (0))" +
		"  (select 1" +
		"   from cte_8911 where cte_8911.col_47665!='');").Check(testkit.Rows("1"))
}
