// Copyright 2023 PingCAP, Inc.
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

package collation

import (
	"testing"

	"github.com/pingcap/tidb/testkit"
)

func TestUTF8MB40900AICIOrder(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test;")
	tk.MustExec("create table t (id int primary key auto_increment, str VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci)")
	tk.MustExec("insert into t(str) values ('ｶ'), ('カ'), ('abc'), ('abuFFFEc'), ('abⓒ'), ('𝒶bc'), ('𝕒bc'), ('ガ'), ('が'), ('abç'), ('äbc'), ('ヵ'), ('か'), ('Abc'), ('abC'), ('File-3'), ('file-12'), ('filé-110'), ('🍣'), ('🍺')")
	tk.MustQuery("select min(id) from t group by str order by str").Check(testkit.Rows(
		"19", "20", "3", "4", "18", "17", "16", "1"))
}

func TestUTF8MB40900AICIStrFunc(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test;")
	// test locate
	tk.MustQuery("select LOCATE('bar' collate utf8mb4_0900_ai_ci, 'FOOBAR' collate utf8mb4_0900_ai_ci)").Check(
		testkit.Rows("4"),
	)
	// test regexp
	tk.MustQuery("select 'FOOBAR' collate utf8mb4_0900_ai_ci REGEXP 'foo.*' collate utf8mb4_0900_ai_ci").Check(
		testkit.Rows("1"),
	)
}
