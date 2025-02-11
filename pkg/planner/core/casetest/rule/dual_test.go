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

package rule

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestDual(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT,d INT);")
	tk.MustQuery("explain select a from (select d as a from t where d = 0) k where k.a = 5").Check(testkit.Rows(
		"TableDual_8 0.00 root  rows:0"))
	tk.MustQuery("select a from (select d as a from t where d = 0) k where k.a = 5").Check(testkit.Rows())
	tk.MustQuery("explain select a from (select 1+2 as a from t where d = 0) k where k.a = 5").Check(testkit.Rows(
		"Projection_8 0.00 root  3->Column#3",
		"└─TableDual_9 0.00 root  rows:0"))
	tk.MustQuery("select a from (select 1+2 as a from t where d = 0) k where k.a = 5").Check(testkit.Rows())
	tk.MustQuery("explain select * from t where d != null;").Check(testkit.Rows(
		"TableDual_6 0.00 root  rows:0"))
	tk.MustQuery("explain select * from t where d > null;").Check(testkit.Rows(
		"TableDual_6 0.00 root  rows:0"))
	tk.MustQuery("explain select * from t where d >= null;").Check(testkit.Rows(
		"TableDual_6 0.00 root  rows:0"))
	tk.MustQuery("explain select * from t where d < null;").Check(testkit.Rows(
		"TableDual_6 0.00 root  rows:0"))
	tk.MustQuery("explain select * from t where d <= null;").Check(testkit.Rows(
		"TableDual_6 0.00 root  rows:0"))
	tk.MustQuery("explain select * from t where d = null;").Check(testkit.Rows(
		"TableDual_6 0.00 root  rows:0"))
}
