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

package ddl_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/spfreshtest"
)

func TestOrdinaryAddIndexWithSPFreshEnabled(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			// Ordinary index backfills must succeed without contacting SPFresh.
			spfreshtest.SetEnabled(t, enabled)
			store := testkit.CreateMockStore(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t (id int primary key, a int, b int)")
			tk.MustExec("insert into t values (1, 10, 100), (2, 10, 200), (3, 30, 300)")
			tk.MustExec("alter table t add index idx_a (a)")
			tk.MustExec("alter table t add unique index idx_b (b)")
			tk.MustQuery("select id from t force index (idx_a) where a = 10 order by id").Check(testkit.Rows("1", "2"))
			tk.MustQuery("select id from t force index (idx_b) where b = 200").Check(testkit.Rows("2"))
			tk.MustExec("admin check table t")
		})
	}
}
