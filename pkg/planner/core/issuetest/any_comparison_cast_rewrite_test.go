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

package issuetest

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestAnyComparisonCastRewrite(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table lhs (v double)")
	tk.MustExec("create table rhs (v char(20))")
	tk.MustExec("insert into lhs values (0.0001), (0), (2), (-1)")
	tk.MustExec("insert into rhs values ('1'), ('3'), (' '), ('w')")

	maxQuery := "select v from lhs where (-v) <= any (select v from rhs) order by v"
	tk.MustQuery(maxQuery).Check(testkit.Rows("-1", "0", "0.0001", "2"))
	maxPlan := fmt.Sprint(tk.MustQuery("explain format='brief' " + maxQuery).Rows())
	require.Contains(t, maxPlan, "max(cast(test.rhs.v, double BINARY))")

	tk.MustExec("truncate table lhs")
	tk.MustExec("truncate table rhs")
	tk.MustExec("insert into lhs values (1), (2), (5), (10)")
	tk.MustExec("insert into rhs values ('2'), ('10')")

	minQuery := "select v from lhs where v >= any (select v from rhs) order by v"
	tk.MustQuery(minQuery).Check(testkit.Rows("2", "5", "10"))
	minPlan := fmt.Sprint(tk.MustQuery("explain format='brief' " + minQuery).Rows())
	require.Contains(t, minPlan, "min(cast(test.rhs.v, double BINARY))")
}
