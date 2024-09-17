// Copyright 2024 PingCAP, Inc.
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

package indexadvisor_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func checkResult(t *testing.T, tk *testkit.TestKit, sql, indexes string) {
	results := tk.MustQuery(sql).Rows()
	resultIndexes := make([]string, 0, len(results))
	for _, r := range results {
		resultIndexes = append(resultIndexes, fmt.Sprintf("%v.%v.%v", r[0], r[1], r[3]))
	}
	sort.Strings(resultIndexes)
	require.Equal(t, indexes, strings.Join(resultIndexes, "|"))
}

func TestIndexAdvisorForSQL(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int)`)

	checkResult(t, tk, `recommend index run for 'select a from t where a=1'`, "test.t.a")
	checkResult(t, tk, `recommend index run for 'select a from t where a=1 and b=1'`, "test.t.a,b")
	checkResult(t, tk, `recommend index run for 'select a from t where a=1 and c=1'`, "test.t.a,c")
}
