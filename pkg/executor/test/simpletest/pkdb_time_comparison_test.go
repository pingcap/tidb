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

package simpletest

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestTimeColumnTemporalComparisonUnderJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("set timestamp = 69006")
	tk.MustExec("create table t_time_join (id int primary key, trx_time time, key(trx_time))")
	tk.MustExec("insert into t_time_join values (1, '19:10:06'), (2, '20:00:00')")

	for _, test := range []struct {
		name     string
		sql      string
		expected string
	}{
		{
			name:     "whereEqual",
			sql:      "select count(*) from t_time_join t1 join t_time_join t2 on t1.id = t2.id where t1.trx_time = cast('1000-01-01 19:10:06' as datetime)",
			expected: "1",
		},
		{
			name:     "whereNotEqual",
			sql:      "select count(*) from t_time_join t1 join t_time_join t2 on t1.id = t2.id where t1.trx_time != cast('1000-01-01 19:10:06' as datetime)",
			expected: "1",
		},
		{
			name:     "whereGreater",
			sql:      "select count(*) from t_time_join t1 join t_time_join t2 on t1.id = t2.id where t1.trx_time > cast('1000-01-01 19:10:06' as datetime)",
			expected: "1",
		},
		{
			name:     "whereGreaterOrEqual",
			sql:      "select count(*) from t_time_join t1 join t_time_join t2 on t1.id = t2.id where t1.trx_time >= cast('1000-01-01 19:10:06' as datetime)",
			expected: "2",
		},
		{
			name:     "whereLess",
			sql:      "select count(*) from t_time_join t1 join t_time_join t2 on t1.id = t2.id where t1.trx_time < cast('1000-01-01 19:10:06' as datetime)",
			expected: "0",
		},
		{
			name:     "whereLessOrEqual",
			sql:      "select count(*) from t_time_join t1 join t_time_join t2 on t1.id = t2.id where t1.trx_time <= cast('1000-01-01 19:10:06' as datetime)",
			expected: "1",
		},
		{
			name:     "on",
			sql:      "select count(*) from t_time_join t1 join t_time_join t2 on t1.id = t2.id and t1.trx_time = cast('1000-01-01 19:10:06' as datetime)",
			expected: "1",
		},
		{
			name:     "onRightColumn",
			sql:      "select count(*) from t_time_join t1 join t_time_join t2 on t1.id = t2.id and t2.trx_time = cast('1000-01-01 19:10:06' as datetime)",
			expected: "1",
		},
		{
			name:     "constantOnLeft",
			sql:      "select count(*) from t_time_join t1 join t_time_join t2 on t1.id = t2.id and cast('1000-01-01 19:10:06' as datetime) = t1.trx_time",
			expected: "1",
		},
		{
			name:     "nullSafeEqual",
			sql:      "select count(*) from t_time_join t1 join t_time_join t2 on t1.id = t2.id and t1.trx_time <=> cast('1000-01-01 19:10:06' as datetime)",
			expected: "1",
		},
		{
			name:     "leftJoinOnRightColumn",
			sql:      "select count(t2.id) from t_time_join t1 left join t_time_join t2 on t1.id = t2.id and t2.trx_time = cast('1000-01-01 19:10:06' as datetime)",
			expected: "1",
		},
		{
			name:     "leftJoinWhereRightColumn",
			sql:      "select count(*) from t_time_join t1 left join t_time_join t2 on t1.id = t2.id where t2.trx_time = cast('1000-01-01 19:10:06' as datetime)",
			expected: "1",
		},
		{
			name:     "derivedColumn",
			sql:      "select count(*) from (select trx_time from t_time_join) d join t_time_join t2 on d.trx_time = t2.trx_time where d.trx_time = cast('1000-01-01 19:10:06' as datetime)",
			expected: "1",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			tk.MustQuery(test.sql).Check(testkit.Rows(test.expected))
		})
	}
}
