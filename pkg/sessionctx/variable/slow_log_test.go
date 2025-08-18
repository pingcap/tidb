// Copyright 2025 PingCAP, Inc.
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

package variable_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestGetValidValueByName(t *testing.T) {
	// int64 fields
	v, err := variable.GetValidValueByName(variable.SlowLogMemMax, "123")
	require.NoError(t, err)
	require.Equal(t, int64(123), v)

	_, err = variable.GetValidValueByName(variable.SlowLogMemMax, "abc")
	require.Error(t, err)

	// uint64 fields
	v, err = variable.GetValidValueByName(variable.SlowLogConnIDStr, "456")
	require.NoError(t, err)
	require.Equal(t, uint64(456), v)

	_, err = variable.GetValidValueByName(variable.SlowLogConnIDStr, "-1") // invalid uint64
	require.Error(t, err)

	// float64 fields
	v, err = variable.GetValidValueByName(variable.SlowLogQueryTimeStr, "1.234")
	require.NoError(t, err)
	require.Equal(t, 1.234, v)

	_, err = variable.GetValidValueByName(variable.SlowLogQueryTimeStr, "abc")
	require.Error(t, err)

	// string fields
	v, err = variable.GetValidValueByName(variable.SlowLogDBStr, "testdb")
	require.NoError(t, err)
	require.Equal(t, "testdb", v)

	// bool fields
	v, err = variable.GetValidValueByName(variable.SlowLogSucc, "true")
	require.NoError(t, err)
	require.Equal(t, true, v)

	v, err = variable.GetValidValueByName(variable.SlowLogSucc, "false")
	require.NoError(t, err)
	require.Equal(t, false, v)

	_, err = variable.GetValidValueByName(variable.SlowLogSucc, "notabool")
	require.Error(t, err)

	// unknown field
	v, err = variable.GetValidValueByName("NonExistField", "xxx")
	require.NoError(t, err)
	require.Nil(t, v)
}

func TestParseSlowLogRules(t *testing.T) {
	require.Equal(t, len(variable.SlowLogRuleFields), 32)

	// a normal test
	slowLogRules, err := variable.ParseSlowLogRules(`Conn_ID: 123, DB: db1, Succ: true, Query_time: 0.5276, Resource_group: rg1;
		Conn_ID: 124, DB: db2, Succ: false, Query_time: 1.5276`)
	require.NoError(t, err)
	rules := []variable.SlowLogRule{
		{
			Conditions: []variable.SlowLogCondition{
				{Field: "Conn_ID", Threshold: uint64(123)},
				{Field: "DB", Threshold: "db1"},
				{Field: "Succ", Threshold: true},
				{Field: "Query_time", Threshold: 0.5276},
				{Field: "Resource_group", Threshold: "rg1"},
			},
		},
		{
			Conditions: []variable.SlowLogCondition{
				{Field: "Conn_ID", Threshold: uint64(124)},
				{Field: "DB", Threshold: "db2"},
				{Field: "Succ", Threshold: false},
				{Field: "Query_time", Threshold: 1.5276},
			},
		},
	}
	allConditionFields := map[string]struct{}{
		"Conn_ID":        {},
		"DB":             {},
		"Succ":           {},
		"Query_time":     {},
		"Resource_group": {},
	}
	require.Equal(t, rules, slowLogRules.Rules)
	require.Equal(t, allConditionFields, slowLogRules.AllConditionFields)

	// return nil
	slowLogRules, err = variable.ParseSlowLogRules("")
	require.NoError(t, err)
	require.Nil(t, rules)
	slowLogRules, err = variable.ParseSlowLogRules("  ; ; ")
	require.NoError(t, err)
	require.Nil(t, rules)

	// exceeding the limit set by the rules
	longRules := strings.Repeat("Conn_ID:1;", 11)
	_, err = variable.ParseSlowLogRules(longRules)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid slow log rules count")

	// unknown field
	_, err = variable.ParseSlowLogRules("UnknownField: 1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown slow log field name")

	// Format error
	_, err = variable.ParseSlowLogRules("Conn_ID 123")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid slow log field format")
	_, err = variable.ParseSlowLogRules("Conn_ID > 123")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid slow log field format")

	// empty fields in a single rule
	slowLogRules, err = variable.ParseSlowLogRules("Conn_ID:1,  , DB:db")
	require.NoError(t, err)
	require.Len(t, rules, 1)
	require.Len(t, rules[0].Conditions, 2)
	require.Contains(t, rules[0].Conditions[0].Field, "Conn_ID")
	require.Contains(t, rules[0].Conditions[1].Field, "DB")
}

func BenchmarkSlowLog(b *testing.B) {
	b.StopTimer()
	b.ReportAllocs()

	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, v int)")
	tk.MustExec("insert into t values (1,1), (2,2)")
	se := tk.Session()
	stmt, err := parser.New().ParseOneStmt("select * from t", "", "")
	require.NoError(b, err)
	compiler := executor.Compiler{Ctx: se}
	execStmt, err := compiler.Compile(context.TODO(), stmt)
	require.NoError(b, err)

	ts := oracle.GoTimeToTS(time.Now())

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		execStmt.LogSlowQuery(ts, true, false)
	}
}
