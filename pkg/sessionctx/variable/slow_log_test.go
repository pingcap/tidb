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
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func newMockCtx() sessionctx.Context {
	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx = stmtctx.NewStmtCtx()
	ctx.GetSessionVars().SlowLogRules = &variable.SlowLogRules{
		Rules:              []variable.SlowLogRule{},
		AllConditionFields: make(map[string]struct{}),
	}
	return ctx
}

func TestSlowLogFieldAccessor(t *testing.T) {
	for field, accessor := range variable.SlowLogRuleFieldAccessors {
		require.Equalf(t, strings.ToLower(field), field, "field %q: field name should be all lowercase", field)
		require.NotNilf(t, accessor.Parse, "field %q: Parse function is missing", field)
		require.NotNilf(t, accessor.Match, "field %q: Match function is missing", field)
		if accessor.Setter == nil {
			if field == strings.ToLower(variable.SlowLogParseTimeStr) ||
				field == strings.ToLower(variable.SlowLogCompileTimeStr) ||
				field == strings.ToLower(variable.SlowLogOptimizeTimeStr) ||
				field == strings.ToLower(variable.SlowLogWaitTSTimeStr) ||
				field == strings.ToLower(variable.SlowLogIsInternalStr) ||
				field == strings.ToLower(variable.SlowLogConnIDStr) ||
				field == strings.ToLower(variable.SlowLogSessAliasStr) ||
				field == strings.ToLower(variable.SlowLogDBStr) {
				continue
			}
			require.NotNilf(t, accessor.Setter, "field %q: Setter function is missing", field)
		}
	}
}

func TestMatchSingleRuleSingleCondition(t *testing.T) {
	ctx := newMockCtx()
	items := &variable.SlowQueryLogItems{MemMax: 200}

	rule := variable.SlowLogRule{
		Conditions: []variable.SlowLogCondition{{
			Field:     variable.SlowLogMemMax,
			Threshold: int64(100),
		}},
	}
	ctx.GetSessionVars().SlowLogRules.Rules = []variable.SlowLogRule{rule}

	require.True(t, executor.Match(ctx.GetSessionVars(), items)) // 200 >= 100
	items.MemMax = 50
	require.False(t, executor.Match(ctx.GetSessionVars(), items)) // 50 < 100
}

func TestMatchSingleRuleMultipleConditions(t *testing.T) {
	ctx := newMockCtx()
	items := &variable.SlowQueryLogItems{
		MemMax: 200,
		Digest: "abc",
		Succ:   true,
	}

	rule := variable.SlowLogRule{
		Conditions: []variable.SlowLogCondition{
			{Field: variable.SlowLogMemMax, Threshold: int64(100)},
			{Field: variable.SlowLogDigestStr, Threshold: "abc"},
			{Field: variable.SlowLogSucc, Threshold: true},
		},
	}
	ctx.GetSessionVars().SlowLogRules.Rules = []variable.SlowLogRule{rule}

	require.True(t, executor.Match(ctx.GetSessionVars(), items))

	items.Succ = false
	require.False(t, executor.Match(ctx.GetSessionVars(), items))
}

func TestMatchMultipleRulesOR(t *testing.T) {
	ctx := newMockCtx()
	sessVars := ctx.GetSessionVars()
	items := &variable.SlowQueryLogItems{}
	items.ExecRetryCount = 5
	items.Digest = "abc"
	items.Succ = true
	items.MemMax = 200

	// rule 1: requires ExecRetryCount >= 3 AND Succ == true
	rule1 := variable.SlowLogRule{
		Conditions: []variable.SlowLogCondition{
			{Field: variable.SlowLogExecRetryCount, Threshold: uint64(3)},
			{Field: variable.SlowLogSucc, Threshold: true},
		},
	}
	// rule 2: requires MemMax >= 500 (not satisfied)
	rule2 := variable.SlowLogRule{
		Conditions: []variable.SlowLogCondition{
			{Field: variable.SlowLogMemMax, Threshold: int64(500)},
		},
	}

	sessVars.SlowLogRules = &variable.SlowLogRules{Rules: []variable.SlowLogRule{rule1, rule2}}

	// should match rule1, return true
	require.True(t, executor.Match(ctx.GetSessionVars(), items))

	// change ExecRetryCount smaller -> no match
	items.ExecRetryCount = 1
	require.False(t, executor.Match(ctx.GetSessionVars(), items))

	// test string matching
	items.Digest = "plan_digest"
	rule3 := variable.SlowLogRule{
		Conditions: []variable.SlowLogCondition{
			{Field: variable.SlowLogDigestStr, Threshold: "plan_digest"},
		},
	}
	sessVars.SlowLogRules = &variable.SlowLogRules{Rules: []variable.SlowLogRule{rule3}}
	require.True(t, executor.Match(ctx.GetSessionVars(), items))
}

func TestMatchDifferentTypesAfterParse(t *testing.T) {
	ctx := newMockCtx()
	items := &variable.SlowQueryLogItems{
		MemMax:            123,                     // int64
		DiskMax:           456,                     // int64
		ExecRetryCount:    uint64(789),             // uint64
		ResourceGroupName: "rg1",                   // string
		Succ:              true,                    // bool
		TimeTotal:         3140 * time.Millisecond, // time.Duration
	}

	slowLogRules, err := variable.ParseSlowLogRules(`Mem_max: 100, Exec_retry_count: 300, Succ: true, Query_time: 2.52, Resource_group: rg1`)
	require.NoError(t, err)
	ctx.GetSessionVars().SlowLogRules = slowLogRules
	require.True(t, executor.Match(ctx.GetSessionVars(), items))
}

func TestParseSingleSlowLogField(t *testing.T) {
	require.Equal(t, len(variable.SlowLogRuleFieldAccessors), 37)
	accessor, ok := variable.SlowLogRuleFieldAccessors[strings.ToLower(variable.SlowLogPlanDigest)]
	require.True(t, ok)
	require.NotNil(t, accessor.Setter)
	require.NotNil(t, accessor.Match)

	// int64 fields
	v, err := variable.ParseSlowLogFieldValue(variable.SlowLogMemMax, "123")
	require.NoError(t, err)
	require.Equal(t, int64(123), v)

	_, err = variable.ParseSlowLogFieldValue(variable.SlowLogMemMax, "abc")
	require.Error(t, err)

	// uint64 fields
	v, err = variable.ParseSlowLogFieldValue(variable.SlowLogConnIDStr, "456")
	require.NoError(t, err)
	require.Equal(t, uint64(456), v)

	_, err = variable.ParseSlowLogFieldValue(variable.SlowLogConnIDStr, "-1")
	require.Error(t, err)

	// float64 fields
	v, err = variable.ParseSlowLogFieldValue(variable.SlowLogQueryTimeStr, "1.234")
	require.NoError(t, err)
	require.Equal(t, 1.234, v)

	_, err = variable.ParseSlowLogFieldValue(variable.SlowLogQueryTimeStr, "abc")
	require.Error(t, err)

	// string fields
	v, err = variable.ParseSlowLogFieldValue(variable.SlowLogDBStr, "testdb")
	require.NoError(t, err)
	require.Equal(t, "testdb", v)

	// bool fields
	v, err = variable.ParseSlowLogFieldValue(variable.SlowLogSucc, "true")
	require.NoError(t, err)
	require.Equal(t, true, v)

	v, err = variable.ParseSlowLogFieldValue(variable.SlowLogSucc, "false")
	require.NoError(t, err)
	require.Equal(t, false, v)

	v, err = variable.ParseSlowLogFieldValue(variable.SlowLogSucc, "notabool")
	require.Error(t, err)
	require.Equal(t, false, v)

	// unknown field
	v, err = variable.ParseSlowLogFieldValue("NonExistField", "xxx")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown slow log field name")
	require.Nil(t, v)
}

func compareConditionsUnordered(t *testing.T, got, want []variable.SlowLogCondition) {
	require.Equal(t, len(got), len(want))
	gotMap := make(map[string]any, len(got))
	for _, c := range got {
		gotMap[strings.ToLower(c.Field)] = c.Threshold
	}
	for _, c := range want {
		val, ok := gotMap[strings.ToLower(c.Field)]
		require.True(t, ok)
		require.Equal(t, c.Threshold, val)
	}
}

func TestParseSlowLogRules(t *testing.T) {
	// normal tests
	// a rule that ends without a ';'
	slowLogRules, err := variable.ParseSlowLogRules(`Conn_ID: 123, DB: 'db1', Succ: true, Query_time: 0.5276, Resource_group: "rg1""`)
	require.NoError(t, err)
	rules := []variable.SlowLogRule{
		{
			Conditions: []variable.SlowLogCondition{
				{Field: strings.ToLower(variable.SlowLogConnIDStr), Threshold: uint64(123)},
				{Field: strings.ToLower(variable.SlowLogDBStr), Threshold: "db1"},
				{Field: strings.ToLower(variable.SlowLogSucc), Threshold: true},
				{Field: strings.ToLower(variable.SlowLogQueryTimeStr), Threshold: 0.5276},
				{Field: strings.ToLower(variable.SlowLogResourceGroup), Threshold: "rg1"},
			},
		},
	}
	allConditionFields := map[string]struct{}{
		strings.ToLower(variable.SlowLogConnIDStr):     {},
		strings.ToLower(variable.SlowLogDBStr):         {},
		strings.ToLower(variable.SlowLogSucc):          {},
		strings.ToLower(variable.SlowLogQueryTimeStr):  {},
		strings.ToLower(variable.SlowLogResourceGroup): {},
	}
	compareConditionsUnordered(t, rules[0].Conditions, slowLogRules.Rules[0].Conditions)
	require.Equal(t, allConditionFields, slowLogRules.AllConditionFields)
	// a rule that ends with a ';'
	slowLogRules, err = variable.ParseSlowLogRules(`Conn_ID: 123, DB: db1, Succ: true, Query_time: 0.5276, Resource_group: rg1;`)
	require.NoError(t, err)
	compareConditionsUnordered(t, rules[0].Conditions, slowLogRules.Rules[0].Conditions)
	require.Equal(t, allConditionFields, slowLogRules.AllConditionFields)
	// some rules
	slowLogRules, err = variable.ParseSlowLogRules(`Conn_ID: 123, DB: db1, Succ: true, Query_time: 0.5276, Resource_group: rg1;
		Conn_ID: 124, DB: db2, Succ: false, Query_time: 1.5276`)
	require.NoError(t, err)
	rules = []variable.SlowLogRule{
		{
			Conditions: []variable.SlowLogCondition{
				{Field: strings.ToLower(variable.SlowLogConnIDStr), Threshold: uint64(123)},
				{Field: strings.ToLower(variable.SlowLogDBStr), Threshold: "db1"},
				{Field: strings.ToLower(variable.SlowLogSucc), Threshold: true},
				{Field: strings.ToLower(variable.SlowLogQueryTimeStr), Threshold: 0.5276},
				{Field: strings.ToLower(variable.SlowLogResourceGroup), Threshold: "rg1"},
			},
		},
		{
			Conditions: []variable.SlowLogCondition{
				{Field: strings.ToLower(variable.SlowLogConnIDStr), Threshold: uint64(124)},
				{Field: strings.ToLower(variable.SlowLogDBStr), Threshold: "db2"},
				{Field: strings.ToLower(variable.SlowLogSucc), Threshold: false},
				{Field: strings.ToLower(variable.SlowLogResourceGroup), Threshold: 1.5276},
			},
		},
	}
	compareConditionsUnordered(t, rules[0].Conditions, slowLogRules.Rules[0].Conditions)
	require.Equal(t, allConditionFields, slowLogRules.AllConditionFields)

	// return nil
	slowLogRules, err = variable.ParseSlowLogRules("  ")
	require.NoError(t, err)
	require.Nil(t, slowLogRules)

	// return an empty rule
	emptySlowLogRules := &variable.SlowLogRules{
		AllConditionFields: make(map[string]struct{}),
		Rules:              make([]variable.SlowLogRule, 0, len(rules))}
	slowLogRules, err = variable.ParseSlowLogRules("  ; ; ")
	require.NoError(t, err)
	require.Equal(t, emptySlowLogRules, slowLogRules)

	// exceeding the limit set by the rules
	longRules := strings.Repeat("Conn_ID:1;", 11)
	_, err = variable.ParseSlowLogRules(longRules)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid slow log rules count")
	// the field has ','
	slowLogRules, err = variable.ParseSlowLogRules(`DB:"a,b", Succ:true`)
	require.NoError(t, err)
	require.Equal(t, 2, len(slowLogRules.Rules[0].Conditions))

	// Format error
	_, err = variable.ParseSlowLogRules("Conn_ID 123")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid slow log rule format:Conn_ID 123")
	_, err = variable.ParseSlowLogRules("Conn_ID > 123")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid slow log rule format:Conn_ID > 123")

	// Field resetting
	slowLogRules, err = variable.ParseSlowLogRules("Mem_max:100,Succ:true,Succ:false,Mem_max:200")
	require.NoError(t, err)
	require.Len(t, slowLogRules.Rules, 1)
	m := make(map[string]any)
	for _, cond := range slowLogRules.Rules[0].Conditions {
		m[cond.Field] = cond.Threshold
	}
	require.Equal(t, int64(200), m[strings.ToLower(variable.SlowLogMemMax)])
	require.Equal(t, false, m[strings.ToLower(variable.SlowLogSucc)])

	// empty fields in a single rule
	slowLogRules, err = variable.ParseSlowLogRules("Conn_ID:1,  , DB:db")
	require.NoError(t, err)
	require.Equal(t, 2, len(slowLogRules.Rules[0].Conditions))
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
