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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/slowlogrule"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func newMockCtx() sessionctx.Context {
	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx = stmtctx.NewStmtCtx()
	ctx.GetSessionVars().SlowLogRules = slowlogrule.NewSessionSlowLogRules(&slowlogrule.SlowLogRules{
		Rules:  []*slowlogrule.SlowLogRule{},
		Fields: make(map[string]struct{}),
	})
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

	rule := &slowlogrule.SlowLogRule{
		Conditions: []slowlogrule.SlowLogCondition{{
			Field:     variable.SlowLogMemMax,
			Threshold: int64(100),
		}},
	}
	ctx.GetSessionVars().SlowLogRules.Rules = []*slowlogrule.SlowLogRule{rule}

	require.True(t, executor.Match(ctx.GetSessionVars(), items, ctx.GetSessionVars().SlowLogRules.SlowLogRules)) // 200 >= 100
	items.MemMax = 50
	require.False(t, executor.Match(ctx.GetSessionVars(), items, ctx.GetSessionVars().SlowLogRules.SlowLogRules)) // 50 < 100
}

func TestMatchSpecialTypeConditions(t *testing.T) {
	ctx := newMockCtx()
	items := &variable.SlowQueryLogItems{
		Succ:              true,
		Digest:            "abc",
		ResourceGroupName: "rg_Test",
	}

	t.Run("string type", func(t *testing.T) {
		ctx.GetSessionVars().CurrentDB = "db_Test"
		ctx.GetSessionVars().SessionAlias = "seA"
		rule := &slowlogrule.SlowLogRule{
			Conditions: []slowlogrule.SlowLogCondition{
				{Field: variable.SlowLogSucc, Threshold: true},
				{Field: variable.SlowLogDigestStr, Threshold: "abc"},
				{Field: variable.SlowLogResourceGroup, Threshold: "rg_test"},
				{Field: variable.SlowLogDBStr, Threshold: "db_test"},
				{Field: variable.SlowLogSessAliasStr, Threshold: "seA"},
			},
		}
		ctx.GetSessionVars().SlowLogRules.Rules = []*slowlogrule.SlowLogRule{rule}

		require.True(t, executor.Match(ctx.GetSessionVars(), items, ctx.GetSessionVars().SlowLogRules.SlowLogRules))

		// test SessionAlias
		ctx.GetSessionVars().SessionAlias = "sea"
		require.False(t, executor.Match(ctx.GetSessionVars(), items, ctx.GetSessionVars().SlowLogRules.SlowLogRules))
		// test Digest
		ctx.GetSessionVars().SessionAlias = "seA"
		items.Digest = "abC"
		require.False(t, executor.Match(ctx.GetSessionVars(), items, ctx.GetSessionVars().SlowLogRules.SlowLogRules))
	})

	checkRet := func(expectMatch bool, condition slowlogrule.SlowLogCondition) {
		rule := &slowlogrule.SlowLogRule{
			Conditions: []slowlogrule.SlowLogCondition{
				condition,
			},
		}
		ctx.GetSessionVars().SlowLogRules.Rules = []*slowlogrule.SlowLogRule{rule}

		if expectMatch {
			require.True(t, executor.Match(ctx.GetSessionVars(), items, ctx.GetSessionVars().SlowLogRules.SlowLogRules))
		} else {
			require.False(t, executor.Match(ctx.GetSessionVars(), items, ctx.GetSessionVars().SlowLogRules.SlowLogRules))
		}
	}

	t.Run("util.ExecDetails type", func(t *testing.T) {
		checkRet(false, slowlogrule.SlowLogCondition{Field: variable.SlowLogKVTotal, Threshold: 0.00000001})
		checkRet(true, slowlogrule.SlowLogCondition{Field: variable.SlowLogKVTotal, Threshold: 0.0})
		tikvExecDetail := util.ExecDetails{
			WaitKVRespDuration: (10 * time.Second).Nanoseconds(),
		}
		childCtx := context.WithValue(context.Background(), util.ExecDetailsKey, &tikvExecDetail)
		accessor := variable.SlowLogRuleFieldAccessors[strings.ToLower(variable.SlowLogKVTotal)]
		accessor.Setter(childCtx, nil, items)
		checkRet(true, slowlogrule.SlowLogCondition{Field: variable.SlowLogKVTotal, Threshold: 0.00000001})
		checkRet(true, slowlogrule.SlowLogCondition{Field: variable.SlowLogKVTotal, Threshold: 0.0})
	})

	t.Run("execdetails.ExecDetails type", func(t *testing.T) {
		seVar := ctx.GetSessionVars()
		seVar.StmtCtx.SyncExecDetails.Reset()

		checkRet(false, slowlogrule.SlowLogCondition{Field: execdetails.ProcessTimeStr, Threshold: float64(1)})
		checkRet(false, slowlogrule.SlowLogCondition{Field: execdetails.TotalKeysStr, Threshold: uint64(2)})
		checkRet(false, slowlogrule.SlowLogCondition{Field: execdetails.PreWriteTimeStr, Threshold: 0.123})
		checkRet(false, slowlogrule.SlowLogCondition{Field: execdetails.PrewriteRegionStr, Threshold: int64(4)})
		// ExecDetail == nil
		checkRet(true, slowlogrule.SlowLogCondition{Field: execdetails.ProcessTimeStr, Threshold: float64(0)})
		checkRet(true, slowlogrule.SlowLogCondition{Field: execdetails.TotalKeysStr, Threshold: uint64(0)})
		checkRet(true, slowlogrule.SlowLogCondition{Field: execdetails.PreWriteTimeStr, Threshold: 0.0})
		checkRet(true, slowlogrule.SlowLogCondition{Field: execdetails.PrewriteRegionStr, Threshold: int64(0)})
		// ExecDetail != nil && d.CommitDetail == nil && d.ScanDetail == nil
		execDetail := &execdetails.ExecDetails{
			CopExecDetails: execdetails.CopExecDetails{
				BackoffTime: time.Millisecond,
			},
		}
		seVar.StmtCtx.SyncExecDetails.MergeCopExecDetails(&execDetail.CopExecDetails, 0)
		accessor := variable.SlowLogRuleFieldAccessors[strings.ToLower(execdetails.TotalKeysStr)]
		accessor.Setter(context.Background(), seVar, items)
		checkRet(true, slowlogrule.SlowLogCondition{Field: execdetails.ProcessTimeStr, Threshold: float64(0)})
		checkRet(true, slowlogrule.SlowLogCondition{Field: execdetails.TotalKeysStr, Threshold: uint64(0)})
		checkRet(true, slowlogrule.SlowLogCondition{Field: execdetails.PreWriteTimeStr, Threshold: 0.0})
		checkRet(true, slowlogrule.SlowLogCondition{Field: execdetails.PrewriteRegionStr, Threshold: int64(0)})
	})
}

func TestMatchSingleRuleMultipleConditions(t *testing.T) {
	ctx := newMockCtx()
	items := &variable.SlowQueryLogItems{
		MemMax:            200,
		Digest:            "abc",
		Succ:              true,
		WriteSQLRespTotal: 1.5e6, // time.Duration
	}

	rule := &slowlogrule.SlowLogRule{
		Conditions: []slowlogrule.SlowLogCondition{
			{Field: variable.SlowLogMemMax, Threshold: int64(100)},
			{Field: variable.SlowLogDigestStr, Threshold: "abc"},
			{Field: variable.SlowLogSucc, Threshold: true},
			{Field: variable.SlowLogWriteSQLRespTotal, Threshold: 0.0015},
		},
	}
	ctx.GetSessionVars().SlowLogRules.Rules = []*slowlogrule.SlowLogRule{rule}

	require.True(t, executor.Match(ctx.GetSessionVars(), items, ctx.GetSessionVars().SlowLogRules.SlowLogRules))

	items.Succ = false
	require.False(t, executor.Match(ctx.GetSessionVars(), items, ctx.GetSessionVars().SlowLogRules.SlowLogRules))
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
	rule1 := &slowlogrule.SlowLogRule{
		Conditions: []slowlogrule.SlowLogCondition{
			{Field: variable.SlowLogExecRetryCount, Threshold: uint64(3)},
			{Field: variable.SlowLogSucc, Threshold: true},
		},
	}
	// rule 2: requires MemMax >= 500 (not satisfied)
	rule2 := &slowlogrule.SlowLogRule{
		Conditions: []slowlogrule.SlowLogCondition{
			{Field: variable.SlowLogMemMax, Threshold: int64(500)},
		},
	}

	sessVars.SlowLogRules = slowlogrule.NewSessionSlowLogRules(&slowlogrule.SlowLogRules{Rules: []*slowlogrule.SlowLogRule{rule1, rule2}})

	// should match rule1, return true
	require.True(t, executor.Match(ctx.GetSessionVars(), items, ctx.GetSessionVars().SlowLogRules.SlowLogRules))

	// change ExecRetryCount smaller -> no match
	items.ExecRetryCount = 1
	require.False(t, executor.Match(ctx.GetSessionVars(), items, ctx.GetSessionVars().SlowLogRules.SlowLogRules))

	// test string matching
	items.Digest = "plan_digest"
	rule3 := &slowlogrule.SlowLogRule{
		Conditions: []slowlogrule.SlowLogCondition{
			{Field: variable.SlowLogDigestStr, Threshold: "plan_digest"},
		},
	}
	sessVars.SlowLogRules = slowlogrule.NewSessionSlowLogRules(&slowlogrule.SlowLogRules{Rules: []*slowlogrule.SlowLogRule{rule3}})
	require.True(t, executor.Match(ctx.GetSessionVars(), items, ctx.GetSessionVars().SlowLogRules.SlowLogRules))
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

	slowLogRules, err := variable.ParseSessionSlowLogRules(`Mem_max: 100, Exec_retry_count: 300, Succ: true, Query_time: 2.52, Resource_group: rg1`)
	require.NoError(t, err)
	ctx.GetSessionVars().SlowLogRules.SlowLogRules = slowLogRules
	require.True(t, executor.Match(ctx.GetSessionVars(), items, ctx.GetSessionVars().SlowLogRules.SlowLogRules))
}

func TestParseSingleSlowLogField(t *testing.T) {
	require.Equal(t, len(variable.SlowLogRuleFieldAccessors), 38)
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
	v, err = variable.ParseSlowLogFieldValue(variable.SlowLogQueryTimeStr, "1.5e6")
	require.NoError(t, err)
	require.Equal(t, 1.5e6, v)

	_, err = variable.ParseSlowLogFieldValue(variable.SlowLogQueryTimeStr, "abc")
	require.Error(t, err)

	v, err = variable.ParseSlowLogFieldValue(variable.SlowLogMemArbitration, "1.2345")
	require.NoError(t, err)
	require.Equal(t, 1.2345, v)

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

func compareConditionsUnordered(t *testing.T, got, want []slowlogrule.SlowLogCondition, rawRule string) {
	require.Equal(t, len(got), len(want))
	gotMap := make(map[string]any, len(got))
	for _, c := range got {
		gotMap[strings.ToLower(c.Field)] = c.Threshold
	}
	rawRule = strings.ToLower(rawRule)
	for _, c := range want {
		field := strings.ToLower(c.Field)
		val, ok := gotMap[field]
		require.True(t, ok)
		kv := fmt.Sprintf("%s: %v", field, val)
		require.True(t, strings.Contains(rawRule, kv),
			fmt.Sprintf("rawRule:%v, substr: %s", rawRule, kv))
		require.Equal(t, c.Threshold, val)
	}
}

func TestParseSessionSlowLogRules(t *testing.T) {
	// normal tests
	// a rule that ends without a ';'
	slowLogRules, err := variable.ParseSessionSlowLogRules(`Conn_ID: 123, DB: db1, Succ: true, Query_time: 0.5276, Resource_group: rg1`)
	require.EqualError(t, err, "do not allow ConnID value:123")
	require.Nil(t, slowLogRules)
	rawRule := `Exec_retry_count: 10, DB: db1, Succ: true, Query_time: 0.5276, Resource_group: rg1`
	slowLogRules, err = variable.ParseSessionSlowLogRules(rawRule)
	require.NoError(t, err)
	rules := []slowlogrule.SlowLogRule{
		{
			Conditions: []slowlogrule.SlowLogCondition{
				{Field: strings.ToLower(variable.SlowLogExecRetryCount), Threshold: uint64(10)},
				{Field: strings.ToLower(variable.SlowLogDBStr), Threshold: "db1"},
				{Field: strings.ToLower(variable.SlowLogSucc), Threshold: true},
				{Field: strings.ToLower(variable.SlowLogQueryTimeStr), Threshold: 0.5276},
				{Field: strings.ToLower(variable.SlowLogResourceGroup), Threshold: "rg1"},
			},
		},
	}
	allConditionFields := map[string]struct{}{
		strings.ToLower(variable.SlowLogExecRetryCount): {},
		strings.ToLower(variable.SlowLogDBStr):          {},
		strings.ToLower(variable.SlowLogSucc):           {},
		strings.ToLower(variable.SlowLogQueryTimeStr):   {},
		strings.ToLower(variable.SlowLogResourceGroup):  {},
	}
	compareConditionsUnordered(t, rules[0].Conditions, slowLogRules.Rules[0].Conditions, rawRule)
	require.Equal(t, allConditionFields, slowLogRules.Fields)
	// a rule that ends with a ';'
	slowLogRules, err = variable.ParseSessionSlowLogRules(`Exec_retry_count: 10, DB: db1, Succ: true, Query_time: 0.5276, Resource_group: rg1;`)
	require.NoError(t, err)
	compareConditionsUnordered(t, rules[0].Conditions, slowLogRules.Rules[0].Conditions, rawRule)
	require.Equal(t, allConditionFields, slowLogRules.Fields)
	// some rules
	rawRule1 := "Exec_retry_count: 123, DB: db1, Succ: true, Query_time: 0.5276, Resource_group: rg1;"
	rawRule2 := "Exec_retry_count: 124, DB: db2, Succ: false, Query_time: 1.5276"
	slowLogRules, err = variable.ParseSessionSlowLogRules(rawRule1 + rawRule2)
	require.NoError(t, err)
	rules = []slowlogrule.SlowLogRule{
		{
			Conditions: []slowlogrule.SlowLogCondition{
				{Field: strings.ToLower(variable.SlowLogExecRetryCount), Threshold: uint64(123)},
				{Field: strings.ToLower(variable.SlowLogDBStr), Threshold: "db1"},
				{Field: strings.ToLower(variable.SlowLogSucc), Threshold: true},
				{Field: strings.ToLower(variable.SlowLogQueryTimeStr), Threshold: 0.5276},
				{Field: strings.ToLower(variable.SlowLogResourceGroup), Threshold: "rg1"},
			},
		},
		{
			Conditions: []slowlogrule.SlowLogCondition{
				{Field: strings.ToLower(variable.SlowLogExecRetryCount), Threshold: uint64(124)},
				{Field: strings.ToLower(variable.SlowLogDBStr), Threshold: "db2"},
				{Field: strings.ToLower(variable.SlowLogSucc), Threshold: false},
				{Field: strings.ToLower(variable.SlowLogQueryTimeStr), Threshold: 1.5276},
			},
		},
	}
	compareConditionsUnordered(t, rules[0].Conditions, slowLogRules.Rules[0].Conditions, rawRule1)
	compareConditionsUnordered(t, rules[1].Conditions, slowLogRules.Rules[1].Conditions, rawRule2)
	require.Equal(t, allConditionFields, slowLogRules.Fields)

	// return nil
	slowLogRules, err = variable.ParseSessionSlowLogRules("  ")
	require.NoError(t, err)
	require.Nil(t, slowLogRules)
	slowLogRules, err = variable.ParseSessionSlowLogRules("  ; ; ")
	require.NoError(t, err)
	require.Nil(t, slowLogRules)

	// exceeding the limit set by the rules
	longRules := strings.Repeat("Conn_ID:1;", 11)
	_, err = variable.ParseSessionSlowLogRules(longRules)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid slow log rules count")
	// the field has ','
	slowLogRules, err = variable.ParseSessionSlowLogRules(`DB:"a,b", Succ:true`)
	require.NoError(t, err)
	require.Equal(t, 2, len(slowLogRules.Rules[0].Conditions))

	// Format error
	_, err = variable.ParseSessionSlowLogRules("Exec_retry_count 123")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid slow log rule format:Exec_retry_count 123")
	_, err = variable.ParseSessionSlowLogRules("Exec_retry_count > 123")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid slow log rule format:Exec_retry_count > 123")

	// Field resetting
	slowLogRules, err = variable.ParseSessionSlowLogRules("Mem_max:100,Succ:true,Succ:false,Mem_max:200")
	require.NoError(t, err)
	require.Len(t, slowLogRules.Rules, 1)
	m := make(map[string]any)
	for _, cond := range slowLogRules.Rules[0].Conditions {
		m[cond.Field] = cond.Threshold
	}
	require.Equal(t, int64(200), m[strings.ToLower(variable.SlowLogMemMax)])
	require.Equal(t, false, m[strings.ToLower(variable.SlowLogSucc)])

	// empty fields in a single rule
	slowLogRules, err = variable.ParseSessionSlowLogRules("Exec_retry_count:1,  , DB:db")
	require.NoError(t, err)
	require.Equal(t, 2, len(slowLogRules.Rules[0].Conditions))
}

func checkRuleByField(t *testing.T, rules []*slowlogrule.SlowLogRule, field, val string) {
	for _, r := range rules {
		for _, cond := range r.Conditions {
			if cond.Field == field {
				require.Equal(t, cond.Threshold, val)
			}
		}
	}
}

func TestParseGlobalSlowLogRules(t *testing.T) {
	// normal tests
	rawRule := `Conn_ID: 123, Exec_retry_count: 10, DB: db1, Succ: true, Query_time: 0.5276, Resource_group: rg1`
	slowLogRuleSet, err := variable.ParseGlobalSlowLogRules(rawRule)
	require.NoError(t, err)
	rules := []slowlogrule.SlowLogRule{
		{
			Conditions: []slowlogrule.SlowLogCondition{
				{Field: strings.ToLower(variable.SlowLogConnIDStr), Threshold: uint64(123)},
				{Field: strings.ToLower(variable.SlowLogExecRetryCount), Threshold: uint64(10)},
				{Field: strings.ToLower(variable.SlowLogDBStr), Threshold: "db1"},
				{Field: strings.ToLower(variable.SlowLogSucc), Threshold: true},
				{Field: strings.ToLower(variable.SlowLogQueryTimeStr), Threshold: 0.5276},
				{Field: strings.ToLower(variable.SlowLogResourceGroup), Threshold: "rg1"},
			},
		},
	}
	allConditionFields := map[string]struct{}{
		strings.ToLower(variable.SlowLogConnIDStr):      {},
		strings.ToLower(variable.SlowLogExecRetryCount): {},
		strings.ToLower(variable.SlowLogDBStr):          {},
		strings.ToLower(variable.SlowLogSucc):           {},
		strings.ToLower(variable.SlowLogQueryTimeStr):   {},
		strings.ToLower(variable.SlowLogResourceGroup):  {},
	}
	require.NotNil(t, slowLogRuleSet)
	require.Len(t, slowLogRuleSet.RulesMap, 1)
	compareConditionsUnordered(t, rules[0].Conditions, slowLogRuleSet.RulesMap[123].Rules[0].Conditions, rawRule)
	require.Equal(t, allConditionFields, slowLogRuleSet.RulesMap[123].Fields)
	require.Nil(t, slowLogRuleSet.RulesMap[variable.UnsetConnID])

	// empty raw rule returns empty map
	slowLogRuleSet, err = variable.ParseGlobalSlowLogRules("")
	require.NoError(t, err)
	require.NotNil(t, slowLogRuleSet)
	require.Empty(t, slowLogRuleSet.RulesMap)
	require.Equal(t, "", slowLogRuleSet.RawRules)
	require.Equal(t, uint64(0x0), slowLogRuleSet.RawRulesHash)

	// special ConnID with empty raw rule
	slowLogRuleSet, err = variable.ParseGlobalSlowLogRules("Conn_id:123;")
	require.NoError(t, err)
	require.NotNil(t, slowLogRuleSet)
	require.Equal(t, slowLogRuleSet.RulesMap[123].Rules[0].Conditions[0].Threshold, uint64(123))
	require.Equal(t, "conn_id:123", slowLogRuleSet.RawRules)

	// invalid connID format
	slowLogRuleSet, err = variable.ParseGlobalSlowLogRules("Conn_ID: -123, DB: db1;")
	require.True(t, strings.Contains(err.Error(), "invalid slow log format"))
	require.Nil(t, slowLogRuleSet)

	// multiple rules with different ConnID
	allConditionFields = map[string]struct{}{
		strings.ToLower(variable.SlowLogConnIDStr): {},
		strings.ToLower(variable.SlowLogDBStr):     {},
	}
	rawRule = `Conn_ID: 123, DB: db1; Conn_ID: 456, DB: db2; DB: db3; Conn_ID: 789; Conn_ID: 123;;`
	slowLogRuleSet, err = variable.ParseGlobalSlowLogRules(rawRule)
	require.NoError(t, err)
	// the result raw rule(in uncertain order): conn_id:123,db:db1;conn_id:123;conn_id:456,db:db2;db:db3;conn_id:789
	require.True(t, strings.Contains(slowLogRuleSet.RawRules, "conn_id:123,db:db1;conn_id:123") ||
		strings.Contains(slowLogRuleSet.RawRules, "db:db1,conn_id:123;conn_id:123"))
	require.True(t, strings.Contains(slowLogRuleSet.RawRules, "conn_id:456,db:db2") ||
		strings.Contains(slowLogRuleSet.RawRules, "db:db2,conn_id:456"))
	require.True(t, strings.Contains(slowLogRuleSet.RawRules, "db:db3"))
	require.True(t, strings.Contains(slowLogRuleSet.RawRules, "conn_id:789"))
	require.Len(t, slowLogRuleSet.RulesMap, 4)
	// Conn_ID: 123
	checkRuleByField(t, slowLogRuleSet.RulesMap[123].Rules, variable.SlowLogDBStr, "db1")
	checkRuleByField(t, slowLogRuleSet.RulesMap[123].Rules, variable.SlowLogConnIDStr, "123")
	require.Equal(t, "", slowLogRuleSet.RulesMap[123].RawRules)
	require.Equal(t, allConditionFields, slowLogRuleSet.RulesMap[123].Fields)
	// Conn_ID: 456
	checkRuleByField(t, slowLogRuleSet.RulesMap[456].Rules, variable.SlowLogDBStr, "db2")
	require.Equal(t, "", slowLogRuleSet.RulesMap[456].RawRules)
	require.Equal(t, allConditionFields, slowLogRuleSet.RulesMap[456].Fields)
	// Conn_ID: -1
	require.Equal(t, "db3", slowLogRuleSet.RulesMap[variable.UnsetConnID].Rules[0].Conditions[0].Threshold)
	require.Equal(t, "", slowLogRuleSet.RulesMap[variable.UnsetConnID].RawRules)
	allConditionFields = map[string]struct{}{
		strings.ToLower(variable.SlowLogDBStr): {},
	}
	require.Equal(t, allConditionFields, slowLogRuleSet.RulesMap[variable.UnsetConnID].Fields)
	// Conn_ID: 789
	require.Equal(t, uint64(789), slowLogRuleSet.RulesMap[789].Rules[0].Conditions[0].Threshold)
}
