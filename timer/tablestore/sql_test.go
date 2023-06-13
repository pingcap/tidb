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

package tablestore

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/timer/api"
	"github.com/stretchr/testify/require"
)

func TestBuildInsertTimerSQL(t *testing.T) {
	now := time.Now()
	sql1 := "INSERT INTO `db1`.`t1` (NAMESPACE, TIMER_KEY, TIMER_DATA, TIMEZONE, SCHED_POLICY_TYPE, SCHED_POLICY_EXPR, " +
		"HOOK_CLASS, WATERMARK, ENABLE, EVENT_ID, EVENT_STATUS, EVENT_START, EVENT_DATA, SUMMARY_DATA, VERSION) " +
		"VALUES (%?, %?, %?, 'TIDB', %?, %?, %?, FROM_UNIXTIME(%?), %?, %?, %?, FROM_UNIXTIME(%?), %?, %?, 1)"
	sql2 := "INSERT INTO `db1`.`t1` (NAMESPACE, TIMER_KEY, TIMER_DATA, TIMEZONE, SCHED_POLICY_TYPE, SCHED_POLICY_EXPR, " +
		"HOOK_CLASS, WATERMARK, ENABLE, EVENT_ID, EVENT_STATUS, EVENT_START, EVENT_DATA, SUMMARY_DATA, VERSION) " +
		"VALUES (%?, %?, %?, 'TIDB', %?, %?, %?, %?, %?, %?, %?, %?, %?, %?, 1)"

	cases := []struct {
		sql    string
		record *api.TimerRecord
		args   []any
	}{
		{
			sql: sql1,
			record: &api.TimerRecord{
				TimerSpec: api.TimerSpec{
					Namespace:       "n1",
					Key:             "k1",
					Data:            []byte("data1"),
					SchedPolicyType: api.SchedEventInterval,
					SchedPolicyExpr: "1h",
					HookClass:       "h1",
					Watermark:       now,
					Enable:          true,
					Tags:            []string{"l1", "l2"},
				},
				EventID:     "e1",
				EventStatus: api.SchedEventTrigger,
				EventStart:  now.Add(time.Second),
				EventData:   []byte("event1"),
				SummaryData: []byte("summary1"),
			},
			args: []any{
				"n1", "k1", []byte("data1"), "INTERVAL", "1h", "h1", now.Unix(),
				true, json.RawMessage(`{"tags":["l1","l2"]}`),
				"e1", "TRIGGER", now.Unix() + 1, []byte("event1"), []byte("summary1"),
			},
		},
		{
			sql: sql2,
			record: &api.TimerRecord{
				TimerSpec: api.TimerSpec{
					Namespace:       "n1",
					Key:             "k1",
					SchedPolicyType: api.SchedEventInterval,
					SchedPolicyExpr: "1h",
				},
			},
			args: []any{
				"n1", "k1", []byte(nil), "INTERVAL", "1h", "", nil,
				false, json.RawMessage("{}"), "", "IDLE", nil, []byte(nil), []byte(nil),
			},
		},
	}

	for _, c := range cases {
		require.Equal(t, strings.Count(c.sql, "%?"), len(c.args))
		sql, args, err := buildInsertTimerSQL("db1", "t1", c.record)
		require.NoError(t, err)
		require.Equal(t, c.sql, sql)
		require.Equal(t, c.args, args)
	}
}

func TestBuildCondCriteria(t *testing.T) {
	cases := []struct {
		cond     api.Cond
		criteria string
		args     []any
	}{
		{
			cond:     nil,
			criteria: "1",
			args:     []any{},
		},
		{
			cond:     &api.TimerCond{},
			criteria: "1",
			args:     []any{},
		},
		{
			cond: &api.TimerCond{
				ID: api.NewOptionalVal("1"),
			},
			criteria: "ID = %?",
			args:     []any{"1"},
		},
		{
			cond: &api.TimerCond{
				Namespace: api.NewOptionalVal("ns1"),
			},
			criteria: "NAMESPACE = %?",
			args:     []any{"ns1"},
		},
		{
			cond: &api.TimerCond{
				Key: api.NewOptionalVal("key1"),
			},
			criteria: "TIMER_KEY = %?",
			args:     []any{"key1"},
		},
		{
			cond: &api.TimerCond{
				Key:       api.NewOptionalVal("key1"),
				KeyPrefix: true,
			},
			criteria: "TIMER_KEY LIKE %?",
			args:     []any{"key1%"},
		},
		{
			cond: &api.TimerCond{
				Namespace: api.NewOptionalVal("ns1"),
				Key:       api.NewOptionalVal("key1"),
			},
			criteria: "NAMESPACE = %? AND TIMER_KEY = %?",
			args:     []any{"ns1", "key1"},
		},
		{
			cond: &api.TimerCond{
				Namespace: api.NewOptionalVal("ns1"),
				Key:       api.NewOptionalVal("key1"),
				KeyPrefix: true,
			},
			criteria: "NAMESPACE = %? AND TIMER_KEY LIKE %?",
			args:     []any{"ns1", "key1%"},
		},
		{
			cond: &api.TimerCond{
				Tags: api.NewOptionalVal([]string{}),
			},
			criteria: "1",
			args:     []any{},
		},
		{
			cond: &api.TimerCond{
				Tags: api.NewOptionalVal([]string{"l1"}),
			},
			criteria: "JSON_EXTRACT(TIMER_EXT, '$.tags') IS NOT NULL AND JSON_CONTAINS((TIMER_EXT->'$.tags'), %?)",
			args:     []any{json.RawMessage(`["l1"]`)},
		},
		{
			cond: &api.TimerCond{
				Tags: api.NewOptionalVal([]string{"l1", "l2"}),
			},
			criteria: "JSON_EXTRACT(TIMER_EXT, '$.tags') IS NOT NULL AND JSON_CONTAINS((TIMER_EXT->'$.tags'), %?)",
			args:     []any{json.RawMessage(`["l1","l2"]`)},
		},
		{
			cond: api.And(
				&api.TimerCond{
					Namespace: api.NewOptionalVal("ns1"),
					Key:       api.NewOptionalVal("key1"),
				},
				&api.TimerCond{
					ID: api.NewOptionalVal("2"),
				},
			),
			criteria: "(NAMESPACE = %? AND TIMER_KEY = %?) AND (ID = %?)",
			args:     []any{"ns1", "key1", "2"},
		},
		{
			cond: api.And(
				&api.TimerCond{},
				&api.TimerCond{
					ID: api.NewOptionalVal("2"),
				},
			),
			criteria: "1 AND (ID = %?)",
			args:     []any{"2"},
		},
		{
			cond: api.And(
				api.Not(&api.TimerCond{}),
				&api.TimerCond{
					ID: api.NewOptionalVal("2"),
				},
			),
			criteria: "0 AND (ID = %?)",
			args:     []any{"2"},
		},
		{
			cond: api.And(
				&api.TimerCond{
					Namespace: api.NewOptionalVal("ns1"),
				},
				&api.TimerCond{},
				&api.TimerCond{
					ID: api.NewOptionalVal("2"),
				},
			),
			criteria: "(NAMESPACE = %?) AND 1 AND (ID = %?)",
			args:     []any{"ns1", "2"},
		},
		{
			cond: api.Not(api.And(
				&api.TimerCond{
					Namespace: api.NewOptionalVal("ns1"),
					Key:       api.NewOptionalVal("key1"),
				},
				&api.TimerCond{
					ID: api.NewOptionalVal("2"),
				},
			)),
			criteria: "!((NAMESPACE = %? AND TIMER_KEY = %?) AND (ID = %?))",
			args:     []any{"ns1", "key1", "2"},
		},
		{
			cond: api.Or(
				&api.TimerCond{
					Namespace: api.NewOptionalVal("ns1"),
					Key:       api.NewOptionalVal("key1"),
				},
				&api.TimerCond{
					ID: api.NewOptionalVal("2"),
				},
			),
			criteria: "(NAMESPACE = %? AND TIMER_KEY = %?) OR (ID = %?)",
			args:     []any{"ns1", "key1", "2"},
		},
		{
			cond: api.Not(api.Or(
				&api.TimerCond{
					Namespace: api.NewOptionalVal("ns1"),
					Key:       api.NewOptionalVal("key1"),
				},
				&api.TimerCond{
					ID: api.NewOptionalVal("2"),
				},
			)),
			criteria: "!((NAMESPACE = %? AND TIMER_KEY = %?) OR (ID = %?))",
			args:     []any{"ns1", "key1", "2"},
		},
		{
			cond: api.Or(
				&api.TimerCond{},
				&api.TimerCond{
					ID: api.NewOptionalVal("2"),
				},
			),
			criteria: "1 OR (ID = %?)",
			args:     []any{"2"},
		},
		{
			cond: api.Or(
				&api.TimerCond{
					Namespace: api.NewOptionalVal("ns1"),
				},
				&api.TimerCond{},
				&api.TimerCond{
					ID: api.NewOptionalVal("2"),
				},
			),
			criteria: "(NAMESPACE = %?) OR 1 OR (ID = %?)",
			args:     []any{"ns1", "2"},
		},
		{
			cond: api.Not(&api.TimerCond{
				ID: api.NewOptionalVal("3"),
			}),
			criteria: "!(ID = %?)",
			args:     []any{"3"},
		},
		{
			cond:     api.Not(&api.TimerCond{}),
			criteria: "0",
			args:     []any{},
		},
		{
			cond:     api.Not(api.Not(&api.TimerCond{})),
			criteria: "1",
			args:     []any{},
		},
		{
			cond: api.Not(&api.TimerCond{
				Namespace: api.NewOptionalVal("ns1"),
				Key:       api.NewOptionalVal("key1"),
			}),
			criteria: "!(NAMESPACE = %? AND TIMER_KEY = %?)",
			args:     []any{"ns1", "key1"},
		},
	}

	for _, c := range cases {
		require.Equal(t, strings.Count(c.criteria, "%?"), len(c.args))
		args := make([]any, 0)
		criteria, args, err := buildCondCriteria(c.cond, args)
		require.NoError(t, err)
		require.Equal(t, c.criteria, criteria)
		require.Equal(t, c.args, args)

		args = []any{"a", "b"}
		criteria, args, err = buildCondCriteria(c.cond, args)
		require.NoError(t, err)
		require.Equal(t, c.criteria, criteria)
		require.Equal(t, append([]any{"a", "b"}, c.args...), args)
	}
}

func TestBuildSelectTimerSQL(t *testing.T) {
	prefix := "SELECT " +
		"ID, NAMESPACE, TIMER_KEY, TIMER_DATA, TIMEZONE, SCHED_POLICY_TYPE, SCHED_POLICY_EXPR, " +
		"HOOK_CLASS, WATERMARK, ENABLE, TIMER_EXT, EVENT_STATUS, EVENT_ID, EVENT_DATA, EVENT_START, SUMMARY_DATA, " +
		"CREATE_TIME, UPDATE_TIME, VERSION FROM `db1`.`t1`"

	cases := []struct {
		cond api.Cond
		sql  string
		args []any
	}{
		{
			cond: nil,
			sql:  prefix + " WHERE 1",
			args: []any{},
		},
		{
			cond: &api.TimerCond{ID: api.NewOptionalVal("2")},
			sql:  prefix + " WHERE ID = %?",
			args: []any{"2"},
		},
		{
			cond: &api.TimerCond{Namespace: api.NewOptionalVal("ns1"), Key: api.NewOptionalVal("key1")},
			sql:  prefix + " WHERE NAMESPACE = %? AND TIMER_KEY = %?",
			args: []any{"ns1", "key1"},
		},
		{
			cond: api.Or(
				&api.TimerCond{ID: api.NewOptionalVal("3")},
				&api.TimerCond{Namespace: api.NewOptionalVal("ns1")},
			),
			sql:  prefix + " WHERE (ID = %?) OR (NAMESPACE = %?)",
			args: []any{"3", "ns1"},
		},
	}

	for _, c := range cases {
		require.Equal(t, strings.Count(c.sql, "%?"), len(c.args))
		sql, args, err := buildSelectTimerSQL("db1", "t1", c.cond)
		require.NoError(t, err)
		require.Equal(t, c.sql, sql)
		require.Equal(t, c.args, args)
	}
}

func TestBuildUpdateCriteria(t *testing.T) {
	now := time.Now()
	var zeroTime time.Time
	cases := []struct {
		update   *api.TimerUpdate
		criteria string
		args     []any
	}{
		{
			update:   &api.TimerUpdate{},
			criteria: "VERSION = VERSION + 1",
			args:     []any{},
		},
		{
			update: &api.TimerUpdate{
				Enable: api.NewOptionalVal(true),
			},
			criteria: "ENABLE = %?, VERSION = VERSION + 1",
			args:     []any{true},
		},
		{
			update: &api.TimerUpdate{
				Enable:          api.NewOptionalVal(false),
				Tags:            api.NewOptionalVal([]string{"l1", "l2"}),
				SchedPolicyType: api.NewOptionalVal(api.SchedEventInterval),
				SchedPolicyExpr: api.NewOptionalVal("1h"),
				EventStatus:     api.NewOptionalVal(api.SchedEventTrigger),
				EventID:         api.NewOptionalVal("event1"),
				EventData:       api.NewOptionalVal([]byte("data1")),
				EventStart:      api.NewOptionalVal(now),
				Watermark:       api.NewOptionalVal(now.Add(time.Second)),
				SummaryData:     api.NewOptionalVal([]byte("summary")),
				CheckEventID:    api.NewOptionalVal("ee"),
				CheckVersion:    api.NewOptionalVal(uint64(1)),
			},
			criteria: "ENABLE = %?, SCHED_POLICY_TYPE = %?, SCHED_POLICY_EXPR = %?, EVENT_STATUS = %?, " +
				"EVENT_ID = %?, EVENT_DATA = %?, EVENT_START = FROM_UNIXTIME(%?), " +
				"WATERMARK = FROM_UNIXTIME(%?), SUMMARY_DATA = %?, " +
				"TIMER_EXT = JSON_MERGE_PATCH(TIMER_EXT, %?), " +
				"VERSION = VERSION + 1",
			args: []any{
				false, "INTERVAL", "1h", "TRIGGER", "event1", []byte("data1"), now.Unix(),
				now.Unix() + 1, []byte("summary"), json.RawMessage(`{"tags":["l1","l2"]}`),
			},
		},
		{
			update: &api.TimerUpdate{
				SchedPolicyExpr: api.NewOptionalVal(""),
				EventID:         api.NewOptionalVal(""),
				EventData:       api.NewOptionalVal([]byte(nil)),
				EventStart:      api.NewOptionalVal(zeroTime),
				Watermark:       api.NewOptionalVal(zeroTime),
				SummaryData:     api.NewOptionalVal([]byte(nil)),
				Tags:            api.NewOptionalVal([]string(nil)),
			},
			criteria: "SCHED_POLICY_EXPR = %?, EVENT_ID = %?, EVENT_DATA = %?, " +
				"EVENT_START = NULL, WATERMARK = NULL, SUMMARY_DATA = %?, " +
				"TIMER_EXT = JSON_MERGE_PATCH(TIMER_EXT, %?), " +
				"VERSION = VERSION + 1",
			args: []any{"", "", []byte(nil), []byte(nil), json.RawMessage(`{"tags":null}`)},
		},
		{
			update: &api.TimerUpdate{
				CheckEventID: api.NewOptionalVal("ee"),
				CheckVersion: api.NewOptionalVal(uint64(1)),
			},
			criteria: "VERSION = VERSION + 1",
			args:     []any{},
		},
	}

	for _, c := range cases {
		require.Equal(t, strings.Count(c.criteria, "%?"), len(c.args))
		criteria, args, err := buildUpdateCriteria(c.update, []any{})
		require.NoError(t, err)
		require.Equal(t, c.criteria, criteria)
		require.Equal(t, c.args, args)

		criteria, args, err = buildUpdateCriteria(c.update, []any{1, "2", "3"})
		require.NoError(t, err)
		require.Equal(t, c.criteria, criteria)
		require.Equal(t, append([]any{1, "2", "3"}, c.args...), args)
	}
}

func TestBuildUpdateTimerSQL(t *testing.T) {
	timerID := "123"
	cases := []struct {
		update *api.TimerUpdate
		sql    string
		args   []any
	}{
		{
			update: &api.TimerUpdate{},
			sql:    "UPDATE `db1`.`tbl1` SET VERSION = VERSION + 1 WHERE ID = %?",
			args:   []any{timerID},
		},
		{
			update: &api.TimerUpdate{
				SchedPolicyType: api.NewOptionalVal(api.SchedEventInterval),
				SchedPolicyExpr: api.NewOptionalVal("1h"),
			},
			sql:  "UPDATE `db1`.`tbl1` SET SCHED_POLICY_TYPE = %?, SCHED_POLICY_EXPR = %?, VERSION = VERSION + 1 WHERE ID = %?",
			args: []any{"INTERVAL", "1h", timerID},
		},
	}

	for _, c := range cases {
		require.Equal(t, strings.Count(c.sql, "%?"), len(c.args))
		sql, args, err := buildUpdateTimerSQL("db1", "tbl1", timerID, c.update)
		require.NoError(t, err)
		require.Equal(t, c.sql, sql)
		require.Equal(t, c.args, args)
	}
}

func TestBuildDeleteTimerSQL(t *testing.T) {
	sql, args := buildDeleteTimerSQL("db1", "tbl1", "123")
	require.Equal(t, "DELETE FROM `db1`.`tbl1` WHERE ID = %?", sql)
	require.Equal(t, []any{"123"}, args)
}
