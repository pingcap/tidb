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
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/timer/api"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestBuildInsertTimerSQL(t *testing.T) {
	now := time.Now()
	sql1 := "INSERT INTO `db1`.`t1` (NAMESPACE, TIMER_KEY, TIMER_DATA, TIMEZONE, SCHED_POLICY_TYPE, SCHED_POLICY_EXPR, " +
		"HOOK_CLASS, WATERMARK, ENABLE, TIMER_EXT, EVENT_ID, EVENT_STATUS, EVENT_START, EVENT_DATA, SUMMARY_DATA, VERSION) " +
		"VALUES (%?, %?, %?, %?, %?, %?, %?, FROM_UNIXTIME(%?), %?, JSON_MERGE_PATCH('{}', %?), %?, %?, FROM_UNIXTIME(%?), %?, %?, 1)"
	sql2 := "INSERT INTO `db1`.`t1` (NAMESPACE, TIMER_KEY, TIMER_DATA, TIMEZONE, SCHED_POLICY_TYPE, SCHED_POLICY_EXPR, " +
		"HOOK_CLASS, WATERMARK, ENABLE, TIMER_EXT, EVENT_ID, EVENT_STATUS, EVENT_START, EVENT_DATA, SUMMARY_DATA, VERSION) " +
		"VALUES (%?, %?, %?, %?, %?, %?, %?, %?, %?, JSON_MERGE_PATCH('{}', %?), %?, %?, %?, %?, %?, 1)"

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
					TimeZone:        "Asia/Shanghai",
					SchedPolicyType: api.SchedEventInterval,
					SchedPolicyExpr: "1h",
					HookClass:       "h1",
					Watermark:       now,
					Enable:          true,
					Tags:            []string{"l1", "l2"},
				},
				ManualRequest: api.ManualRequest{
					ManualRequestID:   "req1",
					ManualRequestTime: time.Unix(123, 0),
					ManualTimeout:     time.Minute,
					ManualProcessed:   true,
					ManualEventID:     "event1",
				},
				EventExtra: api.EventExtra{
					EventManualRequestID: "req1",
					EventWatermark:       time.Unix(456, 0),
				},
				EventID:     "e1",
				EventStatus: api.SchedEventTrigger,
				EventStart:  now.Add(time.Second),
				EventData:   []byte("event1"),
				SummaryData: []byte("summary1"),
			},
			args: []any{
				"n1", "k1", []byte("data1"), "Asia/Shanghai", "INTERVAL", "1h", "h1", now.Unix(),
				true, json.RawMessage(`{"tags":["l1","l2"],` +
					`"manual":{"request_id":"req1","request_time_unix":123,"timeout_sec":60,"processed":true,"event_id":"event1"},` +
					`"event":{"manual_request_id":"req1","watermark_unix":456}}`),
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
				"n1", "k1", []byte(nil), "", "INTERVAL", "1h", "", nil,
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
				TimeZone:        api.NewOptionalVal("Asia/Shanghai"),
				SchedPolicyType: api.NewOptionalVal(api.SchedEventInterval),
				SchedPolicyExpr: api.NewOptionalVal("1h"),
				ManualRequest: api.NewOptionalVal(api.ManualRequest{
					ManualRequestID:   "req1",
					ManualRequestTime: time.Unix(123, 0),
					ManualTimeout:     time.Minute,
					ManualProcessed:   true,
					ManualEventID:     "event1",
				}),
				EventStatus: api.NewOptionalVal(api.SchedEventTrigger),
				EventID:     api.NewOptionalVal("event1"),
				EventData:   api.NewOptionalVal([]byte("data1")),
				EventStart:  api.NewOptionalVal(now),
				EventExtra: api.NewOptionalVal(api.EventExtra{
					EventManualRequestID: "req2",
					EventWatermark:       time.Unix(456, 0),
				}),
				Watermark:    api.NewOptionalVal(now.Add(time.Second)),
				SummaryData:  api.NewOptionalVal([]byte("summary")),
				CheckEventID: api.NewOptionalVal("ee"),
				CheckVersion: api.NewOptionalVal(uint64(1)),
			},
			criteria: "ENABLE = %?, TIMEZONE = %?, SCHED_POLICY_TYPE = %?, SCHED_POLICY_EXPR = %?, EVENT_STATUS = %?, " +
				"EVENT_ID = %?, EVENT_DATA = %?, EVENT_START = FROM_UNIXTIME(%?), " +
				"WATERMARK = FROM_UNIXTIME(%?), SUMMARY_DATA = %?, " +
				"TIMER_EXT = JSON_MERGE_PATCH(TIMER_EXT, %?), " +
				"VERSION = VERSION + 1",
			args: []any{
				false, "Asia/Shanghai", "INTERVAL", "1h", "TRIGGER", "event1", []byte("data1"), now.Unix(),
				now.Unix() + 1, []byte("summary"),
				json.RawMessage(`{` +
					`"event":{"manual_request_id":"req2","watermark_unix":456},` +
					`"manual":{"request_id":"req1","request_time_unix":123,"timeout_sec":60,"processed":true,"event_id":"event1"},` +
					`"tags":["l1","l2"]` +
					`}`),
			},
		},
		{
			update: &api.TimerUpdate{
				EventExtra:    api.NewOptionalVal(api.EventExtra{EventManualRequestID: "req1"}),
				ManualRequest: api.NewOptionalVal(api.ManualRequest{ManualRequestID: "req2"}),
			},
			criteria: "TIMER_EXT = JSON_MERGE_PATCH(TIMER_EXT, %?), VERSION = VERSION + 1",
			args: []any{json.RawMessage(`{` +
				`"event":{"manual_request_id":"req1","watermark_unix":null},` +
				`"manual":{"request_id":"req2","request_time_unix":null,"timeout_sec":null,"processed":null,"event_id":null}` +
				`}`)},
		},
		{
			update: &api.TimerUpdate{
				EventExtra:    api.NewOptionalVal(api.EventExtra{EventWatermark: time.Unix(123, 0)}),
				ManualRequest: api.NewOptionalVal(api.ManualRequest{ManualRequestTime: time.Unix(456, 0)}),
			},
			criteria: "TIMER_EXT = JSON_MERGE_PATCH(TIMER_EXT, %?), VERSION = VERSION + 1",
			args: []any{json.RawMessage(`{` +
				`"event":{"manual_request_id":null,"watermark_unix":123},` +
				`"manual":{"request_id":null,"request_time_unix":456,"timeout_sec":null,"processed":null,"event_id":null}` +
				`}`)},
		},
		{
			update: &api.TimerUpdate{
				TimeZone:        api.NewOptionalVal(""),
				SchedPolicyExpr: api.NewOptionalVal(""),
				EventID:         api.NewOptionalVal(""),
				EventData:       api.NewOptionalVal([]byte(nil)),
				EventStart:      api.NewOptionalVal(zeroTime),
				EventExtra:      api.NewOptionalVal(api.EventExtra{}),
				ManualRequest:   api.NewOptionalVal(api.ManualRequest{}),
				Watermark:       api.NewOptionalVal(zeroTime),
				SummaryData:     api.NewOptionalVal([]byte(nil)),
				Tags:            api.NewOptionalVal([]string(nil)),
			},
			criteria: "TIMEZONE = %?, SCHED_POLICY_EXPR = %?, EVENT_ID = %?, EVENT_DATA = %?, " +
				"EVENT_START = NULL, WATERMARK = NULL, SUMMARY_DATA = %?, " +
				"TIMER_EXT = JSON_MERGE_PATCH(TIMER_EXT, %?), " +
				"VERSION = VERSION + 1",
			args: []any{"", "", "", []byte(nil), []byte(nil), json.RawMessage(`{"event":null,"manual":null,"tags":null}`)},
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

type mockSessionPool struct {
	syssession.Pool
	se  *syssession.Session
	err error
}

func (p *mockSessionPool) WithSession(fn func(*syssession.Session) error) error {
	if p.err != nil {
		return p.err
	}
	return fn(p.se)
}

type mockSession struct {
	mock.Mock
	sessionctx.Context
	sqlexec.SQLExecutor
}

func (p *mockSession) GetSQLExecutor() sqlexec.SQLExecutor {
	return p
}

func (p *mockSession) ExecuteInternal(ctx context.Context, sql string, args ...any) (rs sqlexec.RecordSet, _ error) {
	ret := p.Called(ctx, sql, args)
	if r := ret.Get(0); r != nil {
		rs = r.(sqlexec.RecordSet)
	}
	return rs, ret.Error(1)
}

func (p *mockSession) GetSessionVars() *variable.SessionVars {
	return p.Context.GetSessionVars()
}

func (p *mockSession) GetSessionManager() util.SessionManager {
	return nil
}

func (p *mockSession) Close() {
	p.Called()
}

var matchCtx = mock.MatchedBy(func(ctx context.Context) bool {
	return kv.GetInternalSourceType(ctx) == kv.InternalTimer
})

func TestWithSession(t *testing.T) {
	sctx := &mockSession{}
	pool := &mockSessionPool{}
	core := tableTimerStoreCore{pool: pool}
	resetSe := func() {
		se, err := syssession.NewSessionForTest(sctx)
		require.NoError(t, err)
		pool.se = se
	}
	resetSe()

	mockSuccessInit := func() {
		rs := &sqlexec.SimpleRecordSet{
			ResultFields: []*resolve.ResultField{{
				Column: &model.ColumnInfo{
					FieldType: *types.NewFieldType(mysql.TypeString),
				},
			}},
			MaxChunkSize: 1,
			Rows:         [][]any{{"tz1"}},
		}
		sctx.On("ExecuteInternal", matchCtx, "ROLLBACK", []any(nil)).
			Return(nil, nil).
			Once()
		sctx.On("ExecuteInternal", matchCtx, "SELECT @@time_zone", []any(nil)).
			Return(rs, nil).
			Once()
		sctx.On("ExecuteInternal", matchCtx, "SET @@time_zone='UTC'", []any(nil)).
			Return(nil, nil).
			Once()
	}

	mockRestore := func() {
		sctx.On("ExecuteInternal", matchCtx, "ROLLBACK", []any(nil)).
			Return(nil, nil).
			Once()
		sctx.On("ExecuteInternal", matchCtx, "SET @@time_zone=%?", []any{"tz1"}).
			Return(nil, nil).
			Once()
	}

	mockCb1 := &mock.Mock{}
	cb1 := func(s *syssession.Session) error {
		sctx.AssertExpectations(t)
		defer mockRestore()
		return mockCb1.MethodCalled("cb1", s).Error(0)
	}

	// Pool has an error
	pool.err = errors.New("mockErr")
	require.EqualError(t, core.withSession(cb1), "mockErr")

	// init session returns error
	pool.err = nil
	sctx.On("ExecuteInternal", matchCtx, "ROLLBACK", []any(nil)).
		Return(nil, errors.New("mockErr1")).
		Once()
	require.EqualError(t, core.withSession(cb1), "mockErr1")
	sctx.AssertExpectations(t)

	// init session returns error2
	sctx.On("ExecuteInternal", matchCtx, "ROLLBACK", []any(nil)).
		Return(nil, nil).
		Once()
	sctx.On("ExecuteInternal", matchCtx, "SELECT @@time_zone", []any(nil)).
		Return(nil, errors.New("mockErr2")).
		Once()
	require.EqualError(t, core.withSession(cb1), "mockErr2")
	sctx.AssertExpectations(t)

	// init session panic
	sctx.On("ExecuteInternal", matchCtx, "ROLLBACK", []any(nil)).
		Panic("mockPanic").
		Once()
	require.Panics(t, func() {
		_ = core.withSession(cb1)
	})
	sctx.AssertExpectations(t)

	// returns a session
	mockSuccessInit()
	mockCb1.On("cb1", pool.se).Return(nil).Once()
	require.NoError(t, core.withSession(cb1))
	sctx.AssertExpectations(t)
	mockCb1.AssertExpectations(t)

	// callback failed
	mockSuccessInit()
	mockCb1.On("cb1", pool.se).Return(errors.New("mockErr3")).Once()
	require.EqualError(t, core.withSession(cb1), "mockErr3")
	sctx.AssertExpectations(t)
	mockCb1.AssertExpectations(t)

	// callback panic
	mockSuccessInit()
	mockCb1.On("cb1", pool.se).Panic("panic2").Once()
	require.PanicsWithValue(t, "panic2", func() {
		_ = core.withSession(cb1)
	})
	sctx.AssertExpectations(t)
	mockCb1.AssertExpectations(t)

	// rollback in restore failed, should close session
	mockSuccessInit()
	mockCb1.On("cb1", pool.se).Return(nil).Once()
	require.NoError(t, core.withSession(func(se *syssession.Session) error {
		sctx.AssertExpectations(t)
		sctx.On("ExecuteInternal", matchCtx, "ROLLBACK", []any(nil)).
			Return(nil, errors.New("ROLLBACK error")).
			Once()
		sctx.On("Close").Once()
		mockCb1.MethodCalled("cb1", se)
		return nil
	}))
	require.True(t, pool.se.IsInternalClosed())
	sctx.AssertExpectations(t)
	mockCb1.AssertExpectations(t)
	resetSe()

	// set timezone in restore failed should close session
	mockSuccessInit()
	mockCb1.On("cb1", pool.se).Return(nil).Once()
	require.NoError(t, core.withSession(func(se *syssession.Session) error {
		sctx.AssertExpectations(t)
		sctx.On("ExecuteInternal", matchCtx, "ROLLBACK", []any(nil)).
			Return(nil, nil).
			Once()
		sctx.On("ExecuteInternal", matchCtx, "SET @@time_zone=%?", mock.Anything).
			Return(nil, errors.New("SET tz error")).
			Once()
		sctx.On("Close").Once()
		mockCb1.MethodCalled("cb1", se)
		return nil
	}))
	require.True(t, pool.se.IsInternalClosed())
	sctx.AssertExpectations(t)
	mockCb1.AssertExpectations(t)
	resetSe()

	// withSctx
	mockCb2 := &mock.Mock{}
	cb2 := func(ctx sessionctx.Context) error {
		sctx.AssertExpectations(t)
		defer mockRestore()
		return mockCb2.MethodCalled("cb2", ctx).Error(0)
	}
	mockSuccessInit()
	mockCb2.On("cb2", sctx).Return(nil).Once()
	require.NoError(t, core.withSctx(cb2))
	sctx.AssertExpectations(t)
	mockCb2.AssertExpectations(t)

	// withSctx error
	mockSuccessInit()
	mockCb2.On("cb2", sctx).Return(errors.New("mockErr4")).Once()
	require.EqualError(t, core.withSctx(cb2), "mockErr4")
	sctx.AssertExpectations(t)
	mockCb2.AssertExpectations(t)

	// withSctx panic
	mockSuccessInit()
	mockCb2.On("cb2", sctx).Panic("panic3").Once()
	require.PanicsWithValue(t, "panic3", func() {
		_ = core.withSctx(cb2)
	})
	sctx.AssertExpectations(t)
	mockCb2.AssertExpectations(t)
}

func TestRunInTxn(t *testing.T) {
	se := &mockSession{}

	// success
	se.On("ExecuteInternal", matchCtx, "BEGIN PESSIMISTIC", []any(nil)).
		Return(nil, nil).
		Once()
	se.On("ExecuteInternal", matchCtx, mock.MatchedBy(func(sql string) bool {
		return strings.HasPrefix(sql, "insert")
	}), mock.Anything).
		Return(nil, nil).
		Once()
	se.On("ExecuteInternal", matchCtx, "COMMIT", []any(nil)).
		Return(nil, nil).
		Once()
	require.Nil(t, runInTxn(context.Background(), se, func() error {
		_, err := executeSQL(context.Background(), se, "insert into t value(?)", 1)
		return err
	}))
	se.AssertExpectations(t)

	// start txn failed
	se.On("ExecuteInternal", matchCtx, "BEGIN PESSIMISTIC", []any(nil)).
		Return(nil, errors.New("mockBeginErr")).
		Once()
	err := runInTxn(context.Background(), se, func() error { return nil })
	require.EqualError(t, err, "mockBeginErr")
	se.AssertExpectations(t)

	// exec failed, rollback success
	se.On("ExecuteInternal", matchCtx, "BEGIN PESSIMISTIC", []any(nil)).
		Return(nil, nil).
		Once()
	se.On("ExecuteInternal", matchCtx, "ROLLBACK", []any(nil)).
		Return(nil, nil).
		Once()
	err = runInTxn(context.Background(), se, func() error { return errors.New("mockFuncErr") })
	require.EqualError(t, err, "mockFuncErr")
	se.AssertExpectations(t)

	// commit failed
	se.On("ExecuteInternal", matchCtx, "BEGIN PESSIMISTIC", []any(nil)).
		Return(nil, nil).
		Once()
	se.On("ExecuteInternal", matchCtx, "COMMIT", []any(nil)).
		Return(nil, errors.New("commitErr")).
		Once()
	se.On("ExecuteInternal", matchCtx, "ROLLBACK", []any(nil)).
		Return(nil, nil).
		Once()
	err = runInTxn(context.Background(), se, func() error { return nil })
	require.EqualError(t, err, "commitErr")
	se.AssertExpectations(t)

	// rollback failed
	se.On("ExecuteInternal", matchCtx, "BEGIN PESSIMISTIC", []any(nil)).
		Return(nil, nil).
		Once()
	se.On("ExecuteInternal", matchCtx, "ROLLBACK", []any(nil)).
		Return(nil, errors.New("rollbackErr")).
		Once()
	err = runInTxn(context.Background(), se, func() error { return errors.New("mockFuncErr") })
	require.EqualError(t, err, "mockFuncErr")
	se.AssertExpectations(t)
}
