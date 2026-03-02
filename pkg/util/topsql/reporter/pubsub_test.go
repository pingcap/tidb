// Copyright 2022 PingCAP, Inc.
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

package reporter

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

type mockPubSubDataSinkRegisterer struct{}

func (r *mockPubSubDataSinkRegisterer) Register(dataSink DataSink) error { return nil }

func (r *mockPubSubDataSinkRegisterer) Deregister(dataSink DataSink) {}

type mockPubSubDataSinkStream struct {
	ctx       context.Context
	records   []*tipb.TopSQLRecord
	ruRecords []*tipb.TopRURecord
	sqlMetas  []*tipb.SQLMeta
	planMetas []*tipb.PlanMeta
	sendErr   error
	sendErrAt int
	sendCount int
	onSend    func(*tipb.TopSQLSubResponse, int)
	sync.Mutex
}

func (s *mockPubSubDataSinkStream) Send(resp *tipb.TopSQLSubResponse) error {
	s.Lock()
	defer s.Unlock()
	s.sendCount++

	if s.onSend != nil {
		s.onSend(resp, s.sendCount)
	}
	if s.sendErr != nil && s.sendErrAt > 0 && s.sendCount == s.sendErrAt {
		return s.sendErr
	}

	if resp.GetRecord() != nil {
		s.records = append(s.records, resp.GetRecord())
	}
	if resp.GetRuRecord() != nil {
		s.ruRecords = append(s.ruRecords, resp.GetRuRecord())
	}
	if resp.GetSqlMeta() != nil {
		s.sqlMetas = append(s.sqlMetas, resp.GetSqlMeta())
	}
	if resp.GetPlanMeta() != nil {
		s.planMetas = append(s.planMetas, resp.GetPlanMeta())
	}
	return nil
}

func (s *mockPubSubDataSinkStream) SetHeader(metadata.MD) error {
	return nil
}

func (s *mockPubSubDataSinkStream) SendHeader(metadata.MD) error {
	return nil
}

func (s *mockPubSubDataSinkStream) SetTrailer(metadata.MD) {

}

func (s *mockPubSubDataSinkStream) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}

func (s *mockPubSubDataSinkStream) SendMsg(m any) error {
	return nil
}

func (s *mockPubSubDataSinkStream) RecvMsg(m any) error {
	return nil
}

func TestPubSubDataSink(t *testing.T) {
	mockStream := &mockPubSubDataSinkStream{}
	// Create a subscription request (TopSQL only).
	req := &tipb.TopSQLSubRequest{}
	ds := newPubSubDataSink(req, mockStream, &mockPubSubDataSinkRegisterer{})
	go func() {
		_ = ds.run()
	}()

	panicPath := "github.com/pingcap/tidb/pkg/util/topsql/reporter/mockGrpcLogPanic"
	require.NoError(t, failpoint.Enable(panicPath, "panic"))
	err := ds.TrySend(&ReportData{
		DataRecords: []tipb.TopSQLRecord{{
			SqlDigest:  []byte("S1"),
			PlanDigest: []byte("P1"),
			Items: []*tipb.TopSQLRecordItem{{
				TimestampSec:      1,
				CpuTimeMs:         1,
				StmtExecCount:     1,
				StmtKvExecCount:   map[string]uint64{"": 1},
				StmtDurationSumNs: 1,
			}},
		}},
		RURecords: []tipb.TopRURecord{{
			User:      "user1",
			SqlDigest: []byte("S1"),
			Items: []*tipb.TopRURecordItem{{
				TimestampSec: 1,
				TotalRu:      1.0,
				ExecCount:    1,
				ExecDuration: 1,
			}},
		}},
		SQLMetas: []tipb.SQLMeta{{
			SqlDigest:     []byte("S1"),
			NormalizedSql: "SQL-1",
		}},
		PlanMetas: []tipb.PlanMeta{{
			PlanDigest:     []byte("P1"),
			NormalizedPlan: "PLAN-1",
		}},
	}, time.Now().Add(10*time.Second))
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	mockStream.Lock()
	assert.Len(t, mockStream.records, 1)
	assert.Len(t, mockStream.ruRecords, 0)
	assert.Len(t, mockStream.sqlMetas, 1)
	assert.Len(t, mockStream.planMetas, 1)
	mockStream.Unlock()

	ds.OnReporterClosing()
	require.NoError(t, failpoint.Disable(panicPath))
}

func TestPubSubDataSinkEnableTopRU(t *testing.T) {
	mockStream := &mockPubSubDataSinkStream{}
	req := &tipb.TopSQLSubRequest{
		Collectors: []tipb.CollectorType{
			tipb.CollectorType_COLLECTOR_TYPE_TOPSQL,
			tipb.CollectorType_COLLECTOR_TYPE_TOPRU,
		},
		Topru: &tipb.TopRUConfig{
			ItemIntervalSeconds: tipb.ItemInterval_ITEM_INTERVAL_15S,
		},
	}
	ds := newPubSubDataSink(req, mockStream, &mockPubSubDataSinkRegisterer{})

	topsqlstate.EnableTopRU()
	defer func() {
		for topsqlstate.TopRUEnabled() {
			topsqlstate.DisableTopRU()
		}
	}()

	err := ds.sendTopRURecords(context.Background(), []tipb.TopRURecord{{
		User:      "user1",
		SqlDigest: []byte("S1"),
		Items: []*tipb.TopRURecordItem{{
			TimestampSec: 1,
			TotalRu:      1.0,
			ExecCount:    1,
			ExecDuration: 1,
		}},
	}})
	require.NoError(t, err)

	mockStream.Lock()
	assert.Len(t, mockStream.ruRecords, 1)
	mockStream.Unlock()
}

func TestPubSubMultiSubscriberIsolation(t *testing.T) {
	// Contract: TopRU enable is reference-counted across subscribers, while
	// item interval is last-write-wins until the last subscriber exits.
	for topsqlstate.TopRUEnabled() {
		topsqlstate.DisableTopRU()
	}
	topsqlstate.ResetTopRUItemInterval()

	registererCtx, registererCancel := context.WithCancel(context.Background())
	t.Cleanup(registererCancel)
	registerer := NewDefaultDataSinkRegisterer(registererCtx)
	svc := NewTopSQLPubSubService(&registerer)

	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel1()
		cancel2()
	})

	stream1 := &mockPubSubDataSinkStream{ctx: ctx1}
	stream2 := &mockPubSubDataSinkStream{ctx: ctx2}

	req1 := &tipb.TopSQLSubRequest{
		Collectors: []tipb.CollectorType{
			tipb.CollectorType_COLLECTOR_TYPE_TOPSQL,
			tipb.CollectorType_COLLECTOR_TYPE_TOPRU,
		},
		Topru: &tipb.TopRUConfig{
			ItemIntervalSeconds: tipb.ItemInterval(30),
		},
	}
	req2 := &tipb.TopSQLSubRequest{
		Collectors: []tipb.CollectorType{
			tipb.CollectorType_COLLECTOR_TYPE_TOPSQL,
			tipb.CollectorType_COLLECTOR_TYPE_TOPRU,
		},
		Topru: &tipb.TopRUConfig{
			ItemIntervalSeconds: tipb.ItemInterval(15),
		},
	}

	// Eventually checks make this test robust to async subscribe goroutines.
	go func() { _ = svc.Subscribe(req1, stream1) }()
	require.Eventually(t, func() bool {
		return topsqlstate.TopRUEnabled() && topsqlstate.GetTopRUItemInterval() == 30
	}, time.Second, 10*time.Millisecond)

	go func() { _ = svc.Subscribe(req2, stream2) }()
	require.Eventually(t, func() bool {
		return topsqlstate.TopRUEnabled() && topsqlstate.GetTopRUItemInterval() == 15
	}, time.Second, 10*time.Millisecond)

	cancel2()
	require.Eventually(t, func() bool {
		return topsqlstate.TopRUEnabled() && topsqlstate.GetTopRUItemInterval() == 15
	}, time.Second, 10*time.Millisecond)

	cancel1()
	require.Eventually(t, func() bool {
		return !topsqlstate.TopRUEnabled() && topsqlstate.GetTopRUItemInterval() == int64(topsqlstate.DefTiDBTopRUItemIntervalSeconds)
	}, time.Second, 10*time.Millisecond)
}

type errPubSubDataSinkRegisterer struct{}

func (r *errPubSubDataSinkRegisterer) Register(DataSink) error { return errors.New("register failed") }

func (r *errPubSubDataSinkRegisterer) Deregister(DataSink) {}

func TestSubscribeRegisterFailDoesNotEnableTopRU(t *testing.T) {
	// Registration failure must not leak global TopRU state.
	for topsqlstate.TopRUEnabled() {
		topsqlstate.DisableTopRU()
	}

	req := &tipb.TopSQLSubRequest{
		Collectors: []tipb.CollectorType{
			tipb.CollectorType_COLLECTOR_TYPE_TOPSQL,
			tipb.CollectorType_COLLECTOR_TYPE_TOPRU,
		},
		Topru: &tipb.TopRUConfig{
			ItemIntervalSeconds: tipb.ItemInterval_ITEM_INTERVAL_15S,
		},
	}
	svc := NewTopSQLPubSubService(&errPubSubDataSinkRegisterer{})
	err := svc.Subscribe(req, &mockPubSubDataSinkStream{})
	require.Error(t, err)
	require.False(t, topsqlstate.TopRUEnabled())
}

func TestNormalizeTopRUItemIntervalInvalid(t *testing.T) {
	req := &tipb.TopSQLSubRequest{
		Collectors: []tipb.CollectorType{
			tipb.CollectorType_COLLECTOR_TYPE_TOPSQL,
			tipb.CollectorType_COLLECTOR_TYPE_TOPRU,
		},
		Topru: &tipb.TopRUConfig{
			ItemIntervalSeconds: tipb.ItemInterval(99),
		},
	}
	ds := newPubSubDataSink(req, &mockPubSubDataSinkStream{}, &mockPubSubDataSinkRegisterer{})
	// pubsub stores the raw interval; normalization happens in state.SetTopRUItemInterval.
	require.Equal(t, tipb.ItemInterval(99), ds.itemInterval)
}

func TestParseTopRUSubscription(t *testing.T) {
	cases := []struct {
		name         string
		req          *tipb.TopSQLSubRequest
		enableTopRU  bool
		itemInterval tipb.ItemInterval
	}{
		{
			name:         "nil request",
			req:          nil,
			enableTopRU:  false,
			itemInterval: tipb.ItemInterval_ITEM_INTERVAL_UNSPECIFIED,
		},
		{
			name: "missing topru config",
			req: &tipb.TopSQLSubRequest{
				Collectors: []tipb.CollectorType{tipb.CollectorType_COLLECTOR_TYPE_TOPRU},
			},
			enableTopRU:  false,
			itemInterval: tipb.ItemInterval_ITEM_INTERVAL_UNSPECIFIED,
		},
		{
			name: "missing topru collector",
			req: &tipb.TopSQLSubRequest{
				Collectors: []tipb.CollectorType{tipb.CollectorType_COLLECTOR_TYPE_TOPSQL},
				Topru: &tipb.TopRUConfig{
					ItemIntervalSeconds: tipb.ItemInterval_ITEM_INTERVAL_30S,
				},
			},
			enableTopRU:  false,
			itemInterval: tipb.ItemInterval_ITEM_INTERVAL_UNSPECIFIED,
		},
		{
			name: "enabled with valid collector",
			req: &tipb.TopSQLSubRequest{
				Collectors: []tipb.CollectorType{
					tipb.CollectorType_COLLECTOR_TYPE_TOPSQL,
					tipb.CollectorType_COLLECTOR_TYPE_TOPRU,
				},
				Topru: &tipb.TopRUConfig{
					ItemIntervalSeconds: tipb.ItemInterval_ITEM_INTERVAL_30S,
				},
			},
			enableTopRU:  true,
			itemInterval: tipb.ItemInterval_ITEM_INTERVAL_30S,
		},
		{
			name: "enabled with unknown interval passthrough",
			req: &tipb.TopSQLSubRequest{
				Collectors: []tipb.CollectorType{tipb.CollectorType_COLLECTOR_TYPE_TOPRU},
				Topru: &tipb.TopRUConfig{
					ItemIntervalSeconds: tipb.ItemInterval(99),
				},
			},
			enableTopRU:  true,
			itemInterval: tipb.ItemInterval(99),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			enableTopRU, itemInterval := parseTopRUSubscription(tc.req)
			require.Equal(t, tc.enableTopRU, enableTopRU)
			require.Equal(t, tc.itemInterval, itemInterval)
		})
	}
}

func TestSendTopRURecordsGatingAndErrors(t *testing.T) {
	setGlobalTopRUEnabled := func(enabled bool) {
		for topsqlstate.TopRUEnabled() {
			topsqlstate.DisableTopRU()
		}
		if enabled {
			topsqlstate.EnableTopRU()
		}
	}
	t.Cleanup(func() {
		setGlobalTopRUEnabled(false)
	})

	records := []tipb.TopRURecord{
		{
			User:      "user1",
			SqlDigest: []byte("S1"),
			Items: []*tipb.TopRURecordItem{{
				TimestampSec: 1,
				TotalRu:      1.0,
				ExecCount:    1,
				ExecDuration: 1,
			}},
		},
		{
			User:      "user2",
			SqlDigest: []byte("S2"),
			Items: []*tipb.TopRURecordItem{{
				TimestampSec: 2,
				TotalRu:      2.0,
				ExecCount:    1,
				ExecDuration: 1,
			}},
		},
	}

	t.Run("skip when records empty", func(t *testing.T) {
		setGlobalTopRUEnabled(true)
		stream := &mockPubSubDataSinkStream{}
		ds := &pubSubDataSink{
			stream:      stream,
			enableTopRU: true,
		}

		err := ds.sendTopRURecords(context.Background(), nil)
		require.NoError(t, err)
		require.Len(t, stream.ruRecords, 0)
	})

	t.Run("skip when sink top ru disabled", func(t *testing.T) {
		setGlobalTopRUEnabled(true)
		stream := &mockPubSubDataSinkStream{}
		ds := &pubSubDataSink{
			stream:      stream,
			enableTopRU: false,
		}

		err := ds.sendTopRURecords(context.Background(), records)
		require.NoError(t, err)
		require.Len(t, stream.ruRecords, 0)
	})

	t.Run("skip when global top ru disabled", func(t *testing.T) {
		setGlobalTopRUEnabled(false)
		stream := &mockPubSubDataSinkStream{}
		ds := &pubSubDataSink{
			stream:      stream,
			enableTopRU: true,
		}

		err := ds.sendTopRURecords(context.Background(), records)
		require.NoError(t, err)
		require.Len(t, stream.ruRecords, 0)
	})

	t.Run("return stream send error", func(t *testing.T) {
		setGlobalTopRUEnabled(true)
		sendErr := errors.New("stream send failed")
		stream := &mockPubSubDataSinkStream{
			sendErr:   sendErr,
			sendErrAt: 1,
		}
		ds := &pubSubDataSink{
			stream:      stream,
			enableTopRU: true,
		}

		err := ds.sendTopRURecords(context.Background(), records)
		require.ErrorIs(t, err, sendErr)
		require.Len(t, stream.ruRecords, 0)
	})

	t.Run("stop on context cancel after first send", func(t *testing.T) {
		setGlobalTopRUEnabled(true)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		stream := &mockPubSubDataSinkStream{}
		stream.onSend = func(_ *tipb.TopSQLSubResponse, count int) {
			if count == 1 {
				cancel()
			}
		}
		ds := &pubSubDataSink{
			stream:      stream,
			enableTopRU: true,
		}

		err := ds.sendTopRURecords(ctx, records)
		require.ErrorIs(t, err, context.Canceled)
		require.Len(t, stream.ruRecords, 1)
	})
}

func TestPubSubDataSink_DoSend_OrderIsStable(t *testing.T) {
	setGlobalTopRUEnabled := func(enabled bool) {
		for topsqlstate.TopRUEnabled() {
			topsqlstate.DisableTopRU()
		}
		if enabled {
			topsqlstate.EnableTopRU()
		}
	}
	t.Cleanup(func() {
		setGlobalTopRUEnabled(false)
	})
	setGlobalTopRUEnabled(true)

	responseKind := func(resp *tipb.TopSQLSubResponse) string {
		switch resp.RespOneof.(type) {
		case *tipb.TopSQLSubResponse_Record:
			return "record"
		case *tipb.TopSQLSubResponse_RuRecord:
			return "ru_record"
		case *tipb.TopSQLSubResponse_SqlMeta:
			return "sql_meta"
		case *tipb.TopSQLSubResponse_PlanMeta:
			return "plan_meta"
		default:
			return "unknown"
		}
	}

	data := &ReportData{
		DataRecords: []tipb.TopSQLRecord{
			{SqlDigest: []byte("S1"), PlanDigest: []byte("P1")},
			{SqlDigest: []byte("S2"), PlanDigest: []byte("P2")},
		},
		RURecords: []tipb.TopRURecord{
			{User: "u1", SqlDigest: []byte("R1"), PlanDigest: []byte("RP1")},
			{User: "u2", SqlDigest: []byte("R2"), PlanDigest: []byte("RP2")},
		},
		SQLMetas: []tipb.SQLMeta{
			{SqlDigest: []byte("S1"), NormalizedSql: "sql1"},
		},
		PlanMetas: []tipb.PlanMeta{
			{PlanDigest: []byte("P1"), NormalizedPlan: "plan1"},
		},
	}

	t.Run("stable send order", func(t *testing.T) {
		stream := &mockPubSubDataSinkStream{}
		gotOrder := make([]string, 0, 6)
		stream.onSend = func(resp *tipb.TopSQLSubResponse, _ int) {
			gotOrder = append(gotOrder, responseKind(resp))
		}
		ds := &pubSubDataSink{
			stream:      stream,
			enableTopRU: true,
		}

		err := ds.doSend(context.Background(), data)
		require.NoError(t, err)
		require.Equal(t,
			[]string{"record", "record", "ru_record", "ru_record", "sql_meta", "plan_meta"},
			gotOrder,
		)
	})

	t.Run("context cancel stops sending", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stream := &mockPubSubDataSinkStream{}
		gotOrder := make([]string, 0, 6)
		stream.onSend = func(resp *tipb.TopSQLSubResponse, count int) {
			gotOrder = append(gotOrder, responseKind(resp))
			if count == 3 {
				cancel()
			}
		}
		ds := &pubSubDataSink{
			stream:      stream,
			enableTopRU: true,
		}

		err := ds.doSend(ctx, data)
		require.ErrorIs(t, err, context.Canceled)
		require.Len(t, gotOrder, 3)
		require.Equal(t, []string{"record", "record", "ru_record"}, gotOrder)
	})
}
