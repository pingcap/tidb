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
	sync.Mutex
}

func (s *mockPubSubDataSinkStream) Send(resp *tipb.TopSQLSubResponse) error {
	s.Lock()
	defer s.Unlock()

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
	for topsqlstate.TopRUEnabled() {
		topsqlstate.DisableTopRU()
	}
	topsqlstate.ResetTopRUItemInterval()

	svc := NewTopSQLPubSubService(&mockPubSubDataSinkRegisterer{})

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
