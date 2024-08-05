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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

type mockPubSubDataSinkRegisterer struct{}

func (r *mockPubSubDataSinkRegisterer) Register(dataSink DataSink) error { return nil }

func (r *mockPubSubDataSinkRegisterer) Deregister(dataSink DataSink) {}

type mockPubSubDataSinkStream struct {
	records   []*tipb.TopSQLRecord
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
	ds := newPubSubDataSink(mockStream, &mockPubSubDataSinkRegisterer{})
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
	assert.Len(t, mockStream.sqlMetas, 1)
	assert.Len(t, mockStream.planMetas, 1)
	mockStream.Unlock()

	ds.OnReporterClosing()
	require.NoError(t, failpoint.Disable(panicPath))
}
