package reporter

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
)

type mockPubSubDataSinkRegisterer struct{}

func (r *mockPubSubDataSinkRegisterer) Register(dataSink DataSink) error { return nil }

func (r *mockPubSubDataSinkRegisterer) Deregister(dataSink DataSink) {}

type mockPubSubDataSinkStream struct {
	sync.Mutex
	records   []*tipb.TopSQLRecord
	sqlMetas  []*tipb.SQLMeta
	planMetas []*tipb.PlanMeta
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

func (s *mockPubSubDataSinkStream) SendMsg(m interface{}) error {
	return nil
}

func (s *mockPubSubDataSinkStream) RecvMsg(m interface{}) error {
	return nil
}

func TestPubSubDataSink(t *testing.T) {
	mockStream := &mockPubSubDataSinkStream{}
	ds := newPubSubDataSink(mockStream, &mockPubSubDataSinkRegisterer{})
	go func() {
		_ = ds.run()
	}()

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
}
