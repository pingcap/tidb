package reporter

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util/topsql/reporter/mock"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/assert"
)

type mockSingleTargetDataSinkRegisterer struct{}

func (r *mockSingleTargetDataSinkRegisterer) Register(dataSink DataSink) error { return nil }

func (r *mockSingleTargetDataSinkRegisterer) Deregister(dataSink DataSink) {}

func TestSingleTargetDataSink(t *testing.T) {
	server, err := mock.StartMockAgentServer()
	assert.NoError(t, err)
	defer server.Stop()

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TopSQL.ReceiverAddress = server.Address()
	})

	ds := NewSingleTargetDataSink(&mockSingleTargetDataSinkRegisterer{})
	ds.Start()
	defer ds.Close()

	err = ds.TrySend(&ReportData{
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

	server.WaitCollectCnt(1, 5*time.Second)

	assert.Len(t, server.GetLatestRecords(), 1)
	assert.Len(t, server.GetTotalSQLMetas(), 1)
	sqlMeta, exist := server.GetSQLMetaByDigestBlocking([]byte("S1"), 5*time.Second)
	assert.True(t, exist)
	assert.Equal(t, sqlMeta.NormalizedSql, "SQL-1")
	normalizedPlan, exist := server.GetPlanMetaByDigestBlocking([]byte("P1"), 5*time.Second)
	assert.Equal(t, normalizedPlan, "PLAN-1")
}
