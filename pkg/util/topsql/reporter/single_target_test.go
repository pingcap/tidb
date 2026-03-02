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
	"net"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/util/topsql/reporter/mock"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockSingleTargetDataSinkRegisterer struct{}

func (r *mockSingleTargetDataSinkRegisterer) Register(dataSink DataSink) error { return nil }

func (r *mockSingleTargetDataSinkRegisterer) Deregister(dataSink DataSink) {}

func TestSingleTargetDataSink(t *testing.T) {
	server, err := mock.StartMockAgentServer()
	assert.NoError(t, err)
	defer server.Stop()

	for topsqlstate.TopRUEnabled() {
		topsqlstate.DisableTopRU()
	}
	topsqlstate.EnableTopRU()
	t.Cleanup(func() {
		for topsqlstate.TopRUEnabled() {
			topsqlstate.DisableTopRU()
		}
	})

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TopSQL.ReceiverAddress = server.Address()
	})

	ds := NewSingleTargetDataSink(&mockSingleTargetDataSinkRegisterer{})
	ds.Start()
	defer ds.Close()

	recordsCnt := server.RecordsCnt()
	sqlMetaCnt := server.SQLMetaCnt()
	ruRecordsCnt := server.RURecordsCnt()

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
		RURecords: []tipb.TopRURecord{{
			User:       "user1",
			SqlDigest:  []byte("S1"),
			PlanDigest: []byte("P1"),
			Items: []*tipb.TopRURecordItem{{
				TimestampSec: 1,
				TotalRu:      1.5,
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

	server.WaitCollectCnt(recordsCnt, 1, 5*time.Second)
	server.WaitCollectCntOfSQLMeta(sqlMetaCnt, 1, 5*time.Second)
	start := time.Now()
	for {
		if server.RURecordsCnt() > ruRecordsCnt || time.Since(start) > 5*time.Second {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	assert.Len(t, server.GetLatestRecords(), 1)
	assert.Greater(t, server.RURecordsCnt(), ruRecordsCnt)
	assert.Len(t, server.GetLatestRURecords(), 1)
	assert.Len(t, server.GetTotalSQLMetas(), 1)
	sqlMeta, exist := server.GetSQLMetaByDigestBlocking([]byte("S1"), 5*time.Second)
	assert.True(t, exist)
	assert.Equal(t, sqlMeta.NormalizedSql, "SQL-1")
	normalizedPlan, exist := server.GetPlanMetaByDigestBlocking([]byte("P1"), 5*time.Second)
	assert.True(t, exist)
	assert.Equal(t, normalizedPlan, "PLAN-1")
}

func TestSingleTargetDataSinkSendBatchTopRURecordRespectsTopRUState(t *testing.T) {
	server, err := mock.StartMockAgentServer()
	require.NoError(t, err)
	defer server.Stop()

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TopSQL.ReceiverAddress = server.Address()
	})

	ds := NewSingleTargetDataSink(&mockSingleTargetDataSinkRegisterer{})
	ds.Start()
	defer ds.Close()

	for topsqlstate.TopRUEnabled() {
		topsqlstate.DisableTopRU()
	}
	t.Cleanup(func() {
		for topsqlstate.TopRUEnabled() {
			topsqlstate.DisableTopRU()
		}
	})

	records := []tipb.TopRURecord{{
		User:       "user1",
		SqlDigest:  []byte("S1"),
		PlanDigest: []byte("P1"),
		Items: []*tipb.TopRURecordItem{{
			TimestampSec: 1,
			TotalRu:      1.5,
			ExecCount:    1,
			ExecDuration: 1,
		}},
	}}

	baseCnt := server.RURecordsCnt()

	// TopRU disabled: sendBatchTopRURecord should no-op and not send any batch.
	err = ds.TrySend(&ReportData{RURecords: records}, time.Now().Add(10*time.Second))
	require.NoError(t, err)
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		require.Equal(t, baseCnt, server.RURecordsCnt())
		time.Sleep(10 * time.Millisecond)
	}

	topsqlstate.EnableTopRU()
	err = ds.TrySend(&ReportData{RURecords: records}, time.Now().Add(10*time.Second))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return server.RURecordsCnt() == baseCnt+1
	}, 5*time.Second, 10*time.Millisecond)

	latest := server.GetLatestRURecords()
	require.Len(t, latest, 1)
	assert.Equal(t, "user1", latest[0].User)
	assert.Equal(t, []byte("S1"), latest[0].SqlDigest)
}

type unimplementedTopRUAgentServer struct {
	tipb.UnimplementedTopSQLAgentServer
}

func (s *unimplementedTopRUAgentServer) ReportTopRURecords(tipb.TopSQLAgent_ReportTopRURecordsServer) error {
	return status.Error(codes.Unimplemented, "topru rpc not supported")
}

func TestSingleTargetDataSink_TopRU_Unimplemented_DegradesGracefully(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	tipb.RegisterTopSQLAgentServer(grpcServer, &unimplementedTopRUAgentServer{})
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = lis.Close()
	})

	ds := NewSingleTargetDataSink(&mockSingleTargetDataSinkRegisterer{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, ds.tryEstablishConnection(ctx, lis.Addr().String()))
	t.Cleanup(func() {
		if ds.conn != nil {
			_ = ds.conn.Close()
			ds.conn = nil
		}
	})

	for topsqlstate.TopRUEnabled() {
		topsqlstate.DisableTopRU()
	}
	topsqlstate.EnableTopRU()
	t.Cleanup(func() {
		for topsqlstate.TopRUEnabled() {
			topsqlstate.DisableTopRU()
		}
	})

	records := []tipb.TopRURecord{{
		User:      "user1",
		SqlDigest: []byte("S1"),
		Items: []*tipb.TopRURecordItem{{
			TimestampSec: 1,
			TotalRu:      1,
			ExecCount:    1,
			ExecDuration: 1,
		}},
	}}

	err = ds.sendBatchTopRURecord(ctx, records)
	require.NoError(t, err)
	require.True(t, topsqlstate.TopRUEnabled())

	err = ds.sendBatchTopRURecord(ctx, records)
	require.NoError(t, err)
	require.True(t, topsqlstate.TopRUEnabled())
}
