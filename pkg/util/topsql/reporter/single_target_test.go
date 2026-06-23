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
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/util/topsql/reporter/mock"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockSingleTargetDataSinkRegisterer struct{}

func (r *mockSingleTargetDataSinkRegisterer) Register(dataSink DataSink) error { return nil }

func (r *mockSingleTargetDataSinkRegisterer) Deregister(dataSink DataSink) {}

func mockTopRURecords() []tipb.TopRURecord {
	return []tipb.TopRURecord{{
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
}

// TestSingleTargetDataSink verifies the single-target sink forwards TopSQL
// records and SQL/plan metadata, while TopRU is intentionally not sent.
// It uses an in-process server with bounded waits, so assertions are not timing fragile.
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
	ruRecords := mockTopRURecords()

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
		RURecords: ruRecords,
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
	require.Never(t, func() bool {
		return server.RURecordsCnt() != ruRecordsCnt
	}, 500*time.Millisecond, 10*time.Millisecond)

	assert.Len(t, server.GetLatestRecords(), 1)
	assert.Equal(t, ruRecordsCnt, server.RURecordsCnt())
	assert.Empty(t, server.GetLatestRURecords())
	assert.Len(t, server.GetTotalSQLMetas(), 1)
	sqlMeta, exist := server.GetSQLMetaByDigestBlocking([]byte("S1"), 5*time.Second)
	assert.True(t, exist)
	assert.Equal(t, sqlMeta.NormalizedSql, "SQL-1")
	normalizedPlan, exist := server.GetPlanMetaByDigestBlocking([]byte("P1"), 5*time.Second)
	assert.True(t, exist)
	assert.Equal(t, normalizedPlan, "PLAN-1")
}

// TestSingleTargetDataSinkDropsTopRU verifies TopRU records are dropped by
// SingleTargetDataSink regardless of global TopRU state.
func TestSingleTargetDataSinkDropsTopRU(t *testing.T) {
	t.Run("via TrySend", func(t *testing.T) {
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

		records := mockTopRURecords()
		baseCnt := server.RURecordsCnt()

		err = ds.TrySend(&ReportData{RURecords: records}, time.Now().Add(10*time.Second))
		require.NoError(t, err)
		require.Never(t, func() bool {
			return server.RURecordsCnt() != baseCnt
		}, 500*time.Millisecond, 10*time.Millisecond)

		topsqlstate.EnableTopRU()
		err = ds.TrySend(&ReportData{RURecords: records}, time.Now().Add(10*time.Second))
		require.NoError(t, err)
		require.Never(t, func() bool {
			return server.RURecordsCnt() != baseCnt
		}, 500*time.Millisecond, 10*time.Millisecond)

		latest := server.GetLatestRURecords()
		require.Empty(t, latest)
	})

	t.Run("via sendBatchTopRURecord", func(t *testing.T) {
		ds := NewSingleTargetDataSink(&mockSingleTargetDataSinkRegisterer{})
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		for topsqlstate.TopRUEnabled() {
			topsqlstate.DisableTopRU()
		}
		topsqlstate.EnableTopRU()
		t.Cleanup(func() {
			for topsqlstate.TopRUEnabled() {
				topsqlstate.DisableTopRU()
			}
		})

		records := mockTopRURecords()

		err := ds.sendBatchTopRURecord(ctx, records)
		require.NoError(t, err)
		require.True(t, topsqlstate.TopRUEnabled())

		err = ds.sendBatchTopRURecord(ctx, records)
		require.NoError(t, err)
		require.True(t, topsqlstate.TopRUEnabled())
	})
}

type mockTopRURecordStream struct {
	sendErr     error
	closeErr    error
	closeCalled bool
}

func (m *mockTopRURecordStream) Send(*tipb.TopRURecord) error {
	return m.sendErr
}

func (m *mockTopRURecordStream) CloseAndRecv() (*tipb.EmptyResponse, error) {
	m.closeCalled = true
	return &tipb.EmptyResponse{}, m.closeErr
}

// TestSendTopRURecordsClosesStreamOnUnimplementedSend verifies CloseAndRecv
// is still invoked when Send returns Unimplemented.
func TestSendTopRURecordsClosesStreamOnUnimplementedSend(t *testing.T) {
	stream := &mockTopRURecordStream{
		sendErr:  status.Error(codes.Unimplemented, "topru rpc not supported"),
		closeErr: status.Error(codes.Internal, "close failed"),
	}

	sentCount, err := sendTopRURecords(stream, mockTopRURecords())
	require.NoError(t, err)
	require.Zero(t, sentCount)
	require.True(t, stream.closeCalled)
}
