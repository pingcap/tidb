// Copyright 2021 PingCAP, Inc.
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

package topsql_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/google/pprof/profile"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/topsql"
	"github.com/pingcap/tidb/util/topsql/reporter"
	mockServer "github.com/pingcap/tidb/util/topsql/reporter/mock"
	"github.com/pingcap/tidb/util/topsql/tracecpu"
	"github.com/pingcap/tidb/util/topsql/tracecpu/mock"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type collectorWrapper struct {
	reporter.TopSQLReporter
}

func TestTopSQLCPUProfile(t *testing.T) {
	collector := mock.NewTopSQLCollector()
	tracecpu.GlobalSQLCPUProfiler.SetCollector(&collectorWrapper{collector})
	reqs := []struct {
		sql  string
		plan string
	}{
		{"select * from t where a=?", "point-get"},
		{"select * from t where a>?", "table-scan"},
		{"insert into t values (?)", ""},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, req := range reqs {
		go func(sql, plan string) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					mockExecuteSQL(sql, plan)
				}
			}
		}(req.sql, req.plan)
	}

	// test for StartCPUProfile.
	buf := bytes.NewBuffer(nil)
	err := tracecpu.StartCPUProfile(buf)
	require.NoError(t, err)
	collector.WaitCollectCnt(2)
	err = tracecpu.StopCPUProfile()
	require.NoError(t, err)
	_, err = profile.Parse(buf)
	require.NoError(t, err)

	for _, req := range reqs {
		stats := collector.GetSQLStatsBySQLWithRetry(req.sql, len(req.plan) > 0)
		require.Equal(t, 1, len(stats))
		sql := collector.GetSQL(stats[0].SQLDigest)
		plan := collector.GetPlan(stats[0].PlanDigest)
		require.Equal(t, req.sql, sql)
		require.Equal(t, req.plan, plan)
	}
}

func TestIsEnabled(t *testing.T) {
	setTopSQLEnable(false)
	require.False(t, tracecpu.GlobalSQLCPUProfiler.IsEnabled())

	setTopSQLEnable(true)
	err := tracecpu.StartCPUProfile(bytes.NewBuffer(nil))
	require.NoError(t, err)
	require.True(t, tracecpu.GlobalSQLCPUProfiler.IsEnabled())
	setTopSQLEnable(false)
	require.True(t, tracecpu.GlobalSQLCPUProfiler.IsEnabled())
	err = tracecpu.StopCPUProfile()
	require.NoError(t, err)

	setTopSQLEnable(false)
	require.False(t, tracecpu.GlobalSQLCPUProfiler.IsEnabled())
	setTopSQLEnable(true)
	require.True(t, tracecpu.GlobalSQLCPUProfiler.IsEnabled())
}

func mockPlanBinaryDecoderFunc(plan string) (string, error) {
	return plan, nil
}

func TestTopSQLReporter(t *testing.T) {
	server, err := mockServer.StartMockAgentServer()
	require.NoError(t, err)
	variable.TopSQLVariable.MaxStatementCount.Store(200)
	variable.TopSQLVariable.ReportIntervalSeconds.Store(1)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TopSQL.ReceiverAddress = server.Address()
	})

	report := reporter.NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc)
	ds := reporter.NewSingleTargetDataSink(report)

	defer func() {
		ds.Close()
		report.Close()
	}()

	tracecpu.GlobalSQLCPUProfiler.SetCollector(&collectorWrapper{report})
	reqs := []struct {
		sql  string
		plan string
	}{
		{"select * from t where a=?", "point-get"},
		{"select * from t where a>?", "table-scan"},
		{"insert into t values (?)", ""},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sqlMap := make(map[string]string)
	sql2plan := make(map[string]string)
	for _, req := range reqs {
		sql2plan[req.sql] = req.plan
		sqlDigest := mock.GenSQLDigest(req.sql)
		sqlMap[string(sqlDigest.Bytes())] = req.sql

		go func(sql, plan string) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					mockExecuteSQL(sql, plan)
				}
			}
		}(req.sql, req.plan)
	}

	server.WaitCollectCnt(1, time.Second*5)
	records := server.GetLatestRecords()
	checkSQLPlanMap := map[string]struct{}{}
	for _, req := range records {
		require.Greater(t, len(req.RecordListCpuTimeMs), 0)
		require.Greater(t, req.RecordListCpuTimeMs[0], uint32(0))
		sqlMeta, exist := server.GetSQLMetaByDigestBlocking(req.SqlDigest, time.Second)
		require.True(t, exist)
		expectedNormalizedSQL, exist := sqlMap[string(req.SqlDigest)]
		require.True(t, exist)
		require.Equal(t, expectedNormalizedSQL, sqlMeta.NormalizedSql)

		expectedNormalizedPlan := sql2plan[expectedNormalizedSQL]
		if expectedNormalizedPlan == "" || len(req.PlanDigest) == 0 {
			require.Equal(t, len(req.PlanDigest), 0)
			continue
		}
		normalizedPlan, exist := server.GetPlanMetaByDigestBlocking(req.PlanDigest, time.Second)
		require.True(t, exist)
		require.Equal(t, expectedNormalizedPlan, normalizedPlan)
		checkSQLPlanMap[expectedNormalizedSQL] = struct{}{}
	}
	require.Equal(t, 2, len(checkSQLPlanMap))
}

func TestMaxSQLAndPlanTest(t *testing.T) {
	collector := mock.NewTopSQLCollector()
	tracecpu.GlobalSQLCPUProfiler.SetCollector(&collectorWrapper{collector})

	ctx := context.Background()

	// Test for normal sql and plan
	sql := "select * from t"
	sqlDigest := mock.GenSQLDigest(sql)
	topsql.AttachSQLInfo(ctx, sql, sqlDigest, "", nil, false)
	plan := "TableReader table:t"
	planDigest := genDigest(plan)
	topsql.AttachSQLInfo(ctx, sql, sqlDigest, plan, planDigest, false)

	cSQL := collector.GetSQL(sqlDigest.Bytes())
	require.Equal(t, sql, cSQL)
	cPlan := collector.GetPlan(planDigest.Bytes())
	require.Equal(t, plan, cPlan)

	// Test for huge sql and plan
	sql = genStr(topsql.MaxSQLTextSize + 10)
	sqlDigest = mock.GenSQLDigest(sql)
	topsql.AttachSQLInfo(ctx, sql, sqlDigest, "", nil, false)
	plan = genStr(topsql.MaxBinaryPlanSize + 10)
	planDigest = genDigest(plan)
	topsql.AttachSQLInfo(ctx, sql, sqlDigest, plan, planDigest, false)

	cSQL = collector.GetSQL(sqlDigest.Bytes())
	require.Equal(t, sql[:topsql.MaxSQLTextSize], cSQL)
	cPlan = collector.GetPlan(planDigest.Bytes())
	require.Empty(t, cPlan)
}

func TestTopSQLPubSub(t *testing.T) {
	variable.TopSQLVariable.MaxStatementCount.Store(200)
	variable.TopSQLVariable.ReportIntervalSeconds.Store(1)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TopSQL.ReceiverAddress = "mock"
	})

	report := reporter.NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc)
	defer report.Close()
	tracecpu.GlobalSQLCPUProfiler.SetCollector(&collectorWrapper{report})

	server, err := mockServer.NewMockPubSubServer()
	require.NoError(t, err)
	pubsubService := reporter.NewTopSQLPubSubService(report)
	tipb.RegisterTopSQLPubSubServer(server.Server(), pubsubService)
	go server.Serve()
	defer server.Stop()

	conn, err := grpc.Dial(
		server.Address(),
		grpc.WithBlock(),
		grpc.WithInsecure(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    10 * time.Second,
			Timeout: 3 * time.Second,
		}),
	)
	require.NoError(t, err)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := tipb.NewTopSQLPubSubClient(conn)
	stream, err := client.Subscribe(ctx, &tipb.TopSQLSubRequest{})
	require.NoError(t, err)

	reqs := []struct {
		sql  string
		plan string
	}{
		{"select * from t where a=?", "point-get"},
		{"select * from t where a>?", "table-scan"},
		{"insert into t values (?)", ""},
	}

	digest2sql := make(map[string]string)
	sql2plan := make(map[string]string)
	for _, req := range reqs {
		sql2plan[req.sql] = req.plan
		sqlDigest := mock.GenSQLDigest(req.sql)
		digest2sql[string(sqlDigest.Bytes())] = req.sql

		go func(sql, plan string) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					mockExecuteSQL(sql, plan)
				}
			}
		}(req.sql, req.plan)
	}

	sqlMetas := make(map[string]*tipb.SQLMeta)
	planMetas := make(map[string]string)
	records := make(map[string]*tipb.CPUTimeRecord)

	for {
		r, err := stream.Recv()
		if err != nil {
			break
		}

		if r.GetRecord() != nil {
			rec := r.GetRecord()
			if _, ok := records[string(rec.SqlDigest)]; !ok {
				records[string(rec.SqlDigest)] = rec
			} else {
				cpu := records[string(rec.SqlDigest)]
				if rec.PlanDigest != nil {
					cpu.PlanDigest = rec.PlanDigest
				}
				cpu.RecordListTimestampSec = append(cpu.RecordListTimestampSec, rec.RecordListTimestampSec...)
				cpu.RecordListCpuTimeMs = append(cpu.RecordListCpuTimeMs, rec.RecordListCpuTimeMs...)
			}
		} else if r.GetSqlMeta() != nil {
			sql := r.GetSqlMeta()
			if _, ok := sqlMetas[string(sql.SqlDigest)]; !ok {
				sqlMetas[string(sql.SqlDigest)] = sql
			}
		} else if r.GetPlanMeta() != nil {
			plan := r.GetPlanMeta()
			if _, ok := planMetas[string(plan.PlanDigest)]; !ok {
				planMetas[string(plan.PlanDigest)] = plan.NormalizedPlan
			}
		}
	}

	checkSQLPlanMap := map[string]struct{}{}
	for i := range records {
		record := records[i]
		require.Greater(t, len(record.RecordListCpuTimeMs), 0)
		require.Greater(t, record.RecordListCpuTimeMs[0], uint32(0))
		sqlMeta, exist := sqlMetas[string(record.SqlDigest)]
		require.True(t, exist)
		expectedNormalizedSQL, exist := digest2sql[string(record.SqlDigest)]
		require.True(t, exist)
		require.Equal(t, expectedNormalizedSQL, sqlMeta.NormalizedSql)

		expectedNormalizedPlan := sql2plan[expectedNormalizedSQL]
		if expectedNormalizedPlan == "" || len(record.PlanDigest) == 0 {
			require.Equal(t, len(record.PlanDigest), 0)
			continue
		}
		normalizedPlan, exist := planMetas[string(record.PlanDigest)]
		require.True(t, exist)
		require.Equal(t, expectedNormalizedPlan, normalizedPlan)
		checkSQLPlanMap[expectedNormalizedSQL] = struct{}{}
	}
	require.Equal(t, len(checkSQLPlanMap), 2)
}

func TestPubSubWhenReporterIsStopped(t *testing.T) {
	report := reporter.NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc)

	server, err := mockServer.NewMockPubSubServer()
	require.NoError(t, err)

	pubsubService := reporter.NewTopSQLPubSubService(report)
	tipb.RegisterTopSQLPubSubServer(server.Server(), pubsubService)
	go server.Serve()
	defer server.Stop()

	// stop reporter first
	report.Close()

	// try to subscribe
	conn, err := grpc.Dial(
		server.Address(),
		grpc.WithBlock(),
		grpc.WithInsecure(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    10 * time.Second,
			Timeout: 3 * time.Second,
		}),
	)
	require.NoError(t, err)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := tipb.NewTopSQLPubSubClient(conn)
	stream, err := client.Subscribe(ctx, &tipb.TopSQLSubRequest{})
	require.NoError(t, err)

	_, err = stream.Recv()
	require.Error(t, err, "reporter is closed")
}

func setTopSQLEnable(enabled bool) {
	variable.TopSQLVariable.Enable.Store(enabled)
}

func mockExecuteSQL(sql, plan string) {
	ctx := context.Background()
	sqlDigest := mock.GenSQLDigest(sql)
	topsql.AttachSQLInfo(ctx, sql, sqlDigest, "", nil, false)
	mockExecute(time.Millisecond * 100)
	planDigest := genDigest(plan)
	topsql.AttachSQLInfo(ctx, sql, sqlDigest, plan, planDigest, false)
	mockExecute(time.Millisecond * 300)
}

func mockExecute(d time.Duration) {
	start := time.Now()
	for {
		for i := 0; i < 10e5; i++ {
		}
		if time.Since(start) > d {
			return
		}
	}
}

func genDigest(str string) *parser.Digest {
	if str == "" {
		return parser.NewDigest(nil)
	}
	return parser.DigestNormalized(str)
}

func genStr(n int) string {
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = 'a' + byte(i%25)
	}
	return string(buf)
}
