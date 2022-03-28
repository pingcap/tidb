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
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/cpuprofile"
	"github.com/pingcap/tidb/util/topsql"
	"github.com/pingcap/tidb/util/topsql/collector"
	"github.com/pingcap/tidb/util/topsql/collector/mock"
	"github.com/pingcap/tidb/util/topsql/reporter"
	mockServer "github.com/pingcap/tidb/util/topsql/reporter/mock"
	topsqlstate "github.com/pingcap/tidb/util/topsql/state"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func TestTopSQLCPUProfile(t *testing.T) {
	err := cpuprofile.StartCPUProfiler()
	require.NoError(t, err)
	defer cpuprofile.StopCPUProfiler()

	topsqlstate.EnableTopSQL()
	mc := mock.NewTopSQLCollector()
	topsql.SetupTopSQLForTest(mc)
	sqlCPUCollector := collector.NewSQLCPUCollector(mc)
	sqlCPUCollector.Start()
	defer sqlCPUCollector.Stop()
	reqs := []struct {
		sql  string
		plan string
	}{
		{"select * from t where a=?", "point-get"},
		{"select * from t where a>?", "table-scan"},
		{"insert into t values (?)", ""},
	}
	var wg util.WaitGroupWrapper
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, req := range reqs {
		sql, plan := req.sql, req.plan
		wg.Run(func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					mockExecuteSQL(sql, plan)
				}
			}
		})
	}

	for _, req := range reqs {
		stats := mc.GetSQLStatsBySQLWithRetry(req.sql, len(req.plan) > 0)
		require.Equal(t, 1, len(stats))
		sql := mc.GetSQL(stats[0].SQLAndPlan.SQLDigest)
		plan := mc.GetPlan(stats[0].SQLAndPlan.PlanDigest)
		require.Equal(t, req.sql, sql)
		require.Equal(t, req.plan, plan)
	}
}

func mockPlanBinaryDecoderFunc(plan string) (string, error) {
	return plan, nil
}

func TestTopSQLReporter(t *testing.T) {
	err := cpuprofile.StartCPUProfiler()
	require.NoError(t, err)
	defer cpuprofile.StopCPUProfiler()

	server, err := mockServer.StartMockAgentServer()
	require.NoError(t, err)
	topsqlstate.GlobalState.MaxStatementCount.Store(200)
	topsqlstate.GlobalState.ReportIntervalSeconds.Store(1)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TopSQL.ReceiverAddress = server.Address()
	})

	topsqlstate.EnableTopSQL()
	report := reporter.NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc)
	report.Start()
	ds := reporter.NewSingleTargetDataSink(report)
	ds.Start()
	topsql.SetupTopSQLForTest(report)

	defer func() {
		ds.Close()
		report.Close()
		server.Stop()
	}()

	reqs := []struct {
		sql  string
		plan string
	}{
		{"select * from t where a=?", "point-get"},
		{"select * from t where a>?", "table-scan"},
		{"insert into t values (?)", ""},
	}
	var wg util.WaitGroupWrapper
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sqlMap := make(map[parser.RawDigestString]string)
	sql2plan := make(map[string]string)
	recordsCnt := server.RecordsCnt()
	for _, req := range reqs {
		sql2plan[req.sql] = req.plan
		sqlDigest := mock.GenSQLDigest(req.sql)
		sqlMap[sqlDigest.RawAsString()] = req.sql
		sql, plan := req.sql, req.plan
		wg.Run(func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					mockExecuteSQL(sql, plan)
				}
			}
		})
	}
	checkSQLPlanMap := map[string]struct{}{}
	for retry := 0; retry < 5; retry++ {
		server.WaitCollectCnt(recordsCnt, 1, time.Second*5)
		records := server.GetLatestRecords()
		for _, req := range records {
			require.Greater(t, len(req.Items), 0)
			require.Greater(t, req.Items[0].CpuTimeMs, uint32(0))
			sqlMeta, exist := server.GetSQLMetaByDigestBlocking(parser.RawDigestString(req.SqlDigest), time.Second)
			require.True(t, exist)
			expectedNormalizedSQL, exist := sqlMap[parser.RawDigestString(req.SqlDigest)]
			require.True(t, exist)
			require.Equal(t, expectedNormalizedSQL, sqlMeta.NormalizedSql)

			expectedNormalizedPlan := sql2plan[expectedNormalizedSQL]
			if expectedNormalizedPlan == "" || len(req.PlanDigest) == 0 {
				require.Len(t, req.PlanDigest, 0)
				checkSQLPlanMap[expectedNormalizedSQL] = struct{}{}
				continue
			}
			normalizedPlan, exist := server.GetPlanMetaByDigestBlocking(parser.RawDigestString(req.PlanDigest), time.Second)
			require.True(t, exist)
			require.Equal(t, expectedNormalizedPlan, normalizedPlan)
			checkSQLPlanMap[expectedNormalizedSQL] = struct{}{}
		}
		if len(checkSQLPlanMap) == len(reqs) {
			break
		}
	}
	require.Equal(t, len(reqs), len(checkSQLPlanMap))
}

func TestMaxSQLAndPlanTest(t *testing.T) {
	err := cpuprofile.StartCPUProfiler()
	require.NoError(t, err)
	defer cpuprofile.StopCPUProfiler()

	collector := mock.NewTopSQLCollector()
	topsql.SetupTopSQLForTest(collector)

	ctx := context.Background()

	// Test for normal sql and plan
	sql := "select * from t"
	sqlDigest := mock.GenSQLDigest(sql)
	topsql.AttachSQLInfo(ctx, sql, sqlDigest.RawAsString(), "", "", false)
	plan := "TableReader table:t"
	planDigest := genDigest(plan)
	topsql.AttachSQLInfo(ctx, sql, sqlDigest.RawAsString(), plan, planDigest.RawAsString(), false)

	cSQL := collector.GetSQL(sqlDigest.RawAsString())
	require.Equal(t, sql, cSQL)
	cPlan := collector.GetPlan(planDigest.RawAsString())
	require.Equal(t, plan, cPlan)

	// Test for huge sql and plan
	sql = genStr(topsql.MaxSQLTextSize + 10)
	sqlDigest = mock.GenSQLDigest(sql)
	topsql.AttachSQLInfo(ctx, sql, sqlDigest.RawAsString(), "", "", false)
	plan = genStr(topsql.MaxBinaryPlanSize + 10)
	planDigest = genDigest(plan)
	topsql.AttachSQLInfo(ctx, sql, sqlDigest.RawAsString(), plan, planDigest.RawAsString(), false)

	cSQL = collector.GetSQL(sqlDigest.RawAsString())
	require.Equal(t, sql[:topsql.MaxSQLTextSize], cSQL)
	cPlan = collector.GetPlan(planDigest.RawAsString())
	require.Empty(t, cPlan)
}

func TestTopSQLPubSub(t *testing.T) {
	err := cpuprofile.StartCPUProfiler()
	require.NoError(t, err)
	defer cpuprofile.StopCPUProfiler()

	topsqlstate.GlobalState.MaxStatementCount.Store(200)
	topsqlstate.GlobalState.ReportIntervalSeconds.Store(1)

	topsqlstate.EnableTopSQL()
	report := reporter.NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc)
	report.Start()
	defer report.Close()
	topsql.SetupTopSQLForTest(report)

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
	var wg util.WaitGroupWrapper
	defer wg.Wait()
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

	digest2sql := make(map[parser.RawDigestString]string)
	sql2plan := make(map[string]string)
	for _, req := range reqs {
		sql2plan[req.sql] = req.plan
		sqlDigest := mock.GenSQLDigest(req.sql)
		digest2sql[sqlDigest.RawAsString()] = req.sql
		sql, plan := req.sql, req.plan
		wg.Run(func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					mockExecuteSQL(sql, plan)
				}
			}
		})
	}

	sqlMetas := make(map[parser.RawDigestString]*tipb.SQLMeta)
	planMetas := make(map[parser.RawDigestString]string)
	records := make(map[parser.RawDigestString]*tipb.TopSQLRecord)

	for {
		r, err := stream.Recv()
		if err != nil {
			break
		}

		if r.GetRecord() != nil {
			rec := r.GetRecord()
			if _, ok := records[parser.RawDigestString(rec.SqlDigest)]; !ok {
				records[parser.RawDigestString(rec.SqlDigest)] = rec
			} else {
				record := records[parser.RawDigestString(rec.SqlDigest)]
				if rec.PlanDigest != nil {
					record.PlanDigest = rec.PlanDigest
				}
				record.Items = append(record.Items, rec.Items...)
			}
		} else if r.GetSqlMeta() != nil {
			sql := r.GetSqlMeta()
			if _, ok := sqlMetas[parser.RawDigestString(sql.SqlDigest)]; !ok {
				sqlMetas[parser.RawDigestString(sql.SqlDigest)] = sql
			}
		} else if r.GetPlanMeta() != nil {
			plan := r.GetPlanMeta()
			if _, ok := planMetas[parser.RawDigestString(plan.PlanDigest)]; !ok {
				planMetas[parser.RawDigestString(plan.PlanDigest)] = plan.NormalizedPlan
			}
		}
	}

	checkSQLPlanMap := map[string]struct{}{}
	for i := range records {
		record := records[i]
		require.Greater(t, len(record.Items), 0)
		require.Greater(t, record.Items[0].CpuTimeMs, uint32(0))
		sqlMeta, exist := sqlMetas[parser.RawDigestString(record.SqlDigest)]
		require.True(t, exist)
		expectedNormalizedSQL, exist := digest2sql[parser.RawDigestString(record.SqlDigest)]
		require.True(t, exist)
		require.Equal(t, expectedNormalizedSQL, sqlMeta.NormalizedSql)

		expectedNormalizedPlan := sql2plan[expectedNormalizedSQL]
		if expectedNormalizedPlan == "" || len(record.PlanDigest) == 0 {
			require.Len(t, record.PlanDigest, 0)
			continue
		}
		normalizedPlan, exist := planMetas[parser.RawDigestString(record.PlanDigest)]
		require.True(t, exist)
		require.Equal(t, expectedNormalizedPlan, normalizedPlan)
		checkSQLPlanMap[expectedNormalizedSQL] = struct{}{}
	}
	require.Len(t, checkSQLPlanMap, 2)
}

func TestPubSubWhenReporterIsStopped(t *testing.T) {
	topsqlstate.EnableTopSQL()
	report := reporter.NewRemoteTopSQLReporter(mockPlanBinaryDecoderFunc)
	report.Start()

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

func mockExecuteSQL(sql, plan string) {
	ctx := context.Background()
	sqlDigest := mock.GenSQLDigest(sql)
	topsql.AttachSQLInfo(ctx, sql, sqlDigest.RawAsString(), "", "", false)
	mockExecute(time.Millisecond * 100)
	planDigest := genDigest(plan)
	topsql.AttachSQLInfo(ctx, sql, sqlDigest.RawAsString(), plan, planDigest.RawAsString(), false)
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
