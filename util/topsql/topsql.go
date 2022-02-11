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

package topsql

import (
	"context"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/pingcap/tidb/util/topsql/collector"
	"github.com/pingcap/tidb/util/topsql/reporter"
	"github.com/pingcap/tidb/util/topsql/stmtstats"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	// MaxSQLTextSize exports for testing.
	MaxSQLTextSize = 4 * 1024
	// MaxBinaryPlanSize exports for testing.
	MaxBinaryPlanSize = 2 * 1024
)

var (
	globalTopSQLReport   reporter.TopSQLReporter
	singleTargetDataSink *reporter.SingleTargetDataSink
)

func init() {
	remoteReporter := reporter.NewRemoteTopSQLReporter(plancodec.DecodeNormalizedPlan)
	globalTopSQLReport = remoteReporter
	singleTargetDataSink = reporter.NewSingleTargetDataSink(remoteReporter)
}

// SetupTopSQL sets up the top-sql worker.
func SetupTopSQL() {
	globalTopSQLReport.Start()
	singleTargetDataSink.Start()

	stmtstats.RegisterCollector(globalTopSQLReport)
	stmtstats.SetupAggregator()
}

// SetupTopSQLForTest sets up the global top-sql reporter, it's exporting for test.
func SetupTopSQLForTest(r reporter.TopSQLReporter) {
	globalTopSQLReport = r
}

// RegisterPubSubServer registers TopSQLPubSubService to the given gRPC server.
func RegisterPubSubServer(s *grpc.Server) {
	if register, ok := globalTopSQLReport.(reporter.DataSinkRegisterer); ok {
		service := reporter.NewTopSQLPubSubService(register)
		tipb.RegisterTopSQLPubSubServer(s, service)
	}
}

// Close uses to close and release the top sql resource.
func Close() {
	singleTargetDataSink.Close()
	globalTopSQLReport.Close()
	stmtstats.CloseAggregator()
}

// AttachSQLInfo attach the sql information info top sql.
func AttachSQLInfo(ctx context.Context, normalizedSQL string, sqlDigest *parser.Digest, normalizedPlan string, planDigest *parser.Digest, isInternal bool) context.Context {
	if len(normalizedSQL) == 0 || sqlDigest == nil || len(sqlDigest.Bytes()) == 0 {
		return ctx
	}
	var sqlDigestBytes, planDigestBytes []byte
	sqlDigestBytes = sqlDigest.Bytes()
	if planDigest != nil {
		planDigestBytes = planDigest.Bytes()
	}
	ctx = collector.CtxWithDigest(ctx, sqlDigestBytes, planDigestBytes)
	pprof.SetGoroutineLabels(ctx)

	if len(normalizedPlan) == 0 || len(planDigestBytes) == 0 {
		// If plan digest is '', indicate it is the first time to attach the SQL info, since it only know the sql digest.
		linkSQLTextWithDigest(sqlDigestBytes, normalizedSQL, isInternal)
	} else {
		linkPlanTextWithDigest(planDigestBytes, normalizedPlan)
	}
	failpoint.Inject("mockHighLoadForEachSQL", func(val failpoint.Value) {
		// In integration test, some SQL run very fast that Top SQL pprof profile unable to sample data of those SQL,
		// So need mock some high cpu load to make sure pprof profile successfully samples the data of those SQL.
		// Attention: Top SQL pprof profile unable to sample data of those SQL which run very fast, this behavior is expected.
		// The integration test was just want to make sure each type of SQL will be set goroutine labels and and can be collected.
		if val.(bool) {
			lowerSQL := strings.ToLower(normalizedSQL)
			if strings.Contains(lowerSQL, "mysql") && !strings.Contains(lowerSQL, "global_variables") {
				failpoint.Return(ctx)
			}
			isDML := false
			for _, prefix := range []string{"insert", "update", "delete", "load", "replace", "select", "begin",
				"commit", "analyze", "explain", "trace", "create", "set global"} {
				if strings.HasPrefix(lowerSQL, prefix) {
					isDML = true
					break
				}
			}
			if !isDML {
				failpoint.Return(ctx)
			}
			start := time.Now()
			logutil.BgLogger().Info("attach SQL info", zap.String("sql", normalizedSQL), zap.Bool("has-plan", len(normalizedPlan) > 0))
			for {
				if time.Since(start) > 11*time.Millisecond {
					break
				}
				for i := 0; i < 10e5; i++ {
				}
			}
		}
	})
	return ctx
}

func linkSQLTextWithDigest(sqlDigest []byte, normalizedSQL string, isInternal bool) {
	if len(normalizedSQL) > MaxSQLTextSize {
		normalizedSQL = normalizedSQL[:MaxSQLTextSize]
	}

	globalTopSQLReport.RegisterSQL(sqlDigest, normalizedSQL, isInternal)
}

func linkPlanTextWithDigest(planDigest []byte, normalizedBinaryPlan string) {
	if len(normalizedBinaryPlan) > MaxBinaryPlanSize {
		// ignore the huge size plan
		return
	}

	globalTopSQLReport.RegisterPlan(planDigest, normalizedBinaryPlan)
}
