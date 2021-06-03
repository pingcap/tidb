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
// See the License for the specific language governing permissions and
// limitations under the License.

package topsql

import (
	"context"
	"runtime/pprof"
	"time"

	"github.com/pingcap/parser"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/pingcap/tidb/util/topsql/reporter"
	"github.com/pingcap/tidb/util/topsql/tracecpu"
)

const (
	// TODO: turn reportInterval and reportTimeout into sysvar
	reportInterval = 60 * time.Second
	reportTimeout  = 45 * time.Second
)

// SetupTopSQL sets up the top-sql worker.
func SetupTopSQL() {
	reporter := reporter.NewRemoteTopSQLReporter(&reporter.RemoteTopSQLReporterConfig{
		PlanBinaryDecoder: plancodec.DecodeNormalizedPlan,
		MaxStatementsNum:  int(variable.TopSQLVariable.MaxStatementCount.Load()),
		ReportInterval:    reportInterval,
		ReportTimeout:     reportTimeout,
		AgentGRPCAddress:  variable.TopSQLVariable.AgentAddress.Load(),
	})
	tracecpu.GlobalSQLCPUProfiler.SetReporter(reporter)
	tracecpu.GlobalSQLCPUProfiler.Run()
}

// AttachSQLInfo attach the sql information info top sql.
func AttachSQLInfo(ctx context.Context, normalizedSQL string, sqlDigest *parser.Digest, normalizedPlan string, planDigest *parser.Digest) context.Context {
	if len(normalizedSQL) == 0 || sqlDigest == nil || len(sqlDigest.Bytes()) == 0 {
		return ctx
	}
	var sqlDigestBytes, planDigestBytes []byte
	sqlDigestBytes = sqlDigest.Bytes()
	if planDigest != nil {
		planDigestBytes = planDigest.Bytes()
	}
	ctx = tracecpu.CtxWithDigest(ctx, sqlDigestBytes, planDigestBytes)
	pprof.SetGoroutineLabels(ctx)

	if len(normalizedPlan) == 0 || len(planDigestBytes) == 0 {
		// If plan digest is '', indicate it is the first time to attach the SQL info, since it only know the sql digest.
		linkSQLTextWithDigest(sqlDigestBytes, normalizedSQL)
	} else {
		linkPlanTextWithDigest(planDigestBytes, normalizedPlan)
	}
	return ctx
}

func linkSQLTextWithDigest(sqlDigest []byte, normalizedSQL string) {
	c := tracecpu.GlobalSQLCPUProfiler.GetReporter()
	if c == nil {
		return
	}
	topc, ok := c.(reporter.TopSQLReporter)
	if ok {
		topc.RegisterSQL(sqlDigest, normalizedSQL)
	}
}

func linkPlanTextWithDigest(planDigest []byte, normalizedPlan string) {
	c := tracecpu.GlobalSQLCPUProfiler.GetReporter()
	if c == nil {
		return
	}
	topc, ok := c.(reporter.TopSQLReporter)
	if ok {
		topc.RegisterPlan(planDigest, normalizedPlan)
	}
}
