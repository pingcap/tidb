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

	"github.com/pingcap/tidb/util/topsql/tracecpu"
)

// SetupTopSQL sets up the top-sql worker.
func SetupTopSQL() {
	tracecpu.GlobalTopSQLCPUProfiler.Run()
}

// SetSQLLabels sets the SQL digest label.
func SetSQLLabels(ctx context.Context, normalizedSQL, sqlDigest string) context.Context {
	if len(normalizedSQL) == 0 || len(sqlDigest) == 0 {
		return ctx
	}
	ctx = tracecpu.SetSQLLabels(ctx, normalizedSQL, sqlDigest)
	registerSQL(sqlDigest, normalizedSQL)
	return ctx
}

// SetSQLAndPlanLabels sets the SQL and plan digest label.
func SetSQLAndPlanLabels(ctx context.Context, sqlDigest, planDigest, normalizedPlan string) context.Context {
	ctx = tracecpu.SetSQLAndPlanLabels(ctx, sqlDigest, planDigest)
	registerPlan(planDigest, normalizedPlan)
	return ctx
}

func registerSQL(sqlDigest, normalizedSQL string) {
	c := tracecpu.GlobalTopSQLCPUProfiler.GetCollector()
	if c == nil {
		return
	}
	c.RegisterSQL(sqlDigest, normalizedSQL)
}

func registerPlan(planDigest string, normalizedPlan string) {
	c := tracecpu.GlobalTopSQLCPUProfiler.GetCollector()
	if c == nil {
		return
	}
	c.RegisterPlan(planDigest, normalizedPlan)
}
