// Copyright 2026 PingCAP, Inc.
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

package server

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/resourcegroup"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func BenchmarkClientConnAddMetricsPointSelect(b *testing.B) {
	cc := newClientConnForMetricsBenchmark(b)
	startTime := time.Now().Add(-time.Millisecond)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		cc.addMetrics(mysql.ComStmtExecute, startTime, nil)
	}
}

func BenchmarkClientConnAddMetricsChangingLabels(b *testing.B) {
	cc := newClientConnForMetricsBenchmark(b)
	vars := cc.ctx.GetSessionVars()
	startTime := time.Now().Add(-time.Millisecond)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if vars.StmtCtx.StmtType == "Select" {
			vars.StmtCtx.StmtType = "Update"
			vars.ResourceGroupName = "benchmark_rg"
			vars.StmtCtx.ResourceGroupName = "benchmark_rg"
		} else {
			vars.StmtCtx.StmtType = "Select"
			vars.ResourceGroupName = resourcegroup.DefaultResourceGroupName
			vars.StmtCtx.ResourceGroupName = resourcegroup.DefaultResourceGroupName
		}
		cc.addMetrics(mysql.ComStmtExecute, startTime, nil)
	}
}

func newClientConnForMetricsBenchmark(b *testing.B) *clientConn {
	store := testkit.CreateMockStore(b)
	se, err := session.CreateSession4Test(store)
	require.NoError(b, err)
	b.Cleanup(func() {
		se.Close()
	})

	vars := se.GetSessionVars()
	vars.StmtCtx.StmtType = "Select"
	vars.ResourceGroupName = resourcegroup.DefaultResourceGroupName
	vars.StmtCtx.ResourceGroupName = resourcegroup.DefaultResourceGroupName

	cc := &clientConn{}
	cc.ctx.TiDBContext = &TiDBContext{Session: se}
	return cc
}
