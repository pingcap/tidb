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

package sessiontest

import (
	"context"
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

const (
	statementRUCalibrationUnitsFailpoint = "github.com/pingcap/tidb/pkg/executor/observeStatementRUCalibrationUnitsForTest"
)

type statementRURealTiKVObservation struct {
	sync.Mutex
	calibrationUnits int
	calibrationState string
	cpuWork          float64
	scanBytes        float64
	netBytes         float64
	frontendBytes    float64
}

func TestStatementRUSimpleSelectRealTiKV(t *testing.T) {
	if !*realtikvtest.WithRealTiKV {
		t.Skip("requires a RealTiKV cluster that publishes ExecDetailsV2.RuV2")
	}
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@autocommit = 1")
	tk.MustExec("set @@tidb_enable_non_prepared_plan_cache = off")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int)")
	tk.MustExec("insert into t values (1, 10)")

	connectionID := tk.Session().GetSessionVars().ConnectionID
	observation := &statementRURealTiKVObservation{}
	totalBefore := testutil.ToFloat64(metrics.RUV3Total)
	readBefore := testutil.ToFloat64(metrics.RUV3BySQLType.WithLabelValues(metrics.LblSQLTypeRead))
	tikvBefore := testutil.ToFloat64(metrics.RUV3ByEngine.WithLabelValues(metrics.LblEngineTiKV))
	testfailpoint.EnableCall(t, statementRUCalibrationUnitsFailpoint, func(
		observedConnectionID uint64,
		calibrationState string,
		cpuWork, scanBytes, netBytes, frontendCompileBytes float64,
	) {
		if observedConnectionID != connectionID {
			return
		}
		observation.Lock()
		defer observation.Unlock()
		observation.calibrationUnits++
		observation.calibrationState = calibrationState
		observation.cpuWork = cpuWork
		observation.scanBytes = scanBytes
		observation.netBytes = netBytes
		observation.frontendBytes = frontendCompileBytes
	})
	const query = "select * from t"
	rs, err := tk.ExecWithContext(context.Background(), query)
	require.NoError(t, err)
	require.NotNil(t, rs)
	rows, err := session.GetRows4Test(context.Background(), tk.Session(), rs)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, int64(1), rows[0].GetInt64(0))
	require.Equal(t, int64(10), rows[0].GetInt64(1))

	finisher, ok := rs.(interface{ Finish() error })
	require.True(t, ok)
	require.NoError(t, finisher.Finish())
	require.NoError(t, finisher.Finish())
	observation.Lock()
	require.Zero(t, observation.calibrationUnits)
	observation.Unlock()
	require.Equal(t, totalBefore, testutil.ToFloat64(metrics.RUV3Total))
	require.Equal(t, readBefore, testutil.ToFloat64(metrics.RUV3BySQLType.WithLabelValues(metrics.LblSQLTypeRead)))
	require.Equal(t, tikvBefore, testutil.ToFloat64(metrics.RUV3ByEngine.WithLabelValues(metrics.LblEngineTiKV)))

	require.NoError(t, rs.Close())
	require.NoError(t, rs.Close())
	observation.Lock()
	defer observation.Unlock()
	t.Logf("statement RU observation: calibration=%d state=%s cpu=%v scan=%v net=%v frontend=%v",
		observation.calibrationUnits,
		observation.calibrationState, observation.cpuWork, observation.scanBytes, observation.netBytes, observation.frontendBytes)
	require.Equal(t, 1, observation.calibrationUnits)
	require.Equal(t, "incomplete", observation.calibrationState)
	require.Zero(t, observation.cpuWork)
	require.Positive(t, observation.scanBytes)
	require.Positive(t, observation.netBytes)
	require.Equal(t, float64(len(query)), observation.frontendBytes)
	require.InDelta(t, observation.cpuWork+observation.scanBytes+observation.netBytes+observation.frontendBytes,
		testutil.ToFloat64(metrics.RUV3Total)-totalBefore, 1e-9)
	require.InDelta(t, observation.cpuWork+observation.scanBytes+observation.netBytes+observation.frontendBytes,
		testutil.ToFloat64(metrics.RUV3BySQLType.WithLabelValues(metrics.LblSQLTypeRead))-readBefore, 1e-9)
	require.InDelta(t, observation.scanBytes+observation.netBytes,
		testutil.ToFloat64(metrics.RUV3ByEngine.WithLabelValues(metrics.LblEngineTiKV))-tikvBefore, 1e-9)
	require.Equal(t, observation.netBytes, float64(tk.Session().GetSessionVars().RUV2Metrics.TiKVCoprocessorResponseBytes()))
}
