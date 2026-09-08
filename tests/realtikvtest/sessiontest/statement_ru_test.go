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
	"github.com/pingcap/tidb/pkg/util/chunk"
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
	hashStateRows    float64
	joinOutputRows   float64
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
	tk.MustExec("insert into t values (1, 10), (2, 20), (3, 30)")
	tk.MustExec("set tidb_hash_join_version = 'optimized'")

	connectionID := tk.Session().GetSessionVars().ConnectionID
	observation := &statementRURealTiKVObservation{}
	testfailpoint.EnableCall(t, statementRUCalibrationUnitsFailpoint, func(
		observedConnectionID uint64,
		calibrationState string,
		cpuWork, scanBytes, netBytes, frontendCompileBytes, hashStateRows, joinOutputRows float64,
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
		observation.hashStateRows = hashStateRows
		observation.joinOutputRows = joinOutputRows
	})

	testCases := []struct {
		name           string
		query          string
		rowCount       int
		validateRows   func(*testing.T, []chunk.Row)
		wantCPUWork    bool
		wantHashState  bool
		wantJoinOutput bool
	}{
		{
			name:     "reader",
			query:    "select * from t",
			rowCount: 3,
			validateRows: func(t *testing.T, rows []chunk.Row) {
				for _, row := range rows {
					require.Equal(t, row.GetInt64(0)*10, row.GetInt64(1))
				}
			},
		},
		{
			name:     "hash join",
			query:    "select /*+ HASH_JOIN(t1, t2) */ * from t t1 join t t2 on t1.a = t2.a",
			rowCount: 3,
			validateRows: func(t *testing.T, rows []chunk.Row) {
				for _, row := range rows {
					require.Equal(t, row.GetInt64(0), row.GetInt64(2))
					require.Equal(t, row.GetInt64(1), row.GetInt64(3))
				}
			},
			wantCPUWork:    true,
			wantHashState:  true,
			wantJoinOutput: true,
		},
		{
			name:     "hash aggregation",
			query:    "select /*+ HASH_AGG() */ count(*) from t group by b",
			rowCount: 3,
			validateRows: func(t *testing.T, rows []chunk.Row) {
				for _, row := range rows {
					require.Equal(t, int64(1), row.GetInt64(0))
				}
			},
			wantCPUWork:   true,
			wantHashState: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			observation.Lock()
			observation.calibrationUnits = 0
			observation.calibrationState = ""
			observation.cpuWork = 0
			observation.scanBytes = 0
			observation.netBytes = 0
			observation.frontendBytes = 0
			observation.hashStateRows = 0
			observation.joinOutputRows = 0
			observation.Unlock()

			totalBefore := testutil.ToFloat64(metrics.RUV3Total)
			readBefore := testutil.ToFloat64(metrics.RUV3BySQLType.WithLabelValues(metrics.LblSQLTypeRead))
			tikvBefore := testutil.ToFloat64(metrics.RUV3ByEngine.WithLabelValues(metrics.LblEngineTiKV))
			rs, err := tk.ExecWithContext(context.Background(), tc.query)
			require.NoError(t, err)
			require.NotNil(t, rs)
			rows, err := session.GetRows4Test(context.Background(), tk.Session(), rs)
			require.NoError(t, err)
			require.Len(t, rows, tc.rowCount)
			tc.validateRows(t, rows)

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
			t.Logf("statement RU observation: calibration=%d state=%s cpu=%v scan=%v net=%v frontend=%v hash_state=%v join_output=%v",
				observation.calibrationUnits, observation.calibrationState, observation.cpuWork, observation.scanBytes,
				observation.netBytes, observation.frontendBytes, observation.hashStateRows, observation.joinOutputRows)
			require.Equal(t, 1, observation.calibrationUnits)
			require.Equal(t, "incomplete", observation.calibrationState)
			if tc.wantCPUWork {
				require.Positive(t, observation.cpuWork)
			} else {
				require.Zero(t, observation.cpuWork)
			}
			if tc.wantHashState {
				require.Positive(t, observation.hashStateRows)
			} else {
				require.Zero(t, observation.hashStateRows)
			}
			if tc.wantJoinOutput {
				require.Positive(t, observation.joinOutputRows)
			} else {
				require.Zero(t, observation.joinOutputRows)
			}
			require.Positive(t, observation.scanBytes)
			require.Positive(t, observation.netBytes)
			require.Equal(t, float64(len(tc.query)), observation.frontendBytes)
			totalUnits := observation.cpuWork + observation.scanBytes + observation.netBytes + observation.frontendBytes +
				observation.hashStateRows + observation.joinOutputRows
			require.InDelta(t, totalUnits, testutil.ToFloat64(metrics.RUV3Total)-totalBefore, 1e-9)
			require.InDelta(t, totalUnits,
				testutil.ToFloat64(metrics.RUV3BySQLType.WithLabelValues(metrics.LblSQLTypeRead))-readBefore, 1e-9)
			require.InDelta(t, observation.scanBytes+observation.netBytes,
				testutil.ToFloat64(metrics.RUV3ByEngine.WithLabelValues(metrics.LblEngineTiKV))-tikvBefore, 1e-9)
			require.Equal(t, observation.netBytes, float64(tk.Session().GetSessionVars().RUV2Metrics.TiKVCoprocessorResponseBytes()))
		})
	}
}
