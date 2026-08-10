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

	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

const (
	statementRUEnableCalibrationFailpoint = "github.com/pingcap/tidb/pkg/executor/enableStatementRUCalibrationPublisherForTest"
	statementRUCalibrationUnitsFailpoint  = "github.com/pingcap/tidb/pkg/executor/observeStatementRUCalibrationUnitsForTest"
	statementRUFreezeFailpoint            = "github.com/pingcap/tidb/pkg/executor/observeStatementRUFreezeForTest"
	statementRUResultFailpoint            = "github.com/pingcap/tidb/pkg/executor/observeStatementRUResultForTest"
)

type statementRURealTiKVObservation struct {
	sync.Mutex
	ownerInstallations int
	calibrationAttach  bool
	freezeAttempts     int
	resultPublications int
	calibrationUnits   int
	totalRU            float64
	state              uint8
	scanBytes          float64
	netBytes           float64
	frontendBytes      float64
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
	testfailpoint.EnableCall(t, statementRUEnableCalibrationFailpoint, func(
		observedConnectionID uint64,
		enable func() bool,
	) {
		if observedConnectionID != connectionID {
			return
		}
		observation.Lock()
		observation.ownerInstallations++
		observation.Unlock()
		attached := enable()
		observation.Lock()
		observation.calibrationAttach = attached
		observation.Unlock()
	})
	testfailpoint.EnableCall(t, statementRUCalibrationUnitsFailpoint, func(
		observedConnectionID uint64,
		state uint8,
		scanBytes, netBytes, frontendCompileBytes float64,
	) {
		if observedConnectionID != connectionID {
			return
		}
		observation.Lock()
		defer observation.Unlock()
		observation.calibrationUnits++
		observation.state = state
		observation.scanBytes = scanBytes
		observation.netBytes = netBytes
		observation.frontendBytes = frontendCompileBytes
	})
	testfailpoint.EnableCall(t, statementRUResultFailpoint, func(observedConnectionID uint64, totalRU float64) {
		if observedConnectionID != connectionID {
			return
		}
		observation.Lock()
		defer observation.Unlock()
		observation.resultPublications++
		observation.totalRU = totalRU
	})
	testfailpoint.EnableCall(t, statementRUFreezeFailpoint, func(observedConnectionID uint64) {
		if observedConnectionID != connectionID {
			return
		}
		observation.Lock()
		defer observation.Unlock()
		observation.freezeAttempts++
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
	require.Equal(t, 1, observation.ownerInstallations)
	require.True(t, observation.calibrationAttach)
	require.Zero(t, observation.freezeAttempts)
	require.Zero(t, observation.resultPublications)
	require.Zero(t, observation.calibrationUnits)
	observation.Unlock()

	require.NoError(t, rs.Close())
	require.NoError(t, rs.Close())
	observation.Lock()
	defer observation.Unlock()
	t.Logf("statement RU observation: freezes=%d results=%d calibration=%d state=%d scan=%v net=%v frontend=%v",
		observation.freezeAttempts, observation.resultPublications, observation.calibrationUnits,
		observation.state, observation.scanBytes, observation.netBytes, observation.frontendBytes)
	require.Equal(t, 1, observation.freezeAttempts)
	require.Equal(t, 1, observation.calibrationUnits)
	require.Equal(t, uint8(1), observation.state)
	require.Positive(t, observation.scanBytes)
	require.Positive(t, observation.netBytes)
	require.Equal(t, float64(len(query)), observation.frontendBytes)
	require.Equal(t, 1, observation.resultPublications)
	require.InDelta(t, observation.scanBytes+observation.netBytes+observation.frontendBytes, observation.totalRU, 1e-9)
	require.Equal(t, observation.netBytes, float64(tk.Session().GetSessionVars().RUV2Metrics.TiKVCoprocessorResponseBytes()))
}
