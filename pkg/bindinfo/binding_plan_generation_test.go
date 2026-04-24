// Copyright 2025 PingCAP, Inc.
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

package bindinfo

import (
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/stretchr/testify/require"
)

func roundTo4Decimal(num float64) float64 {
	return math.Round(num*1e4) / 1e4
}

func TestAdjustFixes(t *testing.T) {
	v, err := adjustFix(fixcontrol.Fix44855, "on")
	require.NoError(t, err)
	require.Equal(t, v, vardef.Off)
	v, err = adjustFix(fixcontrol.Fix44855, "off      ")
	require.NoError(t, err)
	require.Equal(t, v, vardef.On)

	v, err = adjustFix(fixcontrol.Fix45132, "1000")
	require.NoError(t, err)
	require.Equal(t, v, "500")
	v, err = adjustFix(fixcontrol.Fix45132, "30")
	require.NoError(t, err)
	require.Equal(t, v, "15")
	v, err = adjustFix(fixcontrol.Fix45132, "8")
	require.NoError(t, err)
	require.Equal(t, v, "8")
}

func TestAdjustVars(t *testing.T) {
	// adjust cost factor
	v, err := adjustVar(vardef.TiDBOptIndexScanCostFactor, 1.0)
	require.NoError(t, err)
	require.Equal(t, v, 5.0)
	v, err = adjustVar(vardef.TiDBOptIndexScanCostFactor, 5.0)
	require.NoError(t, err)
	require.Equal(t, v, 25.0)
	v, err = adjustVar(vardef.TiDBOptIndexScanCostFactor, 1e5)
	require.NoError(t, err)
	require.Equal(t, v, 5e5)
	v, err = adjustVar(vardef.TiDBOptIndexScanCostFactor, 2e6)
	require.NoError(t, err)
	require.Equal(t, v, 2e6)

	v, err = adjustVar(vardef.TiDBOptOrderingIdxSelRatio, -1.0)
	require.NoError(t, err)
	require.Equal(t, roundTo4Decimal(v.(float64)), 0.1)
	v, err = adjustVar(vardef.TiDBOptOrderingIdxSelRatio, 0.2)
	require.NoError(t, err)
	require.Equal(t, roundTo4Decimal(v.(float64)), 0.3)
	v, err = adjustVar(vardef.TiDBOptOrderingIdxSelRatio, 0.55)
	require.NoError(t, err)
	require.Equal(t, roundTo4Decimal(v.(float64)), 0.65)
	v, err = adjustVar(vardef.TiDBOptOrderingIdxSelRatio, 0.95)
	require.NoError(t, err)
	require.Equal(t, roundTo4Decimal(v.(float64)), 0.95)

	_, err = adjustVar(vardef.TiFlashReplicaRead, -1.0)
	require.Error(t, err) // unsupported
}

func TestStartState(t *testing.T) {
	vars := []string{
		vardef.TiDBOptIndexScanCostFactor,
		vardef.TiDBOptIndexReaderCostFactor,
		vardef.TiDBOptTableReaderCostFactor,
		vardef.TiDBOptTableFullScanCostFactor,
		vardef.TiDBOptTableRangeScanCostFactor,
		vardef.TiDBOptTableRowIDScanCostFactor,
		vardef.TiDBOptTableTiFlashScanCostFactor,
		vardef.TiDBOptIndexLookupCostFactor,
		vardef.TiDBOptIndexMergeCostFactor,
		vardef.TiDBOptSortCostFactor,
		vardef.TiDBOptTopNCostFactor,
		vardef.TiDBOptLimitCostFactor,
		vardef.TiDBOptStreamAggCostFactor,
		vardef.TiDBOptHashAggCostFactor,
		vardef.TiDBOptMergeJoinCostFactor,
		vardef.TiDBOptHashJoinCostFactor,
		vardef.TiDBOptIndexJoinCostFactor,
		vardef.TiDBOptOrderingIdxSelRatio,
		vardef.TiDBOptRiskEqSkewRatio,
		vardef.TiDBOptRiskRangeSkewRatio,
		vardef.TiDBOptRiskGroupNDVSkewRatio,
		vardef.TiDBOptSelectivityFactor,
		vardef.TiDBOptPreferRangeScan,
		vardef.TiDBOptEnableNoDecorrelateInSelect,
		vardef.TiDBOptEnableSemiJoinRewrite,
		vardef.TiDBOptCartesianJoinOrderThreshold,
	}
	fixes := []uint64{fixcontrol.Fix44855, fixcontrol.Fix45132, fixcontrol.Fix52869}

	state, err := getStartState(vars, fixes, 0)
	require.NoError(t, err)
	require.Equal(t, state.Encode(), "1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,1.0000,0.0100,0.0000,0.0000,0.0000,0.8000,true,false,false,0.0000,OFF,1000,OFF")
}
