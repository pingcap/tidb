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

package core

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

// generateIndexOnlyJoinPath generates IndexOnlyJoin AccessPaths on this DataSource.
func generateIndexOnlyJoinPath(ds *logicalop.DataSource) error {
	if len(ds.IndexOnlyJoinHints) == 0 {
		return nil
	}
	stmtCtx := ds.SCtx().GetSessionVars().StmtCtx

	for _, hint := range ds.IndexOnlyJoinHints {
		indexNames := hint.IndexHint.IndexNames
		if len(indexNames) < 2 {
			stmtCtx.SetHintWarning(fmt.Sprintf("index_only_join requires exactly 2 index names, got %d", len(indexNames)))
			continue
		}

		driverIdxName := indexNames[0]
		probeIdxName := indexNames[1]

		// Find driver and probe paths among possible access paths.
		var driverPath, probePath *util.AccessPath
		for _, path := range ds.PossibleAccessPaths {
			if path.IsTablePath() || path.Index == nil {
				continue
			}
			if path.Index.Name.L == driverIdxName.L {
				driverPath = path
			}
			if path.Index.Name.L == probeIdxName.L {
				probePath = path
			}
		}

		if driverPath == nil {
			stmtCtx.SetHintWarning(fmt.Sprintf("index_only_join: driver index %s not found", driverIdxName.O))
			continue
		}
		if probePath == nil {
			stmtCtx.SetHintWarning(fmt.Sprintf("index_only_join: probe index %s not found", probeIdxName.O))
			continue
		}

		// Validate probe index is non-unique (handle is appended to IdxCols for non-unique indexes).
		if probePath.Index.Unique || probePath.Index.Primary {
			stmtCtx.SetHintWarning(fmt.Sprintf("index_only_join: probe index %s must be non-unique", probeIdxName.O))
			continue
		}

		// Validate ALL probe index columns (excluding the appended handle) have equality access conditions.
		numProbeIdxCols := len(probePath.Index.Columns)
		if probePath.EqOrInCondCount < numProbeIdxCols {
			stmtCtx.SetHintWarning(fmt.Sprintf(
				"index_only_join: probe index %s requires equality conditions on all %d columns, but only %d have equality conditions",
				probeIdxName.O, numProbeIdxCols, probePath.EqOrInCondCount))
			continue
		}

		// Build probe range template by extending existing probe ranges with handle placeholder.
		// For a non-unique index, IdxCols already has the handle appended.
		// The existing probe ranges cover the first numProbeIdxCols columns.
		// We add placeholder datum(s) for the handle position(s).
		probeRanges := make([]*ranger.Range, 0, len(probePath.Ranges))
		handleCols := ds.HandleCols
		numHandleCols := 1
		if handleCols != nil && !handleCols.IsInt() {
			numHandleCols = handleCols.NumCols()
		}

		for _, r := range probePath.Ranges {
			// Only use point ranges for the probe — all values equal.
			if r.LowExclude || r.HighExclude {
				continue
			}
			newRange := r.Clone()
			// Extend with placeholder datums for handle columns.
			for range numHandleCols {
				newRange.LowVal = append(newRange.LowVal, types.Datum{})
				newRange.HighVal = append(newRange.HighVal, types.Datum{})
				newRange.Collators = append(newRange.Collators, nil)
			}
			probeRanges = append(probeRanges, newRange)
		}

		if len(probeRanges) == 0 {
			stmtCtx.SetHintWarning(fmt.Sprintf("index_only_join: probe index %s has no usable ranges", probeIdxName.O))
			continue
		}

		// Build keyOff2IdxOff: maps handle key position to probe index column position.
		// For int handle: handle key at position 0 maps to IdxCols position numProbeIdxCols.
		// For common handle: handle key at position i maps to IdxCols position numProbeIdxCols + i.
		keyOff2IdxOff := make([]int, numHandleCols)
		for i := range numHandleCols {
			keyOff2IdxOff[i] = numProbeIdxCols + i
		}

		// Collect table filters (conditions not covered by either index).
		evalCtx := ds.SCtx().GetExprCtx().GetEvalCtx()
		var tableFilters []expression.Expression
		for _, cond := range ds.PushedDownConds {
			coveredByDriver := expression.Contains(evalCtx, driverPath.AccessConds, cond)
			coveredByProbe := expression.Contains(evalCtx, probePath.AccessConds, cond)
			if !coveredByDriver && !coveredByProbe {
				tableFilters = append(tableFilters, cond)
			}
		}

		// Create the index-only join AccessPath.
		iojPath := &util.AccessPath{
			IndexOnlyJoinInfo: &util.IndexOnlyJoinInfo{
				DriverPath:    driverPath,
				ProbePath:     probePath,
				ProbeIndex:    probePath.Index,
				ProbeRanges:   probeRanges,
				KeyOff2IdxOff: keyOff2IdxOff,
			},
			// Copy key fields from driver for property matching.
			Index:            driverPath.Index,
			Ranges:           driverPath.Ranges,
			AccessConds:      driverPath.AccessConds,
			IdxCols:          driverPath.IdxCols,
			IdxColLens:       driverPath.IdxColLens,
			CountAfterAccess: driverPath.CountAfterAccess,
			TableFilters:     tableFilters,
		}

		ds.PossibleAccessPaths = append(ds.PossibleAccessPaths, iojPath)
	}

	return nil
}
