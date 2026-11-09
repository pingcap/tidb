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

package core

import (
	"math"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

func deriveTiDBRowIDTablePathStats(
	ds *logicalop.DataSource,
	path *util.AccessPath,
	conds []expression.Expression,
) (bool, error) {
	var selectedCandidates []expression.Expression
	var rangeOps []expression.TiDBRowIDRangeOp
	var rangeBounds []expression.Expression
	var rangeConditions []expression.Expression
	allowedPartitionNames := tiDBRowIDAllowedPartitionNames(ds)
	evalCtx := ds.SCtx().GetExprCtx().GetEvalCtx()
	for _, cond := range ds.AllConds {
		rowID, candidates, not, ok := expression.GetTiDBRowIDPredicate(cond)
		if ok && tiDBRowIDPredicateBelongsToDataSource(rowID, ds) {
			constants := make([]expression.Expression, 0, len(candidates))
			allConstants := true
			for _, candidate := range candidates {
				if _, ok := candidate.(*expression.Constant); !ok {
					allConstants = false
					continue
				}
				constants = append(constants, candidate)
			}
			if _, _, err := resolveTiDBRowIDCandidates(evalCtx, ds.TableInfo, allowedPartitionNames, constants); err != nil {
				return false, err
			}
			if selectedCandidates == nil && !not && allConstants {
				selectedCandidates = candidates
			}
			continue
		}

		rowID, bounds, op, not, ok := expression.GetTiDBRowIDRangePredicate(cond)
		if !ok || !tiDBRowIDPredicateBelongsToDataSource(rowID, ds) {
			continue
		}
		constants := make([]expression.Expression, 0, len(bounds))
		allConstants := true
		for _, bound := range bounds {
			if _, ok := bound.(*expression.Constant); !ok {
				allConstants = false
				continue
			}
			constants = append(constants, bound)
		}
		for _, constant := range constants {
			token, isNull, err := constant.EvalString(evalCtx, chunk.Row{})
			if err != nil {
				return false, err
			}
			if !isNull {
				if _, err := validateTiDBRowIDRangeBound(ds.TableInfo, token); err != nil {
					return false, err
				}
			}
		}
		if !not && allConstants {
			rangeOps = append(rangeOps, op)
			rangeBounds = append(rangeBounds, bounds...)
			rangeConditions = append(rangeConditions, cond)
		}
	}
	if len(selectedCandidates) > 0 && path.StoreType == kv.TiKV {
		handles, partitionIdxs, err := resolveTiDBRowIDCandidates(
			evalCtx,
			ds.TableInfo,
			allowedPartitionNames,
			selectedCandidates,
		)
		if err != nil {
			return false, err
		}
		path.TiDBRowIDCandidates = selectedCandidates
		path.TiDBRowIDHandles = handles
		path.TiDBRowIDPartitionIdxs = partitionIdxs
		path.TiDBRowIDAllowedPartitionNames = allowedPartitionNames
		path.AccessConds = nil
		path.TableFilters = conds
		path.Ranges = make([]*ranger.Range, 0, len(selectedCandidates))
		for i := range selectedCandidates {
			value := types.NewIntDatum(int64(i))
			path.Ranges = append(path.Ranges, &ranger.Range{
				LowVal:    []types.Datum{value},
				HighVal:   []types.Datum{value},
				Collators: collate.GetBinaryCollatorSlice(1),
			})
		}
		path.CountAfterAccess = float64(len(handles))
		path.CountAfterIndex = path.CountAfterAccess
		return true, nil
	}
	if len(rangeOps) == 0 {
		return false, nil
	}

	physicalIDs, keyRanges, err := resolveTiDBRowIDRangeBounds(
		evalCtx, ds.TableInfo, allowedPartitionNames, rangeOps, rangeBounds,
	)
	if err != nil {
		return false, err
	}
	path.TiDBRowIDRangeOps = rangeOps
	path.TiDBRowIDRangeBounds = rangeBounds
	path.TiDBRowIDRangePhysicalIDs = physicalIDs
	path.TiDBRowIDKeyRanges = keyRanges
	path.TiDBRowIDAllowedPartitionNames = allowedPartitionNames
	path.AccessConds = nil
	path.TableFilters = removeTiDBRowIDRangeConditions(conds, rangeConditions)
	if path.IsCommonHandlePath {
		path.Ranges = ranger.FullRange()
	} else {
		path.Ranges = ranger.FullIntRange(false)
	}
	path.CountAfterAccess = math.Max(1, float64(ds.StatisticTable.RealtimeCount)*cost.SelectionFactor)
	path.CountAfterIndex = path.CountAfterAccess
	return true, nil
}

func removeTiDBRowIDRangeConditions(
	conditions []expression.Expression,
	removed []expression.Expression,
) []expression.Expression {
	removedSet := make(map[expression.Expression]struct{}, len(removed))
	for _, condition := range removed {
		removedSet[condition] = struct{}{}
	}
	remaining := make([]expression.Expression, 0, len(conditions))
	for _, condition := range conditions {
		if _, ok := removedSet[condition]; !ok {
			remaining = append(remaining, condition)
		}
	}
	return remaining
}
