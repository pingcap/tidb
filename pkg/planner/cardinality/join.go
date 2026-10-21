// Copyright 2023 PingCAP, Inc.
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

package cardinality

import (
	"math"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/planner/property"
)

// EstimateFullJoinRowCount estimates the row count of a full join.
func EstimateFullJoinRowCount(sctx planctx.PlanContext,
	isCartesian bool,
	leftProfile, rightProfile *property.StatsInfo,
	leftJoinKeys, rightJoinKeys []*expression.Column,
	leftSchema, rightSchema *expression.Schema,
	leftNAJoinKeys, rightNAJoinKeys []*expression.Column) float64 {
	leftCnt := finiteNonNegCount(leftProfile.RowCount)
	rightCnt := finiteNonNegCount(rightProfile.RowCount)
	if isCartesian {
		return finiteNonNegCount(leftCnt * rightCnt)
	}
	var leftKeyNDV, rightKeyNDV float64
	var leftColCnt, rightColCnt int
	if len(leftJoinKeys) > 0 || len(rightJoinKeys) > 0 {
		leftKeyNDV, leftColCnt = EstimateColsNDVWithMatchedLen(leftJoinKeys, leftSchema, leftProfile)
		rightKeyNDV, rightColCnt = EstimateColsNDVWithMatchedLen(rightJoinKeys, rightSchema, rightProfile)
	} else {
		leftKeyNDV, leftColCnt = EstimateColsNDVWithMatchedLen(leftNAJoinKeys, leftSchema, leftProfile)
		rightKeyNDV, rightColCnt = EstimateColsNDVWithMatchedLen(rightNAJoinKeys, rightSchema, rightProfile)
	}
	denom := math.Max(safeJoinKeyNDV(leftKeyNDV), safeJoinKeyNDV(rightKeyNDV))
	count := leftCnt * rightCnt / denom
	if sctx.GetSessionVars().TiDBOptJoinReorderThreshold <= 0 {
		return finiteNonNegCount(count)
	}
	// If we enable the DP choice, we multiple the 0.9 for each remained join key supposing that 0.9 is the correlation factor between them.
	// This estimation logic is referred to Presto.
	return finiteNonNegCount(count * math.Pow(0.9, float64(len(leftJoinKeys)-max(leftColCnt, rightColCnt))))
}

// finiteNonNegCount clamps an estimated count into a value that is safe for
// downstream cost arithmetic. NaN, -Inf and negative values are treated as 0
// (empty or degenerate statistics), while +Inf is capped at math.MaxFloat64 so
// it stays finite. Mapping invalid estimates to 0 instead of letting them
// dominate the row-count/cost sum keeps the plan choice meaningful.
func finiteNonNegCount(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, -1) || v < 0 {
		return 0
	}
	if math.IsInf(v, 1) {
		return math.MaxFloat64
	}
	return v
}

func safeJoinKeyNDV(ndv float64) float64 {
	ndv = finiteNonNegCount(ndv)
	if ndv == 0 {
		return 1
	}
	return ndv
}
