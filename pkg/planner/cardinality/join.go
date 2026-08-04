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

// EstimateJoinMatchedRowCount estimates row pairs matched by join keys,
// or all row pairs for a Cartesian join.
// It does not apply join-type-specific row preservation rules for outer/semi joins.
func EstimateJoinMatchedRowCount(
	sctx planctx.PlanContext,
	isCartesian bool,
	leftProfile, rightProfile *property.StatsInfo,
	leftJoinKeys, rightJoinKeys []*expression.Column,
	leftSchema, rightSchema *expression.Schema,
	leftNAJoinKeys, rightNAJoinKeys []*expression.Column,
) float64 {
	if isCartesian {
		return leftProfile.RowCount * rightProfile.RowCount
	}
	var leftKeyNDV, rightKeyNDV float64
	var leftColCnt, rightColCnt int
	joinKeyLen := len(leftJoinKeys)
	if len(leftJoinKeys) > 0 || len(rightJoinKeys) > 0 {
		leftKeyNDV, leftColCnt = EstimateColsNDVWithMatchedLen(sctx, leftJoinKeys, leftSchema, leftProfile)
		rightKeyNDV, rightColCnt = EstimateColsNDVWithMatchedLen(sctx, rightJoinKeys, rightSchema, rightProfile)
	} else {
		joinKeyLen = len(leftNAJoinKeys)
		leftKeyNDV, leftColCnt = EstimateColsNDVWithMatchedLen(sctx, leftNAJoinKeys, leftSchema, leftProfile)
		rightKeyNDV, rightColCnt = EstimateColsNDVWithMatchedLen(sctx, rightNAJoinKeys, rightSchema, rightProfile)
	}
	count := leftProfile.RowCount * rightProfile.RowCount / max(leftKeyNDV, rightKeyNDV)
	if sctx.GetSessionVars().TiDBOptJoinReorderThreshold <= 0 {
		return count
	}
	// If we enable the DP choice, we multiple the 0.9 for each remained join key supposing that 0.9 is the correlation factor between them.
	// This estimation logic is referred to Presto.
	return count * math.Pow(0.9, float64(joinKeyLen-max(leftColCnt, rightColCnt)))
}

// EstimateSemiJoinRowCount converts a matched pair estimate into the number
// of left rows expected to find at least one matching right row.
func EstimateSemiJoinRowCount(
	sctx planctx.PlanContext,
	matchedPairCount float64,
	leftProfile, rightProfile *property.StatsInfo,
	rightJoinKeys []*expression.Column,
	rightSchema *expression.Schema,
) float64 {
	if leftProfile.RowCount <= 0 || rightProfile.RowCount <= 0 || matchedPairCount <= 0 {
		return 0
	}

	rightKeyNDV, _ := EstimateColsNDVWithMatchedLen(sctx, rightJoinKeys, rightSchema, rightProfile)
	rightKeyNDV = min(max(rightKeyNDV, 1), rightProfile.RowCount)
	rightRowsPerKey := max(rightProfile.RowCount/rightKeyNDV, 1)

	rowCount := matchedPairCount / rightRowsPerKey
	return min(max(rowCount, 0), leftProfile.RowCount)
}
