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
	"github.com/pingcap/tidb/pkg/planner/context"
	"github.com/pingcap/tidb/pkg/planner/property"
)

// EstimateFullJoinRowCount estimates the row count of a full join.
func EstimateFullJoinRowCount(sctx context.PlanContext,
	isCartesian bool,
	leftProfile, rightProfile *property.StatsInfo,
	leftJoinKeys, rightJoinKeys []*expression.Column,
	leftSchema, rightSchema *expression.Schema,
	leftNAJoinKeys, rightNAJoinKeys []*expression.Column) float64 {
	if isCartesian {
		return leftProfile.RowCount * rightProfile.RowCount
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
	count := leftProfile.RowCount * rightProfile.RowCount / max(leftKeyNDV, rightKeyNDV)
	if sctx.GetSessionVars().TiDBOptJoinReorderThreshold <= 0 {
		return count
	}
	// If we enable the DP choice, we multiple the 0.9 for each remained join key supposing that 0.9 is the correlation factor between them.
	// This estimation logic is referred to Presto.
	return count * math.Pow(0.9, float64(len(leftJoinKeys)-max(leftColCnt, rightColCnt)))
}
