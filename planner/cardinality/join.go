package cardinality

import (
	"math"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx"
)

// EstimateFullJoinRowCount estimates the row count of a full join.
func EstimateFullJoinRowCount(sctx sessionctx.Context,
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
