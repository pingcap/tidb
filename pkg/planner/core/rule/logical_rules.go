// Copyright 2024 PingCAP, Inc.
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

package rule

// Note: Most flags follow the same order as optRuleList in optimizer.go.
// FlagJoinReOrder is explored separately in memo-lite after logical optimization.
const (
	FlagGcSubstitute uint64 = 1 << iota
	FlagPruneColumns
	FlagStabilizeResults
	FlagBuildKeyInfo
	FlagDecorrelate
	FlagSemiJoinRewrite
	FlagEliminateAgg
	FlagSkewDistinctAgg
	FlagEliminateProjection
	FlagMaxMinEliminate
	FlagConstantPropagation
	FlagConvertOuterToInnerJoin
	FlagPredicatePushDown
	FlagEliminateOuterJoin
	FlagPartitionProcessor
	FlagCollectPredicateColumnsPoint
	FlagPushDownAgg
	FlagDeriveTopNFromWindow
	FlagPredicateSimplification
	FlagPushDownTopN
	FlagSyncWaitStatsLoadPoint
	FlagOuterJoinToSemiJoin
	FlagPruneColumnsAgain
	FlagPushDownSequence
	FlagEliminateUnionAllDualItem
	FlagEmptySelectionEliminator
	FlagResolveExpand
	FlagJoinReOrder
)

func setPredicatePushDownFlag(u uint64) uint64 {
	u |= FlagPredicatePushDown
	return u
}
