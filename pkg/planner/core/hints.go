// Copyright 2019 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
)

// Hint flags listed here are used by PlanBuilder.subQueryHintFlags.
const (
	// TiDBMergeJoin is hint enforce merge join.
	TiDBMergeJoin = "tidb_smj"
	// HintSMJ is hint enforce merge join.
	HintSMJ = "merge_join"
	// HintNoMergeJoin is the hint to enforce the query not to use merge join.
	HintNoMergeJoin = "no_merge_join"

	// TiDBBroadCastJoin indicates applying broadcast join by force.
	TiDBBroadCastJoin = "tidb_bcj"
	// HintBCJ indicates applying broadcast join by force.
	HintBCJ = "broadcast_join"
	// HintShuffleJoin indicates applying shuffle join by force.
	HintShuffleJoin = "shuffle_join"

	// HintStraightJoin causes TiDB to join tables in the order in which they appear in the FROM clause.
	HintStraightJoin = "straight_join"
	// HintLeading specifies the set of tables to be used as the prefix in the execution plan.
	HintLeading = "leading"

	// TiDBIndexNestedLoopJoin is hint enforce index nested loop join.
	TiDBIndexNestedLoopJoin = "tidb_inlj"
	// HintINLJ is hint enforce index nested loop join.
	HintINLJ = "inl_join"
	// HintINLHJ is hint enforce index nested loop hash join.
	HintINLHJ = "inl_hash_join"
	// HintINLMJ is hint enforce index nested loop merge join.
	HintINLMJ = "inl_merge_join"
	// HintNoIndexJoin is the hint to enforce the query not to use index join.
	HintNoIndexJoin = "no_index_join"
	// HintNoIndexHashJoin is the hint to enforce the query not to use index hash join.
	HintNoIndexHashJoin = "no_index_hash_join"
	// HintNoIndexMergeJoin is the hint to enforce the query not to use index merge join.
	HintNoIndexMergeJoin = "no_index_merge_join"
	// TiDBHashJoin is hint enforce hash join.
	TiDBHashJoin = "tidb_hj"
	// HintNoHashJoin is the hint to enforce the query not to use hash join.
	HintNoHashJoin = "no_hash_join"
	// HintHJ is hint enforce hash join.
	HintHJ = "hash_join"
	// HintHashJoinBuild is hint enforce hash join's build side
	HintHashJoinBuild = "hash_join_build"
	// HintHashJoinProbe is hint enforce hash join's probe side
	HintHashJoinProbe = "hash_join_probe"
	// HintHashAgg is hint enforce hash aggregation.
	HintHashAgg = "hash_agg"
	// HintStreamAgg is hint enforce stream aggregation.
	HintStreamAgg = "stream_agg"
	// HintMPP1PhaseAgg enforces the optimizer to use the mpp-1phase aggregation.
	HintMPP1PhaseAgg = "mpp_1phase_agg"
	// HintMPP2PhaseAgg enforces the optimizer to use the mpp-2phase aggregation.
	HintMPP2PhaseAgg = "mpp_2phase_agg"
	// HintUseIndex is hint enforce using some indexes.
	HintUseIndex = "use_index"
	// HintIgnoreIndex is hint enforce ignoring some indexes.
	HintIgnoreIndex = "ignore_index"
	// HintForceIndex make optimizer to use this index even if it thinks a table scan is more efficient.
	HintForceIndex = "force_index"
	// HintOrderIndex is hint enforce using some indexes and keep the index's order.
	HintOrderIndex = "order_index"
	// HintNoOrderIndex is hint enforce using some indexes and not keep the index's order.
	HintNoOrderIndex = "no_order_index"
	// HintAggToCop is hint enforce pushing aggregation to coprocessor.
	HintAggToCop = "agg_to_cop"
	// HintReadFromStorage is hint enforce some tables read from specific type of storage.
	HintReadFromStorage = "read_from_storage"
	// HintTiFlash is a label represents the tiflash storage type.
	HintTiFlash = "tiflash"
	// HintTiKV is a label represents the tikv storage type.
	HintTiKV = "tikv"
	// HintIndexMerge is a hint to enforce using some indexes at the same time.
	HintIndexMerge = "use_index_merge"
	// HintTimeRange is a hint to specify the time range for metrics summary tables
	HintTimeRange = "time_range"
	// HintIgnorePlanCache is a hint to enforce ignoring plan cache
	HintIgnorePlanCache = "ignore_plan_cache"
	// HintLimitToCop is a hint enforce pushing limit or topn to coprocessor.
	HintLimitToCop = "limit_to_cop"
	// HintMerge is a hint which can switch turning inline for the CTE.
	HintMerge = "merge"
	// HintSemiJoinRewrite is a hint to force we rewrite the semi join operator as much as possible.
	HintSemiJoinRewrite = "semi_join_rewrite"
	// HintNoDecorrelate indicates a LogicalApply not to be decorrelated.
	HintNoDecorrelate = "no_decorrelate"

	// HintMemoryQuota sets the memory limit for a query
	HintMemoryQuota = "memory_quota"
	// HintUseToja is a hint to optimize `in (select ...)` subquery into `join`
	HintUseToja = "use_toja"
	// HintNoIndexMerge is a hint to disable index merge
	HintNoIndexMerge = "no_index_merge"
	// HintMaxExecutionTime specifies the max allowed execution time in milliseconds
	HintMaxExecutionTime = "max_execution_time"

	// HintFlagSemiJoinRewrite corresponds to HintSemiJoinRewrite.
	HintFlagSemiJoinRewrite uint64 = 1 << iota
	// HintFlagNoDecorrelate corresponds to HintNoDecorrelate.
	HintFlagNoDecorrelate
)

type indexNestedLoopJoinTables struct {
	inljTables  []hintTableInfo
	inlhjTables []hintTableInfo
	inlmjTables []hintTableInfo
}

type tableHintInfo struct {
	indexNestedLoopJoinTables
	noIndexJoinTables   indexNestedLoopJoinTables
	sortMergeJoinTables []hintTableInfo
	broadcastJoinTables []hintTableInfo
	shuffleJoinTables   []hintTableInfo
	hashJoinTables      []hintTableInfo
	noHashJoinTables    []hintTableInfo
	noMergeJoinTables   []hintTableInfo
	indexHintList       []indexHintInfo
	tiflashTables       []hintTableInfo
	tikvTables          []hintTableInfo
	aggHints            aggHintInfo
	indexMergeHintList  []indexHintInfo
	timeRangeHint       ast.HintTimeRange
	limitHints          limitHintInfo
	MergeHints          MergeHintInfo
	leadingJoinOrder    []hintTableInfo
	hjBuildTables       []hintTableInfo
	hjProbeTables       []hintTableInfo
}

type limitHintInfo struct {
	preferLimitToCop bool
}

// MergeHintInfo ...one bool flag for cte
type MergeHintInfo struct {
	preferMerge bool
}

type hintTableInfo struct {
	dbName       model.CIStr
	tblName      model.CIStr
	partitions   []model.CIStr
	selectOffset int
	matched      bool
}

type indexHintInfo struct {
	dbName     model.CIStr
	tblName    model.CIStr
	partitions []model.CIStr
	indexHint  *ast.IndexHint
	// Matched indicates whether this index hint
	// has been successfully applied to a DataSource.
	// If an indexHintInfo is not matched after building
	// a Select statement, we will generate a warning for it.
	matched bool
}

func (hint *indexHintInfo) match(dbName, tblName model.CIStr) bool {
	return hint.tblName.L == tblName.L &&
		(hint.dbName.L == dbName.L ||
			hint.dbName.L == "*") // for universal bindings, e.g. *.t
}

func (hint *indexHintInfo) hintTypeString() string {
	switch hint.indexHint.HintType {
	case ast.HintUse:
		return "use_index"
	case ast.HintIgnore:
		return "ignore_index"
	case ast.HintForce:
		return "force_index"
	}
	return ""
}

// indexString formats the indexHint as dbName.tableName[, indexNames].
func (hint *indexHintInfo) indexString() string {
	var indexListString string
	indexList := make([]string, len(hint.indexHint.IndexNames))
	for i := range hint.indexHint.IndexNames {
		indexList[i] = hint.indexHint.IndexNames[i].L
	}
	if len(indexList) > 0 {
		indexListString = fmt.Sprintf(", %s", strings.Join(indexList, ", "))
	}
	return fmt.Sprintf("%s.%s%s", hint.dbName, hint.tblName, indexListString)
}

type aggHintInfo struct {
	preferAggType  uint
	preferAggToCop bool
}

func (info *tableHintInfo) ifPreferMergeJoin(tableNames ...*hintTableInfo) bool {
	return info.matchTableName(tableNames, info.sortMergeJoinTables)
}

func (info *tableHintInfo) ifPreferBroadcastJoin(tableNames ...*hintTableInfo) bool {
	return info.matchTableName(tableNames, info.broadcastJoinTables)
}

func (info *tableHintInfo) ifPreferShuffleJoin(tableNames ...*hintTableInfo) bool {
	return info.matchTableName(tableNames, info.shuffleJoinTables)
}

func (info *tableHintInfo) ifPreferHashJoin(tableNames ...*hintTableInfo) bool {
	return info.matchTableName(tableNames, info.hashJoinTables)
}

func (info *tableHintInfo) ifPreferNoHashJoin(tableNames ...*hintTableInfo) bool {
	return info.matchTableName(tableNames, info.noHashJoinTables)
}

func (info *tableHintInfo) ifPreferNoMergeJoin(tableNames ...*hintTableInfo) bool {
	return info.matchTableName(tableNames, info.noMergeJoinTables)
}

func (info *tableHintInfo) ifPreferHJBuild(tableNames ...*hintTableInfo) bool {
	return info.matchTableName(tableNames, info.hjBuildTables)
}

func (info *tableHintInfo) ifPreferHJProbe(tableNames ...*hintTableInfo) bool {
	return info.matchTableName(tableNames, info.hjProbeTables)
}

func (info *tableHintInfo) ifPreferINLJ(tableNames ...*hintTableInfo) bool {
	return info.matchTableName(tableNames, info.indexNestedLoopJoinTables.inljTables)
}

func (info *tableHintInfo) ifPreferINLHJ(tableNames ...*hintTableInfo) bool {
	return info.matchTableName(tableNames, info.indexNestedLoopJoinTables.inlhjTables)
}

func (info *tableHintInfo) ifPreferINLMJ(tableNames ...*hintTableInfo) bool {
	return info.matchTableName(tableNames, info.indexNestedLoopJoinTables.inlmjTables)
}

func (info *tableHintInfo) ifPreferNoIndexJoin(tableNames ...*hintTableInfo) bool {
	return info.matchTableName(tableNames, info.noIndexJoinTables.inljTables)
}

func (info *tableHintInfo) ifPreferNoIndexHashJoin(tableNames ...*hintTableInfo) bool {
	return info.matchTableName(tableNames, info.noIndexJoinTables.inlhjTables)
}

func (info *tableHintInfo) ifPreferNoIndexMergeJoin(tableNames ...*hintTableInfo) bool {
	return info.matchTableName(tableNames, info.noIndexJoinTables.inlmjTables)
}

func (info *tableHintInfo) ifPreferTiFlash(tableName *hintTableInfo) *hintTableInfo {
	if tableName == nil {
		return nil
	}
	for i, tbl := range info.tiflashTables {
		if tableName.dbName.L == tbl.dbName.L && tableName.tblName.L == tbl.tblName.L && tbl.selectOffset == tableName.selectOffset {
			info.tiflashTables[i].matched = true
			return &tbl
		}
	}
	return nil
}

func (info *tableHintInfo) ifPreferTiKV(tableName *hintTableInfo) *hintTableInfo {
	if tableName == nil {
		return nil
	}
	for i, tbl := range info.tikvTables {
		if tableName.dbName.L == tbl.dbName.L && tableName.tblName.L == tbl.tblName.L && tbl.selectOffset == tableName.selectOffset {
			info.tikvTables[i].matched = true
			return &tbl
		}
	}
	return nil
}

// matchTableName checks whether the hint hit the need.
// Only need either side matches one on the list.
// Even though you can put 2 tables on the list,
// it doesn't mean optimizer will reorder to make them
// join directly.
// Which it joins on with depend on sequence of traverse
// and without reorder, user might adjust themselves.
// This is similar to MySQL hints.
func (*tableHintInfo) matchTableName(tables []*hintTableInfo, hintTables []hintTableInfo) bool {
	hintMatched := false
	for _, table := range tables {
		for i, curEntry := range hintTables {
			if table == nil {
				continue
			}
			if (curEntry.dbName.L == table.dbName.L || curEntry.dbName.L == "*") &&
				curEntry.tblName.L == table.tblName.L &&
				table.selectOffset == curEntry.selectOffset {
				hintTables[i].matched = true
				hintMatched = true
				break
			}
		}
	}
	return hintMatched
}
