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

package hint

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/pingcap/errors"
	mysql "github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/util/dbterror"
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

const (
	// PreferINLJ indicates that the optimizer prefers to use index nested loop join.
	PreferINLJ uint = 1 << iota
	// PreferINLHJ indicates that the optimizer prefers to use index nested loop hash join.
	PreferINLHJ
	// PreferINLMJ indicates that the optimizer prefers to use index nested loop merge join.
	PreferINLMJ
	// PreferHJBuild indicates that the optimizer prefers to use hash join.
	PreferHJBuild
	// PreferHJProbe indicates that the optimizer prefers to use hash join.
	PreferHJProbe
	// PreferHashJoin indicates that the optimizer prefers to use hash join.
	PreferHashJoin
	// PreferNoHashJoin indicates that the optimizer prefers not to use hash join.
	PreferNoHashJoin
	// PreferMergeJoin indicates that the optimizer prefers to use merge join.
	PreferMergeJoin
	// PreferNoMergeJoin indicates that the optimizer prefers not to use merge join.
	PreferNoMergeJoin
	// PreferNoIndexJoin indicates that the optimizer prefers not to use index join.
	PreferNoIndexJoin
	// PreferNoIndexHashJoin indicates that the optimizer prefers not to use index hash join.
	PreferNoIndexHashJoin
	// PreferNoIndexMergeJoin indicates that the optimizer prefers not to use index merge join.
	PreferNoIndexMergeJoin
	// PreferBCJoin indicates that the optimizer prefers to use broadcast join.
	PreferBCJoin
	// PreferShuffleJoin indicates that the optimizer prefers to use shuffle join.
	PreferShuffleJoin
	// PreferRewriteSemiJoin indicates that the optimizer prefers to rewrite semi join.
	PreferRewriteSemiJoin

	// PreferLeftAsINLJInner indicates that the optimizer prefers to use left child as inner child of index nested loop join.
	PreferLeftAsINLJInner
	// PreferRightAsINLJInner indicates that the optimizer prefers to use right child as inner child of index nested loop join.
	PreferRightAsINLJInner
	// PreferLeftAsINLHJInner indicates that the optimizer prefers to use left child as inner child of index nested loop hash join.
	PreferLeftAsINLHJInner
	// PreferRightAsINLHJInner indicates that the optimizer prefers to use right child as inner child of index nested loop hash join.
	PreferRightAsINLHJInner
	// PreferLeftAsINLMJInner indicates that the optimizer prefers to use left child as inner child of index nested loop merge join.
	PreferLeftAsINLMJInner
	// PreferRightAsINLMJInner indicates that the optimizer prefers to use right child as inner child of index nested loop merge join.
	PreferRightAsINLMJInner
	// PreferLeftAsHJBuild indicates that the optimizer prefers to use left child as build child of hash join.
	PreferLeftAsHJBuild
	// PreferRightAsHJBuild indicates that the optimizer prefers to use right child as build child of hash join.
	PreferRightAsHJBuild
	// PreferLeftAsHJProbe indicates that the optimizer prefers to use left child as probe child of hash join.
	PreferLeftAsHJProbe
	// PreferRightAsHJProbe indicates that the optimizer prefers to use right child as probe child of hash join.
	PreferRightAsHJProbe

	// PreferHashAgg indicates that the optimizer prefers to use hash aggregation.
	PreferHashAgg
	// PreferStreamAgg indicates that the optimizer prefers to use stream aggregation.
	PreferStreamAgg
	// PreferMPP1PhaseAgg indicates that the optimizer prefers to use 1-phase aggregation.
	PreferMPP1PhaseAgg
	// PreferMPP2PhaseAgg indicates that the optimizer prefers to use 2-phase aggregation.
	PreferMPP2PhaseAgg
)

const (
	// PreferTiKV indicates that the optimizer prefers to use TiKV layer.
	PreferTiKV = 1 << iota
	// PreferTiFlash indicates that the optimizer prefers to use TiFlash layer.
	PreferTiFlash
)

// StmtHints are hints that apply to the entire statement, like 'max_exec_time', 'memory_quota'.
type StmtHints struct {
	// Hint Information
	MemQuotaQuery           int64
	MaxExecutionTime        uint64
	ReplicaRead             byte
	AllowInSubqToJoinAndAgg bool
	NoIndexMergeHint        bool
	StraightJoinOrder       bool
	// EnableCascadesPlanner is use cascades planner for a single query only.
	EnableCascadesPlanner bool
	// ForceNthPlan indicates the PlanCounterTp number for finding physical plan.
	// -1 for disable.
	ForceNthPlan  int64
	ResourceGroup string

	// Hint flags
	HasAllowInSubqToJoinAndAggHint bool
	HasMemQuotaHint                bool
	HasReplicaReadHint             bool
	HasMaxExecutionTime            bool
	HasEnableCascadesPlannerHint   bool
	HasResourceGroup               bool
	SetVars                        map[string]string

	// the original table hints
	OriginalTableHints []*ast.TableOptimizerHint
}

// TaskMapNeedBackUp indicates that whether we need to back up taskMap during physical optimizing.
func (sh *StmtHints) TaskMapNeedBackUp() bool {
	return sh.ForceNthPlan != -1
}

// Clone the StmtHints struct and returns the pointer of the new one.
func (sh *StmtHints) Clone() *StmtHints {
	var (
		vars       map[string]string
		tableHints []*ast.TableOptimizerHint
	)
	if len(sh.SetVars) > 0 {
		vars = make(map[string]string, len(sh.SetVars))
		for k, v := range sh.SetVars {
			vars[k] = v
		}
	}
	if len(sh.OriginalTableHints) > 0 {
		tableHints = make([]*ast.TableOptimizerHint, len(sh.OriginalTableHints))
		copy(tableHints, sh.OriginalTableHints)
	}
	return &StmtHints{
		MemQuotaQuery:                  sh.MemQuotaQuery,
		MaxExecutionTime:               sh.MaxExecutionTime,
		ReplicaRead:                    sh.ReplicaRead,
		AllowInSubqToJoinAndAgg:        sh.AllowInSubqToJoinAndAgg,
		NoIndexMergeHint:               sh.NoIndexMergeHint,
		StraightJoinOrder:              sh.StraightJoinOrder,
		EnableCascadesPlanner:          sh.EnableCascadesPlanner,
		ForceNthPlan:                   sh.ForceNthPlan,
		ResourceGroup:                  sh.ResourceGroup,
		HasAllowInSubqToJoinAndAggHint: sh.HasAllowInSubqToJoinAndAggHint,
		HasMemQuotaHint:                sh.HasMemQuotaHint,
		HasReplicaReadHint:             sh.HasReplicaReadHint,
		HasMaxExecutionTime:            sh.HasMaxExecutionTime,
		HasEnableCascadesPlannerHint:   sh.HasEnableCascadesPlannerHint,
		HasResourceGroup:               sh.HasResourceGroup,
		SetVars:                        vars,
		OriginalTableHints:             tableHints,
	}
}

// ParseStmtHints parses statement hints.
func ParseStmtHints(hints []*ast.TableOptimizerHint,
	setVarHintChecker func(varName, hint string) (ok bool, warning error),
	replicaReadFollower byte) ( // to avoid cycle import
	stmtHints StmtHints, offs []int, warns []error) {
	if len(hints) == 0 {
		return
	}
	hintOffs := make(map[string]int, len(hints))
	var forceNthPlan *ast.TableOptimizerHint
	var memoryQuotaHintCnt, useToJAHintCnt, useCascadesHintCnt, noIndexMergeHintCnt, readReplicaHintCnt, maxExecutionTimeCnt, forceNthPlanCnt, straightJoinHintCnt, resourceGroupHintCnt int
	setVars := make(map[string]string)
	setVarsOffs := make([]int, 0, len(hints))
	for i, hint := range hints {
		switch hint.HintName.L {
		case "memory_quota":
			hintOffs[hint.HintName.L] = i
			memoryQuotaHintCnt++
		case "resource_group":
			hintOffs[hint.HintName.L] = i
			resourceGroupHintCnt++
		case "use_toja":
			hintOffs[hint.HintName.L] = i
			useToJAHintCnt++
		case "use_cascades":
			hintOffs[hint.HintName.L] = i
			useCascadesHintCnt++
		case "no_index_merge":
			hintOffs[hint.HintName.L] = i
			noIndexMergeHintCnt++
		case "read_consistent_replica":
			hintOffs[hint.HintName.L] = i
			readReplicaHintCnt++
		case "max_execution_time":
			hintOffs[hint.HintName.L] = i
			maxExecutionTimeCnt++
		case "nth_plan":
			forceNthPlanCnt++
			forceNthPlan = hint
		case "straight_join":
			hintOffs[hint.HintName.L] = i
			straightJoinHintCnt++
		case "set_var":
			setVarHint := hint.HintData.(ast.HintSetVar)

			// Not all session variables are permitted for use with SET_VAR
			ok, warning := setVarHintChecker(setVarHint.VarName, hint.HintName.String())
			if warning != nil {
				warns = append(warns, warning)
			}
			if !ok {
				continue
			}

			// If several hints with the same variable name appear in the same statement, the first one is applied and the others are ignored with a warning
			if _, ok := setVars[setVarHint.VarName]; ok {
				msg := fmt.Sprintf("%s(%s=%s)", hint.HintName.String(), setVarHint.VarName, setVarHint.Value)
				warns = append(warns, ErrWarnConflictingHint.FastGenByArgs(msg))
				continue
			}
			setVars[setVarHint.VarName] = setVarHint.Value
			setVarsOffs = append(setVarsOffs, i)
		}
	}
	stmtHints.OriginalTableHints = hints
	stmtHints.SetVars = setVars

	// Handle MEMORY_QUOTA
	if memoryQuotaHintCnt != 0 {
		memoryQuotaHint := hints[hintOffs["memory_quota"]]
		if memoryQuotaHintCnt > 1 {
			warn := errors.NewNoStackErrorf("MEMORY_QUOTA() is defined more than once, only the last definition takes effect: MEMORY_QUOTA(%v)", memoryQuotaHint.HintData.(int64))
			warns = append(warns, warn)
		}
		// Executor use MemoryQuota <= 0 to indicate no memory limit, here use < 0 to handle hint syntax error.
		if memoryQuota := memoryQuotaHint.HintData.(int64); memoryQuota < 0 {
			delete(hintOffs, "memory_quota")
			warn := errors.NewNoStackError("The use of MEMORY_QUOTA hint is invalid, valid usage: MEMORY_QUOTA(10 MB) or MEMORY_QUOTA(10 GB)")
			warns = append(warns, warn)
		} else {
			stmtHints.HasMemQuotaHint = true
			stmtHints.MemQuotaQuery = memoryQuota
			if memoryQuota == 0 {
				warn := errors.NewNoStackError("Setting the MEMORY_QUOTA to 0 means no memory limit")
				warns = append(warns, warn)
			}
		}
	}
	// Handle USE_TOJA
	if useToJAHintCnt != 0 {
		useToJAHint := hints[hintOffs["use_toja"]]
		if useToJAHintCnt > 1 {
			warn := errors.NewNoStackErrorf("USE_TOJA() is defined more than once, only the last definition takes effect: USE_TOJA(%v)", useToJAHint.HintData.(bool))
			warns = append(warns, warn)
		}
		stmtHints.HasAllowInSubqToJoinAndAggHint = true
		stmtHints.AllowInSubqToJoinAndAgg = useToJAHint.HintData.(bool)
	}
	// Handle USE_CASCADES
	if useCascadesHintCnt != 0 {
		useCascadesHint := hints[hintOffs["use_cascades"]]
		if useCascadesHintCnt > 1 {
			warn := errors.NewNoStackErrorf("USE_CASCADES() is defined more than once, only the last definition takes effect: USE_CASCADES(%v)", useCascadesHint.HintData.(bool))
			warns = append(warns, warn)
		}
		stmtHints.HasEnableCascadesPlannerHint = true
		stmtHints.EnableCascadesPlanner = useCascadesHint.HintData.(bool)
	}
	// Handle NO_INDEX_MERGE
	if noIndexMergeHintCnt != 0 {
		if noIndexMergeHintCnt > 1 {
			warn := errors.NewNoStackError("NO_INDEX_MERGE() is defined more than once, only the last definition takes effect")
			warns = append(warns, warn)
		}
		stmtHints.NoIndexMergeHint = true
	}
	// Handle straight_join
	if straightJoinHintCnt != 0 {
		if straightJoinHintCnt > 1 {
			warn := errors.NewNoStackError("STRAIGHT_JOIN() is defined more than once, only the last definition takes effect")
			warns = append(warns, warn)
		}
		stmtHints.StraightJoinOrder = true
	}
	// Handle READ_CONSISTENT_REPLICA
	if readReplicaHintCnt != 0 {
		if readReplicaHintCnt > 1 {
			warn := errors.NewNoStackError("READ_CONSISTENT_REPLICA() is defined more than once, only the last definition takes effect")
			warns = append(warns, warn)
		}
		stmtHints.HasReplicaReadHint = true
		stmtHints.ReplicaRead = replicaReadFollower
	}
	// Handle MAX_EXECUTION_TIME
	if maxExecutionTimeCnt != 0 {
		maxExecutionTime := hints[hintOffs["max_execution_time"]]
		if maxExecutionTimeCnt > 1 {
			warn := errors.NewNoStackErrorf("MAX_EXECUTION_TIME() is defined more than once, only the last definition takes effect: MAX_EXECUTION_TIME(%v)", maxExecutionTime.HintData.(uint64))
			warns = append(warns, warn)
		}
		stmtHints.HasMaxExecutionTime = true
		stmtHints.MaxExecutionTime = maxExecutionTime.HintData.(uint64)
	}
	// Handle RESOURCE_GROUP
	if resourceGroupHintCnt != 0 {
		resourceGroup := hints[hintOffs["resource_group"]]
		if resourceGroupHintCnt > 1 {
			warn := errors.NewNoStackErrorf("RESOURCE_GROUP() is defined more than once, only the last definition takes effect: RESOURCE_GROUP(%v)", resourceGroup.HintData.(string))
			warns = append(warns, warn)
		}
		stmtHints.HasResourceGroup = true
		stmtHints.ResourceGroup = resourceGroup.HintData.(string)
	}
	// Handle NTH_PLAN
	if forceNthPlanCnt != 0 {
		if forceNthPlanCnt > 1 {
			warn := errors.NewNoStackErrorf("NTH_PLAN() is defined more than once, only the last definition takes effect: NTH_PLAN(%v)", forceNthPlan.HintData.(int64))
			warns = append(warns, warn)
		}
		stmtHints.ForceNthPlan = forceNthPlan.HintData.(int64)
		if stmtHints.ForceNthPlan < 1 {
			stmtHints.ForceNthPlan = -1
			warn := errors.NewNoStackError("the hintdata for NTH_PLAN() is too small, hint ignored")
			warns = append(warns, warn)
		}
	} else {
		stmtHints.ForceNthPlan = -1
	}
	for _, off := range hintOffs {
		offs = append(offs, off)
	}
	offs = append(offs, setVarsOffs...)
	// let hint is always ordered, it is convenient to human compare and test.
	sort.Ints(offs)
	return
}

// IndexJoinHints stores hint information about index nested loop join.
type IndexJoinHints struct {
	INLJTables  []HintedTable
	INLHJTables []HintedTable
	INLMJTables []HintedTable
}

// PlanHints are hints that are used to control the optimizer plan choices like 'use_index', 'hash_join'.
// TODO: move ignore_plan_cache, straight_join, no_decorrelate here.
type PlanHints struct {
	IndexJoin          IndexJoinHints // inlj_join, inlhj_join, inlmj_join
	NoIndexJoin        IndexJoinHints // no_inlj_join, no_inlhj_join, no_inlmj_join
	HashJoin           []HintedTable  // hash_join
	NoHashJoin         []HintedTable  // no_hash_join
	SortMergeJoin      []HintedTable  // merge_join
	NoMergeJoin        []HintedTable  // no_merge_join
	BroadcastJoin      []HintedTable  // bcj_join
	ShuffleJoin        []HintedTable  // shuffle_join
	IndexHintList      []HintedIndex  // use_index, ignore_index
	IndexMergeHintList []HintedIndex  // use_index_merge
	TiFlashTables      []HintedTable  // isolation_read_engines(xx=tiflash)
	TiKVTables         []HintedTable  // isolation_read_engines(xx=tikv)
	LeadingJoinOrder   []HintedTable  // leading
	HJBuild            []HintedTable  // hash_join_build
	HJProbe            []HintedTable  // hash_join_probe

	// Hints belows are not associated with any particular table.
	PreferAggType    uint // hash_agg, merge_agg, agg_to_cop and so on
	PreferAggToCop   bool
	PreferLimitToCop bool // limit_to_cop
	CTEMerge         bool // merge
	TimeRangeHint    ast.HintTimeRange
}

// HintedTable indicates which table this hint should take effect on.
type HintedTable struct {
	DBName       model.CIStr   // the database name
	TblName      model.CIStr   // the table name
	Partitions   []model.CIStr // partition information
	SelectOffset int           // the select block offset of this hint
	Matched      bool          // whether this hint is applied successfully
}

// HintedIndex indicates which index this hint should take effect on.
type HintedIndex struct {
	DBName     model.CIStr    // the database name
	TblName    model.CIStr    // the table name
	Partitions []model.CIStr  // partition information
	IndexHint  *ast.IndexHint // the original parser index hint structure
	// Matched indicates whether this index hint
	// has been successfully applied to a DataSource.
	// If an HintedIndex is not Matched after building
	// a Select statement, we will generate a warning for it.
	Matched bool
}

// Match checks whether the hint is matched with the given dbName and tblName.
func (hint *HintedIndex) Match(dbName, tblName model.CIStr) bool {
	return hint.TblName.L == tblName.L &&
		(hint.DBName.L == dbName.L ||
			hint.DBName.L == "*") // for universal bindings, e.g. *.t
}

// HintTypeString returns the string representation of the hint type.
func (hint *HintedIndex) HintTypeString() string {
	switch hint.IndexHint.HintType {
	case ast.HintUse:
		return "use_index"
	case ast.HintIgnore:
		return "ignore_index"
	case ast.HintForce:
		return "force_index"
	}
	return ""
}

// IndexString formats the IndexHint as DBName.tableName[, indexNames].
func (hint *HintedIndex) IndexString() string {
	var indexListString string
	indexList := make([]string, len(hint.IndexHint.IndexNames))
	for i := range hint.IndexHint.IndexNames {
		indexList[i] = hint.IndexHint.IndexNames[i].L
	}
	if len(indexList) > 0 {
		indexListString = fmt.Sprintf(", %s", strings.Join(indexList, ", "))
	}
	return fmt.Sprintf("%s.%s%s", hint.DBName, hint.TblName, indexListString)
}

// IfPreferMergeJoin checks whether the join hint is merge join.
func (pHints *PlanHints) IfPreferMergeJoin(tableNames ...*HintedTable) bool {
	return pHints.MatchTableName(tableNames, pHints.SortMergeJoin)
}

// IfPreferBroadcastJoin checks whether the join hint is broadcast join.
func (pHints *PlanHints) IfPreferBroadcastJoin(tableNames ...*HintedTable) bool {
	return pHints.MatchTableName(tableNames, pHints.BroadcastJoin)
}

// IfPreferShuffleJoin checks whether the join hint is shuffle join.
func (pHints *PlanHints) IfPreferShuffleJoin(tableNames ...*HintedTable) bool {
	return pHints.MatchTableName(tableNames, pHints.ShuffleJoin)
}

// IfPreferHashJoin checks whether the join hint is hash join.
func (pHints *PlanHints) IfPreferHashJoin(tableNames ...*HintedTable) bool {
	return pHints.MatchTableName(tableNames, pHints.HashJoin)
}

// IfPreferNoHashJoin checks whether the join hint is no hash join.
func (pHints *PlanHints) IfPreferNoHashJoin(tableNames ...*HintedTable) bool {
	return pHints.MatchTableName(tableNames, pHints.NoHashJoin)
}

// IfPreferNoMergeJoin checks whether the join hint is no merge join.
func (pHints *PlanHints) IfPreferNoMergeJoin(tableNames ...*HintedTable) bool {
	return pHints.MatchTableName(tableNames, pHints.NoMergeJoin)
}

// IfPreferHJBuild checks whether the join hint is hash join build side.
func (pHints *PlanHints) IfPreferHJBuild(tableNames ...*HintedTable) bool {
	return pHints.MatchTableName(tableNames, pHints.HJBuild)
}

// IfPreferHJProbe checks whether the join hint is hash join probe side.
func (pHints *PlanHints) IfPreferHJProbe(tableNames ...*HintedTable) bool {
	return pHints.MatchTableName(tableNames, pHints.HJProbe)
}

// IfPreferINLJ checks whether the join hint is index nested loop join.
func (pHints *PlanHints) IfPreferINLJ(tableNames ...*HintedTable) bool {
	return pHints.MatchTableName(tableNames, pHints.IndexJoin.INLJTables)
}

// IfPreferINLHJ checks whether the join hint is index nested loop hash join.
func (pHints *PlanHints) IfPreferINLHJ(tableNames ...*HintedTable) bool {
	return pHints.MatchTableName(tableNames, pHints.IndexJoin.INLHJTables)
}

// IfPreferINLMJ checks whether the join hint is index nested loop merge join.
func (pHints *PlanHints) IfPreferINLMJ(tableNames ...*HintedTable) bool {
	return pHints.MatchTableName(tableNames, pHints.IndexJoin.INLMJTables)
}

// IfPreferNoIndexJoin checks whether the join hint is no index join.
func (pHints *PlanHints) IfPreferNoIndexJoin(tableNames ...*HintedTable) bool {
	return pHints.MatchTableName(tableNames, pHints.NoIndexJoin.INLJTables)
}

// IfPreferNoIndexHashJoin checks whether the join hint is no index hash join.
func (pHints *PlanHints) IfPreferNoIndexHashJoin(tableNames ...*HintedTable) bool {
	return pHints.MatchTableName(tableNames, pHints.NoIndexJoin.INLHJTables)
}

// IfPreferNoIndexMergeJoin checks whether the join hint is no index merge join.
func (pHints *PlanHints) IfPreferNoIndexMergeJoin(tableNames ...*HintedTable) bool {
	return pHints.MatchTableName(tableNames, pHints.NoIndexJoin.INLMJTables)
}

// IfPreferTiFlash checks whether the hint hit the need of TiFlash.
func (pHints *PlanHints) IfPreferTiFlash(tableName *HintedTable) *HintedTable {
	return pHints.matchTiKVOrTiFlash(tableName, pHints.TiFlashTables)
}

// IfPreferTiKV checks whether the hint hit the need of TiKV.
func (pHints *PlanHints) IfPreferTiKV(tableName *HintedTable) *HintedTable {
	return pHints.matchTiKVOrTiFlash(tableName, pHints.TiKVTables)
}

func (*PlanHints) matchTiKVOrTiFlash(tableName *HintedTable, hintTables []HintedTable) *HintedTable {
	if tableName == nil {
		return nil
	}
	for i, tbl := range hintTables {
		if tableName.DBName.L == tbl.DBName.L && tableName.TblName.L == tbl.TblName.L && tbl.SelectOffset == tableName.SelectOffset {
			hintTables[i].Matched = true
			return &tbl
		}
	}
	return nil
}

// MatchTableName checks whether the hint hit the need.
// Only need either side matches one on the list.
// Even though you can put 2 tables on the list,
// it doesn't mean optimizer will reorder to make them
// join directly.
// Which it joins on with depend on sequence of traverse
// and without reorder, user might adjust themselves.
// This is similar to MySQL hints.
func (*PlanHints) MatchTableName(tables []*HintedTable, hintTables []HintedTable) bool {
	hintMatched := false
	for _, table := range tables {
		for i, curEntry := range hintTables {
			if table == nil {
				continue
			}
			if (curEntry.DBName.L == table.DBName.L || curEntry.DBName.L == "*") &&
				curEntry.TblName.L == table.TblName.L &&
				table.SelectOffset == curEntry.SelectOffset {
				hintTables[i].Matched = true
				hintMatched = true
				break
			}
		}
	}
	return hintMatched
}

// ParsePlanHints parses *ast.TableOptimizerHint to PlanHints.
func ParsePlanHints(hints []*ast.TableOptimizerHint,
	currentLevel int, currentDB string,
	hintProcessor *QBHintHandler, straightJoinOrder bool,
	handlingExistsSubquery, notHandlingSubquery bool,
	warnHandler hintWarnHandler) (p *PlanHints, subQueryHintFlags uint64, err error) {
	var (
		sortMergeTables, inljTables, inlhjTables, inlmjTables, hashJoinTables, bcTables []HintedTable
		noIndexJoinTables, noIndexHashJoinTables, noIndexMergeJoinTables                []HintedTable
		noHashJoinTables, noMergeJoinTables                                             []HintedTable
		shuffleJoinTables                                                               []HintedTable
		indexHintList, indexMergeHintList                                               []HintedIndex
		tiflashTables, tikvTables                                                       []HintedTable
		preferAggType                                                                   uint
		preferAggToCop                                                                  bool
		timeRangeHint                                                                   ast.HintTimeRange
		preferLimitToCop                                                                bool
		cteMerge                                                                        bool
		leadingJoinOrder                                                                []HintedTable
		hjBuildTables, hjProbeTables                                                    []HintedTable
		leadingHintCnt                                                                  int
	)
	for _, hint := range hints {
		// Set warning for the hint that requires the table name.
		switch hint.HintName.L {
		case TiDBMergeJoin, HintSMJ, TiDBIndexNestedLoopJoin, HintINLJ, HintINLHJ, HintINLMJ,
			HintNoHashJoin, HintNoMergeJoin, TiDBHashJoin, HintHJ, HintUseIndex, HintIgnoreIndex,
			HintForceIndex, HintOrderIndex, HintNoOrderIndex, HintIndexMerge, HintLeading:
			if len(hint.Tables) == 0 {
				var sb strings.Builder
				ctx := format.NewRestoreCtx(0, &sb)
				if err := hint.Restore(ctx); err != nil {
					return nil, 0, err
				}
				errMsg := fmt.Sprintf("Hint %s is inapplicable. Please specify the table names in the arguments.", sb.String())
				warnHandler.SetHintWarning(errMsg)
				continue
			}
		}

		switch hint.HintName.L {
		case TiDBMergeJoin, HintSMJ:
			sortMergeTables = append(sortMergeTables, tableNames2HintTableInfo(currentDB, hint.HintName.L, hint.Tables, hintProcessor, currentLevel, warnHandler)...)
		case TiDBBroadCastJoin, HintBCJ:
			bcTables = append(bcTables, tableNames2HintTableInfo(currentDB, hint.HintName.L, hint.Tables, hintProcessor, currentLevel, warnHandler)...)
		case HintShuffleJoin:
			shuffleJoinTables = append(shuffleJoinTables, tableNames2HintTableInfo(currentDB, hint.HintName.L, hint.Tables, hintProcessor, currentLevel, warnHandler)...)
		case TiDBIndexNestedLoopJoin, HintINLJ:
			inljTables = append(inljTables, tableNames2HintTableInfo(currentDB, hint.HintName.L, hint.Tables, hintProcessor, currentLevel, warnHandler)...)
		case HintINLHJ:
			inlhjTables = append(inlhjTables, tableNames2HintTableInfo(currentDB, hint.HintName.L, hint.Tables, hintProcessor, currentLevel, warnHandler)...)
		case HintINLMJ:
			inlmjTables = append(inlmjTables, tableNames2HintTableInfo(currentDB, hint.HintName.L, hint.Tables, hintProcessor, currentLevel, warnHandler)...)
		case TiDBHashJoin, HintHJ:
			hashJoinTables = append(hashJoinTables, tableNames2HintTableInfo(currentDB, hint.HintName.L, hint.Tables, hintProcessor, currentLevel, warnHandler)...)
		case HintNoHashJoin:
			noHashJoinTables = append(noHashJoinTables, tableNames2HintTableInfo(currentDB, hint.HintName.L, hint.Tables, hintProcessor, currentLevel, warnHandler)...)
		case HintNoMergeJoin:
			noMergeJoinTables = append(noMergeJoinTables, tableNames2HintTableInfo(currentDB, hint.HintName.L, hint.Tables, hintProcessor, currentLevel, warnHandler)...)
		case HintNoIndexJoin:
			noIndexJoinTables = append(noIndexJoinTables, tableNames2HintTableInfo(currentDB, hint.HintName.L, hint.Tables, hintProcessor, currentLevel, warnHandler)...)
		case HintNoIndexHashJoin:
			noIndexHashJoinTables = append(noIndexHashJoinTables, tableNames2HintTableInfo(currentDB, hint.HintName.L, hint.Tables, hintProcessor, currentLevel, warnHandler)...)
		case HintNoIndexMergeJoin:
			noIndexMergeJoinTables = append(noIndexMergeJoinTables, tableNames2HintTableInfo(currentDB, hint.HintName.L, hint.Tables, hintProcessor, currentLevel, warnHandler)...)
		case HintMPP1PhaseAgg:
			preferAggType |= PreferMPP1PhaseAgg
		case HintMPP2PhaseAgg:
			preferAggType |= PreferMPP2PhaseAgg
		case HintHashJoinBuild:
			hjBuildTables = append(hjBuildTables, tableNames2HintTableInfo(currentDB, hint.HintName.L, hint.Tables, hintProcessor, currentLevel, warnHandler)...)
		case HintHashJoinProbe:
			hjProbeTables = append(hjProbeTables, tableNames2HintTableInfo(currentDB, hint.HintName.L, hint.Tables, hintProcessor, currentLevel, warnHandler)...)
		case HintHashAgg:
			preferAggType |= PreferHashAgg
		case HintStreamAgg:
			preferAggType |= PreferStreamAgg
		case HintAggToCop:
			preferAggToCop = true
		case HintUseIndex, HintIgnoreIndex, HintForceIndex, HintOrderIndex, HintNoOrderIndex:
			dbName := hint.Tables[0].DBName
			if dbName.L == "" {
				dbName = model.NewCIStr(currentDB)
			}
			var hintType ast.IndexHintType
			switch hint.HintName.L {
			case HintUseIndex:
				hintType = ast.HintUse
			case HintIgnoreIndex:
				hintType = ast.HintIgnore
			case HintForceIndex:
				hintType = ast.HintForce
			case HintOrderIndex:
				hintType = ast.HintOrderIndex
			case HintNoOrderIndex:
				hintType = ast.HintNoOrderIndex
			}
			indexHintList = append(indexHintList, HintedIndex{
				DBName:     dbName,
				TblName:    hint.Tables[0].TableName,
				Partitions: hint.Tables[0].PartitionList,
				IndexHint: &ast.IndexHint{
					IndexNames: hint.Indexes,
					HintType:   hintType,
					HintScope:  ast.HintForScan,
				},
			})
		case HintReadFromStorage:
			switch hint.HintData.(model.CIStr).L {
			case HintTiFlash:
				tiflashTables = append(tiflashTables, tableNames2HintTableInfo(currentDB, hint.HintName.L, hint.Tables, hintProcessor, currentLevel, warnHandler)...)
			case HintTiKV:
				tikvTables = append(tikvTables, tableNames2HintTableInfo(currentDB, hint.HintName.L, hint.Tables, hintProcessor, currentLevel, warnHandler)...)
			}
		case HintIndexMerge:
			dbName := hint.Tables[0].DBName
			if dbName.L == "" {
				dbName = model.NewCIStr(currentDB)
			}
			indexMergeHintList = append(indexMergeHintList, HintedIndex{
				DBName:     dbName,
				TblName:    hint.Tables[0].TableName,
				Partitions: hint.Tables[0].PartitionList,
				IndexHint: &ast.IndexHint{
					IndexNames: hint.Indexes,
					HintType:   ast.HintUse,
					HintScope:  ast.HintForScan,
				},
			})
		case HintTimeRange:
			timeRangeHint = hint.HintData.(ast.HintTimeRange)
		case HintLimitToCop:
			preferLimitToCop = true
		case HintMerge:
			if hint.Tables != nil {
				warnHandler.SetHintWarning("The MERGE hint is not used correctly, maybe it inputs a table name.")
				continue
			}
			cteMerge = true
		case HintLeading:
			if leadingHintCnt == 0 {
				leadingJoinOrder = append(leadingJoinOrder, tableNames2HintTableInfo(currentDB, hint.HintName.L, hint.Tables, hintProcessor, currentLevel, warnHandler)...)
			}
			leadingHintCnt++
		case HintSemiJoinRewrite:
			if !handlingExistsSubquery {
				warnHandler.SetHintWarning("The SEMI_JOIN_REWRITE hint is not used correctly, maybe it's not in a subquery or the subquery is not EXISTS clause.")
				continue
			}
			subQueryHintFlags |= HintFlagSemiJoinRewrite
		case HintNoDecorrelate:
			if notHandlingSubquery {
				warnHandler.SetHintWarning("NO_DECORRELATE() is inapplicable because it's not in an IN subquery, an EXISTS subquery, an ANY/ALL/SOME subquery or a scalar subquery.")
				continue
			}
			subQueryHintFlags |= HintFlagNoDecorrelate
		default:
			// ignore hints that not implemented
		}
	}
	if leadingHintCnt > 1 || (leadingHintCnt > 0 && straightJoinOrder) {
		// If there are more leading hints or the straight_join hint existes, all leading hints will be invalid.
		leadingJoinOrder = leadingJoinOrder[:0]
		if leadingHintCnt > 1 {
			warnHandler.SetHintWarning("We can only use one leading hint at most, when multiple leading hints are used, all leading hints will be invalid")
		} else if straightJoinOrder {
			warnHandler.SetHintWarning("We can only use the straight_join hint, when we use the leading hint and straight_join hint at the same time, all leading hints will be invalid")
		}
	}
	return &PlanHints{
		SortMergeJoin:      sortMergeTables,
		BroadcastJoin:      bcTables,
		ShuffleJoin:        shuffleJoinTables,
		IndexJoin:          IndexJoinHints{INLJTables: inljTables, INLHJTables: inlhjTables, INLMJTables: inlmjTables},
		NoIndexJoin:        IndexJoinHints{INLJTables: noIndexJoinTables, INLHJTables: noIndexHashJoinTables, INLMJTables: noIndexMergeJoinTables},
		HashJoin:           hashJoinTables,
		NoHashJoin:         noHashJoinTables,
		NoMergeJoin:        noMergeJoinTables,
		IndexHintList:      indexHintList,
		TiFlashTables:      tiflashTables,
		TiKVTables:         tikvTables,
		PreferAggToCop:     preferAggToCop,
		PreferAggType:      preferAggType,
		IndexMergeHintList: indexMergeHintList,
		TimeRangeHint:      timeRangeHint,
		PreferLimitToCop:   preferLimitToCop,
		CTEMerge:           cteMerge,
		LeadingJoinOrder:   leadingJoinOrder,
		HJBuild:            hjBuildTables,
		HJProbe:            hjProbeTables,
	}, subQueryHintFlags, nil
}

// RemoveDuplicatedHints removes duplicated hints in this hit list.
func RemoveDuplicatedHints(hints []*ast.TableOptimizerHint) []*ast.TableOptimizerHint {
	if len(hints) < 2 {
		return hints
	}
	m := make(map[string]struct{}, len(hints))
	res := make([]*ast.TableOptimizerHint, 0, len(hints))
	for _, hint := range hints {
		key := RestoreTableOptimizerHint(hint)
		if _, ok := m[key]; ok {
			continue
		}
		m[key] = struct{}{}
		res = append(res, hint)
	}
	return res
}

// tableNames2HintTableInfo converts table names to HintedTable.
func tableNames2HintTableInfo(currentDB, hintName string, hintTables []ast.HintTable,
	p *QBHintHandler, currentOffset int, warnHandler hintWarnHandler) []HintedTable {
	if len(hintTables) == 0 {
		return nil
	}
	hintTableInfos := make([]HintedTable, 0, len(hintTables))
	defaultDBName := model.NewCIStr(currentDB)
	isInapplicable := false
	for _, hintTable := range hintTables {
		tableInfo := HintedTable{
			DBName:       hintTable.DBName,
			TblName:      hintTable.TableName,
			Partitions:   hintTable.PartitionList,
			SelectOffset: p.GetHintOffset(hintTable.QBName, currentOffset),
		}
		if tableInfo.DBName.L == "" {
			tableInfo.DBName = defaultDBName
		}
		switch hintName {
		case TiDBMergeJoin, HintSMJ, TiDBIndexNestedLoopJoin, HintINLJ,
			HintINLHJ, HintINLMJ, TiDBHashJoin, HintHJ, HintLeading:
			if len(tableInfo.Partitions) > 0 {
				isInapplicable = true
			}
		}
		hintTableInfos = append(hintTableInfos, tableInfo)
	}
	if isInapplicable {
		warnHandler.SetHintWarningFromError(fmt.Errorf("Optimizer Hint %s is inapplicable on specified partitions", Restore2JoinHint(hintName, hintTableInfos)))
		return nil
	}
	return hintTableInfos
}

func restore2TableHint(hintTables ...HintedTable) string {
	buffer := bytes.NewBufferString("")
	for i, table := range hintTables {
		buffer.WriteString(table.TblName.L)
		if len(table.Partitions) > 0 {
			buffer.WriteString(" PARTITION(")
			for j, partition := range table.Partitions {
				if j > 0 {
					buffer.WriteString(", ")
				}
				buffer.WriteString(partition.L)
			}
			buffer.WriteString(")")
		}
		if i < len(hintTables)-1 {
			buffer.WriteString(", ")
		}
	}
	return buffer.String()
}

// Restore2JoinHint restores join hint to string.
func Restore2JoinHint(hintType string, hintTables []HintedTable) string {
	if len(hintTables) == 0 {
		return strings.ToUpper(hintType)
	}
	buffer := bytes.NewBufferString("/*+ ")
	buffer.WriteString(strings.ToUpper(hintType))
	buffer.WriteString("(")
	buffer.WriteString(restore2TableHint(hintTables...))
	buffer.WriteString(") */")
	return buffer.String()
}

// Restore2IndexHint restores index hint to string.
func Restore2IndexHint(hintType string, hintIndex HintedIndex) string {
	buffer := bytes.NewBufferString("/*+ ")
	buffer.WriteString(strings.ToUpper(hintType))
	buffer.WriteString("(")
	buffer.WriteString(restore2TableHint(HintedTable{
		DBName:     hintIndex.DBName,
		TblName:    hintIndex.TblName,
		Partitions: hintIndex.Partitions,
	}))
	if hintIndex.IndexHint != nil && len(hintIndex.IndexHint.IndexNames) > 0 {
		for i, indexName := range hintIndex.IndexHint.IndexNames {
			if i > 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(" " + indexName.L)
		}
	}
	buffer.WriteString(") */")
	return buffer.String()
}

// Restore2StorageHint restores storage hint to string.
func Restore2StorageHint(tiflashTables, tikvTables []HintedTable) string {
	buffer := bytes.NewBufferString("/*+ ")
	buffer.WriteString(strings.ToUpper(HintReadFromStorage))
	buffer.WriteString("(")
	if len(tiflashTables) > 0 {
		buffer.WriteString("tiflash[")
		buffer.WriteString(restore2TableHint(tiflashTables...))
		buffer.WriteString("]")
		if len(tikvTables) > 0 {
			buffer.WriteString(", ")
		}
	}
	if len(tikvTables) > 0 {
		buffer.WriteString("tikv[")
		buffer.WriteString(restore2TableHint(tikvTables...))
		buffer.WriteString("]")
	}
	buffer.WriteString(") */")
	return buffer.String()
}

// ExtractUnmatchedTables extracts unmatched tables from hintTables.
func ExtractUnmatchedTables(hintTables []HintedTable) []string {
	var tableNames []string
	for _, table := range hintTables {
		if !table.Matched {
			tableNames = append(tableNames, table.TblName.O)
		}
	}
	return tableNames
}

// CollectUnmatchedHintWarnings collects warnings for unmatched hints from this TableHintInfo.
func CollectUnmatchedHintWarnings(hintInfo *PlanHints) (warnings []string) {
	warnings = append(warnings, collectUnmatchedIndexHintWarning(hintInfo.IndexHintList, false)...)
	warnings = append(warnings, collectUnmatchedIndexHintWarning(hintInfo.IndexMergeHintList, true)...)
	warnings = append(warnings, collectUnmatchedJoinHintWarning(HintINLJ, TiDBIndexNestedLoopJoin, hintInfo.IndexJoin.INLJTables)...)
	warnings = append(warnings, collectUnmatchedJoinHintWarning(HintINLHJ, "", hintInfo.IndexJoin.INLHJTables)...)
	warnings = append(warnings, collectUnmatchedJoinHintWarning(HintINLMJ, "", hintInfo.IndexJoin.INLMJTables)...)
	warnings = append(warnings, collectUnmatchedJoinHintWarning(HintSMJ, TiDBMergeJoin, hintInfo.SortMergeJoin)...)
	warnings = append(warnings, collectUnmatchedJoinHintWarning(HintBCJ, TiDBBroadCastJoin, hintInfo.BroadcastJoin)...)
	warnings = append(warnings, collectUnmatchedJoinHintWarning(HintShuffleJoin, HintShuffleJoin, hintInfo.ShuffleJoin)...)
	warnings = append(warnings, collectUnmatchedJoinHintWarning(HintHJ, TiDBHashJoin, hintInfo.HashJoin)...)
	warnings = append(warnings, collectUnmatchedJoinHintWarning(HintHashJoinBuild, "", hintInfo.HJBuild)...)
	warnings = append(warnings, collectUnmatchedJoinHintWarning(HintHashJoinProbe, "", hintInfo.HJProbe)...)
	warnings = append(warnings, collectUnmatchedJoinHintWarning(HintLeading, "", hintInfo.LeadingJoinOrder)...)
	warnings = append(warnings, collectUnmatchedStorageHintWarning(hintInfo.TiFlashTables, hintInfo.TiKVTables)...)
	return warnings
}

func collectUnmatchedIndexHintWarning(indexHints []HintedIndex, usedForIndexMerge bool) (warnings []string) {
	for _, hint := range indexHints {
		if !hint.Matched {
			var hintTypeString string
			if usedForIndexMerge {
				hintTypeString = "use_index_merge"
			} else {
				hintTypeString = hint.HintTypeString()
			}
			errMsg := fmt.Sprintf("%s(%s) is inapplicable, check whether the table(%s.%s) exists",
				hintTypeString,
				hint.IndexString(),
				hint.DBName,
				hint.TblName,
			)
			warnings = append(warnings, errMsg)
		}
	}
	return warnings
}

func collectUnmatchedJoinHintWarning(joinType string, joinTypeAlias string, hintTables []HintedTable) (warnings []string) {
	unMatchedTables := ExtractUnmatchedTables(hintTables)
	if len(unMatchedTables) == 0 {
		return
	}
	if len(joinTypeAlias) != 0 {
		joinTypeAlias = fmt.Sprintf(" or %s", Restore2JoinHint(joinTypeAlias, hintTables))
	}

	errMsg := fmt.Sprintf("There are no matching table names for (%s) in optimizer hint %s%s. Maybe you can use the table alias name",
		strings.Join(unMatchedTables, ", "), Restore2JoinHint(joinType, hintTables), joinTypeAlias)
	warnings = append(warnings, errMsg)
	return warnings
}

func collectUnmatchedStorageHintWarning(tiflashTables, tikvTables []HintedTable) (warnings []string) {
	unMatchedTiFlashTables := ExtractUnmatchedTables(tiflashTables)
	unMatchedTiKVTables := ExtractUnmatchedTables(tikvTables)
	if len(unMatchedTiFlashTables)+len(unMatchedTiKVTables) == 0 {
		return
	}
	errMsg := fmt.Sprintf("There are no matching table names for (%s) in optimizer hint %s. Maybe you can use the table alias name",
		strings.Join(append(unMatchedTiFlashTables, unMatchedTiKVTables...), ", "),
		Restore2StorageHint(tiflashTables, tikvTables))
	warnings = append(warnings, errMsg)
	return warnings
}

// ErrWarnConflictingHint is a warning error.
var ErrWarnConflictingHint = dbterror.ClassOptimizer.NewStd(mysql.ErrWarnConflictingHint)
