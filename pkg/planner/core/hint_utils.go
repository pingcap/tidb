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

package core

import (
	"fmt"
	"slices"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	h "github.com/pingcap/tidb/pkg/util/hint"
)

// GenHintsFromFlatPlan generates hints from a FlatPhysicalPlan. It never
// appends statement warnings: besides EXPLAIN, this generator also runs
// lazily after ordinary statement execution (to record the plan hint for
// statements_summary) and inside baseline evolution, where nobody asked for
// diagnostics and a warning would leak into SHOW WARNINGS of statements that
// contain no LEADING hint at all.
func GenHintsFromFlatPlan(flat *FlatPhysicalPlan) []*ast.TableOptimizerHint {
	return genHintsFromFlatPlan(flat, false)
}

// GenHintsFromFlatPlanForExplain is the EXPLAIN FORMAT='hint' entry point.
// The user explicitly asked for generated hints there, so when a join group
// drops its LEADING hint because two operands share one owner-visible
// identity, a statement warning explains the drop instead of silently
// returning a shorter hint set.
func GenHintsFromFlatPlanForExplain(flat *FlatPhysicalPlan) []*ast.TableOptimizerHint {
	return genHintsFromFlatPlan(flat, true)
}

func genHintsFromFlatPlan(flat *FlatPhysicalPlan, warnAmbiguousLeading bool) []*ast.TableOptimizerHint {
	if len(flat.Main) == 0 {
		return nil
	}
	nodeTp := h.TypeSelect
	switch flat.Main[0].Origin.(type) {
	case *Update:
		nodeTp = h.TypeUpdate
	case *Delete:
		nodeTp = h.TypeDelete
	}
	var hints []*ast.TableOptimizerHint
	selectPlan, _ := flat.Main.GetSelectPlan()
	if len(selectPlan) == 0 || !selectPlan[0].IsPhysicalPlan {
		return nil
	}
	// To generate leading hint, we need to extract join group from the plan tree by traversing children of PhysicalJoin
	// operators. We use this map to avoid revisiting the same operator during this process.
	visitedPhysicalJoinIDs := make(map[int]struct{})
	for _, fop := range selectPlan {
		p := fop.Origin.(base.PhysicalPlan)
		hints = genHintsFromSingle(p, nodeTp, fop.StoreType, hints)
		if join, ok := p.(PhysicalJoin); ok {
			joinOrderHint := genJoinOrderHintFromRootPhysicalJoin(join, visitedPhysicalJoinIDs, nodeTp, warnAmbiguousLeading)
			if joinOrderHint != nil {
				hints = append(hints, joinOrderHint)
			}
		}
	}
	for _, cte := range flat.CTEs {
		// TODO(#68977): Recover CTE-visible operand identities before emitting
		// LEADING hints for CTE plans. The derived-table resolver below proves
		// visibility only through SELECT-block alias chains.
		for i, fop := range cte {
			if i == 0 || !fop.IsRoot {
				continue
			}
			p := fop.Origin.(base.PhysicalPlan)
			hints = genHintsFromSingle(p, nodeTp, fop.StoreType, hints)
			if join, ok := p.(PhysicalJoin); ok {
				joinOrderHint := genJoinOrderHintFromRootPhysicalJoin(join, visitedPhysicalJoinIDs, nodeTp, warnAmbiguousLeading)
				if joinOrderHint != nil {
					hints = append(hints, joinOrderHint)
				}
			}
		}
	}
	return h.RemoveDuplicatedHints(hints)
}

// GenHintsFromPhysicalPlan generates hints from physical plan.
func GenHintsFromPhysicalPlan(p base.Plan) []*ast.TableOptimizerHint {
	flat := FlattenPhysicalPlan(p, false)
	return GenHintsFromFlatPlan(flat)
}

func genHintsFromSingle(p base.PhysicalPlan, nodeType h.NodeType, storeType kv.StoreType, res []*ast.TableOptimizerHint) []*ast.TableOptimizerHint {
	qbName, err := h.GenerateQBName(nodeType, p.QueryBlockOffset())
	if err != nil {
		return res
	}
	switch pp := p.(type) {
	case *PhysicalLimit, *PhysicalTopN:
		if storeType == kv.TiKV {
			res = append(res, &ast.TableOptimizerHint{
				QBName:   qbName,
				HintName: model.NewCIStr(h.HintLimitToCop),
			})
		}
	case *PhysicalTableReader:
		tbl, ok := pp.TablePlans[0].(*PhysicalTableScan)
		if !ok {
			return res
		}
		if tbl.StoreType == kv.TiFlash {
			res = append(res, &ast.TableOptimizerHint{
				QBName:   qbName,
				HintName: model.NewCIStr(h.HintReadFromStorage),
				HintData: model.NewCIStr(kv.TiFlash.Name()),
				Tables:   []ast.HintTable{{DBName: tbl.DBName, TableName: getTableName(tbl.Table.Name, tbl.TableAsName)}},
			})
		} else {
			res = append(res, &ast.TableOptimizerHint{
				QBName:   qbName,
				HintName: model.NewCIStr(h.HintUseIndex),
				Tables:   []ast.HintTable{{DBName: tbl.DBName, TableName: getTableName(tbl.Table.Name, tbl.TableAsName)}},
			})
			if tbl.Table.PKIsHandle || tbl.Table.IsCommonHandle { // it's a primary key
				orderHint := h.HintOrderIndex
				if !tbl.KeepOrder {
					orderHint = h.HintNoOrderIndex
				}
				res = append(res, &ast.TableOptimizerHint{
					QBName:   qbName,
					HintName: model.NewCIStr(orderHint),
					Tables:   []ast.HintTable{{DBName: tbl.DBName, TableName: getTableName(tbl.Table.Name, tbl.TableAsName)}},
					Indexes:  []model.CIStr{model.NewCIStr("primary")},
				})
			}
		}
	case *PhysicalIndexLookUpReader:
		index := pp.IndexPlans[0].(*PhysicalIndexScan)
		res = append(res, &ast.TableOptimizerHint{
			QBName:   qbName,
			HintName: model.NewCIStr(h.HintUseIndex),
			Tables:   []ast.HintTable{{DBName: index.DBName, TableName: getTableName(index.Table.Name, index.TableAsName)}},
			Indexes:  []model.CIStr{index.Index.Name},
		})
		orderHint := h.HintOrderIndex
		if !index.KeepOrder {
			orderHint = h.HintNoOrderIndex
		}
		res = append(res, &ast.TableOptimizerHint{
			QBName:   qbName,
			HintName: model.NewCIStr(orderHint),
			Tables:   []ast.HintTable{{DBName: index.DBName, TableName: getTableName(index.Table.Name, index.TableAsName)}},
			Indexes:  []model.CIStr{index.Index.Name},
		})
	case *PhysicalIndexReader:
		index := pp.IndexPlans[0].(*PhysicalIndexScan)
		res = append(res, &ast.TableOptimizerHint{
			QBName:   qbName,
			HintName: model.NewCIStr(h.HintUseIndex),
			Tables:   []ast.HintTable{{DBName: index.DBName, TableName: getTableName(index.Table.Name, index.TableAsName)}},
			Indexes:  []model.CIStr{index.Index.Name},
		})
		orderHint := h.HintOrderIndex
		if !index.KeepOrder {
			orderHint = h.HintNoOrderIndex
		}
		res = append(res, &ast.TableOptimizerHint{
			QBName:   qbName,
			HintName: model.NewCIStr(orderHint),
			Tables:   []ast.HintTable{{DBName: index.DBName, TableName: getTableName(index.Table.Name, index.TableAsName)}},
			Indexes:  []model.CIStr{index.Index.Name},
		})
	case *PhysicalIndexMergeReader:
		indexs := make([]model.CIStr, 0, 2)
		var tableName model.CIStr
		var tableAsName *model.CIStr
		for _, partialPlan := range pp.PartialPlans {
			if index, ok := partialPlan[0].(*PhysicalIndexScan); ok {
				indexs = append(indexs, index.Index.Name)
				tableName = index.Table.Name
				tableAsName = index.TableAsName
			} else {
				indexName := model.NewCIStr("PRIMARY")
				indexs = append(indexs, indexName)
			}
		}
		res = append(res, &ast.TableOptimizerHint{
			QBName:   qbName,
			HintName: model.NewCIStr(h.HintIndexMerge),
			Tables:   []ast.HintTable{{TableName: getTableName(tableName, tableAsName)}},
			Indexes:  indexs,
		})
	case *PhysicalHashAgg:
		res = append(res, &ast.TableOptimizerHint{
			QBName:   qbName,
			HintName: model.NewCIStr(h.HintHashAgg),
		})
		if storeType == kv.TiKV {
			res = append(res, &ast.TableOptimizerHint{
				QBName:   qbName,
				HintName: model.NewCIStr(h.HintAggToCop),
			})
		}
	case *PhysicalStreamAgg:
		res = append(res, &ast.TableOptimizerHint{
			QBName:   qbName,
			HintName: model.NewCIStr(h.HintStreamAgg),
		})
		if storeType == kv.TiKV {
			res = append(res, &ast.TableOptimizerHint{
				QBName:   qbName,
				HintName: model.NewCIStr(h.HintAggToCop),
			})
		}
	case *PhysicalMergeJoin:
		hint := genJoinMethodHintForSinglePhysicalJoin(
			p.SCtx(),
			h.HintSMJ,
			p.QueryBlockOffset(),
			nodeType,
			false,
			pp.Children()...,
		)
		if hint != nil {
			res = append(res, hint)
		}
	case *PhysicalHashJoin:
		// For semi join, hash_join_[build|probe] is not supported. See getHashJoins() for details.
		if pp.JoinType.IsSemiJoin() {
			hint := genJoinMethodHintForSinglePhysicalJoin(
				p.SCtx(),
				h.HintHJ,
				p.QueryBlockOffset(),
				nodeType,
				false,
				pp.Children()...,
			)
			if hint != nil {
				res = append(res, hint)
			}
			break
		}
		var buildSideChild, probeSideChild base.PhysicalPlan
		if pp.RightIsBuildSide() {
			buildSideChild = pp.Children()[1]
			probeSideChild = pp.Children()[0]
		} else {
			buildSideChild = pp.Children()[0]
			probeSideChild = pp.Children()[1]
		}
		hint := genJoinMethodHintForSinglePhysicalJoin(
			p.SCtx(),
			h.HintHashJoinBuild,
			p.QueryBlockOffset(),
			nodeType,
			true,
			buildSideChild,
			probeSideChild,
		)
		if hint != nil {
			res = append(res, hint)
		} else {
			// In case we failed to generate the hint for build side, we try to generate the hint for probe side.
			hint := genJoinMethodHintForSinglePhysicalJoin(
				p.SCtx(),
				h.HintHashJoinProbe,
				p.QueryBlockOffset(),
				nodeType,
				true,
				probeSideChild,
				buildSideChild,
			)
			if hint != nil {
				res = append(res, hint)
			}
		}
	case *PhysicalIndexJoin:
		hint := genJoinMethodHintForSinglePhysicalJoin(
			p.SCtx(),
			h.HintINLJ,
			p.QueryBlockOffset(),
			nodeType,
			true,
			pp.Children()[pp.InnerChildIdx],
			pp.Children()[1-pp.InnerChildIdx],
		)
		if hint != nil {
			res = append(res, hint)
		}
	case *PhysicalIndexMergeJoin:
		hint := genJoinMethodHintForSinglePhysicalJoin(
			p.SCtx(),
			h.HintINLMJ,
			p.QueryBlockOffset(),
			nodeType,
			true,
			pp.Children()[pp.InnerChildIdx],
			pp.Children()[1-pp.InnerChildIdx],
		)
		if hint != nil {
			res = append(res, hint)
		}
	case *PhysicalIndexHashJoin:
		hint := genJoinMethodHintForSinglePhysicalJoin(
			p.SCtx(),
			h.HintINLHJ,
			p.QueryBlockOffset(),
			nodeType,
			true,
			pp.Children()[pp.InnerChildIdx],
			pp.Children()[1-pp.InnerChildIdx],
		)
		if hint != nil {
			res = append(res, hint)
		}
	}
	return res
}

func getTableName(tblName model.CIStr, asName *model.CIStr) model.CIStr {
	if asName != nil && asName.L != "" {
		return *asName
	}
	return tblName
}

// genJoinMethodHintForSinglePhysicalJoin is the entry point of generating join method hint.
// It generates a join method hint for a single physical join operator according to the input joinType.
// Both children of the Join should be passed in as the children arguments, this is for correctly deriving the QB offset
// for the hint.
// For hints like merge_join(), we can generate hint using table name of any one of the two tables. But for hints like
// hash_join_build() and inl_join(), we want to generate hint using table name of a specific side. For this difference,
// we introduce the onlyFirstTbl argument. If onlyFirstTbl is true, we only try to generate hint using the table name of
// the children[0].
func genJoinMethodHintForSinglePhysicalJoin(
	sctx base.PlanContext,
	joinType string,
	parentQBOffset int,
	nodeType h.NodeType,
	onlyFirstTbl bool,
	children ...base.PhysicalPlan,
) *ast.TableOptimizerHint {
	if parentQBOffset == -1 {
		return nil
	}
	hintTbls, hintQBName, qbOffsets := genHintTblForJoinNodes(sctx, children, parentQBOffset, nodeType, false)
	effectiveHintTbls := slices.DeleteFunc(slices.Clone(hintTbls), func(ht *ast.HintTable) bool {
		return ht == nil
	})
	if len(effectiveHintTbls) == 0 {
		return nil
	}

	if onlyFirstTbl && hintTbls[0] == nil {
		return nil
	}
	if onlyFirstTbl && !hintTableIdentityIsUnique(hintTbls, qbOffsets, 0) {
		return nil
	}

	newHint := &ast.TableOptimizerHint{
		HintName: model.NewCIStr(joinType),
		Tables:   []ast.HintTable{*effectiveHintTbls[0]},
	}

	if hintQBName != nil {
		newHint.QBName = *hintQBName
	}
	// TODO: QBName missed out in CTE, see https://github.com/pingcap/tidb/issues/68925, should fix it in the future.

	return newHint
}

// genHintTblForJoinNodes tries to generate ast.HintTable for each join node, and the QB name for the hint itself.
// (Join node here means the operators that are joined, not Join operator itself)
// If the return values is not (nil,nil), len(hintTbls) should be equal to len(joinedNodes). The invalid ones in the
// returned hintTbls slice will be nil.
// The hintQBNamePtr will be nil if it's not needed, or we failed to generate one.
func genHintTblForJoinNodes(
	sctx base.PlanContext,
	joinedNodes []base.PhysicalPlan,
	parentQBOffset int,
	nodeType h.NodeType,
	allowQualifiedConcrete bool,
) (hintTbls []*ast.HintTable, hintQBNamePtr *model.CIStr, qbOffsets []int) {
	// 1. Use genHintTblForSingleJoinNode() to generate QB offset and table name for each join node.

	// Note that if we failed to generate valid information for one element in joinedNodes, we append -1 and nil instead
	// of skipping.
	// So qbOffsets[x] is -1 if and only if hintTbls[x] is nil;
	// and qbOffsets[x] >=0 if and only if hintTbls[x] is not nil.
	hintTbls = make([]*ast.HintTable, 0, len(joinedNodes))
	qbOffsets = make([]int, 0, len(joinedNodes))
	operandFacts := make([]plannerutil.OperandIdentityFacts, 0, len(joinedNodes))
	for _, plan := range joinedNodes {
		facts := plannerutil.ExtractOperandIdentityFacts(plan, parentQBOffset)
		operandFacts = append(operandFacts, facts)
		qbOffset, ht := genHintTblForSingleJoinNode(sctx, plan, parentQBOffset, allowQualifiedConcrete, facts)
		if qbOffset < 0 || ht == nil {
			qbOffsets = append(qbOffsets, -1)
			hintTbls = append(hintTbls, nil)
			continue
		}
		qbOffsets = append(qbOffsets, qbOffset)
		hintTbls = append(hintTbls, ht)
	}
	if allowQualifiedConcrete {
		disambiguateGeneratedLeadingIdentities(hintTbls, qbOffsets, operandFacts, joinedNodes, parentQBOffset)
	}

	// 2. Add QB name for each table name in the hint.

	for i, hintTbl := range hintTbls {
		if hintTbl == nil {
			continue
		}
		// In quick binding, we always put the generated hints in the first valid place in the SQL.
		// That implies hintname(@del_1) and hintname(@upd_1) is unnecessary in UPDATE/DELETE statements, and
		// hintname(@sel_1) is unnecessary in SELECT statements.
		// We don't generate QB name for the table names in the hint in this case to make the result cleaner.
		if (qbOffsets[i] <= 1 && nodeType == h.TypeSelect) ||
			(qbOffsets[i] == 0 && (nodeType == h.TypeUpdate || nodeType == h.TypeDelete)) {
			continue
		}
		tblQBName, err := h.GenerateQBName(nodeType, qbOffsets[i])
		if err != nil {
			continue
		}
		hintTbls[i].QBName = tblQBName
	}

	// 3. Generate QB name for the hint itself based on the QB name of each join node from step 1.

	// Current join reorder may change the QB offset of the join operator, e.g.
	// setting it to -1. The hint belongs to the query block that owns the join
	// group, so use the stable parentQBOffset captured before reorder instead of
	// inferring a query block from the reordered children.
	// Besides, genHintTblForSingleJoinNode() is not powerful enough to handle all cases, it may fail in some cases.
	// If we failed to get QB offset information from one join node, we don't generate QB name for the hint. Because
	// that may cause a wrong QB offset, leaving it blank is probably better.
	if slices.Contains(qbOffsets, -1) {
		return hintTbls, nil, qbOffsets
	}

	// ditto. We don't generate unnecessary QB name for the hint itself.
	// If generating the hint-level QB name fails, keep the already-computed
	// table names and drop only the hint-level QB name, mirroring the
	// qbOffsets=={-1} policy above rather than discarding the whole hint.
	if (parentQBOffset > 1 && nodeType == h.TypeSelect) ||
		(parentQBOffset > 0 && (nodeType == h.TypeUpdate || nodeType == h.TypeDelete)) {
		hintQBName, err := h.GenerateQBName(nodeType, parentQBOffset)
		if err != nil {
			return hintTbls, nil, qbOffsets
		}
		hintQBNamePtr = &hintQBName
	}
	return hintTbls, hintQBNamePtr, qbOffsets
}

type generatedHintTableIdentity struct {
	dbName, tableName string
	qbOffset          int
}

func generatedHintTableIdentityAt(hintTbls []*ast.HintTable, qbOffsets []int, index int) (generatedHintTableIdentity, bool) {
	if index < 0 || index >= len(hintTbls) || index >= len(qbOffsets) ||
		hintTbls[index] == nil || qbOffsets[index] < 0 {
		return generatedHintTableIdentity{}, false
	}
	return generatedHintTableIdentity{
		dbName: hintTbls[index].DBName.L, tableName: hintTbls[index].TableName.L, qbOffset: qbOffsets[index],
	}, true
}

func hintTableIdentityIsUnique(hintTbls []*ast.HintTable, qbOffsets []int, target int) bool {
	targetIdentity, ok := generatedHintTableIdentityAt(hintTbls, qbOffsets, target)
	if !ok {
		return false
	}
	for i := range hintTbls {
		identity, ok := generatedHintTableIdentityAt(hintTbls, qbOffsets, i)
		if ok && i != target && identity == targetIdentity {
			return false
		}
	}
	return true
}

func hintTableIdentitiesAreUnique(hintTbls []*ast.HintTable, qbOffsets []int) bool {
	seen := make(map[generatedHintTableIdentity]struct{}, len(hintTbls))
	for i := range hintTbls {
		identity, ok := generatedHintTableIdentityAt(hintTbls, qbOffsets, i)
		if !ok {
			continue
		}
		if _, exists := seen[identity]; exists {
			return false
		}
		seen[identity] = struct{}{}
	}
	return true
}

// warnAmbiguousLeadingIdentities explains why the join group got no LEADING
// hint: at least two operands resolve to the same owner-visible identity, so
// no operand list could replay the physical order unambiguously.
func warnAmbiguousLeadingIdentities(p PhysicalJoin, hintTbls []*ast.HintTable, qbOffsets []int) {
	identityCounts := make(map[generatedHintTableIdentity]int, len(hintTbls))
	for i := range hintTbls {
		if identity, ok := generatedHintTableIdentityAt(hintTbls, qbOffsets, i); ok {
			identityCounts[identity]++
		}
	}
	duplicated := make([]string, 0, 1)
	for identity, count := range identityCounts {
		if count > 1 {
			duplicated = append(duplicated, identity.dbName+"."+identity.tableName)
		}
	}
	slices.Sort(duplicated)
	p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(fmt.Sprintf(
		"leading hint is not generated because join operands have ambiguous visible identities: %s", duplicated))
}

func disambiguateGeneratedLeadingIdentities(
	hintTbls []*ast.HintTable,
	qbOffsets []int,
	operandFacts []plannerutil.OperandIdentityFacts,
	joinedNodes []base.PhysicalPlan,
	ownerQB int,
) {
	identityCounts := make(map[generatedHintTableIdentity]int, len(hintTbls))
	for i := range hintTbls {
		if identity, ok := generatedHintTableIdentityAt(hintTbls, qbOffsets, i); ok {
			identityCounts[identity]++
		}
	}
	type replacement struct {
		index, qbOffset int
		table           *ast.HintTable
	}
	replacements := make([]replacement, 0)
	for i := range hintTbls {
		identity, ok := generatedHintTableIdentityAt(hintTbls, qbOffsets, i)
		if !ok || identityCounts[identity] <= 1 {
			continue
		}
		qbOffset, table, ok := uniqueConcreteHintIdentity(operandFacts[i], joinedNodes[i], ownerQB)
		if !ok {
			return
		}
		replacements = append(replacements, replacement{index: i, qbOffset: qbOffset, table: table})
	}
	for _, replacement := range replacements {
		hintTbls[replacement.index] = replacement.table
		qbOffsets[replacement.index] = replacement.qbOffset
	}
}

func uniqueConcreteHintIdentity(facts plannerutil.OperandIdentityFacts, plan base.PhysicalPlan, ownerQB int) (int, *ast.HintTable, bool) {
	var concrete *plannerutil.IdentityOccurrence
	concreteQBs := make(map[int]struct{})
	for i := range facts.Occurrences {
		occurrence := &facts.Occurrences[i]
		if occurrence.Kind == plannerutil.ConcreteOccurrence && occurrence.Identity.TblName.L != "" {
			if concrete != nil {
				return -1, nil, false
			}
			concrete = occurrence
		}
		if occurrence.Kind == plannerutil.AliasCandidateOccurrence && occurrence.StartQB > 0 {
			concreteQBs[occurrence.StartQB] = struct{}{}
		}
	}
	if concrete != nil {
		if concrete.StartQB <= 0 || concrete.StartQB == ownerQB {
			return -1, nil, false
		}
		return concrete.StartQB, &ast.HintTable{DBName: concrete.Identity.DBName, TableName: concrete.Identity.TblName}, true
	}
	if len(concreteQBs) != 1 {
		return -1, nil, false
	}
	var sourceQB int
	for qb := range concreteQBs {
		sourceQB = qb
	}
	dbName, tableName := extractTableAsName(plan)
	if tableName == nil || tableName.L == "" {
		return -1, nil, false
	}
	if dbName == nil {
		dbName = &model.CIStr{}
	}
	return sourceQB, &ast.HintTable{DBName: *dbName, TableName: *tableName}, true
}

// genHintTblForSingleJoinNode resolves one physical operand in the frozen
// join-group owner query block.
func genHintTblForSingleJoinNode(
	sctx base.PlanContext,
	joinNode base.PhysicalPlan,
	parentOffset int,
	allowQualifiedConcrete bool,
	facts plannerutil.OperandIdentityFacts,
) (
	qbOffset int,
	ht *ast.HintTable,
) {
	if sctx == nil || parentOffset < 0 {
		return -1, nil
	}
	table, ok := facts.Owner.Identity, facts.Owner.Kind == plannerutil.OwnerUnique
	if !ok {
		hasDerivedAliasCandidate := false
		for _, occurrence := range facts.Occurrences {
			if occurrence.Kind == plannerutil.AliasCandidateOccurrence {
				hasDerivedAliasCandidate = true
				break
			}
		}
		if hasDerivedAliasCandidate || (joinNode.QueryBlockOffset() != parentOffset && !allowQualifiedConcrete) {
			return -1, nil
		}
		dbName, tableName := extractTableAsName(joinNode)
		if tableName == nil || tableName.L == "" {
			return -1, nil
		}
		if dbName == nil {
			dbName = &model.CIStr{}
		}
		qbOffset := joinNode.QueryBlockOffset()
		if qbOffset <= 0 {
			qbOffset = parentOffset
		}
		return qbOffset, &ast.HintTable{DBName: *dbName, TableName: *tableName}
	}
	return table.SelectOffset, &ast.HintTable{DBName: table.DBName, TableName: table.TblName}
}

func extractTableAsName(p base.PhysicalPlan) (*model.CIStr, *model.CIStr) {
	if p == nil {
		return nil, nil
	}
	if len(p.Children()) > 1 {
		return nil, nil
	}
	switch x := p.(type) {
	case *PhysicalTableReader:
		if len(x.TablePlans) == 0 {
			return nil, nil
		}
		ts, ok := x.TablePlans[0].(*PhysicalTableScan)
		if !ok || ts == nil {
			return nil, nil
		}
		if ts.TableAsName != nil && ts.TableAsName.L != "" {
			return &ts.DBName, ts.TableAsName
		}
		return &ts.DBName, &ts.Table.Name
	case *PhysicalIndexReader:
		if len(x.IndexPlans) == 0 {
			return nil, nil
		}
		is, ok := x.IndexPlans[0].(*PhysicalIndexScan)
		if !ok || is == nil {
			return nil, nil
		}
		if is.TableAsName != nil && is.TableAsName.L != "" {
			return &is.DBName, is.TableAsName
		}
		return &is.DBName, &is.Table.Name
	case *PhysicalIndexLookUpReader:
		if len(x.IndexPlans) == 0 {
			return nil, nil
		}
		is, ok := x.IndexPlans[0].(*PhysicalIndexScan)
		if !ok || is == nil {
			return nil, nil
		}
		if is.TableAsName != nil && is.TableAsName.L != "" {
			return &is.DBName, is.TableAsName
		}
		return &is.DBName, &is.Table.Name
	case *PhysicalSort, *PhysicalSelection, *PhysicalUnionScan, *PhysicalProjection,
		*PhysicalHashAgg, *PhysicalStreamAgg:
		children := p.Children()
		if len(children) != 1 {
			return nil, nil
		}
		return extractTableAsName(children[0])
	}
	return nil, nil
}

// genJoinOrderHintFromRootPhysicalJoin is the entry point of generating join order hint.
// warnAmbiguousLeading controls whether dropping the LEADING hint for a join
// group with ambiguous operand identities appends a statement warning; only
// the EXPLAIN FORMAT='hint' entry point (GenHintsFromFlatPlanForExplain)
// enables it, so unhinted statements never carry hint-generation diagnostics.
func genJoinOrderHintFromRootPhysicalJoin(
	p PhysicalJoin,
	visitedIDs map[int]struct{},
	nodeType h.NodeType,
	warnAmbiguousLeading bool,
) *ast.TableOptimizerHint {
	if _, visited := visitedIDs[p.ID()]; visited {
		return nil
	}

	// 1. Get the joined operators in this join group with correct order in the slice.
	// Only mark a join group as visited after we successfully emit a LEADING hint for it.
	// Otherwise a larger unsupported/partially-unmappable group would suppress smaller valid sub-groups.
	localVisitedIDs := make(map[int]struct{})
	orderedJoinGroup := extractOrderedPhysicalJoinGroup(p, localVisitedIDs, 1)
	// If it only involves two tables, we don't need to generate the join order hint.
	if len(orderedJoinGroup) <= 2 {
		return nil
	}

	// 2. Generate the leading hint based on the ordered join nodes.
	hintTbls, hintQBName, qbOffsets := genHintTblForJoinNodes(p.SCtx(), orderedJoinGroup, p.QueryBlockOffset(), nodeType, true)

	// For now, we generate the leading hint only if we successfully generate the names for all nodes.
	if slices.Contains(hintTbls, nil) {
		return nil
	}
	if len(hintTbls) <= 2 {
		return nil
	}
	// LEADING must identify every operand in the ordered join group. If two
	// operands have the same canonical owner-visible identity, no generated
	// list can replay the physical order unambiguously, so no LEADING is
	// emitted. Warn only when the caller serves EXPLAIN FORMAT='hint': this
	// generator also runs after ordinary statements for statements_summary
	// plan-hint capture, and warning there made plain view queries surface
	// "leading hint is not generated" in their SHOW WARNINGS.
	if !hintTableIdentitiesAreUnique(hintTbls, qbOffsets) {
		if warnAmbiguousLeading {
			warnAmbiguousLeadingIdentities(p, hintTbls, qbOffsets)
		}
		return nil
	}
	for id := range localVisitedIDs {
		visitedIDs[id] = struct{}{}
	}

	// The generated LEADING is stored in ONE hint with TWO views over ONE
	// backing array: Tables holds the values, and
	// HintData.(*ast.LeadingList).Items holds pointers into that same array,
	// so an in-place change through either view is immediately visible in the
	// other. The array is fixed-length by construction — never append to
	// hintTblVals after Items has taken addresses (&hintTblVals[i] would
	// dangle across a reallocation). These objects are built once here and
	// only rendered afterwards (EXPLAIN FORMAT='hint', statements_summary,
	// baseline capture); nothing mutates them in between.
	hintTblVals := make([]ast.HintTable, len(hintTbls))
	leadingItems := make([]any, len(hintTbls))
	for i, ht := range hintTbls {
		hintTblVals[i] = *ht
		leadingItems[i] = &hintTblVals[i]
	}
	res := &ast.TableOptimizerHint{
		HintName: model.NewCIStr(h.HintLeading),
		Tables:   hintTblVals,
		HintData: &ast.LeadingList{Items: leadingItems},
	}
	if hintQBName != nil {
		res.QBName = *hintQBName
	}
	return res
}

func extractOrderedPhysicalJoinGroup(p PhysicalJoin, visitedIDs map[int]struct{}, depth uint) []base.PhysicalPlan {
	visitedIDs[p.ID()] = struct{}{}

	// 1. sanity checks

	// In our join reorder implementation, cartesian join will break the join relationship and make its two children
	// two independent join groups. So we don't need to handle it here.
	// Currently, index joins must match the index or PK of the inner table, so cartesian join must be a hash join.
	if hashJoin, ok := p.(*PhysicalHashJoin); ok {
		if len(hashJoin.EqualConditions) == 0 && len(hashJoin.NAEqualConditions) == 0 {
			return nil
		}
	}

	jt := p.GetJoinType()
	// They are the only join types supported by current join reorder.
	if jt != logicalop.InnerJoin && jt != logicalop.LeftOuterJoin && jt != logicalop.RightOuterJoin {
		return nil
	}

	// 2. Extract information from children according to whether the child is another Join, then construct the ordered
	// join group and return.

	var child0IsJoin, child1IsJoin bool
	var childJoin PhysicalJoin
	var childJoinGroup []base.PhysicalPlan
	if childJoin, child0IsJoin = unwrapJoinGroupChild(p.Children()[0]); child0IsJoin {
		childJoinGroup = extractOrderedPhysicalJoinGroup(childJoin, visitedIDs, depth+1)
	}
	if childJoin, child1IsJoin = unwrapJoinGroupChild(p.Children()[1]); child1IsJoin {
		childJoinGroup = extractOrderedPhysicalJoinGroup(childJoin, visitedIDs, depth+1)
	}

	// case 1 - bushy join: not supported now, also should not appear now
	if child0IsJoin && child1IsJoin {
		return nil
	}
	// case 2 - leaf join operator: initialize the join group with the two children
	if !child0IsJoin && !child1IsJoin {
		// preallocate the slice based on the number of join operators to avoid reallocations
		orderedJoinGroup := make([]base.PhysicalPlan, 0, depth+1)
		orderedJoinGroup = append(orderedJoinGroup, p.Children()[0], p.Children()[1])
		return orderedJoinGroup
	}
	// case 3 - non-leaf join operator: append the non-join child to the join group from the Join child
	if len(childJoinGroup) < 2 {
		return nil
	}
	var orderedJoinGroup []base.PhysicalPlan
	if child0IsJoin {
		orderedJoinGroup = append(childJoinGroup, p.Children()[1])
	} else {
		orderedJoinGroup = append(childJoinGroup, p.Children()[0])
	}
	return orderedJoinGroup
}

func unwrapJoinGroupChild(plan base.PhysicalPlan) (PhysicalJoin, bool) {
	for {
		if join, ok := plan.(PhysicalJoin); ok {
			return join, true
		}
		if len(plan.Children()) != 1 {
			return nil, false
		}
		switch plan.(type) {
		case *PhysicalProjection, *PhysicalSelection:
			plan = plan.Children()[0]
		default:
			return nil, false
		}
	}
}
