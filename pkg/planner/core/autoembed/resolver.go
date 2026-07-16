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

package autoembed

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/types"
)

type autoEmbedResolveKey struct {
	plan base.LogicalPlan
	col  *expression.Column
}

// autoEmbedResolveResult represents the three namespace/proof states described
// in the package documentation.
type autoEmbedResolveResult struct {
	// found distinguishes "outside this plan's column namespace" from "the
	// column is present, but its auto-embedding provenance is not provable".
	found bool
	info  *expression.AutoEmbedInfo
}

type autoEmbedResolveState uint8

const (
	autoEmbedResolving autoEmbedResolveState = iota + 1
	autoEmbedResolved
)

// autoEmbedMissingColumnPolicy controls whether an explicitly supported unary
// wrapper may search a child Join's hidden qualified-column namespace.
type autoEmbedMissingColumnPolicy uint8

const (
	autoEmbedRequireOutputColumn autoEmbedMissingColumnPolicy = iota
	autoEmbedSearchChildNamespace
)

// autoEmbedResolver owns the memo and cycle state for one consumer lookup.
type autoEmbedResolver struct {
	state   map[autoEmbedResolveKey]autoEmbedResolveState
	memo    map[autoEmbedResolveKey]autoEmbedResolveResult
	sidecar DualSourceLookup
}

// resolve memoizes (plan, column) pairs for one consumer lookup. Encountering
// an active pair means malformed or recursive lineage; it claims the namespace
// without metadata to stop both recursion and fallback.
func (r *autoEmbedResolver) resolve(plan base.LogicalPlan, target *expression.Column) autoEmbedResolveResult {
	if plan == nil || target == nil || target.RetType == nil {
		return autoEmbedResolveResult{}
	}
	key := autoEmbedResolveKey{plan: plan, col: target}
	switch r.state[key] {
	case autoEmbedResolving:
		return autoEmbedResolveResult{found: true}
	case autoEmbedResolved:
		return r.memo[key]
	}
	r.state[key] = autoEmbedResolving
	result := r.resolvePlan(plan, target)
	r.state[key] = autoEmbedResolved
	r.memo[key] = result
	return result
}

// resolvePlan applies the closed-world propagation policy for one logical
// operator.
func (r *autoEmbedResolver) resolvePlan(plan base.LogicalPlan, target *expression.Column) autoEmbedResolveResult {
	// Keep this switch closed. Transparent operators must identify the exact
	// child value; derived operators must validate the producing expression;
	// multi-source operators require consensus. A new operator fails closed
	// until one of those contracts is implemented explicitly.
	switch p := plan.(type) {
	case *logicalop.DataSource:
		return resolveDataSourceAutoEmbedInfo(p, target)
	case *logicalop.LogicalProjection:
		return r.resolveProjection(p, target)
	case *logicalop.LogicalSelection,
		*logicalop.LogicalSort,
		*logicalop.LogicalTopN,
		*logicalop.LogicalLimit,
		*logicalop.LogicalLock,
		*logicalop.LogicalUnionScan:
		return r.resolveUnaryPassthrough(plan, target, autoEmbedSearchChildNamespace)
	case *logicalop.LogicalMaxOneRow:
		// MaxOneRow clones its child schema and clears NotNullFlag. Pointer
		// identity cannot survive, so this path intentionally matches by the
		// unique column ID with NotNull-only type compatibility.
		return r.resolveUnaryPassthrough(plan, target, autoEmbedSearchChildNamespace)
	case *logicalop.LogicalWindow:
		return r.resolveUnaryPassthrough(plan, target, autoEmbedRequireOutputColumn)
	case *logicalop.LogicalAggregation:
		return r.resolveAggregation(p, target)
	case *logicalop.LogicalUnionAll:
		return r.resolveUnion(plan, target)
	case *logicalop.LogicalPartitionUnionAll:
		return r.resolveUnion(plan, target)
	case *logicalop.LogicalJoin:
		return r.resolveJoin(p, target, false)
	case *logicalop.LogicalApply:
		return r.resolveJoin(&p.LogicalJoin, target, true)
	case *logicalop.LogicalSequence:
		children := p.Children()
		if len(children) == 0 {
			return autoEmbedResolveResult{}
		}
		return r.resolve(children[len(children)-1], target)
	case *logicalop.LogicalCTE:
		return r.resolveCTE(p, target)
	case *logicalop.LogicalTableDual:
		return r.resolveTableDual(p, target)
	case *logicalop.LogicalExpand:
		return autoEmbedResolveResult{found: autoEmbedPlanSchemaContains(plan, target)}
	default:
		// GeneratedExprString used to survive arbitrary Column.Clone calls.
		// Unknown operators intentionally fail closed instead of treating an
		// incidental clone as proof of auto-embedding provenance.
		return autoEmbedResolveResult{found: autoEmbedPlanSchemaContains(plan, target)}
	}
}

// resolveTableDual first claims only the Dual's own namespace, then consults
// an exact-pointer builder sidecar for a discarded build-time input. Once the
// Dual claims the column, source failure must not fall through to another
// namespace such as an INSERT target or join sibling.
func (r *autoEmbedResolver) resolveTableDual(p *logicalop.LogicalTableDual, target *expression.Column) autoEmbedResolveResult {
	matched, _, found, ambiguous := findAutoEmbedColumn(p.Schema(), target)
	if ambiguous {
		return autoEmbedResolveResult{found: true}
	}
	if !found {
		return autoEmbedResolveResult{}
	}
	if p.RowCount != 0 || r.sidecar == nil {
		return autoEmbedResolveResult{found: true}
	}
	source, ok := r.sidecar.SourceOfEmptyDual(p)
	if !ok || source == nil {
		return autoEmbedResolveResult{found: true}
	}
	result := r.resolve(source, matched)
	result.found = true
	return result
}

// resolveProjection accepts only a bare Column expression at the matching
// output ordinal. Casts and scalar functions are derived values even when the
// output column copied a source UniqueID.
func (r *autoEmbedResolver) resolveProjection(p *logicalop.LogicalProjection, target *expression.Column) autoEmbedResolveResult {
	if p == nil || len(p.Children()) != 1 || p.Schema() == nil {
		return autoEmbedResolveResult{}
	}
	_, idx, found, ambiguous := findAutoEmbedColumn(p.Schema(), target)
	if ambiguous {
		return autoEmbedResolveResult{found: true}
	}
	if !found {
		return autoEmbedResolveResult{}
	}
	if idx >= len(p.Exprs) {
		return autoEmbedResolveResult{found: true}
	}
	source, ok := p.Exprs[idx].(*expression.Column)
	if !ok || !autoEmbedVectorTypesCompatible(target, source) {
		return autoEmbedResolveResult{found: true}
	}
	result := r.resolve(p.Children()[0], source)
	result.found = true
	return result
}

// resolveUnaryPassthrough handles explicitly supported unary wrappers. Some
// wrappers omit a qualified redundant USING/NATURAL column from their public
// schema while the child Join.FullSchema still owns it. Window must require an
// output match so a window result cannot be mistaken for a passthrough column.
func (r *autoEmbedResolver) resolveUnaryPassthrough(
	plan base.LogicalPlan,
	target *expression.Column,
	missingColumnPolicy autoEmbedMissingColumnPolicy,
) autoEmbedResolveResult {
	children := plan.Children()
	if len(children) != 1 {
		return autoEmbedResolveResult{}
	}
	matched, _, found, ambiguous := findAutoEmbedColumn(plan.Schema(), target)
	if ambiguous {
		return autoEmbedResolveResult{found: true}
	}
	if !found {
		if missingColumnPolicy == autoEmbedRequireOutputColumn {
			return autoEmbedResolveResult{}
		}
		return r.resolve(children[0], target)
	}
	result := r.resolve(children[0], matched)
	result.found = true
	return result
}

// resolveAggregation accepts the FIRST_ROW(Column) representation used for
// GROUP BY and DISTINCT passthrough values. Other aggregate expressions are
// derived. An aggregation directly above Union is conservatively rejected
// because that shape is also used for UNION DISTINCT.
func (r *autoEmbedResolver) resolveAggregation(p *logicalop.LogicalAggregation, target *expression.Column) autoEmbedResolveResult {
	if p == nil || len(p.Children()) != 1 || p.Schema() == nil {
		return autoEmbedResolveResult{}
	}
	_, idx, found, ambiguous := findAutoEmbedColumn(p.Schema(), target)
	if ambiguous {
		return autoEmbedResolveResult{found: true}
	}
	if !found {
		return autoEmbedResolveResult{}
	}
	if autoEmbedAggregationFollowsUnion(p) {
		return autoEmbedResolveResult{found: true}
	}
	if idx >= len(p.AggFuncs) || p.AggFuncs[idx] == nil ||
		p.AggFuncs[idx].Name != ast.AggFuncFirstRow || len(p.AggFuncs[idx].Args) != 1 {
		return autoEmbedResolveResult{found: true}
	}
	source, ok := p.AggFuncs[idx].Args[0].(*expression.Column)
	if !ok || !autoEmbedVectorTypesCompatible(target, source) {
		return autoEmbedResolveResult{found: true}
	}
	result := r.resolve(p.Children()[0], source)
	result.found = true
	return result
}

// resolveUnion requires every branch at the output ordinal to prove compatible
// vector types and byte-equivalent model/options. One absent, derived, or
// conflicting branch invalidates the whole output.
func (r *autoEmbedResolver) resolveUnion(plan base.LogicalPlan, target *expression.Column) autoEmbedResolveResult {
	children := plan.Children()
	if len(children) == 0 || plan.Schema() == nil {
		return autoEmbedResolveResult{}
	}
	_, idx, found, ambiguous := findAutoEmbedColumn(plan.Schema(), target)
	if ambiguous {
		return autoEmbedResolveResult{found: true}
	}
	if !found {
		return autoEmbedResolveResult{}
	}
	var merged *expression.AutoEmbedInfo
	for _, child := range children {
		if child == nil || child.Schema() == nil || idx >= child.Schema().Len() {
			return autoEmbedResolveResult{found: true}
		}
		source := child.Schema().Columns[idx]
		if !autoEmbedVectorTypesCompatible(target, source) {
			return autoEmbedResolveResult{found: true}
		}
		result := r.resolve(child, source)
		if !result.found || result.info == nil {
			return autoEmbedResolveResult{found: true}
		}
		if merged == nil {
			merged = result.info
		} else if !merged.Equal(result.info) {
			return autoEmbedResolveResult{found: true}
		}
	}
	return autoEmbedResolveResult{found: true, info: copyAutoEmbedInfo(merged)}
}

// resolveJoin requires consensus for a USING/NATURAL coalescence group; other
// outputs must belong to exactly one child. Apply may search only its outer
// child when the Apply itself does not expose the target, because an outer
// reference can remain below a scalar-subquery wrapper.
func (r *autoEmbedResolver) resolveJoin(p *logicalop.LogicalJoin, target *expression.Column, isApply bool) autoEmbedResolveResult {
	if p == nil || len(p.Children()) != 2 {
		return autoEmbedResolveResult{}
	}
	matched, found, ambiguous := findAutoEmbedJoinColumn(p, target)
	if ambiguous {
		return autoEmbedResolveResult{found: true}
	}
	if !found {
		if isApply {
			return r.resolve(p.Children()[0], target)
		}
		return autoEmbedResolveResult{}
	}
	if p.FromSetOperator {
		return autoEmbedResolveResult{found: true}
	}

	pairs := collectAutoEmbedJoinPairs(p)
	var pairSources []*expression.Column
	for _, pair := range pairs {
		if autoEmbedColumnsMatch(pair[0], matched) || autoEmbedColumnsMatch(pair[1], matched) {
			pairSources = appendAutoEmbedColumnOnce(pairSources, pair[0])
			pairSources = appendAutoEmbedColumnOnce(pairSources, pair[1])
		}
	}
	if len(pairSources) > 0 {
		return r.resolveJoinConsensus(p, matched, pairSources)
	}
	if autoEmbedJoinTargetTouchesUnresolvedRedundant(p, matched, pairs) {
		return autoEmbedResolveResult{found: true}
	}

	var sourceResult autoEmbedResolveResult
	foundSources := 0
	for _, child := range p.Children() {
		result := r.resolve(child, matched)
		if !result.found {
			continue
		}
		foundSources++
		sourceResult = result
	}
	if foundSources != 1 {
		return autoEmbedResolveResult{found: true}
	}
	sourceResult.found = true
	return sourceResult
}

// autoEmbedAggregationFollowsUnion identifies the conservative rejection shape
// shared by UNION DISTINCT and an explicit aggregation above UNION ALL.
func autoEmbedAggregationFollowsUnion(p *logicalop.LogicalAggregation) bool {
	if p == nil || len(p.Children()) != 1 {
		return false
	}
	switch p.Children()[0].(type) {
	case *logicalop.LogicalUnionAll, *logicalop.LogicalPartitionUnionAll:
		return true
	default:
		return false
	}
}

// resolveJoinConsensus requires every member of a coalesced Join output to
// resolve from exactly one child with the same metadata and compatible type.
func (r *autoEmbedResolver) resolveJoinConsensus(
	p *logicalop.LogicalJoin,
	target *expression.Column,
	sources []*expression.Column,
) autoEmbedResolveResult {
	var merged *expression.AutoEmbedInfo
	for _, source := range sources {
		if !autoEmbedVectorTypesCompatible(target, source) {
			return autoEmbedResolveResult{found: true}
		}
		var sourceResult autoEmbedResolveResult
		foundSources := 0
		for _, child := range p.Children() {
			result := r.resolve(child, source)
			if !result.found {
				continue
			}
			foundSources++
			sourceResult = result
		}
		if foundSources != 1 || sourceResult.info == nil {
			return autoEmbedResolveResult{found: true}
		}
		if merged == nil {
			merged = sourceResult.info
		} else if !merged.Equal(sourceResult.info) {
			return autoEmbedResolveResult{found: true}
		}
	}
	return autoEmbedResolveResult{found: true, info: copyAutoEmbedInfo(merged)}
}

// resolveCTE maps a non-recursive CTE output position to its seed plan.
// Recursive CTEs can combine values across iterations and are unprovable.
func (r *autoEmbedResolver) resolveCTE(p *logicalop.LogicalCTE, target *expression.Column) autoEmbedResolveResult {
	if p == nil || p.Schema() == nil || p.Cte == nil ||
		p.Cte.RecursivePartLogicalPlan != nil || p.Cte.SeedPartLogicalPlan == nil {
		return autoEmbedResolveResult{found: p != nil && autoEmbedPlanSchemaContains(p, target)}
	}
	_, idx, found, ambiguous := findAutoEmbedColumn(p.Schema(), target)
	if ambiguous {
		return autoEmbedResolveResult{found: true}
	}
	if !found {
		return autoEmbedResolveResult{}
	}
	seed := p.Cte.SeedPartLogicalPlan
	if seed.Schema() == nil || idx >= seed.Schema().Len() {
		return autoEmbedResolveResult{found: true}
	}
	source := seed.Schema().Columns[idx]
	if !autoEmbedVectorTypesCompatible(target, source) {
		return autoEmbedResolveResult{found: true}
	}
	result := r.resolve(seed, source)
	result.found = true
	return result
}

// collectAutoEmbedJoinPairs reconstructs USING/NATURAL equivalence groups in
// two passes. The registered redundant-column map is authoritative when it is
// complete. UPDATE/DELETE can omit that map, so the fallback accepts only a
// redundant FullSchema column connected by an equality condition to one
// same-named column on the opposite child. Every structural check is required
// before consensus may use the pair.
func collectAutoEmbedJoinPairs(p *logicalop.LogicalJoin) [][2]*expression.Column {
	if p == nil || p.FullSchema == nil || len(p.FullNames) != p.FullSchema.Len() {
		return nil
	}
	pairs := make([][2]*expression.Column, 0, len(p.RedundantColsToOutputIdx))
	for redundantID, visibleIdx := range p.RedundantColsToOutputIdx {
		if p.Schema() == nil || visibleIdx < 0 || visibleIdx >= p.Schema().Len() {
			continue
		}
		visible := p.Schema().Columns[visibleIdx]
		redundant := findAutoEmbedColumnByID(p.FullSchema, redundantID)
		if redundant == nil || autoEmbedJoinColumnIsRedundant(p, visible) ||
			!autoEmbedJoinColumnIsRedundant(p, redundant) ||
			!autoEmbedJoinColumnsOnOppositeSides(p, visible, redundant) ||
			!autoEmbedJoinColumnsHaveSameName(p, visible, redundant) {
			continue
		}
		pairs = appendAutoEmbedPairOnce(pairs, [2]*expression.Column{visible, redundant})
	}

	for idx, name := range p.FullNames {
		// UPDATE/DELETE intentionally omit the index map after restoring a
		// merged schema. Redundant is the safety gate for reconstructing only
		// USING/NATURAL pairs from their equality condition.
		if name == nil || !name.Redundant {
			continue
		}
		redundant := p.FullSchema.Columns[idx]
		if autoEmbedPairContainsColumn(pairs, redundant) {
			continue
		}
		var canonical *expression.Column
		for _, condition := range autoEmbedJoinEqualConditions(p) {
			args := condition.GetArgs()
			if len(args) != 2 {
				continue
			}
			left, leftOK := args[0].(*expression.Column)
			right, rightOK := args[1].(*expression.Column)
			if !leftOK || !rightOK {
				continue
			}
			var candidate *expression.Column
			switch {
			case autoEmbedColumnsMatch(left, redundant):
				candidate = right
			case autoEmbedColumnsMatch(right, redundant):
				candidate = left
			default:
				continue
			}
			if autoEmbedJoinColumnsOnOppositeSides(p, redundant, candidate) &&
				autoEmbedJoinColumnsHaveSameName(p, redundant, candidate) {
				if canonical != nil && !autoEmbedColumnsMatch(canonical, candidate) {
					canonical = nil
					break
				}
				canonical = candidate
			}
		}
		if canonical != nil {
			pairs = appendAutoEmbedPairOnce(pairs, [2]*expression.Column{canonical, redundant})
		}
	}
	return pairs
}

func autoEmbedJoinEqualConditions(p *logicalop.LogicalJoin) []*expression.ScalarFunction {
	conditions := make([]*expression.ScalarFunction, 0, len(p.EqualConditions)+len(p.OtherConditions))
	conditions = append(conditions, p.EqualConditions...)
	for _, condition := range p.OtherConditions {
		sf, ok := condition.(*expression.ScalarFunction)
		if ok && sf.FuncName.L == ast.EQ {
			conditions = append(conditions, sf)
		}
	}
	return conditions
}

func findAutoEmbedJoinColumn(p *logicalop.LogicalJoin, target *expression.Column) (matched *expression.Column, found bool, ambiguous bool) {
	if p == nil {
		return nil, false, false
	}
	if matched, _, found, ambiguous = findAutoEmbedColumn(p.Schema(), target); found || ambiguous {
		return matched, found, ambiguous
	}
	matched, _, found, ambiguous = findAutoEmbedColumn(p.FullSchema, target)
	return matched, found, ambiguous
}

// autoEmbedJoinTargetTouchesUnresolvedRedundant detects a target whose
// USING/NATURAL group could not be reconstructed. Claiming that target without
// metadata prevents normal single-child lookup from accepting one group side.
func autoEmbedJoinTargetTouchesUnresolvedRedundant(
	p *logicalop.LogicalJoin,
	target *expression.Column,
	pairs [][2]*expression.Column,
) bool {
	if autoEmbedPairContainsColumn(pairs, target) {
		return false
	}
	if autoEmbedJoinColumnIsRedundant(p, target) {
		return true
	}
	targetName := autoEmbedJoinColumnName(p, target)
	if targetName != nil {
		for idx, name := range p.FullNames {
			if name == nil || !name.Redundant || name.ColName.L != targetName.ColName.L {
				continue
			}
			if autoEmbedJoinColumnsOnOppositeSides(p, target, p.FullSchema.Columns[idx]) {
				return true
			}
		}
	}
	for _, condition := range autoEmbedJoinEqualConditions(p) {
		args := condition.GetArgs()
		if len(args) != 2 {
			continue
		}
		left, leftOK := args[0].(*expression.Column)
		right, rightOK := args[1].(*expression.Column)
		if !leftOK || !rightOK {
			continue
		}
		if autoEmbedColumnsMatch(left, target) && autoEmbedJoinColumnIsRedundant(p, right) ||
			autoEmbedColumnsMatch(right, target) && autoEmbedJoinColumnIsRedundant(p, left) {
			return true
		}
	}
	return false
}

func autoEmbedJoinColumnsOnOppositeSides(p *logicalop.LogicalJoin, left, right *expression.Column) bool {
	leftSide := autoEmbedJoinColumnSide(p, left)
	rightSide := autoEmbedJoinColumnSide(p, right)
	return leftSide >= 0 && rightSide >= 0 && leftSide != rightSide
}

func autoEmbedJoinColumnSide(p *logicalop.LogicalJoin, col *expression.Column) int {
	matchedSide := -1
	for idx, child := range p.Children() {
		if !autoEmbedPlanDirectNamespaceContains(child, col) {
			continue
		}
		if matchedSide >= 0 {
			return -1
		}
		matchedSide = idx
	}
	return matchedSide
}

func autoEmbedJoinColumnsHaveSameName(p *logicalop.LogicalJoin, left, right *expression.Column) bool {
	leftName := autoEmbedJoinColumnName(p, left)
	rightName := autoEmbedJoinColumnName(p, right)
	return leftName != nil && rightName != nil && leftName.ColName.L == rightName.ColName.L
}

func autoEmbedJoinColumnName(p *logicalop.LogicalJoin, col *expression.Column) *types.FieldName {
	if p == nil || p.FullSchema == nil || len(p.FullNames) != p.FullSchema.Len() {
		return nil
	}
	_, idx, found, ambiguous := findAutoEmbedColumn(p.FullSchema, col)
	if !found || ambiguous || idx >= len(p.FullNames) {
		return nil
	}
	return p.FullNames[idx]
}

func autoEmbedJoinColumnIsRedundant(p *logicalop.LogicalJoin, col *expression.Column) bool {
	name := autoEmbedJoinColumnName(p, col)
	return name != nil && name.Redundant
}

func autoEmbedPlanSchemaContains(plan base.LogicalPlan, target *expression.Column) bool {
	if plan == nil {
		return false
	}
	return autoEmbedPlanDirectNamespaceContains(plan, target)
}

func autoEmbedPlanDirectNamespaceContains(plan base.LogicalPlan, target *expression.Column) bool {
	if plan == nil || target == nil {
		return false
	}
	if _, _, found, ambiguous := findAutoEmbedColumn(plan.Schema(), target); found || ambiguous {
		return true
	}
	switch p := plan.(type) {
	case *logicalop.LogicalJoin:
		_, _, found, ambiguous := findAutoEmbedColumn(p.FullSchema, target)
		return found || ambiguous
	case *logicalop.LogicalApply:
		_, _, found, ambiguous := findAutoEmbedColumn(p.FullSchema, target)
		return found || ambiguous
	default:
		return false
	}
}

func appendAutoEmbedColumnOnce(columns []*expression.Column, col *expression.Column) []*expression.Column {
	for _, existing := range columns {
		if autoEmbedColumnsMatch(existing, col) {
			return columns
		}
	}
	return append(columns, col)
}

func appendAutoEmbedPairOnce(pairs [][2]*expression.Column, pair [2]*expression.Column) [][2]*expression.Column {
	for _, existing := range pairs {
		if autoEmbedColumnsMatch(existing[0], pair[0]) && autoEmbedColumnsMatch(existing[1], pair[1]) ||
			autoEmbedColumnsMatch(existing[0], pair[1]) && autoEmbedColumnsMatch(existing[1], pair[0]) {
			return pairs
		}
	}
	return append(pairs, pair)
}

func autoEmbedPairContainsColumn(pairs [][2]*expression.Column, col *expression.Column) bool {
	for _, pair := range pairs {
		if autoEmbedColumnsMatch(pair[0], col) || autoEmbedColumnsMatch(pair[1], col) {
			return true
		}
	}
	return false
}
