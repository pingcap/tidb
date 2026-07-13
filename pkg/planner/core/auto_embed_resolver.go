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
	"reflect"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
)

// Auto-embedding provenance resolution
//
// VEC_EMBED_* needs the model and options from the first argument's generated
// EMBED_TEXT definition. Carrying that definition on expression.Column made an
// arbitrary Column.Clone look like valid provenance. The resolver therefore
// starts at the VEC_EMBED_* consumer and proves the logical value path back to
// one or more catalog columns.
//
// A transparent alias follows one path and can be rewritten:
//
//	CREATE TABLE docs (
//	  text TEXT,
//	  vec VECTOR(3) GENERATED ALWAYS AS
//	    (EMBED_TEXT('mock/json', text, '{"plus":0.2}')) STORED
//	);
//	SELECT VEC_EMBED_L2_DISTANCE(v, 'hello')
//	FROM (SELECT vec AS v FROM docs) AS p;
//
// The proof is Projection(v <- docs.vec) -> DataSource(docs.vec). The rewriter
// produces VEC_L2_DISTANCE(v, EMBED_TEXT('mock/json', 'hello',
// '{"plus":0.2}')).
//
// A coalesced value must prove every possible source. For example,
//
//	SELECT VEC_EMBED_L2_DISTANCE(vec, 'hello')
//	FROM docs JOIN plain_vectors USING (vec);
//
// is rejected when plain_vectors.vec is not the same auto-embedding column.
// The Join owns vec, but its sources do not reach consensus. This distinction
// is represented by three result states: outside the current plan namespace,
// inside without a proof, and inside with AutoEmbedInfo. The middle state stops
// lookup from falling through to docs and incorrectly accepting one Join side.
//
// INSERT ... SELECT is resolved after its source has been optimized. Because
// optimization may replace an empty source with TableDual, the resolver records
// a statement-local snapshot before optimization and rebinds ON DUPLICATE
// columns to that snapshot by UniqueID and compatible vector type.

type autoEmbedResolveKey struct {
	plan base.LogicalPlan
	col  *expression.Column
}

// autoEmbedResolveResult represents the three namespace/proof states described
// in the architecture comment above.
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
	state map[autoEmbedResolveKey]autoEmbedResolveState
	memo  map[autoEmbedResolveKey]autoEmbedResolveResult
}

// autoEmbedSourceSnapshot preserves INSERT SELECT output provenance across
// logical optimization, which may replace the source tree with TableDual. Its
// cloned schema is immutable; lookup relies on corresponding SELECT outputs
// retaining their UniqueIDs and compatible types across optimization.
type autoEmbedSourceSnapshot struct {
	schema  *expression.Schema
	results []autoEmbedResolveResult
}

// snapshotAutoEmbedSource records INSERT SELECT output proofs before logical
// optimization can discard their source plans.
func snapshotAutoEmbedSource(root base.LogicalPlan) *autoEmbedSourceSnapshot {
	if isNilAutoEmbedPlan(root) || root.Schema() == nil {
		return nil
	}
	resolver := &autoEmbedResolver{
		state: make(map[autoEmbedResolveKey]autoEmbedResolveState),
		memo:  make(map[autoEmbedResolveKey]autoEmbedResolveResult),
	}
	snapshot := &autoEmbedSourceSnapshot{
		schema:  root.Schema().Clone(),
		results: make([]autoEmbedResolveResult, root.Schema().Len()),
	}
	for idx, col := range root.Schema().Columns {
		result := resolver.resolve(root, col)
		result.found = true
		snapshot.results[idx] = result
	}
	return snapshot
}

// resolve uses the same pointer-first, then UniqueID-and-type identity rules as
// logical traversal. Ambiguous snapshot matches claim the namespace without
// returning metadata, so INSERT cannot fall back to its target table by chance.
func (s *autoEmbedSourceSnapshot) resolve(target *expression.Column) autoEmbedResolveResult {
	if s == nil {
		return autoEmbedResolveResult{}
	}
	_, idx, found, ambiguous := findAutoEmbedColumn(s.schema, target)
	if ambiguous {
		return autoEmbedResolveResult{found: true}
	}
	if !found || idx >= len(s.results) {
		return autoEmbedResolveResult{}
	}
	return s.results[idx]
}

// autoEmbedPlanHasProvenance is used only before buildSelection replaces a
// constant-false source with TableDual. Keeping that source until expression
// rewrite preserves provenance; normal logical optimization can still produce
// the same empty physical plan afterwards.
func autoEmbedPlanHasProvenance(root base.LogicalPlan) bool {
	if isNilAutoEmbedPlan(root) {
		return false
	}
	resolver := &autoEmbedResolver{
		state: make(map[autoEmbedResolveKey]autoEmbedResolveState),
		memo:  make(map[autoEmbedResolveKey]autoEmbedResolveResult),
	}
	schemas := []*expression.Schema{root.Schema()}
	switch p := root.(type) {
	case *logicalop.LogicalJoin:
		schemas = append(schemas, p.FullSchema)
	case *logicalop.LogicalApply:
		schemas = append(schemas, p.FullSchema)
	}
	for _, schema := range schemas {
		if schema == nil {
			continue
		}
		for _, col := range schema.Columns {
			if result := resolver.resolve(root, col); result.info != nil {
				return true
			}
		}
	}
	return false
}

// resolveAutoEmbedInfo selects exactly one provenance namespace. INSERT target
// columns take precedence only when the SELECT snapshot does not also claim the
// column; source/target ambiguity and an unprovable source both fail closed.
// Ordinary expressions resolve solely from the current logical root.
func resolveAutoEmbedInfo(
	root base.LogicalPlan,
	insert *physicalop.Insert,
	insertSource *autoEmbedSourceSnapshot,
	vector *expression.Column,
) (*expression.AutoEmbedInfo, bool) {
	if vector == nil || vector.RetType == nil || !vector.RetType.EvalType().IsVectorKind() {
		return nil, false
	}

	resolver := &autoEmbedResolver{
		state: make(map[autoEmbedResolveKey]autoEmbedResolveState),
		memo:  make(map[autoEmbedResolveKey]autoEmbedResolveResult),
	}
	if insert != nil {
		if targetInfo, matched := resolveInsertTargetAutoEmbedInfo(insert, vector); matched {
			if insertSource != nil && insertSource.resolve(vector).found {
				return nil, false
			}
			if targetInfo == nil {
				return nil, false
			}
			return copyAutoEmbedInfo(targetInfo), true
		}
		if insertSource != nil {
			result := insertSource.resolve(vector)
			if result.found && result.info != nil {
				return copyAutoEmbedInfo(result.info), true
			}
			return nil, false
		}
	}

	result := resolver.resolve(root, vector)
	if !result.found || result.info == nil {
		return nil, false
	}
	return copyAutoEmbedInfo(result.info), true
}

// resolve memoizes (plan, column) pairs for one consumer lookup. Encountering
// an active pair means malformed or recursive lineage; it claims the namespace
// without metadata to stop both recursion and fallback.
func (r *autoEmbedResolver) resolve(plan base.LogicalPlan, target *expression.Column) autoEmbedResolveResult {
	if isNilAutoEmbedPlan(plan) || target == nil || target.RetType == nil {
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
	case *logicalop.LogicalTableDual, *logicalop.LogicalExpand:
		return autoEmbedResolveResult{found: autoEmbedPlanSchemaContains(plan, target)}
	default:
		// GeneratedExprString used to survive arbitrary Column.Clone calls.
		// Unknown operators intentionally fail closed instead of treating an
		// incidental clone as proof of auto-embedding provenance.
		return autoEmbedResolveResult{found: autoEmbedPlanSchemaContains(plan, target)}
	}
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
		if isNilAutoEmbedPlan(child) || child.Schema() == nil || idx >= child.Schema().Len() {
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
		p.Cte.RecursivePartLogicalPlan != nil || isNilAutoEmbedPlan(p.Cte.SeedPartLogicalPlan) {
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

// resolveDataSourceAutoEmbedInfo proves a provenance root from one public,
// visible, stored generated table column whose parsed expression is a direct
// supported EMBED_TEXT call. Catalog SQL text is never copied to plan columns.
func resolveDataSourceAutoEmbedInfo(ds *logicalop.DataSource, target *expression.Column) autoEmbedResolveResult {
	if ds == nil || ds.Schema() == nil || ds.Table == nil {
		return autoEmbedResolveResult{}
	}
	planCol, _, found, ambiguous := findAutoEmbedColumn(ds.Schema(), target)
	if ambiguous {
		return autoEmbedResolveResult{found: true}
	}
	if !found {
		return autoEmbedResolveResult{}
	}
	var matched *table.Column
	for _, tblCol := range ds.Table.Cols() {
		if tblCol == nil || tblCol.ColumnInfo == nil || tblCol.ID != planCol.ID {
			continue
		}
		if matched != nil {
			return autoEmbedResolveResult{found: true}
		}
		matched = tblCol
	}
	return autoEmbedInfoFromTableColumn(matched, planCol)
}

// resolveInsertTargetAutoEmbedInfo requires pointer identity in TableSchema.
// UniqueID matching is reserved for the SELECT snapshot, preventing a cloned
// source output from being reinterpreted as the destination table column.
func resolveInsertTargetAutoEmbedInfo(insert *physicalop.Insert, target *expression.Column) (*expression.AutoEmbedInfo, bool) {
	if insert == nil || insert.TableSchema == nil || insert.Table == nil || target == nil {
		return nil, false
	}
	matchedIdx := -1
	for idx, col := range insert.TableSchema.Columns {
		if col != target {
			continue
		}
		if matchedIdx >= 0 {
			return nil, true
		}
		matchedIdx = idx
	}
	if matchedIdx < 0 {
		return nil, false
	}
	cols := insert.Table.Cols()
	if matchedIdx >= len(cols) {
		return nil, true
	}
	result := autoEmbedInfoFromTableColumn(cols[matchedIdx], target)
	return result.info, true
}

// autoEmbedInfoFromTableColumn validates the catalog constraints shared by
// DataSource and INSERT target roots, then extracts immutable metadata.
func autoEmbedInfoFromTableColumn(tblCol *table.Column, planCol *expression.Column) autoEmbedResolveResult {
	result := autoEmbedResolveResult{found: true}
	if tblCol == nil || tblCol.ColumnInfo == nil || planCol == nil || planCol.RetType == nil ||
		tblCol.State != model.StatePublic || tblCol.Hidden || !tblCol.IsGenerated() || !tblCol.GeneratedStored ||
		!planCol.RetType.EvalType().IsVectorKind() || !autoEmbedFieldTypesCompatible(planCol.RetType, &tblCol.FieldType) ||
		tblCol.GeneratedExpr == nil || tblCol.GeneratedExpr.Internal() == nil ||
		!expression.IsAutoEmbedFnCallAST(tblCol.GeneratedExpr.Internal()) {
		return result
	}
	info, err := expression.ExtractAutoEmbedInfoFromAST(tblCol.GeneratedExpr.Internal())
	if err != nil {
		return result
	}
	result.info = copyAutoEmbedInfo(info)
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
	if isNilAutoEmbedPlan(plan) {
		return false
	}
	return autoEmbedPlanDirectNamespaceContains(plan, target)
}

func autoEmbedPlanDirectNamespaceContains(plan base.LogicalPlan, target *expression.Column) bool {
	if isNilAutoEmbedPlan(plan) || target == nil {
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

// findAutoEmbedColumn prefers exact object identity, then accepts one
// type-compatible UniqueID match for cloning operators. Duplicate candidates
// are ambiguity, not an arbitrary first match.
func findAutoEmbedColumn(schema *expression.Schema, target *expression.Column) (matched *expression.Column, idx int, found bool, ambiguous bool) {
	if schema == nil || target == nil {
		return nil, -1, false, false
	}
	pointerIdx := -1
	for idx, col := range schema.Columns {
		if col != target {
			continue
		}
		if pointerIdx >= 0 {
			return nil, -1, false, true
		}
		pointerIdx = idx
	}
	if pointerIdx >= 0 {
		return schema.Columns[pointerIdx], pointerIdx, true, false
	}
	matchIdx := -1
	for idx, col := range schema.Columns {
		if !autoEmbedColumnsMatch(col, target) {
			continue
		}
		if matchIdx >= 0 {
			return nil, -1, false, true
		}
		matchIdx = idx
	}
	if matchIdx < 0 {
		return nil, -1, false, false
	}
	return schema.Columns[matchIdx], matchIdx, true, false
}

func findAutoEmbedColumnByID(schema *expression.Schema, id int64) *expression.Column {
	if schema == nil {
		return nil
	}
	var matched *expression.Column
	for _, col := range schema.Columns {
		if col == nil || col.UniqueID != id {
			continue
		}
		if matched != nil {
			return nil
		}
		matched = col
	}
	return matched
}

func autoEmbedColumnsMatch(left, right *expression.Column) bool {
	return left != nil && right != nil && left.UniqueID == right.UniqueID && autoEmbedVectorTypesCompatible(left, right)
}

func autoEmbedVectorTypesCompatible(left, right *expression.Column) bool {
	return left != nil && right != nil && autoEmbedFieldTypesCompatible(left.RetType, right.RetType)
}

// autoEmbedFieldTypesCompatible tolerates only NotNull drift because MaxOneRow
// clears that flag while keeping the same value. Dimensions and every other
// FieldType property remain part of the proof.
func autoEmbedFieldTypesCompatible(left, right *types.FieldType) bool {
	if left == nil || right == nil || !left.EvalType().IsVectorKind() || !right.EvalType().IsVectorKind() {
		return false
	}
	leftCopy := left.Clone()
	rightCopy := right.Clone()
	leftCopy.DelFlag(mysql.NotNullFlag)
	rightCopy.DelFlag(mysql.NotNullFlag)
	return leftCopy.Equals(rightCopy)
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

func copyAutoEmbedInfo(info *expression.AutoEmbedInfo) *expression.AutoEmbedInfo {
	if info == nil {
		return nil
	}
	cloned := *info
	return &cloned
}

// isNilAutoEmbedPlan handles typed nil pointers stored in LogicalPlan
// interfaces before they can reach type-specific methods and panic.
func isNilAutoEmbedPlan(plan base.LogicalPlan) bool {
	if plan == nil {
		return true
	}
	value := reflect.ValueOf(plan)
	return value.Kind() == reflect.Pointer && value.IsNil()
}
