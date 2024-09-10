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

package logicalop

import (
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	base2 "github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	ruleutil "github.com/pingcap/tidb/pkg/planner/core/rule/util"
	fd "github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace/logicaltrace"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/util/intset"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

// LogicalProjection represents a select fields plan.
type LogicalProjection struct {
	LogicalSchemaProducer

	Exprs []expression.Expression

	// CalculateNoDelay indicates this Projection is the root Plan and should be
	// calculated without delay and will not return any result to client.
	// Currently it is "true" only when the current sql query is a "DO" statement.
	// See "https://dev.mysql.com/doc/refman/5.7/en/do.html" for more detail.
	CalculateNoDelay bool

	// Proj4Expand is used for expand to project same column reference, while these
	// col may be filled with null so we couldn't just eliminate this projection itself.
	Proj4Expand bool
}

// Init initializes LogicalProjection.
func (p LogicalProjection) Init(ctx base.PlanContext, qbOffset int) *LogicalProjection {
	p.BaseLogicalPlan = NewBaseLogicalPlan(ctx, plancodec.TypeProj, &p, qbOffset)
	return &p
}

// *************************** start implementation of HashEquals interface ****************************

// Hash64 implements the base.Hash64.<0th> interface.
func (p *LogicalProjection) Hash64(h base2.Hasher) {
	// todo: LogicalSchemaProducer should implement HashEquals interface, otherwise, its self elements
	// like schema and names are lost.
	p.LogicalSchemaProducer.Hash64(h)
	// todo: if we change the logicalProjection's Expr definition as:Exprs []memo.ScalarOperator[any],
	// we should use like below:
	// for _, one := range p.Exprs {
	//		one.Hash64(one)
	// }
	// otherwise, we would use the belowing code.
	//for _, one := range p.Exprs {
	//	one.Hash64(h)
	//}
	h.HashBool(p.CalculateNoDelay)
	h.HashBool(p.Proj4Expand)
}

// Equals implements the base.HashEquals.<1st> interface.
func (p *LogicalProjection) Equals(other any) bool {
	if other == nil {
		return false
	}
	proj, ok := other.(*LogicalProjection)
	if !ok {
		return false
	}
	// todo: LogicalSchemaProducer should implement HashEquals interface, otherwise, its self elements
	// like schema and names are lost.
	if !p.LogicalSchemaProducer.Equals(&proj.LogicalSchemaProducer) {
		return false
	}
	//for i, one := range p.Exprs {
	//	if !one.(memo.ScalarOperator[any]).Equals(other.Exprs[i]) {
	//		return false
	//	}
	//}
	if p.CalculateNoDelay != proj.CalculateNoDelay {
		return false
	}
	return p.Proj4Expand == proj.Proj4Expand
}

// *************************** start implementation of Plan interface **********************************

// ExplainInfo implements Plan interface.
func (p *LogicalProjection) ExplainInfo() string {
	eCtx := p.SCtx().GetExprCtx().GetEvalCtx()
	enableRedactLog := p.SCtx().GetSessionVars().EnableRedactLog
	return expression.ExplainExpressionList(eCtx, p.Exprs, p.Schema(), enableRedactLog)
}

// ReplaceExprColumns implements base.LogicalPlan interface.
func (p *LogicalProjection) ReplaceExprColumns(replace map[string]*expression.Column) {
	for _, expr := range p.Exprs {
		ruleutil.ResolveExprAndReplace(expr, replace)
	}
}

// *************************** end implementation of Plan interface ************************************

// *************************** start implementation of logicalPlan interface ***************************

// HashCode implements base.LogicalPlan.<0th> interface.
func (p *LogicalProjection) HashCode() []byte {
	// PlanType + SelectOffset + ExprNum + [Exprs]
	// Expressions are commonly `Column`s, whose hashcode has the length 9, so
	// we pre-alloc 10 bytes for each expr's hashcode.
	result := make([]byte, 0, 12+len(p.Exprs)*10)
	result = util.EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.TP()))
	result = util.EncodeIntAsUint32(result, p.QueryBlockOffset())
	result = util.EncodeIntAsUint32(result, len(p.Exprs))
	for _, expr := range p.Exprs {
		exprHashCode := expr.HashCode()
		result = util.EncodeIntAsUint32(result, len(exprHashCode))
		result = append(result, exprHashCode...)
	}
	return result
}

// PredicatePushDown implements base.LogicalPlan.<1st> interface.
func (p *LogicalProjection) PredicatePushDown(predicates []expression.Expression, opt *optimizetrace.LogicalOptimizeOp) (ret []expression.Expression, retPlan base.LogicalPlan) {
	for _, expr := range p.Exprs {
		if expression.HasAssignSetVarFunc(expr) {
			_, child := p.BaseLogicalPlan.PredicatePushDown(nil, opt)
			return predicates, child
		}
	}
	canBePushed, canNotBePushed := breakDownPredicates(p, predicates)
	remained, child := p.BaseLogicalPlan.PredicatePushDown(canBePushed, opt)
	return append(remained, canNotBePushed...), child
}

// PruneColumns implements base.LogicalPlan.<2nd> interface.
// If any expression has SetVar function or Sleep function, we do not prune it.
func (p *LogicalProjection) PruneColumns(parentUsedCols []*expression.Column, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, error) {
	used := expression.GetUsedList(p.SCtx().GetExprCtx().GetEvalCtx(), parentUsedCols, p.Schema())
	prunedColumns := make([]*expression.Column, 0)

	allPruned := true
	for i, b := range used {
		if b || expression.ExprHasSetVarOrSleep(p.Exprs[i]) {
			// Set to true to avoid the ExprHasSetVarOrSleep be called multiple times.
			used[i] = true
			allPruned = false
			break
		}
	}
	if allPruned {
		_, ok := p.Children()[0].(*LogicalTableDual)
		if ok {
			// If the child is dual. The proj should not be eliminated.
			// When the dual is generated by `select ....;`` directly(No source SELECT), the dual is created without any output cols.
			// The dual only holds a RowCount, and the proj is used to generate the output cols.
			// But we need reset the used columns in its expression since there can be correlated columns in the expression.
			// e.g. SELECT 1 FROM t1 WHERE TRUE OR ( SELECT 1 FROM (SELECT a) q ) = 1;
			//   The `SELECT a` will create a projection whose expression is the correlated column `a` from outer table t1.
			//   We need to eliminate it so that our decorrelation can work as expected.
			p.Exprs = p.Exprs[:1]
			p.Schema().Columns = p.Schema().Columns[:1]
			p.Exprs[0] = expression.NewZero()
			p.Schema().Columns[0] = &expression.Column{
				UniqueID: p.SCtx().GetSessionVars().AllocPlanColumnID(),
				RetType:  p.Exprs[0].GetType(p.SCtx().GetExprCtx().GetEvalCtx()),
			}
			return p, nil
		}
	}

	// for implicit projected cols, once the ancestor doesn't use it, the implicit expr will be automatically pruned here.
	for i := len(used) - 1; i >= 0; i-- {
		if !used[i] && !expression.ExprHasSetVarOrSleep(p.Exprs[i]) {
			prunedColumns = append(prunedColumns, p.Schema().Columns[i])
			p.Schema().Columns = append(p.Schema().Columns[:i], p.Schema().Columns[i+1:]...)
			p.Exprs = append(p.Exprs[:i], p.Exprs[i+1:]...)
		}
	}
	logicaltrace.AppendColumnPruneTraceStep(p, prunedColumns, opt)
	selfUsedCols := make([]*expression.Column, 0, len(p.Exprs))
	selfUsedCols = expression.ExtractColumnsFromExpressions(selfUsedCols, p.Exprs, nil)
	var err error
	p.Children()[0], err = p.Children()[0].PruneColumns(selfUsedCols, opt)
	if err != nil {
		return nil, err
	}
	// If its columns are all pruned, we directly use its child. The child will output at least one column.
	if p.Schema().Len() == 0 {
		return p.Children()[0], nil
	}
	return p, nil
}

// FindBestTask inherits BaseLogicalPlan.<3rd> implementation.

// BuildKeyInfo implements base.LogicalPlan.<4th> interface.
func (p *LogicalProjection) BuildKeyInfo(selfSchema *expression.Schema, childSchema []*expression.Schema) {
	// `LogicalProjection` use schema from `Exprs` to build key info. See `buildSchemaByExprs`.
	// So call `baseLogicalPlan.BuildKeyInfo` here to avoid duplicated building key info.
	p.BaseLogicalPlan.BuildKeyInfo(selfSchema, childSchema)
	selfSchema.Keys = nil
	schema := p.buildSchemaByExprs(selfSchema)
	for _, key := range childSchema[0].Keys {
		indices := schema.ColumnsIndices(key)
		if indices == nil {
			continue
		}
		newKey := make([]*expression.Column, 0, len(key))
		for _, i := range indices {
			newKey = append(newKey, selfSchema.Columns[i])
		}
		selfSchema.Keys = append(selfSchema.Keys, newKey)
	}
}

// PushDownTopN implements base.LogicalPlan.<5th> interface.
func (p *LogicalProjection) PushDownTopN(topNLogicalPlan base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	var topN *LogicalTopN
	if topNLogicalPlan != nil {
		topN = topNLogicalPlan.(*LogicalTopN)
	}
	for _, expr := range p.Exprs {
		if expression.HasAssignSetVarFunc(expr) {
			return p.BaseLogicalPlan.PushDownTopN(topN, opt)
		}
	}
	if topN != nil {
		exprCtx := p.SCtx().GetExprCtx()
		substitutedExprs := make([]expression.Expression, 0, len(topN.ByItems))
		for _, by := range topN.ByItems {
			substituted := expression.FoldConstant(exprCtx, expression.ColumnSubstitute(exprCtx, by.Expr, p.Schema(), p.Exprs))
			if !expression.IsImmutableFunc(substituted) {
				// after substituting, if the order-by expression is un-deterministic like 'order by rand()', stop pushing down.
				return p.BaseLogicalPlan.PushDownTopN(topN, opt)
			}
			substitutedExprs = append(substitutedExprs, substituted)
		}
		for i, by := range topN.ByItems {
			by.Expr = substitutedExprs[i]
		}

		// remove meaningless constant sort items.
		for i := len(topN.ByItems) - 1; i >= 0; i-- {
			switch topN.ByItems[i].Expr.(type) {
			case *expression.Constant, *expression.CorrelatedColumn:
				topN.ByItems = append(topN.ByItems[:i], topN.ByItems[i+1:]...)
			}
		}

		// if topN.ByItems contains a column(with ID=0) generated by projection, projection will prevent the optimizer from pushing topN down.
		for _, by := range topN.ByItems {
			cols := expression.ExtractColumns(by.Expr)
			for _, col := range cols {
				if col.ID == 0 && p.Schema().Contains(col) {
					// check whether the column is generated by projection
					if !p.Children()[0].Schema().Contains(col) {
						p.Children()[0] = p.Children()[0].PushDownTopN(nil, opt)
						return topN.AttachChild(p, opt)
					}
				}
			}
		}
	}
	p.Children()[0] = p.Children()[0].PushDownTopN(topN, opt)
	return p
}

// DeriveTopN inherits BaseLogicalPlan.<6th> implementation.

// PredicateSimplification inherits BaseLogicalPlan.<7th> implementation.

// ConstantPropagation inherits BaseLogicalPlan.<8th> implementation.

// PullUpConstantPredicates implements base.LogicalPlan.<9th> interface.
func (p *LogicalProjection) PullUpConstantPredicates() []expression.Expression {
	// projection has no column expr
	if !canProjectionBeEliminatedLoose(p) {
		return nil
	}
	candidateConstantPredicates := p.Children()[0].PullUpConstantPredicates()
	// replace predicate by projection expr
	// candidate predicate : a=1
	// projection: a as a'
	// result predicate : a'=1
	replace := make(map[string]*expression.Column)
	for i, expr := range p.Exprs {
		replace[string(expr.HashCode())] = p.Schema().Columns[i]
	}
	result := make([]expression.Expression, 0, len(candidateConstantPredicates))
	for _, predicate := range candidateConstantPredicates {
		// The column of predicate must exist in projection exprs
		columns := expression.ExtractColumns(predicate)
		// The number of columns in candidate predicate must be 1.
		if len(columns) != 1 {
			continue
		}
		if replace[string(columns[0].HashCode())] == nil {
			// The column of predicate will not appear on the upper level
			// This means that this predicate does not apply to the constant propagation optimization rule
			// For example: select * from t, (select b from s where s.a=1) tmp where t.b=s.b
			continue
		}
		clonePredicate := predicate.Clone()
		ruleutil.ResolveExprAndReplace(clonePredicate, replace)
		result = append(result, clonePredicate)
	}
	return result
}

// RecursiveDeriveStats inherits BaseLogicalPlan.<10th> implementation.

// DeriveStats implement base.LogicalPlan.<11th> interface.
func (p *LogicalProjection) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchema []*expression.Schema, colGroups [][]*expression.Column) (*property.StatsInfo, error) {
	childProfile := childStats[0]
	if p.StatsInfo() != nil {
		// Reload GroupNDVs since colGroups may have changed.
		p.StatsInfo().GroupNDVs = p.getGroupNDVs(colGroups, childProfile, selfSchema)
		return p.StatsInfo(), nil
	}
	p.SetStats(&property.StatsInfo{
		RowCount: childProfile.RowCount,
		ColNDVs:  make(map[int64]float64, len(p.Exprs)),
	})
	for i, expr := range p.Exprs {
		cols := expression.ExtractColumns(expr)
		p.StatsInfo().ColNDVs[selfSchema.Columns[i].UniqueID], _ = cardinality.EstimateColsNDVWithMatchedLen(cols, childSchema[0], childProfile)
	}
	p.StatsInfo().GroupNDVs = p.getGroupNDVs(colGroups, childProfile, selfSchema)
	return p.StatsInfo(), nil
}

// ExtractColGroups implements base.LogicalPlan.<12th> interface.
func (p *LogicalProjection) ExtractColGroups(colGroups [][]*expression.Column) [][]*expression.Column {
	if len(colGroups) == 0 {
		return nil
	}
	extColGroups, _ := p.Schema().ExtractColGroups(colGroups)
	if len(extColGroups) == 0 {
		return nil
	}
	extracted := make([][]*expression.Column, 0, len(extColGroups))
	for _, cols := range extColGroups {
		exprs := make([]*expression.Column, len(cols))
		allCols := true
		for i, offset := range cols {
			col, ok := p.Exprs[offset].(*expression.Column)
			// TODO: for functional dependent projections like `col1 + 1` -> `col2`, we can maintain GroupNDVs actually.
			if !ok {
				allCols = false
				break
			}
			exprs[i] = col
		}
		if allCols {
			extracted = append(extracted, expression.SortColumns(exprs))
		}
	}
	return extracted
}

// PreparePossibleProperties implements base.LogicalPlan.<13th> interface.
func (p *LogicalProjection) PreparePossibleProperties(_ *expression.Schema, childrenProperties ...[][]*expression.Column) [][]*expression.Column {
	childProperties := childrenProperties[0]
	oldCols := make([]*expression.Column, 0, p.Schema().Len())
	newCols := make([]*expression.Column, 0, p.Schema().Len())
	for i, expr := range p.Exprs {
		if col, ok := expr.(*expression.Column); ok {
			newCols = append(newCols, p.Schema().Columns[i])
			oldCols = append(oldCols, col)
		}
	}
	tmpSchema := expression.NewSchema(oldCols...)
	newProperties := make([][]*expression.Column, 0, len(childProperties))
	for _, childProperty := range childProperties {
		newChildProperty := make([]*expression.Column, 0, len(childProperty))
		for _, col := range childProperty {
			pos := tmpSchema.ColumnIndex(col)
			if pos < 0 {
				break
			}
			newChildProperty = append(newChildProperty, newCols[pos])
		}
		if len(newChildProperty) != 0 {
			newProperties = append(newProperties, newChildProperty)
		}
	}
	return newProperties
}

// ExhaustPhysicalPlans implements base.LogicalPlan.<14th> interface.
func (p *LogicalProjection) ExhaustPhysicalPlans(prop *property.PhysicalProperty) ([]base.PhysicalPlan, bool, error) {
	return utilfuncp.ExhaustPhysicalPlans4LogicalProjection(p, prop)
}

// ExtractCorrelatedCols implements base.LogicalPlan.<15th> interface.
func (p *LogicalProjection) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := make([]*expression.CorrelatedColumn, 0, len(p.Exprs))
	for _, expr := range p.Exprs {
		corCols = append(corCols, expression.ExtractCorColumns(expr)...)
	}
	return corCols
}

// MaxOneRow inherits BaseLogicalPlan.<16th> implementation.

// Children inherits BaseLogicalPlan.<17th> implementation.

// SetChildren inherits BaseLogicalPlan.<18th> implementation.

// SetChild inherits BaseLogicalPlan.<19th> implementation.

// RollBackTaskMap inherits BaseLogicalPlan.<20th> implementation.

// CanPushToCop inherits BaseLogicalPlan.<21st> implementation.

// ExtractFD implements base.LogicalPlan.<22nd> interface.
func (p *LogicalProjection) ExtractFD() *fd.FDSet {
	// basically extract the children's fdSet.
	fds := p.LogicalSchemaProducer.ExtractFD()
	// collect the output columns' unique ID.
	outputColsUniqueIDs := intset.NewFastIntSet()
	notnullColsUniqueIDs := intset.NewFastIntSet()
	outputColsUniqueIDsArray := make([]int, 0, len(p.Schema().Columns))
	// here schema extended columns may contain expr, const and column allocated with uniqueID.
	for _, one := range p.Schema().Columns {
		outputColsUniqueIDs.Insert(int(one.UniqueID))
		outputColsUniqueIDsArray = append(outputColsUniqueIDsArray, int(one.UniqueID))
	}
	// map the expr hashCode with its unique ID.
	for idx, expr := range p.Exprs {
		switch x := expr.(type) {
		case *expression.Column:
			continue
		case *expression.CorrelatedColumn:
			// t1(a,b,c), t2(m,n)
			// select a, (select c from t2 where m=b) from t1;
			// take c as constant column here.
			continue
		case *expression.Constant:
			hashCode := string(x.HashCode())
			var (
				ok               bool
				constantUniqueID int
			)
			if constantUniqueID, ok = fds.IsHashCodeRegistered(hashCode); !ok {
				constantUniqueID = outputColsUniqueIDsArray[idx]
				fds.RegisterUniqueID(string(x.HashCode()), constantUniqueID)
			}
			fds.AddConstants(intset.NewFastIntSet(constantUniqueID))
		case *expression.ScalarFunction:
			// t1(a,b,c), t2(m,n)
			// select a, (select c+n from t2 where m=b) from t1;
			// expr(c+n) contains correlated column , but we can treat it as constant here.
			hashCode := string(x.HashCode())
			var (
				ok             bool
				scalarUniqueID int
			)
			// If this function is not deterministic, we skip it since it not a stable value.
			if expression.CheckNonDeterministic(x) {
				if scalarUniqueID, ok = fds.IsHashCodeRegistered(hashCode); !ok {
					fds.RegisterUniqueID(hashCode, scalarUniqueID)
				}
				continue
			}
			if scalarUniqueID, ok = fds.IsHashCodeRegistered(hashCode); !ok {
				scalarUniqueID = outputColsUniqueIDsArray[idx]
				fds.RegisterUniqueID(hashCode, scalarUniqueID)
			} else {
				// since the scalar's hash code has been registered before, the equivalence exists between the unique ID
				// allocated by phase of building-projection-for-scalar and that of previous registered unique ID.
				fds.AddEquivalence(intset.NewFastIntSet(scalarUniqueID), intset.NewFastIntSet(outputColsUniqueIDsArray[idx]))
			}
			determinants := intset.NewFastIntSet()
			extractedColumns := expression.ExtractColumns(x)
			extractedCorColumns := expression.ExtractCorColumns(x)
			for _, one := range extractedColumns {
				determinants.Insert(int(one.UniqueID))
				// the dependent columns in scalar function should be also considered as output columns as well.
				outputColsUniqueIDs.Insert(int(one.UniqueID))
			}
			for _, one := range extractedCorColumns {
				determinants.Insert(int(one.UniqueID))
				// the dependent columns in scalar function should be also considered as output columns as well.
				outputColsUniqueIDs.Insert(int(one.UniqueID))
			}
			notnull := util.IsNullRejected(p.SCtx(), p.Schema(), x)
			if notnull || determinants.SubsetOf(fds.NotNullCols) {
				notnullColsUniqueIDs.Insert(scalarUniqueID)
			}
			fds.AddStrictFunctionalDependency(determinants, intset.NewFastIntSet(scalarUniqueID))
		}
	}

	// apply operator's characteristic's FD setting.
	// since the distinct attribute is built as firstRow agg func, we don't need to think about it here.
	// let the fds itself to trace the not null, because after the outer join, some not null column can be nullable.
	fds.MakeNotNull(notnullColsUniqueIDs)
	// select max(a) from t group by b, we should project both `a` & `b` to maintain the FD down here, even if select-fields only contain `a`.
	fds.ProjectCols(outputColsUniqueIDs.Union(fds.GroupByCols))
	// just trace it down in every operator for test checking.
	p.SetFDs(fds)
	return fds
}

// GetBaseLogicalPlan inherits BaseLogicalPlan.<23rd> implementation.

// ConvertOuterToInnerJoin implements base.LogicalPlan.<24th> interface.
func (p *LogicalProjection) ConvertOuterToInnerJoin(predicates []expression.Expression) base.LogicalPlan {
	proj := p.Self().(*LogicalProjection)
	canBePushed, _ := breakDownPredicates(proj, predicates)
	child := proj.Children()[0]
	child = child.ConvertOuterToInnerJoin(canBePushed)
	proj.SetChildren(child)
	return proj
}

// *************************** end implementation of logicalPlan interface ***************************

// GetUsedCols extracts all of the Columns used by proj.
func (p *LogicalProjection) GetUsedCols() (usedCols []*expression.Column) {
	for _, expr := range p.Exprs {
		usedCols = append(usedCols, expression.ExtractColumns(expr)...)
	}
	return usedCols
}

// A bijection exists between columns of a projection's schema and this projection's Exprs.
// Sometimes we need a schema made by expr of Exprs to convert a column in child's schema to a column in this projection's Schema.
func (p *LogicalProjection) buildSchemaByExprs(selfSchema *expression.Schema) *expression.Schema {
	schema := expression.NewSchema(make([]*expression.Column, 0, selfSchema.Len())...)
	for _, expr := range p.Exprs {
		if col, isCol := expr.(*expression.Column); isCol {
			schema.Append(col)
		} else {
			// If the expression is not a column, we add a column to occupy the position.
			schema.Append(&expression.Column{
				UniqueID: p.SCtx().GetSessionVars().AllocPlanColumnID(),
				RetType:  expr.GetType(p.SCtx().GetExprCtx().GetEvalCtx()),
			})
		}
	}
	return schema
}

// TryToGetChildProp will check if this sort property can be pushed or not.
// When a sort column will be replaced by scalar function, we refuse it.
// When a sort column will be replaced by a constant, we just remove it.
func (p *LogicalProjection) TryToGetChildProp(prop *property.PhysicalProperty) (*property.PhysicalProperty, bool) {
	newProp := prop.CloneEssentialFields()
	newCols := make([]property.SortItem, 0, len(prop.SortItems))
	for _, col := range prop.SortItems {
		idx := p.Schema().ColumnIndex(col.Col)
		switch expr := p.Exprs[idx].(type) {
		case *expression.Column:
			newCols = append(newCols, property.SortItem{Col: expr, Desc: col.Desc})
		case *expression.ScalarFunction:
			return nil, false
		}
	}
	newProp.SortItems = newCols
	return newProp, true
}

func (p *LogicalProjection) getGroupNDVs(colGroups [][]*expression.Column, childProfile *property.StatsInfo, selfSchema *expression.Schema) []property.GroupNDV {
	if len(colGroups) == 0 || len(childProfile.GroupNDVs) == 0 {
		return nil
	}
	exprCol2ProjCol := make(map[int64]int64)
	for i, expr := range p.Exprs {
		exprCol, ok := expr.(*expression.Column)
		if !ok {
			continue
		}
		exprCol2ProjCol[exprCol.UniqueID] = selfSchema.Columns[i].UniqueID
	}
	ndvs := make([]property.GroupNDV, 0, len(childProfile.GroupNDVs))
	for _, childGroupNDV := range childProfile.GroupNDVs {
		projCols := make([]int64, len(childGroupNDV.Cols))
		for i, col := range childGroupNDV.Cols {
			projCol, ok := exprCol2ProjCol[col]
			if !ok {
				projCols = nil
				break
			}
			projCols[i] = projCol
		}
		if projCols == nil {
			continue
		}
		slices.Sort(projCols)
		groupNDV := property.GroupNDV{
			Cols: projCols,
			NDV:  childGroupNDV.NDV,
		}
		ndvs = append(ndvs, groupNDV)
	}
	return ndvs
}

// AppendExpr adds the expression to the projection.
func (p *LogicalProjection) AppendExpr(expr expression.Expression) *expression.Column {
	if col, ok := expr.(*expression.Column); ok {
		return col
	}
	expr = expression.ColumnSubstitute(p.SCtx().GetExprCtx(), expr, p.Schema(), p.Exprs)
	p.Exprs = append(p.Exprs, expr)

	col := &expression.Column{
		UniqueID: p.SCtx().GetSessionVars().AllocPlanColumnID(),
		RetType:  expr.GetType(p.SCtx().GetExprCtx().GetEvalCtx()).Clone(),
	}
	col.SetCoercibility(expr.Coercibility())
	col.SetRepertoire(expr.Repertoire())
	p.Schema().Append(col)
	// reset ParseToJSONFlag in order to keep the flag away from json column
	if col.GetStaticType().GetType() == mysql.TypeJSON {
		col.GetStaticType().DelFlag(mysql.ParseToJSONFlag)
	}
	return col
}

// breakDownPredicates breaks down predicates into two sets: canBePushed and cannotBePushed. It also maps columns to projection schema.
func breakDownPredicates(p *LogicalProjection, predicates []expression.Expression) ([]expression.Expression, []expression.Expression) {
	canBePushed := make([]expression.Expression, 0, len(predicates))
	canNotBePushed := make([]expression.Expression, 0, len(predicates))
	exprCtx := p.SCtx().GetExprCtx()
	for _, cond := range predicates {
		substituted, hasFailed, newFilter := expression.ColumnSubstituteImpl(exprCtx, cond, p.Schema(), p.Exprs, true)
		if substituted && !hasFailed && !expression.HasGetSetVarFunc(newFilter) {
			canBePushed = append(canBePushed, newFilter)
		} else {
			canNotBePushed = append(canNotBePushed, cond)
		}
	}
	return canBePushed, canNotBePushed
}

// todo: merge with rule_eliminate_projection.go
func canProjectionBeEliminatedLoose(p *LogicalProjection) bool {
	// project for expand will assign a new col id for col ref, because these column should be
	// data cloned in the execution time and may be filled with null value at the same time.
	// so it's not a REAL column reference. Detect the column ref in projection here and do
	// the elimination here will restore the Expand's grouping sets column back to use the
	// original column ref again. (which is not right)
	if p.Proj4Expand {
		return false
	}
	for _, expr := range p.Exprs {
		_, ok := expr.(*expression.Column)
		if !ok {
			return false
		}
	}
	return true
}
