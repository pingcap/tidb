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

package memo

import (
	"unsafe"

	"github.com/bits-and-blooms/bitset"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	base2 "github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	"github.com/pingcap/tidb/pkg/planner/cascades/util"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/util/intest"
)

var _ base.GroupExpression = &GroupExpression{}

// GroupExpression is a single expression from the equivalent list classes inside a group.
// it is a node in the expression tree, while it takes groups as inputs. This kind of loose
// coupling between Group and GroupExpression is the key to the success of the memory compact
// of representing a forest.
type GroupExpression struct {
	// LogicalPlan is internal logical expression stands for this groupExpr.
	// Define it in the header element can make GE as Logical Plan implementor.
	base.LogicalPlan

	// group is the Group that this GroupExpression belongs to.
	group *Group

	// inputs stores the Groups that this GroupExpression based on.
	Inputs []*Group

	// hash64 is the unique fingerprint of the GroupExpression.
	hash64 uint64

	// mask indicate what rules have been applied in this group expression.
	mask *bitset.BitSet

	// abandoned is used in a case, when this gE has been encapsulated (say) 3 tasks
	// and pushed into the task, this 3 task are all referring to this same gE, one
	// of them has been substituted halfway, the successive task waiting on the task
	// should feel this gE is out of date, and this task is abandoned.
	abandoned bool
}

// GetGroup returns the Group that this GroupExpression belongs to.
func (e *GroupExpression) GetGroup() *Group {
	return e.group
}

// String implements the fmt.Stringer interface.
func (e *GroupExpression) String(w util.StrBufferWriter) {
	e.LogicalPlan.ExplainID()
	w.WriteString("GE:" + e.LogicalPlan.ExplainID().String() + "{")
	for i, input := range e.Inputs {
		if i != 0 {
			w.WriteString(", ")
		}
		input.String(w)
	}
	w.WriteString("}")
}

// GetHash64 returns the cached hash64 of the GroupExpression.
func (e *GroupExpression) GetHash64() uint64 {
	intest.Assert(e.hash64 != 0, "hash64 should not be 0")
	return e.hash64
}

// Hash64 implements the Hash64 interface.
func (e *GroupExpression) Hash64(h base2.Hasher) {
	// logical plan hash.
	e.LogicalPlan.Hash64(h)
	// children group hash.
	for _, child := range e.Inputs {
		child.Hash64(h)
	}
}

// Equals implements the Equals interface.
func (e *GroupExpression) Equals(other any) bool {
	e2, ok := other.(*GroupExpression)
	if !ok {
		return false
	}
	if e == nil {
		return e2 == nil
	}
	if e2 == nil {
		return false
	}
	if len(e.Inputs) != len(e2.Inputs) {
		return false
	}
	if pattern.GetOperand(e.LogicalPlan) != pattern.GetOperand(e2.LogicalPlan) {
		return false
	}
	// current logical operator meta cmp, logical plan don't care logicalPlan's children.
	// when we convert logicalPlan to GroupExpression, we will set children to nil.
	if !e.LogicalPlan.Equals(e2.LogicalPlan) {
		return false
	}
	// if one of the children is different, then the two GroupExpressions are different.
	for i, one := range e.Inputs {
		if !one.Equals(e2.Inputs[i]) {
			return false
		}
	}
	return true
}

// Init initializes the GroupExpression with the given group and hasher.
func (e *GroupExpression) Init(h base2.Hasher) {
	e.Hash64(h)
	e.hash64 = h.Sum64()
}

// IsExplored return whether this gE has explored rule i.
func (e *GroupExpression) IsExplored(i uint) bool {
	return e.mask.Test(i)
}

// SetExplored set this gE as explored in rule i.
func (e *GroupExpression) SetExplored(i uint) {
	e.mask.Set(i)
}

// IsAbandoned returns whether this gE is abandoned.
func (e *GroupExpression) IsAbandoned() bool {
	return e.abandoned
}

// SetAbandoned set this gE as abandoned.
func (e *GroupExpression) SetAbandoned() {
	e.abandoned = true
}

// mergeTo will migrate the src GE state to dst GE and remove src GE from its group.
func (e *GroupExpression) mergeTo(target *GroupExpression) {
	e.GetGroup().Delete(e)
	// rule mask | OR
	target.mask.InPlaceUnion(e.mask)
	// clear parentGE refs work
	for _, childG := range e.Inputs {
		childG.removeParentGEs(e)
	}
	e.Inputs = e.Inputs[:0]
	e.group = nil
}

func (e *GroupExpression) addr() unsafe.Pointer {
	return unsafe.Pointer(e)
}

// GetWrappedLogicalPlan overrides the logical plan interface implemented by BaseLogicalPlan.
func (e *GroupExpression) GetWrappedLogicalPlan() base.LogicalPlan {
	return e.LogicalPlan
}

// GetChildStatsAndSchema overrides the logical plan interface implemented by BaseLogicalPlan.
func (e *GroupExpression) GetChildStatsAndSchema() (stats0 *property.StatsInfo, schema0 *expression.Schema) {
	intest.AssertFunc(func() bool {
		switch e.GetWrappedLogicalPlan().(type) {
		case *logicalop.LogicalJoin, *logicalop.LogicalApply:
			return false
		default:
			return true
		}
	}, "GetChildStatsAndSchema should not be called on join GE, Please use getJoinChildStatsAndSchema.")
	return e.Inputs[0].GetLogicalProperty().Stats, e.Inputs[0].GetLogicalProperty().Schema
}

// GetJoinChildStatsAndSchema overrides the logical plan interface implemented by BaseLogicalPlan.
func (e *GroupExpression) GetJoinChildStatsAndSchema() (stats0, stats1 *property.StatsInfo, schema0, schema1 *expression.Schema) {
	intest.AssertFunc(func() bool {
		switch e.GetWrappedLogicalPlan().(type) {
		case *logicalop.LogicalJoin, *logicalop.LogicalApply:
			return true
		default:
			return false
		}
	}, "GetJoinChildStatsAndSchema should not be called on non-join GE, Please use GetChildStatsAndSchema.")
	stats0, schema0 = e.Inputs[0].GetLogicalProperty().Stats, e.Inputs[0].GetLogicalProperty().Schema
	stats1, schema1 = e.Inputs[1].GetLogicalProperty().Stats, e.Inputs[1].GetLogicalProperty().Schema
	return
}

// InputsLen returns the length of inputs.
func (e *GroupExpression) InputsLen() int {
	return len(e.Inputs)
}

// GetInputSchema returns the logical schema of the idx-th child group.
func (e *GroupExpression) GetInputSchema(idx int) *expression.Schema {
	return e.Inputs[idx].GetLogicalProperty().Schema
}

// DeriveLogicalProp derive the new group's logical property from a specific GE.
// DeriveLogicalProp is not called with recursive, because we only examine and
// init new group from bottom-up, so we can sure that this new group's children
// has already gotten its logical prop.
func (e *GroupExpression) DeriveLogicalProp() (err error) {
	if e.GetGroup().HasLogicalProperty() {
		return nil
	}
	childStats := make([]*property.StatsInfo, 0, len(e.Inputs))
	childSchema := make([]*expression.Schema, 0, len(e.Inputs))
	childProperties := make([][][]*expression.Column, 0, len(e.Inputs))
	for _, childG := range e.Inputs {
		childGProp := childG.GetLogicalProperty()
		childStats = append(childStats, childGProp.Stats)
		childSchema = append(childSchema, childGProp.Schema)
		childProperties = append(childProperties, childGProp.PossibleProps)
	}
	e.GetGroup().SetLogicalProperty(property.NewLogicalProp())
	// currently the schemaProducer side logical op is still useful for group schema.
	tmpFD := e.LogicalPlan.GetBaseLogicalPlan().(*logicalop.BaseLogicalPlan).FDs()
	tmpSchema := e.LogicalPlan.Schema()
	tmpStats := e.LogicalPlan.StatsInfo()
	var tmpPossibleProps [][]*expression.Column
	// the leaves node may have already had their stats in join reorder est phase, while
	// their group ndv signal is passed in CollectPredicateColumnsPoint which is applied
	// behind join reorder rule, we should build their group ndv again (implied in DeriveStats).
	skipDeriveStats := false
	if val, _err_ := failpoint.Eval(_curpkg_("MockPlanSkipMemoDeriveStats")); _err_ == nil {
		skipDeriveStats = val.(bool)
	}
	if !skipDeriveStats {
		// here can only derive the basic stats from bottom up, we can't pass any colGroups required by parents.
		tmpStats, _, err = e.LogicalPlan.DeriveStats(childStats, tmpSchema, childSchema, nil)
		if err != nil {
			return err
		}
		// todo: extractFD should be refactored as take in childFDs, and return the new FDSet rather than depend on tree.
		tmpFD = e.LogicalPlan.ExtractFD()
		// prepare the possible sort columns for the group, which require fillIndexPath to fill index cols.
		tmpPossibleProps = e.LogicalPlan.PreparePossibleProperties(tmpSchema, childProperties...)
	}
	e.GetGroup().GetLogicalProperty().Schema = tmpSchema
	e.GetGroup().GetLogicalProperty().Stats = tmpStats
	e.GetGroup().GetLogicalProperty().FD = tmpFD
	e.GetGroup().GetLogicalProperty().PossibleProps = tmpPossibleProps
	return nil
}

// ExhaustPhysicalPlans4GroupExpression enumerate the physical implementation for concrete ops.
func ExhaustPhysicalPlans4GroupExpression(e *GroupExpression, prop *property.PhysicalProperty) (physicalPlans [][]base.PhysicalPlan, hintCanWork bool, err error) {
	var ops []base.PhysicalPlan
	// once we call GE's ExhaustPhysicalPlans from group expression level, we should judge from here, and get the
	// wrapped logical plan and then call their specific function pointer to handle logic inside. Why not we just
	// remove GE's level implementation, and call wrapped logical plan's implementing? Cuz sometimes, the wrapped
	// logical plan may has some dependency on the children/group's logical property, so we should pass the GE into
	// the specific function pointer, and then iterate its children to get their logical property.
	switch x := e.GetWrappedLogicalPlan().(type) {
	case *logicalop.LogicalCTE:
		// we pass GE rather than logical plan, it's a super set of LogicalPlan interface, which enable cascades
		// framework to iterate its children, and then get their logical property. Meanwhile, we can also get basic
		// wrapped logical plan from GE, so we can use same function pointer to handle logic inside.
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalCTE(x, prop)
	case *logicalop.LogicalSort:
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalSort(x, prop)
	case *logicalop.LogicalTopN:
		// check planner/core/exhaust_physical_plans.go to see why return a slice of slice for topn/limit.
		return physicalop.ExhaustPhysicalPlans4LogicalTopN(x, prop)
	case *logicalop.LogicalLock:
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalLock(x, prop)
	case *logicalop.LogicalJoin:
		ops, hintCanWork, err = utilfuncp.ExhaustPhysicalPlans4LogicalJoin(e, prop)
	case *logicalop.LogicalApply:
		ops, hintCanWork, err = utilfuncp.ExhaustPhysicalPlans4LogicalApply(e, prop)
	case *logicalop.LogicalLimit:
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalLimit(x, prop)
	case *logicalop.LogicalWindow:
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalWindow(x, prop)
	case *logicalop.LogicalExpand:
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalExpand(x, prop)
	case *logicalop.LogicalUnionAll:
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalUnionAll(x, prop)
	case *logicalop.LogicalSequence:
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalSequence(e, prop)
	case *logicalop.LogicalSelection:
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalSelection(x, prop)
	case *logicalop.LogicalMaxOneRow:
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalMaxOneRow(x, prop)
	case *logicalop.LogicalUnionScan:
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalUnionScan(x, prop)
	case *logicalop.LogicalProjection:
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalProjection(e, prop)
	case *logicalop.LogicalAggregation:
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalAggregation(x, prop)
	case *logicalop.LogicalPartitionUnionAll:
		ops, hintCanWork, err = physicalop.ExhaustPhysicalPlans4LogicalPartitionUnionAll(x, prop)
	default:
		panic("unreachable")
	}

	if len(ops) == 0 || err != nil {
		return nil, hintCanWork, err
	}
	return [][]base.PhysicalPlan{ops}, hintCanWork, nil
}

// FindBestTask implements LogicalPlan.<3rd> interface, it's used to override the wrapped logicalPlans.
func (e *GroupExpression) FindBestTask(prop *property.PhysicalProperty) (bestTask base.Task, err error) {
	return physicalop.FindBestTask(e, prop)
}
