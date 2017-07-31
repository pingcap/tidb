// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"bytes"
	"fmt"

	"github.com/pingcap/tidb/expression"
)

func setParents4FinalPlan(plan PhysicalPlan) {
	allPlans := []PhysicalPlan{plan}
	planMark := map[string]bool{}
	planMark[plan.ID()] = true
	for pID := 0; pID < len(allPlans); pID++ {
		allPlans[pID].SetParents()
		switch copPlan := allPlans[pID].(type) {
		case *PhysicalTableReader:
			setParents4FinalPlan(copPlan.tablePlan)
		case *PhysicalIndexReader:
			setParents4FinalPlan(copPlan.indexPlan)
		case *PhysicalIndexLookUpReader:
			setParents4FinalPlan(copPlan.indexPlan)
			setParents4FinalPlan(copPlan.tablePlan)
		}
		for _, p := range allPlans[pID].Children() {
			if !planMark[p.ID()] {
				allPlans = append(allPlans, p.(PhysicalPlan))
				planMark[p.ID()] = true
			}
		}
	}

	allPlans = allPlans[0:1]
	planMark[plan.ID()] = false
	for pID := 0; pID < len(allPlans); pID++ {
		for _, p := range allPlans[pID].Children() {
			p.AddParent(allPlans[pID])
			if planMark[p.ID()] {
				planMark[p.ID()] = false
				allPlans = append(allPlans, p.(PhysicalPlan))
			}
		}
	}
}

// ExplainInfo implements PhysicalPlan interface.
func (p *Selection) ExplainInfo() string {
	return string(expression.ExplainExpressionList(p.Conditions))
}

// ExplainInfo implements PhysicalPlan interface.
func (p *Projection) ExplainInfo() string {
	return string(expression.ExplainExpressionList(p.Exprs))
}

// ExplainInfo implements PhysicalPlan interface.
func (p *TableDual) ExplainInfo() string {
	return fmt.Sprintf("rows:%v", p.RowCount)
}

// ExplainInfo implements PhysicalPlan interface.
func (p *Sort) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	for i, item := range p.ByItems {
		order := "asc"
		if item.Desc {
			order = "desc"
		}
		buffer.WriteString(fmt.Sprintf("%s:%s", item.Expr.ExplainInfo(), order))
		if i+1 < len(p.ByItems) {
			buffer.WriteString(", ")
		}
	}
	return buffer.String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *Limit) ExplainInfo() string {
	return fmt.Sprintf("offset:%v, count:%v", p.Offset, p.Count)
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalAggregation) ExplainInfo() string {
	buffer := bytes.NewBufferString(fmt.Sprintf("type:%s", p.AggType))
	if p.HasGby && len(p.GroupByItems) > 0 {
		buffer.WriteString(fmt.Sprintf(", group by:%s",
			expression.ExplainExpressionList(p.GroupByItems)))
	}
	buffer.WriteString(", funcs:")
	for i, agg := range p.AggFuncs {
		buffer.WriteString(expression.ExplainAggFunc(agg))
		if i+1 < len(p.AggFuncs) {
			buffer.WriteString(", ")
		}
	}
	return buffer.String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalApply) ExplainInfo() string {
	buffer := bytes.NewBufferString(p.PhysicalJoin.ExplainInfo())
	buffer.WriteString(fmt.Sprintf(", right:%s", p.Children()[p.rightChOffset].ID()))
	return buffer.String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalIndexJoin) ExplainInfo() string {
	buffer := bytes.NewBufferString(fmt.Sprintf("outer:%s",
		p.Children()[p.outerIndex].ID()))
	if len(p.OuterJoinKeys) > 0 {
		buffer.WriteString(fmt.Sprintf(", outer key:%s",
			expression.ExplainColumnList(p.OuterJoinKeys)))
	}
	if len(p.InnerJoinKeys) > 0 {
		buffer.WriteString(fmt.Sprintf(", inner key:%s",
			expression.ExplainColumnList(p.InnerJoinKeys)))
	}
	if len(p.LeftConditions) > 0 {
		buffer.WriteString(fmt.Sprintf(", left cond:%s",
			expression.ExplainExpressionList(p.LeftConditions)))
	}
	if len(p.RightConditions) > 0 {
		buffer.WriteString(fmt.Sprintf(", right cond:%s",
			expression.ExplainExpressionList(p.RightConditions)))
	}
	if len(p.OtherConditions) > 0 {
		buffer.WriteString(fmt.Sprintf(", other cond:%s",
			expression.ExplainExpressionList(p.OtherConditions)))
	}
	return buffer.String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalHashJoin) ExplainInfo() string {
	buffer := bytes.NewBufferString(p.JoinType.String())
	buffer.WriteString(fmt.Sprintf(", small:%s", p.Children()[p.SmallTable].ID()))
	if len(p.EqualConditions) > 0 {
		buffer.WriteString(fmt.Sprintf(", equal:%s", p.EqualConditions))
	}
	if len(p.LeftConditions) > 0 {
		buffer.WriteString(fmt.Sprintf(", left cond:%s", p.LeftConditions))
	}
	if len(p.RightConditions) > 0 {
		buffer.WriteString(fmt.Sprintf(", right cond:%s",
			expression.ExplainExpressionList(p.RightConditions)))
	}
	if len(p.OtherConditions) > 0 {
		buffer.WriteString(fmt.Sprintf(", other cond:%s",
			expression.ExplainExpressionList(p.OtherConditions)))
	}
	return buffer.String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalHashSemiJoin) ExplainInfo() string {
	buffer := bytes.NewBufferString(fmt.Sprintf("right:%s", p.Children()[1].ID()))
	if p.WithAux {
		buffer.WriteString(", aux")
	}
	if p.Anti {
		buffer.WriteString(", anti")
	}
	if len(p.EqualConditions) > 0 {
		buffer.WriteString(fmt.Sprintf(", equal:%s", p.EqualConditions))
	}
	if len(p.LeftConditions) > 0 {
		buffer.WriteString(fmt.Sprintf(", left cond:%s", p.LeftConditions))
	}
	if len(p.RightConditions) > 0 {
		buffer.WriteString(fmt.Sprintf(", right cond:%s",
			expression.ExplainExpressionList(p.RightConditions)))
	}
	if len(p.OtherConditions) > 0 {
		buffer.WriteString(fmt.Sprintf(", other cond:%s",
			expression.ExplainExpressionList(p.OtherConditions)))
	}
	return buffer.String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalMergeJoin) ExplainInfo() string {
	buffer := bytes.NewBufferString(p.JoinType.String())
	if len(p.EqualConditions) > 0 {
		buffer.WriteString(fmt.Sprintf(", equal:%s", p.EqualConditions))
	}
	if len(p.LeftConditions) > 0 {
		buffer.WriteString(fmt.Sprintf(", left cond:%s", p.LeftConditions))
	}
	if len(p.RightConditions) > 0 {
		buffer.WriteString(fmt.Sprintf(", right cond:%s",
			expression.ExplainExpressionList(p.RightConditions)))
	}
	if len(p.OtherConditions) > 0 {
		buffer.WriteString(fmt.Sprintf(", other cond:%s",
			expression.ExplainExpressionList(p.OtherConditions)))
	}
	if p.Desc {
		buffer.WriteString("desc")
	} else {
		buffer.WriteString("asc")
	}
	if len(p.leftKeys) > 0 {
		buffer.WriteString(fmt.Sprintf("left key:%s",
			expression.ExplainColumnList(p.leftKeys)))
	}
	if len(p.rightKeys) > 0 {
		buffer.WriteString(fmt.Sprintf("right key:%s",
			expression.ExplainColumnList(p.rightKeys)))
	}
	return buffer.String()
}
