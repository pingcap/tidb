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
	"github.com/pingcap/tidb/expression/aggregation"
)

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalLock) ExplainInfo() string {
	return p.Lock.String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalIndexScan) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	tblName := p.Table.Name.O
	if p.TableAsName != nil && p.TableAsName.O != "" {
		tblName = p.TableAsName.O
	}
	buffer.WriteString(fmt.Sprintf("table:%s", tblName))
	if len(p.Index.Columns) > 0 {
		buffer.WriteString(", index:")
		for i, idxCol := range p.Index.Columns {
			buffer.WriteString(idxCol.Name.O)
			if i+1 < len(p.Index.Columns) {
				buffer.WriteString(", ")
			}
		}
	}
	if len(p.Ranges) > 0 {
		buffer.WriteString(", range:")
		for i, idxRange := range p.Ranges {
			buffer.WriteString(idxRange.String())
			if i+1 < len(p.Ranges) {
				buffer.WriteString(", ")
			}
		}
	}
	buffer.WriteString(fmt.Sprintf(", out of order:%v", p.OutOfOrder))
	if p.Desc {
		buffer.WriteString(", desc")
	}
	return buffer.String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalTableScan) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	tblName := p.Table.Name.O
	if p.TableAsName != nil && p.TableAsName.O != "" {
		tblName = p.TableAsName.O
	}
	buffer.WriteString(fmt.Sprintf("table:%s", tblName))
	if p.pkCol != nil {
		buffer.WriteString(fmt.Sprintf(", pk col:%s", p.pkCol.ExplainInfo()))
	}
	if len(p.Ranges) > 0 {
		buffer.WriteString(", range:")
		for i, idxRange := range p.Ranges {
			buffer.WriteString(idxRange.String())
			if i+1 < len(p.Ranges) {
				buffer.WriteString(", ")
			}
		}
	}
	buffer.WriteString(fmt.Sprintf(", keep order:%v", p.KeepOrder))
	if p.Desc {
		buffer.WriteString(", desc")
	}
	return buffer.String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalTableReader) ExplainInfo() string {
	return fmt.Sprintf("data:%s", p.tablePlan.ExplainID())
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalIndexReader) ExplainInfo() string {
	return fmt.Sprintf("index:%s", p.indexPlan.ExplainID())
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalIndexLookUpReader) ExplainInfo() string {
	return fmt.Sprintf("index:%s, table:%s", p.indexPlan.ExplainID(), p.tablePlan.ExplainID())
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalUnionScan) ExplainInfo() string {
	return string(expression.ExplainExpressionList(p.Conditions))
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalSelection) ExplainInfo() string {
	return string(expression.ExplainExpressionList(p.Conditions))
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalProjection) ExplainInfo() string {
	return string(expression.ExplainExpressionList(p.Exprs))
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalTableDual) ExplainInfo() string {
	return fmt.Sprintf("rows:%v", p.RowCount)
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalSort) ExplainInfo() string {
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
func (p *PhysicalLimit) ExplainInfo() string {
	return fmt.Sprintf("offset:%v, count:%v", p.Offset, p.Count)
}

// ExplainInfo implements PhysicalPlan interface.
func (p *basePhysicalAgg) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	if len(p.GroupByItems) > 0 {
		buffer.WriteString(fmt.Sprintf("group by:%s",
			expression.ExplainExpressionList(p.GroupByItems)))
	}
	buffer.WriteString(", funcs:")
	for i, agg := range p.AggFuncs {
		buffer.WriteString(aggregation.ExplainAggFunc(agg))
		if i+1 < len(p.AggFuncs) {
			buffer.WriteString(", ")
		}
	}
	return buffer.String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalApply) ExplainInfo() string {
	buffer := bytes.NewBufferString(p.PhysicalJoin.ExplainInfo())
	buffer.WriteString(fmt.Sprintf(", right:%s", p.Children()[1].ExplainID()))
	return buffer.String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalIndexJoin) ExplainInfo() string {
	buffer := bytes.NewBufferString(fmt.Sprintf("outer:%s",
		p.Children()[p.OuterIndex].ExplainID()))
	buffer.WriteString(fmt.Sprintf(", %s", p.JoinType))
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
	buffer.WriteString(fmt.Sprintf(", small:%s", p.Children()[p.SmallChildIdx].ExplainID()))
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
	if len(p.leftKeys) > 0 {
		buffer.WriteString(fmt.Sprintf(", left key:%s",
			expression.ExplainColumnList(p.leftKeys)))
	}
	if len(p.rightKeys) > 0 {
		buffer.WriteString(fmt.Sprintf(", right key:%s",
			expression.ExplainColumnList(p.rightKeys)))
	}
	return buffer.String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalTopN) ExplainInfo() string {
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
	buffer.WriteString(fmt.Sprintf(", offset:%v, count:%v", p.Offset, p.Count))
	return buffer.String()
}
