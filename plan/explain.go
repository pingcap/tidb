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
	fmt.Fprintf(buffer, "table:%s", tblName)
	if len(p.Index.Columns) > 0 {
		buffer.WriteString(", index:")
		for i, idxCol := range p.Index.Columns {
			buffer.WriteString(idxCol.Name.O)
			if i+1 < len(p.Index.Columns) {
				buffer.WriteString(", ")
			}
		}
	}
	haveCorCol := false
	for _, cond := range p.AccessCondition {
		if len(expression.ExtractCorColumns(cond)) > 0 {
			haveCorCol = true
			break
		}
	}
	if len(p.rangeDecidedBy) > 0 {
		fmt.Fprintf(buffer, ", range: decided by %s", p.rangeDecidedBy)
	} else if haveCorCol {
		fmt.Fprintf(buffer, ", range: decided by %v", p.AccessCondition)
	} else if len(p.Ranges) > 0 {
		fmt.Fprint(buffer, ", range:")
		for i, idxRange := range p.Ranges {
			fmt.Fprint(buffer, idxRange.String())
			if i+1 < len(p.Ranges) {
				fmt.Fprint(buffer, ", ")
			}
		}
	}
	fmt.Fprintf(buffer, ", keep order:%v", p.KeepOrder)
	if p.Desc {
		buffer.WriteString(", desc")
	}
	if p.stats.usePseudoStats {
		buffer.WriteString(", stats:pseudo")
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
	fmt.Fprintf(buffer, "table:%s", tblName)
	if p.pkCol != nil {
		fmt.Fprintf(buffer, ", pk col:%s", p.pkCol.ExplainInfo())
	}
	haveCorCol := false
	for _, cond := range p.AccessCondition {
		if len(expression.ExtractCorColumns(cond)) > 0 {
			haveCorCol = true
			break
		}
	}
	if len(p.rangeDecidedBy) > 0 {
		fmt.Fprintf(buffer, ", range: decided by %s", p.rangeDecidedBy)
	} else if haveCorCol {
		fmt.Fprintf(buffer, ", range: decided by %v", p.AccessCondition)
	} else if len(p.Ranges) > 0 {
		fmt.Fprint(buffer, ", range:")
		for i, idxRange := range p.Ranges {
			fmt.Fprint(buffer, idxRange.String())
			if i+1 < len(p.Ranges) {
				fmt.Fprint(buffer, ", ")
			}
		}
	}
	fmt.Fprintf(buffer, ", keep order:%v", p.KeepOrder)
	if p.Desc {
		buffer.WriteString(", desc")
	}
	if p.stats.usePseudoStats {
		buffer.WriteString(", stats:pseudo")
	}
	return buffer.String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalTableReader) ExplainInfo() string {
	return "data:" + p.tablePlan.ExplainID()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalIndexReader) ExplainInfo() string {
	return "index:" + p.indexPlan.ExplainID()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalIndexLookUpReader) ExplainInfo() string {
	// The children can be inferred by the relation symbol.
	return ""
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalUnionScan) ExplainInfo() string {
	return string(expression.SortedExplainExpressionList(p.Conditions))
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalSelection) ExplainInfo() string {
	return string(expression.SortedExplainExpressionList(p.Conditions))
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
		fmt.Fprintf(buffer, "%s:%s", item.Expr.ExplainInfo(), order)
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
		fmt.Fprintf(buffer, "group by:%s, ",
			expression.SortedExplainExpressionList(p.GroupByItems))
	}
	if len(p.AggFuncs) > 0 {
		buffer.WriteString("funcs:")
		for i, agg := range p.AggFuncs {
			buffer.WriteString(aggregation.ExplainAggFunc(agg))
			if i+1 < len(p.AggFuncs) {
				buffer.WriteString(", ")
			}
		}
	}
	return buffer.String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalApply) ExplainInfo() string {
	buffer := bytes.NewBufferString(p.PhysicalJoin.ExplainInfo())
	return buffer.String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalIndexJoin) ExplainInfo() string {
	buffer := bytes.NewBufferString(p.JoinType.String())
	fmt.Fprintf(buffer, ", inner:%s", p.Children()[1-p.OuterIndex].ExplainID())
	if len(p.OuterJoinKeys) > 0 {
		fmt.Fprintf(buffer, ", outer key:%s",
			expression.ExplainColumnList(p.OuterJoinKeys))
	}
	if len(p.InnerJoinKeys) > 0 {
		fmt.Fprintf(buffer, ", inner key:%s",
			expression.ExplainColumnList(p.InnerJoinKeys))
	}
	if len(p.LeftConditions) > 0 {
		fmt.Fprintf(buffer, ", left cond:%s",
			expression.SortedExplainExpressionList(p.LeftConditions))
	}
	if len(p.RightConditions) > 0 {
		fmt.Fprintf(buffer, ", right cond:%s",
			expression.SortedExplainExpressionList(p.RightConditions))
	}
	if len(p.OtherConditions) > 0 {
		fmt.Fprintf(buffer, ", other cond:%s",
			expression.SortedExplainExpressionList(p.OtherConditions))
	}
	return buffer.String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalHashJoin) ExplainInfo() string {
	buffer := bytes.NewBufferString(p.JoinType.String())
	fmt.Fprintf(buffer, ", inner:%s", p.Children()[p.InnerChildIdx].ExplainID())
	if len(p.EqualConditions) > 0 {
		fmt.Fprintf(buffer, ", equal:%s", p.EqualConditions)
	}
	if len(p.LeftConditions) > 0 {
		fmt.Fprintf(buffer, ", left cond:%s", p.LeftConditions)
	}
	if len(p.RightConditions) > 0 {
		fmt.Fprintf(buffer, ", right cond:%s",
			expression.SortedExplainExpressionList(p.RightConditions))
	}
	if len(p.OtherConditions) > 0 {
		fmt.Fprintf(buffer, ", other cond:%s",
			expression.SortedExplainExpressionList(p.OtherConditions))
	}
	return buffer.String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *PhysicalMergeJoin) ExplainInfo() string {
	buffer := bytes.NewBufferString(p.JoinType.String())
	if len(p.LeftKeys) > 0 {
		fmt.Fprintf(buffer, ", left key:%s",
			expression.ExplainColumnList(p.LeftKeys))
	}
	if len(p.RightKeys) > 0 {
		fmt.Fprintf(buffer, ", right key:%s",
			expression.ExplainColumnList(p.RightKeys))
	}
	if len(p.LeftConditions) > 0 {
		fmt.Fprintf(buffer, ", left cond:%s", p.LeftConditions)
	}
	if len(p.RightConditions) > 0 {
		fmt.Fprintf(buffer, ", right cond:%s",
			expression.SortedExplainExpressionList(p.RightConditions))
	}
	if len(p.OtherConditions) > 0 {
		fmt.Fprintf(buffer, ", other cond:%s",
			expression.SortedExplainExpressionList(p.OtherConditions))
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
		fmt.Fprintf(buffer, "%s:%s", item.Expr.ExplainInfo(), order)
		if i+1 < len(p.ByItems) {
			buffer.WriteString(", ")
		}
	}
	fmt.Fprintf(buffer, ", offset:%v, count:%v", p.Offset, p.Count)
	return buffer.String()
}
