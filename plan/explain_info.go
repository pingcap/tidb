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

var (
	_ PhysicalPlan = &Selection{}
	_ PhysicalPlan = &Projection{}
	_ PhysicalPlan = &TableDual{}
	_ PhysicalPlan = &Union{}
	_ PhysicalPlan = &Sort{}
	_ PhysicalPlan = &Limit{}
	_ PhysicalPlan = &PhysicalTableScan{}
	_ PhysicalPlan = &PhysicalAggregation{}

	_ PhysicalPlan = &PhysicalIndexScan{}
	_ PhysicalPlan = &Exists{}
	_ PhysicalPlan = &MaxOneRow{}
	_ PhysicalPlan = &Update{}
	_ PhysicalPlan = &Delete{}
	_ PhysicalPlan = &SelectLock{}
	_ PhysicalPlan = &Show{}
	_ PhysicalPlan = &Insert{}
	_ PhysicalPlan = &PhysicalApply{}
	_ PhysicalPlan = &PhysicalHashJoin{}
	_ PhysicalPlan = &PhysicalHashSemiJoin{}
	_ PhysicalPlan = &PhysicalMergeJoin{}
	_ PhysicalPlan = &PhysicalUnionScan{}
	_ PhysicalPlan = &Cache{}
)

// ExplainInfo implements PhysicalPlan interface.
func (p *Selection) ExplainInfo() string {
	buffer := bytes.NewBufferString("conditions:")
	for i, expr := range p.Conditions {
		buffer.WriteString(expr.ExplainInfo())
		if i+1 < len(p.Conditions) {
			buffer.WriteString(", ")
		}
	}
	return buffer.String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *Projection) ExplainInfo() string {
	buffer := bytes.NewBufferString("expressions:")
	for i, expr := range p.Exprs {
		buffer.WriteString(expr.ExplainInfo())
		if i+1 < len(p.Exprs) {
			buffer.WriteString(", ")
		}
	}
	return buffer.String()
}

// ExplainInfo implements PhysicalPlan interface.
func (p *TableDual) ExplainInfo() string {
	return fmt.Sprintf("output row count: %v", p.RowCount)
}

// ExplainInfo implements PhysicalPlan interface.
func (p *Union) ExplainInfo() string {
	return ""
}

// ExplainInfo implements PhysicalPlan interface.
func (p *Sort) ExplainInfo() string {
	buffer := bytes.NewBufferString("sort by:")
	for i, item := range p.ByItems {
		buffer.WriteString(fmt.Sprintf("[%s,", item.Expr.ExplainInfo()))
		if item.Desc {
			buffer.WriteString("desc]")
		} else {
			buffer.WriteString("asc]")
		}
		if i+1<len(p.ByItems) {
			buffer.WriteString(", ")
		}
	}
	return buffer.String()
}

func (p *Limit) ExplainInfo() string {
	return fmt.Sprintf("offset:%v, count:%v", p.Offset, p.Count)
}

func (p *PhysicalTableScan) ExplainInfo() string {
	buffer := bytes.NewBufferString(fmt.Sprintf("table: %s", p.Table.Name.O))
	if p.TableAsName != nil && p.TableAsName.L != "" {
		buffer.WriteString(fmt.Sprintf(" as %s", p.TableAsName.O))
	}
	if p.pkCol != nil {
		buffer.WriteString(fmt.Sprintf(", primiary key: %s", p.pkCol.ExplainInfo()))
	}
	return buffer.String()
}

func (p *PhysicalAggregation) ExplainInfo() string {
	buffer := bytes.NewBufferString("")
	if len(p.AggFuncs)>0 {
		for i, agg := range p.AggFuncs {
			buffer.WriteString(expression.ExplainAggFunc(agg))
			if i+1<len(p.AggFuncs) {
				buffer.WriteString(", ")
			}
		}
	}

	buffer.WriteString(", type:")
	switch p.AggType {
	case StreamedAgg:
		buffer.WriteString("streamed")
	case FinalAgg:
		buffer.WriteString("final")
	case CompleteAgg:
		buffer.WriteString("complete")
	}
	return buffer.String()
}
