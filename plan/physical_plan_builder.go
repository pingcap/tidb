// Copyright 2016 PingCAP, Inc.
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

// Convert2PhysicalPlan implements LogicalPlan Convert2PhysicalPlan interface.
func (p *DataSource) Convert2PhysicalPlan() (np PhysicalPlan) {
	indices, includeTableScan := availableIndices(p.table)
	if includeTableScan && len(indices) == 0 {
		np = &PhysicalTableScan{
			Table:       p.Table,
			Columns:     p.Columns,
			TableAsName: p.TableAsName,
			DBName:      p.DBName,
		}
	} else {
		// TODO: Temporily we choose a random index.
		np = &PhysicalIndexScan{
			Index:       indices[0],
			Table:       p.Table,
			Columns:     p.Columns,
			TableAsName: p.TableAsName,
			DBName:      p.DBName,
		}
	}
	np.SetSchema(p.GetSchema())
	return np
}

// Convert2PhysicalPlan implements LogicalPlan Convert2PhysicalPlan interface.
func (p *Limit) Convert2PhysicalPlan() PhysicalPlan {
	child := p.GetChildByIndex(0).(LogicalPlan).Convert2PhysicalPlan()
	p.SetChildren(child)
	child.SetParents(p)
	return p
}

// Convert2PhysicalPlan implements LogicalPlan Convert2PhysicalPlan interface.
func (p *Join) Convert2PhysicalPlan() PhysicalPlan {
	l := p.GetChildByIndex(0).(LogicalPlan).Convert2PhysicalPlan()
	r := p.GetChildByIndex(1).(LogicalPlan).Convert2PhysicalPlan()
	p.SetChildren(l, r)
	l.SetParents(p)
	r.SetParents(p)
	return p
}

// Convert2PhysicalPlan implements LogicalPlan Convert2PhysicalPlan interface.
func (p *Aggregation) Convert2PhysicalPlan() PhysicalPlan {
	child := p.GetChildByIndex(0).(LogicalPlan).Convert2PhysicalPlan()
	p.SetChildren(child)
	child.SetParents(p)
	return p
}

// Convert2PhysicalPlan implements LogicalPlan Convert2PhysicalPlan interface.
func (p *NewUnion) Convert2PhysicalPlan() PhysicalPlan {
	newChildren := make([]Plan, len(p.GetChildren()))
	for i, child := range p.GetChildren() {
		newChildren[i] = child.(LogicalPlan).Convert2PhysicalPlan()
		child.SetParents(p)
	}
	p.SetChildren(newChildren...)
	return p
}

// Convert2PhysicalPlan implements LogicalPlan Convert2PhysicalPlan interface.
func (p *Selection) Convert2PhysicalPlan() PhysicalPlan {
	child := p.GetChildByIndex(0).(LogicalPlan).Convert2PhysicalPlan()
	p.SetChildren(child)
	child.SetParents(p)
	return p
}

// Convert2PhysicalPlan implements LogicalPlan Convert2PhysicalPlan interface.
func (p *Projection) Convert2PhysicalPlan() PhysicalPlan {
	child := p.GetChildByIndex(0).(LogicalPlan).Convert2PhysicalPlan()
	p.SetChildren(child)
	child.SetParents(p)
	return p
}

// Convert2PhysicalPlan implements LogicalPlan Convert2PhysicalPlan interface.
func (p *NewSort) Convert2PhysicalPlan() PhysicalPlan {
	child := p.GetChildByIndex(0).(LogicalPlan).Convert2PhysicalPlan()
	p.SetChildren(child)
	child.SetParents(p)
	return p
}

// Convert2PhysicalPlan implements LogicalPlan Convert2PhysicalPlan interface.
func (p *Apply) Convert2PhysicalPlan() PhysicalPlan {
	child := p.GetChildByIndex(0).(LogicalPlan).Convert2PhysicalPlan()
	np := &PhysicalApply{
		OuterSchema: p.OuterSchema,
		Checker:     p.Checker,
		InnerPlan:   p.InnerPlan.Convert2PhysicalPlan(),
	}
	np.SetSchema(p.GetSchema())
	np.SetChildren(child)
	child.SetParents(np)
	return np
}

// Convert2PhysicalPlan implements LogicalPlan Convert2PhysicalPlan interface.
func (p *Distinct) Convert2PhysicalPlan() PhysicalPlan {
	child := p.GetChildByIndex(0).(LogicalPlan).Convert2PhysicalPlan()
	p.SetChildren(child)
	child.SetParents(p)
	return p
}

// Convert2PhysicalPlan implements LogicalPlan Convert2PhysicalPlan interface.
func (p *NewTableDual) Convert2PhysicalPlan() PhysicalPlan {
	return p
}

// Convert2PhysicalPlan implements LogicalPlan Convert2PhysicalPlan interface.
func (p *MaxOneRow) Convert2PhysicalPlan() PhysicalPlan {
	child := p.GetChildByIndex(0).(LogicalPlan).Convert2PhysicalPlan()
	p.SetChildren(child)
	child.SetParents(p)
	return p
}

// Convert2PhysicalPlan implements LogicalPlan Convert2PhysicalPlan interface.
func (p *Exists) Convert2PhysicalPlan() PhysicalPlan {
	child := p.GetChildByIndex(0).(LogicalPlan).Convert2PhysicalPlan()
	p.SetChildren(child)
	child.SetParents(p)
	return p
}

// Convert2PhysicalPlan implements LogicalPlan Convert2PhysicalPlan interface.
func (p *Trim) Convert2PhysicalPlan() PhysicalPlan {
	child := p.GetChildByIndex(0).(LogicalPlan).Convert2PhysicalPlan()
	p.SetChildren(child)
	child.SetParents(p)
	return p
}

// Convert2PhysicalPlan implements LogicalPlan Convert2PhysicalPlan interface.
func (p *Insert) Convert2PhysicalPlan() PhysicalPlan {
	if len(p.GetChildren()) == 0 {
		return p
	}
	child := p.GetChildByIndex(0).(LogicalPlan).Convert2PhysicalPlan()
	p.SetChildren(child)
	child.SetParents(p)
	p.SelectPlan = child
	return p
}
