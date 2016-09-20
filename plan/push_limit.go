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

func insertLimit(p PhysicalPlan, l *Limit) *Limit {
	l.SetSchema(p.GetSchema())
	l.SetChildren(p)
	p.SetParents(l)
	return l
}

// PushLimit implements PhysicalPlan PushLimit interface.
func (p *Limit) PushLimit(l *Limit) PhysicalPlan {
	child := p.GetChildByIndex(0).(PhysicalPlan)
	newChild := child.PushLimit(p)
	if l != nil {
		p.Count = l.Count
		p.Offset = l.Offset
		p.SetChildren(newChild)
		newChild.SetParents(p)
		return p
	}
	return newChild
}

// PushLimit implements PhysicalPlan PushLimit interface.
func (p *Sort) PushLimit(l *Limit) PhysicalPlan {
	child := p.GetChildByIndex(0).(PhysicalPlan)
	newChild := child.PushLimit(nil)
	p.ExecLimit = l
	p.SetChildren(newChild)
	newChild.SetParents(p)
	return p
}

// PushLimit implements PhysicalPlan PushLimit interface.
func (p *Selection) PushLimit(l *Limit) PhysicalPlan {
	child := p.GetChildByIndex(0).(PhysicalPlan)
	newChild := child.PushLimit(nil)
	if l != nil {
		return insertLimit(p, l)
	}
	p.SetChildren(newChild)
	newChild.SetParents(l)
	return p
}

// PushLimit implements PhysicalPlan PushLimit interface.
func (p *PhysicalHashSemiJoin) PushLimit(l *Limit) PhysicalPlan {
	lChild := p.GetChildByIndex(0).(PhysicalPlan)
	rChild := p.GetChildByIndex(1).(PhysicalPlan)
	var newLChild, newRChild PhysicalPlan
	if p.WithAux {
		newLChild = lChild.PushLimit(l)
		newRChild = rChild.PushLimit(nil)
	} else {
		newLChild = lChild.PushLimit(nil)
		newRChild = rChild.PushLimit(nil)
	}
	p.SetChildren(newLChild, newRChild)
	newLChild.SetParents(p)
	newRChild.SetParents(p)
	if l != nil && !p.WithAux {
		return insertLimit(p, l)
	}
	return p
}

// PushLimit implements PhysicalPlan PushLimit interface.
func (p *PhysicalHashJoin) PushLimit(l *Limit) PhysicalPlan {
	lChild := p.GetChildByIndex(0).(PhysicalPlan)
	rChild := p.GetChildByIndex(1).(PhysicalPlan)
	var newLChild, newRChild PhysicalPlan
	if p.JoinType == LeftOuterJoin {
		if l != nil {
			limit2Push := *l
			limit2Push.Count += limit2Push.Offset
			limit2Push.Offset = 0
			newLChild = lChild.PushLimit(&limit2Push)
		} else {
			newLChild = lChild.PushLimit(nil)
		}
		newRChild = rChild.PushLimit(nil)
	} else if p.JoinType == RightOuterJoin {
		if l != nil {
			limit2Push := *l
			limit2Push.Count += limit2Push.Offset
			limit2Push.Offset = 0
			newRChild = rChild.PushLimit(&limit2Push)
		} else {
			newRChild = rChild.PushLimit(nil)
		}
		newLChild = lChild.PushLimit(nil)
	} else {
		newLChild = lChild.PushLimit(nil)
		newRChild = rChild.PushLimit(nil)
	}
	p.SetChildren(newLChild, newRChild)
	newLChild.SetParents(p)
	newRChild.SetParents(p)
	if l != nil {
		return insertLimit(p, l)
	}
	return p
}

// PushLimit implements PhysicalPlan PushLimit interface.
func (p *Union) PushLimit(l *Limit) PhysicalPlan {
	for i, child := range p.GetChildren() {
		if l != nil {
			p.children[i] = child.(PhysicalPlan).PushLimit(&Limit{Count: l.Count + l.Offset})
		} else {
			p.children[i] = child.(PhysicalPlan).PushLimit(nil)
		}
		p.children[i].SetParents(p)
	}
	if l != nil {
		return insertLimit(p, l)
	}
	return p
}

// PushLimit implements PhysicalPlan PushLimit interface.
func (p *Projection) PushLimit(l *Limit) PhysicalPlan {
	newChild := p.GetChildByIndex(0).(PhysicalPlan).PushLimit(l)
	p.SetChildren(newChild)
	newChild.SetParents(p)
	return p
}

// PushLimit implements PhysicalPlan PushLimit interface.
func (p *Trim) PushLimit(l *Limit) PhysicalPlan {
	newChild := p.GetChildByIndex(0).(PhysicalPlan).PushLimit(l)
	p.SetChildren(newChild)
	newChild.SetParents(p)
	return p
}

// PushLimit implements PhysicalPlan PushLimit interface.
func (p *SelectLock) PushLimit(l *Limit) PhysicalPlan {
	newChild := p.GetChildByIndex(0).(PhysicalPlan).PushLimit(l)
	p.SetChildren(newChild)
	newChild.SetParents(p)
	return p
}

// PushLimit implements PhysicalPlan PushLimit interface.
func (p *PhysicalApply) PushLimit(l *Limit) PhysicalPlan {
	p.InnerPlan = p.InnerPlan.PushLimit(nil)
	newChild := p.GetChildByIndex(0).(PhysicalPlan).PushLimit(l)
	p.SetChildren(newChild)
	newChild.SetParents(p)
	return p
}

// PushLimit implements PhysicalPlan PushLimit interface.
func (p *PhysicalAggregation) PushLimit(l *Limit) PhysicalPlan {
	newChild := p.GetChildByIndex(0).(PhysicalPlan).PushLimit(nil)
	p.SetChildren(newChild)
	newChild.SetParents(p)
	if l != nil {
		return insertLimit(p, l)
	}
	return p
}

// PushLimit implements PhysicalPlan PushLimit interface.
func (p *Distinct) PushLimit(l *Limit) PhysicalPlan {
	newChild := p.GetChildByIndex(0).(PhysicalPlan).PushLimit(nil)
	p.SetChildren(newChild)
	newChild.SetParents(p)
	if l != nil {
		return insertLimit(p, l)
	}
	return p
}

// PushLimit implements PhysicalPlan PushLimit interface.
func (p *MaxOneRow) PushLimit(_ *Limit) PhysicalPlan {
	newChild := p.GetChildByIndex(0).(PhysicalPlan).PushLimit(&Limit{Count: 2})
	p.SetChildren(newChild)
	newChild.SetParents(p)
	return p
}

// PushLimit implements PhysicalPlan PushLimit interface.
func (p *Exists) PushLimit(_ *Limit) PhysicalPlan {
	newChild := p.GetChildByIndex(0).(PhysicalPlan).PushLimit(&Limit{Count: 1})
	p.SetChildren(newChild)
	newChild.SetParents(p)
	return p
}

// PushLimit implements PhysicalPlan PushLimit interface.
func (p *PhysicalIndexScan) PushLimit(l *Limit) PhysicalPlan {
	if l != nil {
		count := int64(l.Offset + l.Count)
		p.LimitCount = &count
		if l.Offset != 0 {
			return insertLimit(p, l)
		}
	}
	return p
}

// PushLimit implements PhysicalPlan PushLimit interface.
func (p *PhysicalTableScan) PushLimit(l *Limit) PhysicalPlan {
	if l != nil {
		count := int64(l.Offset + l.Count)
		p.LimitCount = &count
		if l.Offset != 0 {
			return insertLimit(p, l)
		}
	}
	return p
}

// PushLimit implements PhysicalPlan PushLimit interface.
func (p *PhysicalDummyScan) PushLimit(l *Limit) PhysicalPlan {
	return p
}

// PushLimit implements PhysicalPlan PushLimit interface.
func (p *Insert) PushLimit(_ *Limit) PhysicalPlan {
	if len(p.GetChildren()) == 0 {
		return p
	}
	np := p.GetChildByIndex(0).(PhysicalPlan).PushLimit(nil)
	p.SetChildren(np)
	p.SelectPlan = np
	np.SetParents(p)
	return p
}

// PushLimit implements PhysicalPlan PushLimit interface.
func (p *TableDual) PushLimit(l *Limit) PhysicalPlan {
	if l == nil {
		return p
	}
	return insertLimit(p, l)
}

// PushLimit implements PhysicalPlan PushLimit interface.
func (p *Update) PushLimit(_ *Limit) PhysicalPlan {
	if len(p.GetChildren()) == 0 {
		return p
	}
	np := p.GetChildByIndex(0).(PhysicalPlan).PushLimit(nil)
	p.SetChildren(np)
	p.SelectPlan = np
	np.SetParents(p)
	return p
}

// PushLimit implements PhysicalPlan PushLimit interface.
func (p *Delete) PushLimit(_ *Limit) PhysicalPlan {
	if len(p.GetChildren()) == 0 {
		return p
	}
	np := p.GetChildByIndex(0).(PhysicalPlan).PushLimit(nil)
	p.SetChildren(np)
	p.SelectPlan = np
	np.SetParents(p)
	return p
}

// PushLimit implements PhysicalPlan PushLimit interface.
func (p *PhysicalUnionScan) PushLimit(l *Limit) PhysicalPlan {
	np := p.GetChildByIndex(0).(PhysicalPlan).PushLimit(l)
	p.SetChildren(np)
	np.SetParents(p)
	if l != nil {
		return insertLimit(p, l)
	}
	return p
}
