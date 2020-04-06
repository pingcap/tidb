// Copyright 2018 PingCAP, Inc.
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

package property

import (
	"fmt"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/util/codec"
)

// wholeTaskTypes records all possible kinds of task that a plan can return. For Agg, TopN and Limit, we will try to get
// these tasks one by one.
var wholeTaskTypes = []TaskType{CopSingleReadTaskType, CopDoubleReadTaskType, RootTaskType}

// Item wraps the column and its order.
type Item struct {
	Col  *expression.Column
	Desc bool
}

// PhysicalProperty stands for the required physical property by parents.
// It contains the orders and the task types.
type PhysicalProperty struct {
	Items []Item

	// TaskTp means the type of task that an operator requires.
	//
	// It needs to be specified because two different tasks can't be compared
	// with cost directly. e.g. If a copTask takes less cost than a rootTask,
	// we can't sure that we must choose the former one. Because the copTask
	// must be finished and increase its cost in sometime, but we can't make
	// sure the finishing time. So the best way to let the comparison fair is
	// to add TaskType to required property.
	TaskTp TaskType

	// ExpectedCnt means this operator may be closed after fetching ExpectedCnt
	// records.
	ExpectedCnt float64

	// hashcode stores the hash code of a PhysicalProperty, will be lazily
	// calculated when function "HashCode()" being called.
	hashcode []byte

	// whether need to enforce property.
	Enforced bool
}

// NewPhysicalProperty builds property from columns.
func NewPhysicalProperty(taskTp TaskType, cols []*expression.Column, desc bool, expectCnt float64, enforced bool) *PhysicalProperty {
	return &PhysicalProperty{
		Items:       ItemsFromCols(cols, desc),
		TaskTp:      taskTp,
		ExpectedCnt: expectCnt,
		Enforced:    enforced,
	}
}

// ItemsFromCols builds property items from columns.
func ItemsFromCols(cols []*expression.Column, desc bool) []Item {
	items := make([]Item, 0, len(cols))
	for _, col := range cols {
		items = append(items, Item{Col: col, Desc: desc})
	}
	return items
}

// AllColsFromSchema checks whether all the columns needed by this physical
// property can be found in the given schema.
func (p *PhysicalProperty) AllColsFromSchema(schema *expression.Schema) bool {
	for _, col := range p.Items {
		if schema.ColumnIndex(col.Col) == -1 {
			return false
		}
	}
	return true
}

// IsFlashOnlyProp return true if this physical property is only allowed to generate flash related task
func (p *PhysicalProperty) IsFlashOnlyProp() bool {
	return p.TaskTp == CopTiFlashLocalReadTaskType || p.TaskTp == CopTiFlashGlobalReadTaskType
}

func (p *PhysicalProperty) GetAllPossibleChildTaskTypes() []TaskType {
	if p.TaskTp == RootTaskType {
		return wholeTaskTypes
	}
	// todo for CopSingleReadTaskType and CopDoubleReadTaskType, this function should never be called
	return []TaskType{p.TaskTp}
}

// IsPrefix checks whether the order property is the prefix of another.
func (p *PhysicalProperty) IsPrefix(prop *PhysicalProperty) bool {
	if len(p.Items) > len(prop.Items) {
		return false
	}
	for i := range p.Items {
		if !p.Items[i].Col.Equal(nil, prop.Items[i].Col) || p.Items[i].Desc != prop.Items[i].Desc {
			return false
		}
	}
	return true
}

// IsEmpty checks whether the order property is empty.
func (p *PhysicalProperty) IsEmpty() bool {
	return len(p.Items) == 0
}

// HashCode calculates hash code for a PhysicalProperty object.
func (p *PhysicalProperty) HashCode() []byte {
	if p.hashcode != nil {
		return p.hashcode
	}
	hashcodeSize := 8 + 8 + 8 + (16+8)*len(p.Items) + 8
	p.hashcode = make([]byte, 0, hashcodeSize)
	if p.Enforced {
		p.hashcode = codec.EncodeInt(p.hashcode, 1)
	} else {
		p.hashcode = codec.EncodeInt(p.hashcode, 0)
	}
	p.hashcode = codec.EncodeInt(p.hashcode, int64(p.TaskTp))
	p.hashcode = codec.EncodeFloat(p.hashcode, p.ExpectedCnt)
	for _, item := range p.Items {
		p.hashcode = append(p.hashcode, item.Col.HashCode(nil)...)
		if item.Desc {
			p.hashcode = codec.EncodeInt(p.hashcode, 1)
		} else {
			p.hashcode = codec.EncodeInt(p.hashcode, 0)
		}
	}
	return p.hashcode
}

// String implements fmt.Stringer interface. Just for test.
func (p *PhysicalProperty) String() string {
	return fmt.Sprintf("Prop{cols: %v, TaskTp: %s, expectedCount: %v}", p.Items, p.TaskTp, p.ExpectedCnt)
}

// Clone returns a copy of PhysicalProperty. Currently, this function is only used to build new
// required property for children plan in `exhaustPhysicalPlans`, so we don't copy `Enforced` field
// because if `Enforced` is true, the `Items` must be empty now, this makes `Enforced` meaningless
// for children nodes.
func (p *PhysicalProperty) Clone() *PhysicalProperty {
	prop := &PhysicalProperty{
		Items:       p.Items,
		TaskTp:      p.TaskTp,
		ExpectedCnt: p.ExpectedCnt,
	}
	return prop
}

// AllSameOrder checks if all the items have same order.
func (p *PhysicalProperty) AllSameOrder() (bool, bool) {
	if len(p.Items) == 0 {
		return true, false
	}
	for i := 1; i < len(p.Items); i++ {
		if p.Items[i].Desc != p.Items[i-1].Desc {
			return false, false
		}
	}
	return true, p.Items[0].Desc
}
