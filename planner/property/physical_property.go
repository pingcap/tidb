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

// SortItem wraps the column and its order.
type SortItem struct {
	Col  *expression.Column
	Desc bool
}

// PartitionType is the way to partition during mpp data exchanging.
type PartitionType int

const (
	// AnyType will not require any special partition types.
	AnyType PartitionType = iota
	// BroadcastType requires current task to broadcast its data.
	BroadcastType
	// HashType requires current task to shuffle its data according to some columns.
	HashType
)

// PhysicalProperty stands for the required physical property by parents.
// It contains the orders and the task types.
type PhysicalProperty struct {
	// SortItems contains the required sort attributes.
	SortItems []SortItem

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

	// If the partition type is hash, the data should be reshuffled by partition cols.
	PartitionCols []*expression.Column

	// which types the exchange sender belongs to, only take effects when it's a mpp task.
	PartitionTp PartitionType
}

// NewPhysicalProperty builds property from columns.
func NewPhysicalProperty(taskTp TaskType, cols []*expression.Column, desc bool, expectCnt float64, enforced bool) *PhysicalProperty {
	return &PhysicalProperty{
		SortItems:   SortItemsFromCols(cols, desc),
		TaskTp:      taskTp,
		ExpectedCnt: expectCnt,
		Enforced:    enforced,
	}
}

// SortItemsFromCols builds property items from columns.
func SortItemsFromCols(cols []*expression.Column, desc bool) []SortItem {
	items := make([]SortItem, 0, len(cols))
	for _, col := range cols {
		items = append(items, SortItem{Col: col, Desc: desc})
	}
	return items
}

// IsSubsetOf check if the keys can match the needs of partition.
func (p *PhysicalProperty) IsSubsetOf(keys []*expression.Column) []int {
	if len(p.PartitionCols) > len(keys) {
		return nil
	}
	matches := make([]int, 0, len(keys))
	for _, partCol := range p.PartitionCols {
		found := false
		for i, key := range keys {
			if partCol.Equal(nil, key) {
				found = true
				matches = append(matches, i)
				break
			}
		}
		if !found {
			return nil
		}
	}
	return matches
}

// AllColsFromSchema checks whether all the columns needed by this physical
// property can be found in the given schema.
func (p *PhysicalProperty) AllColsFromSchema(schema *expression.Schema) bool {
	for _, col := range p.SortItems {
		if schema.ColumnIndex(col.Col) == -1 {
			return false
		}
	}
	return true
}

// IsFlashProp return true if this physical property is only allowed to generate flash related task
func (p *PhysicalProperty) IsFlashProp() bool {
	return p.TaskTp == CopTiFlashLocalReadTaskType || p.TaskTp == CopTiFlashGlobalReadTaskType || p.TaskTp == MppTaskType
}

// GetAllPossibleChildTaskTypes enumrates the possible types of tasks for children.
func (p *PhysicalProperty) GetAllPossibleChildTaskTypes() []TaskType {
	if p.TaskTp == RootTaskType {
		return wholeTaskTypes
	}
	// TODO: For CopSingleReadTaskType and CopDoubleReadTaskType, this function should never be called
	return []TaskType{p.TaskTp}
}

// IsPrefix checks whether the order property is the prefix of another.
func (p *PhysicalProperty) IsPrefix(prop *PhysicalProperty) bool {
	if len(p.SortItems) > len(prop.SortItems) {
		return false
	}
	for i := range p.SortItems {
		if !p.SortItems[i].Col.Equal(nil, prop.SortItems[i].Col) || p.SortItems[i].Desc != prop.SortItems[i].Desc {
			return false
		}
	}
	return true
}

// IsEmpty checks whether the order property is empty.
func (p *PhysicalProperty) IsEmpty() bool {
	return len(p.SortItems) == 0
}

// HashCode calculates hash code for a PhysicalProperty object.
func (p *PhysicalProperty) HashCode() []byte {
	if p.hashcode != nil {
		return p.hashcode
	}
	hashcodeSize := 8 + 8 + 8 + (16+8)*len(p.SortItems) + 8
	p.hashcode = make([]byte, 0, hashcodeSize)
	if p.Enforced {
		p.hashcode = codec.EncodeInt(p.hashcode, 1)
	} else {
		p.hashcode = codec.EncodeInt(p.hashcode, 0)
	}
	p.hashcode = codec.EncodeInt(p.hashcode, int64(p.TaskTp))
	p.hashcode = codec.EncodeFloat(p.hashcode, p.ExpectedCnt)
	for _, item := range p.SortItems {
		p.hashcode = append(p.hashcode, item.Col.HashCode(nil)...)
		if item.Desc {
			p.hashcode = codec.EncodeInt(p.hashcode, 1)
		} else {
			p.hashcode = codec.EncodeInt(p.hashcode, 0)
		}
	}
	if p.TaskTp == MppTaskType {
		p.hashcode = codec.EncodeInt(p.hashcode, int64(p.PartitionTp))
		for _, col := range p.PartitionCols {
			p.hashcode = append(p.hashcode, col.HashCode(nil)...)
		}
	}
	return p.hashcode
}

// String implements fmt.Stringer interface. Just for test.
func (p *PhysicalProperty) String() string {
	return fmt.Sprintf("Prop{cols: %v, TaskTp: %s, expectedCount: %v}", p.SortItems, p.TaskTp, p.ExpectedCnt)
}

// Clone returns a copy of PhysicalProperty. Currently, this function is only used to build new
// required property for children plan in `exhaustPhysicalPlans`, so we don't copy `Enforced` field
// because if `Enforced` is true, the `SortItems` must be empty now, this makes `Enforced` meaningless
// for children nodes.
func (p *PhysicalProperty) Clone() *PhysicalProperty {
	prop := &PhysicalProperty{
		SortItems:     p.SortItems,
		TaskTp:        p.TaskTp,
		ExpectedCnt:   p.ExpectedCnt,
		Enforced:      p.Enforced,
		PartitionTp:   p.PartitionTp,
		PartitionCols: p.PartitionCols,
	}
	return prop
}

// AllSameOrder checks if all the items have same order.
func (p *PhysicalProperty) AllSameOrder() (bool, bool) {
	if len(p.SortItems) == 0 {
		return true, false
	}
	for i := 1; i < len(p.SortItems); i++ {
		if p.SortItems[i].Desc != p.SortItems[i-1].Desc {
			return false, false
		}
	}
	return true, p.SortItems[0].Desc
}
