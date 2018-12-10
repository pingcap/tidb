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

// PhysicalProperty stands for the required physical property by parents.
// It contains the orders, if the order is desc and the task types.
type PhysicalProperty struct {
	Cols []*expression.Column
	Desc bool

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

// AllColsFromSchema checks whether all the columns needed by this physical
// property can be found in the given schema.
func (p *PhysicalProperty) AllColsFromSchema(schema *expression.Schema) bool {
	return schema.ColumnsIndices(p.Cols) != nil
}

// IsPrefix checks whether the order property is the prefix of another.
func (p *PhysicalProperty) IsPrefix(prop *PhysicalProperty) bool {
	if len(p.Cols) > len(prop.Cols) || p.Desc != prop.Desc {
		return false
	}
	for i := range p.Cols {
		if !p.Cols[i].Equal(nil, prop.Cols[i]) {
			return false
		}
	}
	return true
}

// IsEmpty checks whether the order property is empty.
func (p *PhysicalProperty) IsEmpty() bool {
	return len(p.Cols) == 0
}

// HashCode calculates hash code for a PhysicalProperty object.
func (p *PhysicalProperty) HashCode() []byte {
	if p.hashcode != nil {
		return p.hashcode
	}
	hashcodeSize := 8 + 8 + 8 + 16*len(p.Cols) + 8
	p.hashcode = make([]byte, 0, hashcodeSize)
	if p.Desc {
		p.hashcode = codec.EncodeInt(p.hashcode, 1)
	} else {
		p.hashcode = codec.EncodeInt(p.hashcode, 0)
	}
	if p.Enforced {
		p.hashcode = codec.EncodeInt(p.hashcode, 1)
	} else {
		p.hashcode = codec.EncodeInt(p.hashcode, 0)
	}
	p.hashcode = codec.EncodeInt(p.hashcode, int64(p.TaskTp))
	p.hashcode = codec.EncodeFloat(p.hashcode, p.ExpectedCnt)
	for i, length := 0, len(p.Cols); i < length; i++ {
		p.hashcode = append(p.hashcode, p.Cols[i].HashCode(nil)...)
	}
	return p.hashcode
}

// String implements fmt.Stringer interface. Just for test.
func (p *PhysicalProperty) String() string {
	return fmt.Sprintf("Prop{cols: %v, desc: %v, TaskTp: %s, expectedCount: %v}", p.Cols, p.Desc, p.TaskTp, p.ExpectedCnt)
}

// Clone returns a copy of PhysicalProperty. Currently, this function is only used to build new
// required property for children plan in `exhaustPhysicalPlans`, so we don't copy `Enforced` field
// because if `Enforced` is true, the `Cols` must be empty now, this makes `Enforced` meaningless
// for children nodes.
func (p *PhysicalProperty) Clone() *PhysicalProperty {
	prop := &PhysicalProperty{
		Cols:        p.Cols,
		Desc:        p.Desc,
		TaskTp:      p.TaskTp,
		ExpectedCnt: p.ExpectedCnt,
	}
	return prop
}
