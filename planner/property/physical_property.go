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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package property

import (
	"bytes"
	"fmt"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tipb/go-tipb"
)

// wholeTaskTypes records all possible kinds of task that a plan can return. For Agg, TopN and Limit, we will try to get
// these tasks one by one.
var wholeTaskTypes = []TaskType{CopSingleReadTaskType, CopDoubleReadTaskType, RootTaskType}

// SortItem wraps the column and its order.
type SortItem struct {
	Col  *expression.Column
	Desc bool
}

func (s *SortItem) String() string {
	if s.Desc {
		return fmt.Sprintf("{%s desc}", s.Col)
	}
	return fmt.Sprintf("{%s asc}", s.Col)
}

// Clone makes a copy of SortItem.
func (s SortItem) Clone() SortItem {
	return SortItem{Col: s.Col.Clone().(*expression.Column), Desc: s.Desc}
}

// MPPPartitionType is the way to partition during mpp data exchanging.
type MPPPartitionType int

const (
	// AnyType will not require any special partition types.
	AnyType MPPPartitionType = iota
	// BroadcastType requires current task to broadcast its data.
	BroadcastType
	// HashType requires current task to shuffle its data according to some columns.
	HashType
	// SinglePartitionType requires all the task pass the data to one node (tidb/tiflash).
	SinglePartitionType
)

// ToExchangeType generates ExchangeType from MPPPartitionType
func (t MPPPartitionType) ToExchangeType() tipb.ExchangeType {
	switch t {
	case BroadcastType:
		return tipb.ExchangeType_Broadcast
	case HashType:
		return tipb.ExchangeType_Hash
	case SinglePartitionType:
		return tipb.ExchangeType_PassThrough
	default:
		log.Warn("generate an exchange with any partition type, which is illegal.")
		return tipb.ExchangeType_PassThrough
	}
}

// MPPPartitionColumn is the column that will be used in MPP Hash Exchange
type MPPPartitionColumn struct {
	Col       *expression.Column
	CollateID int32
}

func (partitionCol *MPPPartitionColumn) hashCode(ctx *stmtctx.StatementContext) []byte {
	hashcode := partitionCol.Col.HashCode(ctx)
	if partitionCol.CollateID < 0 {
		// collateId < 0 means new collation is not enabled
		hashcode = codec.EncodeInt(hashcode, int64(partitionCol.CollateID))
	} else {
		hashcode = codec.EncodeInt(hashcode, 1)
	}
	return hashcode
}

// Equal returns true if partitionCol == other
func (partitionCol *MPPPartitionColumn) Equal(other *MPPPartitionColumn) bool {
	if partitionCol.CollateID < 0 {
		// collateId only matters if new collation is enabled
		if partitionCol.CollateID != other.CollateID {
			return false
		}
	}
	return partitionCol.Col.Equal(nil, other.Col)
}

// ExplainColumnList generates explain information for a list of columns.
func ExplainColumnList(cols []*MPPPartitionColumn) []byte {
	buffer := bytes.NewBufferString("")
	for i, col := range cols {
		buffer.WriteString("[name: ")
		buffer.WriteString(col.Col.ExplainInfo())
		buffer.WriteString(", collate: ")
		if collate.NewCollationEnabled() {
			buffer.WriteString(GetCollateNameByIDForPartition(col.CollateID))
		} else {
			buffer.WriteString("N/A")
		}
		buffer.WriteString("]")
		if i+1 < len(cols) {
			buffer.WriteString(", ")
		}
	}
	return buffer.Bytes()
}

// GetCollateIDByNameForPartition returns collate id by collation name
func GetCollateIDByNameForPartition(coll string) int32 {
	collateID := int32(collate.CollationName2ID(coll))
	return collate.RewriteNewCollationIDIfNeeded(collateID)
}

// GetCollateNameByIDForPartition returns collate id by collation name
func GetCollateNameByIDForPartition(collateID int32) string {
	collateID = collate.RestoreCollationIDIfNeeded(collateID)
	return collate.CollationID2Name(collateID)
}

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

	// indicates that whether we are allowed to add an enforcer.
	CanAddEnforcer bool

	// If the partition type is hash, the data should be reshuffled by partition cols.
	MPPPartitionCols []*MPPPartitionColumn

	// which types the exchange sender belongs to, only take effects when it's a mpp task.
	MPPPartitionTp MPPPartitionType

	// SortItemsForPartition means these sort only need to sort the data of one partition, instead of global.
	// It is added only if it is used to sort the sharded data of the window function.
	// Non-MPP tasks do not care about it.
	SortItemsForPartition []SortItem

	// RejectSort means rejecting the sort property from its children, but it only works for MPP tasks.
	// Non-MPP tasks do not care about it.
	RejectSort bool
}

// NewPhysicalProperty builds property from columns.
func NewPhysicalProperty(taskTp TaskType, cols []*expression.Column, desc bool, expectCnt float64, enforced bool) *PhysicalProperty {
	return &PhysicalProperty{
		SortItems:      SortItemsFromCols(cols, desc),
		TaskTp:         taskTp,
		ExpectedCnt:    expectCnt,
		CanAddEnforcer: enforced,
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
func (p *PhysicalProperty) IsSubsetOf(keys []*MPPPartitionColumn) []int {
	if len(p.MPPPartitionCols) > len(keys) {
		return nil
	}
	matches := make([]int, 0, len(keys))
	for _, partCol := range p.MPPPartitionCols {
		found := false
		for i, key := range keys {
			if partCol.Equal(key) {
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
	return p.TaskTp == MppTaskType
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

// IsSortItemAllForPartition check whether SortItems is same as SortItemsForPartition
func (p *PhysicalProperty) IsSortItemAllForPartition() bool {
	if len(p.SortItemsForPartition) != len(p.SortItems) {
		return false
	}
	for i := range p.SortItemsForPartition {
		if !p.SortItemsForPartition[i].Col.Equal(nil, p.SortItems[i].Col) || p.SortItemsForPartition[i].Desc != p.SortItems[i].Desc {
			return false
		}
	}
	return true
}

// IsSortItemEmpty checks whether the order property is empty.
func (p *PhysicalProperty) IsSortItemEmpty() bool {
	return len(p.SortItems) == 0
}

// HashCode calculates hash code for a PhysicalProperty object.
func (p *PhysicalProperty) HashCode() []byte {
	if p.hashcode != nil {
		return p.hashcode
	}
	hashcodeSize := 8 + 8 + 8 + (16+8)*len(p.SortItems) + 8
	p.hashcode = make([]byte, 0, hashcodeSize)
	if p.CanAddEnforcer {
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
		p.hashcode = codec.EncodeInt(p.hashcode, int64(p.MPPPartitionTp))
		for _, col := range p.MPPPartitionCols {
			p.hashcode = append(p.hashcode, col.hashCode(nil)...)
		}
	}
	return p.hashcode
}

// String implements fmt.Stringer interface. Just for test.
func (p *PhysicalProperty) String() string {
	return fmt.Sprintf("Prop{cols: %v, TaskTp: %s, expectedCount: %v}", p.SortItems, p.TaskTp, p.ExpectedCnt)
}

// CloneEssentialFields returns a copy of PhysicalProperty. We only copy the essential fields that really indicate the
// property, specifically, `CanAddEnforcer` should not be included.
func (p *PhysicalProperty) CloneEssentialFields() *PhysicalProperty {
	prop := &PhysicalProperty{
		SortItems:             p.SortItems,
		SortItemsForPartition: p.SortItemsForPartition,
		TaskTp:                p.TaskTp,
		ExpectedCnt:           p.ExpectedCnt,
		MPPPartitionTp:        p.MPPPartitionTp,
		MPPPartitionCols:      p.MPPPartitionCols,
		RejectSort:            p.RejectSort,
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
