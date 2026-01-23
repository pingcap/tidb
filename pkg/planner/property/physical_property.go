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
	"unsafe"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/intset"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tipb/go-tipb"
)

// wholeTaskTypes records all possible kinds of task that a plan can return. For Agg, TopN and Limit, we will try to get
// these tasks one by one.
var wholeTaskTypes = []TaskType{CopSingleReadTaskType, CopMultiReadTaskType, RootTaskType}

// SortItem wraps the column and its order.
type SortItem struct {
	Col  *expression.Column
	Desc bool
}

// Hash64 implements the HashEquals interface.
func (s *SortItem) Hash64(h base.Hasher) {
	if s.Col == nil {
		h.HashByte(base.NilFlag)
	} else {
		h.HashByte(base.NotNilFlag)
		s.Col.Hash64(h)
	}
	h.HashBool(s.Desc)
}

// Equals implements the HashEquals interface.
func (s *SortItem) Equals(other any) bool {
	s2, ok := other.(*SortItem)
	if !ok {
		return false
	}
	if s == nil {
		return s2 == nil
	}
	if s2 == nil {
		return false
	}
	return s.Col.Equals(s2.Col) && s.Desc == s2.Desc
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

// MemoryUsage return the memory usage of SortItem
func (s SortItem) MemoryUsage() (sum int64) {
	sum = size.SizeOfBool
	if s.Col != nil {
		sum += s.Col.MemoryUsage()
	}
	return
}

// ExplainPartitionBy produce text for p.PartitionBy. Common for window functions and TopN.
func ExplainPartitionBy(ctx expression.EvalContext, buffer *bytes.Buffer,
	partitionBy []SortItem, normalized bool) *bytes.Buffer {
	if len(partitionBy) > 0 {
		buffer.WriteString("partition by ")
		for i, item := range partitionBy {
			fmt.Fprintf(buffer, "%s", item.Col.ColumnExplainInfo(ctx, normalized))
			if i+1 < len(partitionBy) {
				buffer.WriteString(", ")
			}
		}
	}
	return buffer
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

// ResolveIndices resolve index for MPPPartitionColumn
func (partitionCol *MPPPartitionColumn) ResolveIndices(schema *expression.Schema) (*MPPPartitionColumn, error) {
	newColExpr, err := partitionCol.Col.ResolveIndices(schema)
	if err != nil {
		return nil, err
	}
	newCol, _ := newColExpr.(*expression.Column)
	return &MPPPartitionColumn{
		Col:       newCol,
		CollateID: partitionCol.CollateID,
	}, nil
}

// Clone makes a copy of MPPPartitionColumn.
func (partitionCol *MPPPartitionColumn) Clone() *MPPPartitionColumn {
	return &MPPPartitionColumn{
		Col:       partitionCol.Col.Clone().(*expression.Column),
		CollateID: partitionCol.CollateID,
	}
}

func (partitionCol *MPPPartitionColumn) hashCode() []byte {
	hashcode := partitionCol.Col.HashCode()
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
	return partitionCol.Col.EqualColumn(other.Col)
}

// MemoryUsage return the memory usage of MPPPartitionColumn
func (partitionCol *MPPPartitionColumn) MemoryUsage() (sum int64) {
	if partitionCol == nil {
		return
	}

	sum = size.SizeOfInt32
	if partitionCol.Col != nil {
		sum += partitionCol.Col.MemoryUsage()
	}
	return
}

// ChoosePartitionKeys chooses partition keys according to the matches.
func ChoosePartitionKeys(keys []*MPPPartitionColumn, matches []int) []*MPPPartitionColumn {
	newKeys := make([]*MPPPartitionColumn, 0, len(matches))
	for _, id := range matches {
		newKeys = append(newKeys, keys[id])
	}
	return newKeys
}

// ExplainColumnList generates explain information for a list of columns.
func ExplainColumnList(ctx expression.EvalContext, cols []*MPPPartitionColumn) []byte {
	buffer := bytes.NewBufferString("")
	for i, col := range cols {
		buffer.WriteString("[name: ")
		buffer.WriteString(col.Col.ExplainInfo(ctx))
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

// cteProducerStatus indicates whether we can let the current CTE consumer/reader be executed on the MPP nodes.
type cteProducerStatus int

// Constants for CTE status.
const (
	NoCTEOrAllProducerCanMPP cteProducerStatus = iota
	SomeCTEFailedMpp
	AllCTECanMpp
)

// PhysicalPropMatchResult describes the result of matching PhysicalProperty against an access path.
type PhysicalPropMatchResult int

const (
	// PropNotMatched means the access path cannot satisfy the required order.
	PropNotMatched PhysicalPropMatchResult = iota
	// PropMatched means the access path can satisfy the required property directly.
	PropMatched
	// PropMatchedNeedMergeSort means the access path can satisfy the required property, but a merge sort between range
	// groups is needed.
	// Corresponding information will be recorded in AccessPath.GroupedRanges and AccessPath.GroupByColIdxs.
	PropMatchedNeedMergeSort
)

// Matched returns true if the required order can be satisfied.
func (r PhysicalPropMatchResult) Matched() bool {
	return r == PropMatched || r == PropMatchedNeedMergeSort
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

	CTEProducerStatus cteProducerStatus

	VectorProp struct {
		*expression.VSInfo
		TopK uint32
	}

	IndexJoinProp *IndexJoinRuntimeProp

	// NoCopPushDown indicates if planner must not push this agg down to coprocessor.
	// It is true when the agg is in the outer child tree of apply.
	NoCopPushDown bool

	// PartialOrderInfo is used for TopN's partial order optimization.
	// When this field is not nil, it indicates that prefix index can be used
	// to provide partial order for TopN.
	// For example:
	// query: order by a, b limit 10
	// partialOrderInfo: sortItems: [a, b]
	// The partialOrderInfo property will pass through to the datasource and try to matchPartialOrderProperty such as:
	// index: (a, b(10) )
	PartialOrderInfo *PartialOrderInfo
}

// PartialOrderInfo records information needed for partial order optimization.
// When PhysicalProperty.PartialOrderInfo is not nil, it indicates that
// prefix index can be used to provide partial order.
type PartialOrderInfo struct {
	// SortItems are the ORDER BY columns from TopN
	SortItems []*SortItem

	// PrefixColID is the UniqueID of the prefix index column
	// This field is set by matchPartialOrderProperty in skylinePruning
	PrefixColID int64

	// PrefixLen is the length (in bytes) of the prefix index
	// This field is set by matchPartialOrderProperty in skylinePruning
	PrefixLen int
}

// AllSameOrder checks if all the items have same order.
func (p *PartialOrderInfo) AllSameOrder() (isSame bool, desc bool) {
	if len(p.SortItems) == 0 {
		return true, false
	}
	for i := 1; i < len(p.SortItems); i++ {
		if p.SortItems[i].Desc != p.SortItems[i-1].Desc {
			return
		}
	}
	return true, p.SortItems[0].Desc
}

// IndexJoinRuntimeProp is the inner runtime property for index join.
type IndexJoinRuntimeProp struct {
	// for complete the last col range access, cuz its runtime constant.
	OtherConditions []expression.Expression
	// for filling the range msg info
	OuterJoinKeys []*expression.Column
	// for inner ds/index to detect the range, cuz its runtime constant.
	InnerJoinKeys []*expression.Column
	// AvgInnerRowCnt is computed from join.EqualCondCount / outerChild.RowCount.
	// since ds only can build empty range before seeing runtime data, the so inner
	// ds can get an accurate countAfterAccess. Once index join prop pushed to the
	// deeper side like through join, the deeper DS's countAfterAccess should be
	// thought twice.
	AvgInnerRowCnt float64
	// since tableRangeScan and indexRangeScan can't be told which one is better at
	// copTask phase because of the latter attached operators into cop and the single
	// and double reader cost consideration. Therefore, we introduce another bool to
	// indicate prefer tableRangeScan or indexRangeScan each at a time.
	TableRangeScan bool
}

// NewPhysicalProperty builds property from columns.
func NewPhysicalProperty(taskTp TaskType, cols []*expression.Column,
	desc bool, expectCnt float64, enforced bool) *PhysicalProperty {
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

// NeedMPPExchangeByEquivalence checks if the keys can match the needs of partition with equivalence.
// "Equivalence" refers to the process where we utilize a hash column to obtain equivalent columns,
// and then use these equivalent columns to compare with the MPP partition column to determine whether an exchange is
// necessary.
//
// for example:
//  1. requiredPartitionColumn: [18，13，16]
//  2. currentPartitionColumn: 9
//  3. FD: (1)-->(2-6,8), ()-->(7), (9)-->(10-17), (1,10)==(1,10), (18,21)-->(19,20,22-33), (9,18)==(9,18)
//     In this case, we can see that the child supplied partition keys is subset of parent required partition cols.
func (p *PhysicalProperty) NeedMPPExchangeByEquivalence(
	currentPartitionColumn []*MPPPartitionColumn, fd *funcdep.FDSet) bool {
	requiredPartitionCols := p.MPPPartitionCols
	uniqueID2requiredPartitionCols := make(map[*MPPPartitionColumn]intset.FastIntSet, len(requiredPartitionCols))
	// for each partition column, we calculate the equivalence alternative closure of it.
	for _, pCol := range requiredPartitionCols {
		uniqueID2requiredPartitionCols[pCol] = fd.ClosureOfEquivalence(intset.NewFastIntSet(int(pCol.Col.UniqueID)))
	}

	// there is a subset theorem here, if the child supplied keys is a subset of parent required mpp partition cols,
	// the mpp partition exchanger can also be eliminated.
SubsetLoop:
	for _, key := range currentPartitionColumn {
		for pCol, equivSet := range uniqueID2requiredPartitionCols {
			if checkEquivalence(equivSet, key, pCol) {
				// yes, child can supply the same col partition prop. continue to next child supplied key.
				continue SubsetLoop
			}
		}
		// once a child supplied keys can't find direct/in-direct equiv all parent required partition cols,
		// we can break the subset check.
		//
		// it's subset case, we don't need to add exchanger.
		// once there is a column outside the parent required partition cols, we need to add exchanger.
		// we build a  case like:
		// parent prop require: partition cols:   1, 2, 3
		// the child can supply: partition cols:  1, 4, 5
		// fd: {2,3,4} = {2,3,4}
		// column 5 will mixture the data distribute, even if parent required columns are all satisfied by child.
		return true
	}

	return false
}

func checkEquivalence(equivSet intset.FastIntSet, key, pCol *MPPPartitionColumn) bool {
	// if the equiv set contain the key, it means it can supply the same col partition prop directly or in-indirectly.
	// according to the old logic, when the child can supply the same key partition, we should check its collate-id
	// when the new collation is enabled suggested by the collateID is negative. Or new collation is not set.
	return equivSet.Has(int(key.Col.UniqueID)) &&
		((key.CollateID < 0 && pCol.CollateID == key.CollateID) || key.CollateID >= 0)
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
		if !p.SortItems[i].Col.EqualColumn(prop.SortItems[i].Col) || p.SortItems[i].Desc != prop.SortItems[i].Desc {
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
		if !p.SortItemsForPartition[i].Col.EqualColumn(p.SortItems[i].Col) ||
			p.SortItemsForPartition[i].Desc != p.SortItems[i].Desc {
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
	if p.PartialOrderInfo != nil {
		hashcodeSize += (16 + 8) * len(p.PartialOrderInfo.SortItems)
	} else {
		hashcodeSize += 8
	}
	p.hashcode = make([]byte, 0, hashcodeSize)
	if p.CanAddEnforcer {
		p.hashcode = codec.EncodeInt(p.hashcode, 1)
	} else {
		p.hashcode = codec.EncodeInt(p.hashcode, 0)
	}
	p.hashcode = codec.EncodeInt(p.hashcode, int64(p.TaskTp))
	p.hashcode = codec.EncodeFloat(p.hashcode, p.ExpectedCnt)
	for _, item := range p.SortItems {
		p.hashcode = append(p.hashcode, item.Col.HashCode()...)
		if item.Desc {
			p.hashcode = codec.EncodeInt(p.hashcode, 1)
		} else {
			p.hashcode = codec.EncodeInt(p.hashcode, 0)
		}
	}
	if p.TaskTp == MppTaskType {
		p.hashcode = codec.EncodeInt(p.hashcode, int64(p.MPPPartitionTp))
		for _, col := range p.MPPPartitionCols {
			p.hashcode = append(p.hashcode, col.hashCode()...)
		}
		if p.VectorProp.VSInfo != nil {
			// We only accept the vector information from the TopN which is directly above the DataSource.
			// So it's safe to not hash the vector constant.
			p.hashcode = append(p.hashcode, p.VectorProp.Column.HashCode()...)
			p.hashcode = codec.EncodeInt(p.hashcode, int64(p.VectorProp.FnPbCode))
		}
	}
	p.hashcode = append(p.hashcode, codec.EncodeInt(nil, int64(p.CTEProducerStatus))...)
	// encode indexJoinProp into physical prop's hashcode.
	if p.IndexJoinProp != nil {
		for _, expr := range p.IndexJoinProp.OtherConditions {
			p.hashcode = append(p.hashcode, expr.HashCode()...)
		}
		for _, col := range p.IndexJoinProp.OuterJoinKeys {
			p.hashcode = append(p.hashcode, col.HashCode()...)
		}
		for _, col := range p.IndexJoinProp.InnerJoinKeys {
			p.hashcode = append(p.hashcode, col.HashCode()...)
		}
		p.hashcode = codec.EncodeFloat(p.hashcode, p.IndexJoinProp.AvgInnerRowCnt)
		if p.IndexJoinProp.TableRangeScan {
			p.hashcode = codec.EncodeInt(p.hashcode, 1)
		} else {
			p.hashcode = codec.EncodeInt(p.hashcode, 0)
		}
	}
	// encode NoCopPushDown into physical prop's hashcode.
	if p.NoCopPushDown {
		p.hashcode = codec.EncodeInt(p.hashcode, 1)
	} else {
		p.hashcode = codec.EncodeInt(p.hashcode, 0)
	}
	// encode PartialOrderInfo into physical prop's hashcode.
	if p.PartialOrderInfo != nil {
		p.hashcode = codec.EncodeInt(p.hashcode, 1)
		for _, item := range p.PartialOrderInfo.SortItems {
			p.hashcode = append(p.hashcode, item.Col.HashCode()...)
			if item.Desc {
				p.hashcode = codec.EncodeInt(p.hashcode, 1)
			} else {
				p.hashcode = codec.EncodeInt(p.hashcode, 0)
			}
		}
	} else {
		p.hashcode = codec.EncodeInt(p.hashcode, 0)
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
		CTEProducerStatus:     p.CTEProducerStatus,
		NoCopPushDown:         p.NoCopPushDown,
		PartialOrderInfo:      p.PartialOrderInfo, // Copy PartialOrderInfo for TopN partial order optimization
		// we default not to clone basic indexJoinProp by default.
		// and only call admitIndexJoinProp to inherit the indexJoinProp for special pattern operators.
	}
	return prop
}

// AllSameOrder checks if all the items have same order.
func (p *PhysicalProperty) AllSameOrder() (isSame bool, desc bool) {
	if len(p.SortItems) == 0 {
		return true, false
	}
	for i := 1; i < len(p.SortItems); i++ {
		if p.SortItems[i].Desc != p.SortItems[i-1].Desc {
			return
		}
	}
	return true, p.SortItems[0].Desc
}

// NeedKeepOrder returns whether the property requires maintaining order.
// It handles both normal sorting (SortItems) and partial order (PartialOrderInfo).
func (p *PhysicalProperty) NeedKeepOrder() bool {
	return !p.IsSortItemEmpty() || p.PartialOrderInfo != nil
}

// GetSortDesc returns the sort direction (descending or not).
// It prioritizes PartialOrderInfo over SortItems.
// This method reuses the existing AllSameOrder methods.
func (p *PhysicalProperty) GetSortDesc() bool {
	if p.PartialOrderInfo != nil {
		_, desc := p.PartialOrderInfo.AllSameOrder()
		return desc
	}
	_, desc := p.AllSameOrder()
	return desc
}

// GetSortItemsForKeepOrder returns the sort items used for KeepOrder.
// It prioritizes PartialOrderInfo over SortItems.
// Returns a copy of SortItems (converting from []*SortItem to []SortItem if from PartialOrderInfo).
func (p *PhysicalProperty) GetSortItemsForKeepOrder() []SortItem {
	if p.PartialOrderInfo != nil && len(p.PartialOrderInfo.SortItems) > 0 {
		items := make([]SortItem, 0, len(p.PartialOrderInfo.SortItems))
		for _, si := range p.PartialOrderInfo.SortItems {
			items = append(items, *si)
		}
		return items
	}
	return p.SortItems
}

const emptyPhysicalPropertySize = int64(unsafe.Sizeof(PhysicalProperty{}))

// MemoryUsage return the memory usage of PhysicalProperty
func (p *PhysicalProperty) MemoryUsage() (sum int64) {
	if p == nil {
		return
	}

	sum = emptyPhysicalPropertySize + int64(cap(p.hashcode))
	for _, sortItem := range p.SortItems {
		sum += sortItem.MemoryUsage()
	}
	for _, sortItem := range p.SortItemsForPartition {
		sum += sortItem.MemoryUsage()
	}
	for _, mppCol := range p.MPPPartitionCols {
		sum += mppCol.MemoryUsage()
	}
	return
}

// NeedEnforceExchanger checks if we need to enforce an exchange operator on the top of the mpp task.
func NeedEnforceExchanger(mtp MPPPartitionType, mHashCols []*MPPPartitionColumn,
	prop *PhysicalProperty, fd *funcdep.FDSet) bool {
	switch prop.MPPPartitionTp {
	case AnyType:
		return false
	case BroadcastType:
		return true
	case SinglePartitionType:
		return mtp != SinglePartitionType
	default:
		if mtp != HashType {
			return true
		}
		// for example, if already partitioned by hash(B,C), then same (A,B,C) must distribute on a same node.
		if fd != nil && len(mHashCols) != 0 {
			return prop.NeedMPPExchangeByEquivalence(mHashCols, fd)
		}
		if len(prop.MPPPartitionCols) != len(mHashCols) {
			return true
		}
		for i, col := range prop.MPPPartitionCols {
			if !col.Equal(mHashCols[i]) {
				return true
			}
		}
		return false
	}
}
