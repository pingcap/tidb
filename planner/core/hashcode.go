// Copyright 2019 PingCAP, Inc.
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

package core

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"unsafe"

	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/plancodec"
)

// EncodeIntAsUint32 append int to []byte.
func EncodeIntAsUint32(result []byte, value int) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(value))
	return append(result, buf[:]...)
}

// EncodeUintAsUint32 append int to []byte.
func EncodeUintAsUint32(result []byte, value uint) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(value))
	return append(result, buf[:]...)
}

// EncodeBool append bool to []byte.
func EncodeBool(result []byte, value bool) []byte {
	if value {
		result = append(result, uint8(1))
	} else {
		result = append(result, uint8(0))
	}
	return result
}

// EncodeBool append uintptr to []byte.
func EncodeUintptr(result []byte, value uintptr) []byte {
	size := unsafe.Sizeof(value)
	var buf [size]byte
	switch size {
	case 4:
		binary.BigEndian.PutUint32(buf[:], uint32(value))
		return append(result, buf[:]...)
	case 8:
		binary.BigEndian.PutUint64(buf[:], uint64(value))
		return append(result, buf[:]...)
	default:
		panic(fmt.Sprintf("unknown uintptr size: %v", size))
	}
	return result
}

// Encode append array to []byte.
// size = Sizeof(len(array)) + len(array) * (Sizeof(len(array[i])) + Sizeof(array[i]))
//		= 4 + len(array) * (4 + Sizeof(array[i]))
func Encode(result []byte, hashCode func(index int) []byte, size int) []byte {
	result = EncodeIntAsUint32(result, size)
	for i := 0; i < size; i++ {
		hashCode := hashCode(i)
		result = EncodeIntAsUint32(result, len(hashCode))
		result = append(result, hashCode...)
	}
	return result
}

// EncodeAndSort sort and append array to []byte.
// size = Sizeof(len(array)) + len(array) * (Sizeof(len(array[i])) + Sizeof(array[i]))
//		= 4 + len(array) * (4 + Sizeof(array[i]))
func EncodeAndSort(result []byte, hashCode func(index int) []byte, size int) []byte {
	hashCodes := make([][]byte, size)
	for i := 0; i < size; i++ {
		hashCodes[i] = hashCode(i)
	}
	sort.Slice(hashCodes, func(i, j int) bool { return bytes.Compare(hashCodes[i], hashCodes[j]) < 0 })
	result = EncodeIntAsUint32(result, size)
	for _, hashCode := range hashCodes {
		result = EncodeIntAsUint32(result, len(hashCode))
		result = append(result, hashCode...)
	}
	return result
}

// HashCode implements LogicalPlan interface.
func (p *baseLogicalPlan) HashCode() []byte {
	// We use PlanID for the default hash, so if two plans do not have
	// the same id, the hash value will never be the same.
	result := make([]byte, 0, 4)
	result = EncodeIntAsUint32(result, p.id)
	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalProjection) HashCode() []byte {
	// PlanType + SelectOffset + Encode(Exprs)
	// Expressions are commonly `Column`s, whose hashcode has the length 9, so
	// we pre-alloc 10 bytes for each expr's hashcode.
	result := make([]byte, 0, 12+len(p.Exprs)*14)
	result = EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = EncodeIntAsUint32(result, p.SelectBlockOffset())
	exprHashCode := func(i int) []byte { return p.Exprs[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = Encode(result, exprHashCode, len(p.Exprs))
	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalTableDual) HashCode() []byte {
	// PlanType + SelectOffset + RowCount
	result := make([]byte, 0, 12)
	result = EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = EncodeIntAsUint32(result, p.SelectBlockOffset())
	result = EncodeIntAsUint32(result, p.RowCount)
	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalSelection) HashCode() []byte {
	// PlanType + SelectOffset + Encode(Conditions)
	// Conditions are commonly `ScalarFunction`s, whose hashcode usually has a
	// length larger than 20, so we pre-alloc 25 bytes for each expr's hashcode.
	result := make([]byte, 0, 12+len(p.Conditions)*29)
	result = EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = EncodeIntAsUint32(result, p.SelectBlockOffset())
	condHashCode := func(i int) []byte { return p.Conditions[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = EncodeAndSort(result, condHashCode, len(p.Conditions))
	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalLimit) HashCode() []byte {
	// PlanType + SelectOffset + Offset + Count
	result := make([]byte, 24)
	binary.BigEndian.PutUint32(result, uint32(plancodec.TypeStringToPhysicalID(p.tp)))
	binary.BigEndian.PutUint32(result[4:], uint32(p.SelectBlockOffset()))
	binary.BigEndian.PutUint64(result[8:], p.Offset)
	binary.BigEndian.PutUint64(result[16:], p.Count)
	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalSort) HashCode() []byte {
	// PlanType + SelectOffset + Encode(ByItems)
	// ByItems are commonly (bool + Column) which hashcode has the length 10,
	// so we pre-alloc 11 bytes for each ByItems's hashcode.
	result := make([]byte, 12+len(p.ByItems)*15)
	binary.BigEndian.PutUint32(result, uint32(plancodec.TypeStringToPhysicalID(p.tp)))
	binary.BigEndian.PutUint32(result[4:], uint32(p.SelectBlockOffset()))
	byItemHashCode := func(i int) []byte { return p.ByItems[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = Encode(result, byItemHashCode, len(p.ByItems))
	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalTopN) HashCode() []byte {
	// PlanType + SelectOffset + Encode(ByItems)
	// ByItems are commonly (bool + Column) which hashcode has the length 10,
	// so we pre-alloc 11 bytes for each ByItems's hashcode.
	result := make([]byte, 28+len(p.ByItems)*15)
	binary.BigEndian.PutUint32(result, uint32(plancodec.TypeStringToPhysicalID(p.tp)))
	binary.BigEndian.PutUint32(result[4:], uint32(p.SelectBlockOffset()))
	binary.BigEndian.PutUint64(result[8:], p.Offset)
	binary.BigEndian.PutUint64(result[16:], p.Count)
	byItemHashCode := func(i int) []byte { return p.ByItems[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = Encode(result, byItemHashCode, len(p.ByItems))
	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalJoin) HashCode() []byte {
	// PlanType + SelectOffset + JoinType + preferJoinType + Encode(EqualConditions) + Encode(LeftConditions) + Encode(RightConditions) + Encode(OtherConditions)
	// Conditions are commonly `ScalarFunction`s, whose hashcode usually has a
	// length larger than 20, so we pre-alloc 25 bytes for each expr's hashcode.
	result := make([]byte, 0, 32+(len(p.EqualConditions)+len(p.LeftConditions)+len(p.RightConditions)+len(p.OtherConditions))*29)
	result = EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = EncodeIntAsUint32(result, p.SelectBlockOffset())
	result = EncodeIntAsUint32(result, int(p.JoinType))
	result = EncodeUintAsUint32(result, p.preferJoinType)

	eqCondHashCode := func(i int) []byte { return p.EqualConditions[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = EncodeAndSort(result, eqCondHashCode, len(p.EqualConditions))

	lHashCode := func(i int) []byte { return p.LeftConditions[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = EncodeAndSort(result, lHashCode, len(p.LeftConditions))

	rHashCode := func(i int) []byte { return p.RightConditions[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = EncodeAndSort(result, rHashCode, len(p.RightConditions))

	otherHashCode := func(i int) []byte { return p.OtherConditions[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = EncodeAndSort(result, otherHashCode, len(p.OtherConditions))

	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalAggregation) HashCode() []byte {
	// PlanType + SelectOffset + AggFuncs[0].Mode + p.aggHints.preferAggType + p.aggHints.preferAggToCop + Encode(AggFuncs) + Encode(GroupByItems)
	// AggFuncs are commonly `ScalarFunction`s, whose hashcode usually has a
	// length larger than 20, so we pre-alloc 25 bytes for each expr's hashcode.
	// ByItems are commonly Column which hashcode has the length 9,
	// so we pre-alloc 10 bytes for each ByItems's hashcode.
	result := make([]byte, 0, 25+len(p.AggFuncs)*29+len(p.GroupByItems)*14)
	result = EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = EncodeIntAsUint32(result, p.SelectBlockOffset())
	result = EncodeIntAsUint32(result, int(p.AggFuncs[0].Mode))

	result = EncodeUintAsUint32(result, uint(p.aggHints.preferAggType))
	result = EncodeBool(result, p.aggHints.preferAggToCop)

	aggFuncHashCode := func(i int) []byte { return p.AggFuncs[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = Encode(result, aggFuncHashCode, len(p.AggFuncs))

	groupByHashCode := func(i int) []byte { return p.GroupByItems[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = EncodeAndSort(result, groupByHashCode, len(p.GroupByItems))

	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalMaxOneRow) HashCode() []byte {
	// PlanType + SelectOffset
	result := make([]byte, 0, 8)
	result = EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = EncodeIntAsUint32(result, p.SelectBlockOffset())
	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalShowDDLJobs) HashCode() []byte {
	// PlanType + SelectOffset + JobNumber
	result := make([]byte, 0, 16)
	result = EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = EncodeIntAsUint32(result, p.SelectBlockOffset())
	binary.BigEndian.PutUint64(result[8:], uint64(p.JobNumber))
	return result
}

// HashCode implements LogicalPlan interface.
func (p *DataSource) HashCode() []byte {
	// PlanType + SelectOffset + id + isPartition + physicalTableID
	// usually we would not copy a dataSource same as other one and we can use PlanID as hashCode.
	// but in rule_partition_processor, it will copy the old dataSource,
	// and only change isPartition,physicalTableID and share the same PlanID.
	// So we should append isPartition and physicalTableID.
	result := make([]byte, 0, 13)
	result = EncodeIntAsUint32(result, p.id)

	result = EncodeBool(result, p.isPartition)
	binary.BigEndian.PutUint64(result[13:], uint64(p.physicalTableID))
	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalIndexScan) HashCode() []byte {
	// PlanType + SelectOffset + IsDoubleRead + EqCondCount + Index.ID + Source.HashCode() +
	// EncodeAndSort(AccessConds) + Encode(FullIdxCols) + Encode(FullIdxCols) + Encode(FullIdxColLens) + Encode(FullIdxCols)
	result := make([]byte, 0, 58+len(p.AccessConds)*29+len(p.FullIdxCols)*9+len(p.FullIdxColLens)*4+len(p.IdxCols)*9+len(p.IdxColLens)*4)
	result = EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = EncodeIntAsUint32(result, p.SelectBlockOffset())
	result = EncodeBool(result, p.IsDoubleRead)
	result = EncodeIntAsUint32(result, p.EqCondCount)
	binary.BigEndian.PutUint64(result[13:], uint64(p.Index.ID))
	result = append(result, p.Source.HashCode()...)

	result = EncodeIntAsUint32(result, len(p.Columns))
	for _, col := range p.Columns {
		result = codec.EncodeInt(result, col.ID)
	}

	accessCondHashCode := func(i int) []byte { return p.AccessConds[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = EncodeAndSort(result, accessCondHashCode, len(p.AccessConds))

	result = EncodeIntAsUint32(result, len(p.FullIdxCols))
	for _, col := range p.FullIdxCols {
		result = append(result, col.HashCode(p.ctx.GetSessionVars().StmtCtx)...)
	}

	result = EncodeIntAsUint32(result, len(p.FullIdxColLens))
	for _, colLen := range p.FullIdxColLens {
		result = EncodeIntAsUint32(result, colLen)
	}

	result = EncodeIntAsUint32(result, len(p.IdxCols))
	for _, col := range p.IdxCols {
		result = append(result, col.HashCode(p.ctx.GetSessionVars().StmtCtx)...)
	}

	result = EncodeIntAsUint32(result, len(p.IdxColLens))
	for _, colLen := range p.IdxColLens {
		result = EncodeIntAsUint32(result, colLen)
	}

	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalTableScan) HashCode() []byte {
	// PlanType + SelectOffset + Source.HashCode() + p.Handle + Encode(AccessConds)
	result := make([]byte, 0, 34+len(p.AccessConds)*29)
	result = EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = EncodeIntAsUint32(result, p.SelectBlockOffset())
	result = append(result, p.Source.HashCode()...)

	result = append(result, p.Handle.HashCode(p.ctx.GetSessionVars().StmtCtx)...)

	accessCondHashCode := func(i int) []byte { return p.AccessConds[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = EncodeAndSort(result, accessCondHashCode, len(p.AccessConds))

	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalUnionAll) HashCode() []byte {
	// PlanType + SelectOffset
	result := make([]byte, 0, 8)
	result = EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = EncodeIntAsUint32(result, p.SelectBlockOffset())
	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalApply) HashCode() []byte {
	// p.LogicalJoin.HashCode() + Encode(CorCols)
	result := p.LogicalJoin.HashCode()
	corColHashCode := func(i int) []byte { return p.CorCols[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = Encode(result, corColHashCode, len(p.CorCols))
	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalWindow) HashCode() []byte {
	result := make([]byte, 0, 48+len(p.WindowFuncDescs)*29+14*(len(p.PartitionBy)+len(p.OrderBy)))
	result = EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = EncodeIntAsUint32(result, p.SelectBlockOffset())

	wfdHashCode := func(i int) []byte { return p.WindowFuncDescs[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = Encode(result, wfdHashCode, len(p.WindowFuncDescs))

	partitionByHashCode := func(i int) []byte { return p.PartitionBy[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = EncodeAndSort(result, partitionByHashCode, len(p.PartitionBy))

	orderByHashCode := func(i int) []byte { return p.OrderBy[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = Encode(result, orderByHashCode, len(p.OrderBy))

	result = append(result, p.Frame.HashCode(p.ctx.GetSessionVars().StmtCtx)...)

	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalMemTable) HashCode() []byte {
	result := make([]byte, 0, 37)
	result = EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = EncodeIntAsUint32(result, p.SelectBlockOffset())

	binary.BigEndian.PutUint64(result[8:], uint64(p.TableInfo.ID))
	result = codec.EncodeCompactBytes(result, hack.Slice(p.DBName.L))
	RangeBytes := make([]byte, 0, 16)
	binary.BigEndian.PutUint64(RangeBytes[:], uint64(p.QueryTimeRange.From.Unix()))
	binary.BigEndian.PutUint64(RangeBytes[8:], uint64(p.QueryTimeRange.To.Unix()))
	result = append(result, RangeBytes...)
	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalUnionScan) HashCode() []byte {
	result := make([]byte, 0, 30+13*(len(p.conditions)))
	result = EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = EncodeIntAsUint32(result, p.SelectBlockOffset())
	result = append(result, p.handleCol.HashCode(p.ctx.GetSessionVars().StmtCtx)...)
	condHashCode := func(i int) []byte { return p.conditions[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = EncodeAndSort(result, condHashCode, len(p.conditions))
	result = append(result, p.handleCol.HashCode(p.ctx.GetSessionVars().StmtCtx)...)
	return result
}

// HashCode implements LogicalPlan interface.
func (p *TiKVSingleGather) HashCode() []byte {
	dsHashCode := p.Source.HashCode()
	result := make([]byte, 0, 16+len(dsHashCode))
	result = EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = EncodeIntAsUint32(result, p.SelectBlockOffset())
	binary.BigEndian.PutUint64(result[8:], uint64(p.Index.ID))
	result = append(dsHashCode, dsHashCode...)
	return result
}
