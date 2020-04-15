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

	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/plancodec"
)

// HashCode implements LogicalPlan interface.
func (p *baseLogicalPlan) HashCode() []byte {
	// We use PlanID for the default hash, so if two plans do not have
	// the same id, the hash value will never be the same.
	result := make([]byte, 0, 4)
	result = codec.EncodeIntAsUint32(result, p.id)
	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalProjection) HashCode() []byte {
	// PlanType + SelectOffset + Encode(Exprs)
	// Expressions are commonly `Column`s, whose hashcode has the length 9, so
	// we pre-alloc 10 bytes for each expr's hashcode.
	// we pre-alloc total bytes size = SizeOf(PlanType)+SizeOf(SelectOffset)+SizeOf(Encode(Exprs))
	//								 = 4+4+(4+len(Exprs)*(4+SizeOf(Expr.hashcode)))
	//								 = 12+len(Exprs)*14
	result := make([]byte, 0, 12+len(p.Exprs)*14)
	result = codec.EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = codec.EncodeIntAsUint32(result, p.SelectBlockOffset())
	exprHashCode := func(i int) []byte { return p.Exprs[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = codec.Encode(result, exprHashCode, len(p.Exprs))
	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalTableDual) HashCode() []byte {
	// PlanType + SelectOffset + RowCount
	result := make([]byte, 0, 12)
	result = codec.EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = codec.EncodeIntAsUint32(result, p.SelectBlockOffset())
	result = codec.EncodeIntAsUint32(result, p.RowCount)
	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalSelection) HashCode() []byte {
	// PlanType + SelectOffset + Encode(Conditions)
	// Conditions are commonly `ScalarFunction`s, whose hashcode usually has a
	// length larger than 20, so we pre-alloc 25 bytes for each expr's hashcode.
	// we pre-alloc total bytes size = SizeOf(PlanType)+SizeOf(SelectOffset)+SizeOf(Encode(Conditions))
	//								 = 4+4+(4+len(Conditions)*(4+SizeOf(Condition.hashcode)))
	//								 = 12+len(Conditions)*29
	result := make([]byte, 0, 12+len(p.Conditions)*29)
	result = codec.EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = codec.EncodeIntAsUint32(result, p.SelectBlockOffset())
	condHashCode := func(i int) []byte { return p.Conditions[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = codec.EncodeAndSort(result, condHashCode, len(p.Conditions))
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
	// ByItems are commonly (bool + Column)s whose hashcode has the length 10,
	// so we pre-alloc 11 bytes for each ByItems's hashcode.
	// we pre-alloc total bytes size = SizeOf(PlanType)+SizeOf(SelectOffset)+SizeOf(Encode(ByItems))
	//								 = 4+4+(4+len(ByItems)*(4+SizeOf(ByItems.hashcode)))
	//								 = 12+len(ByItems)*15
	result := make([]byte, 0, 12+len(p.ByItems)*15)
	result = codec.EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = codec.EncodeIntAsUint32(result, p.SelectBlockOffset())
	byItemHashCode := func(i int) []byte { return p.ByItems[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = codec.Encode(result, byItemHashCode, len(p.ByItems))
	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalTopN) HashCode() []byte {
	// PlanType + SelectOffset + Encode(ByItems)
	// we pre-alloc total bytes size = SizeOf(LogicalSort.hashcode)+ SizeOf(Offset)+SizeOf(Count)
	//								 = (12+len(ByItems)*15)+8+8
	//								 = 28+len(ByItems)*15
	result := make([]byte, 0, 28+len(p.ByItems)*15)
	result = codec.EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = codec.EncodeIntAsUint32(result, p.SelectBlockOffset())
	result = codec.EncodeUint(result, p.Offset)
	result = codec.EncodeUint(result, p.Count)
	byItemHashCode := func(i int) []byte { return p.ByItems[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = codec.Encode(result, byItemHashCode, len(p.ByItems))
	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalJoin) HashCode() []byte {
	// PlanType + SelectOffset + JoinType + Encode(EqualConditions) + Encode(LeftConditions) + Encode(RightConditions) + Encode(OtherConditions)
	// Conditions are commonly `ScalarFunction`s, whose hashcode usually has a
	// length larger than 20, so we pre-alloc 25 bytes for each Condition's hashcode.
	// we pre-alloc total bytes size = SizeOf(PlanType)+SizeOf(SelectOffset)+SizeOf(JoinType)+SizeOf(Encode(EqualConditions))+SizeOf(Encode(LeftConditions))+SizeOf(Encode(RightConditions))+SizeOf(Encode(OtherConditions))
	//								 = 4+4+4+(4+len(EqualConditions)*(4+SizeOf(EqualCondition.hashcode)))+(4+len(LeftConditions)*(4+SizeOf(LeftConditions.hashcode)))+(4+len(RightConditions)*(4+SizeOf(RightConditions.hashcode)))+(4+len(OtherConditions)*(4+SizeOf(OtherConditions.hashcode)))
	//								 = 4+4+4+4*4+(4+SizeOf(condition.hashcode))*((len(p.EqualConditions)+len(p.LeftConditions)+len(p.RightConditions)+len(p.OtherConditions)))
	//								 = 28+29*(len(p.EqualConditions)+len(p.LeftConditions)+len(p.RightConditions)+len(p.OtherConditions))
	result := make([]byte, 0, 28+(len(p.EqualConditions)+len(p.LeftConditions)+len(p.RightConditions)+len(p.OtherConditions))*29)
	result = codec.EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = codec.EncodeIntAsUint32(result, p.SelectBlockOffset())
	result = codec.EncodeIntAsUint32(result, int(p.JoinType))

	eqCondHashCode := func(i int) []byte {
		ctx := p.ctx.GetSessionVars().StmtCtx
		leftHashCode := p.EqualConditions[i].GetArgs()[0].HashCode(ctx)
		rightHashCode := p.EqualConditions[i].GetArgs()[1].HashCode(ctx)
		buffer := make([]byte, 0, len(leftHashCode) + len(rightHashCode))
		if bytes.Compare(leftHashCode, rightHashCode) > 0 {
			buffer = append(buffer, leftHashCode...)
			buffer = append(buffer, rightHashCode...)
			return buffer
		} else {
			buffer = append(buffer, rightHashCode...)
			buffer = append(buffer, leftHashCode...)
			return buffer
		}
		//return p.EqualConditions[i].HashCode(p.ctx.GetSessionVars().StmtCtx)
	}
	result = codec.EncodeAndSort(result, eqCondHashCode, len(p.EqualConditions))

	lHashCode := func(i int) []byte { return p.LeftConditions[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = codec.EncodeAndSort(result, lHashCode, len(p.LeftConditions))

	rHashCode := func(i int) []byte { return p.RightConditions[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = codec.EncodeAndSort(result, rHashCode, len(p.RightConditions))

	otherHashCode := func(i int) []byte { return p.OtherConditions[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = codec.EncodeAndSort(result, otherHashCode, len(p.OtherConditions))

	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalApply) HashCode() []byte {
	// p.LogicalJoin.HashCode() + Encode(CorCols)
	// we pre-alloc total bytes size = SizeOf(p.join.HashCode)+SizeOf(Encode(CorCols))
	//								 = SizeOf(p.join.HashCode)+(4+len(CorCols)*(4+Sizeof(CorCol)))
	//								 = SizeOf(p.join.HashCode)+4+len(CorCols)*13
	joinHashCode := p.LogicalJoin.HashCode()
	result := make([]byte, 0, len(joinHashCode)+4+len(p.CorCols)*13)
	result = append(result, joinHashCode...)
	corColHashCode := func(i int) []byte { return p.CorCols[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = codec.Encode(result, corColHashCode, len(p.CorCols))
	//rewrite PlanType
	binary.BigEndian.PutUint32(result[:], uint32(plancodec.TypeStringToPhysicalID(p.tp)))
	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalAggregation) HashCode() []byte {
	// PlanType + SelectOffset + AggFuncs[0].Mode + Encode(AggFuncs) + Encode(GroupByItems)
	// AggFuncs are commonly `ScalarFunction`s, whose hashcode usually has a
	// length larger than 20, so we pre-alloc 25 bytes for each AggFunc's hashcode.
	// GroupByItems are commonly Column whose hashcode has the length 9,
	// so we pre-alloc 10 bytes for each GroupByItem's hashcode.
	// we pre-alloc total bytes size = SizeOf(PlanType)+SizeOf(SelectOffset)+SizeOf(AggMode)+SizeOf(Encode(AggFuncs))+SizeOf(Encode(GroupByItems))
	//								 = 4+4+4+(4+len(AggFuncs)*(4+SizeOf(AggFunc.hashcode)))+(4+len(GroupByItems)*(4+SizeOf(GroupByItem.hashcode)))
	//								 = 20+len(p.AggFuncs)*29+len(p.GroupByItems)*14
	result := make([]byte, 0, 20+len(p.AggFuncs)*29+len(p.GroupByItems)*14)
	result = codec.EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = codec.EncodeIntAsUint32(result, p.SelectBlockOffset())
	if len(p.AggFuncs) > 0 {
		result = codec.EncodeIntAsUint32(result, int(p.AggFuncs[0].Mode))
	} else {
		result = codec.EncodeIntAsUint32(result, 0)
	}

	aggFuncHashCode := func(i int) []byte { return p.AggFuncs[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = codec.Encode(result, aggFuncHashCode, len(p.AggFuncs))

	groupByHashCode := func(i int) []byte { return p.GroupByItems[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = codec.EncodeAndSort(result, groupByHashCode, len(p.GroupByItems))

	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalMaxOneRow) HashCode() []byte {
	// PlanType + SelectOffset
	result := make([]byte, 0, 8)
	result = codec.EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = codec.EncodeIntAsUint32(result, p.SelectBlockOffset())
	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalShowDDLJobs) HashCode() []byte {
	// PlanType + SelectOffset + JobNumber
	result := make([]byte, 0, 16)
	result = codec.EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = codec.EncodeIntAsUint32(result, p.SelectBlockOffset())
	result = codec.EncodeInt(result, p.JobNumber)
	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalUnionScan) HashCode() []byte {
	// PlanType + SelectOffset + handleCol + Encode(conditions)
	// conditions are commonly `ScalarFunction`s, whose hashcode usually has a
	// length larger than 20, so we pre-alloc 25 bytes for each expr's hashcode.
	// we pre-alloc total bytes size = SizeOf(PlanType)+SizeOf(SelectOffset)+SizeOf(handleCol)+SizeOf(Encode(conditions))
	//								 = 4+4+9+(4+len(conditions)*(4+Sizeof(condition.hashcode)))
	//								 = 21+len(conditions)*29
	result := make([]byte, 0, 21+len(p.conditions)*29)
	result = codec.EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = codec.EncodeIntAsUint32(result, p.SelectBlockOffset())
	result = append(result, p.handleCol.HashCode(p.ctx.GetSessionVars().StmtCtx)...)
	condHashCode := func(i int) []byte { return p.conditions[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = codec.EncodeAndSort(result, condHashCode, len(p.conditions))
	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalUnionAll) HashCode() []byte {
	// PlanType + SelectOffset
	result := make([]byte, 0, 8)
	result = codec.EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = codec.EncodeIntAsUint32(result, p.SelectBlockOffset())
	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalWindow) HashCode() []byte {
	// PlanType + SelectOffset + Encode(WindowFuncDescs) + Encode(PartitionBys) + Encode(OrderBys) + Frame
	// WindowFuncDescs are commonly has at most one arg, so we pre-alloc 30 bytes for each WindowFuncDesc's hashcode.
	// we pre-alloc total bytes size = SizeOf(PlanType)+SizeOf(SelectOffset)+SizeOf(Encode(WindowFuncDescs))+SizeOf(Encode(PartitionBys))+SizeOf(Encode(OrderBys))+SizeOf(Frame)
	//								 = 4+4+(4+len(WindowFuncDescs)*(4+Sizeof(WindowFuncDesc)))+(4+len(PartitionBys)*(4+Sizeof(PartitionBy)))+(4+len(OrderBys)*(4+Sizeof(OrderBy)))+SizeOf(Frame.hashcode)
	//								 = 20+34*len(WindowFuncDescs)+14*(len(p.PartitionBy)+len(p.OrderBy))+SizeOf(Frame.hashcode)
	frameHashcode := p.Frame.HashCode(p.ctx.GetSessionVars().StmtCtx)

	result := make([]byte, 0, 20+len(p.WindowFuncDescs)*34+14*(len(p.PartitionBy)+len(p.OrderBy))+len(frameHashcode))
	result = codec.EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = codec.EncodeIntAsUint32(result, p.SelectBlockOffset())

	wfdHashCode := func(i int) []byte { return p.WindowFuncDescs[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = codec.Encode(result, wfdHashCode, len(p.WindowFuncDescs))

	partitionByHashCode := func(i int) []byte { return p.PartitionBy[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = codec.EncodeAndSort(result, partitionByHashCode, len(p.PartitionBy))

	orderByHashCode := func(i int) []byte { return p.OrderBy[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = codec.Encode(result, orderByHashCode, len(p.OrderBy))

	result = append(result, frameHashcode...)

	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalMemTable) HashCode() []byte {
	// PlanType + SelectOffset + TableInfo.ID + QueryTimeRange
	result := make([]byte, 0, 32)
	result = codec.EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = codec.EncodeIntAsUint32(result, p.SelectBlockOffset())

	result = codec.EncodeInt(result, p.TableInfo.ID)

	result = codec.EncodeInt(result, p.QueryTimeRange.From.Unix())
	result = codec.EncodeInt(result, p.QueryTimeRange.To.Unix())
	return result
}

// HashCode implements LogicalPlan interface.
//func (p *DataSource) HashCode() []byte {
//	// PlanType + SelectOffset + tableInfo.ID + Encode(allConds)
//	// allConds are commonly `ScalarFunction`s, whose hashcode usually has a
//	// length larger than 20, so we pre-alloc 25 bytes for each expr's hashcode.
//	// we pre-alloc total bytes size = SizeOf(PlanType)+SizeOf(SelectOffset)+SizeOf(tableInfo.ID)+SizeOf(Encode(allConds))
//	//								 = 4+4+8+(4+len(allConds)*(4+Sizeof(condition.hashcode)))
//	//								 = 20+len(allConds)*29
//	if p == nil {
//		return nil
//	}
//
//	result := make([]byte, 0, 20+len(p.allConds)*29)
//	result = codec.EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
//	result = codec.EncodeIntAsUint32(result, p.SelectBlockOffset())
//	result = codec.EncodeInt(result, p.tableInfo.ID)
//	condHashCode := func(i int) []byte { return p.allConds[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
//	result = codec.EncodeAndSort(result, condHashCode, len(p.allConds))
//	return result
//}

// HashCode implements LogicalPlan interface.
func (p *TiKVSingleGather) HashCode() []byte {
	// PlanType + SelectOffset + Source.tableInfo.ID + IsIndexGather + Index.ID
	result := make([]byte, 0, 25)
	result = codec.EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = codec.EncodeIntAsUint32(result, p.SelectBlockOffset())
	if p.Source != nil {
		result = codec.EncodeInt(result, p.Source.tableInfo.ID)
	}
	result = codec.EncodeBool(result, p.IsIndexGather)
	if p.IsIndexGather && p.Index != nil {
		result = codec.EncodeInt(result, p.Index.ID)
	}
	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalTableScan) HashCode() []byte {
	// PlanType + SelectOffset + Source.tableInfo.ID + Handle + Encode(AccessConds)
	// AccessConds are commonly `ScalarFunction`s, whose hashcode usually has a
	// length larger than 20, so we pre-alloc 25 bytes for each expr's hashcode.
	// we pre-alloc total bytes size = SizeOf(PlanType)+SizeOf(SelectOffset)+SizeOf(tableInfo.ID)+SizeOf(Handle)+SizeOf(Encode(AccessConds))
	//								 = 4+4+8+9+(4+len(AccessConds)*(4+Sizeof(AccessCond.hashcode)))
	//								 = 29+len(AccessConds)*29
	result := make([]byte, 0, 29+len(p.AccessConds)*29)
	result = codec.EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = codec.EncodeIntAsUint32(result, p.SelectBlockOffset())

	if p.Source != nil {
		result = codec.EncodeInt(result, p.Source.tableInfo.ID)
	}

	if p.Handle != nil {
		result = append(result, p.Handle.HashCode(p.ctx.GetSessionVars().StmtCtx)...)
	}

	accessCondHashCode := func(i int) []byte { return p.AccessConds[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = codec.EncodeAndSort(result, accessCondHashCode, len(p.AccessConds))

	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalIndexScan) HashCode() []byte {
	// PlanType + SelectOffset + IsDoubleRead + EqCondCount + Source.tableInfo.ID + Index.ID + Encode(AccessConds) + FullIdxCols + FullIdxColLens + IdxCols + IdxColLens
	// AccessConds are commonly `ScalarFunction`s, whose hashcode usually has a
	// length larger than 20, so we pre-alloc 25 bytes for each expr's hashcode.
	// we pre-alloc total bytes size = SizeOf(PlanType)+SizeOf(SelectOffset)+SizeOf(IsDoubleRead)+SizeOf(EqCondCount)+SizeOf(tableInfo.ID)+SizeOf(Index.ID)+SizeOf(Encode(AccessConds))+SizeOf(FullIdxCols)+SizeOf(FullIdxColLens)+SizeOf(IdxCols)+SizeOf(IdxColLens)
	//								 = 4+4+1+4+8+8+(4+len(AccessConds)*(4+Sizeof(AccessCond.hashcode)))+(4+len(FullIdxCols)*(4+SizeOf(FullIdxCol.hashcode)))+(4+len(FullIdxColLens)*SizeOf(FullIdxColLen.hashcode))+(4+len(IdxCols)*(4+SizeOf(IdxCol.hashcode)))+(4+len(IdxColLens)*SizeOf(IdxColLen.hashcode))
	//								 = 49+len(p.AccessConds)*29+len(p.FullIdxCols)*13+len(p.FullIdxColLens)*4+len(p.IdxCols)*13+len(p.IdxColLens)*4
	result := make([]byte, 0, 49+len(p.AccessConds)*29+len(p.FullIdxCols)*13+len(p.FullIdxColLens)*4+len(p.IdxCols)*13+len(p.IdxColLens)*4)
	result = codec.EncodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = codec.EncodeIntAsUint32(result, p.SelectBlockOffset())
	result = codec.EncodeBool(result, p.IsDoubleRead)
	result = codec.EncodeIntAsUint32(result, p.EqCondCount)
	if p.Source != nil {
		result = codec.EncodeInt(result, p.Source.tableInfo.ID)
	}
	if p.Index != nil {
		result = codec.EncodeInt(result, p.Index.ID)
	}

	accessCondHashCode := func(i int) []byte { return p.AccessConds[i].HashCode(p.ctx.GetSessionVars().StmtCtx) }
	result = codec.EncodeAndSort(result, accessCondHashCode, len(p.AccessConds))

	result = codec.EncodeIntAsUint32(result, len(p.FullIdxCols))
	for i, col := range p.FullIdxCols {
		result = codec.EncodeIntAsUint32(result, i)
		if col != nil {
			result = append(result, col.HashCode(p.ctx.GetSessionVars().StmtCtx)...)
		}
	}

	result = codec.EncodeIntAsUint32(result, len(p.FullIdxColLens))
	for _, colLen := range p.FullIdxColLens {
		result = codec.EncodeIntAsUint32(result, colLen)
	}

	result = codec.EncodeIntAsUint32(result, len(p.IdxCols))
	for i, col := range p.IdxCols {
		result = codec.EncodeIntAsUint32(result, i)
		if col != nil {
			result = append(result, col.HashCode(p.ctx.GetSessionVars().StmtCtx)...)
		}
	}

	result = codec.EncodeIntAsUint32(result, len(p.IdxColLens))
	for _, colLen := range p.IdxColLens {
		result = codec.EncodeIntAsUint32(result, colLen)
	}

	return result
}
