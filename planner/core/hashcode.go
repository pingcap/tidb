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
	"encoding/binary"

	"github.com/pingcap/tidb/util/plancodec"
)

func encodeIntAsUint32(result []byte, value int) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(value))
	return append(result, buf[:]...)
}

// HashCode implements LogicalPlan interface.
func (p *baseLogicalPlan) HashCode() []byte {
	// PlanType + PlanID
	// We use PlanID for the default hash, so if two plans do not have
	// the same id, the hash value will never be the same.
	result := make([]byte, 0, 8)
	result = encodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = encodeIntAsUint32(result, p.id)
	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalProjection) HashCode() []byte {
	// PlanType + ExprNum + [Exprs]
	result := make([]byte, 0, 8)
	result = encodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = encodeIntAsUint32(result, len(p.Exprs))
	for _, expr := range p.Exprs {
		exprHashCode := expr.HashCode(p.ctx.GetSessionVars().StmtCtx)
		result = encodeIntAsUint32(result, len(exprHashCode))
		result = append(result, exprHashCode...)
	}
	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalTableDual) HashCode() []byte {
	// PlanType + RowCount
	result := make([]byte, 0, 8)
	result = encodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = encodeIntAsUint32(result, p.RowCount)
	return result
}

// HashCode implements LogicalPlan interface.
func (p *LogicalSelection) HashCode() []byte {
	// PlanType + ConditionNum + [Conditions]
	result := make([]byte, 0, 8)
	result = encodeIntAsUint32(result, plancodec.TypeStringToPhysicalID(p.tp))
	result = encodeIntAsUint32(result, len(p.Conditions))
	for _, expr := range p.Conditions {
		exprHashCode := expr.HashCode(p.ctx.GetSessionVars().StmtCtx)
		result = encodeIntAsUint32(result, len(exprHashCode))
		result = append(result, exprHashCode...)
	}
	return result
}
