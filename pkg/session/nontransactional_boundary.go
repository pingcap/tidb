// Copyright 2026 PingCAP, Inc.
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

package session

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/codec"
)

type nonTransactionalDMLBoundary struct {
	value     types.Datum
	encoded   []byte
	inclusive bool
	hasValue  bool
}

func encodeNonTransactionalDMLBoundary(sc *stmtctx.StatementContext, desc *nonTransactionalDMLHandleDescriptor,
	value types.Datum, inclusive bool) (nonTransactionalDMLBoundary, error) {
	if sc == nil || desc == nil {
		return nonTransactionalDMLBoundary{}, errors.New("Non-transactional DML boundary requires statement context and handle descriptor")
	}
	encoded, err := codec.EncodeKey(sc.TimeZone(), nil, value)
	if err != nil {
		return nonTransactionalDMLBoundary{}, err
	}
	return nonTransactionalDMLBoundary{
		value:     *value.Clone(),
		encoded:   encoded,
		inclusive: inclusive,
		hasValue:  true,
	}, nil
}

func decodeNonTransactionalDMLBoundary(sc *stmtctx.StatementContext, desc *nonTransactionalDMLHandleDescriptor,
	encoded []byte, inclusive bool) (nonTransactionalDMLBoundary, error) {
	if sc == nil || desc == nil {
		return nonTransactionalDMLBoundary{}, errors.New("Non-transactional DML boundary requires statement context and handle descriptor")
	}
	remaining, value, err := codec.DecodeOne(encoded)
	if err != nil {
		return nonTransactionalDMLBoundary{}, err
	}
	if len(remaining) != 0 {
		return nonTransactionalDMLBoundary{}, errors.New("Non-transactional DML boundary contains trailing encoded bytes")
	}
	encodedCopy := append([]byte(nil), encoded...)
	return nonTransactionalDMLBoundary{
		value:     value,
		encoded:   encodedCopy,
		inclusive: inclusive,
		hasValue:  true,
	}, nil
}

func buildNonTransactionalDMLRangeCondition(desc *nonTransactionalDMLHandleDescriptor,
	lower *nonTransactionalDMLBoundary, upper *nonTransactionalDMLBoundary) ast.ExprNode {
	var condition ast.ExprNode
	appendCondition := func(next ast.ExprNode) {
		if condition == nil {
			condition = next
			return
		}
		condition = &ast.BinaryOperationExpr{
			Op: opcode.LogicAnd,
			L:  condition,
			R:  next,
		}
	}
	if lower != nil && lower.hasValue {
		op := opcode.GT
		if lower.inclusive {
			op = opcode.GE
		}
		appendCondition(&ast.BinaryOperationExpr{
			Op: op,
			L:  nonTransactionalDMLHandleColumnExpr(desc),
			R:  nonTransactionalDMLBoundaryValueExpr(desc, *lower),
		})
	}
	if upper != nil && upper.hasValue {
		op := opcode.LT
		if upper.inclusive {
			op = opcode.LE
		}
		appendCondition(&ast.BinaryOperationExpr{
			Op: op,
			L:  nonTransactionalDMLHandleColumnExpr(desc),
			R:  nonTransactionalDMLBoundaryValueExpr(desc, *upper),
		})
	}
	return condition
}

func nonTransactionalDMLHandleColumnExpr(desc *nonTransactionalDMLHandleDescriptor) ast.ExprNode {
	name := desc.columnName
	return &ast.ColumnNameExpr{Name: &name}
}

func nonTransactionalDMLBoundaryValueExpr(desc *nonTransactionalDMLHandleDescriptor, boundary nonTransactionalDMLBoundary) ast.ExprNode {
	value := &driver.ValueExpr{}
	value.Type = desc.fieldType
	value.Datum = boundary.value
	return value
}
