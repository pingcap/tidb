// Copyright 2017 PingCAP, Inc.
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

package expression

import (
	"bytes"
	"fmt"
	"slices"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// ExplainInfo implements the Expression interface.
func (expr *ScalarFunction) ExplainInfo(ctx EvalContext) string {
	return expr.explainInfo(ctx, false)
}

func (expr *ScalarFunction) explainInfo(ctx EvalContext, normalized bool) string {
	// we only need ctx for non-normalized explain info.
	intest.Assert(normalized || ctx != nil)
	var buffer bytes.Buffer
	fmt.Fprintf(&buffer, "%s(", expr.FuncName.L)
	// convert `in(_tidb_pid, -1)` to `in(_tidb_pid, dual)` whether normalized equals to true or false.
	if expr.FuncName.L == ast.In {
		args := expr.GetArgs()
		if len(args) == 2 && args[0].ExplainNormalizedInfo() == model.ExtraPartitionIdName.L && args[1].(*Constant).Value.GetInt64() == -1 {
			buffer.WriteString(model.ExtraPartitionIdName.L + ", dual)")
			return buffer.String()
		}
	}
	switch expr.FuncName.L {
	case ast.Cast:
		for _, arg := range expr.GetArgs() {
			if normalized {
				buffer.WriteString(arg.ExplainNormalizedInfo())
			} else {
				intest.Assert(ctx != nil)
				buffer.WriteString(arg.ExplainInfo(ctx))
			}
			buffer.WriteString(", ")
			buffer.WriteString(expr.RetType.String())
		}
	default:
		for i, arg := range expr.GetArgs() {
			if normalized {
				buffer.WriteString(arg.ExplainNormalizedInfo())
			} else {
				intest.Assert(ctx != nil)
				buffer.WriteString(arg.ExplainInfo(ctx))
			}
			if i+1 < len(expr.GetArgs()) {
				buffer.WriteString(", ")
			}
		}
	}
	buffer.WriteString(")")
	return buffer.String()
}

// ExplainNormalizedInfo implements the Expression interface.
func (expr *ScalarFunction) ExplainNormalizedInfo() string {
	return expr.explainInfo(nil, true)
}

// ExplainNormalizedInfo4InList implements the Expression interface.
func (expr *ScalarFunction) ExplainNormalizedInfo4InList() string {
	var buffer bytes.Buffer
	fmt.Fprintf(&buffer, "%s(", expr.FuncName.L)
	switch expr.FuncName.L {
	case ast.Cast:
		for _, arg := range expr.GetArgs() {
			buffer.WriteString(arg.ExplainNormalizedInfo4InList())
			buffer.WriteString(", ")
			buffer.WriteString(expr.RetType.String())
		}
	case ast.In:
		buffer.WriteString("...")
	default:
		for i, arg := range expr.GetArgs() {
			buffer.WriteString(arg.ExplainNormalizedInfo4InList())
			if i+1 < len(expr.GetArgs()) {
				buffer.WriteString(", ")
			}
		}
	}
	buffer.WriteString(")")
	return buffer.String()
}

// ColumnExplainInfo returns the explained info for column.
func (col *Column) ColumnExplainInfo(normalized bool) string {
	if normalized {
		if col.OrigName != "" {
			return col.OrigName
		}
		return "?"
	}
	return col.String()
}

// ExplainInfo implements the Expression interface.
func (col *Column) ExplainInfo(ctx EvalContext) string {
	return col.ColumnExplainInfo(false)
}

// ExplainNormalizedInfo implements the Expression interface.
func (col *Column) ExplainNormalizedInfo() string {
	return col.ColumnExplainInfo(true)
}

// ExplainNormalizedInfo4InList implements the Expression interface.
func (col *Column) ExplainNormalizedInfo4InList() string {
	return col.ColumnExplainInfo(true)
}

// ExplainInfo implements the Expression interface.
func (expr *Constant) ExplainInfo(ctx EvalContext) string {
	dt, err := expr.Eval(ctx, chunk.Row{})
	if err != nil {
		return "not recognized const value"
	}
	return expr.format(dt)
}

// ExplainNormalizedInfo implements the Expression interface.
func (expr *Constant) ExplainNormalizedInfo() string {
	return "?"
}

// ExplainNormalizedInfo4InList implements the Expression interface.
func (expr *Constant) ExplainNormalizedInfo4InList() string {
	return "?"
}

func (expr *Constant) format(dt types.Datum) string {
	switch dt.Kind() {
	case types.KindNull:
		return "NULL"
	case types.KindString, types.KindBytes, types.KindMysqlEnum, types.KindMysqlSet,
		types.KindMysqlJSON, types.KindBinaryLiteral, types.KindMysqlBit:
		return fmt.Sprintf("\"%v\"", dt.GetValue())
	}
	return fmt.Sprintf("%v", dt.GetValue())
}

// ExplainExpressionList generates explain information for a list of expressions.
func ExplainExpressionList(exprs []Expression, schema *Schema) string {
	builder := &strings.Builder{}
	for i, expr := range exprs {
		switch expr.(type) {
		case *Column, *CorrelatedColumn:
			builder.WriteString(expr.String())
			if expr.String() != schema.Columns[i].String() {
				// simple col projected again with another uniqueID without origin name.
				builder.WriteString("->")
				builder.WriteString(schema.Columns[i].String())
			}
		case *Constant:
			v := expr.String()
			length := 64
			if len(v) < length {
				builder.WriteString(v)
			} else {
				builder.WriteString(v[:length])
				fmt.Fprintf(builder, "(len:%d)", len(v))
			}
			builder.WriteString("->")
			builder.WriteString(schema.Columns[i].String())
		default:
			builder.WriteString(expr.String())
			builder.WriteString("->")
			builder.WriteString(schema.Columns[i].String())
		}
		if i+1 < len(exprs) {
			builder.WriteString(", ")
		}
	}
	return builder.String()
}

// SortedExplainExpressionList generates explain information for a list of expressions in order.
// In some scenarios, the expr's order may not be stable when executing multiple times.
// So we add a sort to make its explain result stable.
func SortedExplainExpressionList(ctx EvalContext, exprs []Expression) []byte {
	return sortedExplainExpressionList(ctx, exprs, false, false)
}

// SortedExplainExpressionListIgnoreInlist generates explain information for a list of expressions in order.
func SortedExplainExpressionListIgnoreInlist(exprs []Expression) []byte {
	return sortedExplainExpressionList(nil, exprs, false, true)
}

func sortedExplainExpressionList(ctx EvalContext, exprs []Expression, normalized bool, ignoreInlist bool) []byte {
	intest.Assert(ignoreInlist || normalized || ctx != nil)
	buffer := bytes.NewBufferString("")
	exprInfos := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		if ignoreInlist {
			exprInfos = append(exprInfos, expr.ExplainNormalizedInfo4InList())
		} else if normalized {
			exprInfos = append(exprInfos, expr.ExplainNormalizedInfo())
		} else {
			intest.Assert(ctx != nil)
			exprInfos = append(exprInfos, expr.ExplainInfo(ctx))
		}
	}
	slices.Sort(exprInfos)
	for i, info := range exprInfos {
		buffer.WriteString(info)
		if i+1 < len(exprInfos) {
			buffer.WriteString(", ")
		}
	}
	return buffer.Bytes()
}

// SortedExplainNormalizedExpressionList is same like SortedExplainExpressionList, but use for generating normalized information.
func SortedExplainNormalizedExpressionList(exprs []Expression) []byte {
	return sortedExplainExpressionList(nil, exprs, true, false)
}

// SortedExplainNormalizedScalarFuncList is same like SortedExplainExpressionList, but use for generating normalized information.
func SortedExplainNormalizedScalarFuncList(exprs []*ScalarFunction) []byte {
	expressions := make([]Expression, len(exprs))
	for i := range exprs {
		expressions[i] = exprs[i]
	}
	return sortedExplainExpressionList(nil, expressions, true, false)
}

// ExplainColumnList generates explain information for a list of columns.
func ExplainColumnList(ctx EvalContext, cols []*Column) []byte {
	buffer := bytes.NewBufferString("")
	for i, col := range cols {
		buffer.WriteString(col.ExplainInfo(ctx))
		if i+1 < len(cols) {
			buffer.WriteString(", ")
		}
	}
	return buffer.Bytes()
}
