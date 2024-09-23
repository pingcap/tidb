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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// ExplainInfo implements the Expression interface.
func (expr *ScalarFunction) ExplainInfo() string {
	return expr.explainInfo(false)
}

func (expr *ScalarFunction) explainInfo(normalized bool) string {
	var buffer bytes.Buffer
	fmt.Fprintf(&buffer, "%s(", expr.FuncName.L)
	switch expr.FuncName.L {
	case ast.Cast:
		for _, arg := range expr.GetArgs() {
			if normalized {
				buffer.WriteString(arg.ExplainNormalizedInfo())
			} else {
				buffer.WriteString(arg.ExplainInfo())
			}
			buffer.WriteString(", ")
			buffer.WriteString(expr.RetType.String())
		}
	default:
		for i, arg := range expr.GetArgs() {
			if normalized {
				buffer.WriteString(arg.ExplainNormalizedInfo())
			} else {
				buffer.WriteString(arg.ExplainInfo())
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
	return expr.explainInfo(true)
}

// ExplainInfo implements the Expression interface.
func (col *Column) ExplainInfo(ctx EvalContext) string {
	return col.ColumnExplainInfo(ctx, false)
}

// ExplainNormalizedInfo implements the Expression interface.
func (col *Column) ExplainNormalizedInfo() string {
	return col.ColumnExplainInfoNormalized()
}

// ExplainNormalizedInfo4InList implements the Expression interface.
func (col *Column) ExplainNormalizedInfo4InList() string {
	return col.ColumnExplainInfoNormalized()
}

// ExplainInfo implements the Expression interface.
func (expr *Constant) ExplainInfo(ctx sessionctx.Context) string {
	redact := ctx.GetTiDBRedactLog()
	if redact == errors.RedactLogEnable {
		return "?"
	}
	dt, err := expr.Eval(ctx, chunk.Row{})
	if err != nil {
		return "not recognized const vanue"
	}
	if redact == errors.RedactLogMarker {
		builder := new(strings.Builder)
		builder.WriteString("‹")
		builder.WriteString(expr.format(dt))
		builder.WriteString("›")
		return builder.String()
	}
	return expr.format(dt)
}

// ExplainNormalizedInfo implements the Expression interface.
func (expr *Constant) ExplainNormalizedInfo() string {
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
<<<<<<< HEAD
func ExplainExpressionList(exprs []Expression, schema *Schema) string {
=======
func ExplainExpressionList(ctx EvalContext, exprs []Expression, schema *Schema, redactMode string) string {
>>>>>>> f5ac1c4a453 (*: support tidb_redact_log for explain (#54553))
	builder := &strings.Builder{}
	for i, expr := range exprs {
		switch expr.(type) {
		case *Column, *CorrelatedColumn:
<<<<<<< HEAD
			builder.WriteString(expr.String())
			if expr.String() != schema.Columns[i].String() {
				// simple col projected again with another uniqueID without origin name.
				builder.WriteString("->")
				builder.WriteString(schema.Columns[i].String())
			}
		case *Constant:
			v := expr.String()
=======
			builder.WriteString(expr.StringWithCtx(ctx, redactMode))
			if expr.StringWithCtx(ctx, redactMode) != schema.Columns[i].StringWithCtx(ctx, redactMode) {
				// simple col projected again with another uniqueID without origin name.
				builder.WriteString("->")
				builder.WriteString(schema.Columns[i].StringWithCtx(ctx, redactMode))
			}
		case *Constant:
			v := expr.StringWithCtx(ctx, errors.RedactLogDisable)
>>>>>>> f5ac1c4a453 (*: support tidb_redact_log for explain (#54553))
			length := 64
			if len(v) < length {
				redact.WriteRedact(builder, v, redactMode)
			} else {
				redact.WriteRedact(builder, v[:length], redactMode)
				fmt.Fprintf(builder, "(len:%d)", len(v))
			}
			builder.WriteString("->")
<<<<<<< HEAD
			builder.WriteString(schema.Columns[i].String())
		default:
			builder.WriteString(expr.String())
			builder.WriteString("->")
			builder.WriteString(schema.Columns[i].String())
=======
			builder.WriteString(schema.Columns[i].StringWithCtx(ctx, redactMode))
		default:
			builder.WriteString(expr.StringWithCtx(ctx, redactMode))
			builder.WriteString("->")
			builder.WriteString(schema.Columns[i].StringWithCtx(ctx, redactMode))
>>>>>>> f5ac1c4a453 (*: support tidb_redact_log for explain (#54553))
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
func SortedExplainExpressionList(exprs []Expression) []byte {
	return sortedExplainExpressionList(exprs, false)
}

func sortedExplainExpressionList(exprs []Expression, normalized bool) []byte {
	buffer := bytes.NewBufferString("")
	exprInfos := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		if normalized {
			exprInfos = append(exprInfos, expr.ExplainNormalizedInfo())
		} else {
			exprInfos = append(exprInfos, expr.ExplainInfo())
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
	return sortedExplainExpressionList(exprs, true)
}

// SortedExplainNormalizedScalarFuncList is same like SortedExplainExpressionList, but use for generating normalized information.
func SortedExplainNormalizedScalarFuncList(exprs []*ScalarFunction) []byte {
	expressions := make([]Expression, len(exprs))
	for i := range exprs {
		expressions[i] = exprs[i]
	}
	return sortedExplainExpressionList(expressions, true)
}

// ExplainColumnList generates explain information for a list of columns.
func ExplainColumnList(cols []*Column) []byte {
	buffer := bytes.NewBufferString("")
	for i, col := range cols {
		buffer.WriteString(col.ExplainInfo())
		if i+1 < len(cols) {
			buffer.WriteString(", ")
		}
	}
	return buffer.Bytes()
}
