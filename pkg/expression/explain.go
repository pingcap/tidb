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
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/redact"
)

// ExplainInfo implements the Expression interface.
func (expr *ScalarFunction) ExplainInfo(sctx sessionctx.Context) string {
	return expr.explainInfo(sctx, false)
}

func (expr *ScalarFunction) explainInfo(sctx sessionctx.Context, normalized bool) string {
	var buffer bytes.Buffer
	fmt.Fprintf(&buffer, "%s(", expr.FuncName.L)
	switch expr.FuncName.L {
	case ast.Cast:
		for _, arg := range expr.GetArgs() {
			if normalized {
				buffer.WriteString(arg.ExplainNormalizedInfo())
			} else {
				buffer.WriteString(arg.ExplainInfo(sctx))
			}
			buffer.WriteString(", ")
			buffer.WriteString(expr.RetType.String())
		}
	default:
		for i, arg := range expr.GetArgs() {
			if normalized {
				buffer.WriteString(arg.ExplainNormalizedInfo())
			} else {
				buffer.WriteString(arg.ExplainInfo(sctx))
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

// ExplainInfo implements the Expression interface.
func (col *Column) ExplainInfo(ctx sessionctx.Context) string {
	return col.ColumnExplainInfo(ctx.GetSessionVars().EnableRedactLog, false)
}

// ExplainNormalizedInfo implements the Expression interface.
func (col *Column) ExplainNormalizedInfo() string {
	return col.ColumnExplainInfoNormalized()
}

// ExplainNormalizedInfo4InList implements the Expression interface.
func (col *Column) ExplainNormalizedInfo4InList() string {
	return col.ColumnExplainInfoNormalized()
}

// ColumnExplainInfo returns the explained info for column.
func (col *Column) ColumnExplainInfo(redact, normalized bool) string {
	if normalized {
		return col.ColumnExplainInfoNormalized()
	}
	return col.StringWithCtx(redact)
}

// ColumnExplainInfoNormalized returns the normalized explained info for column.
func (col *Column) ColumnExplainInfoNormalized() string {
	if col.OrigName != "" {
		return col.OrigName
	}
	return "?"
}

// ExplainInfo implements the Expression interface.
func (expr *Constant) ExplainInfo(ctx sessionctx.Context) string {
	if ctx != nil && ctx.GetSessionVars().EnableRedactLog {
		return "?"
	}
	dt, err := expr.Eval(chunk.Row{})
	if err != nil {
		return "not recognized const vanue"
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
func ExplainExpressionList(ctx sessionctx.Context, exprs []Expression, schema *Schema) string {
	enableRedact := ctx.GetSessionVars().EnableRedactLog
	builder := &strings.Builder{}
	for i, expr := range exprs {
		switch expr.(type) {
		case *Column, *CorrelatedColumn:
			builder.WriteString(expr.StringWithCtx(enableRedact))
			if expr.StringWithCtx(enableRedact) != schema.Columns[i].StringWithCtx(enableRedact) {
				// simple col projected again with another uniqueID without origin name.
				builder.WriteString("->")
				builder.WriteString(schema.Columns[i].StringWithCtx(enableRedact))
			}
		case *Constant:
			v := expr.StringWithCtx(false)
			length := 64
			if len(v) < length {
				redact.WriteRedact(builder, v, enableRedact)
			} else {
				redact.WriteRedact(builder, v[:length], enableRedact)
				fmt.Fprintf(builder, "(len:%d)", len(v))
			}
			builder.WriteString("->")
			builder.WriteString(schema.Columns[i].StringWithCtx(enableRedact))
		default:
			builder.WriteString(expr.StringWithCtx(enableRedact))
			builder.WriteString("->")
			builder.WriteString(schema.Columns[i].StringWithCtx(enableRedact))
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
func SortedExplainExpressionList(ctx sessionctx.Context, exprs []Expression) []byte {
	return sortedExplainExpressionList(ctx, exprs, false)
}

func sortedExplainExpressionList(ctx sessionctx.Context, exprs []Expression, normalized bool) []byte {
	buffer := bytes.NewBufferString("")
	exprInfos := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		if normalized {
			exprInfos = append(exprInfos, expr.ExplainNormalizedInfo())
		} else {
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
func SortedExplainNormalizedExpressionList(ctx sessionctx.Context, exprs []Expression) []byte {
	return sortedExplainExpressionList(ctx, exprs, true)
}

// SortedExplainNormalizedScalarFuncList is same like SortedExplainExpressionList, but use for generating normalized information.
func SortedExplainNormalizedScalarFuncList(ctx sessionctx.Context, exprs []*ScalarFunction) []byte {
	expressions := make([]Expression, len(exprs))
	for i := range exprs {
		expressions[i] = exprs[i]
	}
	return sortedExplainExpressionList(ctx, expressions, true)
}

// ExplainColumnList generates explain information for a list of columns.
func ExplainColumnList(ctx sessionctx.Context, cols []*Column) []byte {
	buffer := bytes.NewBufferString("")
	for i, col := range cols {
		buffer.WriteString(col.ExplainInfo(ctx))
		if i+1 < len(cols) {
			buffer.WriteString(", ")
		}
	}
	return buffer.Bytes()
}
