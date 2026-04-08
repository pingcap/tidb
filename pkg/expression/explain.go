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
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/redact"
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
	// convert `in(_tidb_tid, -1)` to `in(_tidb_tid, dual)` whether normalized equals to true or false.
	if expr.FuncName.L == ast.In {
		args := expr.GetArgs()
		if len(args) == 2 && strings.HasSuffix(args[0].ExplainNormalizedInfo(), model.ExtraPhysTblIDName.L) && args[1].(*Constant).Value.GetInt64() == -1 {
			buffer.WriteString(args[0].ExplainNormalizedInfo() + ", dual)")
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
func (col *Column) ColumnExplainInfo(ctx ParamValues, normalized bool) string {
	if normalized {
		return col.ColumnExplainInfoNormalized()
	}
	return col.StringWithCtxForExplain(ctx, errors.RedactLogDisable, shouldRemoveColumnNumbers(ctx))
}

// ColumnExplainInfoNormalized returns the normalized explained info for column.
func (col *Column) ColumnExplainInfoNormalized() string {
	if col.OrigName != "" {
		return col.OrigName
	}
	return "?"
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
func (expr *Constant) ExplainInfo(ctx EvalContext) string {
	redact := ctx.GetTiDBRedactLog()
	if redact == errors.RedactLogEnable {
		if expr.SubqueryRefID > 0 {
			return fmt.Sprintf("ScalarQueryCol#%d(?)", expr.SubqueryRefID)
		}
		return "?"
	}

	dt, err := expr.Eval(ctx, chunk.Row{})
	if err != nil {
		return "not recognized const value"
	}

	valueStr := expr.format(dt)

	if redact == errors.RedactLogMarker {
		valueStr = "‹" + valueStr + "›"
	}
	if expr.SubqueryRefID > 0 {
		return fmt.Sprintf("ScalarQueryCol#%d(%s)", expr.SubqueryRefID, valueStr)
	}
	return valueStr
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
		return fmt.Sprintf("\"%s\"", dt.TruncatedStringify())
	}
	return dt.TruncatedStringify()
}

// ExplainExpressionList generates explain information for a list of expressions.
func ExplainExpressionList(ctx EvalContext, exprs []Expression, schema *Schema, redactMode string) string {
	builder := &strings.Builder{}
	// Check explain format once at the start - it doesn't change during a single explain output
	removeColNums := shouldRemoveColumnNumbers(ctx)
	for i, expr := range exprs {
		switch expr := expr.(type) {
		case *Column, *CorrelatedColumn:
			// Both Column and CorrelatedColumn use the same StringWithCtxForExplain method
			// (CorrelatedColumn embeds Column), so they can share the same logic
			var col *Column
			switch c := expr.(type) {
			case *Column:
				col = c
			case *CorrelatedColumn:
				col = &c.Column
			}
			exprStr := col.StringWithCtxForExplain(ctx, redactMode, removeColNums)
			schemaColStr := schema.Columns[i].StringWithCtxForExplain(ctx, redactMode, removeColNums)
			builder.WriteString(exprStr)
			if exprStr != schemaColStr {
				// simple col projected again with another uniqueID without origin name.
				builder.WriteString("->")
				builder.WriteString(schemaColStr)
			}
		case *Constant:
			v := expr.StringWithCtx(ctx, errors.RedactLogDisable)
			redact.WriteRedact(builder, v, redactMode)
			builder.WriteString("->")
			builder.WriteString(schema.Columns[i].StringWithCtxForExplain(ctx, redactMode, removeColNums))
		default:
			builder.WriteString(expr.StringWithCtx(ctx, redactMode))
			builder.WriteString("->")
			builder.WriteString(schema.Columns[i].StringWithCtxForExplain(ctx, redactMode, removeColNums))
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
