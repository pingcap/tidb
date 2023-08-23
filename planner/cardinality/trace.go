// Copyright 2023 PingCAP, Inc.
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

package cardinality

import (
	"bytes"
	"errors"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/sessionctx"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/tracing"
	"go.uber.org/zap"
)

// CETraceExpr appends an expression and related information into CE trace
func CETraceExpr(sctx sessionctx.Context, tableID int64, tp string, expr expression.Expression, rowCount float64) {
	exprStr, err := ExprToString(expr)
	if err != nil {
		logutil.BgLogger().Debug("Failed to trace CE of an expression", zap.String("category", "OptimizerTrace"),
			zap.Any("expression", expr))
		return
	}
	rec := tracing.CETraceRecord{
		TableID:  tableID,
		Type:     tp,
		Expr:     exprStr,
		RowCount: uint64(rowCount),
	}
	sc := sctx.GetSessionVars().StmtCtx
	sc.OptimizerCETrace = append(sc.OptimizerCETrace, &rec)
}

// ExprToString prints an Expression into a string which can appear in a SQL.
//
// It might be too tricky because it makes use of TiDB allowing using internal function name in SQL.
// For example, you can write `eq`(a, 1), which is the same as a = 1.
// We should have implemented this by first implementing a method to turn an expression to an AST
//
//	then call astNode.Restore(), like the Constant case here. But for convenience, we use this trick for now.
//
// It may be more appropriate to put this in expression package. But currently we only use it for CE trace,
//
//	and it may not be general enough to handle all possible expressions. So we put it here for now.
func ExprToString(e expression.Expression) (string, error) {
	switch expr := e.(type) {
	case *expression.ScalarFunction:
		var buffer bytes.Buffer
		buffer.WriteString("`" + expr.FuncName.L + "`(")
		switch expr.FuncName.L {
		case ast.Cast:
			for _, arg := range expr.GetArgs() {
				argStr, err := ExprToString(arg)
				if err != nil {
					return "", err
				}
				buffer.WriteString(argStr)
				buffer.WriteString(", ")
				buffer.WriteString(expr.RetType.String())
			}
		default:
			for i, arg := range expr.GetArgs() {
				argStr, err := ExprToString(arg)
				if err != nil {
					return "", err
				}
				buffer.WriteString(argStr)
				if i+1 != len(expr.GetArgs()) {
					buffer.WriteString(", ")
				}
			}
		}
		buffer.WriteString(")")
		return buffer.String(), nil
	case *expression.Column:
		return expr.String(), nil
	case *expression.CorrelatedColumn:
		return "", errors.New("tracing for correlated columns not supported now")
	case *expression.Constant:
		value, err := expr.Eval(chunk.Row{})
		if err != nil {
			return "", err
		}
		valueExpr := driver.ValueExpr{Datum: value}
		var buffer bytes.Buffer
		restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &buffer)
		err = valueExpr.Restore(restoreCtx)
		if err != nil {
			return "", err
		}
		return buffer.String(), nil
	}
	return "", errors.New("unexpected type of Expression")
}
