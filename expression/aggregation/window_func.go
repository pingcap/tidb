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
// See the License for the specific language governing permissions and
// limitations under the License.

package aggregation

import (
	"strings"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
)

// WindowFuncDesc describes a window function signature, only used in planner.
type WindowFuncDesc struct {
	baseFuncDesc
}

// NewWindowFuncDesc creates a window function signature descriptor.
func NewWindowFuncDesc(ctx sessionctx.Context, name string, args []expression.Expression) (*WindowFuncDesc, error) {
	switch strings.ToLower(name) {
	case ast.WindowFuncNthValue:
		val, isNull, ok := expression.GetUint64FromConstant(args[1])
		// nth_value does not allow `0`, but allows `null`.
		if !ok || (val == 0 && !isNull) {
			return nil, nil
		}
	case ast.WindowFuncNtile:
		val, isNull, ok := expression.GetUint64FromConstant(args[0])
		// ntile does not allow `0`, but allows `null`.
		if !ok || (val == 0 && !isNull) {
			return nil, nil
		}
	case ast.WindowFuncLead, ast.WindowFuncLag:
		if len(args) < 2 {
			break
		}
		_, isNull, ok := expression.GetUint64FromConstant(args[1])
		if !ok || isNull {
			return nil, nil
		}
	}
	base, err := newBaseFuncDesc(ctx, name, args)
	if err != nil {
		return nil, err
	}
	return &WindowFuncDesc{base}, nil
}

// ignoreFrameWindowFuncs is the functions that operate on the entire partition,
// they should not have frame specifications.
// see https://dev.mysql.com/doc/refman/8.0/en/window-functions-frames.html
var ignoreFrameWindowFuncs = map[string]struct{}{
	ast.WindowFuncCumeDist:    {},
	ast.WindowFuncDenseRank:   {},
	ast.WindowFuncLag:         {},
	ast.WindowFuncLead:        {},
	ast.WindowFuncNtile:       {},
	ast.WindowFuncPercentRank: {},
	ast.WindowFuncRank:        {},
	ast.WindowFuncRowNumber:   {},
}

// IgnoreFrame checks if the function ignores frame specification.
func IgnoreFrame(name string) bool {
	_, ok := ignoreFrameWindowFuncs[strings.ToLower(name)]
	return ok
}
