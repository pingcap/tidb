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
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
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

// noFrameWindowFuncs is the functions that operate on the entire partition,
// they should not have frame specifications.
var noFrameWindowFuncs = map[string]struct{}{
	ast.WindowFuncCumeDist:    {},
	ast.WindowFuncDenseRank:   {},
	ast.WindowFuncLag:         {},
	ast.WindowFuncLead:        {},
	ast.WindowFuncNtile:       {},
	ast.WindowFuncPercentRank: {},
	ast.WindowFuncRank:        {},
	ast.WindowFuncRowNumber:   {},
}

// NeedFrame checks if the function need frame specification.
func NeedFrame(name string) bool {
	_, ok := noFrameWindowFuncs[strings.ToLower(name)]
	return !ok
}

// HashCode creates the hashcode for WindowFuncDesc which can be used to identify itself from other WindowFuncDesc.
func (f *WindowFuncDesc) HashCode(sc *stmtctx.StatementContext) []byte {
	// the max length of WindowFuncName ('percent_rank') is 12.
	// Arg is commonly Column whose hashcode has the length 9,
	// so we pre-alloc 10 bytes for Arg's hashcode.
	// we pre-alloc total bytes size = SizeOf(WindowFuncName)+SizeOf(Encode(args))
	// 								 = 12+SizeOf(len(args))+len(args)*(SizeOf(SizeOf(arg.hashcode))+SizeOf(arg.hashcode))
	//                  	         = 12+4+len(args)*(4+SizeOf(arg.hashcode))
	//                  	         = 16+len(args)*14
	hashcode := make([]byte, 0, 16+len(f.Args)*14)
	hashcode = codec.EncodeCompactBytes(hashcode, hack.Slice(f.Name))
	argHashCode := func(i int) []byte { return f.Args[i].HashCode(sc) }
	hashcode = codec.Encode(hashcode, argHashCode, len(f.Args))
	return hashcode
}
