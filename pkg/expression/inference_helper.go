// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// AutoEmbedInfo returns the info about **how to** do auto embedding. Auto embedding
// is done by a generated column using EMBED_TEXT() function.
type AutoEmbedInfo struct {
	ModelNameWithProvider string
	OptsInJSON            string // Optional
}

type autoEmbedFnVisitor struct {
	found bool
}

func (v *autoEmbedFnVisitor) Enter(in ast.Node) (ast.Node, bool) {
	if fnCall, ok := in.(*ast.FuncCallExpr); ok {
		if fnCall.FnName.L == ast.EmbedText {
			v.found = true
			return in, true
		}
	}
	return in, false
}

func (v *autoEmbedFnVisitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

// ContainsAutoEmbedFnAST returns true if the given expression AST contains an auto-embedding
// function call at any level of the expression tree.
func ContainsAutoEmbedFnAST(expr ast.ExprNode) bool {
	visitor := &autoEmbedFnVisitor{}
	expr.Accept(visitor)
	return visitor.found
}

// IsAutoEmbedFnCallAST checks whether the given expression AST is a direct function call to EMBED_TEXT().
// Note that even if this function returns true, the expression may not be in a supported form.
// In such cases, errors should be generated. To additionally check whether this expression is
// in a supported form, use ExtractAutoEmbedInfoFromAST() after IsAutoEmbedFnCallAST() returns true.
func IsAutoEmbedFnCallAST(expr ast.ExprNode) bool {
	fnCall, ok := expr.(*ast.FuncCallExpr)
	if !ok {
		return false
	}
	if fnCall.FnName.L != ast.EmbedText {
		return false
	}
	return true
}

// ExtractAutoEmbedInfoFromAST extracts the auto-embedding information from an auto-embedding
// direct function call expression.
// Returns error if the expression is not an auto-embedding expression or if the expression
// is not in a supported form.
func ExtractAutoEmbedInfoFromAST(expr ast.ExprNode) (*AutoEmbedInfo, error) {
	fnCall, ok := expr.(*ast.FuncCallExpr)
	if !ok {
		return nil, fmt.Errorf("only generated column using EMBED_TEXT() are allowed")
	}
	if fnCall.FnName.L != ast.EmbedText {
		return nil, fmt.Errorf("only generated column using EMBED_TEXT() are allowed")
	}
	if len(fnCall.Args) < 2 {
		return nil, fmt.Errorf("invalid EMBED_TEXT() usage")
	}

	// For now, let's reject the case where the generated column is written as:
	//
	// vec_col = EMBED_TEXT(model_col, text_col)
	//                      ^^^^^^^^^ col not supported, must be a string constant
	//
	// Because our final form will be:
	//
	// VEC_XX_DISTANCE(EMBED_TEXT(model_col, text_col), vec_col)
	//
	// In this case, EMBED_TEXT() cannot be const-folded any more, must be evaluated row by row,
	// whose performance will be very bad.

	modelExpr := fnCall.Args[0]
	modelConst, ok := modelExpr.(ast.ValueExpr)
	if !ok {
		return nil, fmt.Errorf("EMBED_TEXT() only accepts model name using string constant")
	}
	modelVal := modelConst.GetValue()
	modelValStr, ok := modelVal.(string)
	if !ok {
		return nil, fmt.Errorf("EMBED_TEXT() only accepts model name using string constant")
	}

	info := &AutoEmbedInfo{
		ModelNameWithProvider: modelValStr,
		OptsInJSON:            "",
	}

	if len(fnCall.Args) > 2 {
		optsExpr := fnCall.Args[2]
		optsConst, ok := optsExpr.(ast.ValueExpr)
		if !ok {
			return nil, fmt.Errorf("EMBED_TEXT() only accepts JSON options using string constant")
		}
		optsVal := optsConst.GetValue()
		optsValStr, ok := optsVal.(string)
		if !ok {
			return nil, fmt.Errorf("EMBED_TEXT() only accepts JSON options using string constant")
		}
		info.OptsInJSON = optsValStr

		// We don't check whether OptsInJSON is a valid JSON, because embedding providers
		// will check it anyway.
	}

	return info, nil
}
