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

// ExtractAutoEmbedInfoFromAST returns the model name and options if the expression is a valid
// auto-embedding function call, or an error otherwise.
func ExtractAutoEmbedInfoFromAST(expr ast.ExprNode) (*AutoEmbedInfo, error) {
	fnCall, ok := expr.(*ast.FuncCallExpr)
	if !ok {
		return nil, fmt.Errorf("not a function call")
	}
	if fnCall.FnName.L != ast.EmbedText {
		return nil, fmt.Errorf("not an EMBED_TEXT function")
	}
	if len(fnCall.Args) < 2 || len(fnCall.Args) > 3 {
		return nil, fmt.Errorf("invalid EMBED_TEXT arguments")
	}

	argModel, ok := fnCall.Args[0].(ast.ValueExpr)
	if !ok {
		return nil, fmt.Errorf("EMBED_TEXT requires the model name to be a string literal")
	}
	modelNameWithProvider, ok := argModel.GetValue().(string)
	if !ok {
		return nil, fmt.Errorf("EMBED_TEXT requires the model name to be a string literal")
	}

	argColumn, ok := fnCall.Args[1].(*ast.ColumnNameExpr)
	if !ok || argColumn.Name == nil {
		return nil, fmt.Errorf("EMBED_TEXT requires the column name as the second argument")
	}

	if len(fnCall.Args) == 3 {
		argOpts, ok := fnCall.Args[2].(ast.ValueExpr)
		if !ok {
			return nil, fmt.Errorf("EMBED_TEXT requires options to be a string literal")
		}
		optsInJSON, ok := argOpts.GetValue().(string)
		if !ok {
			return nil, fmt.Errorf("EMBED_TEXT requires options to be a string literal")
		}
		return &AutoEmbedInfo{
			ModelNameWithProvider: modelNameWithProvider,
			OptsInJSON:            optsInJSON,
		}, nil
	}

	return &AutoEmbedInfo{
		ModelNameWithProvider: modelNameWithProvider,
		OptsInJSON:            "",
	}, nil
}
