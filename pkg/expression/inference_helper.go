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

package expression

import (
	"encoding/json"
	"fmt"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// EmbedTextInfo describes the constant arguments of an EMBED_TEXT generated-column expression.
type EmbedTextInfo struct {
	ModelNameWithProvider string
	OptsInJSON            string
}

// Equal compares EMBED_TEXT metadata. Options are intentionally compared as
// raw JSON text because extraction preserves the user-specified constant rather
// than producing a canonical JSON representation.
func (info *EmbedTextInfo) Equal(other *EmbedTextInfo) bool {
	if info == nil || other == nil {
		return info == other
	}
	return info.ModelNameWithProvider == other.ModelNameWithProvider && info.OptsInJSON == other.OptsInJSON
}

type embedTextFnVisitor struct {
	found bool
}

func (v *embedTextFnVisitor) Enter(in ast.Node) (ast.Node, bool) {
	if fnCall, ok := in.(*ast.FuncCallExpr); ok && fnCall.FnName.L == ast.EmbedText {
		v.found = true
		return in, true
	}
	return in, false
}

func (*embedTextFnVisitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

// ContainsEmbedTextFunc reports whether expr contains EMBED_TEXT at any level.
func ContainsEmbedTextFunc(expr ast.ExprNode) bool {
	if expr == nil {
		return false
	}
	visitor := &embedTextFnVisitor{}
	expr.Accept(visitor)
	return visitor.found
}

// IsEmbedTextFuncCall reports whether expr is a direct EMBED_TEXT call. It does
// not validate the argument shape; use ExtractEmbedTextInfo for that.
func IsEmbedTextFuncCall(expr ast.ExprNode) bool {
	fnCall, ok := expr.(*ast.FuncCallExpr)
	return ok && fnCall.FnName.L == ast.EmbedText
}

// ExtractEmbedTextInfo validates a direct EMBED_TEXT call used by a generated
// column and returns its constant model and options.
func ExtractEmbedTextInfo(expr ast.ExprNode) (*EmbedTextInfo, error) {
	fnCall, ok := expr.(*ast.FuncCallExpr)
	if !ok || fnCall.FnName.L != ast.EmbedText {
		return nil, fmt.Errorf("only generated columns using EMBED_TEXT() are allowed")
	}
	if len(fnCall.Args) < 2 || len(fnCall.Args) > 3 {
		return nil, fmt.Errorf("invalid EMBED_TEXT() usage")
	}

	modelConst, ok := fnCall.Args[0].(ast.ValueExpr)
	if !ok {
		return nil, fmt.Errorf("EMBED_TEXT() only accepts model name using string constant")
	}
	model, ok := modelConst.GetValue().(string)
	if !ok {
		return nil, fmt.Errorf("EMBED_TEXT() only accepts model name using string constant")
	}

	info := &EmbedTextInfo{ModelNameWithProvider: model}
	if len(fnCall.Args) == 2 {
		return info, nil
	}

	optsConst, ok := fnCall.Args[2].(ast.ValueExpr)
	if !ok {
		return nil, fmt.Errorf("EMBED_TEXT() only accepts JSON options using string constant")
	}
	opts, ok := optsConst.GetValue().(string)
	if !ok {
		return nil, fmt.Errorf("EMBED_TEXT() only accepts JSON options using string constant")
	}
	if opts != "" {
		var parsed map[string]any
		if err := json.Unmarshal([]byte(opts), &parsed); err != nil || parsed == nil {
			return nil, fmt.Errorf("EMBED_TEXT expects options in JSON format")
		}
	}
	info.OptsInJSON = opts
	return info, nil
}
