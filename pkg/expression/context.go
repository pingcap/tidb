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

package expression

import (
	"time"

	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/expression/contextopt"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// EvalContext is used to evaluate an expression
type EvalContext = context.EvalContext

// BuildContext is used to build an expression
type BuildContext = context.BuildContext

// AggFuncBuildContext is used to build an aggregation expression
type AggFuncBuildContext = context.ExprContext

// OptionalEvalPropKey is an alias of context.OptionalEvalPropKey
type OptionalEvalPropKey = context.OptionalEvalPropKey

// OptionalEvalPropProvider is an alias of context.OptionalEvalPropProvider
type OptionalEvalPropProvider = context.OptionalEvalPropProvider

// OptionalEvalPropKeySet is an alias of context.OptionalEvalPropKeySet
type OptionalEvalPropKeySet = context.OptionalEvalPropKeySet

// OptionalEvalPropDesc is an alias of context.OptionalEvalPropDesc
type OptionalEvalPropDesc = context.OptionalEvalPropDesc

func sqlMode(ctx EvalContext) mysql.SQLMode {
	return ctx.SQLMode()
}

func typeCtx(ctx EvalContext) types.Context {
	return ctx.TypeCtx()
}

func errCtx(ctx EvalContext) errctx.Context {
	return ctx.ErrCtx()
}

func location(ctx EvalContext) (loc *time.Location) {
	return ctx.Location()
}

func warningCount(ctx EvalContext) int {
	return ctx.WarningCount()
}

func truncateWarnings(ctx EvalContext, start int) []stmtctx.SQLWarn {
	return ctx.TruncateWarnings(start)
}

// assertionEvalContext is used to do some assertions.
// It is only used in tests.
type assertionEvalContext struct {
	EvalContext
	fn builtinFunc
}

func wrapEvalAssert(ctx EvalContext, fn builtinFunc) (ret *assertionEvalContext) {
	originalCtx := ctx
	if assertCtx, ok := ctx.(*assertionEvalContext); ok {
		originalCtx = assertCtx.EvalContext
		if assertCtx.fn == fn {
			ret = assertCtx
		}
	}

	checkEvalCtx(originalCtx)
	if ret == nil {
		ret = &assertionEvalContext{EvalContext: originalCtx, fn: fn}
	}

	return
}

func checkEvalCtx(ctx EvalContext) {
	loc := ctx.Location().String()
	tc := ctx.TypeCtx()
	tcLoc := tc.Location().String()
	intest.Assert(loc == tcLoc, "location mismatch, evalCtx: %s, typeCtx: %s", loc, tcLoc)
	if ctx.GetOptionalPropSet().Contains(context.OptPropSessionVars) {
		vars, err := contextopt.SessionVarsPropReader{}.GetSessionVars(ctx)
		intest.AssertNoError(err)
		stmtLoc := vars.StmtCtx.TimeZone().String()
		intest.Assert(loc == stmtLoc, "location mismatch, evalCtx: %s, stmtCtx: %s", loc, stmtLoc)
	}
}

func (ctx *assertionEvalContext) GetOptionalPropProvider(key OptionalEvalPropKey) (OptionalEvalPropProvider, bool) {
	var requiredOptionalProps OptionalEvalPropKeySet
	if ctx.fn != nil {
		requiredOptionalProps = ctx.fn.RequiredOptionalEvalProps()
	}

	intest.Assert(
		requiredOptionalProps.Contains(key),
		"optional property '%s' is read in function '%T' but not declared in RequiredOptionalEvalProps",
		key, ctx.fn,
	)
	return ctx.EvalContext.GetOptionalPropProvider(key)
}

// StringerWithCtx is the interface for expressions that can be stringified with context.
type StringerWithCtx interface {
	// StringWithCtx returns the string representation of the expression with context.
	// NOTE: any implementation of `StringWithCtx` should not panic if the context is nil.
	StringWithCtx(redact string) string
}
