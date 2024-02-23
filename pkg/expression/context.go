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
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// EvalContext is used to evaluate an expression
type EvalContext = context.EvalContext

// BuildContext is used to build an expression
type BuildContext = context.BuildContext

func sqlMode(ctx EvalContext) mysql.SQLMode {
	return ctx.SQLMode()
}

func typeCtx(ctx EvalContext) types.Context {
	return ctx.GetSessionVars().StmtCtx.TypeCtx()
}

func errCtx(ctx EvalContext) errctx.Context {
	return ctx.GetSessionVars().StmtCtx.ErrCtx()
}

func location(ctx EvalContext) *time.Location {
	vars := ctx.GetSessionVars()
	sc := vars.StmtCtx
	tc := sc.TypeCtx()
	intest.Assert(vars.Location() == sc.TimeZone())
	intest.Assert(sc.TimeZone() == tc.Location())
	return tc.Location()
}

func warningCount(ctx EvalContext) int {
	return int(ctx.GetSessionVars().StmtCtx.WarningCount())
}

func truncateWarnings(ctx EvalContext, start int) []stmtctx.SQLWarn {
	return ctx.GetSessionVars().StmtCtx.TruncateWarnings(start)
}
