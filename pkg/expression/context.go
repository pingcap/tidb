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
	"fmt"
	"time"

	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// EvalContext is used to evaluate an expression
type EvalContext interface {
	// GetSessionVars gets the session variables.
	GetSessionVars() *variable.SessionVars
	// Value returns the value associated with this context for key.
	Value(key fmt.Stringer) any
	// IsDDLOwner checks whether this session is DDL owner.
	IsDDLOwner() bool
	// GetAdvisoryLock acquires an advisory lock (aka GET_LOCK()).
	GetAdvisoryLock(string, int64) error
	// IsUsedAdvisoryLock checks for existing locks (aka IS_USED_LOCK()).
	IsUsedAdvisoryLock(string) uint64
	// ReleaseAdvisoryLock releases an advisory lock (aka RELEASE_LOCK()).
	ReleaseAdvisoryLock(string) bool
	// ReleaseAllAdvisoryLocks releases all advisory locks that this session holds.
	ReleaseAllAdvisoryLocks() int
	// GetStore returns the store of session.
	GetStore() kv.Storage
	// GetInfoSchema returns the current infoschema
	GetInfoSchema() sessionctx.InfoschemaMetaVersion
	// GetDomainInfoSchema returns the latest information schema in domain
	GetDomainInfoSchema() sessionctx.InfoschemaMetaVersion
}

func sqlMode(ctx EvalContext) mysql.SQLMode {
	return ctx.GetSessionVars().SQLMode
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

// BuildContext is used to build an expression
type BuildContext interface {
	EvalContext
	// GetSessionVars gets the session variables.
	GetSessionVars() *variable.SessionVars
	// SetValue saves a value associated with this context for key.
	SetValue(key fmt.Stringer, value any)
	// BuiltinFunctionUsageInc increase the counting of each builtin function usage
	// Notice that this is a thread safe function
	BuiltinFunctionUsageInc(scalarFuncSigName string)
}
