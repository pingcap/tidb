// Copyright 2024 PingCAP, Inc.
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

package context

import (
	"time"

	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/mathutil"
)

// EvalContext is used to evaluate an expression
type EvalContext interface {
	// CtxID indicates the id of the context.
	CtxID() uint64
	// SQLMode returns the sql mode
	SQLMode() mysql.SQLMode
	// TypeCtx returns the types.Context
	TypeCtx() types.Context
	// ErrCtx returns the errctx.Context
	ErrCtx() errctx.Context
	// Location returns the timezone info
	Location() *time.Location
	// AppendWarning append warnings to the context.
	AppendWarning(err error)
	// WarningCount gets warning count.
	WarningCount() int
	// TruncateWarnings truncates warnings begin from start and returns the truncated warnings.
	TruncateWarnings(start int) []stmtctx.SQLWarn
	// CurrentDB return the current database name
	CurrentDB() string
	// CurrentTime returns the current time.
	// Multiple calls for CurrentTime() should return the same value for the same `CtxID`.
	CurrentTime() (time.Time, error)
	// GetMaxAllowedPacket returns the value of the 'max_allowed_packet' system variable.
	GetMaxAllowedPacket() uint64
	// GetDefaultWeekFormatMode returns the value of the 'default_week_format' system variable.
	GetDefaultWeekFormatMode() string
	// GetDivPrecisionIncrement returns the specified value of DivPrecisionIncrement.
	GetDivPrecisionIncrement() int
	// RequestVerification verifies user privilege
	RequestVerification(db, table, column string, priv mysql.PrivilegeType) bool
	// RequestDynamicVerification verifies user privilege for a DYNAMIC privilege.
	RequestDynamicVerification(privName string, grantable bool) bool
	// GetOptionalPropSet returns the optional properties provided by this context.
	GetOptionalPropSet() OptionalEvalPropKeySet
	// GetOptionalPropProvider gets the optional property provider by key
	GetOptionalPropProvider(OptionalEvalPropKey) (OptionalEvalPropProvider, bool)
}

// BuildContext is used to build an expression
type BuildContext interface {
	// GetEvalCtx returns the EvalContext.
	GetEvalCtx() EvalContext
	// GetCharsetInfo gets charset and collation for current context.
	GetCharsetInfo() (string, string)
	// GetDefaultCollationForUTF8MB4 returns the default collation of UTF8MB4.
	GetDefaultCollationForUTF8MB4() string
	// GetBlockEncryptionMode returns the variable `block_encryption_mode`.
	GetBlockEncryptionMode() string
	// GetSysdateIsNow returns a bool to determine whether Sysdate is an alias of Now function.
	// It is the value of variable `tidb_sysdate_is_now`.
	GetSysdateIsNow() bool
	// GetNoopFuncsMode returns the noop function mode: OFF/ON/WARN values as 0/1/2.
	GetNoopFuncsMode() int
	// Rng is used to generate random values.
	Rng() *mathutil.MysqlRng
	// IsUseCache indicates whether to cache the build expression in plan cache.
	IsUseCache() bool
	// SetSkipPlanCache sets to skip the plan cache and records the reason.
	SetSkipPlanCache(reason error)
	// AllocPlanColumnID allocates column id for plan.
	AllocPlanColumnID() int64
	// SetInNullRejectCheck sets the flag to indicate whether the expression is in null reject check.
	SetInNullRejectCheck(in bool)
	// IsInNullRejectCheck returns the flag to indicate whether the expression is in null reject check.
	IsInNullRejectCheck() bool
	// SetInUnionCast sets the flag to indicate whether the expression is in union cast.
	SetInUnionCast(in bool)
	// IsInUnionCast indicates whether executing in special cast context that negative unsigned num will be zero.
	IsInUnionCast() bool
	// Deprecated: This method is deprecated and may be removed in the future because it is coupled with statement.
	// InInsertOrUpdate returns whether when are building an expression for insert or update statement.
	InInsertOrUpdate() bool
	// ConnectionID indicates the connection ID of the current session.
	// If the context is not in a session, it should return 0.
	ConnectionID() uint64
}

// ExprContext contains full context for expression building and evaluating.
// It also provides some additional information for to build aggregate functions.
type ExprContext interface {
	BuildContext
	// GetWindowingUseHighPrecision determines whether to compute window operations without loss of precision.
	// see https://dev.mysql.com/doc/refman/8.0/en/window-function-optimization.html for more details.
	GetWindowingUseHighPrecision() bool
	// GetGroupConcatMaxLen returns the value of the 'group_concat_max_len' system variable.
	GetGroupConcatMaxLen() uint64
}

// AssertLocationWithSessionVars asserts the location in the context and session variables are the same.
// It is only used for testing.
func AssertLocationWithSessionVars(ctxLoc *time.Location, vars *variable.SessionVars) {
	varsLoc := vars.Location()
	stmtLoc := vars.StmtCtx.TimeZone()
	intest.Assert(ctxLoc == varsLoc && ctxLoc == stmtLoc,
		"location mismatch, ctxLoc: %s, varsLoc: %s, stmtLoc: %s",
		ctxLoc.String(), varsLoc.String(), stmtLoc.String(),
	)
}
