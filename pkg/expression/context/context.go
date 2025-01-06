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
	"fmt"
	"time"

	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
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
	// GetTiDBRedactLog returns the value of the 'tidb_redact_log' system variable.
	GetTiDBRedactLog() string
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
	// GetSessionVars gets the session variables.
	GetSessionVars() *variable.SessionVars
	// Value returns the value associated with this context for key.
	Value(key fmt.Stringer) any
	// SetValue saves a value associated with this context for key.
	SetValue(key fmt.Stringer, value any)
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
