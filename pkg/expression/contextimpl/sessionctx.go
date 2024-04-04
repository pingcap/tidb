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

package contextimpl

import (
	"context"
	"math"
	"time"

	"github.com/pingcap/tidb/pkg/errctx"
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/expression/contextopt"
	infoschema "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

// sessionctx.Context + *ExprCtxExtendedImpl should implement `expression.BuildContext`
// Only used to assert `ExprCtxExtendedImpl` should implement all methods not in `sessionctx.Context`
var _ exprctx.BuildContext = struct {
	sessionctx.Context
	*ExprCtxExtendedImpl
}{}

// ExprCtxExtendedImpl extends the sessionctx.Context to implement `expression.BuildContext`
type ExprCtxExtendedImpl struct {
	*SessionEvalContext
}

// NewExprExtendedImpl creates a new ExprCtxExtendedImpl.
func NewExprExtendedImpl(sctx sessionctx.Context) *ExprCtxExtendedImpl {
	return &ExprCtxExtendedImpl{
		SessionEvalContext: NewSessionEvalContext(sctx),
	}
}

// GetEvalCtx returns the EvalContext.
func (ctx *ExprCtxExtendedImpl) GetEvalCtx() exprctx.EvalContext {
	return ctx.SessionEvalContext
}

// SessionEvalContext implements the `expression.EvalContext` interface to provide evaluation context in session.
type SessionEvalContext struct {
	sctx  sessionctx.Context
	props contextopt.OptionalEvalPropProviders
}

// NewSessionEvalContext creates a new SessionEvalContext.
func NewSessionEvalContext(sctx sessionctx.Context) *SessionEvalContext {
	ctx := &SessionEvalContext{sctx: sctx}
	// set all optional properties
	ctx.setOptionalProp(currentUserProp(sctx))
	ctx.setOptionalProp(contextopt.NewSessionVarsProvider(sctx))
	ctx.setOptionalProp(infoSchemaProp(sctx))
	ctx.setOptionalProp(contextopt.KVStorePropProvider(sctx.GetStore))
	ctx.setOptionalProp(sqlExecutorProp(sctx))
	ctx.setOptionalProp(sequenceOperatorProp(sctx))
	ctx.setOptionalProp(contextopt.NewAdvisoryLockPropProvider(sctx))
	ctx.setOptionalProp(contextopt.DDLOwnerInfoProvider(sctx.IsDDLOwner))
	// When EvalContext is created from a session, it should contain all the optional properties.
	intest.Assert(ctx.props.PropKeySet().IsFull())
	return ctx
}

func (ctx *SessionEvalContext) setOptionalProp(prop exprctx.OptionalEvalPropProvider) {
	intest.AssertFunc(func() bool {
		return !ctx.props.Contains(prop.Desc().Key())
	})
	ctx.props.Add(prop)
}

// CtxID returns the context id.
func (ctx *SessionEvalContext) CtxID() uint64 {
	return ctx.sctx.GetSessionVars().StmtCtx.CtxID()
}

// SQLMode returns the sql mode
func (ctx *SessionEvalContext) SQLMode() mysql.SQLMode {
	return ctx.sctx.GetSessionVars().SQLMode
}

// TypeCtx returns the types.Context
func (ctx *SessionEvalContext) TypeCtx() types.Context {
	return ctx.sctx.GetSessionVars().StmtCtx.TypeCtx()
}

// ErrCtx returns the errctx.Context
func (ctx *SessionEvalContext) ErrCtx() errctx.Context {
	return ctx.sctx.GetSessionVars().StmtCtx.ErrCtx()
}

// Location returns the timezone info
func (ctx *SessionEvalContext) Location() *time.Location {
	tc := ctx.TypeCtx()
	return tc.Location()
}

// AppendWarning append warnings to the context.
func (ctx *SessionEvalContext) AppendWarning(err error) {
	ctx.sctx.GetSessionVars().StmtCtx.AppendWarning(err)
}

// WarningCount gets warning count.
func (ctx *SessionEvalContext) WarningCount() int {
	return int(ctx.sctx.GetSessionVars().StmtCtx.WarningCount())
}

// TruncateWarnings truncates warnings begin from start and returns the truncated warnings.
func (ctx *SessionEvalContext) TruncateWarnings(start int) []stmtctx.SQLWarn {
	return ctx.sctx.GetSessionVars().StmtCtx.TruncateWarnings(start)
}

// CurrentDB returns the current database name
func (ctx *SessionEvalContext) CurrentDB() string {
	return ctx.sctx.GetSessionVars().CurrentDB
}

// CurrentTime returns the current time
func (ctx *SessionEvalContext) CurrentTime() (time.Time, error) {
	return getStmtTimestamp(ctx.sctx)
}

// GetMaxAllowedPacket returns the value of the 'max_allowed_packet' system variable.
func (ctx *SessionEvalContext) GetMaxAllowedPacket() uint64 {
	return ctx.sctx.GetSessionVars().MaxAllowedPacket
}

// GetDefaultWeekFormatMode returns the value of the 'default_week_format' system variable.
func (ctx *SessionEvalContext) GetDefaultWeekFormatMode() string {
	mode, ok := ctx.sctx.GetSessionVars().GetSystemVar(variable.DefaultWeekFormat)
	if !ok || mode == "" {
		return "0"
	}
	return mode
}

// GetDivPrecisionIncrement returns the specified value of DivPrecisionIncrement.
func (ctx *SessionEvalContext) GetDivPrecisionIncrement() int {
	return ctx.sctx.GetSessionVars().GetDivPrecisionIncrement()
}

// GetOptionalPropSet gets the optional property set from context
func (ctx *SessionEvalContext) GetOptionalPropSet() exprctx.OptionalEvalPropKeySet {
	return ctx.props.PropKeySet()
}

// GetOptionalPropProvider gets the optional property provider by key
func (ctx *SessionEvalContext) GetOptionalPropProvider(key exprctx.OptionalEvalPropKey) (exprctx.OptionalEvalPropProvider, bool) {
	return ctx.props.Get(key)
}

// RequestVerification verifies user privilege
func (ctx *SessionEvalContext) RequestVerification(db, table, column string, priv mysql.PrivilegeType) bool {
	checker := privilege.GetPrivilegeManager(ctx.sctx)
	if checker == nil {
		return true
	}
	return checker.RequestVerification(ctx.sctx.GetSessionVars().ActiveRoles, db, table, column, priv)
}

// RequestDynamicVerification verifies user privilege for a DYNAMIC privilege.
func (ctx *SessionEvalContext) RequestDynamicVerification(privName string, grantable bool) bool {
	checker := privilege.GetPrivilegeManager(ctx.sctx)
	if checker == nil {
		return true
	}
	return checker.RequestDynamicVerification(ctx.sctx.GetSessionVars().ActiveRoles, privName, grantable)
}

func getStmtTimestamp(ctx sessionctx.Context) (time.Time, error) {
	if ctx != nil {
		staleTSO, err := ctx.GetSessionVars().StmtCtx.GetStaleTSO()
		if staleTSO != 0 && err == nil {
			return oracle.GetTimeFromTS(staleTSO), nil
		} else if err != nil {
			logutil.BgLogger().Error("get stale tso failed", zap.Error(err))
		}
	}

	now := time.Now()

	if ctx == nil {
		return now, nil
	}

	sessionVars := ctx.GetSessionVars()
	timestampStr, err := sessionVars.GetSessionOrGlobalSystemVar(context.Background(), "timestamp")
	if err != nil {
		return now, err
	}

	timestamp, err := types.StrToFloat(sessionVars.StmtCtx.TypeCtx(), timestampStr, false)
	if err != nil {
		return time.Time{}, err
	}
	seconds, fractionalSeconds := math.Modf(timestamp)
	return time.Unix(int64(seconds), int64(fractionalSeconds*float64(time.Second))), nil
}

func currentUserProp(sctx sessionctx.Context) exprctx.OptionalEvalPropProvider {
	return contextopt.CurrentUserPropProvider(func() (*auth.UserIdentity, []*auth.RoleIdentity) {
		vars := sctx.GetSessionVars()
		return vars.User, vars.ActiveRoles
	})
}

func infoSchemaProp(sctx sessionctx.Context) contextopt.InfoSchemaPropProvider {
	return func(isDomain bool) infoschema.MetaOnlyInfoSchema {
		if isDomain {
			return sctx.GetDomainInfoSchema()
		}
		return sctx.GetInfoSchema()
	}
}

func sqlExecutorProp(sctx sessionctx.Context) contextopt.SQLExecutorPropProvider {
	return func() (contextopt.SQLExecutor, error) {
		return sctx.GetRestrictedSQLExecutor(), nil
	}
}

type sequenceOperator struct {
	sctx sessionctx.Context
	db   string
	name string
	tbl  util.SequenceTable
}

func (s *sequenceOperator) GetSequenceID() int64 {
	return s.tbl.GetSequenceID()
}

func (s *sequenceOperator) GetSequenceNextVal() (int64, error) {
	return s.tbl.GetSequenceNextVal(s.sctx, s.db, s.name)
}

func (s *sequenceOperator) SetSequenceVal(newVal int64) (int64, bool, error) {
	return s.tbl.SetSequenceVal(s.sctx, newVal, s.db, s.name)
}

func sequenceOperatorProp(sctx sessionctx.Context) contextopt.SequenceOperatorProvider {
	return func(db, name string) (contextopt.SequenceOperator, error) {
		sequence, err := util.GetSequenceByName(sctx.GetInfoSchema(), model.NewCIStr(db), model.NewCIStr(name))
		if err != nil {
			return nil, err
		}
		return &sequenceOperator{sctx, db, name, sequence}, nil
	}
}
