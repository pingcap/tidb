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
	"context"
	"fmt"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"time"

	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// EvalContext is used to evaluate an expression
type EvalContext interface {
	//// GetSessionVars gets the session variables.
	//GetSessionVars() *variable.SessionVars
	// Value returns the value associated with this context for key.
	Value(key fmt.Stringer) interface{}
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

type EvalVars struct {
	SQLMode                     mysql.SQLMode
	Location                    func() *time.Location
	GetSystemVar                func(name string) (string, bool)
	GetCharsetInfo              func() (charset, collation string)
	GlobalVarsAccessor          variable.GlobalVarAccessor
	User                        *auth.UserIdentity
	CurrentDB                   string
	LastFoundRows               uint64
	ResourceGroupName           string
	ConnectionID                uint64
	ConnectionInfo              *variable.ConnectionInfo
	ActiveRoles                 []*auth.RoleIdentity
	SetLastInsertID             func(insertID uint64)
	MaxExecutionTime            uint64
	SequenceState               *variable.SequenceState
	GetUserVarVal               func(name string) (types.Datum, bool)
	SetUserVarVal               func(name string, dt types.Datum)
	CurrInsertValues            chunk.Row
	SetStringUserVar            func(name string, strVal string, collation string)
	PlanCacheParams             *variable.PlanCacheParamList
	GetSessionOrGlobalSystemVar func(ctx context.Context, name string) (string, error)
	EnableVectorizedExpression  bool
	SQLKiller                   sqlkiller.SQLKiller
	StmtCtx                     *StmtCtx
}

type StmtCtx struct {
	tc                     types.Context
	ec                     errctx.Context
	GetStaleTSO            func() (uint64, error)
	WarningCount           func() uint16
	TruncateWarnings       func(start int) []stmtctx.SQLWarn
	UseCache               bool
	PrevLastInsertID       uint64
	PrevAffectedRows       int64
	BadNullAsWarning       bool
	GetOrStoreStmtCache    func(key stmtctx.StmtCacheKey, value interface{}) interface{}
	InSelectStmt           bool
	InInsertStmt           bool
	InUpdateStmt           bool
	InDeleteStmt           bool
	InExplainStmt          bool
	DividedByZeroAsWarning bool
	TableIDs               []int64
	AppendExtraWarning     func(err error)
}

func NewEvalVars(vars *variable.SessionVars) *EvalVars {
	return &EvalVars{
		SQLMode:                     vars.SQLMode,
		Location:                    vars.Location,
		GetSystemVar:                vars.GetSystemVar,
		GetCharsetInfo:              vars.GetCharsetInfo,
		GlobalVarsAccessor:          vars.GlobalVarsAccessor,
		User:                        vars.User,
		CurrentDB:                   vars.CurrentDB,
		LastFoundRows:               vars.LastFoundRows,
		ResourceGroupName:           vars.ResourceGroupName,
		ConnectionID:                vars.ConnectionID,
		ConnectionInfo:              vars.ConnectionInfo,
		ActiveRoles:                 vars.ActiveRoles,
		SetLastInsertID:             vars.SetLastInsertID,
		MaxExecutionTime:            vars.MaxExecutionTime,
		SequenceState:               vars.SequenceState,
		SetUserVarVal:               vars.SetUserVarVal,
		GetUserVarVal:               vars.GetUserVarVal,
		CurrInsertValues:            vars.CurrInsertValues,
		SetStringUserVar:            vars.SetStringUserVar,
		PlanCacheParams:             vars.PlanCacheParams,
		GetSessionOrGlobalSystemVar: vars.GetSessionOrGlobalSystemVar,
		SQLKiller:                   vars.SQLKiller,
		StmtCtx: &StmtCtx{
			tc:                     vars.StmtCtx.TypeCtx(),
			ec:                     vars.StmtCtx.ErrCtx(),
			GetStaleTSO:            vars.StmtCtx.GetStaleTSO,
			WarningCount:           vars.StmtCtx.WarningCount,
			TruncateWarnings:       vars.StmtCtx.TruncateWarnings,
			UseCache:               vars.StmtCtx.UseCache,
			PrevLastInsertID:       vars.StmtCtx.PrevLastInsertID,
			PrevAffectedRows:       vars.StmtCtx.PrevAffectedRows,
			BadNullAsWarning:       vars.StmtCtx.BadNullAsWarning,
			GetOrStoreStmtCache:    vars.StmtCtx.GetOrStoreStmtCache,
			InSelectStmt:           vars.StmtCtx.InSelectStmt,
			InInsertStmt:           vars.StmtCtx.InInsertStmt,
			InUpdateStmt:           vars.StmtCtx.InUpdateStmt,
			InDeleteStmt:           vars.StmtCtx.InDeleteStmt,
			InExplainStmt:          vars.StmtCtx.InExplainStmt,
			DividedByZeroAsWarning: vars.StmtCtx.DividedByZeroAsWarning,
			TableIDs:               vars.StmtCtx.TableIDs,
			AppendExtraWarning:     vars.StmtCtx.AppendExtraWarning,
		},
	}
}

func evalVars(ctx EvalContext) *EvalVars {
	return NewEvalVars(ctx.(sessionctx.Context).GetSessionVars())
}

func (ctx *StmtCtx) TypeCtx() types.Context {
	return ctx.tc
}

func (ctx *StmtCtx) TypeFlags() types.Flags {
	return ctx.tc.Flags()
}

func (ctx *StmtCtx) ErrCtx() errctx.Context {
	return ctx.ec
}

func (ctx *StmtCtx) HandleTruncate(err error) error {
	return ctx.tc.HandleTruncate(err)
}

func (ctx *StmtCtx) HandleOverflow(err error, warnError error) error {
	if types.ErrOverflow.Equal(err) {
		return ctx.HandleErrorWithAlias(err, warnError, warnError)
	}
	return err
}

func (ctx *StmtCtx) AppendWarning(err error) {
	ctx.tc.AppendWarning(err)
}

func (ctx *StmtCtx) HandleError(err error) error {
	return ctx.ec.HandleError(err)
}

func (ctx *StmtCtx) HandleErrorWithAlias(internalErr error, err error, warnErr error) error {
	return ctx.ec.HandleErrorWithAlias(internalErr, err, warnErr)
}

func (ctx *StmtCtx) TimeZone() *time.Location {
	tc := ctx.TypeCtx()
	return tc.Location()
}
