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

package planctx

import (
	"iter"

	"github.com/pingcap/tidb/pkg/expression/exprctx"
	infoschema "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	tablelock "github.com/pingcap/tidb/pkg/lock/context"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// Common represents the common API for plan context and session context.
// Not sure how to name this interface, so it is named `Common` for now.
type Common interface {
	contextutil.ValueStoreContext
	// GetStore returns the store of session.
	GetStore() kv.Storage
	GetSessionVars() *variable.SessionVars
	// Deprecated: the semantics of session.GetInfoSchema() is ambiguous
	// If you want to get the infoschema of the current transaction in SQL layer,
	// use sessiontxn.GetTxnManager(ctx).GetTxnInfoSchema().
	// If you want to get the latest infoschema use `GetLatestInfoSchema`
	GetInfoSchema() infoschema.MetaOnlyInfoSchema
	// GetLatestInfoSchema returns the latest information schema.
	// except schema of physical schema objects, the information schema returned
	// also includes the temporary table definitions stored in session.
	GetLatestInfoSchema() infoschema.MetaOnlyInfoSchema
	// GetLatestISWithoutSessExt is same as GetLatestInfoSchema, except that it
	// does NOT include the temporary table definitions stored in session.
	GetLatestISWithoutSessExt() infoschema.MetaOnlyInfoSchema
	// GetClient gets a kv.Client.
	GetClient() kv.Client
	// GetMPPClient gets a kv.MPPClient.
	GetMPPClient() kv.MPPClient
	GetSessionManager() sessmgr.Manager
	// GetSQLExecutor returns the sqlexec.SQLExecutor.
	GetSQLExecutor() sqlexec.SQLExecutor
	// GetRestrictedSQLExecutor returns the sqlexec.RestrictedSQLExecutor.
	GetRestrictedSQLExecutor() sqlexec.RestrictedSQLExecutor
	// GetExprCtx returns the expression context of the session.
	GetExprCtx() exprctx.ExprContext
	// GetRangerCtx returns the context used in `ranger` related functions
	GetRangerCtx() *rangerctx.RangerContext
	// GetBuildPBCtx gets the ctx used in `ToPB` of the current session
	GetBuildPBCtx() *BuildPBContext
	IsCrossKS() bool
	// UpdateColStatsUsage updates the column stats usage.
	UpdateColStatsUsage(predicateColumns iter.Seq[model.TableItemID])
	// Txn returns the current transaction which is created before executing a statement.
	// The returned kv.Transaction is not nil, but it maybe pending or invalid.
	// If the active parameter is true, call this function will wait for the pending txn
	// to become valid.
	Txn(active bool) (kv.Transaction, error)
	// HasDirtyContent checks whether there's dirty update on the given table.
	HasDirtyContent(tid int64) bool
	// BuiltinFunctionUsageInc increase the counting of each builtin function usage
	// Notice that this is a thread safe function
	BuiltinFunctionUsageInc(scalarFuncSigName string)
}

// PlanContext is the context for building plan.
type PlanContext interface {
	Common
	tablelock.TableLockReadContext
	// GetNullRejectCheckExprCtx gets the expression context with null rejected check.
	GetNullRejectCheckExprCtx() exprctx.ExprContext
	// AdviseTxnWarmup advises the txn to warm up.
	AdviseTxnWarmup() error
	// SetReadonlyUserVarMap sets the readonly user variable map.
	SetReadonlyUserVarMap(readonlyUserVars map[string]struct{})
	// GetReadonlyUserVarMap gets the readonly user variable map.
	GetReadonlyUserVarMap() map[string]struct{}
	// Reset reset the local context.
	Reset()
}

// EmptyPlanContextExtended is used to provide some empty implementations for PlanContext.
// It is used by some mock contexts that are only required to implement PlanContext
// but do not care about the actual implementation.
type EmptyPlanContextExtended struct{}

// AdviseTxnWarmup advises the txn to warm up.
func (EmptyPlanContextExtended) AdviseTxnWarmup() error { return nil }

// SetReadonlyUserVarMap sets the readonly user variable map.
func (EmptyPlanContextExtended) SetReadonlyUserVarMap(map[string]struct{}) {}

// GetReadonlyUserVarMap gets the readonly user variable map.
func (EmptyPlanContextExtended) GetReadonlyUserVarMap() map[string]struct{} { return nil }

// Reset implements the PlanContext interface.
func (EmptyPlanContextExtended) Reset() {}

// BuildPBContext is used to build the `*tipb.Executor` according to the plan.
type BuildPBContext struct {
	ExprCtx exprctx.BuildContext
	Client  kv.Client

	TiFlashFastScan                    bool
	TiFlashFineGrainedShuffleBatchSize uint64

	// the following fields are used to build `expression.PushDownContext`.
	// TODO: it'd be better to embed `expression.PushDownContext` in `BuildPBContext`. But `expression` already
	// depends on this package, so we need to move `expression.PushDownContext` to a standalone package first.
	GroupConcatMaxLen uint64
	InExplainStmt     bool
	WarnHandler       contextutil.WarnAppender
	ExtraWarnghandler contextutil.WarnAppender
}

// GetExprCtx returns the expression context.
func (b *BuildPBContext) GetExprCtx() exprctx.BuildContext {
	return b.ExprCtx
}

// GetClient returns the kv client.
func (b *BuildPBContext) GetClient() kv.Client {
	return b.Client
}

// Detach detaches this context from the session context.
//
// NOTE: Though this session context can be used parallelly with this context after calling
// it, the `StatementContext` cannot. The session context should create a new `StatementContext`
// before executing another statement.
func (b *BuildPBContext) Detach(staticExprCtx exprctx.BuildContext) *BuildPBContext {
	newCtx := *b
	newCtx.ExprCtx = staticExprCtx
	return &newCtx
}
