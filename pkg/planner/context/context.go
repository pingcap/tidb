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
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	infoschema "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	tablelock "github.com/pingcap/tidb/pkg/lock/context"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// PlanContext is the context for building plan.
type PlanContext interface {
	contextutil.ValueStoreContext
	tablelock.TableLockReadContext
	// GetSQLExecutor gets the SQLExecutor.
	GetSQLExecutor() sqlexec.SQLExecutor
	// GetRestrictedSQLExecutor gets the RestrictedSQLExecutor.
	GetRestrictedSQLExecutor() sqlexec.RestrictedSQLExecutor
	// GetExprCtx gets the expression context.
	GetExprCtx() exprctx.ExprContext
	// GetNullRejectCheckExprCtx gets the expression context with null rejected check.
	GetNullRejectCheckExprCtx() exprctx.ExprContext
	// GetStore returns the store of session.
	GetStore() kv.Storage
	// GetSessionVars gets the session variables.
	GetSessionVars() *variable.SessionVars
	// GetDomainInfoSchema returns the latest information schema in domain
	// Different with `domain.InfoSchema()`, the information schema returned by this method
	// includes the temporary table definitions stored in session
	GetDomainInfoSchema() infoschema.MetaOnlyInfoSchema
	// GetInfoSchema returns the current infoschema
	GetInfoSchema() infoschema.MetaOnlyInfoSchema
	// UpdateColStatsUsage updates the column stats usage.
	UpdateColStatsUsage(predicateColumns []model.TableItemID)
	// GetClient gets a kv.Client.
	GetClient() kv.Client
	// GetMPPClient gets a kv.MPPClient.
	GetMPPClient() kv.MPPClient
	// GetSessionManager gets the session manager.
	GetSessionManager() util.SessionManager
	// Txn returns the current transaction which is created before executing a statement.
	// The returned kv.Transaction is not nil, but it maybe pending or invalid.
	// If the active parameter is true, call this function will wait for the pending txn
	// to become valid.
	Txn(active bool) (kv.Transaction, error)
	// HasDirtyContent checks whether there's dirty update on the given table.
	HasDirtyContent(tid int64) bool
	// AdviseTxnWarmup advises the txn to warm up.
	AdviseTxnWarmup() error
	// GetRangerCtx returns the context used in `ranger` functions
	GetRangerCtx() *rangerctx.RangerContext
	// GetBuildPBCtx returns the context used in `ToPB` method.
	GetBuildPBCtx() *BuildPBContext
}

// EmptyPlanContextExtended is used to provide some empty implementations for PlanContext.
// It is used by some mock contexts that are only required to implement PlanContext
// but do not care about the actual implementation.
type EmptyPlanContextExtended struct{}

// AdviseTxnWarmup advises the txn to warm up.
func (EmptyPlanContextExtended) AdviseTxnWarmup() error { return nil }

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
