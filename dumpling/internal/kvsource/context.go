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

package kvsource

import (
	"context"
	"fmt"
	"iter"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/domain/sqlsvrapi"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/expression/sessionexpr"
	"github.com/pingcap/tidb/pkg/extension"
	infoschema "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/infoschema/validatorapi"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/session/cursor"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage/indexusage"
	"github.com/pingcap/tidb/pkg/table/tblctx"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
	"github.com/pingcap/tidb/pkg/util/sli"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"github.com/tikv/client-go/v2/oracle"
)

// schemaContext supplies the session state used by SHOW CREATE schema rendering.
// Storage, SQL execution, and other session services are unavailable; unsupported
// methods panic so new dependencies cannot silently change offline export behavior.
type schemaContext struct {
	sessionVars *variable.SessionVars
	exprCtx     *sessionexpr.ExprContext
}

var _ sessionctx.Context = (*schemaContext)(nil)

func newSchemaContext() *schemaContext {
	vars := variable.NewSessionVars(nil)
	if err := vars.SetSystemVar(vardef.SQLModeVar, mysql.DefaultSQLMode); err != nil {
		panic(err)
	}
	vars.TimeZone = time.UTC
	vars.StmtCtx.SetTimeZone(time.UTC)
	// Match SHOW conversion semantics in executor.ResetContextOfStmt.
	vars.StmtCtx.SetTypeFlags(vars.StmtCtx.TypeFlags().
		WithIgnoreTruncateErr(true).
		WithIgnoreZeroInDate(true).
		WithIgnoreInvalidDateErr(vars.SQLMode.HasAllowInvalidDatesMode()).
		WithSkipUTF8Check(vars.SkipUTF8Check).
		WithSkipSACIICheck(vars.SkipASCIICheck).
		WithSkipUTF8MB4Check(!config.GetGlobalConfig().Instance.CheckMb4ValueInUTF8.Load()))
	ctx := &schemaContext{sessionVars: vars}
	ctx.exprCtx = sessionexpr.NewExprContext(ctx)
	return ctx
}

func (c *schemaContext) GetSessionVars() *variable.SessionVars { return c.sessionVars }
func (c *schemaContext) GetExprCtx() exprctx.ExprContext       { return c.exprCtx }

func (*schemaContext) CheckCanMigrateStates() error { panic("unimplemented") }

func (*schemaContext) EncodeStates(context.Context, *sessionstates.SessionStates) error {
	panic("unimplemented")
}

func (*schemaContext) DecodeStates(context.Context, *sessionstates.SessionStates) error {
	panic("unimplemented")
}

func (*schemaContext) RollbackTxn(context.Context) { panic("unimplemented") }

func (*schemaContext) CommitTxn(context.Context) error { panic("unimplemented") }

func (*schemaContext) GetSchemaValidator() validatorapi.Validator { panic("unimplemented") }

func (*schemaContext) GetSQLServer() sqlsvrapi.Server { panic("unimplemented") }

func (*schemaContext) GetTableCtx() tblctx.MutateContext { panic("unimplemented") }

func (*schemaContext) GetPlanCtx() planctx.PlanContext { panic("unimplemented") }

func (*schemaContext) GetDistSQLCtx() *distsqlctx.DistSQLContext { panic("unimplemented") }

func (*schemaContext) RefreshTxnCtx(context.Context) error { panic("unimplemented") }

func (*schemaContext) GetSessionPlanCache() sessionctx.SessionPlanCache { panic("unimplemented") }

func (*schemaContext) StmtCommit(context.Context) { panic("unimplemented") }

func (*schemaContext) StmtRollback(context.Context, bool) { panic("unimplemented") }

func (*schemaContext) IsDDLOwner() bool { panic("unimplemented") }

func (*schemaContext) PrepareTSFuture(context.Context, oracle.Future, string) error {
	panic("unimplemented")
}

func (*schemaContext) GetPreparedTxnFuture() sessionctx.TxnFuture { panic("unimplemented") }

func (*schemaContext) GetTxnWriteThroughputSLI() *sli.TxnWriteThroughputSLI { panic("unimplemented") }

func (*schemaContext) GetBuiltinFunctionUsage() map[string]uint32 { panic("unimplemented") }

func (*schemaContext) GetStmtStats() *stmtstats.StatementStats { panic("unimplemented") }

func (*schemaContext) ShowProcess() *sessmgr.ProcessInfo { panic("unimplemented") }

func (*schemaContext) GetAdvisoryLock(string, int64) error { panic("unimplemented") }

func (*schemaContext) IsUsedAdvisoryLock(string) uint64 { panic("unimplemented") }

func (*schemaContext) ReleaseAdvisoryLock(string) bool { panic("unimplemented") }

func (*schemaContext) ReleaseAllAdvisoryLocks() int { panic("unimplemented") }

func (*schemaContext) GetExtensions() *extension.SessionExtensions { panic("unimplemented") }

func (*schemaContext) InSandBoxMode() bool { panic("unimplemented") }

func (*schemaContext) EnableSandBoxMode() { panic("unimplemented") }

func (*schemaContext) DisableSandBoxMode() { panic("unimplemented") }

func (*schemaContext) ReportUsageStats() { panic("unimplemented") }

func (*schemaContext) NewStmtIndexUsageCollector() *indexusage.StmtIndexUsageCollector {
	panic("unimplemented")
}

func (*schemaContext) GetCursorTracker() cursor.Tracker { panic("unimplemented") }

func (*schemaContext) GetCommitWaitGroup() *sync.WaitGroup { panic("unimplemented") }

func (*schemaContext) GetTraceCtx() context.Context { panic("unimplemented") }

func (*schemaContext) GetStore() kv.Storage { panic("unimplemented") }

func (*schemaContext) GetInfoSchema() infoschema.MetaOnlyInfoSchema { panic("unimplemented") }

func (*schemaContext) GetLatestInfoSchema() infoschema.MetaOnlyInfoSchema { panic("unimplemented") }

func (*schemaContext) GetLatestISWithoutSessExt() infoschema.MetaOnlyInfoSchema {
	panic("unimplemented")
}

func (*schemaContext) GetClient() kv.Client { panic("unimplemented") }

func (*schemaContext) GetMPPClient() kv.MPPClient { panic("unimplemented") }

func (*schemaContext) GetSessionManager() sessmgr.Manager { panic("unimplemented") }

func (*schemaContext) GetSQLExecutor() sqlexec.SQLExecutor { panic("unimplemented") }

func (*schemaContext) GetRestrictedSQLExecutor() sqlexec.RestrictedSQLExecutor {
	panic("unimplemented")
}

func (*schemaContext) GetRangerCtx() *rangerctx.RangerContext { panic("unimplemented") }

func (*schemaContext) GetBuildPBCtx() *planctx.BuildPBContext { panic("unimplemented") }

func (*schemaContext) IsCrossKS() bool { panic("unimplemented") }

func (*schemaContext) UpdateColStatsUsage(iter.Seq[model.TableItemID]) { panic("unimplemented") }

func (*schemaContext) Txn(bool) (kv.Transaction, error) { panic("unimplemented") }

func (*schemaContext) HasDirtyContent(int64) bool { panic("unimplemented") }

func (*schemaContext) BuiltinFunctionUsageInc(string) { panic("unimplemented") }

func (*schemaContext) CheckTableLocked(int64) (bool, ast.TableLockType) { panic("unimplemented") }

func (*schemaContext) GetAllTableLocks() []model.TableLockTpInfo { panic("unimplemented") }

func (*schemaContext) HasLockedTables() bool { panic("unimplemented") }

func (*schemaContext) AddTableLock([]model.TableLockTpInfo) { panic("unimplemented") }

func (*schemaContext) ReleaseTableLocks([]model.TableLockTpInfo) { panic("unimplemented") }

func (*schemaContext) ReleaseTableLockByTableIDs([]int64) { panic("unimplemented") }

func (*schemaContext) ReleaseAllTableLocks() { panic("unimplemented") }

func (*schemaContext) SetValue(fmt.Stringer, any) { panic("unimplemented") }

func (*schemaContext) Value(fmt.Stringer) any { panic("unimplemented") }

func (*schemaContext) ClearValue(fmt.Stringer) { panic("unimplemented") }

func (*schemaContext) GetDomain() any { panic("unimplemented") }
