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

package export

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

// kvContext supplies the session state used by SHOW CREATE schema rendering.
// Storage, SQL execution, and other session services are unavailable; unsupported
// methods panic so new dependencies cannot silently change offline export behavior.
type kvContext struct {
	sessionVars *variable.SessionVars
	exprCtx     *sessionexpr.ExprContext
}

var _ sessionctx.Context = (*kvContext)(nil)

func newKVContext() *kvContext {
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
	ctx := &kvContext{sessionVars: vars}
	ctx.exprCtx = sessionexpr.NewExprContext(ctx)
	return ctx
}

func (c *kvContext) GetSessionVars() *variable.SessionVars { return c.sessionVars }
func (c *kvContext) GetExprCtx() exprctx.ExprContext       { return c.exprCtx }

func (*kvContext) EncodeStates(context.Context, *sessionstates.SessionStates) error {
	panic("unimplemented")
}

func (*kvContext) DecodeStates(context.Context, *sessionstates.SessionStates) error {
	panic("unimplemented")
}

func (*kvContext) RollbackTxn(context.Context) { panic("unimplemented") }

func (*kvContext) CommitTxn(context.Context) error { panic("unimplemented") }

func (*kvContext) GetSchemaValidator() validatorapi.Validator { panic("unimplemented") }

func (*kvContext) GetSQLServer() sqlsvrapi.Server { panic("unimplemented") }

func (*kvContext) GetTableCtx() tblctx.MutateContext { panic("unimplemented") }

func (*kvContext) GetPlanCtx() planctx.PlanContext { panic("unimplemented") }

func (*kvContext) GetDistSQLCtx() *distsqlctx.DistSQLContext { panic("unimplemented") }

func (*kvContext) RefreshTxnCtx(context.Context) error { panic("unimplemented") }

func (*kvContext) GetSessionPlanCache() sessionctx.SessionPlanCache { panic("unimplemented") }

func (*kvContext) StmtCommit(context.Context) { panic("unimplemented") }

func (*kvContext) StmtRollback(context.Context, bool) { panic("unimplemented") }

func (*kvContext) IsDDLOwner() bool { panic("unimplemented") }

func (*kvContext) PrepareTSFuture(context.Context, oracle.Future, string) error {
	panic("unimplemented")
}

func (*kvContext) GetPreparedTxnFuture() sessionctx.TxnFuture { panic("unimplemented") }

func (*kvContext) GetTxnWriteThroughputSLI() *sli.TxnWriteThroughputSLI { panic("unimplemented") }

func (*kvContext) GetBuiltinFunctionUsage() map[string]uint32 { panic("unimplemented") }

func (*kvContext) GetStmtStats() *stmtstats.StatementStats { panic("unimplemented") }

func (*kvContext) ShowProcess() *sessmgr.ProcessInfo { panic("unimplemented") }

func (*kvContext) GetAdvisoryLock(string, int64) error { panic("unimplemented") }

func (*kvContext) IsUsedAdvisoryLock(string) uint64 { panic("unimplemented") }

func (*kvContext) ReleaseAdvisoryLock(string) bool { panic("unimplemented") }

func (*kvContext) ReleaseAllAdvisoryLocks() int { panic("unimplemented") }

func (*kvContext) GetExtensions() *extension.SessionExtensions { panic("unimplemented") }

func (*kvContext) InSandBoxMode() bool { panic("unimplemented") }

func (*kvContext) EnableSandBoxMode() { panic("unimplemented") }

func (*kvContext) DisableSandBoxMode() { panic("unimplemented") }

func (*kvContext) ReportUsageStats() { panic("unimplemented") }

func (*kvContext) NewStmtIndexUsageCollector() *indexusage.StmtIndexUsageCollector {
	panic("unimplemented")
}

func (*kvContext) GetCursorTracker() cursor.Tracker { panic("unimplemented") }

func (*kvContext) GetCommitWaitGroup() *sync.WaitGroup { panic("unimplemented") }

func (*kvContext) GetTraceCtx() context.Context { panic("unimplemented") }

func (*kvContext) GetStore() kv.Storage { panic("unimplemented") }

func (*kvContext) GetInfoSchema() infoschema.MetaOnlyInfoSchema { panic("unimplemented") }

func (*kvContext) GetLatestInfoSchema() infoschema.MetaOnlyInfoSchema { panic("unimplemented") }

func (*kvContext) GetLatestISWithoutSessExt() infoschema.MetaOnlyInfoSchema { panic("unimplemented") }

func (*kvContext) GetClient() kv.Client { panic("unimplemented") }

func (*kvContext) GetMPPClient() kv.MPPClient { panic("unimplemented") }

func (*kvContext) GetSessionManager() sessmgr.Manager { panic("unimplemented") }

func (*kvContext) GetSQLExecutor() sqlexec.SQLExecutor { panic("unimplemented") }

func (*kvContext) GetRestrictedSQLExecutor() sqlexec.RestrictedSQLExecutor { panic("unimplemented") }

func (*kvContext) GetRangerCtx() *rangerctx.RangerContext { panic("unimplemented") }

func (*kvContext) GetBuildPBCtx() *planctx.BuildPBContext { panic("unimplemented") }

func (*kvContext) IsCrossKS() bool { panic("unimplemented") }

func (*kvContext) UpdateColStatsUsage(iter.Seq[model.TableItemID]) { panic("unimplemented") }

func (*kvContext) Txn(bool) (kv.Transaction, error) { panic("unimplemented") }

func (*kvContext) HasDirtyContent(int64) bool { panic("unimplemented") }

func (*kvContext) BuiltinFunctionUsageInc(string) { panic("unimplemented") }

func (*kvContext) CheckTableLocked(int64) (bool, ast.TableLockType) { panic("unimplemented") }

func (*kvContext) GetAllTableLocks() []model.TableLockTpInfo { panic("unimplemented") }

func (*kvContext) HasLockedTables() bool { panic("unimplemented") }

func (*kvContext) AddTableLock([]model.TableLockTpInfo) { panic("unimplemented") }

func (*kvContext) ReleaseTableLocks([]model.TableLockTpInfo) { panic("unimplemented") }

func (*kvContext) ReleaseTableLockByTableIDs([]int64) { panic("unimplemented") }

func (*kvContext) ReleaseAllTableLocks() { panic("unimplemented") }

func (*kvContext) SetValue(fmt.Stringer, any) { panic("unimplemented") }

func (*kvContext) Value(fmt.Stringer) any { panic("unimplemented") }

func (*kvContext) ClearValue(fmt.Stringer) { panic("unimplemented") }

func (*kvContext) GetDomain() any { panic("unimplemented") }
