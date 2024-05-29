// Copyright 2015 PingCAP, Inc.
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

// Package mock is just for test only.
package mock

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	exprctximpl "github.com/pingcap/tidb/pkg/expression/contextsession"
	"github.com/pingcap/tidb/pkg/extension"
	infoschema "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	planctx "github.com/pingcap/tidb/pkg/planner/context"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage/indexusage"
	tbctx "github.com/pingcap/tidb/pkg/table/context"
	tbctximpl "github.com/pingcap/tidb/pkg/table/contextimpl"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/memory"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
	"github.com/pingcap/tidb/pkg/util/sli"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"github.com/pingcap/tipb/go-binlog"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
)

var (
	_ sessionctx.Context  = (*Context)(nil)
	_ planctx.PlanContext = (*Context)(nil)
	_ sqlexec.SQLExecutor = (*Context)(nil)
)

// Context represents mocked sessionctx.Context.
type Context struct {
	planctx.EmptyPlanContextExtended
	*exprctximpl.SessionExprContext
	txn           wrapTxn    // mock global variable
	Store         kv.Storage // mock global variable
	ctx           context.Context
	sm            util.SessionManager
	is            infoschema.MetaOnlyInfoSchema
	values        map[fmt.Stringer]any
	sessionVars   *variable.SessionVars
	tblctx        *tbctximpl.TableContextImpl
	cancel        context.CancelFunc
	pcache        sessionctx.PlanCache
	level         kvrpcpb.DiskFullOpt
	inSandBoxMode bool
	isDDLOwner    bool
}

type wrapTxn struct {
	kv.Transaction
	tsFuture oracle.Future
}

func (txn *wrapTxn) validOrPending() bool {
	return txn.tsFuture != nil || txn.Transaction.Valid()
}

func (txn *wrapTxn) pending() bool {
	return txn.Transaction == nil && txn.tsFuture != nil
}

// Wait creates a new kvTransaction
func (txn *wrapTxn) Wait(_ context.Context, sctx sessionctx.Context) (kv.Transaction, error) {
	if !txn.validOrPending() {
		return txn, errors.AddStack(kv.ErrInvalidTxn)
	}
	if txn.pending() {
		ts, err := txn.tsFuture.Wait()
		if err != nil {
			return nil, err
		}
		kvTxn, err := sctx.GetStore().Begin(tikv.WithStartTS(ts))
		if err != nil {
			return nil, errors.Trace(err)
		}
		txn.Transaction = kvTxn
	}
	return txn, nil
}

func (txn *wrapTxn) Valid() bool {
	return txn.Transaction != nil && txn.Transaction.Valid()
}

func (txn *wrapTxn) CacheTableInfo(id int64, info *model.TableInfo) {
	if txn.Transaction == nil {
		return
	}
	txn.Transaction.CacheTableInfo(id, info)
}

func (txn *wrapTxn) GetTableInfo(id int64) *model.TableInfo {
	if txn.Transaction == nil {
		return nil
	}
	return txn.Transaction.GetTableInfo(id)
}

// SetDiskFullOpt implements the interface.
func (*wrapTxn) SetDiskFullOpt(_ kvrpcpb.DiskFullOpt) {}

// SetOption implements the interface.
func (*wrapTxn) SetOption(_ int, _ any) {}

// StartTS implements the interface.
func (*wrapTxn) StartTS() uint64 { return uint64(time.Now().UnixNano()) }

// Get implements the interface.
func (txn *wrapTxn) Get(ctx context.Context, k kv.Key) ([]byte, error) {
	if txn.Transaction == nil {
		return nil, nil
	}
	return txn.Transaction.Get(ctx, k)
}

// Execute implements sqlexec.SQLExecutor Execute interface.
func (*Context) Execute(_ context.Context, _ string) ([]sqlexec.RecordSet, error) {
	return nil, errors.Errorf("Not Supported")
}

// ExecuteStmt implements sqlexec.SQLExecutor ExecuteStmt interface.
func (*Context) ExecuteStmt(_ context.Context, _ ast.StmtNode) (sqlexec.RecordSet, error) {
	return nil, errors.Errorf("Not Supported")
}

// ParseWithParams implements sqlexec.RestrictedSQLExecutor ParseWithParams interface.
func (*Context) ParseWithParams(_ context.Context, _ string, _ ...any) (ast.StmtNode, error) {
	return nil, errors.Errorf("Not Supported")
}

// ExecRestrictedStmt implements sqlexec.RestrictedSQLExecutor ExecRestrictedStmt interface.
func (*Context) ExecRestrictedStmt(_ context.Context, _ ast.StmtNode, _ ...sqlexec.OptionFuncAlias) ([]chunk.Row, []*ast.ResultField, error) {
	return nil, nil, errors.Errorf("Not Supported")
}

// ExecRestrictedSQL implements sqlexec.RestrictedSQLExecutor ExecRestrictedSQL interface.
func (*Context) ExecRestrictedSQL(_ context.Context, _ []sqlexec.OptionFuncAlias, _ string, _ ...any) ([]chunk.Row, []*ast.ResultField, error) {
	return nil, nil, errors.Errorf("Not Supported")
}

// GetSQLExecutor returns the SQLExecutor.
func (c *Context) GetSQLExecutor() sqlexec.SQLExecutor {
	return c
}

// GetRestrictedSQLExecutor returns the RestrictedSQLExecutor.
func (c *Context) GetRestrictedSQLExecutor() sqlexec.RestrictedSQLExecutor {
	return c
}

// ExecuteInternal implements sqlexec.SQLExecutor ExecuteInternal interface.
func (*Context) ExecuteInternal(_ context.Context, _ string, _ ...any) (sqlexec.RecordSet, error) {
	return nil, errors.Errorf("Not Supported")
}

// ShowProcess implements sessionctx.Context ShowProcess interface.
func (*Context) ShowProcess() *util.ProcessInfo {
	return &util.ProcessInfo{}
}

// SetIsDDLOwner sets return value of IsDDLOwner.
func (c *Context) SetIsDDLOwner(isOwner bool) {
	c.isDDLOwner = isOwner
}

// IsDDLOwner checks whether this session is DDL owner.
func (c *Context) IsDDLOwner() bool {
	return c.isDDLOwner
}

// SetValue implements sessionctx.Context SetValue interface.
func (c *Context) SetValue(key fmt.Stringer, value any) {
	c.values[key] = value
}

// Value implements sessionctx.Context Value interface.
func (c *Context) Value(key fmt.Stringer) any {
	value := c.values[key]
	return value
}

// ClearValue implements sessionctx.Context ClearValue interface.
func (c *Context) ClearValue(key fmt.Stringer) {
	delete(c.values, key)
}

// HasDirtyContent implements sessionctx.Context ClearValue interface.
func (*Context) HasDirtyContent(_ int64) bool {
	return false
}

// GetSessionVars implements the sessionctx.Context GetSessionVars interface.
func (c *Context) GetSessionVars() *variable.SessionVars {
	return c.sessionVars
}

// GetPlanCtx returns the PlanContext.
func (c *Context) GetPlanCtx() planctx.PlanContext {
	return c
}

// GetNullRejectCheckExprCtx gets the expression context with null rejected check.
func (c *Context) GetNullRejectCheckExprCtx() exprctx.ExprContext {
	return exprctx.WithNullRejectCheck(c)
}

// GetExprCtx returns the expression context of the session.
func (c *Context) GetExprCtx() exprctx.ExprContext {
	return c
}

// GetTableCtx returns the table.MutateContext
func (c *Context) GetTableCtx() tbctx.MutateContext {
	return c.tblctx
}

// GetDistSQLCtx returns the distsql context of the session
func (c *Context) GetDistSQLCtx() *distsqlctx.DistSQLContext {
	vars := c.GetSessionVars()
	sc := vars.StmtCtx

	return &distsqlctx.DistSQLContext{
		WarnHandler:                          sc.WarnHandler,
		InRestrictedSQL:                      sc.InRestrictedSQL,
		Client:                               c.GetClient(),
		EnabledRateLimitAction:               vars.EnabledRateLimitAction,
		EnableChunkRPC:                       vars.EnableChunkRPC,
		OriginalSQL:                          sc.OriginalSQL,
		KVVars:                               vars.KVVars,
		KvExecCounter:                        sc.KvExecCounter,
		SessionMemTracker:                    vars.MemTracker,
		Location:                             sc.TimeZone(),
		RuntimeStatsColl:                     sc.RuntimeStatsColl,
		SQLKiller:                            &vars.SQLKiller,
		ErrCtx:                               sc.ErrCtx(),
		TiFlashReplicaRead:                   vars.TiFlashReplicaRead,
		TiFlashMaxThreads:                    vars.TiFlashMaxThreads,
		TiFlashMaxBytesBeforeExternalJoin:    vars.TiFlashMaxBytesBeforeExternalJoin,
		TiFlashMaxBytesBeforeExternalGroupBy: vars.TiFlashMaxBytesBeforeExternalGroupBy,
		TiFlashMaxBytesBeforeExternalSort:    vars.TiFlashMaxBytesBeforeExternalSort,
		TiFlashMaxQueryMemoryPerNode:         vars.TiFlashMaxQueryMemoryPerNode,
		TiFlashQuerySpillRatio:               vars.TiFlashQuerySpillRatio,
		ExecDetails:                          &sc.SyncExecDetails,
	}
}

// GetRangerCtx returns the context used in `ranger` related functions
func (c *Context) GetRangerCtx() *rangerctx.RangerContext {
	return &rangerctx.RangerContext{
		ExprCtx: c.GetExprCtx(),
		TypeCtx: c.GetSessionVars().StmtCtx.TypeCtx(),
		ErrCtx:  c.GetSessionVars().StmtCtx.ErrCtx(),

		InPreparedPlanBuilding:   c.GetSessionVars().StmtCtx.InPreparedPlanBuilding,
		RegardNULLAsPoint:        c.GetSessionVars().RegardNULLAsPoint,
		OptPrefixIndexSingleScan: c.GetSessionVars().OptPrefixIndexSingleScan,
		OptimizerFixControl:      c.GetSessionVars().OptimizerFixControl,

		PlanCacheTracker:     &c.GetSessionVars().StmtCtx.PlanCacheTracker,
		RangeFallbackHandler: &c.GetSessionVars().StmtCtx.RangeFallbackHandler,
	}
}

// GetBuildPBCtx returns the `ToPB` context of the session
func (c *Context) GetBuildPBCtx() *planctx.BuildPBContext {
	return &planctx.BuildPBContext{
		ExprCtx: c.GetExprCtx(),
		Client:  c.GetClient(),

		TiFlashFastScan:                    c.GetSessionVars().TiFlashFastScan,
		TiFlashFineGrainedShuffleBatchSize: c.GetSessionVars().TiFlashFineGrainedShuffleBatchSize,

		// the following fields are used to build `expression.PushDownContext`.
		// TODO: it'd be better to embed `expression.PushDownContext` in `BuildPBContext`. But `expression` already
		// depends on this package, so we need to move `expression.PushDownContext` to a standalone package first.
		GroupConcatMaxLen: c.GetSessionVars().GroupConcatMaxLen,
		InExplainStmt:     c.GetSessionVars().StmtCtx.InExplainStmt,
		WarnHandler:       c.GetSessionVars().StmtCtx.WarnHandler,
		ExtraWarnghandler: c.GetSessionVars().StmtCtx.ExtraWarnHandler,
	}
}

// Txn implements sessionctx.Context Txn interface.
func (c *Context) Txn(bool) (kv.Transaction, error) {
	return &c.txn, nil
}

// GetClient implements sessionctx.Context GetClient interface.
func (c *Context) GetClient() kv.Client {
	if c.Store == nil {
		return nil
	}
	return c.Store.GetClient()
}

// GetMPPClient implements sessionctx.Context GetMPPClient interface.
func (c *Context) GetMPPClient() kv.MPPClient {
	if c.Store == nil {
		return nil
	}
	return c.Store.GetMPPClient()
}

// GetInfoSchema implements sessionctx.Context GetInfoSchema interface.
func (c *Context) GetInfoSchema() infoschema.MetaOnlyInfoSchema {
	vars := c.GetSessionVars()
	if snap, ok := vars.SnapshotInfoschema.(infoschema.MetaOnlyInfoSchema); ok {
		return snap
	}
	if vars.TxnCtx != nil && vars.InTxn() {
		if is, ok := vars.TxnCtx.InfoSchema.(infoschema.MetaOnlyInfoSchema); ok {
			return is
		}
	}
	if c.is == nil {
		c.is = MockInfoschema(nil)
	}
	return c.is
}

// MockInfoschema only serves for test.
var MockInfoschema func(tbList []*model.TableInfo) infoschema.MetaOnlyInfoSchema

// GetDomainInfoSchema returns the latest information schema in domain
func (c *Context) GetDomainInfoSchema() infoschema.MetaOnlyInfoSchema {
	if c.is == nil {
		c.is = MockInfoschema(nil)
	}
	return c.is
}

// GetBuiltinFunctionUsage implements sessionctx.Context GetBuiltinFunctionUsage interface.
func (*Context) GetBuiltinFunctionUsage() map[string]uint32 {
	return make(map[string]uint32)
}

// GetGlobalSysVar implements GlobalVarAccessor GetGlobalSysVar interface.
func (*Context) GetGlobalSysVar(_ sessionctx.Context, name string) (string, error) {
	v := variable.GetSysVar(name)
	if v == nil {
		return "", variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	return v.Value, nil
}

// SetGlobalSysVar implements GlobalVarAccessor SetGlobalSysVar interface.
func (*Context) SetGlobalSysVar(_ sessionctx.Context, name string, value string) error {
	v := variable.GetSysVar(name)
	if v == nil {
		return variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	v.Value = value
	return nil
}

// GetSessionPlanCache implements the sessionctx.Context interface.
func (c *Context) GetSessionPlanCache() sessionctx.PlanCache {
	return c.pcache
}

// NewTxn implements the sessionctx.Context interface.
func (c *Context) NewTxn(context.Context) error {
	if c.Store == nil {
		return errors.New("store is not set")
	}
	if c.txn.Valid() {
		err := c.txn.Commit(c.ctx)
		if err != nil {
			return errors.Trace(err)
		}
	}

	txn, err := c.Store.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	c.txn.Transaction = txn
	return nil
}

// NewStaleTxnWithStartTS implements the sessionctx.Context interface.
func (c *Context) NewStaleTxnWithStartTS(ctx context.Context, _ uint64) error {
	return c.NewTxn(ctx)
}

// RefreshTxnCtx implements the sessionctx.Context interface.
func (c *Context) RefreshTxnCtx(ctx context.Context) error {
	return errors.Trace(c.NewTxn(ctx))
}

// RollbackTxn indicates an expected call of RollbackTxn.
func (c *Context) RollbackTxn(_ context.Context) {
	defer c.sessionVars.SetInTxn(false)
	if c.txn.Valid() {
		terror.Log(c.txn.Rollback())
	}
}

// CommitTxn indicates an expected call of CommitTxn.
func (c *Context) CommitTxn(ctx context.Context) error {
	defer c.sessionVars.SetInTxn(false)
	c.txn.SetDiskFullOpt(c.level)
	if c.txn.Valid() {
		return c.txn.Commit(ctx)
	}
	return nil
}

// GetStore gets the store of session.
func (c *Context) GetStore() kv.Storage {
	return c.Store
}

// GetSessionManager implements the sessionctx.Context interface.
func (c *Context) GetSessionManager() util.SessionManager {
	return c.sm
}

// SetSessionManager set the session manager.
func (c *Context) SetSessionManager(sm util.SessionManager) {
	c.sm = sm
}

// Cancel implements the Session interface.
func (c *Context) Cancel() {
	c.cancel()
}

// GoCtx returns standard sessionctx.Context that bind with current transaction.
func (c *Context) GoCtx() context.Context {
	return c.ctx
}

// UpdateColStatsUsage updates the column stats usage.
func (*Context) UpdateColStatsUsage(_ []model.TableItemID) {}

// StoreIndexUsage strores the index usage information.
func (*Context) StoreIndexUsage(_ int64, _ int64, _ int64) {}

// GetTxnWriteThroughputSLI implements the sessionctx.Context interface.
func (*Context) GetTxnWriteThroughputSLI() *sli.TxnWriteThroughputSLI {
	return &sli.TxnWriteThroughputSLI{}
}

// StmtCommit implements the sessionctx.Context interface.
func (*Context) StmtCommit(context.Context) {}

// StmtRollback implements the sessionctx.Context interface.
func (*Context) StmtRollback(context.Context, bool) {}

// StmtGetMutation implements the sessionctx.Context interface.
func (*Context) StmtGetMutation(_ int64) *binlog.TableMutation {
	return nil
}

// AddTableLock implements the sessionctx.Context interface.
func (*Context) AddTableLock(_ []model.TableLockTpInfo) {
}

// ReleaseTableLocks implements the sessionctx.Context interface.
func (*Context) ReleaseTableLocks(_ []model.TableLockTpInfo) {
}

// ReleaseTableLockByTableIDs implements the sessionctx.Context interface.
func (*Context) ReleaseTableLockByTableIDs(_ []int64) {
}

// CheckTableLocked implements the sessionctx.Context interface.
func (*Context) CheckTableLocked(_ int64) (bool, model.TableLockType) {
	return false, model.TableLockNone
}

// GetAllTableLocks implements the sessionctx.Context interface.
func (*Context) GetAllTableLocks() []model.TableLockTpInfo {
	return nil
}

// ReleaseAllTableLocks implements the sessionctx.Context interface.
func (*Context) ReleaseAllTableLocks() {
}

// HasLockedTables implements the sessionctx.Context interface.
func (*Context) HasLockedTables() bool {
	return false
}

// PrepareTSFuture implements the sessionctx.Context interface.
func (c *Context) PrepareTSFuture(_ context.Context, future oracle.Future, _ string) error {
	c.txn.Transaction = nil
	c.txn.tsFuture = future
	return nil
}

// GetPreparedTxnFuture returns the TxnFuture if it is prepared.
// It returns nil otherwise.
func (c *Context) GetPreparedTxnFuture() sessionctx.TxnFuture {
	if !c.txn.validOrPending() {
		return nil
	}
	return &c.txn
}

// GetStmtStats implements the sessionctx.Context interface.
func (*Context) GetStmtStats() *stmtstats.StatementStats {
	return nil
}

// GetAdvisoryLock acquires an advisory lock
func (*Context) GetAdvisoryLock(_ string, _ int64) error {
	return nil
}

// IsUsedAdvisoryLock check if a lock name is in use
func (*Context) IsUsedAdvisoryLock(_ string) uint64 {
	return 0
}

// ReleaseAdvisoryLock releases an advisory lock
func (*Context) ReleaseAdvisoryLock(_ string) bool {
	return true
}

// ReleaseAllAdvisoryLocks releases all advisory locks
func (*Context) ReleaseAllAdvisoryLocks() int {
	return 0
}

// EncodeSessionStates implements sessionctx.Context EncodeSessionStates interface.
func (*Context) EncodeSessionStates(context.Context, sessionctx.Context, *sessionstates.SessionStates) error {
	return errors.Errorf("Not Supported")
}

// DecodeSessionStates implements sessionctx.Context DecodeSessionStates interface.
func (*Context) DecodeSessionStates(context.Context, sessionctx.Context, *sessionstates.SessionStates) error {
	return errors.Errorf("Not Supported")
}

// GetExtensions returns the `*extension.SessionExtensions` object
func (*Context) GetExtensions() *extension.SessionExtensions {
	return nil
}

// EnableSandBoxMode enable the sandbox mode.
func (c *Context) EnableSandBoxMode() {
	c.inSandBoxMode = true
}

// DisableSandBoxMode enable the sandbox mode.
func (c *Context) DisableSandBoxMode() {
	c.inSandBoxMode = false
}

// InSandBoxMode indicates that this Session is in sandbox mode
func (c *Context) InSandBoxMode() bool {
	return c.inSandBoxMode
}

// SetInfoSchema is to set info shema for the test.
func (c *Context) SetInfoSchema(is infoschema.MetaOnlyInfoSchema) {
	c.is = is
}

// ResetSessionAndStmtTimeZone resets the timezone for session and statement.
func (c *Context) ResetSessionAndStmtTimeZone(tz *time.Location) {
	c.GetSessionVars().TimeZone = tz
	c.GetSessionVars().StmtCtx.SetTimeZone(tz)
}

// ReportUsageStats implements the sessionctx.Context interface.
func (*Context) ReportUsageStats() {}

// Close implements the sessionctx.Context interface.
func (*Context) Close() {}

// NewStmtIndexUsageCollector implements the sessionctx.Context interface
func (*Context) NewStmtIndexUsageCollector() *indexusage.StmtIndexUsageCollector {
	return nil
}

// NewContext creates a new mocked sessionctx.Context.
func NewContext() *Context {
	ctx, cancel := context.WithCancel(context.Background())
	sctx := &Context{
		values: make(map[fmt.Stringer]any),
		ctx:    ctx,
		cancel: cancel,
	}
	vars := variable.NewSessionVars(sctx)
	sctx.sessionVars = vars
	sctx.SessionExprContext = exprctximpl.NewSessionExprContext(sctx)
	sctx.tblctx = tbctximpl.NewTableContextImpl(sctx, sctx)
	vars.InitChunkSize = 2
	vars.MaxChunkSize = 32
	vars.TimeZone = time.UTC
	vars.StmtCtx.SetTimeZone(time.UTC)
	vars.MemTracker.SetBytesLimit(-1)
	vars.DiskTracker.SetBytesLimit(-1)
	vars.StmtCtx.MemTracker, vars.StmtCtx.DiskTracker = memory.NewTracker(-1, -1), disk.NewTracker(-1, -1)
	vars.MemTracker.AttachTo(vars.MemTracker)
	vars.DiskTracker.AttachTo(vars.DiskTracker)
	vars.GlobalVarsAccessor = variable.NewMockGlobalAccessor()
	vars.EnablePaging = variable.DefTiDBEnablePaging
	vars.MinPagingSize = variable.DefMinPagingSize
	vars.CostModelVersion = variable.DefTiDBCostModelVer
	vars.EnableChunkRPC = true
	vars.DivPrecisionIncrement = variable.DefDivPrecisionIncrement
	if err := sctx.GetSessionVars().SetSystemVar(variable.MaxAllowedPacket, "67108864"); err != nil {
		panic(err)
	}
	if err := sctx.GetSessionVars().SetSystemVar(variable.CharacterSetConnection, "utf8mb4"); err != nil {
		panic(err)
	}
	return sctx
}

// HookKeyForTest is as alias, used by context.WithValue.
// golint forbits using string type as key in context.WithValue.
type HookKeyForTest string
