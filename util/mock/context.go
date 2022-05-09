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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/sli"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/topsql/stmtstats"
	"github.com/pingcap/tipb/go-binlog"
	"github.com/tikv/client-go/v2/tikv"
)

var (
	_ sessionctx.Context  = (*Context)(nil)
	_ sqlexec.SQLExecutor = (*Context)(nil)
)

// Context represents mocked sessionctx.Context.
type Context struct {
	values      map[fmt.Stringer]interface{}
	txn         wrapTxn    // mock global variable
	Store       kv.Storage // mock global variable
	sessionVars *variable.SessionVars
	ctx         context.Context
	cancel      context.CancelFunc
	sm          util.SessionManager
	pcache      *kvcache.SimpleLRUCache
}

type wrapTxn struct {
	kv.Transaction
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

// Execute implements sqlexec.SQLExecutor Execute interface.
func (c *Context) Execute(ctx context.Context, sql string) ([]sqlexec.RecordSet, error) {
	return nil, errors.Errorf("Not Supported")
}

// ExecuteStmt implements sqlexec.SQLExecutor ExecuteStmt interface.
func (c *Context) ExecuteStmt(ctx context.Context, stmtNode ast.StmtNode) (sqlexec.RecordSet, error) {
	return nil, errors.Errorf("Not Supported")
}

// SetDiskFullOpt sets allowed options of current operation in each TiKV disk usage level.
func (c *Context) SetDiskFullOpt(level kvrpcpb.DiskFullOpt) {
	c.txn.Transaction.SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
}

// ClearDiskFullOpt clears allowed options of current operation in each TiKV disk usage level.
func (c *Context) ClearDiskFullOpt() {
	c.txn.Transaction.ClearDiskFullOpt()
}

// ExecuteInternal implements sqlexec.SQLExecutor ExecuteInternal interface.
func (c *Context) ExecuteInternal(ctx context.Context, sql string, args ...interface{}) (sqlexec.RecordSet, error) {
	return nil, errors.Errorf("Not Supported")
}

// ShowProcess implements sessionctx.Context ShowProcess interface.
func (c *Context) ShowProcess() *util.ProcessInfo {
	return &util.ProcessInfo{}
}

// IsDDLOwner checks whether this session is DDL owner.
func (c *Context) IsDDLOwner() bool {
	return true
}

// SetValue implements sessionctx.Context SetValue interface.
func (c *Context) SetValue(key fmt.Stringer, value interface{}) {
	c.values[key] = value
}

// Value implements sessionctx.Context Value interface.
func (c *Context) Value(key fmt.Stringer) interface{} {
	value := c.values[key]
	return value
}

// ClearValue implements sessionctx.Context ClearValue interface.
func (c *Context) ClearValue(key fmt.Stringer) {
	delete(c.values, key)
}

// HasDirtyContent implements sessionctx.Context ClearValue interface.
func (c *Context) HasDirtyContent(tid int64) bool {
	return false
}

// GetSessionVars implements the sessionctx.Context GetSessionVars interface.
func (c *Context) GetSessionVars() *variable.SessionVars {
	return c.sessionVars
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
func (c *Context) GetInfoSchema() sessionctx.InfoschemaMetaVersion {
	vars := c.GetSessionVars()
	if snap, ok := vars.SnapshotInfoschema.(sessionctx.InfoschemaMetaVersion); ok {
		return snap
	}
	if vars.TxnCtx != nil && vars.InTxn() {
		if is, ok := vars.TxnCtx.InfoSchema.(sessionctx.InfoschemaMetaVersion); ok {
			return is
		}
	}
	return nil
}

// GetBuiltinFunctionUsage implements sessionctx.Context GetBuiltinFunctionUsage interface.
func (c *Context) GetBuiltinFunctionUsage() map[string]uint32 {
	return make(map[string]uint32)
}

// BuiltinFunctionUsageInc implements sessionctx.Context.
func (c *Context) BuiltinFunctionUsageInc(scalarFuncSigName string) {

}

// GetGlobalSysVar implements GlobalVarAccessor GetGlobalSysVar interface.
func (c *Context) GetGlobalSysVar(ctx sessionctx.Context, name string) (string, error) {
	v := variable.GetSysVar(name)
	if v == nil {
		return "", variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	return v.Value, nil
}

// SetGlobalSysVar implements GlobalVarAccessor SetGlobalSysVar interface.
func (c *Context) SetGlobalSysVar(ctx sessionctx.Context, name string, value string) error {
	v := variable.GetSysVar(name)
	if v == nil {
		return variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	v.Value = value
	return nil
}

// PreparedPlanCache implements the sessionctx.Context interface.
func (c *Context) PreparedPlanCache() *kvcache.SimpleLRUCache {
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
func (c *Context) NewStaleTxnWithStartTS(ctx context.Context, startTS uint64) error {
	return c.NewTxn(ctx)
}

// GetSnapshotWithTS return a snapshot with ts
func (c *Context) GetSnapshotWithTS(ts uint64) kv.Snapshot {
	return c.Store.GetSnapshot(kv.Version{Ver: ts})
}

// RefreshTxnCtx implements the sessionctx.Context interface.
func (c *Context) RefreshTxnCtx(ctx context.Context) error {
	return errors.Trace(c.NewTxn(ctx))
}

// RefreshVars implements the sessionctx.Context interface.
func (c *Context) RefreshVars(ctx context.Context) error {
	return nil
}

// InitTxnWithStartTS implements the sessionctx.Context interface with startTS.
func (c *Context) InitTxnWithStartTS(startTS uint64) error {
	if c.txn.Valid() {
		return nil
	}
	if c.Store != nil {
		txn, err := c.Store.Begin(tikv.WithTxnScope(kv.GlobalTxnScope), tikv.WithStartTS(startTS))
		if err != nil {
			return errors.Trace(err)
		}
		c.txn.Transaction = txn
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

// StoreQueryFeedback stores the query feedback.
func (c *Context) StoreQueryFeedback(_ interface{}) {}

// UpdateColStatsUsage updates the column stats usage.
func (c *Context) UpdateColStatsUsage(_ []model.TableColumnID) {}

// StoreIndexUsage strores the index usage information.
func (c *Context) StoreIndexUsage(_ int64, _ int64, _ int64) {}

// GetTxnWriteThroughputSLI implements the sessionctx.Context interface.
func (c *Context) GetTxnWriteThroughputSLI() *sli.TxnWriteThroughputSLI {
	return &sli.TxnWriteThroughputSLI{}
}

// StmtCommit implements the sessionctx.Context interface.
func (c *Context) StmtCommit() {}

// StmtRollback implements the sessionctx.Context interface.
func (c *Context) StmtRollback() {
}

// StmtGetMutation implements the sessionctx.Context interface.
func (c *Context) StmtGetMutation(tableID int64) *binlog.TableMutation {
	return nil
}

// AddTableLock implements the sessionctx.Context interface.
func (c *Context) AddTableLock(_ []model.TableLockTpInfo) {
}

// ReleaseTableLocks implements the sessionctx.Context interface.
func (c *Context) ReleaseTableLocks(locks []model.TableLockTpInfo) {
}

// ReleaseTableLockByTableIDs implements the sessionctx.Context interface.
func (c *Context) ReleaseTableLockByTableIDs(tableIDs []int64) {
}

// CheckTableLocked implements the sessionctx.Context interface.
func (c *Context) CheckTableLocked(_ int64) (bool, model.TableLockType) {
	return false, model.TableLockNone
}

// GetAllTableLocks implements the sessionctx.Context interface.
func (c *Context) GetAllTableLocks() []model.TableLockTpInfo {
	return nil
}

// ReleaseAllTableLocks implements the sessionctx.Context interface.
func (c *Context) ReleaseAllTableLocks() {
}

// HasLockedTables implements the sessionctx.Context interface.
func (c *Context) HasLockedTables() bool {
	return false
}

// PrepareTSFuture implements the sessionctx.Context interface.
func (c *Context) PrepareTSFuture(ctx context.Context) {
}

// GetStmtStats implements the sessionctx.Context interface.
func (c *Context) GetStmtStats() *stmtstats.StatementStats {
	return nil
}

// GetAdvisoryLock acquires an advisory lock
func (c *Context) GetAdvisoryLock(lockName string, timeout int64) error {
	return nil
}

// ReleaseAdvisoryLock releases an advisory lock
func (c *Context) ReleaseAdvisoryLock(lockName string) bool {
	return true
}

// ReleaseAllAdvisoryLocks releases all advisory locks
func (c *Context) ReleaseAllAdvisoryLocks() int {
	return 0
}

// Close implements the sessionctx.Context interface.
func (c *Context) Close() {
}

// NewContext creates a new mocked sessionctx.Context.
func NewContext() *Context {
	ctx, cancel := context.WithCancel(context.Background())
	sctx := &Context{
		values:      make(map[fmt.Stringer]interface{}),
		sessionVars: variable.NewSessionVars(),
		ctx:         ctx,
		cancel:      cancel,
	}
	sctx.sessionVars.InitChunkSize = 2
	sctx.sessionVars.MaxChunkSize = 32
	sctx.sessionVars.StmtCtx.TimeZone = time.UTC
	sctx.sessionVars.StmtCtx.MemTracker = memory.NewTracker(-1, -1)
	sctx.sessionVars.StmtCtx.DiskTracker = disk.NewTracker(-1, -1)
	sctx.sessionVars.GlobalVarsAccessor = variable.NewMockGlobalAccessor()
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
