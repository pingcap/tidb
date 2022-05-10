// Copyright 2018 PingCAP, Inc.
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

package sessionctx

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/sli"
	"github.com/pingcap/tidb/util/topsql/stmtstats"
	"github.com/pingcap/tipb/go-binlog"
	"github.com/tikv/client-go/v2/oracle"
)

// InfoschemaMetaVersion is a workaround. Due to circular dependency,
// can not return the complete interface. But SchemaMetaVersion is widely used for logging.
// So we give a convenience for that.
// FIXME: remove this interface
type InfoschemaMetaVersion interface {
	SchemaMetaVersion() int64
}

// Context is an interface for transaction and executive args environment.
type Context interface {
	// NewTxn creates a new transaction for further execution.
	// If old transaction is valid, it is committed first.
	// It's used in BEGIN statement and DDL statements to commit old transaction.
	NewTxn(context.Context) error
	// NewStaleTxnWithStartTS initializes a staleness transaction with the given StartTS.
	NewStaleTxnWithStartTS(ctx context.Context, startTS uint64) error

	// Txn returns the current transaction which is created before executing a statement.
	// The returned kv.Transaction is not nil, but it maybe pending or invalid.
	// If the active parameter is true, call this function will wait for the pending txn
	// to become valid.
	Txn(active bool) (kv.Transaction, error)

	// GetClient gets a kv.Client.
	GetClient() kv.Client

	// GetMPPClient gets a kv.MPPClient.
	GetMPPClient() kv.MPPClient

	// SetValue saves a value associated with this context for key.
	SetValue(key fmt.Stringer, value interface{})

	// Value returns the value associated with this context for key.
	Value(key fmt.Stringer) interface{}

	// ClearValue clears the value associated with this context for key.
	ClearValue(key fmt.Stringer)

	// Deprecated: the semantics of session.GetInfoSchema() is ambiguous
	// If you want to get the infoschema of the current transaction in SQL layer, use sessiontxn.GetTxnManager(ctx).GetTxnInfoSchema()
	// If you want to get the latest infoschema use domain.GetDomain(ctx).GetInfoSchema()
	GetInfoSchema() InfoschemaMetaVersion

	GetSessionVars() *variable.SessionVars

	GetSessionManager() util.SessionManager

	// RefreshTxnCtx commits old transaction without retry,
	// and creates a new transaction.
	// now just for load data and batch insert.
	RefreshTxnCtx(context.Context) error

	// RefreshVars refreshes modified global variable to current session.
	// only used to daemon session like `statsHandle` to detect global variable change.
	RefreshVars(context.Context) error

	// InitTxnWithStartTS initializes a transaction with startTS.
	// It should be called right before we builds an executor.
	InitTxnWithStartTS(startTS uint64) error

	// GetSnapshotWithTS returns a snapshot with start ts
	GetSnapshotWithTS(ts uint64) kv.Snapshot

	// GetStore returns the store of session.
	GetStore() kv.Storage

	// PreparedPlanCache returns the cache of the physical plan
	PreparedPlanCache() *kvcache.SimpleLRUCache

	// StoreQueryFeedback stores the query feedback.
	StoreQueryFeedback(feedback interface{})

	// UpdateColStatsUsage updates the column stats usage.
	// TODO: maybe we can use a method called GetSessionStatsCollector to replace both StoreQueryFeedback and UpdateColStatsUsage but we need to deal with import circle if we do so.
	UpdateColStatsUsage(predicateColumns []model.TableColumnID)

	// HasDirtyContent checks whether there's dirty update on the given table.
	HasDirtyContent(tid int64) bool

	// StmtCommit flush all changes by the statement to the underlying transaction.
	StmtCommit()
	// StmtRollback provides statement level rollback.
	StmtRollback()
	// StmtGetMutation gets the binlog mutation for current statement.
	StmtGetMutation(int64) *binlog.TableMutation
	// IsDDLOwner checks whether this session is DDL owner.
	IsDDLOwner() bool
	// AddTableLock adds table lock to the session lock map.
	AddTableLock([]model.TableLockTpInfo)
	// ReleaseTableLocks releases table locks in the session lock map.
	ReleaseTableLocks(locks []model.TableLockTpInfo)
	// ReleaseTableLockByTableIDs releases table locks in the session lock map by table IDs.
	ReleaseTableLockByTableIDs(tableIDs []int64)
	// CheckTableLocked checks the table lock.
	CheckTableLocked(tblID int64) (bool, model.TableLockType)
	// GetAllTableLocks gets all table locks table id and db id hold by the session.
	GetAllTableLocks() []model.TableLockTpInfo
	// ReleaseAllTableLocks releases all table locks hold by the session.
	ReleaseAllTableLocks()
	// HasLockedTables uses to check whether this session locked any tables.
	HasLockedTables() bool
	// PrepareTSFuture uses to prepare timestamp by future.
	PrepareTSFuture(ctx context.Context)
	// StoreIndexUsage stores the index usage information.
	StoreIndexUsage(tblID int64, idxID int64, rowsSelected int64)
	// GetTxnWriteThroughputSLI returns the TxnWriteThroughputSLI.
	GetTxnWriteThroughputSLI() *sli.TxnWriteThroughputSLI
	// GetBuiltinFunctionUsage returns the BuiltinFunctionUsage of current Context, which is not thread safe.
	// Use primitive map type to prevent circular import. Should convert it to telemetry.BuiltinFunctionUsage before using.
	GetBuiltinFunctionUsage() map[string]uint32
	// BuiltinFunctionUsageInc increase the counting of each builtin function usage
	// Notice that this is a thread safe function
	BuiltinFunctionUsageInc(scalarFuncSigName string)
	// GetStmtStats returns stmtstats.StatementStats owned by implementation.
	GetStmtStats() *stmtstats.StatementStats
	// ShowProcess returns ProcessInfo running in current Context
	ShowProcess() *util.ProcessInfo
	// GetAdvisoryLock acquires an advisory lock (aka GET_LOCK()).
	GetAdvisoryLock(string, int64) error
	// ReleaseAdvisoryLock releases an advisory lock (aka RELEASE_LOCK()).
	ReleaseAdvisoryLock(string) bool
	// ReleaseAllAdvisoryLocks releases all advisory locks that this session holds.
	ReleaseAllAdvisoryLocks() int
}

type basicCtxType int

func (t basicCtxType) String() string {
	switch t {
	case QueryString:
		return "query_string"
	case Initing:
		return "initing"
	case LastExecuteDDL:
		return "last_execute_ddl"
	}
	return "unknown"
}

// Context keys.
const (
	// QueryString is the key for original query string.
	QueryString basicCtxType = 1
	// Initing is the key for indicating if the server is running bootstrap or upgrade job.
	Initing basicCtxType = 2
	// LastExecuteDDL is the key for whether the session execute a ddl command last time.
	LastExecuteDDL basicCtxType = 3
)

// ValidateSnapshotReadTS strictly validates that readTS does not exceed the PD timestamp
func ValidateSnapshotReadTS(ctx context.Context, sctx Context, readTS uint64) error {
	latestTS, err := sctx.GetStore().GetOracle().GetLowResolutionTimestamp(ctx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	// If we fail to get latestTS or the readTS exceeds it, get a timestamp from PD to double check
	if err != nil || readTS > latestTS {
		metrics.ValidateReadTSFromPDCount.Inc()
		currentVer, err := sctx.GetStore().CurrentVersion(oracle.GlobalTxnScope)
		if err != nil {
			return errors.Errorf("fail to validate read timestamp: %v", err)
		}
		if readTS > currentVer.Ver {
			return errors.Errorf("cannot set read timestamp to a future time")
		}
	}
	return nil
}

// How far future from now ValidateStaleReadTS allows at most
const allowedTimeFromNow = 100 * time.Millisecond

// ValidateStaleReadTS validates that readTS does not exceed the current time not strictly.
func ValidateStaleReadTS(ctx context.Context, sctx Context, readTS uint64) error {
	currentTS, err := sctx.GetStore().GetOracle().GetStaleTimestamp(ctx, oracle.GlobalTxnScope, 0)
	// If we fail to calculate currentTS from local time, fallback to get a timestamp from PD
	if err != nil {
		metrics.ValidateReadTSFromPDCount.Inc()
		currentVer, err := sctx.GetStore().CurrentVersion(oracle.GlobalTxnScope)
		if err != nil {
			return errors.Errorf("fail to validate read timestamp: %v", err)
		}
		currentTS = currentVer.Ver
	}
	if oracle.GetTimeFromTS(readTS).After(oracle.GetTimeFromTS(currentTS).Add(allowedTimeFromNow)) {
		return errors.Errorf("cannot set read timestamp to a future time")
	}
	return nil
}

// SysProcTracker is used to track background sys processes
type SysProcTracker interface {
	Track(id uint64, proc Context) error
	UnTrack(id uint64)
	GetSysProcessList() map[uint64]*util.ProcessInfo
	KillSysProcess(id uint64)
}
