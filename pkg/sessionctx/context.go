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
	"time"

	"github.com/pingcap/errors"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/extension"
	infoschema "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	tablelock "github.com/pingcap/tidb/pkg/lock/context"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/model"
	planctx "github.com/pingcap/tidb/pkg/planner/context"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage/indexusage"
	tbctx "github.com/pingcap/tidb/pkg/table/context"
	"github.com/pingcap/tidb/pkg/util"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/kvcache"
	utilpc "github.com/pingcap/tidb/pkg/util/plancache"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
	"github.com/pingcap/tidb/pkg/util/sli"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"github.com/pingcap/tipb/go-binlog"
	"github.com/tikv/client-go/v2/oracle"
)

// SessionStatesHandler is an interface for encoding and decoding session states.
type SessionStatesHandler interface {
	// EncodeSessionStates encodes session states into a JSON.
	EncodeSessionStates(context.Context, Context, *sessionstates.SessionStates) error
	// DecodeSessionStates decodes a map into session states.
	DecodeSessionStates(context.Context, Context, *sessionstates.SessionStates) error
}

// PlanCache is an interface for prepare and non-prepared plan cache
type PlanCache interface {
	Get(key kvcache.Key, opts *utilpc.PlanCacheMatchOpts) (value kvcache.Value, ok bool)
	Put(key kvcache.Key, value kvcache.Value, opts *utilpc.PlanCacheMatchOpts)
	Delete(key kvcache.Key)
	DeleteAll()
	Size() int
	SetCapacity(capacity uint) error
	Close()
}

// Context is an interface for transaction and executive args environment.
type Context interface {
	SessionStatesHandler
	contextutil.ValueStoreContext
	tablelock.TableLockContext
	// RollbackTxn rolls back the current transaction.
	RollbackTxn(ctx context.Context)
	// CommitTxn commits the current transaction.
	// buffered KV changes will be discarded, call StmtCommit if you want to commit them.
	CommitTxn(ctx context.Context) error
	// Txn returns the current transaction which is created before executing a statement.
	// The returned kv.Transaction is not nil, but it maybe pending or invalid.
	// If the active parameter is true, call this function will wait for the pending txn
	// to become valid.
	Txn(active bool) (kv.Transaction, error)

	// GetClient gets a kv.Client.
	GetClient() kv.Client

	// GetMPPClient gets a kv.MPPClient.
	GetMPPClient() kv.MPPClient

	// Deprecated: the semantics of session.GetInfoSchema() is ambiguous
	// If you want to get the infoschema of the current transaction in SQL layer, use sessiontxn.GetTxnManager(ctx).GetTxnInfoSchema()
	// If you want to get the latest infoschema use `GetDomainInfoSchema`
	GetInfoSchema() infoschema.MetaOnlyInfoSchema

	// GetDomainInfoSchema returns the latest information schema in domain
	// Different with `domain.InfoSchema()`, the information schema returned by this method
	// includes the temporary table definitions stored in session
	GetDomainInfoSchema() infoschema.MetaOnlyInfoSchema

	GetSessionVars() *variable.SessionVars

	// GetSQLExecutor returns the sqlexec.SQLExecutor.
	GetSQLExecutor() sqlexec.SQLExecutor

	// GetRestrictedSQLExecutor returns the sqlexec.RestrictedSQLExecutor.
	GetRestrictedSQLExecutor() sqlexec.RestrictedSQLExecutor

	// GetExprCtx returns the expression context of the session.
	GetExprCtx() exprctx.ExprContext

	// GetTableCtx returns the table.MutateContext
	GetTableCtx() tbctx.MutateContext

	// GetPlanCtx gets the plan context of the current session.
	GetPlanCtx() planctx.PlanContext

	// GetDistSQLCtx gets the distsql ctx of the current session
	GetDistSQLCtx() *distsqlctx.DistSQLContext

	// GetRangerCtx returns the context used in `ranger` related functions
	GetRangerCtx() *rangerctx.RangerContext

	// GetBuildPBCtx gets the ctx used in `ToPB` of the current session
	GetBuildPBCtx() *planctx.BuildPBContext

	GetSessionManager() util.SessionManager

	// RefreshTxnCtx commits old transaction without retry,
	// and creates a new transaction.
	// now just for load data and batch insert.
	RefreshTxnCtx(context.Context) error

	// GetStore returns the store of session.
	GetStore() kv.Storage

	// GetSessionPlanCache returns the session-level cache of the physical plan.
	GetSessionPlanCache() PlanCache

	// UpdateColStatsUsage updates the column stats usage.
	UpdateColStatsUsage(predicateColumns []model.TableItemID)

	// HasDirtyContent checks whether there's dirty update on the given table.
	HasDirtyContent(tid int64) bool

	// StmtCommit flush all changes by the statement to the underlying transaction.
	// it must be called before CommitTxn, else all changes since last StmtCommit
	// will be lost. For SQL statement, StmtCommit or StmtRollback is called automatically.
	// the "Stmt" not only means SQL statement, but also any KV changes, such as
	// meta KV.
	StmtCommit(ctx context.Context)
	// StmtRollback provides statement level rollback. The parameter `forPessimisticRetry` should be true iff it's used
	// for auto-retrying execution of DMLs in pessimistic transactions.
	// if error happens when you are handling batch of KV changes since last StmtCommit
	// or StmtRollback, and you don't want them to be committed, you must call StmtRollback
	// before you start another batch, otherwise, the previous changes might be committed
	// unexpectedly.
	StmtRollback(ctx context.Context, isForPessimisticRetry bool)
	// StmtGetMutation gets the binlog mutation for current statement.
	StmtGetMutation(int64) *binlog.TableMutation
	// IsDDLOwner checks whether this session is DDL owner.
	IsDDLOwner() bool
	// PrepareTSFuture uses to prepare timestamp by future.
	PrepareTSFuture(ctx context.Context, future oracle.Future, scope string) error
	// GetPreparedTxnFuture returns the TxnFuture if it is valid or pending.
	// It returns nil otherwise.
	GetPreparedTxnFuture() TxnFuture
	// GetTxnWriteThroughputSLI returns the TxnWriteThroughputSLI.
	GetTxnWriteThroughputSLI() *sli.TxnWriteThroughputSLI
	// GetStmtStats returns stmtstats.StatementStats owned by implementation.
	GetStmtStats() *stmtstats.StatementStats
	// ShowProcess returns ProcessInfo running in current Context
	ShowProcess() *util.ProcessInfo
	// GetAdvisoryLock acquires an advisory lock (aka GET_LOCK()).
	GetAdvisoryLock(string, int64) error
	// IsUsedAdvisoryLock checks for existing locks (aka IS_USED_LOCK()).
	IsUsedAdvisoryLock(string) uint64
	// ReleaseAdvisoryLock releases an advisory lock (aka RELEASE_LOCK()).
	ReleaseAdvisoryLock(string) bool
	// ReleaseAllAdvisoryLocks releases all advisory locks that this session holds.
	ReleaseAllAdvisoryLocks() int
	// GetExtensions returns the `*extension.SessionExtensions` object
	GetExtensions() *extension.SessionExtensions
	// InSandBoxMode indicates that this Session is in sandbox mode
	// Ref about sandbox mode: https://dev.mysql.com/doc/refman/8.0/en/expired-password-handling.html
	InSandBoxMode() bool
	// EnableSandBoxMode enable the sandbox mode of this Session
	EnableSandBoxMode()
	// DisableSandBoxMode enable the sandbox mode of this Session
	DisableSandBoxMode()
	// ReportUsageStats reports the usage stats to the global collector
	ReportUsageStats()
	// NewStmtIndexUsageCollector creates a new index usage collector for statement
	NewStmtIndexUsageCollector() *indexusage.StmtIndexUsageCollector
}

// TxnFuture is an interface where implementations have a kv.Transaction field and after
// calling Wait of the TxnFuture, the kv.Transaction will become valid.
type TxnFuture interface {
	// Wait converts pending txn to valid
	Wait(ctx context.Context, sctx Context) (kv.Transaction, error)
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
func ValidateStaleReadTS(ctx context.Context, sc *stmtctx.StatementContext, store kv.Storage, readTS uint64) error {
	currentTS, err := sc.GetStaleTSO()
	if currentTS == 0 || err != nil {
		currentTS, err = store.GetOracle().GetStaleTimestamp(ctx, oracle.GlobalTxnScope, 0)
	}
	// If we fail to calculate currentTS from local time, fallback to get a timestamp from PD
	if err != nil {
		metrics.ValidateReadTSFromPDCount.Inc()
		currentVer, err := store.CurrentVersion(oracle.GlobalTxnScope)
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
