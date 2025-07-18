// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sessionapi

import (
	"context"
	"sync"

	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/domain/sqlsvrapi"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/infoschema/validatorapi"
	"github.com/pingcap/tidb/pkg/kv"
	lockctx "github.com/pingcap/tidb/pkg/lock/context"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/session/cursor"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage/indexusage"
	"github.com/pingcap/tidb/pkg/table/tblctx"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/sli"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"github.com/tikv/client-go/v2/oracle"
)

// SessionStatesHandler is an interface for encoding and decoding session states.
type SessionStatesHandler interface {
	// EncodeSessionStates encodes session states into a JSON.
	EncodeSessionStates(context.Context, Context, *sessionstates.SessionStates) error
	// DecodeSessionStates decodes a map into session states.
	DecodeSessionStates(context.Context, Context, *sessionstates.SessionStates) error
}

// Context is an interface for transaction and executive args environment.
type Context interface {
	planctx.Common
	// EncodeStates encodes session states into a JSON.
	EncodeStates(context.Context, *sessionstates.SessionStates) error
	// DecodeStates decodes a map into session states.
	DecodeStates(context.Context, *sessionstates.SessionStates) error
	lockctx.TableLockContext
	// RollbackTxn rolls back the current transaction.
	RollbackTxn(ctx context.Context)
	// CommitTxn commits the current transaction.
	// buffered KV changes will be discarded, call StmtCommit if you want to commit them.
	CommitTxn(ctx context.Context) error
	// GetSchemaValidator returns the schema validator.
	GetSchemaValidator() validatorapi.Validator
	// GetSQLServer returns the sqlsvrapi.Server.
	GetSQLServer() sqlsvrapi.Server
	// GetTableCtx returns the table.MutateContext
	GetTableCtx() tblctx.MutateContext
	// GetPlanCtx gets the plan context of the current session.
	GetPlanCtx() planctx.PlanContext
	// GetDistSQLCtx gets the distsql ctx of the current session
	GetDistSQLCtx() *distsqlctx.DistSQLContext
	// RefreshTxnCtx commits old transaction without retry,
	// and creates a new transaction.
	// now just for load data and batch insert.
	RefreshTxnCtx(context.Context) error
	// GetSessionPlanCache returns the session-level cache of the physical plan.
	GetSessionPlanCache() sessionctx.SessionPlanCache
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
	// IsDDLOwner checks whether this session is DDL owner.
	IsDDLOwner() bool
	// PrepareTSFuture uses to prepare timestamp by future.
	PrepareTSFuture(ctx context.Context, future oracle.Future, scope string) error
	// GetPreparedTxnFuture returns the TxnFuture if it is valid or pending.
	// It returns nil otherwise.
	GetPreparedTxnFuture() TxnFuture
	// GetTxnWriteThroughputSLI returns the TxnWriteThroughputSLI.
	GetTxnWriteThroughputSLI() *sli.TxnWriteThroughputSLI
	// GetBuiltinFunctionUsage returns the BuiltinFunctionUsage of current Context, which is not thread safe.
	// Use primitive map type to prevent circular import. Should convert it to telemetry.BuiltinFunctionUsage before using.
	GetBuiltinFunctionUsage() map[string]uint32
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
	// GetCursorTracker returns the cursor tracker of the session
	GetCursorTracker() cursor.Tracker
	// GetCommitWaitGroup returns the wait group for async commit and secondary lock cleanup background goroutines
	GetCommitWaitGroup() *sync.WaitGroup
}

// TxnFuture is an interface where implementations have a kv.Transaction field and after
// calling Wait of the TxnFuture, the kv.Transaction will become valid.
type TxnFuture interface {
	// Wait converts pending txn to valid
	Wait(ctx context.Context, sctx Context) (kv.Transaction, error)
}
