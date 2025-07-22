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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package session

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	stderrs "errors"
	"fmt"
	"iter"
	"math"
	"math/rand"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/disttask/importinto"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/domain/sqlsvrapi"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/executor/staticrecordset"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/expression/sessionexpr"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/extension/extensionimpl"
	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemactx "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/infoschema/isvalidator"
	"github.com/pingcap/tidb/pkg/infoschema/validatorapi"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/param"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	planctx "github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/plugin"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/privilege/conn"
	"github.com/pingcap/tidb/pkg/privilege/privileges"
	"github.com/pingcap/tidb/pkg/session/cursor"
	session_metrics "github.com/pingcap/tidb/pkg/session/metrics"
	"github.com/pingcap/tidb/pkg/session/txninfo"
	"github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	statshandle "github.com/pingcap/tidb/pkg/statistics/handle"
	"github.com/pingcap/tidb/pkg/statistics/handle/syncload"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage/indexusage"
	kvstore "github.com/pingcap/tidb/pkg/store"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tblctx"
	"github.com/pingcap/tidb/pkg/table/tblsession"
	"github.com/pingcap/tidb/pkg/table/temptable"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/telemetry"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/logutil/consistency"
	"github.com/pingcap/tidb/pkg/util/memory"
	parserutil "github.com/pingcap/tidb/pkg/util/parser"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/pingcap/tidb/pkg/util/sem"
	"github.com/pingcap/tidb/pkg/util/sli"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/pingcap/tidb/pkg/util/topsql"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"github.com/pingcap/tidb/pkg/util/tracing"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	tikvutil "github.com/tikv/client-go/v2/util"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func init() {
	executor.CreateSession = func(ctx sessionctx.Context) (sessionctx.Context, error) {
		return CreateSession(ctx.GetStore())
	}
	executor.CloseSession = func(ctx sessionctx.Context) {
		if se, ok := ctx.(types.Session); ok {
			se.Close()
		}
	}
}

var _ types.Session = (*session)(nil)

type stmtRecord struct {
	st      sqlexec.Statement
	stmtCtx *stmtctx.StatementContext
}

// StmtHistory holds all histories of statements in a txn.
type StmtHistory struct {
	history []*stmtRecord
}

// Add appends a stmt to history list.
func (h *StmtHistory) Add(st sqlexec.Statement, stmtCtx *stmtctx.StatementContext) {
	s := &stmtRecord{
		st:      st,
		stmtCtx: stmtCtx,
	}
	h.history = append(h.history, s)
}

// Count returns the count of the history.
func (h *StmtHistory) Count() int {
	return len(h.history)
}

type session struct {
	// processInfo is used by ShowProcess(), and should be modified atomically.
	processInfo atomic.Pointer[util.ProcessInfo]
	txn         LazyTxn

	mu struct {
		sync.RWMutex
		values map[fmt.Stringer]any
	}

	currentCtx  context.Context // only use for runtime.trace, Please NEVER use it.
	currentPlan base.Plan

	// dom is *domain.Domain, use `any` to avoid import cycle.
	// cross keyspace session doesn't have domain set.
	dom any
	// we cannot compare dom == nil, as dom is untyped, golang will always return false.
	crossKS         bool
	schemaValidator validatorapi.Validator
	infoCache       *infoschema.InfoCache
	store           kv.Storage

	sessionPlanCache sessionctx.SessionPlanCache

	sessionVars    *variable.SessionVars
	sessionManager util.SessionManager

	pctx    *planContextImpl
	exprctx *sessionexpr.ExprContext
	tblctx  *tblsession.MutateContext

	statsCollector *usage.SessionStatsItem
	// ddlOwnerManager is used in `select tidb_is_ddl_owner()` statement;
	ddlOwnerManager owner.Manager
	// lockedTables use to record the table locks hold by the session.
	lockedTables map[int64]model.TableLockTpInfo

	// client shared coprocessor client per session
	client kv.Client

	mppClient kv.MPPClient

	// indexUsageCollector collects index usage information.
	idxUsageCollector *indexusage.SessionIndexUsageCollector

	functionUsageMu struct {
		syncutil.RWMutex
		builtinFunctionUsage telemetry.BuiltinFunctionsUsage
	}
	// allowed when tikv disk full happened.
	diskFullOpt kvrpcpb.DiskFullOpt

	// StmtStats is used to count various indicators of each SQL in this session
	// at each point in time. These data will be periodically taken away by the
	// background goroutine. The background goroutine will continue to aggregate
	// all the local data in each session, and finally report them to the remote
	// regularly.
	stmtStats *stmtstats.StatementStats

	// Used to encode and decode each type of session states.
	sessionStatesHandlers map[sessionstates.SessionStateType]sessionctx.SessionStatesHandler

	// Contains a list of sessions used to collect advisory locks.
	advisoryLocks map[string]*advisoryLock

	extensions *extension.SessionExtensions

	sandBoxMode bool

	cursorTracker cursor.Tracker

	// Used to wait for all async commit background jobs to finish.
	commitWaitGroup sync.WaitGroup
}

// AddTableLock adds table lock to the session lock map.
func (s *session) AddTableLock(locks []model.TableLockTpInfo) {
	for _, l := range locks {
		// read only lock is session unrelated, skip it when adding lock to session.
		if l.Tp != ast.TableLockReadOnly {
			s.lockedTables[l.TableID] = l
		}
	}
}

// ReleaseTableLocks releases table lock in the session lock map.
func (s *session) ReleaseTableLocks(locks []model.TableLockTpInfo) {
	for _, l := range locks {
		delete(s.lockedTables, l.TableID)
	}
}

// ReleaseTableLockByTableIDs releases table lock in the session lock map by table ID.
func (s *session) ReleaseTableLockByTableIDs(tableIDs []int64) {
	for _, tblID := range tableIDs {
		delete(s.lockedTables, tblID)
	}
}

// CheckTableLocked checks the table lock.
func (s *session) CheckTableLocked(tblID int64) (bool, ast.TableLockType) {
	lt, ok := s.lockedTables[tblID]
	if !ok {
		return false, ast.TableLockNone
	}
	return true, lt.Tp
}

// GetAllTableLocks gets all table locks table id and db id hold by the session.
func (s *session) GetAllTableLocks() []model.TableLockTpInfo {
	lockTpInfo := make([]model.TableLockTpInfo, 0, len(s.lockedTables))
	for _, tl := range s.lockedTables {
		lockTpInfo = append(lockTpInfo, tl)
	}
	return lockTpInfo
}

// HasLockedTables uses to check whether this session locked any tables.
// If so, the session can only visit the table which locked by self.
func (s *session) HasLockedTables() bool {
	b := len(s.lockedTables) > 0
	return b
}

// ReleaseAllTableLocks releases all table locks hold by the session.
func (s *session) ReleaseAllTableLocks() {
	s.lockedTables = make(map[int64]model.TableLockTpInfo)
}

// IsDDLOwner checks whether this session is DDL owner.
func (s *session) IsDDLOwner() bool {
	return s.ddlOwnerManager.IsOwner()
}

func (s *session) cleanRetryInfo() {
	if s.sessionVars.RetryInfo.Retrying {
		return
	}

	retryInfo := s.sessionVars.RetryInfo
	defer retryInfo.Clean()
	if len(retryInfo.DroppedPreparedStmtIDs) == 0 {
		return
	}

	planCacheEnabled := s.GetSessionVars().EnablePreparedPlanCache
	var cacheKey string
	var err error
	var preparedObj *plannercore.PlanCacheStmt
	if planCacheEnabled {
		firstStmtID := retryInfo.DroppedPreparedStmtIDs[0]
		if preparedPointer, ok := s.sessionVars.PreparedStmts[firstStmtID]; ok {
			preparedObj, ok = preparedPointer.(*plannercore.PlanCacheStmt)
			if ok {
				cacheKey, _, _, _, err = plannercore.NewPlanCacheKey(s, preparedObj)
				if err != nil {
					logutil.Logger(s.currentCtx).Warn("clean cached plan failed", zap.Error(err))
					return
				}
			}
		}
	}
	for i, stmtID := range retryInfo.DroppedPreparedStmtIDs {
		if planCacheEnabled {
			if i > 0 && preparedObj != nil {
				cacheKey, _, _, _, err = plannercore.NewPlanCacheKey(s, preparedObj)
				if err != nil {
					logutil.Logger(s.currentCtx).Warn("clean cached plan failed", zap.Error(err))
					return
				}
			}
			if !s.sessionVars.IgnorePreparedCacheCloseStmt { // keep the plan in cache
				s.GetSessionPlanCache().Delete(cacheKey)
			}
		}
		s.sessionVars.RemovePreparedStmt(stmtID)
	}
}

func (s *session) Status() uint16 {
	return s.sessionVars.Status()
}

func (s *session) LastInsertID() uint64 {
	if s.sessionVars.StmtCtx.LastInsertID > 0 {
		return s.sessionVars.StmtCtx.LastInsertID
	}
	return s.sessionVars.StmtCtx.InsertID
}

func (s *session) LastMessage() string {
	return s.sessionVars.StmtCtx.GetMessage()
}

func (s *session) AffectedRows() uint64 {
	return s.sessionVars.StmtCtx.AffectedRows()
}

func (s *session) SetClientCapability(capability uint32) {
	s.sessionVars.ClientCapability = capability
}

func (s *session) SetConnectionID(connectionID uint64) {
	s.sessionVars.ConnectionID = connectionID
}

func (s *session) SetTLSState(tlsState *tls.ConnectionState) {
	// If user is not connected via TLS, then tlsState == nil.
	if tlsState != nil {
		s.sessionVars.TLSConnectionState = tlsState
	}
}

func (s *session) SetCompressionAlgorithm(ca int) {
	s.sessionVars.CompressionAlgorithm = ca
}

func (s *session) SetCompressionLevel(level int) {
	s.sessionVars.CompressionLevel = level
}

func (s *session) SetCommandValue(command byte) {
	atomic.StoreUint32(&s.sessionVars.CommandValue, uint32(command))
}

func (s *session) SetCollation(coID int) error {
	cs, co, err := charset.GetCharsetInfoByID(coID)
	if err != nil {
		return err
	}
	// If new collations are enabled, switch to the default
	// collation if this one is not supported.
	co = collate.SubstituteMissingCollationToDefault(co)
	for _, v := range vardef.SetNamesVariables {
		terror.Log(s.sessionVars.SetSystemVarWithoutValidation(v, cs))
	}
	return s.sessionVars.SetSystemVarWithoutValidation(vardef.CollationConnection, co)
}

func (s *session) GetSessionPlanCache() sessionctx.SessionPlanCache {
	// use the prepared plan cache
	if !s.GetSessionVars().EnablePreparedPlanCache && !s.GetSessionVars().EnableNonPreparedPlanCache {
		return nil
	}
	if s.sessionPlanCache == nil { // lazy construction
		s.sessionPlanCache = plannercore.NewLRUPlanCache(uint(s.GetSessionVars().SessionPlanCacheSize),
			vardef.PreparedPlanCacheMemoryGuardRatio.Load(), plannercore.PreparedPlanCacheMaxMemory.Load(), s, false)
	}
	return s.sessionPlanCache
}

func (s *session) SetSessionManager(sm util.SessionManager) {
	s.sessionManager = sm
}

func (s *session) GetSessionManager() util.SessionManager {
	return s.sessionManager
}

func (s *session) UpdateColStatsUsage(colStatsUsage iter.Seq[model.TableItemID]) {
	if s.statsCollector == nil {
		return
	}
	t := time.Now()
	s.statsCollector.UpdateColStatsUsage(colStatsUsage, t)
}

// FieldList returns fields list of a table.
func (s *session) FieldList(tableName string) ([]*resolve.ResultField, error) {
	is := s.GetInfoSchema().(infoschema.InfoSchema)
	dbName := ast.NewCIStr(s.GetSessionVars().CurrentDB)
	tName := ast.NewCIStr(tableName)
	pm := privilege.GetPrivilegeManager(s)
	if pm != nil && s.sessionVars.User != nil {
		if !pm.RequestVerification(s.sessionVars.ActiveRoles, dbName.O, tName.O, "", mysql.AllPrivMask) {
			user := s.sessionVars.User
			u := user.Username
			h := user.Hostname
			if len(user.AuthUsername) > 0 && len(user.AuthHostname) > 0 {
				u = user.AuthUsername
				h = user.AuthHostname
			}
			return nil, plannererrors.ErrTableaccessDenied.GenWithStackByArgs("SELECT", u, h, tableName)
		}
	}
	table, err := is.TableByName(context.Background(), dbName, tName)
	if err != nil {
		return nil, err
	}

	cols := table.Cols()
	fields := make([]*resolve.ResultField, 0, len(cols))
	for _, col := range table.Cols() {
		rf := &resolve.ResultField{
			ColumnAsName: col.Name,
			TableAsName:  tName,
			DBName:       dbName,
			Table:        table.Meta(),
			Column:       col.ColumnInfo,
		}
		fields = append(fields, rf)
	}
	return fields, nil
}

// TxnInfo returns a pointer to a *copy* of the internal TxnInfo, thus is *read only*
// Process field may not initialize if this is a session used internally.
func (s *session) TxnInfo() *txninfo.TxnInfo {
	s.txn.mu.RLock()
	// Copy on read to get a snapshot, this API shouldn't be frequently called.
	txnInfo := s.txn.mu.TxnInfo
	s.txn.mu.RUnlock()

	if txnInfo.StartTS == 0 {
		return nil
	}

	processInfo := s.ShowProcess()
	if processInfo == nil {
		return &txnInfo
	}
	txnInfo.ProcessInfo = &txninfo.ProcessInfo{
		ConnectionID:    processInfo.ID,
		Username:        processInfo.User,
		CurrentDB:       processInfo.DB,
		RelatedTableIDs: make(map[int64]struct{}),
	}
	s.GetSessionVars().GetRelatedTableForMDL().Range(func(key, _ any) bool {
		txnInfo.ProcessInfo.RelatedTableIDs[key.(int64)] = struct{}{}
		return true
	})
	return &txnInfo
}

func (s *session) doCommit(ctx context.Context) error {
	if !s.txn.Valid() {
		return nil
	}

	defer func() {
		s.txn.changeToInvalid()
		s.sessionVars.SetInTxn(false)
		s.sessionVars.ClearDiskFullOpt()
	}()
	// check if the transaction is read-only
	if s.txn.IsReadOnly() {
		return nil
	}
	// check if the cluster is read-only
	if !s.sessionVars.InRestrictedSQL && (vardef.RestrictedReadOnly.Load() || vardef.VarTiDBSuperReadOnly.Load()) {
		// It is not internal SQL, and the cluster has one of RestrictedReadOnly or SuperReadOnly
		// We need to privilege check again: a privilege check occurred during planning, but we need
		// to prevent the case that a long running auto-commit statement is now trying to commit.
		pm := privilege.GetPrivilegeManager(s)
		roles := s.sessionVars.ActiveRoles
		if pm != nil && !pm.HasExplicitlyGrantedDynamicPrivilege(roles, "RESTRICTED_REPLICA_WRITER_ADMIN", false) {
			s.RollbackTxn(ctx)
			return plannererrors.ErrSQLInReadOnlyMode
		}
	}
	err := s.checkPlacementPolicyBeforeCommit(ctx)
	if err != nil {
		return err
	}
	// mockCommitError and mockGetTSErrorInRetry use to test PR #8743.
	failpoint.Inject("mockCommitError", func(val failpoint.Value) {
		if val.(bool) {
			if _, err := failpoint.Eval("tikvclient/mockCommitErrorOpt"); err == nil {
				failpoint.Return(kv.ErrTxnRetryable)
			}
		}
	})

	sessVars := s.GetSessionVars()

	var commitTSChecker func(uint64) bool
	if tables := sessVars.TxnCtx.CachedTables; len(tables) > 0 {
		c := cachedTableRenewLease{tables: tables}
		now := time.Now()
		err := c.start(ctx)
		defer c.stop(ctx)
		sessVars.StmtCtx.WaitLockLeaseTime += time.Since(now)
		if err != nil {
			return errors.Trace(err)
		}
		commitTSChecker = c.commitTSCheck
	}
	if err = sessiontxn.GetTxnManager(s).SetOptionsBeforeCommit(s.txn.Transaction, commitTSChecker); err != nil {
		return err
	}

	err = s.commitTxnWithTemporaryData(tikvutil.SetSessionID(ctx, sessVars.ConnectionID), &s.txn)
	if err != nil {
		err = s.handleAssertionFailure(ctx, err)
	}
	return err
}

type cachedTableRenewLease struct {
	tables map[int64]any
	lease  []uint64 // Lease for each visited cached tables.
	exit   chan struct{}
}

func (c *cachedTableRenewLease) start(ctx context.Context) error {
	c.exit = make(chan struct{})
	c.lease = make([]uint64, len(c.tables))
	wg := make(chan error, len(c.tables))
	ith := 0
	for _, raw := range c.tables {
		tbl := raw.(table.CachedTable)
		go tbl.WriteLockAndKeepAlive(ctx, c.exit, &c.lease[ith], wg)
		ith++
	}

	// Wait for all LockForWrite() return, this function can return.
	var err error
	for ; ith > 0; ith-- {
		tmp := <-wg
		if tmp != nil {
			err = tmp
		}
	}
	return err
}

func (c *cachedTableRenewLease) stop(_ context.Context) {
	close(c.exit)
}

func (c *cachedTableRenewLease) commitTSCheck(commitTS uint64) bool {
	for i := range c.lease {
		lease := atomic.LoadUint64(&c.lease[i])
		if commitTS >= lease {
			// Txn fails to commit because the write lease is expired.
			return false
		}
	}
	return true
}

// handleAssertionFailure extracts the possible underlying assertionFailed error,
// gets the corresponding MVCC history and logs it.
// If it's not an assertion failure, returns the original error.
func (s *session) handleAssertionFailure(ctx context.Context, err error) error {
	var assertionFailure *tikverr.ErrAssertionFailed
	if !stderrs.As(err, &assertionFailure) {
		return err
	}
	key := assertionFailure.Key
	newErr := kv.ErrAssertionFailed.GenWithStackByArgs(
		hex.EncodeToString(key), assertionFailure.Assertion.String(), assertionFailure.StartTs,
		assertionFailure.ExistingStartTs, assertionFailure.ExistingCommitTs,
	)

	rmode := s.GetSessionVars().EnableRedactLog
	if rmode == errors.RedactLogEnable {
		return newErr
	}

	var decodeFunc func(kv.Key, *kvrpcpb.MvccGetByKeyResponse, map[string]any)
	// if it's a record key or an index key, decode it
	if infoSchema, ok := s.sessionVars.TxnCtx.InfoSchema.(infoschema.InfoSchema); ok &&
		infoSchema != nil && (tablecodec.IsRecordKey(key) || tablecodec.IsIndexKey(key)) {
		tableOrPartitionID := tablecodec.DecodeTableID(key)
		tbl, ok := infoSchema.TableByID(ctx, tableOrPartitionID)
		if !ok {
			tbl, _, _ = infoSchema.FindTableByPartitionID(tableOrPartitionID)
		}
		if tbl == nil {
			logutil.Logger(ctx).Warn("cannot find table by id", zap.Int64("tableID", tableOrPartitionID), zap.String("key", hex.EncodeToString(key)))
			return newErr
		}

		if tablecodec.IsRecordKey(key) {
			decodeFunc = consistency.DecodeRowMvccData(tbl.Meta())
		} else {
			tableInfo := tbl.Meta()
			_, indexID, _, e := tablecodec.DecodeIndexKey(key)
			if e != nil {
				logutil.Logger(ctx).Error("assertion failed but cannot decode index key", zap.Error(e))
				return newErr
			}
			var indexInfo *model.IndexInfo
			for _, idx := range tableInfo.Indices {
				if idx.ID == indexID {
					indexInfo = idx
					break
				}
			}
			if indexInfo == nil {
				return newErr
			}
			decodeFunc = consistency.DecodeIndexMvccData(indexInfo)
		}
	}
	if store, ok := s.store.(helper.Storage); ok {
		content := consistency.GetMvccByKey(store, key, decodeFunc)
		logutil.Logger(ctx).Error("assertion failed", zap.String("message", newErr.Error()), zap.String("mvcc history", redact.String(rmode, content)))
	}
	return newErr
}

func (s *session) commitTxnWithTemporaryData(ctx context.Context, txn kv.Transaction) error {
	sessVars := s.sessionVars
	txnTempTables := sessVars.TxnCtx.TemporaryTables
	if len(txnTempTables) == 0 {
		failpoint.Inject("mockSleepBeforeTxnCommit", func(v failpoint.Value) {
			ms := v.(int)
			time.Sleep(time.Millisecond * time.Duration(ms))
		})
		return txn.Commit(ctx)
	}

	sessionData := sessVars.TemporaryTableData
	var (
		stage           kv.StagingHandle
		localTempTables *infoschema.SessionTables
	)

	if sessVars.LocalTemporaryTables != nil {
		localTempTables = sessVars.LocalTemporaryTables.(*infoschema.SessionTables)
	} else {
		localTempTables = new(infoschema.SessionTables)
	}

	defer func() {
		// stage != kv.InvalidStagingHandle means error occurs, we need to cleanup sessionData
		if stage != kv.InvalidStagingHandle {
			sessionData.Cleanup(stage)
		}
	}()

	for tblID, tbl := range txnTempTables {
		if !tbl.GetModified() {
			continue
		}

		if tbl.GetMeta().TempTableType != model.TempTableLocal {
			continue
		}
		if _, ok := localTempTables.TableByID(tblID); !ok {
			continue
		}

		if stage == kv.InvalidStagingHandle {
			stage = sessionData.Staging()
		}

		tblPrefix := tablecodec.EncodeTablePrefix(tblID)
		endKey := tablecodec.EncodeTablePrefix(tblID + 1)

		txnMemBuffer := s.txn.GetMemBuffer()
		iter, err := txnMemBuffer.Iter(tblPrefix, endKey)
		if err != nil {
			return err
		}

		for iter.Valid() {
			key := iter.Key()
			if !bytes.HasPrefix(key, tblPrefix) {
				break
			}

			value := iter.Value()
			if len(value) == 0 {
				err = sessionData.DeleteTableKey(tblID, key)
			} else {
				err = sessionData.SetTableKey(tblID, key, iter.Value())
			}

			if err != nil {
				return err
			}

			err = iter.Next()
			if err != nil {
				return err
			}
		}
	}

	err := txn.Commit(ctx)
	if err != nil {
		return err
	}

	if stage != kv.InvalidStagingHandle {
		sessionData.Release(stage)
		stage = kv.InvalidStagingHandle
	}

	return nil
}

// errIsNoisy is used to filter DUPLICATE KEY errors.
// These can observed by users in INFORMATION_SCHEMA.CLIENT_ERRORS_SUMMARY_GLOBAL instead.
//
// The rationale for filtering these errors is because they are "client generated errors". i.e.
// of the errors defined in kv/error.go, these look to be clearly related to a client-inflicted issue,
// and the server is only responsible for handling the error correctly. It does not need to log.
func errIsNoisy(err error) bool {
	if kv.ErrKeyExists.Equal(err) {
		return true
	}
	if storeerr.ErrLockAcquireFailAndNoWaitSet.Equal(err) {
		return true
	}
	return false
}

func (s *session) doCommitWithRetry(ctx context.Context) error {
	defer func() {
		s.GetSessionVars().SetTxnIsolationLevelOneShotStateForNextTxn()
		s.txn.changeToInvalid()
		s.cleanRetryInfo()
		sessiontxn.GetTxnManager(s).OnTxnEnd()
	}()
	if !s.txn.Valid() {
		// If the transaction is invalid, maybe it has already been rolled back by the client.
		return nil
	}
	isInternalTxn := false
	if internal := s.txn.GetOption(kv.RequestSourceInternal); internal != nil && internal.(bool) {
		isInternalTxn = true
	}
	var err error
	txnSize := s.txn.Size()
	isPessimistic := s.txn.IsPessimistic()
	isPipelined := s.txn.IsPipelined()
	r, ctx := tracing.StartRegionEx(ctx, "session.doCommitWithRetry")
	defer r.End()

	err = s.doCommit(ctx)
	if err != nil {
		// polish the Write Conflict error message
		newErr := s.tryReplaceWriteConflictError(ctx, err)
		if newErr != nil {
			err = newErr
		}

		commitRetryLimit := s.sessionVars.RetryLimit
		if !s.sessionVars.TxnCtx.CouldRetry {
			commitRetryLimit = 0
		}
		// Don't retry in BatchInsert mode. As a counter-example, insert into t1 select * from t2,
		// BatchInsert already commit the first batch 1000 rows, then it commit 1000-2000 and retry the statement,
		// Finally t1 will have more data than t2, with no errors return to user!
		if s.isTxnRetryableError(err) && !s.sessionVars.BatchInsert && commitRetryLimit > 0 && !isPessimistic && !isPipelined {
			logutil.Logger(ctx).Warn("sql",
				zap.String("label", s.GetSQLLabel()),
				zap.Error(err),
				zap.String("txn", s.txn.GoString()))
			// Transactions will retry 2 ~ commitRetryLimit times.
			// We make larger transactions retry less times to prevent cluster resource outage.
			txnSizeRate := float64(txnSize) / float64(kv.TxnTotalSizeLimit.Load())
			maxRetryCount := commitRetryLimit - int64(float64(commitRetryLimit-1)*txnSizeRate)
			err = s.retry(ctx, uint(maxRetryCount))
		} else if !errIsNoisy(err) {
			logutil.Logger(ctx).Warn("can not retry txn",
				zap.String("label", s.GetSQLLabel()),
				zap.Error(err),
				zap.Bool("IsBatchInsert", s.sessionVars.BatchInsert),
				zap.Bool("IsPessimistic", isPessimistic),
				zap.Bool("InRestrictedSQL", s.sessionVars.InRestrictedSQL),
				zap.Int64("tidb_retry_limit", s.sessionVars.RetryLimit),
				zap.Bool("tidb_disable_txn_auto_retry", s.sessionVars.DisableTxnAutoRetry))
		}
	}
	counter := s.sessionVars.TxnCtx.StatementCount
	duration := time.Since(s.GetSessionVars().TxnCtx.CreateTime).Seconds()
	s.recordOnTransactionExecution(err, counter, duration, isInternalTxn)

	if err != nil {
		if !errIsNoisy(err) {
			logutil.Logger(ctx).Warn("commit failed",
				zap.String("finished txn", s.txn.GoString()),
				zap.Error(err))
		}
		return err
	}
	s.updateStatsDeltaToCollector()
	return nil
}

// adds more information about the table in the error message
// precondition: oldErr is a 9007:WriteConflict Error
func (s *session) tryReplaceWriteConflictError(ctx context.Context, oldErr error) (newErr error) {
	if !kv.ErrWriteConflict.Equal(oldErr) {
		return nil
	}
	if errors.RedactLogEnabled.Load() == errors.RedactLogEnable {
		return nil
	}
	originErr := errors.Cause(oldErr)
	inErr, _ := originErr.(*errors.Error)
	// we don't want to modify the oldErr, so copy the args list
	oldArgs := inErr.Args()
	args := make([]any, len(oldArgs))
	copy(args, oldArgs)
	is := sessiontxn.GetTxnManager(s).GetTxnInfoSchema()
	if is == nil {
		return nil
	}
	newKeyTableField, ok := addTableNameInTableIDField(ctx, args[3], is)
	if ok {
		args[3] = newKeyTableField
	}
	newPrimaryKeyTableField, ok := addTableNameInTableIDField(ctx, args[5], is)
	if ok {
		args[5] = newPrimaryKeyTableField
	}
	return kv.ErrWriteConflict.FastGenByArgs(args...)
}

// precondition: is != nil
func addTableNameInTableIDField(ctx context.Context, tableIDField any, is infoschema.InfoSchema) (enhancedMsg string, done bool) {
	keyTableID, ok := tableIDField.(string)
	if !ok {
		return "", false
	}
	stringsInTableIDField := strings.Split(keyTableID, "=")
	if len(stringsInTableIDField) == 0 {
		return "", false
	}
	tableIDStr := stringsInTableIDField[len(stringsInTableIDField)-1]
	tableID, err := strconv.ParseInt(tableIDStr, 10, 64)
	if err != nil {
		return "", false
	}
	var tableName string
	tbl, ok := is.TableByID(ctx, tableID)
	if !ok {
		tableName = "unknown"
	} else {
		dbInfo, ok := infoschema.SchemaByTable(is, tbl.Meta())
		if !ok {
			tableName = "unknown." + tbl.Meta().Name.String()
		} else {
			tableName = dbInfo.Name.String() + "." + tbl.Meta().Name.String()
		}
	}
	enhancedMsg = keyTableID + ", tableName=" + tableName
	return enhancedMsg, true
}

func (s *session) updateStatsDeltaToCollector() {
	mapper := s.GetSessionVars().TxnCtx.TableDeltaMap
	if s.statsCollector != nil && mapper != nil {
		for _, item := range mapper {
			if item.TableID > 0 {
				s.statsCollector.Update(item.TableID, item.Delta, item.Count)
			}
		}
	}
}

func (s *session) CommitTxn(ctx context.Context) error {
	r, ctx := tracing.StartRegionEx(ctx, "session.CommitTxn")
	defer r.End()

	s.setLastTxnInfoBeforeTxnEnd()
	var commitDetail *tikvutil.CommitDetails
	ctx = context.WithValue(ctx, tikvutil.CommitDetailCtxKey, &commitDetail)
	err := s.doCommitWithRetry(ctx)
	if commitDetail != nil {
		s.sessionVars.StmtCtx.MergeExecDetails(commitDetail)
	}

	if err == nil && s.txn.lastCommitTS > 0 {
		// lastCommitTS could be the same, e.g. when the txn is considered readonly
		if s.txn.lastCommitTS < s.sessionVars.LastCommitTS {
			logutil.BgLogger().Error("check lastCommitTS failed",
				zap.Uint64("sessionLastCommitTS", s.sessionVars.LastCommitTS),
				zap.Uint64("txnLastCommitTS", s.txn.lastCommitTS),
				zap.String("sql", redact.String(s.sessionVars.EnableRedactLog, s.sessionVars.StmtCtx.OriginalSQL)),
			)
			return fmt.Errorf("txn commit_ts:%d is before session last_commit_ts:%d",
				s.txn.lastCommitTS, s.sessionVars.LastCommitTS)
		}
		s.sessionVars.LastCommitTS = s.txn.lastCommitTS
	}

	// record the TTLInsertRows in the metric
	metrics.TTLInsertRowsCount.Add(float64(s.sessionVars.TxnCtx.InsertTTLRowsCount))
	metrics.DDLCommitTempIndexWrite(s.sessionVars.ConnectionID)

	failpoint.Inject("keepHistory", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(err)
		}
	})
	s.sessionVars.TxnCtx.Cleanup()
	s.sessionVars.CleanupTxnReadTSIfUsed()
	return err
}

func (s *session) RollbackTxn(ctx context.Context) {
	r, ctx := tracing.StartRegionEx(ctx, "session.RollbackTxn")
	defer r.End()

	s.setLastTxnInfoBeforeTxnEnd()
	if s.txn.Valid() {
		terror.Log(s.txn.Rollback())
	}
	if ctx.Value(inCloseSession{}) == nil {
		s.cleanRetryInfo()
	}
	s.txn.changeToInvalid()
	s.sessionVars.TxnCtx.Cleanup()
	s.sessionVars.CleanupTxnReadTSIfUsed()
	s.sessionVars.SetInTxn(false)
	sessiontxn.GetTxnManager(s).OnTxnEnd()
	metrics.DDLRollbackTempIndexWrite(s.sessionVars.ConnectionID)
}

// setLastTxnInfoBeforeTxnEnd sets the @@last_txn_info variable before commit/rollback the transaction.
// The `LastTxnInfo` updated with a JSON string that contains start_ts, for_update_ts, etc.
// The `LastTxnInfo` is updated without the `commit_ts` fields because it is unknown
// until the commit is done (or do not need to commit for readonly or a rollback transaction).
// The non-readonly transaction will overwrite the `LastTxnInfo` again after commit to update the `commit_ts` field.
func (s *session) setLastTxnInfoBeforeTxnEnd() {
	txnCtx := s.GetSessionVars().TxnCtx
	if txnCtx.StartTS == 0 {
		// If the txn is not active, for example, executing "SELECT 1", skip setting the last txn info.
		return
	}

	lastTxnInfo, err := json.Marshal(transaction.TxnInfo{
		TxnScope: txnCtx.TxnScope,
		StartTS:  txnCtx.StartTS,
	})
	terror.Log(err)
	s.GetSessionVars().LastTxnInfo = string(lastTxnInfo)
}

func (s *session) GetClient() kv.Client {
	return s.client
}

func (s *session) GetMPPClient() kv.MPPClient {
	return s.mppClient
}

func (s *session) String() string {
	// TODO: how to print binded context in values appropriately?
	sessVars := s.sessionVars
	data := map[string]any{
		"id":         sessVars.ConnectionID,
		"user":       sessVars.User,
		"currDBName": sessVars.CurrentDB,
		"status":     sessVars.Status(),
		"strictMode": sessVars.SQLMode.HasStrictMode(),
	}
	if s.txn.Valid() {
		// if txn is committed or rolled back, txn is nil.
		data["txn"] = s.txn.String()
	}
	if sessVars.SnapshotTS != 0 {
		data["snapshotTS"] = sessVars.SnapshotTS
	}
	if sessVars.StmtCtx.LastInsertID > 0 {
		data["lastInsertID"] = sessVars.StmtCtx.LastInsertID
	}
	if len(sessVars.PreparedStmts) > 0 {
		data["preparedStmtCount"] = len(sessVars.PreparedStmts)
	}
	b, err := json.MarshalIndent(data, "", "  ")
	terror.Log(errors.Trace(err))
	return string(b)
}

const sqlLogMaxLen = 1024

// SchemaChangedWithoutRetry is used for testing.
var SchemaChangedWithoutRetry uint32

func (s *session) GetSQLLabel() string {
	if s.sessionVars.InRestrictedSQL {
		return metrics.LblInternal
	}
	return metrics.LblGeneral
}

func (s *session) isInternal() bool {
	return s.sessionVars.InRestrictedSQL
}

func (*session) isTxnRetryableError(err error) bool {
	if atomic.LoadUint32(&SchemaChangedWithoutRetry) == 1 {
		return kv.IsTxnRetryableError(err)
	}
	return kv.IsTxnRetryableError(err) || domain.ErrInfoSchemaChanged.Equal(err)
}

func isEndTxnStmt(stmt ast.StmtNode, vars *variable.SessionVars) (bool, error) {
	switch n := stmt.(type) {
	case *ast.RollbackStmt, *ast.CommitStmt:
		return true, nil
	case *ast.ExecuteStmt:
		ps, err := plannercore.GetPreparedStmt(n, vars)
		if err != nil {
			return false, err
		}
		return isEndTxnStmt(ps.PreparedAst.Stmt, vars)
	}
	return false, nil
}

func (s *session) checkTxnAborted(stmt sqlexec.Statement) error {
	if atomic.LoadUint32(&s.GetSessionVars().TxnCtx.LockExpire) == 0 {
		return nil
	}
	// If the transaction is aborted, the following statements do not need to execute, except `commit` and `rollback`,
	// because they are used to finish the aborted transaction.
	if ok, err := isEndTxnStmt(stmt.(*executor.ExecStmt).StmtNode, s.sessionVars); err == nil && ok {
		return nil
	} else if err != nil {
		return err
	}
	return kv.ErrLockExpire
}

func (s *session) retry(ctx context.Context, maxCnt uint) (err error) {
	var retryCnt uint
	defer func() {
		s.sessionVars.RetryInfo.Retrying = false
		// retryCnt only increments on retryable error, so +1 here.
		if s.sessionVars.InRestrictedSQL {
			session_metrics.TransactionRetryInternal.Observe(float64(retryCnt + 1))
		} else {
			session_metrics.TransactionRetryGeneral.Observe(float64(retryCnt + 1))
		}
		s.sessionVars.SetInTxn(false)
		if err != nil {
			s.RollbackTxn(ctx)
		}
		s.txn.changeToInvalid()
	}()

	connID := s.sessionVars.ConnectionID
	s.sessionVars.RetryInfo.Retrying = true
	if atomic.LoadUint32(&s.sessionVars.TxnCtx.ForUpdate) == 1 {
		err = ErrForUpdateCantRetry.GenWithStackByArgs(connID)
		return err
	}

	nh := GetHistory(s)
	var schemaVersion int64
	sessVars := s.GetSessionVars()
	orgStartTS := sessVars.TxnCtx.StartTS
	label := s.GetSQLLabel()
	for {
		if err = s.PrepareTxnCtx(ctx); err != nil {
			return err
		}
		s.sessionVars.RetryInfo.ResetOffset()
		for i, sr := range nh.history {
			st := sr.st
			s.sessionVars.StmtCtx = sr.stmtCtx
			s.sessionVars.StmtCtx.CTEStorageMap = map[int]*executor.CTEStorages{}
			s.sessionVars.StmtCtx.ResetForRetry()
			s.sessionVars.PlanCacheParams.Reset()
			schemaVersion, err = st.RebuildPlan(ctx)
			if err != nil {
				return err
			}

			if retryCnt == 0 {
				// We do not have to log the query every time.
				// We print the queries at the first try only.
				sql := sqlForLog(st.GetTextToLog(false))
				if sessVars.EnableRedactLog != errors.RedactLogEnable {
					sql += redact.String(sessVars.EnableRedactLog, sessVars.PlanCacheParams.String())
				}
				logutil.Logger(ctx).Warn("retrying",
					zap.Int64("schemaVersion", schemaVersion),
					zap.Uint("retryCnt", retryCnt),
					zap.Int("queryNum", i),
					zap.String("sql", sql))
			} else {
				logutil.Logger(ctx).Warn("retrying",
					zap.Int64("schemaVersion", schemaVersion),
					zap.Uint("retryCnt", retryCnt),
					zap.Int("queryNum", i))
			}
			_, digest := s.sessionVars.StmtCtx.SQLDigest()
			s.txn.onStmtStart(digest.String())
			if err = sessiontxn.GetTxnManager(s).OnStmtStart(ctx, st.GetStmtNode()); err == nil {
				_, err = st.Exec(ctx)
			}
			s.txn.onStmtEnd()
			if err != nil {
				s.StmtRollback(ctx, false)
				break
			}
			s.StmtCommit(ctx)
		}
		logutil.Logger(ctx).Warn("transaction association",
			zap.Uint64("retrying txnStartTS", s.GetSessionVars().TxnCtx.StartTS),
			zap.Uint64("original txnStartTS", orgStartTS))
		failpoint.Inject("preCommitHook", func() {
			hook, ok := ctx.Value("__preCommitHook").(func())
			if ok {
				hook()
			}
		})
		if err == nil {
			err = s.doCommit(ctx)
			if err == nil {
				break
			}
		}
		if !s.isTxnRetryableError(err) {
			logutil.Logger(ctx).Warn("sql",
				zap.String("label", label),
				zap.Stringer("session", s),
				zap.Error(err))
			metrics.SessionRetryErrorCounter.WithLabelValues(label, metrics.LblUnretryable).Inc()
			return err
		}
		retryCnt++
		if retryCnt >= maxCnt {
			logutil.Logger(ctx).Warn("sql",
				zap.String("label", label),
				zap.Uint("retry reached max count", retryCnt))
			metrics.SessionRetryErrorCounter.WithLabelValues(label, metrics.LblReachMax).Inc()
			return err
		}
		logutil.Logger(ctx).Warn("sql",
			zap.String("label", label),
			zap.Error(err),
			zap.String("txn", s.txn.GoString()))
		kv.BackOff(retryCnt)
		s.txn.changeToInvalid()
		s.sessionVars.SetInTxn(false)
	}
	return err
}

func sqlForLog(sql string) string {
	if len(sql) > sqlLogMaxLen {
		sql = sql[:sqlLogMaxLen] + fmt.Sprintf("(len:%d)", len(sql))
	}
	return executor.QueryReplacer.Replace(sql)
}

func (s *session) sysSessionPool() util.SessionPool {
	return domain.GetDomain(s).SysSessionPool()
}

func getSessionFactory(store kv.Storage) pools.Factory {
	facWithDom := getSessionFactoryInternal(store, func(store kv.Storage, _ *domain.Domain) (*session, error) {
		return createSession(store)
	})
	return func() (pools.Resource, error) {
		return facWithDom(nil)
	}
}

func getSessionFactoryWithDom(store kv.Storage) func(*domain.Domain) (pools.Resource, error) {
	return getSessionFactoryInternal(store, CreateSessionWithDomain)
}

func getCrossKSSessionFactory(currKSStore kv.Storage, targetKS string) pools.Factory {
	facWithDom := getSessionFactoryInternal(currKSStore, func(store kv.Storage, _ *domain.Domain) (*session, error) {
		return createCrossKSSession(store, targetKS)
	})
	return func() (pools.Resource, error) {
		return facWithDom(nil)
	}
}

func getSessionFactoryInternal(store kv.Storage, createSessFn func(store kv.Storage, dom *domain.Domain) (*session, error)) func(*domain.Domain) (pools.Resource, error) {
	return func(dom *domain.Domain) (pools.Resource, error) {
		se, err := createSessFn(store, dom)
		if err != nil {
			return nil, err
		}
		err = se.sessionVars.SetSystemVar(vardef.AutoCommit, "1")
		if err != nil {
			return nil, err
		}
		err = se.sessionVars.SetSystemVar(vardef.MaxExecutionTime, "0")
		if err != nil {
			return nil, errors.Trace(err)
		}
		err = se.sessionVars.SetSystemVar(vardef.MaxAllowedPacket, strconv.FormatUint(vardef.DefMaxAllowedPacket, 10))
		if err != nil {
			return nil, errors.Trace(err)
		}
		err = se.sessionVars.SetSystemVar(vardef.TiDBConstraintCheckInPlacePessimistic, vardef.On)
		if err != nil {
			return nil, errors.Trace(err)
		}
		se.sessionVars.CommonGlobalLoaded = true
		se.sessionVars.InRestrictedSQL = true
		// Internal session uses default format to prevent memory leak problem.
		se.sessionVars.EnableChunkRPC = false
		return se, nil
	}
}

func drainRecordSet(ctx context.Context, se *session, rs sqlexec.RecordSet, alloc chunk.Allocator) ([]chunk.Row, error) {
	var rows []chunk.Row
	var req *chunk.Chunk
	req = rs.NewChunk(alloc)
	for {
		err := rs.Next(ctx, req)
		if err != nil || req.NumRows() == 0 {
			return rows, err
		}
		iter := chunk.NewIterator4Chunk(req)
		for r := iter.Begin(); r != iter.End(); r = iter.Next() {
			rows = append(rows, r)
		}
		req = chunk.Renew(req, se.sessionVars.MaxChunkSize)
	}
}

// getTableValue executes restricted sql and the result is one column.
// It returns a string value.
func (s *session) getTableValue(ctx context.Context, tblName string, varName string) (string, error) {
	if ctx.Value(kv.RequestSourceKey) == nil {
		ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnSysVar)
	}
	rows, fields, err := s.ExecRestrictedSQL(ctx, nil, "SELECT VARIABLE_VALUE FROM %n.%n WHERE VARIABLE_NAME=%?", mysql.SystemDB, tblName, varName)
	if err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "", errResultIsEmpty
	}
	d := rows[0].GetDatum(0, &fields[0].Column.FieldType)
	value, err := d.ToString()
	if err != nil {
		return "", err
	}
	return value, nil
}

// replaceGlobalVariablesTableValue executes restricted sql updates the variable value
// It will then notify the etcd channel that the value has changed.
func (s *session) replaceGlobalVariablesTableValue(ctx context.Context, varName, val string, updateLocal bool) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnSysVar)
	_, _, err := s.ExecRestrictedSQL(ctx, nil, `REPLACE INTO %n.%n (variable_name, variable_value) VALUES (%?, %?)`, mysql.SystemDB, mysql.GlobalVariablesTable, varName, val)
	if err != nil {
		return err
	}
	domain.GetDomain(s).NotifyUpdateSysVarCache(updateLocal)
	return err
}

// GetGlobalSysVar implements GlobalVarAccessor.GetGlobalSysVar interface.
func (s *session) GetGlobalSysVar(name string) (string, error) {
	if s.Value(sessionctx.Initing) != nil {
		// When running bootstrap or upgrade, we should not access global storage.
		return "", nil
	}

	sv := variable.GetSysVar(name)
	if sv == nil {
		// It might be a recently unregistered sysvar. We should return unknown
		// since GetSysVar is the canonical version, but we can update the cache
		// so the next request doesn't attempt to load this.
		logutil.BgLogger().Info("sysvar does not exist. sysvar cache may be stale", zap.String("name", name))
		return "", variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}

	sysVar, err := domain.GetDomain(s).GetGlobalVar(name)
	if err != nil {
		// The sysvar exists, but there is no cache entry yet.
		// This might be because the sysvar was only recently registered.
		// In which case it is safe to return the default, but we can also
		// update the cache for the future.
		logutil.BgLogger().Info("sysvar not in cache yet. sysvar cache may be stale", zap.String("name", name))
		sysVar, err = s.getTableValue(context.TODO(), mysql.GlobalVariablesTable, name)
		if err != nil {
			return sv.Value, nil
		}
	}
	// It might have been written from an earlier TiDB version, so we should do type validation
	// See https://github.com/pingcap/tidb/issues/30255 for why we don't do full validation.
	// If validation fails, we should return the default value:
	// See: https://github.com/pingcap/tidb/pull/31566
	sysVar, err = sv.ValidateFromType(s.GetSessionVars(), sysVar, vardef.ScopeGlobal)
	if err != nil {
		return sv.Value, nil
	}
	return sysVar, nil
}

// SetGlobalSysVar implements GlobalVarAccessor.SetGlobalSysVar interface.
// it is called (but skipped) when setting instance scope
func (s *session) SetGlobalSysVar(ctx context.Context, name string, value string) (err error) {
	sv := variable.GetSysVar(name)
	if sv == nil {
		return variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	if value, err = sv.Validate(s.sessionVars, value, vardef.ScopeGlobal); err != nil {
		return err
	}
	if err = sv.SetGlobalFromHook(ctx, s.sessionVars, value, false); err != nil {
		return err
	}
	if sv.HasInstanceScope() { // skip for INSTANCE scope
		return nil
	}
	if sv.GlobalConfigName != "" {
		domain.GetDomain(s).NotifyGlobalConfigChange(sv.GlobalConfigName, variable.OnOffToTrueFalse(value))
	}
	return s.replaceGlobalVariablesTableValue(context.TODO(), sv.Name, value, true)
}

// SetGlobalSysVarOnly updates the sysvar, but does not call the validation function or update aliases.
// This is helpful to prevent duplicate warnings being appended from aliases, or recursion.
// updateLocal indicates whether to rebuild the local SysVar Cache. This is helpful to prevent recursion.
func (s *session) SetGlobalSysVarOnly(ctx context.Context, name string, value string, updateLocal bool) (err error) {
	sv := variable.GetSysVar(name)
	if sv == nil {
		return variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	if err = sv.SetGlobalFromHook(ctx, s.sessionVars, value, true); err != nil {
		return err
	}
	if sv.HasInstanceScope() { // skip for INSTANCE scope
		return nil
	}
	return s.replaceGlobalVariablesTableValue(ctx, sv.Name, value, updateLocal)
}

// SetTiDBTableValue implements GlobalVarAccessor.SetTiDBTableValue interface.
func (s *session) SetTiDBTableValue(name, value, comment string) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnSysVar)
	_, _, err := s.ExecRestrictedSQL(ctx, nil, `REPLACE INTO mysql.tidb (variable_name, variable_value, comment) VALUES (%?, %?, %?)`, name, value, comment)
	return err
}

// GetTiDBTableValue implements GlobalVarAccessor.GetTiDBTableValue interface.
func (s *session) GetTiDBTableValue(name string) (string, error) {
	return s.getTableValue(context.TODO(), mysql.TiDBTable, name)
}

var _ sqlexec.SQLParser = &session{}

func (s *session) ParseSQL(ctx context.Context, sql string, params ...parser.ParseParam) ([]ast.StmtNode, []error, error) {
	defer tracing.StartRegion(ctx, "ParseSQL").End()
	p := parserutil.GetParser()
	defer func() {
		parserutil.DestroyParser(p)
	}()

	sqlMode := s.sessionVars.SQLMode
	if s.isInternal() {
		sqlMode = mysql.DelSQLMode(sqlMode, mysql.ModeNoBackslashEscapes)
	}
	p.SetSQLMode(sqlMode)
	p.SetParserConfig(s.sessionVars.BuildParserConfig())
	tmp, warn, err := p.ParseSQL(sql, params...)
	// The []ast.StmtNode is referenced by the parser, to reuse the parser, make a copy of the result.
	res := make([]ast.StmtNode, len(tmp))
	copy(res, tmp)
	return res, warn, err
}

func (s *session) SetProcessInfo(sql string, t time.Time, command byte, maxExecutionTime uint64) {
	// If command == mysql.ComSleep, it means the SQL execution is finished. The processinfo is reset to SLEEP.
	// If the SQL finished and the session is not in transaction, the current start timestamp need to reset to 0.
	// Otherwise, it should be set to the transaction start timestamp.
	// Why not reset the transaction start timestamp to 0 when transaction committed?
	// Because the select statement and other statements need this timestamp to read data,
	// after the transaction is committed. e.g. SHOW MASTER STATUS;
	var curTxnStartTS uint64
	var curTxnCreateTime time.Time
	if command != mysql.ComSleep || s.GetSessionVars().InTxn() {
		curTxnStartTS = s.sessionVars.TxnCtx.StartTS
		curTxnCreateTime = s.sessionVars.TxnCtx.CreateTime

		// For stale read and autocommit path, the `TxnCtx.StartTS` is 0.
		if curTxnStartTS == 0 {
			curTxnStartTS = s.sessionVars.TxnCtx.StaleReadTs
		}
	}
	// Set curTxnStartTS to SnapshotTS directly when the session is trying to historic read.
	// It will avoid the session meet GC lifetime too short error.
	if s.GetSessionVars().SnapshotTS != 0 {
		curTxnStartTS = s.GetSessionVars().SnapshotTS
	}
	p := s.currentPlan
	if explain, ok := p.(*plannercore.Explain); ok && explain.Analyze && explain.TargetPlan != nil {
		p = explain.TargetPlan
	}

	sqlCPUUsages := &s.sessionVars.SQLCPUUsages
	// If command == mysql.ComSleep, it means the SQL execution is finished. Then cpu usages should be nil.
	if command == mysql.ComSleep {
		sqlCPUUsages = nil
	}

	pi := util.ProcessInfo{
		ID:                    s.sessionVars.ConnectionID,
		Port:                  s.sessionVars.Port,
		DB:                    s.sessionVars.CurrentDB,
		Command:               command,
		Plan:                  p,
		BriefBinaryPlan:       plannercore.GetBriefBinaryPlan(p),
		RuntimeStatsColl:      s.sessionVars.StmtCtx.RuntimeStatsColl,
		Time:                  t,
		State:                 s.Status(),
		Info:                  sql,
		CurTxnStartTS:         curTxnStartTS,
		CurTxnCreateTime:      curTxnCreateTime,
		StmtCtx:               s.sessionVars.StmtCtx,
		SQLCPUUsage:           sqlCPUUsages,
		RefCountOfStmtCtx:     &s.sessionVars.RefCountOfStmtCtx,
		MemTracker:            s.sessionVars.MemTracker,
		DiskTracker:           s.sessionVars.DiskTracker,
		RunawayChecker:        s.sessionVars.StmtCtx.RunawayChecker,
		StatsInfo:             plannercore.GetStatsInfo,
		OOMAlarmVariablesInfo: s.getOomAlarmVariablesInfo(),
		TableIDs:              s.sessionVars.StmtCtx.TableIDs,
		IndexNames:            s.sessionVars.StmtCtx.IndexNames,
		MaxExecutionTime:      maxExecutionTime,
		RedactSQL:             s.sessionVars.EnableRedactLog,
		ResourceGroupName:     s.sessionVars.StmtCtx.ResourceGroupName,
		SessionAlias:          s.sessionVars.SessionAlias,
		CursorTracker:         s.cursorTracker,
	}
	oldPi := s.ShowProcess()
	if p == nil {
		// Store the last valid plan when the current plan is nil.
		// This is for `explain for connection` statement has the ability to query the last valid plan.
		if oldPi != nil && oldPi.Plan != nil && len(oldPi.BriefBinaryPlan) > 0 {
			pi.Plan = oldPi.Plan
			pi.RuntimeStatsColl = oldPi.RuntimeStatsColl
			pi.BriefBinaryPlan = oldPi.BriefBinaryPlan
		}
	}
	// We set process info before building plan, so we extended execution time.
	if oldPi != nil && oldPi.Info == pi.Info && oldPi.Command == pi.Command {
		pi.Time = oldPi.Time
	}
	if oldPi != nil && oldPi.CurTxnStartTS != 0 && oldPi.CurTxnStartTS == pi.CurTxnStartTS {
		// Keep the last expensive txn log time, avoid print too many expensive txn logs.
		pi.ExpensiveTxnLogTime = oldPi.ExpensiveTxnLogTime
	}
	_, digest := s.sessionVars.StmtCtx.SQLDigest()
	pi.Digest = digest.String()
	// DO NOT reset the currentPlan to nil until this query finishes execution, otherwise reentrant calls
	// of SetProcessInfo would override Plan and BriefBinaryPlan to nil.
	if command == mysql.ComSleep {
		s.currentPlan = nil
	}
	if s.sessionVars.User != nil {
		pi.User = s.sessionVars.User.Username
		pi.Host = s.sessionVars.User.Hostname
	}
	s.processInfo.Store(&pi)
}

// UpdateProcessInfo updates the session's process info for the running statement.
func (s *session) UpdateProcessInfo() {
	pi := s.ShowProcess()
	if pi == nil || pi.CurTxnStartTS != 0 {
		return
	}
	// do not modify this two fields in place, see issue: issues/50607
	shallowCP := pi.Clone()
	// Update the current transaction start timestamp.
	shallowCP.CurTxnStartTS = s.sessionVars.TxnCtx.StartTS
	if shallowCP.CurTxnStartTS == 0 {
		// For stale read and autocommit path, the `TxnCtx.StartTS` is 0.
		shallowCP.CurTxnStartTS = s.sessionVars.TxnCtx.StaleReadTs
	}
	shallowCP.CurTxnCreateTime = s.sessionVars.TxnCtx.CreateTime
	s.processInfo.Store(shallowCP)
}

func (s *session) getOomAlarmVariablesInfo() util.OOMAlarmVariablesInfo {
	return util.OOMAlarmVariablesInfo{
		SessionAnalyzeVersion:         s.sessionVars.AnalyzeVersion,
		SessionEnabledRateLimitAction: s.sessionVars.EnabledRateLimitAction,
		SessionMemQuotaQuery:          s.sessionVars.MemQuotaQuery,
	}
}

func (s *session) ExecuteInternal(ctx context.Context, sql string, args ...any) (rs sqlexec.RecordSet, err error) {
	origin := s.sessionVars.InRestrictedSQL
	s.sessionVars.InRestrictedSQL = true
	defer func() {
		s.sessionVars.InRestrictedSQL = origin
		// Restore the goroutine label by using the original ctx after execution is finished.
		pprof.SetGoroutineLabels(ctx)
	}()

	r, ctx := tracing.StartRegionEx(ctx, "session.ExecuteInternal")
	defer r.End()
	logutil.Eventf(ctx, "execute: %s", sql)

	stmtNode, err := s.ParseWithParams(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	rs, err = s.ExecuteStmt(ctx, stmtNode)
	if err != nil {
		s.sessionVars.StmtCtx.AppendError(err)
	}
	if rs == nil {
		return nil, err
	}

	return rs, err
}

// Execute is deprecated, we can remove it as soon as plugins are migrated.
func (s *session) Execute(ctx context.Context, sql string) (recordSets []sqlexec.RecordSet, err error) {
	r, ctx := tracing.StartRegionEx(ctx, "session.Execute")
	defer r.End()
	logutil.Eventf(ctx, "execute: %s", sql)

	stmtNodes, err := s.Parse(ctx, sql)
	if err != nil {
		return nil, err
	}
	if len(stmtNodes) != 1 {
		return nil, errors.New("Execute() API doesn't support multiple statements any more")
	}

	rs, err := s.ExecuteStmt(ctx, stmtNodes[0])
	if err != nil {
		s.sessionVars.StmtCtx.AppendError(err)
	}
	if rs == nil {
		return nil, err
	}
	return []sqlexec.RecordSet{rs}, err
}

// Parse parses a query string to raw ast.StmtNode.
func (s *session) Parse(ctx context.Context, sql string) ([]ast.StmtNode, error) {
	logutil.Logger(ctx).Debug("parse", zap.String("sql", sql))
	parseStartTime := time.Now()

	// Load the session variables to the context.
	// This is necessary for the parser to get the current sql_mode.
	if err := s.loadCommonGlobalVariablesIfNeeded(); err != nil {
		return nil, err
	}

	stmts, warns, err := s.ParseSQL(ctx, sql, s.sessionVars.GetParseParams()...)
	if err != nil {
		s.rollbackOnError(ctx)
		err = util.SyntaxError(err)

		// Only print log message when this SQL is from the user.
		// Mute the warning for internal SQLs.
		if !s.sessionVars.InRestrictedSQL {
			logutil.Logger(ctx).Warn("parse SQL failed", zap.Error(err), zap.String("SQL", redact.String(s.sessionVars.EnableRedactLog, sql)))
			s.sessionVars.StmtCtx.AppendError(err)
		}
		return nil, err
	}

	durParse := time.Since(parseStartTime)
	s.GetSessionVars().DurationParse = durParse
	isInternal := s.isInternal()
	if isInternal {
		session_metrics.SessionExecuteParseDurationInternal.Observe(durParse.Seconds())
	} else {
		session_metrics.SessionExecuteParseDurationGeneral.Observe(durParse.Seconds())
	}
	for _, warn := range warns {
		s.sessionVars.StmtCtx.AppendWarning(util.SyntaxWarn(warn))
	}
	return stmts, nil
}

// ParseWithParams parses a query string, with arguments, to raw ast.StmtNode.
// Note that it will not do escaping if no variable arguments are passed.
func (s *session) ParseWithParams(ctx context.Context, sql string, args ...any) (ast.StmtNode, error) {
	var err error
	if len(args) > 0 {
		sql, err = sqlescape.EscapeSQL(sql, args...)
		if err != nil {
			return nil, err
		}
	}

	internal := s.isInternal()

	var stmts []ast.StmtNode
	var warns []error
	parseStartTime := time.Now()
	if internal {
		// Do no respect the settings from clients, if it is for internal usage.
		// Charsets from clients may give chance injections.
		// Refer to https://stackoverflow.com/questions/5741187/sql-injection-that-gets-around-mysql-real-escape-string/12118602.
		stmts, warns, err = s.ParseSQL(ctx, sql)
	} else {
		stmts, warns, err = s.ParseSQL(ctx, sql, s.sessionVars.GetParseParams()...)
	}
	if len(stmts) != 1 && err == nil {
		err = errors.New("run multiple statements internally is not supported")
	}
	if err != nil {
		s.rollbackOnError(ctx)
		logSQL := sql[:min(500, len(sql))]
		logutil.Logger(ctx).Warn("parse SQL failed", zap.Error(err), zap.String("SQL", redact.String(s.sessionVars.EnableRedactLog, logSQL)))
		return nil, util.SyntaxError(err)
	}
	durParse := time.Since(parseStartTime)
	if internal {
		session_metrics.SessionExecuteParseDurationInternal.Observe(durParse.Seconds())
	} else {
		session_metrics.SessionExecuteParseDurationGeneral.Observe(durParse.Seconds())
	}
	for _, warn := range warns {
		s.sessionVars.StmtCtx.AppendWarning(util.SyntaxWarn(warn))
	}
	if topsqlstate.TopSQLEnabled() {
		normalized, digest := parser.NormalizeDigest(sql)
		if digest != nil {
			// Reset the goroutine label when internal sql execute finish.
			// Specifically reset in ExecRestrictedStmt function.
			s.sessionVars.StmtCtx.IsSQLRegistered.Store(true)
			topsql.AttachAndRegisterSQLInfo(ctx, normalized, digest, s.sessionVars.InRestrictedSQL)
		}
	}
	return stmts[0], nil
}

// GetAdvisoryLock acquires an advisory lock of lockName.
// Note that a lock can be acquired multiple times by the same session,
// in which case we increment a reference count.
// Each lock needs to be held in a unique session because
// we need to be able to ROLLBACK in any arbitrary order
// in order to release the locks.
func (s *session) GetAdvisoryLock(lockName string, timeout int64) error {
	if lock, ok := s.advisoryLocks[lockName]; ok {
		lock.IncrReferences()
		return nil
	}
	sess, err := createSession(s.store)
	if err != nil {
		return err
	}
	infosync.StoreInternalSession(sess)
	lock := &advisoryLock{session: sess, ctx: context.TODO(), owner: s.ShowProcess().ID}
	err = lock.GetLock(lockName, timeout)
	if err != nil {
		return err
	}
	s.advisoryLocks[lockName] = lock
	return nil
}

// IsUsedAdvisoryLock checks if a lockName is already in use
func (s *session) IsUsedAdvisoryLock(lockName string) uint64 {
	// Same session
	if lock, ok := s.advisoryLocks[lockName]; ok {
		return lock.owner
	}

	// Check for transaction on advisory_locks table
	sess, err := createSession(s.store)
	if err != nil {
		return 0
	}
	lock := &advisoryLock{session: sess, ctx: context.TODO(), owner: s.ShowProcess().ID}
	err = lock.IsUsedLock(lockName)
	if err != nil {
		// TODO: Return actual owner pid
		// TODO: Check for mysql.ErrLockWaitTimeout and DeadLock
		return 1
	}
	return 0
}

// ReleaseAdvisoryLock releases an advisory locks held by the session.
// It returns FALSE if no lock by this name was held (by this session),
// and TRUE if a lock was held and "released".
// Note that the lock is not actually released if there are multiple
// references to the same lockName by the session, instead the reference
// count is decremented.
func (s *session) ReleaseAdvisoryLock(lockName string) (released bool) {
	if lock, ok := s.advisoryLocks[lockName]; ok {
		lock.DecrReferences()
		if lock.ReferenceCount() <= 0 {
			lock.Close()
			delete(s.advisoryLocks, lockName)
			infosync.DeleteInternalSession(lock.session)
		}
		return true
	}
	return false
}

// ReleaseAllAdvisoryLocks releases all advisory locks held by the session
// and returns a count of the locks that were released.
// The count is based on unique locks held, so multiple references
// to the same lock do not need to be accounted for.
func (s *session) ReleaseAllAdvisoryLocks() int {
	var count int
	for lockName, lock := range s.advisoryLocks {
		lock.Close()
		count += lock.ReferenceCount()
		delete(s.advisoryLocks, lockName)
		infosync.DeleteInternalSession(lock.session)
	}
	return count
}

// GetExtensions returns the `*extension.SessionExtensions` object
func (s *session) GetExtensions() *extension.SessionExtensions {
	return s.extensions
}

// SetExtensions sets the `*extension.SessionExtensions` object
func (s *session) SetExtensions(extensions *extension.SessionExtensions) {
	s.extensions = extensions
}

// InSandBoxMode indicates that this session is in sandbox mode
func (s *session) InSandBoxMode() bool {
	return s.sandBoxMode
}

// EnableSandBoxMode enable the sandbox mode.
func (s *session) EnableSandBoxMode() {
	s.sandBoxMode = true
}

// DisableSandBoxMode enable the sandbox mode.
func (s *session) DisableSandBoxMode() {
	s.sandBoxMode = false
}

// ParseWithParams4Test wrapper (s *session) ParseWithParams for test
func ParseWithParams4Test(ctx context.Context, s types.Session,
	sql string, args ...any) (ast.StmtNode, error) {
	return s.(*session).ParseWithParams(ctx, sql, args)
}

var _ sqlexec.RestrictedSQLExecutor = &session{}
var _ sqlexec.SQLExecutor = &session{}

// ExecRestrictedStmt implements RestrictedSQLExecutor interface.
func (s *session) ExecRestrictedStmt(ctx context.Context, stmtNode ast.StmtNode, opts ...sqlexec.OptionFuncAlias) (
	[]chunk.Row, []*resolve.ResultField, error) {
	defer pprof.SetGoroutineLabels(ctx)
	execOption := sqlexec.GetExecOption(opts)
	var se *session
	var clean func()
	var err error
	if execOption.UseCurSession {
		se, clean, err = s.useCurrentSession(execOption)
	} else {
		se, clean, err = s.getInternalSession(execOption)
	}
	if err != nil {
		return nil, nil, err
	}
	defer clean()

	startTime := time.Now()
	metrics.SessionRestrictedSQLCounter.Inc()
	ctx = execdetails.ContextWithInitializedExecDetails(ctx)
	rs, err := se.ExecuteStmt(ctx, stmtNode)
	if err != nil {
		se.sessionVars.StmtCtx.AppendError(err)
	}
	if rs == nil {
		return nil, nil, err
	}
	defer func() {
		if closeErr := rs.Close(); closeErr != nil {
			err = closeErr
		}
	}()
	var rows []chunk.Row
	rows, err = drainRecordSet(ctx, se, rs, nil)
	if err != nil {
		return nil, nil, err
	}

	vars := se.GetSessionVars()
	for _, dbName := range GetDBNames(vars) {
		metrics.QueryDurationHistogram.WithLabelValues(metrics.LblInternal, dbName, vars.StmtCtx.ResourceGroupName).Observe(time.Since(startTime).Seconds())
	}
	return rows, rs.Fields(), err
}

// ExecRestrictedStmt4Test wrapper `(s *session) ExecRestrictedStmt` for test.
func ExecRestrictedStmt4Test(ctx context.Context, s types.Session,
	stmtNode ast.StmtNode, opts ...sqlexec.OptionFuncAlias) (
	[]chunk.Row, []*resolve.ResultField, error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	return s.(*session).ExecRestrictedStmt(ctx, stmtNode, opts...)
}

// only set and clean session with execOption
func (s *session) useCurrentSession(execOption sqlexec.ExecOption) (*session, func(), error) {
	var err error
	orgSnapshotInfoSchema, orgSnapshotTS := s.sessionVars.SnapshotInfoschema, s.sessionVars.SnapshotTS
	if execOption.SnapshotTS != 0 {
		if err = s.sessionVars.SetSystemVar(vardef.TiDBSnapshot, strconv.FormatUint(execOption.SnapshotTS, 10)); err != nil {
			return nil, nil, err
		}
		s.sessionVars.SnapshotInfoschema, err = getSnapshotInfoSchema(s, execOption.SnapshotTS)
		if err != nil {
			return nil, nil, err
		}
	}
	prevStatsVer := s.sessionVars.AnalyzeVersion
	if execOption.AnalyzeVer != 0 {
		s.sessionVars.AnalyzeVersion = execOption.AnalyzeVer
	}
	prevAnalyzeSnapshot := s.sessionVars.EnableAnalyzeSnapshot
	if execOption.AnalyzeSnapshot != nil {
		s.sessionVars.EnableAnalyzeSnapshot = *execOption.AnalyzeSnapshot
	}
	prePruneMode := s.sessionVars.PartitionPruneMode.Load()
	if len(execOption.PartitionPruneMode) > 0 {
		s.sessionVars.PartitionPruneMode.Store(execOption.PartitionPruneMode)
	}
	prevSQL := s.sessionVars.StmtCtx.OriginalSQL
	prevStmtType := s.sessionVars.StmtCtx.StmtType
	prevTables := s.sessionVars.StmtCtx.Tables
	return s, func() {
		s.sessionVars.AnalyzeVersion = prevStatsVer
		s.sessionVars.EnableAnalyzeSnapshot = prevAnalyzeSnapshot
		if err := s.sessionVars.SetSystemVar(vardef.TiDBSnapshot, ""); err != nil {
			logutil.BgLogger().Error("set tidbSnapshot error", zap.Error(err))
		}
		s.sessionVars.SnapshotInfoschema = orgSnapshotInfoSchema
		s.sessionVars.SnapshotTS = orgSnapshotTS
		s.sessionVars.PartitionPruneMode.Store(prePruneMode)
		s.sessionVars.StmtCtx.OriginalSQL = prevSQL
		s.sessionVars.StmtCtx.StmtType = prevStmtType
		s.sessionVars.StmtCtx.Tables = prevTables
		s.sessionVars.MemTracker.Detach()
	}, nil
}

func (s *session) getInternalSession(execOption sqlexec.ExecOption) (*session, func(), error) {
	tmp, err := s.sysSessionPool().Get()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	se := tmp.(*session)

	// The special session will share the `InspectionTableCache` with current session
	// if the current session in inspection mode.
	if cache := s.sessionVars.InspectionTableCache; cache != nil {
		se.sessionVars.InspectionTableCache = cache
	}
	se.sessionVars.OptimizerUseInvisibleIndexes = s.sessionVars.OptimizerUseInvisibleIndexes

	preSkipStats := s.sessionVars.SkipMissingPartitionStats
	se.sessionVars.SkipMissingPartitionStats = s.sessionVars.SkipMissingPartitionStats

	if execOption.SnapshotTS != 0 {
		if err := se.sessionVars.SetSystemVar(vardef.TiDBSnapshot, strconv.FormatUint(execOption.SnapshotTS, 10)); err != nil {
			return nil, nil, err
		}
		se.sessionVars.SnapshotInfoschema, err = getSnapshotInfoSchema(s, execOption.SnapshotTS)
		if err != nil {
			return nil, nil, err
		}
	}

	prevStatsVer := se.sessionVars.AnalyzeVersion
	if execOption.AnalyzeVer != 0 {
		se.sessionVars.AnalyzeVersion = execOption.AnalyzeVer
	}

	prevAnalyzeSnapshot := se.sessionVars.EnableAnalyzeSnapshot
	if execOption.AnalyzeSnapshot != nil {
		se.sessionVars.EnableAnalyzeSnapshot = *execOption.AnalyzeSnapshot
	}

	prePruneMode := se.sessionVars.PartitionPruneMode.Load()
	if len(execOption.PartitionPruneMode) > 0 {
		se.sessionVars.PartitionPruneMode.Store(execOption.PartitionPruneMode)
	}

	return se, func() {
		se.sessionVars.AnalyzeVersion = prevStatsVer
		se.sessionVars.EnableAnalyzeSnapshot = prevAnalyzeSnapshot
		if err := se.sessionVars.SetSystemVar(vardef.TiDBSnapshot, ""); err != nil {
			logutil.BgLogger().Error("set tidbSnapshot error", zap.Error(err))
		}
		se.sessionVars.SnapshotInfoschema = nil
		se.sessionVars.SnapshotTS = 0
		if !execOption.IgnoreWarning {
			if se != nil && se.GetSessionVars().StmtCtx.WarningCount() > 0 {
				warnings := se.GetSessionVars().StmtCtx.GetWarnings()
				s.GetSessionVars().StmtCtx.AppendWarnings(warnings)
			}
		}
		se.sessionVars.PartitionPruneMode.Store(prePruneMode)
		se.sessionVars.OptimizerUseInvisibleIndexes = false
		se.sessionVars.SkipMissingPartitionStats = preSkipStats
		se.sessionVars.InspectionTableCache = nil
		se.sessionVars.MemTracker.Detach()
		s.sysSessionPool().Put(tmp)
	}, nil
}

func (s *session) withRestrictedSQLExecutor(ctx context.Context, opts []sqlexec.OptionFuncAlias, fn func(context.Context, *session) ([]chunk.Row, []*resolve.ResultField, error)) ([]chunk.Row, []*resolve.ResultField, error) {
	execOption := sqlexec.GetExecOption(opts)
	var se *session
	var clean func()
	var err error
	if execOption.UseCurSession {
		se, clean, err = s.useCurrentSession(execOption)
	} else {
		se, clean, err = s.getInternalSession(execOption)
	}
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	defer clean()
	if execOption.TrackSysProcID > 0 {
		err = execOption.TrackSysProc(execOption.TrackSysProcID, se)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		// unTrack should be called before clean (return sys session)
		defer execOption.UnTrackSysProc(execOption.TrackSysProcID)
	}
	return fn(ctx, se)
}

func (s *session) ExecRestrictedSQL(ctx context.Context, opts []sqlexec.OptionFuncAlias, sql string, params ...any) ([]chunk.Row, []*resolve.ResultField, error) {
	return s.withRestrictedSQLExecutor(ctx, opts, func(ctx context.Context, se *session) ([]chunk.Row, []*resolve.ResultField, error) {
		stmt, err := se.ParseWithParams(ctx, sql, params...)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		defer pprof.SetGoroutineLabels(ctx)
		startTime := time.Now()
		metrics.SessionRestrictedSQLCounter.Inc()
		ctx = execdetails.ContextWithInitializedExecDetails(ctx)
		rs, err := se.ExecuteInternalStmt(ctx, stmt)
		if err != nil {
			se.sessionVars.StmtCtx.AppendError(err)
		}
		if rs == nil {
			return nil, nil, err
		}
		defer func() {
			if closeErr := rs.Close(); closeErr != nil {
				err = closeErr
			}
		}()
		var rows []chunk.Row
		rows, err = drainRecordSet(ctx, se, rs, nil)
		if err != nil {
			return nil, nil, err
		}

		vars := se.GetSessionVars()
		for _, dbName := range GetDBNames(vars) {
			metrics.QueryDurationHistogram.WithLabelValues(metrics.LblInternal, dbName, vars.StmtCtx.ResourceGroupName).Observe(time.Since(startTime).Seconds())
		}
		return rows, rs.Fields(), err
	})
}

// ExecuteInternalStmt execute internal stmt
func (s *session) ExecuteInternalStmt(ctx context.Context, stmtNode ast.StmtNode) (sqlexec.RecordSet, error) {
	origin := s.sessionVars.InRestrictedSQL
	s.sessionVars.InRestrictedSQL = true
	defer func() {
		s.sessionVars.InRestrictedSQL = origin
		// Restore the goroutine label by using the original ctx after execution is finished.
		pprof.SetGoroutineLabels(ctx)
	}()
	return s.ExecuteStmt(ctx, stmtNode)
}

func (s *session) ExecuteStmt(ctx context.Context, stmtNode ast.StmtNode) (sqlexec.RecordSet, error) {
	r, ctx := tracing.StartRegionEx(ctx, "session.ExecuteStmt")
	defer r.End()

	if err := s.PrepareTxnCtx(ctx); err != nil {
		return nil, err
	}
	if err := s.loadCommonGlobalVariablesIfNeeded(); err != nil {
		return nil, err
	}

	sessVars := s.sessionVars
	sessVars.StartTime = time.Now()

	// Some executions are done in compile stage, so we reset them before compile.
	if err := executor.ResetContextOfStmt(s, stmtNode); err != nil {
		return nil, err
	}
	if execStmt, ok := stmtNode.(*ast.ExecuteStmt); ok {
		if binParam, ok := execStmt.BinaryArgs.([]param.BinaryParam); ok {
			args, err := expression.ExecBinaryParam(s.GetSessionVars().StmtCtx.TypeCtx(), binParam)
			if err != nil {
				return nil, err
			}
			execStmt.BinaryArgs = args
		}
	}

	normalizedSQL, digest := s.sessionVars.StmtCtx.SQLDigest()
	cmdByte := byte(atomic.LoadUint32(&s.GetSessionVars().CommandValue))
	if topsqlstate.TopSQLEnabled() {
		s.sessionVars.StmtCtx.IsSQLRegistered.Store(true)
		ctx = topsql.AttachAndRegisterSQLInfo(ctx, normalizedSQL, digest, s.sessionVars.InRestrictedSQL)
	}
	if sessVars.InPlanReplayer {
		sessVars.StmtCtx.EnableOptimizerDebugTrace = true
	} else if dom := domain.GetDomain(s); dom != nil && !sessVars.InRestrictedSQL {
		// This is the earliest place we can get the SQL digest for this execution.
		// If we find this digest is registered for PLAN REPLAYER CAPTURE, we need to enable optimizer debug trace no matter
		// the plan digest will be matched or not.
		if planReplayerHandle := dom.GetPlanReplayerHandle(); planReplayerHandle != nil {
			tasks := planReplayerHandle.GetTasks()
			for _, task := range tasks {
				if task.SQLDigest == digest.String() {
					sessVars.StmtCtx.EnableOptimizerDebugTrace = true
				}
			}
		}
	}
	if sessVars.StmtCtx.EnableOptimizerDebugTrace {
		plannercore.DebugTraceReceivedCommand(s.pctx, cmdByte, stmtNode)
	}

	if err := s.validateStatementInTxn(stmtNode); err != nil {
		return nil, err
	}

	if err := s.validateStatementReadOnlyInStaleness(stmtNode); err != nil {
		return nil, err
	}

	// Uncorrelated subqueries will execute once when building plan, so we reset process info before building plan.
	s.currentPlan = nil // reset current plan
	s.SetProcessInfo(stmtNode.Text(), time.Now(), cmdByte, 0)
	s.txn.onStmtStart(digest.String())
	defer sessiontxn.GetTxnManager(s).OnStmtEnd()
	defer s.txn.onStmtEnd()

	if err := s.onTxnManagerStmtStartOrRetry(ctx, stmtNode); err != nil {
		return nil, err
	}

	failpoint.Inject("mockStmtSlow", func(val failpoint.Value) {
		if strings.Contains(stmtNode.Text(), "/* sleep */") {
			v, _ := val.(int)
			time.Sleep(time.Duration(v) * time.Millisecond)
		}
	})

	var stmtLabel string
	if execStmt, ok := stmtNode.(*ast.ExecuteStmt); ok {
		prepareStmt, err := plannercore.GetPreparedStmt(execStmt, s.sessionVars)
		if err == nil && prepareStmt.PreparedAst != nil {
			stmtLabel = ast.GetStmtLabel(prepareStmt.PreparedAst.Stmt)
		}
	}
	if stmtLabel == "" {
		stmtLabel = ast.GetStmtLabel(stmtNode)
	}
	s.setRequestSource(ctx, stmtLabel, stmtNode)

	// Transform abstract syntax tree to a physical plan(stored in executor.ExecStmt).
	compiler := executor.Compiler{Ctx: s}
	stmt, err := compiler.Compile(ctx, stmtNode)
	// check if resource group hint is valid, can't do this in planner.Optimize because we can access
	// infoschema there.
	if sessVars.StmtCtx.ResourceGroupName != sessVars.ResourceGroupName {
		// if target resource group doesn't exist, fallback to the origin resource group.
		if _, ok := s.infoCache.GetLatest().ResourceGroupByName(ast.NewCIStr(sessVars.StmtCtx.ResourceGroupName)); !ok {
			logutil.Logger(ctx).Warn("Unknown resource group from hint", zap.String("name", sessVars.StmtCtx.ResourceGroupName))
			sessVars.StmtCtx.ResourceGroupName = sessVars.ResourceGroupName
			if txn, err := s.Txn(false); err == nil && txn != nil && txn.Valid() {
				kv.SetTxnResourceGroup(txn, sessVars.ResourceGroupName)
			}
		}
	}
	if err != nil {
		s.rollbackOnError(ctx)

		// Only print log message when this SQL is from the user.
		// Mute the warning for internal SQLs.
		if !s.sessionVars.InRestrictedSQL {
			if !variable.ErrUnknownSystemVar.Equal(err) {
				sql := stmtNode.Text()
				sql = parser.Normalize(sql, s.sessionVars.EnableRedactLog)
				logutil.Logger(ctx).Warn("compile SQL failed", zap.Error(err),
					zap.String("SQL", sql))
			}
		}
		return nil, err
	}

	durCompile := time.Since(s.sessionVars.StartTime)
	s.GetSessionVars().DurationCompile = durCompile
	if s.isInternal() {
		session_metrics.SessionExecuteCompileDurationInternal.Observe(durCompile.Seconds())
	} else {
		session_metrics.SessionExecuteCompileDurationGeneral.Observe(durCompile.Seconds())
	}
	s.currentPlan = stmt.Plan
	if execStmt, ok := stmtNode.(*ast.ExecuteStmt); ok {
		if execStmt.Name == "" {
			// for exec-stmt on bin-protocol, ignore the plan detail in `show process` to gain performance benefits.
			s.currentPlan = nil
		}
	}

	// Execute the physical plan.
	defer logStmt(stmt, s) // defer until txnStartTS is set

	var recordSet sqlexec.RecordSet
	if stmt.PsStmt != nil { // point plan short path
		recordSet, err = stmt.PointGet(ctx)
		s.setLastTxnInfoBeforeTxnEnd()
		s.txn.changeToInvalid()
	} else {
		recordSet, err = runStmt(ctx, s, stmt)
	}

	// Observe the resource group query total counter if the resource control is enabled and the
	// current session is attached with a resource group.
	resourceGroupName := s.GetSessionVars().StmtCtx.ResourceGroupName
	if len(resourceGroupName) > 0 {
		metrics.ResourceGroupQueryTotalCounter.WithLabelValues(resourceGroupName, resourceGroupName).Inc()
	}

	if err != nil {
		if !errIsNoisy(err) {
			logutil.Logger(ctx).Warn("run statement failed",
				zap.Int64("schemaVersion", s.GetInfoSchema().SchemaMetaVersion()),
				zap.Error(err),
				zap.String("session", s.String()))
		}
		return recordSet, err
	}
	if !s.isInternal() && config.GetGlobalConfig().EnableTelemetry {
		telemetry.CurrentExecuteCount.Inc()
		tiFlashPushDown, tiFlashExchangePushDown := plannercore.IsTiFlashContained(stmt.Plan)
		if tiFlashPushDown {
			telemetry.CurrentTiFlashPushDownCount.Inc()
		}
		if tiFlashExchangePushDown {
			telemetry.CurrentTiFlashExchangePushDownCount.Inc()
		}
	}
	return recordSet, nil
}

func (s *session) GetSQLExecutor() sqlexec.SQLExecutor {
	return s
}

func (s *session) GetRestrictedSQLExecutor() sqlexec.RestrictedSQLExecutor {
	return s
}

func (s *session) onTxnManagerStmtStartOrRetry(ctx context.Context, node ast.StmtNode) error {
	if s.sessionVars.RetryInfo.Retrying {
		return sessiontxn.GetTxnManager(s).OnStmtRetry(ctx)
	}
	return sessiontxn.GetTxnManager(s).OnStmtStart(ctx, node)
}

func (s *session) validateStatementInTxn(stmtNode ast.StmtNode) error {
	vars := s.GetSessionVars()
	if _, ok := stmtNode.(*ast.ImportIntoStmt); ok && vars.InTxn() {
		return errors.New("cannot run IMPORT INTO in explicit transaction")
	}
	return nil
}

func (s *session) validateStatementReadOnlyInStaleness(stmtNode ast.StmtNode) error {
	vars := s.GetSessionVars()
	if !vars.TxnCtx.IsStaleness && vars.TxnReadTS.PeakTxnReadTS() == 0 && !vars.EnableExternalTSRead || vars.InRestrictedSQL {
		return nil
	}
	errMsg := "only support read-only statement during read-only staleness transactions"
	node := stmtNode.(ast.Node)
	switch v := node.(type) {
	case *ast.SplitRegionStmt:
		return nil
	case *ast.SelectStmt:
		// select lock statement needs start a transaction which will be conflict to stale read,
		// we forbid select lock statement in stale read for now.
		if v.LockInfo != nil {
			return errors.New("select lock hasn't been supported in stale read yet")
		}
		if !planner.IsReadOnly(stmtNode, vars) {
			return errors.New(errMsg)
		}
		return nil
	case *ast.ExplainStmt, *ast.DoStmt, *ast.ShowStmt, *ast.SetOprStmt, *ast.ExecuteStmt, *ast.SetOprSelectList:
		if !planner.IsReadOnly(stmtNode, vars) {
			return errors.New(errMsg)
		}
		return nil
	default:
	}
	// covered DeleteStmt/InsertStmt/UpdateStmt/CallStmt/LoadDataStmt
	if _, ok := stmtNode.(ast.DMLNode); ok {
		return errors.New(errMsg)
	}
	return nil
}

// fileTransInConnKeys contains the keys of queries that will be handled by handleFileTransInConn.
var fileTransInConnKeys = []fmt.Stringer{
	executor.LoadDataVarKey,
	executor.LoadStatsVarKey,
	executor.PlanReplayerLoadVarKey,
}

func (s *session) hasFileTransInConn() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, k := range fileTransInConnKeys {
		v := s.mu.values[k]
		if v != nil {
			return true
		}
	}
	return false
}

// runStmt executes the sqlexec.Statement and commit or rollback the current transaction.
func runStmt(ctx context.Context, se *session, s sqlexec.Statement) (rs sqlexec.RecordSet, err error) {
	failpoint.Inject("assertTxnManagerInRunStmt", func() {
		sessiontxn.RecordAssert(se, "assertTxnManagerInRunStmt", true)
		if stmt, ok := s.(*executor.ExecStmt); ok {
			sessiontxn.AssertTxnManagerInfoSchema(se, stmt.InfoSchema)
		}
	})

	r, ctx := tracing.StartRegionEx(ctx, "session.runStmt")
	defer r.End()
	if r.Span != nil {
		r.Span.LogKV("sql", s.Text())
	}

	se.SetValue(sessionctx.QueryString, s.Text())
	if _, ok := s.(*executor.ExecStmt).StmtNode.(ast.DDLNode); ok {
		se.SetValue(sessionctx.LastExecuteDDL, true)
	} else {
		se.ClearValue(sessionctx.LastExecuteDDL)
	}

	sessVars := se.sessionVars

	// Save origTxnCtx here to avoid it reset in the transaction retry.
	origTxnCtx := sessVars.TxnCtx
	err = se.checkTxnAborted(s)
	if err != nil {
		return nil, err
	}
	if sessVars.TxnCtx.CouldRetry && !s.IsReadOnly(sessVars) {
		// Only when the txn is could retry and the statement is not read only, need to do stmt-count-limit check,
		// otherwise, the stmt won't be add into stmt history, and also don't need check.
		// About `stmt-count-limit`, see more in https://docs.pingcap.com/tidb/stable/tidb-configuration-file#stmt-count-limit
		if err := checkStmtLimit(ctx, se, false); err != nil {
			return nil, err
		}
	}

	rs, err = s.Exec(ctx)

	if se.txn.Valid() && se.txn.IsPipelined() {
		// Pipelined-DMLs can return assertion errors and write conflicts here because they flush
		// during execution, handle these errors as we would handle errors after a commit.
		if err != nil {
			err = se.handleAssertionFailure(ctx, err)
		}
		newErr := se.tryReplaceWriteConflictError(ctx, err)
		if newErr != nil {
			err = newErr
		}
	}

	se.updateTelemetryMetric(s.(*executor.ExecStmt))
	sessVars.TxnCtx.StatementCount++
	if rs != nil {
		if se.GetSessionVars().StmtCtx.IsExplainAnalyzeDML {
			if !sessVars.InTxn() {
				se.StmtCommit(ctx)
				if err := se.CommitTxn(ctx); err != nil {
					return nil, err
				}
			}
		}
		return &execStmtResult{
			RecordSet: rs,
			sql:       s,
			se:        se,
		}, err
	}

	err = finishStmt(ctx, se, err, s)
	if se.hasFileTransInConn() {
		// The query will be handled later in handleFileTransInConn,
		// then should call the ExecStmt.FinishExecuteStmt to finish this statement.
		se.SetValue(ExecStmtVarKey, s.(*executor.ExecStmt))
	} else {
		// If it is not a select statement or special query, we record its slow log here,
		// then it could include the transaction commit time.
		s.(*executor.ExecStmt).FinishExecuteStmt(origTxnCtx.StartTS, err, false)
	}
	return nil, err
}

// ExecStmtVarKeyType is a dummy type to avoid naming collision in context.
type ExecStmtVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (ExecStmtVarKeyType) String() string {
	return "exec_stmt_var_key"
}

// ExecStmtVarKey is a variable key for ExecStmt.
const ExecStmtVarKey ExecStmtVarKeyType = 0

// execStmtResult is the return value of ExecuteStmt and it implements the sqlexec.RecordSet interface.
// Why we need a struct to wrap a RecordSet and provide another RecordSet?
// This is because there are so many session state related things that definitely not belongs to the original
// RecordSet, so this struct exists and RecordSet.Close() is overridden to handle that.
type execStmtResult struct {
	sqlexec.RecordSet
	se     *session
	sql    sqlexec.Statement
	once   sync.Once
	closed bool
}

func (rs *execStmtResult) Finish() error {
	var err error
	rs.once.Do(func() {
		var err1 error
		if f, ok := rs.RecordSet.(interface{ Finish() error }); ok {
			err1 = f.Finish()
		}
		err2 := finishStmt(context.Background(), rs.se, err, rs.sql)
		if err1 != nil {
			err = err1
		} else {
			err = err2
		}
	})
	return err
}

func (rs *execStmtResult) Close() error {
	if rs.closed {
		return nil
	}
	err1 := rs.Finish()
	err2 := rs.RecordSet.Close()
	rs.closed = true
	if err1 != nil {
		return err1
	}
	return err2
}

func (rs *execStmtResult) TryDetach() (sqlexec.RecordSet, bool, error) {
	// If `TryDetach` is called, the connection must have set `mysql.ServerStatusCursorExists`, or
	// the `StatementContext` will be re-used and cause data race.
	intest.Assert(rs.se.GetSessionVars().HasStatusFlag(mysql.ServerStatusCursorExists))

	if !rs.sql.IsReadOnly(rs.se.GetSessionVars()) {
		return nil, false, nil
	}
	if !plannercore.IsAutoCommitTxn(rs.se.GetSessionVars()) {
		return nil, false, nil
	}

	drs, ok := rs.RecordSet.(sqlexec.DetachableRecordSet)
	if !ok {
		return nil, false, nil
	}
	detachedRS, ok, err := drs.TryDetach()
	if !ok || err != nil {
		return nil, ok, err
	}
	cursorHandle := rs.se.GetCursorTracker().NewCursor(
		cursor.State{StartTS: rs.se.GetSessionVars().TxnCtx.StartTS},
	)
	crs := staticrecordset.WrapRecordSetWithCursor(cursorHandle, detachedRS)

	// Now, a transaction is not needed for the detached record set, so we commit the transaction and cleanup
	// the session state.
	err = finishStmt(context.Background(), rs.se, nil, rs.sql)
	if err != nil {
		err2 := detachedRS.Close()
		if err2 != nil {
			logutil.BgLogger().Error("close detached record set failed", zap.Error(err2))
		}
		return nil, true, err
	}

	return crs, true, nil
}

// GetExecutor4Test exports the internal executor for test purpose.
func (rs *execStmtResult) GetExecutor4Test() any {
	return rs.RecordSet.(interface{ GetExecutor4Test() any }).GetExecutor4Test()
}

// rollbackOnError makes sure the next statement starts a new transaction with the latest InfoSchema.
func (s *session) rollbackOnError(ctx context.Context) {
	if !s.sessionVars.InTxn() {
		s.RollbackTxn(ctx)
	}
}

// PrepareStmt is used for executing prepare statement in binary protocol
func (s *session) PrepareStmt(sql string) (stmtID uint32, paramCount int, fields []*resolve.ResultField, err error) {
	defer func() {
		if s.sessionVars.StmtCtx != nil {
			s.sessionVars.StmtCtx.DetachMemDiskTracker()
		}
	}()
	if s.sessionVars.TxnCtx.InfoSchema == nil {
		// We don't need to create a transaction for prepare statement, just get information schema will do.
		s.sessionVars.TxnCtx.InfoSchema = s.infoCache.GetLatest()
	}
	err = s.loadCommonGlobalVariablesIfNeeded()
	if err != nil {
		return
	}

	ctx := context.Background()
	// NewPrepareExec may need startTS to build the executor, for example prepare statement has subquery in int.
	// So we have to call PrepareTxnCtx here.
	if err = s.PrepareTxnCtx(ctx); err != nil {
		return
	}

	prepareStmt := &ast.PrepareStmt{SQLText: sql}
	if err = s.onTxnManagerStmtStartOrRetry(ctx, prepareStmt); err != nil {
		return
	}

	if err = sessiontxn.GetTxnManager(s).AdviseWarmup(); err != nil {
		return
	}
	prepareExec := executor.NewPrepareExec(s, sql)
	err = prepareExec.Next(ctx, nil)
	// Rollback even if err is nil.
	s.rollbackOnError(ctx)

	if err != nil {
		return
	}
	return prepareExec.ID, prepareExec.ParamCount, prepareExec.Fields, nil
}

// ExecutePreparedStmt executes a prepared statement.
func (s *session) ExecutePreparedStmt(ctx context.Context, stmtID uint32, params []expression.Expression) (sqlexec.RecordSet, error) {
	prepStmt, err := s.sessionVars.GetPreparedStmtByID(stmtID)
	if err != nil {
		err = plannererrors.ErrStmtNotFound
		logutil.Logger(ctx).Error("prepared statement not found", zap.Uint32("stmtID", stmtID))
		return nil, err
	}
	stmt, ok := prepStmt.(*plannercore.PlanCacheStmt)
	if !ok {
		return nil, errors.Errorf("invalid PlanCacheStmt type")
	}
	execStmt := &ast.ExecuteStmt{
		BinaryArgs: params,
		PrepStmt:   stmt,
	}
	return s.ExecuteStmt(ctx, execStmt)
}

func (s *session) DropPreparedStmt(stmtID uint32) error {
	vars := s.sessionVars
	if _, ok := vars.PreparedStmts[stmtID]; !ok {
		return plannererrors.ErrStmtNotFound
	}
	vars.RetryInfo.DroppedPreparedStmtIDs = append(vars.RetryInfo.DroppedPreparedStmtIDs, stmtID)
	return nil
}

func (s *session) Txn(active bool) (kv.Transaction, error) {
	if !active {
		return &s.txn, nil
	}
	_, err := sessiontxn.GetTxnManager(s).ActivateTxn()
	s.SetMemoryFootprintChangeHook()
	return &s.txn, err
}

func (s *session) SetValue(key fmt.Stringer, value any) {
	s.mu.Lock()
	s.mu.values[key] = value
	s.mu.Unlock()
}

func (s *session) Value(key fmt.Stringer) any {
	s.mu.RLock()
	value := s.mu.values[key]
	s.mu.RUnlock()
	return value
}

func (s *session) ClearValue(key fmt.Stringer) {
	s.mu.Lock()
	delete(s.mu.values, key)
	s.mu.Unlock()
}

type inCloseSession struct{}

// Close function does some clean work when session end.
// Close should release the table locks which hold by the session.
func (s *session) Close() {
	// TODO: do clean table locks when session exited without execute Close.
	// TODO: do clean table locks when tidb-server was `kill -9`.
	if s.HasLockedTables() && config.TableLockEnabled() {
		if ds := config.TableLockDelayClean(); ds > 0 {
			time.Sleep(time.Duration(ds) * time.Millisecond)
		}
		lockedTables := s.GetAllTableLocks()
		err := domain.GetDomain(s).DDLExecutor().UnlockTables(s, lockedTables)
		if err != nil {
			logutil.BgLogger().Error("release table lock failed", zap.Uint64("conn", s.sessionVars.ConnectionID))
		}
	}
	s.ReleaseAllAdvisoryLocks()
	if s.statsCollector != nil {
		s.statsCollector.Delete()
	}
	if s.idxUsageCollector != nil {
		s.idxUsageCollector.Flush()
	}
	telemetry.GlobalBuiltinFunctionsUsage.Collect(s.GetBuiltinFunctionUsage())
	bindValue := s.Value(bindinfo.SessionBindInfoKeyType)
	if bindValue != nil {
		bindValue.(bindinfo.SessionBindingHandle).Close()
	}
	ctx := context.WithValue(context.TODO(), inCloseSession{}, struct{}{})
	s.RollbackTxn(ctx)
	if s.sessionVars != nil {
		s.sessionVars.WithdrawAllPreparedStmt()
	}
	if s.stmtStats != nil {
		s.stmtStats.SetFinished()
	}
	s.sessionVars.ClearDiskFullOpt()
	if s.sessionPlanCache != nil {
		s.sessionPlanCache.Close()
	}
}

// GetSessionVars implements the context.Context interface.
func (s *session) GetSessionVars() *variable.SessionVars {
	return s.sessionVars
}

// GetPlanCtx returns the PlanContext.
func (s *session) GetPlanCtx() planctx.PlanContext {
	return s.pctx
}

// GetExprCtx returns the expression context of the session.
func (s *session) GetExprCtx() exprctx.ExprContext {
	return s.exprctx
}

// GetTableCtx returns the table.MutateContext
func (s *session) GetTableCtx() tblctx.MutateContext {
	return s.tblctx
}

// GetDistSQLCtx returns the context used in DistSQL
func (s *session) GetDistSQLCtx() *distsqlctx.DistSQLContext {
	vars := s.GetSessionVars()
	sc := vars.StmtCtx

	dctx := sc.GetOrInitDistSQLFromCache(func() *distsqlctx.DistSQLContext {
		// cross ks session does not have domain.
		dom := s.GetDomain().(*domain.Domain)
		var rgCtl *rmclient.ResourceGroupsController
		if dom != nil {
			rgCtl = dom.ResourceGroupsController()
		}
		return &distsqlctx.DistSQLContext{
			WarnHandler:     sc.WarnHandler,
			InRestrictedSQL: sc.InRestrictedSQL,
			Client:          s.GetClient(),

			EnabledRateLimitAction: vars.EnabledRateLimitAction,
			EnableChunkRPC:         vars.EnableChunkRPC,
			OriginalSQL:            sc.OriginalSQL,
			KVVars:                 vars.KVVars,
			KvExecCounter:          sc.KvExecCounter,
			SessionMemTracker:      vars.MemTracker,

			Location:         sc.TimeZone(),
			RuntimeStatsColl: sc.RuntimeStatsColl,
			SQLKiller:        &vars.SQLKiller,
			CPUUsage:         &vars.SQLCPUUsages,
			ErrCtx:           sc.ErrCtx(),

			TiFlashReplicaRead:                   vars.TiFlashReplicaRead,
			TiFlashMaxThreads:                    vars.TiFlashMaxThreads,
			TiFlashMaxBytesBeforeExternalJoin:    vars.TiFlashMaxBytesBeforeExternalJoin,
			TiFlashMaxBytesBeforeExternalGroupBy: vars.TiFlashMaxBytesBeforeExternalGroupBy,
			TiFlashMaxBytesBeforeExternalSort:    vars.TiFlashMaxBytesBeforeExternalSort,
			TiFlashMaxQueryMemoryPerNode:         vars.TiFlashMaxQueryMemoryPerNode,
			TiFlashQuerySpillRatio:               vars.TiFlashQuerySpillRatio,
			TiFlashHashJoinVersion:               vars.TiFlashHashJoinVersion,

			DistSQLConcurrency:            vars.DistSQLScanConcurrency(),
			ReplicaReadType:               vars.GetReplicaRead(),
			WeakConsistency:               sc.WeakConsistency,
			RCCheckTS:                     sc.RCCheckTS,
			NotFillCache:                  sc.NotFillCache,
			TaskID:                        sc.TaskID,
			Priority:                      sc.Priority,
			ResourceGroupTagger:           sc.GetResourceGroupTagger(),
			EnablePaging:                  vars.EnablePaging,
			MinPagingSize:                 vars.MinPagingSize,
			MaxPagingSize:                 vars.MaxPagingSize,
			RequestSourceType:             vars.RequestSourceType,
			ExplicitRequestSourceType:     vars.ExplicitRequestSourceType,
			StoreBatchSize:                vars.StoreBatchSize,
			ResourceGroupName:             sc.ResourceGroupName,
			LoadBasedReplicaReadThreshold: vars.LoadBasedReplicaReadThreshold,
			RunawayChecker:                sc.RunawayChecker,
			RUConsumptionReporter:         rgCtl,
			TiKVClientReadTimeout:         vars.GetTiKVClientReadTimeout(),
			MaxExecutionTime:              vars.GetMaxExecutionTime(),

			ReplicaClosestReadThreshold: vars.ReplicaClosestReadThreshold,
			ConnectionID:                vars.ConnectionID,
			SessionAlias:                vars.SessionAlias,

			ExecDetails: &sc.SyncExecDetails,
		}
	})

	// Check if the runaway checker is updated. This is to avoid that evaluating a non-correlated subquery
	// during the optimization phase will cause the `*distsqlctx.DistSQLContext` to be created before the
	// runaway checker is set later at the execution phase.
	// Ref: https://github.com/pingcap/tidb/issues/61899
	if dctx.RunawayChecker != sc.RunawayChecker {
		dctx.RunawayChecker = sc.RunawayChecker
	}

	return dctx
}

// GetRangerCtx returns the context used in `ranger` related functions
func (s *session) GetRangerCtx() *rangerctx.RangerContext {
	vars := s.GetSessionVars()
	sc := vars.StmtCtx

	rctx := sc.GetOrInitRangerCtxFromCache(func() any {
		return &rangerctx.RangerContext{
			ExprCtx: s.GetExprCtx(),
			TypeCtx: s.GetSessionVars().StmtCtx.TypeCtx(),
			ErrCtx:  s.GetSessionVars().StmtCtx.ErrCtx(),

			InPreparedPlanBuilding:   s.GetSessionVars().StmtCtx.InPreparedPlanBuilding,
			RegardNULLAsPoint:        s.GetSessionVars().RegardNULLAsPoint,
			OptPrefixIndexSingleScan: s.GetSessionVars().OptPrefixIndexSingleScan,
			OptimizerFixControl:      s.GetSessionVars().OptimizerFixControl,

			PlanCacheTracker:     &s.GetSessionVars().StmtCtx.PlanCacheTracker,
			RangeFallbackHandler: &s.GetSessionVars().StmtCtx.RangeFallbackHandler,
		}
	})

	return rctx.(*rangerctx.RangerContext)
}

// GetBuildPBCtx returns the context used in `ToPB` method
func (s *session) GetBuildPBCtx() *planctx.BuildPBContext {
	vars := s.GetSessionVars()
	sc := vars.StmtCtx

	bctx := sc.GetOrInitBuildPBCtxFromCache(func() any {
		return &planctx.BuildPBContext{
			ExprCtx: s.GetExprCtx(),
			Client:  s.GetClient(),

			TiFlashFastScan:                    s.GetSessionVars().TiFlashFastScan,
			TiFlashFineGrainedShuffleBatchSize: s.GetSessionVars().TiFlashFineGrainedShuffleBatchSize,

			// the following fields are used to build `expression.PushDownContext`.
			// TODO: it'd be better to embed `expression.PushDownContext` in `BuildPBContext`. But `expression` already
			// depends on this package, so we need to move `expression.PushDownContext` to a standalone package first.
			GroupConcatMaxLen: s.GetSessionVars().GroupConcatMaxLen,
			InExplainStmt:     s.GetSessionVars().StmtCtx.InExplainStmt,
			WarnHandler:       s.GetSessionVars().StmtCtx.WarnHandler,
			ExtraWarnghandler: s.GetSessionVars().StmtCtx.ExtraWarnHandler,
		}
	})

	return bctx.(*planctx.BuildPBContext)
}

func (s *session) AuthPluginForUser(ctx context.Context, user *auth.UserIdentity) (string, error) {
	pm := privilege.GetPrivilegeManager(s)
	authplugin, err := pm.GetAuthPluginForConnection(ctx, user.Username, user.Hostname)
	if err != nil {
		return "", err
	}
	return authplugin, nil
}

// Auth validates a user using an authentication string and salt.
// If the password fails, it will keep trying other users until exhausted.
// This means it can not be refactored to use MatchIdentity yet.
func (s *session) Auth(user *auth.UserIdentity, authentication, salt []byte, authConn conn.AuthConn) error {
	hasPassword := "YES"
	if len(authentication) == 0 {
		hasPassword = "NO"
	}
	pm := privilege.GetPrivilegeManager(s)
	authUser, err := s.MatchIdentity(context.Background(), user.Username, user.Hostname)
	if err != nil {
		return privileges.ErrAccessDenied.FastGenByArgs(user.Username, user.Hostname, hasPassword)
	}
	// Check whether continuous login failure is enabled to lock the account.
	// If enabled, determine whether to unlock the account and notify TiDB to update the cache.
	enableAutoLock := pm.IsAccountAutoLockEnabled(authUser.Username, authUser.Hostname)
	if enableAutoLock {
		err = failedLoginTrackingBegin(s)
		if err != nil {
			return err
		}
		lockStatusChanged, err := verifyAccountAutoLock(s, authUser.Username, authUser.Hostname)
		if err != nil {
			rollbackErr := failedLoginTrackingRollback(s)
			if rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
		err = failedLoginTrackingCommit(s)
		if err != nil {
			rollbackErr := failedLoginTrackingRollback(s)
			if rollbackErr != nil {
				return rollbackErr
			}
			return err
		}
		if lockStatusChanged {
			// Notification auto unlock.
			err = domain.GetDomain(s).NotifyUpdatePrivilege([]string{authUser.Username})
			if err != nil {
				return err
			}
		}
	}

	info, err := pm.ConnectionVerification(user, authUser.Username, authUser.Hostname, authentication, salt, s.sessionVars, authConn)
	if err != nil {
		if info.FailedDueToWrongPassword {
			// when user enables the account locking function for consecutive login failures,
			// the system updates the login failure count and determines whether to lock the account when authentication fails.
			if enableAutoLock {
				err := failedLoginTrackingBegin(s)
				if err != nil {
					return err
				}
				lockStatusChanged, passwordLocking, trackingErr := authFailedTracking(s, authUser.Username, authUser.Hostname)
				if trackingErr != nil {
					if rollBackErr := failedLoginTrackingRollback(s); rollBackErr != nil {
						return rollBackErr
					}
					return trackingErr
				}
				if err := failedLoginTrackingCommit(s); err != nil {
					if rollBackErr := failedLoginTrackingRollback(s); rollBackErr != nil {
						return rollBackErr
					}
					return err
				}
				if lockStatusChanged {
					// Notification auto lock.
					err := autolockAction(s, passwordLocking, authUser.Username, authUser.Hostname)
					if err != nil {
						return err
					}
				}
			}
		}
		return err
	}

	if vardef.EnableResourceControl.Load() && info.ResourceGroupName != "" {
		s.sessionVars.SetResourceGroupName(info.ResourceGroupName)
	}

	if info.InSandBoxMode {
		// Enter sandbox mode, only execute statement for resetting password.
		s.EnableSandBoxMode()
	}
	if enableAutoLock {
		err := failedLoginTrackingBegin(s)
		if err != nil {
			return err
		}
		// The password is correct. If the account is not locked, the number of login failure statistics will be cleared.
		err = authSuccessClearCount(s, authUser.Username, authUser.Hostname)
		if err != nil {
			if rollBackErr := failedLoginTrackingRollback(s); rollBackErr != nil {
				return rollBackErr
			}
			return err
		}
		err = failedLoginTrackingCommit(s)
		if err != nil {
			if rollBackErr := failedLoginTrackingRollback(s); rollBackErr != nil {
				return rollBackErr
			}
			return err
		}
	}
	pm.AuthSuccess(authUser.Username, authUser.Hostname)
	user.AuthUsername = authUser.Username
	user.AuthHostname = authUser.Hostname
	s.sessionVars.User = user
	s.sessionVars.ActiveRoles = pm.GetDefaultRoles(context.Background(), user.AuthUsername, user.AuthHostname)
	return nil
}

func authSuccessClearCount(s *session, user string, host string) error {
	// Obtain accurate lock status and failure count information.
	passwordLocking, err := getFailedLoginUserAttributes(s, user, host)
	if err != nil {
		return err
	}
	// If the account is locked, it may be caused by the untimely update of the cache,
	// directly report the account lock.
	if passwordLocking.AutoAccountLocked {
		if passwordLocking.PasswordLockTimeDays == -1 {
			return privileges.GenerateAccountAutoLockErr(passwordLocking.FailedLoginAttempts, user, host,
				"unlimited", "unlimited")
		}

		lds := strconv.FormatInt(passwordLocking.PasswordLockTimeDays, 10)
		return privileges.GenerateAccountAutoLockErr(passwordLocking.FailedLoginAttempts, user, host, lds, lds)
	}
	if passwordLocking.FailedLoginCount != 0 {
		// If the number of account login failures is not zero, it will be updated to 0.
		passwordLockingJSON := privileges.BuildSuccessPasswordLockingJSON(passwordLocking.FailedLoginAttempts,
			passwordLocking.PasswordLockTimeDays)
		if passwordLockingJSON != "" {
			if err := s.passwordLocking(user, host, passwordLockingJSON); err != nil {
				return err
			}
		}
	}
	return nil
}

func verifyAccountAutoLock(s *session, user, host string) (bool, error) {
	pm := privilege.GetPrivilegeManager(s)
	// Use the cache to determine whether to unlock the account.
	// If the account needs to be unlocked, read the database information to determine whether
	// the account needs to be unlocked. Otherwise, an error message is displayed.
	lockStatusInMemory, err := pm.VerifyAccountAutoLockInMemory(user, host)
	if err != nil {
		return false, err
	}
	// If the lock status in the cache is Unlock, the automatic unlock is skipped.
	// If memory synchronization is slow and there is a lock in the database, it will be processed upon successful login.
	if !lockStatusInMemory {
		return false, nil
	}
	lockStatusChanged := false
	var plJSON string
	// After checking the cache, obtain the latest data from the database and determine
	// whether to automatically unlock the database to prevent repeated unlock errors.
	pl, err := getFailedLoginUserAttributes(s, user, host)
	if err != nil {
		return false, err
	}
	if pl.AutoAccountLocked {
		// If it is locked, need to check whether it can be automatically unlocked.
		lockTimeDay := pl.PasswordLockTimeDays
		if lockTimeDay == -1 {
			return false, privileges.GenerateAccountAutoLockErr(pl.FailedLoginAttempts, user, host, "unlimited", "unlimited")
		}
		lastChanged := pl.AutoLockedLastChanged
		d := time.Now().Unix() - lastChanged
		if d <= lockTimeDay*24*60*60 {
			lds := strconv.FormatInt(lockTimeDay, 10)
			rds := strconv.FormatInt(int64(math.Ceil(float64(lockTimeDay)-float64(d)/(24*60*60))), 10)
			return false, privileges.GenerateAccountAutoLockErr(pl.FailedLoginAttempts, user, host, lds, rds)
		}
		// Generate unlock json string.
		plJSON = privileges.BuildPasswordLockingJSON(pl.FailedLoginAttempts,
			pl.PasswordLockTimeDays, "N", 0, time.Now().Format(time.UnixDate))
	}
	if plJSON != "" {
		lockStatusChanged = true
		if err = s.passwordLocking(user, host, plJSON); err != nil {
			return false, err
		}
	}
	return lockStatusChanged, nil
}

func authFailedTracking(s *session, user string, host string) (bool, *privileges.PasswordLocking, error) {
	// Obtain the number of consecutive password login failures.
	passwordLocking, err := getFailedLoginUserAttributes(s, user, host)
	if err != nil {
		return false, nil, err
	}
	// Consecutive wrong password login failure times +1,
	// If the lock condition is satisfied, the lock status is updated and the update cache is notified.
	lockStatusChanged, err := userAutoAccountLocked(s, user, host, passwordLocking)
	if err != nil {
		return false, nil, err
	}
	return lockStatusChanged, passwordLocking, nil
}

func autolockAction(s *session, passwordLocking *privileges.PasswordLocking, user, host string) error {
	// Don't want to update the cache frequently, and only trigger the update cache when the lock status is updated.
	err := domain.GetDomain(s).NotifyUpdatePrivilege([]string{user})
	if err != nil {
		return err
	}
	// The number of failed login attempts reaches FAILED_LOGIN_ATTEMPTS.
	// An error message is displayed indicating permission denial and account lock.
	if passwordLocking.PasswordLockTimeDays == -1 {
		return privileges.GenerateAccountAutoLockErr(passwordLocking.FailedLoginAttempts, user, host,
			"unlimited", "unlimited")
	}
	lds := strconv.FormatInt(passwordLocking.PasswordLockTimeDays, 10)
	return privileges.GenerateAccountAutoLockErr(passwordLocking.FailedLoginAttempts, user, host, lds, lds)
}

func (s *session) passwordLocking(user string, host string, newAttributesStr string) error {
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, "UPDATE %n.%n SET ", mysql.SystemDB, mysql.UserTable)
	sqlescape.MustFormatSQL(sql, "user_attributes=json_merge_patch(coalesce(user_attributes, '{}'), %?)", newAttributesStr)
	sqlescape.MustFormatSQL(sql, " WHERE Host=%? and User=%?;", host, user)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	_, err := s.ExecuteInternal(ctx, sql.String())
	return err
}

func failedLoginTrackingBegin(s *session) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	_, err := s.ExecuteInternal(ctx, "BEGIN PESSIMISTIC")
	return err
}

func failedLoginTrackingCommit(s *session) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	_, err := s.ExecuteInternal(ctx, "COMMIT")
	if err != nil {
		_, rollBackErr := s.ExecuteInternal(ctx, "ROLLBACK")
		if rollBackErr != nil {
			return rollBackErr
		}
	}
	return err
}

func failedLoginTrackingRollback(s *session) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	_, err := s.ExecuteInternal(ctx, "ROLLBACK")
	return err
}

// getFailedLoginUserAttributes queries the exact number of consecutive password login failures (concurrency is not allowed).
func getFailedLoginUserAttributes(s *session, user string, host string) (*privileges.PasswordLocking, error) {
	passwordLocking := &privileges.PasswordLocking{}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	rs, err := s.ExecuteInternal(ctx, `SELECT user_attributes from mysql.user WHERE USER = %? AND HOST = %? for update`, user, host)
	if err != nil {
		return passwordLocking, err
	}
	defer func() {
		if closeErr := rs.Close(); closeErr != nil {
			err = closeErr
		}
	}()
	req := rs.NewChunk(nil)
	iter := chunk.NewIterator4Chunk(req)
	err = rs.Next(ctx, req)
	if err != nil {
		return passwordLocking, err
	}
	if req.NumRows() == 0 {
		return passwordLocking, fmt.Errorf("user_attributes by `%s`@`%s` not found", user, host)
	}
	row := iter.Begin()
	if !row.IsNull(0) {
		passwordLockingJSON := row.GetJSON(0)
		return passwordLocking, passwordLocking.ParseJSON(passwordLockingJSON)
	}
	return passwordLocking, fmt.Errorf("user_attributes by `%s`@`%s` not found", user, host)
}

func userAutoAccountLocked(s *session, user string, host string, pl *privileges.PasswordLocking) (bool, error) {
	// Indicates whether the user needs to update the lock status change.
	lockStatusChanged := false
	// The number of consecutive login failures is stored in the database.
	// If the current login fails, one is added to the number of consecutive login failures
	// stored in the database to determine whether the user needs to be locked and the number of update failures.
	failedLoginCount := pl.FailedLoginCount + 1
	// If the cache is not updated, but it is already locked, it will report that the account is locked.
	if pl.AutoAccountLocked {
		if pl.PasswordLockTimeDays == -1 {
			return false, privileges.GenerateAccountAutoLockErr(pl.FailedLoginAttempts, user, host,
				"unlimited", "unlimited")
		}
		lds := strconv.FormatInt(pl.PasswordLockTimeDays, 10)
		return false, privileges.GenerateAccountAutoLockErr(pl.FailedLoginAttempts, user, host, lds, lds)
	}

	autoAccountLocked := "N"
	autoLockedLastChanged := ""
	if pl.FailedLoginAttempts == 0 || pl.PasswordLockTimeDays == 0 {
		return false, nil
	}

	if failedLoginCount >= pl.FailedLoginAttempts {
		autoLockedLastChanged = time.Now().Format(time.UnixDate)
		autoAccountLocked = "Y"
		lockStatusChanged = true
	}

	newAttributesStr := privileges.BuildPasswordLockingJSON(pl.FailedLoginAttempts,
		pl.PasswordLockTimeDays, autoAccountLocked, failedLoginCount, autoLockedLastChanged)
	if newAttributesStr != "" {
		return lockStatusChanged, s.passwordLocking(user, host, newAttributesStr)
	}
	return lockStatusChanged, nil
}

// MatchIdentity finds the matching username + password in the MySQL privilege tables
// for a username + hostname, since MySQL can have wildcards.
func (s *session) MatchIdentity(ctx context.Context, username, remoteHost string) (*auth.UserIdentity, error) {
	pm := privilege.GetPrivilegeManager(s)
	var success bool
	var skipNameResolve bool
	var user = &auth.UserIdentity{}
	varVal, err := s.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(vardef.SkipNameResolve)
	if err == nil && variable.TiDBOptOn(varVal) {
		skipNameResolve = true
	}
	user.Username, user.Hostname, success = pm.MatchIdentity(ctx, username, remoteHost, skipNameResolve)
	if success {
		return user, nil
	}
	// This error will not be returned to the user, access denied will be instead
	return nil, fmt.Errorf("could not find matching user in MatchIdentity: %s, %s", username, remoteHost)
}

// AuthWithoutVerification is required by the ResetConnection RPC
func (s *session) AuthWithoutVerification(ctx context.Context, user *auth.UserIdentity) bool {
	pm := privilege.GetPrivilegeManager(s)
	authUser, err := s.MatchIdentity(ctx, user.Username, user.Hostname)
	if err != nil {
		return false
	}
	if pm.GetAuthWithoutVerification(authUser.Username, authUser.Hostname) {
		user.AuthUsername = authUser.Username
		user.AuthHostname = authUser.Hostname
		s.sessionVars.User = user
		s.sessionVars.ActiveRoles = pm.GetDefaultRoles(ctx, user.AuthUsername, user.AuthHostname)
		return true
	}
	return false
}

// SetSessionStatesHandler implements the Session.SetSessionStatesHandler interface.
func (s *session) SetSessionStatesHandler(stateType sessionstates.SessionStateType, handler sessionctx.SessionStatesHandler) {
	s.sessionStatesHandlers[stateType] = handler
}

// ReportUsageStats reports the usage stats
func (s *session) ReportUsageStats() {
	if s.idxUsageCollector != nil {
		s.idxUsageCollector.Report()
	}
}

// CreateSession4Test creates a new session environment for test.
func CreateSession4Test(store kv.Storage) (types.Session, error) {
	se, err := CreateSession4TestWithOpt(store, nil)
	if err == nil {
		// Cover both chunk rpc encoding and default encoding.
		// nolint:gosec
		if rand.Intn(2) == 0 {
			se.GetSessionVars().EnableChunkRPC = false
		} else {
			se.GetSessionVars().EnableChunkRPC = true
		}
	}
	return se, err
}

// Opt describes the option for creating session
type Opt struct {
	PreparedPlanCache sessionctx.SessionPlanCache
}

// CreateSession4TestWithOpt creates a new session environment for test.
func CreateSession4TestWithOpt(store kv.Storage, opt *Opt) (types.Session, error) {
	s, err := CreateSessionWithOpt(store, opt)
	if err == nil {
		// initialize session variables for test.
		s.GetSessionVars().InitChunkSize = 2
		s.GetSessionVars().MaxChunkSize = 32
		s.GetSessionVars().MinPagingSize = vardef.DefMinPagingSize
		s.GetSessionVars().EnablePaging = vardef.DefTiDBEnablePaging
		s.GetSessionVars().StmtCtx.SetTimeZone(s.GetSessionVars().Location())
		err = s.GetSessionVars().SetSystemVarWithoutValidation(vardef.CharacterSetConnection, "utf8mb4")
	}
	return s, err
}

// CreateSession creates a new session environment.
func CreateSession(store kv.Storage) (types.Session, error) {
	return CreateSessionWithOpt(store, nil)
}

// CreateSessionWithOpt creates a new session environment with option.
// Use default option if opt is nil.
func CreateSessionWithOpt(store kv.Storage, opt *Opt) (types.Session, error) {
	do, err := domap.Get(store)
	if err != nil {
		return nil, err
	}
	s, err := createSessionWithOpt(store, do, do.GetSchemaValidator(), do.InfoCache(), opt)
	if err != nil {
		return nil, err
	}

	// Add auth here.
	extensions, err := extension.GetExtensions()
	if err != nil {
		return nil, err
	}
	pm := privileges.NewUserPrivileges(do.PrivilegeHandle(), extensions)
	privilege.BindPrivilegeManager(s, pm)

	// Add stats collector, and it will be freed by background stats worker
	// which periodically updates stats using the collected data.
	if do.StatsHandle() != nil && do.StatsUpdating() {
		s.statsCollector = do.StatsHandle().NewSessionStatsItem().(*usage.SessionStatsItem)
		if config.GetGlobalConfig().Instance.EnableCollectExecutionInfo.Load() {
			s.idxUsageCollector = do.StatsHandle().NewSessionIndexUsageCollector()
		}
	}

	s.cursorTracker = cursor.NewTracker()

	return s, nil
}

// loadCollationParameter loads collation parameter from mysql.tidb
func loadCollationParameter(ctx context.Context, se *session) (bool, error) {
	para, err := se.getTableValue(ctx, mysql.TiDBTable, TidbNewCollationEnabled)
	if err != nil {
		return false, err
	}
	if para == varTrue {
		return true, nil
	} else if para == varFalse {
		return false, nil
	}
	logutil.BgLogger().Warn(
		"Unexpected value of 'new_collation_enabled' in 'mysql.tidb', use 'False' instead",
		zap.String("value", para))
	return false, nil
}

// TableBasicInfo contains the basic information of a table used in DDL.
type TableBasicInfo struct {
	ID   int64
	Name string
	SQL  string
}

type versionedDDLTables struct {
	ver    meta.DDLTableVersion
	tables []TableBasicInfo
}

var (
	errResultIsEmpty = dbterror.ClassExecutor.NewStd(errno.ErrResultIsEmpty)
	// DDLJobTables is a list of tables definitions used in concurrent DDL.
	DDLJobTables = []TableBasicInfo{
		{ID: metadef.TiDBDDLJobTableID, Name: "tidb_ddl_job", SQL: ddl.JobTableSQL},
		{ID: metadef.TiDBDDLReorgTableID, Name: "tidb_ddl_reorg", SQL: ddl.ReorgTableSQL},
		{ID: metadef.TiDBDDLHistoryTableID, Name: "tidb_ddl_history", SQL: ddl.HistoryTableSQL},
	}
	// MDLTables is a list of tables definitions used for metadata lock.
	MDLTables = []TableBasicInfo{
		{ID: metadef.TiDBMDLInfoTableID, Name: "tidb_mdl_info", SQL: ddl.MDLTableSQL},
	}
	// BackfillTables is a list of tables definitions used in dist reorg DDL.
	BackfillTables = []TableBasicInfo{
		{ID: metadef.TiDBBackgroundSubtaskTableID, Name: "tidb_background_subtask", SQL: ddl.BackgroundSubtaskTableSQL},
		{ID: metadef.TiDBBackgroundSubtaskHistoryTableID, Name: "tidb_background_subtask_history", SQL: ddl.BackgroundSubtaskHistoryTableSQL},
	}
	// DDLNotifierTables contains the table definitions used in DDL notifier.
	// It only contains the notifier table.
	// Put it here to reuse a unified initialization function and make it easier to find.
	DDLNotifierTables = []TableBasicInfo{
		{ID: metadef.TiDBDDLNotifierTableID, Name: "tidb_ddl_notifier", SQL: ddl.NotifierTableSQL},
	}

	ddlTableVersionTables = []versionedDDLTables{
		{ver: meta.BaseDDLTableVersion, tables: DDLJobTables},
		{ver: meta.MDLTableVersion, tables: MDLTables},
		{ver: meta.BackfillTableVersion, tables: BackfillTables},
		{ver: meta.DDLNotifierTableVersion, tables: DDLNotifierTables},
	}
)

func splitAndScatterTable(store kv.Storage, tableIDs []int64) {
	if s, ok := store.(kv.SplittableStore); ok && atomic.LoadUint32(&ddl.EnableSplitTableRegion) == 1 {
		ctxWithTimeout, cancel := context.WithTimeout(context.Background(), vardef.DefWaitSplitRegionTimeout*time.Second)
		var regionIDs []uint64
		for _, id := range tableIDs {
			regionIDs = append(regionIDs, ddl.SplitRecordRegion(ctxWithTimeout, s, id, id, vardef.DefTiDBScatterRegion))
		}
		if vardef.DefTiDBScatterRegion != vardef.ScatterOff {
			ddl.WaitScatterRegionFinish(ctxWithTimeout, s, regionIDs...)
		}
		cancel()
	}
}

// InitDDLTables creates system tables that DDL uses. Because CREATE TABLE is
// also a DDL, we must directly modify KV data to create these tables.
func InitDDLTables(store kv.Storage) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	return kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMutator(txn)
		currVer, err := t.CheckDDLTableVersion()
		if err != nil {
			return errors.Trace(err)
		}
		dbID, err := t.CreateMySQLDatabaseIfNotExists()
		if err != nil {
			return err
		}

		largestVer := currVer
		for _, vt := range ddlTableVersionTables {
			if currVer >= vt.ver {
				continue
			}
			logutil.BgLogger().Info("init DDL tables", zap.Int("currVer", int(currVer)),
				zap.Int("targetVer", int(vt.ver)))
			largestVer = max(largestVer, vt.ver)
			if err = createAndSplitTables(store, t, dbID, vt.tables); err != nil {
				return err
			}
		}
		if largestVer > currVer {
			return t.SetDDLTableVersion(largestVer)
		}
		return nil
	})
}

func createAndSplitTables(store kv.Storage, t *meta.Mutator, dbID int64, tables []TableBasicInfo) error {
	tableIDs := make([]int64, 0, len(tables))
	for _, tbl := range tables {
		tableIDs = append(tableIDs, tbl.ID)
	}
	splitAndScatterTable(store, tableIDs)
	p := parser.New()
	for _, tbl := range tables {
		stmt, err := p.ParseOneStmt(tbl.SQL, "", "")
		if err != nil {
			return errors.Trace(err)
		}
		tblInfo, err := ddl.BuildTableInfoFromAST(metabuild.NewContext(), stmt.(*ast.CreateTableStmt))
		if err != nil {
			return errors.Trace(err)
		}
		tblInfo.State = model.StatePublic
		tblInfo.ID = tbl.ID
		tblInfo.UpdateTS = t.StartTS
		err = t.CreateTableOrView(dbID, tblInfo)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// InitMDLVariableForBootstrap initializes the metadata lock variable.
func InitMDLVariableForBootstrap(store kv.Storage) error {
	err := kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), store, true, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMutator(txn)
		return t.SetMetadataLock(true)
	})
	if err != nil {
		return err
	}
	vardef.EnableMDL.Store(true)
	return nil
}

// InitTiDBSchemaCacheSize initializes the tidb schema cache size.
func InitTiDBSchemaCacheSize(store kv.Storage) error {
	var (
		isNull bool
		size   uint64
		err    error
	)
	err = kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), store, true, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMutator(txn)
		size, isNull, err = t.GetSchemaCacheSize()
		if err != nil {
			return errors.Trace(err)
		}
		if isNull {
			size = vardef.DefTiDBSchemaCacheSize
			return t.SetSchemaCacheSize(size)
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	vardef.SchemaCacheSize.Store(size)
	return nil
}

// InitMDLVariableForUpgrade initializes the metadata lock variable.
func InitMDLVariableForUpgrade(store kv.Storage) (bool, error) {
	isNull := false
	enable := false
	var err error
	err = kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), store, true, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMutator(txn)
		enable, isNull, err = t.GetMetadataLock()
		if err != nil {
			return err
		}
		return nil
	})
	if isNull || !enable {
		vardef.EnableMDL.Store(false)
	} else {
		vardef.EnableMDL.Store(true)
	}
	return isNull, err
}

// InitMDLVariable initializes the metadata lock variable.
func InitMDLVariable(store kv.Storage) error {
	isNull := false
	enable := false
	var err error
	err = kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), store, true, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMutator(txn)
		enable, isNull, err = t.GetMetadataLock()
		if err != nil {
			return err
		}
		if isNull {
			// Workaround for version: nightly-2022-11-07 to nightly-2022-11-17.
			enable = true
			logutil.BgLogger().Warn("metadata lock is null")
			err = t.SetMetadataLock(true)
			if err != nil {
				return err
			}
		}
		return nil
	})
	vardef.EnableMDL.Store(enable)
	return err
}

// BootstrapSession bootstrap session and domain.
func BootstrapSession(store kv.Storage) (*domain.Domain, error) {
	return bootstrapSessionImpl(context.Background(), store, createSessions)
}

// BootstrapSession4DistExecution bootstrap session and dom for Distributed execution test, only for unit testing.
func BootstrapSession4DistExecution(store kv.Storage) (*domain.Domain, error) {
	return bootstrapSessionImpl(context.Background(), store, createSessions4DistExecution)
}

// bootstrapSessionImpl bootstraps session and domain.
// the process works as follows:
// - if we haven't bootstrapped to the target version
//   - create/init/start domain
//   - bootstrap or upgrade, some variables will be initialized and stored to system
//     table in the process, such as system time-zone
//   - close domain
//
// - create/init another domain
// - initialization global variables from system table that's required to use sessionCtx,
// such as system time zone
// - start domain and other routines.
func bootstrapSessionImpl(ctx context.Context, store kv.Storage, createSessionsImpl func(store kv.Storage, cnt int) ([]*session, error)) (*domain.Domain, error) {
	ver := getStoreBootstrapVersionWithCache(store)
	if keyspace.IsRunningOnUser() {
		systemKSVer := mustGetStoreBootstrapVersion(kvstore.GetSystemStorage())
		if systemKSVer == notBootstrapped {
			logutil.BgLogger().Fatal("SYSTEM keyspace is not bootstrapped")
		} else if ver > systemKSVer {
			logutil.BgLogger().Fatal("bootstrap version of user keyspace must be smaller or equal to that of SYSTEM keyspace",
				zap.Int64("user", ver), zap.Int64("system", systemKSVer))
		}
	}

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBootstrap)
	cfg := config.GetGlobalConfig()
	if len(cfg.Instance.PluginLoad) > 0 {
		err := plugin.Load(context.Background(), plugin.Config{
			Plugins:   strings.Split(cfg.Instance.PluginLoad, ","),
			PluginDir: cfg.Instance.PluginDir,
		})
		if err != nil {
			return nil, err
		}
	}
	err := InitDDLTables(store)
	if err != nil {
		return nil, err
	}
	err = InitTiDBSchemaCacheSize(store)
	if err != nil {
		return nil, err
	}
	if ver < currentBootstrapVersion {
		runInBootstrapSession(store, ver)
	} else {
		logutil.BgLogger().Info("cluster already bootstrapped", zap.Int64("version", ver))
		err = InitMDLVariable(store)
		if err != nil {
			return nil, err
		}
	}

	// initiate disttask framework components which need a store
	scheduler.RegisterSchedulerFactory(
		proto.ImportInto,
		func(ctx context.Context, task *proto.Task, param scheduler.Param) scheduler.Scheduler {
			return importinto.NewImportScheduler(ctx, task, param, store)
		},
	)
	taskexecutor.RegisterTaskType(
		proto.ImportInto,
		func(ctx context.Context, task *proto.Task, param taskexecutor.Param) taskexecutor.TaskExecutor {
			return importinto.NewImportExecutor(ctx, task, param, store)
		},
	)

	concurrency := config.GetGlobalConfig().Performance.StatsLoadConcurrency
	if concurrency == 0 {
		// if concurrency is 0, we will set the concurrency of sync load by CPU.
		concurrency = syncload.GetSyncLoadConcurrencyByCPU()
	}
	if concurrency < 0 { // it is only for test, in the production, negative value is illegal.
		concurrency = 0
	}

	ses, err := createSessionsImpl(store, 10)
	if err != nil {
		return nil, err
	}
	ses[0].GetSessionVars().InRestrictedSQL = true

	// get system tz from mysql.tidb
	tz, err := ses[0].getTableValue(ctx, mysql.TiDBTable, tidbSystemTZ)
	if err != nil {
		return nil, err
	}
	timeutil.SetSystemTZ(tz)

	// get the flag from `mysql`.`tidb` which indicating if new collations are enabled.
	newCollationEnabled, err := loadCollationParameter(ctx, ses[0])
	if err != nil {
		return nil, err
	}
	collate.SetNewCollationEnabledForTest(newCollationEnabled)

	// only start the domain after we have initialized some global variables.
	dom := domain.GetDomain(ses[0])
	err = dom.Start(ddl.Normal)
	if err != nil {
		return nil, err
	}

	// To deal with the location partition failure caused by inconsistent NewCollationEnabled values(see issue #32416).
	rebuildAllPartitionValueMapAndSorted(ctx, ses[0])

	// We should make the load bind-info loop before other loops which has internal SQL.
	// Because the internal SQL may access the global bind-info handler. As the result, the data race occurs here as the
	// LoadBindInfoLoop inits global bind-info handler.
	err = dom.InitBindingHandle()
	if err != nil {
		return nil, err
	}

	if !config.GetGlobalConfig().Security.SkipGrantTable {
		err = dom.LoadPrivilegeLoop(ses[3])
		if err != nil {
			return nil, err
		}
	}

	// Rebuild sysvar cache in a loop
	err = dom.LoadSysVarCacheLoop(ses[4])
	if err != nil {
		return nil, err
	}

	if config.GetGlobalConfig().DisaggregatedTiFlash && !config.GetGlobalConfig().UseAutoScaler {
		// Invalid client-go tiflash_compute store cache if necessary.
		err = dom.WatchTiFlashComputeNodeChange()
		if err != nil {
			return nil, err
		}
	}

	if err = extensionimpl.Bootstrap(context.Background(), dom); err != nil {
		return nil, err
	}

	if len(cfg.Instance.PluginLoad) > 0 {
		err := plugin.Init(context.Background(), plugin.Config{EtcdClient: dom.GetEtcdClient()})
		if err != nil {
			return nil, err
		}
	}

	err = executor.LoadExprPushdownBlacklist(ses[5])
	if err != nil {
		return nil, err
	}
	err = executor.LoadOptRuleBlacklist(ctx, ses[5])
	if err != nil {
		return nil, err
	}

	if config.GetGlobalConfig().EnableTelemetry {
		// There is no way to turn telemetry on with global variable `tidb_enable_telemetry`
		// when it is disabled in config. See IsTelemetryEnabled function in telemetry/telemetry.go
		go func() {
			dom.TelemetryLoop(ses[5])
		}()
	}

	planReplayerWorkerCnt := config.GetGlobalConfig().Performance.PlanReplayerDumpWorkerConcurrency
	planReplayerWorkersSctx := make([]sessionctx.Context, planReplayerWorkerCnt)
	pworkerSes, err := createSessions(store, int(planReplayerWorkerCnt))
	if err != nil {
		return nil, err
	}
	for i := range int(planReplayerWorkerCnt) {
		planReplayerWorkersSctx[i] = pworkerSes[i]
	}
	// setup plan replayer handle
	dom.SetupPlanReplayerHandle(ses[6], planReplayerWorkersSctx)
	dom.StartPlanReplayerHandle()
	// setup dumpFileGcChecker
	dom.SetupDumpFileGCChecker(ses[7])
	dom.DumpFileGcCheckerLoop()
	// setup historical stats worker
	dom.SetupHistoricalStatsWorker(ses[8])
	dom.StartHistoricalStatsWorker()
	failToLoadOrParseSQLFile := false // only used for unit test
	if runBootstrapSQLFile {
		pm := &privileges.UserPrivileges{
			Handle: dom.PrivilegeHandle(),
		}
		privilege.BindPrivilegeManager(ses[9], pm)
		if err := doBootstrapSQLFile(ses[9]); err != nil && intest.EnableInternalCheck {
			failToLoadOrParseSQLFile = true
		}
	}

	// setup extract Handle
	extractWorkers := 1
	sctxs, err := createSessions(store, extractWorkers)
	if err != nil {
		return nil, err
	}
	extractWorkerSctxs := make([]sessionctx.Context, 0)
	for _, sctx := range sctxs {
		extractWorkerSctxs = append(extractWorkerSctxs, sctx)
	}
	dom.SetupExtractHandle(extractWorkerSctxs)

	// setup init stats loader
	initStatsCtx, err := createSession(store)
	if err != nil {
		return nil, err
	}
	if err = dom.LoadAndUpdateStatsLoop(concurrency, initStatsCtx); err != nil {
		return nil, err
	}

	// init the instance plan cache
	dom.InitInstancePlanCache()

	// setup workload-based learning worker
	dom.SetupWorkloadBasedLearningWorker()

	// start TTL job manager after setup stats collector
	// because TTL could modify a lot of columns, and need to trigger auto analyze
	statshandle.AttachStatsCollector = func(s sqlexec.SQLExecutor) sqlexec.SQLExecutor {
		if s, ok := s.(*session); ok {
			return attachStatsCollector(s, dom)
		}
		return s
	}
	statshandle.DetachStatsCollector = func(s sqlexec.SQLExecutor) sqlexec.SQLExecutor {
		if s, ok := s.(*session); ok {
			return detachStatsCollector(s)
		}
		return s
	}
	dom.StartTTLJobManager()

	dom.LoadSigningCertLoop(cfg.Security.SessionTokenSigningCert, cfg.Security.SessionTokenSigningKey)

	if raw, ok := store.(kv.EtcdBackend); ok {
		err = raw.StartGCWorker()
		if err != nil {
			return nil, err
		}
	}

	// This only happens in testing, since the failure of loading or parsing sql file
	// would panic the bootstrapping.
	if intest.EnableInternalCheck && failToLoadOrParseSQLFile {
		dom.Close()
		return nil, errors.New("Fail to load or parse sql file")
	}
	err = dom.InitDistTaskLoop()
	if err != nil {
		return nil, err
	}
	return dom, err
}

// GetDomain gets the associated domain for store.
func GetDomain(store kv.Storage) (*domain.Domain, error) {
	return domap.Get(store)
}

func getStartMode(ver int64) ddl.StartMode {
	if ver == notBootstrapped {
		return ddl.Bootstrap
	} else if ver < currentBootstrapVersion {
		return ddl.Upgrade
	}
	return ddl.Normal
}

// runInBootstrapSession create a special session for bootstrap to run.
// If no bootstrap and storage is remote, we must use a little lease time to
// bootstrap quickly, after bootstrapped, we will reset the lease time.
// TODO: Using a bootstrap tool for doing this may be better later.
func runInBootstrapSession(store kv.Storage, ver int64) {
	startMode := getStartMode(ver)
	startTime := time.Now()
	defer func() {
		logutil.BgLogger().Info("bootstrap cluster finished",
			zap.String("bootMode", string(startMode)),
			zap.Duration("cost", time.Since(startTime)))
	}()
	if startMode == ddl.Upgrade {
		// TODO at this time domain must not be created, else it will register server
		// info, and cause deadlock, we need to make sure this in a clear way
		logutil.BgLogger().Info("[upgrade] get owner lock to upgrade")
		releaseFn, err := acquireLock(store)
		if err != nil {
			logutil.BgLogger().Fatal("[upgrade] get owner lock failed", zap.Error(err))
		}
		defer releaseFn()
		currVer := mustGetStoreBootstrapVersion(store)
		if currVer >= currentBootstrapVersion {
			// It is already bootstrapped/upgraded by another TiDB instance, but
			// we still need to go through the following domain Start/Close code
			// right now as we have already initialized it when creating the session,
			// so we switch to normal mode.
			// TODO remove this after we can refactor below code out in this case.
			logutil.BgLogger().Info("[upgrade] already upgraded by other nodes, switch to normal mode")
			startMode = ddl.Normal
		}
	}
	s, err := createSession(store)
	if err != nil {
		// Bootstrap fail will cause program exit.
		logutil.BgLogger().Fatal("createSession error", zap.Error(err))
	}
	dom := domain.GetDomain(s)
	err = dom.Start(startMode)
	if err != nil {
		// Bootstrap fail will cause program exit.
		logutil.BgLogger().Fatal("start domain error", zap.Error(err))
	}

	// For the bootstrap SQLs, the following variables should be compatible with old TiDB versions.
	// TODO we should have a createBootstrapSession to init those special variables.
	s.sessionVars.EnableClusteredIndex = vardef.ClusteredIndexDefModeIntOnly

	s.SetValue(sessionctx.Initing, true)
	if startMode == ddl.Bootstrap {
		bootstrap(s)
	} else if startMode == ddl.Upgrade {
		// below sleep is used to mitigate https://github.com/pingcap/tidb/issues/57003,
		// to let the older owner have time to notice that it's already retired.
		time.Sleep(owner.WaitTimeOnForceOwner)
		upgrade(s)
	}
	finishBootstrap(store)
	s.ClearValue(sessionctx.Initing)

	dom.Close()
	if intest.InTest {
		infosync.MockGlobalServerInfoManagerEntry.Close()
	}
	domap.Delete(store)
}

func createSessions(store kv.Storage, cnt int) ([]*session, error) {
	return createSessionsImpl(store, cnt)
}

func createSessions4DistExecution(store kv.Storage, cnt int) ([]*session, error) {
	domap.Delete(store)

	return createSessionsImpl(store, cnt)
}

func createSessionsImpl(store kv.Storage, cnt int) ([]*session, error) {
	// Then we can create new dom
	ses := make([]*session, cnt)
	for i := range cnt {
		se, err := createSession(store)
		if err != nil {
			return nil, err
		}
		ses[i] = se
	}

	return ses, nil
}

// createSession creates a new session.
// Please note that such a session is not tracked by the internal session list.
// This means the min ts reporter is not aware of it and may report a wrong min start ts.
// In most cases you should use a session pool in domain instead.
func createSession(store kv.Storage) (*session, error) {
	dom, err := domap.Get(store)
	if err != nil {
		return nil, err
	}
	return createSessionWithOpt(store, dom, dom.GetSchemaValidator(), dom.InfoCache(), nil)
}

func createCrossKSSession(currKSStore kv.Storage, targetKS string) (*session, error) {
	if currKSStore.GetKeyspace() == targetKS {
		return nil, errors.New("cannot create session for the same keyspace")
	}
	dom, err := domap.Get(currKSStore)
	if err != nil {
		return nil, err
	}

	store, err := dom.GetKSStore(targetKS)
	if err != nil {
		return nil, err
	}
	infoCache, err := dom.GetKSInfoCache(targetKS)
	if err != nil {
		return nil, err
	}
	// TODO: use the schema validator of the target keyspace when we implement
	// the info schema syncer for cross keyspace access.
	return createSessionWithOpt(store, nil, isvalidator.NewNoop(), infoCache, nil)
}

func createSessionWithOpt(
	store kv.Storage,
	dom *domain.Domain,
	schemaValidator validatorapi.Validator,
	infoCache *infoschema.InfoCache,
	opt *Opt,
) (*session, error) {
	var ddlOwnerMgr owner.Manager
	if dom != nil {
		// we don't set dom for cross keyspace access.
		ddlOwnerMgr = dom.DDL().OwnerManager()
	}
	crossKS := dom == nil
	s := &session{
		dom:                   dom,
		crossKS:               crossKS,
		schemaValidator:       schemaValidator,
		infoCache:             infoCache,
		store:                 store,
		ddlOwnerManager:       ddlOwnerMgr,
		client:                store.GetClient(),
		mppClient:             store.GetMPPClient(),
		stmtStats:             stmtstats.CreateStatementStats(),
		sessionStatesHandlers: make(map[sessionstates.SessionStateType]sessionctx.SessionStatesHandler),
	}
	s.sessionVars = variable.NewSessionVars(s)
	s.exprctx = sessionexpr.NewExprContext(s)
	s.pctx = newPlanContextImpl(s)
	s.tblctx = tblsession.NewMutateContext(s)

	s.functionUsageMu.builtinFunctionUsage = make(telemetry.BuiltinFunctionsUsage)
	if opt != nil && opt.PreparedPlanCache != nil {
		s.sessionPlanCache = opt.PreparedPlanCache
	}
	s.mu.values = make(map[fmt.Stringer]any)
	s.lockedTables = make(map[int64]model.TableLockTpInfo)
	s.advisoryLocks = make(map[string]*advisoryLock)

	// session implements variable.GlobalVarAccessor. Bind it to ctx.
	s.sessionVars.GlobalVarsAccessor = s
	s.txn.init()

	sessionBindHandle := bindinfo.NewSessionBindingHandle()
	s.SetValue(bindinfo.SessionBindInfoKeyType, sessionBindHandle)
	s.SetSessionStatesHandler(sessionstates.StateBinding, sessionBindHandle)
	return s, nil
}

// attachStatsCollector attaches the stats collector in the dom for the session
func attachStatsCollector(s *session, dom *domain.Domain) *session {
	if dom.StatsHandle() != nil && dom.StatsUpdating() {
		if s.statsCollector == nil {
			s.statsCollector = dom.StatsHandle().NewSessionStatsItem().(*usage.SessionStatsItem)
		}
		if s.idxUsageCollector == nil && config.GetGlobalConfig().Instance.EnableCollectExecutionInfo.Load() {
			s.idxUsageCollector = dom.StatsHandle().NewSessionIndexUsageCollector()
		}
	}

	return s
}

// detachStatsCollector removes the stats collector in the session
func detachStatsCollector(s *session) *session {
	if s.statsCollector != nil {
		s.statsCollector.Delete()
		s.statsCollector = nil
	}
	if s.idxUsageCollector != nil {
		s.idxUsageCollector.Flush()
		s.idxUsageCollector = nil
	}
	return s
}

// CreateSessionWithDomain creates a new Session and binds it with a Domain.
// We need this because when we start DDL in Domain, the DDL need a session
// to change some system tables. But at that time, we have been already in
// a lock context, which cause we can't call createSession directly.
func CreateSessionWithDomain(store kv.Storage, dom *domain.Domain) (*session, error) {
	return createSessionWithOpt(store, dom, dom.GetSchemaValidator(), dom.InfoCache(), nil)
}

const (
	notBootstrapped = 0
)

func mustGetStoreBootstrapVersion(store kv.Storage) int64 {
	var ver int64
	// check in kv store
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	err := kv.RunInNewTxn(ctx, store, false, func(_ context.Context, txn kv.Transaction) error {
		var err error
		t := meta.NewReader(txn)
		ver, err = t.GetBootstrapVersion()
		return err
	})
	if err != nil {
		logutil.BgLogger().Fatal("get store bootstrap version failed", zap.Error(err))
	}
	return ver
}

func getStoreBootstrapVersionWithCache(store kv.Storage) int64 {
	// check in memory
	_, ok := store.GetOption(StoreBootstrappedKey)
	if ok {
		return currentBootstrapVersion
	}

	ver := mustGetStoreBootstrapVersion(store)

	if ver > notBootstrapped {
		// here mean memory is not ok, but other server has already finished it
		store.SetOption(StoreBootstrappedKey, true)
	}

	modifyBootstrapVersionForTest(ver)
	return ver
}

func finishBootstrap(store kv.Storage) {
	store.SetOption(StoreBootstrappedKey, true)

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnBootstrap)
	err := kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMutator(txn)
		err := t.FinishBootstrap(currentBootstrapVersion)
		return err
	})
	if err != nil {
		logutil.BgLogger().Fatal("finish bootstrap failed",
			zap.Error(err))
	}
}

const quoteCommaQuote = "', '"

// loadCommonGlobalVariablesIfNeeded loads and applies commonly used global variables for the session.
func (s *session) loadCommonGlobalVariablesIfNeeded() error {
	vars := s.sessionVars
	if vars.CommonGlobalLoaded {
		return nil
	}
	if s.Value(sessionctx.Initing) != nil {
		// When running bootstrap or upgrade, we should not access global storage.
		// But we need to init max_allowed_packet to use concat function during bootstrap or upgrade.
		err := vars.SetSystemVar(vardef.MaxAllowedPacket, strconv.FormatUint(vardef.DefMaxAllowedPacket, 10))
		if err != nil {
			logutil.BgLogger().Error("set system variable max_allowed_packet error", zap.Error(err))
		}
		return nil
	}

	vars.CommonGlobalLoaded = true

	// Deep copy sessionvar cache
	sessionCache, err := domain.GetDomain(s).GetSessionCache()
	if err != nil {
		return err
	}
	for varName, varVal := range sessionCache {
		if _, ok := vars.GetSystemVar(varName); !ok {
			err = vars.SetSystemVarWithRelaxedValidation(varName, varVal)
			if err != nil {
				if variable.ErrUnknownSystemVar.Equal(err) {
					continue // sessionCache is stale; sysvar has likely been unregistered
				}
				return err
			}
		}
	}
	// when client set Capability Flags CLIENT_INTERACTIVE, init wait_timeout with interactive_timeout
	if vars.ClientCapability&mysql.ClientInteractive > 0 {
		if varVal, ok := vars.GetSystemVar(vardef.InteractiveTimeout); ok {
			if err := vars.SetSystemVar(vardef.WaitTimeout, varVal); err != nil {
				return err
			}
		}
	}
	return nil
}

// PrepareTxnCtx begins a transaction, and creates a new transaction context.
// It is called before we execute a sql query.
func (s *session) PrepareTxnCtx(ctx context.Context) error {
	s.currentCtx = ctx
	if s.txn.validOrPending() {
		return nil
	}

	txnMode := ast.Optimistic
	if !s.sessionVars.IsAutocommit() || (config.GetGlobalConfig().PessimisticTxn.
		PessimisticAutoCommit.Load() && !s.GetSessionVars().BulkDMLEnabled) {
		if s.sessionVars.TxnMode == ast.Pessimistic {
			txnMode = ast.Pessimistic
		}
	}

	if s.sessionVars.RetryInfo.Retrying {
		txnMode = ast.Pessimistic
	}

	return sessiontxn.GetTxnManager(s).EnterNewTxn(ctx, &sessiontxn.EnterNewTxnRequest{
		Type:    sessiontxn.EnterNewTxnBeforeStmt,
		TxnMode: txnMode,
	})
}

// PrepareTSFuture uses to try to get ts future.
func (s *session) PrepareTSFuture(ctx context.Context, future oracle.Future, scope string) error {
	if s.txn.Valid() {
		return errors.New("cannot prepare ts future when txn is valid")
	}

	failpoint.Inject("assertTSONotRequest", func() {
		if _, ok := future.(sessiontxn.ConstantFuture); !ok && !s.isInternal() {
			panic("tso shouldn't be requested")
		}
	})

	failpoint.InjectContext(ctx, "mockGetTSFail", func() {
		future = txnFailFuture{}
	})

	s.txn.changeToPending(&txnFuture{
		future:                          future,
		store:                           s.store,
		txnScope:                        scope,
		pipelined:                       s.usePipelinedDmlOrWarn(ctx),
		pipelinedFlushConcurrency:       s.GetSessionVars().PipelinedFlushConcurrency,
		pipelinedResolveLockConcurrency: s.GetSessionVars().PipelinedResolveLockConcurrency,
		pipelinedWriteThrottleRatio:     s.GetSessionVars().PipelinedWriteThrottleRatio,
	})
	return nil
}

// GetPreparedTxnFuture returns the TxnFuture if it is valid or pending.
// It returns nil otherwise.
func (s *session) GetPreparedTxnFuture() sessionctx.TxnFuture {
	if !s.txn.validOrPending() {
		return nil
	}
	return &s.txn
}

// RefreshTxnCtx implements context.RefreshTxnCtx interface.
func (s *session) RefreshTxnCtx(ctx context.Context) error {
	var commitDetail *tikvutil.CommitDetails
	ctx = context.WithValue(ctx, tikvutil.CommitDetailCtxKey, &commitDetail)
	err := s.doCommit(ctx)
	if commitDetail != nil {
		s.GetSessionVars().StmtCtx.MergeExecDetails(commitDetail)
	}
	if err != nil {
		return err
	}

	s.updateStatsDeltaToCollector()

	return sessiontxn.NewTxn(ctx, s)
}

// GetStore gets the store of session.
func (s *session) GetStore() kv.Storage {
	return s.store
}

func (s *session) ShowProcess() *util.ProcessInfo {
	return s.processInfo.Load()
}

// GetStartTSFromSession returns the startTS in the session `se`
func GetStartTSFromSession(se any) (startTS, processInfoID uint64) {
	tmp, ok := se.(*session)
	if !ok {
		logutil.BgLogger().Error("GetStartTSFromSession failed, can't transform to session struct")
		return 0, 0
	}
	txnInfo := tmp.TxnInfo()
	if txnInfo != nil {
		startTS = txnInfo.StartTS
		if txnInfo.ProcessInfo != nil {
			processInfoID = txnInfo.ProcessInfo.ConnectionID
		}
	}
	logutil.BgLogger().Debug(
		"GetStartTSFromSession getting startTS of internal session",
		zap.Uint64("startTS", startTS), zap.Time("start time", oracle.GetTimeFromTS(startTS)))

	return startTS, processInfoID
}

// logStmt logs some crucial SQL including: CREATE USER/GRANT PRIVILEGE/CHANGE PASSWORD/DDL etc and normal SQL
// if variable.ProcessGeneralLog is set.
func logStmt(execStmt *executor.ExecStmt, s *session) {
	vars := s.GetSessionVars()
	isCrucial := false
	switch stmt := execStmt.StmtNode.(type) {
	case *ast.DropIndexStmt:
		isCrucial = true
		if stmt.IsHypo {
			isCrucial = false
		}
	case *ast.CreateIndexStmt:
		isCrucial = true
		if stmt.IndexOption != nil && stmt.IndexOption.Tp == ast.IndexTypeHypo {
			isCrucial = false
		}
	case *ast.CreateUserStmt, *ast.DropUserStmt, *ast.AlterUserStmt, *ast.SetPwdStmt, *ast.GrantStmt,
		*ast.RevokeStmt, *ast.AlterTableStmt, *ast.CreateDatabaseStmt, *ast.CreateTableStmt,
		*ast.DropDatabaseStmt, *ast.DropTableStmt, *ast.RenameTableStmt, *ast.TruncateTableStmt,
		*ast.RenameUserStmt, *ast.CreateBindingStmt, *ast.DropBindingStmt, *ast.SetBindingStmt, *ast.BRIEStmt:
		isCrucial = true
	}

	if isCrucial {
		user := vars.User
		schemaVersion := s.GetInfoSchema().SchemaMetaVersion()
		if ss, ok := execStmt.StmtNode.(ast.SensitiveStmtNode); ok {
			logutil.BgLogger().Info("CRUCIAL OPERATION",
				zap.Uint64("conn", vars.ConnectionID),
				zap.Int64("schemaVersion", schemaVersion),
				zap.String("secure text", ss.SecureText()),
				zap.Stringer("user", user))
		} else {
			logutil.BgLogger().Info("CRUCIAL OPERATION",
				zap.Uint64("conn", vars.ConnectionID),
				zap.Int64("schemaVersion", schemaVersion),
				zap.String("cur_db", vars.CurrentDB),
				zap.String("sql", execStmt.StmtNode.Text()),
				zap.Stringer("user", user))
		}
	} else {
		logGeneralQuery(execStmt, s, false)
	}
}

func logGeneralQuery(execStmt *executor.ExecStmt, s *session, isPrepared bool) {
	vars := s.GetSessionVars()
	if vardef.ProcessGeneralLog.Load() && !vars.InRestrictedSQL {
		var query string
		if isPrepared {
			query = execStmt.OriginText()
		} else {
			query = execStmt.GetTextToLog(false)
		}

		query = executor.QueryReplacer.Replace(query)
		if vars.EnableRedactLog != errors.RedactLogEnable {
			query += redact.String(vars.EnableRedactLog, vars.PlanCacheParams.String())
		}

		fields := []zapcore.Field{
			zap.Uint64("conn", vars.ConnectionID),
			zap.String("session_alias", vars.SessionAlias),
			zap.String("user", vars.User.LoginString()),
			zap.Int64("schemaVersion", s.GetInfoSchema().SchemaMetaVersion()),
			zap.Uint64("txnStartTS", vars.TxnCtx.StartTS),
			zap.Uint64("forUpdateTS", vars.TxnCtx.GetForUpdateTS()),
			zap.Bool("isReadConsistency", vars.IsIsolation(ast.ReadCommitted)),
			zap.String("currentDB", vars.CurrentDB),
			zap.Bool("isPessimistic", vars.TxnCtx.IsPessimistic),
			zap.String("sessionTxnMode", vars.GetReadableTxnMode()),
			zap.String("sql", query),
		}
		if ot := execStmt.OriginText(); ot != execStmt.Text() {
			fields = append(fields, zap.String("originText", strconv.Quote(ot)))
		}
		logutil.GeneralLogger.Info("GENERAL_LOG", fields...)
	}
}

func (s *session) recordOnTransactionExecution(err error, counter int, duration float64, isInternal bool) {
	if s.sessionVars.TxnCtx.IsPessimistic {
		if err != nil {
			if isInternal {
				session_metrics.TransactionDurationPessimisticAbortInternal.Observe(duration)
				session_metrics.StatementPerTransactionPessimisticErrorInternal.Observe(float64(counter))
			} else {
				session_metrics.TransactionDurationPessimisticAbortGeneral.Observe(duration)
				session_metrics.StatementPerTransactionPessimisticErrorGeneral.Observe(float64(counter))
			}
		} else {
			if isInternal {
				session_metrics.TransactionDurationPessimisticCommitInternal.Observe(duration)
				session_metrics.StatementPerTransactionPessimisticOKInternal.Observe(float64(counter))
			} else {
				session_metrics.TransactionDurationPessimisticCommitGeneral.Observe(duration)
				session_metrics.StatementPerTransactionPessimisticOKGeneral.Observe(float64(counter))
			}
		}
	} else {
		if err != nil {
			if isInternal {
				session_metrics.TransactionDurationOptimisticAbortInternal.Observe(duration)
				session_metrics.StatementPerTransactionOptimisticErrorInternal.Observe(float64(counter))
			} else {
				session_metrics.TransactionDurationOptimisticAbortGeneral.Observe(duration)
				session_metrics.StatementPerTransactionOptimisticErrorGeneral.Observe(float64(counter))
			}
		} else {
			if isInternal {
				session_metrics.TransactionDurationOptimisticCommitInternal.Observe(duration)
				session_metrics.StatementPerTransactionOptimisticOKInternal.Observe(float64(counter))
			} else {
				session_metrics.TransactionDurationOptimisticCommitGeneral.Observe(duration)
				session_metrics.StatementPerTransactionOptimisticOKGeneral.Observe(float64(counter))
			}
		}
	}
}

func (s *session) checkPlacementPolicyBeforeCommit(ctx context.Context) error {
	var err error
	// Get the txnScope of the transaction we're going to commit.
	txnScope := s.GetSessionVars().TxnCtx.TxnScope
	if txnScope == "" {
		txnScope = kv.GlobalTxnScope
	}
	if txnScope != kv.GlobalTxnScope {
		is := s.GetInfoSchema().(infoschema.InfoSchema)
		deltaMap := s.GetSessionVars().TxnCtx.TableDeltaMap
		for physicalTableID := range deltaMap {
			var tableName string
			var partitionName string
			tblInfo, _, partInfo := is.FindTableByPartitionID(physicalTableID)
			if tblInfo != nil && partInfo != nil {
				tableName = tblInfo.Meta().Name.String()
				partitionName = partInfo.Name.String()
			} else {
				tblInfo, _ := is.TableByID(ctx, physicalTableID)
				tableName = tblInfo.Meta().Name.String()
			}
			bundle, ok := is.PlacementBundleByPhysicalTableID(physicalTableID)
			if !ok {
				errMsg := fmt.Sprintf("table %v doesn't have placement policies with txn_scope %v",
					tableName, txnScope)
				if len(partitionName) > 0 {
					errMsg = fmt.Sprintf("table %v's partition %v doesn't have placement policies with txn_scope %v",
						tableName, partitionName, txnScope)
				}
				err = dbterror.ErrInvalidPlacementPolicyCheck.GenWithStackByArgs(errMsg)
				break
			}
			dcLocation, ok := bundle.GetLeaderDC(placement.DCLabelKey)
			if !ok {
				errMsg := fmt.Sprintf("table %v's leader placement policy is not defined", tableName)
				if len(partitionName) > 0 {
					errMsg = fmt.Sprintf("table %v's partition %v's leader placement policy is not defined", tableName, partitionName)
				}
				err = dbterror.ErrInvalidPlacementPolicyCheck.GenWithStackByArgs(errMsg)
				break
			}
			if dcLocation != txnScope {
				errMsg := fmt.Sprintf("table %v's leader location %v is out of txn_scope %v", tableName, dcLocation, txnScope)
				if len(partitionName) > 0 {
					errMsg = fmt.Sprintf("table %v's partition %v's leader location %v is out of txn_scope %v",
						tableName, partitionName, dcLocation, txnScope)
				}
				err = dbterror.ErrInvalidPlacementPolicyCheck.GenWithStackByArgs(errMsg)
				break
			}
			// FIXME: currently we assume the physicalTableID is the partition ID. In future, we should consider the situation
			// if the physicalTableID belongs to a Table.
			partitionID := physicalTableID
			tbl, _, partitionDefInfo := is.FindTableByPartitionID(partitionID)
			if tbl != nil {
				tblInfo := tbl.Meta()
				state := tblInfo.Partition.GetStateByID(partitionID)
				if state == model.StateGlobalTxnOnly {
					err = dbterror.ErrInvalidPlacementPolicyCheck.GenWithStackByArgs(
						fmt.Sprintf("partition %s of table %s can not be written by local transactions when its placement policy is being altered",
							tblInfo.Name, partitionDefInfo.Name))
					break
				}
			}
		}
	}
	return err
}

func (s *session) SetPort(port string) {
	s.sessionVars.Port = port
}

// GetTxnWriteThroughputSLI implements the Context interface.
func (s *session) GetTxnWriteThroughputSLI() *sli.TxnWriteThroughputSLI {
	return &s.txn.writeSLI
}

// GetInfoSchema returns snapshotInfoSchema if snapshot schema is set.
// Transaction infoschema is returned if inside an explicit txn.
// Otherwise the latest infoschema is returned.
func (s *session) GetInfoSchema() infoschemactx.MetaOnlyInfoSchema {
	vars := s.GetSessionVars()
	var is infoschema.InfoSchema
	if snap, ok := vars.SnapshotInfoschema.(infoschema.InfoSchema); ok {
		logutil.BgLogger().Info("use snapshot schema", zap.Uint64("conn", vars.ConnectionID), zap.Int64("schemaVersion", snap.SchemaMetaVersion()))
		is = snap
	} else {
		vars.TxnCtxMu.Lock()
		if vars.TxnCtx != nil {
			if tmp, ok := vars.TxnCtx.InfoSchema.(infoschema.InfoSchema); ok {
				is = tmp
			}
		}
		vars.TxnCtxMu.Unlock()
	}

	if is == nil {
		is = s.infoCache.GetLatest()
	}

	// Override the infoschema if the session has temporary table.
	return temptable.AttachLocalTemporaryTableInfoSchema(s, is)
}

func (s *session) GetLatestInfoSchema() infoschemactx.MetaOnlyInfoSchema {
	is := s.infoCache.GetLatest()
	extIs := &infoschema.SessionExtendedInfoSchema{InfoSchema: is}
	return temptable.AttachLocalTemporaryTableInfoSchema(s, extIs)
}

func (s *session) GetLatestISWithoutSessExt() infoschemactx.MetaOnlyInfoSchema {
	return s.infoCache.GetLatest()
}

func (s *session) GetSQLServer() sqlsvrapi.Server {
	return s.dom.(sqlsvrapi.Server)
}

func (s *session) IsCrossKS() bool {
	return s.crossKS
}

func (s *session) GetSchemaValidator() validatorapi.Validator {
	return s.schemaValidator
}

func getSnapshotInfoSchema(s sessionctx.Context, snapshotTS uint64) (infoschema.InfoSchema, error) {
	is, err := domain.GetDomain(s).GetSnapshotInfoSchema(snapshotTS)
	if err != nil {
		return nil, err
	}
	// Set snapshot does not affect the witness of the local temporary table.
	// The session always see the latest temporary tables.
	return temptable.AttachLocalTemporaryTableInfoSchema(s, is), nil
}

func (s *session) updateTelemetryMetric(es *executor.ExecStmt) {
	if es.Ti == nil {
		return
	}
	if s.isInternal() {
		return
	}

	ti := es.Ti
	if ti.UseRecursive {
		session_metrics.TelemetryCTEUsageRecurCTE.Inc()
	} else if ti.UseNonRecursive {
		session_metrics.TelemetryCTEUsageNonRecurCTE.Inc()
	} else {
		session_metrics.TelemetryCTEUsageNotCTE.Inc()
	}

	if ti.UseIndexMerge {
		session_metrics.TelemetryIndexMerge.Inc()
	}

	if ti.UseMultiSchemaChange {
		session_metrics.TelemetryMultiSchemaChangeUsage.Inc()
	}

	if ti.UseFlashbackToCluster {
		session_metrics.TelemetryFlashbackClusterUsage.Inc()
	}

	if ti.UseExchangePartition {
		session_metrics.TelemetryExchangePartitionUsage.Inc()
	}

	if ti.PartitionTelemetry != nil {
		if ti.PartitionTelemetry.UseTablePartition {
			session_metrics.TelemetryTablePartitionUsage.Inc()
			session_metrics.TelemetryTablePartitionMaxPartitionsUsage.Add(float64(ti.PartitionTelemetry.TablePartitionMaxPartitionsNum))
		}
		if ti.PartitionTelemetry.UseTablePartitionList {
			session_metrics.TelemetryTablePartitionListUsage.Inc()
		}
		if ti.PartitionTelemetry.UseTablePartitionRange {
			session_metrics.TelemetryTablePartitionRangeUsage.Inc()
		}
		if ti.PartitionTelemetry.UseTablePartitionHash {
			session_metrics.TelemetryTablePartitionHashUsage.Inc()
		}
		if ti.PartitionTelemetry.UseTablePartitionRangeColumns {
			session_metrics.TelemetryTablePartitionRangeColumnsUsage.Inc()
		}
		if ti.PartitionTelemetry.UseTablePartitionRangeColumnsGt1 {
			session_metrics.TelemetryTablePartitionRangeColumnsGt1Usage.Inc()
		}
		if ti.PartitionTelemetry.UseTablePartitionRangeColumnsGt2 {
			session_metrics.TelemetryTablePartitionRangeColumnsGt2Usage.Inc()
		}
		if ti.PartitionTelemetry.UseTablePartitionRangeColumnsGt3 {
			session_metrics.TelemetryTablePartitionRangeColumnsGt3Usage.Inc()
		}
		if ti.PartitionTelemetry.UseTablePartitionListColumns {
			session_metrics.TelemetryTablePartitionListColumnsUsage.Inc()
		}
		if ti.PartitionTelemetry.UseCreateIntervalPartition {
			session_metrics.TelemetryTablePartitionCreateIntervalUsage.Inc()
		}
		if ti.PartitionTelemetry.UseAddIntervalPartition {
			session_metrics.TelemetryTablePartitionAddIntervalUsage.Inc()
		}
		if ti.PartitionTelemetry.UseDropIntervalPartition {
			session_metrics.TelemetryTablePartitionDropIntervalUsage.Inc()
		}
		if ti.PartitionTelemetry.UseCompactTablePartition {
			session_metrics.TelemetryTableCompactPartitionUsage.Inc()
		}
		if ti.PartitionTelemetry.UseReorganizePartition {
			session_metrics.TelemetryReorganizePartitionUsage.Inc()
		}
	}

	if ti.AccountLockTelemetry != nil {
		session_metrics.TelemetryLockUserUsage.Add(float64(ti.AccountLockTelemetry.LockUser))
		session_metrics.TelemetryUnlockUserUsage.Add(float64(ti.AccountLockTelemetry.UnlockUser))
		session_metrics.TelemetryCreateOrAlterUserUsage.Add(float64(ti.AccountLockTelemetry.CreateOrAlterUser))
	}

	if ti.UseTableLookUp.Load() && s.sessionVars.StoreBatchSize > 0 {
		session_metrics.TelemetryStoreBatchedUsage.Inc()
	}
}

// GetBuiltinFunctionUsage returns the replica of counting of builtin function usage
func (s *session) GetBuiltinFunctionUsage() map[string]uint32 {
	replica := make(map[string]uint32)
	s.functionUsageMu.RLock()
	defer s.functionUsageMu.RUnlock()
	for key, value := range s.functionUsageMu.builtinFunctionUsage {
		replica[key] = value
	}
	return replica
}

// BuiltinFunctionUsageInc increase the counting of the builtin function usage
func (s *session) BuiltinFunctionUsageInc(scalarFuncSigName string) {
	s.functionUsageMu.Lock()
	defer s.functionUsageMu.Unlock()
	s.functionUsageMu.builtinFunctionUsage.Inc(scalarFuncSigName)
}

func (s *session) GetStmtStats() *stmtstats.StatementStats {
	return s.stmtStats
}

// SetMemoryFootprintChangeHook sets the hook that is called when the memdb changes its size.
// Call this after s.txn becomes valid, since TxnInfo is initialized when the txn becomes valid.
func (s *session) SetMemoryFootprintChangeHook() {
	if s.txn.MemHookSet() {
		return
	}
	if config.GetGlobalConfig().Performance.TxnTotalSizeLimit != config.DefTxnTotalSizeLimit {
		// if the user manually specifies the config, don't involve the new memory tracker mechanism, let the old config
		// work as before.
		return
	}
	hook := func(mem uint64) {
		if s.sessionVars.MemDBFootprint == nil {
			tracker := memory.NewTracker(memory.LabelForMemDB, -1)
			tracker.AttachTo(s.sessionVars.MemTracker)
			s.sessionVars.MemDBFootprint = tracker
		}
		s.sessionVars.MemDBFootprint.ReplaceBytesUsed(int64(mem))
	}
	s.txn.SetMemoryFootprintChangeHook(hook)
}

// EncodeSessionStates implements SessionStatesHandler.EncodeSessionStates interface.
func (s *session) EncodeSessionStates(ctx context.Context,
	_ sessionctx.Context, sessionStates *sessionstates.SessionStates) error {
	// Transaction status is hard to encode, so we do not support it.
	s.txn.mu.Lock()
	valid := s.txn.Valid()
	s.txn.mu.Unlock()
	if valid {
		return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs("session has an active transaction")
	}
	// Data in local temporary tables is hard to encode, so we do not support it.
	// Check temporary tables here to avoid circle dependency.
	if s.sessionVars.LocalTemporaryTables != nil {
		localTempTables := s.sessionVars.LocalTemporaryTables.(*infoschema.SessionTables)
		if localTempTables.Count() > 0 {
			return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs("session has local temporary tables")
		}
	}
	// The advisory locks will be released when the session is closed.
	if len(s.advisoryLocks) > 0 {
		return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs("session has advisory locks")
	}
	// The TableInfo stores session ID and server ID, so the session cannot be migrated.
	if len(s.lockedTables) > 0 {
		return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs("session has locked tables")
	}
	// It's insecure to migrate sandBoxMode because users can fake it.
	if s.InSandBoxMode() {
		return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs("session is in sandbox mode")
	}

	if err := s.sessionVars.EncodeSessionStates(ctx, sessionStates); err != nil {
		return err
	}
	sessionStates.ResourceGroupName = s.sessionVars.ResourceGroupName

	hasRestrictVarPriv := false
	checker := privilege.GetPrivilegeManager(s)
	if checker == nil || checker.RequestDynamicVerification(s.sessionVars.ActiveRoles, "RESTRICTED_VARIABLES_ADMIN", false) {
		hasRestrictVarPriv = true
	}
	// Encode session variables. We put it here instead of SessionVars to avoid cycle import.
	sessionStates.SystemVars = make(map[string]string)
	for _, sv := range variable.GetSysVars() {
		switch {
		case sv.HasNoneScope(), !sv.HasSessionScope():
			// Hidden attribute is deprecated.
			// None-scoped variables cannot be modified.
			// Noop variables should also be migrated even if they are noop.
			continue
		case sv.ReadOnly:
			// Skip read-only variables here. We encode them into SessionStates manually.
			continue
		}
		// Get all session variables because the default values may change between versions.
		val, keep, err := s.sessionVars.GetSessionStatesSystemVar(sv.Name)
		switch {
		case err != nil:
			return err
		case !keep:
			continue
		case !hasRestrictVarPriv && sem.IsEnabled() && sem.IsInvisibleSysVar(sv.Name):
			// If the variable has a global scope, it should be the same with the global one.
			// Otherwise, it should be the same with the default value.
			defaultVal := sv.Value
			if sv.HasGlobalScope() {
				// If the session value is the same with the global one, skip it.
				if defaultVal, err = sv.GetGlobalFromHook(ctx, s.sessionVars); err != nil {
					return err
				}
			}
			if val != defaultVal {
				// Case 1: the RESTRICTED_VARIABLES_ADMIN is revoked after setting the session variable.
				// Case 2: the global variable is updated after the session is created.
				// In any case, the variable can't be set in the new session, so give up.
				return sessionstates.ErrCannotMigrateSession.GenWithStackByArgs(fmt.Sprintf("session has set invisible variable '%s'", sv.Name))
			}
		default:
			sessionStates.SystemVars[sv.Name] = val
		}
	}

	// Encode prepared statements and sql bindings.
	for _, handler := range s.sessionStatesHandlers {
		if err := handler.EncodeSessionStates(ctx, s, sessionStates); err != nil {
			return err
		}
	}
	return nil
}

// DecodeSessionStates implements SessionStatesHandler.DecodeSessionStates interface.
func (s *session) DecodeSessionStates(ctx context.Context,
	_ sessionctx.Context, sessionStates *sessionstates.SessionStates) error {
	// Decode prepared statements and sql bindings.
	for _, handler := range s.sessionStatesHandlers {
		if err := handler.DecodeSessionStates(ctx, s, sessionStates); err != nil {
			return err
		}
	}

	// Decode session variables.
	names := variable.OrderByDependency(sessionStates.SystemVars)
	// Some variables must be set before others, e.g. tidb_enable_noop_functions should be before noop variables.
	for _, name := range names {
		val := sessionStates.SystemVars[name]
		// Experimental system variables may change scope, data types, or even be removed.
		// We just ignore the errors and continue.
		if err := s.sessionVars.SetSystemVar(name, val); err != nil {
			logutil.Logger(ctx).Warn("set session variable during decoding session states error",
				zap.String("name", name), zap.String("value", val), zap.Error(err))
		}
	}

	// Put resource group privilege check from sessionVars to session to avoid circular dependency.
	if sessionStates.ResourceGroupName != s.sessionVars.ResourceGroupName {
		hasPriv := true
		if vardef.EnableResourceControlStrictMode.Load() {
			checker := privilege.GetPrivilegeManager(s)
			if checker != nil {
				hasRgAdminPriv := checker.RequestDynamicVerification(s.sessionVars.ActiveRoles, "RESOURCE_GROUP_ADMIN", false)
				hasRgUserPriv := checker.RequestDynamicVerification(s.sessionVars.ActiveRoles, "RESOURCE_GROUP_USER", false)
				hasPriv = hasRgAdminPriv || hasRgUserPriv
			}
		}
		if hasPriv {
			s.sessionVars.SetResourceGroupName(sessionStates.ResourceGroupName)
		} else {
			logutil.Logger(ctx).Warn("set session states error, no privilege to set resource group, skip changing resource group",
				zap.String("source_resource_group", s.sessionVars.ResourceGroupName), zap.String("target_resource_group", sessionStates.ResourceGroupName))
		}
	}

	// Decoding session vars / prepared statements may override stmt ctx, such as warnings,
	// so we decode stmt ctx at last.
	return s.sessionVars.DecodeSessionStates(ctx, sessionStates)
}

func (s *session) setRequestSource(ctx context.Context, stmtLabel string, stmtNode ast.StmtNode) {
	if !s.isInternal() {
		if txn, _ := s.Txn(false); txn != nil && txn.Valid() {
			if txn.IsPipelined() {
				stmtLabel = "pdml"
			}
			txn.SetOption(kv.RequestSourceType, stmtLabel)
		}
		s.sessionVars.RequestSourceType = stmtLabel
		return
	}
	if source := ctx.Value(kv.RequestSourceKey); source != nil {
		requestSource := source.(kv.RequestSource)
		if requestSource.RequestSourceType != "" {
			s.sessionVars.RequestSourceType = requestSource.RequestSourceType
			return
		}
	}
	// panic in test mode in case there are requests without source in the future.
	// log warnings in production mode.
	if intest.EnableInternalCheck {
		panic("unexpected no source type context, if you see this error, " +
			"the `RequestSourceTypeKey` is missing in your context")
	}
	logutil.Logger(ctx).Warn("unexpected no source type context, if you see this warning, "+
		"the `RequestSourceTypeKey` is missing in the context",
		zap.Bool("internal", s.isInternal()),
		zap.String("sql", stmtNode.Text()))
}

// NewStmtIndexUsageCollector creates a new `*indexusage.StmtIndexUsageCollector` based on the internal session index
// usage collector
func (s *session) NewStmtIndexUsageCollector() *indexusage.StmtIndexUsageCollector {
	if s.idxUsageCollector == nil {
		return nil
	}

	return indexusage.NewStmtIndexUsageCollector(s.idxUsageCollector)
}

// usePipelinedDmlOrWarn returns the current statement can be executed as a pipelined DML.
func (s *session) usePipelinedDmlOrWarn(ctx context.Context) bool {
	if !s.sessionVars.BulkDMLEnabled {
		return false
	}
	stmtCtx := s.sessionVars.StmtCtx
	if stmtCtx == nil {
		return false
	}
	if stmtCtx.IsReadOnly {
		return false
	}
	vars := s.GetSessionVars()
	if !vars.TxnCtx.EnableMDL {
		stmtCtx.AppendWarning(
			errors.New(
				"Pipelined DML can not be used without Metadata Lock. Fallback to standard mode",
			),
		)
		return false
	}
	if (vars.BatchCommit || vars.BatchInsert || vars.BatchDelete) && vars.DMLBatchSize > 0 && vardef.EnableBatchDML.Load() {
		stmtCtx.AppendWarning(errors.New("Pipelined DML can not be used with the deprecated Batch DML. Fallback to standard mode"))
		return false
	}
	if !(stmtCtx.InInsertStmt || stmtCtx.InDeleteStmt || stmtCtx.InUpdateStmt) {
		if !stmtCtx.IsReadOnly {
			stmtCtx.AppendWarning(errors.New("Pipelined DML can only be used for auto-commit INSERT, REPLACE, UPDATE or DELETE. Fallback to standard mode"))
		}
		return false
	}
	if s.isInternal() {
		stmtCtx.AppendWarning(errors.New("Pipelined DML can not be used for internal SQL. Fallback to standard mode"))
		return false
	}
	if vars.InTxn() {
		stmtCtx.AppendWarning(errors.New("Pipelined DML can not be used in transaction. Fallback to standard mode"))
		return false
	}
	if !vars.IsAutocommit() {
		stmtCtx.AppendWarning(errors.New("Pipelined DML can only be used in autocommit mode. Fallback to standard mode"))
		return false
	}
	if s.GetSessionVars().ConstraintCheckInPlace {
		// we enforce that pipelined DML must lazily check key.
		stmtCtx.AppendWarning(
			errors.New(
				"Pipelined DML can not be used when tidb_constraint_check_in_place=ON. " +
					"Fallback to standard mode",
			),
		)
		return false
	}
	is, ok := s.GetLatestInfoSchema().(infoschema.InfoSchema)
	if !ok {
		stmtCtx.AppendWarning(errors.New("Pipelined DML failed to get latest InfoSchema. Fallback to standard mode"))
		return false
	}
	for _, t := range stmtCtx.Tables {
		// get table schema from current infoschema
		tbl, err := is.TableByName(ctx, ast.NewCIStr(t.DB), ast.NewCIStr(t.Table))
		if err != nil {
			stmtCtx.AppendWarning(errors.New("Pipelined DML failed to get table schema. Fallback to standard mode"))
			return false
		}
		if tbl.Meta().IsView() {
			stmtCtx.AppendWarning(errors.New("Pipelined DML can not be used on view. Fallback to standard mode"))
			return false
		}
		if tbl.Meta().IsSequence() {
			stmtCtx.AppendWarning(errors.New("Pipelined DML can not be used on sequence. Fallback to standard mode"))
			return false
		}
		if vars.ForeignKeyChecks && (len(tbl.Meta().ForeignKeys) > 0 || len(is.GetTableReferredForeignKeys(t.DB, t.Table)) > 0) {
			stmtCtx.AppendWarning(
				errors.New(
					"Pipelined DML can not be used on table with foreign keys when foreign_key_checks = ON. Fallback to standard mode",
				),
			)
			return false
		}
		if tbl.Meta().TempTableType != model.TempTableNone {
			stmtCtx.AppendWarning(
				errors.New(
					"Pipelined DML can not be used on temporary tables. " +
						"Fallback to standard mode",
				),
			)
			return false
		}
		if tbl.Meta().TableCacheStatusType != model.TableCacheStatusDisable {
			stmtCtx.AppendWarning(
				errors.New(
					"Pipelined DML can not be used on cached tables. " +
						"Fallback to standard mode",
				),
			)
			return false
		}
	}

	// tidb_dml_type=bulk will invalidate the config pessimistic-auto-commit.
	// The behavior is as if the config is set to false. But we generate a warning for it.
	if config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Load() {
		stmtCtx.AppendWarning(
			errors.New(
				"pessimistic-auto-commit config is ignored in favor of Pipelined DML",
			),
		)
	}
	return true
}

// RemoveLockDDLJobs removes the DDL jobs which doesn't get the metadata lock from job2ver.
func RemoveLockDDLJobs(s types.Session, job2ver map[int64]int64, job2ids map[int64]string, printLog bool) {
	sv := s.GetSessionVars()
	if sv.InRestrictedSQL {
		return
	}
	sv.TxnCtxMu.Lock()
	defer sv.TxnCtxMu.Unlock()
	if sv.TxnCtx == nil {
		return
	}
	sv.GetRelatedTableForMDL().Range(func(tblID, value any) bool {
		for jobID, ver := range job2ver {
			ids := util.Str2Int64Map(job2ids[jobID])
			if _, ok := ids[tblID.(int64)]; ok && value.(int64) < ver {
				delete(job2ver, jobID)
				elapsedTime := time.Since(oracle.GetTimeFromTS(sv.TxnCtx.StartTS))
				if elapsedTime > time.Minute && printLog {
					logutil.BgLogger().Info("old running transaction block DDL", zap.Int64("table ID", tblID.(int64)), zap.Int64("jobID", jobID), zap.Uint64("connection ID", sv.ConnectionID), zap.Duration("elapsed time", elapsedTime))
				} else {
					logutil.BgLogger().Debug("old running transaction block DDL", zap.Int64("table ID", tblID.(int64)), zap.Int64("jobID", jobID), zap.Uint64("connection ID", sv.ConnectionID), zap.Duration("elapsed time", elapsedTime))
				}
			}
		}
		return true
	})
}

// GetDBNames gets the sql layer database names from the session.
func GetDBNames(seVar *variable.SessionVars) []string {
	dbNames := make(map[string]struct{})
	if seVar == nil || !config.GetGlobalConfig().Status.RecordDBLabel {
		return []string{""}
	}
	if seVar.StmtCtx != nil {
		for _, t := range seVar.StmtCtx.Tables {
			dbNames[t.DB] = struct{}{}
		}
	}
	if len(dbNames) == 0 {
		dbNames[seVar.CurrentDB] = struct{}{}
	}
	ns := make([]string, 0, len(dbNames))
	for n := range dbNames {
		ns = append(ns, n)
	}
	return ns
}

// GetCursorTracker returns the internal `cursor.Tracker`
func (s *session) GetCursorTracker() cursor.Tracker {
	return s.cursorTracker
}

// GetCommitWaitGroup returns the internal `sync.WaitGroup` for async commit and secondary key lock cleanup
func (s *session) GetCommitWaitGroup() *sync.WaitGroup {
	return &s.commitWaitGroup
}

// GetDomain get domain from session.
func (s *session) GetDomain() any {
	return s.dom
}
