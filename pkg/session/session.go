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
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"iter"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/config"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/sqlsvrapi"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/expression/sessionexpr"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/infoschema"
	infoschemactx "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/infoschema/validatorapi"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	planctx "github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/session/cursor"
	session_metrics "github.com/pingcap/tidb/pkg/session/metrics"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/session/txninfo"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage/indexusage"
	"github.com/pingcap/tidb/pkg/table/tblctx"
	"github.com/pingcap/tidb/pkg/table/tblsession"
	"github.com/pingcap/tidb/pkg/table/temptable"
	"github.com/pingcap/tidb/pkg/telemetry"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	rangerctx "github.com/pingcap/tidb/pkg/util/ranger/context"
	sem "github.com/pingcap/tidb/pkg/util/sem/compat"
	"github.com/pingcap/tidb/pkg/util/sli"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
	"go.uber.org/zap"
)

func init() {
	executor.CreateSession = func(ctx sessionctx.Context) (sessionctx.Context, error) {
		return CreateSession(ctx.GetStore())
	}
	executor.CloseSession = func(ctx sessionctx.Context) {
		if se, ok := ctx.(sessionapi.Session); ok {
			se.Close()
		}
	}
}

var _ sessionapi.Session = (*session)(nil)

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
	processInfo atomic.Pointer[sessmgr.ProcessInfo]
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
	sessionManager sessmgr.Manager

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

// GetTraceCtx returns the trace context of the session.
func (s *session) GetTraceCtx() context.Context {
	return s.currentCtx
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

func (s *session) SetSessionManager(sm sessmgr.Manager) {
	s.sessionManager = sm
}

func (s *session) GetSessionManager() sessmgr.Manager {
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

func getCrossKSSessionFactory(currKSStore kv.Storage, targetKS string, schemaValidator validatorapi.Validator) pools.Factory {
	facWithDom := getSessionFactoryInternal(currKSStore, func(store kv.Storage, _ *domain.Domain) (*session, error) {
		return createCrossKSSession(store, targetKS, schemaValidator)
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
	return s.replaceGlobalVariablesTableValue(ctx, sv.Name, value, updateLocal)
}

// SetInstanceSysVar implements InstanceVarAccessor.SetInstanceSysVar interface.
func (s *session) SetInstanceSysVar(ctx context.Context, name string, value string) (err error) {
	sv := variable.GetSysVar(name)
	if sv == nil {
		return variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	if value, err = sv.Validate(s.sessionVars, value, vardef.ScopeInstance); err != nil {
		return err
	}
	return sv.SetGlobalFromHook(ctx, s.sessionVars, value, false)
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

const (
	coreSQLToken = 1 << iota
	bypassSQLToken
	isSelectSQLToken

	defOOMRiskCheckDur   = time.Millisecond * 100 // 100ms: sleep duration when mem-arbitrator is at memory risk
	defSuffixSplitDot    = ", "
	defSuffixParseSQL    = defSuffixSplitDot + "path=ParseSQL"
	defSuffixCompilePlan = defSuffixSplitDot + "path=CompilePlan"

	// mem quota for compiling plan per token.
	// 1. prepare tpc-c
	// 2. run tpc-c workload with multiple threads for a few minutes
	// 3. observe the memory consumption of TiDB instance without copr-cache
	// 4. calculate the average memory consumption of compiling plan per token:
	//    executor.(*Compiler).Compile / threads / avg token count per SQL / 2 * 1.2(more 20%)
	defCompilePlanQuotaPerToken = 63091 * 12 / 10

	// mem quota for parsing SQL per token (similar method as above)
	//    session.(*session).ParseSQL / threads / avg token count per SQL / 2 * 1.2(more 20%)
	defParseSQLQuotaPerToken = 12036 * 12 / 10
)

var keySQLToken = map[string]int{
	"select": isSelectSQLToken,
	"from":   coreSQLToken, "insert": coreSQLToken, "update": coreSQLToken, "delete": coreSQLToken, "replace": coreSQLToken,
	// ignore prepare / execute statements
	"explain": bypassSQLToken, "desc": bypassSQLToken, "analyze": bypassSQLToken,
}

// approximate memory quota related token count for parsing a SQL statement which covers most DML statements
// 1. ignore comments
// 2. count keywords, identifiers, numbers, "?", string/identifier literals as one token
// 3. if the SQL has `select` clause, it must have `from` clause: ignore SQL like `select expr()` or `select @@var`
// 4. return 0 if the SQL has NO core token (e.g. `set`, `use`, `begin`, `commit`, `rollback`, etc)

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
	se, clean, err := s.getInternalSession(sqlexec.GetExecOption(nil))
	if err != nil {
		return err
	}
	lock := &advisoryLock{session: se, ctx: context.TODO(), owner: s.ShowProcess().ID, clean: clean}
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
	se, clean, err := s.getInternalSession(sqlexec.GetExecOption(nil))
	if err != nil {
		return 0
	}
	lock := &advisoryLock{session: se, ctx: context.TODO(), owner: s.ShowProcess().ID, clean: clean}
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
	s.sessionVars.WithdrawAllPreparedStmt()
	if s.stmtStats != nil {
		s.stmtStats.SetFinished()
	}
	s.sessionVars.ClearDiskFullOpt()
	if s.sessionPlanCache != nil {
		s.sessionPlanCache.Close()
	}
	if s.sessionVars.ConnectionID != 0 {
		memory.RemovePoolFromGlobalMemArbitrator(s.sessionVars.ConnectionID)
	}
	// Detach session trackers during session cleanup.
	// ANALYZE attaches session MemTracker to GlobalAnalyzeMemoryTracker; without
	// detachment, closed sessions cannot be garbage collected.
	if s.sessionVars.MemTracker != nil {
		s.sessionVars.MemTracker.Detach()
	}
	if s.sessionVars.DiskTracker != nil {
		s.sessionVars.DiskTracker.Detach()
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



func (s *session) ShowProcess() *sessmgr.ProcessInfo {
	return s.processInfo.Load()
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

func (s *session) EncodeStates(ctx context.Context,
	sessionStates *sessionstates.SessionStates) error {
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

func (s *session) DecodeStates(ctx context.Context,
	sessionStates *sessionstates.SessionStates) error {
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
		dbNames[strings.ToLower(seVar.CurrentDB)] = struct{}{}
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
