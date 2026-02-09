package pkdbremoteexec

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/pkdb_remote/pb"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/server/internal/column"
	"github.com/pingcap/tidb/pkg/session"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/zap"
)

// preparedStmtEntry holds a prepared statement with its metadata
type preparedStmtEntry struct {
	stmtID     uint32
	paramCount int
	lastUsed   time.Time
}

// sessionWithStmtCache wraps a session with its prepared statement cache
type sessionWithStmtCache struct {
	session   sessiontypes.Session
	stmtCache map[string]*preparedStmtEntry // key: normalized SQL
	mu        sync.Mutex
	lastUsed  time.Time
	inUse     bool // whether the session is currently in use
}

// Server handles remote execution requests
type Server struct {
	pb.UnimplementedRemoteExecServiceServer

	sm  util.SessionManager
	dom *domain.Domain

	// Session pool for reusing sessions with their prepared statement caches
	// Changed from map[string]*sessionWithStmtCache to a slice-based pool
	// to support multiple concurrent sessions
	sessionPool   []*sessionWithStmtCache
	sessionPoolMu sync.Mutex

	// Worker pool for batch remote execution to reduce goroutine creation overhead
	// and potential morestack issues in FFI scenarios.
	// The pool is elastic: it can grow under burst load and shrink after idle.
	workerPool *remoteExecWorkerPool

	closeOnce     sync.Once
	closeCh       chan struct{}
	cleanupExited chan struct{}
	activeStreams sync.WaitGroup

	// Configuration
	stmtCacheSize      int
	sessionIdleTimeout time.Duration
	maxPoolSize        int // maximum number of sessions in the pool
}

const (
	// defaultWorkerPoolMinSize is the baseline size of the worker pool for batch remote execution.
	// Using a pool helps reduce goroutine creation overhead and potential morestack issues in FFI scenarios.
	defaultWorkerPoolMinSize = 256
	// defaultWorkerPoolSoftMaxSize is the initial soft cap for the elastic worker pool.
	// Under sustained load (e.g. slow requests), the pool can raise the soft cap up to hard max.
	defaultWorkerPoolSoftMaxSize = defaultWorkerPoolMinSize * 4
	// defaultWorkerPoolHardMaxSize is the absolute cap to prevent unbounded growth.
	defaultWorkerPoolHardMaxSize = defaultWorkerPoolSoftMaxSize * 10
	// defaultWorkerPoolIdleTimeout controls how long extra workers stay alive when idle.
	defaultWorkerPoolIdleTimeout = 30 * time.Second
)

// remoteExecTask represents a task for the worker pool
type remoteExecTask struct {
	ctx              context.Context
	server           *Server
	requestID        uint64
	request          *pb.RemoteRequest
	responseChan     chan<- *pb.StreamResponse
	activeRequests   *map[uint64]*requestContext
	activeRequestsMu *sync.Mutex
	responsesClosed  *int32
	wg               *sync.WaitGroup
}

// RecoverArgs provides arguments for util.Recover.
func (t *remoteExecTask) RecoverArgs() (metricsLabel string, funcInfo string, recoverFn func(), quit bool) {
	return "remote-exec", "processRequest", func() {
		logutil.BgLogger().Error("panic in batch remote execute", zap.Stack("stack"))
		// Try to send error to client if channel is not closed
		if atomic.LoadInt32(t.responsesClosed) == 0 {
			select {
			case <-t.ctx.Done():
			case t.responseChan <- &pb.StreamResponse{
				RequestId: t.requestID,
				Err:       "server panic",
				HasMore:   false,
			}:
			}
		}
	}, false
}

// NewServer creates a RemoteExecService gRPC server.
func NewServer(sm util.SessionManager, dom *domain.Domain) *Server {
	pool := newRemoteExecWorkerPool(defaultWorkerPoolMinSize, defaultWorkerPoolSoftMaxSize, defaultWorkerPoolHardMaxSize, defaultWorkerPoolIdleTimeout)

	s := &Server{
		sm:                 sm,
		dom:                dom,
		sessionPool:        make([]*sessionWithStmtCache, 0, 100),
		workerPool:         pool,
		closeCh:            make(chan struct{}),
		cleanupExited:      make(chan struct{}),
		stmtCacheSize:      1000,
		sessionIdleTimeout: 5 * time.Minute,
		maxPoolSize:        100, // limit pool size to prevent unbounded growth
	}
	go func() {
		defer close(s.cleanupExited)
		s.cleanupIdleSessions()
	}()
	return s
}

// Close releases resources owned by this server.
func (s *Server) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		close(s.closeCh)
		<-s.cleanupExited

		s.activeStreams.Wait()

		if s.workerPool != nil {
			s.workerPool.Close()
			s.workerPool = nil
		}

		s.sessionPoolMu.Lock()
		sessions := s.sessionPool
		s.sessionPool = nil
		s.sessionPoolMu.Unlock()
		for _, sw := range sessions {
			s.closeSessionWithCache(sw)
		}
	})
}

func (s *Server) cleanupIdleSessions() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.sessionPoolMu.Lock()
			now := time.Now()
			// Clean up idle sessions that are not in use
			newPool := make([]*sessionWithStmtCache, 0, len(s.sessionPool))
			for _, sw := range s.sessionPool {
				if !sw.inUse && now.Sub(sw.lastUsed) > s.sessionIdleTimeout {
					s.closeSessionWithCache(sw)
				} else {
					newPool = append(newPool, sw)
				}
			}
			s.sessionPool = newPool
			s.sessionPoolMu.Unlock()
		case <-s.closeCh:
			return
		}
	}
}

func (s *Server) getOrCreateSession(ctx context.Context, req *pb.RemoteRequest) (*sessionWithStmtCache, error) {
	s.sessionPoolMu.Lock()
	// Find an available session that is not in use
	for _, sw := range s.sessionPool {
		if !sw.inUse {
			sw.inUse = true
			sw.lastUsed = time.Now()
			metrics.RemotePlanSessionPoolInUse.Inc()
			s.sessionPoolMu.Unlock()
			if err := s.restoreSessionContext(ctx, sw.session, req); err != nil {
				// Failed to restore context, remove from pool and close this session
				s.sessionPoolMu.Lock()
				// Remove from pool
				for i, poolSw := range s.sessionPool {
					if poolSw == sw {
						s.sessionPool = append(s.sessionPool[:i], s.sessionPool[i+1:]...)
						break
					}
				}
				poolSize := len(s.sessionPool)
				s.sessionPoolMu.Unlock()
				metrics.RemotePlanSessionPoolSize.Set(float64(poolSize))
				metrics.RemotePlanSessionPoolInUse.Dec()
				s.closeSessionWithCache(sw)
				return s.createNewSession(ctx, req)
			}
			return sw, nil
		}
	}
	s.sessionPoolMu.Unlock()

	return s.createNewSession(ctx, req)
}

func (s *Server) createNewSession(ctx context.Context, req *pb.RemoteRequest) (*sessionWithStmtCache, error) {
	store := s.dom.Store()
	se, err := session.CreateSession(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err := s.restoreSessionContext(ctx, se, req); err != nil {
		se.Close()
		return nil, errors.Trace(err)
	}
	if s.sm != nil {
		s.sm.StoreInternalSession(se)
	}
	sw := &sessionWithStmtCache{
		session:   se,
		stmtCache: make(map[string]*preparedStmtEntry),
		lastUsed:  time.Now(),
		inUse:     true, // Mark as in use immediately
	}

	// Add to pool if not exceeding max size
	s.sessionPoolMu.Lock()
	if len(s.sessionPool) < s.maxPoolSize {
		s.sessionPool = append(s.sessionPool, sw)
		metrics.RemotePlanSessionPoolSize.Set(float64(len(s.sessionPool)))
	}
	s.sessionPoolMu.Unlock()
	metrics.RemotePlanSessionPoolInUse.Inc()

	return sw, nil
}

func (s *Server) returnSession(req *pb.RemoteRequest, sw *sessionWithStmtCache) {
	if sw == nil {
		return
	}

	// Decrement in-use counter
	metrics.RemotePlanSessionPoolInUse.Dec()

	// Ensure no leftover transaction state leaks across pooled sessions.
	// Without this, an old StartTS can survive reuse and eventually fall behind GC safepoint.
	sw.session.RollbackTxn(context.Background())

	// Clean up memory trackers before returning session to pool
	// This is critical to prevent memory leaks when sessions are reused.
	// Without this cleanup, the statement context's memory tracker remains attached
	// to the session's memory tracker, causing memory to accumulate across requests.
	// This mirrors the cleanup done in ResetContextOfStmt (pkg/executor/select.go).
	sessVars := sw.session.GetSessionVars()
	if sessVars != nil {
		// Detach and reset memory tracker
		sessVars.MemTracker.Detach()
		sessVars.MemTracker.UnbindActions()
		sessVars.MemTracker.ResetMaxConsumed()

		// Detach and reset disk tracker
		sessVars.DiskTracker.Detach()
		sessVars.DiskTracker.ResetMaxConsumed()

		// Also detach statement context's memory tracker if it exists
		if sessVars.StmtCtx != nil {
			sessVars.StmtCtx.DetachMemDiskTracker()
		}
	}

	s.sessionPoolMu.Lock()
	defer s.sessionPoolMu.Unlock()

	sw.lastUsed = time.Now()
	sw.inUse = false

	// Check if session is in the pool
	inPool := false
	for _, poolSw := range s.sessionPool {
		if poolSw == sw {
			inPool = true
			break
		}
	}

	// If not in pool and pool is not full, add it
	if !inPool {
		if len(s.sessionPool) < s.maxPoolSize {
			s.sessionPool = append(s.sessionPool, sw)
			metrics.RemotePlanSessionPoolSize.Set(float64(len(s.sessionPool)))
		} else {
			// Pool is full, close this session
			s.closeSessionWithCache(sw)
		}
	}
}

func (s *Server) closeSessionWithCache(sw *sessionWithStmtCache) {
	if sw == nil || sw.session == nil {
		return
	}
	sw.mu.Lock()
	for _, entry := range sw.stmtCache {
		if err := sw.session.DropPreparedStmt(entry.stmtID); err != nil {
			logutil.BgLogger().Warn("failed to drop prepared statement",
				zap.Uint32("stmtID", entry.stmtID), zap.Error(err))
		}
	}
	sw.stmtCache = nil
	sw.mu.Unlock()

	if s.sm != nil {
		s.sm.DeleteInternalSession(sw.session)
	}

	// Set InRestrictedSQL to true before rollback to bypass permission checks
	// This is necessary because the session may not have proper user context
	// when being closed from the session pool cleanup
	sw.session.GetSessionVars().InRestrictedSQL = true
	sw.session.RollbackTxn(context.Background())
	sw.session.Close()
}

func (s *Server) restoreSessionContext(ctx context.Context, se sessiontypes.Session, req *pb.RemoteRequest) error {
	sessVars := se.GetSessionVars()
	snapshot := req.GetSession()

	// Mark this session as executing forwarded SQL, so lower layers can gate behaviors that are only
	se.SetValue(sessionctx.ForwardedForRemoteExecKey{}, true)
	if sessVars != nil {
		sessVars.ForwardedForRemoteExec = true
	}

	if snapshot == nil {
		return nil
	}

	// Restore user authentication info first, before any SQL execution
	// This is critical for permission checks
	if snapshot.AuthUsername != "" || snapshot.AuthHostname != "" {
		sessVars.User = &auth.UserIdentity{
			Username:     snapshot.AuthUsername,
			Hostname:     snapshot.AuthHostname,
			CurrentUser:  true, // Set CurrentUser to true for proper permission checks
			AuthUsername: snapshot.AuthUsername,
			AuthHostname: snapshot.AuthHostname,
		}
		logutil.BgLogger().Debug("remote exec restored user context",
			zap.String("username", snapshot.AuthUsername),
			zap.String("hostname", snapshot.AuthHostname))
	}

	if snapshot.CurrentDb != "" {
		// Directly set the CurrentDB in session variables
		// We don't use USE statement because it requires database privileges
		// which may not be available in the remote execution context
		sessVars.CurrentDB = snapshot.CurrentDb
	}
	sessVars.SQLMode = mysql.SQLMode(snapshot.SqlMode)
	if snapshot.TimeZone != "" && snapshot.TimeZone != "Local" {
		// Use timeutil.LoadLocation which has built-in caching to avoid
		// expensive file system access. time.LoadLocation reads from file
		// system each time, which is slow.
		if tz, err := timeutil.LoadLocation(snapshot.TimeZone); err == nil {
			sessVars.TimeZone = tz
		}
	}
	if snapshot.Isolation != "" {
		if err := sessVars.SetSystemVarWithoutValidation(variable.TxnIsolation, snapshot.Isolation); err != nil {
			logutil.Logger(ctx).Warn("failed to set isolation level", zap.String("isolation", snapshot.Isolation), zap.Error(err))
		}
	}
	sessVars.TxnMode = snapshot.TxnMode
	if snapshot.ResourceGroup != "" {
		sessVars.ResourceGroupName = snapshot.ResourceGroup
	}
	txnCtx := snapshot.GetTxn()
	if txnCtx != nil {
		if txnCtx.Autocommit {
			sessVars.SetStatusFlag(mysql.ServerStatusAutocommit, true)
		} else {
			sessVars.SetStatusFlag(mysql.ServerStatusAutocommit, false)
		}
		if txnCtx.StartTs > 0 && txnCtx.InTxn {
			sessVars.TxnCtx.StartTS = txnCtx.StartTs
			sessVars.SetInTxn(true)
		}
	}

	if snapshot.PartitionPruneMode != "" {
		if err := sessVars.SetSystemVarWithoutValidation(variable.TiDBPartitionPruneMode, snapshot.PartitionPruneMode); err != nil {
			logutil.Logger(ctx).Warn("failed to set partition prune mode", zap.String("mode", snapshot.PartitionPruneMode), zap.Error(err))
		}
	}
	if err := sessVars.SetSystemVarWithoutValidation(variable.TiDBSkipMissingPartitionStats, variable.BoolToOnOff(snapshot.SkipMissingPartitionStats)); err != nil {
		logutil.Logger(ctx).Warn("failed to set skip missing partition stats", zap.Bool("skip", snapshot.SkipMissingPartitionStats), zap.Error(err))
	}

	// Restore isolation read engines
	if snapshot.IsolationReadEngines != "" {
		sessVars.IsolationReadEngines = make(map[kv.StoreType]struct{})
		for _, engine := range strings.Split(snapshot.IsolationReadEngines, ",") {
			switch strings.TrimSpace(engine) {
			case kv.TiDB.Name():
				sessVars.IsolationReadEngines[kv.TiDB] = struct{}{}
			case kv.TiKV.Name():
				sessVars.IsolationReadEngines[kv.TiKV] = struct{}{}
			case kv.TiFlash.Name():
				sessVars.IsolationReadEngines[kv.TiFlash] = struct{}{}
			}
		}
	}

	if snapshot.SelectLimit > 0 {
		sessVars.SelectLimit = snapshot.SelectLimit
	}

	if snapshot.ConnCharset != "" {
		sessVars.SetSystemVarWithoutValidation(variable.CharacterSetConnection, snapshot.ConnCharset)
	}
	if snapshot.ConnCollation != "" {
		sessVars.SetSystemVarWithoutValidation(variable.CollationConnection, snapshot.ConnCollation)
	}

	sessVars.InRestrictedSQL = snapshot.InRestrictedSql
	sessVars.ForeignKeyChecks = snapshot.ForeignKeyChecks
	sessVars.StmtCtx.ForShareLockEnabledByNoop = snapshot.ForShareLockEnabledByNoop
	sessVars.SharedLockPromotion = snapshot.SharedLockPromotion

	sessVars.EnableChunkRPC = true
	sessVars.EnablePlanCacheForSubquery = true
	sessVars.EnableRemotePlan = false

	// Restore plan cache settings from control side
	// This ensures the remote side respects the control side's plan cache configuration
	// for correctness and debugging purposes (e.g., when plan cache is explicitly disabled)
	// We use SetSystemVarWithoutValidation instead of directly setting the bool field
	// because loadCommonGlobalVariablesIfNeeded checks the systems map to decide
	// whether to load global variables. If we only set the bool field, the systems map
	// won't have the entry, and loadCommonGlobalVariablesIfNeeded will overwrite our setting.
	if snapshot.EnablePrepPlanCache {
		sessVars.SetSystemVarWithoutValidation(variable.TiDBEnablePrepPlanCache, variable.On)
	} else {
		sessVars.SetSystemVarWithoutValidation(variable.TiDBEnablePrepPlanCache, variable.Off)
	}
	if snapshot.EnableNonPrepPlanCache {
		sessVars.SetSystemVarWithoutValidation(variable.TiDBEnableNonPreparedPlanCache, variable.On)
	} else {
		sessVars.SetSystemVarWithoutValidation(variable.TiDBEnableNonPreparedPlanCache, variable.Off)
	}
	if snapshot.EnablePlanCacheForParamLimit {
		sessVars.SetSystemVarWithoutValidation(variable.TiDBEnablePlanCacheForParamLimit, variable.On)
	} else {
		sessVars.SetSystemVarWithoutValidation(variable.TiDBEnablePlanCacheForParamLimit, variable.Off)
	}

	// Restore instance plan cache setting from control side
	// Note: This is a global setting, so we only update it if the control side has it enabled
	// and the remote side doesn't. We don't disable it if the remote side has it enabled
	// because other sessions might be using it.
	if snapshot.EnableInstancePlanCache && !variable.EnableInstancePlanCache.Load() {
		variable.EnableInstancePlanCache.Store(true)
	}

	// Restore async commit / 1PC / row format so remote side matches control side session.
	asyncCommitVal := variable.BoolToOnOff(snapshot.EnableAsyncCommit)
	if err := sessVars.SetSystemVarWithoutValidation(variable.TiDBEnableAsyncCommit, asyncCommitVal); err != nil {
		logutil.Logger(ctx).Warn("failed to set async commit", zap.Error(err))
	}
	onePCVal := variable.BoolToOnOff(snapshot.Enable_1Pc)
	if err := sessVars.SetSystemVarWithoutValidation(variable.TiDBEnable1PC, onePCVal); err != nil {
		logutil.Logger(ctx).Warn("failed to set 1pc", zap.Error(err))
	}
	if snapshot.RowFormatVersion != 0 {
		val := strconv.FormatUint(uint64(snapshot.RowFormatVersion), 10)
		if err := sessVars.SetSystemVarWithoutValidation(variable.TiDBRowFormatVersion, val); err != nil {
			logutil.Logger(ctx).Warn("failed to set row format version", zap.String("value", val), zap.Error(err))
		}
	}

	if snapshot.MaxChunkSize > 0 {
		val := strconv.FormatUint(uint64(snapshot.MaxChunkSize), 10)
		if err := sessVars.SetSystemVarWithoutValidation(variable.TiDBMaxChunkSize, val); err != nil {
			logutil.Logger(ctx).Warn("failed to set max chunk size", zap.String("value", val), zap.Error(err))
		}
	}
	if err := sessVars.SetSystemVarWithoutValidation(variable.TiDBXEnableIndexLookUpPushDown, variable.BoolToOnOff(snapshot.TidbxEnableIndexLookupPushDown)); err != nil {
		logutil.Logger(ctx).Warn("failed to set index lookup push down", zap.Error(err))
	}
	if variable.EnableFastPath.Load() != snapshot.TidbxFastPath {
		val := variable.BoolToOnOff(snapshot.TidbxFastPath)
		if sv := variable.GetSysVar(variable.TiDBXEnableFastPath); sv != nil {
			if err := sv.SetGlobalFromHook(ctx, sessVars, val, false); err != nil {
				logutil.Logger(ctx).Warn("failed to set tidbx_fast_path", zap.String("value", val), zap.Error(err))
			}
		} else {
			variable.EnableFastPath.Store(snapshot.TidbxFastPath)
		}
	}

	return nil
}

func (s *Server) executeSQLWithCache(ctx context.Context, sw *sessionWithStmtCache, req *pb.RemoteRequest) (sqlexec.RecordSet, error) {
	sql := req.SqlText
	if sql == "" {
		return nil, errors.New("empty SQL text")
	}
	if sw != nil && sw.session != nil && (req.NormalizedSql != "" || req.SqlDigest != "") {
		sw.session.SetValue(sessionctx.ForwardedSQLDigestKey{}, sessionctx.ForwardedSQLDigest{
			Normalized: req.NormalizedSql,
			Digest:     req.SqlDigest,
		})
		defer sw.session.ClearValue(sessionctx.ForwardedSQLDigestKey{})
	}

	// Convert parameters to expressions (may be empty for statements without placeholders)
	params, err := convertParamsToExpressions(req.Params)
	if err != nil {
		return nil, errors.Annotate(err, "failed to convert parameters")
	}

	// Check if plan cache info is provided from the control side
	// If so, use the optimized path that avoids parsing SQL AST
	versionInfo := convertPlanCacheInfo(req.PlanCacheInfo)

	// Extract ParamTypes from params to ensure correct type matching in plan cache lookup
	if versionInfo != nil && len(params) > 0 {
		versionInfo.ParamTypes = make([]*types.FieldType, len(params))
		for i, p := range params {
			if c, ok := p.(*expression.Constant); ok && c.RetType != nil {
				versionInfo.ParamTypes[i] = c.RetType
			}
		}
	}

	// Use ExecuteWithRemotePlanCache for all cases (with or without parameters)
	// This allows prepared statements without placeholders (e.g., "prepare s from 'select 1'")
	// to also benefit from plan cache optimization
	return sw.session.ExecuteWithRemotePlanCache(ctx, sql, params, versionInfo)
}

// convertPlanCacheInfo converts pb.PlanCacheInfo to sessiontypes.PlanCacheVersionInfo
func convertPlanCacheInfo(info *pb.PlanCacheInfo) *sessiontypes.PlanCacheVersionInfo {
	if info == nil {
		return nil
	}

	// Convert relate versions from repeated TableVersion to map
	relateVersion := make(map[int64]uint64, len(info.RelateVersions))
	for _, tv := range info.RelateVersions {
		relateVersion[tv.TableId] = tv.Revision
	}

	var isReadOnly *bool
	switch v := info.IsReadOnlyOpt.(type) {
	case *pb.PlanCacheInfo_IsReadOnly:
		b := v.IsReadOnly
		isReadOnly = &b
	}

	return &sessiontypes.PlanCacheVersionInfo{
		SchemaVersion:       info.SchemaVersion,
		RelateVersion:       relateVersion,
		LatestSchemaVersion: info.LatestSchemaVersion,
		IsReadOnly:          isReadOnly,
		HasSubquery:         info.HasSubquery,
		HasLimit:            info.HasLimit,
		LimitValues:         info.LimitValues,
		StatsVerHash:        info.StatsVerHash,
		PlanCacheEnabled:    info.PlanCacheEnabled,
	}
}

func convertParamsToExpressions(pbParams []*pb.Param) ([]expression.Expression, error) {
	if len(pbParams) == 0 {
		return nil, nil
	}
	params := make([]expression.Expression, 0, len(pbParams))
	for i, p := range pbParams {
		expr, err := convertParamToExpression(p)
		if err != nil {
			return nil, errors.Annotatef(err, "failed to convert parameter %d", i)
		}
		params = append(params, expr)
	}
	return params, nil
}

func convertParamToExpression(p *pb.Param) (expression.Expression, error) {
	if p == nil {
		return nil, errors.New("nil parameter")
	}
	// Use structured FieldType from protobuf for efficient deserialization
	ft := protoToFieldType(p.FieldType)
	datum, err := convertValueToDatum(p.Value, ft)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &expression.Constant{Value: datum, RetType: ft}, nil
}

// protoToFieldType converts pb.FieldType to types.FieldType.
// This is the inverse of fieldTypeToProto in remote.go.
// Using structured protobuf is much more efficient than JSON parsing.
func protoToFieldType(pbFt *pb.FieldType) *types.FieldType {
	if pbFt == nil {
		ft := types.NewFieldType(mysql.TypeVarchar)
		ft.SetFlen(types.UnspecifiedLength)
		return ft
	}
	ft := types.NewFieldType(byte(pbFt.Tp))
	ft.SetFlag(uint(pbFt.Flag))
	ft.SetFlen(int(pbFt.Flen))
	ft.SetDecimal(int(pbFt.Decimal))
	if pbFt.Charset != "" {
		ft.SetCharset(pbFt.Charset)
	}
	if pbFt.Collate != "" {
		ft.SetCollate(pbFt.Collate)
	}
	if len(pbFt.Elems) > 0 {
		ft.SetElems(pbFt.Elems)
	}
	if pbFt.Array {
		ft.SetArray(true)
	}
	return ft
}

func convertValueToDatum(value string, ft *types.FieldType) (types.Datum, error) {
	var datum types.Datum
	if value == "" && ft.GetType() == mysql.TypeNull {
		datum.SetNull()
		return datum, nil
	}
	if value == "<nil>" {
		datum.SetNull()
		return datum, nil
	}
	switch ft.GetType() {
	case mysql.TypeNull:
		datum.SetNull()
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			v, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return datum, errors.Trace(err)
			}
			datum.SetUint64(v)
		} else {
			v, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return datum, errors.Trace(err)
			}
			datum.SetInt64(v)
		}
	case mysql.TypeFloat:
		v, err := strconv.ParseFloat(value, 32)
		if err != nil {
			return datum, errors.Trace(err)
		}
		datum.SetFloat32(float32(v))
	case mysql.TypeDouble:
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return datum, errors.Trace(err)
		}
		datum.SetFloat64(v)
	case mysql.TypeNewDecimal:
		d := new(types.MyDecimal)
		if err := d.FromString([]byte(value)); err != nil {
			return datum, errors.Trace(err)
		}
		datum.SetMysqlDecimal(d)
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		t, err := types.ParseTime(types.DefaultStmtNoWarningContext, value, ft.GetType(), ft.GetDecimal())
		if err != nil {
			return datum, errors.Trace(err)
		}
		datum.SetMysqlTime(t)
	case mysql.TypeDuration:
		d, _, err := types.ParseDuration(types.DefaultStmtNoWarningContext, value, ft.GetDecimal())
		if err != nil {
			return datum, errors.Trace(err)
		}
		datum.SetMysqlDuration(d)
	case mysql.TypeYear:
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return datum, errors.Trace(err)
		}
		datum.SetInt64(v)
	case mysql.TypeJSON:
		j, err := types.ParseBinaryJSONFromString(value)
		if err != nil {
			return datum, errors.Trace(err)
		}
		datum.SetMysqlJSON(j)
	case mysql.TypeEnum:
		datum.SetString(value, ft.GetCollate())
	case mysql.TypeSet:
		datum.SetString(value, ft.GetCollate())
	case mysql.TypeBit:
		datum.SetBytes([]byte(value))
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		datum.SetBytes([]byte(value))
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString:
		datum.SetString(value, ft.GetCollate())
	default:
		datum.SetString(value, ft.GetCollate())
	}
	return datum, nil
}

func convertColumnInfoToPB(ci *column.Info, field *resolve.ResultField) *pb.ColumnInfo {
	pbInfo := &pb.ColumnInfo{
		Schema:       ci.Schema,
		Table:        ci.Table,
		OrgTable:     ci.OrgTable,
		Name:         ci.Name,
		OrgName:      ci.OrgName,
		ColumnLength: ci.ColumnLength,
		Charset:      uint32(ci.Charset),
		Flag:         uint32(ci.Flag),
		Decimal:      uint32(ci.Decimal),
		Type:         uint32(ci.Type),
	}
	if ci.DefaultValue != nil {
		if data, err := json.Marshal(ci.DefaultValue); err == nil {
			pbInfo.DefaultValue = data
		}
	}
	if field != nil && field.Column != nil {
		ft := &field.Column.FieldType
		pbInfo.Flen = int32(ft.GetFlen())
		pbInfo.CharsetName = ft.GetCharset()
		pbInfo.Collate = ft.GetCollate()
	}
	return pbInfo
}

// ============================================================================
// BatchRemoteExecute - Bidirectional Streaming Implementation
// ============================================================================

// requestContext holds the context for a single request in the batch stream
type requestContext struct {
	requestID uint64
	req       *pb.RemoteRequest
	sw        *sessionWithStmtCache
	rs        sqlexec.RecordSet
	fields    []*resolve.ResultField
	codec     *chunk.Codec
	chk       *chunk.Chunk
	sequence  uint32
	done      bool
	err       error
}

// BatchRemoteExecute implements the bidirectional streaming RPC
// It handles multiple requests over a single stream, matching responses by request ID
func (s *Server) BatchRemoteExecute(stream pb.RemoteExecService_BatchRemoteExecuteServer) error {
	s.activeStreams.Add(1)
	defer s.activeStreams.Done()

	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	// Active requests map: request_id -> requestContext
	activeRequests := make(map[uint64]*requestContext)
	var activeRequestsMu sync.Mutex

	// Channel for responses to send
	responseChan := make(chan *pb.StreamResponse, 100)

	// Error channel for goroutine errors
	errChan := make(chan error, 1)

	// WaitGroup for tracking active request processors
	var wg sync.WaitGroup

	// Flag to indicate if responseChan is closed
	var responsesClosed int32

	// Start response sender goroutine with batching to reduce HOL blocking
	// Simple strategy: after collecting first response, wait up to 500μs for more
	go func() {
		batch := make([]*pb.StreamResponse, 0, 64)
		var batchFirstTime time.Time

		for {
			// Wait for at least one response
			select {
			case <-ctx.Done():
				return
			case resp, ok := <-responseChan:
				if !ok {
					return
				}
				if len(batch) == 0 {
					batchFirstTime = time.Now()
				}
				batch = append(batch, resp)
			}

			// Non-blocking: collect any responses already waiting
		collectMore:
			for len(batch) < 64 {
				select {
				case resp, ok := <-responseChan:
					if !ok {
						if len(batch) > 0 {
							if !batchFirstTime.IsZero() {
								metrics.RemotePlanRespBatchWaitDuration.Observe(time.Since(batchFirstTime).Seconds())
							}
							sendStart := time.Now()
							stream.Send(&pb.BatchRemoteResponse{Responses: batch})
							metrics.RemotePlanGrpcSendDuration.Observe(time.Since(sendStart).Seconds())
							metrics.RemotePlanRespBatchSize.Observe(float64(len(batch)))
						}
						return
					}
					batch = append(batch, resp)
				default:
					break collectMore
				}
			}

			if len(batch) < 4 {
				select {
				case resp, ok := <-responseChan:
					if !ok {
						break
					}
					batch = append(batch, resp)
				case <-time.After(500 * time.Microsecond):
					// Timeout, send what we have
				case <-ctx.Done():
					return
				}
			}

			// Send the batch
			if len(batch) > 0 {
				if !batchFirstTime.IsZero() {
					metrics.RemotePlanRespBatchWaitDuration.Observe(time.Since(batchFirstTime).Seconds())
				}
				sendStart := time.Now()
				if err := stream.Send(&pb.BatchRemoteResponse{Responses: batch}); err != nil {
					select {
					case errChan <- err:
					default:
					}
					return
				}
				metrics.RemotePlanGrpcSendDuration.Observe(time.Since(sendStart).Seconds())
				metrics.RemotePlanRespBatchSize.Observe(float64(len(batch)))
				batch = batch[:0]
				batchFirstTime = time.Time{}
			}
		}
	}()

	// Helper function to safely close responseChan
	closeResponseChan := func() {
		if atomic.CompareAndSwapInt32(&responsesClosed, 0, 1) {
			close(responseChan)
		}
	}

	// Main loop: receive requests from client
	for {
		select {
		case <-ctx.Done():
			cancel()
			wg.Wait()
			closeResponseChan()
			return ctx.Err()
		case err := <-errChan:
			cancel()
			wg.Wait()
			closeResponseChan()
			return err
		default:
		}

		batchReq, err := stream.Recv()
		if err != nil {
			cancel()
			// Wait for all request processors to finish before closing the channel
			// This prevents "send on closed channel" panic
			wg.Wait()
			closeResponseChan()
			if errors.ErrorEqual(err, context.Canceled) {
				return nil
			}
			// EOF from client CloseSend() is normal, not an error
			if err.Error() == "EOF" {
				return nil
			}
			return err
		}

		// Process each request in the batch
		for i, req := range batchReq.Requests {
			var requestID uint64
			if i < len(batchReq.RequestIds) {
				requestID = batchReq.RequestIds[i]
			}

			wg.Add(1)
			// Use an elastic worker pool instead of creating a new goroutine each time.
			// This reuses goroutines and helps reduce potential morestack overhead in FFI scenarios.
			task := &remoteExecTask{
				ctx:              ctx,
				server:           s,
				requestID:        requestID,
				request:          req,
				responseChan:     responseChan,
				activeRequests:   &activeRequests,
				activeRequestsMu: &activeRequestsMu,
				responsesClosed:  &responsesClosed,
				wg:               &wg,
			}
			if !s.workerPool.Submit(ctx, task) {
				// If the stream is already canceled or server is closing, avoid leaking the WaitGroup counter.
				wg.Done()
			}
		}
	}
}

// processRequest handles a single request and sends responses through the channel
func (s *Server) processRequest(
	ctx context.Context,
	requestID uint64,
	req *pb.RemoteRequest,
	responseChan chan<- *pb.StreamResponse,
	activeRequests *map[uint64]*requestContext,
	activeRequestsMu *sync.Mutex,
	responsesClosed *int32,
) {
	logutil.BgLogger().Debug("batch remote execute: processing request",
		zap.Uint64("requestID", requestID),
		zap.String("sql", req.SqlText),
		zap.String("db", req.GetSession().GetCurrentDb()))

	processStart := time.Now()
	defer func() {
		metrics.RemotePlanProcessRequestDuration.Observe(time.Since(processStart).Seconds())
	}()

	start := time.Now()
	var handleErr error
	defer func() {
		if handleErr == nil {
			metrics.RemotePlanForwardRecvOKCounter.Inc()
			metrics.RemotePlanForwardRecvOKDuration.Observe(time.Since(start).Seconds())
			return
		}
		metrics.RemotePlanForwardRecvErrCounter.Inc()
		metrics.RemotePlanForwardRecvErrDuration.Observe(time.Since(start).Seconds())
	}()

	// Create request context
	reqCtx := &requestContext{
		requestID: requestID,
		req:       req,
	}

	// Register active request
	activeRequestsMu.Lock()
	(*activeRequests)[requestID] = reqCtx
	activeRequestsMu.Unlock()

	// Cleanup function - must close rs BEFORE returning session to pool
	// to avoid concurrent map access (rs.Close() may access session vars)
	defer func() {
		// Cleanup active requests map
		activeRequestsMu.Lock()
		delete(*activeRequests, requestID)
		activeRequestsMu.Unlock()

		// Close result set BEFORE returning session to pool
		// This is critical because rs.Close() triggers FinishExecuteStmt which
		// accesses session variables (like GetCharsetInfo reading systems map)
		if reqCtx.rs != nil {
			reqCtx.rs.Close()
		}

		// Return session to pool AFTER rs.Close() completes
		if reqCtx.sw != nil {
			s.returnSession(req, reqCtx.sw)
		}

		// Return request to vtproto pool if enabled
		if req != nil {
			if vt, ok := any(req).(interface{ ReturnToVTPool() }); ok {
				vt.ReturnToVTPool()
			}
		}
	}()

	// Get or create session
	sessionAcquireStart := time.Now()
	sw, err := s.getOrCreateSession(ctx, req)
	metrics.RemotePlanSessionAcquireDuration.Observe(time.Since(sessionAcquireStart).Seconds())
	if err != nil {
		handleErr = err
		s.sendStreamError(ctx, responseChan, requestID, errors.Annotate(err, "failed to create session"), responsesClosed)
		return
	}
	reqCtx.sw = sw

	ctx = context.WithValue(ctx, core.InRemoteExec{}, true)

	sw.session.GetSessionVars().CommonGlobalLoaded = true

	// Execute SQL
	localCallStats := &tikvrpc.LocalCallStats{}
	ctx = tikvrpc.WithLocalCallStats(ctx, localCallStats)
	execStart := time.Now()
	rs, err := s.executeSQLWithCache(ctx, sw, req)
	if err != nil {
		metrics.RemotePlanExecErrDuration.Observe(time.Since(execStart).Seconds())
		handleErr = err
		s.sendStreamError(ctx, responseChan, requestID, errors.Annotate(err, "failed to execute SQL"), responsesClosed)
		return
	}
	metrics.RemotePlanExecOKDuration.Observe(time.Since(execStart).Seconds())

	// Handle DML result (no result set)
	if rs == nil {
		s.sendStreamDMLResult(ctx, responseChan, requestID, sw, localCallStats, responsesClosed)
		return
	}

	reqCtx.rs = rs

	// Stream results
	if err := s.streamResultsToChannel(ctx, requestID, rs, sw, localCallStats, responseChan, responsesClosed); err != nil {
		handleErr = err
		return
	}
}

// sendStreamError sends an error response through the channel
// This function blocks until the error is sent or the context is cancelled.
// We must not drop error responses as that would leave the client's pending request hanging indefinitely.
func (s *Server) sendStreamError(ctx context.Context, responseChan chan<- *pb.StreamResponse, requestID uint64, err error, responsesClosed *int32) {
	logutil.BgLogger().Warn("batch remote execute error",
		zap.Uint64("requestID", requestID),
		zap.Error(err),
		zap.String("errStack", errors.ErrorStack(err)))

	// Check if channel is closed before sending
	if atomic.LoadInt32(responsesClosed) == 1 {
		return
	}

	// Block until we can send the error response
	// We must not drop errors as that would leave the client hanging
	select {
	case <-ctx.Done():
		return
	case responseChan <- &pb.StreamResponse{
		RequestId: requestID,
		Err:       err.Error(),
		HasMore:   false,
	}:
	}
}

// sendStreamDMLResult sends DML result through the channel
func (s *Server) sendStreamDMLResult(ctx context.Context, responseChan chan<- *pb.StreamResponse, requestID uint64, sw *sessionWithStmtCache, localCallStats *tikvrpc.LocalCallStats, responsesClosed *int32) {
	// Check if channel is closed before sending
	if atomic.LoadInt32(responsesClosed) == 1 {
		return
	}

	sessVars := sw.session.GetSessionVars()
	warns := sessVars.StmtCtx.GetWarnings()
	pbWarns := make([]*pb.Warning, 0, len(warns))
	for _, w := range warns {
		cause := errors.Cause(w.Err)
		code := uint32(mysql.ErrUnknown)
		msg := ""
		switch x := cause.(type) {
		case *terror.Error:
			sqlErr := terror.ToSQLError(x)
			code = uint32(sqlErr.Code)
			msg = sqlErr.Message
		case *terror.TiDBError:
			code = uint32(x.MYSQLERRNO)
			msg = x.MESSAGETEXT
		default:
			if cause != nil {
				msg = cause.Error()
			}
		}
		pbWarns = append(pbWarns, &pb.Warning{
			Level:   w.Level,
			Code:    code,
			Message: msg,
		})
	}

	resp := &pb.StreamResponse{
		RequestId: requestID,
		DmlResult: &pb.DMLResult{
			AffectedRows: sessVars.StmtCtx.AffectedRows(),
			LastInsertId: sessVars.StmtCtx.LastInsertID,
			WarningCount: uint32(sessVars.StmtCtx.WarningCount()),
			StatusFlags:  uint32(sessVars.Status()),
			Info:         sessVars.StmtCtx.GetMessage(),
			Warnings:     pbWarns,
		},
		HasMore: false,
	}
	resp.Feedback = s.buildRemoteExecFeedback(sw, 0, 0, localCallStats)

	select {
	case <-ctx.Done():
		return
	case responseChan <- resp:
	}
}

// streamResultsToChannel streams query results through the response channel
func (s *Server) streamResultsToChannel(ctx context.Context, requestID uint64, rs sqlexec.RecordSet, sw *sessionWithStmtCache, localCallStats *tikvrpc.LocalCallStats, responseChan chan<- *pb.StreamResponse, responsesClosed *int32) error {
	// Check if channel is closed before sending
	if atomic.LoadInt32(responsesClosed) == 1 {
		return errors.New("responses channel closed")
	}

	fields := rs.Fields()
	columnInfos := make([]*pb.ColumnInfo, 0, len(fields))
	fieldTypes := make([]*types.FieldType, 0, len(fields))

	for _, f := range fields {
		ci := column.ConvertColumnInfo(f)
		pbColInfo := convertColumnInfoToPB(ci, f)
		columnInfos = append(columnInfos, pbColInfo)
		fieldTypes = append(fieldTypes, &f.Column.FieldType)
	}

	// Send column info (sequence 0)
	if atomic.LoadInt32(responsesClosed) == 1 {
		return errors.New("responses channel closed")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case responseChan <- &pb.StreamResponse{
		RequestId:   requestID,
		ColumnInfos: columnInfos,
		HasMore:     true,
		Sequence:    0,
	}:
	}

	// Create codec and chunk for streaming
	codec := chunk.NewCodec(fieldTypes)
	chk := rs.NewChunk(nil)
	sequence := uint32(1)
	firstNext := true
	var resultBytes uint64
	var resultRows uint64
	var encodeBuffer []byte // Reusable buffer for chunk encoding

	getStmtCtxRequestCount := func() int {
		if sw == nil || sw.session == nil {
			return 0
		}
		sessVars := sw.session.GetSessionVars()
		if sessVars == nil || sessVars.StmtCtx == nil {
			return 0
		}
		return sessVars.StmtCtx.GetExecDetails().RequestCount
	}
	getLocalCallRequestCount := func() uint64 {
		if localCallStats == nil {
			return 0
		}
		return localCallStats.RequestCount.Load()
	}

	// Stream data chunks
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Check if channel is closed
		if atomic.LoadInt32(responsesClosed) == 1 {
			return errors.New("responses channel closed")
		}

		stmtReqBefore := getStmtCtxRequestCount()
		localReqBefore := getLocalCallRequestCount()
		nextStart := time.Now()
		nextErr := rs.Next(ctx, chk)
		nextDur := time.Since(nextStart)
		metrics.RemotePlanRSNextDuration.Observe(nextDur.Seconds())
		if firstNext {
			metrics.RemotePlanRSFirstNextDuration.Observe(nextDur.Seconds())
			firstNext = false
		}
		stmtReqAfter := getStmtCtxRequestCount()
		localReqAfter := getLocalCallRequestCount()

		localDelta := uint64(0)
		if localReqAfter >= localReqBefore {
			localDelta = localReqAfter - localReqBefore
		}
		if localDelta > 0 {
			metrics.RemotePlanRSNextCopCount.Observe(float64(localDelta))
		} else if stmtDelta := stmtReqAfter - stmtReqBefore; stmtDelta >= 0 {
			metrics.RemotePlanRSNextCopCount.Observe(float64(stmtDelta))
		}
		if nextErr != nil {
			s.sendStreamError(ctx, responseChan, requestID, errors.Annotate(nextErr, "failed to fetch next chunk"), responsesClosed)
			return nextErr
		}

		if chk.NumRows() == 0 {
			break
		}

		encodeStart := time.Now()
		encodeBuffer = codec.EncodeToBuffer(chk, encodeBuffer)
		encodedData := encodeBuffer
		metrics.RemotePlanChunkEncodeDuration.Observe(time.Since(encodeStart).Seconds())

		numRows := chk.NumRows()
		resultBytes += uint64(len(encodedData))
		resultRows += uint64(numRows)

		// Record chunk size metrics
		metrics.RemotePlanChunkRows.Observe(float64(numRows))
		metrics.RemotePlanChunkBytes.Observe(float64(len(encodedData)))

		// Check if channel is closed before sending
		if atomic.LoadInt32(responsesClosed) == 1 {
			return errors.New("responses channel closed")
		}

		sendStart := time.Now()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case responseChan <- &pb.StreamResponse{
			RequestId: requestID,
			Chunk: &pb.ChunkData{
				Data:    encodedData,
				NumRows: uint32(numRows),
			},
			HasMore:  true,
			Sequence: sequence,
		}:
		}
		metrics.RemotePlanStreamSendDuration.Observe(time.Since(sendStart).Seconds())

		sequence++
		chk.Reset()
	}

	// Send final response (no more data)
	if atomic.LoadInt32(responsesClosed) == 1 {
		return errors.New("responses channel closed")
	}
	feedback := s.buildRemoteExecFeedback(sw, resultRows, resultBytes, localCallStats)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case responseChan <- &pb.StreamResponse{
		RequestId: requestID,
		HasMore:   false,
		Sequence:  sequence,
		Feedback:  feedback,
	}:
	}
	return nil
}

func (s *Server) buildRemoteExecFeedback(sw *sessionWithStmtCache, resultRows, resultBytes uint64, localCallStats *tikvrpc.LocalCallStats) *pb.RemoteExecFeedback {
	fb := &pb.RemoteExecFeedback{
		ResultRows:  resultRows,
		ResultBytes: resultBytes,
	}

	if sw != nil && sw.session != nil {
		sessVars := sw.session.GetSessionVars()
		if sessVars != nil && sessVars.StmtCtx != nil {
			detail := sessVars.StmtCtx.GetExecDetails()
			if detail.RequestCount > 0 {
				fb.KvRequestCount = uint64(detail.RequestCount)
			}
			if sd := detail.ScanDetail; sd != nil {
				if sd.ProcessedKeys > 0 {
					fb.TikvProcessedKeys = uint64(sd.ProcessedKeys)
				}
				if sd.ProcessedKeysSize > 0 {
					fb.TikvProcessedKeysSize = uint64(sd.ProcessedKeysSize)
				}
			}
		}
	}

	if localCallStats != nil {
		fb.KvLocalCallRequestCount = localCallStats.RequestCount.Load()
	}
	return fb
}
