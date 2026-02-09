package pkdbremote

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/pkdb_remote/pb"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikvrpc"
	txnkvtxn "github.com/tikv/client-go/v2/txnkv/transaction"
	"go.uber.org/zap"
)

var remoteRequestPool = sync.Pool{
	New: func() any { return &pb.RemoteRequest{} },
}

var sessionSnapshotPool = sync.Pool{
	New: func() any { return &pb.SessionSnapshot{} },
}

var paramPool = sync.Pool{
	New: func() any { return &pb.Param{} },
}

var fieldTypePool = sync.Pool{
	New: func() any { return &pb.FieldType{} },
}

var tableVersionPool = sync.Pool{
	New: func() any { return &pb.TableVersion{} },
}

var dmlRecordSetPool = sync.Pool{
	New: func() any { return &DMLRecordSet{} },
}

func acquireRemoteRequest() *pb.RemoteRequest {
	req, _ := remoteRequestPool.Get().(*pb.RemoteRequest)
	if req == nil {
		req = &pb.RemoteRequest{}
	}
	resetRemoteRequest(req)
	return req
}

func releaseRemoteRequest(req *pb.RemoteRequest) {
	if req == nil {
		return
	}
	// Client-side created request: recycle to local pool only.
	if len(req.Params) > 0 {
		for _, p := range req.Params {
			if p == nil {
				continue
			}
			if p.FieldType != nil {
				releaseFieldType(p.FieldType)
				p.FieldType = nil
			}
			resetParam(p)
			paramPool.Put(p)
		}
		req.Params = req.Params[:0]
	}
	if req.PlanCacheInfo != nil {
		releasePlanCacheInfo(req.PlanCacheInfo)
		req.PlanCacheInfo = nil
	}
	if req.Session != nil {
		releaseSessionSnapshot(req.Session)
		req.Session = nil
	}
	resetRemoteRequest(req)
	remoteRequestPool.Put(req)
}

func resetRemoteRequest(req *pb.RemoteRequest) {
	req.Target = ""
	req.SqlText = ""
	req.NormalizedSql = ""
	req.PlanDigest = ""
	req.SqlDigest = ""
	req.PlanCacheInfo = nil
	req.Params = nil
	if req.Session != nil {
		releaseSessionSnapshot(req.Session)
		req.Session = nil
	}
}

func resetParam(p *pb.Param) {
	p.Value = ""
	p.FieldType = nil
}

func acquireFieldType() *pb.FieldType {
	ft, _ := fieldTypePool.Get().(*pb.FieldType)
	if ft == nil {
		ft = &pb.FieldType{}
	}
	resetFieldType(ft)
	return ft
}

func resetFieldType(ft *pb.FieldType) {
	ft.Tp = 0
	ft.Flag = 0
	ft.Flen = 0
	ft.Decimal = 0
	ft.Charset = ""
	ft.Collate = ""
	ft.Elems = nil
	ft.ElemsIsBinaryLit = nil
	ft.Array = false
}

func releaseFieldType(ft *pb.FieldType) {
	if ft == nil {
		return
	}
	resetFieldType(ft)
	fieldTypePool.Put(ft)
}

func releasePlanCacheInfo(info *pb.PlanCacheInfo) {
	if info == nil {
		return
	}
	info.SchemaVersion = 0
	info.LatestSchemaVersion = 0
	info.HasSubquery = false
	info.HasLimit = false
	info.LimitValues = nil
	info.StatsVerHash = 0
	info.PlanCacheEnabled = false
	info.IsReadOnlyOpt = nil
	if len(info.RelateVersions) > 0 {
		for i := range info.RelateVersions {
			tv := info.RelateVersions[i]
			if tv == nil {
				continue
			}
			resetTableVersion(tv)
			tableVersionPool.Put(tv)
			info.RelateVersions[i] = nil
		}
		info.RelateVersions = info.RelateVersions[:0]
	}
}

func resetTableVersion(tv *pb.TableVersion) {
	tv.TableId = 0
	tv.Revision = 0
}

func acquireSessionSnapshot() *pb.SessionSnapshot {
	snap, _ := sessionSnapshotPool.Get().(*pb.SessionSnapshot)
	if snap == nil {
		snap = &pb.SessionSnapshot{}
	}
	resetSessionSnapshot(snap)
	return snap
}

func releaseSessionSnapshot(snap *pb.SessionSnapshot) {
	if snap == nil {
		return
	}
	resetSessionSnapshot(snap)
	sessionSnapshotPool.Put(snap)
}

func resetSessionSnapshot(snap *pb.SessionSnapshot) {
	snap.CurrentDb = ""
	snap.SqlMode = 0
	snap.TimeZone = ""
	snap.Isolation = ""
	snap.TxnMode = ""
	snap.ResourceGroup = ""
	snap.AuthUsername = ""
	snap.AuthHostname = ""
	snap.PartitionPruneMode = ""
	snap.IsolationReadEngines = ""
	snap.SelectLimit = 0
	snap.ConnCharset = ""
	snap.ConnCollation = ""
	snap.InRestrictedSql = false
	snap.ForeignKeyChecks = false
	snap.ForShareLockEnabledByNoop = false
	snap.SharedLockPromotion = false
	snap.EnablePrepPlanCache = false
	snap.EnableNonPrepPlanCache = false
	snap.EnableInstancePlanCache = false
	snap.EnableAsyncCommit = false
	snap.Enable_1Pc = false
	snap.RowFormatVersion = 0
	snap.EnablePlanCacheForParamLimit = false
	snap.InstancePlanCacheMaxSize = 0
	snap.MaxChunkSize = 0
	snap.TidbxEnableIndexLookupPushDown = false
	snap.TidbxEnableSingleStoreTxn_1Pc = false
	snap.TidbxStoreBatchGet = false
	snap.TidbxFastPath = false
	if snap.Txn == nil {
		snap.Txn = &pb.TxnContext{}
	} else {
		snap.Txn.InTxn = false
		snap.Txn.Autocommit = false
		snap.Txn.StartTs = 0
		snap.Txn.ForUpdateTs = 0
	}
}

// DMLRecordSet is a special RecordSet for DML statements (INSERT/UPDATE/DELETE)
// that carries metadata like affected rows and last insert ID.
// It implements sqlexec.RecordSet but returns no rows.
type DMLRecordSet struct {
	affectedRows uint64
	lastInsertID uint64
	statusFlags  uint32
	warningCount uint32
	info         string
	warnings     []contextutil.SQLWarn
	sctx         sessionctx.Context
	closed       int32
}

func acquireDMLRecordSet() *DMLRecordSet {
	rs, _ := dmlRecordSetPool.Get().(*DMLRecordSet)
	if rs == nil {
		rs = &DMLRecordSet{}
	}
	rs.affectedRows = 0
	rs.lastInsertID = 0
	rs.statusFlags = 0
	rs.warningCount = 0
	rs.info = ""
	rs.warnings = nil
	rs.sctx = nil
	atomic.StoreInt32(&rs.closed, 0)
	return rs
}

func releaseDMLRecordSet(rs *DMLRecordSet) {
	if rs == nil {
		return
	}
	rs.affectedRows = 0
	rs.lastInsertID = 0
	rs.statusFlags = 0
	rs.warningCount = 0
	rs.info = ""
	rs.warnings = nil
	rs.sctx = nil
	dmlRecordSetPool.Put(rs)
}

// AffectedRows returns the number of rows affected by the DML statement.
func (d *DMLRecordSet) AffectedRows() uint64 {
	return d.affectedRows
}

// LastInsertID returns the last insert ID from the DML statement.
func (d *DMLRecordSet) LastInsertID() uint64 {
	return d.lastInsertID
}

// StatusFlags returns the status flags from the DML statement.
func (d *DMLRecordSet) StatusFlags() uint32 {
	return d.statusFlags
}

// WarningCount returns the warning count from the DML statement.
func (d *DMLRecordSet) WarningCount() uint32 {
	return d.warningCount
}

// Info returns the OK packet info string from the remote execution.
func (d *DMLRecordSet) Info() string {
	return d.info
}

// Warnings returns the warnings captured in the remote statement context.
func (d *DMLRecordSet) Warnings() []contextutil.SQLWarn {
	return d.warnings
}

// Fields returns an empty slice since DML statements have no result columns.
func (d *DMLRecordSet) Fields() []*resolve.ResultField {
	return nil
}

// Next always returns an empty chunk since DML statements have no result rows.
func (d *DMLRecordSet) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	return nil
}

// NewChunk returns a new empty chunk.
func (d *DMLRecordSet) NewChunk(alloc chunk.Allocator) *chunk.Chunk {
	return chunk.New(nil, 0, 0)
}

// Close detaches the memory tracker and recycles the record set.
func (d *DMLRecordSet) Close() error {
	if !atomic.CompareAndSwapInt32(&d.closed, 0, 1) {
		return nil
	}
	if d.sctx != nil {
		d.sctx.GetSessionVars().StmtCtx.DetachMemDiskTracker()
	}
	releaseDMLRecordSet(d)
	return nil
}

// IsDMLRecordSet checks if a RecordSet is a DMLRecordSet.
func IsDMLRecordSet(rs sqlexec.RecordSet) (*DMLRecordSet, bool) {
	dml, ok := rs.(*DMLRecordSet)
	return dml, ok
}

// Client is the interface for remote execution clients.
type Client interface {
	Execute(ctx context.Context, req *pb.RemoteRequest, sctx sessionctx.Context) (sqlexec.RecordSet, error)
	Close() error
}

type remotePlanFeedbackConfig struct {
	disableAfter  int32
	cooldown      time.Duration
	noShrinkRatio int32
	minLocalCall  int32
}

func (c remotePlanFeedbackConfig) enabled() bool {
	return c.disableAfter > 0 && c.cooldown > 0 && c.noShrinkRatio > 0 && c.minLocalCall > 0
}

func remotePlanFeedbackConfigFromVars(vars *variable.SessionVars) remotePlanFeedbackConfig {
	if vars == nil {
		return remotePlanFeedbackConfig{}
	}
	noShrinkRatio := vars.RemotePlanFeedbackNoShrinkRatio
	if noShrinkRatio > 100 {
		noShrinkRatio = 100
	}
	minLocalCall := vars.RemotePlanFeedbackMinLocalCallRequests
	if minLocalCall > uint64(math.MaxInt32) {
		minLocalCall = uint64(math.MaxInt32)
	}
	return remotePlanFeedbackConfig{
		disableAfter:  int32(vars.RemotePlanFeedbackDisableAfter),
		cooldown:      time.Duration(vars.RemotePlanFeedbackCooldown) * time.Second,
		noShrinkRatio: int32(noShrinkRatio),
		minLocalCall:  int32(minLocalCall),
	}
}

type remotePlanFeedbackRecordSet interface {
	sqlexec.RecordSet
	attachRemotePlanFeedback(feedback *core.RemotePlanFeedback, cfg remotePlanFeedbackConfig)
}

var defaultClientOverride Client

// GetDefaultClient returns the remote execution client used by this package.
// It is a function (instead of a package variable) to avoid init-order issues.
func GetDefaultClient() Client {
	if defaultClientOverride != nil {
		return defaultClientOverride
	}
	return DefaultBatchClient
}

// TryEarlyForwardExecute attempts to forward the execution to a remote TiDB node
// BEFORE compile (plan generation). This is called during COM_EXECUTE when we have
// the prepared statement but haven't generated the plan yet.
//
// For statements without partitioned tables (including multi-table queries), we can determine
// the location immediately from EarlyLocationInfo extracted during PREPARE phase.
//
// For partitioned tables with CachedLocationInfo (set after first plan generation),
// we can use partition pruning with parameters to determine the location.
//
// Returns:
// - (RecordSet, true, nil): Successfully forwarded to remote
// - (nil, true, error): Forwarding attempted but failed
// - (nil, false, nil): Cannot forward early, need to go through normal compile path
func TryEarlyForwardExecute(ctx context.Context, sctx sessionctx.Context, execStmt *ast.ExecuteStmt) (sqlexec.RecordSet, bool, error) {
	vars := sctx.GetSessionVars()

	// Get the prepared statement
	prepStmt, ok := execStmt.PrepStmt.(*core.PlanCacheStmt)
	if !ok {
		return nil, false, nil
	}

	// Check if we have EarlyLocationInfo
	earlyLocInfo := prepStmt.EarlyLocationInfo
	if earlyLocInfo == nil {
		return nil, false, nil
	}

	forceRemotePlan := !vars.ForwardedForRemoteExec && earlyLocInfo.ForceRemotePlanHint

	// Check if remote plan forwarding is enabled
	if !vars.EnableRemotePlan && !forceRemotePlan {
		return nil, false, nil
	}

	// Track that we attempted forwarding (forwarding is enabled)
	GlobalForwardingStats.RecordAttempt()

	cmd := byte(atomic.LoadUint32(&sctx.GetSessionVars().CommandValue))
	isBinaryProtocol := cmd == mysql.ComStmtExecute
	isTextProtocol := cmd == mysql.ComQuery

	// For binary protocol, BinaryArgs must be present
	if isBinaryProtocol && execStmt.BinaryArgs == nil {
		GlobalForwardingStats.RecordSkipped()
		return nil, false, nil
	}

	// Only allow binary protocol or text protocol EXECUTE
	if !isBinaryProtocol && !isTextProtocol {
		GlobalForwardingStats.RecordSkipped()
		return nil, false, nil
	}

	// Do not forward SELECT statements that acquire row locks (FOR UPDATE / FOR SHARE / SKIP LOCKED, etc.).
	// These statements must run on the local session/transaction to preserve locking semantics.
	if hasSelectLock(prepStmt) {
		GlobalForwardingStats.RecordSkipped()
		logutil.Logger(ctx).Debug("[remote] TryEarlyForwardExecute: skipped - select lock not supported")
		return nil, false, nil
	}

	planCtx := sctx.GetPlanCtx()
	resolveParams := func() ([]expression.Expression, error) {
		if execStmt.BinaryArgs != nil {
			params, _ := execStmt.BinaryArgs.([]expression.Expression)
			return params, nil
		}
		if len(execStmt.UsingVars) == 0 {
			return nil, nil
		}
		convertedParams := make([]expression.Expression, 0, len(execStmt.UsingVars))
		for _, astExpr := range execStmt.UsingVars {
			expr, err := plannerutil.RewriteAstExprWithPlanCtx(planCtx, astExpr, nil, nil, false)
			if err != nil {
				return nil, err
			}
			convertedParams = append(convertedParams, expr)
		}
		return convertedParams, nil
	}

	// Extract params only when needed for partition pruning to avoid redundant work.
	var params []expression.Expression
	needParamsForLocation := earlyLocInfo.HasPartitionTable
	if !needParamsForLocation && earlyLocInfo.CachedLocationInfo != nil && earlyLocInfo.CachedLocationInfo.HasPartitionTable {
		needParamsForLocation = true
	}
	if needParamsForLocation {
		var err error
		params, err = resolveParams()
		if err != nil {
			GlobalForwardingStats.RecordSkipped()
			logutil.Logger(ctx).Debug("[remote] TryEarlyForwardExecute: failed to rewrite UsingVars",
				zap.Error(err))
			return nil, false, nil
		}
	}

	// Determine plan location using EarlyLocationInfo
	is := sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema()
	planLocation := earlyLocInfo.DetermineLocationWithOptions(planCtx, is, params, forceRemotePlan)

	// If DetermineLocation returns nil, it means we need full planning
	// (e.g., partitioned table without CachedLocationInfo)
	if planLocation == nil {
		GlobalForwardingStats.RecordSkipped()
		logutil.Logger(ctx).Debug("[remote] TryEarlyForwardExecute: need full planning",
			zap.Bool("hasPartitionTable", earlyLocInfo.HasPartitionTable),
			zap.Bool("hasCachedInfo", earlyLocInfo.HasCachedLocationInfo()))
		return nil, false, nil
	}

	// Only forward if the plan type indicates remote execution
	if !planLocation.PlanType.ShouldForward() {
		GlobalForwardingStats.RecordSkipped()
		logutil.Logger(ctx).Debug("[remote] TryEarlyForwardExecute: skipped - plan type does not require forwarding",
			zap.String("planType", planLocation.PlanType.String()))
		return nil, false, nil
	}

	feedbackCfg := remotePlanFeedbackConfigFromVars(vars)
	if !forceRemotePlan && feedbackCfg.enabled() && earlyLocInfo.RemotePlanFeedback.ForwardingDisabled(time.Time{}) {
		GlobalForwardingStats.RecordSkipped()
		disabledUntil := time.Unix(0, earlyLocInfo.RemotePlanFeedback.DisabledUntilUnixNano())
		logutil.Logger(ctx).Debug("[remote] TryEarlyForwardExecute: skipped - feedback disabled",
			zap.Time("disabledUntil", disabledUntil),
			zap.Duration("remaining", time.Until(disabledUntil)))
		return nil, false, nil
	}

	// By default we skip forwarding inside transactions to avoid correctness issues (e.g. read-your-writes).
	// When enabled, we only allow forwarding for read-only prepared statements and only when the referenced
	// tables are clean (no dirty writes in the current txn).
	inTxn := !vars.IsAutocommit() || vars.InTxn()
	if inTxn {
		if !forceRemotePlan {
			if !vars.EnableRemotePlanInTxnRead {
				GlobalForwardingStats.RecordSkipped()
				return nil, false, nil
			}
			// READ-COMMITTED uses statement-level snapshot timestamps. Remote execution currently only
			// forwards the transaction StartTS, so it may read from a stale snapshot.
			if vars.IsIsolation(ast.ReadCommitted) {
				GlobalForwardingStats.RecordSkipped()
				return nil, false, nil
			}
			if prepStmt.PreparedAst == nil || !prepStmt.PreparedAst.IsReadOnly {
				GlobalForwardingStats.RecordSkipped()
				return nil, false, nil
			}

			// Ensure the local transaction has a stable StartTS so remote reads share the same snapshot.
			if _, err := sctx.Txn(true); err != nil {
				GlobalForwardingStats.RecordSkipped()
				logutil.Logger(ctx).Debug("[remote] TryEarlyForwardExecute: failed to activate txn",
					zap.Error(err))
				return nil, false, nil
			}
			// Avoid forwarding when any referenced table has dirty data in the current txn.
			for _, tableID := range earlyLocInfo.TableIDs {
				tbl, ok := is.TableByID(ctx, tableID)
				if !ok || tbl == nil {
					GlobalForwardingStats.RecordSkipped()
					return nil, false, nil
				}
				tblInfo := tbl.Meta()
				pi := tblInfo.GetPartitionInfo()
				if pi == nil {
					if planCtx.HasDirtyContent(tableID) {
						GlobalForwardingStats.RecordSkipped()
						return nil, false, nil
					}
					continue
				}
				for _, part := range pi.Definitions {
					if planCtx.HasDirtyContent(part.ID) {
						GlobalForwardingStats.RecordSkipped()
						return nil, false, nil
					}
				}
			}
		} else {
			// Ensure the local transaction has a stable StartTS so remote reads share the same snapshot.
			if _, err := sctx.Txn(true); err != nil {
				GlobalForwardingStats.RecordSkipped()
				logutil.Logger(ctx).Debug("[remote] TryEarlyForwardExecute: failed to activate txn",
					zap.Error(err))
				return nil, false, nil
			}
		}
	}

	// Resolve params for request payload if we haven't done it yet.
	if params == nil && (execStmt.BinaryArgs != nil || len(execStmt.UsingVars) > 0) {
		var err error
		params, err = resolveParams()
		if err != nil {
			GlobalForwardingStats.RecordSkipped()
			logutil.Logger(ctx).Debug("[remote] TryEarlyForwardExecute: failed to rewrite UsingVars",
				zap.Error(err))
			return nil, false, nil
		}
	}

	logutil.Logger(ctx).Debug("[remote] TryEarlyForwardExecute: forwarding to remote (early path)",
		zap.String("target", planLocation.TargetStore),
		zap.String("planType", planLocation.PlanType.String()),
		zap.String("sql", prepStmt.StmtText))

	req, err := buildRequestPayload(sctx, prepStmt, planLocation, params)
	if err != nil {
		GlobalForwardingStats.RecordError()
		return nil, true, err
	}
	start := time.Now()
	rs, err := GetDefaultClient().Execute(ctx, req, sctx)
	if err != nil {
		metrics.RemotePlanForwardSendErrDuration.Observe(time.Since(start).Seconds())
		metrics.RemotePlanForwardSendErrCounter.Inc()
		GlobalForwardingStats.RecordError()
		return nil, true, err
	}

	if _, ok := IsDMLRecordSet(rs); ok {
		metrics.RemotePlanForwardSendOKDuration.Observe(time.Since(start).Seconds())
		metrics.RemotePlanForwardSendOKCounter.Inc()
	} else if streamRS, ok := rs.(*batchStreamingRecordSet); ok {
		streamRS.forwardStart = start
	} else {
		metrics.RemotePlanForwardSendOKDuration.Observe(time.Since(start).Seconds())
		metrics.RemotePlanForwardSendOKCounter.Inc()
	}

	if feedbackCfg.enabled() && rs != nil {
		if frs, ok := rs.(remotePlanFeedbackRecordSet); ok {
			frs.attachRemotePlanFeedback(&earlyLocInfo.RemotePlanFeedback, feedbackCfg)
		}
	}

	// Successfully forwarded
	GlobalForwardingStats.RecordSuccess()
	return rs, true, nil
}

func hasSelectLock(prepStmt *core.PlanCacheStmt) bool {
	if prepStmt == nil || prepStmt.PreparedAst == nil {
		return false
	}
	sel, ok := prepStmt.PreparedAst.Stmt.(*ast.SelectStmt)
	if !ok || sel == nil || sel.LockInfo == nil {
		return false
	}
	return sel.LockInfo.LockType != ast.SelectLockNone
}

// buildRequestPayload builds the request payload for forwarding.
func buildRequestPayload(sctx sessionctx.Context, prepStmt *core.PlanCacheStmt, planLocation *core.PlanLocationInfo, params []expression.Expression) (*pb.RemoteRequest, error) {
	target := planLocation.TargetStore

	payload := acquireRemoteRequest()
	payload.Target = target
	payload.SqlText = prepStmt.StmtText
	normalizedSQL := prepStmt.NormalizedSQL
	sqlDigest := prepStmt.SQLDigest
	if vars := sctx.GetSessionVars(); vars != nil && vars.StmtCtx != nil {
		if normalized, digest := vars.StmtCtx.SQLDigest(); normalized != "" {
			normalizedSQL = normalized
			sqlDigest = digest
		}
	}
	payload.NormalizedSql = normalizedSQL
	payload.PlanDigest = digestString(prepStmt.PlanDigest)
	payload.SqlDigest = digestString(sqlDigest)
	payload.Session = snapshotSession(sctx)

	// Snapshot params for remote execution
	if len(params) > 0 {
		evalCtx := sctx.GetExprCtx().GetEvalCtx()
		payload.Params = snapshotParams(params, evalCtx)

		// Populate parameter markers so limit-related plan cache key parts can be computed
		// without parsing SQL on the remote side.
		if prepStmt.HasLimit() && sctx.GetSessionVars().EnablePlanCacheForParamLimit {
			if err := core.SetParameterValuesIntoSCtx(sctx.GetPlanCtx(), false, prepStmt.Params, params); err != nil {
				releaseRemoteRequest(payload)
				return nil, err
			}
		}
	}

	// Build PlanCacheInfo from prepStmt for direct plan cache lookup on remote side
	payload.PlanCacheInfo = buildPlanCacheInfo(sctx, prepStmt)

	return payload, nil
}

// buildPlanCacheInfo builds PlanCacheInfo from PlanCacheStmt for remote execution.
// This allows the remote side to directly look up the plan cache without parsing SQL.
func buildPlanCacheInfo(sctx sessionctx.Context, prepStmt *core.PlanCacheStmt) *pb.PlanCacheInfo {
	if prepStmt == nil {
		return nil
	}

	// Convert RelateVersion map to repeated TableVersion
	var relateVersions []*pb.TableVersion
	if prepStmt.RelateVersion != nil {
		relateVersions = make([]*pb.TableVersion, 0, len(prepStmt.RelateVersion))
		for tableID, revision := range prepStmt.RelateVersion {
			tv := tableVersionPool.Get().(*pb.TableVersion)
			resetTableVersion(tv)
			tv.TableId = tableID
			tv.Revision = revision
			relateVersions = append(relateVersions, tv)
		}
	}

	// Get latest schema version for RC isolation
	var latestSchemaVersion int64
	vars := sctx.GetSessionVars()
	if vars.IsIsolation(ast.ReadCommitted) || prepStmt.ForUpdateRead {
		// For RC or FOR UPDATE READ, match NewPlanCacheKey behavior by using the latest schema version.
		latestSchemaVersion = domain.GetDomain(sctx).InfoSchema().SchemaMetaVersion()
	}

	// Get limit information from the statement
	hasLimit := prepStmt.HasLimit()
	limitValues := prepStmt.GetLimitValues()

	// Get stats version hash if PlanCacheInvalidationOnFreshStats is enabled
	var statsVerHash uint64
	if vars.PlanCacheInvalidationOnFreshStats {
		statsVerHash = prepStmt.GetStatsVerHash(sctx)
	}

	// Force plan enable in remote side.
	planCacheEnabled := true
	isReadOnly := prepStmt.PreparedAst.IsReadOnly

	return &pb.PlanCacheInfo{
		SchemaVersion:       prepStmt.SchemaVersion,
		RelateVersions:      relateVersions,
		LatestSchemaVersion: latestSchemaVersion,
		IsReadOnlyOpt:       &pb.PlanCacheInfo_IsReadOnly{IsReadOnly: isReadOnly},
		HasSubquery:         prepStmt.HasSubquery(),
		HasLimit:            hasLimit,
		LimitValues:         limitValues,
		StatsVerHash:        statsVerHash,
		PlanCacheEnabled:    planCacheEnabled,
	}
}

// fieldTypeToProto converts types.FieldType to pb.FieldType for efficient serialization.
// This avoids JSON marshaling overhead on the sender side and JSON parsing on the receiver side.
func fieldTypeToProto(ft *types.FieldType) *pb.FieldType {
	if ft == nil {
		return nil
	}
	pbFt := acquireFieldType()
	pbFt.Tp = uint32(ft.GetType())
	pbFt.Flag = uint32(ft.GetFlag())
	pbFt.Flen = int32(ft.GetFlen())
	pbFt.Decimal = int32(ft.GetDecimal())
	pbFt.Charset = ft.GetCharset()
	pbFt.Collate = ft.GetCollate()
	pbFt.Elems = ft.GetElems()
	pbFt.Array = ft.IsArray()
	// Note: ElemsIsBinaryLit is not commonly used, skip for now
	return pbFt
}

func snapshotParams(exprs []expression.Expression, evalCtx exprctx.EvalContext) []*pb.Param {
	if len(exprs) == 0 {
		return nil
	}
	params := make([]*pb.Param, 0, len(exprs))
	for _, expr := range exprs {
		var val types.Datum
		var ft *types.FieldType
		var evaluated bool

		// Try to get value from constant first
		if c, ok := expr.(*expression.Constant); ok {
			val = c.Value
			ft = c.RetType
			evaluated = true
		} else {
			// For non-constant expressions (like GetVar for user variables),
			// try to evaluate them to get the actual value
			ft = expr.GetType(evalCtx)
			evaluated = evalExprToDatum(expr, ft, evalCtx, &val)
		}

		// Convert FieldType to protobuf struct for efficient serialization
		// This avoids JSON marshaling/unmarshaling overhead
		pbFieldType := fieldTypeToProto(ft)

		var valStr string
		if !evaluated {
			valStr = ""
		} else if val.IsNull() {
			valStr = "<nil>"
		} else {
			valStr = datumToString(&val)
		}

		p := paramPool.Get().(*pb.Param)
		resetParam(p)
		p.Value = valStr
		p.FieldType = pbFieldType
		params = append(params, p)
	}
	return params
}

func datumToString(val *types.Datum) string {
	switch val.Kind() {
	case types.KindInt64:
		return strconv.FormatInt(val.GetInt64(), 10)
	case types.KindUint64:
		return strconv.FormatUint(val.GetUint64(), 10)
	case types.KindFloat32:
		return strconv.FormatFloat(float64(val.GetFloat32()), 'g', -1, 32)
	case types.KindFloat64:
		return strconv.FormatFloat(val.GetFloat64(), 'g', -1, 64)
	case types.KindString:
		return val.GetString()
	case types.KindBytes, types.KindBinaryLiteral, types.KindRaw:
		return string(val.GetBytes())
	case types.KindMysqlDecimal:
		return val.GetMysqlDecimal().String()
	case types.KindMysqlDuration:
		return val.GetMysqlDuration().String()
	case types.KindMysqlTime:
		return val.GetMysqlTime().String()
	case types.KindMysqlEnum:
		return val.GetMysqlEnum().String()
	case types.KindMysqlSet:
		return val.GetMysqlSet().String()
	case types.KindMysqlJSON:
		return val.GetMysqlJSON().String()
	default:
		return fmt.Sprint(val.GetValue())
	}
}

// evalExprToDatum evaluates an expression and stores the result in the datum.
// Returns true if evaluation was successful.
func evalExprToDatum(expr expression.Expression, ft *types.FieldType, evalCtx exprctx.EvalContext, val *types.Datum) bool {
	if ft == nil {
		return false
	}

	// Choose evaluation method based on the expression's return type
	switch ft.EvalType() {
	case types.ETInt:
		result, isNull, err := expr.EvalInt(evalCtx, chunk.Row{})
		if err == nil && !isNull {
			if mysql.HasUnsignedFlag(ft.GetFlag()) {
				val.SetUint64(uint64(result))
			} else {
				val.SetInt64(result)
			}
			return true
		}
	case types.ETReal:
		result, isNull, err := expr.EvalReal(evalCtx, chunk.Row{})
		if err == nil && !isNull {
			val.SetFloat64(result)
			return true
		}
	case types.ETDecimal:
		result, isNull, err := expr.EvalDecimal(evalCtx, chunk.Row{})
		if err == nil && !isNull {
			val.SetMysqlDecimal(result)
			return true
		}
	case types.ETString:
		result, isNull, err := expr.EvalString(evalCtx, chunk.Row{})
		if err == nil && !isNull {
			val.SetString(result, ft.GetCollate())
			return true
		}
	case types.ETDatetime, types.ETTimestamp:
		result, isNull, err := expr.EvalTime(evalCtx, chunk.Row{})
		if err == nil && !isNull {
			val.SetMysqlTime(result)
			return true
		}
	case types.ETDuration:
		result, isNull, err := expr.EvalDuration(evalCtx, chunk.Row{})
		if err == nil && !isNull {
			val.SetMysqlDuration(result)
			return true
		}
	case types.ETJson:
		result, isNull, err := expr.EvalJSON(evalCtx, chunk.Row{})
		if err == nil && !isNull {
			val.SetMysqlJSON(result)
			return true
		}
	}

	// Fallback: try string evaluation for any type
	result, isNull, err := expr.EvalString(evalCtx, chunk.Row{})
	if err == nil && !isNull {
		val.SetString(result, ft.GetCollate())
		return true
	}

	return false
}

func snapshotSession(sctx sessionctx.Context) *pb.SessionSnapshot {
	vars := sctx.GetSessionVars()
	isolation, _ := vars.GetSystemVar(variable.TxnIsolation)
	timeZone := "Local"
	if vars.TimeZone != nil {
		timeZone = vars.TimeZone.String()
	}

	snap := acquireSessionSnapshot()

	// Get user info
	var authUsername, authHostname string
	if vars.User != nil {
		authUsername = vars.User.AuthUsername
		authHostname = vars.User.AuthHostname
	}

	// Get charset and collation
	connCharset, connCollation := vars.GetCharsetInfo()

	isolationReadEngines := isolationReadEnginesString(vars)

	startTS := uint64(0)
	if vars.InTxn() {
		startTS = vars.TxnCtx.StartTS
	}
	// if startTS == 0 {
	// 	startTS = getReadTSFromOracle(sctx)
	// }

	snap.CurrentDb = vars.CurrentDB
	snap.SqlMode = uint64(vars.SQLMode)
	snap.TimeZone = timeZone
	snap.Isolation = isolation
	snap.TxnMode = vars.TxnMode
	snap.ResourceGroup = vars.ResourceGroupName
	snap.AuthUsername = authUsername
	snap.AuthHostname = authHostname
	snap.PartitionPruneMode = vars.PartitionPruneMode.Load()
	snap.SkipMissingPartitionStats = vars.SkipMissingPartitionStats
	snap.IsolationReadEngines = isolationReadEngines
	snap.SelectLimit = vars.SelectLimit
	snap.ConnCharset = connCharset
	snap.ConnCollation = connCollation
	snap.InRestrictedSql = vars.InRestrictedSQL
	snap.ForeignKeyChecks = vars.ForeignKeyChecks
	snap.ForShareLockEnabledByNoop = vars.StmtCtx.ForShareLockEnabledByNoop
	snap.SharedLockPromotion = vars.SharedLockPromotion
	snap.EnablePrepPlanCache = vars.EnablePreparedPlanCache
	snap.EnableNonPrepPlanCache = vars.EnableNonPreparedPlanCache
	snap.EnableInstancePlanCache = variable.EnableInstancePlanCache.Load()
	snap.EnablePlanCacheForParamLimit = vars.EnablePlanCacheForParamLimit
	snap.EnableAsyncCommit = vars.EnableAsyncCommit
	snap.Enable_1Pc = vars.Enable1PC
	if vars.RowEncoder.Enable {
		snap.RowFormatVersion = variable.DefTiDBRowFormatV2
	} else {
		snap.RowFormatVersion = variable.DefTiDBRowFormatV1
	}
	snap.InstancePlanCacheMaxSize = uint64(variable.InstancePlanCacheMaxMemSize.Load())
	snap.MaxChunkSize = uint32(vars.MaxChunkSize)
	snap.TidbxEnableIndexLookupPushDown = vars.EnableIndexLookUpPushDown
	snap.TidbxEnableSingleStoreTxn_1Pc = txnkvtxn.SingleStoreTxnCommitEnabled
	snap.TidbxStoreBatchGet = tikvrpc.TiDBXStoreBatchGet.Load()
	snap.TidbxFastPath = variable.EnableFastPath.Load()

	if snap.Txn == nil {
		snap.Txn = &pb.TxnContext{}
	}
	snap.Txn.InTxn = vars.InTxn()
	snap.Txn.Autocommit = vars.IsAutocommit()
	snap.Txn.StartTs = startTS
	snap.Txn.ForUpdateTs = vars.TxnCtx.GetForUpdateTS()

	return snap
}

func isolationReadEnginesString(vars *variable.SessionVars) string {
	if vars == nil || len(vars.IsolationReadEngines) == 0 {
		return ""
	}
	hasTiDB := false
	hasTiKV := false
	hasTiFlash := false
	if _, ok := vars.IsolationReadEngines[kv.TiDB]; ok {
		hasTiDB = true
	}
	if _, ok := vars.IsolationReadEngines[kv.TiKV]; ok {
		hasTiKV = true
	}
	if _, ok := vars.IsolationReadEngines[kv.TiFlash]; ok {
		hasTiFlash = true
	}
	count := 0
	totalLen := 0
	if hasTiDB {
		count++
		totalLen += len(kv.TiDB.Name())
	}
	if hasTiKV {
		count++
		totalLen += len(kv.TiKV.Name())
	}
	if hasTiFlash {
		count++
		totalLen += len(kv.TiFlash.Name())
	}
	if count == 0 {
		return ""
	}
	totalLen += count - 1
	var b strings.Builder
	b.Grow(totalLen)
	first := true
	if hasTiDB {
		b.WriteString(kv.TiDB.Name())
		first = false
	}
	if hasTiKV {
		if !first {
			b.WriteByte(',')
		}
		b.WriteString(kv.TiKV.Name())
		first = false
	}
	if hasTiFlash {
		if !first {
			b.WriteByte(',')
		}
		b.WriteString(kv.TiFlash.Name())
	}
	return b.String()
}

// getReadTSFromOracle fetches a timestamp directly from Oracle without creating a full transaction.
// This is a lightweight alternative to GetStmtReadTS() which avoids:
// - Creating a new TiKV transaction (NewTiKVTxn)
// - Setting up transaction context and options
// - Memory allocation for transaction buffers
func getReadTSFromOracle(sctx sessionctx.Context) uint64 {
	vars := sctx.GetSessionVars()
	oracleStore := sctx.GetStore().GetOracle()
	txnScope := vars.TxnCtx.TxnScope
	if txnScope == "" {
		txnScope = oracle.GlobalTxnScope
	}
	option := &oracle.Option{TxnScope: txnScope}

	var ts uint64
	var err error
	if vars.UseLowResolutionTSO() {
		ts, err = oracleStore.GetLowResolutionTimestamp(context.Background(), option)
	} else {
		ts, err = oracleStore.GetTimestamp(context.Background(), option)
	}
	if err != nil {
		logutil.BgLogger().Warn("[remote] failed to get timestamp from oracle, using 0",
			zap.Error(err))
		return 0
	}
	return ts
}

func digestString(d *parser.Digest) string {
	if d == nil {
		return ""
	}
	return d.String()
}

func appendBinaryValue(chk *chunk.Chunk, colIdx int, ft *types.FieldType, data []byte) error {
	switch ft.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		if len(data) < 8 {
			return errors.Errorf("invalid binary data length for integer: %d", len(data))
		}
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			chk.AppendUint64(colIdx, binary.LittleEndian.Uint64(data))
		} else {
			chk.AppendInt64(colIdx, int64(binary.LittleEndian.Uint64(data)))
		}
	case mysql.TypeFloat:
		if len(data) < 4 {
			return errors.Errorf("invalid binary data length for float32: %d", len(data))
		}
		bits := binary.LittleEndian.Uint32(data)
		chk.AppendFloat32(colIdx, math.Float32frombits(bits))
	case mysql.TypeDouble:
		if len(data) < 8 {
			return errors.Errorf("invalid binary data length for float64: %d", len(data))
		}
		bits := binary.LittleEndian.Uint64(data)
		chk.AppendFloat64(colIdx, math.Float64frombits(bits))
	case mysql.TypeNewDecimal:
		d := new(types.MyDecimal)
		if err := d.FromString(data); err != nil {
			return err
		}
		chk.AppendMyDecimal(colIdx, d)
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		t, err := types.ParseTime(types.DefaultStmtNoWarningContext, string(data), ft.GetType(), ft.GetDecimal())
		if err != nil {
			return err
		}
		chk.AppendTime(colIdx, t)
	case mysql.TypeDuration:
		if len(data) < 8 {
			return errors.Errorf("invalid binary data length for duration: %d", len(data))
		}
		nanos := int64(binary.LittleEndian.Uint64(data))
		d := types.Duration{Duration: time.Duration(nanos), Fsp: ft.GetDecimal()}
		chk.AppendDuration(colIdx, d)
	case mysql.TypeJSON:
		j, err := types.ParseBinaryJSONFromString(string(data))
		if err != nil {
			return err
		}
		chk.AppendJSON(colIdx, j)
	case mysql.TypeEnum:
		if len(data) < 8 {
			return errors.Errorf("invalid binary data length for enum: %d", len(data))
		}
		val := binary.LittleEndian.Uint64(data)
		chk.AppendEnum(colIdx, types.Enum{Value: val})
	case mysql.TypeSet:
		if len(data) < 8 {
			return errors.Errorf("invalid binary data length for set: %d", len(data))
		}
		val := binary.LittleEndian.Uint64(data)
		chk.AppendSet(colIdx, types.Set{Value: val})
	case mysql.TypeYear:
		if len(data) >= 8 {
			chk.AppendInt64(colIdx, int64(binary.LittleEndian.Uint64(data)))
		} else if len(data) >= 2 {
			chk.AppendInt64(colIdx, int64(binary.LittleEndian.Uint16(data)))
		} else {
			return errors.Errorf("invalid binary data length for year: %d", len(data))
		}
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		chk.AppendBytes(colIdx, data)
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString:
		chk.AppendBytes(colIdx, data)
	case mysql.TypeGeometry:
		chk.AppendBytes(colIdx, data)
	case mysql.TypeBit:
		chk.AppendBytes(colIdx, data)
	default:
		chk.AppendBytes(colIdx, data)
	}
	return nil
}

func appendStringValue(chk *chunk.Chunk, colIdx int, ft *types.FieldType, val string) error {
	switch ft.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			v, err := strconv.ParseUint(val, 10, 64)
			if err != nil {
				return err
			}
			chk.AppendUint64(colIdx, v)
		} else {
			v, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return err
			}
			chk.AppendInt64(colIdx, v)
		}
	case mysql.TypeFloat:
		v, err := strconv.ParseFloat(val, 32)
		if err != nil {
			return err
		}
		chk.AppendFloat32(colIdx, float32(v))
	case mysql.TypeDouble:
		v, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return err
		}
		chk.AppendFloat64(colIdx, v)
	case mysql.TypeNewDecimal:
		d := new(types.MyDecimal)
		if err := d.FromString([]byte(val)); err != nil {
			return err
		}
		chk.AppendMyDecimal(colIdx, d)
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		t, err := types.ParseTime(types.DefaultStmtNoWarningContext, val, ft.GetType(), ft.GetDecimal())
		if err != nil {
			return err
		}
		chk.AppendTime(colIdx, t)
	case mysql.TypeDuration:
		d, _, err := types.ParseDuration(types.DefaultStmtNoWarningContext, val, ft.GetDecimal())
		if err != nil {
			return err
		}
		chk.AppendDuration(colIdx, d)
	case mysql.TypeJSON:
		j, err := types.ParseBinaryJSONFromString(val)
		if err != nil {
			return err
		}
		chk.AppendJSON(colIdx, j)
	case mysql.TypeEnum:
		chk.AppendString(colIdx, val)
	case mysql.TypeSet:
		chk.AppendString(colIdx, val)
	case mysql.TypeYear:
		v, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return err
		}
		chk.AppendInt64(colIdx, v)
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		chk.AppendBytes(colIdx, []byte(val))
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString:
		chk.AppendString(colIdx, val)
	case mysql.TypeGeometry:
		chk.AppendBytes(colIdx, []byte(val))
	case mysql.TypeBit:
		chk.AppendBytes(colIdx, []byte(val))
	default:
		chk.AppendString(colIdx, val)
	}
	return nil
}
