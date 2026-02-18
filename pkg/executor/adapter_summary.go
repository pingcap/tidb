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

package executor

import (
	"context"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/pingcap/tidb/pkg/util/replayer"
	"github.com/pingcap/tidb/pkg/util/stmtsummary"
	stmtsummaryv2 "github.com/pingcap/tidb/pkg/util/stmtsummary/v2"
	"github.com/pingcap/tidb/pkg/util/topsql"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/pingcap/tidb/pkg/util/traceevent"
	"go.uber.org/zap"
)

type planDigestAlias struct {
	Digest string
}

func (digest planDigestAlias) planDigestDumpTriggerCheck(config *traceevent.DumpTriggerConfig) bool {
	return config.UserCommand.PlanDigest == digest.Digest
}

// SummaryStmt collects statements for information_schema.statements_summary
func (a *ExecStmt) SummaryStmt(succ bool) {
	sessVars := a.Ctx.GetSessionVars()
	var userString string
	if sessVars.User != nil {
		userString = sessVars.User.Username
	}

	// Internal SQLs must also be recorded to keep the consistency of `PrevStmt` and `PrevStmtDigest`.
	// If this SQL is under `explain explore {SQL}`, we still want to record them in stmt summary.
	isInternalSQL := (sessVars.InRestrictedSQL || len(userString) == 0) && !sessVars.InExplainExplore
	if !stmtsummaryv2.Enabled() || (isInternalSQL && !stmtsummaryv2.EnabledInternal()) {
		sessVars.SetPrevStmtDigest("")
		return
	}
	// Ignore `PREPARE` statements, but record `EXECUTE` statements.
	if _, ok := a.StmtNode.(*ast.PrepareStmt); ok {
		return
	}
	stmtCtx := sessVars.StmtCtx
	// Make sure StmtType is filled even if succ is false.
	if stmtCtx.StmtType == "" {
		stmtCtx.StmtType = stmtctx.GetStmtLabel(context.Background(), a.StmtNode)
	}
	normalizedSQL, digest := stmtCtx.SQLDigest()
	costTime := sessVars.GetTotalCostDuration()
	charset, collation := sessVars.GetCharsetInfo()

	var prevSQL, prevSQLDigest string
	if _, ok := a.StmtNode.(*ast.CommitStmt); ok {
		// If prevSQLDigest is not recorded, it means this `commit` is the first SQL once stmt summary is enabled,
		// so it's OK just to ignore it.
		if prevSQLDigest = sessVars.GetPrevStmtDigest(); len(prevSQLDigest) == 0 {
			return
		}
		prevSQL = sessVars.PrevStmt.String()
	}
	sessVars.SetPrevStmtDigest(digest.String())

	// Generating plan digest is slow, only generate it once if it's 'Point_Get'.
	// If it's a point get, different SQLs leads to different plans, so SQL digest
	// is enough to distinguish different plans in this case.
	var planDigest string
	if a.Plan.TP() != plancodec.TypePointGet {
		_, tmp := GetPlanDigest(stmtCtx)
		planDigest = tmp.String()
		traceevent.CheckFlightRecorderDumpTrigger(a.GoCtx, "dump_trigger.user_command.plan_digest", planDigestAlias{planDigest}.planDigestDumpTriggerCheck)
	}

	execDetail := stmtCtx.GetExecDetails()
	copTaskInfo := stmtCtx.CopTasksSummary()
	memMax := sessVars.MemTracker.MaxConsumed()
	diskMax := sessVars.DiskTracker.MaxConsumed()
	stmtDetail, tikvExecDetail, ruDetail := execdetails.GetExecDetailsFromContext(a.GoCtx)

	if stmtCtx.WaitLockLeaseTime > 0 {
		if execDetail.BackoffSleep == nil {
			execDetail.BackoffSleep = make(map[string]time.Duration)
		}
		execDetail.BackoffSleep["waitLockLeaseForCacheTable"] = stmtCtx.WaitLockLeaseTime
		execDetail.BackoffTime += stmtCtx.WaitLockLeaseTime
		execDetail.TimeDetail.WaitTime += stmtCtx.WaitLockLeaseTime
	}

	var keyspaceID uint32
	keyspaceName := keyspace.GetKeyspaceNameBySettings()
	if !keyspace.IsKeyspaceNameEmpty(keyspaceName) {
		keyspaceID = uint32(a.Ctx.GetStore().GetCodec().GetKeyspaceID())
	}

	if sessVars.CacheStmtExecInfo == nil {
		sessVars.CacheStmtExecInfo = &stmtsummary.StmtExecInfo{}
	}
	stmtExecInfo := sessVars.CacheStmtExecInfo
	stmtExecInfo.SchemaName = strings.ToLower(sessVars.CurrentDB)
	stmtExecInfo.Charset = charset
	stmtExecInfo.Collation = collation
	stmtExecInfo.NormalizedSQL = normalizedSQL
	stmtExecInfo.Digest = digest.String()
	stmtExecInfo.PrevSQL = prevSQL
	stmtExecInfo.PrevSQLDigest = prevSQLDigest
	stmtExecInfo.PlanDigest = planDigest
	stmtExecInfo.User = userString
	stmtExecInfo.TotalLatency = costTime
	stmtExecInfo.ParseLatency = sessVars.DurationParse
	stmtExecInfo.CompileLatency = sessVars.DurationCompile
	stmtExecInfo.StmtCtx = stmtCtx
	stmtExecInfo.CopTasks = copTaskInfo
	stmtExecInfo.ExecDetail = execDetail
	stmtExecInfo.MemMax = memMax
	stmtExecInfo.DiskMax = diskMax
	stmtExecInfo.StartTime = sessVars.StartTime
	stmtExecInfo.IsInternal = isInternalSQL
	stmtExecInfo.Succeed = succ
	stmtExecInfo.PlanInCache = sessVars.FoundInPlanCache
	stmtExecInfo.PlanInBinding = sessVars.FoundInBinding
	stmtExecInfo.ExecRetryCount = a.retryCount
	stmtExecInfo.StmtExecDetails = stmtDetail
	stmtExecInfo.ResultRows = stmtCtx.GetResultRowsCount()
	stmtExecInfo.TiKVExecDetails = &tikvExecDetail
	stmtExecInfo.Prepared = a.isPreparedStmt
	stmtExecInfo.KeyspaceName = keyspaceName
	stmtExecInfo.KeyspaceID = keyspaceID
	stmtExecInfo.RUDetail = ruDetail
	stmtExecInfo.ResourceGroupName = sessVars.StmtCtx.ResourceGroupName
	stmtExecInfo.CPUUsages = sessVars.SQLCPUUsages.GetCPUUsages()
	stmtExecInfo.PlanCacheUnqualified = sessVars.StmtCtx.PlanCacheUnqualified()
	stmtExecInfo.LazyInfo = a
	if a.retryCount > 0 {
		stmtExecInfo.ExecRetryTime = costTime - sessVars.DurationParse - sessVars.DurationCompile - time.Since(a.retryStartTime)
	}
	stmtExecInfo.MemArbitration = stmtCtx.MemTracker.MemArbitration().Seconds()

	stmtsummaryv2.Add(stmtExecInfo)
}

// GetOriginalSQL implements StmtExecLazyInfo interface.
func (a *ExecStmt) GetOriginalSQL() string {
	stmt := a.getLazyStmtText()
	return stmt.String()
}

// GetEncodedPlan implements StmtExecLazyInfo interface.
func (a *ExecStmt) GetEncodedPlan() (p string, h string, e any) {
	defer func() {
		e = recover()
		if e != nil {
			logutil.BgLogger().Warn("fail to generate plan info",
				zap.Stack("backtrace"),
				zap.Any("error", e))
		}
	}()

	sessVars := a.Ctx.GetSessionVars()
	p, h = getEncodedPlan(sessVars.StmtCtx, !sessVars.InRestrictedSQL)
	return
}

// GetBinaryPlan implements StmtExecLazyInfo interface.
func (a *ExecStmt) GetBinaryPlan() string {
	if variable.GenerateBinaryPlan.Load() {
		return getBinaryPlan(a.Ctx)
	}
	return ""
}

// GetPlanDigest implements StmtExecLazyInfo interface.
func (a *ExecStmt) GetPlanDigest() string {
	if a.Plan.TP() == plancodec.TypePointGet {
		_, planDigest := GetPlanDigest(a.Ctx.GetSessionVars().StmtCtx)
		return planDigest.String()
	}
	return ""
}

// GetBindingSQLAndDigest implements StmtExecLazyInfo interface, providing the
// normalized SQL and digest, with additional rules specific to bindings.
func (a *ExecStmt) GetBindingSQLAndDigest() (s string, d string) {
	normalizedSQL, digest := parser.NormalizeDigestForBinding(bindinfo.RestoreDBForBinding(a.StmtNode, a.Ctx.GetSessionVars().CurrentDB))
	return normalizedSQL, digest.String()
}

// GetTextToLog return the query text to log.
func (a *ExecStmt) GetTextToLog(keepHint bool) string {
	var sql string
	sessVars := a.Ctx.GetSessionVars()
	rmode := sessVars.EnableRedactLog
	if rmode == errors.RedactLogEnable {
		if keepHint {
			sql = parser.NormalizeKeepHint(sessVars.StmtCtx.OriginalSQL)
		} else {
			sql, _ = sessVars.StmtCtx.SQLDigest()
		}
	} else if sensitiveStmt, ok := a.StmtNode.(ast.SensitiveStmtNode); ok {
		sql = sensitiveStmt.SecureText()
	} else {
		sql = redact.String(rmode, sessVars.StmtCtx.OriginalSQL+sessVars.PlanCacheParams.String())
	}
	return sql
}

// getLazyText is equivalent to `a.GetTextToLog(false)`. Note that the s.Params is a shallow copy of
// `sessVars.PlanCacheParams`, so you can only use the lazy text within the current stmt context.
func (a *ExecStmt) getLazyStmtText() (s variable.LazyStmtText) {
	sessVars := a.Ctx.GetSessionVars()
	rmode := sessVars.EnableRedactLog
	if rmode == errors.RedactLogEnable {
		sql, _ := sessVars.StmtCtx.SQLDigest()
		s.SetText(sql)
	} else if sensitiveStmt, ok := a.StmtNode.(ast.SensitiveStmtNode); ok {
		sql := sensitiveStmt.SecureText()
		s.SetText(sql)
	} else {
		s.Redact = rmode
		s.SQL = sessVars.StmtCtx.OriginalSQL
		s.Params = *sessVars.PlanCacheParams
	}
	return
}

// updatePrevStmt is equivalent to `sessVars.PrevStmt = FormatSQL(a.GetTextToLog(false))`
func (a *ExecStmt) updatePrevStmt() {
	sessVars := a.Ctx.GetSessionVars()
	if sessVars.PrevStmt == nil {
		sessVars.PrevStmt = &variable.LazyStmtText{Format: formatSQL}
	}
	rmode := sessVars.EnableRedactLog
	if rmode == errors.RedactLogEnable {
		sql, _ := sessVars.StmtCtx.SQLDigest()
		sessVars.PrevStmt.SetText(sql)
	} else if sensitiveStmt, ok := a.StmtNode.(ast.SensitiveStmtNode); ok {
		sql := sensitiveStmt.SecureText()
		sessVars.PrevStmt.SetText(sql)
	} else {
		sessVars.PrevStmt.Update(rmode, sessVars.StmtCtx.OriginalSQL, sessVars.PlanCacheParams)
	}
}

func (a *ExecStmt) observeStmtBeginForTopProfiling(ctx context.Context) context.Context {
	if !topsqlstate.TopProfilingEnabled() && IsFastPlan(a.Plan) {
		// To reduce the performance impact on fast plan.
		// Drop them does not cause notable accuracy issue in Top Profiling.
		return ctx
	}

	vars := a.Ctx.GetSessionVars()
	sc := vars.StmtCtx
	normalizedSQL, sqlDigest := sc.SQLDigest()
	normalizedPlan, planDigest := GetPlanDigest(sc)
	var sqlDigestByte, planDigestByte []byte
	if sqlDigest != nil {
		sqlDigestByte = sqlDigest.Bytes()
	}
	if planDigest != nil {
		planDigestByte = planDigest.Bytes()
	}
	stats := a.Ctx.GetStmtStats()
	if !topsqlstate.TopProfilingEnabled() {
		// Always attach the SQL and plan info uses to catch the running SQL when Top Profiling is enabled in execution.
		// Note: Goroutine labels for CPU profiling are only set when TopSQL is enabled.
		if stats != nil {
			stats.OnExecutionBegin(sqlDigestByte, planDigestByte, vars.InPacketBytes.Load())
		}
		return topsql.AttachSQLAndPlanInfo(ctx, sqlDigest, planDigest)
	}

	if stats != nil {
		stats.OnExecutionBegin(sqlDigestByte, planDigestByte, vars.InPacketBytes.Load())
		// This is a special logic prepared for TiKV's SQLExecCount.
		sc.KvExecCounter = stats.CreateKvExecCounter(sqlDigestByte, planDigestByte)
	}

	isSQLRegistered := sc.IsSQLRegistered.Load()
	if !isSQLRegistered {
		topsql.RegisterSQL(normalizedSQL, sqlDigest, vars.InRestrictedSQL)
	}
	sc.IsSQLAndPlanRegistered.Store(true)
	if len(normalizedPlan) == 0 {
		return ctx
	}
	topsql.RegisterPlan(normalizedPlan, planDigest)
	return topsql.AttachSQLAndPlanInfo(ctx, sqlDigest, planDigest)
}

// UpdatePlanCacheRuntimeInfo updates the runtime information of the plan in the plan cache.
func (a *ExecStmt) UpdatePlanCacheRuntimeInfo() {
	if !vardef.EnableInstancePlanCache.Load() {
		return // only record for Instance Plan Cache
	}
	v := a.Ctx.GetSessionVars().PlanCacheValue
	if v == nil {
		return
	}
	pcv, ok := v.(*plannercore.PlanCacheValue)
	if !ok {
		return
	}

	execDetail := a.Ctx.GetSessionVars().StmtCtx.GetExecDetails()
	var procKeys, totKeys int64
	if execDetail.ScanDetail != nil { // only support TiKV
		procKeys = execDetail.ScanDetail.ProcessedKeys
		totKeys = execDetail.ScanDetail.TotalKeys
	}
	costTime := a.Ctx.GetSessionVars().GetTotalCostDuration()
	pcv.UpdateRuntimeInfo(procKeys, totKeys, int64(costTime))
	a.Ctx.GetSessionVars().PlanCacheValue = nil // reset
}

func (a *ExecStmt) observeStmtFinishedForTopProfiling() {
	vars := a.Ctx.GetSessionVars()
	if vars == nil {
		return
	}
	if stats := a.Ctx.GetStmtStats(); stats != nil && topsqlstate.TopProfilingEnabled() {
		sqlDigest, planDigest := a.getSQLPlanDigest()
		execDuration := vars.GetTotalCostDuration()
		stats.OnExecutionFinished(sqlDigest, planDigest, execDuration, vars.OutPacketBytes.Load())
	}
}

func (a *ExecStmt) getSQLPlanDigest() (sqlDigest, planDigest []byte) {
	vars := a.Ctx.GetSessionVars()
	if _, d := vars.StmtCtx.SQLDigest(); d != nil {
		sqlDigest = d.Bytes()
	}
	if _, d := vars.StmtCtx.GetPlanDigest(); d != nil {
		planDigest = d.Bytes()
	}
	return sqlDigest, planDigest
}

// only allow select/delete/update/insert/execute stmt captured by continues capture
func checkPlanReplayerContinuesCaptureValidStmt(stmtNode ast.StmtNode) bool {
	switch stmtNode.(type) {
	case *ast.SelectStmt, *ast.DeleteStmt, *ast.UpdateStmt, *ast.InsertStmt, *ast.ExecuteStmt:
		return true
	default:
		return false
	}
}

func checkPlanReplayerCaptureTask(sctx sessionctx.Context, stmtNode ast.StmtNode, startTS uint64) {
	dom := domain.GetDomain(sctx)
	if dom == nil {
		return
	}
	handle := dom.GetPlanReplayerHandle()
	if handle == nil {
		return
	}
	tasks := handle.GetTasks()
	if len(tasks) == 0 {
		return
	}
	_, sqlDigest := sctx.GetSessionVars().StmtCtx.SQLDigest()
	_, planDigest := sctx.GetSessionVars().StmtCtx.GetPlanDigest()
	if sqlDigest == nil || planDigest == nil {
		return
	}
	key := replayer.PlanReplayerTaskKey{
		SQLDigest:  sqlDigest.String(),
		PlanDigest: planDigest.String(),
	}
	for _, task := range tasks {
		if task.SQLDigest == sqlDigest.String() {
			if task.PlanDigest == "*" || task.PlanDigest == planDigest.String() {
				sendPlanReplayerDumpTask(key, sctx, stmtNode, startTS, false)
				return
			}
		}
	}
}

func checkPlanReplayerContinuesCapture(sctx sessionctx.Context, stmtNode ast.StmtNode, startTS uint64) {
	dom := domain.GetDomain(sctx)
	if dom == nil {
		return
	}
	handle := dom.GetPlanReplayerHandle()
	if handle == nil {
		return
	}
	_, sqlDigest := sctx.GetSessionVars().StmtCtx.SQLDigest()
	_, planDigest := sctx.GetSessionVars().StmtCtx.GetPlanDigest()
	key := replayer.PlanReplayerTaskKey{
		SQLDigest:  sqlDigest.String(),
		PlanDigest: planDigest.String(),
	}
	existed := sctx.GetSessionVars().CheckPlanReplayerFinishedTaskKey(key)
	if existed {
		return
	}
	sendPlanReplayerDumpTask(key, sctx, stmtNode, startTS, true)
	sctx.GetSessionVars().AddPlanReplayerFinishedTaskKey(key)
}

func sendPlanReplayerDumpTask(key replayer.PlanReplayerTaskKey, sctx sessionctx.Context, stmtNode ast.StmtNode,
	startTS uint64, isContinuesCapture bool) {
	stmtCtx := sctx.GetSessionVars().StmtCtx
	handle := sctx.Value(bindinfo.SessionBindInfoKeyType).(bindinfo.SessionBindingHandle)
	bindings := handle.GetAllSessionBindings()
	dumpTask := &domain.PlanReplayerDumpTask{
		PlanReplayerTaskKey: key,
		StartTS:             startTS,
		TblStats:            stmtCtx.TableStats,
		SessionBindings:     [][]*bindinfo.Binding{bindings},
		SessionVars:         sctx.GetSessionVars(),
		ExecStmts:           []ast.StmtNode{stmtNode},
		Analyze:             false,
		IsCapture:           true,
		IsContinuesCapture:  isContinuesCapture,
	}
	dumpTask.EncodedPlan, _ = getEncodedPlan(stmtCtx, false)
	if execStmtAst, ok := stmtNode.(*ast.ExecuteStmt); ok {
		planCacheStmt, err := plannercore.GetPreparedStmt(execStmtAst, sctx.GetSessionVars())
		if err != nil {
			logutil.BgLogger().Warn("fail to find prepared ast for dumping plan replayer", zap.String("category", "plan-replayer-capture"),
				zap.String("sqlDigest", key.SQLDigest),
				zap.String("planDigest", key.PlanDigest),
				zap.Error(err))
		} else {
			dumpTask.ExecStmts = []ast.StmtNode{planCacheStmt.PreparedAst.Stmt}
		}
	}
	domain.GetDomain(sctx).GetPlanReplayerHandle().SendTask(dumpTask)
}
