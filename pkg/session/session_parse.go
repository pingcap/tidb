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
	"regexp"
	"runtime/pprof"
	"slices"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	session_metrics "github.com/pingcap/tidb/pkg/session/metrics"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	parserutil "github.com/pingcap/tidb/pkg/util/parser"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/topsql"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/pingcap/tidb/pkg/util/traceevent"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"go.uber.org/zap"
)

func approxParseSQLTokenCnt(sql string) (tokenCnt int64) {
	f := false
	buffer := struct {
		d [10]byte
		n int
	}{}

	hitCoreToken := false
	hasSelect := false
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		if 'a' <= c && c <= 'z' || '0' <= c && c <= '9' || c == '_' {
			f = true
			if !hitCoreToken {
				if buffer.n < len(buffer.d) {
					buffer.d[buffer.n] = c
					buffer.n++
				}
			}
			continue
		}
		if f {
			f = false
			tokenCnt++
			if !hitCoreToken {
				token := keySQLToken[string(buffer.d[:buffer.n])]
				if token&isSelectSQLToken > 0 {
					hasSelect = true
				} else if token&coreSQLToken > 0 {
					hitCoreToken = true
				} else if token&bypassSQLToken == 0 {
					if !hasSelect {
						return 0
					}
					// expect `from` after `select`
				}
				buffer.n = 0
			}
		}
		if sql[i] == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			i += 2 // skip "/*"
			for i+1 < len(sql) && !(sql[i] == '*' && sql[i+1] == '/') {
				i++
			}
			i++ // skip "*/"
			continue
		}
		if sql[i] == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			i += 2 // skip "--"
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			continue
		}
		if sql[i] == '#' {
			i++ // skip "#"
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			continue
		}
		if sql[i] == '"' || sql[i] == '\'' {
			quote := sql[i]
			i++ // skip quote
			for i < len(sql) && sql[i] != quote {
				if sql[i] == '\\' && i+1 < len(sql) {
					i++ // skip escape character
				}
				i++
			}
			tokenCnt++
			continue
		}
		if sql[i] == '`' {
			i++ // skip "`"
			for i < len(sql) && sql[i] != '`' {
				if sql[i] == '\\' && i+1 < len(sql) {
					i++ // skip escape character
				}
				i++
			}
			tokenCnt++
			continue
		}
		if sql[i] == '?' {
			tokenCnt++
			continue
		}
	}
	if f {
		tokenCnt++
	}
	if !hitCoreToken {
		return 0
	}
	return
}

// approximate memory quota related token count for compiling plan of a `Normalized` SQL statement
// if the SQL has `select` clause, it must have `from` clause.
func approxCompilePlanTokenCnt(sql string, hasSelect bool) (tokenCnt int64) {
	const tokenFrom = "from"
	const lenTokenFrom = len(tokenFrom)
	n := 0
	hasSelectFrom := false
	for i, c := range sql {
		if 'a' <= c && c <= 'z' || '0' <= c && c <= '9' || c == '_' || c == '`' || c == '.' {
			n++
			continue
		}
		if n > 0 {
			tokenCnt++
			if hasSelect && !hasSelectFrom && n == lenTokenFrom && sql[i-lenTokenFrom:i] == tokenFrom {
				hasSelectFrom = true
			}
			n = 0
		}
		if c == '?' {
			tokenCnt++
			continue
		}
	}
	if n > 0 {
		tokenCnt++
	}
	if hasSelect && !hasSelectFrom {
		return 0
	}
	return
}

// approximate memory quota for parsing SQL
func approxParseSQLMemQuota(sql string) int64 {
	tokenCnt := approxParseSQLTokenCnt(sql)
	return tokenCnt * defParseSQLQuotaPerToken
}

// approximate memory quota for compiling plan of a `Normalized` SQL statement
func approxCompilePlanMemQuota(sql string, hasSelect bool) int64 {
	tokenCnt := approxCompilePlanTokenCnt(sql, hasSelect)
	return tokenCnt * defCompilePlanQuotaPerToken
}

func (s *session) ParseSQL(ctx context.Context, sql string, params ...parser.ParseParam) ([]ast.StmtNode, []error, error) {
	globalMemArbitrator := memory.GlobalMemArbitrator()
	execUseArbitrator := false
	parseSQLMemQuota := int64(0)
	if globalMemArbitrator != nil && s.sessionVars.ConnectionID != 0 {
		if s.sessionVars.MemArbitrator.WaitAverse != variable.MemArbitratorNolimit {
			parseSQLMemQuota = approxParseSQLMemQuota(sql)
			execUseArbitrator = parseSQLMemQuota > 0
		}
	}

	if execUseArbitrator {
		uid := s.sessionVars.ConnectionID

		if globalMemArbitrator.AtMemRisk() {
			if s.sessionPlanCache != nil {
				s.sessionPlanCache.DeleteAll()
			}
			for globalMemArbitrator.AtMemRisk() {
				if globalMemArbitrator.AtOOMRisk() {
					metrics.GlobalMemArbitratorSubTasks.ForceKillParse.Inc()
					return nil, nil, exeerrors.ErrQueryExecStopped.GenWithStackByArgs(memory.ArbitratorOOMRiskKill.String()+defSuffixParseSQL, uid)
				}
				time.Sleep(defOOMRiskCheckDur)
			}
		}

		globalMemArbitrator.ConsumeQuotaFromAwaitFreePool(uid, parseSQLMemQuota)
		defer globalMemArbitrator.ConsumeQuotaFromAwaitFreePool(uid, -parseSQLMemQuota)
	}

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
	res := slices.Clone(tmp)
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

	pi := sessmgr.ProcessInfo{
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
		StatsInfo:             physicalop.GetStatsInfo,
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

func (s *session) getOomAlarmVariablesInfo() sessmgr.OOMAlarmVariablesInfo {
	return sessmgr.OOMAlarmVariablesInfo{
		SessionAnalyzeVersion:         s.sessionVars.AnalyzeVersion,
		SessionEnabledRateLimitAction: s.sessionVars.EnabledRateLimitAction,
		SessionMemQuotaQuery:          s.sessionVars.MemQuotaQuery,
	}
}

func (s *session) ExecuteInternal(ctx context.Context, sql string, args ...any) (rs sqlexec.RecordSet, err error) {
	if sink := tracing.GetSink(ctx); sink == nil {
		trace := traceevent.NewTrace()
		ctx = tracing.WithFlightRecorder(ctx, trace)
		defer trace.DiscardOrFlush(ctx)

		// A developer debugging event so we can see what trace is missing!
		if traceevent.IsEnabled(tracing.DevDebug) {
			traceevent.TraceEvent(ctx, tracing.DevDebug, "ExecuteInternal missing trace ctx",
				zap.String("sql", sql),
				zap.Stack("stack"))
			traceevent.CheckFlightRecorderDumpTrigger(ctx, "dump_trigger.suspicious_event.dev_debug", func(config *traceevent.DumpTriggerConfig) bool {
				return config.Event.DevDebug.Type == traceevent.DevDebugTypeExecuteInternalTraceMissing
			})
		}
	}

	rs, err = s.executeInternalImpl(ctx, sql, args...)
	return rs, err
}

func (s *session) executeInternalImpl(ctx context.Context, sql string, args ...any) (rs sqlexec.RecordSet, err error) {
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

type sqlRegexp struct {
	regexp string
}

func (s sqlRegexp) sqlRegexpDumpTriggerCheck(cfg *traceevent.DumpTriggerConfig) bool {
	// TODO: pre-compile the regexp to improve performance
	match, err := regexp.MatchString(cfg.UserCommand.SQLRegexp, s.regexp)
	return err == nil && match
}

// Parse parses a query string to raw ast.StmtNode.
func (s *session) Parse(ctx context.Context, sql string) ([]ast.StmtNode, error) {
	logutil.Logger(ctx).Debug("parse", zap.String("sql", sql))
	parseStartTime := time.Now()

	traceevent.CheckFlightRecorderDumpTrigger(ctx, "dump_trigger.user_command.sql_regexp", sqlRegexp{sql}.sqlRegexpDumpTriggerCheck)

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
	if topsqlstate.TopProfilingEnabled() {
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
