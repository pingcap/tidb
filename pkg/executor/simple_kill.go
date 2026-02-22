// Copyright 2016 PingCAP, Inc.
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
	"math"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/plugin"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/util/globalconn"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

func (e *SimpleExec) executeKillStmt(ctx context.Context, s *ast.KillStmt) error {
	if x, ok := s.Expr.(*ast.FuncCallExpr); ok {
		if x.FnName.L == ast.ConnectionID {
			sm := e.Ctx().GetSessionManager()
			sm.Kill(e.Ctx().GetSessionVars().ConnectionID, s.Query, false, false)
			return nil
		}
		return errors.New("Invalid operation. Please use 'KILL TIDB [CONNECTION | QUERY] [connectionID | CONNECTION_ID()]' instead")
	}
	if !config.GetGlobalConfig().EnableGlobalKill {
		conf := config.GetGlobalConfig()
		if s.TiDBExtension || conf.CompatibleKillQuery {
			sm := e.Ctx().GetSessionManager()
			if sm == nil {
				return nil
			}
			sm.Kill(s.ConnectionID, s.Query, false, false)
		} else {
			err := errors.NewNoStackError("Invalid operation. Please use 'KILL TIDB [CONNECTION | QUERY] [connectionID | CONNECTION_ID()]' instead")
			e.Ctx().GetSessionVars().StmtCtx.AppendWarning(err)
		}
		return nil
	}

	sm := e.Ctx().GetSessionManager()
	if sm == nil {
		return nil
	}
	if e.IsFromRemote {
		logutil.BgLogger().Info("Killing connection in current instance redirected from remote TiDB", zap.Uint64("conn", s.ConnectionID), zap.Bool("query", s.Query),
			zap.String("sourceAddr", e.Ctx().GetSessionVars().SourceAddr.IP.String()))
		sm.Kill(s.ConnectionID, s.Query, false, false)
		return nil
	}

	gcid, isTruncated, err := globalconn.ParseConnID(s.ConnectionID)
	if err != nil {
		err1 := errors.NewNoStackError("Parse ConnectionID failed: " + err.Error())
		e.Ctx().GetSessionVars().StmtCtx.AppendWarning(err1)
		return nil
	}
	if isTruncated {
		message := "Kill failed: Received a 32bits truncated ConnectionID, expect 64bits. Please execute 'KILL [CONNECTION | QUERY] ConnectionID' to send a Kill without truncating ConnectionID."
		logutil.BgLogger().Warn(message, zap.Uint64("conn", s.ConnectionID))
		// Notice that this warning cannot be seen if KILL is triggered by "CTRL-C" of mysql client,
		//   as the KILL is sent by a new connection.
		err := errors.NewNoStackError(message)
		e.Ctx().GetSessionVars().StmtCtx.AppendWarning(err)
		return nil
	}

	if gcid.ServerID != sm.ServerID() {
		if err := killRemoteConn(ctx, e.Ctx(), &gcid, s.Query); err != nil {
			err1 := errors.NewNoStackError("KILL remote connection failed: " + err.Error())
			e.Ctx().GetSessionVars().StmtCtx.AppendWarning(err1)
		}
	} else {
		sm.Kill(s.ConnectionID, s.Query, false, false)
	}

	return nil
}

func killRemoteConn(ctx context.Context, sctx sessionctx.Context, gcid *globalconn.GCID, query bool) error {
	if gcid.ServerID == 0 {
		return errors.New("Unexpected ZERO ServerID. Please file a bug to the TiDB Team")
	}

	killExec := &tipb.Executor{
		Tp:   tipb.ExecType_TypeKill,
		Kill: &tipb.Kill{ConnID: gcid.ToConnID(), Query: query},
	}

	dagReq := &tipb.DAGRequest{}
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = timeutil.Zone(sctx.GetSessionVars().Location())
	sc := sctx.GetSessionVars().StmtCtx
	if sc.RuntimeStatsColl != nil {
		collExec := true
		dagReq.CollectExecutionSummaries = &collExec
	}
	dagReq.Flags = sc.PushDownFlags()
	dagReq.Executors = []*tipb.Executor{killExec}

	var builder distsql.RequestBuilder
	kvReq, err := builder.
		SetDAGRequest(dagReq).
		SetFromSessionVars(sctx.GetDistSQLCtx()).
		SetFromInfoSchema(sctx.GetInfoSchema()).
		SetStoreType(kv.TiDB).
		SetTiDBServerID(gcid.ServerID).
		SetStartTS(math.MaxUint64). // To make check visibility success.
		Build()
	if err != nil {
		return err
	}
	resp := sctx.GetClient().Send(ctx, kvReq, sctx.GetSessionVars().KVVars, &kv.ClientSendOption{})
	if resp == nil {
		err := errors.New("client returns nil response")
		return err
	}

	// Must consume & close the response, otherwise coprocessor task will leak.
	defer func() {
		_ = resp.Close()
	}()
	if _, err := resp.Next(ctx); err != nil {
		return errors.Trace(err)
	}

	logutil.BgLogger().Info("Killed remote connection", zap.Uint64("serverID", gcid.ServerID),
		zap.Uint64("conn", gcid.ToConnID()), zap.Bool("query", query))
	return err
}

func (e *SimpleExec) executeRefreshStats(ctx context.Context, s *ast.RefreshStatsStmt) error {
	intest.AssertFunc(func() bool {
		for _, obj := range s.RefreshObjects {
			switch obj.RefreshObjectScope {
			case ast.RefreshObjectScopeDatabase, ast.RefreshObjectScopeTable:
				if obj.DBName.L == "" {
					return false
				}
			}
		}
		return true
	}, "Refresh stats broadcast requires database-qualified names")
	// Note: Restore the statement to a SQL string so we can broadcast fully qualified
	// table names to every instance. For example, `REFRESH STATS tbl` executed in
	// database `db` must be sent as `REFRESH STATS db.tbl`; otherwise a peer without
	// that current database would skip the table.
	sql, err := restoreRefreshStatsSQL(s)
	if err != nil {
		statslogutil.StatsErrVerboseLogger().Error("Failed to format refresh stats statement", zap.Error(err))
		return err
	}
	if e.IsFromRemote {
		if err := e.executeRefreshStatsOnCurrentInstance(ctx, s); err != nil {
			statslogutil.StatsErrVerboseLogger().Error("Failed to refresh stats from remote", zap.String("sql", sql), zap.Error(err))
			return err
		}
		statslogutil.StatsLogger().Info("Successfully refreshed statistics from remote", zap.String("sql", sql))
		return nil
	}
	if s.IsClusterWide {
		if err := broadcast(ctx, e.Ctx(), sql); err != nil {
			statslogutil.StatsErrVerboseLogger().Error("Failed to broadcast refresh stats command", zap.String("sql", sql), zap.Error(err))
			return err
		}
		logutil.BgLogger().Info("Successfully broadcast query", zap.String("sql", sql))
		return nil
	}
	if err := e.executeRefreshStatsOnCurrentInstance(ctx, s); err != nil {
		statslogutil.StatsErrVerboseLogger().Error("Failed to refresh stats on the current instance", zap.String("sql", sql), zap.Error(err))
		return err
	}
	statslogutil.StatsLogger().Info("Successfully refreshed statistics on the current instance", zap.String("sql", sql))
	return nil
}

func restoreRefreshStatsSQL(s *ast.RefreshStatsStmt) (string, error) {
	var sb strings.Builder
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	if err := s.Restore(restoreCtx); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func (e *SimpleExec) executeRefreshStatsOnCurrentInstance(ctx context.Context, s *ast.RefreshStatsStmt) error {
	intest.Assert(len(s.RefreshObjects) > 0, "RefreshObjects should not be empty")
	intest.AssertFunc(func() bool {
		origCount := len(s.RefreshObjects)
		s.Dedup()
		return origCount == len(s.RefreshObjects)
	}, "RefreshObjects should be deduplicated in the building phase")
	tableIDs := make([]int64, 0, len(s.RefreshObjects))
	isGlobalScope := len(s.RefreshObjects) == 1 && s.RefreshObjects[0].RefreshObjectScope == ast.RefreshObjectScopeGlobal
	is := sessiontxn.GetTxnManager(e.Ctx()).GetTxnInfoSchema()
	if !isGlobalScope {
		for _, refreshObject := range s.RefreshObjects {
			switch refreshObject.RefreshObjectScope {
			case ast.RefreshObjectScopeDatabase:
				exists := is.SchemaExists(refreshObject.DBName)
				if !exists {
					e.Ctx().GetSessionVars().StmtCtx.AppendWarning(infoschema.ErrDatabaseNotExists.FastGenByArgs(refreshObject.DBName))
					statslogutil.StatsLogger().Warn("Failed to find database when refreshing stats", zap.String("db", refreshObject.DBName.O))
					continue
				}
				tables, err := is.SchemaTableInfos(ctx, refreshObject.DBName)
				if err != nil {
					return errors.Trace(err)
				}
				if len(tables) == 0 {
					// Note: We do not warn about databases without tables because we cannot issue a warning
					// for every such database when refreshing with `REFRESH STATS *.*`.(Technically, we can, but no point to do so.)
					// Instead, we simply log the information to remain consistent across all cases.
					statslogutil.StatsLogger().Info("No table in the database when refreshing stats", zap.String("db", refreshObject.DBName.O))
					continue
				}
				for _, table := range tables {
					tableIDs = append(tableIDs, table.ID)
				}
			case ast.RefreshObjectScopeTable:
				table, err := is.TableInfoByName(refreshObject.DBName, refreshObject.TableName)
				if err != nil {
					if infoschema.ErrTableNotExists.Equal(err) {
						e.Ctx().GetSessionVars().StmtCtx.AppendWarning(infoschema.ErrTableNotExists.FastGenByArgs(refreshObject.DBName, refreshObject.TableName))
						statslogutil.StatsLogger().Warn("Failed to find table when refreshing stats", zap.String("db", refreshObject.DBName.O), zap.String("table", refreshObject.TableName.O))
						continue
					}
					return errors.Trace(err)
				}
				if table == nil {
					intest.Assert(false, "Table should not be nil here")
					e.Ctx().GetSessionVars().StmtCtx.AppendWarning(infoschema.ErrTableNotExists.FastGenByArgs(refreshObject.DBName, refreshObject.TableName))
					statslogutil.StatsLogger().Warn("Failed to find table when refreshing stats", zap.String("db", refreshObject.DBName.O), zap.String("table", refreshObject.TableName.O))
					continue
				}
				tableIDs = append(tableIDs, table.ID)
			default:
				intest.Assert(false, "No other scopes should be here")
			}
		}
		// If all specified databases or tables do not exist, we do nothing.
		if len(tableIDs) == 0 {
			statslogutil.StatsLogger().Info("No valid database or table to refresh stats")
			return nil
		}
	}
	// Note: tableIDs is empty means to refresh all tables.
	h := domain.GetDomain(e.Ctx()).StatsHandle()
	if s.RefreshMode != nil {
		if *s.RefreshMode == ast.RefreshStatsModeLite {
			return h.InitStatsLite(ctx, tableIDs...)
		}
		return h.InitStats(ctx, is, tableIDs...)
	}
	liteInitStats := config.GetGlobalConfig().Performance.LiteInitStats
	if liteInitStats {
		return h.InitStatsLite(ctx, tableIDs...)
	}
	return h.InitStats(ctx, is, tableIDs...)
}

func broadcast(ctx context.Context, sctx sessionctx.Context, sql string) error {
	broadcastExec := &tipb.Executor{
		Tp: tipb.ExecType_TypeBroadcastQuery,
		BroadcastQuery: &tipb.BroadcastQuery{
			Query: &sql,
		},
	}
	dagReq := &tipb.DAGRequest{}
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = timeutil.Zone(sctx.GetSessionVars().Location())
	sc := sctx.GetSessionVars().StmtCtx
	if sc.RuntimeStatsColl != nil {
		collExec := true
		dagReq.CollectExecutionSummaries = &collExec
	}
	dagReq.Flags = sc.PushDownFlags()
	dagReq.Executors = []*tipb.Executor{broadcastExec}

	var builder distsql.RequestBuilder
	kvReq, err := builder.
		SetDAGRequest(dagReq).
		SetFromSessionVars(sctx.GetDistSQLCtx()).
		SetFromInfoSchema(sctx.GetInfoSchema()).
		SetStoreType(kv.TiDB).
		// Send to all TiDB instances.
		SetTiDBServerID(0).
		SetStartTS(math.MaxUint64).
		Build()
	if err != nil {
		return err
	}
	resp := sctx.GetClient().Send(ctx, kvReq, sctx.GetSessionVars().KVVars, &kv.ClientSendOption{})
	if resp == nil {
		err := errors.New("client returns nil response")
		return err
	}

	// Must consume & close the response, otherwise coprocessor task will leak.
	defer func() {
		_ = resp.Close()
	}()
	for {
		subset, err := resp.Next(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if subset == nil {
			break // all remote tasks finished cleanly
		}
	}

	return nil
}

func (e *SimpleExec) executeFlush(ctx context.Context, s *ast.FlushStmt) error {
	switch s.Tp {
	case ast.FlushTables:
		if s.ReadLock {
			return errors.New("FLUSH TABLES WITH READ LOCK is not supported.  Please use @@tidb_snapshot")
		}
	case ast.FlushPrivileges:
		dom := domain.GetDomain(e.Ctx())
		return dom.NotifyUpdateAllUsersPrivilege()
	case ast.FlushTiDBPlugin:
		dom := domain.GetDomain(e.Ctx())
		for _, pluginName := range s.Plugins {
			err := plugin.NotifyFlush(dom, pluginName)
			if err != nil {
				return err
			}
		}
	case ast.FlushClientErrorsSummary:
		errno.FlushStats()
	case ast.FlushStatsDelta:
		h := domain.GetDomain(e.Ctx()).StatsHandle()
		if e.IsFromRemote {
			err := h.DumpStatsDeltaToKV(true)
			if err != nil {
				statslogutil.StatsErrVerboseLogger().Error("Failed to dump stats delta to KV from remote", zap.Error(err))
			} else {
				statslogutil.StatsLogger().Info("Successfully dumped stats delta to KV from remote")
			}
			return err
		}
		if s.IsCluster {
			var sb strings.Builder
			restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
			if err := s.Restore(restoreCtx); err != nil {
				statslogutil.StatsErrVerboseLogger().Error("Failed to format flush stats delta statement", zap.Error(err))
				return err
			}
			sql := sb.String()
			if err := broadcast(ctx, e.Ctx(), sql); err != nil {
				statslogutil.StatsErrVerboseLogger().Error("Failed to broadcast flush stats delta command", zap.String("sql", sql), zap.Error(err))
				return err
			}
			logutil.BgLogger().Info("Successfully broadcast query", zap.String("sql", sql))
			return nil
		}
		return h.DumpStatsDeltaToKV(true)
	}
	return nil
}

