// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package domain

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/stmtsummary"
	storekv "github.com/tikv/client-go/v2/kv"
	"go.uber.org/zap"
)

// The sysvar cache replaces the GlobalVariableCache.
// It is an improvement because it operates similar to privilege cache,
// where it caches for 5 minutes instead of 2 seconds, plus it listens on etcd
// for updates from other servers.

// SysVarCache represents the cache of system variables broken up into session and global scope.
type SysVarCache struct {
	sync.RWMutex // protects global and session maps
	global       map[string]string
	session      map[string]string
	rebuildLock  sync.Mutex // protects concurrent rebuild
}

// GetSysVarCache gets the global variable cache.
func (do *Domain) GetSysVarCache() *SysVarCache {
	return &do.sysVarCache
}

func (svc *SysVarCache) rebuildCacheIfNeeded(ctx sessionctx.Context) (err error) {
	svc.RLock()
	cacheNeedsRebuild := len(svc.session) == 0 || len(svc.global) == 0
	svc.RUnlock()
	if cacheNeedsRebuild {
		logutil.BgLogger().Warn("sysvar cache is empty, triggering rebuild")
		if err = svc.RebuildSysVarCache(ctx); err != nil {
			logutil.BgLogger().Error("rebuilding sysvar cache failed", zap.Error(err))
		}
	}
	return err
}

// GetSessionCache gets a copy of the session sysvar cache.
// The intention is to copy it directly to the systems[] map
// on creating a new session.
func (svc *SysVarCache) GetSessionCache(ctx sessionctx.Context) (map[string]string, error) {
	if err := svc.rebuildCacheIfNeeded(ctx); err != nil {
		return nil, err
	}
	svc.RLock()
	defer svc.RUnlock()
	// Perform a deep copy since this will be assigned directly to the session
	newMap := make(map[string]string, len(svc.session))
	for k, v := range svc.session {
		newMap[k] = v
	}
	return newMap, nil
}

// GetGlobalVar gets an individual global var from the sysvar cache.
func (svc *SysVarCache) GetGlobalVar(ctx sessionctx.Context, name string) (string, error) {
	if err := svc.rebuildCacheIfNeeded(ctx); err != nil {
		return "", err
	}
	svc.RLock()
	defer svc.RUnlock()

	if val, ok := svc.global[name]; ok {
		return val, nil
	}
	logutil.BgLogger().Warn("could not find key in global cache", zap.String("name", name))
	return "", variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
}

func (svc *SysVarCache) fetchTableValues(ctx sessionctx.Context) (map[string]string, error) {
	tableContents := make(map[string]string)
	// Copy all variables from the table to tableContents
	exec := ctx.(sqlexec.RestrictedSQLExecutor)
	stmt, err := exec.ParseWithParams(context.Background(), `SELECT variable_name, variable_value FROM mysql.global_variables`)
	if err != nil {
		return tableContents, err
	}
	rows, _, err := exec.ExecRestrictedStmt(context.TODO(), stmt)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		name := row.GetString(0)
		val := row.GetString(1)
		tableContents[name] = val
	}
	return tableContents, nil
}

// RebuildSysVarCache rebuilds the sysvar cache both globally and for session vars.
// It needs to be called when sysvars are added or removed.
func (svc *SysVarCache) RebuildSysVarCache(ctx sessionctx.Context) error {
	newSessionCache := make(map[string]string)
	newGlobalCache := make(map[string]string)
	// Only one rebuild can be in progress at a time, this prevents a lost update race
	// where an earlier fetchTableValues() finishes last.
	svc.rebuildLock.Lock()
	defer svc.rebuildLock.Unlock()
	tableContents, err := svc.fetchTableValues(ctx)
	if err != nil {
		return err
	}

	for _, sv := range variable.GetSysVars() {
		sVal := sv.Value
		if _, ok := tableContents[sv.Name]; ok {
			sVal = tableContents[sv.Name]
		}
		// session cache stores non-skippable variables, which essentially means session scope.
		// for historical purposes there are some globals, but these should eventually be removed.
		if !sv.SkipInit() {
			newSessionCache[sv.Name] = sVal
		}
		if sv.HasGlobalScope() {
			newGlobalCache[sv.Name] = sVal
		}
		// Propagate any changes to the server scoped variables
		checkEnableServerGlobalVar(sv.Name, sVal)
	}

	logutil.BgLogger().Debug("rebuilding sysvar cache")

	svc.Lock()
	defer svc.Unlock()
	svc.session = newSessionCache
	svc.global = newGlobalCache
	return nil
}

// checkEnableServerGlobalVar processes variables that acts in server and global level.
// This is required because the SetGlobal function on the sysvar struct only executes on
// the initiating tidb-server. There is no current method to say "run this function on all
// tidb servers when the value of this variable changes". If you do not require changes to
// be applied on all servers, use a getter/setter instead! You don't need to add to this list.
func checkEnableServerGlobalVar(name, sVal string) {
	var err error
	switch name {
	case variable.TiDBEnableLocalTxn:
		variable.EnableLocalTxn.Store(variable.TiDBOptOn(sVal))
	case variable.TiDBEnableStmtSummary:
		err = stmtsummary.StmtSummaryByDigestMap.SetEnabled(sVal, false)
	case variable.TiDBStmtSummaryInternalQuery:
		err = stmtsummary.StmtSummaryByDigestMap.SetEnabledInternalQuery(sVal, false)
	case variable.TiDBStmtSummaryRefreshInterval:
		err = stmtsummary.StmtSummaryByDigestMap.SetRefreshInterval(sVal, false)
	case variable.TiDBStmtSummaryHistorySize:
		err = stmtsummary.StmtSummaryByDigestMap.SetHistorySize(sVal, false)
	case variable.TiDBStmtSummaryMaxStmtCount:
		err = stmtsummary.StmtSummaryByDigestMap.SetMaxStmtCount(sVal, false)
	case variable.TiDBStmtSummaryMaxSQLLength:
		err = stmtsummary.StmtSummaryByDigestMap.SetMaxSQLLength(sVal, false)
	case variable.TiDBCapturePlanBaseline:
		variable.CapturePlanBaseline.Set(sVal, false)
	case variable.TiDBEnableTopSQL:
		variable.TopSQLVariable.Enable.Store(variable.TiDBOptOn(sVal))
	case variable.TiDBTopSQLPrecisionSeconds:
		var val int64
		val, err = strconv.ParseInt(sVal, 10, 64)
		if err != nil {
			break
		}
		variable.TopSQLVariable.PrecisionSeconds.Store(val)
	case variable.TiDBTopSQLMaxStatementCount:
		var val int64
		val, err = strconv.ParseInt(sVal, 10, 64)
		if err != nil {
			break
		}
<<<<<<< HEAD
		variable.TopSQLVariable.MaxStatementCount.Store(val)
	case variable.TiDBTopSQLMaxCollect:
=======
		topsqlstate.GlobalState.MaxCollect.Store(val)
	case variable.TiDBRestrictedReadOnly:
		variable.RestrictedReadOnly.Store(variable.TiDBOptOn(sVal))
	case variable.TiDBSuperReadOnly:
		variable.VarTiDBSuperReadOnly.Store(variable.TiDBOptOn(sVal))
	case variable.TiDBStoreLimit:
>>>>>>> 1a146fabd... variables: add constraints on tidb_super_read_only when tidb_restricted_read_only is turned on (#31746)
		var val int64
		val, err = strconv.ParseInt(sVal, 10, 64)
		if err != nil {
			break
		}
		variable.TopSQLVariable.MaxCollect.Store(val)
	case variable.TiDBTopSQLReportIntervalSeconds:
		var val int64
		val, err = strconv.ParseInt(sVal, 10, 64)
		if err != nil {
			break
		}
		variable.TopSQLVariable.ReportIntervalSeconds.Store(val)
	case variable.TiDBRestrictedReadOnly:
		variable.RestrictedReadOnly.Store(variable.TiDBOptOn(sVal))
	case variable.TiDBStoreLimit:
		var val int64
		val, err = strconv.ParseInt(sVal, 10, 64)
		if err != nil {
			break
		}
		storekv.StoreLimit.Store(val)
	}
	if err != nil {
		logutil.BgLogger().Error(fmt.Sprintf("load global variable %s error", name), zap.Error(err))
	}
}
