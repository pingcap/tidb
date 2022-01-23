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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package domain

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/stmtsummary"
	topsqlstate "github.com/pingcap/tidb/util/topsql/state"
	storekv "github.com/tikv/client-go/v2/kv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// The sysvar cache replaces the GlobalVariableCache.
// It is an improvement because it operates similar to privilege cache:
// - it caches for 30s instead of 2s
// - the cache is invalidated on update
// - an etcd notification is sent to other tidb servers.

// sysVarCache represents the cache of system variables broken up into session and global scope.
type sysVarCache struct {
	sync.RWMutex // protects global and session maps
	global       map[string]string
	session      map[string]string
	rebuildLock  sync.Mutex // protects concurrent rebuild
}

func (do *Domain) rebuildSysVarCacheIfNeeded() (err error) {
	do.sysVarCache.RLock()
	cacheNeedsRebuild := len(do.sysVarCache.session) == 0 || len(do.sysVarCache.global) == 0
	do.sysVarCache.RUnlock()
	if cacheNeedsRebuild {
		logutil.BgLogger().Warn("sysvar cache is empty, triggering rebuild")
		if err = do.rebuildSysVarCache(nil); err != nil {
			logutil.BgLogger().Error("rebuilding sysvar cache failed", zap.Error(err))
		}
	}
	return err
}

// GetSessionCache gets a copy of the session sysvar cache.
// The intention is to copy it directly to the systems[] map
// on creating a new session.
func (do *Domain) GetSessionCache() (map[string]string, error) {
	if err := do.rebuildSysVarCacheIfNeeded(); err != nil {
		return nil, err
	}
	do.sysVarCache.RLock()
	defer do.sysVarCache.RUnlock()
	// Perform a deep copy since this will be assigned directly to the session
	newMap := make(map[string]string, len(do.sysVarCache.session))
	for k, v := range do.sysVarCache.session {
		newMap[k] = v
	}
	return newMap, nil
}

// GetGlobalVar gets an individual global var from the sysvar cache.
func (do *Domain) GetGlobalVar(name string) (string, error) {
	if err := do.rebuildSysVarCacheIfNeeded(); err != nil {
		return "", err
	}
	do.sysVarCache.RLock()
	defer do.sysVarCache.RUnlock()

	if val, ok := do.sysVarCache.global[name]; ok {
		return val, nil
	}
	logutil.BgLogger().Warn("could not find key in global cache", zap.String("name", name))
	return "", variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
}

func (do *Domain) fetchTableValues(ctx sessionctx.Context) (map[string]string, error) {
	tableContents := make(map[string]string)
	// Copy all variables from the table to tableContents
	exec := ctx.(sqlexec.RestrictedSQLExecutor)
	stmt, err := exec.ParseWithParams(context.Background(), true, `SELECT variable_name, variable_value FROM mysql.global_variables`)
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

// rebuildSysVarCache rebuilds the sysvar cache both globally and for session vars.
// It needs to be called when sysvars are added or removed.
func (do *Domain) rebuildSysVarCache(ctx sessionctx.Context) error {
	newSessionCache := make(map[string]string)
	newGlobalCache := make(map[string]string)
	if ctx == nil {
		sysSessionPool := do.SysSessionPool()
		res, err := sysSessionPool.Get()
		if err != nil {
			return err
		}
		defer sysSessionPool.Put(res)
		ctx = res.(sessionctx.Context)
	}
	// Only one rebuild can be in progress at a time, this prevents a lost update race
	// where an earlier fetchTableValues() finishes last.
	do.sysVarCache.rebuildLock.Lock()
	defer do.sysVarCache.rebuildLock.Unlock()
	tableContents, err := do.fetchTableValues(ctx)
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
		do.checkEnableServerGlobalVar(sv.Name, sVal)
	}

	logutil.BgLogger().Debug("rebuilding sysvar cache")

	do.sysVarCache.Lock()
	defer do.sysVarCache.Unlock()
	do.sysVarCache.session = newSessionCache
	do.sysVarCache.global = newGlobalCache
	return nil
}

// checkEnableServerGlobalVar processes variables that acts in server and global level.
// This is required because the SetGlobal function on the sysvar struct only executes on
// the initiating tidb-server. There is no current method to say "run this function on all
// tidb servers when the value of this variable changes". If you do not require changes to
// be applied on all servers, use a getter/setter instead! You don't need to add to this list.
func (do *Domain) checkEnableServerGlobalVar(name, sVal string) {
	var err error
	switch name {
	case variable.TiDBTSOClientBatchMaxWaitTime:
		var val float64
		val, err = strconv.ParseFloat(sVal, 64)
		if err != nil {
			break
		}
		err = do.SetPDClientDynamicOption(pd.MaxTSOBatchWaitInterval, time.Duration(float64(time.Millisecond)*val))
		if err != nil {
			break
		}
		variable.MaxTSOBatchWaitInterval.Store(val)
	case variable.TiDBEnableTSOFollowerProxy:
		val := variable.TiDBOptOn(sVal)
		err = do.SetPDClientDynamicOption(pd.EnableTSOFollowerProxy, val)
		if err != nil {
			break
		}
		variable.EnableTSOFollowerProxy.Store(val)
	case variable.TiDBEnableLocalTxn:
		variable.EnableLocalTxn.Store(variable.TiDBOptOn(sVal))
	case variable.TiDBEnableStmtSummary:
		err = stmtsummary.StmtSummaryByDigestMap.SetEnabled(variable.TiDBOptOn(sVal))
	case variable.TiDBStmtSummaryInternalQuery:
		err = stmtsummary.StmtSummaryByDigestMap.SetEnabledInternalQuery(variable.TiDBOptOn(sVal))
	case variable.TiDBStmtSummaryRefreshInterval:
		err = stmtsummary.StmtSummaryByDigestMap.SetRefreshInterval(variable.TidbOptInt64(sVal, variable.DefTiDBStmtSummaryRefreshInterval))
	case variable.TiDBStmtSummaryHistorySize:
		err = stmtsummary.StmtSummaryByDigestMap.SetHistorySize(variable.TidbOptInt(sVal, variable.DefTiDBStmtSummaryHistorySize))
	case variable.TiDBStmtSummaryMaxStmtCount:
		err = stmtsummary.StmtSummaryByDigestMap.SetMaxStmtCount(uint(variable.TidbOptInt(sVal, variable.DefTiDBStmtSummaryMaxStmtCount)))
	case variable.TiDBStmtSummaryMaxSQLLength:
		err = stmtsummary.StmtSummaryByDigestMap.SetMaxSQLLength(variable.TidbOptInt(sVal, variable.DefTiDBStmtSummaryMaxSQLLength))
	case variable.TiDBTopSQLMaxTimeSeriesCount:
		var val int64
		val, err = strconv.ParseInt(sVal, 10, 64)
		if err != nil {
			break
		}
		topsqlstate.GlobalState.MaxStatementCount.Store(val)
	case variable.TiDBTopSQLMaxMetaCount:
		var val int64
		val, err = strconv.ParseInt(sVal, 10, 64)
		if err != nil {
			break
		}
		topsqlstate.GlobalState.MaxCollect.Store(val)
	case variable.TiDBRestrictedReadOnly:
		variable.RestrictedReadOnly.Store(variable.TiDBOptOn(sVal))
	case variable.TiDBSuperReadOnly:
		variable.VarTiDBSuperReadOnly.Store(variable.TiDBOptOn(sVal))
	case variable.TiDBStoreLimit:
		var val int64
		val, err = strconv.ParseInt(sVal, 10, 64)
		if err != nil {
			break
		}
		storekv.StoreLimit.Store(val)
	case variable.TiDBTableCacheLease:
		var val int64
		val, err = strconv.ParseInt(sVal, 10, 64)
		if err != nil {
			break
		}
		variable.TableCacheLease.Store(val)
	case variable.TiDBPersistAnalyzeOptions:
		variable.PersistAnalyzeOptions.Store(variable.TiDBOptOn(sVal))
	case variable.TiDBEnableColumnTracking:
		variable.EnableColumnTracking.Store(variable.TiDBOptOn(sVal))
	case variable.TiDBStatsLoadSyncWait:
		var val int64
		val, err = strconv.ParseInt(sVal, 10, 64)
		if err != nil {
			break
		}
		variable.StatsLoadSyncWait.Store(val)
	case variable.TiDBStatsLoadPseudoTimeout:
		variable.StatsLoadPseudoTimeout.Store(variable.TiDBOptOn(sVal))
	}
	if err != nil {
		logutil.BgLogger().Error(fmt.Sprintf("load global variable %s error", name), zap.Error(err))
	}
}

// SetPDClientDynamicOption is used to set the dynamic option into the PD client.
func (do *Domain) SetPDClientDynamicOption(option pd.DynamicOption, val interface{}) error {
	store, ok := do.store.(interface{ GetPDClient() pd.Client })
	if !ok {
		return nil
	}
	pdClient := store.GetPDClient()
	if pdClient == nil {
		return nil
	}
	return pdClient.UpdateOption(option, val)
}
