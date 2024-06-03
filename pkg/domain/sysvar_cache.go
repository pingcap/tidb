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
	"maps"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	"go.uber.org/zap"
)

// The sysvar cache replaces the GlobalVariableCache.
// It is an improvement because it operates similar to privilege cache:
// - it caches for 30s instead of 2s
// - the cache is invalidated on update
// - an etcd notification is sent to other tidb servers.

// sysVarCache represents the cache of system variables broken up into session and global scope.
type sysVarCache struct {
	syncutil.RWMutex // protects global and session maps
	global           map[string]string
	session          map[string]string
	rebuildLock      syncutil.Mutex // protects concurrent rebuild
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
	maps.Copy(newMap, do.sysVarCache.session)
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

func (*Domain) fetchTableValues(sctx sessionctx.Context) (map[string]string, error) {
	tableContents := make(map[string]string)
	// Copy all variables from the table to tableContents
	exec := sctx.GetRestrictedSQLExecutor()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnSysVar)
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, `SELECT variable_name, variable_value FROM mysql.global_variables`)
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

			// Call the SetGlobal func for this sysvar if it exists.
			// SET GLOBAL only calls the SetGlobal func on the calling instances.
			// This ensures it is run on all tidb servers.
			// This does not apply to INSTANCE scoped vars (HasGlobalScope() is false)
			if sv.SetGlobal != nil && !sv.SkipSysvarCache() {
				sVal = sv.ValidateWithRelaxedValidation(ctx.GetSessionVars(), sVal, variable.ScopeGlobal)
				err = sv.SetGlobal(context.Background(), ctx.GetSessionVars(), sVal)
				if err != nil {
					logutil.BgLogger().Error(fmt.Sprintf("load global variable %s error", sv.Name), zap.Error(err))
				}
			}
		}
	}

	logutil.BgLogger().Debug("rebuilding sysvar cache")

	do.sysVarCache.Lock()
	defer do.sysVarCache.Unlock()
	do.sysVarCache.session = newSessionCache
	do.sysVarCache.global = newGlobalCache
	do.infoCache.ReSize(int(variable.SchemaVersionCacheLimit.Load()))
	return nil
}
