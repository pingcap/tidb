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
	"sync"
	"time"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
)

// The sysvar cache replaces the "global_vars_cache". It is an improvement because it operates similar to privilege cache,
// Where it caches for 5 minutes instead of 2 seconds, plus it listens on etcd for updates from other servers.

// When a new session is created, SessionVars.systems needs to be populated
// with a cache of common system variables. Previously this populated the
// "common sysvars" only, with uncommon ones being populated on-demand.
//
// This was both *not* MySQL compatible (it should not pick up global scoped changes after the session starts),
// and it has a cache miss-path which can be quite slow, such as when running
// SHOW VARIABLES for the first time in a session (which will hit all session vars).

type SysVarCache struct {
	sync.RWMutex
	isHealthy  bool
	lastModify time.Time
	global     map[string]string
	session    map[string]string
}

// GetGlobalVarsCache gets the global variable cache.
func (do *Domain) GetSysVarCache() *SysVarCache {
	return &do.sysVarCache
}

func (svc *SysVarCache) GetSessionCache() map[string]string {
	svc.RLock()
	defer svc.RUnlock()

	// Perform a deep copy since this will be assigned directly to the session
	newMap := make(map[string]string, len(svc.session))
	for k, v := range svc.session {
		newMap[k] = v
	}
	return newMap
}

func (svc *SysVarCache) GetGlobalVar(name string) (string, error) {
	svc.RLock()
	defer svc.RUnlock()
	if val, ok := svc.global[name]; ok {
		return val, nil
	}
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
	svc.Lock()
	defer svc.Unlock()

	logutil.BgLogger().Info("rebuilding sysvar cache")

	// Create a new map to hold the new cache,
	// and a cache if what's available in the mysql_global_variables table
	newSessionCache := make(map[string]string)
	newGlobalCache := make(map[string]string)
	tableContents, err := svc.fetchTableValues(ctx)
	if err != nil {
		return err
	}

	for _, sv := range variable.GetSysVars() {
		if sv.HasSessionScope() {
			if _, ok := tableContents[sv.Name]; ok {
				newSessionCache[sv.Name] = tableContents[sv.Name]
			} else {
				newSessionCache[sv.Name] = sv.Value // use default
			}
		}
		if sv.HasGlobalScope() {
			if _, ok := tableContents[sv.Name]; ok {
				newGlobalCache[sv.Name] = tableContents[sv.Name]
			} else {
				newGlobalCache[sv.Name] = sv.Value // use default
			}
		}
	}

	svc.session = newSessionCache
	svc.global = newGlobalCache
	return nil
}
