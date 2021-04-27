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

package session

import (
	"context"
	"sync"

	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
)

// When a new session is created, SessionVars.systems needs to be populated
// with a cache of common system variables. Previously this populated the
// "common sysvars" only, with uncommon ones being populated on-demand.
//
// This is both *not* MySQL compatible (it should not pick up global scoped changes after the session starts),
// and it has a cache miss-path which can be quite slow, such as when running
// SHOW VARIABLES for the first time in a session (which will hit all session vars).

type sysVarCache struct {
	sync.RWMutex
	isHealthy bool
	global    map[string]string
	session   map[string]string
}

var svcache sysVarCache

func (s *session) fetchTableValues() (tableContents map[string]string) {
	tableContents = make(map[string]string)
	// Copy all variables from the table to tableContents
	rs, err := s.ExecuteInternal(context.Background(), "SELECT variable_name, variable_value FROM mysql.global_variables")
	if err != nil {
		return
	}
	defer terror.Call(rs.Close)
	req := rs.NewChunk()
	for {
		err = rs.Next(context.TODO(), req)
		if err != nil {
			return
		}
		if req.NumRows() == 0 {
			return
		}
		it := chunk.NewIterator4Chunk(req)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			name := row.GetString(0)
			val := row.GetString(1)
			tableContents[name] = val
		}
	}
}

func (s *session) buildSysVarCacheIfNeeded() {
	if len(svcache.global) == 0 || len(svcache.session) == 0 {
		s.RebuildSysVarCache()
	}
}

// RebuildSysVarCache rebuilds the sysvar cache both globally and for session vars.
// It needs to be called when sysvars are added or removed. For global sysvar changes
// UpdateSysVarCacheForKey can be called instead.
func (s *session) RebuildSysVarCache() error {
	svcache.Lock()
	defer svcache.Unlock()

	logutil.BgLogger().Info("rebuilding sysvar cache")

	// Create a new map to hold the new cache,
	// and a cache if what's available in the mysql_global_variables table
	newSessionCache := make(map[string]string)
	newGlobalCache := make(map[string]string)
	tableContents := s.fetchTableValues()

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

	// Update the cache
	svcache.session = newSessionCache
	svcache.global = newGlobalCache
	svcache.isHealthy = true
	return nil
}

// UpdateSysVarCacheForKey is an optimization where we patch the contents of the
// global and session cache rather than run RebuildSysVarCache()
func (s *session) UpdateSysVarCacheForKey(nameInLower string, value string) error {
	if !svcache.isHealthy {
		return s.RebuildSysVarCache()
	}

	svcache.Lock()
	defer svcache.Unlock()

	sv := variable.GetSysVar(nameInLower)
	if sv.HasSessionScope() {
		svcache.session[nameInLower] = value
	}
	if sv.HasGlobalScope() {
		svcache.global[nameInLower] = value
	}
	return nil
}
