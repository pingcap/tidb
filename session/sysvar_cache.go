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

	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/chunk"
)

// When a new session is created, SessionVars.systems needs to be populated
// with a cache of common system variables. Previously this populated the
// "common sysvars" only, with uncommon ones being populated on-demand.
//
// This is both *not* MySQL compatible (it should not pick up global scoped changes after the session starts),
// and it has a cache miss-path which can be quite slow, such as when running
// SHOW VARIABLES for the first time in a session (which will hit all session vars).

var sessionVarCache map[string]string

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

func (s *session) RebuildSessionVarsCache() error {

	// Create a new map to hold the new cache,
	// and a cache if what's available in the mysql_global_variables table
	// (includes global-only vars that need to be ignored..)
	newCache := make(map[string]string)
	tableContents := s.fetchTableValues()

	for _, sv := range variable.GetSysVars() {
		if sv.Scope&variable.ScopeSession != 0 {
			if _, ok := tableContents[sv.Name]; ok {
				newCache[sv.Name] = tableContents[sv.Name]
			} else {
				// fmt.Printf("%%%% could not find k: %s in cache!\n", sv.Name)
				newCache[sv.Name] = sv.Value // use default
			}
		}
	}

	// Set the sessionVarCache to be the new cache.
	sessionVarCache = newCache
	return nil
}
