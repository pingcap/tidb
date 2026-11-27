// Copyright 2026 PingCAP, Inc.
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

package variable

import (
	"context"
	"sync"
	"testing"
)

// TestGetSessionOrGlobalSystemVarConcurrent reproduces issue #73702: the branches
// of a UNION (or a parallel projection) read information_schema.variables_info in
// separate goroutines that share the same SessionVars. setDataForVariablesInfo
// calls GetSessionOrGlobalSystemVar for every system variable, and for a
// session-scoped variable that is not yet cached that function lazily writes
// s.systems. Without synchronization the concurrent goroutines trigger a Go
// "concurrent map read and map write" / "concurrent map writes" fatal that kills
// the whole tidb-server process.
//
// Run with -race: before the fix the race detector reports a data race on
// SessionVars.systems; after the fix (systemsMu) it is clean.
func TestGetSessionOrGlobalSystemVarConcurrent(t *testing.T) {
	// Collect the session-scoped variables that take the lazy-population path
	// (no GetSession hook), i.e. exactly the ones that write s.systems on first
	// read. This mirrors what setDataForVariablesInfo iterates over.
	var names []string
	for _, sv := range GetSysVars() {
		if sv.HasSessionScope() && sv.GetSession == nil {
			names = append(names, sv.Name)
		}
	}
	if len(names) == 0 {
		t.Skip("no session-scoped variables without a GetSession hook")
	}

	const branches = 8
	for iter := 0; iter < 20; iter++ {
		// A fresh session whose systems map is not yet populated, like a new
		// connection running the UNION as its first statement.
		vars := NewSessionVars(nil)
		vars.GlobalVarsAccessor = NewMockGlobalAccessor()

		var wg sync.WaitGroup
		start := make(chan struct{})
		for b := 0; b < branches; b++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for _, name := range names {
					_, _ = vars.GetSessionOrGlobalSystemVar(context.Background(), name)
				}
			}()
		}
		close(start)
		wg.Wait()
	}
}
