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
	"sync"

	"github.com/petermattis/goid"
)

type storedRoutineEvalGuard struct {
	mu    sync.Mutex
	cond  *sync.Cond
	owner int64
	depth int
}

func (g *storedRoutineEvalGuard) lock() {
	gid := goid.Get()
	g.mu.Lock()
	if g.cond == nil {
		g.cond = sync.NewCond(&g.mu)
	}
	for g.depth > 0 && g.owner != gid {
		g.cond.Wait()
	}
	g.owner = gid
	g.depth++
	g.mu.Unlock()
}

func (g *storedRoutineEvalGuard) unlock() {
	gid := goid.Get()
	g.mu.Lock()
	if g.owner != gid || g.depth == 0 {
		g.mu.Unlock()
		panic("stored routine eval guard unlocked by non-owner")
	}
	g.depth--
	if g.depth == 0 {
		g.owner = 0
		g.cond.Signal()
	}
	g.mu.Unlock()
}

// LockStoredRoutineEval serializes stored routine execution per session while allowing nested
// stored function/procedure calls in the same goroutine to re-enter.
func (s *SessionVars) LockStoredRoutineEval() {
	s.storedRoutineEvalGuard.lock()
}

// UnlockStoredRoutineEval releases the stored routine execution guard.
func (s *SessionVars) UnlockStoredRoutineEval() {
	s.storedRoutineEvalGuard.unlock()
}
