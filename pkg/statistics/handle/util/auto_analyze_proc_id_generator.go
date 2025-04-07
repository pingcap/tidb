// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"sync"

	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"golang.org/x/exp/maps"
)

// AutoAnalyzeProcIDGenerator is used to generate auto analyze proc ID.
type AutoAnalyzeProcIDGenerator interface {
	// AutoAnalyzeProcID generates an analyze ID.
	AutoAnalyzeProcID() uint64
	ReleaseAutoAnalyzeProcID(uint64)
}

var _ AutoAnalyzeProcIDGenerator = (*generator)(nil)

type generator struct {
	// autoAnalyzeProcIDGetter is used to generate auto analyze ID.
	autoAnalyzeProcIDGetter  func() uint64
	releaseAutoAnalyzeProcID func(uint64)
}

// NewGenerator creates a new Generator.
func NewGenerator(autoAnalyzeProcIDGetter func() uint64, releaseAutoAnalyzeProcID func(uint64)) AutoAnalyzeProcIDGenerator {
	return &generator{
		autoAnalyzeProcIDGetter:  autoAnalyzeProcIDGetter,
		releaseAutoAnalyzeProcID: releaseAutoAnalyzeProcID,
	}
}

// AutoAnalyzeProcID implements AutoAnalyzeProcIDGenerator.
func (g *generator) AutoAnalyzeProcID() uint64 {
	return g.autoAnalyzeProcIDGetter()
}

// ReleaseAutoAnalyzeProcID implements AutoAnalyzeProcIDGenerator.
func (g *generator) ReleaseAutoAnalyzeProcID(id uint64) {
	g.releaseAutoAnalyzeProcID(id)
}

// GlobalAutoAnalyzeProcessList is used to track the auto analyze process.
var GlobalAutoAnalyzeProcessList = newGlobalAutoAnalyzeProcessList()

type globalAutoAnalyzeProcessList struct {
	processes map[uint64]struct{}
	mu        sync.RWMutex
}

func newGlobalAutoAnalyzeProcessList() *globalAutoAnalyzeProcessList {
	return &globalAutoAnalyzeProcessList{
		processes: make(map[uint64]struct{}),
	}
}

// Tracker is used to track the auto analyze process.
func (g *globalAutoAnalyzeProcessList) Tracker(id uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.processes[id] = struct{}{}
}

// Untracker is used to untrack the auto analyze process.
func (g *globalAutoAnalyzeProcessList) Untracker(id uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.processes, id)
}

// AutoAnalyzeTracker is used to track the auto analyze process.
type AutoAnalyzeTracker struct {
	track   func(id uint64, ctx sysproctrack.TrackProc) error
	untrack func(id uint64)
}

// All returns all the auto analyze process IDs.
func (g *globalAutoAnalyzeProcessList) All() []uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return maps.Keys(g.processes)
}

// Contains checks whether the auto analyze process ID is in the list.
func (g *globalAutoAnalyzeProcessList) Contains(id uint64) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	_, ok := g.processes[id]
	return ok
}

// NewAutoAnalyzeTracker creates a new AutoAnalyzeTracker.
func NewAutoAnalyzeTracker(track func(id uint64, ctx sysproctrack.TrackProc) error, untrack func(id uint64)) *AutoAnalyzeTracker {
	return &AutoAnalyzeTracker{
		track:   track,
		untrack: untrack,
	}
}

// Track is used to track the auto analyze process.
func (t *AutoAnalyzeTracker) Track(id uint64, ctx sysproctrack.TrackProc) error {
	GlobalAutoAnalyzeProcessList.Tracker(id)
	return t.track(id, ctx)
}

// UnTrack is used to untrack the auto analyze process.
func (t *AutoAnalyzeTracker) UnTrack(id uint64) {
	GlobalAutoAnalyzeProcessList.Untracker(id)
	t.untrack(id)
}
