// Copyright 2018 PingCAP, Inc.
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

package autoanalyze

import (
	"math/rand"
	"time"
)

const (
	// maxAutoAnalyzeInterval is the max interval time for a table's auto analyze.
	maxAutoAnalyzeInterval = 10 * time.Minute
	// minAutoAnalyzeInterval is the min interval time for a table's auto analyze.
	minAutoAnalyzeInterval = 5 * time.Minute
)

// AutoAnalyze is used to store the recent analyzed tables.
type AutoAnalyze struct {
	recentAnalyzedTables map[int64]time.Time // tid -> ts
}

// NewAutoAnalyze returns a new AutoAnalyze.
func NewAutoAnalyze() *AutoAnalyze {
	return &AutoAnalyze{
		recentAnalyzedTables: make(map[int64]time.Time),
	}
}

// AddRecentAnalyzedTables adds a table to the recent analyzed tables.
func (a *AutoAnalyze) AddRecentAnalyzedTables(tid int64, ts time.Time) {
	s := rand.Int63n(60 * 5) //nolint:gosec
	a.recentAnalyzedTables[tid] =
		ts.Add(minAutoAnalyzeInterval + time.Duration(s)*time.Second)
}

// IsRecentAnalyzedTables checks whether a table is recently analyzed.
func (a *AutoAnalyze) IsRecentAnalyzedTables(tid int64) bool {
	ts, ok := a.recentAnalyzedTables[tid]
	if time.Since(ts) > maxAutoAnalyzeInterval {
		delete(a.recentAnalyzedTables, tid)
		return false
	}
	return ok
}

// GCRecentAnalyzedTables removes the old tables from the recent analyzed tables.
func (a *AutoAnalyze) GCRecentAnalyzedTables() {
	if len(a.recentAnalyzedTables) < 1000 {
		return
	}
	for k, ts := range a.recentAnalyzedTables {
		if time.Since(ts) > maxAutoAnalyzeInterval {
			delete(a.recentAnalyzedTables, k)
		}
	}
}
