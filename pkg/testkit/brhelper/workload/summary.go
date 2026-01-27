// Copyright 2025 PingCAP, Inc.
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

package workload

import "sync"

type SummaryEntry struct {
	Case    string `json:"case"`
	Summary any    `json:"summary"`
}

type TableSummary interface {
	SummaryTable() string
}

type Summary struct {
	mu      sync.Mutex
	byCase  map[string]int
	entries []SummaryEntry
}

func NewSummary() *Summary {
	return &Summary{
		byCase: make(map[string]int),
	}
}

func (s *Summary) Set(caseName string, summary any) {
	if s == nil || caseName == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if idx, ok := s.byCase[caseName]; ok {
		s.entries[idx].Summary = summary
		return
	}

	s.byCase[caseName] = len(s.entries)
	s.entries = append(s.entries, SummaryEntry{Case: caseName, Summary: summary})
}

func (s *Summary) Entries() []SummaryEntry {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]SummaryEntry, len(s.entries))
	copy(out, s.entries)
	return out
}

func (s *Summary) Empty() bool {
	if s == nil {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries) == 0
}
