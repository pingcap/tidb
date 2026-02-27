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

package mvdeltamergeagg

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecideMinMaxFast(t *testing.T) {
	cases := []struct {
		name        string
		isMax       bool
		oldExists   bool
		shouldExist bool
		addIsDelta  bool
		delIsDelta  bool
		cmpDeltaOld int
		addedCnt    int64
		removedCnt  int64
		expected    minMaxDecision
	}{
		{
			name:        "max old missing tie dominant no net add requires recompute",
			isMax:       true,
			oldExists:   false,
			shouldExist: true,
			addIsDelta:  true,
			delIsDelta:  true,
			cmpDeltaOld: 1,
			addedCnt:    2,
			removedCnt:  2,
			expected:    minMaxDecisionRecompute,
		},
		{
			name:        "max delta stronger tie dominant with net add uses added",
			isMax:       true,
			oldExists:   true,
			shouldExist: true,
			addIsDelta:  true,
			delIsDelta:  true,
			cmpDeltaOld: 1,
			addedCnt:    3,
			removedCnt:  2,
			expected:    minMaxDecisionUseAdded,
		},
		{
			name:        "max delta stronger tie dominant without net add recompute",
			isMax:       true,
			oldExists:   true,
			shouldExist: true,
			addIsDelta:  true,
			delIsDelta:  true,
			cmpDeltaOld: 1,
			addedCnt:    2,
			removedCnt:  2,
			expected:    minMaxDecisionRecompute,
		},
		{
			name:        "max equal dominant delete only recompute",
			isMax:       true,
			oldExists:   true,
			shouldExist: true,
			addIsDelta:  false,
			delIsDelta:  true,
			cmpDeltaOld: 0,
			addedCnt:    0,
			removedCnt:  1,
			expected:    minMaxDecisionRecompute,
		},
		{
			name:        "min delta smaller tie dominant with net add uses added",
			isMax:       false,
			oldExists:   true,
			shouldExist: true,
			addIsDelta:  true,
			delIsDelta:  true,
			cmpDeltaOld: -1,
			addedCnt:    4,
			removedCnt:  1,
			expected:    minMaxDecisionUseAdded,
		},
		{
			name:        "min no result uses null",
			isMax:       false,
			oldExists:   true,
			shouldExist: false,
			addIsDelta:  true,
			delIsDelta:  false,
			cmpDeltaOld: -1,
			addedCnt:    1,
			removedCnt:  0,
			expected:    minMaxDecisionUseNull,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := decideMinMaxFast(
				tc.isMax,
				tc.oldExists,
				tc.shouldExist,
				tc.addIsDelta,
				tc.delIsDelta,
				tc.cmpDeltaOld,
				tc.addedCnt,
				tc.removedCnt,
			)
			require.Equal(t, tc.expected, got)
		})
	}
}
