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

package planner

import (
	"testing"

	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

func correlateRoundForTest(t *testing.T) alternativeRound {
	t.Helper()
	for _, round := range alternativeRounds {
		if round.name == "correlate" {
			return round
		}
	}
	t.Fatal("correlate alternative round not found")
	return alternativeRound{}
}

func TestAlternativeRoundSessionIsolation(t *testing.T) {
	round := correlateRoundForTest(t)
	first := &variable.SessionVars{EnableCorrelateSubquery: false}
	second := &variable.SessionVars{EnableCorrelateSubquery: true}
	firstStarted, secondStarted, firstFinished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	secondFinished := make(chan struct{})
	go func() {
		restore := round.setup(first)
		close(firstStarted)
		<-secondStarted
		restore()
		close(firstFinished)
	}()
	go func() {
		<-firstStarted
		restore := round.setup(second)
		close(secondStarted)
		<-firstFinished
		restore()
		close(secondFinished)
	}()
	<-secondFinished
	// Both setups overlap. A shared restore slot would leave the first session enabled.
	require.False(t, first.EnableCorrelateSubquery)
	require.True(t, second.EnableCorrelateSubquery)
}

func TestAlternativeRoundRestoreOnPanic(t *testing.T) {
	round := correlateRoundForTest(t)
	for _, enabled := range []bool{false, true} {
		sv := &variable.SessionVars{EnableCorrelateSubquery: enabled}
		require.PanicsWithValue(t, "optimization failed", func() {
			defer round.setup(sv)()
			require.True(t, sv.EnableCorrelateSubquery)
			panic("optimization failed")
		})
		require.Equal(t, enabled, sv.EnableCorrelateSubquery)
	}
}
