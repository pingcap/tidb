// Copyright 2023 PingCAP, Inc.
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

package proto

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSubtaskIsDone(t *testing.T) {
	cases := []struct {
		state SubtaskState
		done  bool
	}{
		{SubtaskStatePending, false},
		{SubtaskStateRunning, false},
		{SubtaskStateSucceed, true},
		{SubtaskStateReverting, false},
		{SubtaskStateRevertPending, false},
		{SubtaskStateFailed, true},
		{SubtaskStateRevertFailed, true},
		{SubtaskStatePaused, false},
		{SubtaskStateReverted, true},
		{SubtaskStateCanceled, true},
	}
	for _, c := range cases {
		require.Equal(t, c.done, (&Subtask{State: c.state}).IsDone())
	}
}
