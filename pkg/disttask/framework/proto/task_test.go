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

func TestTaskStep(t *testing.T) {
	// make sure we don't change the value of StepInit accidentally
	require.Equal(t, int64(-1), int64(StepInit))
	require.Equal(t, int64(-2), int64(StepDone))
}

func TestTaskIsDone(t *testing.T) {
	cases := []struct {
		state TaskState
		done  bool
	}{
		{TaskStatePending, false},
		{TaskStateRunning, false},
		{TaskStateSucceed, true},
		{TaskStateReverting, false},
		{TaskStateFailed, true},
		{TaskStateRevertFailed, false},
		{TaskStateCancelling, false},
		{TaskStateCanceled, false},
		{TaskStatePausing, false},
		{TaskStatePaused, false},
		{TaskStateReverted, true},
	}
	for _, c := range cases {
		require.Equal(t, c.done, (&Task{State: c.state}).IsDone())
	}
}
