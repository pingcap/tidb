// Copyright 2024 PingCAP, Inc.
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

package taskexecutor

import (
	"testing"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/stretchr/testify/require"
)

func TestVerifySubtaskStateTransform(t *testing.T) {
	testCases := []struct {
		oldState proto.SubtaskState
		newState proto.SubtaskState
		expect   bool
	}{
		{proto.SubtaskStatePending, proto.SubtaskStateRunning, true},
		{proto.SubtaskStateRunning, proto.SubtaskStatePaused, true},
		{proto.SubtaskStateRunning, proto.SubtaskStateSucceed, true},
		{proto.SubtaskStateRunning, proto.SubtaskStateFailed, true},
		{proto.SubtaskStateRunning, proto.SubtaskStateCanceled, true},
		{proto.SubtaskStateRevertPending, proto.SubtaskStateReverting, true},
		{proto.SubtaskStateReverting, proto.SubtaskStateReverted, true},
		{proto.SubtaskStateReverting, proto.SubtaskStateRevertFailed, true},
		{proto.SubtaskStatePending, proto.SubtaskStatePaused, false},
		{proto.SubtaskStateRunning, proto.SubtaskStateReverting, false},
		{proto.SubtaskStatePaused, proto.SubtaskStateReverting, false},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.expect, VerifySubtaskStateTransform(tc.oldState, tc.newState))
	}
}
