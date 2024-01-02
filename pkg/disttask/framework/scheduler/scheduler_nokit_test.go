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

package scheduler

import (
	"testing"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/stretchr/testify/require"
)

func TestSchedulerIsStepSucceed(t *testing.T) {
	s := &BaseScheduler{}
	require.True(t, s.isStepSucceed(nil))
	require.True(t, s.isStepSucceed(map[proto.SubtaskState]int64{}))
	require.True(t, s.isStepSucceed(map[proto.SubtaskState]int64{
		proto.SubtaskStateSucceed: 1,
	}))
	for _, state := range []proto.SubtaskState{
		proto.SubtaskStateCanceled,
		proto.SubtaskStateFailed,
		proto.SubtaskStateReverting,
	} {
		require.False(t, s.isStepSucceed(map[proto.SubtaskState]int64{
			state: 1,
		}))
	}
}
