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

package taskexecutor

import (
	"testing"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/stretchr/testify/require"
)

func TestSlotManager(t *testing.T) {
	sm := slotManager{
		executorSlotInfos: make(map[int64]*slotInfo),
		available:         10,
	}

	var (
		taskID  = int64(1)
		taskID2 = int64(2)
	)

	task := &proto.Task{
		ID:          taskID,
		Priority:    1,
		Concurrency: 1,
	}
	require.True(t, sm.canAlloc(task))
	sm.alloc(task)
	require.Equal(t, 1, sm.executorSlotInfos[taskID].priority)
	require.Equal(t, 1, sm.executorSlotInfos[taskID].slotCount)
	require.Equal(t, 9, sm.available)

	require.False(t, sm.canAlloc(&proto.Task{
		ID:          taskID2,
		Priority:    2,
		Concurrency: 10,
	}))

	sm.free(taskID)
	require.Nil(t, sm.executorSlotInfos[taskID])
}
