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

package initstats

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRangeWorkerTaskPanic(t *testing.T) {
	ls := NewRangeWorker("test", func(Task) error {
		panic("mock panic in processTask")
	}, 1, 1, 0)
	ls.SendTask(Task{StartTid: 1, EndTid: 2})
	close(ls.taskChan)
	// A panic in a task is logged as an error, and the worker loop goes on.
	require.NotPanics(t, ls.loadStats)
	require.Equal(t, uint64(1), ls.completeTaskCnt.Load())
}
