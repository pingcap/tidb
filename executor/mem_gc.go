// Copyright 2022 PingCAP, Inc.
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

package executor

import (
	"go.uber.org/zap"
	"runtime"
	"sync"

	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
)

var memMapToRelease = newReleasedMemMap()

type releasedMemMap struct {
	sync.RWMutex
	total  int64
	memMap map[*memory.Tracker]int64
}

func newReleasedMemMap() *releasedMemMap {
	released := &releasedMemMap{total: 0}
	released.memMap = make(map[*memory.Tracker]int64)
	return released
}

func ReleaseMemory(memTracker *memory.Tracker, toRelease int64) {
	gcTrigger := variable.GCTriggerForTrackTest.Load()
	if gcTrigger <= 0 {
		memTracker.Consume(-toRelease)
		return
	}
	memMapToRelease.Lock()
	defer memMapToRelease.Unlock()
	memMapToRelease.total += toRelease
	if memVal, exists := memMapToRelease.memMap[memTracker]; exists {
		memMapToRelease.memMap[memTracker] = memVal + toRelease
	} else {
		memMapToRelease.memMap[memTracker] = toRelease
	}
}

// TryGCIfNeeded is to trigger GC manually when GCTriggerForTrackTest is ON, since GOGC's late trigger.
func TryGCIfNeeded(force bool) {
	gcTrigger := variable.GCTriggerForTrackTest.Load()
	if gcTrigger <= 0 {
		return
	}
	memMapToRelease.Lock()
	defer memMapToRelease.Unlock()
	if force || memMapToRelease.total > gcTrigger {
		runtime.GC()
		for tracker, memVal := range memMapToRelease.memMap {
			tracker.Consume(-memVal)
			logutil.BgLogger().Info("memory", zap.Int64("release", memVal))
		}
		memMapToRelease.memMap = make(map[*memory.Tracker]int64)
		memMapToRelease.total = 0
	}
}
