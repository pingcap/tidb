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

package syncpoint

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
)

const syncScriptTestPath = "github.com/pingcap/tidb/br/pkg/utiltest/syncpoint"

func TestSequence(t *testing.T) {
	script := New(t)
	seqCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	events := make(chan string, 3)
	script.BeginSeq(seqCtx,
		Step(syncScriptTestPath+"/sync-script-a", func() { events <- "a" }),
		Step(syncScriptTestPath+"/sync-script-b", func() { events <- "b" }),
		Step(syncScriptTestPath+"/sync-script-c", func() { events <- "c" }),
	)

	var wg sync.WaitGroup
	for _, name := range []string{"sync-script-c", "sync-script-b", "sync-script-a"} {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			triggerSyncScriptPoint(name)
		}(name)
	}

	require.Equal(t, "a", <-events)
	require.Equal(t, "b", <-events)
	require.Equal(t, "c", <-events)

	script.EndSeq()
	wg.Wait()
}

func TestIgnoresRegisteredStepsWithoutActiveSequence(t *testing.T) {
	script := New(t)
	var hits atomic.Int32

	seqCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	script.BeginSeq(seqCtx, Step(syncScriptTestPath+"/sync-script-a", func() {
		hits.Add(1)
	}))

	triggerSyncScriptPoint("sync-script-a")
	script.EndSeq()
	require.Equal(t, int32(1), hits.Load())

	triggerSyncScriptPoint("sync-script-a")
	require.Equal(t, int32(1), hits.Load())
}

func triggerSyncScriptPoint(name string) {
	switch name {
	case "sync-script-a":
		failpoint.InjectCall("sync-script-a")
	case "sync-script-b":
		failpoint.InjectCall("sync-script-b")
	case "sync-script-c":
		failpoint.InjectCall("sync-script-c")
	default:
		panic("unknown syncpoint test failpoint: " + name)
	}
}
