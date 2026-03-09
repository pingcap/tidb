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

package testutil

import (
	"sync"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
)

const syncScriptTestPath = "github.com/pingcap/tidb/br/pkg/stream/crr/internal/testutil"

func TestSyncScriptRequireSeq(t *testing.T) {
	script := NewSyncScript(t, syncScriptTestPath)
	script.RequireSeq("sync-script-a", "sync-script-b", "sync-script-c")

	events := make(chan string, 3)
	script.On("sync-script-a", func(ctx InjectContext) { events <- "a" })
	script.On("sync-script-b", func(ctx InjectContext) { events <- "b" })
	script.On("sync-script-c", func(ctx InjectContext) { events <- "c" })

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

	script.WaitUntil("sync-script-a", 1)
	script.WaitUntil("sync-script-b", 1)
	script.WaitUntil("sync-script-c", 1)
	wg.Wait()
}

func TestSyncScriptInterleave(t *testing.T) {
	script := NewSyncScript(t, syncScriptTestPath)
	left := []string{"sync-script-a1", "sync-script-a2", "sync-script-a3"}
	right := []string{"sync-script-b1", "sync-script-b2"}
	script.Interleave(left, right)

	events := make(chan string, len(left)+len(right))
	for _, name := range left {
		script.On(name, func(ctx InjectContext) { events <- name })
	}
	for _, name := range right {
		script.On(name, func(ctx InjectContext) { events <- name })
	}

	all := append(append([]string(nil), left...), right...)
	var wg sync.WaitGroup
	for _, name := range all {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			triggerSyncScriptPoint(name)
		}(name)
	}

	got := make([]string, 0, len(all))
	for i := 0; i < len(all); i++ {
		got = append(got, <-events)
	}
	wg.Wait()

	require.Len(t, got, len(all))
	require.Equal(t, left, subseq(got, left))
	require.Equal(t, right, subseq(got, right))
}

func subseq(seq, universe []string) []string {
	allowed := make(map[string]struct{}, len(universe))
	for _, item := range universe {
		allowed[item] = struct{}{}
	}
	filtered := make([]string, 0, len(universe))
	for _, item := range seq {
		if _, ok := allowed[item]; ok {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

func triggerSyncScriptPoint(name string) {
	switch name {
	case "sync-script-a":
		failpoint.InjectCall("sync-script-a")
	case "sync-script-b":
		failpoint.InjectCall("sync-script-b")
	case "sync-script-c":
		failpoint.InjectCall("sync-script-c")
	case "sync-script-a1":
		failpoint.InjectCall("sync-script-a1")
	case "sync-script-a2":
		failpoint.InjectCall("sync-script-a2")
	case "sync-script-a3":
		failpoint.InjectCall("sync-script-a3")
	case "sync-script-b1":
		failpoint.InjectCall("sync-script-b1")
	case "sync-script-b2":
		failpoint.InjectCall("sync-script-b2")
	default:
		panic("unknown sync script test failpoint: " + name)
	}
}
