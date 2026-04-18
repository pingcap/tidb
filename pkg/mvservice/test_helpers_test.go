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

package mvservice

import (
	"context"
	"testing"
	"time"
)

const (
	testEventuallyWait = time.Second
	testEventuallyTick = time.Millisecond
)

func runMVServiceForTest(t *testing.T, svc *MVService, cancel context.CancelFunc) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		svc.Run()
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(testEventuallyWait):
			t.Fatal("mv service did not stop in time")
		}
	})
}

func newRunningMVServiceForTest(t *testing.T, helper Helper) *MVService {
	t.Helper()
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())
	svc.executor.Run()
	t.Cleanup(func() {
		svc.executor.Close()
	})
	return svc
}

func setHistoryGCOwnerForTest(svc *MVService, ownerHash uint32) {
	svc.nextHistoryGCAtMillis.Store(0)
	svc.sch.mu.Lock()
	svc.sch.ID = "nodeA"
	svc.sch.chash.hashFunc = mustHash(map[string]uint32{
		"nodeA#0":           10,
		"nodeB#0":           30,
		mvHistoryGCOwnerKey: ownerHash,
	})
	svc.sch.servers = map[string]serverInfo{
		"nodeA": {ID: "nodeA"},
		"nodeB": {ID: "nodeB"},
	}
	svc.sch.chash.Rebuild(svc.sch.servers)
	svc.sch.mu.Unlock()
}
