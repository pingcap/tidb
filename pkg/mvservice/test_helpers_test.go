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
	svc.runTaskExecutors()
	t.Cleanup(func() {
		svc.closeTaskExecutors()
	})
	return svc
}

func setHistoryGCOwnerForTest(svc *MVService, ownerHash uint32) {
	setServiceOwnersForTest(svc, map[string]uint32{
		mvHistoryGCOwnerKey: ownerHash,
	})
	svc.nextHistoryGCAtMillis.Store(0)
}

func setRefreshAlertCleanupOwnerForTest(svc *MVService, ownerHash uint32) {
	setServiceOwnersForTest(svc, map[string]uint32{
		mvRefreshAlertCleanupOwnerKey: ownerHash,
	})
}

func addMVRefreshAlertTasksForTest(svc *MVService, tasks ...*mv) {
	pending := make(map[int64]*mv, len(tasks))
	for _, task := range tasks {
		if task == nil {
			continue
		}
		if task.orderTs == 0 && !task.nextRefresh.IsZero() {
			task.orderTs = task.nextRefresh.UnixMilli()
		}
		pending[task.ID] = task
	}
	svc.buildMVRefreshAlertTasks(pending)
}

func addMVRefreshExecutionTasksForTest(svc *MVService, tasks ...*mv) {
	svc.mvRefreshMu.Lock()
	defer svc.mvRefreshMu.Unlock()
	if svc.mvRefreshMu.pending == nil {
		svc.mvRefreshMu.pending = make(map[int64]mvItem, len(tasks))
	}
	for _, task := range tasks {
		if task == nil {
			continue
		}
		if task.orderTs == 0 && !task.nextRefresh.IsZero() {
			task.orderTs = task.nextRefresh.UnixMilli()
		}
		svc.mvRefreshMu.pending[task.ID] = svc.mvRefreshMu.prio.Push(task)
	}
}

func setTaskOwnersForTest(svc *MVService, taskHashes map[int64]uint32) {
	overrides := make(map[string]uint32, len(taskHashes))
	for id, hash := range taskHashes {
		overrides[string(int64KeyToBinaryBytes(id))] = hash
	}
	setServiceHashOverridesForTest(svc, overrides)
}

func setServiceOwnersForTest(svc *MVService, ownerHashes map[string]uint32) {
	setServiceHashOverridesForTest(svc, ownerHashes)
}

func setServiceHashOverridesForTest(svc *MVService, overrides map[string]uint32) {
	svc.sch.mu.Lock()
	svc.sch.ID = "nodeA"
	svc.sch.chash.replicas = 1
	mapping := map[string]uint32{
		"nodeA#0":                      10,
		"nodeB#0":                      30,
		mvHistoryGCOwnerKey:            10,
		mvRefreshAlertCheckerOwnerKey1: 10,
		mvRefreshAlertCheckerOwnerKey2: 10,
		mvRefreshAlertCleanupOwnerKey:  10,
	}
	for key, hash := range overrides {
		mapping[key] = hash
	}
	svc.sch.chash.hashFunc = mustHash(mapping)
	svc.sch.servers = map[string]serverInfo{
		"nodeA": {ID: "nodeA"},
		"nodeB": {ID: "nodeB"},
	}
	svc.sch.chash.Rebuild(svc.sch.servers)
	svc.sch.mu.Unlock()
}

func setThreeNodeRefreshAlertCheckerRingForTest(svc *MVService, selfID string) {
	svc.sch.mu.Lock()
	svc.sch.ID = selfID
	svc.sch.chash.replicas = 1
	svc.sch.chash.hashFunc = mustHash(map[string]uint32{
		"nodeA#0":                      10,
		"nodeB#0":                      20,
		"nodeC#0":                      30,
		mvRefreshAlertCheckerOwnerKey1: 15, // owner: nodeB.
		mvRefreshAlertCheckerOwnerKey2: 25, // owner: nodeC.
		mvRefreshAlertCleanupOwnerKey:  25,
	})
	svc.sch.servers = map[string]serverInfo{
		"nodeA": {ID: "nodeA"},
		"nodeB": {ID: "nodeB"},
		"nodeC": {ID: "nodeC"},
	}
	svc.sch.chash.Rebuild(svc.sch.servers)
	svc.sch.mu.Unlock()
}
