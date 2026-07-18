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

package serverstate

import (
	"context"

	"github.com/pingcap/failpoint"
	clientv3 "go.etcd.io/etcd/client/v3"
	atomicutil "go.uber.org/atomic"
)

// NewMemSyncer creates a new memSyncer.
func NewMemSyncer() Syncer {
	return &memSyncer{}
}

// clusterState mocks cluster state.
// We move it from memSyncer to here. Because we want to make it unaffected by ddl close.
var clusterState *atomicutil.Pointer[StateInfo]

// memSyncer is memory based server state syncer used for uni-store.
// it's mainly for test as there is only 1 instance for uni-store.
type memSyncer struct {
	globalVerCh chan clientv3.WatchResponse
	mockSession chan struct{}
}

// Init implements Syncer.Init interface.
func (s *memSyncer) Init(context.Context) error {
	s.globalVerCh = make(chan clientv3.WatchResponse, 1)
	s.mockSession = make(chan struct{}, 1)
	state := NewStateInfo(StateNormalRunning)
	if clusterState == nil {
		clusterState = atomicutil.NewPointer(state)
	}
	return nil
}

// UpdateGlobalState implements Syncer.UpdateGlobalState interface.
func (s *memSyncer) UpdateGlobalState(_ context.Context, stateInfo *StateInfo) error {
	failpoint.Inject("mockUpgradingState", func(val failpoint.Value) {
		if val.(bool) {
			clusterState.Store(stateInfo)
			failpoint.Return(nil)
		}
	})
	s.globalVerCh <- clientv3.WatchResponse{}
	clusterState.Store(stateInfo)
	return nil
}

// GetGlobalState implements Syncer.GetGlobalState interface.
func (*memSyncer) GetGlobalState(context.Context) (*StateInfo, error) {
	return clusterState.Load(), nil
}

// IsUpgradingState implements Syncer.IsUpgradingState interface.
func (*memSyncer) IsUpgradingState() bool {
	return clusterState.Load().State == StateUpgrading
}

// WatchChan implements Syncer.WatchChan interface.
func (s *memSyncer) WatchChan() clientv3.WatchChan {
	return s.globalVerCh
}

// Rewatch implements Syncer.Rewatch interface.
func (*memSyncer) Rewatch(context.Context) {}
