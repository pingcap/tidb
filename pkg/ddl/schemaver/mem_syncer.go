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

package schemaver

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const checkVersionsInterval = 2 * time.Millisecond

// MemSyncer is in memory schema version syncer, used for uni-store where there is
// only 1 TiDB instance. it's mainly for test.
// exported for testing.
type MemSyncer struct {
	selfSchemaVersion int64
	mdlSchemaVersions sync.Map
	globalVerCh       chan clientv3.WatchResponse
	mockSession       chan struct{}
}

var _ Syncer = &MemSyncer{}

// NewMemSyncer creates a new memory Syncer.
func NewMemSyncer() Syncer {
	return &MemSyncer{}
}

// Init implements Syncer.Init interface.
func (s *MemSyncer) Init(_ context.Context) error {
	s.mdlSchemaVersions = sync.Map{}
	s.globalVerCh = make(chan clientv3.WatchResponse, 1)
	s.mockSession = make(chan struct{}, 1)
	return nil
}

// GlobalVersionCh implements Syncer.GlobalVersionCh interface.
func (s *MemSyncer) GlobalVersionCh() clientv3.WatchChan {
	return s.globalVerCh
}

// WatchGlobalSchemaVer implements Syncer.WatchGlobalSchemaVer interface.
func (*MemSyncer) WatchGlobalSchemaVer(context.Context) {}

// UpdateSelfVersion implements Syncer.UpdateSelfVersion interface.
func (s *MemSyncer) UpdateSelfVersion(_ context.Context, jobID int64, version int64) error {
	failpoint.Inject("mockUpdateMDLToETCDError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.New("mock update mdl to etcd error"))
		}
	})
	if vardef.EnableMDL.Load() {
		s.mdlSchemaVersions.Store(jobID, version)
	} else {
		atomic.StoreInt64(&s.selfSchemaVersion, version)
	}
	return nil
}

// Done implements Syncer.Done interface.
func (s *MemSyncer) Done() <-chan struct{} {
	return s.mockSession
}

// CloseSession mockSession, it is exported for testing.
func (s *MemSyncer) CloseSession() {
	close(s.mockSession)
}

// Restart implements Syncer.Restart interface.
func (s *MemSyncer) Restart(_ context.Context) error {
	s.mockSession = make(chan struct{}, 1)
	return nil
}

// OwnerUpdateGlobalVersion implements Syncer.OwnerUpdateGlobalVersion interface.
func (s *MemSyncer) OwnerUpdateGlobalVersion(_ context.Context, _ int64) error {
	select {
	case s.globalVerCh <- clientv3.WatchResponse{}:
	default:
	}
	return nil
}

// WaitVersionSynced implements Syncer.WaitVersionSynced interface.
func (s *MemSyncer) WaitVersionSynced(ctx context.Context, jobID int64, latestVer int64) error {
	ticker := time.NewTicker(checkVersionsInterval)
	defer ticker.Stop()

	failpoint.Inject("mockOwnerCheckAllVersionSlow", func(val failpoint.Value) {
		if v, ok := val.(int); ok && v == int(jobID) {
			time.Sleep(2 * time.Second)
		}
	})

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			if vardef.EnableMDL.Load() {
				ver, ok := s.mdlSchemaVersions.Load(jobID)
				if ok && ver.(int64) >= latestVer {
					return nil
				}
			} else {
				ver := atomic.LoadInt64(&s.selfSchemaVersion)
				if ver >= latestVer {
					return nil
				}
			}
		}
	}
}

// SyncJobSchemaVerLoop implements Syncer.SyncJobSchemaVerLoop interface.
func (*MemSyncer) SyncJobSchemaVerLoop(context.Context) {
}

// Close implements Syncer.Close interface.
func (*MemSyncer) Close() {}
