// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://wwm.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/juju/errors"
	goctx "golang.org/x/net/context"
)

var _ OwnerManager = &mockOwnerManager{}
var _ SchemaSyncer = &mockSchemaSyncer{}

// mockOwnerManager represents the structure which is used for electing owner.
// It's used for local store and testing.
// So this worker will always be the ddl owner and background owner.
type mockOwnerManager struct {
	ddlOwner int32
	bgOwner  int32
	ddlID    string // id is the ID of DDL.
	cancel   goctx.CancelFunc
}

// NewMockOwnerManager creates a new mock OwnerManager.
func NewMockOwnerManager(id string, cancel goctx.CancelFunc) OwnerManager {
	return &mockOwnerManager{
		ddlID:  id,
		cancel: cancel,
	}
}

// ID implements mockOwnerManager.ID interface.
func (m *mockOwnerManager) ID() string {
	return m.ddlID
}

// IsOwner implements mockOwnerManager.IsOwner interface.
func (m *mockOwnerManager) IsOwner() bool {
	return atomic.LoadInt32(&m.ddlOwner) == 1
}

// SetOwner implements mockOwnerManager.SetOwner interface.
func (m *mockOwnerManager) SetOwner(isOwner bool) {
	if isOwner {
		atomic.StoreInt32(&m.ddlOwner, 1)
	} else {
		atomic.StoreInt32(&m.ddlOwner, 0)
	}
}

// Cancel implements mockOwnerManager.Cancel interface.
func (m *mockOwnerManager) Cancel() {
	m.cancel()
}

// IsBgOwner implements mockOwnerManager.IsBgOwner interface.
func (m *mockOwnerManager) IsBgOwner() bool {
	return atomic.LoadInt32(&m.bgOwner) == 1
}

// SetBgOwner implements mockOwnerManager.SetBgOwner interface.
func (m *mockOwnerManager) SetBgOwner(isOwner bool) {
	if isOwner {
		atomic.StoreInt32(&m.bgOwner, 1)
	} else {
		atomic.StoreInt32(&m.bgOwner, 0)
	}
}

// CampaignOwners implements mockOwnerManager.CampaignOwners interface.
func (m *mockOwnerManager) CampaignOwners(_ goctx.Context, _ *sync.WaitGroup) error {
	m.SetOwner(true)
	m.SetBgOwner(true)
	return nil
}

const mockCheckVersInterval = 2 * time.Millisecond

type mockSchemaSyncer struct {
	selfSchemaVersion int64
	globalVerCh       chan clientv3.WatchResponse
}

// NewMockSchemaSyncer creates a new mock SchemaSyncer.
func NewMockSchemaSyncer() SchemaSyncer {
	return &mockSchemaSyncer{}
}

// Init implements SchemaSyncer.Init interface.
func (s *mockSchemaSyncer) Init(ctx goctx.Context) error {
	s.globalVerCh = make(chan clientv3.WatchResponse, 1)
	return nil
}

// GlobalVersionCh implements SchemaSyncer.GlobalVersionCh interface.
func (s *mockSchemaSyncer) GlobalVersionCh() clientv3.WatchChan {
	return s.globalVerCh
}

// UpdateSelfVersion implements SchemaSyncer.UpdateSelfVersion interface.
func (s *mockSchemaSyncer) UpdateSelfVersion(ctx goctx.Context, version int64) error {
	atomic.StoreInt64(&s.selfSchemaVersion, version)
	return nil
}

// RemoveSelfVersionPath implements SchemaSyncer.RemoveSelfVersionPath interface.
func (s *mockSchemaSyncer) RemoveSelfVersionPath() error { return nil }

// OwnerUpdateGlobalVersion implements SchemaSyncer.OwnerUpdateGlobalVersion interface.
func (s *mockSchemaSyncer) OwnerUpdateGlobalVersion(ctx goctx.Context, version int64) error {
	select {
	case s.globalVerCh <- clientv3.WatchResponse{}:
	default:
	}
	return nil
}

// OwnerCheckAllVersions implements SchemaSyncer.OwnerCheckAllVersions interface.
func (s *mockSchemaSyncer) OwnerCheckAllVersions(ctx goctx.Context, latestVer int64) error {
	ticker := time.NewTicker(mockCheckVersInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			ver := atomic.LoadInt64(&s.selfSchemaVersion)
			if ver == latestVer {
				return nil
			}
		}
	}
}
