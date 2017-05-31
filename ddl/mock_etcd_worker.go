// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Package ddl is just for test only.
package ddl

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/juju/errors"
	goctx "golang.org/x/net/context"
)

var _ EtcdWorker = &mockEtcdWorker{}

// mockEtcdWorker represents the structure which is used for electing owner.
// It's used for local store and testing.
// So this worker always the ddl owner and background owner.
type mockEtcdWorker struct {
	*mockSchemaSyncer
	ddlOwner int32
	bgOwner  int32
	ddlID    string // id is the ID of DDL.
	cancel   goctx.CancelFunc
}

// NewMockEtcdWorker creates a new mockEtcdWorker.
func NewMockEtcdWorker(etcdCli *clientv3.Client, id string, cancel goctx.CancelFunc) EtcdWorker {
	return &mockEtcdWorker{
		mockSchemaSyncer: &mockSchemaSyncer{},
		ddlID:            id,
		cancel:           cancel,
		ddlOwner:         int32(1), // Make sure the worker is the DDL owner.
		bgOwner:          int32(1), // Make sure the worker is the background owner.
	}
}

// ID implements mockEtcdWorker.ID interface.
func (w *mockEtcdWorker) ID() string {
	return w.ddlID
}

// IsOwner implements mockEtcdWorker.IsOwner interface.
func (w *mockEtcdWorker) IsOwner() bool {
	return atomic.LoadInt32(&w.ddlOwner) == 1
}

// SetOwner implements mockEtcdWorker.SetOwner interface.
func (w *mockEtcdWorker) SetOwner(isOwner bool) {
	if isOwner {
		atomic.StoreInt32(&w.ddlOwner, 1)
	} else {
		atomic.StoreInt32(&w.ddlOwner, 0)
	}
}

// Cancel implements mockEtcdWorker.Cancel interface.
func (w *mockEtcdWorker) Cancel() {
	w.cancel()
}

// IsBgOwner implements mockEtcdWorker.IsBgOwner interface.
func (w *mockEtcdWorker) IsBgOwner() bool {
	return atomic.LoadInt32(&w.bgOwner) == 1
}

// SetBgOwner implements mockEtcdWorker.SetBgOwner interface.
func (w *mockEtcdWorker) SetBgOwner(isOwner bool) {
	if isOwner {
		atomic.StoreInt32(&w.bgOwner, 1)
	} else {
		atomic.StoreInt32(&w.bgOwner, 0)
	}
}

// CampaignOwners implements mockEtcdWorker.CampaignOwners interface.
func (w *mockEtcdWorker) CampaignOwners(_ goctx.Context, _ *sync.WaitGroup) error { return nil }

const mockCheckVersInterval = 5 * time.Millisecond

type mockSchemaSyncer struct {
	selfSchemaVersion int64
	globalVerCh       chan clientv3.WatchResponse
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

// UpdateGlobalVersion implements SchemaSyncer.UpdateGlobalVersion interface.
func (s *mockSchemaSyncer) UpdateGlobalVersion(ctx goctx.Context, version int64) error {
	select {
	case s.globalVerCh <- clientv3.WatchResponse{}:
	default:
	}
	return nil
}

// RemoveSelfVersionPath implements SchemaSyncer.RemoveSelfVersionPath interface.
func (s *mockSchemaSyncer) RemoveSelfVersionPath() error {
	return nil
}

// CheckAllVersions implements SchemaSyncer.CheckAllVersions interface.
func (s *mockSchemaSyncer) CheckAllVersions(ctx goctx.Context, latestVer int64) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		default:
		}
		ver := atomic.LoadInt64(&s.selfSchemaVersion)
		if ver == latestVer {
			return nil
		}
		time.Sleep(mockCheckVersInterval)
	}
}
