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

package owner

import (
	"sync/atomic"

	"github.com/juju/errors"
	goctx "golang.org/x/net/context"
)

var _ OwnerManager = &mockOwnerManager{}

// mockOwnerManager represents the structure which is used for electing owner.
// It's used for local store and testing.
// So this worker will always be the owner.
type mockOwnerManager struct {
	owner  int32
	id     string // id is the ID of manager.
	cancel goctx.CancelFunc
}

// NewMockOwnerManager creates a new mock OwnerManager.
func NewMockOwnerManager(id string, cancel goctx.CancelFunc) OwnerManager {
	return &mockOwnerManager{
		id:     id,
		cancel: cancel,
	}
}

// ID implements mockOwnerManager.ID interface.
func (m *mockOwnerManager) ID() string {
	return m.id
}

// IsOwner implements mockOwnerManager.IsOwner interface.
func (m *mockOwnerManager) IsOwner() bool {
	return atomic.LoadInt32(&m.owner) == 1
}

// SetOwner implements mockOwnerManager.SetOwner interface.
func (m *mockOwnerManager) SetOwner(isOwner bool) {
	if isOwner {
		atomic.StoreInt32(&m.owner, 1)
	} else {
		atomic.StoreInt32(&m.owner, 0)
	}
}

// Cancel implements mockOwnerManager.Cancel interface.
func (m *mockOwnerManager) Cancel() {
	m.cancel()
}

// GetOwnerID implements OwnerManager.GetOwnerID interface.
func (m *mockOwnerManager) GetOwnerID(ctx goctx.Context) (string, error) {
	if m.IsOwner() {
		return m.ID(), nil
	}
	return "", errors.New("no owner")
}

// CampaignOwner implements mockOwnerManager.CampaignOwner interface.
func (m *mockOwnerManager) CampaignOwner(_ goctx.Context) error {
	m.SetOwner(true)
	return nil
}
