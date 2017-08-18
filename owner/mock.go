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

var _ Manager = &mockManager{}

// mockManager represents the structure which is used for electing owner.
// It's used for local store and testing.
// So this worker will always be the owner.
type mockManager struct {
	owner  int32
	id     string // id is the ID of manager.
	cancel goctx.CancelFunc
}

// NewMockManager creates a new mock Manager.
func NewMockManager(id string, cancel goctx.CancelFunc) Manager {
	return &mockManager{
		id:     id,
		cancel: cancel,
	}
}

// ID implements Manager.ID interface.
func (m *mockManager) ID() string {
	return m.id
}

// IsOwner implements Manager.IsOwner interface.
func (m *mockManager) IsOwner() bool {
	return atomic.LoadInt32(&m.owner) == 1
}

// SetOwner implements Manager.SetOwner interface.
func (m *mockManager) SetOwner(isOwner bool) {
	if isOwner {
		atomic.StoreInt32(&m.owner, 1)
	} else {
		atomic.StoreInt32(&m.owner, 0)
	}
}

// Cancel implements Manager.Cancel interface.
func (m *mockManager) Cancel() {
	m.cancel()
}

// GetOwnerID implements Manager.GetOwnerID interface.
func (m *mockManager) GetOwnerID(ctx goctx.Context) (string, error) {
	if m.IsOwner() {
		return m.ID(), nil
	}
	return "", errors.New("no owner")
}

// CampaignOwner implements Manager.CampaignOwner interface.
func (m *mockManager) CampaignOwner(_ goctx.Context) error {
	m.SetOwner(true)
	return nil
}
