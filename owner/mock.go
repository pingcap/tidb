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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package owner

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl/util"
)

var _ Manager = &mockManager{}

// mockManager represents the structure which is used for electing owner.
// It's used for local store and testing.
// So this worker will always be the owner.
type mockManager struct {
	owner       int32
	id          string // id is the ID of manager.
	cancel      context.CancelFunc
	beOwnerHook func()
}

// NewMockManager creates a new mock Manager.
func NewMockManager(ctx context.Context, id string) Manager {
	_, cancelFunc := context.WithCancel(ctx)
	return &mockManager{
		id:     id,
		cancel: cancelFunc,
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

func (m *mockManager) toBeOwner() {
	if m.beOwnerHook != nil {
		m.beOwnerHook()
	}
	atomic.StoreInt32(&m.owner, 1)
}

// RetireOwner implements Manager.RetireOwner interface.
func (m *mockManager) RetireOwner() {
	atomic.StoreInt32(&m.owner, 0)
}

// Cancel implements Manager.Cancel interface.
func (m *mockManager) Cancel() {
	m.cancel()
}

// GetOwnerID implements Manager.GetOwnerID interface.
func (m *mockManager) GetOwnerID(_ context.Context) (string, error) {
	if m.IsOwner() {
		return m.ID(), nil
	}
	return "", errors.New("no owner")
}

var mockOwnerOpValue = OpNone

func (*mockManager) SetOwnerOpValue(_ context.Context, op OpType) error {
	mockOwnerOpValue = op
	return nil
}

// CampaignOwner implements Manager.CampaignOwner interface.
func (m *mockManager) CampaignOwner() error {
	m.toBeOwner()
	return nil
}

// ResignOwner lets the owner start a new election.
func (m *mockManager) ResignOwner(_ context.Context) error {
	if m.IsOwner() {
		m.RetireOwner()
	}
	return nil
}

// RequireOwner implements Manager.RequireOwner interface.
func (*mockManager) RequireOwner(context.Context) error {
	return nil
}

func (m *mockManager) SetBeOwnerHook(hook func()) {
	m.beOwnerHook = hook
}

// CampaignCancel implements Manager.CampaignCancel interface
func (*mockManager) CampaignCancel() {
	// do nothing
}

func mockDelOwnerKey(mockCal, ownerKey string, m *ownerManager) error {
	checkIsOwner := func(m *ownerManager, checkTrue bool) error {
		// 5s
		for i := 0; i < 100; i++ {
			if m.IsOwner() == checkTrue {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if m.IsOwner() != checkTrue {
			return errors.Errorf("expect manager state:%v", checkTrue)
		}
		return nil
	}

	needCheckOwner := false
	switch mockCal {
	case "delOwnerKeyAndNotOwner":
		m.CampaignCancel()
		// Make sure the manager is not owner. And it will exit campaignLoop.
		err := checkIsOwner(m, false)
		if err != nil {
			return err
		}
	case "onlyDelOwnerKey":
		needCheckOwner = true
	}

	err := util.DeleteKeyFromEtcd(ownerKey, m.etcdCli, 1, keyOpDefaultTimeout)
	if err != nil {
		return errors.Trace(err)
	}
	if needCheckOwner {
		// Mock the manager become not owner because the owner is deleted(like TTL is timeout).
		// And then the manager campaigns the owner again, and become the owner.
		err = checkIsOwner(m, true)
		if err != nil {
			return err
		}
	}
	return nil
}
