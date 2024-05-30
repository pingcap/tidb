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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"go.uber.org/zap"
)

var _ Manager = &mockManager{}

// mockManager represents the structure which is used for electing owner.
// It's used for local store and testing.
// So this worker will always be the owner.
type mockManager struct {
	id           string // id is the ID of manager.
	storeID      string
	key          string
	ctx          context.Context
	wg           sync.WaitGroup
	cancel       context.CancelFunc
	listener     Listener
	retireHook   func()
	campaignDone chan struct{}
	resignDone   chan struct{}
}

var mockOwnerOpValue atomic.Pointer[OpType]

// NewMockManager creates a new mock Manager.
func NewMockManager(ctx context.Context, id string, store kv.Storage, ownerKey string) Manager {
	cancelCtx, cancelFunc := context.WithCancel(ctx)
	storeID := "mock_store_id"
	if store != nil {
		storeID = store.UUID()
	}

	// Make sure the mockOwnerOpValue is initialized before GetOwnerOpValue in bootstrap.
	op := OpNone
	mockOwnerOpValue.Store(&op)
	return &mockManager{
		id:           id,
		storeID:      storeID,
		key:          ownerKey,
		ctx:          cancelCtx,
		cancel:       cancelFunc,
		campaignDone: make(chan struct{}),
		resignDone:   make(chan struct{}),
	}
}

// ID implements Manager.ID interface.
func (m *mockManager) ID() string {
	return m.id
}

// IsOwner implements Manager.IsOwner interface.
func (m *mockManager) IsOwner() bool {
	logutil.BgLogger().Debug("owner manager checks owner", zap.String("category", "ddl"),
		zap.String("ID", m.id), zap.String("ownerKey", m.key))
	return util.MockGlobalStateEntry.OwnerKey(m.storeID, m.key).IsOwner(m.id)
}

func (m *mockManager) toBeOwner() {
	ok := util.MockGlobalStateEntry.OwnerKey(m.storeID, m.key).SetOwner(m.id)
	if ok {
		logutil.BgLogger().Info("owner manager gets owner", zap.String("category", "ddl"),
			zap.String("ID", m.id), zap.String("ownerKey", m.key))
		if m.listener != nil {
			m.listener.OnBecomeOwner()
		}
	}
}

// RetireOwner implements Manager.RetireOwner interface.
func (m *mockManager) RetireOwner() {
	util.MockGlobalStateEntry.OwnerKey(m.storeID, m.key).UnsetOwner(m.id)
	logutil.BgLogger().Info("owner manager retire owner", zap.String("category", "ddl"),
		zap.String("ID", m.id), zap.String("ownerKey", m.key))
	if m.listener != nil {
		m.listener.OnRetireOwner()
	}
}

// Cancel implements Manager.Cancel interface.
func (m *mockManager) Cancel() {
	m.cancel()
	m.wg.Wait()
	logutil.BgLogger().Info("owner manager is canceled", zap.String("category", "ddl"),
		zap.String("ID", m.id), zap.String("ownerKey", m.key))
}

// GetOwnerID implements Manager.GetOwnerID interface.
func (m *mockManager) GetOwnerID(_ context.Context) (string, error) {
	if m.IsOwner() {
		return m.ID(), nil
	}
	return "", errors.New("no owner")
}

func (*mockManager) SetOwnerOpValue(_ context.Context, op OpType) error {
	failpoint.Inject("MockNotSetOwnerOp", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil)
		}
	})
	mockOwnerOpValue.Store(&op)
	return nil
}

// CampaignOwner implements Manager.CampaignOwner interface.
func (m *mockManager) CampaignOwner(_ ...int) error {
	m.wg.Add(1)
	go func() {
		logutil.BgLogger().Debug("owner manager campaign owner", zap.String("category", "ddl"),
			zap.String("ID", m.id), zap.String("ownerKey", m.key))
		defer m.wg.Done()
		for {
			select {
			case <-m.campaignDone:
				m.RetireOwner()
				logutil.BgLogger().Debug("owner manager campaign done", zap.String("category", "ddl"), zap.String("ID", m.id))
				return
			case <-m.ctx.Done():
				m.RetireOwner()
				logutil.BgLogger().Debug("owner manager is cancelled", zap.String("category", "ddl"), zap.String("ID", m.id))
				return
			case <-m.resignDone:
				m.RetireOwner()
				//nolint: errcheck
				timeutil.Sleep(m.ctx, 1*time.Second) // Give a chance to the other owner managers to get owner.
			default:
				m.toBeOwner()
				//nolint: errcheck
				timeutil.Sleep(m.ctx, 1*time.Second) // Speed up domain.Close()
				logutil.BgLogger().Debug("owner manager tick", zap.String("category", "ddl"), zap.String("ID", m.id),
					zap.String("ownerKey", m.key), zap.String("currentOwner", util.MockGlobalStateEntry.OwnerKey(m.storeID, m.key).GetOwner()))
			}
		}
	}()
	return nil
}

// ResignOwner lets the owner start a new election.
func (m *mockManager) ResignOwner(_ context.Context) error {
	m.resignDone <- struct{}{}
	return nil
}

// RequireOwner implements Manager.RequireOwner interface.
func (*mockManager) RequireOwner(context.Context) error {
	return nil
}

// SetListener implements Manager.SetListener interface.
func (m *mockManager) SetListener(listener Listener) {
	m.listener = listener
}

// CampaignCancel implements Manager.CampaignCancel interface
func (m *mockManager) CampaignCancel() {
	m.campaignDone <- struct{}{}
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
