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
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/terror"
	util2 "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

// Manager is used to campaign the owner and manage the owner information.
type Manager interface {
	// ID returns the ID of the manager.
	ID() string
	// IsOwner returns whether the ownerManager is the owner.
	IsOwner() bool
	// RetireOwner make the manager to be a not owner. It's exported for testing.
	RetireOwner()
	// GetOwnerID gets the owner ID.
	GetOwnerID(ctx context.Context) (string, error)
	// CampaignOwner campaigns the owner.
	CampaignOwner() error
	// ResignOwner lets the owner start a new election.
	ResignOwner(ctx context.Context) error
	// Cancel cancels this etcd ownerManager campaign.
	Cancel()
	// RequireOwner requires the ownerManager is owner.
	RequireOwner(ctx context.Context) error
}

const (
	keyOpDefaultTimeout = 5 * time.Second
)

// DDLOwnerChecker is used to check whether tidb is owner.
type DDLOwnerChecker interface {
	// IsOwner returns whether the ownerManager is the owner.
	IsOwner() bool
}

// ownerManager represents the structure which is used for electing owner.
type ownerManager struct {
	id        string // id is the ID of the manager.
	key       string
	ctx       context.Context
	prompt    string
	logPrefix string
	logCtx    context.Context
	etcdCli   *clientv3.Client
	cancel    context.CancelFunc
	elec      unsafe.Pointer
	wg        sync.WaitGroup
}

// NewOwnerManager creates a new Manager.
func NewOwnerManager(ctx context.Context, etcdCli *clientv3.Client, prompt, id, key string) Manager {
	logPrefix := fmt.Sprintf("[%s] %s ownerManager %s", prompt, key, id)
	ctx, cancelFunc := context.WithCancel(ctx)
	return &ownerManager{
		etcdCli:   etcdCli,
		id:        id,
		key:       key,
		ctx:       ctx,
		prompt:    prompt,
		cancel:    cancelFunc,
		logPrefix: logPrefix,
		logCtx:    logutil.WithKeyValue(context.Background(), "owner info", logPrefix),
	}
}

// ID implements Manager.ID interface.
func (m *ownerManager) ID() string {
	return m.id
}

// IsOwner implements Manager.IsOwner interface.
func (m *ownerManager) IsOwner() bool {
	return atomic.LoadPointer(&m.elec) != unsafe.Pointer(nil)
}

// Cancel implements Manager.Cancel interface.
func (m *ownerManager) Cancel() {
	m.cancel()
	m.wg.Wait()
}

// RequireOwner implements Manager.RequireOwner interface.
func (m *ownerManager) RequireOwner(ctx context.Context) error {
	return nil
}

// ManagerSessionTTL is the etcd session's TTL in seconds. It's exported for testing.
var ManagerSessionTTL = 60

// setManagerSessionTTL sets the ManagerSessionTTL value, it's used for testing.
func setManagerSessionTTL() error {
	ttlStr := os.Getenv("tidb_manager_ttl")
	if ttlStr == "" {
		return nil
	}
	ttl, err := strconv.Atoi(ttlStr)
	if err != nil {
		return errors.Trace(err)
	}
	ManagerSessionTTL = ttl
	return nil
}

// CampaignOwner implements Manager.CampaignOwner interface.
func (m *ownerManager) CampaignOwner() error {
	logPrefix := fmt.Sprintf("[%s] %s", m.prompt, m.key)
	logutil.BgLogger().Info("start campaign owner", zap.String("ownerInfo", logPrefix))
	session, err := util2.NewSession(m.ctx, logPrefix, m.etcdCli, util2.NewSessionDefaultRetryCnt, ManagerSessionTTL)
	if err != nil {
		return errors.Trace(err)
	}
	m.wg.Add(1)
	go m.campaignLoop(session)
	return nil
}

// ResignOwner lets the owner start a new election.
func (m *ownerManager) ResignOwner(ctx context.Context) error {
	elec := (*concurrency.Election)(atomic.LoadPointer(&m.elec))
	if elec == nil {
		return errors.Errorf("This node is not a ddl owner, can't be resigned")
	}

	childCtx, cancel := context.WithTimeout(ctx, keyOpDefaultTimeout)
	err := elec.Resign(childCtx)
	cancel()
	if err != nil {
		return errors.Trace(err)
	}

	logutil.Logger(m.logCtx).Warn("resign ddl owner success")
	return nil
}

func (m *ownerManager) toBeOwner(elec *concurrency.Election) {
	atomic.StorePointer(&m.elec, unsafe.Pointer(elec))
}

// RetireOwner make the manager to be a not owner.
func (m *ownerManager) RetireOwner() {
	atomic.StorePointer(&m.elec, nil)
}

func (m *ownerManager) campaignLoop(etcdSession *concurrency.Session) {
	var cancel context.CancelFunc
	ctx, cancel := context.WithCancel(m.ctx)
	defer func() {
		cancel()
		if r := recover(); r != nil {
			logutil.BgLogger().Error("recover panic", zap.String("prompt", m.prompt), zap.Any("error", r), zap.Stack("buffer"))
			metrics.PanicCounter.WithLabelValues(metrics.LabelDDLOwner).Inc()
		}
		m.wg.Done()
	}()

	logPrefix := m.logPrefix
	logCtx := m.logCtx
	var err error
	for {
		if err != nil {
			metrics.CampaignOwnerCounter.WithLabelValues(m.prompt, err.Error()).Inc()
		}

		select {
		case <-etcdSession.Done():
			logutil.Logger(logCtx).Info("etcd session is done, creates a new one")
			leaseID := etcdSession.Lease()
			etcdSession, err = util2.NewSession(ctx, logPrefix, m.etcdCli, util2.NewSessionRetryUnlimited, ManagerSessionTTL)
			if err != nil {
				logutil.Logger(logCtx).Info("break campaign loop, NewSession failed", zap.Error(err))
				m.revokeSession(logPrefix, leaseID)
				return
			}
		case <-ctx.Done():
			logutil.Logger(logCtx).Info("break campaign loop, context is done")
			m.revokeSession(logPrefix, etcdSession.Lease())
			return
		default:
		}
		// If the etcd server turns clocks forward，the following case may occur.
		// The etcd server deletes this session's lease ID, but etcd session doesn't find it.
		// In this time if we do the campaign operation, the etcd server will return ErrLeaseNotFound.
		if terror.ErrorEqual(err, rpctypes.ErrLeaseNotFound) {
			if etcdSession != nil {
				err = etcdSession.Close()
				logutil.Logger(logCtx).Info("etcd session encounters the error of lease not found, closes it", zap.Error(err))
			}
			continue
		}

		elec := concurrency.NewElection(etcdSession, m.key)
		err = elec.Campaign(ctx, m.id)
		if err != nil {
			logutil.Logger(logCtx).Info("failed to campaign", zap.Error(err))
			continue
		}

		ownerKey, err := GetOwnerInfo(ctx, logCtx, elec, m.id)
		if err != nil {
			continue
		}

		m.toBeOwner(elec)
		m.watchOwner(ctx, etcdSession, ownerKey)
		m.RetireOwner()

		metrics.CampaignOwnerCounter.WithLabelValues(m.prompt, metrics.NoLongerOwner).Inc()
		logutil.Logger(logCtx).Warn("is not the owner")
	}
}

func (m *ownerManager) revokeSession(logPrefix string, leaseID clientv3.LeaseID) {
	// Revoke the session lease.
	// If revoke takes longer than the ttl, lease is expired anyway.
	cancelCtx, cancel := context.WithTimeout(context.Background(),
		time.Duration(ManagerSessionTTL)*time.Second)
	_, err := m.etcdCli.Revoke(cancelCtx, leaseID)
	cancel()
	logutil.Logger(m.logCtx).Info("revoke session", zap.Error(err))
}

// GetOwnerID implements Manager.GetOwnerID interface.
func (m *ownerManager) GetOwnerID(ctx context.Context) (string, error) {
	resp, err := m.etcdCli.Get(ctx, m.key, clientv3.WithFirstCreate()...)
	if err != nil {
		return "", errors.Trace(err)
	}
	if len(resp.Kvs) == 0 {
		return "", concurrency.ErrElectionNoLeader
	}
	return string(resp.Kvs[0].Value), nil
}

// GetOwnerInfo gets the owner information.
func GetOwnerInfo(ctx, logCtx context.Context, elec *concurrency.Election, id string) (string, error) {
	resp, err := elec.Leader(ctx)
	if err != nil {
		// If no leader elected currently, it returns ErrElectionNoLeader.
		logutil.Logger(logCtx).Info("failed to get leader", zap.Error(err))
		return "", errors.Trace(err)
	}
	ownerID := string(resp.Kvs[0].Value)
	logutil.Logger(logCtx).Info("get owner", zap.String("ownerID", ownerID))
	if ownerID != id {
		logutil.Logger(logCtx).Warn("is not the owner")
		return "", errors.New("ownerInfoNotMatch")
	}

	return string(resp.Kvs[0].Key), nil
}

func (m *ownerManager) watchOwner(ctx context.Context, etcdSession *concurrency.Session, key string) {
	logPrefix := fmt.Sprintf("[%s] ownerManager %s watch owner key %v", m.prompt, m.id, key)
	logCtx := logutil.WithKeyValue(context.Background(), "owner info", logPrefix)
	logutil.BgLogger().Debug(logPrefix)
	watchCh := m.etcdCli.Watch(ctx, key)
	for {
		select {
		case resp, ok := <-watchCh:
			if !ok {
				metrics.WatchOwnerCounter.WithLabelValues(m.prompt, metrics.WatcherClosed).Inc()
				logutil.Logger(logCtx).Info("watcher is closed, no owner")
				return
			}
			if resp.Canceled {
				metrics.WatchOwnerCounter.WithLabelValues(m.prompt, metrics.Cancelled).Inc()
				logutil.Logger(logCtx).Info("watch canceled, no owner")
				return
			}

			for _, ev := range resp.Events {
				if ev.Type == mvccpb.DELETE {
					metrics.WatchOwnerCounter.WithLabelValues(m.prompt, metrics.Deleted).Inc()
					logutil.Logger(logCtx).Info("watch failed, owner is deleted")
					return
				}
			}
		case <-etcdSession.Done():
			metrics.WatchOwnerCounter.WithLabelValues(m.prompt, metrics.SessionDone).Inc()
			return
		case <-ctx.Done():
			metrics.WatchOwnerCounter.WithLabelValues(m.prompt, metrics.CtxDone).Inc()
			return
		}
	}
}

func init() {
	err := setManagerSessionTTL()
	if err != nil {
		logutil.BgLogger().Warn("set manager session TTL failed", zap.Error(err))
	}
}
