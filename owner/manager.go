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
	"fmt"
	"math"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/terror"
	log "github.com/sirupsen/logrus"
	goctx "golang.org/x/net/context"
)

const (
	newSessionRetryInterval = 200 * time.Millisecond
	logIntervalCnt          = int(3 * time.Second / newSessionRetryInterval)
)

// Manager is used to campaign the owner and manage the owner information.
type Manager interface {
	// ID returns the ID of the manager.
	ID() string
	// IsOwner returns whether the ownerManager is the owner.
	IsOwner() bool
	// SetOwner sets whether the ownerManager is the owner.
	SetOwner(isOwner bool)
	// GetOwnerID gets the owner ID.
	GetOwnerID(ctx goctx.Context) (string, error)
	// CampaignOwner campaigns the owner.
	CampaignOwner(ctx goctx.Context) error
	// Cancel cancels this etcd ownerManager campaign.
	Cancel()
}

const (
	// NewSessionDefaultRetryCnt is the default retry times when create new session.
	NewSessionDefaultRetryCnt = 3
	// NewSessionRetryUnlimited is the unlimited retry times when create new session.
	NewSessionRetryUnlimited = math.MaxInt64
)

// ownerManager represents the structure which is used for electing owner.
type ownerManager struct {
	owner   int32
	id      string // id is the ID of the manager.
	key     string
	prompt  string
	etcdCli *clientv3.Client
	cancel  goctx.CancelFunc
}

// NewOwnerManager creates a new Manager.
func NewOwnerManager(etcdCli *clientv3.Client, prompt, id, key string, cancel goctx.CancelFunc) Manager {
	return &ownerManager{
		etcdCli: etcdCli,
		id:      id,
		key:     key,
		prompt:  prompt,
		cancel:  cancel,
	}
}

// ID implements Manager.ID interface.
func (m *ownerManager) ID() string {
	return m.id
}

// IsOwner implements Manager.IsOwner interface.
func (m *ownerManager) IsOwner() bool {
	return atomic.LoadInt32(&m.owner) == 1
}

// SetOwner implements Manager.SetOwner interface.
func (m *ownerManager) SetOwner(isOwner bool) {
	if isOwner {
		atomic.StoreInt32(&m.owner, 1)
	} else {
		atomic.StoreInt32(&m.owner, 0)
	}
}

// Cancel implements Manager.Cancel interface.
func (m *ownerManager) Cancel() {
	m.cancel()
}

// ManagerSessionTTL is the etcd session's TTL in seconds. It's exported for testing.
var ManagerSessionTTL = 60

// setManagerSessionTTL sets the ManagerSessionTTL value, it's used for testing.
func setManagerSessionTTL() error {
	ttlStr := os.Getenv("tidb_manager_ttl")
	if len(ttlStr) == 0 {
		return nil
	}
	ttl, err := strconv.Atoi(ttlStr)
	if err != nil {
		return errors.Trace(err)
	}
	ManagerSessionTTL = ttl
	return nil
}

// NewSession creates a new etcd session.
func NewSession(ctx goctx.Context, logPrefix string, etcdCli *clientv3.Client, retryCnt, ttl int) (*concurrency.Session, error) {
	var err error
	var etcdSession *concurrency.Session
	failedCnt := 0
	for i := 0; i < retryCnt; i++ {
		if isContextDone(ctx) {
			return etcdSession, errors.Trace(ctx.Err())
		}

		etcdSession, err = concurrency.NewSession(etcdCli,
			concurrency.WithTTL(ttl), concurrency.WithContext(ctx))
		if err == nil {
			break
		}
		if failedCnt%logIntervalCnt == 0 {
			log.Warnf("%s failed to new session, err %v", logPrefix, err)
		}
		time.Sleep(newSessionRetryInterval)
		failedCnt++
	}
	return etcdSession, errors.Trace(err)
}

// CampaignOwner implements Manager.CampaignOwner interface.
func (m *ownerManager) CampaignOwner(ctx goctx.Context) error {
	logPrefix := fmt.Sprintf("[%s] %s", m.prompt, m.key)
	session, err := NewSession(ctx, logPrefix, m.etcdCli, NewSessionDefaultRetryCnt, ManagerSessionTTL)
	if err != nil {
		return errors.Trace(err)
	}
	cancelCtx, _ := goctx.WithCancel(ctx)
	go m.campaignLoop(cancelCtx, session)
	return nil
}

func (m *ownerManager) campaignLoop(ctx goctx.Context, etcdSession *concurrency.Session) {
	logPrefix := fmt.Sprintf("[%s] %s ownerManager %s", m.prompt, m.key, m.id)
	var err error
	for {
		select {
		case <-etcdSession.Done():
			log.Infof("%s etcd session is done, creates a new one", logPrefix)
			leaseID := etcdSession.Lease()
			etcdSession, err = NewSession(ctx, logPrefix, m.etcdCli, NewSessionRetryUnlimited, ManagerSessionTTL)
			if err != nil {
				log.Infof("%s break campaign loop, NewSession err %v", logPrefix, err)
				m.revokeSession(logPrefix, leaseID)
				return
			}
		case <-ctx.Done():
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
				log.Infof("%s etcd session encounters the error of lease not found, closes it err %s", logPrefix, err)
			}
			continue
		}

		elec := concurrency.NewElection(etcdSession, m.key)
		err = elec.Campaign(ctx, m.id)
		if err != nil {
			log.Infof("%s failed to campaign, err %v", logPrefix, err)
			continue
		}

		ownerKey, err := GetOwnerInfo(ctx, elec, logPrefix, m.id)
		if err != nil {
			continue
		}
		m.SetOwner(true)
		m.watchOwner(ctx, etcdSession, ownerKey)
		m.SetOwner(false)
	}
}

func (m *ownerManager) revokeSession(logPrefix string, leaseID clientv3.LeaseID) {
	// Revoke the session lease.
	// If revoke takes longer than the ttl, lease is expired anyway.
	cancelCtx, cancel := goctx.WithTimeout(goctx.Background(),
		time.Duration(ManagerSessionTTL)*time.Second)
	_, err := m.etcdCli.Revoke(cancelCtx, leaseID)
	cancel()
	log.Infof("%s break campaign loop, revoke err %v", logPrefix, err)
}

// GetOwnerID implements Manager.GetOwnerID interface.
func (m *ownerManager) GetOwnerID(ctx goctx.Context) (string, error) {
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
func GetOwnerInfo(ctx goctx.Context, elec *concurrency.Election, logPrefix, id string) (string, error) {
	resp, err := elec.Leader(ctx)
	if err != nil {
		// If no leader elected currently, it returns ErrElectionNoLeader.
		log.Infof("%s failed to get leader, err %v", logPrefix, err)
		return "", errors.Trace(err)
	}
	ownerID := string(resp.Kvs[0].Value)
	log.Infof("%s, owner is %v", logPrefix, ownerID)
	if ownerID != id {
		log.Warnf("%s isn't the owner", logPrefix)
		return "", errors.New("ownerInfoNotMatch")
	}

	return string(resp.Kvs[0].Key), nil
}

func (m *ownerManager) watchOwner(ctx goctx.Context, etcdSession *concurrency.Session, key string) {
	logPrefix := fmt.Sprintf("[%s] ownerManager %s watch owner key %v", m.prompt, m.id, key)
	log.Debugf("%s", logPrefix)
	watchCh := m.etcdCli.Watch(ctx, key)
	for {
		select {
		case resp := <-watchCh:
			if resp.Canceled {
				log.Infof("%s failed, no owner", logPrefix)
				return
			}

			for _, ev := range resp.Events {
				if ev.Type == mvccpb.DELETE {
					log.Infof("%s failed, owner is deleted", logPrefix)
					return
				}
			}
		case <-etcdSession.Done():
			return
		case <-ctx.Done():
			return
		}
	}
}

func init() {
	err := setManagerSessionTTL()
	if err != nil {
		log.Warnf("set manager session TTL failed %v", err)
	}
}

func isContextDone(ctx goctx.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
	}
	return false
}
