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
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/terror"
	util2 "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

// Listener is used to listen the ownerManager's owner state.
type Listener interface {
	OnBecomeOwner()
	OnRetireOwner()
}

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
	// SetOwnerOpValue updates the owner op value.
	SetOwnerOpValue(ctx context.Context, op OpType) error
	// CampaignOwner campaigns the owner.
	CampaignOwner(...int) error
	// ResignOwner lets the owner start a new election.
	ResignOwner(ctx context.Context) error
	// Cancel cancels this etcd ownerManager.
	Cancel()
	// RequireOwner requires the ownerManager is owner.
	RequireOwner(ctx context.Context) error
	// CampaignCancel cancels one etcd campaign
	CampaignCancel()
	// SetListener sets the listener, set before CampaignOwner.
	SetListener(listener Listener)
}

const (
	keyOpDefaultTimeout = 5 * time.Second
)

// OpType is the owner key value operation type.
type OpType byte

// List operation of types.
const (
	OpNone               OpType = 0
	OpSyncUpgradingState OpType = 1
)

// String implements fmt.Stringer interface.
func (ot OpType) String() string {
	switch ot {
	case OpSyncUpgradingState:
		return "sync upgrading state"
	default:
		return "none"
	}
}

// IsSyncedUpgradingState represents whether the upgrading state is synchronized.
func (ot OpType) IsSyncedUpgradingState() bool {
	return ot == OpSyncUpgradingState
}

// DDLOwnerChecker is used to check whether tidb is owner.
type DDLOwnerChecker interface {
	// IsOwner returns whether the ownerManager is the owner.
	IsOwner() bool
}

// ownerManager represents the structure which is used for electing owner.
type ownerManager struct {
	id             string // id is the ID of the manager.
	key            string
	ctx            context.Context
	prompt         string
	logPrefix      string
	logCtx         context.Context
	etcdCli        *clientv3.Client
	cancel         context.CancelFunc
	elec           atomic.Pointer[concurrency.Election]
	sessionLease   *atomicutil.Int64
	wg             sync.WaitGroup
	campaignCancel context.CancelFunc

	listener Listener
}

// NewOwnerManager creates a new Manager.
func NewOwnerManager(ctx context.Context, etcdCli *clientv3.Client, prompt, id, key string) Manager {
	logPrefix := fmt.Sprintf("[%s] %s ownerManager %s", prompt, key, id)
	ctx, cancelFunc := context.WithCancel(ctx)
	return &ownerManager{
		etcdCli:      etcdCli,
		id:           id,
		key:          key,
		ctx:          ctx,
		prompt:       prompt,
		cancel:       cancelFunc,
		logPrefix:    logPrefix,
		logCtx:       logutil.WithKeyValue(context.Background(), "owner info", logPrefix),
		sessionLease: atomicutil.NewInt64(0),
	}
}

// ID implements Manager.ID interface.
func (m *ownerManager) ID() string {
	return m.id
}

// IsOwner implements Manager.IsOwner interface.
func (m *ownerManager) IsOwner() bool {
	return m.elec.Load() != nil
}

// Cancel implements Manager.Cancel interface.
func (m *ownerManager) Cancel() {
	m.cancel()
	m.wg.Wait()
}

// RequireOwner implements Manager.RequireOwner interface.
func (*ownerManager) RequireOwner(_ context.Context) error {
	return nil
}

func (m *ownerManager) SetListener(listener Listener) {
	m.listener = listener
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
func (m *ownerManager) CampaignOwner(withTTL ...int) error {
	ttl := ManagerSessionTTL
	if len(withTTL) == 1 {
		ttl = withTTL[0]
	}
	logPrefix := fmt.Sprintf("[%s] %s", m.prompt, m.key)
	logutil.BgLogger().Info("start campaign owner", zap.String("ownerInfo", logPrefix))
	session, err := util2.NewSession(m.ctx, logPrefix, m.etcdCli, util2.NewSessionDefaultRetryCnt, ttl)
	if err != nil {
		return errors.Trace(err)
	}
	m.sessionLease.Store(int64(session.Lease()))
	m.wg.Add(1)
	go m.campaignLoop(session)
	return nil
}

// ResignOwner lets the owner start a new election.
func (m *ownerManager) ResignOwner(ctx context.Context) error {
	elec := m.elec.Load()
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
	m.elec.Store(elec)
	logutil.Logger(m.logCtx).Info("become owner")
	if m.listener != nil {
		m.listener.OnBecomeOwner()
	}
}

// RetireOwner make the manager to be a not owner.
func (m *ownerManager) RetireOwner() {
	m.elec.Store(nil)
	logutil.Logger(m.logCtx).Info("retire owner")
	if m.listener != nil {
		m.listener.OnRetireOwner()
	}
}

// CampaignCancel implements Manager.CampaignCancel interface.
func (m *ownerManager) CampaignCancel() {
	m.campaignCancel()
	m.wg.Wait()
}

func (m *ownerManager) campaignLoop(etcdSession *concurrency.Session) {
	var campaignContext context.Context
	campaignContext, m.campaignCancel = context.WithCancel(m.ctx)
	defer func() {
		m.campaignCancel()
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
			etcdSession, err = util2.NewSession(campaignContext, logPrefix, m.etcdCli, util2.NewSessionRetryUnlimited, ManagerSessionTTL)
			if err != nil {
				logutil.Logger(logCtx).Info("break campaign loop, NewSession failed", zap.Error(err))
				m.revokeSession(logPrefix, leaseID)
				return
			}
			m.sessionLease.Store(int64(etcdSession.Lease()))
		case <-campaignContext.Done():
			failpoint.Inject("MockDelOwnerKey", func(v failpoint.Value) {
				if v.(string) == "delOwnerKeyAndNotOwner" {
					logutil.Logger(logCtx).Info("mock break campaign and don't clear related info")
					return
				}
			})
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
		err = elec.Campaign(campaignContext, m.id)
		if err != nil {
			logutil.Logger(logCtx).Info("failed to campaign", zap.Error(err))
			continue
		}

		ownerKey, err := GetOwnerKey(campaignContext, logCtx, m.etcdCli, m.key, m.id)
		if err != nil {
			continue
		}

		m.toBeOwner(elec)
		m.watchOwner(campaignContext, etcdSession, ownerKey)
		m.RetireOwner()

		metrics.CampaignOwnerCounter.WithLabelValues(m.prompt, metrics.NoLongerOwner).Inc()
		logutil.Logger(logCtx).Warn("is not the owner")
	}
}

func (m *ownerManager) revokeSession(_ string, leaseID clientv3.LeaseID) {
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
	_, ownerID, _, _, err := getOwnerInfo(ctx, m.logCtx, m.etcdCli, m.key)
	return string(ownerID), errors.Trace(err)
}

func getOwnerInfo(ctx, logCtx context.Context, etcdCli *clientv3.Client, ownerPath string) (string, []byte, OpType, int64, error) {
	var op OpType
	var resp *clientv3.GetResponse
	var err error
	for i := 0; i < 3; i++ {
		if err = ctx.Err(); err != nil {
			return "", nil, op, 0, errors.Trace(err)
		}

		childCtx, cancel := context.WithTimeout(ctx, util.KeyOpDefaultTimeout)
		resp, err = etcdCli.Get(childCtx, ownerPath, clientv3.WithFirstCreate()...)
		cancel()
		if err == nil {
			break
		}
		logutil.BgLogger().Info("etcd-cli get owner info failed", zap.String("category", "ddl"), zap.String("key", ownerPath), zap.Int("retryCnt", i), zap.Error(err))
		time.Sleep(util.KeyOpRetryInterval)
	}
	if err != nil {
		logutil.Logger(logCtx).Warn("etcd-cli get owner info failed", zap.Error(err))
		return "", nil, op, 0, errors.Trace(err)
	}
	if len(resp.Kvs) == 0 {
		return "", nil, op, 0, concurrency.ErrElectionNoLeader
	}

	var ownerID []byte
	ownerID, op = splitOwnerValues(resp.Kvs[0].Value)
	logutil.Logger(logCtx).Info("get owner", zap.ByteString("owner key", resp.Kvs[0].Key),
		zap.ByteString("ownerID", ownerID), zap.Stringer("op", op))
	return string(resp.Kvs[0].Key), ownerID, op, resp.Kvs[0].ModRevision, nil
}

// GetOwnerKey gets the owner key information.
func GetOwnerKey(ctx, logCtx context.Context, etcdCli *clientv3.Client, etcdKey, id string) (string, error) {
	ownerKey, ownerID, _, _, err := getOwnerInfo(ctx, logCtx, etcdCli, etcdKey)
	if err != nil {
		return "", errors.Trace(err)
	}
	if string(ownerID) != id {
		logutil.Logger(logCtx).Warn("is not the owner")
		return "", errors.New("ownerInfoNotMatch")
	}

	return ownerKey, nil
}

func splitOwnerValues(val []byte) ([]byte, OpType) {
	vals := bytes.Split(val, []byte("_"))
	var op OpType
	if len(vals) == 2 {
		op = OpType(vals[1][0])
	}
	return vals[0], op
}

func joinOwnerValues(vals ...[]byte) []byte {
	return bytes.Join(vals, []byte("_"))
}

// SetOwnerOpValue implements Manager.SetOwnerOpValue interface.
func (m *ownerManager) SetOwnerOpValue(ctx context.Context, op OpType) error {
	// owner don't change.
	ownerKey, ownerID, currOp, modRevision, err := getOwnerInfo(ctx, m.logCtx, m.etcdCli, m.key)
	if err != nil {
		return errors.Trace(err)
	}
	if currOp == op {
		logutil.Logger(m.logCtx).Info("set owner op is the same as the original, so do nothing.", zap.Stringer("op", op))
		return nil
	}
	if string(ownerID) != m.id {
		return errors.New("ownerInfoNotMatch")
	}
	newOwnerVal := joinOwnerValues(ownerID, []byte{byte(op)})

	failpoint.Inject("MockDelOwnerKey", func(v failpoint.Value) {
		if valStr, ok := v.(string); ok {
			if err := mockDelOwnerKey(valStr, ownerKey, m); err != nil {
				failpoint.Return(err)
			}
		}
	})

	leaseOp := clientv3.WithLease(clientv3.LeaseID(m.sessionLease.Load()))
	resp, err := m.etcdCli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(ownerKey), "=", modRevision)).
		Then(clientv3.OpPut(ownerKey, string(newOwnerVal), leaseOp)).
		Commit()
	if err == nil && !resp.Succeeded {
		err = errors.New("put owner key failed, cmp is false")
	}
	logutil.BgLogger().Info("set owner op value", zap.String("owner key", ownerKey), zap.ByteString("ownerID", ownerID),
		zap.Stringer("old Op", currOp), zap.Stringer("op", op), zap.Error(err))
	metrics.WatchOwnerCounter.WithLabelValues(m.prompt, metrics.PutValue+"_"+metrics.RetLabel(err)).Inc()
	return errors.Trace(err)
}

// GetOwnerOpValue gets the owner op value.
func GetOwnerOpValue(ctx context.Context, etcdCli *clientv3.Client, ownerPath, logPrefix string) (OpType, error) {
	// It's using for testing.
	if etcdCli == nil {
		return *mockOwnerOpValue.Load(), nil
	}

	logCtx := logutil.WithKeyValue(context.Background(), "owner info", logPrefix)
	_, _, op, _, err := getOwnerInfo(ctx, logCtx, etcdCli, ownerPath)
	return op, errors.Trace(err)
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
