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
	// CampaignOwner campaigns the owner. It will start a background goroutine to
	// campaign owner in a loop, and when become or retire owner, it will call methods
	// of the listener.
	CampaignOwner(...int) error
	// CampaignCancel cancels one etcd campaign, it will also close the underlying
	// etcd session. After this method is called, the manager can be used to campaign
	// owner again.
	CampaignCancel()
	// BreakCampaignLoop breaks the campaign loop, related listener methods will
	// be called. The underlying etcd session the related campaign key will remain,
	// so if some instance is the owner before, after break and campaign again, it
	// will still be the owner.
	BreakCampaignLoop()
	// ResignOwner will resign and start a new election if it's the owner.
	ResignOwner(ctx context.Context) error
	// Close closes the manager, after close, no methods can be called.
	Close()
	// SetListener sets the listener, set before CampaignOwner.
	SetListener(listener Listener)
	// ForceToBeOwner restart the owner election and trying to be the new owner by
	// end campaigns of all candidates and start a new campaign in a single transaction.
	//
	// This method is only used during upgrade and try to make node of newer version
	// to be the DDL owner, to mitigate the issue https://github.com/pingcap/tidb/issues/54689,
	// current instance shouldn't call CampaignOwner before calling this method.
	// don't use it in other cases.
	//
	// Note: only one instance can call this method at a time, so you have to use
	// a distributed lock when there are multiple instances of new version TiDB trying
	// to be the owner. See runInBootstrapSession for where we lock it in DDL.
	ForceToBeOwner(ctx context.Context) error
}

const (
	keyOpDefaultTimeout = 5 * time.Second

	// WaitTimeOnForceOwner is the time to wait before or after force to be owner.
	WaitTimeOnForceOwner = 5 * time.Second
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
	elec           atomic.Pointer[concurrency.Election]
	sessionLease   *atomicutil.Int64
	wg             sync.WaitGroup
	campaignCancel context.CancelFunc

	listener Listener
	etcdSes  *concurrency.Session
}

// NewOwnerManager creates a new Manager.
func NewOwnerManager(ctx context.Context, etcdCli *clientv3.Client, prompt, id, key string) Manager {
	logPrefix := fmt.Sprintf("[%s] %s ownerManager %s", prompt, key, id)
	return &ownerManager{
		etcdCli:      etcdCli,
		id:           id,
		key:          key,
		ctx:          ctx,
		prompt:       prompt,
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

// Close implements Manager.Close interface.
func (m *ownerManager) Close() {
	// same as CampaignCancel
	m.CampaignCancel()
}

func (m *ownerManager) SetListener(listener Listener) {
	m.listener = listener
}

func (m *ownerManager) ForceToBeOwner(context.Context) error {
	logPrefix := fmt.Sprintf("[%s] %s", m.prompt, m.key)
	logutil.BgLogger().Info("force to be owner", zap.String("ownerInfo", logPrefix))
	if err := m.refreshSession(util2.NewSessionDefaultRetryCnt, ManagerSessionTTL); err != nil {
		return errors.Trace(err)
	}

	// due to issue https://github.com/pingcap/tidb/issues/54689, if the cluster
	// version before upgrade don't have fix, when retire owners runs on older version
	// and trying to be the new owner, it's possible that multiple owner exist at
	// the same time, it cannot be avoided completely, but we can use below 2 strategies
	// to mitigate this issue:
	//   1. when trying to be owner, we delete all existing owner related keys and
	//      put new key in a single txn, if we delete the key one by one, other node
	//      might become the owner, it will have more chances to trigger previous issue.
	//   2. sleep for a while before trying to be owner to make sure there is an owner in
	//      the cluster, and it has started watching. in the case of upgrade using
	//      tiup, tiup might restart current owner node to do rolling upgrade.
	//      before the restarted node force owner, another node might try to be
	//      the new owner too, it's still possible to trigger the issue. so we
	//      sleep a while to wait the cluster have a new owner and start watching.
	for i := 0; i < 3; i++ {
		// we need to sleep in every retry, as other TiDB nodes will start campaign
		// immediately after we delete their key.
		time.Sleep(WaitTimeOnForceOwner)
		if err := m.tryToBeOwnerOnce(); err != nil {
			logutil.Logger(m.logCtx).Warn("failed to retire owner on older version", zap.Error(err))
			continue
		}
		break
	}
	return nil
}

func (m *ownerManager) tryToBeOwnerOnce() error {
	lease := m.etcdSes.Lease()
	keyPrefix := m.key + "/"

	getResp, err := m.etcdCli.Get(m.ctx, keyPrefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	// modifications to the same key multiple times within a single transaction are
	// forbidden in etcd, so we cannot use delete by prefix and put in a single txn.
	// it will report "duplicate key given in txn request" error.
	// It's possible that other nodes put campaign keys between we get the keys and
	// the txn to put new key, we relay on the sleep before calling this method to
	// make sure all TiDBs have already put the key, and the distributed lock inside
	// bootstrap to make sure no concurrent ForceToBeOwner is called.
	txnOps := make([]clientv3.Op, 0, len(getResp.Kvs)+1)
	// below key structure is copied from Election.Campaign.
	campaignKey := fmt.Sprintf("%s%x", keyPrefix, lease)
	for _, kv := range getResp.Kvs {
		key := string(kv.Key)
		if key == campaignKey {
			// if below campaign failed, it will resign automatically, but if resign
			// also failed, the old key might already exist
			continue
		}
		txnOps = append(txnOps, clientv3.OpDelete(key))
	}
	txnOps = append(txnOps, clientv3.OpPut(campaignKey, m.id, clientv3.WithLease(lease)))
	_, err = m.etcdCli.Txn(m.ctx).Then(txnOps...).Commit()
	if err != nil {
		return errors.Trace(err)
	}
	// Campaign will wait until there is no key with smaller create-revision, either
	// current instance become owner or all the keys are deleted, in case other nodes
	// put keys in between previous get and txn, and makes current node never become
	// the owner, so we add a timeout to avoid blocking.
	ctx, cancel := context.WithTimeout(m.ctx, keyOpDefaultTimeout)
	defer cancel()
	elec := concurrency.NewElection(m.etcdSes, m.key)
	if err = elec.Campaign(ctx, m.id); err != nil {
		return errors.Trace(err)
	}

	// Campaign assumes that it's the only client managing the lifecycle of the campaign
	// key, it only checks whether there are any keys with smaller create-revision,
	// so it will also return when all the campaign keys are deleted by other TiDB
	// instances when the distributed lock has failed to keep alive and another TiDB
	// get the lock. It's a quite rare case, and the TiDB must be of newer version
	// which has the fix of the issue, so it's ok to return now.
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
func (m *ownerManager) CampaignOwner(withTTL ...int) error {
	ttl := ManagerSessionTTL
	if len(withTTL) == 1 {
		ttl = withTTL[0]
	}
	logPrefix := fmt.Sprintf("[%s] %s", m.prompt, m.key)
	if m.etcdSes == nil {
		logutil.BgLogger().Info("start campaign owner", zap.String("ownerInfo", logPrefix))
		if err := m.refreshSession(util2.NewSessionDefaultRetryCnt, ttl); err != nil {
			return errors.Trace(err)
		}
	} else {
		logutil.BgLogger().Info("start campaign owner with existing session",
			zap.String("ownerInfo", logPrefix),
			zap.String("lease", util2.FormatLeaseID(m.etcdSes.Lease())))
	}
	m.wg.Add(1)
	var campaignContext context.Context
	campaignContext, m.campaignCancel = context.WithCancel(m.ctx)
	go m.campaignLoop(campaignContext)
	return nil
}

// ResignOwner lets the owner start a new election.
func (m *ownerManager) ResignOwner(ctx context.Context) error {
	elec := m.elec.Load()
	if elec == nil {
		return errors.Errorf("This node is not a owner, can't be resigned")
	}

	childCtx, cancel := context.WithTimeout(ctx, keyOpDefaultTimeout)
	err := elec.Resign(childCtx)
	cancel()
	if err != nil {
		return errors.Trace(err)
	}

	logutil.Logger(m.logCtx).Warn("resign owner success")
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
	m.BreakCampaignLoop()
	m.closeSession()
}

func (m *ownerManager) BreakCampaignLoop() {
	if m.campaignCancel != nil {
		m.campaignCancel()
	}
	m.wg.Wait()
}

func (m *ownerManager) campaignLoop(campaignContext context.Context) {
	defer func() {
		m.campaignCancel()
		if r := recover(); r != nil {
			logutil.BgLogger().Error("recover panic", zap.String("prompt", m.prompt), zap.Any("error", r), zap.Stack("buffer"))
			metrics.PanicCounter.WithLabelValues(metrics.LabelDDLOwner).Inc()
		}
		m.wg.Done()
	}()

	logCtx := m.logCtx
	var err error
	leaseNotFoundCh := make(chan struct{})
	for {
		if err != nil {
			metrics.CampaignOwnerCounter.WithLabelValues(m.prompt, err.Error()).Inc()
		}

		select {
		case <-m.etcdSes.Done():
			logutil.Logger(logCtx).Info("etcd session done, refresh it")
			if err2 := m.refreshSession(util2.NewSessionRetryUnlimited, ManagerSessionTTL); err2 != nil {
				logutil.Logger(logCtx).Info("break campaign loop, refresh session failed", zap.Error(err2))
				return
			}
		case <-leaseNotFoundCh:
			logutil.Logger(logCtx).Info("meet lease not found error, refresh session")
			if err2 := m.refreshSession(util2.NewSessionRetryUnlimited, ManagerSessionTTL); err2 != nil {
				logutil.Logger(logCtx).Info("break campaign loop, refresh session failed", zap.Error(err2))
				return
			}
			leaseNotFoundCh = make(chan struct{})
		case <-campaignContext.Done():
			failpoint.Inject("MockDelOwnerKey", func(v failpoint.Value) {
				if v.(string) == "delOwnerKeyAndNotOwner" {
					logutil.Logger(logCtx).Info("mock break campaign and don't clear related info")
					return
				}
			})
			logutil.Logger(logCtx).Info("break campaign loop, context is done")
			return
		default:
		}
		// If the etcd server turns clocks forward，the following case may occur.
		// The etcd server deletes this session's lease ID, but etcd session doesn't find it.
		// In this time if we do the campaign operation, the etcd server will return ErrLeaseNotFound.
		if terror.ErrorEqual(err, rpctypes.ErrLeaseNotFound) {
			close(leaseNotFoundCh)
			err = nil
			continue
		}

		elec := concurrency.NewElection(m.etcdSes, m.key)
		err = elec.Campaign(campaignContext, m.id)
		if err != nil {
			logutil.Logger(logCtx).Info("failed to campaign", zap.Error(err))
			continue
		}

		ownerKey, currRev, err := GetOwnerKeyInfo(campaignContext, logCtx, m.etcdCli, m.key, m.id)
		if err != nil {
			continue
		}

		m.toBeOwner(elec)
		err = m.watchOwner(campaignContext, m.etcdSes, ownerKey, currRev)
		logutil.Logger(logCtx).Info("watch owner finished", zap.Error(err))
		m.RetireOwner()

		metrics.CampaignOwnerCounter.WithLabelValues(m.prompt, metrics.NoLongerOwner).Inc()
		logutil.Logger(logCtx).Info("is not the owner")
	}
}

func (m *ownerManager) closeSession() {
	if m.etcdSes != nil {
		if err := m.etcdSes.Close(); err != nil {
			logutil.Logger(m.logCtx).Info("etcd session close failed", zap.Error(err))
		}
		m.etcdSes = nil
	}
}

func (m *ownerManager) refreshSession(retryCnt, ttl int) error {
	m.closeSession()
	// Note: we must use manager's context to create session. If we use campaign
	// context and the context is cancelled, the created session cannot be closed
	// as session close depends on the context.
	// One drawback is that when you want to break the campaign loop, and the campaign
	// loop is refreshing the session, it might wait for a long time to return, it
	// should be fine as long as network is ok, and acceptable to wait when not.
	sess, err2 := util2.NewSession(m.ctx, m.logPrefix, m.etcdCli, retryCnt, ttl)
	if err2 != nil {
		return errors.Trace(err2)
	}
	m.etcdSes = sess
	m.sessionLease.Store(int64(m.etcdSes.Lease()))
	return nil
}

func (m *ownerManager) revokeSession(leaseID clientv3.LeaseID) {
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
	_, ownerID, _, _, _, err := getOwnerInfo(ctx, m.logCtx, m.etcdCli, m.key)
	return string(ownerID), errors.Trace(err)
}

func getOwnerInfo(ctx, logCtx context.Context, etcdCli *clientv3.Client, ownerPath string) (string, []byte, OpType, int64, int64, error) {
	var op OpType
	var resp *clientv3.GetResponse
	var err error
	for i := 0; i < 3; i++ {
		if err = ctx.Err(); err != nil {
			return "", nil, op, 0, 0, errors.Trace(err)
		}

		childCtx, cancel := context.WithTimeout(ctx, util.KeyOpDefaultTimeout)
		resp, err = etcdCli.Get(childCtx, ownerPath, clientv3.WithFirstCreate()...)
		cancel()
		if err == nil {
			break
		}
		logutil.Logger(logCtx).Info("etcd-cli get owner info failed", zap.String("key", ownerPath), zap.Int("retryCnt", i), zap.Error(err))
		time.Sleep(util.KeyOpRetryInterval)
	}
	if err != nil {
		logutil.Logger(logCtx).Warn("etcd-cli get owner info failed", zap.Error(err))
		return "", nil, op, 0, 0, errors.Trace(err)
	}
	if len(resp.Kvs) == 0 {
		return "", nil, op, 0, 0, concurrency.ErrElectionNoLeader
	}

	var ownerID []byte
	ownerID, op = splitOwnerValues(resp.Kvs[0].Value)
	logutil.Logger(logCtx).Info("get owner", zap.ByteString("owner key", resp.Kvs[0].Key),
		zap.ByteString("ownerID", ownerID), zap.Stringer("op", op))
	return string(resp.Kvs[0].Key), ownerID, op, resp.Header.Revision, resp.Kvs[0].ModRevision, nil
}

// GetOwnerKeyInfo gets the owner key and current revision.
func GetOwnerKeyInfo(
	ctx, logCtx context.Context,
	etcdCli *clientv3.Client,
	etcdKey, id string,
) (string, int64, error) {
	ownerKey, ownerID, _, currRevision, _, err := getOwnerInfo(ctx, logCtx, etcdCli, etcdKey)
	if err != nil {
		return "", 0, errors.Trace(err)
	}
	if string(ownerID) != id {
		logutil.Logger(logCtx).Warn("is not the owner")
		return "", 0, errors.New("ownerInfoNotMatch")
	}

	return ownerKey, currRevision, nil
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
	ownerKey, ownerID, currOp, _, modRevision, err := getOwnerInfo(ctx, m.logCtx, m.etcdCli, m.key)
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
	_, _, op, _, _, err := getOwnerInfo(ctx, logCtx, etcdCli, ownerPath)
	return op, errors.Trace(err)
}

// WatchOwnerForTest watches the ownerKey.
// This function is used to test watchOwner().
func WatchOwnerForTest(ctx context.Context, m Manager, etcdSession *concurrency.Session, key string, createRevison int64) error {
	if ownerManager, ok := m.(*ownerManager); ok {
		return ownerManager.watchOwner(ctx, etcdSession, key, createRevison)
	}
	return nil
}

func (m *ownerManager) watchOwner(ctx context.Context, etcdSession *concurrency.Session, key string, currRev int64) error {
	logPrefix := fmt.Sprintf("[%s] ownerManager %s watch owner key %v", m.prompt, m.id, key)
	logCtx := logutil.WithKeyValue(context.Background(), "owner info", logPrefix)
	logutil.BgLogger().Debug(logPrefix)
	// we need to watch the ownerKey since currRev + 1.
	watchCh := m.etcdCli.Watch(ctx, key, clientv3.WithRev(currRev+1))
	for {
		select {
		case resp, ok := <-watchCh:
			if !ok {
				metrics.WatchOwnerCounter.WithLabelValues(m.prompt, metrics.WatcherClosed).Inc()
				logutil.Logger(logCtx).Info("watcher is closed, no owner")
				return errors.Errorf("watcher is closed, key: %v", key)
			}
			if resp.Canceled {
				metrics.WatchOwnerCounter.WithLabelValues(m.prompt, metrics.Cancelled).Inc()
				logutil.Logger(logCtx).Info("watch canceled, no owner")
				return errors.Errorf("watch canceled, key: %v", key)
			}

			for _, ev := range resp.Events {
				if ev.Type == mvccpb.DELETE {
					metrics.WatchOwnerCounter.WithLabelValues(m.prompt, metrics.Deleted).Inc()
					logutil.Logger(logCtx).Info("watch failed, owner is deleted")
					return nil
				}
			}
		case <-etcdSession.Done():
			metrics.WatchOwnerCounter.WithLabelValues(m.prompt, metrics.SessionDone).Inc()
			return nil
		case <-ctx.Done():
			metrics.WatchOwnerCounter.WithLabelValues(m.prompt, metrics.CtxDone).Inc()
			return nil
		}
	}
}

func init() {
	err := setManagerSessionTTL()
	if err != nil {
		logutil.BgLogger().Warn("set manager session TTL failed", zap.Error(err))
	}
}

// AcquireDistributedLock creates a mutex with ETCD client, and returns a mutex release function.
func AcquireDistributedLock(
	ctx context.Context,
	cli *clientv3.Client,
	key string,
	ttlInSec int,
) (release func(), err error) {
	se, err := concurrency.NewSession(cli, concurrency.WithTTL(ttlInSec))
	if err != nil {
		return nil, err
	}
	mu := concurrency.NewMutex(se, key)
	maxRetryCnt := 10
	err = util2.RunWithRetry(maxRetryCnt, util2.RetryInterval, func() (bool, error) {
		err = mu.Lock(ctx)
		if err != nil {
			return true, err
		}
		return false, nil
	})
	failpoint.Inject("mockAcquireDistLockFailed", func() {
		err = errors.Errorf("requested lease not found")
	})
	if err != nil {
		err1 := se.Close()
		if err1 != nil {
			logutil.Logger(ctx).Warn("close session error", zap.Error(err1))
		}
		return nil, err
	}
	logutil.Logger(ctx).Info("acquire distributed lock success", zap.String("key", key))
	return func() {
		err = mu.Unlock(ctx)
		if err != nil {
			logutil.Logger(ctx).Warn("release distributed lock error", zap.Error(err), zap.String("key", key))
		} else {
			logutil.Logger(ctx).Info("release distributed lock success", zap.String("key", key))
		}
		err = se.Close()
		if err != nil {
			logutil.Logger(ctx).Warn("close session error", zap.Error(err))
		}
	}, nil
}

// ListenersWrapper is a list of listeners.
// A way to broadcast events to multiple listeners.
type ListenersWrapper struct {
	listeners []Listener
}

// OnBecomeOwner broadcasts the OnBecomeOwner event to all listeners.
func (ol *ListenersWrapper) OnBecomeOwner() {
	for _, l := range ol.listeners {
		l.OnBecomeOwner()
	}
}

// OnRetireOwner broadcasts the OnRetireOwner event to all listeners.
func (ol *ListenersWrapper) OnRetireOwner() {
	for _, l := range ol.listeners {
		l.OnRetireOwner()
	}
}

// NewListenersWrapper creates a new OwnerListeners.
func NewListenersWrapper(listeners ...Listener) *ListenersWrapper {
	return &ListenersWrapper{listeners: listeners}
}
