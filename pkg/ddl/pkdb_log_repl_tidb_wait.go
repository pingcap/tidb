package ddl

import (
	"context"
	"crypto/tls"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/logreplicationpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/redact"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/rangetask"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/constants"
	"github.com/tikv/pd/client/opt"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const pkdbDefaultReadStoreConcurrency = 16

var (
	// tidbWaitJobWatchRetryInterval is used when the etcd watch needs to be
	// recreated. Tests can shorten it to speed up retry loops.
	tidbWaitJobWatchRetryInterval = 3 * time.Second
	// tidbWaitJobAfterInitWatch is a test hook called after initWatch computes the
	// next revision and before the first etcd watch is created.
	tidbWaitJobAfterInitWatch func()
	// tidbWaitJobAfterCreateWatch is a test hook called after creating or renewing
	// the etcd watch. It is guarded by `intest.InTest`, so it does not affect
	// production builds.
	tidbWaitJobAfterCreateWatch func()
)

func pkdbPDSecurityOption() pd.SecurityOption {
	cfg := config.GetGlobalConfig()
	return pd.SecurityOption{
		CAPath:   cfg.Security.ClusterSSLCA,
		CertPath: cfg.Security.ClusterSSLCert,
		KeyPath:  cfg.Security.ClusterSSLKey,
	}
}

func pkdbTLSConfig() (*tls.Config, error) {
	security := config.GetGlobalConfig().Security.ClusterSecurity()
	return security.ToTLSConfig()
}

func pkdbDialOptions() ([]grpc.DialOption, error) {
	tlsCfg, err := pkdbTLSConfig()
	if err != nil {
		return nil, err
	}

	var transportCreds credentials.TransportCredentials = insecure.NewCredentials()
	if tlsCfg != nil {
		transportCreds = credentials.NewTLS(tlsCfg)
	}
	return []grpc.DialOption{
		grpc.WithTransportCredentials(transportCreds),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)),
	}, nil
}

// tidbWaitJob represents a log replication action that need to use tidb-server's
// capability to wait something happens. The log replication manager in PD uses etcd
// to communicate with tidb-server to send and receive these jobs.
type tidbWaitJob interface {
	// reqKey returns the etcd key where PD writes the request for this job. Should
	// have the common prefix PkdbTiDBWaitReqPrefix.
	reqKey() string
	// respKey returns the etcd key where tidb-server writes back the response for
	// this job. Should have the common prefix PkdbTiDBWaitRespPrefix.
	respKey() string
	getVersion([]byte) uint64
	// checkAndWait checks if there is a new job to process compared to
	// lastFinishedVersion, if so, it processes the job. It returns the version of
	// newJobBytes
	checkAndWait(
		ctx context.Context,
		newJobBytes []byte,
		lastFinishedVersion uint64,
		store kv.Storage,
		etcdCli *clientv3.Client,
	) (version uint64, err error)
}

type waitFlashback struct{}

var _ tidbWaitJob = &waitFlashback{}

func (w waitFlashback) reqKey() string {
	return constants.PkdbWaitFlashbackReq
}

func (w waitFlashback) respKey() string {
	return constants.PkdbWaitFlashbackResp
}

func (w waitFlashback) getVersion(bs []byte) uint64 {
	tmp := &pdpb.ReplicaWaitFlashback{}
	if err := tmp.Unmarshal(bs); err != nil {
		logutil.BgLogger().Error("waitFlashback failed to unmarshal request to get version", zap.Error(err))
		return 0
	}
	return tmp.GetVersion()
}

func (w waitFlashback) checkAndWait(
	ctx context.Context,
	newJobBytes []byte,
	lastFinishedVersion uint64,
	store kv.Storage,
	_ *clientv3.Client,
) (version uint64, err error) {
	curr := &pdpb.ReplicaWaitFlashback{}
	if err = curr.Unmarshal(newJobBytes); err != nil {
		return 0, errors.Errorf("waitFlashback failed to unmarshal request: %v", err)
	}
	version = curr.GetVersion()
	if version <= lastFinishedVersion {
		return version, nil
	}

	tikvStore, ok := store.(tikv.Storage)
	if !ok {
		return 0, errors.Errorf("waitFlashback requires tikv.Storage, got %T", store)
	}

	logutil.BgLogger().Info("start to flashback cluster",
		zap.Uint64("flashback TS", curr.FlashbackTs),
		zap.Uint64("start TS", curr.StartTs),
		zap.Uint64("commit TS", curr.CommitTs))
	r := model.KeyRange{
		StartKey: []byte{0},
		EndKey:   nil,
	}
	// TODO(lance6716): Flashback is an all-region operation (range task over the
	// whole keyspace). On clusters with huge region counts it can be extremely
	// expensive and may increase memory usage (e.g. via region cache growth).
	// Keep this in mind when adding any additional per-region bookkeeping here.
	err = flashbackToVersion(ctx, store,
		func(ctx context.Context, r tikvstore.KeyRange) (rangetask.TaskStat, error) {
			stats, err := SendPrepareFlashbackToVersionRPC(
				ctx, tikvStore, curr.FlashbackTs, curr.StartTs, r,
			)
			return stats, err
		}, r.StartKey, r.EndKey)
	if err != nil {
		return version, errors.Trace(err)
	}

	err = flashbackToVersion(ctx, store,
		func(ctx context.Context, r tikvstore.KeyRange) (rangetask.TaskStat, error) {
			stats, err := SendFlashbackToVersionRPC(
				ctx, tikvStore, curr.FlashbackTs, curr.StartTs, curr.CommitTs, r,
			)
			return stats, err
		}, r.StartKey, r.EndKey)
	return version, errors.Trace(err)
}

type saveRaftIndex struct{}

func (s saveRaftIndex) reqKey() string {
	return constants.PkdbSaveRaftIndexReq
}

func (s saveRaftIndex) respKey() string {
	return constants.PkdbSaveRaftIndexResp
}

func (s saveRaftIndex) getVersion(bs []byte) uint64 {
	tmp := &pdpb.ReplicaSaveRaftProgress{}
	if err := tmp.Unmarshal(bs); err != nil {
		logutil.BgLogger().Error("saveRaftIndex failed to unmarshal request to get version", zap.Error(err))
		return 0
	}
	return tmp.GetVersion()
}

func (s saveRaftIndex) checkAndWait(
	ctx context.Context,
	newJobBytes []byte,
	lastFinishedVersion uint64,
	store kv.Storage,
	_ *clientv3.Client,
) (version uint64, err error) {
	curr := &pdpb.ReplicaSaveRaftProgress{}
	if err = curr.Unmarshal(newJobBytes); err != nil {
		return 0, errors.Errorf("saveRaftIndex failed to unmarshal request: %v", err)
	}
	version = curr.GetVersion()
	if version <= lastFinishedVersion {
		return version, nil
	}
	raftProgressID := curr.GetRaftProgressId()

	storeWithPD, ok := store.(kv.StorageWithPD)
	if !ok {
		return 0, errors.Errorf("saveRaftIndex requires kv.StorageWithPD, got %T", store)
	}
	pdCli := storeWithPD.GetPDClient()
	if pdCli == nil {
		return 0, errors.New("saveRaftIndex got nil pd client")
	}

	physical, logical, err := pdCli.GetTS(ctx)
	if err != nil {
		return 0, err
	}
	maxTS := oracle.ComposeTS(physical, logical)

	logutil.BgLogger().Info("start saveRaftIndex",
		zap.Uint64("version", version),
		zap.String("raftProgressID", raftProgressID),
		zap.Uint64("maxTS", maxTS),
		zap.Int("storeConcurrency", pkdbDefaultReadStoreConcurrency),
	)

	savedStates, err := saveStoreRegionProgresses(ctx, pdCli, raftProgressID, maxTS)
	if err != nil {
		return 0, err
	}

	logutil.BgLogger().Info("finish saveRaftIndex",
		zap.Uint64("version", version),
		zap.String("raftProgressID", raftProgressID),
		zap.Uint64("savedStates", savedStates),
		zap.Uint64("maxTS", maxTS),
	)
	return version, nil
}

type loadRaftIndex struct{}

func (l loadRaftIndex) reqKey() string {
	return constants.PkdbLoadRaftIndexReq
}

func (l loadRaftIndex) respKey() string {
	return constants.PkdbLoadRaftIndexResp
}

func (l loadRaftIndex) getVersion(bs []byte) uint64 {
	tmp := &pdpb.ReplicaLoadRaftProgress{}
	if err := tmp.Unmarshal(bs); err != nil {
		logutil.BgLogger().Error("loadRaftIndex failed to unmarshal request to get version", zap.Error(err))
		return 0
	}
	return tmp.GetVersion()
}

func (l loadRaftIndex) checkAndWait(ctx context.Context, newJobBytes []byte, lastFinishedVersion uint64, _ kv.Storage, _ *clientv3.Client) (version uint64, err error) {
	curr := &pdpb.ReplicaLoadRaftProgress{}
	if err = curr.Unmarshal(newJobBytes); err != nil {
		return 0, errors.Errorf("loadRaftIndex failed to unmarshal request: %v", err)
	}
	version = curr.GetVersion()
	if version <= lastFinishedVersion {
		return version, nil
	}
	raftProgressID := curr.GetRaftProgressId()

	pdAddrs := curr.GetNewPrimaryPdAddrs()
	if len(pdAddrs) == 0 {
		return 0, errors.New("loadRaftIndex got empty new primary pd addrs")
	}

	primaryPDCli, err := pd.NewClientWithContext(ctx, pdAddrs, pkdbPDSecurityOption())
	if err != nil {
		return 0, err
	}
	defer primaryPDCli.Close()

	cfg := config.GetGlobalConfig()
	cli, err := rawkv.NewClient(
		ctx,
		strings.Split(cfg.Path, ","),
		cfg.Security.ClusterSecurity(),
	)
	if err != nil {
		return 0, err
	}
	defer cli.Close()

	logutil.BgLogger().Info("start loadRaftIndex",
		zap.Uint64("version", version),
		zap.String("raftProgressID", raftProgressID),
		zap.Strings("newPrimaryPdAddrs", pdAddrs),
	)

	states, err := loadStoreRegionProgresses(ctx, primaryPDCli, raftProgressID)
	if err != nil {
		return 0, err
	}

	g, gCtx := util.NewErrorGroupWithRecoverWithCtx(ctx)
	g.SetLimit(pkdbDefaultReadStoreConcurrency)
	for _, state := range states {
		state := state
		g.Go(func() error {
			if err := cli.UpdateLogReplState(gCtx, state); err != nil {
				return errors.Annotatef(
					err,
					"update log replication state for range [%s, %s)",
					redact.Key(state.GetStartKey()),
					redact.Key(state.GetEndKey()),
				)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return 0, err
	}

	logutil.BgLogger().Info("finish loadRaftIndex",
		zap.Uint64("version", version),
		zap.String("raftProgressID", raftProgressID),
		zap.Int("loadedStates", len(states)),
	)
	return version, nil
}

func saveStoreRegionProgresses(ctx context.Context, pdCli pd.Client, raftProgressID string, maxTS uint64) (uint64, error) {
	stores, err := pdCli.GetAllStores(ctx, opt.WithExcludeTombstone())
	if err != nil {
		return 0, err
	}
	dialOptions, err := pkdbDialOptions()
	if err != nil {
		return 0, err
	}

	clusterID := pdCli.GetClusterID(ctx)
	var savedStates atomic.Uint64

	g, gCtx := util.NewErrorGroupWithRecoverWithCtx(ctx)
	g.SetLimit(pkdbDefaultReadStoreConcurrency)
	for _, store := range stores {
		if tikvrpc.GetStoreTypeByMeta(store) != tikvrpc.TiKV {
			continue
		}
		if store.GetState() != metapb.StoreState_Up {
			continue
		}
		storeID, addr := store.GetId(), store.GetAddress()
		g.Go(func() error {
			conn, err := grpc.DialContext(gCtx, addr, dialOptions...)
			if err != nil {
				return errors.Annotatef(err, "dial store %d(%s)", storeID, addr)
			}
			defer func() { _ = conn.Close() }()

			client := logreplicationpb.NewLogReplicationClient(conn)
			resp, err := client.SaveStoreRegionProgresses(gCtx, &logreplicationpb.SaveStoreRegionProgressesRequest{
				Header:         &logreplicationpb.Header{ClusterId: clusterID},
				MaxTs:          maxTS,
				RaftProgressId: raftProgressID,
			})
			if err != nil {
				return errors.Annotatef(err, "save store region progresses on store %d(%s)", storeID, addr)
			}
			savedStates.Add(resp.GetSavedCount())
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return 0, err
	}
	return savedStates.Load(), nil
}

func loadStoreRegionProgresses(ctx context.Context, pdCli pd.Client, raftProgressID string) ([]*logreplicationpb.LogReplicationState, error) {
	stores, err := pdCli.GetAllStores(ctx, opt.WithExcludeTombstone())
	if err != nil {
		return nil, err
	}
	dialOptions, err := pkdbDialOptions()
	if err != nil {
		return nil, err
	}

	clusterID := pdCli.GetClusterID(ctx)
	var states []*logreplicationpb.LogReplicationState
	var mu sync.Mutex

	g, gCtx := util.NewErrorGroupWithRecoverWithCtx(ctx)
	g.SetLimit(pkdbDefaultReadStoreConcurrency)
	for _, store := range stores {
		if tikvrpc.GetStoreTypeByMeta(store) != tikvrpc.TiKV {
			continue
		}
		if store.GetState() != metapb.StoreState_Up {
			continue
		}
		storeID, addr := store.GetId(), store.GetAddress()
		g.Go(func() error {
			conn, err := grpc.DialContext(gCtx, addr, dialOptions...)
			if err != nil {
				return errors.Annotatef(err, "dial store %d(%s)", storeID, addr)
			}
			defer func() { _ = conn.Close() }()

			client := logreplicationpb.NewLogReplicationClient(conn)
			resp, err := client.LoadStoreRegionProgresses(gCtx, &logreplicationpb.LoadStoreRegionProgressesRequest{
				Header:         &logreplicationpb.Header{ClusterId: clusterID},
				RaftProgressId: raftProgressID,
			})
			if err != nil {
				return errors.Annotatef(err, "load store region progresses from store %d(%s)", storeID, addr)
			}
			mu.Lock()
			states = append(states, resp.GetStates()...)
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	type regionKey struct {
		regionID uint64
		version  uint64
	}
	deduped := make(map[regionKey]*logreplicationpb.LogReplicationState, len(states))
	for _, state := range states {
		key := regionKey{
			regionID: state.GetRegion().GetId(),
			version:  state.GetRegion().GetRegionEpoch().GetVersion(),
		}
		if prev := deduped[key]; prev == nil || state.GetAppliedIndex() > prev.GetAppliedIndex() {
			deduped[key] = state
		}
	}

	merged := make([]*logreplicationpb.LogReplicationState, 0, len(deduped))
	for _, state := range deduped {
		merged = append(merged, state)
	}
	return merged, nil
}

// tidbWaitJobManager manages all supported tidbWaitJob.
type tidbWaitJobManager struct {
	ctx     context.Context
	store   kv.Storage
	etcdCli *clientv3.Client

	wg util.WaitGroupWrapper
	// reqKey -> jobState
	jobMap map[string]*jobState
}

type jobState struct {
	job             tidbWaitJob
	lastFinishedVer uint64
	cancel          context.CancelFunc

	mu sync.Mutex
}

func (s *jobState) checkAndWait(
	ctx context.Context,
	newJobBytes []byte,
	wg *util.WaitGroupWrapper,
	etcdCli *clientv3.Client,
	store kv.Storage,
) {
	s.mu.Lock()
	prevCancel := s.cancel
	lastFinishedVer := s.lastFinishedVer
	runCtx, runCancel := context.WithCancel(ctx)
	s.cancel = runCancel
	s.mu.Unlock()

	// Cancel the previous run (if any) so each job type has at most one in-flight
	// waiter. This is important when PD overwrites the same reqKey to cancel a
	// long-running wait (e.g. during failover).
	if prevCancel != nil {
		prevCancel()
	}

	wg.RunWithLog(func() {
		defer runCancel()
		var lastErr error
		for {
			if err := runCtx.Err(); err != nil {
				logutil.BgLogger().Warn("tidbWaitJobManager jobState context done", zap.Error(err))
				return
			}
			if lastErr != nil {
				logutil.BgLogger().Info(
					"tidbWaitJobManager meets error when running job, will retry later",
					zap.String("jobReqKey", s.job.reqKey()), zap.Error(lastErr))
				sleep(runCtx, time.Second*2)
			}

			logutil.BgLogger().Info("got a tidbWaitJob to process",
				zap.String("jobReqKey", s.job.reqKey()))
			var ver uint64
			ver, lastErr = s.job.checkAndWait(runCtx, newJobBytes, lastFinishedVer, store, etcdCli)
			if lastErr != nil {
				continue
			}
			logutil.BgLogger().Info("finish a tidbWaitJob",
				zap.String("jobRespKey", s.job.respKey()))

			// write back to etcd
			// TODO(lance6716): handle leader change so 2 leaders are writing.
			_, lastErr = etcdCli.Put(runCtx, s.job.respKey(), string(newJobBytes))
			if lastErr != nil {
				continue
			}

			s.mu.Lock()
			if ver > s.lastFinishedVer {
				s.lastFinishedVer = ver
			}
			s.mu.Unlock()
			return
		}
	})
}

func newTiDBWaitJobManager(
	ctx context.Context,
	store kv.Storage,
	etcdCli *clientv3.Client,
) *tidbWaitJobManager {
	return newTiDBWaitJobMgrWithJobs(
		ctx, store, etcdCli,
		&waitFlashback{},
		&saveRaftIndex{},
		&loadRaftIndex{},
	)
}

func newTiDBWaitJobMgrWithJobs(
	ctx context.Context,
	store kv.Storage,
	etcdCli *clientv3.Client,
	jobs ...tidbWaitJob,
) *tidbWaitJobManager {
	ret := &tidbWaitJobManager{
		ctx:     ctx,
		store:   store,
		etcdCli: etcdCli,
	}

	ret.jobMap = initJobMap(ctx, etcdCli, jobs...)

	return ret
}

func initJobMap(
	ctx context.Context,
	etcdCli *clientv3.Client,
	jobs ...tidbWaitJob,
) map[string]*jobState {
	if etcdCli == nil {
		return map[string]*jobState{}
	}
	ret := make(map[string]*jobState, len(jobs))
	respKey2ReqKey := make(map[string]string, len(jobs))
	for _, j := range jobs {
		ret[j.reqKey()] = &jobState{
			job: j,
		}
		respKey2ReqKey[j.respKey()] = j.reqKey()
	}

	var getResp *clientv3.GetResponse
	err := backoffWait(ctx, func() (bool, error) {
		resp, err := etcdCli.Get(ctx, constants.PkdbTiDBWaitRespPrefix, clientv3.WithPrefix())
		if err != nil {
			logutil.BgLogger().Error("tidbWaitJobManager failed to read etcd with prefix during init, will retry",
				zap.String("prefix", constants.PkdbTiDBWaitRespPrefix), zap.Error(err))
			return false, nil
		}
		getResp = resp
		return true, nil
	})
	if err != nil {
		logutil.BgLogger().Info("tidbWaitJobManager stopped initializing job map",
			zap.String("prefix", constants.PkdbTiDBWaitRespPrefix), zap.Error(err))
		return ret
	}

	for _, kv := range getResp.Kvs {
		reqKey, ok := respKey2ReqKey[string(kv.Key)]
		if !ok {
			logutil.BgLogger().Error("unexpected key in response from etcd",
				zap.String("key", string(kv.Key)),
				zap.Binary("value", kv.Value))
			continue
		}
		ver := ret[reqKey].job.getVersion(kv.Value)
		ret[reqKey].lastFinishedVer = ver
	}

	return ret
}

func (m *tidbWaitJobManager) start() {
	m.wg.RunWithLog(m.watchLoop)
}

func (m *tidbWaitJobManager) createWatch(nextRev int64) clientv3.WatchChan {
	watchCh := m.etcdCli.Watch(m.ctx, constants.PkdbTiDBWaitReqPrefix, clientv3.WithPrefix(), clientv3.WithRev(nextRev))
	if intest.InTest && tidbWaitJobAfterCreateWatch != nil {
		tidbWaitJobAfterCreateWatch()
	}
	return watchCh
}

func (m *tidbWaitJobManager) watchLoop() {
	nextRev := m.initWatch()
	if intest.InTest && tidbWaitJobAfterInitWatch != nil {
		tidbWaitJobAfterInitWatch()
	}

	watchCh := m.createWatch(nextRev)

	for {
		select {
		case <-m.ctx.Done():
			return
		case watchResp, ok := <-watchCh:
			if !ok || watchResp.Canceled {
				logutil.BgLogger().Info("pkdb job manager etcd watch interruped, will renew it")
				sleep(m.ctx, tidbWaitJobWatchRetryInterval)
				nextRev = m.initWatch()
				watchCh = m.createWatch(nextRev)
				continue
			}
			for _, event := range watchResp.Events {
				if event.Type != mvccpb.PUT {
					continue
				}
				reqKey := string(event.Kv.Key)
				if state, ok2 := m.jobMap[reqKey]; ok2 {
					state.checkAndWait(m.ctx, event.Kv.Value, &m.wg, m.etcdCli, m.store)
				} else {
					logutil.BgLogger().Error("tidbWaitJobManager received unexpected etcd key",
						zap.String("key", reqKey), zap.Binary("value", event.Kv.Value))
				}
			}
			nextRev = watchResp.Header.Revision + 1
		}
	}
}

func (m *tidbWaitJobManager) initWatch() (nextRev int64) {
	var (
		getResp *clientv3.GetResponse
		err     error
	)
	for {
		if m.ctx.Err() != nil {
			return 0
		}
		getResp, err = m.etcdCli.Get(m.ctx, constants.PkdbTiDBWaitReqPrefix, clientv3.WithPrefix())
		if err == nil {
			break
		}
		logutil.BgLogger().Error(
			"tidbWaitJobManager failed to read etcd with prefix, will retry later",
			zap.String("prefix", constants.PkdbTiDBWaitReqPrefix), zap.Error(err))
		sleep(m.ctx, time.Second)
	}

	nextRev = getResp.Header.Revision + 1

	for _, kv := range getResp.Kvs {
		reqKey := string(kv.Key)
		state, ok := m.jobMap[reqKey]
		if !ok {
			logutil.BgLogger().Error("tidbWaitJobManager received unknown etcd key during init",
				zap.String("key", reqKey), zap.Binary("value", kv.Value))
			continue
		}
		state.checkAndWait(m.ctx, kv.Value, &m.wg, m.etcdCli, m.store)
	}
	return nextRev
}

func (m *tidbWaitJobManager) close() {
	m.wg.Wait()
}

func backoffWait(ctx context.Context, f func() (bool, error)) error {
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 100 * time.Millisecond
	bo.RandomizationFactor = 0
	bo.MaxInterval = time.Second * 2
	bo.Multiplier = 1.5
	bo.MaxElapsedTime = 0
	bo.Reset()

	err := backoff.Retry(func() error {
		finished, err := f()
		if err != nil {
			return backoff.Permanent(err)
		}
		if finished {
			return nil
		}
		return errors.New("not finished yet")
	}, backoff.WithContext(bo, ctx))
	if err != nil && ctx.Err() != nil {
		err = context.Cause(ctx)
	}
	return err
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
