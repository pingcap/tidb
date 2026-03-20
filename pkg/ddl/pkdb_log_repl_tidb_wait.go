package ddl

import (
	"bytes"
	"context"
	"crypto/tls"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/kvproto/pkg/logreplicationpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/rangetask"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/constants"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const logReplStateCf = "lr-state"

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

// waitRegionsInit implements tidbWaitJob for waiting all regions to be initialized.
type waitRegionsInit struct{}

var _ tidbWaitJob = waitRegionsInit{}

func (j waitRegionsInit) reqKey() string {
	return constants.PkdbWaitAllRegionInitReq
}

func (j waitRegionsInit) respKey() string {
	return constants.PkdbWaitAllRegionInitResp
}

func (j waitRegionsInit) getVersion(bs []byte) uint64 {
	tmp := &pdpb.ReplicaWaitRegionsInit{}
	if err := tmp.Unmarshal(bs); err != nil {
		logutil.BgLogger().Error("waitRegionsInit failed to unmarshal request to get version", zap.Error(err))
		return 0
	}
	return tmp.GetVersion()
}

func (j waitRegionsInit) checkAndWait(
	ctx context.Context,
	newJobBytes []byte,
	lastFinishedVersion uint64,
	_ kv.Storage,
	etcdCli *clientv3.Client,
) (version uint64, err error) {
	curr := &pdpb.ReplicaWaitRegionsInit{}
	if err = curr.Unmarshal(newJobBytes); err != nil {
		return 0, errors.Errorf("waitRegionsInit failed to unmarshal request: %v", err)
	}
	version = curr.GetVersion()
	if version <= lastFinishedVersion {
		return version, nil
	}

	if !curr.GetNeedWait() {
		return version, nil
	}
	err = j.wait(ctx, curr.GetSourcePdAddrs(), etcdCli)
	if err != nil {
		return 0, err
	}
	return version, nil
}

func (j waitRegionsInit) wait(
	runCtx context.Context,
	sourcePDAddrs []string,
	etcdCli *clientv3.Client,
) error {
	cfg := config.GetGlobalConfig()
	kvCli, err := rawkv.NewClient(
		runCtx,
		strings.Split(cfg.Path, ","),
		cfg.Security.ClusterSecurity(),
	)
	if err != nil {
		return err
	}
	defer kvCli.Close()

	sourcePDCli, err := pd.NewClientWithContext(runCtx, sourcePDAddrs, pd.SecurityOption{})
	if err != nil {
		return err
	}
	defer sourcePDCli.Close()

	return backoffWait(runCtx, func() (bool, error) {
		return checkInitedAndUpdateProgress(runCtx, kvCli, sourcePDCli, etcdCli)
	})
}

type waitDataSync struct{}

var _ tidbWaitJob = &waitDataSync{}

func (w waitDataSync) reqKey() string {
	return constants.PkdbWaitDataSyncReq
}

func (w waitDataSync) respKey() string {
	return constants.PkdbWaitDataSyncResp
}

func (w waitDataSync) getVersion(bs []byte) uint64 {
	tmp := &pdpb.ReplicaWaitDataSync{}
	if err := tmp.Unmarshal(bs); err != nil {
		logutil.BgLogger().Error("waitDataSync failed to unmarshal request to get version", zap.Error(err))
		return 0
	}
	return tmp.GetVersion()
}

func (w waitDataSync) checkAndWait(ctx context.Context, newJobBytes []byte, lastFinishedVersion uint64, store kv.Storage, etcdCli *clientv3.Client) (version uint64, err error) {
	curr := &pdpb.ReplicaWaitDataSync{}
	if err = curr.Unmarshal(newJobBytes); err != nil {
		return 0, errors.Errorf("waitRegionsInit failed to unmarshal request: %v", err)
	}
	version = curr.GetVersion()
	if version <= lastFinishedVersion {
		return version, nil
	}

	if !curr.GetNeedWait() {
		return version, nil
	}
	err = w.wait(ctx, curr.GetSourcePdAddrs())
	if err != nil {
		return 0, err
	}
	return version, nil
}

func (w waitDataSync) wait(runCtx context.Context, sourcePDAddrs []string) error {
	cfg := config.GetGlobalConfig()
	cli, err := rawkv.NewClient(
		runCtx,
		strings.Split(cfg.Path, ","),
		cfg.Security.ClusterSecurity(),
	)
	if err != nil {
		return err
	}
	defer cli.Close()

	commitIndexes, err := getRegionCommitIndexes(runCtx, sourcePDAddrs)
	if err != nil {
		return err
	}

	return backoffWait(runCtx, func() (bool, error) {
		return isSynced(runCtx, cli, commitIndexes)
	})
}

type regionCommitIndex struct {
	region      *metapb.Region
	commitIndex uint64
}

func pkdbBatchScanAllRegions(ctx context.Context, pdCli pd.Client) ([]*pd.Region, error) {
	var (
		all     []*pd.Region
		nextKey []byte
	)
	for {
		batch, err := pdCli.BatchScanRegions(
			ctx,
			[]pd.KeyRange{{StartKey: nextKey}},
			pkdbDefaultScanPageSize,
			pd.WithAllowFollowerHandle(),
		)
		if err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			return nil, errors.New("batch scan regions returned empty batch when scanning all regions")
		}
		all = append(all, batch...)

		last := batch[len(batch)-1]
		if last == nil || last.Meta == nil {
			return nil, errors.New("batch scan regions returned nil region meta")
		}
		endKey := last.Meta.GetEndKey()
		if len(endKey) == 0 {
			return all, nil
		}
		if bytes.Compare(endKey, nextKey) <= 0 {
			return nil, errors.New("batch scan regions returned non-progressing end key")
		}
		nextKey = endKey
	}
}

func getRegionCommitIndexes(ctx context.Context, pdAddrs []string) ([]regionCommitIndex, error) {
	pdCli, err := pd.NewClientWithContext(ctx, pdAddrs, pd.SecurityOption{})
	if err != nil {
		return nil, err
	}
	defer pdCli.Close()

	stores, err := pdCli.GetAllStores(ctx)
	if err != nil {
		return nil, err
	}
	storeAddrs := make(map[uint64]string)
	for _, store := range stores {
		storeAddrs[store.GetId()] = store.GetAddress()
	}

	regions, err := pkdbBatchScanAllRegions(ctx, pdCli)
	if err != nil {
		return nil, err
	}
	storeAddrToRegionIDs := make(map[string][]uint64)
	for _, region := range regions {
		if region.Leader == nil || region.Leader.Id == 0 {
			return nil, errors.Errorf("region %d has no leader", region.Meta.Id)
		}
		storeAddr, ok := storeAddrs[region.Leader.GetStoreId()]
		if !ok {
			return nil, errors.Errorf("store %d not found", region.Leader.GetStoreId())
		}
		storeAddrToRegionIDs[storeAddr] = append(storeAddrToRegionIDs[storeAddr], region.Meta.Id)
	}

	var (
		result   []regionCommitIndex
		resultMu sync.Mutex
	)
	g, gCtx := errgroup.WithContext(ctx)
	for storeAddr, regionIDs := range storeAddrToRegionIDs {
		storeAddr := storeAddr // seems unnecessary after go1.22?
		regionIDs := regionIDs // seems unnecessary after go1.22?
		g.Go(func() error {
			conn, err := grpc.DialContext(gCtx, storeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return err
			}
			defer conn.Close()

			debugCli := debugpb.NewDebugClient(conn)

			for _, regionID := range regionIDs {
				resp, err := debugCli.RegionInfo(ctx, &debugpb.RegionInfoRequest{RegionId: regionID})
				if err != nil {
					return err
				}
				resultMu.Lock()
				result = append(result, regionCommitIndex{
					region:      resp.RegionLocalState.Region,
					commitIndex: resp.RaftLocalState.HardState.Commit,
				})
				resultMu.Unlock()
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return result, nil
}

func isSynced(ctx context.Context, cli *rawkv.Client, commitIndexes []regionCommitIndex) (bool, error) {
	states, err := scanReplStates(ctx, cli)
	if err != nil {
		return false, err
	}

	slices.SortFunc(commitIndexes, func(a, b regionCommitIndex) int {
		return bytes.Compare(a.region.StartKey, b.region.StartKey)
	})

	for _, ci := range commitIndexes {
		region, commitIndex := ci.region, ci.commitIndex
		startKey, endKey := region.StartKey, region.EndKey
		allCovered := false
		for !allCovered && len(states) > 0 {
			first := states[0]
			// first is completely before the region.
			if len(first.EndKey) > 0 && bytes.Compare(first.EndKey, startKey) <= 0 {
				states = states[1:]
				continue
			}
			// first is completely after the region.
			if len(endKey) > 0 && bytes.Compare(first.StartKey, endKey) >= 0 {
				break
			}
			// first and region overlaps.
			if bytes.Compare(first.StartKey, startKey) > 0 {
				logutil.BgLogger().Info(
					"range gap found during wait for standby sync",
					zap.Binary("from", startKey),
					zap.Binary("to", first.StartKey),
				)
				return false, nil
			}
			if first.Region.RegionEpoch.Version < region.RegionEpoch.Version ||
				first.Region.RegionEpoch.Version == region.RegionEpoch.Version && first.AppliedIndex < commitIndex {
				logutil.BgLogger().Info(
					"region not synced yet during wait for standby sync",
					zap.Uint64("regionID", region.Id),
					zap.Stringer("regionEpoch", region.RegionEpoch),
					zap.Uint64("commitIndex", commitIndex),
					zap.Stringer("appliedEpoch", first.Region.RegionEpoch),
					zap.Uint64("appliedIndex", first.AppliedIndex),
				)
				return false, nil
			}
			if len(first.EndKey) > 0 && (len(endKey) == 0 || bytes.Compare(first.EndKey, endKey) < 0) {
				startKey = first.EndKey
				states = states[1:]
			} else {
				startKey = endKey // all covered
				allCovered = true
				break
			}
		}
		if !allCovered {
			to := zap.Binary("to", endKey)
			if len(endKey) == 0 {
				to = zap.String("to", "infinity")
			}
			logutil.BgLogger().Info(
				"range gap found during wait for standby sync",
				zap.Binary("from", startKey),
				to,
			)
			return false, nil
		}
	}

	return true, nil
}

type waitCleanupTiKV struct{}

var _ tidbWaitJob = &waitCleanupTiKV{}

func (w waitCleanupTiKV) reqKey() string {
	return constants.PkdbWaitCleanupTiKVReq
}

func (w waitCleanupTiKV) respKey() string {
	return constants.PkdbWaitCleanupTiKVResp
}

func (w waitCleanupTiKV) getVersion(bs []byte) uint64 {
	tmp := &pdpb.ReplicaCleanup{}
	if err := tmp.Unmarshal(bs); err != nil {
		logutil.BgLogger().Error("waitCleanupTiKV failed to unmarshal request to get version", zap.Error(err))
		return 0
	}
	return tmp.GetVersion()
}

func (w waitCleanupTiKV) checkAndWait(
	ctx context.Context,
	newJobBytes []byte,
	lastFinishedVersion uint64,
	_ kv.Storage,
	_ *clientv3.Client,
) (version uint64, err error) {
	curr := &pdpb.ReplicaCleanup{}
	if err = curr.Unmarshal(newJobBytes); err != nil {
		return 0, errors.Errorf("waitCleanupTiKV failed to unmarshal request: %v", err)
	}
	version = curr.GetVersion()
	if version <= lastFinishedVersion {
		return version, nil
	}

	if !curr.GetCleanupMetadata() {
		return version, nil
	}
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
	return version, cli.DeleteRange(ctx, nil, nil, rawkv.SetColumnFamily(logReplStateCf))
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

type resetTS struct{}

var _ tidbWaitJob = &resetTS{}

func (r resetTS) reqKey() string {
	return constants.PkdbResetTSReq
}

func (r resetTS) respKey() string {
	return constants.PkdbResetTSResp
}

func (r resetTS) getVersion(bs []byte) uint64 {
	tmp := &pdpb.ReplicaResetTS{}
	if err := tmp.Unmarshal(bs); err != nil {
		logutil.BgLogger().Error("resetTS failed to unmarshal request to get version", zap.Error(err))
		return 0
	}
	return tmp.GetVersion()
}

func (r resetTS) checkAndWait(
	ctx context.Context,
	newJobBytes []byte,
	lastFinishedVersion uint64,
	store kv.Storage,
	_ *clientv3.Client,
) (version uint64, err error) {
	curr := &pdpb.ReplicaResetTS{}
	if err = curr.Unmarshal(newJobBytes); err != nil {
		return 0, errors.Errorf("resetTS failed to unmarshal request: %v", err)
	}
	version = curr.GetVersion()
	if version <= lastFinishedVersion {
		return version, nil
	}

	cfg := config.GetGlobalConfig()
	rawCli, err := rawkv.NewClient(
		ctx,
		strings.Split(cfg.Path, ","),
		cfg.Security.ClusterSecurity(),
	)
	if err != nil {
		return 0, err
	}
	defer rawCli.Close()

	states, err := scanReplStates(ctx, rawCli)
	if err != nil {
		return 0, err
	}

	var maxTS uint64
	for _, state := range states {
		if state.GetMaxTs() > maxTS {
			maxTS = state.GetMaxTs()
		}
	}
	if maxTS == 0 {
		logutil.BgLogger().Warn("resetTS got max_ts=0, skip resetting PD TSO", zap.Uint64("version", version))
		return version, nil
	}

	storeWithPD, ok := store.(kv.StorageWithPD)
	if !ok {
		return 0, errors.Errorf("resetTS requires kv.StorageWithPD, got %T", store)
	}
	pdHTTPCli := storeWithPD.GetPDHTTPClient()
	if pdHTTPCli == nil {
		return 0, errors.New("resetTS got nil pd HTTP client")
	}

	logutil.BgLogger().Info("start resetTS",
		zap.Uint64("version", version),
		zap.Uint64("maxTS", maxTS),
	)

	if err := pdHTTPCli.ResetTS(ctx, maxTS, true); err != nil {
		return 0, errors.Trace(err)
	}

	logutil.BgLogger().Info("finish resetTS",
		zap.Uint64("version", version),
		zap.Uint64("maxTS", maxTS),
	)
	return version, nil
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
		return 0, errors.Errorf("waitCleanupTiKV failed to unmarshal request: %v", err)
	}
	version = curr.GetVersion()
	if version <= lastFinishedVersion {
		return version, nil
	}

	storeWithPD, ok := store.(kv.StorageWithPD)
	if !ok {
		return 0, errors.Errorf("saveRaftIndex requires kv.StorageWithPD, got %T", store)
	}
	pdCli := storeWithPD.GetPDClient()
	if pdCli == nil {
		return 0, errors.New("saveRaftIndex got nil pd client")
	}

	var tlsCfg *tls.Config
	if backend, ok := store.(kv.EtcdBackend); ok {
		tlsCfg = backend.TLSConfig()
	}

	collector := newPkdbRaftIndexCollector(pdCli, tlsCfg)
	defer collector.Close()

	logutil.BgLogger().Info("start saveRaftIndex",
		zap.Uint64("version", version),
		zap.Int("pageSize", pkdbDefaultScanPageSize),
		zap.Int("storeConcurrency", pkdbDefaultReadStoreConcurrency),
	)

	err = collector.collectRaftLogIndexes(ctx, pkdbRaftIndexCollectOptions{
		pageSize:             pkdbDefaultScanPageSize,
		readStoreConcurrency: pkdbDefaultReadStoreConcurrency,
		writePDConcurrency:   pkdbDefaultWritePDConcurrency,
		retryWait:            pkdbDefaultRetryWait,
	}, saveRaftAndRegionState2PD(pdCli))
	if err != nil {
		return 0, err
	}

	logutil.BgLogger().Info("finish saveRaftIndex", zap.Uint64("version", version))
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

	pdAddrs := curr.GetNewPrimaryPdAddrs()
	if len(pdAddrs) == 0 {
		return 0, errors.New("loadRaftIndex got empty new primary pd addrs")
	}

	primaryPDCli, err := pd.NewClientWithContext(ctx, pdAddrs, pd.SecurityOption{})
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
		zap.Uint64("pageSize", pkdbDefaultScanPageSize),
		zap.Strings("newPrimaryPdAddrs", pdAddrs),
	)

	var (
		startID      uint64
		loadedStates uint64
	)
	for {
		raftStates, _, regionStates, err := primaryPDCli.ScanRaftAndRegionState(ctx, startID, pkdbDefaultScanPageSize)
		if err != nil {
			return 0, err
		}
		if len(raftStates) == 0 || len(regionStates) == 0 {
			break
		}

		maxSeen := startID
		for regionID, regionLocalState := range regionStates {
			if regionID > maxSeen {
				maxSeen = regionID
			}

			if regionLocalState == nil {
				return 0, errors.Errorf("loadRaftIndex got nil region local state, region id: %d", regionID)
			}
			raftLocalState := raftStates[regionID]
			if raftLocalState == nil {
				return 0, errors.Errorf("loadRaftIndex got nil raft apply state, region id: %d", regionID)
			}
			region := regionLocalState.GetRegion()
			if region == nil {
				return 0, errors.Errorf("loadRaftIndex got nil region meta, region id: %d", regionID)
			}

			state := &logreplicationpb.LogReplicationState{
				Region:       region,
				StartKey:     region.StartKey,
				EndKey:       region.EndKey,
				AppliedIndex: raftLocalState.GetHardState().GetCommit(),
			}
			err = cli.UpdateLogReplState(ctx, state)
			if err != nil {
				return 0, err
			}
			loadedStates++
		}

		if maxSeen == ^uint64(0) {
			break
		}
		startID = maxSeen + 1
	}

	logutil.BgLogger().Info("finish loadRaftIndex",
		zap.Uint64("version", version),
		zap.Uint64("loadedStates", loadedStates),
	)
	return version, nil
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
		&waitRegionsInit{},
		&waitDataSync{},
		&waitCleanupTiKV{},
		&waitFlashback{},
		&resetTS{},
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

func isInited(ctx context.Context, cli *rawkv.Client) (bool, []*logreplicationpb.LogReplicationState, error) {
	states, err := scanReplStates(ctx, cli)
	if err != nil {
		return false, nil, err
	}

	var lastEndKey []byte
	for _, state := range states {
		if bytes.Compare(state.StartKey, lastEndKey) > 0 {
			logutil.BgLogger().Info(
				"range gap found during wait for standby init",
				zap.Binary("from", lastEndKey),
				zap.Binary("to", state.StartKey),
			)
			return false, states, nil
		}
		if len(state.EndKey) == 0 {
			return true, states, nil
		}
		if bytes.Compare(state.EndKey, lastEndKey) > 0 {
			lastEndKey = state.EndKey
		}
	}
	logutil.BgLogger().Info(
		"range gap found during wait for standby init",
		zap.Binary("from", lastEndKey),
		zap.String("to", "infinity"),
	)
	return false, states, nil
}

func checkInitedAndUpdateProgress(
	ctx context.Context,
	cli *rawkv.Client,
	sourcePDCli pd.Client,
	etcdCli *clientv3.Client,
) (bool, error) {
	inited, states, err := isInited(ctx, cli)
	if err != nil {
		return false, err
	}
	if inited {
		_, err = etcdCli.Put(ctx, constants.PkdbInitPercentage, "100")
		if err != nil {
			return false, err
		}
		return true, nil
	}
	regions, err := pkdbBatchScanAllRegions(ctx, sourcePDCli)
	if err != nil {
		return false, err
	}
	percentage := getStandbyInitPercentage(regions, states)
	_, err = etcdCli.Put(ctx, constants.PkdbInitPercentage, strconv.FormatFloat(percentage, 'f', -1, 32))
	return false, err
}

func getStandbyInitPercentage(regions []*pd.Region, states []*logreplicationpb.LogReplicationState) float64 {
	done := 0
	for _, region := range regions {
		startKey, endKey := region.Meta.StartKey, region.Meta.EndKey
		allCovered := false
		for len(states) > 0 {
			first := states[0]
			// first is completely before the region.
			if len(first.EndKey) > 0 && bytes.Compare(first.EndKey, startKey) <= 0 {
				states = states[1:]
				continue
			}
			// first is completely after the region.
			if len(endKey) > 0 && bytes.Compare(first.StartKey, endKey) >= 0 {
				break
			}
			// [startKey, first.StartKey) is not covered.
			if bytes.Compare(first.StartKey, startKey) > 0 {
				break
			}
			// [startKey, first.EndKey) is covered.
			// [first.EndKey, endKey) need to be checked in next loop.
			if len(first.EndKey) > 0 && (len(endKey) == 0 || bytes.Compare(first.EndKey, endKey) < 0) {
				startKey = first.EndKey
				states = states[1:]
				continue
			}
			allCovered = true
			break
		}
		if allCovered {
			done++
		}
	}

	return float64(done) / float64(len(regions)) * 100
}

func scanReplStates(ctx context.Context, cli *rawkv.Client) ([]*logreplicationpb.LogReplicationState, error) {
	var states []*logreplicationpb.LogReplicationState

	var startKey []byte
	for {
		keys, values, err := cli.Scan(ctx, startKey, nil, 1000, rawkv.SetColumnFamily(logReplStateCf))
		if err != nil {
			return nil, err
		}
		if len(keys) == 0 {
			return states, nil
		}
		for _, v := range values {
			pb := &logreplicationpb.LogReplicationState{}
			err := pb.Unmarshal(v)
			if err != nil {
				return nil, err
			}
			states = append(states, pb)
		}
		startKey = keys[len(keys)-1]
		startKey = append(startKey, 0)
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
