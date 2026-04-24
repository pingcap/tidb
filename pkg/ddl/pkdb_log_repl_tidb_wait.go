package ddl

import (
	"bytes"
	"context"
	"crypto/tls"
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
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/redact"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/rangetask"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/constants"
	pdhttp "github.com/tikv/pd/client/http"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
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

	// tidbWaitDataSyncScanRetryWaitDuration is used when waitDataSync retries PD
	// region scans due to transient errors or inconsistent results. Tests can
	// shorten it to speed up retry loops.
	tidbWaitDataSyncScanRetryWaitDuration = pkdbDefaultRetryWaitDuration
	// tidbWaitDataSyncScanBatchSize controls the batch size when scanning regions
	// from PD in waitDataSync.
	tidbWaitDataSyncScanBatchSize = pkdbDefaultScanPageSize
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
	pdHTTPCli := pdhttp.NewClientWithServiceDiscovery(
		"log replication",
		sourcePDCli.GetServiceDiscovery(),
	)
	defer pdHTTPCli.Close()

	progressTracker := newStandbyInitProgressTracker(pdHTTPCli, etcdCli)
	estRegionCnt, err := progressTracker.estimateTotalRegionCnt(runCtx)
	if err != nil {
		return err
	}
	return backoffWait(runCtx, func() (bool, error) {
		return progressTracker.checkInitedAndUpdateProgress(runCtx, kvCli, estRegionCnt)
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
		return 0, errors.Errorf("waitDataSync failed to unmarshal request: %v", err)
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

	ctx, cancel := context.WithCancelCause(runCtx)
	defer cancel(nil)

	sourcePDCli, err := pd.NewClientWithContext(ctx, sourcePDAddrs, pd.SecurityOption{})
	if err != nil {
		return err
	}
	defer sourcePDCli.Close()

	stores, err := sourcePDCli.GetAllStores(ctx)
	if err != nil {
		return err
	}
	storeAddrs := make(map[uint64]string)
	for _, store := range stores {
		storeAddrs[store.GetId()] = store.GetAddress()
	}

	debugCliPool := newPkdbDebugClientPool([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	})
	defer debugCliPool.Close()

	// Scan source regions and fetch their raft commit indexes in batches, and
	// wait each batch to be synced before proceeding.
	//
	// This keeps peak memory bounded by the batch size (instead of O(#regions))
	// and pipelines "fetch next batch" with "wait current batch".
	commitIndexBatches := make(chan []regionCommitIndex, 1)
	go func() {
		defer util.Recover(metrics.LabelDDL, "waitDataSync", func() {
			cancel(errors.New("panic in waitDataSync"))
		}, false)
		defer close(commitIndexBatches)
		// TODO(lance6716): after primary TiKV disabling write, there still may exist
		// some in-flight raft log that will commit in future. From yujie.
		err := fetchRegionCommitIndexInBatch(
			ctx,
			sourcePDCli,
			storeAddrs,
			debugCliPool,
			func(batch []regionCommitIndex) error {
				select {
				case commitIndexBatches <- batch:
					return nil
				case <-ctx.Done():
					return context.Cause(ctx)
				}
			},
		)
		if err != nil {
			cancel(err)
		}
	}()

	var waitErr error
	for batch := range commitIndexBatches {
		// Always drain commitIndexBatches to ensure the fetch goroutine has fully
		// exited.
		if waitErr != nil {
			continue
		}
		err := backoffWait(ctx, func() (bool, error) {
			return isSynced(ctx, cli, batch)
		})
		if err != nil {
			waitErr = err
			cancel(err)
			continue
		}
	}

	if waitErr != nil {
		return waitErr
	}
	return context.Cause(ctx)
}

type regionCommitIndex struct {
	region      *metapb.Region
	commitIndex uint64
}

type regionScanner interface {
	BatchScanRegions(ctx context.Context, ranges []pd.KeyRange, limit int, opts ...pd.GetRegionOption) ([]*pd.Region, error)
}

func fetchRegionCommitIndexInBatch(
	ctx context.Context,
	pdCli regionScanner,
	storeAddrs map[uint64]string,
	debugCliPool *pkdbDebugClientPool,
	onBatch func([]regionCommitIndex) error,
) error {
	var nextKey []byte
scanRegions:
	for {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}

		// NOTE: We intentionally do NOT use opt.WithAllowFollowerHandle() here.
		// waitDataSync relies on the leader who has the latest region info.
		batch, err := pdCli.BatchScanRegions(
			ctx,
			[]pd.KeyRange{{StartKey: nextKey}},
			tidbWaitDataSyncScanBatchSize,
		)
		if err != nil {
			logutil.BgLogger().Warn(
				"log replication fetchRegionCommitIndexInBatch failed, retrying",
				zap.Error(err),
				zap.String("startKey", redact.Key(nextKey)),
			)
			sleep(ctx, tidbWaitDataSyncScanRetryWaitDuration)
			continue
		}
		// NOTE: PD `BatchScanRegions` returns regions ordered by start key. We
		// rely on this ordering so each commit index batch is already sorted by
		// start key, which `isSynced` requires.
		if err := checkPartRegionConsistency(nextKey, batch); err != nil {
			logutil.BgLogger().Warn(
				"log replication fetchRegionCommitIndexInBatch got inconsistent result, retrying",
				zap.Error(err),
				zap.String("startKey", redact.Key(nextKey)),
				zap.Int("regionLength", len(batch)),
			)
			sleep(ctx, tidbWaitDataSyncScanRetryWaitDuration)
			continue
		}

		var commitIndexes []regionCommitIndex
		for {
			if ctx.Err() != nil {
				return context.Cause(ctx)
			}
			commitIndexes, err = getRegionCommitIndexesFromBatch(ctx, batch, storeAddrs, debugCliPool)
			if err == nil {
				break
			}

			// Region epoch/confver can change during splits/merges. In this case, the
			// PD scan result becomes stale and we need to re-scan regions.
			switch err.(type) {
			case *pkdbErrRegionEpochMismatch, *pkdbErrMissingRegionMeta, *pkdbErrRegionInfoNotFound:
				logutil.BgLogger().Warn(
					"log replication fetchRegionCommitIndexInBatch got stale region meta while fetching commit indexes, retrying scan regions",
					zap.Error(err),
					zap.String("startKey", redact.Key(nextKey)),
					zap.Int("regionLength", len(batch)),
				)
				sleep(ctx, tidbWaitDataSyncScanRetryWaitDuration)
				continue scanRegions
			}
			// Retry transient RegionInfo errors with the same PD scan result.
			if pkdbIsRetryableRegionInfoError(err) {
				logutil.BgLogger().Warn(
					"log replication fetchRegionCommitIndexInBatch failed to fetch region commit indexes, retrying",
					zap.Error(err),
					zap.String("startKey", redact.Key(nextKey)),
					zap.Int("regionLength", len(batch)),
				)
				sleep(ctx, tidbWaitDataSyncScanRetryWaitDuration)
				continue
			}
			return err
		}
		if err = onBatch(commitIndexes); err != nil {
			return err
		}

		endKey := batch[len(batch)-1].Meta.GetEndKey()
		if len(endKey) == 0 {
			return nil
		}
		// Defensive: endKey should always move forward; otherwise we'd keep
		// scanning the same range and loop forever.
		if bytes.Compare(endKey, nextKey) <= 0 {
			logutil.BgLogger().Error(
				"log replication fetchRegionCommitIndexInBatch got non-progressing end key",
				zap.String("startKey", redact.Key(nextKey)),
				zap.String("endKey", redact.Key(endKey)),
				zap.Int("regionLength", len(batch)),
			)
			return errors.New("batch scan regions returned non-progressing end key")
		}
		nextKey = endKey
	}
}

// checkPartRegionConsistency only checks the continuity of regions and the
// first region consistency (like BR's split module does).
func checkPartRegionConsistency(startKey []byte, regions []*pd.Region) error {
	// PD can't guarantee the consistency of returned regions.
	if len(regions) == 0 {
		return errors.New("scan regions returned empty result")
	}

	first := regions[0]
	if first == nil || first.Meta == nil {
		return errors.New("scan regions returned nil region meta")
	}
	if bytes.Compare(first.Meta.StartKey, startKey) > 0 {
		return errors.Errorf("first region %d's start key is after scan start key", first.Meta.Id)
	}
	if first.Leader == nil || first.Leader.GetStoreId() == 0 {
		return errors.Errorf("region %d has no leader store", first.Meta.Id)
	}

	cur := first
	for _, r := range regions[1:] {
		if r == nil || r.Meta == nil {
			return errors.New("scan regions returned nil region meta")
		}
		if r.Leader == nil || r.Leader.GetStoreId() == 0 {
			return errors.Errorf("region %d has no leader store", r.Meta.Id)
		}
		if !bytes.Equal(cur.Meta.EndKey, r.Meta.StartKey) {
			return errors.Errorf("region %d's end key does not match next region %d's start key", cur.Meta.Id, r.Meta.Id)
		}
		cur = r
	}
	return nil
}

func getRegionCommitIndexesFromBatch(
	ctx context.Context,
	batch []*pd.Region,
	storeAddrs map[uint64]string,
	debugCliPool *pkdbDebugClientPool,
) ([]regionCommitIndex, error) {
	type regionFetchTask struct {
		regionID uint64
		idx      int
	}

	storeAddrToTasks := make(map[string][]regionFetchTask)
	for i, region := range batch {
		storeAddr, ok := storeAddrs[region.Leader.GetStoreId()]
		if !ok {
			return nil, errors.Errorf("store %d not found", region.Leader.GetStoreId())
		}
		storeAddrToTasks[storeAddr] = append(storeAddrToTasks[storeAddr], regionFetchTask{
			regionID: region.Meta.Id,
			idx:      i,
		})
	}

	// The output order matches PD's BatchScanRegions order (start key order).
	result := make([]regionCommitIndex, len(batch))

	g, gCtx := util.NewErrorGroupWithRecoverWithCtx(ctx)
	g.SetLimit(pkdbDefaultReadStoreConcurrency)
	for storeAddr, tasks := range storeAddrToTasks {
		g.Go(func() error {
			if gCtx.Err() != nil {
				return context.Cause(gCtx)
			}

			debugCli, err := debugCliPool.Get(gCtx, storeAddr)
			if err != nil {
				return err
			}
			for _, task := range tasks {
				// TODO(lance6716): add batch RPC
				resp, err := debugCli.RegionInfo(gCtx, &debugpb.RegionInfoRequest{RegionId: task.regionID})
				if err != nil {
					if pkdbIsRegionInfoNotFound(err) {
						// TiKV debug service returns NOT_FOUND when the store doesn't have any local
						// metadata for this region. In this case, retrying the same store with the same
						// PD scan result can loop forever. Let the caller rescan regions from PD.
						return &pkdbErrRegionInfoNotFound{
							regionID:  task.regionID,
							storeAddr: storeAddr,
						}
					}
					if pkdbIsRetryableRegionInfoError(err) {
						// Force re-dial next time for transient connection issues.
						debugCliPool.Delete(storeAddr)
					}
					return err
				}

				regionLocalState := resp.GetRegionLocalState()
				regionMeta := regionLocalState.GetRegion()
				if regionMeta == nil {
					return &pkdbErrMissingRegionMeta{
						regionID:  task.regionID,
						storeAddr: storeAddr,
					}
				}
				actualEpoch := regionMeta.GetRegionEpoch()
				expectedEpoch := batch[task.idx].Meta.GetRegionEpoch()
				if actualEpoch.GetConfVer() != expectedEpoch.GetConfVer() ||
					actualEpoch.GetVersion() != expectedEpoch.GetVersion() {
					return &pkdbErrRegionEpochMismatch{
						regionID:  task.regionID,
						storeAddr: storeAddr,
						expected:  expectedEpoch,
						actual:    actualEpoch,
					}
				}

				raftLocalState := resp.GetRaftLocalState()
				if raftLocalState == nil || raftLocalState.HardState == nil {
					return errors.Errorf("region %d got nil raft local state from store %s", task.regionID, storeAddr)
				}
				result[task.idx] = regionCommitIndex{
					region:      batch[task.idx].Meta,
					commitIndex: raftLocalState.GetHardState().GetCommit(),
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return result, nil
}

type rawKVScanClient interface {
	Scan(ctx context.Context, startKey, endKey []byte, limit int, options ...rawkv.RawOption) (keys [][]byte, values [][]byte, err error)
	ReverseScan(ctx context.Context, startKey, endKey []byte, limit int, options ...rawkv.RawOption) (keys [][]byte, values [][]byte, err error)
}

func isSynced(ctx context.Context, cli rawKVScanClient, commitIndexes []regionCommitIndex) (bool, error) {
	if len(commitIndexes) == 0 {
		return true, nil
	}

	// Only scan the replica state for the key range covered by this batch. Because
	// the log replication has been initialized, TiKV will maintain the states to be
	// continuous.
	states, err := scanReplStatesInRange(
		ctx,
		cli,
		commitIndexes[0].region.StartKey,
		commitIndexes[len(commitIndexes)-1].region.EndKey,
	)
	if err != nil {
		return false, err
	}

	// Ensure the scanned states cover the batch start key; otherwise treat it as a
	// gap and keep waiting (instead of returning a permanent error).
	if len(states) > 0 && bytes.Compare(states[0].StartKey, commitIndexes[0].region.StartKey) > 0 {
		logutil.BgLogger().Info(
			"range gap found during wait for standby sync",
			zap.String("from", redact.Key(commitIndexes[0].region.StartKey)),
			zap.String("to", redact.Key(states[0].StartKey)),
		)
		return false, nil
	}

	for _, ci := range commitIndexes {
		region, commitIndex := ci.region, ci.commitIndex
		startKey, endKey := region.StartKey, region.EndKey
		allCovered := false
		for len(states) > 0 {
			firstState := states[0]
			// first is completely before the region.
			if len(firstState.EndKey) > 0 && bytes.Compare(firstState.EndKey, startKey) <= 0 {
				states = states[1:]
				continue
			}
			// first is completely after the region.
			if len(endKey) > 0 && bytes.Compare(firstState.StartKey, endKey) >= 0 {
				break
			}
			// first and region overlaps.
			if bytes.Compare(firstState.StartKey, startKey) > 0 {
				logutil.BgLogger().Info(
					"range gap found during wait for standby sync",
					zap.String("from", redact.Key(startKey)),
					zap.String("to", redact.Key(firstState.StartKey)),
				)
				return false, nil
			}
			if firstState.Region.RegionEpoch.Version < region.RegionEpoch.Version ||
				firstState.Region.RegionEpoch.Version == region.RegionEpoch.Version && firstState.AppliedIndex < commitIndex {
				logutil.BgLogger().Info(
					"region not synced yet during wait for standby sync",
					zap.Uint64("regionID", region.Id),
					zap.Stringer("regionEpoch", region.RegionEpoch),
					zap.Uint64("commitIndex", commitIndex),
					zap.Stringer("appliedEpoch", firstState.Region.RegionEpoch),
					zap.Uint64("appliedIndex", firstState.AppliedIndex),
				)
				return false, nil
			}
			if len(firstState.EndKey) > 0 && (len(endKey) == 0 || bytes.Compare(firstState.EndKey, endKey) < 0) {
				startKey = firstState.EndKey
				states = states[1:]
			} else {
				startKey = endKey // all covered
				allCovered = true
				break
			}
		}
		if !allCovered {
			to := zap.String("to", redact.Key(endKey))
			if len(endKey) == 0 {
				to = zap.String("to", "infinity")
			}
			logutil.BgLogger().Info(
				"range gap found during wait for standby sync",
				zap.String("from", redact.Key(startKey)),
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

	// TODO(lance6716): We only need max_ts, but scanReplStates materializes all
	// states in memory. This can OOM on large-region clusters; consider scanning
	// in a streaming fashion and only keeping the running max.
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
		retryWait:            pkdbDefaultRetryWaitDuration,
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

func checkInited(
	ctx context.Context,
	cli rawKVScanClient,
	resumeKey []byte,
) (allDone bool, advancedStateCnt int, nextResumeKey []byte, err error) {
	// resumeKey is also the end key of the last continuous state prefix obtained
	// from the last call of this function.
	currResumeKey := resumeKey
	advancedStateCnt = 0

	advanceWithState := func(state *logreplicationpb.LogReplicationState) (allDone bool, gap bool) {
		if bytes.Compare(state.StartKey, currResumeKey) > 0 {
			logutil.BgLogger().Info(
				"range gap found during wait for standby init",
				zap.String("from", redact.Key(currResumeKey)),
				zap.String("to", redact.Key(state.StartKey)),
			)
			return false, true
		}
		if len(state.EndKey) == 0 {
			return true, false
		}
		if bytes.Compare(state.EndKey, currResumeKey) > 0 {
			currResumeKey = state.EndKey
			advancedStateCnt++
		}
		return false, false
	}

	for {
		keys, values, err := cli.Scan(ctx, currResumeKey, nil, pkdbDefaultScanPageSize, rawkv.SetColumnFamily(logReplStateCf))
		if err != nil {
			return false, 0, nil, err
		}

		if len(keys) == 0 || !bytes.Equal(keys[0], currResumeKey) {
			if len(currResumeKey) == 0 {
				gapEnd := "infinity"
				if len(keys) > 0 {
					gapEnd = redact.Key(keys[0])
				}
				logutil.BgLogger().Info(
					"range gap found during wait for standby init",
					zap.String("from", "min-key"),
					zap.String("to", gapEnd),
				)
				return false, advancedStateCnt, bytes.Clone(currResumeKey), nil
			}

			// Between two scans, the state whose start key equals scanKey may have been
			// removed due to region merge. Reverse-scan one KV to see if a previous state
			// now covers scanKey.
			_, prevValues, err := cli.ReverseScan(ctx, currResumeKey, nil, 1, rawkv.SetColumnFamily(logReplStateCf))
			if err != nil {
				return false, 0, nil, err
			}
			if len(prevValues) == 0 {
				logutil.BgLogger().Info(
					"range gap found during wait for standby init",
					zap.String("from", "(unknown prev key)"),
					zap.String("to", redact.Key(currResumeKey)),
				)
				return false, advancedStateCnt, bytes.Clone(currResumeKey), nil
			}

			prev := &logreplicationpb.LogReplicationState{}
			if err := prev.Unmarshal(prevValues[0]); err != nil {
				return false, 0, nil, err
			}
			allDone, gap := advanceWithState(prev)
			if gap {
				return false, advancedStateCnt, bytes.Clone(currResumeKey), nil
			}
			if allDone {
				return true, advancedStateCnt, nil, nil
			}
		}

		for _, v := range values {
			state := &logreplicationpb.LogReplicationState{}
			if err := state.Unmarshal(v); err != nil {
				return false, 0, nil, err
			}
			allDone, gap := advanceWithState(state)
			if gap {
				return false, advancedStateCnt, bytes.Clone(currResumeKey), nil
			}
			if allDone {
				return true, advancedStateCnt, nil, nil
			}
		}

		// continue next scan if we have some progress
		if len(keys) == pkdbDefaultScanPageSize {
			continue
		}
		logutil.BgLogger().Info(
			"range gap found during wait for standby init",
			zap.String("from", redact.Key(currResumeKey)),
			zap.String("to", "infinity"),
		)
		return false, advancedStateCnt, bytes.Clone(currResumeKey), nil
	}
}

type standbyInitProgressTracker struct {
	pdHTTPCli pdhttp.Client
	etcdCli   clientv3.KV

	doneStateCnt int
	resumeKey    []byte
}

// newStandbyInitProgressTracker does not take ownership of given clients.
func newStandbyInitProgressTracker(pdHTTPCli pdhttp.Client, etcdCli clientv3.KV) *standbyInitProgressTracker {
	return &standbyInitProgressTracker{
		pdHTTPCli: pdHTTPCli,
		etcdCli:   etcdCli,
	}
}

func (t *standbyInitProgressTracker) estimateTotalRegionCnt(ctx context.Context) (int, error) {
	stats, err := t.pdHTTPCli.GetRegionStatusByKeyRange(ctx, pdhttp.NewKeyRange(nil, nil), true)
	if err != nil {
		return 0, err
	}
	if stats == nil || stats.Count <= 0 {
		return 0, errors.New("get region count returned non-positive count")
	}
	return stats.Count, nil
}

func (t *standbyInitProgressTracker) checkInitedAndUpdateProgress(
	ctx context.Context,
	cli rawKVScanClient,
	estRegionCnt int,
) (bool, error) {
	allDone, advancedStateCnt, resumeKey, err := checkInited(ctx, cli, t.resumeKey)
	if err != nil {
		return false, err
	}
	t.doneStateCnt += advancedStateCnt
	t.resumeKey = resumeKey
	if allDone {
		_, err = t.etcdCli.Put(ctx, constants.PkdbInitPercentage, "100")
		if err != nil {
			return false, err
		}
		return true, nil
	}

	percentage := t.percentage(estRegionCnt)
	_, err = t.etcdCli.Put(ctx, constants.PkdbInitPercentage, strconv.FormatFloat(percentage, 'f', -1, 32))
	return false, err
}

func (t *standbyInitProgressTracker) percentage(estRegionCnt int) float64 {
	if estRegionCnt <= 0 {
		return 0
	}
	percentage := float64(t.doneStateCnt) / float64(estRegionCnt) * 100
	// Only write 100% once standby is fully inited.
	if percentage >= 100 {
		return float64(t.doneStateCnt) / float64(t.doneStateCnt+1) * 100
	}
	return percentage
}

func scanReplStates(ctx context.Context, cli rawKVScanClient) ([]*logreplicationpb.LogReplicationState, error) {
	// TODO(lance6716): This scans the whole lr-state CF and materializes all
	// states into memory. In log replication workflows this may be invoked in
	// retry loops (e.g. resetTS), so a large region count can cause
	// repeated large allocations and OOM. Consider a streaming scan API that
	// yields states to a caller-provided callback.
	return scanReplStatesInRange(ctx, cli, nil, nil)
}

// scanReplStatesInRange returns the covering log replication states for
// [startKey, endKey). It does not guarantee the returned states are continuous,
// but different call sites may have enough knowledge to know that. For startKey
// which is not minKey(""), it will prepend one previous state to covers startKey
// to avoid false "gap" reports.
func scanReplStatesInRange(
	ctx context.Context,
	cli rawKVScanClient,
	startKey, endKey []byte,
) ([]*logreplicationpb.LogReplicationState, error) {
	var states []*logreplicationpb.LogReplicationState

	scanKey := startKey
	for {
		keys, values, err := cli.Scan(ctx, scanKey, endKey, pkdbDefaultScanPageSize, rawkv.SetColumnFamily(logReplStateCf))
		if err != nil {
			return nil, err
		}
		if len(keys) == 0 {
			break
		}
		for _, v := range values {
			pb := &logreplicationpb.LogReplicationState{}
			err := pb.Unmarshal(v)
			if err != nil {
				return nil, err
			}
			states = append(states, pb)
		}
		scanKey = keys[len(keys)-1]
		scanKey = append(scanKey, 0)
	}

	// The lr-state CF is usually keyed by range start key, but a state whose key
	// is < startKey can still cover startKey (e.g. after split/merge changes). If
	// the first scanned state starts after startKey, include one previous state
	// to avoid false "gap" reports.
	if len(startKey) > 0 && (len(states) == 0 || bytes.Compare(states[0].StartKey, startKey) > 0) {
		_, prevValues, err := cli.ReverseScan(ctx, startKey, nil, 1, rawkv.SetColumnFamily(logReplStateCf))
		if err != nil {
			return nil, err
		}
		if len(prevValues) > 0 {
			prev := &logreplicationpb.LogReplicationState{}
			if err := prev.Unmarshal(prevValues[0]); err != nil {
				return nil, err
			}
			if len(prev.EndKey) == 0 || bytes.Compare(prev.EndKey, startKey) > 0 {
				states = append([]*logreplicationpb.LogReplicationState{prev}, states...)
			}
		}
	}
	return states, nil
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
