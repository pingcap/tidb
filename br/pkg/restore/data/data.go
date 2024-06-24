// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.
package data

import (
	"context"
	"io"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	recovpb "github.com/pingcap/kvproto/pkg/recoverdatapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/common"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/utils/storewatch"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/util"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/rangetask"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

const gRPCBackOffMaxDelay = 3 * time.Second

type RecoveryStage int

const (
	StageUnknown RecoveryStage = iota
	StageCollectingMeta
	StageMakingRecoveryPlan
	StageResetPDAllocateID
	StageRecovering
	StageFlashback
)

func (s RecoveryStage) String() string {
	switch s {
	case StageCollectingMeta:
		return "collecting meta"
	case StageMakingRecoveryPlan:
		return "making recovery plan"
	case StageResetPDAllocateID:
		return "resetting PD allocate ID"
	case StageRecovering:
		return "recovering"
	case StageFlashback:
		return "flashback"
	default:
		return "unknown"
	}
}

type recoveryError struct {
	error
	atStage RecoveryStage
}

func FailedAt(err error) RecoveryStage {
	if rerr, ok := err.(recoveryError); ok {
		return rerr.atStage
	}
	return StageUnknown
}

type recoveryBackoffer struct {
	state utils.RetryState
}

func newRecoveryBackoffer() *recoveryBackoffer {
	return &recoveryBackoffer{
		state: utils.InitialRetryState(16, 30*time.Second, 4*time.Minute),
	}
}

func (bo *recoveryBackoffer) NextBackoff(err error) time.Duration {
	s := FailedAt(err)
	switch s {
	case StageCollectingMeta, StageMakingRecoveryPlan, StageResetPDAllocateID, StageRecovering:
		log.Info("Recovery data retrying.", zap.Error(err), zap.Stringer("stage", s))
		return bo.state.ExponentialBackoff()
	case StageFlashback:
		log.Info("Giving up retry for flashback stage.", zap.Error(err), zap.Stringer("stage", s))
		bo.state.GiveUp()
		return 0
	}
	log.Warn("unknown stage of backing off.", zap.Int("val", int(s)))
	return bo.state.ExponentialBackoff()
}

func (bo *recoveryBackoffer) Attempt() int {
	return bo.state.Attempt()
}

// RecoverData recover the tikv cluster
// 1. read all meta data from tikvs
// 2. make recovery plan and then recovery max allocate ID firstly
// 3. send the recover plan and the wait tikv to apply, in waitapply, all assigned region leader will check apply log to the last log
// 4. ensure all region apply to last log
// 5. prepare the flashback
// 6. flashback to resolveTS
func RecoverData(ctx context.Context, resolveTS uint64, allStores []*metapb.Store, mgr *conn.Mgr, progress glue.Progress, restoreTS uint64, concurrency uint32) (int, error) {
	// Roughly handle the case that some TiKVs are rebooted during making plan.
	// Generally, retry the whole procedure will be fine for most cases. But perhaps we can do finer-grained retry,
	// say, we may reuse the recovery plan, and probably no need to rebase PD allocation ID once we have done it.
	return utils.WithRetryV2(ctx, newRecoveryBackoffer(), func(ctx context.Context) (int, error) {
		return doRecoveryData(ctx, resolveTS, allStores, mgr, progress, restoreTS, concurrency)
	})
}

func doRecoveryData(ctx context.Context, resolveTS uint64, allStores []*metapb.Store, mgr *conn.Mgr, progress glue.Progress, restoreTS uint64, concurrency uint32) (int, error) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	var recovery = NewRecovery(allStores, mgr, progress, concurrency)
	if err := recovery.ReadRegionMeta(ctx); err != nil {
		return 0, recoveryError{error: err, atStage: StageCollectingMeta}
	}

	totalRegions := recovery.GetTotalRegions()

	if err := recovery.MakeRecoveryPlan(); err != nil {
		return totalRegions, recoveryError{error: err, atStage: StageMakingRecoveryPlan}
	}

	log.Info("recover the alloc id to pd", zap.Uint64("max alloc id", recovery.MaxAllocID))
	if err := recovery.mgr.RecoverBaseAllocID(ctx, recovery.MaxAllocID); err != nil {
		return totalRegions, recoveryError{error: err, atStage: StageResetPDAllocateID}
	}

	// Once TiKV shuts down and reboot then, it may be left with no leader because of the recovery mode.
	// This wathcher will retrigger `RecoveryRegions` for those stores.
	recovery.SpawnTiKVShutDownWatchers(ctx)
	if err := recovery.RecoverRegions(ctx); err != nil {
		return totalRegions, recoveryError{error: err, atStage: StageRecovering}
	}

	if err := recovery.PrepareFlashbackToVersion(ctx, resolveTS, restoreTS-1); err != nil {
		return totalRegions, recoveryError{error: err, atStage: StageFlashback}
	}

	if err := recovery.FlashbackToVersion(ctx, resolveTS, restoreTS); err != nil {
		return totalRegions, recoveryError{error: err, atStage: StageFlashback}
	}

	return totalRegions, nil
}

type StoreMeta struct {
	StoreId     uint64
	RegionMetas []*recovpb.RegionMeta
}

func NewStoreMeta(storeId uint64) StoreMeta {
	var meta = make([]*recovpb.RegionMeta, 0)
	return StoreMeta{storeId, meta}
}

// for test
type Recovery struct {
	allStores    []*metapb.Store
	StoreMetas   []StoreMeta
	RecoveryPlan map[uint64][]*recovpb.RecoverRegionRequest
	MaxAllocID   uint64
	mgr          *conn.Mgr
	progress     glue.Progress
	concurrency  uint32
}

func NewRecovery(allStores []*metapb.Store, mgr *conn.Mgr, progress glue.Progress, concurrency uint32) Recovery {
	totalStores := len(allStores)
	var StoreMetas = make([]StoreMeta, totalStores)
	var regionRecovers = make(map[uint64][]*recovpb.RecoverRegionRequest, totalStores)
	return Recovery{
		allStores:    allStores,
		StoreMetas:   StoreMetas,
		RecoveryPlan: regionRecovers,
		MaxAllocID:   0,
		mgr:          mgr,
		progress:     progress,
		concurrency:  concurrency}
}

func (recovery *Recovery) newRecoveryClient(ctx context.Context, storeAddr string) (recovpb.RecoverDataClient, *grpc.ClientConn, error) {
	// Connect to the Recovery service on the given TiKV node.
	bfConf := backoff.DefaultConfig
	bfConf.MaxDelay = gRPCBackOffMaxDelay

	conn, err := utils.GRPCConn(ctx, storeAddr, recovery.mgr.GetTLSConfig(),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: bfConf}),
		grpc.WithKeepaliveParams(recovery.mgr.GetKeepalive()),
	)
	if err != nil {
		return nil, conn, errors.Trace(err)
	}

	client := recovpb.NewRecoverDataClient(conn)

	return client, conn, nil
}

func getStoreAddress(allStores []*metapb.Store, storeId uint64) string {
	var addr string
	for _, store := range allStores {
		if store.GetId() == storeId {
			addr = store.GetAddress()
		}
	}

	if len(addr) == 0 {
		log.Error("there is no tikv has this Id")
	}
	return addr
}

// ReadRegionMeta read all region meta from tikvs
func (recovery *Recovery) ReadRegionMeta(ctx context.Context) error {
	eg, ectx := errgroup.WithContext(ctx)
	totalStores := len(recovery.allStores)
	workers := util.NewWorkerPool(uint(min(totalStores, common.MaxStoreConcurrency)), "Collect Region Meta") // TODO: int overflow?

	// TODO: optimize the ErrorGroup when TiKV is panic
	metaChan := make(chan StoreMeta, 1024)
	defer close(metaChan)

	for i := 0; i < totalStores; i++ {
		i := i
		storeId := recovery.allStores[i].GetId()
		storeAddr := recovery.allStores[i].GetAddress()

		if err := ectx.Err(); err != nil {
			break
		}

		workers.ApplyOnErrorGroup(eg, func() error {
			recoveryClient, conn, err := recovery.newRecoveryClient(ectx, storeAddr)
			if err != nil {
				return errors.Trace(err)
			}
			defer conn.Close()
			log.Info("read meta from tikv", zap.String("tikv address", storeAddr), zap.Uint64("store id", storeId))
			stream, err := recoveryClient.ReadRegionMeta(ectx, &recovpb.ReadRegionMetaRequest{StoreId: storeId})
			if err != nil {
				log.Error("read region meta failed", zap.Uint64("store id", storeId))
				return errors.Trace(err)
			}

			storeMeta := NewStoreMeta(storeId)
			// for a TiKV, received the stream
			for {
				var meta *recovpb.RegionMeta
				if meta, err = stream.Recv(); err == nil {
					storeMeta.RegionMetas = append(storeMeta.RegionMetas, meta)
				} else if err == io.EOF {
					//read to end of stream or server close the connection.
					break
				} else {
					return errors.Trace(err)
				}
			}

			metaChan <- storeMeta
			return nil
		})
	}

	for i := 0; i < totalStores; i++ {
		select {
		case <-ectx.Done(): // err or cancel, eg.wait will catch the error
			break
		case storeMeta := <-metaChan:
			recovery.StoreMetas[i] = storeMeta
			log.Info("received region meta from", zap.Int("store", int(storeMeta.StoreId)))
		}

		recovery.progress.Inc()
	}

	return eg.Wait()
}

func (recovery *Recovery) GetTotalRegions() int {
	// Group region peer info by region id.
	var regions = make(map[uint64]struct{}, 0)
	for _, v := range recovery.StoreMetas {
		for _, m := range v.RegionMetas {
			if _, ok := regions[m.RegionId]; !ok {
				regions[m.RegionId] = struct{}{}
			}
		}
	}
	return len(regions)
}

func (recovery *Recovery) RecoverRegionOfStore(ctx context.Context, storeID uint64, plan []*recovpb.RecoverRegionRequest) error {
	storeAddr := getStoreAddress(recovery.allStores, storeID)
	recoveryClient, conn, err := recovery.newRecoveryClient(ctx, storeAddr)
	if err != nil {
		log.Error("create tikv client failed", zap.Uint64("store id", storeID))
		return errors.Trace(err)
	}
	defer conn.Close()
	log.Info("send recover region to tikv", zap.String("tikv address", storeAddr), zap.Uint64("store id", storeID))
	stream, err := recoveryClient.RecoverRegion(ctx)
	if err != nil {
		log.Error("create recover region failed", zap.Uint64("store id", storeID))
		return errors.Trace(err)
	}

	// for a TiKV, send the stream
	for _, s := range plan {
		if err = stream.Send(s); err != nil {
			log.Error("send recover region failed", zap.Error(err))
			return errors.Trace(err)
		}
	}

	reply, err := stream.CloseAndRecv()
	if err != nil {
		log.Error("close the stream failed")
		return errors.Trace(err)
	}
	recovery.progress.Inc()
	log.Info("recover region execution success", zap.Uint64("store id", reply.GetStoreId()))
	return nil
}

// RecoverRegions send the recovery plan to recovery region (force leader etc)
// only tikvs have regions whose have to recover be sent
func (recovery *Recovery) RecoverRegions(ctx context.Context) (err error) {
	eg, ectx := errgroup.WithContext(ctx)
	totalRecoveredStores := len(recovery.RecoveryPlan)
	workers := util.NewWorkerPool(uint(min(totalRecoveredStores, common.MaxStoreConcurrency)), "Recover Regions")

	for storeId, plan := range recovery.RecoveryPlan {
		if err := ectx.Err(); err != nil {
			break
		}
		storeId := storeId
		plan := plan

		workers.ApplyOnErrorGroup(eg, func() error {
			return recovery.RecoverRegionOfStore(ectx, storeId, plan)
		})
	}
	// Wait for all TiKV instances force leader and wait apply to last log.
	return eg.Wait()
}

func (recovery *Recovery) SpawnTiKVShutDownWatchers(ctx context.Context) {
	rebootStores := map[uint64]struct{}{}
	cb := storewatch.MakeCallback(storewatch.WithOnReboot(func(s *metapb.Store) {
		log.Info("Store reboot detected, will regenerate leaders.", zap.Uint64("id", s.GetId()))
		rebootStores[s.Id] = struct{}{}
	}), storewatch.WithOnDisconnect(func(s *metapb.Store) {
		log.Warn("A store disconnected.", zap.Uint64("id", s.GetId()), zap.String("addr", s.GetAddress()))
	}), storewatch.WithOnNewStoreRegistered(func(s *metapb.Store) {
		log.Info("Start to observing the state of store.", zap.Uint64("id", s.GetId()))
	}))
	watcher := storewatch.New(recovery.mgr.PDClient(), cb)
	tick := time.NewTicker(30 * time.Second)
	mainLoop := func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				err := watcher.Step(ctx)
				if err != nil {
					log.Warn("Failed to step watcher.", logutil.ShortError(err))
				}
				for id := range rebootStores {
					plan, ok := recovery.RecoveryPlan[id]
					if !ok {
						log.Warn("Store reboot detected, but no recovery plan found.", zap.Uint64("id", id))
						continue
					}
					err := recovery.RecoverRegionOfStore(ctx, id, plan)
					if err != nil {
						log.Warn("Store reboot detected, but failed to regenerate leader.", zap.Uint64("id", id), logutil.ShortError(err))
						continue
					}
					log.Info("Succeed to reload the leader in store.", zap.Uint64("id", id))
					delete(rebootStores, id)
				}
			}
		}
	}

	go mainLoop()
}

// prepare the region for flashback the data, the purpose is to stop region service, put region in flashback state
func (recovery *Recovery) PrepareFlashbackToVersion(ctx context.Context, resolveTS uint64, startTS uint64) (err error) {
	retryState := utils.InitialRetryState(utils.FlashbackRetryTime, utils.FlashbackWaitInterval, utils.FlashbackMaxWaitInterval)
	retryErr := utils.WithRetry(
		ctx,
		func() error {
			handler := func(ctx context.Context, r tikvstore.KeyRange) (rangetask.TaskStat, error) {
				stats, err := ddl.SendPrepareFlashbackToVersionRPC(ctx, recovery.mgr.GetStorage().(tikv.Storage), resolveTS, startTS, r)
				if err != nil {
					log.Warn("region may not ready to serve, retry it...", zap.Error(err))
				}
				return stats, err
			}

			runner := rangetask.NewRangeTaskRunner("br-flashback-prepare-runner", recovery.mgr.GetStorage().(tikv.Storage), int(recovery.concurrency), handler)
			// Run prepare flashback on the entire TiKV cluster. Empty keys means the range is unbounded.
			err = runner.RunOnRange(ctx, []byte(""), []byte(""))
			if err != nil {
				log.Warn("region flashback prepare get error")
				return errors.Trace(err)
			}
			log.Info("region flashback prepare complete", zap.Int("regions", runner.CompletedRegions()))
			return nil
		}, &retryState)

	recovery.progress.Inc()
	return retryErr
}

// flashback the region data to version resolveTS
func (recovery *Recovery) FlashbackToVersion(ctx context.Context, resolveTS uint64, commitTS uint64) (err error) {
	handler := func(ctx context.Context, r tikvstore.KeyRange) (rangetask.TaskStat, error) {
		stats, err := ddl.SendFlashbackToVersionRPC(ctx, recovery.mgr.GetStorage().(tikv.Storage), resolveTS, commitTS-1, commitTS, r)
		return stats, err
	}

	runner := rangetask.NewRangeTaskRunner("br-flashback-runner", recovery.mgr.GetStorage().(tikv.Storage), int(recovery.concurrency), handler)
	// Run flashback on the entire TiKV cluster. Empty keys means the range is unbounded.
	err = runner.RunOnRange(ctx, []byte(""), []byte(""))
	if err != nil {
		log.Error("region flashback get error",
			zap.Uint64("resolveTS", resolveTS),
			zap.Uint64("commitTS", commitTS),
			zap.Int("regions", runner.CompletedRegions()))
		return errors.Trace(err)
	}

	log.Info("region flashback complete",
		zap.Uint64("resolveTS", resolveTS),
		zap.Uint64("commitTS", commitTS),
		zap.Int("regions", runner.CompletedRegions()))

	recovery.progress.Inc()
	return nil
}

type RecoverRegion struct {
	*recovpb.RegionMeta
	StoreId uint64
}

// generate the related the recovery plan to tikvs:
// 1. check overlap the region, make a recovery decision
// 2. build a leader list for all region during the tikv startup
// 3. get max allocate id
func (recovery *Recovery) MakeRecoveryPlan() error {
	storeBalanceScore := make(map[uint64]int, len(recovery.allStores))
	// Group region peer info by region id. find the max allocateId
	// region [id] [peer[0-n]]
	var regions = make(map[uint64][]*RecoverRegion, 0)
	for _, v := range recovery.StoreMetas {
		storeId := v.StoreId
		maxId := storeId
		for _, m := range v.RegionMetas {
			if regions[m.RegionId] == nil {
				regions[m.RegionId] = make([]*RecoverRegion, 0, len(recovery.allStores))
			}
			regions[m.RegionId] = append(regions[m.RegionId], &RecoverRegion{m, storeId})
			maxId = max(maxId, max(m.RegionId, m.PeerId))
		}
		recovery.MaxAllocID = max(recovery.MaxAllocID, maxId)
	}

	regionInfos := SortRecoverRegions(regions)

	validPeers, err := CheckConsistencyAndValidPeer(regionInfos)
	if err != nil {
		return errors.Trace(err)
	}

	// all plans per region key=StoreId, value=reqs stream
	//regionsPlan := make(map[uint64][]*recovmetapb.RecoveryCmdRequest, 0)
	// Generate recover commands.
	for regionId, peers := range regions {
		if _, ok := validPeers[regionId]; !ok {
			// TODO: Generate a tombstone command.
			// 1, peer is tombstone
			// 2, split region in progressing, old one can be a tombstone
			log.Warn("detected tombstone peer for region", zap.Uint64("region id", regionId))
			for _, peer := range peers {
				plan := &recovpb.RecoverRegionRequest{Tombstone: true, AsLeader: false}
				recovery.RecoveryPlan[peer.StoreId] = append(recovery.RecoveryPlan[peer.StoreId], plan)
			}
		} else {
			// Generate normal commands.
			log.Debug("detected valid region", zap.Uint64("region id", regionId))
			// calc the leader candidates
			leaderCandidates, err := LeaderCandidates(peers)
			if err != nil {
				log.Warn("region without peer", zap.Uint64("region id", regionId))
				return errors.Trace(err)
			}

			// select the leader base on tikv storeBalanceScore
			leader := SelectRegionLeader(storeBalanceScore, leaderCandidates)
			log.Debug("as leader peer", zap.Uint64("store id", leader.StoreId), zap.Uint64("region id", leader.RegionId))
			plan := &recovpb.RecoverRegionRequest{RegionId: leader.RegionId, AsLeader: true}
			recovery.RecoveryPlan[leader.StoreId] = append(recovery.RecoveryPlan[leader.StoreId], plan)
			storeBalanceScore[leader.StoreId] += 1
		}
	}
	return nil
}
