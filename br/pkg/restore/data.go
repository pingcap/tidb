// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.
package restore

import (
	"context"
	"io"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	recovpb "github.com/pingcap/kvproto/pkg/recoverdatapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/common"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/util/mathutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

// RecoverData recover the tikv cluster
// 1. read all meta data from tikvs
// 2. make recovery plan and then recovery max allocate ID firstly
// 3. send the recover plan and the wait tikv to apply, in waitapply, all assigned region leader will check apply log to the last log
// 4. ensure all region apply to last log
// 5. send the resolvedTs to tikv for deleting data.
func RecoverData(ctx context.Context, resolvedTs uint64, allStores []*metapb.Store, mgr *conn.Mgr, progress glue.Progress) (int, error) {
	var recovery = NewRecovery(allStores, mgr, progress)
	if err := recovery.ReadRegionMeta(ctx); err != nil {
		return 0, errors.Trace(err)
	}

	totalRegions := recovery.GetTotalRegions()

	if err := recovery.MakeRecoveryPlan(); err != nil {
		return totalRegions, errors.Trace(err)
	}

	log.Info("recover the alloc id to pd", zap.Uint64("max alloc id", recovery.MaxAllocID))
	if err := recovery.mgr.RecoverBaseAllocID(ctx, recovery.MaxAllocID); err != nil {
		return totalRegions, errors.Trace(err)
	}

	if err := recovery.RecoverRegions(ctx); err != nil {
		return totalRegions, errors.Trace(err)
	}

	if err := recovery.WaitApply(ctx); err != nil {
		return totalRegions, errors.Trace(err)
	}

	if err := recovery.ResolveData(ctx, resolvedTs); err != nil {
		return totalRegions, errors.Trace(err)
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
}

func NewRecovery(allStores []*metapb.Store, mgr *conn.Mgr, progress glue.Progress) Recovery {
	totalStores := len(allStores)
	var StoreMetas = make([]StoreMeta, totalStores)
	var regionRecovers = make(map[uint64][]*recovpb.RecoverRegionRequest, totalStores)
	return Recovery{
		allStores:    allStores,
		StoreMetas:   StoreMetas,
		RecoveryPlan: regionRecovers,
		MaxAllocID:   0,
		mgr:          mgr,
		progress:     progress}
}

func (recovery *Recovery) newRecoveryClient(ctx context.Context, storeAddr string) (recovpb.RecoverDataClient, *grpc.ClientConn, error) {
	// Connect to the Recovery service on the given TiKV node.
	bfConf := backoff.DefaultConfig
	bfConf.MaxDelay = gRPCBackOffMaxDelay
	//TODO: connection may need some adjust
	//keepaliveConf keepalive.ClientParameters
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
	workers := utils.NewWorkerPool(uint(mathutil.Min(totalStores, common.MaxStoreConcurrency)), "Collect Region Meta") // TODO: int overflow?

	// TODO: optimize the ErroGroup when TiKV is panic
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

// TODO: map may be more suitable for this function

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

// RecoverRegions send the recovery plan to recovery region (force leader etc)
// only tikvs have regions whose have to recover be sent
func (recovery *Recovery) RecoverRegions(ctx context.Context) (err error) {
	eg, ectx := errgroup.WithContext(ctx)
	totalRecoveredStores := len(recovery.RecoveryPlan)
	workers := utils.NewWorkerPool(uint(mathutil.Min(totalRecoveredStores, common.MaxStoreConcurrency)), "Recover Regions")

	for storeId, plan := range recovery.RecoveryPlan {
		if err := ectx.Err(); err != nil {
			break
		}

		storeAddr := getStoreAddress(recovery.allStores, storeId)
		recoveryPlan := plan
		recoveryStoreId := storeId
		workers.ApplyOnErrorGroup(eg, func() error {
			recoveryClient, conn, err := recovery.newRecoveryClient(ectx, storeAddr)
			if err != nil {
				log.Error("create tikv client failed", zap.Uint64("store id", recoveryStoreId))
				return errors.Trace(err)
			}
			defer conn.Close()
			log.Info("send recover region to tikv", zap.String("tikv address", storeAddr), zap.Uint64("store id", recoveryStoreId))
			stream, err := recoveryClient.RecoverRegion(ectx)
			if err != nil {
				log.Error("create recover region failed", zap.Uint64("store id", recoveryStoreId))
				return errors.Trace(err)
			}

			// for a TiKV, send the stream
			for _, s := range recoveryPlan {
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
		})
	}
	// Wait for all TiKV instances force leader and wait apply to last log.
	return eg.Wait()
}

// WaitApply send wait apply to all tikv ensure all region peer apply log into the last
func (recovery *Recovery) WaitApply(ctx context.Context) (err error) {
	eg, ectx := errgroup.WithContext(ctx)
	totalStores := len(recovery.allStores)
	workers := utils.NewWorkerPool(uint(mathutil.Min(totalStores, common.MaxStoreConcurrency)), "wait apply")

	for _, store := range recovery.allStores {
		if err := ectx.Err(); err != nil {
			break
		}
		storeAddr := getStoreAddress(recovery.allStores, store.Id)
		storeId := store.Id

		workers.ApplyOnErrorGroup(eg, func() error {
			recoveryClient, conn, err := recovery.newRecoveryClient(ectx, storeAddr)
			if err != nil {
				return errors.Trace(err)
			}
			defer conn.Close()
			log.Info("send wait apply to tikv", zap.String("tikv address", storeAddr), zap.Uint64("store id", storeId))
			req := &recovpb.WaitApplyRequest{StoreId: storeId}
			_, err = recoveryClient.WaitApply(ectx, req)
			if err != nil {
				log.Error("wait apply failed", zap.Uint64("store id", storeId))
				return errors.Trace(err)
			}

			recovery.progress.Inc()
			log.Info("wait apply execution success", zap.Uint64("store id", storeId))
			return nil
		})
	}
	// Wait for all TiKV instances force leader and wait apply to last log.
	return eg.Wait()
}

// ResolveData a worker pool to all tikv for execute delete all data whose has ts > resolvedTs
func (recovery *Recovery) ResolveData(ctx context.Context, resolvedTs uint64) (err error) {
	eg, ectx := errgroup.WithContext(ctx)
	totalStores := len(recovery.allStores)
	workers := utils.NewWorkerPool(uint(mathutil.Min(totalStores, common.MaxStoreConcurrency)), "resolve data from tikv")

	// TODO: what if the resolved data take long time take long time?, it look we need some handling here, at least some retry may necessary
	// TODO: what if the network disturbing, a retry machanism may need here
	for _, store := range recovery.allStores {
		if err := ectx.Err(); err != nil {
			break
		}
		storeAddr := getStoreAddress(recovery.allStores, store.Id)
		storeId := store.Id
		workers.ApplyOnErrorGroup(eg, func() error {
			recoveryClient, conn, err := recovery.newRecoveryClient(ectx, storeAddr)
			if err != nil {
				return errors.Trace(err)
			}
			defer conn.Close()
			log.Info("resolve data to tikv", zap.String("tikv address", storeAddr), zap.Uint64("store id", storeId))
			req := &recovpb.ResolveKvDataRequest{ResolvedTs: resolvedTs}
			stream, err := recoveryClient.ResolveKvData(ectx, req)
			if err != nil {
				log.Error("send the resolve kv data failed", zap.Uint64("store id", storeId))
				return errors.Trace(err)
			}
			// for a TiKV, received the stream
			for {
				var resp *recovpb.ResolveKvDataResponse
				if resp, err = stream.Recv(); err == nil {
					log.Info("current delete key", zap.Uint64("resolved key num", resp.ResolvedKeyCount), zap.Uint64("store id", resp.StoreId))
				} else if err == io.EOF {
					break
				} else {
					return errors.Trace(err)
				}
			}
			recovery.progress.Inc()
			log.Info("resolve kv data done", zap.String("tikv address", storeAddr), zap.Uint64("store id", storeId))
			return nil
		})
	}
	// Wait for all TiKV instances force leader and wait apply to last log.
	return eg.Wait()
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
			maxId = mathutil.Max(maxId, mathutil.Max(m.RegionId, m.PeerId))
		}
		recovery.MaxAllocID = mathutil.Max(recovery.MaxAllocID, maxId)
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
			log.Debug("detected valid peer", zap.Uint64("region id", regionId))
			for i, peer := range peers {
				log.Debug("make plan", zap.Uint64("store id", peer.StoreId), zap.Uint64("region id", peer.RegionId))
				plan := &recovpb.RecoverRegionRequest{RegionId: peer.RegionId, AsLeader: i == 0}
				// sorted by log term -> last index -> commit index in a region
				if plan.AsLeader {
					log.Debug("as leader peer", zap.Uint64("store id", peer.StoreId), zap.Uint64("region id", peer.RegionId))
					recovery.RecoveryPlan[peer.StoreId] = append(recovery.RecoveryPlan[peer.StoreId], plan)
				}
			}
		}
	}
	return nil
}
