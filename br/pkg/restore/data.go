// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.
package restore

import (
	"context"
	"io"
	"sort"
	"time"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	recovpb "github.com/pingcap/kvproto/pkg/recoverdatapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/conn"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/utils"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// recover the tikv cluster
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

	totalRegions := recovery.getTotalRegions()

	if err := recovery.makeRecoveryPlan(); err != nil {
		return totalRegions, errors.Trace(err)
	}

	log.Info("recover the alloc id to pd", zap.Uint64("max alloc id", recovery.maxAllocID))
	if err := recovery.mgr.RecoverBaseAllocID(ctx, recovery.maxAllocID); err != nil {
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
	storeId     uint64
	regionMetas []*recovpb.RegionMeta
}

func NewStoreMeta(storeId uint64) StoreMeta {
	var meta = make([]*recovpb.RegionMeta, 0)
	return StoreMeta{storeId, meta}
}

type Recovery struct {
	allStores    []*metapb.Store
	storeMetas   []StoreMeta
	recoveryPlan map[uint64][]*recovpb.RecoverRegionRequest
	maxAllocID   uint64
	mgr          *conn.Mgr
	progress     glue.Progress
}

func NewRecovery(allStores []*metapb.Store, mgr *conn.Mgr, progress glue.Progress) Recovery {
	totalStores := len(allStores)
	var storeMetas = make([]StoreMeta, totalStores)
	var regionRecovers = make(map[uint64][]*recovpb.RecoverRegionRequest, totalStores)
	return Recovery{
		allStores:    allStores,
		storeMetas:   storeMetas,
		recoveryPlan: regionRecovers,
		maxAllocID:   0,
		mgr:          mgr,
		progress:     progress}
}

func (recovery *Recovery) newTiKVRecoveryClient(ctx context.Context, tikvAddr string) (recovpb.RecoverDataClient, *grpc.ClientConn, error) {
	// Connect to the Recovery service on the given TiKV node.
	bfConf := backoff.DefaultConfig
	bfConf.MaxDelay = gRPCBackOffMaxDelay
	opt := grpc.WithInsecure()
	if recovery.mgr.GetTLSConfig() != nil {
		opt = grpc.WithTransportCredentials(credentials.NewTLS(recovery.mgr.GetTLSConfig()))
	}
	//TODO: conneciton may need some adjust
	//keepaliveConf keepalive.ClientParameters
	conn, err := grpc.DialContext(
		ctx,
		tikvAddr,
		opt,
		grpc.WithBlock(),
		grpc.FailOnNonTempDialError(true),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: bfConf}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    time.Duration(10) * time.Second,
			Timeout: time.Duration(300) * time.Second,
		}),
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

// read all region meta from tikvs
func (recovery *Recovery) ReadRegionMeta(ctx context.Context) error {
	eg, ectx := errgroup.WithContext(ctx)
	totalStores := len(recovery.allStores)
	workers := utils.NewWorkerPool(uint(totalStores), "Collect Region Meta")

	// TODO: optimize the ErroGroup when TiKV is panic
	metaChan := make(chan StoreMeta, 1024)
	defer close(metaChan)

	for i := 0; i < totalStores; i++ {
		i := i
		storeId := recovery.allStores[i].GetId()
		storeAddr := recovery.allStores[i].GetAddress()

		if err := ectx.Err(); err != nil {
			return errors.Trace(err)
		}

		tikvClient, conn, err := recovery.newTiKVRecoveryClient(ectx, storeAddr)
		if err != nil {
			return errors.Trace(err)
		}
		defer conn.Close()

		workers.ApplyOnErrorGroup(eg, func() error {
			log.Info("read meta from tikv", zap.String("tikv address", storeAddr), zap.Uint64("store id", storeId))
			stream, err := tikvClient.ReadRegionMeta(ectx, &recovpb.ReadRegionMetaRequest{StoreId: storeId})
			if err != nil {
				log.Error("read region meta failied", zap.Uint64("storeID", storeId))
				return errors.Trace(err)
			}

			storeMeta := NewStoreMeta(storeId)
			// for a TiKV, received the stream
			for {
				var meta *recovpb.RegionMeta
				if meta, err = stream.Recv(); err == nil {
					storeMeta.regionMetas = append(storeMeta.regionMetas, meta)
				} else if err == io.EOF {
					break
				} else if err != nil {
					return errors.Trace(err)
				}
			}

			metaChan <- storeMeta
			return nil
		})
	}

	for i := 0; i < totalStores; i++ {
		select {
		case <-ctx.Done(): // cancel the main thread
			break
		case <-ectx.Done(): // err or cancel, eg.wait will catch the error
			break
		case storeMeta := <-metaChan:
			recovery.storeMetas[i] = storeMeta
			log.Info("receieved region meta from", zap.Int("store", int(storeMeta.storeId)))
		}

		recovery.progress.Inc()
	}

	return eg.Wait()
}

func (recovery *Recovery) getTotalRegions() int {
	// Group region peer info by region id.
	var regions = make(map[uint64][]Regions, 0)
	for _, v := range recovery.storeMetas {
		storeId := v.storeId
		for _, m := range v.regionMetas {
			if regions[m.RegionId] == nil {
				regions[m.RegionId] = make([]Regions, 0, len(recovery.allStores))
			}
			regions[m.RegionId] = append(regions[m.RegionId], Regions{m, storeId})
		}
	}
	return len(regions)
}

// send the recovery plan to recovery region (force leader etc)
// only tikvs have regions whose have to recover be sent
func (recovery *Recovery) RecoverRegions(ctx context.Context) (err error) {
	eg, ectx := errgroup.WithContext(ctx)
	totalRecoveredStores := len(recovery.recoveryPlan)
	workers := utils.NewWorkerPool(uint(totalRecoveredStores), "Recover Regions")

	for storeId, plan := range recovery.recoveryPlan {
		if err := ectx.Err(); err != nil {
			return errors.Trace(err)
		}

		storeAddr := getStoreAddress(recovery.allStores, storeId)
		tikvClient, conn, err := recovery.newTiKVRecoveryClient(ectx, storeAddr)
		if err != nil {
			log.Error("create tikv client failed", zap.Uint64("storeID", storeId))
			return errors.Trace(err)
		}
		defer conn.Close()
		cmd := plan
		storeId := storeId
		workers.ApplyOnErrorGroup(eg, func() error {
			log.Info("send recover cmd to tikv", zap.String("tikv address", storeAddr), zap.Uint64("store id", storeId))
			stream, err := tikvClient.RecoverRegion(ectx)
			if err != nil {
				log.Error("create recover cmd failed", zap.Uint64("storeID", storeId))
				return errors.Trace(err)
			}

			// for a TiKV, send the stream
			for _, s := range cmd {
				if err = stream.Send(s); err != nil {
					log.Error("send region recovery command failed", zap.Error(err))
					return errors.Trace(err)
				}
			}

			reply, err := stream.CloseAndRecv()
			if err != nil {
				log.Error("close the stream failed")
				return errors.Trace(err)
			}
			recovery.progress.Inc()
			log.Info("recovery command execution success", zap.Uint64("storeID", reply.GetStoreId()))
			return nil
		})
	}
	// Wait for all TiKV instances force leader and wait apply to last log.
	return eg.Wait()
}

// send wait apply to all tikv ensure all region peer apply log into the last
func (recovery *Recovery) WaitApply(ctx context.Context) (err error) {
	eg, ectx := errgroup.WithContext(ctx)
	totalStores := len(recovery.allStores)
	workers := utils.NewWorkerPool(uint(totalStores), "wait apply")

	for _, store := range recovery.allStores {
		if err := ectx.Err(); err != nil {
			return errors.Trace(err)
		}
		storeAddr := getStoreAddress(recovery.allStores, store.Id)
		tikvClient, conn, err := recovery.newTiKVRecoveryClient(ectx, storeAddr)
		if err != nil {
			return errors.Trace(err)
		}
		defer conn.Close()
		storeId := store.Id

		workers.ApplyOnErrorGroup(eg, func() error {
			log.Info("send wait apply to tikv", zap.String("tikv address", storeAddr), zap.Uint64("store id", storeId))
			req := &recovpb.WaitApplyRequest{StoreId: storeId}
			_, err := tikvClient.WaitApply(ectx, req)
			if err != nil {
				log.Error("wait apply failed", zap.Uint64("storeID", storeId))
				return errors.Trace(err)
			}

			recovery.progress.Inc()
			log.Info("recovery wait apply execution success", zap.Uint64("storeID", storeId))
			return nil
		})
	}
	// Wait for all TiKV instances force leader and wait apply to last log.
	return eg.Wait()
}

// a worker pool to all tikv for execute delete all data whose has ts > resolvedTs
func (recovery *Recovery) ResolveData(ctx context.Context, resolvedTs uint64) (err error) {

	eg, ectx := errgroup.WithContext(ctx)
	totalStores := len(recovery.allStores)
	workers := utils.NewWorkerPool(uint(totalStores), "resolve data from tikv")

	// TODO: what if the resolved data take long time take long time?, it look we need some handling here, at least some retry may neccessary
	for _, store := range recovery.allStores {
		if err := ectx.Err(); err != nil {
			return errors.Trace(err)
		}
		storeAddr := getStoreAddress(recovery.allStores, store.Id)
		tikvClient, conn, err := recovery.newTiKVRecoveryClient(ectx, storeAddr)
		if err != nil {
			return errors.Trace(err)
		}
		defer conn.Close()
		storeId := store.Id
		workers.ApplyOnErrorGroup(eg, func() error {
			log.Info("resolved data to tikv", zap.String("tikv address", storeAddr), zap.Uint64("store id", storeId))
			req := &recovpb.ResolveKvDataRequest{ResolvedTs: resolvedTs}
			stream, err := tikvClient.ResolveKvData(ectx, req)
			if err != nil {
				log.Error("send the resolve kv data failed", zap.Uint64("storeID", storeId))
				return errors.Trace(err)
			}
			// for a TiKV, received the stream
			for {
				var resp *recovpb.ResolveKvDataResponse
				if resp, err = stream.Recv(); err == nil {
					log.Info("current delete key", zap.Uint64("resolved key num", resp.ResolvedKeyCount), zap.Uint64("store id", resp.StoreId))
				} else if err == io.EOF {
					break
				} else if err != nil {
					return errors.Trace(err)
				}
			}
			recovery.progress.Inc()
			log.Info("resolvedkv data done", zap.String("tikv address", storeAddr), zap.Uint64("store id", storeId))
			return nil
		})
	}
	// Wait for all TiKV instances finished
	return eg.Wait()

}

type Regions struct {
	*recovpb.RegionMeta
	storeId uint64
}

// generate the related the recovery plan to tikvs:
// 1. check overlap the region, make a recovery decision
// 2. build a leader list for all region during the tikv startup
// 3. get max allocate id
func (recovery *Recovery) makeRecoveryPlan() error {

	// Group region peer info by region id. find the max allcateId
	// region [id] [peer[0-n]]
	var regions = make(map[uint64][]Regions, 0)
	for _, v := range recovery.storeMetas {
		storeId := v.storeId
		maxId := storeId
		for _, m := range v.regionMetas {
			if regions[m.RegionId] == nil {
				regions[m.RegionId] = make([]Regions, 0, len(recovery.allStores))
			}
			regions[m.RegionId] = append(regions[m.RegionId], Regions{m, storeId})
			maxId = utils.Max(maxId, utils.Max(m.RegionId, m.PeerId))
		}
		recovery.maxAllocID = utils.Max(recovery.maxAllocID, maxId)
	}

	// last log term -> last index -> commit index
	cmps := []func(a, b *Regions) int{
		func(a, b *Regions) int {
			return int(a.GetLastLogTerm() - b.GetLastLogTerm())
		},
		func(a, b *Regions) int {
			return int(a.GetLastIndex() - b.GetLastIndex())
		},
		func(a, b *Regions) int {
			return int(a.GetCommitIndex() - b.GetCommitIndex())
		},
	}

	type Region struct {
		regionId      uint64
		regionVersion uint64
	}
	// Sort region peer by last log term -> last index -> commit index, and collect all regions' version.
	var versions = make([]Region, 0, len(regions))
	for regionId, peers := range regions {
		sort.Slice(peers, func(i, j int) bool {
			for _, cmp := range cmps {
				if v := cmp(&peers[i], &peers[j]); v != 0 {
					return v > 0
				}
			}
			return false
		})
		var v = peers[0].Version
		versions = append(versions, Region{regionId, v})
	}

	sort.Slice(versions, func(i, j int) bool { return versions[i].regionVersion > versions[j].regionVersion })

	type RegionEndKey struct {
		endKey []byte
		rid    uint64
	}
	// split and merge in progressing during the backup, there may some overlap region, we have to handle it
	// Resolve version conflicts.
	var topo = treemap.NewWith(keyCmpInterface)
	for _, p := range versions {
		var sk = prefixStartKey(regions[p.regionId][0].StartKey)
		var ek = prefixEndKey(regions[p.regionId][0].EndKey)
		var fk, fv interface{}
		fk, _ = topo.Ceiling(sk)
		// keysapce overlap sk within ceiling - fk
		if fk != nil && (keyEq(fk.([]byte), sk) || keyCmp(fk.([]byte), ek) < 0) {
			continue
		}

		// keysapce overlap sk within floor - fk.end_key
		fk, fv = topo.Floor(sk)
		if fk != nil && keyCmp(fv.(RegionEndKey).endKey, sk) > 0 {
			continue
		}
		topo.Put(sk, RegionEndKey{ek, p.regionId})
	}

	// After resolved, all validPeer regions shouldn't be tombstone.
	// do some sanity check
	var validPeer = make(map[uint64]struct{}, 0)
	var iter = topo.Iterator()
	var prevEndKey = prefixStartKey([]byte{})
	var prevR uint64 = 0
	for iter.Next() {
		v := iter.Value().(RegionEndKey)
		if regions[v.rid][0].Tombstone {
			log.Error("validPeer shouldn't be tombstone", zap.Uint64("regionID", v.rid))
			// TODO, some enhancement may need, a PoC or test may need for decision
			return errors.Annotatef(berrors.ErrRestoreInvalidPeer,
				"Peer shouldn't be tombstone")
		}
		if !keyEq(prevEndKey, iter.Key().([]byte)) {
			log.Error("region doesn't conject to region", zap.Uint64("preRegion", prevR), zap.Uint64("curRegion", v.rid))
			// TODO, some enhancement may need, a PoC or test may need for decision
			return errors.Annotatef(berrors.ErrRestoreInvalidRange,
				"invalid region range")
		}
		prevEndKey = v.endKey
		prevR = v.rid
		validPeer[v.rid] = struct{}{}
	}

	// all plans per region key=storeId, value=reqs stream
	//regionsPlan := make(map[uint64][]*recovmetapb.RecoveryCmdRequest, 0)
	// Generate recover commands.
	for regionId, peers := range regions {
		if _, ok := validPeer[regionId]; !ok {
			// TODO: Generate a tombstone command.
			// 1, peer is tomebstone
			// 2, split region in progressing, old one can be a tomebstone
			for _, peer := range peers {
				plan := &recovpb.RecoverRegionRequest{Tombstone: true, AsLeader: false}
				recovery.recoveryPlan[peer.storeId] = append(recovery.recoveryPlan[peer.storeId], plan)
			}
		} else {
			// Generate normal commands.
			log.Debug("valid peer", zap.Uint64("peer", regionId))
			for i, peer := range peers {
				log.Debug("make plan", zap.Uint64("storeid", peer.storeId), zap.Uint64("regionid", peer.RegionId))
				plan := &recovpb.RecoverRegionRequest{RegionId: peer.RegionId, AsLeader: (i == 0)}
				// sorted by log term -> last index -> commit index in a region
				if plan.AsLeader {
					log.Debug("as leader peer", zap.Uint64("storeid", peer.storeId), zap.Uint64("regionid", peer.RegionId))
					recovery.recoveryPlan[peer.storeId] = append(recovery.recoveryPlan[peer.storeId], plan)
				}
			}
		}
	}

	return nil
}

func prefixStartKey(key []byte) []byte {
	var sk = make([]byte, 0, len(key)+1)
	sk = append(sk, 'z')
	sk = append(sk, key...)
	return sk
}

func prefixEndKey(key []byte) []byte {
	if len(key) == 0 {
		return []byte{'z' + 1}
	}
	return prefixStartKey(key)
}

func keyEq(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func keyCmp(a, b []byte) int {
	var length = 0
	var chosen = 0
	if len(a) < len(b) {
		length = len(a)
		chosen = -1
	} else if len(a) == len(b) {
		length = len(a)
		chosen = 0
	} else {
		length = len(b)
		chosen = 1
	}
	for i := 0; i < length; i++ {
		if a[i] < b[i] {
			return -1
		} else if a[i] > b[i] {
			return 1
		}
	}
	return chosen
}

func keyCmpInterface(a, b interface{}) int {
	return keyCmp(a.([]byte), b.([]byte))
}
