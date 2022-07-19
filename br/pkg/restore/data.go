// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.
package restore

import (
	"context"
	"crypto/tls"
	"io"
	"sort"
	"time"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	recovpb "github.com/pingcap/kvproto/pkg/recoverdatapb"
	"github.com/pingcap/log"
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
// 2. Assemble region meta and make recovery plan
// 3. send the recover plan and the wait tikv to apply, in waitapply, all assigned region leader will check and apply log to the last log
// 4. send the resolvedTs to tikv for deleting data.
func RecoverData(ctx context.Context, resolvedTs uint64, allStores []*metapb.Store, tls *tls.Config, dumpRegionInfo bool) (int, error) {

	numOfTiKVs := len(allStores)
	var recovery = NewRecovery(numOfTiKVs, tls)

	err := recovery.ReadRegionMeta(ctx, numOfTiKVs, allStores)
	if err != nil {
		return 0, errors.Trace(err)
	}

	if dumpRegionInfo {
		recovery.dumpRegionInfo()
	}

	totalRegions := recovery.getTotalRegions()

	recovery.makeRecoveryPlan()

	err = recovery.RecoverRegions(ctx, allStores)
	if err != nil {
		return totalRegions, errors.Trace(err)
	}

	// TODO Sleep is workaround for sync last index, it will be removed when TiKV part of work been done
	time.Sleep(time.Second * 3)

	err = recovery.ResolveData(ctx, allStores, resolvedTs)
	if err != nil {
		return totalRegions, errors.Trace(err)
	}
	return totalRegions, nil
}

type RecoveryMeta struct {
	storeId      uint64
	recoveryMeta []*recovpb.RegionMeta
}

func NewRecoveryMeta(storeId uint64) RecoveryMeta {
	var meta = make([]*recovpb.RegionMeta, 0)
	return RecoveryMeta{storeId, meta}
}

type RecoveryPlan struct {
	storeId      uint64
	recoveryPlan []*recovpb.RecoverCmdRequest
}

func NewRecoveryPlan(storeId uint64) RecoveryPlan {
	var meta = make([]*recovpb.RecoverCmdRequest, 0)
	return RecoveryPlan{storeId, meta}
}

type Recovery struct {
	totalStores  int
	regionMetas  []RecoveryMeta
	recoveryPlan map[uint64][]*recovpb.RecoverCmdRequest
	resolvedTs   *uint64
	tls          *tls.Config
}

func NewRecovery(totalStores int, tls *tls.Config) Recovery {
	var regionMetas = make([]RecoveryMeta, totalStores)
	var regionRecovers = make(map[uint64][]*recovpb.RecoverCmdRequest, totalStores)
	var resolvedTs = new(uint64)
	return Recovery{totalStores, regionMetas, regionRecovers, resolvedTs, tls}
}

func (recovery *Recovery) newTiKVRecoveryClient(ctx context.Context, tikvAddr string) (recovpb.RecoverDataClient, error) {
	// Connect to the Recovery service on the given TiKV node.
	bfConf := backoff.DefaultConfig
	bfConf.MaxDelay = gRPCBackOffMaxDelay
	opt := grpc.WithInsecure()
	if recovery.tls != nil {
		opt = grpc.WithTransportCredentials(credentials.NewTLS(recovery.tls))
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
			Timeout: time.Duration(3) * time.Second,
		}),
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	defer conn.Close()

	client := recovpb.NewRecoverDataClient(conn)

	return client, nil
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
func (recovery *Recovery) ReadRegionMeta(ctx context.Context, totalTiKVs int, allStores []*metapb.Store) error {
	eg, ectx := errgroup.WithContext(ctx)
	workers := utils.NewWorkerPool(uint(totalTiKVs), "Collect Region Meta")

	// TODO: optimize the ErroGroup when TiKV is panic
	metaChan := make(chan RecoveryMeta, 1024)
	defer close(metaChan)

	for i := 0; i < totalTiKVs; i++ {
		i := i
		storeId := allStores[i].GetId()
		tikvClient, err := recovery.newTiKVRecoveryClient(ectx, allStores[i].GetAddress())
		if err != nil {
			log.Error("create tikv client failied", zap.Uint64("storeID", storeId))
			return errors.Trace(err)
		}

		workers.ApplyWithIDInErrorGroup(eg, func(id uint64) error {
			stream, err := tikvClient.ReadRegionMeta(ectx, &recovpb.ReadRegionMetaRequest{StoreId: storeId})
			if err != nil {
				log.Error("read region meta failied", zap.Uint64("storeID", storeId))
				return errors.Trace(err)
			}

			tikvMeta := NewRecoveryMeta(storeId)
			// for a TiKV, received the stream
			for {
				var meta *recovpb.RegionMeta
				if meta, err = stream.Recv(); err == nil {
					tikvMeta.recoveryMeta = append(tikvMeta.recoveryMeta, meta)
				} else if err == io.EOF {
					break
				} else if err != nil {
					log.Error("peer info receieved failed", zap.Error(err))
					return errors.Trace(err)
				}
			}

			metaChan <- tikvMeta
			return nil
		})
	}

	for i := 0; i < totalTiKVs; i++ {
		tikvMeta := <-metaChan
		recovery.regionMetas[i] = tikvMeta
		log.Debug("TiKV", zap.Int("recived from tikv", int(tikvMeta.storeId)))
	}

	return eg.Wait()
}

func (recovery *Recovery) getTotalRegions() int {
	// Group region peer info by region id.
	var regions = make(map[uint64][]Regions, 0)
	for _, v := range recovery.regionMetas {
		storeId := v.storeId
		for _, m := range v.recoveryMeta {
			if regions[m.RegionId] == nil {
				regions[m.RegionId] = make([]Regions, 0, recovery.totalStores)
			}
			regions[m.RegionId] = append(regions[m.RegionId], Regions{m, storeId})
		}
	}
	return len(regions)
}

// TODO: function provide for dump server received peer info into a file
// function shall be enabled by debug enabled
func (recovery *Recovery) dumpRegionInfo() {
	log.Debug("dump region info")
	// Group region peer info by region id.
	var regions = make(map[uint64][]Regions, 0)
	for _, v := range recovery.regionMetas {
		storeId := v.storeId
		for _, m := range v.recoveryMeta {
			if regions[m.RegionId] == nil {
				regions[m.RegionId] = make([]Regions, 0, recovery.totalStores)
			}
			regions[m.RegionId] = append(regions[m.RegionId], Regions{m, storeId})
		}
	}

	for region_id, peers := range regions {
		log.Debug("Region", zap.Uint64("RegionID", region_id))
		for _, m := range peers {
			log.Debug("tikv", zap.Int("storeId", int(m.storeId)))
			log.Debug("meta:", zap.Uint64("last_log_term", m.GetLastLogTerm()))
			log.Debug("meta:", zap.Uint64("last_index", m.GetLastIndex()))
			log.Debug("meta:", zap.Uint64("version", m.GetVersion()))
			log.Debug("meta:", zap.Bool("tombstone", m.GetTombstone()))
			log.Debug("meta:", zap.ByteString("start_key", m.GetStartKey()))
			log.Debug("meta:", zap.ByteString("end_key", m.GetEndKey()))
		}
	}

}

// TODO : function provide for dump RecoveryPlan
func (recovery *Recovery) dumpRecoverPlan() {
	log.Debug("dump recovery plan")
	// Group region peer info by region id.
	for store_id, _ := range recovery.recoveryPlan {
		log.Debug("TiKV", zap.Uint64("store id", store_id))
	}
}

// send the recovery plan to recovery region (force leader etc)
func (recovery *Recovery) RecoverRegions(ctx context.Context, allStores []*metapb.Store) (err error) {
	eg, ectx := errgroup.WithContext(ctx)
	totalTiKVs := len(recovery.recoveryPlan)
	workers := utils.NewWorkerPool(uint(totalTiKVs), "Recover Regions")

	for storeId, plan := range recovery.recoveryPlan {
		storeAddr := getStoreAddress(allStores, storeId)
		tikvClient, err := recovery.newTiKVRecoveryClient(ectx, storeAddr)
		if err != nil {
			log.Error("create tikv client failed", zap.Uint64("storeID", storeId))
			return errors.Trace(err)
		}
		cmd := plan
		workers.ApplyWithIDInErrorGroup(eg, func(id uint64) error {
			stream, err := tikvClient.RecoverCmd(ectx)
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

			log.Debug("send recovery command success", zap.Uint64("storeID", reply.GetStoreId()))
			return nil
		})
	}
	// Wait for all TiKV instances force leader and wait apply to last log.
	return eg.Wait()
}

// a worker pool to all tikv for execute delete all data whose has ts > resolvedTs
func (recovery *Recovery) ResolveData(ctx context.Context, allStores []*metapb.Store, resolvedTs uint64) (err error) {

	eg, ectx := errgroup.WithContext(ctx)
	totalTiKVs := recovery.totalStores
	workers := utils.NewWorkerPool(uint(totalTiKVs), "resolve data from tikv")

	// TODO: what if the resolved data take long time take long time?, it look we need some handling here, at leader some retry may neccessary
	for _, store := range allStores {
		storeAddr := getStoreAddress(allStores, store.Id)
		tikvClient, err := recovery.newTiKVRecoveryClient(ectx, storeAddr)
		if err != nil {
			log.Error("create tikv client failed", zap.String("ip", storeAddr))
			return errors.Trace(err)
		}

		workers.ApplyWithIDInErrorGroup(eg, func(id uint64) error {
			req := &recovpb.ResolveKvDataRequest{ResolvedTs: resolvedTs}
			resp, err := tikvClient.ResolveKvData(ectx, req)
			if err != nil {
				log.Error("send the resolve kv data failed", zap.Uint64("storeID", store.Id))
				return errors.Trace(err)
			}

			log.Debug("resolved kv data", zap.Bool("done", resp.Done))
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
// 3. TODO: set region as tombstone
// TODO: This function has to refactoring, leader selection shall follow the design
// this is temp solution, peer with the max last index will be a leader.
func (recovery *Recovery) makeRecoveryPlan() {
	type peer struct {
		rid uint64
		ver uint64
	}

	type regionEndKey struct {
		end_key []byte
		rid     uint64
	}

	// Group region peer info by region id.
	// region [id] [peer[0-n]]
	var regions = make(map[uint64][]Regions, 0)
	for _, v := range recovery.regionMetas {
		storeId := v.storeId
		for _, m := range v.recoveryMeta {
			if regions[m.RegionId] == nil {
				regions[m.RegionId] = make([]Regions, 0, recovery.totalStores)
			}
			regions[m.RegionId] = append(regions[m.RegionId], Regions{m, storeId})
		}
	}

	// TODO: last log term -> last index -> commit index
	// currently solution: last index
	// Reverse sort replicas by last index, and collect all regions' version.
	var versions = make([]peer, 0, len(regions))
	for r, x := range regions {
		sort.Slice(x, func(i, j int) bool { return x[i].LastIndex > x[j].LastIndex })
		var v = x[0].Version
		versions = append(versions, peer{r, v})
	}

	sort.Slice(versions, func(i, j int) bool { return versions[i].ver > versions[j].ver })

	// split and merge in progressing during the backup, there may some overlap region, we have to handle it
	// Resolve version conflicts.
	var topo = treemap.NewWith(keyCmpInterface)
	for _, p := range versions {
		var sk = prefixStartKey(regions[p.rid][0].StartKey)
		var ek = prefixEndKey(regions[p.rid][0].EndKey)
		var fk, fv interface{}
		fk, _ = topo.Ceiling(sk)
		// keysapce overlap sk within ceiling - fk
		if fk != nil && (keyEq(fk.([]byte), sk) || keyCmp(fk.([]byte), ek) < 0) {
			continue
		}

		// keysapce overlap sk within floor - fk.end_key
		fk, fv = topo.Floor(sk)
		if fk != nil && keyCmp(fv.(regionEndKey).end_key, sk) > 0 {
			continue
		}
		topo.Put(sk, regionEndKey{ek, p.rid})
	}

	// After resolved, all validPeer regions shouldn't be tombstone.
	// do some sanity check
	var validPeer = make(map[uint64]struct{}, 0)
	var iter = topo.Iterator()
	var prevEndKey = prefixStartKey([]byte{})
	var prevR uint64 = 0
	for iter.Next() {
		v := iter.Value().(regionEndKey)
		if regions[v.rid][0].Tombstone {
			log.Error("validPeer shouldn't be tombstone", zap.Uint64("regionID", v.rid))
			// TODO, panic or something we have to do, a PoC or test may need for decision
			panic("validPeer shouldn't be tombstone")
		}
		if !keyEq(prevEndKey, iter.Key().([]byte)) {
			log.Error("region doesn't conject to region", zap.Uint64("preRegion", prevR), zap.Uint64("curRegion", v.rid))
			// TODO, panic or something we have to do, a PoC or test may need for decision
			panic("regions should conject to each other")
		}
		prevEndKey = v.end_key
		prevR = v.rid
		validPeer[v.rid] = struct{}{}
	}

	// all plans per region key=storeId, value=reqs stream
	//regionsPlan := make(map[uint64][]*recovmetapb.RecoveryCmdRequest, 0)
	// Generate recover commands.
	for r, x := range regions {
		if _, ok := validPeer[r]; !ok {
			// TODO: Generate a tombstone command.
			// 1, peer is tomebstone
			// 2, split region in progressing, old one can be a tomebstone
			for _, m := range x {
				plan := &recovpb.RecoverCmdRequest{Tombstone: true, AsLeader: false}
				recovery.recoveryPlan[m.storeId] = append(recovery.recoveryPlan[m.storeId], plan)
			}
		} else {
			// Generate normal commands.
			log.Info("valid peer", zap.Uint64("peer", r))
			var maxTerm uint64 = 0
			for _, m := range x {
				if m.LastLogTerm > maxTerm {
					maxTerm = m.LastLogTerm
				}
			}
			for i, m := range x {
				log.Info("make plan", zap.Uint64("storeid", m.storeId), zap.Uint64("regionid", m.RegionId))
				plan := &recovpb.RecoverCmdRequest{RegionId: m.RegionId, AsLeader: (i == 0)}
				// max last index as a leader
				if plan.AsLeader {
					log.Info("as leader peer", zap.Uint64("storeid", m.storeId), zap.Uint64("regionid", m.RegionId))
					recovery.recoveryPlan[m.storeId] = append(recovery.recoveryPlan[m.storeId], plan)
				}
			}
		}
	}
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
