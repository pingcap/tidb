// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.
package restore

import (
	"context"
	"crypto/tls"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	recovmetapb "github.com/pingcap/kvproto/pkg/recoverymetapb"
	recovpb "github.com/pingcap/kvproto/pkg/recoverypb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/utils"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

type Regions struct {
	*recovmetapb.RegionMeta
	storeId uint64
}

// privode functions to restore EBS
func SwitchToRecoveryMode(pdClient pd.Client) {
	//TODO set a marker to PD, when TiKV is startup, it read marker from PD, and enter into recovery mode.
	log.Info("switch to pd recovery mode")
}

// TODO after restored, we switch the pd backup normal mode.
func SwitchToNormalMode(pdClient pd.Client) {
	log.Info("switch to pd back to normal mode")
}

func (recovery Recovery) newTiKVClient(ctx context.Context, tikvAddr string) (recovmetapb.RecoveryMetaClient, error) {
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

	if err != nil {
		return nil, errors.Trace(err)
	}

	//TODO: when we release this connection
	//defer conn.Close()

	client := recovmetapb.NewRecoveryMetaClient(conn)

	return client, nil
}

func (recovery Recovery) newTiKVRecoveryClient(ctx context.Context, tikvAddr string) (recovpb.RecoveryClient, error) {
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

	if err != nil {
		return nil, errors.Trace(err)
	}

	//TODO: when we release this connection
	//defer conn.Close()

	client := recovpb.NewRecoveryClient(conn)

	return client, nil
}

type RecoveryMeta struct {
	storeId      uint64
	recoveryMeta []*recovmetapb.RegionMeta
}

// TODO will this function is redundent
func NewRecoveryMeta(storeId uint64) RecoveryMeta {
	var meta = make([]*recovmetapb.RegionMeta, 0)
	return RecoveryMeta{storeId, meta}
}

type RecoveryPlan struct {
	storeId      uint64
	recoveryPlan []*recovmetapb.RecoveryCmdRequest
}

// TODO will this function is redundent
func NewRecoveryPlan(storeId uint64) RecoveryPlan {
	var meta = make([]*recovmetapb.RecoveryCmdRequest, 0)
	return RecoveryPlan{storeId, meta}
}

type Recovery struct {
	instances    int
	instanceId   *uint32
	received     *sync.WaitGroup // Whether all reports are collected or not.
	generated    *sync.WaitGroup // Whether commands are generated or not.
	finished     *sync.WaitGroup // Whether all commands are sent out or not.
	regionMetas  []RecoveryMeta
	recoveryPlan map[uint64][]*recovmetapb.RecoveryCmdRequest
	resolvedTs   *uint64
	tls          *tls.Config
}

func NewRecovery(instances int, tls *tls.Config) Recovery {
	var instanceId = new(uint32)
	var received = new(sync.WaitGroup)
	received.Add(instances)
	var generated = new(sync.WaitGroup)
	generated.Add(1)
	var finished = new(sync.WaitGroup)
	finished.Add(instances)
	var regionMetas = make([]RecoveryMeta, instances)
	var regionRecovers = make(map[uint64][]*recovmetapb.RecoveryCmdRequest, instances)
	var resolvedTs = new(uint64)
	return Recovery{instances, instanceId, received, generated, finished, regionMetas, regionRecovers, resolvedTs, tls}
}

// read all region meta from tikvs
func (recovery Recovery) ReadRegionMeta(ctx context.Context, totalTiKVs int, allStores []*metapb.Store) error {
	eg, ectx := errgroup.WithContext(ctx)
	workers := utils.NewWorkerPool(uint(totalTiKVs), "Collect Region Meta")

	for i := 0; i < totalTiKVs; i++ {
		log.Info("TiKV", zap.Int("store id", int(allStores[i].Id)))
		i := i
		storeId := allStores[i].GetId()
		workers.ApplyWithIDInErrorGroup(eg, func(id uint64) error {
			tikv, _ := recovery.newTiKVClient(ectx, allStores[i].GetAddress())
			stream, err := tikv.ReadRegionMeta(ectx, &recovmetapb.ReadRegionMetaRequest{StoreId: storeId})
			if err != nil {
				log.Error("read region meta failied", zap.Uint64("storeID", storeId))
				return err
			}
			// for a TiKV, received the stream
			for {
				var meta *recovmetapb.RegionMeta
				recovery.regionMetas[i] = NewRecoveryMeta(id)
				if meta, err = stream.Recv(); err == nil {
					recovery.regionMetas[i].recoveryMeta = append(recovery.regionMetas[i].recoveryMeta, meta)
				} else if err == io.EOF {
					break
				} else {
					log.Error("peer info receieved failed", zap.Error(err))
					return errors.Trace(err)
				}
			}

			return err
		})
	}
	// Wait for all TiKV instances reporting peer info.
	return eg.Wait()

}

// function provide for dump server received peer info into a file
// function shall be enalbed by debug enabled
func (recovery Recovery) printRegionInfo() {
	log.Info("dump service log information.")
	// Group region peer info by region id.
	var regions = make(map[uint64][]Regions, 0)
	for _, v := range recovery.regionMetas {
		storeId := v.storeId
		for _, m := range v.recoveryMeta {
			if regions[m.RegionId] == nil {
				regions[m.RegionId] = make([]Regions, 0, recovery.instances)
			}
			regions[m.RegionId] = append(regions[m.RegionId], Regions{m, storeId})
		}
	}

	for region_id, peers := range regions {
		log.Info("Region", zap.Uint64("RegionID", region_id))
		for offset, m := range peers {
			log.Info("tikv", zap.Int("storeId", offset))
			log.Info("meta:", zap.Uint64("applied_index", m.GetAppliedIndex()))
			log.Info("meta:", zap.Uint64("last_index", m.GetLastIndex()))
			log.Info("meta:", zap.Uint64("term", m.GetTerm()))
			log.Info("meta:", zap.Uint64("version", m.GetVersion()))
			log.Info("meta:", zap.Bool("tombstone", m.GetTombstone()))
			log.Info("meta:", zap.ByteString("start_key", m.GetStartKey()))
			log.Info("meta:", zap.ByteString("end_key", m.GetEndKey()))
		}
	}

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
	return ""
}

// send the recovery plan to recovery region (force leader etc)
func (recovery Recovery) RecoverRegions(ctx context.Context, allStores []*metapb.Store) (err error) {
	eg, ectx := errgroup.WithContext(ctx)
	totalTiKVs := len(recovery.recoveryPlan)
	workers := utils.NewWorkerPool(uint(totalTiKVs), "Recovery Region Meta")

	for storeId, plan := range recovery.recoveryPlan {
		log.Info("TiKV", zap.Uint64("storeId", storeId))
		storeAddr := getStoreAddress(allStores, storeId)
		cmd := plan
		workers.ApplyWithIDInErrorGroup(eg, func(id uint64) error {
			tikv, _ := recovery.newTiKVClient(ectx, storeAddr)
			stream, err := tikv.RecoveryCmd(ectx)
			if err != nil {
				log.Error("read region meta failied", zap.Uint64("storeID", storeId))
				return err
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
				log.Error("client.RecoveryCmd failed")
			}

			log.Debug("send recovery command success", zap.Bool("OK", reply.Ok))
			return err
		})
	}
	// Wait for all TiKV instances reporting peer info.
	return eg.Wait()
}

// check if all region aligned with last index
// Spike: what if we direcly applyindex after force leader, will it cause problem?
func (recovery Recovery) ApplyRecoveryPlan(ctx context.Context, allStores []*metapb.Store) (err error) {
	eg, ectx := errgroup.WithContext(ctx)
	totalTiKVs := len(recovery.recoveryPlan)
	workers := utils.NewWorkerPool(uint(totalTiKVs), "apply leader last log")

	// TODO: what if the waitapply take long time?, it look we need some handling here, at leader some retry may neccessary
	for storeId, plan := range recovery.recoveryPlan {
		log.Info("TiKV", zap.Uint64("storeId", storeId))
		storeAddr := getStoreAddress(allStores, storeId)
		cmd := plan
		workers.ApplyWithIDInErrorGroup(eg, func(id uint64) error {
			tikv, _ := recovery.newTiKVRecoveryClient(ectx, storeAddr)
			//TODO P1: it should be a stream to each tikv
			req := &recovpb.GetRaftStatusRequest{RegionId: cmd[0].RegionId, CommitIndex: cmd[0].LeaderCommitIndex}
			resp, err := tikv.CheckRaftStatus(ectx, req)
			if err != nil {
				log.Error("read region meta failied", zap.Uint64("storeID", storeId))
				return err
			}

			log.Debug("Apply the last log index success", zap.Bool("OK", resp.Aligned))
			return err
		})
	}
	// Wait for all TiKV instances apply to the last index
	return eg.Wait()
}

// a worker pool to all tikv for execute delete all data whose has ts > resolvedTs
func (recovery Recovery) ResolvedData(ctx context.Context, allStores []*metapb.Store, resolvedTs uint64) (err error) {

	eg, ectx := errgroup.WithContext(ctx)
	totalTiKVs := recovery.instances
	workers := utils.NewWorkerPool(uint(totalTiKVs), "resolved data from tikv")

	// TODO: what if the resolved data take long time take long time?, it look we need some handling here, at leader some retry may neccessary
	for _, store := range allStores {
		log.Info("TiKV", zap.Uint64("storeId", store.Id))
		storeAddr := store.Address
		workers.ApplyWithIDInErrorGroup(eg, func(id uint64) error {
			tikv, _ := recovery.newTiKVRecoveryClient(ectx, storeAddr)
			req := &recovpb.ResolvedRequest{ResolvedTs: resolvedTs}
			resp, err := tikv.ResolvedKvData(ectx, req)
			if err != nil {
				log.Error("read region meta failied", zap.Uint64("storeID", store.Id))
				return err
			}

			log.Debug("Apply the last log index success", zap.Bool("done", resp.Done))
			return err
		})
	}
	// Wait for all TiKV instances apply to the last index
	return eg.Wait()

}

// recover the tikv cluster
// 1. read all meta data from tikvs
// 2. start assemble region meta and make recovery plan
// 3. trigger the waitapply to tikv, in waitapply, all assigned region leader will check and move the apply index to the last index.
// 4. send the resolved_ts untill it finished.
func RecoverCluster(ctx context.Context, resolvedTs uint64, totalTiKVs int, allStores []*metapb.Store, tls *tls.Config) error {

	// start recovery service
	log.Info("region recovery service start")

	var recovery = NewRecovery(totalTiKVs, tls)
	// a work pool to get all metadata
	recovery.ReadRegionMeta(ctx, totalTiKVs, allStores)

	// TODO: here there is issue when tikv report without tombstone, but service has tombstone in meta
	// the printRegionInfo make the service work fine without tombstone, it looks like a concurrency issue.
	recovery.printRegionInfo()

	recovery.makeRecoveryPlan()
	log.Info("region recovery commands are generated")
	recovery.RecoverRegions(ctx, allStores)

	recovery.ApplyRecoveryPlan(ctx, allStores)
	// TODO send message to clone the streams
	time.Sleep(time.Second * 3) // Sleep to wait grpc streams get closed.

	recovery.ResolvedData(ctx, allStores, resolvedTs)
	log.Info("region recovery phase completed.")

	return nil
}

// generate the related the recovery plan to tikvs:
// 1. check overlap the region, make a recovery decision
// 2. assign a leader for region during the tikv startup
// 3. set region as tombstone
func (recovery Recovery) makeRecoveryPlan() {
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
				regions[m.RegionId] = make([]Regions, 0, recovery.instances)
			}
			regions[m.RegionId] = append(regions[m.RegionId], Regions{m, storeId})
		}
	}

	// TODO: last log term -> last index -> commit index -> applied index
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
	regionsPlan := make(map[uint64][]*recovmetapb.RecoveryCmdRequest, 0)
	// Generate recover commands.
	for r, x := range regions {
		if _, ok := validPeer[r]; !ok {
			// TODO: Generate a tombstone command.
			// 1, peer is tomebstone
			// 2, split region in progressing, old one can be a tomebstone
			for _, m := range x {
				plan := &recovmetapb.RecoveryCmdRequest{Tombstone: true, AsLeader: false}
				regionsPlan[m.storeId] = append(regionsPlan[m.storeId], plan)
			}
		} else {
			// Generate normal commands.
			var maxTerm uint64 = 0
			for _, m := range x {
				if m.Term > maxTerm {
					maxTerm = m.Term
				}
			}
			for i, m := range x {
				plan := &recovmetapb.RecoveryCmdRequest{RegionId: m.RegionId, Term: maxTerm, AsLeader: (i == 0)}
				// max last index as a leader
				if plan.Term != m.Term || plan.AsLeader {
					regionsPlan[m.storeId] = append(regionsPlan[m.storeId], plan)
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
