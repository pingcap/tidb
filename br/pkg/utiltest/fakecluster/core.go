// Copyright 2026 PingCAP, Inc.
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

package fakecluster

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	backup "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	logbackup "github.com/pingcap/kvproto/pkg/logbackuppb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/br/pkg/streamhelper/spans"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type FlushSimulator struct {
	FlushedEpoch atomic.Uint64
	Enabled      bool
}

func (c *FlushSimulator) makeError(requestedEpoch uint64) *errorpb.Error {
	if !c.Enabled {
		return nil
	}
	if c.FlushedEpoch.Load() == 0 {
		return &errorpb.Error{Message: "not flushed"}
	}
	if c.FlushedEpoch.Load() != requestedEpoch {
		return &errorpb.Error{Message: "flushed epoch not match"}
	}
	return nil
}

func (c *FlushSimulator) fork() FlushSimulator {
	return FlushSimulator{Enabled: c.Enabled}
}

type Region struct {
	Range      kv.KeyRange
	Leader     uint64
	Epoch      uint64
	ID         uint64
	Checkpoint atomic.Uint64
	FlushSim   FlushSimulator
	Locks      []*txnlock.Lock
}

func NewRegion(
	id uint64,
	startKey, endKey []byte,
	leader uint64,
	epoch uint64,
	checkpoint uint64,
	flushSimEnabled bool,
) *Region {
	r := &Region{
		Range: kv.KeyRange{
			StartKey: bytes.Clone(startKey),
			EndKey:   bytes.Clone(endKey),
		},
		Leader: leader,
		Epoch:  epoch,
		ID:     id,
		FlushSim: FlushSimulator{
			Enabled: flushSimEnabled,
		},
	}
	r.Checkpoint.Store(checkpoint)
	return r
}

func (r *Region) SplitAt(newID uint64, key string) *Region {
	newRegion := &Region{
		Range: kv.KeyRange{
			StartKey: []byte(key),
			EndKey:   bytes.Clone(r.Range.EndKey),
		},
		Leader:   r.Leader,
		Epoch:    r.Epoch + 1,
		ID:       newID,
		FlushSim: r.FlushSim.fork(),
	}
	newRegion.Checkpoint.Store(r.Checkpoint.Load())
	r.Range.EndKey = []byte(key)
	r.Epoch++
	r.FlushSim = r.FlushSim.fork()
	return newRegion
}

func (r *Region) Flush() {
	r.FlushSim.FlushedEpoch.Store(r.Epoch)
}

func (r *Region) String() string {
	return fmt.Sprintf("%d(%d):[%s, %s);%dL%dF%d",
		r.ID,
		r.Epoch,
		hex.EncodeToString(r.Range.StartKey),
		hex.EncodeToString(r.Range.EndKey),
		r.Checkpoint.Load(),
		r.Leader,
		r.FlushSim.FlushedEpoch.Load())
}

type trivialFlushStream struct {
	c  <-chan *logbackup.SubscribeFlushEventResponse
	cx context.Context
}

func (t trivialFlushStream) Recv() (*logbackup.SubscribeFlushEventResponse, error) {
	select {
	case item, ok := <-t.c:
		if !ok {
			return nil, io.EOF
		}
		return item, nil
	case <-t.cx.Done():
		select {
		case item, ok := <-t.c:
			if !ok {
				return nil, io.EOF
			}
			return item, nil
		default:
		}
		return nil, status.Error(codes.Canceled, t.cx.Err().Error())
	}
}

func (t trivialFlushStream) Header() (metadata.MD, error) {
	return make(metadata.MD), nil
}

func (t trivialFlushStream) Trailer() metadata.MD {
	return make(metadata.MD)
}

func (t trivialFlushStream) CloseSend() error {
	return nil
}

func (t trivialFlushStream) Context() context.Context {
	return t.cx
}

func (t trivialFlushStream) SendMsg(any) error {
	return nil
}

func (t trivialFlushStream) RecvMsg(any) error {
	return nil
}

type Store struct {
	ID                               uint64
	RegionMap                        map[uint64]*Region
	ClientMu                         sync.Mutex
	SupportsSub                      bool
	BootstrapAt                      uint64
	OnGetRegionCheckpoint            func(*logbackup.GetLastFlushTSOfRegionRequest) error
	LegacyRegionCheckpointRPCEnabled bool
	FlushTaskName                    string
	subscribers                      map[chan *logbackup.SubscribeFlushEventResponse]struct{}
}

func (f *Store) FlushNow(
	ctx context.Context,
	in *logbackup.FlushNowRequest,
	opts ...grpc.CallOption,
) (*logbackup.FlushNowResponse, error) {
	_ = ctx
	_ = in
	_ = opts
	f.Flush()
	taskName := f.FlushTaskName
	if taskName == "" {
		taskName = "Universe"
	}
	return &logbackup.FlushNowResponse{
		Results: []*logbackup.FlushResult{{TaskName: taskName, Success: true}},
	}, nil
}

func (f *Store) GetID() uint64 {
	return f.ID
}

func (f *Store) SubscribeFlushEvent(
	ctx context.Context,
	in *logbackup.SubscribeFlushEventRequest,
	opts ...grpc.CallOption,
) (logbackup.LogBackup_SubscribeFlushEventClient, error) {
	_ = in
	_ = opts

	f.ClientMu.Lock()
	defer f.ClientMu.Unlock()
	if !f.SupportsSub {
		return nil, status.Error(codes.Unimplemented, "meow?")
	}

	ch := make(chan *logbackup.SubscribeFlushEventResponse, 1024)
	if f.subscribers == nil {
		f.subscribers = make(map[chan *logbackup.SubscribeFlushEventResponse]struct{})
	}
	f.subscribers[ch] = struct{}{}
	go func() {
		<-ctx.Done()
		f.ClientMu.Lock()
		delete(f.subscribers, ch)
		f.ClientMu.Unlock()
	}()
	return trivialFlushStream{c: ch, cx: ctx}, nil
}

func (f *Store) SetSupportFlushSub(b bool) {
	f.ClientMu.Lock()
	defer f.ClientMu.Unlock()
	f.BootstrapAt++
	f.SupportsSub = b
}

func (f *Store) SetGetRegionCheckpointHook(hook func(*logbackup.GetLastFlushTSOfRegionRequest) error) {
	f.OnGetRegionCheckpoint = hook
}

func (f *Store) GetLastFlushTSOfRegion(
	ctx context.Context,
	in *logbackup.GetLastFlushTSOfRegionRequest,
	opts ...grpc.CallOption,
) (*logbackup.GetLastFlushTSOfRegionResponse, error) {
	_ = ctx
	_ = opts

	if !f.LegacyRegionCheckpointRPCEnabled {
		return nil, status.Error(codes.Unimplemented, "GetLastFlushTSOfRegion is legacy and disabled in DRR harness")
	}
	if f.OnGetRegionCheckpoint != nil {
		if err := f.OnGetRegionCheckpoint(in); err != nil {
			return nil, err
		}
	}
	resp := &logbackup.GetLastFlushTSOfRegionResponse{
		Checkpoints: []*logbackup.RegionCheckpoint{},
	}
	for _, r := range in.Regions {
		region, ok := f.RegionMap[r.Id]
		if !ok || region.Leader != f.ID {
			resp.Checkpoints = append(resp.Checkpoints, &logbackup.RegionCheckpoint{
				Err: &errorpb.Error{Message: "not found"},
				Region: &logbackup.RegionIdentity{
					Id:           r.Id,
					EpochVersion: r.EpochVersion,
				},
			})
			continue
		}
		if err := region.FlushSim.makeError(r.EpochVersion); err != nil {
			resp.Checkpoints = append(resp.Checkpoints, &logbackup.RegionCheckpoint{
				Err: err,
				Region: &logbackup.RegionIdentity{
					Id:           region.ID,
					EpochVersion: region.Epoch,
				},
			})
			continue
		}
		if region.Epoch != r.EpochVersion {
			resp.Checkpoints = append(resp.Checkpoints, &logbackup.RegionCheckpoint{
				Err: &errorpb.Error{Message: "epoch not match"},
				Region: &logbackup.RegionIdentity{
					Id:           region.ID,
					EpochVersion: region.Epoch,
				},
			})
			continue
		}
		resp.Checkpoints = append(resp.Checkpoints, &logbackup.RegionCheckpoint{
			Checkpoint: region.Checkpoint.Load(),
			Region: &logbackup.RegionIdentity{
				Id:           region.ID,
				EpochVersion: region.Epoch,
			},
		})
	}
	log.Debug("Get last flush ts of region", zap.Stringer("in", in), zap.Stringer("out", resp))
	return resp, nil
}

func (f *Store) emitFlushEvents(resp *logbackup.SubscribeFlushEventResponse) {
	f.ClientMu.Lock()
	defer f.ClientMu.Unlock()
	for ch := range f.subscribers {
		select {
		case ch <- resp:
		default:
		}
	}
}

func (f *Store) FlushExcept(keys ...string) {
	resp := make([]*logbackup.FlushEvent, 0, len(f.RegionMap))
outer:
	for _, r := range f.RegionMap {
		if r.Leader != f.ID {
			continue
		}
		for _, key := range keys {
			if utils.CompareBytesExt(r.Range.StartKey, false, []byte(key), false) <= 0 &&
				utils.CompareBytesExt([]byte(key), false, r.Range.EndKey, true) < 0 {
				continue outer
			}
		}
		r.Flush()
		resp = append(resp, &logbackup.FlushEvent{
			StartKey:   codec.EncodeBytes(nil, r.Range.StartKey),
			EndKey:     codec.EncodeBytes(nil, r.Range.EndKey),
			Checkpoint: r.Checkpoint.Load(),
		})
	}
	f.emitFlushEvents(&logbackup.SubscribeFlushEventResponse{Events: resp})
}

func (f *Store) Flush() {
	f.FlushExcept()
}

func (f *Store) String() string {
	buf := new(strings.Builder)
	fmt.Fprintf(buf, "%d: ", f.ID)
	for _, r := range f.RegionMap {
		fmt.Fprintf(buf, "%s ", r)
	}
	return buf.String()
}

type RegionState struct {
	ID         uint64
	Epoch      uint64
	StoreID    uint64
	StartKey   []byte
	EndKey     []byte
	Checkpoint uint64
}

func stateFromRegion(r *Region) RegionState {
	return RegionState{
		ID:         r.ID,
		Epoch:      r.Epoch,
		StoreID:    r.Leader,
		StartKey:   bytes.Clone(r.Range.StartKey),
		EndKey:     bytes.Clone(r.Range.EndKey),
		Checkpoint: r.Checkpoint.Load(),
	}
}

type Cluster struct {
	Mu                        sync.Mutex
	IDAlloced                 uint64
	StoreMap                  map[uint64]*Store
	Regions                   []*Region
	MaxTS                     uint64
	OnGetClient               func(uint64) error
	OnClearCache              func(uint64) error
	ServiceGCSafePoint        uint64
	ServiceGCSafePointSet     bool
	ServiceGCSafePointDeleted bool
	CurrentTS                 uint64
}

func New() *Cluster {
	return &Cluster{
		StoreMap: make(map[uint64]*Store),
		Regions:  make([]*Region, 0),
	}
}

func NewBasicCluster(n int, simEnabled bool) *Cluster {
	c := New()
	stores := make([]*Store, 0, n)
	for range n {
		s := &Store{
			ID:                               c.AllocID(),
			RegionMap:                        make(map[uint64]*Region),
			LegacyRegionCheckpointRPCEnabled: true,
		}
		stores = append(stores, s)
	}
	initialRegion := NewRegion(c.AllocID(), nil, nil, stores[0].ID, 0, 0, simEnabled)
	for i := range 3 {
		if i < len(stores) {
			stores[i].RegionMap[initialRegion.ID] = initialRegion
		}
	}
	for _, s := range stores {
		c.StoreMap[s.ID] = s
	}
	c.Regions = append(c.Regions, initialRegion)
	return c
}

func (f *Cluster) AllocID() uint64 {
	f.IDAlloced++
	return f.IDAlloced
}

func (f *Cluster) EnsureStore(id uint64, bootstrapAt uint64) *Store {
	store, ok := f.StoreMap[id]
	if ok {
		return store
	}
	if id > f.IDAlloced {
		f.IDAlloced = id
	}
	store = &Store{
		ID:                               id,
		BootstrapAt:                      bootstrapAt,
		RegionMap:                        make(map[uint64]*Region),
		LegacyRegionCheckpointRPCEnabled: true,
	}
	f.StoreMap[id] = store
	return store
}

func (f *Cluster) AddRegion(region *Region, peerStoreIDs []uint64) {
	if region.ID > f.IDAlloced {
		f.IDAlloced = region.ID
	}
	f.Regions = append(f.Regions, region)
	for _, storeID := range peerStoreIDs {
		store := f.EnsureStore(storeID, 0)
		store.RegionMap[region.ID] = region
	}
}

func (f *Cluster) SetOnGetClient(hook func(uint64) error) {
	f.OnGetClient = hook
}

func (f *Cluster) SetOnClearCache(hook func(uint64) error) {
	f.OnClearCache = hook
}

func (f *Cluster) StoreList() []*Store {
	result := make([]*Store, 0, len(f.StoreMap))
	for _, s := range f.StoreMap {
		result = append(result, s)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func (f *Cluster) StoreIDs() []uint64 {
	result := make([]uint64, 0, len(f.StoreMap))
	for id := range f.StoreMap {
		result = append(result, id)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func (f *Cluster) RegionList() []*Region {
	result := append([]*Region(nil), f.Regions...)
	sort.Slice(result, func(i, j int) bool {
		return bytes.Compare(result[i].Range.StartKey, result[j].Range.StartKey) < 0
	})
	return result
}

func (f *Cluster) CurrentTSO() uint64 {
	f.Mu.Lock()
	defer f.Mu.Unlock()
	return f.CurrentTS
}

func (f *Cluster) SetCurrentTS(ts uint64) {
	f.Mu.Lock()
	defer f.Mu.Unlock()
	f.CurrentTS = ts
}

func (f *Cluster) AllocTSO() uint64 {
	f.Mu.Lock()
	defer f.Mu.Unlock()
	physical := oracle.ExtractPhysical(f.CurrentTS) + 1
	f.CurrentTS = oracle.ComposeTS(physical, 0)
	return f.CurrentTS
}

func (f *Cluster) BlockGCUntil(ctx context.Context, at uint64) (uint64, error) {
	_ = ctx
	f.Mu.Lock()
	defer f.Mu.Unlock()
	if f.ServiceGCSafePoint > at {
		return f.ServiceGCSafePoint, errors.Errorf(
			"minimal safe point %d is greater than the target %d",
			f.ServiceGCSafePoint,
			at,
		)
	}
	f.ServiceGCSafePoint = at
	f.ServiceGCSafePointSet = true
	return at, nil
}

func (f *Cluster) UnblockGC(ctx context.Context) error {
	_ = ctx
	f.Mu.Lock()
	defer f.Mu.Unlock()
	f.ServiceGCSafePointDeleted = true
	return nil
}

func (f *Cluster) FetchCurrentTS(ctx context.Context) (uint64, error) {
	_ = ctx
	return f.CurrentTSO(), nil
}

func (f *Cluster) RegionScan(
	ctx context.Context,
	key []byte,
	endKey []byte,
	limit int,
) ([]streamhelper.RegionWithLeader, error) {
	_ = ctx
	regions := f.RegionList()
	result := make([]streamhelper.RegionWithLeader, 0, limit)
	for _, region := range regions {
		if spans.Overlaps(kv.KeyRange{StartKey: key, EndKey: endKey}, region.Range) && len(result) < limit {
			regionInfo := streamhelper.RegionWithLeader{
				Region: &metapb.Region{
					Id:       region.ID,
					StartKey: region.Range.StartKey,
					EndKey:   region.Range.EndKey,
					RegionEpoch: &metapb.RegionEpoch{
						Version: region.Epoch,
					},
				},
				Leader: &metapb.Peer{StoreId: region.Leader},
			}
			result = append(result, regionInfo)
		} else if bytes.Compare(region.Range.StartKey, key) > 0 {
			break
		}
	}
	return result, nil
}

func (f *Cluster) GetLogBackupClient(ctx context.Context, storeID uint64) (logbackup.LogBackupClient, error) {
	_ = ctx
	if f.OnGetClient != nil {
		if err := f.OnGetClient(storeID); err != nil {
			return nil, err
		}
	}
	cli, ok := f.StoreMap[storeID]
	if !ok {
		return nil, fmt.Errorf("the store %d doesn't exist", storeID)
	}
	return cli, nil
}

func (f *Cluster) ClearCache(ctx context.Context, storeID uint64) error {
	_ = ctx
	if f.OnClearCache != nil {
		if err := f.OnClearCache(storeID); err != nil {
			return err
		}
	}
	return nil
}

func (f *Cluster) Stores(ctx context.Context) ([]streamhelper.Store, error) {
	_ = ctx
	r := make([]streamhelper.Store, 0, len(f.StoreMap))
	for id, s := range f.StoreMap {
		r = append(r, streamhelper.Store{ID: id, BootAt: s.BootstrapAt})
	}
	sort.Slice(r, func(i, j int) bool { return r[i].ID < r[j].ID })
	return r, nil
}

func (f *Cluster) FindRegionByID(rid uint64) *Region {
	for _, r := range f.Regions {
		if r.ID == rid {
			return r
		}
	}
	return nil
}

func (f *Cluster) LockRegion(r *Region, locks []*txnlock.Lock) *Region {
	r.Locks = locks
	return r
}

func (f *Cluster) FindRegionByKey(key []byte) *Region {
	for _, r := range f.Regions {
		if bytes.Compare(key, r.Range.StartKey) >= 0 && (len(r.Range.EndKey) == 0 || bytes.Compare(key, r.Range.EndKey) < 0) {
			return r
		}
	}
	panic(fmt.Sprintf("inconsistent key space; key = %X", key))
}

func (f *Cluster) TransferRegionTo(rid uint64, newPeers []uint64) {
	r := f.FindRegionByID(rid)
storeLoop:
	for _, store := range f.StoreMap {
		for _, pid := range newPeers {
			if pid == store.ID {
				store.RegionMap[rid] = r
				continue storeLoop
			}
		}
		delete(store.RegionMap, rid)
	}
}

func (f *Cluster) SetRegionLeader(rid uint64, leader uint64) {
	r := f.FindRegionByID(rid)
	if r != nil {
		r.Leader = leader
	}
}

func (f *Cluster) SetRegionCheckpoint(rid uint64, checkpoint uint64) {
	r := f.FindRegionByID(rid)
	if r != nil {
		r.Checkpoint.Store(checkpoint)
	}
}

func (f *Cluster) BumpRegionEpoch(rid uint64) {
	r := f.FindRegionByID(rid)
	if r != nil {
		r.Epoch++
	}
}

func (f *Cluster) SplitAt(key string) {
	k := []byte(key)
	r := f.FindRegionByKey(k)
	newRegion := r.SplitAt(f.AllocID(), key)
	for _, store := range f.StoreMap {
		if _, ok := store.RegionMap[r.ID]; ok {
			store.RegionMap[newRegion.ID] = newRegion
		}
	}
	f.Regions = append(f.Regions, newRegion)
}

func (f *Cluster) chooseStores(n int) []uint64 {
	s := make([]uint64, 0, len(f.StoreMap))
	for id := range f.StoreMap {
		s = append(s, id)
	}
	rand.Shuffle(len(s), func(i, j int) {
		s[i], s[j] = s[j], s[i]
	})
	return s[:n]
}

func (f *Cluster) FindPeers(rid uint64) (result []uint64) {
	for _, store := range f.StoreMap {
		if _, ok := store.RegionMap[rid]; ok {
			result = append(result, store.ID)
		}
	}
	return
}

func (f *Cluster) shuffleLeader(rid uint64) {
	r := f.FindRegionByID(rid)
	peers := f.FindPeers(rid)
	rand.Shuffle(len(peers), func(i, j int) {
		peers[i], peers[j] = peers[j], peers[i]
	})
	r.Leader = peers[0]
}

func (f *Cluster) SplitAndScatter(keys ...string) {
	f.Mu.Lock()
	defer f.Mu.Unlock()
	for _, key := range keys {
		f.SplitAt(key)
	}
	for _, r := range f.Regions {
		chosen := f.chooseStores(3)
		f.TransferRegionTo(r.ID, chosen)
		f.shuffleLeader(r.ID)
	}
}

func (f *Cluster) RemoveStore(id uint64) {
	f.Mu.Lock()
	defer f.Mu.Unlock()

	s := f.StoreMap[id]
	for _, r := range s.RegionMap {
		if r.Leader == id {
			f.UpdateRegion(r.ID, func(r *Region) {
				ps := f.FindPeers(r.ID)
				for _, p := range ps {
					if p != r.Leader {
						log.Info("remove store: transforming leader",
							zap.Uint64("region", r.ID),
							zap.Uint64("new-leader", p),
							zap.Uint64("old-leader", r.Leader))
						r.Leader = p
						break
					}
				}
			})
		}
	}

	delete(f.StoreMap, id)
}

func (f *Cluster) UpdateRegion(rid uint64, mut func(*Region)) {
	r := f.FindRegionByID(rid)
	mut(r)
}

func (f *Cluster) AdvanceCheckpoints() uint64 {
	minCheckpoint := uint64(math.MaxUint64)
	for _, r := range f.Regions {
		f.UpdateRegion(r.ID, func(r *Region) {
			cp := r.Checkpoint.Add(rand.Uint64()%256 + 1)
			if cp < minCheckpoint {
				minCheckpoint = cp
			}
			r.FlushSim.FlushedEpoch.Store(0)
		})
	}
	log.Info("checkpoint updated", zap.Uint64("to", minCheckpoint))
	return minCheckpoint
}

func (f *Cluster) AdvanceCheckpointBy(duration time.Duration) uint64 {
	minCheckpoint := uint64(math.MaxUint64)
	for _, r := range f.Regions {
		f.UpdateRegion(r.ID, func(r *Region) {
			newCheckpointTime := oracle.GetTimeFromTS(r.Checkpoint.Load()).Add(duration)
			newCheckpoint := oracle.GoTimeToTS(newCheckpointTime)
			r.Checkpoint.Store(newCheckpoint)
			if newCheckpoint < minCheckpoint {
				minCheckpoint = newCheckpoint
			}
			r.FlushSim.FlushedEpoch.Store(0)
		})
	}
	log.Info("checkpoint updated", zap.Uint64("to", minCheckpoint))
	return minCheckpoint
}

func (f *Cluster) AdvanceClusterTimeBy(duration time.Duration) uint64 {
	newTime := oracle.GoTimeToTS(oracle.GetTimeFromTS(f.CurrentTS).Add(duration))
	f.CurrentTS = newTime
	return newTime
}

func (f *Cluster) FlushAll() {
	for _, r := range f.StoreMap {
		r.Flush()
	}
}

func (f *Cluster) FlushAllExcept(keys ...string) {
	for _, s := range f.StoreMap {
		s.FlushExcept(keys...)
	}
}

func (f *Cluster) String() string {
	buf := new(strings.Builder)
	fmt.Fprint(buf, ">>> fake cluster <<<\nregions: ")
	for _, region := range f.Regions {
		fmt.Fprint(buf, region, " ")
	}
	fmt.Fprintln(buf)
	for _, store := range f.StoreMap {
		fmt.Fprintln(buf, store)
	}
	return buf.String()
}

func (f *Cluster) RegionIDs() []uint64 {
	regions := f.RegionList()
	ids := make([]uint64, 0, len(regions))
	for _, r := range regions {
		ids = append(ids, r.ID)
	}
	return ids
}

func (f *Cluster) RegionSnapshot(regionID uint64) (RegionState, bool) {
	r := f.FindRegionByID(regionID)
	if r == nil {
		return RegionState{}, false
	}
	return stateFromRegion(r), true
}

func (f *Cluster) RegionSnapshotsOnStore(storeID uint64) ([]RegionState, error) {
	store, ok := f.StoreMap[storeID]
	if !ok {
		return nil, fmt.Errorf("store %d not found", storeID)
	}
	result := make([]RegionState, 0, len(store.RegionMap))
	for _, r := range store.RegionMap {
		result = append(result, stateFromRegion(r))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (f *Cluster) ApplyCheckpointToStore(
	ctx context.Context,
	storeID uint64,
	checkpoint uint64,
) ([]RegionState, error) {
	store, ok := f.StoreMap[storeID]
	if !ok {
		return nil, fmt.Errorf("store %d not found", storeID)
	}
	regionIDs := make([]uint64, 0, len(store.RegionMap))
	for regionID := range store.RegionMap {
		regionIDs = append(regionIDs, regionID)
	}
	sort.Slice(regionIDs, func(i, j int) bool { return regionIDs[i] < regionIDs[j] })

	states := make([]RegionState, 0, len(regionIDs))
	events := make([]*logbackup.FlushEvent, 0, len(regionIDs))
	for _, regionID := range regionIDs {
		r := store.RegionMap[regionID]
		if checkpoint <= r.Checkpoint.Load() {
			return nil, fmt.Errorf(
				"region %d checkpoint %d must be greater than current %d",
				regionID,
				checkpoint,
				r.Checkpoint.Load(),
			)
		}
	}
	for _, regionID := range regionIDs {
		r := store.RegionMap[regionID]
		r.Checkpoint.Store(checkpoint)
		state := stateFromRegion(r)
		states = append(states, state)
		events = append(events, &logbackup.FlushEvent{
			StartKey:   codec.EncodeBytes(nil, state.StartKey),
			EndKey:     codec.EncodeBytes(nil, state.EndKey),
			Checkpoint: checkpoint,
		})
	}
	if checkpoint > f.CurrentTS {
		f.CurrentTS = checkpoint
	}

	resp := &logbackup.SubscribeFlushEventResponse{Events: events}
	store.ClientMu.Lock()
	receivers := make([]chan *logbackup.SubscribeFlushEventResponse, 0, len(store.subscribers))
	for ch := range store.subscribers {
		receivers = append(receivers, ch)
	}
	store.ClientMu.Unlock()
	for _, ch := range receivers {
		select {
		case ch <- resp:
		case <-ctx.Done():
			return nil, fmt.Errorf("send flush events for store %d to subscribers: %w", storeID, ctx.Err())
		}
	}
	return states, nil
}

func (f *Cluster) NewTaskEvent(taskName string, startTS uint64) streamhelper.TaskEvent {
	return streamhelper.TaskEvent{
		Type: streamhelper.EventAdd,
		Name: taskName,
		Info: &backup.StreamBackupTaskInfo{
			Name:    taskName,
			StartTs: startTS,
		},
		Ranges: []kv.KeyRange{{}},
	}
}
