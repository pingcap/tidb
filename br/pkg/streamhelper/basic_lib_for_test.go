// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper_test

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
	"testing"
	"time"

	backup "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	logbackup "github.com/pingcap/kvproto/pkg/logbackuppb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/br/pkg/streamhelper/spans"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type flushSimulator struct {
	flushedEpoch atomic.Uint64
	enabled      bool
}

func (c *flushSimulator) makeError(requestedEpoch uint64) *errorpb.Error {
	if !c.enabled {
		return nil
	}
	if c.flushedEpoch.Load() == 0 {
		e := errorpb.Error{
			Message: "not flushed",
		}
		return &e
	}
	if c.flushedEpoch.Load() != requestedEpoch {
		e := errorpb.Error{
			Message: "flushed epoch not match",
		}
		return &e
	}
	return nil
}

func (c *flushSimulator) fork() flushSimulator {
	return flushSimulator{
		enabled: c.enabled,
	}
}

type region struct {
	rng        spans.Span
	leader     uint64
	epoch      uint64
	id         uint64
	checkpoint atomic.Uint64

	fsim flushSimulator

	locks []*txnlock.Lock
}

type fakeStore struct {
	id      uint64
	regions map[uint64]*region

	clientMu              sync.Mutex
	supportsSub           bool
	bootstrapAt           uint64
	fsub                  func(logbackup.SubscribeFlushEventResponse)
	onGetRegionCheckpoint func(*logbackup.GetLastFlushTSOfRegionRequest) error
}

type fakeCluster struct {
	mu        sync.Mutex
	idAlloced uint64
	stores    map[uint64]*fakeStore
	regions   []*region
	testCtx   *testing.T

	onGetClient               func(uint64) error
	onClearCache              func(uint64) error
	serviceGCSafePoint        uint64
	serviceGCSafePointDeleted bool
	currentTS                 uint64
}

func (r *region) splitAt(newID uint64, k string) *region {
	newRegion := &region{
		rng:    kv.KeyRange{StartKey: []byte(k), EndKey: r.rng.EndKey},
		leader: r.leader,
		epoch:  r.epoch + 1,
		id:     newID,
		fsim:   r.fsim.fork(),
	}
	newRegion.checkpoint.Store(r.checkpoint.Load())
	r.rng.EndKey = []byte(k)
	r.epoch += 1
	r.fsim = r.fsim.fork()
	return newRegion
}

func (r *region) flush() {
	r.fsim.flushedEpoch.Store(r.epoch)
}

type trivialFlushStream struct {
	c  <-chan logbackup.SubscribeFlushEventResponse
	cx context.Context
}

func (t trivialFlushStream) Recv() (*logbackup.SubscribeFlushEventResponse, error) {
	select {
	case item, ok := <-t.c:
		if !ok {
			return nil, io.EOF
		}
		return &item, nil
	case <-t.cx.Done():
		select {
		case item, ok := <-t.c:
			if !ok {
				return nil, io.EOF
			}
			return &item, nil
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

func (t trivialFlushStream) SendMsg(m any) error {
	return nil
}

func (t trivialFlushStream) RecvMsg(m any) error {
	return nil
}

func (f *fakeStore) GetID() uint64 {
	return f.id
}

func (f *fakeStore) SubscribeFlushEvent(ctx context.Context, in *logbackup.SubscribeFlushEventRequest, opts ...grpc.CallOption) (logbackup.LogBackup_SubscribeFlushEventClient, error) {
	f.clientMu.Lock()
	defer f.clientMu.Unlock()
	if !f.supportsSub {
		return nil, status.Error(codes.Unimplemented, "meow?")
	}

	ch := make(chan logbackup.SubscribeFlushEventResponse, 1024)
	f.fsub = func(glftrr logbackup.SubscribeFlushEventResponse) {
		ch <- glftrr
	}
	return trivialFlushStream{c: ch, cx: ctx}, nil
}

func (f *fakeStore) SetSupportFlushSub(b bool) {
	f.clientMu.Lock()
	defer f.clientMu.Unlock()

	f.bootstrapAt += 1
	f.supportsSub = b
}

func (f *fakeStore) GetLastFlushTSOfRegion(ctx context.Context, in *logbackup.GetLastFlushTSOfRegionRequest, opts ...grpc.CallOption) (*logbackup.GetLastFlushTSOfRegionResponse, error) {
	if f.onGetRegionCheckpoint != nil {
		err := f.onGetRegionCheckpoint(in)
		if err != nil {
			return nil, err
		}
	}
	resp := &logbackup.GetLastFlushTSOfRegionResponse{
		Checkpoints: []*logbackup.RegionCheckpoint{},
	}
	for _, r := range in.Regions {
		region, ok := f.regions[r.Id]
		if !ok || region.leader != f.id {
			resp.Checkpoints = append(resp.Checkpoints, &logbackup.RegionCheckpoint{
				Err: &errorpb.Error{
					Message: "not found",
				},
				Region: &logbackup.RegionIdentity{
					Id:           region.id,
					EpochVersion: region.epoch,
				},
			})
			continue
		}
		if err := region.fsim.makeError(r.EpochVersion); err != nil {
			resp.Checkpoints = append(resp.Checkpoints, &logbackup.RegionCheckpoint{
				Err: err,
				Region: &logbackup.RegionIdentity{
					Id:           region.id,
					EpochVersion: region.epoch,
				},
			})
			continue
		}
		if region.epoch != r.EpochVersion {
			resp.Checkpoints = append(resp.Checkpoints, &logbackup.RegionCheckpoint{
				Err: &errorpb.Error{
					Message: "epoch not match",
				},
				Region: &logbackup.RegionIdentity{
					Id:           region.id,
					EpochVersion: region.epoch,
				},
			})
			continue
		}
		resp.Checkpoints = append(resp.Checkpoints, &logbackup.RegionCheckpoint{
			Checkpoint: region.checkpoint.Load(),
			Region: &logbackup.RegionIdentity{
				Id:           region.id,
				EpochVersion: region.epoch,
			},
		})
	}
	log.Debug("Get last flush ts of region", zap.Stringer("in", in), zap.Stringer("out", resp))
	return resp, nil
}

// Updates the service GC safe point for the cluster.
// Returns the latest service GC safe point.
// If the arguments is `0`, this would remove the service safe point.
func (f *fakeCluster) BlockGCUntil(ctx context.Context, at uint64) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.serviceGCSafePoint > at {
		return f.serviceGCSafePoint, nil
	}
	f.serviceGCSafePoint = at
	return at, nil
}

func (f *fakeCluster) UnblockGC(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.serviceGCSafePointDeleted = true
	return nil
}

func (f *fakeCluster) FetchCurrentTS(ctx context.Context) (uint64, error) {
	return f.currentTS, nil
}

// RegionScan gets a list of regions, starts from the region that contains key.
// Limit limits the maximum number of regions returned.
func (f *fakeCluster) RegionScan(ctx context.Context, key []byte, endKey []byte, limit int) ([]streamhelper.RegionWithLeader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	sort.Slice(f.regions, func(i, j int) bool {
		return bytes.Compare(f.regions[i].rng.StartKey, f.regions[j].rng.StartKey) < 0
	})

	result := make([]streamhelper.RegionWithLeader, 0, limit)
	for _, region := range f.regions {
		if spans.Overlaps(kv.KeyRange{StartKey: key, EndKey: endKey}, region.rng) && len(result) < limit {
			regionInfo := streamhelper.RegionWithLeader{
				Region: &metapb.Region{
					Id:       region.id,
					StartKey: region.rng.StartKey,
					EndKey:   region.rng.EndKey,
					RegionEpoch: &metapb.RegionEpoch{
						Version: region.epoch,
					},
				},
				Leader: &metapb.Peer{
					StoreId: region.leader,
				},
			}
			result = append(result, regionInfo)
		} else if bytes.Compare(region.rng.StartKey, key) > 0 {
			break
		}
	}
	return result, nil
}

func (f *fakeCluster) GetLogBackupClient(ctx context.Context, storeID uint64) (logbackup.LogBackupClient, error) {
	if f.onGetClient != nil {
		err := f.onGetClient(storeID)
		if err != nil {
			return nil, err
		}
	}
	cli, ok := f.stores[storeID]
	if !ok {
		f.testCtx.Fatalf("the store %d doesn't exist", storeID)
	}
	return cli, nil
}

func (f *fakeCluster) ClearCache(ctx context.Context, storeID uint64) error {
	if f.onClearCache != nil {
		err := f.onClearCache(storeID)
		if err != nil {
			return err
		}
		return nil
	}
	return nil
}

// Stores returns the store metadata from the cluster.
func (f *fakeCluster) Stores(ctx context.Context) ([]streamhelper.Store, error) {
	r := make([]streamhelper.Store, 0, len(f.stores))
	for id, s := range f.stores {
		r = append(r, streamhelper.Store{ID: id, BootAt: s.bootstrapAt})
	}
	return r, nil
}

func (f *fakeCluster) findRegionById(rid uint64) *region {
	for _, r := range f.regions {
		if r.id == rid {
			return r
		}
	}
	return nil
}

func (f *fakeCluster) LockRegion(r *region, locks []*txnlock.Lock) *region {
	r.locks = locks
	return r
}

func (f *fakeCluster) findRegionByKey(key []byte) *region {
	for _, r := range f.regions {
		if bytes.Compare(key, r.rng.StartKey) >= 0 && (len(r.rng.EndKey) == 0 || bytes.Compare(key, r.rng.EndKey) < 0) {
			return r
		}
	}
	panic(fmt.Sprintf("inconsistent key space; key = %X", key))
}

func (f *fakeCluster) transferRegionTo(rid uint64, newPeers []uint64) {
	r := f.findRegionById(rid)
storeLoop:
	for _, store := range f.stores {
		for _, pid := range newPeers {
			if pid == store.id {
				store.regions[rid] = r
				continue storeLoop
			}
		}
		delete(store.regions, rid)
	}
}

func (f *fakeCluster) splitAt(key string) {
	k := []byte(key)
	r := f.findRegionByKey(k)
	newRegion := r.splitAt(f.idAlloc(), key)
	for _, store := range f.stores {
		_, ok := store.regions[r.id]
		if ok {
			store.regions[newRegion.id] = newRegion
		}
	}
	f.regions = append(f.regions, newRegion)
}

func (f *fakeCluster) idAlloc() uint64 {
	f.idAlloced++
	return f.idAlloced
}

func (f *fakeCluster) chooseStores(n int) []uint64 {
	s := make([]uint64, 0, len(f.stores))
	for id := range f.stores {
		s = append(s, id)
	}
	rand.Shuffle(len(s), func(i, j int) {
		s[i], s[j] = s[j], s[i]
	})
	return s[:n]
}

func (f *fakeCluster) findPeers(rid uint64) (result []uint64) {
	for _, store := range f.stores {
		if _, ok := store.regions[rid]; ok {
			result = append(result, store.id)
		}
	}
	return
}

func (f *fakeCluster) shuffleLeader(rid uint64) {
	r := f.findRegionById(rid)
	peers := f.findPeers(rid)
	rand.Shuffle(len(peers), func(i, j int) {
		peers[i], peers[j] = peers[j], peers[i]
	})

	newLeader := peers[0]
	r.leader = newLeader
}

func (f *fakeCluster) splitAndScatter(keys ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, key := range keys {
		f.splitAt(key)
	}
	for _, r := range f.regions {
		chosen := f.chooseStores(3)
		f.transferRegionTo(r.id, chosen)
		f.shuffleLeader(r.id)
	}
}

// Remove a store.
// Note: this won't add new peer for regions from the store.
func (f *fakeCluster) removeStore(id uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	s := f.stores[id]
	for _, r := range s.regions {
		if r.leader == id {
			f.updateRegion(r.id, func(r *region) {
				ps := f.findPeers(r.id)
				for _, p := range ps {
					if p != r.leader {
						log.Info("remove store: transforming leader",
							zap.Uint64("region", r.id),
							zap.Uint64("new-leader", p),
							zap.Uint64("old-leader", r.leader))
						r.leader = p
						break
					}
				}
			})
		}
	}

	delete(f.stores, id)
}

// a stub once in the future we want to make different stores hold different region instances.
func (f *fakeCluster) updateRegion(rid uint64, mut func(*region)) {
	r := f.findRegionById(rid)
	mut(r)
}

func (f *fakeCluster) advanceCheckpoints() uint64 {
	minCheckpoint := uint64(math.MaxUint64)
	for _, r := range f.regions {
		f.updateRegion(r.id, func(r *region) {
			// The current implementation assumes that the server never returns checkpoint with value 0.
			// This assumption is true for the TiKV implementation, simulating it here.
			cp := r.checkpoint.Add(rand.Uint64()%256 + 1)
			if cp < minCheckpoint {
				minCheckpoint = cp
			}
			r.fsim.flushedEpoch.Store(0)
		})
	}
	log.Info("checkpoint updated", zap.Uint64("to", minCheckpoint))
	return minCheckpoint
}

func (f *fakeCluster) advanceCheckpointBy(duration time.Duration) uint64 {
	minCheckpoint := uint64(math.MaxUint64)
	for _, r := range f.regions {
		f.updateRegion(r.id, func(r *region) {
			newCheckpointTime := oracle.GetTimeFromTS(r.checkpoint.Load()).Add(duration)
			newCheckpoint := oracle.GoTimeToTS(newCheckpointTime)
			r.checkpoint.Store(newCheckpoint)
			if newCheckpoint < minCheckpoint {
				minCheckpoint = newCheckpoint
			}
			r.fsim.flushedEpoch.Store(0)
		})
	}
	log.Info("checkpoint updated", zap.Uint64("to", minCheckpoint))
	return minCheckpoint
}

func (f *fakeCluster) advanceClusterTimeBy(duration time.Duration) uint64 {
	newTime := oracle.GoTimeToTS(oracle.GetTimeFromTS(f.currentTS).Add(duration))
	f.currentTS = newTime
	return newTime
}

func createFakeCluster(t *testing.T, n int, simEnabled bool) *fakeCluster {
	c := &fakeCluster{
		stores:  map[uint64]*fakeStore{},
		regions: []*region{},
		testCtx: t,
	}
	stores := make([]*fakeStore, 0, n)
	for i := 0; i < n; i++ {
		s := new(fakeStore)
		s.id = c.idAlloc()
		s.regions = map[uint64]*region{}
		stores = append(stores, s)
	}
	initialRegion := &region{
		rng:    kv.KeyRange{},
		leader: stores[0].id,
		epoch:  0,
		id:     c.idAlloc(),
		fsim: flushSimulator{
			enabled: simEnabled,
		},
	}
	for i := 0; i < 3; i++ {
		if i < len(stores) {
			stores[i].regions[initialRegion.id] = initialRegion
		}
	}
	for _, s := range stores {
		c.stores[s.id] = s
	}
	c.regions = append(c.regions, initialRegion)
	return c
}

func (r *region) String() string {
	return fmt.Sprintf("%d(%d):[%s, %s);%dL%dF%d",
		r.id,
		r.epoch,
		hex.EncodeToString(r.rng.StartKey),
		hex.EncodeToString(r.rng.EndKey),
		r.checkpoint.Load(),
		r.leader,
		r.fsim.flushedEpoch.Load())
}

func (f *fakeStore) String() string {
	buf := new(strings.Builder)
	fmt.Fprintf(buf, "%d: ", f.id)
	for _, r := range f.regions {
		fmt.Fprintf(buf, "%s ", r)
	}
	return buf.String()
}

func (f *fakeCluster) flushAll() {
	for _, r := range f.stores {
		r.flush()
	}
}

func (f *fakeCluster) flushAllExcept(keys ...string) {
	for _, s := range f.stores {
		s.flushExcept(keys...)
	}
}

func (f *fakeStore) flushExcept(keys ...string) {
	resp := make([]*logbackup.FlushEvent, 0, len(f.regions))
outer:
	for _, r := range f.regions {
		if r.leader != f.id {
			continue
		}
		// Note: can we make it faster?
		for _, key := range keys {
			if utils.CompareBytesExt(r.rng.StartKey, false, []byte(key), false) <= 0 &&
				utils.CompareBytesExt([]byte(key), false, r.rng.EndKey, true) < 0 {
				continue outer
			}
		}
		if r.leader == f.id {
			r.flush()
			resp = append(resp, &logbackup.FlushEvent{
				StartKey:   codec.EncodeBytes(nil, r.rng.StartKey),
				EndKey:     codec.EncodeBytes(nil, r.rng.EndKey),
				Checkpoint: r.checkpoint.Load(),
			})
		}
	}

	if f.fsub != nil {
		f.fsub(logbackup.SubscribeFlushEventResponse{
			Events: resp,
		})
	}
}

func (f *fakeStore) flush() {
	f.flushExcept()
}

func (f *fakeCluster) String() string {
	buf := new(strings.Builder)
	fmt.Fprint(buf, ">>> fake cluster <<<\nregions: ")
	for _, region := range f.regions {
		fmt.Fprint(buf, region, " ")
	}
	fmt.Fprintln(buf)
	for _, store := range f.stores {
		fmt.Fprintln(buf, store)
	}
	return buf.String()
}

type testEnv struct {
	*fakeCluster
	checkpoint uint64
	testCtx    *testing.T
	ranges     []kv.KeyRange
	taskCh     chan<- streamhelper.TaskEvent

	resolveLocks func([]*txnlock.Lock, *tikv.KeyLocation) (*tikv.KeyLocation, error)

	mu sync.Mutex
	pd.Client
}

func (t *testEnv) Begin(ctx context.Context, ch chan<- streamhelper.TaskEvent) error {
	rngs := t.ranges
	if len(rngs) == 0 {
		rngs = []kv.KeyRange{{}}
	}
	tsk := streamhelper.TaskEvent{
		Type: streamhelper.EventAdd,
		Name: "whole",
		Info: &backup.StreamBackupTaskInfo{
			Name: "whole",
		},
		Ranges: rngs,
	}
	ch <- tsk
	t.taskCh = ch
	return nil
}

func (t *testEnv) UploadV3GlobalCheckpointForTask(ctx context.Context, _ string, checkpoint uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if checkpoint < t.checkpoint {
		t.testCtx.Fatalf("checkpoint rolling back (from %d to %d)", t.checkpoint, checkpoint)
	}
	t.checkpoint = checkpoint
	return nil
}

func (t *testEnv) ClearV3GlobalCheckpointForTask(ctx context.Context, taskName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.checkpoint = 0
	return nil
}

func (t *testEnv) PauseTask(ctx context.Context, taskName string) error {
	t.taskCh <- streamhelper.TaskEvent{
		Type: streamhelper.EventPause,
		Name: taskName,
	}
	return nil
}

func (t *testEnv) ResumeTask(ctx context.Context) error {
	t.taskCh <- streamhelper.TaskEvent{
		Type: streamhelper.EventResume,
		Name: "whole",
	}
	return nil
}

func (t *testEnv) getCheckpoint() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.checkpoint
}

func (t *testEnv) unregisterTask() {
	t.taskCh <- streamhelper.TaskEvent{
		Type: streamhelper.EventDel,
		Name: "whole",
	}
}

func (t *testEnv) putTask() {
	rngs := t.ranges
	if len(rngs) == 0 {
		rngs = []kv.KeyRange{{}}
	}
	tsk := streamhelper.TaskEvent{
		Type: streamhelper.EventAdd,
		Name: "whole",
		Info: &backup.StreamBackupTaskInfo{
			Name: "whole",
		},
		Ranges: rngs,
	}
	t.taskCh <- tsk
}

func (t *testEnv) ScanLocksInOneRegion(bo *tikv.Backoffer, key []byte, maxVersion uint64, limit uint32) ([]*txnlock.Lock, *tikv.KeyLocation, error) {
	for _, r := range t.regions {
		if len(r.locks) != 0 {
			return r.locks, &tikv.KeyLocation{
				Region: tikv.NewRegionVerID(r.id, 0, 0),
			}, nil
		}
	}
	return nil, nil, nil
}

func (t *testEnv) ResolveLocksInOneRegion(bo *tikv.Backoffer, locks []*txnlock.Lock, loc *tikv.KeyLocation) (*tikv.KeyLocation, error) {
	for _, r := range t.regions {
		if loc != nil && loc.Region.GetID() == r.id {
			// reset locks
			r.locks = nil
			return t.resolveLocks(locks, loc)
		}
	}
	return nil, nil
}

func (t *testEnv) Identifier() string {
	return "advance test"
}

func (t *testEnv) GetStore() tikv.Storage {
	// only used for GetRegionCache once in resolve lock
	return &mockTiKVStore{regionCache: tikv.NewRegionCache(&mockPDClient{fakeRegions: t.regions})}
}

type mockKVStore struct {
	kv.Storage
}

type mockTiKVStore struct {
	mockKVStore
	tikv.Storage
	regionCache *tikv.RegionCache
}

func (s *mockTiKVStore) GetRegionCache() *tikv.RegionCache {
	return s.regionCache
}

func (s *mockTiKVStore) SendReq(bo *tikv.Backoffer, req *tikvrpc.Request, regionID tikv.RegionVerID, timeout time.Duration) (*tikvrpc.Response, error) {
	scanResp := kvrpcpb.ScanLockResponse{
		// we don't need mock locks here, because we already have mock locks in testEnv.Scanlocks.
		// this behaviour is align with gc_worker_test
		Locks:       nil,
		RegionError: nil,
	}
	return &tikvrpc.Response{Resp: &scanResp}, nil
}

type mockPDClient struct {
	pd.Client
	fakeRegions []*region
}

func (p *mockPDClient) ScanRegions(ctx context.Context, key, endKey []byte, limit int, _ ...pd.GetRegionOption) ([]*pd.Region, error) {
	sort.Slice(p.fakeRegions, func(i, j int) bool {
		return bytes.Compare(p.fakeRegions[i].rng.StartKey, p.fakeRegions[j].rng.StartKey) < 0
	})

	result := make([]*pd.Region, 0, len(p.fakeRegions))
	for _, region := range p.fakeRegions {
		if spans.Overlaps(kv.KeyRange{StartKey: key, EndKey: endKey}, region.rng) && len(result) < limit {
			regionInfo := newMockRegion(region.id, region.rng.StartKey, region.rng.EndKey)
			result = append(result, regionInfo)
		} else if bytes.Compare(region.rng.StartKey, key) > 0 {
			break
		}
	}
	return result, nil
}

func (p *mockPDClient) GetStore(_ context.Context, storeID uint64) (*metapb.Store, error) {
	return &metapb.Store{
		Id:      storeID,
		Address: fmt.Sprintf("127.0.0.%d", storeID),
	}, nil
}

func (p *mockPDClient) GetClusterID(ctx context.Context) uint64 {
	return 1
}

func newMockRegion(regionID uint64, startKey []byte, endKey []byte) *pd.Region {
	leader := &metapb.Peer{
		Id:      regionID,
		StoreId: 1,
		Role:    metapb.PeerRole_Voter,
	}

	return &pd.Region{
		Meta: &metapb.Region{
			Id:       regionID,
			StartKey: startKey,
			EndKey:   endKey,
			Peers:    []*metapb.Peer{leader},
		},
		Leader: leader,
	}
}
