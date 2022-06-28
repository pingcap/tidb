// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"

	backup "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	logbackup "github.com/pingcap/kvproto/pkg/logbackuppb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/br/pkg/streamhelper/config"
	"github.com/pingcap/tidb/kv"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

type flushSimulator struct {
	flushedEpoch uint64
	enabled      bool
}

func (c flushSimulator) makeError(requestedEpoch uint64) *errorpb.Error {
	if !c.enabled {
		return nil
	}
	if c.flushedEpoch == 0 {
		e := errorpb.Error{
			Message: "not flushed",
		}
		return &e
	}
	if c.flushedEpoch != requestedEpoch {
		e := errorpb.Error{
			Message: "flushed epoch not match",
		}
		return &e
	}
	return nil
}

func (c flushSimulator) fork() flushSimulator {
	return flushSimulator{
		enabled: c.enabled,
	}
}

type region struct {
	rng        kv.KeyRange
	leader     uint64
	epoch      uint64
	id         uint64
	checkpoint uint64

	fsim flushSimulator
}

type fakeStore struct {
	id      uint64
	regions map[uint64]*region
}

type fakeCluster struct {
	idAlloced uint64
	stores    map[uint64]*fakeStore
	regions   []*region
}

func overlaps(a, b kv.KeyRange) bool {
	if len(b.EndKey) == 0 {
		return len(a.EndKey) == 0 || bytes.Compare(a.EndKey, b.StartKey) > 0
	}
	if len(a.EndKey) == 0 {
		return len(b.EndKey) == 0 || bytes.Compare(b.EndKey, a.StartKey) > 0
	}
	return bytes.Compare(a.StartKey, b.EndKey) < 0 && bytes.Compare(b.StartKey, a.EndKey) < 0
}

func (f *region) splitAt(newID uint64, k string) *region {
	newRegion := &region{
		rng:        kv.KeyRange{StartKey: []byte(k), EndKey: f.rng.EndKey},
		leader:     f.leader,
		epoch:      f.epoch + 1,
		id:         newID,
		checkpoint: f.checkpoint,
		fsim:       f.fsim.fork(),
	}
	f.rng.EndKey = []byte(k)
	f.epoch += 1
	f.fsim = f.fsim.fork()
	return newRegion
}

func (f *region) flush() {
	f.fsim.flushedEpoch = f.epoch
}

func (f *fakeStore) GetLastFlushTSOfRegion(ctx context.Context, in *logbackup.GetLastFlushTSOfRegionRequest, opts ...grpc.CallOption) (*logbackup.GetLastFlushTSOfRegionResponse, error) {
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
			Checkpoint: region.checkpoint,
			Region: &logbackup.RegionIdentity{
				Id:           region.id,
				EpochVersion: region.epoch,
			},
		})
	}
	return resp, nil
}

// RegionScan gets a list of regions, starts from the region that contains key.
// Limit limits the maximum number of regions returned.
func (f *fakeCluster) RegionScan(ctx context.Context, key []byte, endKey []byte, limit int) ([]streamhelper.RegionWithLeader, error) {
	sort.Slice(f.regions, func(i, j int) bool {
		return bytes.Compare(f.regions[i].rng.StartKey, f.regions[j].rng.StartKey) < 0
	})

	result := make([]streamhelper.RegionWithLeader, 0, limit)
	for _, region := range f.regions {
		if overlaps(kv.KeyRange{StartKey: key, EndKey: endKey}, region.rng) && len(result) < limit {
			regionInfo := streamhelper.RegionWithLeader{
				Region: &metapb.Region{
					Id: region.id,
					RegionEpoch: &metapb.RegionEpoch{
						Version: region.epoch,
					},
				},
				Leader: &metapb.Peer{
					StoreId: region.leader,
				},
			}
			result = append(result, regionInfo)
		} else {
			break
		}
	}
	return result, nil
}

func (f *fakeCluster) GetLogBackupClient(ctx context.Context, storeID uint64) (logbackup.LogBackupClient, error) {
	return f.stores[storeID], nil
}

func (f *fakeCluster) findRegionById(rid uint64) *region {
	for _, r := range f.regions {
		if r.id == rid {
			return r
		}
	}
	return nil
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

func (f *fakeCluster) suffleLeader(rid uint64) {
	r := f.findRegionById(rid)
	peers := f.findPeers(rid)
	rand.Shuffle(len(peers), func(i, j int) {
		peers[i], peers[j] = peers[j], peers[i]
	})

	newLeader := peers[0]
	r.leader = newLeader
}

func (f *fakeCluster) splitAndScatter(keys ...string) {
	for _, key := range keys {
		f.splitAt(key)
	}
	for _, r := range f.regions {
		f.transferRegionTo(r.id, f.chooseStores(3))
		f.suffleLeader(r.id)
	}
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
			r.checkpoint += rand.Uint64() % 256
			if r.checkpoint < minCheckpoint {
				minCheckpoint = r.checkpoint
			}
			r.fsim.flushedEpoch = 0
		})
	}
	return minCheckpoint
}

func createFakeCluster(n int, simEnabled bool) *fakeCluster {
	c := &fakeCluster{
		stores:  map[uint64]*fakeStore{},
		regions: []*region{},
	}
	stores := make([]*fakeStore, 0, n)
	for i := 0; i < n; i++ {
		s := new(fakeStore)
		s.id = c.idAlloc()
		s.regions = map[uint64]*region{}
		stores = append(stores, s)
	}
	initialRegion := &region{
		rng:        kv.KeyRange{},
		leader:     stores[0].id,
		epoch:      0,
		id:         c.idAlloc(),
		checkpoint: 0,
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
	return fmt.Sprintf("%d(%d):[%s,%s);%dL%d", r.id, r.epoch, hex.EncodeToString(r.rng.StartKey), hex.EncodeToString(r.rng.EndKey), r.checkpoint, r.leader)
}

func (s *fakeStore) String() string {
	buf := new(strings.Builder)
	fmt.Fprintf(buf, "%d: ", s.id)
	for _, r := range s.regions {
		fmt.Fprintf(buf, "%s ", r)
	}
	return buf.String()
}

func (f *fakeCluster) flushAll() {
	for _, r := range f.regions {
		r.flush()
	}
}

func (s *fakeStore) flush() {
	for _, r := range s.regions {
		if r.leader == s.id {
			r.flush()
		}
	}
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

	mu sync.Mutex
}

func (t *testEnv) Begin(ctx context.Context, ch chan<- streamhelper.TaskEvent) error {
	tsk := streamhelper.TaskEvent{
		Type: streamhelper.EventAdd,
		Name: "whole",
		Info: &backup.StreamBackupTaskInfo{
			Name: "whole",
		},
	}
	ch <- tsk
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

func (t *testEnv) getCheckpoint() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.checkpoint
}

func TestBasic(t *testing.T) {
	c := createFakeCluster(4, false)
	defer func() {
		fmt.Println(c)
	}()
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	ctx := context.Background()
	minCheckpoint := c.advanceCheckpoints()
	env := &testEnv{fakeCluster: c, testCtx: t}
	adv := streamhelper.NewCheckpointAdvancer(env)
	coll := streamhelper.NewClusterCollector(ctx, env)
	err := adv.GetCheckpointInRange(ctx, []byte{}, []byte{}, coll)
	require.NoError(t, err)
	r, err := coll.Wait(ctx)
	require.NoError(t, err)
	require.Len(t, r.FailureSubranges, 0)
	require.Equal(t, r.Checkpoint, minCheckpoint, "%d %d", r.Checkpoint, minCheckpoint)
}

func TestTick(t *testing.T) {
	c := createFakeCluster(4, false)
	defer func() {
		fmt.Println(c)
	}()
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	env := &testEnv{fakeCluster: c, testCtx: t}
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.StartTaskListener(ctx)
	adv.UpdateConfigWith(func(cac *config.Config) {
		cac.FullScanTick = 0
	})
	require.NoError(t, adv.OnTick(ctx))
	for i := 0; i < 5; i++ {
		cp := c.advanceCheckpoints()
		require.NoError(t, adv.OnTick(ctx))
		require.Equal(t, env.getCheckpoint(), cp)
	}
}

func TestWithFailure(t *testing.T) {
	log.SetLevel(zapcore.DebugLevel)
	c := createFakeCluster(4, true)
	defer func() {
		fmt.Println(c)
	}()
	c.splitAndScatter("01", "02", "022", "023", "033", "04", "043")
	c.flushAll()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	env := &testEnv{fakeCluster: c, testCtx: t}
	adv := streamhelper.NewCheckpointAdvancer(env)
	adv.StartTaskListener(ctx)
	adv.UpdateConfigWith(func(cac *config.Config) {
		cac.FullScanTick = 0
	})
	require.NoError(t, adv.OnTick(ctx))

	cp := c.advanceCheckpoints()
	for _, v := range c.stores {
		v.flush()
		break
	}
	require.NoError(t, adv.OnTick(ctx))
	require.Less(t, env.getCheckpoint(), cp, "%d %d", env.getCheckpoint(), cp)

	for _, v := range c.stores {
		v.flush()
	}

	require.NoError(t, adv.OnTick(ctx))
	require.Equal(t, env.getCheckpoint(), cp)
}
