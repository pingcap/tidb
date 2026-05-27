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

package streamhelper_test

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	backup "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/br/pkg/streamhelper/spans"
	"github.com/pingcap/tidb/br/pkg/utiltest/fakecluster"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/clients/router"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeCluster struct {
	*fakecluster.Cluster
}

func createFakeCluster(t *testing.T, n int, simEnabled bool) *fakeCluster {
	t.Helper()
	return &fakeCluster{Cluster: fakecluster.NewBasicCluster(n, simEnabled)}
}

func (f *fakeCluster) splitAndScatter(keys ...string) {
	f.SplitAndScatter(keys...)
}

func (f *fakeCluster) removeStore(id uint64) {
	f.RemoveStore(id)
}

func (f *fakeCluster) advanceCheckpoints() uint64 {
	return f.AdvanceCheckpoints()
}

func (f *fakeCluster) advanceCheckpointBy(duration time.Duration) uint64 {
	return f.AdvanceCheckpointBy(duration)
}

func (f *fakeCluster) advanceClusterTimeBy(duration time.Duration) uint64 {
	return f.AdvanceClusterTimeBy(duration)
}

func (f *fakeCluster) flushAll() {
	f.FlushAll()
}

func (f *fakeCluster) flushAllExcept(keys ...string) {
	f.FlushAllExcept(keys...)
}

func (f *fakeCluster) storeList() []*fakecluster.Store {
	return f.StoreList()
}

type testEnv struct {
	*fakecluster.Cluster
	checkpoint     uint64
	pdDisconnected atomic.Bool
	testCtx        *testing.T
	ranges         []kv.KeyRange
	taskCh         chan<- streamhelper.TaskEvent
	task           streamhelper.TaskEvent

	resolveLocks func([]*txnlock.Lock, *tikv.KeyLocation) (*tikv.KeyLocation, error)

	mu sync.Mutex
	pd.Client
}

func newTestEnv(c *fakeCluster, t *testing.T) *testEnv {
	env := &testEnv{
		Cluster: c.Cluster,
		testCtx: t,
	}
	rngs := env.ranges
	if len(rngs) == 0 {
		rngs = []kv.KeyRange{{}}
	}
	env.task = streamhelper.TaskEvent{
		Type: streamhelper.EventAdd,
		Name: "whole",
		Info: &backup.StreamBackupTaskInfo{
			Name:    "whole",
			StartTs: 0,
		},
		Ranges: rngs,
	}
	return env
}

func (t *testEnv) Begin(ctx context.Context, ch chan<- streamhelper.TaskEvent) error {
	_ = ctx
	ch <- t.task
	t.taskCh = ch
	return nil
}

func (t *testEnv) UploadV3GlobalCheckpointForTask(ctx context.Context, _ string, checkpoint uint64) error {
	_ = ctx
	t.mu.Lock()
	defer t.mu.Unlock()

	if checkpoint < t.checkpoint {
		log.Error("checkpoint rolling back",
			zap.Uint64("from", t.checkpoint),
			zap.Uint64("to", checkpoint),
			zap.Stack("stack"))
		return errors.New("checkpoint rolling back")
	}
	t.checkpoint = checkpoint
	return nil
}

func (t *testEnv) mockPDConnectionError() {
	t.pdDisconnected.Store(true)
}

func (t *testEnv) connectPD() bool {
	if !t.pdDisconnected.Load() {
		return true
	}
	t.pdDisconnected.Store(false)
	return false
}

func (t *testEnv) GetGlobalCheckpointForTask(ctx context.Context, taskName string) (uint64, error) {
	_ = ctx
	_ = taskName
	if !t.connectPD() {
		return 0, status.Error(codes.Unavailable, "pd disconnected")
	}
	return t.checkpoint, nil
}

func (t *testEnv) ClearV3GlobalCheckpointForTask(ctx context.Context, taskName string) error {
	_ = ctx
	_ = taskName
	t.mu.Lock()
	defer t.mu.Unlock()

	t.checkpoint = 0
	return nil
}

func (t *testEnv) PauseTask(ctx context.Context, taskName string, _ ...streamhelper.PauseTaskOption) error {
	_ = ctx
	t.taskCh <- streamhelper.TaskEvent{
		Type: streamhelper.EventPause,
		Name: taskName,
	}
	return nil
}

func (t *testEnv) ResumeTask(ctx context.Context) error {
	_ = ctx
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

func (t *testEnv) advanceCheckpointBy(duration time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()

	log.Info("advance checkpoint", zap.Duration("duration", duration), zap.Uint64("from", t.checkpoint))
	t.checkpoint = oracle.GoTimeToTS(oracle.GetTimeFromTS(t.checkpoint).Add(duration))
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
	t.taskCh <- streamhelper.TaskEvent{
		Type: streamhelper.EventAdd,
		Name: "whole",
		Info: &backup.StreamBackupTaskInfo{
			Name:    "whole",
			StartTs: 0,
		},
		Ranges: rngs,
	}
}

func (t *testEnv) ScanLocksInOneRegion(
	bo *tikv.Backoffer,
	key []byte,
	endKey []byte,
	maxVersion uint64,
	limit uint32,
) ([]*txnlock.Lock, *tikv.KeyLocation, error) {
	_ = bo
	_ = key
	_ = endKey
	_ = limit

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.MaxTS != maxVersion {
		return nil, nil, errors.Errorf("unexpect max version in scan lock, expected %d, actual %d", t.MaxTS, maxVersion)
	}
	for _, r := range t.RegionList() {
		if len(r.Locks) != 0 {
			locks := make([]*txnlock.Lock, 0, len(r.Locks))
			for _, l := range r.Locks {
				if l.TxnID < maxVersion {
					locks = append(locks, l)
				}
			}
			return locks, &tikv.KeyLocation{
				Region: tikv.NewRegionVerID(r.ID, 0, 0),
			}, nil
		}
	}
	return nil, &tikv.KeyLocation{}, nil
}

func (t *testEnv) ResolveLocksInOneRegion(
	bo *tikv.Backoffer,
	locks []*txnlock.Lock,
	loc *tikv.KeyLocation,
) (*tikv.KeyLocation, error) {
	_ = bo
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, r := range t.RegionList() {
		if loc != nil && loc.Region.GetID() == r.ID {
			r.Locks = nil
			return t.resolveLocks(locks, loc)
		}
	}
	return loc, nil
}

func (t *testEnv) Identifier() string {
	return "advance test"
}

func (t *testEnv) GetStore() tikv.Storage {
	return &mockTiKVStore{regionCache: tikv.NewRegionCache(&mockPDClient{fakeRegions: t.RegionList()})}
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

func (s *mockTiKVStore) SendReq(
	bo *tikv.Backoffer,
	req *tikvrpc.Request,
	regionID tikv.RegionVerID,
	timeout time.Duration,
) (*tikvrpc.Response, error) {
	_ = bo
	_ = req
	_ = regionID
	_ = timeout
	scanResp := kvrpcpb.ScanLockResponse{
		Locks:       nil,
		RegionError: nil,
	}
	return &tikvrpc.Response{Resp: &scanResp}, nil
}

type mockPDClient struct {
	pd.Client
	fakeRegions []*fakecluster.Region
}

func (p *mockPDClient) ScanRegions(
	ctx context.Context,
	key []byte,
	endKey []byte,
	limit int,
	_ ...opt.GetRegionOption,
) ([]*router.Region, error) {
	_ = ctx
	sort.Slice(p.fakeRegions, func(i, j int) bool {
		return bytes.Compare(p.fakeRegions[i].Range.StartKey, p.fakeRegions[j].Range.StartKey) < 0
	})

	result := make([]*router.Region, 0, len(p.fakeRegions))
	for _, region := range p.fakeRegions {
		if spans.Overlaps(kv.KeyRange{StartKey: key, EndKey: endKey}, region.Range) && len(result) < limit {
			result = append(result, newMockRegion(region.ID, region.Range.StartKey, region.Range.EndKey))
		} else if bytes.Compare(region.Range.StartKey, key) > 0 {
			break
		}
	}
	return result, nil
}

func (p *mockPDClient) GetStore(_ context.Context, storeID uint64, _ ...opt.GetStoreOption) (*metapb.Store, error) {
	return &metapb.Store{
		Id:      storeID,
		Address: fmt.Sprintf("127.0.0.%d", storeID),
	}, nil
}

func (p *mockPDClient) GetAllStores(_ context.Context, _ ...opt.GetStoreOption) ([]*metapb.Store, error) {
	return []*metapb.Store{
		{
			Id:      1,
			Address: "127.0.0.1",
		},
	}, nil
}

func (p *mockPDClient) GetClusterID(ctx context.Context) uint64 {
	_ = ctx
	return 1
}

func (p *mockPDClient) WithCallerComponent(_ caller.Component) pd.Client {
	return p
}

func newMockRegion(regionID uint64, startKey []byte, endKey []byte) *router.Region {
	leader := &metapb.Peer{
		Id:      regionID,
		StoreId: 1,
		Role:    metapb.PeerRole_Voter,
	}
	return &router.Region{
		Meta: &metapb.Region{
			Id:       regionID,
			StartKey: startKey,
			EndKey:   endKey,
			Peers:    []*metapb.Peer{leader},
		},
		Leader: leader,
	}
}
