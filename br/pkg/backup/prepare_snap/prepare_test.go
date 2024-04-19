// Copyright 2024 PingCAP, Inc.
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

package preparesnap_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	brpb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	. "github.com/pingcap/tidb/br/pkg/backup/prepare_snap"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap/zapcore"
)

type mockStore struct {
	mu sync.Mutex

	output     chan brpb.PrepareSnapshotBackupResponse
	leaseUntil time.Time

	successRegions []metapb.Region
	onWaitApply    func(*metapb.Region) error

	waitApplyDelay     func()
	delaiedWaitApplies sync.WaitGroup

	injectConnErr <-chan error
	now           func() time.Time
}

func (s *mockStore) Send(req *brpb.PrepareSnapshotBackupRequest) error {
	switch req.Ty {
	case brpb.PrepareSnapshotBackupRequestType_WaitApply:
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, region := range req.Regions {
			resp := brpb.PrepareSnapshotBackupResponse{
				Ty:     brpb.PrepareSnapshotBackupEventType_WaitApplyDone,
				Region: region,
			}
			if s.onWaitApply != nil {
				if err := s.onWaitApply(region); err != nil {
					resp.Error = &errorpb.Error{
						Message: err.Error(),
					}
				}
			}
			if s.waitApplyDelay != nil {
				s.delaiedWaitApplies.Add(1)
				go func() {
					defer s.delaiedWaitApplies.Done()
					s.waitApplyDelay()
					s.sendResp(resp)
				}()
			} else {
				s.sendResp(resp)
			}
			if resp.Error == nil {
				s.successRegions = append(s.successRegions, *region)
			}
		}
	case brpb.PrepareSnapshotBackupRequestType_UpdateLease:
		s.mu.Lock()
		defer s.mu.Unlock()
		expired := s.leaseUntil.Before(s.now())
		s.leaseUntil = s.now().Add(time.Duration(req.LeaseInSeconds) * time.Second)
		s.sendResp(brpb.PrepareSnapshotBackupResponse{
			Ty:               brpb.PrepareSnapshotBackupEventType_UpdateLeaseResult,
			LastLeaseIsValid: !expired,
		})
	case brpb.PrepareSnapshotBackupRequestType_Finish:
		s.mu.Lock()
		defer s.mu.Unlock()
		expired := s.leaseUntil.Before(s.now())
		s.leaseUntil = time.Time{}
		s.sendResp(brpb.PrepareSnapshotBackupResponse{
			Ty:               brpb.PrepareSnapshotBackupEventType_UpdateLeaseResult,
			LastLeaseIsValid: !expired,
		})
		close(s.output)
	}
	return nil
}

func (s *mockStore) sendResp(resp brpb.PrepareSnapshotBackupResponse) {
	s.output <- resp
}

func (s *mockStore) Recv() (*brpb.PrepareSnapshotBackupResponse, error) {
	for {
		select {
		case out, ok := <-s.output:
			if !ok {
				return nil, io.EOF
			}
			return &out, nil
		case err, ok := <-s.injectConnErr:
			if ok {
				return nil, err
			} else {
				s.injectConnErr = nil
			}
		}
	}
}

type mockStores struct {
	mu               sync.Mutex
	stores           map[uint64]*mockStore
	onCreateStore    func(*mockStore)
	connectDelay     func(uint64) <-chan struct{}
	onConnectToStore func(uint64) error

	pdc *tikv.RegionCache
}

func newTestEnv(pdc pd.Client) *mockStores {
	r := tikv.NewRegionCache(pdc)
	stores, err := pdc.GetAllStores(context.Background())
	if err != nil {
		panic(err)
	}
	ss := map[uint64]*mockStore{}
	for _, store := range stores {
		ss[store.Id] = nil
	}
	ms := &mockStores{
		stores:        ss,
		pdc:           r,
		onCreateStore: func(ms *mockStore) {},
	}
	return ms
}

func (m *mockStores) GetAllLiveStores(ctx context.Context) ([]*metapb.Store, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	res := []*metapb.Store{}
	for id := range m.stores {
		res = append(res, &metapb.Store{Id: id})
	}
	return res, nil
}

func (m *mockStores) ConnectToStore(ctx context.Context, storeID uint64) (PrepareClient, error) {
	m.mu.Lock()
	defer func() {
		m.mu.Unlock()
		if m.connectDelay != nil {
			if ch := m.connectDelay(storeID); ch != nil {
				<-ch
			}
		}
	}()

	if m.onConnectToStore != nil {
		err := m.onConnectToStore(storeID)
		if err != nil {
			return nil, err
		}
	}

	s, ok := m.stores[storeID]
	if !ok || s == nil {
		m.stores[storeID] = &mockStore{
			output:         make(chan brpb.PrepareSnapshotBackupResponse, 20480),
			successRegions: []metapb.Region{},
			onWaitApply: func(r *metapb.Region) error {
				return nil
			},
			now: func() time.Time {
				return time.Now()
			},
		}
		m.onCreateStore(m.stores[storeID])
	}
	return AdaptForGRPCInTest(m.stores[storeID]), nil
}

func (m *mockStores) LoadRegionsInKeyRange(ctx context.Context, startKey []byte, endKey []byte) (regions []Region, err error) {
	if len(endKey) == 0 {
		// This is encoded [0xff; 8].
		// Workaround for https://github.com/tikv/client-go/issues/1051.
		endKey = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	}
	rs, err := m.pdc.LoadRegionsInKeyRange(tikv.NewBackoffer(ctx, 100), startKey, endKey)
	if err != nil {
		return nil, err
	}
	rrs := make([]Region, 0, len(rs))
	for _, r := range rs {
		rrs = append(rrs, r)
	}
	return rrs, nil
}

type rng [2][]byte

func (m *mockStores) AssertSafeForBackup(t *testing.T) {
	m.mu.Lock()
	defer m.mu.Unlock()

	res := []rng{}
	for _, store := range m.stores {
		store.mu.Lock()
		for _, region := range store.successRegions {
			res = append(res, rng{region.StartKey, region.EndKey})
		}
		now := store.now()
		if store.leaseUntil.Before(now) {
			t.Fatalf("lease has expired: at %s, now is %s", store.leaseUntil, now)
		}
		store.mu.Unlock()
	}
	slices.SortFunc(res, func(a, b rng) int {
		return bytes.Compare(a[0], b[0])
	})
	for i := 1; i < len(res); i++ {
		if bytes.Compare(res[i-1][1], res[i][0]) < 0 {
			t.Fatalf("hole: %s %s", hex.EncodeToString(res[i-1][1]), hex.EncodeToString(res[i][0]))
		}
	}
}

func (m *mockStores) AssertIsNormalMode(t *testing.T) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id, store := range m.stores {
		store.mu.Lock()
		if !store.leaseUntil.Before(store.now()) {
			t.Fatalf("lease in store %d doesn't expire, the store may not work as normal", id)
		}
		store.mu.Unlock()
	}
}

func fakeCluster(t *testing.T, nodes int, keys ...[]byte) pd.Client {
	tmp := t.TempDir()
	_, pdc, cluster, err := unistore.New(tmp, nil)
	unistore.BootstrapWithMultiStores(cluster, nodes)
	require.NoError(t, err)
	cluster.SplitArbitrary(keys...)
	return pdc
}

func dummyRegions(size int) [][]byte {
	// Generate regions like "a", "b", ..., "z", "aa", "ba", ..., "zz", "aaa"
	res := [][]byte{}
	for i := 0; i < size; i++ {
		s := make([]byte, 0, i/26)
		for j := i; j > 0; j /= 26 {
			s = append(s, byte('a')+byte(j%26))
		}
		res = append(res, s)
	}
	slices.SortFunc(res, bytes.Compare)
	return res
}

func TestBasic(t *testing.T) {
	req := require.New(t)
	pdc := fakeCluster(t, 3, dummyRegions(100)...)
	ms := newTestEnv(pdc)

	ctx := context.Background()
	prep := New(ms)
	req.NoError(prep.DriveLoopAndWaitPrepare(ctx))
	ms.AssertSafeForBackup(t)
	req.NoError(prep.Finalize(ctx))
	ms.AssertIsNormalMode(t)
}

func TestFailDueToErr(t *testing.T) {
	req := require.New(t)
	pdc := fakeCluster(t, 3, dummyRegions(100)...)
	ms := newTestEnv(pdc)

	ms.onCreateStore = func(ms *mockStore) {
		ms.onWaitApply = func(r *metapb.Region) error {
			return errors.New("failed meow")
		}
	}

	ctx := context.Background()
	prep := New(ms)
	prep.RetryBackoff = 100 * time.Millisecond
	prep.RetryLimit = 3
	now := time.Now()
	req.Error(prep.DriveLoopAndWaitPrepare(ctx))
	req.Greater(time.Since(now), 300*time.Millisecond)
	req.NoError(prep.Finalize(ctx))
	ms.AssertIsNormalMode(t)
}

func TestError(t *testing.T) {
	req := require.New(t)
	pdc := fakeCluster(t, 3, dummyRegions(100)...)
	ms := newTestEnv(pdc)

	ms.onCreateStore = func(ms *mockStore) {
		failed := false
		ms.onWaitApply = func(r *metapb.Region) error {
			if !failed {
				failed = true
				return errors.New("failed")
			}
			return nil
		}
	}

	ctx := context.Background()
	prep := New(ms)
	prep.RetryBackoff = 0
	req.NoError(prep.DriveLoopAndWaitPrepare(ctx))
	ms.AssertSafeForBackup(t)
	req.NoError(prep.Finalize(ctx))
	ms.AssertIsNormalMode(t)
}

func TestLeaseTimeout(t *testing.T) {
	log.SetLevel(zapcore.DebugLevel)
	req := require.New(t)
	pdc := fakeCluster(t, 3, dummyRegions(100)...)
	ms := newTestEnv(pdc)
	tt := struct {
		now time.Time
		mu  sync.Mutex
	}{now: time.Now()}

	ms.onCreateStore = func(ms *mockStore) {
		ms.now = func() time.Time {
			tt.mu.Lock()
			defer tt.mu.Unlock()
			return tt.now
		}
	}

	ctx := context.Background()
	prep := New(ms)
	req.NoError(prep.DriveLoopAndWaitPrepare(ctx))
	ms.AssertSafeForBackup(t)
	tt.mu.Lock()
	tt.now = tt.now.Add(100 * time.Minute)
	tt.mu.Unlock()
	req.Error(prep.Finalize(ctx))
}

func TestLeaseTimeoutWhileTakingSnapshot(t *testing.T) {
	log.SetLevel(zapcore.DebugLevel)
	req := require.New(t)
	pdc := fakeCluster(t, 3, dummyRegions(100)...)
	ms := newTestEnv(pdc)
	tt := struct {
		now time.Time
		mu  sync.Mutex
	}{now: time.Now()}

	ms.onCreateStore = func(ms *mockStore) {
		ms.now = func() time.Time {
			tt.mu.Lock()
			defer tt.mu.Unlock()
			return tt.now
		}
	}

	ctx := context.Background()
	prep := New(ms)
	prep.LeaseDuration = 4 * time.Second
	req.NoError(prep.AdvanceState(ctx))
	tt.mu.Lock()
	tt.now = tt.now.Add(100 * time.Minute)
	tt.mu.Unlock()
	time.Sleep(2 * time.Second)
	cx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	for {
		err := prep.WaitAndHandleNextEvent(cx)
		if err != nil {
			req.ErrorContains(err, "the lease has expired")
			break
		}
	}
}

func TestRetryEnv(t *testing.T) {
	log.SetLevel(zapcore.DebugLevel)
	req := require.New(t)
	pdc := fakeCluster(t, 3, dummyRegions(100)...)
	tms := newTestEnv(pdc)
	failed := new(sync.Once)
	tms.onConnectToStore = func(u uint64) error {
		shouldFail := false
		failed.Do(func() {
			shouldFail = true
		})
		if shouldFail {
			return errors.New("nya?")
		}
		return nil
	}
	ms := RetryAndSplitRequestEnv{Env: tms}
	ms.GetBackoffer = func() utils.Backoffer {
		o := utils.InitialRetryState(2, 0, 0)
		return &o
	}
	prep := New(ms)
	ctx := context.Background()
	req.NoError(prep.DriveLoopAndWaitPrepare(ctx))
	req.NoError(prep.Finalize(ctx))
}

type counterClient struct {
	send    int
	regions []*metapb.Region
}

func (c *counterClient) Send(req *brpb.PrepareSnapshotBackupRequest) error {
	c.send += 1
	c.regions = append(c.regions, req.Regions...)
	return nil
}

func (c *counterClient) Recv() (*brpb.PrepareSnapshotBackupResponse, error) {
	panic("not implemented")
}

func TestSplitEnv(t *testing.T) {
	log.SetLevel(zapcore.DebugLevel)
	cc := SplitRequestClient{PrepareClient: &counterClient{}, MaxRequestSize: 1024}
	reset := func() {
		cc.PrepareClient.(*counterClient).send = 0
		cc.PrepareClient.(*counterClient).regions = nil
	}
	makeHugeRequestRegions := func(n int, eachSize int) []*metapb.Region {
		regions := []*metapb.Region{}
		for i := 0; i < n; i++ {
			regions = append(regions, &metapb.Region{
				StartKey: append(make([]byte, eachSize-1), byte(i)),
				EndKey:   append(make([]byte, eachSize-1), byte(i+1)),
			})
		}
		return regions
	}

	hugeRequest := brpb.PrepareSnapshotBackupRequest{
		Ty:      brpb.PrepareSnapshotBackupRequestType_WaitApply,
		Regions: makeHugeRequestRegions(100, 128),
	}
	require.NoError(t, cc.Send(&hugeRequest))
	require.GreaterOrEqual(t, cc.PrepareClient.(*counterClient).send, 20)
	require.ElementsMatch(t, cc.PrepareClient.(*counterClient).regions, hugeRequest.Regions)

	reset()
	reallyHugeRequest := brpb.PrepareSnapshotBackupRequest{
		Ty:      brpb.PrepareSnapshotBackupRequestType_WaitApply,
		Regions: makeHugeRequestRegions(10, 2048),
	}
	require.NoError(t, cc.Send(&reallyHugeRequest))
	require.Equal(t, cc.PrepareClient.(*counterClient).send, 10)
	require.ElementsMatch(t, cc.PrepareClient.(*counterClient).regions, reallyHugeRequest.Regions)

	reset()
	tinyRequest := brpb.PrepareSnapshotBackupRequest{
		Ty:      brpb.PrepareSnapshotBackupRequestType_WaitApply,
		Regions: makeHugeRequestRegions(10, 10),
	}
	require.NoError(t, cc.Send(&tinyRequest))
	require.Equal(t, cc.PrepareClient.(*counterClient).send, 1)
	require.ElementsMatch(t, cc.PrepareClient.(*counterClient).regions, tinyRequest.Regions)
}

func TestConnectionDelay(t *testing.T) {
	req := require.New(t)
	pdc := fakeCluster(t, 3, dummyRegions(100)...)
	ms := newTestEnv(pdc)
	called := 0
	delayConn := make(chan struct{})
	blocked := make(chan struct{}, 64)
	ms.connectDelay = func(i uint64) <-chan struct{} {
		called += 1
		if called == 2 {
			blocked <- struct{}{}
			return delayConn
		}
		return nil
	}
	ctx := context.Background()
	prep := New(ms)
	connectionPrepareResult := make(chan error)
	go func() {
		connectionPrepareResult <- prep.PrepareConnections(ctx)
	}()
	<-blocked
	ms.mu.Lock()
	nonNilStore := 0
	for id, store := range ms.stores {
		// We must not create and lease (i.e. reject admin command from any tikv) here.
		if store != nil {
			req.True(store.leaseUntil.Before(time.Now()), "%d->%s", id, store.leaseUntil)
			nonNilStore += 1
		}
	}
	req.GreaterOrEqual(nonNilStore, 2)
	ms.mu.Unlock()
	delayConn <- struct{}{}
	req.NoError(<-connectionPrepareResult)
}

func TestHooks(t *testing.T) {
	req := require.New(t)
	pdc := fakeCluster(t, 3, dummyRegions(100)...)
	pauseWaitApply := make(chan struct{})
	ms := newTestEnv(pdc)
	ms.onCreateStore = func(ms *mockStore) {
		ms.onWaitApply = func(r *metapb.Region) error {
			<-pauseWaitApply
			return nil
		}
	}
	adv := New(ms)
	connectionsEstablished := new(atomic.Bool)
	adv.AfterConnectionsEstablished = func() {
		connectionsEstablished.Store(true)
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- adv.DriveLoopAndWaitPrepare(context.Background())
	}()
	req.Eventually(connectionsEstablished.Load, 1*time.Second, 100*time.Millisecond)
	close(pauseWaitApply)
	req.NoError(<-errCh)
	ms.AssertSafeForBackup(t)
	req.NoError(adv.Finalize(context.Background()))
	ms.AssertIsNormalMode(t)
}

func TestManyMessagesWhenFinalizing(t *testing.T) {
	req := require.New(t)
	pdc := fakeCluster(t, 3, dummyRegions(10240)...)
	ms := newTestEnv(pdc)
	blockCh := make(chan struct{})
	injectErr := make(chan error)
	ms.onCreateStore = func(ms *mockStore) {
		ms.waitApplyDelay = func() {
			<-blockCh
		}
		ms.injectConnErr = injectErr
	}
	prep := New(ms)
	ctx := context.Background()
	req.NoError(prep.PrepareConnections(ctx))
	errC := async(func() error { return prep.DriveLoopAndWaitPrepare(ctx) })
	injectErr <- errors.NewNoStackError("whoa!")
	req.Error(<-errC)
	close(blockCh)
	for _, s := range ms.stores {
		s.delaiedWaitApplies.Wait()
	}
	// Closing the stream should be error.
	req.Error(prep.Finalize(ctx))
}

func async[T any](f func() T) <-chan T {
	ch := make(chan T)
	go func() {
		ch <- f()
	}()
	return ch
}
