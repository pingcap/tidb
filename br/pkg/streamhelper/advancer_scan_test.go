// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	logbackup "github.com/pingcap/kvproto/pkg/logbackuppb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/streamhelper/config"
	"github.com/pingcap/tidb/br/pkg/streamhelper/spans"
	"github.com/pingcap/tidb/br/pkg/utils"
	tidbconfig "github.com/pingcap/tidb/pkg/config"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

type subscriptionConnectionPD struct {
	pd.Client
	store *metapb.Store
}

func (p subscriptionConnectionPD) GetStore(context.Context, uint64) (*metapb.Store, error) {
	return p.store, nil
}

func (p subscriptionConnectionPD) GetAllStores(context.Context, ...pd.GetStoreOption) ([]*metapb.Store, error) {
	if p.store.GetState() == metapb.StoreState_Tombstone {
		return nil, nil
	}
	return []*metapb.Store{p.store}, nil
}

type subscriptionConnectionServer struct {
	logbackup.UnimplementedLogBackupServer
	events            chan uint64
	initialCheckpoint uint64
	poll              func(context.Context) (*logbackup.GetLastFlushTSOfRegionResponse, error)
}

func (s *subscriptionConnectionServer) GetLastFlushTSOfRegion(ctx context.Context, _ *logbackup.GetLastFlushTSOfRegionRequest) (*logbackup.GetLastFlushTSOfRegionResponse, error) {
	if s.poll != nil {
		return s.poll(ctx)
	}
	return nil, status.Error(codes.DeadlineExceeded, "polling request timed out")
}

func (s *subscriptionConnectionServer) SubscribeFlushEvent(_ *logbackup.SubscribeFlushEventRequest, stream logbackup.LogBackup_SubscribeFlushEventServer) error {
	checkpoint := s.initialCheckpoint
	if checkpoint == 0 {
		checkpoint = 1
	}
	for {
		if err := stream.Send(&logbackup.SubscribeFlushEventResponse{Events: []*logbackup.FlushEvent{{Checkpoint: checkpoint}}}); err != nil {
			return err
		}
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case checkpoint = <-s.events:
		}
	}
}

func startConnectionTestServer(t *testing.T, service *subscriptionConnectionServer) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	t.Cleanup(server.Stop)
	logbackup.RegisterLogBackupServer(server, service)
	go func() { _ = server.Serve(listener) }()
	return listener.Addr().String()
}

func newConnectionTestEnv(t *testing.T, service *subscriptionConnectionServer) (context.Context, clusterEnv, *metapb.Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	store := &metapb.Store{Id: 1, Address: startConnectionTestServer(t, service), StartTimestamp: 1}
	manager := utils.NewStoreManager(subscriptionConnectionPD{store: store}, keepalive.ClientParameters{}, nil)
	t.Cleanup(manager.Close)
	return ctx, CliEnv(manager, nil, nil).(clusterEnv), store
}

func receiveConnectionTestCheckpoint(t *testing.T, ctx context.Context, subscriber *FlushSubscriber, checkpoint uint64) {
	t.Helper()
	select {
	case batch := <-subscriber.Events():
		require.Len(t, batch, 1)
		require.Equal(t, checkpoint, batch[0].Value)
	case <-ctx.Done():
		t.Fatal("subscription did not deliver checkpoint:", ctx.Err())
	}
}

func TestSubscriptionConnectionLifecycle(t *testing.T) {
	for _, operation := range []string{"clear", "remove", "restart"} {
		t.Run(operation, func(t *testing.T) {
			ctx, env, store := newConnectionTestEnv(t, &subscriptionConnectionServer{})
			var pollingConn *grpc.ClientConn
			require.NoError(t, env.clis.WithConn(ctx, 1, func(conn *grpc.ClientConn) { pollingConn = conn }))
			subscriber := NewSubscriber(env, env, WithMasterContext(ctx))
			t.Cleanup(subscriber.Drop)
			require.NoError(t, subscriber.UpdateStoreTopology(ctx))
			receiveConnectionTestCheckpoint(t, ctx, subscriber, 1)
			var oldConn *grpc.ClientConn
			require.NoError(t, subscriber.dialer.(clusterEnv).clis.WithConn(ctx, 1, func(conn *grpc.ClientConn) { oldConn = conn }))
			checkpoint := uint64(1)
			switch operation {
			case "clear":
				subscriber.Clear()
				require.Empty(t, subscriber.subscriptions)
			case "remove":
				store.State = metapb.StoreState_Tombstone
				require.NoError(t, subscriber.UpdateStoreTopology(ctx))
				require.Empty(t, subscriber.subscriptions)
				store.State = metapb.StoreState_Up
			case "restart":
				// Keep the old endpoint alive to detect reuse of its cached connection.
				store.Address = startConnectionTestServer(t, &subscriptionConnectionServer{initialCheckpoint: 2})
				store.StartTimestamp++
				require.NoError(t, subscriber.UpdateStoreTopology(ctx))
				checkpoint = 2
			}
			require.Equal(t, connectivity.Shutdown, oldConn.GetState())
			require.NotEqual(t, connectivity.Shutdown, pollingConn.GetState())
			require.NoError(t, subscriber.UpdateStoreTopology(ctx))
			receiveConnectionTestCheckpoint(t, ctx, subscriber, checkpoint)
			require.NoError(t, subscriber.PendingErrors())
		})
	}
}

func TestPollingDeadlineRefreshesStoreAddress(t *testing.T) {
	ctx, env, store := newConnectionTestEnv(t, &subscriptionConnectionServer{
		poll: func(ctx context.Context) (*logbackup.GetLastFlushTSOfRegionResponse, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	})
	var oldConn *grpc.ClientConn
	require.NoError(t, env.clis.WithConn(ctx, 1, func(conn *grpc.ClientConn) { oldConn = conn }))
	poll := func(pollCtx context.Context) error {
		collector := NewClusterCollector(pollCtx, env)
		if err := collector.CollectRegion(RegionWithLeader{Region: &metapb.Region{Id: 1}, Leader: &metapb.Peer{StoreId: 1}}); err != nil {
			return err
		}
		_, err := collector.Finish(ctx)
		return err
	}
	pollCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	require.Error(t, poll(pollCtx))
	require.ErrorIs(t, pollCtx.Err(), context.DeadlineExceeded)
	require.Equal(t, connectivity.Shutdown, oldConn.GetState())
	store.Address = startConnectionTestServer(t, &subscriptionConnectionServer{
		poll: func(context.Context) (*logbackup.GetLastFlushTSOfRegionResponse, error) {
			return &logbackup.GetLastFlushTSOfRegionResponse{}, nil
		},
	})
	require.NoError(t, poll(ctx), "polling must reread the address without a subscriber resetting its cache")
}

type delayedPollingService struct {
	LogBackupService
	entered chan struct{}
	resume  chan struct{}
}

func (s *delayedPollingService) GetLogBackupClient(ctx context.Context, storeID uint64) (logbackup.LogBackupClient, error) {
	client, err := s.LogBackupService.GetLogBackupClient(ctx, storeID)
	if err != nil {
		return nil, err
	}
	bound := *client.(*logBackupClient)
	bound.LogBackupClient = &delayedPollingClient{LogBackupClient: bound.LogBackupClient, service: s}
	return &bound, nil
}

type delayedPollingClient struct {
	logbackup.LogBackupClient
	service *delayedPollingService
}

func (c *delayedPollingClient) GetLastFlushTSOfRegion(ctx context.Context, req *logbackup.GetLastFlushTSOfRegionRequest, opts ...grpc.CallOption) (*logbackup.GetLastFlushTSOfRegionResponse, error) {
	resp, err := c.LogBackupClient.GetLastFlushTSOfRegion(ctx, req, opts...)
	close(c.service.entered)
	<-c.service.resume
	return resp, err
}

func TestPollingLateCleanupKeepsReplacement(t *testing.T) {
	ctx, env, store := newConnectionTestEnv(t, &subscriptionConnectionServer{
		poll: func(ctx context.Context) (*logbackup.GetLastFlushTSOfRegionResponse, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	})
	service := &delayedPollingService{LogBackupService: env, entered: make(chan struct{}), resume: make(chan struct{})}
	defer func() {
		select {
		case <-service.resume:
		default:
			close(service.resume)
		}
	}()
	// The tick returns at its deadline, before its old collector has finished.
	pollCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	collector := NewClusterCollector(pollCtx, service)
	require.NoError(t, collector.CollectRegion(RegionWithLeader{Region: &metapb.Region{Id: 1}, Leader: &metapb.Peer{StoreId: 1}}))
	_, err := collector.Finish(pollCtx)
	require.Error(t, err)
	select {
	case <-service.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Another reset replaces the failed connection while cleanup is delayed.
	require.NoError(t, env.ClearCache(ctx, 1))
	store.Address = startConnectionTestServer(t, &subscriptionConnectionServer{
		poll: func(context.Context) (*logbackup.GetLastFlushTSOfRegionResponse, error) {
			return &logbackup.GetLastFlushTSOfRegionResponse{}, nil
		},
	})
	var replacement *grpc.ClientConn
	require.NoError(t, env.clis.WithConn(ctx, 1, func(conn *grpc.ClientConn) { replacement = conn }))
	close(service.resume)
	select {
	case <-collector.collectors[1].collector.doneMessenger:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.NotEqual(t, connectivity.Shutdown, replacement.GetState())
	client, err := env.GetLogBackupClient(ctx, 1)
	require.NoError(t, err)
	_, err = client.GetLastFlushTSOfRegion(ctx, &logbackup.GetLastFlushTSOfRegionRequest{})
	require.NoError(t, err, "late cleanup must leave the replacement usable")
}

func TestPollingCancellationKeepsConnection(t *testing.T) {
	entered := make(chan struct{})
	ctx, env, _ := newConnectionTestEnv(t, &subscriptionConnectionServer{
		poll: func(ctx context.Context) (*logbackup.GetLastFlushTSOfRegionResponse, error) {
			close(entered)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	})
	var conn *grpc.ClientConn
	require.NoError(t, env.clis.WithConn(ctx, 1, func(c *grpc.ClientConn) { conn = c }))
	pollCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-entered:
			cancel()
		case <-ctx.Done():
		}
	}()
	collector := NewClusterCollector(pollCtx, env)
	require.NoError(t, collector.CollectRegion(RegionWithLeader{Region: &metapb.Region{Id: 1}, Leader: &metapb.Peer{StoreId: 1}}))
	_, err := collector.Finish(ctx)
	require.Error(t, err)
	require.Equal(t, context.Canceled, pollCtx.Err())
	require.NotEqual(t, connectivity.Shutdown, conn.GetState())
}

func TestPollingConnectionDoesNotInterruptSubscription(t *testing.T) {
	for _, mode := range []string{"cli", "tidb"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			server := grpc.NewServer()
			t.Cleanup(server.Stop)
			service := &subscriptionConnectionServer{events: make(chan uint64, 1)}
			logbackup.RegisterLogBackupServer(server, service)
			go func() { _ = server.Serve(listener) }()
			pdClient := subscriptionConnectionPD{store: &metapb.Store{Id: 1, Address: listener.Addr().String(), StartTimestamp: 1}}
			manager := utils.NewStoreManager(pdClient, keepalive.ClientParameters{}, nil)
			t.Cleanup(manager.Close)
			env := CliEnv(manager, nil, nil)
			if mode == "tidb" {
				env, err = TiDBEnv(nil, pdClient, nil, tidbconfig.NewConfig())
				require.NoError(t, err)
				t.Cleanup(env.(clusterEnv).clis.Close)
			}
			var pollingConn *grpc.ClientConn
			require.NoError(t, env.(clusterEnv).clis.WithConn(ctx, 1, func(conn *grpc.ClientConn) { pollingConn = conn }))
			subscriber := NewSubscriber(env, env, WithMasterContext(ctx))
			t.Cleanup(func() {
				if subscriber != nil {
					subscriber.Drop()
				}
			})
			require.NoError(t, subscriber.UpdateStoreTopology(ctx))
			var subscriptionConn *grpc.ClientConn
			require.NoError(t, subscriber.dialer.(clusterEnv).clis.WithConn(ctx, 1, func(conn *grpc.ClientConn) { subscriptionConn = conn }))
			receive := func(checkpoint uint64) {
				t.Helper()
				select {
				case batch := <-subscriber.Events():
					require.Len(t, batch, 1)
					require.Equal(t, checkpoint, batch[0].Value)
				case <-ctx.Done():
					t.Fatal("subscription did not deliver checkpoint:", ctx.Err())
				}
			}
			receive(1)

			collector := NewClusterCollector(ctx, env)
			require.NoError(t, collector.CollectRegion(RegionWithLeader{
				Region: &metapb.Region{Id: 1}, Leader: &metapb.Peer{StoreId: 1},
			}))
			_, err = collector.Finish(ctx)
			require.ErrorContains(t, err, "polling request timed out")
			require.Equal(t, connectivity.Shutdown, pollingConn.GetState())
			require.NotEqual(t, connectivity.Shutdown, subscriptionConn.GetState(), "polling must not close the subscription connection")
			service.events <- 2
			receive(2)
			require.NoError(t, subscriber.PendingErrors())

			// Clearing the subscriber's cache must also leave polling independent.
			require.NoError(t, env.(clusterEnv).clis.WithConn(ctx, 1, func(conn *grpc.ClientConn) { pollingConn = conn }))
			require.NoError(t, subscriber.dialer.ClearCache(ctx, 1))
			require.Equal(t, connectivity.Shutdown, subscriptionConn.GetState())
			require.NotEqual(t, connectivity.Shutdown, pollingConn.GetState())
			require.Eventually(t, func() bool { return subscriber.PendingErrors() != nil }, time.Second, time.Millisecond)
			subscriber.HandleErrors(ctx)
			receive(1)
			require.NoError(t, subscriber.PendingErrors())
			require.NoError(t, subscriber.dialer.(clusterEnv).clis.WithConn(ctx, 1, func(conn *grpc.ClientConn) { subscriptionConn = conn }))
			subscriber.Drop()
			subscriber = nil
			require.Equal(t, connectivity.Shutdown, subscriptionConn.GetState(), "dropping the subscriber must close its connections")
			require.NotEqual(t, connectivity.Shutdown, pollingConn.GetState())
		})
	}
}

type checkpointScanEnv struct {
	Env
	now  uint64
	scan func(context.Context) ([]RegionWithLeader, error)
}

func (e checkpointScanEnv) FetchCurrentTS(context.Context) (uint64, error) {
	return e.now, nil
}

func (e checkpointScanEnv) RegionScan(ctx context.Context, _, _ []byte, _ int) ([]RegionWithLeader, error) {
	if e.scan != nil {
		return e.scan(ctx)
	}
	return []RegionWithLeader{{Region: &metapb.Region{Id: 1}, Leader: &metapb.Peer{StoreId: 1}}}, nil
}

func TestCheckpointSelectionSnapshotsAndThresholds(t *testing.T) {
	tree := spans.NewFullWith(spans.Full(), 0)
	tree.Merge(spans.Valued{Key: spans.Span{EndKey: []byte("b")}, Value: 9})
	tree.Merge(spans.Valued{Key: spans.Span{StartKey: []byte("b"), EndKey: []byte("c")}, Value: 1})
	tree.Merge(spans.Valued{Key: spans.Span{StartKey: []byte("c"), EndKey: []byte("d")}, Value: 2})
	tree.Merge(spans.Valued{Key: spans.Span{StartKey: []byte("d")}, Value: 3})
	selection := checkpointSelection{threshold: 2}
	resolveTargets := selection.scan(tree, 3)
	require.Equal(t, []spans.Valued{
		{Key: spans.Span{StartKey: []byte("b"), EndKey: []byte("c")}, Value: 1},
	}, selection.targets)
	require.Equal(t, []spans.Valued{
		{Key: spans.Span{StartKey: []byte("b"), EndKey: []byte("c")}, Value: 1},
		{Key: spans.Span{StartKey: []byte("c"), EndKey: []byte("d")}, Value: 2},
	}, resolveTargets)
	require.Equal(t, selection.targets[0], selection.minimum)

	// An in-place subscriber update after unlocking must not change the saved
	// minimum or either target list, all of which must be value snapshots.
	tree.Merge(spans.Valued{Key: spans.Span{StartKey: []byte("b"), EndKey: []byte("c")}, Value: 20})
	require.EqualValues(t, 1, selection.minimum.Value)
	require.EqualValues(t, 1, selection.targets[0].Value)
	require.EqualValues(t, 1, resolveTargets[0].Value)
	resolveTargets = selection.scan(tree, 3)
	require.EqualValues(t, 2, selection.minimum.Value)
	require.Empty(t, selection.targets, "threshold equality must not select a range")
	require.Len(t, resolveTargets, 1)
	require.Equal(t, "c", string(resolveTargets[0].Key.StartKey))

	selection.scan(spans.NewFullWith(nil, 0), 3)
	require.False(t, selection.valid)
	require.Equal(t, spans.Valued{}, selection.minimum)
	require.Empty(t, selection.targets)
	// Zero and MaxUint64 are both valid minima; an unselected scan allocates nothing.
	for _, value := range []uint64{0, ^uint64(0)} {
		selection = checkpointSelection{}
		single := spans.NewFullWith(spans.Full(), value)
		require.Zero(t, testing.AllocsPerRun(100, func() { selection.scan(single, 0) }))
		require.True(t, selection.valid)
		require.Equal(t, value, selection.minimum.Value)
		require.Empty(t, selection.targets)
	}
}

func TestResolveTraversalReusedForPolling(t *testing.T) {
	now := time.Now()
	c := NewCheckpointAdvancer(checkpointScanEnv{now: oracle.GoTimeToTS(now)})
	c.lastCheckpoint = newCheckpointWithTS(oracle.GoTimeToTS(now.Add(-time.Minute)))
	c.lastCheckpoint.resolveLockTime = now.Add(-time.Hour)
	minimum := oracle.GoTimeToTS(now.Add(-time.Second))
	c.checkpoints = spans.NewFullWith(spans.Full(), minimum)
	selection := checkpointSelection{}
	c.tryResolveLocksForCheckpoint(context.Background(), &selection)
	require.True(t, selection.valid)
	require.Equal(t, minimum, selection.minimum.Value)
	require.False(t, c.inResolvingLock.Load(), "the minimum has advanced, so no locks are resolved")

	// A second index traversal would panic. Polling must consume the selection
	// made by the earlier resolve-lock traversal in this tick.
	c.checkpoints = nil
	actual, err := c.calculateGlobalCheckpointLight(context.Background(), &selection)
	require.NoError(t, err)
	require.Equal(t, minimum, actual.Value)
}

func TestResolveSelectionRequiresCurrentMinimum(t *testing.T) {
	c := NewCheckpointAdvancer(nil)
	c.checkpoints = spans.NewFullWith(spans.Full(), 10)
	cp := newCheckpointWithTS(10)
	selection := checkpointSelection{threshold: 12}
	require.Len(t, c.resolveLockTargetsForCheckpoint(cp, 11, &selection), 1)
	require.Len(t, selection.targets, 1)
	c.checkpoints.Merge(spans.Valued{Value: 11})
	require.Empty(t, c.resolveLockTargetsForCheckpoint(cp, 12, nil))
}

func TestPollingStopsAfterCancellation(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	restore := log.ReplaceGlobals(zap.New(core), nil)
	defer restore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var scans atomic.Int64
	env := checkpointScanEnv{scan: func(context.Context) ([]RegionWithLeader, error) {
		scans.Add(1)
		cancel()
		return nil, context.Canceled
	}}
	c := NewCheckpointAdvancer(env)
	c.taskRange = spans.Full()
	err := c.tryAdvance(ctx, 10000, func(i int) spans.Span {
		return spans.Span{StartKey: []byte(fmt.Sprintf("%08d", i*2)), EndKey: []byte(fmt.Sprintf("%08d", i*2+1))}
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Positive(t, scans.Load())
	require.LessOrEqual(t, scans.Load(), int64(config.DefaultMaxConcurrencyAdvance*4))

	// Canceled ticks neither build polling ranges nor call PD again.
	err = c.tryAdvance(ctx, 10000, func(int) spans.Span {
		t.Fatal("canceled polling must not enumerate ranges")
		return spans.Span{}
	})
	require.ErrorIs(t, err, context.Canceled)
	before := scans.Load()
	_, err = IterateRegion(env, nil, nil).Next(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, before, scans.Load())
	require.Zero(t, logs.Len(), "canceled range scans must not emit per-range warnings")
}

func TestSubscriptionBatchStopsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := NewCheckpointAdvancer(nil)
	c.checkpoints = spans.NewFullWith(spans.Full(), 0)
	c.SpawnSubscriptionHandler(ctx)
	defer c.subscriber.Drop()

	batch := make([]spans.Valued, 1000)
	for i := range batch {
		batch[i].Value = uint64(i + 1)
	}
	func() {
		// Let the consumer dequeue the batch, but prevent its first merge from
		// completing until cancellation. At most that in-flight merge may finish.
		c.checkpointsMu.Lock()
		defer c.checkpointsMu.Unlock()
		c.subscriber.eventsTunnel <- batch
		require.Eventually(t, func() bool {
			return len(c.subscriber.eventsTunnel) == 0
		}, time.Second, time.Millisecond)
		cancel()
	}()
	require.Never(t, func() bool {
		var advanced bool
		c.WithCheckpoints(func(tree *spans.ValuedFull) {
			tree.TraverseRef(func(v *spans.Valued) bool {
				advanced = v.Value > 1
				return false
			})
		})
		return advanced
	}, 100*time.Millisecond, time.Millisecond, "canceled batches must not continue merging")
}

type importantTickScanEnv struct {
	Env
	uploaded uint64
}

func (e *importantTickScanEnv) UploadV3GlobalCheckpointForTask(_ context.Context, _ string, ts uint64) error {
	e.uploaded = ts
	return fmt.Errorf("stop after checkpoint upload")
}

func TestImportantTickReadsFreshMinimum(t *testing.T) {
	env := &importantTickScanEnv{}
	c := NewCheckpointAdvancer(env)
	c.task = &backuppb.StreamBackupTaskInfo{Name: "test"}
	c.lastCheckpoint = newCheckpointWithTS(0)
	c.checkpoints = spans.NewFullWith(spans.Full(), 10)
	c.checkpoints.Merge(spans.Valued{Key: spans.Span{EndKey: []byte("b")}, Value: 20})
	c.checkpoints.Merge(spans.Valued{Key: spans.Span{StartKey: []byte("c")}, Value: 20})
	require.ErrorContains(t, c.importantTick(context.Background()), "stop after checkpoint upload")
	require.EqualValues(t, 10, env.uploaded)
	require.Equal(t, []byte("b"), c.lastCheckpoint.StartKey)
	require.Equal(t, []byte("c"), c.lastCheckpoint.EndKey)
	// Subscriber updates after optionalTick must be reflected by importantTick.
	c.checkpoints.Merge(spans.Valued{Key: spans.Span{StartKey: []byte("b"), EndKey: []byte("c")}, Value: 30})
	require.ErrorContains(t, c.importantTick(context.Background()), "stop after checkpoint upload")
	require.EqualValues(t, 20, env.uploaded)
	require.Empty(t, c.lastCheckpoint.StartKey, "equal minima choose the first range by key")
	require.Equal(t, []byte("b"), c.lastCheckpoint.EndKey)
}
