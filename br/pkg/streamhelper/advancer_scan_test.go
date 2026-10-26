// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/streamhelper/config"
	"github.com/pingcap/tidb/br/pkg/streamhelper/spans"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

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
