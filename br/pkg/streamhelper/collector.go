// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	logbackup "github.com/pingcap/kvproto/pkg/logbackuppb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	defaultBatchSize = 1024
)

type onSuccessHook = func(uint64, kv.KeyRange)

// storeCollector collects the region checkpoints from some store.
// it receives requests from the input channel, batching the requests, and send them to the store.
// because the server supports batching, the range of request regions can be discrete.
// note this is a temporary struct, its lifetime is shorter that the tick of advancer.
type storeCollector struct {
	storeID   uint64
	batchSize int

	service LogBackupService

	input chan RegionWithLeader
	// the oneshot error reporter.
	err *atomic.Error
	// whether the recv and send loop has exited.
	doneMessenger chan struct{}
	onSuccess     onSuccessHook

	// concurrency safety:
	// those fields should only be write on the goroutine running `recvLoop`.
	// Once it exits, we can read those fields.
	currentRequest logbackup.GetLastFlushTSOfRegionRequest
	checkpoint     uint64
	inconsistent   []kv.KeyRange
	regionMap      map[uint64]kv.KeyRange
}

func newStoreCollector(storeID uint64, srv LogBackupService) *storeCollector {
	return &storeCollector{
		storeID:       storeID,
		batchSize:     defaultBatchSize,
		service:       srv,
		input:         make(chan RegionWithLeader, defaultBatchSize),
		err:           new(atomic.Error),
		doneMessenger: make(chan struct{}),
		regionMap:     make(map[uint64]kv.KeyRange),
	}
}

func (c *storeCollector) reportErr(err error) {
	if oldErr := c.err.Load(); oldErr != nil {
		log.Warn("reporting error twice, ignoring", logutil.AShortError("old", err), logutil.AShortError("new", oldErr))
		return
	}
	c.err.Store(err)
}

func (c *storeCollector) setOnSuccessHook(hook onSuccessHook) {
	c.onSuccess = hook
}

func (c *storeCollector) begin(ctx context.Context) {
	err := c.recvLoop(ctx)
	if err != nil {
		log.Warn("collector loop meet error", logutil.ShortError(err))
		c.reportErr(err)
	}
	close(c.doneMessenger)
}

func (c *storeCollector) recvLoop(ctx context.Context) (err error) {
	defer utils.PanicToErr(&err)
	log.Debug("Begin recv loop", zap.Uint64("store", c.storeID))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r, ok := <-c.input:
			if !ok {
				return c.sendPendingRequests(ctx)
			}

			if r.Leader.StoreId != c.storeID {
				log.Warn("trying to request to store which isn't the leader of region.",
					zap.Uint64("region", r.Region.Id),
					zap.Uint64("target-store", c.storeID),
					zap.Uint64("leader", r.Leader.StoreId),
				)
			}
			c.appendRegionMap(r)
			c.currentRequest.Regions = append(c.currentRequest.Regions, &logbackup.RegionIdentity{
				Id:           r.Region.GetId(),
				EpochVersion: r.Region.GetRegionEpoch().GetVersion(),
			})
			if len(c.currentRequest.Regions) >= c.batchSize {
				err := c.sendPendingRequests(ctx)
				if err != nil {
					return err
				}
			}
		}
	}
}

func (c *storeCollector) appendRegionMap(r RegionWithLeader) {
	c.regionMap[r.Region.GetId()] = kv.KeyRange{StartKey: r.Region.StartKey, EndKey: r.Region.EndKey}
}

type StoreCheckpoints struct {
	HasCheckpoint    bool
	Checkpoint       uint64
	FailureSubRanges []kv.KeyRange
}

func (s *StoreCheckpoints) merge(other StoreCheckpoints) {
	if other.HasCheckpoint && (other.Checkpoint < s.Checkpoint || !s.HasCheckpoint) {
		s.Checkpoint = other.Checkpoint
		s.HasCheckpoint = true
	}
	s.FailureSubRanges = append(s.FailureSubRanges, other.FailureSubRanges...)
}

func (s *StoreCheckpoints) String() string {
	sb := new(strings.Builder)
	sb.WriteString("StoreCheckpoints:")
	if s.HasCheckpoint {
		sb.WriteString(strconv.Itoa(int(s.Checkpoint)))
	} else {
		sb.WriteString("none")
	}
	fmt.Fprintf(sb, ":(remaining %d ranges)", len(s.FailureSubRanges))
	return sb.String()
}

func (c *storeCollector) spawn(ctx context.Context) func(context.Context) (StoreCheckpoints, error) {
	go c.begin(ctx)
	return func(cx context.Context) (StoreCheckpoints, error) {
		close(c.input)
		select {
		case <-cx.Done():
			return StoreCheckpoints{}, cx.Err()
		case <-c.doneMessenger:
		}
		if err := c.err.Load(); err != nil {
			return StoreCheckpoints{}, err
		}
		sc := StoreCheckpoints{
			HasCheckpoint:    c.checkpoint != 0,
			Checkpoint:       c.checkpoint,
			FailureSubRanges: c.inconsistent,
		}
		return sc, nil
	}
}

func (c *storeCollector) sendPendingRequests(ctx context.Context) error {
	log.Debug("sending batch", zap.Int("size", len(c.currentRequest.Regions)), zap.Uint64("store", c.storeID))
	defer log.Debug("sending batch done", zap.Uint64("store", c.storeID))
	cli, err := c.service.GetLogBackupClient(ctx, c.storeID)
	if err != nil {
		return err
	}
	cps, err := cli.GetLastFlushTSOfRegion(ctx, &c.currentRequest)
	if err != nil {
		// try disable connection cache if met error
		_ = c.service.ClearCache(ctx, c.storeID)
		return err
	}
	metrics.GetCheckpointBatchSize.WithLabelValues("checkpoint").Observe(float64(len(c.currentRequest.GetRegions())))
	c.currentRequest = logbackup.GetLastFlushTSOfRegionRequest{}
	for _, checkpoint := range cps.Checkpoints {
		if checkpoint.Err != nil {
			log.Debug("failed to get region checkpoint", zap.Stringer("err", checkpoint.Err))
			if checkpoint.Err.EpochNotMatch != nil {
				metrics.RegionCheckpointFailure.WithLabelValues("epoch-not-match").Inc()
			}
			if checkpoint.Err.NotLeader != nil {
				metrics.RegionCheckpointFailure.WithLabelValues("not-leader").Inc()
			}
			metrics.RegionCheckpointRequest.WithLabelValues("fail").Inc()
			c.inconsistent = append(c.inconsistent, c.regionMap[checkpoint.Region.Id])
		} else {
			metrics.RegionCheckpointRequest.WithLabelValues("success").Inc()
			if c.onSuccess != nil {
				c.onSuccess(checkpoint.Checkpoint, c.regionMap[checkpoint.Region.Id])
			}
			// assuming the checkpoint would never be zero, use it as the placeholder. (1970 is so far away...)
			if checkpoint.Checkpoint < c.checkpoint || c.checkpoint == 0 {
				c.checkpoint = checkpoint.Checkpoint
			}
		}
	}
	return nil
}

type runningStoreCollector struct {
	collector *storeCollector
	wait      func(context.Context) (StoreCheckpoints, error)
}

// clusterCollector is the controller for collecting region checkpoints for the cluster.
// It creates multi store collectors.
/*
                             ┌──────────────────────┐ Requesting   ┌────────────┐
                          ┌─►│ StoreCollector[id=1] ├─────────────►│ TiKV[id=1] │
                          │  └──────────────────────┘              └────────────┘
                          │
                          │Owns
 ┌──────────────────┐     │  ┌──────────────────────┐ Requesting   ┌────────────┐
 │ ClusterCollector ├─────┼─►│ StoreCollector[id=4] ├─────────────►│ TiKV[id=4] │
 └──────────────────┘     │  └──────────────────────┘              └────────────┘
                          │
                          │
                          │  ┌──────────────────────┐ Requesting   ┌────────────┐
                          └─►│ StoreCollector[id=5] ├─────────────►│ TiKV[id=5] │
                             └──────────────────────┘              └────────────┘
*/
type clusterCollector struct {
	mu         sync.Mutex
	collectors map[uint64]runningStoreCollector
	noLeaders  []kv.KeyRange
	onSuccess  onSuccessHook

	// The context for spawning sub collectors.
	// Because the collectors are running lazily,
	// keep the initial context for all subsequent goroutines,
	// so we can make sure we can cancel all subtasks.
	masterCtx context.Context
	cancel    context.CancelFunc
	srv       LogBackupService
}

// NewClusterCollector creates a new cluster collector.
// collectors are the structure transform region information to checkpoint information,
// by requesting the checkpoint of regions in the store.
func NewClusterCollector(ctx context.Context, srv LogBackupService) *clusterCollector {
	cx, cancel := context.WithCancel(ctx)
	return &clusterCollector{
		collectors: map[uint64]runningStoreCollector{},
		masterCtx:  cx,
		cancel:     cancel,
		srv:        srv,
	}
}

// SetOnSuccessHook sets the hook when getting checkpoint of some region.
func (c *clusterCollector) SetOnSuccessHook(hook onSuccessHook) {
	c.onSuccess = hook
}

// CollectRegion adds a region to the collector.
func (c *clusterCollector) CollectRegion(r RegionWithLeader) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.masterCtx.Err() != nil {
		return nil
	}

	if r.Leader.GetStoreId() == 0 {
		log.Warn("there are regions without leader", zap.Uint64("region", r.Region.GetId()))
		c.noLeaders = append(c.noLeaders, kv.KeyRange{StartKey: r.Region.StartKey, EndKey: r.Region.EndKey})
		return nil
	}
	leader := r.Leader.StoreId
	_, ok := c.collectors[leader]
	if !ok {
		coll := newStoreCollector(leader, c.srv)
		if c.onSuccess != nil {
			coll.setOnSuccessHook(c.onSuccess)
		}
		c.collectors[leader] = runningStoreCollector{
			collector: coll,
			wait:      coll.spawn(c.masterCtx),
		}
	}

	sc := c.collectors[leader].collector
	select {
	case sc.input <- r:
		return nil
	case <-sc.doneMessenger:
		err := sc.err.Load()
		if err != nil {
			c.cancel()
		}
		return err
	}
}

// Finish finishes collecting the region checkpoints, wait and returning the final result.
// Note this takes the ownership of this collector, you may create a new collector for next use.
func (c *clusterCollector) Finish(ctx context.Context) (StoreCheckpoints, error) {
	defer c.cancel()
	result := StoreCheckpoints{FailureSubRanges: c.noLeaders}
	for id, coll := range c.collectors {
		r, err := coll.wait(ctx)
		if err != nil {
			return StoreCheckpoints{}, errors.Annotatef(err, "store %d", id)
		}
		result.merge(r)
		log.Debug("get checkpoint", zap.Stringer("checkpoint", &r), zap.Stringer("merged", &result))
	}
	return result, nil
}
