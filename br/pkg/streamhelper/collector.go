// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	logbackup "github.com/pingcap/kvproto/pkg/logbackuppb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"go.uber.org/zap"
)

const (
	defaultBatchSize = 1024
)

type onSuccessHook = func(uint64, kv.KeyRange)

// storeCollector collects the region checkpoints from some store.
// it receives requests from the input channel, batching the requests, and send them to the store.
// because the server supports batching, the range of request regions can be discreted.
type storeCollector struct {
	storeID   uint64
	batchSize int

	service LogBackupService

	input     chan RegionWithLeader
	onSuccess onSuccessHook

	// concurrency safety:
	// those fields should only be write on the goroutine running `blockOnRecvAndSend`.
	// Once it exits, we can read those fields.
	currentRequest logbackup.GetLastFlushTSOfRegionRequest
	checkpoint     uint64
	inconsistent   []kv.KeyRange
	regionMap      map[uint64]kv.KeyRange
}

func newStoreCollector(storeID uint64, srv LogBackupService) *storeCollector {
	return &storeCollector{
		storeID:   storeID,
		batchSize: defaultBatchSize,
		service:   srv,
		input:     make(chan RegionWithLeader, defaultBatchSize),
		regionMap: make(map[uint64]kv.KeyRange),
	}
}

func (c *storeCollector) setOnSuccessHook(hook onSuccessHook) {
	c.onSuccess = hook
}

func (c *storeCollector) blockOnRecvAndSend(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case r, ok := <-c.input:
			if !ok {
				return c.flush(ctx)
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
			if len(c.currentRequest.Regions) > c.batchSize {
				err := c.flush(ctx)
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
	FailureSubranges []kv.KeyRange
}

func (s *StoreCheckpoints) merge(other StoreCheckpoints) {
	if other.HasCheckpoint && (other.Checkpoint < s.Checkpoint || !s.HasCheckpoint) {
		s.Checkpoint = other.Checkpoint
		s.HasCheckpoint = true
	}
	s.FailureSubranges = append(s.FailureSubranges, other.FailureSubranges...)
}

func (c *storeCollector) spawn(ctx context.Context) func() (StoreCheckpoints, error) {
	ch := make(chan error)
	go func() {
		ch <- c.blockOnRecvAndSend(ctx)
	}()
	return func() (StoreCheckpoints, error) {
		close(c.input)
		err := <-ch
		if err != nil {
			return StoreCheckpoints{}, nil
		}
		sc := StoreCheckpoints{
			HasCheckpoint:    c.checkpoint != 0,
			Checkpoint:       c.checkpoint,
			FailureSubranges: c.inconsistent,
		}
		return sc, nil
	}
}

func (c *storeCollector) flush(ctx context.Context) error {
	cli, err := c.service.GetLogBackupClient(ctx, c.storeID)
	if err != nil {
		return err
	}
	cps, err := cli.GetLastFlushTSOfRegion(ctx, &c.currentRequest)
	if err != nil {
		return err
	}
	metrics.GetCheckpointBatchSize.WithLabelValues("checkpoint").Observe(float64(len(c.currentRequest.GetRegions())))
	c.currentRequest = logbackup.GetLastFlushTSOfRegionRequest{}
	for _, checkpoint := range cps.Checkpoints {
		if checkpoint.Err != nil {
			log.Debug("failed to get region checkpoint", zap.Stringer("err", checkpoint.Err))
			c.inconsistent = append(c.inconsistent, c.regionMap[checkpoint.Region.Id])
		} else {
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
	wait      func() (StoreCheckpoints, error)
}

type clusterCollector struct {
	mu         sync.Mutex
	collectors map[uint64]runningStoreCollector
	noLeaders  []kv.KeyRange
	onSuccess  onSuccessHook

	// The context for spawning sub collectors.
	// Because the collectors are running lazily,
	// keep the initial context for all subsquent goroutines,
	// so we can make sure we can cancel all subtasks.
	masterCtx context.Context
	cancel    context.CancelFunc
	srv       LogBackupService
}

func NewClusterCollector(ctx context.Context, srv LogBackupService) *clusterCollector {
	cx, cancel := context.WithCancel(ctx)
	return &clusterCollector{
		collectors: map[uint64]runningStoreCollector{},
		masterCtx:  cx,
		cancel:     cancel,
		srv:        srv,
	}
}

func (c *clusterCollector) setOnSuccessHook(hook onSuccessHook) {
	c.onSuccess = hook
}

func (c *clusterCollector) collectRegion(r RegionWithLeader) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.masterCtx.Err() != nil {
		return
	}

	if r.Leader.GetStoreId() == 0 {
		log.Warn("there is regions without leader", zap.Uint64("region", r.Region.GetId()))
		c.noLeaders = append(c.noLeaders, kv.KeyRange{StartKey: r.Region.StartKey, EndKey: r.Region.EndKey})
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
	sc.input <- r
}

func (c *clusterCollector) Wait(ctx context.Context) (StoreCheckpoints, error) {
	defer c.cancel()
	result := StoreCheckpoints{FailureSubranges: c.noLeaders}
	for id, coll := range c.collectors {
		r, err := coll.wait()
		if err != nil {
			return StoreCheckpoints{}, errors.Annotatef(err, "store %d", id)
		}
		result.merge(r)
		log.Debug("get checkpoint", zap.Any("checkpoint", r), zap.Any("merged", result))
	}
	return result, nil
}
