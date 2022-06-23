// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper

import (
	"bytes"
	"context"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	logbackup "github.com/pingcap/kvproto/pkg/logbackuppb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/streamhelper/config"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type CheckpointAdvancer struct {
	env Env

	task   *backuppb.StreamBackupTaskInfo
	taskMu sync.Mutex

	cfg config.Config

	cache Checkpoints

	state          advancerState
	fullScanTick   int
	lastCheckpoint uint64
}

type advancerState int

const (
	stateFullScan advancerState = iota
	stateUpdateSmallTree
)

type getCheckpointInRangeFunc func(ctx context.Context, start, end []byte) (uint64, []kv.KeyRange, error)

func NewCheckpointAdvancer(env Env) *CheckpointAdvancer {
	return &CheckpointAdvancer{
		env:   env,
		cfg:   config.Default(),
		cache: NewCheckpoints(),
	}
}

func (c *CheckpointAdvancer) UpdateConfig(newConf config.Config) {
	c.cfg = newConf
}

func (c *CheckpointAdvancer) UpdateConfigWith(f func(*config.Config)) {
	f(&c.cfg)
}

// getCheckpointsOfStore calculates the checkpoint of some store: by requesting the regions provided.
func (c *CheckpointAdvancer) getCheckpointsOfStore(ctx context.Context, store uint64, regions []RegionWithLeader) ([]*logbackup.RegionCheckpoint, error) {
	defer c.recordTimeCost("get checkpoints of store", zap.Int("batch", len(regions)), zap.Uint64("store", store))()
	cli, err := c.env.GetLogBackupClient(ctx, store)
	if err != nil {
		return nil, errors.Annotatef(err, "during getting log backup client for store %d", store)
	}
	req := logbackup.GetLastFlushTSOfRegionRequest{}
	for _, r := range regions {
		req.Regions = append(req.Regions, &logbackup.RegionIdentity{
			Id:           r.Region.GetId(),
			EpochVersion: r.Region.GetRegionEpoch().GetVersion(),
		})
	}
	resp, err := cli.GetLastFlushTSOfRegion(ctx, &req)
	if err != nil {
		return nil, errors.Annotatef(err, "request = %v", req)
	}
	return resp.Checkpoints, nil
}

func (c *CheckpointAdvancer) Config() config.Config {
	return c.cfg
}

func (c *CheckpointAdvancer) GetCheckpointInRange(ctx context.Context, start, end []byte) (uint64, []kv.KeyRange, error) {
	log.Debug("scanning range", logutil.Key("start", start), logutil.Key("end", end))
	cp := uint64(math.MaxUint64)
	iter := IterateRegion(c.env, start, end)
	rm := map[uint64]RegionWithLeader{}
	failed := []*logbackup.RegionCheckpoint{}
	for !iter.Done() {
		rs, err := iter.Next(ctx)
		if err != nil {
			return 0, nil, err
		}
		log.Debug("scan region", zap.Int("len", len(rs)))
		partitioned := map[uint64][]RegionWithLeader{}
		for _, r := range rs {
			partitioned[r.Leader.GetStoreId()] = append(partitioned[r.Leader.GetStoreId()], r)
		}
		appendRegionMap(rs, rm)
		for k, v := range partitioned {
			if k == 0 {
				log.Warn("there is regions without leader", zap.Int("len", len(v)))
				for _, region := range v {
					failed = append(failed, &logbackup.RegionCheckpoint{
						Region: &logbackup.RegionIdentity{
							Id:           region.Region.GetId(),
							EpochVersion: region.Region.GetRegionEpoch().GetVersion(),
						},
						Checkpoint: 0,
					})
				}
				continue
			}

			checkpoints, err := c.getCheckpointsOfStore(ctx, k, v)
			if err != nil {
				return 0, nil, err
			}
			for _, checkpoint := range checkpoints {
				if checkpoint.Err != nil {
					log.Debug("failed to get region checkpoint", zap.Stringer("err", checkpoint.Err))
					failed = append(failed, checkpoint)
				} else {
					log.Debug("get checkpoint of region", zap.Stringer("region", checkpoint.Region), zap.Uint64("checkpoint", checkpoint.Checkpoint))
					c.cache.InsertRegion(checkpoint.Checkpoint, rm[checkpoint.Region.Id])
					if checkpoint.Checkpoint < cp {
						cp = checkpoint.Checkpoint
					}
				}
			}
		}
	}
	return cp, CollectFailureRange(len(failed), func(i int) kv.KeyRange {
		meta := rm[failed[i].Region.Id].Region
		return kv.KeyRange{StartKey: meta.StartKey, EndKey: meta.EndKey}
	}), nil
}

func (c *CheckpointAdvancer) recordTimeCost(message string, fields ...zap.Field) func() {
	now := time.Now()
	return func() {
		fields = append(fields, zap.Stringer("take", time.Since(now)))
		log.Debug(message, fields...)
	}
}

func (c *CheckpointAdvancer) tryAdvance(ctx context.Context, rst *RangesSharesTS) error {
	defer c.recordTimeCost("try advance", zap.Uint64("checkpoint", rst.TS), zap.Int("len", len(rst.Ranges)))()
	ranges := CollectFailureRange(len(rst.Ranges), func(i int) kv.KeyRange { return rst.Ranges[i] })
	failures := make(chan kv.KeyRange, 1024)
	workers := utils.NewWorkerPool(4, "subranges")
	eg, cx := errgroup.WithContext(ctx)
	ts := uint64(math.MaxUint64)
	for _, r := range ranges {
		workers.ApplyOnErrorGroup(eg, func() error {
			defer c.recordTimeCost("get checkpoints in range", zap.Uint64("checkpoint", rst.TS))()
			rts, inconsistent, err := c.GetCheckpointInRange(cx, r.StartKey, r.EndKey)
			if err != nil {
				if err != context.Canceled {
					c.cache.insertDirect(*rst)
				}
				return err
			}
			if rts < ts {
				ts = rts
			}
			for _, i := range inconsistent {
				failures <- i
			}
			return nil
		})
	}
	err := eg.Wait()
	if err != nil {
		return err
	}
	fr := make([]kv.KeyRange, 0, len(failures))
collect:
	for {
		select {
		case f, ok := <-failures:
			if !ok {
				break
			}
			fr = append(fr, f)
		default:
			break collect
		}
	}
	if len(fr) != 0 || ts <= rst.TS {
		log.Info("failure regions collected", zap.Int("size", len(fr)))
		c.cache.insertDirect(RangesSharesTS{
			TS:     rst.TS,
			Ranges: fr,
		})
	}
	return nil
}

func (c *CheckpointAdvancer) UpdateGlobalCheckpointLight(ctx context.Context) (uint64, error) {
	log.Debug("current tree", zap.Stringer("ct", &c.cache))
	rsts := c.cache.PopRangesWithGapGT(3 * time.Minute)
	if len(rsts) == 0 {
		return 0, nil
	}
	workers := utils.NewWorkerPool(4, "regions")
	eg, cx := errgroup.WithContext(ctx)
	for _, rst := range rsts {
		workers.ApplyOnErrorGroup(eg, func() error { return c.tryAdvance(cx, rst) })
	}
	err := eg.Wait()
	if err != nil {
		return 0, err
	}
	ts := c.cache.CheckpointTS()
	return ts, nil
}

func (c *CheckpointAdvancer) CalculateGlobalCheckpoint(ctx context.Context, getCheckpoint getCheckpointInRangeFunc) (uint64, error) {
	var (
		cp = uint64(math.MaxInt64)
		// TODO: Use The task range here.
		thisRun []kv.KeyRange = []kv.KeyRange{{}}
		nextRun []kv.KeyRange
	)
	cx, cancel := context.WithTimeout(ctx, c.cfg.MaxBackoffTime)
	defer cancel()
	for {
		for _, u := range thisRun {
			subCP, subUnformed, err := getCheckpoint(cx, u.StartKey, u.EndKey)
			if err != nil {
				return 0, err
			}
			nextRun = append(nextRun, subUnformed...)
			if cp > subCP {
				cp = subCP
			}
		}
		if len(nextRun) == 0 {
			return cp, nil
		}
		thisRun = nextRun
		nextRun = nil
		log.Debug("backoffing with subranges", zap.Int("subranges", len(thisRun)))
		time.Sleep(c.cfg.BackoffTime)
	}
}

func appendRegionMap(region []RegionWithLeader, rm map[uint64]RegionWithLeader) {
	for _, r := range region {
		rm[r.Region.GetId()] = r
	}
}

func CollectFailureRange(length int, getRange func(int) kv.KeyRange) []kv.KeyRange {
	frs := make([]kv.KeyRange, 0, length)
	for i := 0; i < length; i++ {
		frs = append(frs, getRange(i))
	}

	sort.Slice(frs, func(i, j int) bool {
		return bytes.Compare(frs[i].StartKey, frs[j].StartKey) < 0
	})

	result := make([]kv.KeyRange, 0, len(frs))
	i := 0
	for i < len(frs) {
		item := frs[i]
		for {
			i++
			if i >= len(frs) || bytes.Compare(frs[i].StartKey, item.EndKey) >= 0 {
				break
			}
			if bytes.Compare(item.EndKey, frs[i].EndKey) < 0 || len(frs[i].EndKey) == 0 {
				item.EndKey = frs[i].EndKey
			}
		}
		result = append(result, item)
	}
	return result
}

func (c *CheckpointAdvancer) Loop(ctx context.Context) error {
	timer := time.NewTicker(c.cfg.TickDuration)
	c.StartTaskListener(ctx)
	defer timer.Stop()
	if err := c.OnTick(ctx); err != nil {
		log.Warn("Ticking failed, would try again.", logutil.ShortError(err))
	}
	for {
		select {
		case <-timer.C:
			if err := c.OnTick(ctx); err != nil {
				log.Warn("Ticking failed, would try again.", logutil.ShortError(err))
			}
		case <-ctx.Done():
			log.Info("CheckpointAdvancer loop exited.")
			return nil
		}
	}
}

func (c *CheckpointAdvancer) consumeAllTask(ctx context.Context, ch <-chan TaskEvent) error {
	for {
		select {
		case e, ok := <-ch:
			if !ok {
				return nil
			}
			log.Info("meet task event", zap.Any("event", e))
			if err := c.onTaskEvent(e); err != nil {
				if errors.Cause(e.Err) != context.Canceled {
					log.Error("listen task meet error, would reopen.", logutil.ShortError(err))
					return err
				}
				return nil
			}
		default:
			return nil
		}
	}
}

func (c *CheckpointAdvancer) beginListenTaskChange(ctx context.Context) (<-chan TaskEvent, error) {
	ch := make(chan TaskEvent, 1024)
	if err := c.env.Begin(ctx, ch); err != nil {
		return nil, err
	}
	err := c.consumeAllTask(ctx, ch)
	if err != nil {
		return nil, err
	}
	return ch, nil
}

func (c *CheckpointAdvancer) StartTaskListener(ctx context.Context) {
	cx, cancel := context.WithCancel(ctx)
	var ch <-chan TaskEvent
	for {
		if cx.Err() != nil {
			// make linter happy.
			cancel()
			return
		}
		var err error
		ch, err = c.beginListenTaskChange(cx)
		if err == nil {
			break
		}
		log.Warn("failed to begin listening, retrying...", logutil.ShortError(err))
		time.Sleep(c.cfg.BackoffTime)
	}

	go func() {
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return
			case e, ok := <-ch:
				if !ok {
					return
				}
				log.Info("meet task event", zap.Any("event", e))
				if err := c.onTaskEvent(e); err != nil {
					if errors.Cause(e.Err) != context.Canceled {
						log.Error("listen task meet error, would reopen.", logutil.ShortError(err))
						time.AfterFunc(c.cfg.BackoffTime, func() { c.StartTaskListener(ctx) })
					}
					return
				}
			}
		}
	}()
}

func (c *CheckpointAdvancer) onTaskEvent(e TaskEvent) error {
	c.taskMu.Lock()
	defer c.taskMu.Unlock()
	switch e.Type {
	case EventAdd:
		c.task = e.Info
	case EventDel:
		c.task = nil
	case EventErr:
		return e.Err
	}
	return nil
}

func (c *CheckpointAdvancer) advanceCheckpointBy(ctx context.Context, getCheckpoint func() (uint64, error)) error {
	start := time.Now()
	cp, err := getCheckpoint()
	if err != nil {
		return err
	}
	if cp < c.lastCheckpoint {
		log.Warn("failed to update global checkpoint: stale", zap.Uint64("old", c.lastCheckpoint), zap.Uint64("new", cp))
		return nil
	}
	log.Debug("uploading checkpoint for task",
		zap.Stringer("checkpoint", oracle.GetTimeFromTS(cp)),
		zap.String("task", c.task.Name),
		zap.Stringer("take", time.Since(start)))
	if err := c.env.UploadV3GlobalCheckpointForTask(ctx, c.task.Name, cp); err != nil {
		return errors.Annotate(err, "failed to upload global checkpoint")
	}
	c.lastCheckpoint = cp
	metrics.LastCheckpoint.Set(float64(c.lastCheckpoint))
	return nil
}

func (c *CheckpointAdvancer) OnTick(ctx context.Context) (err error) {
	defer func() {
		e := recover()
		if e != nil {
			err = errors.Annotatef(berrors.ErrUnknown, "panic during handling tick: %s", e)
		}
	}()
	err = c.tick(ctx)
	return
}

func (c *CheckpointAdvancer) tick(ctx context.Context) error {
	c.taskMu.Lock()
	defer c.taskMu.Unlock()

	switch c.state {
	case stateFullScan:
		if c.fullScanTick > 0 {
			c.fullScanTick--
			break
		}
		if c.task == nil {
			log.Debug("No tasks yet, skipping advancing.")
			return nil
		}
		defer func() {
			c.fullScanTick = c.cfg.FullScanTick
		}()
		err := c.advanceCheckpointBy(ctx, func() (uint64, error) { return c.CalculateGlobalCheckpoint(ctx, c.GetCheckpointInRange) })
		if err != nil {
			return err
		}

		c.state = stateUpdateSmallTree
	case stateUpdateSmallTree:
		err := c.advanceCheckpointBy(ctx, func() (uint64, error) { return c.UpdateGlobalCheckpointLight(ctx) })
		if err != nil {
			return err
		}
	}
	return nil
}
