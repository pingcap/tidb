// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper

import (
	"bytes"
	"context"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
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

// CheckpointAdvancer is the central node for advancing the checkpoint of log backup.
// It's a part of "checkpoint v3".
// Generally, it scan the regions in the task range, collect checkpoints from tikvs.
//                                         ┌──────┐
//                                   ┌────►│ TiKV │
//                                   │     └──────┘
//                                   │
//                                   │
// ┌──────────┐GetLastFlushTSOfRegion│     ┌──────┐
// │ Advancer ├──────────────────────┼────►│ TiKV │
// └────┬─────┘                      │     └──────┘
//      │                            │
//      │                            │
//      │                            │     ┌──────┐
//      │                            └────►│ TiKV │
//      │                                  └──────┘
//      │
//      │ UploadCheckpointV3   ┌──────────────────┐
//      └─────────────────────►│  PD              │
//                             └──────────────────┘
type CheckpointAdvancer struct {
	env Env

	// The concurrency accessed task:
	// both by the task listener and ticking.
	task   *backuppb.StreamBackupTaskInfo
	taskMu sync.Mutex

	// the read-only config.
	// once tick begin, this should not be changed for now.
	cfg config.Config

	// the cache of region checkpoints.
	// so we can advance only ranges with huge gap.
	cache CheckpointsCache

	// the internal state of advancer.
	state advancerState
	// the cached last checkpoint.
	// if no progress, this cache can help us don't to send useless requests.
	lastCheckpoint uint64
}

// advancerState is the sealed type for the state of advancer.
// the advancer has two stage: full scan and update small tree.
type advancerState interface {
	// Note:
	// Go doesn't support sealed classes or ADTs currently.
	// (it can only be used at generic constraints...)
	// Leave it empty for now.

	// ~*fullScan | ~*updateSmallTree
}

// fullScan is the initial state of advancer.
// in this stage, we would "fill" the cache:
// insert ranges that union of them become the full range of task.
type fullScan struct {
	fullScanTick int
}

// updateSmallTree is the "incremental stage" of advancer.
// we have build a "filled" cache, and we can pop a subrange of it,
// try to advance the checkpoint of those ranges.
type updateSmallTree struct {
	consistencyCheckTick int
}

// NewCheckpointAdvancer creates a checkpoint advancer with the env.
func NewCheckpointAdvancer(env Env) *CheckpointAdvancer {
	return &CheckpointAdvancer{
		env:   env,
		cfg:   config.Default(),
		cache: NewCheckpoints(),
		state: &fullScan{},
	}
}

// disableCache removes the cache.
// note this won't lock the checkpoint advancer at `fullScan` state forever,
// you may need to change the config `AdvancingByCache`.
func (c *CheckpointAdvancer) disableCache() {
	c.cache = NoOPCheckpointCache{}
	c.state = &fullScan{}
}

// enable the cache.
// also check `AdvancingByCache` in the config.
func (c *CheckpointAdvancer) enableCache() {
	c.cache = NewCheckpoints()
	c.state = &fullScan{}
}

// UpdateConfig updates the config for the advancer.
// Note this should be called before starting the loop, because there isn't locks,
// TODO: support updating config when advancer starts working.
// (Maybe by applying changes at begin of ticking, and add locks.)
func (c *CheckpointAdvancer) UpdateConfig(newConf config.Config) {
	needRefreshCache := newConf.AdvancingByCache != c.cfg.AdvancingByCache
	c.cfg = newConf
	if needRefreshCache {
		if c.cfg.AdvancingByCache {
			c.enableCache()
		} else {
			c.disableCache()
		}
	}
}

// UpdateConfigWith updates the config by modifying the current config.
func (c *CheckpointAdvancer) UpdateConfigWith(f func(*config.Config)) {
	cfg := c.cfg
	f(&cfg)
	c.UpdateConfig(cfg)
}

// Config returns the current config.
func (c *CheckpointAdvancer) Config() config.Config {
	return c.cfg
}

// GetCheckpointInRange scans the regions in the range,
// collect them to the collector.
func (c *CheckpointAdvancer) GetCheckpointInRange(ctx context.Context, start, end []byte, collector *clusterCollector) error {
	log.Debug("scanning range", logutil.Key("start", start), logutil.Key("end", end))
	iter := IterateRegion(c.env, start, end)
	for !iter.Done() {
		rs, err := iter.Next(ctx)
		if err != nil {
			return err
		}
		log.Debug("scan region", zap.Int("len", len(rs)))
		for _, r := range rs {
			err := collector.collectRegion(r)
			if err != nil {
				log.Warn("meet error during getting checkpoint", logutil.ShortError(err))
				return err
			}
		}
	}
	return nil
}

func (c *CheckpointAdvancer) recordTimeCost(message string, fields ...zap.Field) func() {
	now := time.Now()
	label := strings.ReplaceAll(message, " ", "-")
	return func() {
		cost := time.Since(now)
		fields = append(fields, zap.Stringer("take", cost))
		metrics.AdvancerTickDuration.WithLabelValues(label).Observe(cost.Seconds())
		log.Debug(message, fields...)
	}
}

// tryAdvance tries to advance the checkpoint ts of a set of ranges which shares the same checkpoint.
func (c *CheckpointAdvancer) tryAdvance(ctx context.Context, rst RangesSharesTS) (err error) {
	defer c.recordTimeCost("try advance", zap.Uint64("checkpoint", rst.TS), zap.Int("len", len(rst.Ranges)))()
	defer func() {
		if err != nil {
			log.Warn("failed to advance", logutil.ShortError(err), zap.Object("target", rst.Zap()))
			c.cache.InsertRanges(rst)
		}
	}()
	defer utils.PanicToErr(&err)

	ranges := CollapseRanges(len(rst.Ranges), func(i int) kv.KeyRange { return rst.Ranges[i] })
	workers := utils.NewWorkerPool(4, "sub ranges")
	eg, cx := errgroup.WithContext(ctx)
	collector := NewClusterCollector(ctx, c.env)
	collector.setOnSuccessHook(c.cache.InsertRange)
	for _, r := range ranges {
		r := r
		workers.ApplyOnErrorGroup(eg, func() (e error) {
			defer c.recordTimeCost("get regions in range", zap.Uint64("checkpoint", rst.TS))()
			defer utils.PanicToErr(&e)
			return c.GetCheckpointInRange(cx, r.StartKey, r.EndKey, collector)
		})
	}
	err = eg.Wait()
	if err != nil {
		return err
	}

	result, err := collector.Finish(ctx)
	if err != nil {
		return err
	}
	fr := result.FailureSubRanges
	if len(fr) != 0 {
		log.Debug("failure regions collected", zap.Int("size", len(fr)))
		c.cache.InsertRanges(RangesSharesTS{
			TS:     rst.TS,
			Ranges: fr,
		})
	}
	return nil
}

// CalculateGlobalCheckpointLight tries to advance the global checkpoint by the cache.
func (c *CheckpointAdvancer) CalculateGlobalCheckpointLight(ctx context.Context) (uint64, error) {
	log.Info("[log backup advancer hint] advancer with cache: current tree", zap.Stringer("ct", c.cache))
	rsts := c.cache.PopRangesWithGapGT(config.DefaultTryAdvanceThreshold)
	if len(rsts) == 0 {
		return 0, nil
	}
	samples := rsts
	if len(rsts) > 3 {
		samples = rsts[:3]
	}
	for _, sample := range samples {
		log.Info("[log backup advancer hint] sample range.", zap.Object("range", sample.Zap()), zap.Int("total-len", len(rsts)))
	}

	workers := utils.NewWorkerPool(uint(config.DefaultMaxConcurrencyAdvance), "regions")
	eg, cx := errgroup.WithContext(ctx)
	for _, rst := range rsts {
		rst := rst
		workers.ApplyOnErrorGroup(eg, func() (err error) {
			return c.tryAdvance(cx, *rst)
		})
	}
	err := eg.Wait()
	if err != nil {
		return 0, err
	}
	ts := c.cache.CheckpointTS()
	return ts, nil
}

// CalculateGlobalCheckpoint calculates the global checkpoint, which won't use the cache.
func (c *CheckpointAdvancer) CalculateGlobalCheckpoint(ctx context.Context) (uint64, error) {
	var (
		cp = uint64(math.MaxInt64)
		// TODO: Use The task range here.
		thisRun []kv.KeyRange = []kv.KeyRange{{}}
		nextRun []kv.KeyRange
	)
	defer c.recordTimeCost("record all")
	cx, cancel := context.WithTimeout(ctx, c.cfg.MaxBackoffTime)
	defer cancel()
	for {
		coll := NewClusterCollector(ctx, c.env)
		coll.setOnSuccessHook(c.cache.InsertRange)
		for _, u := range thisRun {
			err := c.GetCheckpointInRange(cx, u.StartKey, u.EndKey, coll)
			if err != nil {
				return 0, err
			}
		}
		result, err := coll.Finish(ctx)
		if err != nil {
			return 0, err
		}
		log.Debug("full: a run finished", zap.Any("checkpoint", result))

		nextRun = append(nextRun, result.FailureSubRanges...)
		if cp > result.Checkpoint {
			cp = result.Checkpoint
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

// CollapseRanges collapse ranges overlapping or adjacent.
// Example:
// CollapseRanges({[1, 4], [2, 8], [3, 9]}) == {[1, 9]}
// CollapseRanges({[1, 3], [4, 7], [2, 3]}) == {[1, 3], [4, 7]}
func CollapseRanges(length int, getRange func(int) kv.KeyRange) []kv.KeyRange {
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
			if i >= len(frs) || (len(item.EndKey) != 0 && bytes.Compare(frs[i].StartKey, item.EndKey) > 0) {
				break
			}
			if len(item.EndKey) != 0 && bytes.Compare(item.EndKey, frs[i].EndKey) < 0 || len(frs[i].EndKey) == 0 {
				item.EndKey = frs[i].EndKey
			}
		}
		result = append(result, item)
	}
	return result
}

func (c *CheckpointAdvancer) consumeAllTask(ctx context.Context, ch <-chan TaskEvent) error {
	for {
		select {
		case e, ok := <-ch:
			if !ok {
				return nil
			}
			log.Info("meet task event", zap.Stringer("event", &e))
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

// beginListenTaskChange bootstraps the initial task set,
// and returns a channel respecting the change of tasks.
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

// StartTaskListener starts the task listener for the advancer.
// When no task detected, advancer would do nothing, please call this before begin the tick loop.
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
				log.Info("meet task event", zap.Stringer("event", &e))
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
		c.state = &fullScan{}
		c.cache.Clear()
	case EventErr:
		return e.Err
	}
	return nil
}

// advanceCheckpointBy advances the checkpoint by a checkpoint getter function.
func (c *CheckpointAdvancer) advanceCheckpointBy(ctx context.Context, getCheckpoint func(context.Context) (uint64, error)) error {
	start := time.Now()
	cp, err := getCheckpoint(ctx)
	if err != nil {
		return err
	}
	log.Info("get checkpoint", zap.Uint64("old", c.lastCheckpoint), zap.Uint64("new", cp))
	if cp < c.lastCheckpoint {
		log.Warn("failed to update global checkpoint: stale", zap.Uint64("old", c.lastCheckpoint), zap.Uint64("new", cp))
	}
	if cp <= c.lastCheckpoint {
		return nil
	}

	log.Info("uploading checkpoint for task",
		zap.Stringer("checkpoint", oracle.GetTimeFromTS(cp)),
		zap.Uint64("checkpoint", cp),
		zap.String("task", c.task.Name),
		zap.Stringer("take", time.Since(start)))
	if err := c.env.UploadV3GlobalCheckpointForTask(ctx, c.task.Name, cp); err != nil {
		return errors.Annotate(err, "failed to upload global checkpoint")
	}
	c.lastCheckpoint = cp
	metrics.LastCheckpoint.WithLabelValues(c.task.GetName()).Set(float64(c.lastCheckpoint))
	return nil
}

// OnTick advances the inner logic clock for the advancer.
// It's synchronous: this would only return after the events triggered by the clock has all been done.
// It's generally panic-free, you may not need to trying recover a panic here.
func (c *CheckpointAdvancer) OnTick(ctx context.Context) (err error) {
	defer c.recordTimeCost("tick")()
	defer func() {
		e := recover()
		if e != nil {
			log.Error("panic during handing tick", zap.Stack("stack"), logutil.ShortError(err))
			err = errors.Annotatef(berrors.ErrUnknown, "panic during handling tick: %s", e)
		}
	}()
	err = c.tick(ctx)
	return
}

func (c *CheckpointAdvancer) onConsistencyCheckTick(s *updateSmallTree) error {
	if s.consistencyCheckTick > 0 {
		s.consistencyCheckTick--
		return nil
	}
	defer c.recordTimeCost("consistency check")()
	err := c.cache.ConsistencyCheck()
	if err != nil {
		log.Error("consistency check failed! log backup may lose data! rolling back to full scan for saving.", logutil.ShortError(err))
		c.state = &fullScan{}
		return err
	} else {
		log.Debug("consistency check passed.")
	}
	s.consistencyCheckTick = config.DefaultConsistencyCheckTick
	return nil
}

func (c *CheckpointAdvancer) tick(ctx context.Context) error {
	c.taskMu.Lock()
	defer c.taskMu.Unlock()

	switch s := c.state.(type) {
	case *fullScan:
		if s.fullScanTick > 0 {
			s.fullScanTick--
			break
		}
		if c.task == nil {
			log.Debug("No tasks yet, skipping advancing.")
			return nil
		}
		defer func() {
			s.fullScanTick = c.cfg.FullScanTick
		}()
		err := c.advanceCheckpointBy(ctx, c.CalculateGlobalCheckpoint)
		if err != nil {
			return err
		}

		if c.cfg.AdvancingByCache {
			c.state = &updateSmallTree{}
		}
	case *updateSmallTree:
		if err := c.onConsistencyCheckTick(s); err != nil {
			return err
		}
		err := c.advanceCheckpointBy(ctx, c.CalculateGlobalCheckpointLight)
		if err != nil {
			return err
		}
	default:
		log.Error("Unknown state type, skipping tick", zap.Stringer("type", reflect.TypeOf(c.state)))
	}
	return nil
}
