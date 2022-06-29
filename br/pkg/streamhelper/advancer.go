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

type CheckpointAdvancer struct {
	env Env

	task   *backuppb.StreamBackupTaskInfo
	taskMu sync.Mutex

	cfg config.Config

	cache CheckpointsCache

	state          advancerState
	lastCheckpoint uint64
}

type advancerState interface{}

type fullScan struct {
	fullScanTick int
}

type updateSmallTree struct {
	consistencyCheckTick int
}

func NewCheckpointAdvancer(env Env) *CheckpointAdvancer {
	return &CheckpointAdvancer{
		env:   env,
		cfg:   config.Default(),
		cache: NewCheckpoints(),
		state: &fullScan{},
	}
}

func (c *CheckpointAdvancer) disableCache() {
	c.cache = NoOPCheckpointCache{}
	c.state = fullScan{}
}

func (c *CheckpointAdvancer) enableCache() {
	c.cache = NewCheckpoints()
	c.state = fullScan{}
}

// UpdateConfig updates the config for the advancer.
// Note this should be called before starting the loop, because there isn't locks,
// TODO: support updating config when advancing starts working. (Maybe by applying changes at begin of ticking)
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

func (c *CheckpointAdvancer) UpdateConfigWith(f func(*config.Config)) {
	cfg := c.cfg
	f(&cfg)
	c.UpdateConfig(cfg)
}

func (c *CheckpointAdvancer) Config() config.Config {
	return c.cfg
}

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
			collector.collectRegion(r)
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

func (c *CheckpointAdvancer) tryAdvance(ctx context.Context, rst *RangesSharesTS) error {
	defer c.recordTimeCost("try advance", zap.Uint64("checkpoint", rst.TS), zap.Int("len", len(rst.Ranges)))()
	ranges := CollapseRanges(len(rst.Ranges), func(i int) kv.KeyRange { return rst.Ranges[i] })
	workers := utils.NewWorkerPool(4, "subranges")
	eg, cx := errgroup.WithContext(ctx)
	collector := NewClusterCollector(ctx, c.env)
	collector.setOnSuccessHook(c.cache.InsertRange)
	for _, r := range ranges {
		r := r
		workers.ApplyOnErrorGroup(eg, func() error {
			defer c.recordTimeCost("get regions in range", zap.Uint64("checkpoint", rst.TS))()
			return c.GetCheckpointInRange(cx, r.StartKey, r.EndKey, collector)
		})
	}
	err := eg.Wait()
	if err != nil {
		c.cache.InsertRanges(*rst)
		return err
	}

	result, err := collector.Wait(ctx)
	if err != nil {
		c.cache.InsertRanges(*rst)
		return err
	}
	fr := result.FailureSubranges
	if len(fr) != 0 {
		log.Info("failure regions collected", zap.Int("size", len(fr)))
		c.cache.InsertRanges(RangesSharesTS{
			TS:     rst.TS,
			Ranges: fr,
		})
	}
	return nil
}

func (c *CheckpointAdvancer) UpdateGlobalCheckpointLight(ctx context.Context) (uint64, error) {
	log.Debug("current tree", zap.Stringer("ct", c.cache))
	rsts := c.cache.PopRangesWithGapGT(config.DefaultTryAdvanceThreshold)
	if len(rsts) == 0 {
		return 0, nil
	}
	workers := utils.NewWorkerPool(config.DefaultMaxConcurrencyAdvance, "regions")
	eg, cx := errgroup.WithContext(ctx)
	for _, rst := range rsts {
		rst := rst
		workers.ApplyOnErrorGroup(eg, func() error { return c.tryAdvance(cx, rst) })
	}
	err := eg.Wait()
	if err != nil {
		return 0, err
	}
	log.Debug("new tree", zap.Stringer("cache", c.cache))
	ts := c.cache.CheckpointTS()
	return ts, nil
}

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
		result, err := coll.Wait(ctx)
		if err != nil {
			return 0, err
		}
		log.Debug("full: a run finished", zap.Any("checkpoint", result))

		nextRun = append(nextRun, result.FailureSubranges...)
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

func (c *CheckpointAdvancer) onConsistencyCheckTick(s *updateSmallTree) {
	if s.consistencyCheckTick > 0 {
		s.consistencyCheckTick--
		return
	}
	defer c.recordTimeCost("consistency check")()
	err := c.cache.ConsistencyCheck()
	if err != nil {
		log.Error("consistency check failed! log backup may lose data!", logutil.ShortError(err))
	} else {
		log.Debug("consistency check passed.")
	}
	s.consistencyCheckTick = config.DefaultConsistencyCheckTick
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
		err := c.advanceCheckpointBy(ctx, func() (uint64, error) { return c.CalculateGlobalCheckpoint(ctx) })
		if err != nil {
			return err
		}

		if c.cfg.AdvancingByCache {
			c.state = &updateSmallTree{}
		}
	case *updateSmallTree:
		c.onConsistencyCheckTick(s)
		err := c.advanceCheckpointBy(ctx, func() (uint64, error) { return c.UpdateGlobalCheckpointLight(ctx) })
		if err != nil {
			return err
		}
	default:
		log.Error("Unknown state type, skipping tick", zap.Stringer("type", reflect.TypeOf(c.state)))
	}
	return nil
}
