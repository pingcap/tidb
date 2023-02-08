// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/streamhelper/config"
	"github.com/pingcap/tidb/br/pkg/streamhelper/spans"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// CheckpointAdvancer is the central node for advancing the checkpoint of log backup.
// It's a part of "checkpoint v3".
// Generally, it scan the regions in the task range, collect checkpoints from tikvs.
/*
                                         ┌──────┐
                                   ┌────►│ TiKV │
                                   │     └──────┘
                                   │
                                   │
 ┌──────────┐GetLastFlushTSOfRegion│     ┌──────┐
 │ Advancer ├──────────────────────┼────►│ TiKV │
 └────┬─────┘                      │     └──────┘
      │                            │
      │                            │
      │                            │     ┌──────┐
      │                            └────►│ TiKV │
      │                                  └──────┘
      │
      │ UploadCheckpointV3   ┌──────────────────┐
      └─────────────────────►│  PD              │
                             └──────────────────┘
*/
type CheckpointAdvancer struct {
	env Env

	// The concurrency accessed task:
	// both by the task listener and ticking.
	task      *backuppb.StreamBackupTaskInfo
	taskRange []kv.KeyRange
	taskMu    sync.Mutex

	// the read-only config.
	// once tick begin, this should not be changed for now.
	cfg config.Config

	// the cached last checkpoint.
	// if no progress, this cache can help us don't to send useless requests.
	lastCheckpoint uint64

	checkpoints   *spans.ValueSortedFull
	checkpointsMu sync.Mutex

	subscriber   *FlushSubscriber
	subscriberMu sync.Mutex
}

// NewCheckpointAdvancer creates a checkpoint advancer with the env.
func NewCheckpointAdvancer(env Env) *CheckpointAdvancer {
	return &CheckpointAdvancer{
		env: env,
		cfg: config.Default(),
	}
}

// UpdateConfig updates the config for the advancer.
// Note this should be called before starting the loop, because there isn't locks,
// TODO: support updating config when advancer starts working.
// (Maybe by applying changes at begin of ticking, and add locks.)
func (c *CheckpointAdvancer) UpdateConfig(newConf config.Config) {
	c.cfg = newConf
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
			err := collector.CollectRegion(r)
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
func (c *CheckpointAdvancer) tryAdvance(ctx context.Context, length int, getRange func(int) kv.KeyRange) (err error) {
	defer c.recordTimeCost("try advance", zap.Int("len", length))()
	defer utils.PanicToErr(&err)

	ranges := spans.Collapse(length, getRange)
	workers := utils.NewWorkerPool(uint(config.DefaultMaxConcurrencyAdvance)*4, "sub ranges")
	eg, cx := errgroup.WithContext(ctx)
	collector := NewClusterCollector(ctx, c.env)
	collector.SetOnSuccessHook(func(u uint64, kr kv.KeyRange) {
		c.checkpointsMu.Lock()
		defer c.checkpointsMu.Unlock()
		c.checkpoints.Merge(spans.Valued{Key: kr, Value: u})
	})
	clampedRanges := utils.IntersectAll(ranges, utils.CloneSlice(c.taskRange))
	for _, r := range clampedRanges {
		r := r
		workers.ApplyOnErrorGroup(eg, func() (e error) {
			defer c.recordTimeCost("get regions in range")()
			defer utils.PanicToErr(&e)
			return c.GetCheckpointInRange(cx, r.StartKey, r.EndKey, collector)
		})
	}
	err = eg.Wait()
	if err != nil {
		return err
	}

	_, err = collector.Finish(ctx)
	if err != nil {
		return err
	}
	return nil
}

func tsoBefore(n time.Duration) uint64 {
	now := time.Now()
	return oracle.ComposeTS(now.UnixMilli()-n.Milliseconds(), 0)
}

func (c *CheckpointAdvancer) CalculateGlobalCheckpointLight(ctx context.Context, threshold time.Duration) (uint64, error) {
	var targets []spans.Valued
	c.checkpoints.TraverseValuesLessThan(tsoBefore(threshold), func(v spans.Valued) bool {
		targets = append(targets, v)
		return true
	})
	if len(targets) == 0 {
		c.checkpointsMu.Lock()
		defer c.checkpointsMu.Unlock()
		return c.checkpoints.MinValue(), nil
	}
	samples := targets
	if len(targets) > 3 {
		samples = targets[:3]
	}
	for _, sample := range samples {
		log.Info("[log backup advancer hint] sample range.", zap.Stringer("sample", sample), zap.Int("total-len", len(targets)))
	}

	err := c.tryAdvance(ctx, len(targets), func(i int) kv.KeyRange { return targets[i].Key })
	if err != nil {
		return 0, err
	}
	c.checkpointsMu.Lock()
	ts := c.checkpoints.MinValue()
	c.checkpointsMu.Unlock()
	return ts, nil
}

func (c *CheckpointAdvancer) consumeAllTask(ctx context.Context, ch <-chan TaskEvent) error {
	for {
		select {
		case e, ok := <-ch:
			if !ok {
				return nil
			}
			log.Info("meet task event", zap.Stringer("event", &e))
			if err := c.onTaskEvent(ctx, e); err != nil {
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
					log.Info("[log backup advancer] Task watcher exits due to stream ends.")
					return
				}
				log.Info("[log backup advancer] Meet task event", zap.Stringer("event", &e))
				if err := c.onTaskEvent(ctx, e); err != nil {
					if errors.Cause(e.Err) != context.Canceled {
						log.Error("listen task meet error, would reopen.", logutil.ShortError(err))
						time.AfterFunc(c.cfg.BackoffTime, func() { c.StartTaskListener(ctx) })
					}
					log.Info("[log backup advancer] Task watcher exits due to some error.", logutil.ShortError(err))
					return
				}
			}
		}
	}()
}

func (c *CheckpointAdvancer) onTaskEvent(ctx context.Context, e TaskEvent) error {
	c.taskMu.Lock()
	defer c.taskMu.Unlock()
	switch e.Type {
	case EventAdd:
		utils.LogBackupTaskCountInc()
		c.task = e.Info
		c.taskRange = spans.Collapse(len(e.Ranges), func(i int) kv.KeyRange { return e.Ranges[i] })
		c.checkpoints = spans.Sorted(spans.NewFullWith(e.Ranges, 0))
		c.lastCheckpoint = e.Info.StartTs
		log.Info("added event", zap.Stringer("task", e.Info), zap.Stringer("ranges", logutil.StringifyKeys(c.taskRange)))
	case EventDel:
		utils.LogBackupTaskCountDec()
		c.task = nil
		c.taskRange = nil
		c.checkpoints = nil
		// This would be synced by `taskMu`, perhaps we'd better rename that to `tickMu`.
		// Do the null check because some of test cases won't equip the advancer with subscriber.
		if c.subscriber != nil {
			c.subscriber.Clear()
		}
		if err := c.env.ClearV3GlobalCheckpointForTask(ctx, e.Name); err != nil {
			log.Warn("failed to clear global checkpoint", logutil.ShortError(err))
		}
		metrics.LastCheckpoint.DeleteLabelValues(e.Name)
	case EventErr:
		return e.Err
	}
	return nil
}

func (c *CheckpointAdvancer) setCheckpoint(cp uint64) bool {
	if cp < c.lastCheckpoint {
		log.Warn("failed to update global checkpoint: stale", zap.Uint64("old", c.lastCheckpoint), zap.Uint64("new", cp))
		return false
	}
	if cp <= c.lastCheckpoint {
		return false
	}
	c.lastCheckpoint = cp
	return true
}

// advanceCheckpointBy advances the checkpoint by a checkpoint getter function.
func (c *CheckpointAdvancer) advanceCheckpointBy(ctx context.Context, getCheckpoint func(context.Context) (uint64, error)) error {
	start := time.Now()
	cp, err := getCheckpoint(ctx)
	if err != nil {
		return err
	}

	if c.setCheckpoint(cp) {
		log.Info("uploading checkpoint for task",
			zap.Stringer("checkpoint", oracle.GetTimeFromTS(cp)),
			zap.Uint64("checkpoint", cp),
			zap.String("task", c.task.Name),
			zap.Stringer("take", time.Since(start)))
		metrics.LastCheckpoint.WithLabelValues(c.task.GetName()).Set(float64(c.lastCheckpoint))
	}
	return nil
}

func (c *CheckpointAdvancer) stopSubscriber() {
	c.subscriberMu.Lock()
	defer c.subscriberMu.Unlock()
	c.subscriber.Drop()
	c.subscriber = nil
}

func (c *CheckpointAdvancer) spawnSubscriptionHandler(ctx context.Context) {
	c.subscriberMu.Lock()
	defer c.subscriberMu.Unlock()
	c.subscriber = NewSubscriber(c.env, c.env, WithMasterContext(ctx))
	es := c.subscriber.Events()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-es:
				if !ok {
					return
				}
				c.checkpointsMu.Lock()
				log.Debug("Accepting region flush event.",
					zap.Stringer("range", logutil.StringifyRange(event.Key)),
					zap.Uint64("checkpoint", event.Value))
				c.checkpoints.Merge(event)
				c.checkpointsMu.Unlock()
			}
		}
	}()
}

func (c *CheckpointAdvancer) subscribeTick(ctx context.Context) error {
	if c.subscriber == nil {
		return nil
	}
	if err := c.subscriber.UpdateStoreTopology(ctx); err != nil {
		log.Warn("[log backup advancer] Error when updating store topology.", logutil.ShortError(err))
	}
	c.subscriber.HandleErrors(ctx)
	return c.subscriber.PendingErrors()
}

func (c *CheckpointAdvancer) importantTick(ctx context.Context) error {
	c.checkpointsMu.Lock()
	c.setCheckpoint(c.checkpoints.MinValue())
	c.checkpointsMu.Unlock()
	if err := c.env.UploadV3GlobalCheckpointForTask(ctx, c.task.Name, c.lastCheckpoint); err != nil {
		return errors.Annotate(err, "failed to upload global checkpoint")
	}
	return nil
}

func (c *CheckpointAdvancer) optionalTick(cx context.Context) error {
	threshold := c.Config().GetDefaultStartPollThreshold()
	if err := c.subscribeTick(cx); err != nil {
		log.Warn("[log backup advancer] Subscriber meet error, would polling the checkpoint.", logutil.ShortError(err))
		threshold = c.Config().GetSubscriberErrorStartPollThreshold()
	}

	err := c.advanceCheckpointBy(cx, func(cx context.Context) (uint64, error) {
		return c.CalculateGlobalCheckpointLight(cx, threshold)
	})
	if err != nil {
		return err
	}
	return nil
}

func (c *CheckpointAdvancer) tick(ctx context.Context) error {
	c.taskMu.Lock()
	defer c.taskMu.Unlock()
	if c.task == nil {
		log.Debug("No tasks yet, skipping advancing.")
		return nil
	}

	var errs error

	cx, cancel := context.WithTimeout(ctx, c.Config().TickTimeout())
	defer cancel()
	err := c.optionalTick(cx)
	if err != nil {
		log.Warn("[log backup advancer] option tick failed.", logutil.ShortError(err))
		errs = multierr.Append(errs, err)
	}

	err = c.importantTick(ctx)
	if err != nil {
		log.Warn("[log backup advancer] important tick failed.", logutil.ShortError(err))
		errs = multierr.Append(errs, err)
	}

	return errs
}
