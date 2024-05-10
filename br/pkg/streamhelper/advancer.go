// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/streamhelper/config"
	"github.com/pingcap/tidb/br/pkg/streamhelper/spans"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/rangetask"
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
	lastCheckpoint   *checkpoint
	lastCheckpointMu sync.Mutex
	inResolvingLock  atomic.Bool
	isPaused         atomic.Bool

	checkpoints   *spans.ValueSortedFull
	checkpointsMu sync.Mutex

	subscriber   *FlushSubscriber
	subscriberMu sync.Mutex
}

// HasTask returns whether the advancer has been bound to a task.
func (c *CheckpointAdvancer) HasTask() bool {
	c.taskMu.Lock()
	defer c.taskMu.Unlock()

	return c.task != nil
}

// HasSubscriber returns whether the advancer is associated with a subscriber.
func (c *CheckpointAdvancer) HasSubscribion() bool {
	c.subscriberMu.Lock()
	defer c.subscriberMu.Unlock()

	return c.subscriber != nil && len(c.subscriber.subscriptions) > 0
}

// checkpoint represents the TS with specific range.
// it's only used in advancer.go.
type checkpoint struct {
	StartKey []byte
	EndKey   []byte
	TS       uint64

	// It's better to use PD timestamp in future, for now
	// use local time to decide the time to resolve lock is ok.
	resolveLockTime time.Time
}

func newCheckpointWithTS(ts uint64) *checkpoint {
	return &checkpoint{
		TS:              ts,
		resolveLockTime: time.Now(),
	}
}

func NewCheckpointWithSpan(s spans.Valued) *checkpoint {
	return &checkpoint{
		StartKey:        s.Key.StartKey,
		EndKey:          s.Key.EndKey,
		TS:              s.Value,
		resolveLockTime: time.Now(),
	}
}

func (c *checkpoint) safeTS() uint64 {
	return c.TS - 1
}

func (c *checkpoint) equal(o *checkpoint) bool {
	return bytes.Equal(c.StartKey, o.StartKey) &&
		bytes.Equal(c.EndKey, o.EndKey) && c.TS == o.TS
}

// if a checkpoint stay in a time too long(3 min)
// we should try to resolve lock for the range
// to keep the RPO in 5 min.
func (c *checkpoint) needResolveLocks() bool {
	failpoint.Inject("NeedResolveLocks", func(val failpoint.Value) {
		failpoint.Return(val.(bool))
	})
	return time.Since(c.resolveLockTime) > 3*time.Minute
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

// UpdateLastCheckpoint modify the checkpoint in ticking.
func (c *CheckpointAdvancer) UpdateLastCheckpoint(p *checkpoint) {
	c.lastCheckpointMu.Lock()
	c.lastCheckpoint = p
	c.lastCheckpointMu.Unlock()
}

// Config returns the current config.
func (c *CheckpointAdvancer) Config() config.Config {
	return c.cfg
}

// GetInResolvingLock only used for test.
func (c *CheckpointAdvancer) GetInResolvingLock() bool {
	return c.inResolvingLock.Load()
}

// GetCheckpointInRange scans the regions in the range,
// collect them to the collector.
func (c *CheckpointAdvancer) GetCheckpointInRange(ctx context.Context, start, end []byte,
	collector *clusterCollector) error {
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
func (c *CheckpointAdvancer) tryAdvance(ctx context.Context, length int,
	getRange func(int) kv.KeyRange) (err error) {
	defer c.recordTimeCost("try advance", zap.Int("len", length))()
	defer utils.PanicToErr(&err)

	ranges := spans.Collapse(length, getRange)
	workers := util.NewWorkerPool(uint(config.DefaultMaxConcurrencyAdvance)*4, "sub ranges")
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

func tsoAfter(ts uint64, n time.Duration) uint64 {
	return oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(n))
}

func (c *CheckpointAdvancer) WithCheckpoints(f func(*spans.ValueSortedFull)) {
	c.checkpointsMu.Lock()
	defer c.checkpointsMu.Unlock()

	f(c.checkpoints)
}

// only used for test
func (c *CheckpointAdvancer) NewCheckpoints(cps *spans.ValueSortedFull) {
	c.checkpoints = cps
}

func (c *CheckpointAdvancer) fetchRegionHint(ctx context.Context, startKey []byte) string {
	region, err := locateKeyOfRegion(ctx, c.env, startKey)
	if err != nil {
		return errors.Annotate(err, "failed to fetch region").Error()
	}
	r := region.Region
	l := region.Leader
	prs := []int{}
	for _, p := range r.GetPeers() {
		prs = append(prs, int(p.StoreId))
	}
	metrics.LogBackupCurrentLastRegionID.Set(float64(r.Id))
	metrics.LogBackupCurrentLastRegionLeaderStoreID.Set(float64(l.StoreId))
	return fmt.Sprintf("ID=%d,Leader=%d,ConfVer=%d,Version=%d,Peers=%v,RealRange=%s",
		r.GetId(), l.GetStoreId(), r.GetRegionEpoch().GetConfVer(), r.GetRegionEpoch().GetVersion(),
		prs, logutil.StringifyRangeOf(r.GetStartKey(), r.GetEndKey()))
}

func (c *CheckpointAdvancer) CalculateGlobalCheckpointLight(ctx context.Context,
	threshold time.Duration) (spans.Valued, error) {
	var targets []spans.Valued
	var minValue spans.Valued
	thresholdTso := tsoBefore(threshold)
	c.WithCheckpoints(func(vsf *spans.ValueSortedFull) {
		vsf.TraverseValuesLessThan(thresholdTso, func(v spans.Valued) bool {
			targets = append(targets, v)
			return true
		})
		minValue = vsf.Min()
	})
	sctx, cancel := context.WithTimeout(ctx, time.Second)
	// Always fetch the hint and update the metrics.
	hint := c.fetchRegionHint(sctx, minValue.Key.StartKey)
	logger := log.Debug
	if minValue.Value < thresholdTso {
		logger = log.Info
	}
	logger("current last region", zap.String("category", "log backup advancer hint"),
		zap.Stringer("min", minValue), zap.Int("for-polling", len(targets)),
		zap.String("min-ts", oracle.GetTimeFromTS(minValue.Value).Format(time.RFC3339)),
		zap.String("region-hint", hint),
	)
	cancel()
	if len(targets) == 0 {
		return minValue, nil
	}
	err := c.tryAdvance(ctx, len(targets), func(i int) kv.KeyRange { return targets[i].Key })
	if err != nil {
		return minValue, err
	}
	return minValue, nil
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
					log.Info("Task watcher exits due to stream ends.", zap.String("category", "log backup advancer"))
					return
				}
				log.Info("Meet task event", zap.String("category", "log backup advancer"), zap.Stringer("event", &e))
				if err := c.onTaskEvent(ctx, e); err != nil {
					if errors.Cause(e.Err) != context.Canceled {
						log.Error("listen task meet error, would reopen.", logutil.ShortError(err))
						time.AfterFunc(c.cfg.BackoffTime, func() { c.StartTaskListener(ctx) })
					}
					log.Info("Task watcher exits due to some error.", zap.String("category", "log backup advancer"),
						logutil.ShortError(err))
					return
				}
			}
		}
	}()
}

func (c *CheckpointAdvancer) setCheckpoints(cps *spans.ValueSortedFull) {
	c.checkpointsMu.Lock()
	c.checkpoints = cps
	c.checkpointsMu.Unlock()
}

func (c *CheckpointAdvancer) onTaskEvent(ctx context.Context, e TaskEvent) error {
	c.taskMu.Lock()
	defer c.taskMu.Unlock()
	switch e.Type {
	case EventAdd:
		utils.LogBackupTaskCountInc()
		c.task = e.Info
		c.taskRange = spans.Collapse(len(e.Ranges), func(i int) kv.KeyRange { return e.Ranges[i] })
		c.setCheckpoints(spans.Sorted(spans.NewFullWith(e.Ranges, 0)))
		c.lastCheckpoint = newCheckpointWithTS(e.Info.StartTs)
		p, err := c.env.BlockGCUntil(ctx, c.task.StartTs)
		if err != nil {
			log.Warn("failed to upload service GC safepoint, skipping.", logutil.ShortError(err))
		}
		log.Info("added event", zap.Stringer("task", e.Info),
			zap.Stringer("ranges", logutil.StringifyKeys(c.taskRange)), zap.Uint64("current-checkpoint", p))
	case EventDel:
		utils.LogBackupTaskCountDec()
		c.task = nil
		c.isPaused.Store(false)
		c.taskRange = nil
		// This would be synced by `taskMu`, perhaps we'd better rename that to `tickMu`.
		// Do the null check because some of test cases won't equip the advancer with subscriber.
		if c.subscriber != nil {
			c.subscriber.Clear()
		}
		c.setCheckpoints(nil)
		if err := c.env.ClearV3GlobalCheckpointForTask(ctx, e.Name); err != nil {
			log.Warn("failed to clear global checkpoint", logutil.ShortError(err))
		}
		if err := c.env.UnblockGC(ctx); err != nil {
			log.Warn("failed to remove service GC safepoint", logutil.ShortError(err))
		}
		metrics.LastCheckpoint.DeleteLabelValues(e.Name)
	case EventPause:
		if c.task.GetName() == e.Name {
			c.isPaused.Store(true)
		}
	case EventResume:
		if c.task.GetName() == e.Name {
			c.isPaused.Store(false)
		}
	case EventErr:
		return e.Err
	}
	return nil
}

func (c *CheckpointAdvancer) setCheckpoint(ctx context.Context, s spans.Valued) bool {
	cp := NewCheckpointWithSpan(s)
	if cp.TS < c.lastCheckpoint.TS {
		log.Warn("failed to update global checkpoint: stale",
			zap.Uint64("old", c.lastCheckpoint.TS), zap.Uint64("new", cp.TS))
		return false
	}
	// Need resolve lock for different range and same TS
	// so check the range and TS here.
	if cp.equal(c.lastCheckpoint) {
		return false
	}
	c.UpdateLastCheckpoint(cp)
	metrics.LastCheckpoint.WithLabelValues(c.task.GetName()).Set(float64(c.lastCheckpoint.TS))
	return true
}

// advanceCheckpointBy advances the checkpoint by a checkpoint getter function.
func (c *CheckpointAdvancer) advanceCheckpointBy(ctx context.Context,
	getCheckpoint func(context.Context) (spans.Valued, error)) error {
	start := time.Now()
	cp, err := getCheckpoint(ctx)
	if err != nil {
		return err
	}

	if c.setCheckpoint(ctx, cp) {
		log.Info("uploading checkpoint for task",
			zap.Stringer("checkpoint", oracle.GetTimeFromTS(cp.Value)),
			zap.Uint64("checkpoint", cp.Value),
			zap.String("task", c.task.Name),
			zap.Stringer("take", time.Since(start)))
	}
	return nil
}

func (c *CheckpointAdvancer) stopSubscriber() {
	c.subscriberMu.Lock()
	defer c.subscriberMu.Unlock()
	c.subscriber.Drop()
	c.subscriber = nil
}

func (c *CheckpointAdvancer) SpawnSubscriptionHandler(ctx context.Context) {
	c.subscriberMu.Lock()
	defer c.subscriberMu.Unlock()
	c.subscriber = NewSubscriber(c.env, c.env, WithMasterContext(ctx))
	es := c.subscriber.Events()
	log.Info("Subscription handler spawned.", zap.String("category", "log backup subscription manager"))

	go func() {
		defer utils.CatchAndLogPanic()
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-es:
				if !ok {
					return
				}
				failpoint.Inject("subscription-handler-loop", func() {})
				c.WithCheckpoints(func(vsf *spans.ValueSortedFull) {
					if vsf == nil {
						log.Warn("Span tree not found, perhaps stale event of removed tasks.",
							zap.String("category", "log backup subscription manager"))
						return
					}
					log.Debug("Accepting region flush event.",
						zap.Stringer("range", logutil.StringifyRange(event.Key)),
						zap.Uint64("checkpoint", event.Value))
					vsf.Merge(event)
				})
			}
		}
	}()
}

func (c *CheckpointAdvancer) subscribeTick(ctx context.Context) error {
	c.subscriberMu.Lock()
	defer c.subscriberMu.Unlock()
	if c.subscriber == nil {
		return nil
	}
	failpoint.Inject("get_subscriber", nil)
	if err := c.subscriber.UpdateStoreTopology(ctx); err != nil {
		log.Warn("Error when updating store topology.",
			zap.String("category", "log backup advancer"), logutil.ShortError(err))
	}
	c.subscriber.HandleErrors(ctx)
	return c.subscriber.PendingErrors()
}

func (c *CheckpointAdvancer) isCheckpointLagged(ctx context.Context) (bool, error) {
	if c.cfg.CheckPointLagLimit <= 0 {
		return false, nil
	}

	now, err := c.env.FetchCurrentTS(ctx)
	if err != nil {
		return false, err
	}

	lagDuration := oracle.GetTimeFromTS(now).Sub(oracle.GetTimeFromTS(c.lastCheckpoint.TS))
	if lagDuration > c.cfg.CheckPointLagLimit {
		log.Warn("checkpoint lag is too large", zap.String("category", "log backup advancer"),
			zap.Stringer("lag", lagDuration))
		return true, nil
	}
	return false, nil
}

func (c *CheckpointAdvancer) importantTick(ctx context.Context) error {
	c.checkpointsMu.Lock()
	c.setCheckpoint(ctx, c.checkpoints.Min())
	c.checkpointsMu.Unlock()
	if err := c.env.UploadV3GlobalCheckpointForTask(ctx, c.task.Name, c.lastCheckpoint.TS); err != nil {
		return errors.Annotate(err, "failed to upload global checkpoint")
	}
	isLagged, err := c.isCheckpointLagged(ctx)
	if err != nil {
		return errors.Annotate(err, "failed to check timestamp")
	}
	if isLagged {
		err := c.env.PauseTask(ctx, c.task.Name)
		if err != nil {
			return errors.Annotate(err, "failed to pause task")
		}
		return errors.Annotate(errors.Errorf("check point lagged too large"), "check point lagged too large")
	}
	p, err := c.env.BlockGCUntil(ctx, c.lastCheckpoint.safeTS())
	if err != nil {
		return errors.Annotatef(err,
			"failed to update service GC safe point, current checkpoint is %d, target checkpoint is %d",
			c.lastCheckpoint.safeTS(), p)
	}
	if p <= c.lastCheckpoint.safeTS() {
		log.Info("updated log backup GC safe point.",
			zap.Uint64("checkpoint", p), zap.Uint64("target", c.lastCheckpoint.safeTS()))
	}
	if p > c.lastCheckpoint.safeTS() {
		log.Warn("update log backup GC safe point failed: stale.",
			zap.Uint64("checkpoint", p), zap.Uint64("target", c.lastCheckpoint.safeTS()))
	}
	return nil
}

func (c *CheckpointAdvancer) optionalTick(cx context.Context) error {
	// lastCheckpoint is not increased too long enough.
	// assume the cluster has expired locks for whatever reasons.
	var targets []spans.Valued
	if c.lastCheckpoint != nil && c.lastCheckpoint.needResolveLocks() && c.inResolvingLock.CompareAndSwap(false, true) {
		c.WithCheckpoints(func(vsf *spans.ValueSortedFull) {
			// when get locks here. assume these locks are not belong to same txn,
			// but these locks' start ts are close to 1 minute. try resolve these locks at one time
			vsf.TraverseValuesLessThan(tsoAfter(c.lastCheckpoint.TS, time.Minute), func(v spans.Valued) bool {
				targets = append(targets, v)
				return true
			})
		})
		if len(targets) != 0 {
			log.Info("Advancer starts to resolve locks", zap.Int("targets", len(targets)))
			// use new context here to avoid timeout
			ctx := context.Background()
			c.asyncResolveLocksForRanges(ctx, targets)
		} else {
			// don't forget set state back
			c.inResolvingLock.Store(false)
		}
	}
	threshold := c.Config().GetDefaultStartPollThreshold()
	if err := c.subscribeTick(cx); err != nil {
		log.Warn("Subscriber meet error, would polling the checkpoint.", zap.String("category", "log backup advancer"),
			logutil.ShortError(err))
		threshold = c.Config().GetSubscriberErrorStartPollThreshold()
	}

	return c.advanceCheckpointBy(cx, func(cx context.Context) (spans.Valued, error) {
		return c.CalculateGlobalCheckpointLight(cx, threshold)
	})
}

func (c *CheckpointAdvancer) tick(ctx context.Context) error {
	c.taskMu.Lock()
	defer c.taskMu.Unlock()
	if c.task == nil || c.isPaused.Load() {
		log.Debug("No tasks yet, skipping advancing.")
		return nil
	}

	var errs error

	cx, cancel := context.WithTimeout(ctx, c.Config().TickTimeout())
	defer cancel()
	err := c.optionalTick(cx)
	if err != nil {
		log.Warn("option tick failed.", zap.String("category", "log backup advancer"), logutil.ShortError(err))
		errs = multierr.Append(errs, err)
	}

	err = c.importantTick(ctx)
	if err != nil {
		log.Warn("important tick failed.", zap.String("category", "log backup advancer"), logutil.ShortError(err))
		errs = multierr.Append(errs, err)
	}

	return errs
}

func (c *CheckpointAdvancer) asyncResolveLocksForRanges(ctx context.Context, targets []spans.Valued) {
	// run in another goroutine
	// do not block main tick here
	go func() {
		failpoint.Inject("AsyncResolveLocks", func() {})
		handler := func(ctx context.Context, r tikvstore.KeyRange) (rangetask.TaskStat, error) {
			// we will scan all locks and try to resolve them by check txn status.
			return tikv.ResolveLocksForRange(
				ctx, c.env, math.MaxUint64, r.StartKey, r.EndKey, tikv.NewGcResolveLockMaxBackoffer, tikv.GCScanLockLimit)
		}
		workerPool := util.NewWorkerPool(uint(config.DefaultMaxConcurrencyAdvance), "advancer resolve locks")
		var wg sync.WaitGroup
		for _, r := range targets {
			targetRange := r
			wg.Add(1)
			workerPool.Apply(func() {
				defer wg.Done()
				// Run resolve lock on the whole TiKV cluster.
				// it will use startKey/endKey to scan region in PD.
				// but regionCache already has a codecPDClient. so just use decode key here.
				// and it almost only include one region here. so set concurrency to 1.
				runner := rangetask.NewRangeTaskRunner("advancer-resolve-locks-runner",
					c.env.GetStore(), 1, handler)
				err := runner.RunOnRange(ctx, targetRange.Key.StartKey, targetRange.Key.EndKey)
				if err != nil {
					// wait for next tick
					log.Warn("resolve locks failed, wait for next tick", zap.String("category", "advancer"),
						zap.String("uuid", "log backup advancer"),
						zap.Error(err))
				}
			})
		}
		wg.Wait()
		log.Info("finish resolve locks for checkpoint", zap.String("category", "advancer"),
			zap.String("uuid", "log backup advancer"),
			logutil.Key("StartKey", c.lastCheckpoint.StartKey),
			logutil.Key("EndKey", c.lastCheckpoint.EndKey),
			zap.Int("targets", len(targets)))
		c.lastCheckpointMu.Lock()
		c.lastCheckpoint.resolveLockTime = time.Now()
		c.lastCheckpointMu.Unlock()
		c.inResolvingLock.Store(false)
	}()
}

func (c *CheckpointAdvancer) TEST_registerCallbackForSubscriptions(f func()) int {
	cnt := 0
	for _, sub := range c.subscriber.subscriptions {
		sub.onDaemonExit = f
		cnt += 1
	}
	return cnt
}
