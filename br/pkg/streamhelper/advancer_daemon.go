// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/streamhelper/config"
	"github.com/pingcap/tidb/br/pkg/streamhelper/spans"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/owner"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/rangetask"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	ownerPrompt = "log-backup"
	ownerPath   = "/tidb/br-stream/owner"
)

func resolveLockTickTime() time.Duration {
	failpoint.Inject("ResolveLockTickTime", func(val failpoint.Value) {
		t := time.Duration(val.(int))
		failpoint.Return(t * time.Second)
	})
	return 5 * time.Second
}

// OnTick advances the inner logic clock for the advancer.
// It's synchronous: this would only return after the events triggered by the clock has all been done.
// It's generally panic-free, you may not need to trying recover a panic here.
func (c *CheckpointAdvancer) OnTick(ctx context.Context) (err error) {
	defer c.recordTimeCost("tick")()
	defer utils.PanicToErr(&err)
	return c.tick(ctx)
}

// OnStart implements daemon.Interface, which will be called when log backup service starts.
func (c *CheckpointAdvancer) OnStart(ctx context.Context) {
	c.StartTaskListener(ctx)
}

// OnBecomeOwner implements daemon.Interface. If the tidb-server become owner, this function will be called.
func (c *CheckpointAdvancer) OnBecomeOwner(ctx context.Context) {
	metrics.AdvancerOwner.Set(1.0)
	c.spawnSubscriptionHandler(ctx)
	go func() {
		t := resolveLockTickTime()
		tick := time.NewTicker(t)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				// no longger be an owner. return it.
				return
			case <-tick.C:
				{
					// lastCheckpoint is not increased too long enough.
					// assume the cluster has expired locks for whatever reasons.
					if c.lastCheckpoint.needResolveLocks() {
						var targets []spans.Valued
						c.WithCheckpoints(func(vsf *spans.ValueSortedFull) {
							// when get locks here. assume these locks are not belong to same txn,
							// but startTS close to 1 minute. try resolve these locks at one time
							vsf.TraverseValuesLessThan(tsoAfter(c.lastCheckpoint.TS, time.Minute), func(v spans.Valued) bool {
								targets = append(targets, v)
								return true
							})
						})
						handler := func(ctx context.Context, r tikvstore.KeyRange) (rangetask.TaskStat, error) {
							// we will scan all locks and try to resolve them by check txn status.
							return tikv.ResolveLocksForRange(ctx, "log backup advancer", c.env, math.MaxUint64, r.StartKey, r.EndKey)
						}
						workerPool := utils.NewWorkerPool(uint(config.DefaultMaxConcurrencyAdvance), "advancer resolve locks")
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
									log.Error("resolve locks failed", zap.String("category", "advancer"),
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
						//c.lastCheckpoint.resolveLockTime = time.Now()
					}
				}
			}
		}
	}()
	go func() {
		<-ctx.Done()
		c.onStop()
	}()
}

// Name implements daemon.Interface.
func (c *CheckpointAdvancer) Name() string {
	return "LogBackup::Advancer"
}

func (c *CheckpointAdvancer) onStop() {
	metrics.AdvancerOwner.Set(0.0)
	c.stopSubscriber()
}

func OwnerManagerForLogBackup(ctx context.Context, etcdCli *clientv3.Client) owner.Manager {
	id := uuid.New()
	return owner.NewOwnerManager(ctx, etcdCli, ownerPrompt, id.String(), ownerPath)
}
