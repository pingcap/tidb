// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper

import (
	"context"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/streamhelper/config"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/gcutil"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/txnkv/rangetask"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	ownerPrompt = "log-backup"
	ownerPath   = "/tidb/br-stream/owner"
)

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
		tick := time.NewTicker(30 * time.Second)
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
						handler := func(ctx context.Context, r tikvstore.KeyRange) (rangetask.TaskStat, error) {
							// we will scan all locks and try to resolve them by check txn status.
							return gcutil.ResolveLocksForRange(ctx, "log backup advancer", c.env, math.MaxUint64, r.StartKey, r.EndKey)
						}
						runner := rangetask.NewRangeTaskRunner("advancer-resolve-locks-runner",
							c.env.GetStore(), config.DefaultMaxConcurrencyAdvance, handler)
						// Run resolve lock on the whole TiKV cluster.
						// it will use startKey/endKey to scan region in PD. so we need encode key here.
						encodedStartKey := codec.EncodeBytes([]byte{}, c.lastCheckpoint.StartKey)
						encodedEndKey := codec.EncodeBytes([]byte{}, c.lastCheckpoint.EndKey)
						err := runner.RunOnRange(ctx, encodedStartKey, encodedEndKey)
						if err != nil {
							// wait for next tick
							log.Error("resolve locks failed", zap.String("category", "advancer"),
								zap.String("uuid", "log backup advancer"),
								zap.Error(err))
						}
						log.Info("finish resolve locks", zap.String("category", "advancer"),
							zap.String("uuid", "log backup advancer"),
							zap.Int("regions", runner.CompletedRegions()))
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
