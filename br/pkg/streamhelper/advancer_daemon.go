// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper

import (
	"context"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/owner"
	clientv3 "go.etcd.io/etcd/client/v3"
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

// OnStart implements daemon.Interface.
func (c *CheckpointAdvancer) OnStart(ctx context.Context) {
	metrics.AdvancerOwner.Set(1.0)
	c.StartTaskListener(ctx)
	c.spawnSubscriptionHandler(ctx)
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
