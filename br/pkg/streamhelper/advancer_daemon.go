// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/owner"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	ownerPrompt = "log-backup"
	ownerPath   = "/tidb/br-stream/owner"
)

// AdvancerDaemon is a "high-avalibility" version of advancer.
// It involved the manager for electing a owner and doing things.
type AdvancerDaemon struct {
	adv     *CheckpointAdvancer
	manager owner.Manager
}

func NewAdvancerDaemon(adv *CheckpointAdvancer, manager owner.Manager) *AdvancerDaemon {
	return &AdvancerDaemon{
		adv:     adv,
		manager: manager,
	}
}

func OwnerManagerForLogBackup(ctx context.Context, etcdCli *clientv3.Client) owner.Manager {
	id := uuid.New()
	return owner.NewOwnerManager(ctx, etcdCli, ownerPrompt, id.String(), ownerPath)
}

// Begin starts the daemon.
func (ad *AdvancerDaemon) Begin(ctx context.Context) (func(), error) {
	log.Info("begin advancer daemon", zap.String("id", ad.manager.ID()))
	if err := ad.manager.CampaignOwner(); err != nil {
		return nil, err
	}

	ad.adv.StartTaskListener(ctx)
	tick := time.NewTicker(ad.adv.cfg.TickDuration)
	loop := func() {
		log.Info("begin advancer daemon loop", zap.String("id", ad.manager.ID()))
		for {
			select {
			case <-ctx.Done():
				log.Info("advancer loop exits", zap.String("id", ad.manager.ID()))
				return
			case <-tick.C:
				if ad.manager.IsOwner() {
					metrics.AdvancerOwner.Set(1.0)
					if err := ad.adv.OnTick(ctx); err != nil {
						log.Warn("failed on tick", logutil.ShortError(err))
					}
				} else {
					metrics.AdvancerOwner.Set(0.0)
				}
			}
		}
	}
	return loop, nil
}
