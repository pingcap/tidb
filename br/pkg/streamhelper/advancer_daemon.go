// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package streamhelper

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/owner"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	ownerPrompt = "log-backup"
	ownerPath   = "/tidb/br-stream/owner"
)

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
	if err := ad.manager.CampaignOwner(); err != nil {
		return nil, err
	}

	ad.adv.StartTaskListener(ctx)
	tick := time.NewTicker(ad.adv.cfg.TickDuration)
	loop := func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				if ad.manager.IsOwner() {
					if err := ad.adv.OnTick(ctx); err != nil {
						log.Warn("failed on tick", logutil.ShortError(err))
					}
				}
			}
		}
	}
	return loop, nil
}
