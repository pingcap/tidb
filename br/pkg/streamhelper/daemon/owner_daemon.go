// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package daemon

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/owner"
	"go.uber.org/zap"
)

// OwnerDaemon is a wrapper for running "daemon" in the TiDB cluster.
// Generally, it uses the etcd election API (wrapped in the `owner.Manager` interface),
// and shares nothing between nodes.
// Please make sure the daemon is "stateless" (i.e. it doesn't depend on the local storage or memory state.)
// This struct is "synchronous" (which means there are no race accessing of these variables.).
type OwnerDaemon struct {
	daemon       Interface
	manager      owner.Manager
	tickInterval time.Duration

	// When not `nil`, implies the daemon is running.
	cancel context.CancelFunc
}

// New creates a new owner daemon.
func New(daemon Interface, manager owner.Manager, tickInterval time.Duration) *OwnerDaemon {
	return &OwnerDaemon{
		daemon:       daemon,
		manager:      manager,
		tickInterval: tickInterval,
	}
}

// Running tests whether the daemon is running (i.e. is it the owner?)
func (od *OwnerDaemon) Running() bool {
	return od.cancel != nil
}

func (od *OwnerDaemon) cancelRun() {
	if od.Running() {
		log.Info("cancel running daemon", zap.String("daemon", od.daemon.Name()))
		od.cancel()
		od.cancel = nil
	}
}

func (od *OwnerDaemon) ownerTick(ctx context.Context) {
	// If not running, switching to running.
	if !od.Running() {
		cx, cancel := context.WithCancel(ctx)
		od.cancel = cancel
		log.Info("daemon became owner", zap.String("id", od.manager.ID()), zap.String("daemon-id", od.daemon.Name()))
		// Note: maybe save the context so we can cancel the tick when we are not owner?
		od.daemon.OnStart(cx)
	}

	// Tick anyway.
	if err := od.daemon.OnTick(ctx); err != nil {
		log.Warn("failed on tick", logutil.ShortError(err))
	}
}

// Begin starts the daemon.
// It would do some bootstrap task, and return a closure that would begin the main loop.
func (od *OwnerDaemon) Begin(ctx context.Context) (func(), error) {
	log.Info("begin advancer daemon", zap.String("daemon-id", od.daemon.Name()))
	if err := od.manager.CampaignOwner(); err != nil {
		return nil, err
	}

	tick := time.NewTicker(od.tickInterval)
	loop := func() {
		log.Info("begin running daemon", zap.String("id", od.manager.ID()), zap.String("daemon-id", od.daemon.Name()))
		for {
			select {
			case <-ctx.Done():
				log.Info("daemon loop exits", zap.String("id", od.manager.ID()), zap.String("daemon-id", od.daemon.Name()))
				return
			case <-tick.C:
				log.Debug("daemon tick start", zap.Bool("is-owner", od.manager.IsOwner()), zap.String("daemon-id", od.daemon.Name()))
				if od.manager.IsOwner() {
					od.ownerTick(ctx)
				} else {
					od.cancelRun()
				}
			}
		}
	}
	return loop, nil
}
