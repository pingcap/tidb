// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package operator

import (
	"context"
	"crypto/tls"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/br/pkg/utils"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/keepalive"
)

func dialPD(ctx context.Context, cfg *task.Config) (*pdutil.PdController, error) {
	pdAddrs := strings.Join(cfg.PD, ",")
	var tc *tls.Config
	if cfg.TLS.IsEnabled() {
		var err error
		tc, err = cfg.TLS.ToTLSConfig()
		if err != nil {
			return nil, err
		}
	}
	mgr, err := pdutil.NewPdController(ctx, pdAddrs, tc, cfg.TLS.ToPDSecurityOption())
	if err != nil {
		return nil, err
	}
	return mgr, nil
}

func (cx *AdaptEnvForSnapshotBackupContext) cleanUpWith(f func(ctx context.Context)) {
	_ = cx.cleanUpWithErr(func(ctx context.Context) error { f(ctx); return nil })
}

func (cx *AdaptEnvForSnapshotBackupContext) cleanUpWithErr(f func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), cx.cfg.TTL)
	defer cancel()
	return f(ctx)
}

type AdaptEnvForSnapshotBackupContext struct {
	context.Context

	pdMgr *pdutil.PdController
	kvMgr *utils.StoreManager
	cfg   PauseGcConfig

	rdGrp  sync.WaitGroup
	runGrp *errgroup.Group
}

func (cx *AdaptEnvForSnapshotBackupContext) ReadyL(name string, notes ...zap.Field) {
	logutil.CL(cx).Info("Stage ready.", append(notes, zap.String("component", name))...)
	cx.rdGrp.Done()
}

func hintAllReady() {
	// Hacking: some version of operators using the follow two logs to check whether we are ready...
	log.Info("Schedulers are paused.")
	log.Info("GC is paused.")
	log.Info("All ready.")
}

// AdaptEnvForSnapshotBackup blocks the current goroutine and pause the GC safepoint and remove the scheduler by the config.
// This function will block until the context being canceled.
func AdaptEnvForSnapshotBackup(ctx context.Context, cfg *PauseGcConfig) error {
	mgr, err := dialPD(ctx, &cfg.Config)
	if err != nil {
		return errors.Annotate(err, "failed to dial PD")
	}
	var tconf *tls.Config
	if cfg.TLS.IsEnabled() {
		tconf, err = cfg.TLS.ToTLSConfig()
		if err != nil {
			return errors.Annotate(err, "invalid tls config")
		}
	}
	kvMgr := utils.NewStoreManager(mgr.GetPDClient(), keepalive.ClientParameters{
		Time:    cfg.Config.GRPCKeepaliveTime,
		Timeout: cfg.Config.GRPCKeepaliveTimeout,
	}, tconf)
	eg, ectx := errgroup.WithContext(ctx)
	cx := &AdaptEnvForSnapshotBackupContext{
		Context: logutil.ContextWithField(ectx, zap.String("tag", "br_operator")),
		pdMgr:   mgr,
		kvMgr:   kvMgr,
		cfg:     *cfg,
		rdGrp:   sync.WaitGroup{},
		runGrp:  eg,
	}
	cx.rdGrp.Add(3)

<<<<<<< HEAD
	eg.Go(func() error { return pauseGCKeeper(cx) })
	eg.Go(func() error { return pauseSchedulerKeeper(cx) })
	eg.Go(func() error { return pauseImporting(cx) })
=======
	initChan := make(chan struct{})
	cx.run(func() error { return pauseGCKeeper(cx) })
	cx.run(func() error {
		log.Info("Pause scheduler waiting all connections established.")
		<-initChan
		log.Info("Pause scheduler noticed connections established.")
		return pauseSchedulerKeeper(cx)
	})
	cx.run(func() error { return pauseAdminAndWaitApply(cx, initChan) })
>>>>>>> 411e945da33 (operator: pause scheduler after all connections established (#51823))
	go func() {
		cx.rdGrp.Wait()
		hintAllReady()
	}()

	return eg.Wait()
}

<<<<<<< HEAD
func pauseImporting(cx *AdaptEnvForSnapshotBackupContext) error {
	denyLightning := utils.NewSuspendImporting("prepare_for_snapshot_backup", cx.kvMgr)
	if _, err := denyLightning.DenyAllStores(cx, cx.cfg.TTL); err != nil {
		return errors.Trace(err)
	}
	cx.ReadyL("pause_lightning")
	cx.runGrp.Go(func() error {
		err := denyLightning.Keeper(cx, cx.cfg.TTL)
		if errors.Cause(err) != context.Canceled {
			logutil.CL(cx).Warn("keeper encounters error.", logutil.ShortError(err))
=======
func pauseAdminAndWaitApply(cx *AdaptEnvForSnapshotBackupContext, afterConnectionsEstablished chan<- struct{}) error {
	env := preparesnap.CliEnv{
		Cache: tikv.NewRegionCache(cx.pdMgr.GetPDClient()),
		Mgr:   cx.kvMgr,
	}
	defer env.Cache.Close()
	retryEnv := preparesnap.RetryAndSplitRequestEnv{Env: env}
	begin := time.Now()
	prep := preparesnap.New(retryEnv)
	prep.LeaseDuration = cx.cfg.TTL
	prep.AfterConnectionsEstablished = func() {
		log.Info("All connections are stablished.")
		close(afterConnectionsEstablished)
	}

	defer cx.cleanUpWith(func(ctx context.Context) {
		if err := prep.Finalize(ctx); err != nil {
			logutil.CL(ctx).Warn("failed to finalize the prepare stream", logutil.ShortError(err))
>>>>>>> 411e945da33 (operator: pause scheduler after all connections established (#51823))
		}
		return cx.cleanUpWithErr(func(ctx context.Context) error {
			for {
				if ctx.Err() != nil {
					return errors.Annotate(ctx.Err(), "cleaning up timed out")
				}
				res, err := denyLightning.AllowAllStores(ctx)
				if err != nil {
					logutil.CL(ctx).Warn("Failed to restore lightning, will retry.", logutil.ShortError(err))
					// Retry for 10 times.
					time.Sleep(cx.cfg.TTL / 10)
					continue
				}
				return denyLightning.ConsistentWithPrev(res)
			}
		})
	})
	return nil
}

func pauseGCKeeper(ctx *AdaptEnvForSnapshotBackupContext) error {
	// Note: should we remove the service safepoint as soon as this exits?
	sp := utils.BRServiceSafePoint{
		ID:       utils.MakeSafePointID(),
		TTL:      int64(ctx.cfg.TTL.Seconds()),
		BackupTS: ctx.cfg.SafePoint,
	}
	if sp.BackupTS == 0 {
		rts, err := ctx.pdMgr.GetMinResolvedTS(ctx)
		if err != nil {
			return err
		}
		logutil.CL(ctx).Info("No service safepoint provided, using the minimal resolved TS.", zap.Uint64("min-resolved-ts", rts))
		sp.BackupTS = rts
	}
	err := utils.StartServiceSafePointKeeper(ctx, ctx.pdMgr.GetPDClient(), sp)
	if err != nil {
		return err
	}
	ctx.ReadyL("pause_gc", zap.Object("safepoint", sp))
	// Note: in fact we can directly return here.
	// But the name `keeper` implies once the function exits,
	// the GC should be resume, so let's block here.
	<-ctx.Done()
	return nil
}

func pauseSchedulerKeeper(ctx *AdaptEnvForSnapshotBackupContext) error {
	undo, err := ctx.pdMgr.RemoveAllPDSchedulers(ctx)
	if undo != nil {
		defer ctx.cleanUpWith(func(ctx context.Context) {
			if err := undo(ctx); err != nil {
				log.Warn("failed to restore pd scheduler.", logutil.ShortError(err))
			}
		})
	}
	if err != nil {
		return err
	}
	ctx.ReadyL("pause_scheduler")
	// Wait until the context canceled.
	// So we can properly do the clean up work.
	<-ctx.Done()
	return nil
}
