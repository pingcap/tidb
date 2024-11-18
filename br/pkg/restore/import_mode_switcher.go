// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"context"
	"crypto/tls"
	"time"

	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/conn/util"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type ImportModeSwitcher struct {
	pdClient pd.Client

	switchModeInterval time.Duration
	tlsConf            *tls.Config

	switchCh chan struct{}
}

func NewImportModeSwitcher(
	pdClient pd.Client,
	switchModeInterval time.Duration,
	tlsConf *tls.Config,
) *ImportModeSwitcher {
	return &ImportModeSwitcher{
		pdClient:           pdClient,
		switchModeInterval: switchModeInterval,
		tlsConf:            tlsConf,
		switchCh:           make(chan struct{}),
	}
}

// switchToNormalMode switch tikv cluster to normal mode.
func (switcher *ImportModeSwitcher) switchToNormalMode(ctx context.Context) error {
	close(switcher.switchCh)
	return switcher.switchTiKVMode(ctx, import_sstpb.SwitchMode_Normal)
}

func (switcher *ImportModeSwitcher) switchTiKVMode(
	ctx context.Context,
	mode import_sstpb.SwitchMode,
) error {
	stores, err := util.GetAllTiKVStores(ctx, switcher.pdClient, util.SkipTiFlash)
	if err != nil {
		return errors.Trace(err)
	}
	bfConf := backoff.DefaultConfig
	bfConf.MaxDelay = time.Second * 3

	workerPool := tidbutil.NewWorkerPool(uint(len(stores)), "switch import mode")
	eg, ectx := errgroup.WithContext(ctx)
	for _, store := range stores {
		if err := ectx.Err(); err != nil {
			return errors.Trace(err)
		}

		finalStore := store
		workerPool.ApplyOnErrorGroup(eg,
			func() error {
				opt := grpc.WithTransportCredentials(insecure.NewCredentials())
				if switcher.tlsConf != nil {
					opt = grpc.WithTransportCredentials(credentials.NewTLS(switcher.tlsConf))
				}
				gctx, cancel := context.WithTimeout(ectx, time.Second*5)
				connection, err := grpc.DialContext(
					gctx,
					finalStore.GetAddress(),
					opt,
					grpc.WithBlock(),
					grpc.FailOnNonTempDialError(true),
					grpc.WithConnectParams(grpc.ConnectParams{Backoff: bfConf}),
					// we don't need to set keepalive timeout here, because the connection lives
					// at most 5s. (shorter than minimal value for keepalive time!)
				)
				cancel()
				if err != nil {
					return errors.Trace(err)
				}
				client := import_sstpb.NewImportSSTClient(connection)
				_, err = client.SwitchMode(ctx, &import_sstpb.SwitchModeRequest{
					Mode: mode,
				})
				if err != nil {
					return errors.Trace(err)
				}
				err = connection.Close()
				if err != nil {
					log.Error("close grpc connection failed in switch mode", zap.Error(err))
				}
				return nil
			})
	}

	if err = eg.Wait(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// switchToImportMode switch tikv cluster to import mode.
func (switcher *ImportModeSwitcher) switchToImportMode(
	ctx context.Context,
) {
	// tikv automatically switch to normal mode in every 10 minutes
	// so we need ping tikv in less than 10 minute
	go func() {
		tick := time.NewTicker(switcher.switchModeInterval)
		defer tick.Stop()

		// [important!] switch tikv mode into import at the beginning
		log.Info("switch to import mode at beginning")
		err := switcher.switchTiKVMode(ctx, import_sstpb.SwitchMode_Import)
		if err != nil {
			log.Warn("switch to import mode failed", zap.Error(err))
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				log.Info("switch to import mode")
				err := switcher.switchTiKVMode(ctx, import_sstpb.SwitchMode_Import)
				if err != nil {
					log.Warn("switch to import mode failed", zap.Error(err))
				}
			case <-switcher.switchCh:
				log.Info("stop automatic switch to import mode")
				return
			}
		}
	}()
}

// RestorePreWork executes some prepare work before restore.
// TODO make this function returns a restore post work.
func RestorePreWork(
	ctx context.Context,
	mgr *conn.Mgr,
	switcher *ImportModeSwitcher,
	isOnline bool,
	switchToImport bool,
) (pdutil.UndoFunc, *pdutil.ClusterConfig, error) {
	if isOnline {
		return pdutil.Nop, nil, nil
	}

	if switchToImport {
		// Switch TiKV cluster to import mode (adjust rocksdb configuration).
		switcher.switchToImportMode(ctx)
	}

	return mgr.RemoveSchedulersWithConfig(ctx)
}

// RestorePostWork executes some post work after restore.
// TODO: aggregate all lifetime manage methods into batcher's context manager field.
func RestorePostWork(
	ctx context.Context,
	switcher *ImportModeSwitcher,
	restoreSchedulers pdutil.UndoFunc,
	isOnline bool,
) {
	if isOnline {
		return
	}

	if ctx.Err() != nil {
		log.Warn("context canceled, try shutdown")
		ctx = context.Background()
	}

	if err := switcher.switchToNormalMode(ctx); err != nil {
		log.Warn("fail to switch to normal mode", zap.Error(err))
	}
	if err := restoreSchedulers(ctx); err != nil {
		log.Warn("failed to restore PD schedulers", zap.Error(err))
	}
}
