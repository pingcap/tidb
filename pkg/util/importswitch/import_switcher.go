// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importswitch

import (
	"context"
	"crypto/tls"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/log"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/engine"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
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

	mu     sync.Mutex
	cancel context.CancelFunc // Manages goroutine lifecycle
	wg     sync.WaitGroup
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
	}
}

// switchToNormalMode switch tikv cluster to normal mode.
func (switcher *ImportModeSwitcher) SwitchToNormalMode(ctx context.Context) error {
	switcher.mu.Lock()
	defer switcher.mu.Unlock()

	if switcher.cancel == nil {
		log.Info("TiKV is already in normal mode")
		return nil
	}
	log.Info("Stopping the import mode goroutine")
	switcher.cancel()
	switcher.cancel = nil
	// wait for switch goroutine exits
	switcher.wg.Wait()
	return switcher.switchTiKVMode(ctx, import_sstpb.SwitchMode_Normal)
}

func (switcher *ImportModeSwitcher) switchTiKVMode(
	ctx context.Context,
	mode import_sstpb.SwitchMode,
) error {
	stores, err := switcher.pdClient.GetAllStores(ctx, opt.WithExcludeTombstone())
	if err != nil {
		return err
	}

	j := 0
	for _, store := range stores {
		if engine.IsTiFlash(store) {
			continue
		}
		stores[j] = store
		j++
	}
	stores = stores[:j]

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

// GoSwitchToImportMode switch tikv cluster to import mode.
func (switcher *ImportModeSwitcher) GoSwitchToImportMode(
	ctx context.Context,
) error {
	switcher.mu.Lock()
	defer switcher.mu.Unlock()

	if switcher.cancel != nil {
		log.Info("TiKV is already in import mode")
		return nil
	}

	// Create a new context for the goroutine
	ctx, cancel := context.WithCancel(ctx)
	switcher.cancel = cancel

	// [important!] switch tikv mode into import at the beginning
	log.Info("switch to import mode at beginning")
	err := switcher.switchTiKVMode(ctx, import_sstpb.SwitchMode_Import)
	if err != nil {
		log.Warn("switch to import mode failed", zap.Error(err))
		return errors.Trace(err)
	}
	switcher.wg.Add(1)
	// tikv automatically switch to normal mode in every 10 minutes
	// so we need ping tikv in less than 10 minute
	go func() {
		tick := time.NewTicker(switcher.switchModeInterval)
		defer func() {
			switcher.wg.Done()
			tick.Stop()
		}()

		for {
			select {
			case <-ctx.Done():
				log.Info("stop automatic switch to import mode when context done")
				return
			case <-tick.C:
				log.Info("switch to import mode")
				err := switcher.switchTiKVMode(ctx, import_sstpb.SwitchMode_Import)
				if err != nil {
					log.Warn("switch to import mode failed", zap.Error(err))
				}
			}
		}
	}()
	return nil
}
