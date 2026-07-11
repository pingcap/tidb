// Copyright 2026 PingCAP, Inc.
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

package operator

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/conn"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/stream/crr/service"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
)

type cleanupFunc func()

const etcdGRPCBackOffMaxDelay = 3 * time.Second

// NewCRRCheckpointService creates the CRR checkpoint service and its dependent clients.
func NewCRRCheckpointService(
	ctx context.Context,
	g glue.Glue,
	cfg CRRCheckpointConfig,
) (*service.Service, cleanupFunc, error) {
	_, upstreamStorage, err := task.GetStorage(ctx, cfg.UpstreamStorage, &cfg.Config)
	if err != nil {
		return nil, nil, err
	}
	if err := checkCRRExternalStorage(ctx, upstreamStorage, "upstream"); err != nil {
		upstreamStorage.Close()
		return nil, nil, err
	}
	_, downstreamStorage, err := task.GetStorage(ctx, cfg.DownstreamStorage, &cfg.Config)
	if err != nil {
		upstreamStorage.Close()
		return nil, nil, err
	}
	if err := checkCRRExternalStorage(ctx, downstreamStorage, "downstream"); err != nil {
		log.Warn("failed to check the downstream storage", zap.Error(err))
	}

	mgr, err := task.NewMgr(
		ctx,
		g,
		cfg.KeyspaceName,
		cfg.PD,
		cfg.TLS,
		task.GetKeepalive(&cfg.Config),
		cfg.CheckRequirements,
		false,
		conn.StreamVersionChecker,
	)
	if err != nil {
		upstreamStorage.Close()
		downstreamStorage.Close()
		return nil, nil, err
	}

	etcdCli, err := dialEtcdWithCfg(ctx, cfg.Config)
	if err != nil {
		upstreamStorage.Close()
		downstreamStorage.Close()
		mgr.Close()
		return nil, nil, err
	}

	env := streamhelper.CliEnv(mgr.StoreManager, mgr.GetStore(), etcdCli)
	syncChecker, err := buildObjectSyncChecker(
		upstreamStorage,
		downstreamStorage,
		cfg.CheckSyncedFromDownstreamStorage,
	)
	if err != nil {
		downstreamStorage.Close()
		upstreamStorage.Close()
		closeEtcdClient(etcdCli)
		mgr.Close()
		return nil, nil, err
	}
	stateStore := buildResumeStateStore(downstreamStorage)
	svc, err := service.New(
		service.Deps{
			PD:       env,
			Watcher:  streamhelper.NewMetaDataClient(etcdCli),
			Upstream: upstreamStorage,
			Sync:     syncChecker,
			State:    stateStore,
		},
		service.Config{
			CalculatorConfig: service.CalculatorConfig{
				TaskName:            cfg.CRRConfig.TaskName,
				PollInterval:        cfg.CRRConfig.PollInterval,
				MetaReadConcurrency: cfg.CRRConfig.MetaReadConcurrency,
			},
			RetryInterval: cfg.CRRConfig.RetryInterval,
		},
	)
	if err != nil {
		downstreamStorage.Close()
		upstreamStorage.Close()
		closeEtcdClient(etcdCli)
		mgr.Close()
		return nil, nil, err
	}

	cleanup := func() {
		downstreamStorage.Close()
		upstreamStorage.Close()
		closeEtcdClient(etcdCli)
		mgr.Close()
	}
	return svc, cleanup, nil
}

func checkCRRExternalStorage(ctx context.Context, storage storeapi.Storage, source string) error {
	exists, err := storage.FileExists(ctx, metautil.LockFile)
	if err != nil {
		return errors.Annotatef(err, "error occurred when checking %s file in %s storage", metautil.LockFile, source)
	}
	if !exists {
		return errors.Annotatef(berrors.ErrInvalidArgument,
			"%s storage %s is not a log backup directory because %s does not exist",
			source, storage.URI(), metautil.LockFile,
		)
	}
	return nil
}

func buildObjectSyncChecker(
	upstreamStorage storeapi.Storage,
	downstreamStorage storeapi.Storage,
	checkSyncedFromDownstreamStorage bool,
) (service.ObjectSyncChecker, error) {
	if checkSyncedFromDownstreamStorage && downstreamStorage != nil {
		return service.NewExistenceSyncChecker(downstreamStorage), nil
	}
	if upstreamChecker, ok := upstreamStorage.(service.ObjectSyncChecker); ok {
		return upstreamChecker, nil
	}
	return nil, fmt.Errorf(
		"upstream storage cannot check object sync; to confirm replication by downstream storage existence, enable --check-synced-from-downstream-storage",
	)
}

type storageResumeStateStore struct {
	storage storeapi.Storage
	path    string
}

func buildResumeStateStore(
	storage storeapi.Storage,
) service.ResumeStateStore {
	return &storageResumeStateStore{
		storage: storage,
		path:    service.GetStatusFileName(),
	}
}

func (s *storageResumeStateStore) LoadState(ctx context.Context) (*service.PersistentState, error) {
	exists, err := s.storage.FileExists(ctx, s.path)
	if err != nil {
		return nil, fmt.Errorf("check persisted resume state %s: %w", s.path, err)
	}
	if !exists {
		return nil, nil
	}

	payload, err := s.storage.ReadFile(ctx, s.path)
	if err != nil {
		return nil, fmt.Errorf("read persisted resume state %s: %w", s.path, err)
	}

	var state service.PersistentState
	if err := json.Unmarshal(payload, &state); err != nil {
		return nil, fmt.Errorf("decode persisted resume state %s: %w", s.path, err)
	}
	return &state, nil
}

func (s *storageResumeStateStore) SaveState(ctx context.Context, state service.PersistentState) error {
	payload, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode persisted resume state %s: %w", s.path, err)
	}
	if err := s.storage.WriteFile(ctx, s.path, payload); err != nil {
		return fmt.Errorf("write persisted resume state %s: %w", s.path, err)
	}
	return nil
}

func closeEtcdClient(etcdCli *clientv3.Client) {
	if closeErr := etcdCli.Close(); closeErr != nil {
		log.Warn("failed to close etcd client", zap.Error(closeErr))
	}
}

func etcdGRPCBackoffConfig() backoff.Config {
	backoffCfg := backoff.DefaultConfig
	backoffCfg.MaxDelay = etcdGRPCBackOffMaxDelay
	return backoffCfg
}

func etcdKeepaliveParams(cfg task.Config) keepalive.ClientParameters {
	return keepalive.ClientParameters{
		Time:                cfg.GRPCKeepaliveTime,
		Timeout:             cfg.GRPCKeepaliveTimeout,
		PermitWithoutStream: true,
	}
}

func newEtcdClientConfig(ctx context.Context, cfg task.Config) (clientv3.Config, error) {
	var (
		tlsConfig *tls.Config
		err       error
	)

	if cfg.TLS.IsEnabled() {
		tlsConfig, err = cfg.TLS.ToTLSConfig()
		if err != nil {
			var empty clientv3.Config
			return empty, errors.Trace(err)
		}
	}

	return clientv3.Config{
		TLS:              tlsConfig,
		Endpoints:        cfg.PD,
		AutoSyncInterval: 30 * time.Second,
		DialTimeout:      5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: etcdGRPCBackoffConfig(),
			}),
			grpc.WithKeepaliveParams(etcdKeepaliveParams(cfg)),
			grpc.WithBlock(),
			grpc.WithReturnConnectionError(),
		},
		Context: ctx,
	}, nil
}

func dialEtcdWithCfg(ctx context.Context, cfg task.Config) (*clientv3.Client, error) {
	etcdCfg, err := newEtcdClientConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	etcdCli, err := clientv3.New(etcdCfg)
	if err != nil {
		return nil, err
	}
	return etcdCli, nil
}
