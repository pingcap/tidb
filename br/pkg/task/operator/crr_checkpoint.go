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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/stream/crr/service"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/br/pkg/task"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type cleanupFunc func()

// NewCRRCheckpointService creates the CRR checkpoint service and its dependent clients.
func NewCRRCheckpointService(
	ctx context.Context,
	g glue.Glue,
	cfg CRRCheckpointConfig,
) (*service.Service, cleanupFunc, error) {
	mgr, err := task.NewMgr(
		ctx,
		g,
		cfg.PD,
		cfg.TLS,
		task.GetKeepalive(&cfg.Config),
		cfg.CheckRequirements,
		false,
		conn.StreamVersionChecker,
	)
	if err != nil {
		return nil, nil, err
	}

	etcdCli, err := dialEtcdWithCfg(ctx, cfg.Config)
	if err != nil {
		mgr.Close()
		return nil, nil, err
	}

	_, upstreamStorage, err := task.GetStorage(ctx, cfg.UpstreamStorage, &cfg.Config)
	if err != nil {
		closeEtcdClient(etcdCli)
		mgr.Close()
		return nil, nil, err
	}

	_, downstreamStorage, err := task.GetStorage(ctx, cfg.DownstreamStorage, &cfg.Config)
	if err != nil {
		upstreamStorage.Close()
		closeEtcdClient(etcdCli)
		mgr.Close()
		return nil, nil, err
	}

	env := streamhelper.CliEnv(mgr.StoreManager, mgr.GetStore(), etcdCli)
	svc, err := service.New(
		service.Deps{
			PD:         env,
			Watcher:    streamhelper.NewMetaDataClient(etcdCli),
			Upstream:   upstreamStorage,
			Downstream: downstreamStorage,
		},
		service.Config{
			CalculatorConfig: service.CalculatorConfig{
				TaskName: cfg.TaskName,
			},
			RetryInterval: 0,
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

func closeEtcdClient(etcdCli *clientv3.Client) {
	if closeErr := etcdCli.Close(); closeErr != nil {
		log.Warn("failed to close etcd client", zap.Error(closeErr))
	}
}

func dialEtcdWithCfg(ctx context.Context, cfg task.Config) (*clientv3.Client, error) {
	var (
		tlsConfig *tls.Config
		err       error
	)

	if cfg.TLS.IsEnabled() {
		tlsConfig, err = cfg.TLS.ToTLSConfig()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	etcdCli, err := clientv3.New(clientv3.Config{
		TLS:              tlsConfig,
		Endpoints:        cfg.PD,
		AutoSyncInterval: 30 * time.Second,
		DialTimeout:      5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                cfg.GRPCKeepaliveTime,
				Timeout:             cfg.GRPCKeepaliveTimeout,
				PermitWithoutStream: false,
			}),
			grpc.WithBlock(),
			grpc.WithReturnConnectionError(),
		},
		Context: ctx,
	})
	if err != nil {
		return nil, err
	}
	return etcdCli, nil
}
