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

package extworkload

import (
	"context"
	"crypto/tls"
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/extworkload/client"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// Worker type identifiers used in metric labels.
const (
	WorkerTypeGCV2        = "gcv2"
	WorkerTypeTTL         = "ttl"
	WorkerTypeAutoAnalyze = "auto-analyze"
)

// defGCLifeTimeSec is the gc_life_time (seconds) seeded to the controller
// at InitializeGCV2 time, before the local gc_life_time variable is loaded.
const defGCLifeTimeSec = 600

// dialTimeout bounds the synchronous dial + Ping during InitManager.
const dialTimeout = 30 * time.Second

var _ Manager = (*manager)(nil)

type manager struct {
	cli  client.Client
	role string
	meta *keyspacepb.KeyspaceMeta
}

// InitManager builds and installs the global Manager for this TiDB process.
//
// It is a no-op outside Starter deploy mode, when the [external-workload] section
// is disabled, or when a Manager is already installed. Errors from dialing
// the controller propagate to the caller; main.go decides whether they are
// fatal for the local role.
func InitManager(ctx context.Context, keyspaceMeta *keyspacepb.KeyspaceMeta, cfg config.ExternalWorkload) error {
	if !deploymode.IsStarter() || !cfg.Enable {
		return nil
	}
	if globalManager != nil {
		return nil
	}
	if keyspaceMeta == nil {
		return errors.New("external workload controller requires a non-nil keyspace meta")
	}

	cli, err := dialClient(ctx, keyspaceMeta, cfg)
	if err != nil {
		return errors.Annotate(err, "init external workload client")
	}
	globalManager = &manager{cli: cli, role: cfg.Role, meta: keyspaceMeta}
	logutil.BgLogger().Info("external workload manager installed",
		zap.String("role", cfg.Role),
		zap.String("keyspace", keyspaceMeta.GetName()),
		zap.Uint32("keyspace-id", keyspaceMeta.GetId()))
	return nil
}

func dialClient(ctx context.Context, keyspaceMeta *keyspacepb.KeyspaceMeta, cfg config.ExternalWorkload) (client.Client, error) {
	var tlsCfg *tls.Config
	sec := config.GetGlobalConfig().Security
	if len(sec.ClusterSSLCA) > 0 {
		clusterSec := sec.ClusterSecurity()
		var err error
		tlsCfg, err = clusterSec.ToTLSConfig()
		if err != nil {
			return nil, errors.Annotate(err, "build external workload TLS config")
		}
	}
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	cli, err := client.New(dialCtx, &client.Option{
		KeyspaceID:     keyspaceMeta.GetId(),
		KeyspaceName:   keyspaceMeta.GetName(),
		TiDBPool:       cfg.TidbPool,
		ControllerAddr: cfg.APIServerAddr,
		TLSConfig:      tlsCfg,
	})
	if err != nil {
		return nil, err
	}
	if err := cli.Ping(dialCtx); err != nil {
		if closeErr := cli.Close(); closeErr != nil {
			logutil.BgLogger().Warn("failed to close external workload client after ping failure",
				zap.Error(closeErr))
		}
		return nil, errors.Annotate(err, "ping external workload controller")
	}
	return cli, nil
}

func (m *manager) Role() string                   { return m.role }
func (m *manager) Meta() *keyspacepb.KeyspaceMeta { return m.meta }

// bumpCounter increments the per-event counter exposed by pkg/metrics.
func bumpCounter(workerType, action string) {
	metrics.ExternalWorkloadTaskCounter.WithLabelValues(workerType, action).Inc()
}

func (m *manager) InitializeGCV2(ctx context.Context) error {
	bumpCounter(WorkerTypeGCV2, metrics.WorkerActionInit)
	return m.cli.RegisterGCV2(ctx, 0, defGCLifeTimeSec)
}

func (m *manager) AbortGCV2(ctx context.Context) error {
	bumpCounter(WorkerTypeGCV2, metrics.WorkerActionAbort)
	return m.cli.RecycleGCV2(ctx, math.MaxUint64)
}

func (m *manager) RegisterGCV2(ctx context.Context, safePoint uint64, gcLifeTime int64) error {
	bumpCounter(WorkerTypeGCV2, metrics.WorkerActionRegister)
	return m.cli.RegisterGCV2(ctx, safePoint, gcLifeTime)
}

func (m *manager) RecycleGCV2(ctx context.Context, safePoint uint64) error {
	bumpCounter(WorkerTypeGCV2, metrics.WorkerActionRecycle)
	return m.cli.RecycleGCV2(ctx, safePoint)
}

func (m *manager) UpdateGCLifeTime(ctx context.Context, gcLifeTime int64) error {
	return m.cli.UpdateGCLifeTime(ctx, gcLifeTime)
}

func (m *manager) RegisterTTLTask(ctx context.Context, tableID int64, ttlJobEnable bool) error {
	bumpCounter(WorkerTypeTTL, metrics.WorkerActionRegister)
	return m.cli.RegisterTTLTask(ctx, tableID, ttlJobEnable)
}

func (m *manager) DeleteTTLTableInfo(ctx context.Context, tableID int64) error {
	return m.cli.DeleteTTLTableInfo(ctx, tableID)
}

func (m *manager) RecycleTTLTask(ctx context.Context, completedJobCreateTime uint64) error {
	bumpCounter(WorkerTypeTTL, metrics.WorkerActionRecycle)
	return m.cli.RecycleTTLTask(ctx, completedJobCreateTime)
}

func (m *manager) UpdateTTLJobEnable(ctx context.Context, ttlJobEnable bool) error {
	return m.cli.UpdateTTLJobEnable(ctx, ttlJobEnable)
}

func (m *manager) RegisterAutoAnalyze(ctx context.Context, taskID uint64) error {
	bumpCounter(WorkerTypeAutoAnalyze, metrics.WorkerActionRegister)
	return m.cli.RegisterAutoAnalyze(ctx, taskID)
}

func (m *manager) RecycleAutoAnalyze(ctx context.Context, taskID uint64) error {
	bumpCounter(WorkerTypeAutoAnalyze, metrics.WorkerActionRecycle)
	return m.cli.RecycleAutoAnalyze(ctx, taskID)
}
