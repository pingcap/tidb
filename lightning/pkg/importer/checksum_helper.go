// Copyright 2023 PingCAP, Inc.
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

package importer

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"go.uber.org/zap"
)

// NewChecksumManager creates a new checksum manager.
func NewChecksumManager(ctx context.Context, rc *Controller, store kv.Storage) (local.ChecksumManager, error) {
	// if we don't need checksum, just return nil
	if rc.cfg.TikvImporter.Backend == config.BackendTiDB || rc.cfg.PostRestore.Checksum == config.OpLevelOff {
		return nil, nil
	}

	pdVersion, err := pdutil.FetchPDVersion(ctx, rc.pdHTTPCli)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// for v4.0.0 or upper, we can use the gc ttl api
	var manager local.ChecksumManager
	if pdVersion.Major >= 4 && !rc.cfg.PostRestore.ChecksumViaSQL {
		backoffWeight, err := common.GetBackoffWeightFromDB(ctx, rc.db)
		// only set backoff weight when it's smaller than default value
		if err == nil && backoffWeight >= local.DefaultBackoffWeight {
			log.FromContext(ctx).Info("get tidb_backoff_weight", zap.Int("backoff_weight", backoffWeight))
		} else {
			log.FromContext(ctx).Info("set tidb_backoff_weight to default", zap.Int("backoff_weight", local.DefaultBackoffWeight))
			backoffWeight = local.DefaultBackoffWeight
		}

		manager = local.NewTiKVChecksumManager(store.GetClient(), rc.pdCli, uint(rc.cfg.TiDB.DistSQLScanConcurrency), backoffWeight, rc.resourceGroupName, rc.taskType)
	} else {
		manager = local.NewTiDBChecksumExecutor(rc.db)
	}

	return manager, nil
}

// DoChecksum do checksum for tables.
// table should be in <db>.<table>, format.  e.g. foo.bar
func DoChecksum(ctx context.Context, table *checkpoints.TidbTableInfo) (*local.RemoteChecksum, error) {
	var err error
	manager, ok := ctx.Value(&checksumManagerKey).(local.ChecksumManager)
	if !ok {
		return nil, errors.New("No gcLifeTimeManager found in context, check context initialization")
	}

	task := log.FromContext(ctx).With(zap.String("table", table.Name)).Begin(zap.InfoLevel, "remote checksum")

	cs, err := manager.Checksum(ctx, table)
	dur := task.End(zap.ErrorLevel, err)
	if m, ok := metric.FromContext(ctx); ok {
		m.ChecksumSecondsHistogram.Observe(dur.Seconds())
	}

	return cs, err
}
