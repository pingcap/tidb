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

package domain

import (
	"context"
	"time"

	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sqlblocklist"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/etcd"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// LoadSQLBlocklistLoop creates a goroutine that reloads SQL blocklist data in a loop.
func (do *Domain) LoadSQLBlocklistLoop(ctx sessionctx.Context) error {
	ctx.GetSessionVars().InRestrictedSQL = true
	if err := sqlblocklist.Load(ctx); err != nil {
		return err
	}
	var watchCh clientv3.WatchChan
	duration := 30 * time.Second
	if do.etcdClient != nil {
		watchCh = do.etcdClient.Watch(do.ctx, sqlBlocklistKey)
	}

	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Info("LoadSQLBlocklistLoop exited.")
		}()
		defer tidbutil.Recover(metrics.LabelDomain, "LoadSQLBlocklistLoop", nil, false)

		var count int
		for {
			select {
			case <-do.exit:
				return
			case _, ok := <-watchCh:
				if !ok {
					if do.etcdClient == nil {
						continue
					}
					logutil.BgLogger().Warn("LoadSQLBlocklistLoop watch channel closed")
					watchCh = do.etcdClient.Watch(do.ctx, sqlBlocklistKey)
					count++
					if count > 10 {
						time.Sleep(time.Duration(count) * time.Second)
					}
					continue
				}
			case <-time.After(duration):
			}

			count = 0
			if err := sqlblocklist.Load(ctx); err != nil {
				logutil.BgLogger().Warn("LoadSQLBlocklistLoop failed", zap.Error(err))
			}
		}
	}, "LoadSQLBlocklistLoop")
	return nil
}

// NotifyUpdateSQLBlocklist updates the SQL blocklist key in etcd and reloads it locally.
func (do *Domain) NotifyUpdateSQLBlocklist(ctx sessionctx.Context) error {
	if do.etcdClient != nil {
		if err := ddlutil.PutKVToEtcd(context.Background(), do.etcdClient, etcd.KeyOpDefaultRetryCnt, sqlBlocklistKey, ""); err != nil {
			logutil.BgLogger().Warn("notify update sql blocklist failed", zap.Error(err))
		}
	}
	return sqlblocklist.Load(ctx)
}
