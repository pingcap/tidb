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

package domain

import (
	"context"
	"time"

	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sqlblacklist"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/etcd"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// LoadSQLBlacklistLoop creates a goroutine that reloads SQL blacklist data in a loop.
func (do *Domain) LoadSQLBlacklistLoop(ctx sessionctx.Context) error {
	ctx.GetSessionVars().InRestrictedSQL = true
	if err := sqlblacklist.Load(ctx); err != nil {
		return err
	}
	var watchCh clientv3.WatchChan
	duration := 30 * time.Second
	if do.etcdClient != nil {
		watchCh = do.etcdClient.Watch(do.ctx, sqlBlacklistKey)
	}

	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Info("LoadSQLBlacklistLoop exited.")
		}()
		defer tidbutil.Recover(metrics.LabelDomain, "LoadSQLBlacklistLoop", nil, false)

		var count int
		for {
			ok := true
			select {
			case <-do.exit:
				return
			case _, ok = <-watchCh:
			case <-time.After(duration):
			}

			if !ok {
				if do.etcdClient == nil {
					continue
				}
				logutil.BgLogger().Warn("LoadSQLBlacklistLoop watch channel closed")
				watchCh = do.etcdClient.Watch(do.ctx, sqlBlacklistKey)
				count++
				if count > 10 {
					time.Sleep(time.Duration(count) * time.Second)
				}
				continue
			}
			count = 0
			if err := sqlblacklist.Load(ctx); err != nil {
				logutil.BgLogger().Warn("LoadSQLBlacklistLoop failed", zap.Error(err))
			}
		}
	}, "LoadSQLBlacklistLoop")
	return nil
}

// NotifyUpdateSQLBlacklist updates the SQL blacklist key in etcd and reloads it locally.
func (do *Domain) NotifyUpdateSQLBlacklist(ctx sessionctx.Context) error {
	if do.etcdClient != nil {
		if err := ddlutil.PutKVToEtcd(context.Background(), do.etcdClient, etcd.KeyOpDefaultRetryCnt, sqlBlacklistKey, ""); err != nil {
			logutil.BgLogger().Warn("notify update sql blacklist failed", zap.Error(err))
		}
	}
	return sqlblacklist.Load(ctx)
}
