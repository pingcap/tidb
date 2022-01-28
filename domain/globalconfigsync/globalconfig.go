// Copyright 2021 PingCAP, Inc.
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

package globalconfigsync

import (
	"context"

	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	globalConfigPath     = "/global/config/"
	keyOpDefaultRetryCnt = 5
)

// GlobalConfigSyncer stores global config into etcd.
type GlobalConfigSyncer struct {
	etcdCli  *clientv3.Client
	NotifyCh chan ConfigEntry
}

// NewGlobalConfigSyncer returns a new GlobalConfigSyncer.
func NewGlobalConfigSyncer(etcdCli *clientv3.Client) *GlobalConfigSyncer {
	return &GlobalConfigSyncer{
		etcdCli:  etcdCli,
		NotifyCh: make(chan ConfigEntry, 8),
	}
}

// ConfigEntry contain the global config Name and Value.
type ConfigEntry struct {
	Name  string
	Value string
}

// Notify sends the config entry into notify channel.
func (c *GlobalConfigSyncer) Notify(name, value string) {
	c.NotifyCh <- ConfigEntry{Name: name, Value: value}
}

// StoreGlobalConfig stores the global config into etcd.
func (c *GlobalConfigSyncer) StoreGlobalConfig(ctx context.Context, entry ConfigEntry) error {
	if c.etcdCli == nil {
		return nil
	}

	key := globalConfigPath + entry.Name
	err := util.PutKVToEtcd(ctx, c.etcdCli, keyOpDefaultRetryCnt, key, entry.Value)
	if err != nil {
		return err
	}
	logutil.BgLogger().Info("store global config", zap.String("Name", entry.Name), zap.String("Value", entry.Value))
	return nil
}

// SetEtcdClient exports for testing.
func (c *GlobalConfigSyncer) SetEtcdClient(etcdCli *clientv3.Client) {
	c.etcdCli = etcdCli
}
