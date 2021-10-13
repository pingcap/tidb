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
	"path"
	"sync"

	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	globalConfigPath     = "/global/config"
	keyOpDefaultRetryCnt = 5
)

type GlobalConfigSyncer struct {
	sync.Mutex
	cache    map[string]string
	etcdCli  *clientv3.Client
	NotifyCh chan ConfigEntry
}

func NewGlobalConfigSyncer(etcdCli *clientv3.Client) *GlobalConfigSyncer {
	return &GlobalConfigSyncer{
		cache:    map[string]string{},
		etcdCli:  etcdCli,
		NotifyCh: make(chan ConfigEntry, 8),
	}
}

type ConfigEntry struct {
	name  string
	value string
}

func (c *GlobalConfigSyncer) Notify(name, value string) {
	c.NotifyCh <- ConfigEntry{name: name, value: value}
}

func (c *GlobalConfigSyncer) StoreGlobalConfig(ctx context.Context, entry ConfigEntry) error {
	if c.etcdCli == nil || !c.needUpdate(entry) {
		return nil
	}

	key := path.Join(globalConfigPath, entry.name)
	err := util.PutKVToEtcd(ctx, c.etcdCli, keyOpDefaultRetryCnt, key, entry.value)
	if err != nil {
		return err
	}
	c.updateCache(entry)
	logutil.BgLogger().Info("global config store", zap.String("name", entry.name), zap.String("value", entry.value))
	return nil
}

func (c *GlobalConfigSyncer) needUpdate(entry ConfigEntry) bool {
	c.Lock()
	oldValue, exist := c.cache[entry.name]
	c.Unlock()
	return !exist || oldValue != entry.value
}

func (c *GlobalConfigSyncer) updateCache(entry ConfigEntry) {
	c.Lock()
	c.cache[entry.name] = entry.value
	c.Unlock()
}
