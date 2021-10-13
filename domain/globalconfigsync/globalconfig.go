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
	c.NotifyCh <- ConfigEntry{
		name:  name,
		value: value,
	}
}

func (c *GlobalConfigSyncer) StoreGlobalConfig(ctx context.Context, entry ConfigEntry) error {
	if c.etcdCli == nil {
		return nil
	}

	if !c.needUpdate(entry) {
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
