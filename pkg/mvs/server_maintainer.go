package utils

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type serverInfo struct {
	ID string
}

type ServerHelper interface {
	serverFilter(serverInfo) bool
	getServerInfo() (serverInfo, error)
	getAllServerInfo(ctx context.Context) (map[string]serverInfo, error)
}

type ServerConsistentHash struct {
	servers map[string]serverInfo
	chash   ConsistentHash // use consistent hash to reduce task movements after nodes changed
	mu      sync.RWMutex
	ID      string // current server ID
	helper  ServerHelper
}

func NewServerConsistentHash(replicas int, helper ServerHelper) *ServerConsistentHash {
	return &ServerConsistentHash{
		servers: make(map[string]serverInfo),
		chash:   *NewConsistentHash(replicas),
		helper:  helper,
	}
}

// Init initializes current server ID. It keeps retrying until successful.
func (sch *ServerConsistentHash) Init() {
	if sch == nil {
		return
	}
	backoff := time.Second
	for {
		info, err := sch.helper.getServerInfo()
		if err == nil {
			sch.mu.Lock()
			sch.ID = info.ID
			sch.mu.Unlock()
			break
		}
		logutil.BgLogger().Warn("get local TiDB server info failed", zap.Error(err))

		backoff = min(backoff, time.Second*5)
		time.Sleep(backoff)
		backoff *= 2
	}

	for {
		err := sch.Fetch(context.Background())
		// only break when refresh is successful
		if err == nil {
			break
		}
		logutil.BgLogger().Warn("initial server consistent hash refresh failed", zap.Error(err))
		backoff = min(backoff, time.Second*5)
		time.Sleep(backoff)
		backoff *= 2
	}
}

func (sch *ServerConsistentHash) addServer(srv serverInfo) {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	sch.servers[srv.ID] = srv
	sch.chash.AddNode(srv.ID)
}

func (sch *ServerConsistentHash) removeServer(srvID string) {
	sch.mu.Lock()
	defer sch.mu.Unlock()
	delete(sch.servers, srvID)
	sch.chash.RemoveNode(srvID)
}

func (sch *ServerConsistentHash) Fetch(ctx context.Context) error {
	newServerInfos, err := sch.helper.getAllServerInfo(ctx)
	if err != nil {
		logutil.BgLogger().Warn("get available TiDB nodes failed", zap.Error(err))
		return err
	}
	// filter servers by the given filter function
	for k, v := range newServerInfos {
		if !sch.helper.serverFilter(v) {
			delete(newServerInfos, k)
		}
	}

	{ // if no change, return directly
		sch.mu.RLock()

		noChanged := len(sch.servers) == len(newServerInfos)
		if noChanged {
			for id := range newServerInfos {
				if _, ok := sch.servers[id]; !ok {
					noChanged = false
					break
				}
			}
		}

		sch.mu.RUnlock()

		if noChanged {
			return nil
		}
	}
	{
		sch.mu.Lock() // guard server map and hash ring rebuild

		sch.servers = newServerInfos
		RebuildFromMap(&sch.chash, sch.servers)

		sch.mu.Unlock() // release guard after rebuild
	}
	return nil
}

func (sch *ServerConsistentHash) ToServerID(key string) string {
	sch.mu.RLock()
	defer sch.mu.RUnlock()
	return sch.chash.GetNode(key)
}

func (sch *ServerConsistentHash) Available(key string) bool {
	return sch.ToServerID(key) == sch.ID
}
