package mvs

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
	ctx     context.Context
	servers map[string]serverInfo
	chash   ConsistentHash // use consistent hash to reduce task movements after nodes changed
	mu      sync.RWMutex
	ID      string // current server ID
	helper  ServerHelper
}

func NewServerConsistentHash(ctx context.Context, replicas int, helper ServerHelper) *ServerConsistentHash {
	if ctx == nil {
		ctx = context.Background()
	}
	if helper == nil {
		panic("ServerHelper cannot be nil")
	}
	return &ServerConsistentHash{
		ctx:     ctx,
		servers: make(map[string]serverInfo),
		chash:   *NewConsistentHash(replicas),
		helper:  helper,
	}
}

func (sch *ServerConsistentHash) init() bool {
	backoff := time.Second
	for {
		info, err := sch.helper.getServerInfo() // get local server info, if failed, retry until success, otherwise the server will never be added to the hash ring and won't receive any task
		if err == nil {
			sch.mu.Lock()

			sch.ID = info.ID

			sch.mu.Unlock()
			return true
		}

		waitBackoff := min(backoff, time.Second*5)
		logutil.BgLogger().Warn("get local TiDB server info failed, retrying after backoff", zap.Error(err), zap.Duration("backoff", waitBackoff))
		select {
		case <-sch.ctx.Done():
			return false
		case <-time.After(waitBackoff):
			backoff = waitBackoff * 2
		}
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

func (sch *ServerConsistentHash) refresh() error {
	newServerInfos, err := sch.helper.getAllServerInfo(sch.ctx)
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
